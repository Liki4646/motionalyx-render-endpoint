// server.js — Motionalyx Render Endpoint (NO speed-up)
// - Video length = audio length + 4s
// - Subtitles length = exactly audio length (NOT +4s)
// - Subtitles timeline is proportionally fit to audio length
// - Premium typography + safe margins defaults (overrideable via payload.style)
// - Background download cached + timeout
// - ffmpeg kill timeout

import express from "express";
import multer from "multer";
import { execFile } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";
import https from "https";
import http from "http";
import crypto from "crypto";

const app = express();
app.set("trust proxy", 1);

// --------- Storage / static output ---------
const PUBLIC_DIR = path.join(os.tmpdir(), "mxpublic");
const CACHE_DIR = path.join(os.tmpdir(), "mxcache");
fs.mkdirSync(PUBLIC_DIR, { recursive: true });
fs.mkdirSync(CACHE_DIR, { recursive: true });

app.use(
  "/tmp",
  express.static(PUBLIC_DIR, {
    maxAge: "6h",
    immutable: false,
    setHeaders: (res) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Accept-Ranges", "bytes");
    }
  })
);

// --------- Upload ---------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 60 * 1024 * 1024 }
});

// --------- Core rules ---------
const EXTRA_TAIL_MS = 4000;
const FFMPEG_TIMEOUT_MS = 240000;
const BG_DOWNLOAD_TIMEOUT_MS = 20000;

// --------- Health ---------
app.get("/health", (_, res) => res.status(200).json({ ok: true }));

// --------- Helpers ---------
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function downloadToFile(url, outPath, timeoutMs = BG_DOWNLOAD_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;

    const req = client.get(url, (res) => {
      // follow redirects
      if (
        res.statusCode &&
        res.statusCode >= 300 &&
        res.statusCode < 400 &&
        res.headers.location
      ) {
        res.resume();
        return downloadToFile(res.headers.location, outPath, timeoutMs)
          .then(resolve)
          .catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`Failed to download background: ${res.statusCode}`));
        return;
      }

      const file = fs.createWriteStream(outPath);
      res.pipe(file);

      file.on("finish", () => file.close(resolve));
      file.on("error", reject);
      res.on("error", reject);
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Background download timed out after ${timeoutMs}ms`));
    });

    req.on("error", reject);
  });
}

async function getOrDownloadBackground(bgUrl) {
  const urlObj = new URL(bgUrl);
  const ext = path.extname(urlObj.pathname || "") || ".bin";
  const key = sha1(bgUrl);
  const cached = path.join(CACHE_DIR, `${key}${ext}`);

  if (fs.existsSync(cached) && fs.statSync(cached).size > 0) return cached;

  const tmp = cached + ".part";
  await downloadToFile(bgUrl, tmp, BG_DOWNLOAD_TIMEOUT_MS);
  fs.renameSync(tmp, cached);
  return cached;
}

function runFfmpeg(args, timeoutMs = FFMPEG_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const child = execFile(
      "ffmpeg",
      args,
      { maxBuffer: 1024 * 1024 * 80 },
      (err, stdout, stderr) => {
        if (err) reject(new Error(stderr || err.message));
        else resolve({ stdout, stderr });
      }
    );

    const t = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`ffmpeg timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("exit", () => clearTimeout(t));
  });
}

function runFfprobe(args) {
  return new Promise((resolve, reject) => {
    execFile("ffprobe", args, { maxBuffer: 1024 * 1024 * 10 }, (err, stdout, stderr) => {
      if (err) reject(new Error(stderr || err.message));
      else resolve({ stdout, stderr });
    });
  });
}

async function getAudioDurationMs(audioPath) {
  const { stdout } = await runFfprobe([
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=nw=1:nk=1",
    audioPath
  ]);
  const seconds = parseFloat(String(stdout).trim());
  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  return Math.round(seconds * 1000);
}

const clamp = (n, min, max) => Math.max(min, Math.min(max, n));

function msToAssTime(ms) {
  const total = Math.max(0, Math.floor(ms));
  const cs = Math.floor((total % 1000) / 10);
  const s = Math.floor(total / 1000) % 60;
  const m = Math.floor(total / 60000) % 60;
  const h = Math.floor(total / 3600000);
  const pad = (n, w = 2) => String(n).padStart(w, "0");
  return `${h}:${pad(m)}:${pad(s)}.${pad(cs)}`;
}

function escapeAssText(text) {
  let t = String(text ?? "");
  t = t.replace(/[\r\n]+/g, " ").trim();
  t = t.replace(/[{}]/g, "");
  t = t.replace(/\\/g, "\\\\");
  return t;
}

// Wrap to max 2 lines with maxChars per line, ASS uses \N for newline.
function wrapTwoLines(text, maxChars = 24) {
  const t = String(text ?? "").trim();
  if (!t) return "";
  if (t.length <= maxChars) return t;

  const words = t.split(/\s+/);
  let line1 = "";
  let line2 = "";

  for (const w of words) {
    if (!line1) {
      line1 = w;
      continue;
    }
    if ((line1 + " " + w).length <= maxChars) {
      line1 += " " + w;
      continue;
    }
    if (!line2) {
      line2 = w;
      continue;
    }
    if ((line2 + " " + w).length <= maxChars) {
      line2 += " " + w;
    } else {
      break;
    }
  }

  if (!line2) {
    const a = t.slice(0, maxChars).trim();
    const b = t.slice(maxChars, maxChars * 2).trim();
    return b ? `${a}\\N${b}` : a;
  }

  return `${line1}\\N${line2}`;
}

function normalizeSubtitleLines(rawLines) {
  const cleaned = (Array.isArray(rawLines) ? rawLines : [])
    .filter((l) => typeof l?.start_ms === "number" && typeof l?.end_ms === "number" && l?.text)
    .map((l) => ({
      start_ms: Math.max(0, Math.floor(l.start_ms)),
      end_ms: Math.max(0, Math.floor(l.end_ms)),
      text: String(l.text)
    }))
    .sort((a, b) => a.start_ms - b.start_ms);

  let prevEnd = 0;
  const out = [];
  for (const l of cleaned) {
    const start = Math.max(l.start_ms, prevEnd);
    const end = Math.max(l.end_ms, start + 150);
    out.push({ start_ms: start, end_ms: end, text: l.text });
    prevEnd = end;
  }
  return out;
}

// Makes subtitles consecutive with min/max duration per line (snappier defaults).
function enforceConsecutiveWithClamp(lines, minDur = 550, maxDur = 1700) {
  if (!Array.isArray(lines) || lines.length === 0) return [];
  const out = [];
  let cursor = 0;

  for (const l of lines) {
    const origDur = Math.max(1, l.end_ms - l.start_ms);
    const dur = clamp(origDur, minDur, maxDur);
    const start = cursor;
    const end = start + dur;
    out.push({ start_ms: start, end_ms: end, text: l.text });
    cursor = end;
  }

  return out;
}

// Proportionally fits the whole subtitle timeline to exactly targetMs (audio).
function proportionalFitToTarget(lines, targetMs) {
  if (!Array.isArray(lines) || lines.length === 0) return [];
  const lastEnd = lines[lines.length - 1].end_ms;
  if (!lastEnd || lastEnd <= 0) return lines;

  const delta = targetMs - lastEnd;
  if (Math.abs(delta) <= 250) {
    const out = [...lines];
    out[out.length - 1] = { ...out[out.length - 1], end_ms: targetMs };
    return out;
  }

  const scale = targetMs / lastEnd;
  const scaled = lines.map((l) => ({
    start_ms: Math.floor(l.start_ms * scale),
    end_ms: Math.floor(l.end_ms * scale),
    text: l.text
  }));

  const normalized = enforceConsecutiveWithClamp(scaled, 550, 1700);
  normalized[normalized.length - 1].end_ms = targetMs;
  return normalized;
}

function escapeDrawtext(s) {
  let t = String(s ?? "");
  t = t.replace(/\\/g, "\\\\");
  t = t.replace(/:/g, "\\:");
  t = t.replace(/'/g, "\\'");
  t = t.replace(/%/g, "\\%");
  t = t.replace(/[\r\n]+/g, " ").trim();
  return t;
}

// --------- Typography defaults (premium, safe margins) ---------
const DEJAVU_TTF = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

const DEFAULT_STYLE = {
  safe: {
    // Title y is absolute; footer y is from bottom.
    top_y: 210,
    bottom_y_from_bottom: 300
  },
  title: {
    size: 78,
    box: true,
    box_alpha: 0.22,
    box_border: 18,
    shadow_alpha: 0.35
  },
  footer: {
    size: 44,
    box: false,
    shadow_alpha: 0.35
  },
  subs: {
    font: "DejaVu Sans",
    size: 54,
    outline: 5,
    shadow: 1,
    margin_lr: 140,
    margin_v: 340,
    max_chars: 24,
    min_dur: 550,
    max_dur: 1700
  }
};

function readStyle(payloadStyle) {
  // Allow payload.style overrides but clamp hard for safety.
  const s = payloadStyle || {};

  const out = JSON.parse(JSON.stringify(DEFAULT_STYLE));

  if (s?.safe) {
    if (Number.isFinite(s.safe.top_y)) out.safe.top_y = clamp(s.safe.top_y, 120, 380);
    if (Number.isFinite(s.safe.bottom_y_from_bottom))
      out.safe.bottom_y_from_bottom = clamp(s.safe.bottom_y_from_bottom, 220, 420);
    // legacy support: bottom_y (absolute). If provided, translate to from_bottom.
    if (Number.isFinite(s.safe.bottom_y)) {
      // 1920 height assumed; clamp then convert
      const abs = clamp(s.safe.bottom_y, 1400, 1820);
      out.safe.bottom_y_from_bottom = clamp(1920 - abs, 100, 520);
    }
  }

  if (s?.title) {
    if (Number.isFinite(s.title.size)) out.title.size = clamp(s.title.size, 64, 92);
    if (typeof s.title.box === "boolean") out.title.box = s.title.box;
    if (Number.isFinite(s.title.box_alpha)) out.title.box_alpha = clamp(s.title.box_alpha, 0, 0.6);
    if (Number.isFinite(s.title.box_border)) out.title.box_border = clamp(s.title.box_border, 8, 30);
    if (Number.isFinite(s.title.shadow_alpha))
      out.title.shadow_alpha = clamp(s.title.shadow_alpha, 0, 0.7);
  }

  if (s?.footer) {
    if (Number.isFinite(s.footer.size)) out.footer.size = clamp(s.footer.size, 34, 54);
    if (typeof s.footer.box === "boolean") out.footer.box = s.footer.box;
    if (Number.isFinite(s.footer.shadow_alpha))
      out.footer.shadow_alpha = clamp(s.footer.shadow_alpha, 0, 0.7);
  }

  if (s?.subs) {
    if (Number.isFinite(s.subs.size)) out.subs.size = clamp(s.subs.size, 44, 62);
    if (Number.isFinite(s.subs.outline)) out.subs.outline = clamp(s.subs.outline, 2, 8);
    if (Number.isFinite(s.subs.shadow)) out.subs.shadow = clamp(s.subs.shadow, 0, 4);
    if (Number.isFinite(s.subs.margin_lr)) out.subs.margin_lr = clamp(s.subs.margin_lr, 90, 220);
    if (Number.isFinite(s.subs.margin_v)) out.subs.margin_v = clamp(s.subs.margin_v, 260, 520);
    if (Number.isFinite(s.subs.max_chars)) out.subs.max_chars = clamp(s.subs.max_chars, 18, 30);
    if (Number.isFinite(s.subs.min_dur)) out.subs.min_dur = clamp(s.subs.min_dur, 350, 900);
    if (Number.isFinite(s.subs.max_dur)) out.subs.max_dur = clamp(s.subs.max_dur, 900, 2600);
  }

  // ensure min <= max
  if (out.subs.min_dur > out.subs.max_dur) out.subs.min_dur = out.subs.max_dur;

  return out;
}

// --------- Route ---------
app.post("/render", upload.single("audio"), async (req, res) => {
  let workDir = null;

  try {
    if (!req.file) return res.status(400).json({ error: "Missing audio file field 'audio'." });
    if (!req.body?.payload) return res.status(400).json({ error: "Missing text field 'payload'." });

    let payload;
    try {
      payload = JSON.parse(req.body.payload);
    } catch {
      return res.status(400).json({ error: "Payload is not valid JSON." });
    }

    const bgUrl = payload?.assets?.background_template_url;
    const title = payload?.text?.title ?? "";
    const footer = payload?.text?.footer ?? "";
    const rawLines = payload?.subtitles?.lines ?? [];
    const style = readStyle(payload?.style);

    if (!bgUrl) return res.status(400).json({ error: "payload.assets.background_template_url is required." });
    if (!Array.isArray(rawLines)) return res.status(400).json({ error: "payload.subtitles.lines must be an array." });

    workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mxrender-"));

    const audioPath = path.join(workDir, "audio.mp3");
    fs.writeFileSync(audioPath, req.file.buffer);

    const publicName = `mx_${Date.now()}_${Math.random().toString(16).slice(2)}.mp4`;
    const publicOutPath = path.join(PUBLIC_DIR, publicName);

    const bgFile = await getOrDownloadBackground(bgUrl);

    // durations
    let audioMs = await getAudioDurationMs(audioPath);
    if (!audioMs) audioMs = 10000;

    const audioSec = (audioMs / 1000).toFixed(3);
    const subsTargetMs = audioMs;
    const videoTargetMs = audioMs + EXTRA_TAIL_MS;
    const videoTargetSec = (videoTargetMs / 1000).toFixed(3);

    // subtitles: normalize → clamp → fit to audio → clamp again
    let lines = normalizeSubtitleLines(rawLines);
    lines = enforceConsecutiveWithClamp(lines, style.subs.min_dur, style.subs.max_dur);
    lines = proportionalFitToTarget(lines, subsTargetMs);
    lines = enforceConsecutiveWithClamp(lines, style.subs.min_dur, style.subs.max_dur);
    if (lines.length) lines[lines.length - 1].end_ms = subsTargetMs;

    // ASS subtitles
    const assPath = path.join(workDir, "subs.ass");

    const ASS_FONT = style.subs.font;
    const SUB_FONT_SIZE = style.subs.size;
    const OUTLINE = style.subs.outline;
    const SHADOW = style.subs.shadow;
    const MARGIN_LR = style.subs.margin_lr;
    const MARGIN_V = style.subs.margin_v;
    const MAX_CHARS = style.subs.max_chars;

    const assHeader = `[Script Info]
Title: Motionalyx Subtitles
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 2
ScaledBorderAndShadow: yes
Collisions: Normal

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,${ASS_FONT},${SUB_FONT_SIZE},&H00FFFFFF,&H00FFFFFF,&H00000000,&H64000000,1,0,0,0,100,100,0,0,1,${OUTLINE},${SHADOW},2,${MARGIN_LR},${MARGIN_LR},${MARGIN_V},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
`;

    const assEvents = lines
      .map((l) => {
        const start = msToAssTime(l.start_ms);
        const end = msToAssTime(l.end_ms);
        const clean = escapeAssText(l.text);
        const wrapped = wrapTwoLines(clean, MAX_CHARS);
        return `Dialogue: 0,${start},${end},Default,,0,0,0,,${wrapped}`;
      })
      .join("\n");

    fs.writeFileSync(assPath, assHeader + assEvents + "\n");

    // Title + Footer drawtext
    const tTitle = escapeDrawtext(title);
    const tFooter = escapeDrawtext(footer);

    const titleY = style.safe.top_y;
    const footerY = `h-${style.safe.bottom_y_from_bottom}`;

    const titleShadow = clamp(style.title.shadow_alpha, 0, 0.7).toFixed(2);
    const footerShadow = clamp(style.footer.shadow_alpha, 0, 0.7).toFixed(2);

    const titleBox = style.title.box
      ? `:box=1:boxcolor=black@${clamp(style.title.box_alpha, 0, 0.6).toFixed(
          2
        )}:boxborderw=${style.title.box_border}`
      : "";

    const footerBox = style.footer.box
      ? `:box=1:boxcolor=black@0.18:boxborderw=16`
      : "";

    const titleFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=${style.title.size}:fontcolor=white` +
      `:x=(w-text_w)/2:y=${titleY}:text='${tTitle}'` +
      `:shadowx=0:shadowy=2:shadowcolor=black@${titleShadow}${titleBox}`;

    const footerFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=${style.footer.size}:fontcolor=white` +
      `:x=(w-text_w)/2:y=${footerY}:text='${tFooter}'` +
      `:shadowx=0:shadowy=2:shadowcolor=black@${footerShadow}${footerBox}`;

    const vf = `${titleFilter},${footerFilter},subtitles=${assPath}`;

    // Audio: NO speed-up, just trim to measured duration
    const audioFilter = `atrim=0:${audioSec},asetpts=N/SR/TB`;

    const isImage = /\.(png|jpg|jpeg)$/i.test(bgFile);

    const commonVideoArgs = [
      "-vf",
      vf,
      "-t",
      videoTargetSec,
      "-r",
      "30",
      "-s",
      "1080x1920",
      "-pix_fmt",
      "yuv420p",
      "-movflags",
      "+faststart",
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-crf",
      "28",
      "-af",
      audioFilter,
      "-c:a",
      "aac",
      "-b:a",
      "192k"
    ];

    const args = isImage
      ? ["-y", "-loop", "1", "-i", bgFile, "-i", audioPath, "-tune", "stillimage", ...commonVideoArgs, publicOutPath]
      : ["-y", "-stream_loop", "-1", "-i", bgFile, "-i", audioPath, ...commonVideoArgs, publicOutPath];

    await runFfmpeg(args, FFMPEG_TIMEOUT_MS);

    const proto = req.get("x-forwarded-proto") || req.protocol;
    const baseUrl = `${proto}://${req.get("host")}`;

    return res.status(200).json({
      ok: true,
      download_url: `${baseUrl}/tmp/${publicName}`,
      debug: {
        audio_ms: audioMs,
        subtitles_ms: subsTargetMs,
        video_ms: videoTargetMs,
        style_used: style
      }
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  } finally {
    try {
      if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    } catch {}
  }
});

// --------- Start ---------
const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render endpoint running on :${port}`));
