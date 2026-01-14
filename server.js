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

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 60 * 1024 * 1024 }
});

// ====== SETTINGS ======
const EXTRA_TAIL_MS = 4000;        // video = audio + 4s
const FFMPEG_TIMEOUT_MS = 240000;  // kill ffmpeg if it runs too long

app.get("/health", (_, res) => res.status(200).json({ ok: true }));

// ---------- Download + cache helpers ----------
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function downloadToFile(url, outPath, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;

    const req = client.get(url, (res) => {
      // redirects
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
        reject(new Error(`Failed to download asset: ${res.statusCode}`));
        return;
      }

      const file = fs.createWriteStream(outPath);
      res.pipe(file);

      file.on("finish", () => file.close(resolve));
      file.on("error", reject);
      res.on("error", reject);
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Download timed out after ${timeoutMs}ms`));
    });

    req.on("error", reject);
  });
}

async function getOrDownload(url) {
  const urlObj = new URL(url);
  const ext = path.extname(urlObj.pathname || "") || ".bin";
  const key = sha1(url);
  const cached = path.join(CACHE_DIR, `${key}${ext}`);

  if (fs.existsSync(cached) && fs.statSync(cached).size > 0) return cached;

  const tmp = cached + ".part";
  await downloadToFile(url, tmp, 20000);
  fs.renameSync(tmp, cached);
  return cached;
}

// ---------- ffmpeg helpers ----------
function runFfmpeg(args, timeoutMs = FFMPEG_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const child = execFile(
      "ffmpeg",
      args,
      { maxBuffer: 1024 * 1024 * 60 },
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

// ---------- Subtitle helpers ----------
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

function wrapTwoLines(text, maxLen = 26) {
  const t = String(text ?? "").trim();
  if (t.length <= maxLen) return t;

  const words = t.split(/\s+/);
  let line1 = "";
  let line2 = "";

  for (const w of words) {
    if (!line1) {
      line1 = w;
      continue;
    }
    if ((line1 + " " + w).length <= maxLen) {
      line1 += " " + w;
      continue;
    }
    if (!line2) {
      line2 = w;
      continue;
    }
    if ((line2 + " " + w).length <= maxLen) {
      line2 += " " + w;
    } else {
      break;
    }
  }

  if (!line2) return t.slice(0, maxLen) + "\\N" + t.slice(maxLen, maxLen * 2);
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
    const end = Math.max(l.end_ms, start + 200);
    out.push({ start_ms: start, end_ms: end, text: l.text });
    prevEnd = end;
  }
  return out;
}

function enforceConsecutiveWithClamp(lines, minDur = 650, maxDur = 2200) {
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

function proportionalFitToTarget(lines, targetMs) {
  if (!Array.isArray(lines) || lines.length === 0) return [];
  const lastEnd = lines[lines.length - 1].end_ms;
  if (!lastEnd || lastEnd <= 0) return lines;

  const delta = targetMs - lastEnd;
  if (Math.abs(delta) <= 350) {
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

  const normalized = enforceConsecutiveWithClamp(scaled, 650, 2200);
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

const DEJAVU_TTF = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// ---------- Main render endpoint ----------
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

    // ---- NEW ASSETS (Option 3) ----
    const baseBgUrl =
      payload?.assets?.base_background_url ||
      payload?.assets?.background_template_url; // backward compat

    const endCardUrl = payload?.assets?.end_card_url || null;

    const cardUrls = Array.isArray(payload?.assets?.card_image_urls)
      ? payload.assets.card_image_urls.filter((u) => typeof u === "string" && u.trim()).slice(0, 3)
      : [];

    const title = payload?.text?.title ?? "";
    const footer = payload?.text?.footer ?? "";
    const rawLines = payload?.subtitles?.lines ?? [];

    if (!baseBgUrl) return res.status(400).json({ error: "payload.assets.base_background_url is required." });
    if (!endCardUrl) return res.status(400).json({ error: "payload.assets.end_card_url is required." });
    if (!Array.isArray(rawLines)) return res.status(400).json({ error: "payload.subtitles.lines must be an array." });

    workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mxrender-"));

    const audioPath = path.join(workDir, "audio.mp3");
    fs.writeFileSync(audioPath, req.file.buffer);

    const publicName = `mx_${Date.now()}_${Math.random().toString(16).slice(2)}.mp4`;
    const publicOutPath = path.join(PUBLIC_DIR, publicName);

    // download/cached assets
    const baseBgFile = await getOrDownload(baseBgUrl);
    const endCardFile = await getOrDownload(endCardUrl);
    const cardFiles = [];
    for (const u of cardUrls) cardFiles.push(await getOrDownload(u));

    // measure audio
    let audioMs = await getAudioDurationMs(audioPath);
    if (!audioMs) audioMs = 10000;

    const audioSec = (audioMs / 1000).toFixed(3);

    // subtitles target = audio length
    const subsTargetMs = audioMs;

    // video target = audio length + 4s
    const videoTargetMs = audioMs + EXTRA_TAIL_MS;
    const videoTargetSec = (videoTargetMs / 1000).toFixed(3);

    // ---- subtitles pipeline ----
    let lines = normalizeSubtitleLines(rawLines);
    lines = enforceConsecutiveWithClamp(lines, 650, 2200);
    lines = proportionalFitToTarget(lines, subsTargetMs);
    lines = enforceConsecutiveWithClamp(lines, 650, 2200);
    if (lines.length) lines[lines.length - 1].end_ms = subsTargetMs;

    const assPath = path.join(workDir, "subs.ass");

    const ASS_FONT = "DejaVu Sans";
    const SUB_FONT_SIZE = 56;
    const OUTLINE = 6;
    const SHADOW = 1;
    const MARGIN_LR = 120;
    const MARGIN_V = 420;

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
        const wrapped = wrapTwoLines(clean, 26);
        return `Dialogue: 0,${start},${end},Default,,0,0,0,,${wrapped}`;
      })
      .join("\n");

    fs.writeFileSync(assPath, assHeader + assEvents + "\n");

    // ---- drawtext (ONLY during audio) ----
    const tTitle = escapeDrawtext(title);
    const tFooter = escapeDrawtext(footer);

    const titleFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=82:fontcolor=white:` +
      `x=(w-text_w)/2:y=220:text='${tTitle}':shadowx=0:shadowy=2:shadowcolor=black@0.35:` +
      `enable=lt(t\\,${audioSec})`;

    const footerFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=46:fontcolor=white:` +
      `x=(w-text_w)/2:y=h-260:text='${tFooter}':shadowx=0:shadowy=2:shadowcolor=black@0.35:` +
      `enable=lt(t\\,${audioSec})`;

    // ---- card overlay plan (3 segments across audio) ----
    const seg1 = (audioMs / 1000 / 3).toFixed(3);
    const seg2 = (audioMs / 1000 * 2 / 3).toFixed(3);
    const audioEnd = (audioMs / 1000).toFixed(3);

    const CARD_W = 820;
    const CARD_H = 820;
    const CARD_X = `(W-${CARD_W})/2`;
    const CARD_Y = `560`;

    // Inputs:
    // 0: base background
    // 1..N: cards
    // endCardIndex: end card
    // audioIndex: audio
    const inputArgs = [];
    inputArgs.push("-loop", "1", "-i", baseBgFile);
    for (const f of cardFiles) inputArgs.push("-loop", "1", "-i", f);
    inputArgs.push("-loop", "1", "-i", endCardFile);
    inputArgs.push("-i", audioPath);

    const endCardInputIndex = 1 + cardFiles.length;
    const audioInputIndex = endCardInputIndex + 1;

    // Filter graph
    const parts = [];
    parts.push(`[0:v]scale=1080:1920,format=rgba[base];`);
    parts.push(`[${endCardInputIndex}:v]scale=1080:1920,format=rgba[end];`);

    let current = "[base]";
    for (let i = 0; i < cardFiles.length; i++) {
      const inIdx = 1 + i;
      const label = `card${i + 1}`;
      parts.push(
        `[${inIdx}:v]scale=${CARD_W}:${CARD_H}:force_original_aspect_ratio=cover,` +
          `crop=${CARD_W}:${CARD_H},format=rgba[${label}];`
      );

      let enableExpr = "";
      if (i === 0) enableExpr = `between(t\\,0\\,${seg1})`;
      if (i === 1) enableExpr = `between(t\\,${seg1}\\,${seg2})`;
      if (i === 2) enableExpr = `between(t\\,${seg2}\\,${audioEnd})`;

      const out = `v_card_${i + 1}`;
      parts.push(`${current}[${label}]overlay=x=${CARD_X}:y=${CARD_Y}:enable=${enableExpr}[${out}];`);
      current = `[${out}]`;
    }

    parts.push(`${current}${titleFilter}[v_t1];`);
    parts.push(`[v_t1]${footerFilter}[v_t2];`);
    parts.push(`[v_t2]subtitles=${assPath}[v_sub];`);

    // End card full overlay for last 4 seconds
    parts.push(`[v_sub][end]overlay=x=0:y=0:enable=between(t\\,${audioEnd}\\,${videoTargetSec})[vout]`);

    // CRITICAL: no trailing ';' at end of filter_complex
    const filterComplex = parts.join("").replace(/;$/, "");

    const audioFilter = `atrim=0:${audioSec},asetpts=N/SR/TB`;

    const args = [
      "-y",
      ...inputArgs,
      "-filter_complex",
      filterComplex,
      "-map",
      "[vout]",
      "-map",
      `${audioInputIndex}:a`,
      "-t",
      videoTargetSec,
      "-r",
      "30",
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
      "192k",
      publicOutPath
    ];

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
        cards_used: cardFiles.length
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

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render endpoint running on :${port}`));
