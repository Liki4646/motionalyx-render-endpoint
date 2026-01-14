import express from "express";
import multer from "multer";
import { execFile } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";
import https from "https";
import http from "http";

const app = express();
app.set("trust proxy", 1);

const PUBLIC_DIR = path.join(os.tmpdir(), "mxpublic");
fs.mkdirSync(PUBLIC_DIR, { recursive: true });

// Serve rendered files from a public temp folder
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
  limits: { fileSize: 60 * 1024 * 1024 } // 60MB
});

// Health check
app.get("/health", (_, res) => res.status(200).json({ ok: true }));

function downloadToFile(url, outPath) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    const req = client.get(url, (res) => {
      // Handle redirects (common on CDNs)
      if (
        res.statusCode &&
        res.statusCode >= 300 &&
        res.statusCode < 400 &&
        res.headers.location
      ) {
        res.resume();
        return downloadToFile(res.headers.location, outPath).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        reject(new Error(`Failed to download background: ${res.statusCode}`));
        res.resume();
        return;
      }

      const file = fs.createWriteStream(outPath);
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
      file.on("error", reject);
    });

    req.on("error", reject);
  });
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    execFile("ffmpeg", args, { maxBuffer: 1024 * 1024 * 60 }, (err, stdout, stderr) => {
      if (err) reject(new Error(stderr || err.message));
      else resolve({ stdout, stderr });
    });
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
  // Best-effort: duration in ms
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
  // Remove braces to avoid ASS override tags, normalize whitespace, escape backslashes.
  let t = String(text ?? "");
  t = t.replace(/[\r\n]+/g, " ").trim();
  t = t.replace(/[{}]/g, "");
  t = t.replace(/\\/g, "\\\\");
  return t;
}

// Wrap into max 2 lines, aiming for maxLen per line
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
      break; // max 2 lines
    }
  }

  if (!line2) {
    // hard split if we couldn't create a second line
    return t.slice(0, maxLen) + "\\N" + t.slice(maxLen, maxLen * 2);
  }
  return `${line1}\\N${line2}`;
}

function normalizeSubtitleLines(rawLines) {
  // Basic cleanup: keep valid, sort, fix negatives, ensure end>start
  const cleaned = (Array.isArray(rawLines) ? rawLines : [])
    .filter((l) => typeof l?.start_ms === "number" && typeof l?.end_ms === "number" && l?.text)
    .map((l) => ({
      start_ms: Math.max(0, Math.floor(l.start_ms)),
      end_ms: Math.max(0, Math.floor(l.end_ms)),
      text: String(l.text)
    }))
    .sort((a, b) => a.start_ms - b.start_ms);

  // Ensure increasing & no overlaps (soft)
  let prevEnd = 0;
  const out = [];
  for (const l of cleaned) {
    let start = Math.max(l.start_ms, prevEnd);
    let end = Math.max(l.end_ms, start + 200); // temporary, refined later
    out.push({ start_ms: start, end_ms: end, text: l.text });
    prevEnd = end;
  }
  return out;
}

function enforceDurationsAndConsecutive(lines, minDur = 650, maxDur = 2200) {
  // Forces consecutive chunks with min/max durations.
  // Keeps order; adjusts end times; removes overlaps/gaps.
  if (!Array.isArray(lines) || lines.length === 0) return [];

  const out = [];
  let cursor = 0;

  for (const l of lines) {
    let start = Math.max(cursor, Math.floor(l.start_ms));
    let desiredEnd = Math.floor(l.end_ms);

    // Make it consecutive: start exactly at cursor
    start = cursor;

    // Derive duration from original, then clamp
    let dur = desiredEnd - Math.floor(l.start_ms);
    if (!Number.isFinite(dur)) dur = maxDur;

    dur = clamp(dur, minDur, maxDur);
    let end = start + dur;

    out.push({ start_ms: start, end_ms: end, text: l.text });
    cursor = end;
  }

  return out;
}

function proportionalFitToTarget(lines, targetMs) {
  if (!Array.isArray(lines) || lines.length === 0) return [];

  const lastEnd = lines[lines.length - 1].end_ms;
  if (!lastEnd || lastEnd <= 0) return lines;

  const scale = targetMs / lastEnd;

  // Scale starts/ends
  const scaled = lines.map((l) => ({
    start_ms: Math.floor(l.start_ms * scale),
    end_ms: Math.floor(l.end_ms * scale),
    text: l.text
  }));

  // Re-normalize consecutive + min/max durations
  const normalized = enforceDurationsAndConsecutive(scaled, 650, 2200);

  // Force final end exactly to target (keeps exact match to audio)
  normalized[normalized.length - 1].end_ms = targetMs;

  return normalized;
}

function decideAndFitTimeline(lines, audioMs) {
  // If we can’t read audio duration, leave as-is after basic normalization
  if (!audioMs || !Number.isFinite(audioMs) || audioMs <= 0) return lines;

  // Keep reels in a sensible range (you can change these later)
  const target = clamp(audioMs, 8000, 28000);

  const lastEnd = lines.length ? lines[lines.length - 1].end_ms : 0;
  if (!lastEnd) return lines;

  const delta = target - lastEnd;

  // If it's already very close (<= 350ms), tiny fix: align final end only
  if (Math.abs(delta) <= 350) {
    const out = [...lines];
    out[out.length - 1] = { ...out[out.length - 1], end_ms: target };
    return out;
  }

  // Otherwise, do full proportional scaling (your preference)
  return proportionalFitToTarget(lines, target);
}

function escapeDrawtext(s) {
  // ffmpeg drawtext escaping: backslash, colon, apostrophe, percent
  let t = String(s ?? "");
  t = t.replace(/\\/g, "\\\\");
  t = t.replace(/:/g, "\\:");
  t = t.replace(/'/g, "\\'");
  t = t.replace(/%/g, "\\%");
  t = t.replace(/[\r\n]+/g, " ").trim();
  return t;
}

// DejaVu ttf exists with fonts-dejavu-core on Debian bookworm
const DEJAVU_TTF = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// POST /render
// multipart/form-data:
// - audio: file (mp3)
// - payload: text (JSON string)
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

    // Current payload paths (keep compatible with your Make)
    const bgUrl = payload?.assets?.background_template_url;
    const title = payload?.text?.title ?? "";
    const footer = payload?.text?.footer ?? "";
    const rawLines = payload?.subtitles?.lines ?? [];

    if (!bgUrl) return res.status(400).json({ error: "payload.assets.background_template_url is required." });
    if (!Array.isArray(rawLines)) return res.status(400).json({ error: "payload.subtitles.lines must be an array." });

    // Temp working dir
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mxrender-"));
    const audioPath = path.join(workDir, "audio.mp3");

    // Public output
    const publicName = `mx_${Date.now()}_${Math.random().toString(16).slice(2)}.mp4`;
    const publicOutPath = path.join(PUBLIC_DIR, publicName);

    fs.writeFileSync(audioPath, req.file.buffer);

    // Download background (png/jpg/mp4)
    const urlObj = new URL(bgUrl);
    const ext = path.extname(urlObj.pathname || "").replace(".", "") || "mp4";
    const bgFile = path.join(workDir, `bg.${ext}`);
    await downloadToFile(bgUrl, bgFile);

    // Measure audio duration
    let audioMs = null;
    try {
      audioMs = await getAudioDurationMs(audioPath);
    } catch {
      audioMs = null;
    }

    // Subtitle pipeline:
    // 1) normalize
    // 2) enforce reasonable durations (initial)
    // 3) proportional fit to audio target (scale everything)
    // 4) final enforce consecutive + min/max
    let lines = normalizeSubtitleLines(rawLines);
    lines = enforceDurationsAndConsecutive(lines, 650, 2200);
    lines = decideAndFitTimeline(lines, audioMs);
    lines = enforceDurationsAndConsecutive(lines, 650, 2200);

    // Build ASS subtitle file (premium defaults)
    const assPath = path.join(workDir, "subs.ass");

    // Safer subtitle placement (avoid IG UI): bottom-center but lifted up
    const ASS_FONT = "DejaVu Sans";
    const SUB_FONT_SIZE = 56;
    const OUTLINE = 6;
    const SHADOW = 1;
    const MARGIN_LR = 120;
    const MARGIN_V = 420; // higher = safer (less likely to collide with UI/footer)

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

    // Title/Footer drawtext (use fontfile to avoid fallback)
    const tTitle = escapeDrawtext(title);
    const tFooter = escapeDrawtext(footer);

    const titleFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=82:fontcolor=white:` +
      `x=(w-text_w)/2:y=220:text='${tTitle}':shadowx=0:shadowy=2:shadowcolor=black@0.35`;

    const footerFilter =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=46:fontcolor=white:` +
      `x=(w-text_w)/2:y=h-260:text='${tFooter}':shadowx=0:shadowy=2:shadowcolor=black@0.35`;

    const vf = `${titleFilter},${footerFilter},subtitles=${assPath}`;

    const isImage = /\.(png|jpg|jpeg)$/i.test(bgFile);

    // Encoding: decent quality + fast, IG-friendly MP4
    const commonVideoArgs = [
      "-c:v",
      "libx264",
      "-preset",
      "veryfast",
      "-crf",
      "24",
      "-movflags",
      "+faststart",
      "-pix_fmt",
      "yuv420p",
      "-vf",
      vf,
      "-shortest",
      "-r",
      "30",
      "-s",
      "1080x1920",
      "-c:a",
      "aac",
      "-b:a",
      "192k"
    ];

    const args = isImage
      ? [
          "-y",
          "-loop",
          "1",
          "-i",
          bgFile,
          "-i",
          audioPath,
          "-tune",
          "stillimage",
          ...commonVideoArgs,
          publicOutPath
        ]
      : ["-y", "-i", bgFile, "-i", audioPath, ...commonVideoArgs, publicOutPath];

    await runFfmpeg(args);

    const proto = req.get("x-forwarded-proto") || req.protocol;
    const baseUrl = `${proto}://${req.get("host")}`;

    return res.status(200).json({
      ok: true,
      download_url: `${baseUrl}/tmp/${publicName}`
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  } finally {
    // Best-effort cleanup (keeps public mp4 in PUBLIC_DIR)
    try {
      if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    } catch {}
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render endpoint running on :${port}`));
