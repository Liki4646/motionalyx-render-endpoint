// server.js (Node 20, ES modules)
// Motionalyx Render Endpoint — Option 3 (CORE):
// base background + title/footer + subtitles during audio
// end card full-screen for last 4s (no title/footer/subs)

import express from "express";
import multer from "multer";
import crypto from "crypto";
import path from "path";
import fs from "fs";
import fsp from "fs/promises";
import os from "os";
import { spawn } from "child_process";

const app = express();

// Render/Proxy friendly (so req.protocol becomes https on Render)
app.set("trust proxy", 1);

// ---------- Paths ----------
const TMP_ROOT = "/tmp";
const CACHE_DIR = path.join(TMP_ROOT, "mxcache");
const PUBLIC_DIR = path.join(TMP_ROOT, "mxpublic");
const WORK_DIR = path.join(TMP_ROOT, "mxwork");
const UPLOAD_DIR = path.join(TMP_ROOT, "mxuploads");

// DejaVuSans is installed by your Dockerfile (fonts-dejavu-core)
const FONT_FILE = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// ---------- Ensure dirs ----------
async function ensureDirs() {
  await fsp.mkdir(CACHE_DIR, { recursive: true });
  await fsp.mkdir(PUBLIC_DIR, { recursive: true });
  await fsp.mkdir(WORK_DIR, { recursive: true });
  await fsp.mkdir(UPLOAD_DIR, { recursive: true });
}
await ensureDirs();

// ---------- Static serving for outputs ----------
app.use(
  "/tmp",
  express.static(PUBLIC_DIR, {
    fallthrough: false,
    setHeaders(res) {
      res.setHeader("Cache-Control", "no-store");
    },
  })
);

// ---------- Health routes ----------
app.get("/", (_req, res) => res.type("text/plain").send("ok"));
app.get("/health", (_req, res) => res.json({ ok: true }));

// ---------- Multer (multipart form-data) ----------
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const id = crypto.randomBytes(16).toString("hex");
    const ext = path.extname(file.originalname || "").toLowerCase() || ".mp3";
    cb(null, `${id}${ext}`);
  },
});
const upload = multer({
  storage,
  limits: {
    fileSize: 25 * 1024 * 1024, // 25MB
    files: 1,
    fields: 20,
  },
});

// ---------- Helpers ----------
function sha1(str) {
  return crypto.createHash("sha1").update(str).digest("hex");
}

function safeJsonParse(str) {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch (e) {
    return { ok: false, error: e };
  }
}

// Download with cache (Node 20 has global fetch)
async function downloadToCache(url) {
  if (!url || typeof url !== "string") throw new Error("Missing asset URL");
  const key = sha1(url);
  const filePathBase = path.join(CACHE_DIR, key);

  // Try to find an existing cached file with any common extension
  const existing = await findExistingCached(filePathBase);
  if (existing) return existing;

  // Download
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent": "motionalyx-render-endpoint/1.0",
      Accept: "*/*",
    },
  });

  if (!res.ok) {
    throw new Error(`Failed to download asset: ${res.status} ${res.statusText}`);
  }

  // Determine extension
  const contentType = (res.headers.get("content-type") || "").toLowerCase();
  let ext = "";
  if (contentType.includes("png")) ext = ".png";
  else if (contentType.includes("jpeg") || contentType.includes("jpg")) ext = ".jpg";
  else if (contentType.includes("webp")) ext = ".webp";
  else {
    // fallback: try URL path ext
    const uext = path.extname(new URL(url).pathname).toLowerCase();
    ext = uext || ".bin";
  }

  const outPath = `${filePathBase}${ext}`;
  const buf = Buffer.from(await res.arrayBuffer());
  await fsp.writeFile(outPath, buf);
  return outPath;
}

async function findExistingCached(filePathBase) {
  const exts = [".png", ".jpg", ".jpeg", ".webp", ".bin"];
  for (const ext of exts) {
    const p = `${filePathBase}${ext}`;
    if (fs.existsSync(p)) return p;
  }
  // also if someone already wrote without extension (unlikely)
  if (fs.existsSync(filePathBase)) return filePathBase;
  return null;
}

function spawnAsync(cmd, args, { timeoutMs = 0, logPrefix = "" } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    let killedByTimeout = false;
    let t = null;

    if (timeoutMs > 0) {
      t = setTimeout(() => {
        killedByTimeout = true;
        try {
          child.kill("SIGKILL");
        } catch {}
      }, timeoutMs);
    }

    child.on("error", (err) => {
      if (t) clearTimeout(t);
      reject(err);
    });

    child.on("close", (code) => {
      if (t) clearTimeout(t);
      if (killedByTimeout) {
        const err = new Error(`${logPrefix}${cmd} timeout after ${timeoutMs}ms`);
        err.code = "ETIMEDOUT";
        err.stdout = stdout;
        err.stderr = stderr;
        return reject(err);
      }
      if (code === 0) return resolve({ stdout, stderr });
      const err = new Error(`${logPrefix}${cmd} exited with code ${code}`);
      err.code = code;
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
    });
  });
}

async function ffprobeDurationMs(audioPath, timeoutMs = 15000) {
  const args = [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    audioPath,
  ];
  const { stdout } = await spawnAsync("ffprobe", args, { timeoutMs, logPrefix: "[ffprobe] " });
  const sec = Number(String(stdout).trim());
  if (!Number.isFinite(sec) || sec <= 0) throw new Error(`Invalid ffprobe duration: "${stdout}"`);
  return Math.round(sec * 1000);
}

// drawtext escaping (ffmpeg filter string)
function escapeDrawtextText(input) {
  // For ffmpeg drawtext:
  // - escape backslash
  // - escape colon
  // - escape apostrophe
  // - escape percent
  // - convert newlines to \n
  // Keep it conservative.
  return String(input ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'")
    .replace(/%/g, "\\%")
    .replace(/\r\n|\r|\n/g, "\\n");
}

// Subtitle wrapping: max 2 lines, max 26 chars/line (simple word wrap)
function wrapSubtitle(text, maxChars = 26, maxLines = 2) {
  const raw = String(text ?? "").trim().replace(/\s+/g, " ");
  if (!raw) return "";

  // If it already fits in one line
  if (raw.length <= maxChars) return raw;

  const words = raw.split(" ");
  const lines = [];
  let current = "";

  for (const w of words) {
    if (!current) {
      current = w;
      continue;
    }
    if ((current + " " + w).length <= maxChars) {
      current += " " + w;
    } else {
      lines.push(current);
      current = w;
      if (lines.length === maxLines) break;
    }
  }
  if (lines.length < maxLines && current) lines.push(current);

  // If still too long or more than maxLines, truncate last line hard
  if (lines.length > maxLines) lines.length = maxLines;

  // Ensure each line <= maxChars
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].length > maxChars) lines[i] = lines[i].slice(0, maxChars);
  }

  // If text got cut and still words remain, slightly trim last line
  const joined = lines.join(" ");
  if (joined.length < raw.length && lines.length) {
    // optional: no ellipsis to avoid typos; just keep clean
    lines[lines.length - 1] = lines[lines.length - 1].trim();
  }

  // Return with ASS newline
  return lines.slice(0, maxLines).join("\\N");
}

function msToAssTime(ms) {
  const total = Math.max(0, Math.round(ms));
  const cs = Math.floor((total % 1000) / 10); // centiseconds
  const s = Math.floor(total / 1000) % 60;
  const m = Math.floor(total / 60000) % 60;
  const h = Math.floor(total / 3600000);
  const pad2 = (n) => String(n).padStart(2, "0");
  const pad2c = (n) => String(n).padStart(2, "0");
  return `${h}:${pad2(m)}:${pad2(s)}.${pad2c(cs)}`;
}

// Scale subtitle timings to match audio length (keeps relative spacing)
function scaleSubtitleLinesToAudio(lines, audioMs) {
  if (!Array.isArray(lines) || lines.length === 0) return [];
  const lastEnd = Number(lines[lines.length - 1]?.end_ms);
  if (!Number.isFinite(lastEnd) || lastEnd <= 0) return lines;

  // If already matches (or very close), just clamp final end
  const delta = Math.abs(lastEnd - audioMs);
  if (delta <= 50) {
    const out = lines.map((l) => ({ ...l }));
    out[out.length - 1].end_ms = audioMs;
    return out;
  }

  const factor = audioMs / lastEnd;
  const out = lines.map((l) => {
    const s = Math.round(Number(l.start_ms) * factor);
    const e = Math.round(Number(l.end_ms) * factor);
    return {
      start_ms: s,
      end_ms: e,
      text: String(l.text ?? ""),
    };
  });

  // Fix consecutiveness: force each start = prev end
  out[0].start_ms = 0;
  for (let i = 1; i < out.length; i++) out[i].start_ms = out[i - 1].end_ms;

  // Force final end = audioMs
  out[out.length - 1].end_ms = audioMs;

  // Also ensure monotonic non-decreasing
  for (let i = 0; i < out.length; i++) {
    out[i].start_ms = Math.max(0, out[i].start_ms);
    out[i].end_ms = Math.max(out[i].start_ms + 1, out[i].end_ms);
  }

  return out;
}

async function writeAssFile(lines, outPath) {
  // ASS header + one style
  // Alignment=2 (bottom center), MarginV pushes it up from bottom
  const header = [
    "[Script Info]",
    "ScriptType: v4.00+",
    "WrapStyle: 0",
    "ScaledBorderAndShadow: yes",
    "YCbCr Matrix: TV.709",
    "",
    "[V4+ Styles]",
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
    // White text, black outline, subtle shadow
    "Style: Default,DejaVu Sans,56,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,3,1,2,60,60,160,1",
    "",
    "[Events]",
    "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
  ].join("\n");

  const events = [];
  for (const l of lines) {
    const start = msToAssTime(l.start_ms);
    const end = msToAssTime(l.end_ms);
    const wrapped = wrapSubtitle(l.text, 26, 2);
    // Escape ASS special chars minimally
    const safe = wrapped.replace(/{/g, "\\{").replace(/}/g, "\\}");
    events.push(`Dialogue: 0,${start},${end},Default,,0,0,0,,${safe}`);
  }

  await fsp.writeFile(outPath, `${header}\n${events.join("\n")}\n`, "utf8");
}

function buildFilterComplex({
  assPath,
  title,
  footer,
  audioSec,
  videoSec,
}) {
  // IMPORTANT: No trailing ";" and no empty segments
  const filters = [];

  // Start with base background input [0:v]
  // Apply subtitles first (on base)
  // Note: escape backslashes in windows paths not needed here (linux)
  const assEsc = assPath.replace(/\\/g, "\\\\").replace(/:/g, "\\:");

  // subtitles filter
  filters.push(`[0:v]subtitles='${assEsc}'[v0]`);

  // drawtext title and footer only during audio (0..audioSec)
  const tText = escapeDrawtextText(title);
  const fText = escapeDrawtextText(footer);

  // enable requires escaping comma: t\,<audioSec>
  const enableAudioOnly = `lt(t\\,${audioSec.toFixed(3)})`;

  // Title near top
  filters.push(
    `[v0]drawtext=fontfile=${FONT_FILE}:text='${tText}':fontsize=78:fontcolor=white:x=(w-text_w)/2:y=320:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v1]`
  );

  // Footer near bottom (above subtitles)
  filters.push(
    `[v1]drawtext=fontfile=${FONT_FILE}:text='${fText}':fontsize=54:fontcolor=white:x=(w-text_w)/2:y=1520:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v2]`
  );

  // End card overlay full screen during last 4s
  // end card input index [2:v]
  // enable between audioSec and videoSec
  const enableEndCard = `between(t\\,${audioSec.toFixed(3)}\\,${videoSec.toFixed(3)})`;
  filters.push(`[v2][2:v]overlay=0:0:enable='${enableEndCard}'[vout]`);

  return filters.join(";");
}

function validatePayload(payload) {
  if (!payload || typeof payload !== "object") throw new Error("payload must be an object");
  const spec = payload.spec || {};
  const assets = payload.assets || {};
  const text = payload.text || {};
  const subtitles = payload.subtitles || {};

  const width = Number(spec.width ?? 1080);
  const height = Number(spec.height ?? 1920);
  const fps = Number(spec.fps ?? 30);

  if (!Number.isFinite(width) || !Number.isFinite(height) || !Number.isFinite(fps)) {
    throw new Error("Invalid spec (width/height/fps)");
  }

  if (!assets.base_background_url) throw new Error("Missing assets.base_background_url");
  if (!assets.end_card_url) throw new Error("Missing assets.end_card_url");

  const title = String(text.title ?? "").trim();
  const footer = String(text.footer ?? "").trim();

  // subtitles.lines is required for core render
  const lines = Array.isArray(subtitles.lines) ? subtitles.lines : [];
  if (lines.length === 0) throw new Error("Missing subtitles.lines");

  return {
    spec: { width, height, fps },
    assets: {
      base_background_url: String(assets.base_background_url),
      end_card_url: String(assets.end_card_url),
      card_image_urls: Array.isArray(assets.card_image_urls) ? assets.card_image_urls : [],
    },
    text: { title, footer },
    subtitles: { lines },
  };
}

// ---------- Render endpoint ----------
app.post("/render", upload.single("audio"), async (req, res) => {
  const startedAt = Date.now();
  console.log("[/render] request start");

  // Hard timeout for whole render (Make timeout is 300s)
  const HARD_TIMEOUT_MS = 240_000; // 240s
  const FFMPEG_TIMEOUT_MS = 210_000; // 210s for ffmpeg itself

  const abortController = new AbortController();
  const hardTimer = setTimeout(() => abortController.abort(), HARD_TIMEOUT_MS);

  let audioPath = null;

  try {
    if (!req.file?.path) {
      return res.status(400).json({ ok: false, error: "Missing multipart file field: audio" });
    }
    audioPath = req.file.path;

    const payloadStr = req.body?.payload;
    if (!payloadStr || typeof payloadStr !== "string") {
      return res.status(400).json({ ok: false, error: "Missing text field: payload (JSON string)" });
    }

    const parsed = safeJsonParse(payloadStr);
    if (!parsed.ok) {
      return res.status(400).json({ ok: false, error: "Invalid JSON in payload" });
    }

    const payload = validatePayload(parsed.value);

    // Download/cache assets
    const baseBgPath = await downloadToCache(payload.assets.base_background_url);
    const endCardPath = await downloadToCache(payload.assets.end_card_url);

    // Measure audio duration
    const audioMs = await ffprobeDurationMs(audioPath);
    const audioSec = audioMs / 1000;
    const videoSec = audioSec + 4.0;
    const videoMs = Math.round(videoSec * 1000);

    // Scale subtitles to match actual audio length
    const scaledLines = scaleSubtitleLinesToAudio(payload.subtitles.lines, audioMs);

    // Write ASS file
    const jobId = crypto.randomBytes(12).toString("hex");
    const assPath = path.join(WORK_DIR, `${jobId}.ass`);
    await writeAssFile(scaledLines, assPath);

    // Output file
    const outName = `${jobId}.mp4`;
    const outPath = path.join(PUBLIC_DIR, outName);

    // Build filter_complex (NO trailing ;)
    const filterComplex = buildFilterComplex({
      assPath,
      title: payload.text.title,
      footer: payload.text.footer,
      audioSec,
      videoSec,
    });

    // ffmpeg args
    // Input 0: base background (loop)
    // Input 1: audio
    // Input 2: end card (loop)
    const fps = payload.spec.fps;

    // Extend audio by 4s of silence so audio track matches video length cleanly
    const audioFilter = `apad=pad_dur=4`;

    const ffmpegArgs = [
      "-y",
      "-hide_banner",
      "-loglevel",
      "error",

      "-loop",
      "1",
      "-i",
      baseBgPath,

      "-i",
      audioPath,

      "-loop",
      "1",
      "-i",
      endCardPath,

      "-filter_complex",
      filterComplex,

      // audio filter
      "-filter:a",
      audioFilter,

      // Map outputs
      "-map",
      "[vout]",
      "-map",
      "1:a",

      // Force exact duration
      "-t",
      videoSec.toFixed(3),

      // Output settings
      "-r",
      String(fps),
      "-c:v",
      "libx264",
      "-pix_fmt",
      "yuv420p",
      "-profile:v",
      "high",
      "-preset",
      "veryfast",
      "-crf",
      "20",

      "-c:a",
      "aac",
      "-b:a",
      "192k",

      "-movflags",
      "+faststart",

      outPath,
    ];

    console.log("[/render] ffmpeg start");

    // Run ffmpeg with timeout + hard abort
    const renderPromise = spawnAsync("ffmpeg", ffmpegArgs, {
      timeoutMs: FFMPEG_TIMEOUT_MS,
      logPrefix: "[ffmpeg] ",
    });

    const abortPromise = new Promise((_, reject) => {
      abortController.signal.addEventListener("abort", () => {
        const err = new Error("Hard timeout reached");
        err.code = "HARD_TIMEOUT";
        reject(err);
      });
    });

    try {
      await Promise.race([renderPromise, abortPromise]);
    } catch (err) {
      if (err?.code === "ETIMEDOUT" || err?.code === "HARD_TIMEOUT") {
        console.log("[/render] error timeout");
        return res.status(504).json({
          ok: false,
          error: "Render timeout",
          debug: { audio_ms: audioMs, video_ms: videoMs },
        });
      }
      throw err;
    }

    console.log("[/render] done");

    const downloadUrl = `${req.protocol}://${req.get("host")}/tmp/${outName}`;
    return res.json({
      ok: true,
      download_url: downloadUrl,
      debug: {
        audio_ms: audioMs,
        video_ms: videoMs,
        elapsed_ms: Date.now() - startedAt,
      },
    });
  } catch (err) {
    console.log("[/render] error", err?.message || err);

    // Include ffmpeg stderr if present (helps debugging)
    const debug = {};
    if (err?.stderr) debug.ffmpeg_stderr = String(err.stderr).slice(0, 4000);

    return res.status(500).json({
      ok: false,
      error: err?.message || "Unknown error",
      debug,
    });
  } finally {
    clearTimeout(hardTimer);

    // Cleanup upload + temporary ASS (best-effort)
    try {
      if (audioPath && fs.existsSync(audioPath)) await fsp.unlink(audioPath);
    } catch {}
  }
});

// ---------- Start ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`motionalyx-render-endpoint listening on :${PORT}`);
});
