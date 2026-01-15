// server.js (Node 20, ES modules)
// Motionalyx Render Endpoint — Option 3 (CORE, stable on Render free plan)
//
// Endpoints:
//   GET  /        -> "ok"
//   GET  /health  -> {"ok":true}
//   POST /render  -> multipart/form-data:
//        file field:  audio (mp3)
//        text field:  payload (JSON string)
//
// Behavior:
// - Measure audio duration (ffprobe).
// - Subtitles shown ONLY during audio (0..audio_end).
// - Title/Footer shown ONLY during audio.
// - End card full-screen ONLY during last 4s (audio_end..video_end).
// - Video length = audio length + 4s.
// - Output: 1080x1920 30fps H.264 + AAC + faststart
//
// Robustness:
// - Caches assets in /tmp/mxcache (hash filenames).
// - Serves outputs from /tmp/mxpublic via /tmp/<file>.mp4
// - Hard timeout -> 504 (so Make won't hang).
// - Logs markers: request start, ffmpeg start, done, error
// - Limits ffmpeg CPU: ultrafast + threads=1 + tune stillimage
// - Single render at a time (returns 503 if busy)

import express from "express";
import multer from "multer";
import crypto from "crypto";
import path from "path";
import fs from "fs";
import fsp from "fs/promises";
import { spawn } from "child_process";

const app = express();
app.set("trust proxy", 1);

// ---------- Paths ----------
const TMP_ROOT = "/tmp";
const CACHE_DIR = path.join(TMP_ROOT, "mxcache");
const PUBLIC_DIR = path.join(TMP_ROOT, "mxpublic");
const WORK_DIR = path.join(TMP_ROOT, "mxwork");
const UPLOAD_DIR = path.join(TMP_ROOT, "mxuploads");

// DejaVuSans installed by Dockerfile (fonts-dejavu-core)
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

// ---------- Multer ----------
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
    fields: 30,
  },
});

// ---------- Global single-render lock ----------
let renderBusy = false;

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

function nowIso() {
  return new Date().toISOString();
}

function log(reqId, msg, extra) {
  if (extra !== undefined) {
    console.log(`${nowIso()} [${reqId}] ${msg}`, extra);
  } else {
    console.log(`${nowIso()} [${reqId}] ${msg}`);
  }
}

async function exists(p) {
  try {
    await fsp.access(p);
    return true;
  } catch {
    return false;
  }
}

async function findExistingCached(filePathBase) {
  const exts = [".png", ".jpg", ".jpeg", ".webp", ".bin"];
  for (const ext of exts) {
    const p = `${filePathBase}${ext}`;
    if (await exists(p)) return p;
  }
  if (await exists(filePathBase)) return filePathBase;
  return null;
}

// Download with cache (Node 20 has global fetch)
async function downloadToCache(url, { timeoutMs = 20000 } = {}) {
  if (!url || typeof url !== "string") throw new Error("Missing asset URL");
  const key = sha1(url);
  const filePathBase = path.join(CACHE_DIR, key);

  const existing = await findExistingCached(filePathBase);
  if (existing) return existing;

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  let res;
  try {
    res = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "motionalyx-render-endpoint/1.0",
        Accept: "*/*",
      },
    });
  } finally {
    clearTimeout(t);
  }

  if (!res?.ok) {
    const status = res?.status ?? "NO_RESPONSE";
    const st = res?.statusText ?? "";
    throw new Error(`Failed to download asset: ${status} ${st}`.trim());
  }

  const contentType = (res.headers.get("content-type") || "").toLowerCase();
  let ext = "";
  if (contentType.includes("png")) ext = ".png";
  else if (contentType.includes("jpeg") || contentType.includes("jpg")) ext = ".jpg";
  else if (contentType.includes("webp")) ext = ".webp";
  else {
    const uext = path.extname(new URL(url).pathname).toLowerCase();
    ext = uext || ".bin";
  }

  const outPath = `${filePathBase}${ext}`;
  const buf = Buffer.from(await res.arrayBuffer());
  await fsp.writeFile(outPath, buf);
  return outPath;
}

function spawnLogged(cmd, args, { timeoutMs = 0, reqId = "na", prefix = "" } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    let killedByTimeout = false;

    const onData = (streamName) => (d) => {
      const s = d.toString();
      if (streamName === "stdout") stdout += s;
      else stderr += s;

      // stream ffmpeg warnings to logs for debug (trim very noisy)
      const lines = s.split("\n").map((x) => x.trim()).filter(Boolean);
      for (const line of lines) {
        // keep it readable
        log(reqId, `${prefix}${streamName}: ${line.slice(0, 500)}`);
      }
    };

    child.stdout.on("data", onData("stdout"));
    child.stderr.on("data", onData("stderr"));

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
        const err = new Error(`${cmd} timeout after ${timeoutMs}ms`);
        err.code = "ETIMEDOUT";
        err.stdout = stdout;
        err.stderr = stderr;
        return reject(err);
      }
      if (code === 0) return resolve({ stdout, stderr });
      const err = new Error(`${cmd} exited with code ${code}`);
      err.code = code;
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
    });
  });
}

async function ffprobeDurationMs(audioPath, { timeoutMs = 15000, reqId = "na" } = {}) {
  const args = [
    "-v", "error",
    "-show_entries", "format=duration",
    "-of", "default=noprint_wrappers=1:nokey=1",
    audioPath,
  ];
  const { stdout } = await spawnLogged("ffprobe", args, { timeoutMs, reqId, prefix: "[ffprobe] " });
  const sec = Number(String(stdout).trim());
  if (!Number.isFinite(sec) || sec <= 0) throw new Error(`Invalid ffprobe duration: "${stdout}"`);
  return Math.round(sec * 1000);
}

// drawtext escaping
function escapeDrawtextText(input) {
  return String(input ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'")
    .replace(/%/g, "\\%")
    .replace(/\r\n|\r|\n/g, "\\n");
}

// Subtitles wrap: max 2 lines, max 26 chars/line (simple word wrap)
function wrapSubtitle(text, maxChars = 26, maxLines = 2) {
  const raw = String(text ?? "").trim().replace(/\s+/g, " ");
  if (!raw) return "";
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

  for (let i = 0; i < lines.length; i++) {
    if (lines[i].length > maxChars) lines[i] = lines[i].slice(0, maxChars);
  }

  return lines.slice(0, maxLines).join("\\N");
}

function msToAssTime(ms) {
  const total = Math.max(0, Math.round(ms));
  const cs = Math.floor((total % 1000) / 10);
  const s = Math.floor(total / 1000) % 60;
  const m = Math.floor(total / 60000) % 60;
  const h = Math.floor(total / 3600000);
  const pad2 = (n) => String(n).padStart(2, "0");
  return `${h}:${pad2(m)}:${pad2(s)}.${pad2(cs)}`;
}

// If input subtitles end != audio length, scale all chunks proportionally
function scaleSubtitleLinesToAudio(lines, audioMs) {
  if (!Array.isArray(lines) || lines.length === 0) return [];
  const lastEnd = Number(lines[lines.length - 1]?.end_ms);
  if (!Number.isFinite(lastEnd) || lastEnd <= 0) return lines;

  const delta = Math.abs(lastEnd - audioMs);
  if (delta <= 50) {
    const out = lines.map((l) => ({ ...l }));
    out[out.length - 1].end_ms = audioMs;
    return out;
  }

  const factor = audioMs / lastEnd;
  const out = lines.map((l) => ({
    start_ms: Math.round(Number(l.start_ms) * factor),
    end_ms: Math.round(Number(l.end_ms) * factor),
    text: String(l.text ?? ""),
  }));

  out[0].start_ms = 0;
  for (let i = 1; i < out.length; i++) out[i].start_ms = out[i - 1].end_ms;
  out[out.length - 1].end_ms = audioMs;

  for (let i = 0; i < out.length; i++) {
    out[i].start_ms = Math.max(0, out[i].start_ms);
    out[i].end_ms = Math.max(out[i].start_ms + 1, out[i].end_ms);
  }

  return out;
}

async function writeAssFile(lines, outPath) {
  const header = [
    "[Script Info]",
    "ScriptType: v4.00+",
    "WrapStyle: 0",
    "ScaledBorderAndShadow: yes",
    "YCbCr Matrix: TV.709",
    "",
    "[V4+ Styles]",
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
    // Alignment=2 bottom center. MarginV moves up from bottom (avoid UI/pill).
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

    // minimal ASS escaping
    const safe = wrapped.replace(/{/g, "\\{").replace(/}/g, "\\}");
    events.push(`Dialogue: 0,${start},${end},Default,,0,0,0,,${safe}`);
  }

  await fsp.writeFile(outPath, `${header}\n${events.join("\n")}\n`, "utf8");
}

function buildFilterComplex({ assPath, title, footer, audioSec, videoSec }) {
  // IMPORTANT: no trailing ";" and no empty segments
  const filters = [];

  const assEsc = assPath.replace(/\\/g, "\\\\").replace(/:/g, "\\:");
  filters.push(`[0:v]subtitles='${assEsc}'[v0]`);

  const tText = escapeDrawtextText(title);
  const fText = escapeDrawtextText(footer);

  // enable expressions MUST escape commas: t\,X
  const enableAudioOnly = `lt(t\\,${audioSec.toFixed(3)})`;

  // Title (top)
  filters.push(
    `[v0]drawtext=fontfile=${FONT_FILE}:text='${tText}':fontsize=78:fontcolor=white:x=(w-text_w)/2:y=320:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v1]`
  );

  // Footer (above subtitles)
  filters.push(
    `[v1]drawtext=fontfile=${FONT_FILE}:text='${fText}':fontsize=54:fontcolor=white:x=(w-text_w)/2:y=1520:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v2]`
  );

  // End card overlay: full screen during last 4 seconds
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

// ---------- POST /render ----------
app.post("/render", upload.single("audio"), async (req, res) => {
  const reqId = crypto.randomBytes(6).toString("hex");
  const startedAt = Date.now();

  if (renderBusy) {
    // Render free plan: keep it simple; no parallel ffmpeg
    return res.status(503).json({ ok: false, error: "Renderer busy. Try again." });
  }

  renderBusy = true;
  log(reqId, "[/render] request start");

  // Timeouts (Make has 300s)
  const HARD_TIMEOUT_MS = Number(process.env.HARD_TIMEOUT_MS || 240000);   // overall
  const FFMPEG_TIMEOUT_MS = Number(process.env.FFMPEG_TIMEOUT_MS || 210000); // ffmpeg only

  // Hard kill guard
  let hardTimer = null;
  const hardAbort = { aborted: false };
  hardTimer = setTimeout(() => {
    hardAbort.aborted = true;
  }, HARD_TIMEOUT_MS);

  let audioPath = null;
  let assPath = null;

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

    // Measure audio
    const audioMs = await ffprobeDurationMs(audioPath, { reqId });
    const audioSec = audioMs / 1000;
    const videoSec = audioSec + 4.0;
    const videoMs = Math.round(videoSec * 1000);

    // Scale subtitles to audio length
    const scaledLines = scaleSubtitleLinesToAudio(payload.subtitles.lines, audioMs);

    // Write ASS
    const jobId = crypto.randomBytes(12).toString("hex");
    assPath = path.join(WORK_DIR, `${jobId}.ass`);
    await writeAssFile(scaledLines, assPath);

    // Output
    const outName = `${jobId}.mp4`;
    const outPath = path.join(PUBLIC_DIR, outName);

    // Filtergraph (no trailing ;)
    const filterComplex = buildFilterComplex({
      assPath,
      title: payload.text.title,
      footer: payload.text.footer,
      audioSec,
      videoSec,
    });

    // ---------- ffmpeg args (optimized for Render free plan) ----------
    // -threads 1 reduces CPU spikes (stability)
    // -preset ultrafast + -tune stillimage reduces encode load
    // CRF slightly higher for speed; you can lower later for quality.
    const fps = payload.spec.fps;

    const ffmpegArgs = [
      "-y",
      "-hide_banner",
      "-loglevel",
      "warning",

      // Base background (looped)
      "-loop", "1",
      "-i", baseBgPath,

      // Audio
      "-i", audioPath,

      // End card (looped)
      "-loop", "1",
      "-i", endCardPath,

      // Compose
      "-filter_complex", filterComplex,

      // Extend audio by 4s silence then trim to exact duration
      "-filter:a", "apad=pad_dur=4",

      // Map
      "-map", "[vout]",
      "-map", "1:a",

      // Exact total duration
      "-t", videoSec.toFixed(3),

      // Video encode
      "-r", String(fps),
      "-c:v", "libx264",
      "-pix_fmt", "yuv420p",
      "-profile:v", "high",
      "-preset", "ultrafast",
      "-tune", "stillimage",
      "-threads", "1",
      "-crf", "24",

      // Audio encode
      "-c:a", "aac",
      "-b:a", "160k",

      // Fast start for web playback
      "-movflags", "+faststart",

      outPath,
    ];

    log(reqId, "[/render] ffmpeg start");

    // If hard timeout already triggered, abort early
    if (hardAbort.aborted) {
      return res.status(504).json({
        ok: false,
        error: "Hard timeout before ffmpeg",
        debug: { audio_ms: audioMs, video_ms: videoMs },
      });
    }

    // Run ffmpeg with timeout
    try {
      await spawnLogged("ffmpeg", ffmpegArgs, { timeoutMs: FFMPEG_TIMEOUT_MS, reqId, prefix: "[ffmpeg] " });
    } catch (err) {
      if (err?.code === "ETIMEDOUT") {
        log(reqId, "[/render] error timeout");
        return res.status(504).json({
          ok: false,
          error: "Render timeout",
          debug: { audio_ms: audioMs, video_ms: videoMs },
        });
      }
      throw err;
    }

    // If hard timeout triggered after ffmpeg, still respond as timeout (safer for Make)
    if (hardAbort.aborted) {
      log(reqId, "[/render] hard timeout after ffmpeg");
      return res.status(504).json({
        ok: false,
        error: "Hard timeout",
        debug: { audio_ms: audioMs, video_ms: videoMs },
      });
    }

    log(reqId, "[/render] done");

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
    log(reqId, "[/render] error", err?.message || err);

    const debug = {};
    if (err?.stderr) debug.ffmpeg_stderr = String(err.stderr).slice(0, 4000);

    return res.status(500).json({
      ok: false,
      error: err?.message || "Unknown error",
      debug,
    });
  } finally {
    renderBusy = false;
    if (hardTimer) clearTimeout(hardTimer);

    // Cleanup uploads + temp ass (best effort)
    try {
      if (audioPath && fs.existsSync(audioPath)) await fsp.unlink(audioPath);
    } catch {}
    try {
      if (assPath && fs.existsSync(assPath)) await fsp.unlink(assPath);
    } catch {}
  }
});

// ---------- Graceful shutdown (helps when Render sends SIGTERM) ----------
const server = app.listen(process.env.PORT || 3000, () => {
  console.log(`motionalyx-render-endpoint listening on :${process.env.PORT || 3000}`);
});

function shutdown(signal) {
  console.log(`${nowIso()} [shutdown] received ${signal}`);
  server.close(() => {
    console.log(`${nowIso()} [shutdown] server closed`);
    process.exit(0);
  });
  // Force exit if it hangs
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
