// server.js (Node 20, ES modules)
// Motionalyx Render Endpoint — Diagnostic build
//
// Goals:
// - Show if ffmpeg is slow (free plan CPU) vs. stuck (hang)
// - Log ffmpeg progress (out_time_ms, speed, fps) every few seconds
// - Keep same API contract: GET /, GET /health, POST /render multipart
//
// POST /render expects:
//  - multipart file field: audio (mp3)
//  - text field: payload (JSON string)
//
// payload schema (as you described) + optional:
//  payload.debug.disable_subtitles: true   -> skip ASS burn-in for A/B test
//
// Output JSON:
// {
//   ok: true,
//   download_url,
//   debug: { audio_ms, video_ms, elapsed_ms, ffmpeg_last, ffmpeg_samples }
// }

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

// Docker installs fonts-dejavu-core
const FONT_FILE = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// ---------- Ensure dirs ----------
async function ensureDirs() {
  await fsp.mkdir(CACHE_DIR, { recursive: true });
  await fsp.mkdir(PUBLIC_DIR, { recursive: true });
  await fsp.mkdir(WORK_DIR, { recursive: true });
  await fsp.mkdir(UPLOAD_DIR, { recursive: true });
}
await ensureDirs();

// ---------- Static serving ----------
app.use(
  "/tmp",
  express.static(PUBLIC_DIR, {
    fallthrough: false,
    setHeaders(res) {
      res.setHeader("Cache-Control", "no-store");
    },
  })
);

// ---------- Health ----------
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
  limits: { fileSize: 25 * 1024 * 1024, files: 1, fields: 50 },
});

// ---------- Single render lock ----------
let renderBusy = false;

// ---------- Utils ----------
function nowIso() {
  return new Date().toISOString();
}
function log(reqId, msg, extra) {
  if (extra !== undefined) console.log(`${nowIso()} [${reqId}] ${msg}`, extra);
  else console.log(`${nowIso()} [${reqId}] ${msg}`);
}
function sha1(str) {
  return crypto.createHash("sha1").update(str).digest("hex");
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
function safeJsonParse(str) {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch (e) {
    return { ok: false, error: e };
  }
}

// ---------- Download + cache ----------
async function downloadToCache(url, { timeoutMs = 20000 } = {}) {
  if (!url || typeof url !== "string") throw new Error("Missing asset URL");

  const key = sha1(url);
  const base = path.join(CACHE_DIR, key);

  const existing = await findExistingCached(base);
  if (existing) return existing;

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  let res;
  try {
    res = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "motionalyx-render-endpoint/diagnostic",
        Accept: "*/*",
      },
    });
  } finally {
    clearTimeout(t);
  }

  if (!res?.ok) {
    throw new Error(`Failed to download asset: ${res?.status ?? "NO"} ${res?.statusText ?? ""}`.trim());
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

  const outPath = `${base}${ext}`;
  const buf = Buffer.from(await res.arrayBuffer());
  await fsp.writeFile(outPath, buf);
  return outPath;
}

// ---------- ffprobe ----------
async function spawnSimple(cmd, args, { timeoutMs = 15000, reqId = "na", prefix = "" } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";

    const t = setTimeout(() => {
      try { child.kill("SIGKILL"); } catch {}
      const e = new Error(`${cmd} timeout after ${timeoutMs}ms`);
      e.code = "ETIMEDOUT";
      reject(e);
    }, timeoutMs);

    child.stdout.on("data", (d) => (out += d.toString()));
    child.stderr.on("data", (d) => {
      const s = d.toString();
      err += s;
      const lines = s.split("\n").map(x => x.trim()).filter(Boolean);
      for (const line of lines) log(reqId, `${prefix}stderr: ${line.slice(0, 500)}`);
    });

    child.on("error", (e) => {
      clearTimeout(t);
      reject(e);
    });
    child.on("close", (code) => {
      clearTimeout(t);
      if (code === 0) return resolve({ stdout: out, stderr: err });
      const e = new Error(`${cmd} exited with code ${code}`);
      e.code = code;
      e.stderr = err;
      reject(e);
    });
  });
}

async function ffprobeDurationMs(audioPath, { reqId } = {}) {
  const args = [
    "-v", "error",
    "-show_entries", "format=duration",
    "-of", "default=noprint_wrappers=1:nokey=1",
    audioPath,
  ];
  const { stdout } = await spawnSimple("ffprobe", args, { timeoutMs: 15000, reqId, prefix: "[ffprobe] " });
  const sec = Number(String(stdout).trim());
  if (!Number.isFinite(sec) || sec <= 0) throw new Error(`Invalid ffprobe duration: "${stdout}"`);
  return Math.round(sec * 1000);
}

// ---------- ASS subtitles ----------
function wrapSubtitle(text, maxChars = 26, maxLines = 2) {
  const raw = String(text ?? "").trim().replace(/\s+/g, " ");
  if (!raw) return "";
  if (raw.length <= maxChars) return raw;

  const words = raw.split(" ");
  const lines = [];
  let cur = "";

  for (const w of words) {
    if (!cur) { cur = w; continue; }
    if ((cur + " " + w).length <= maxChars) cur += " " + w;
    else {
      lines.push(cur);
      cur = w;
      if (lines.length === maxLines) break;
    }
  }
  if (lines.length < maxLines && cur) lines.push(cur);
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

  return out;
}

async function writeAssFile(lines, outPath) {
  const header = [
    "[Script Info]",
    "ScriptType: v4.00+",
    "PlayResX: 1080",
    "PlayResY: 1920",
    "WrapStyle: 0",
    "ScaledBorderAndShadow: yes",
    "YCbCr Matrix: TV.709",
    "",
    "[V4+ Styles]",
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
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
    const safe = wrapped.replace(/{/g, "\\{").replace(/}/g, "\\}");
    events.push(`Dialogue: 0,${start},${end},Default,,0,0,0,,${safe}`);
  }

  await fsp.writeFile(outPath, `${header}\n${events.join("\n")}\n`, "utf8");
}

// ---------- drawtext escape ----------
function escapeDrawtextText(input) {
  return String(input ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'")
    .replace(/%/g, "\\%")
    .replace(/\r\n|\r|\n/g, "\\n");
}

// ---------- filtergraph builder ----------
function buildFilterComplex({ assPath, title, footer, audioSec, videoSec, disableSubtitles }) {
  const filters = [];

  let baseLabel = "[0:v]";
  if (!disableSubtitles) {
    const assEsc = assPath.replace(/\\/g, "\\\\").replace(/:/g, "\\:");
    filters.push(`[0:v]subtitles='${assEsc}'[v0]`);
    baseLabel = "[v0]";
  } else {
    filters.push(`[0:v]null[v0]`);
    baseLabel = "[v0]";
  }

  const tText = escapeDrawtextText(title);
  const fText = escapeDrawtextText(footer);

  const enableAudioOnly = `lt(t\\,${audioSec.toFixed(3)})`;
  filters.push(
    `${baseLabel}drawtext=fontfile=${FONT_FILE}:text='${tText}':fontsize=78:fontcolor=white:x=(w-text_w)/2:y=320:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v1]`
  );
  filters.push(
    `[v1]drawtext=fontfile=${FONT_FILE}:text='${fText}':fontsize=54:fontcolor=white:x=(w-text_w)/2:y=1520:shadowcolor=black:shadowx=2:shadowy=2:enable='${enableAudioOnly}'[v2]`
  );

  const enableEndCard = `between(t\\,${audioSec.toFixed(3)}\\,${videoSec.toFixed(3)})`;
  filters.push(`[v2][2:v]overlay=0:0:enable='${enableEndCard}'[vout]`);

  // no trailing ;
  return filters.join(";");
}

// ---------- payload validation ----------
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

  const lines = Array.isArray(subtitles.lines) ? subtitles.lines : [];
  if (lines.length === 0) throw new Error("Missing subtitles.lines");

  const debug = payload.debug && typeof payload.debug === "object" ? payload.debug : {};
  const disableSubtitles = debug.disable_subtitles === true;

  return {
    spec: { width, height, fps },
    assets: {
      base_background_url: String(assets.base_background_url),
      end_card_url: String(assets.end_card_url),
    },
    text: {
      title: String(text.title ?? "").trim(),
      footer: String(text.footer ?? "").trim(),
    },
    subtitles: { lines },
    debug: { disableSubtitles },
  };
}

// ---------- ffmpeg runner with progress parsing ----------
function runFfmpegWithProgress({ reqId, args, timeoutMs, hardTimeoutMs }) {
  return new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });

    const started = Date.now();
    let killed = false;

    // ffmpeg -progress pipe:2 outputs key=value lines; progress=end at the end
    const last = {
      out_time_ms: 0,
      out_time: "",
      speed: "",
      fps: "",
      frame: "",
      progress: "",
    };

    const samples = []; // store a few samples for response (not too big)

    // heartbeat: log every 5 seconds
    const hb = setInterval(() => {
      const elapsed = Date.now() - started;
      log(reqId, `[ffmpeg] heartbeat elapsed_ms=${elapsed} out_time_ms=${last.out_time_ms} speed=${last.speed} fps=${last.fps} frame=${last.frame}`);
    }, 5000);

    const softTimer = setTimeout(() => {
      if (killed) return;
      killed = true;
      try { child.kill("SIGKILL"); } catch {}
      const e = new Error(`ffmpeg timeout after ${timeoutMs}ms`);
      e.code = "ETIMEDOUT";
      e.ffmpeg_last = { ...last };
      e.ffmpeg_samples = samples.slice(-20);
      reject(e);
    }, timeoutMs);

    const hardTimer = setTimeout(() => {
      if (killed) return;
      killed = true;
      try { child.kill("SIGKILL"); } catch {}
      const e = new Error(`ffmpeg HARD timeout after ${hardTimeoutMs}ms`);
      e.code = "HARD_TIMEOUT";
      e.ffmpeg_last = { ...last };
      e.ffmpeg_samples = samples.slice(-20);
      reject(e);
    }, hardTimeoutMs);

    let buf = "";
    child.stderr.on("data", (d) => {
      buf += d.toString();
      let idx;
      while ((idx = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);

        if (!line) continue;

        // progress format: key=value
        const eq = line.indexOf("=");
        if (eq > 0) {
          const key = line.slice(0, eq).trim();
          const val = line.slice(eq + 1).trim();

          if (key in last) {
            last[key] = val;
            if (key === "out_time_ms") {
              const n = Number(val);
              if (Number.isFinite(n)) last.out_time_ms = n;
            }
          }

          if (key === "progress") {
            last.progress = val;
            // store small sample
            samples.push({
              t_ms: Date.now() - started,
              out_time_ms: last.out_time_ms,
              speed: last.speed,
              fps: last.fps,
              frame: last.frame,
              progress: val,
            });
            if (samples.length > 60) samples.shift(); // keep last 60 only
          }
        } else {
          // other stderr lines, log as warning (trim)
          log(reqId, `[ffmpeg] ${line.slice(0, 500)}`);
        }
      }
    });

    child.on("error", (err) => {
      clearInterval(hb);
      clearTimeout(softTimer);
      clearTimeout(hardTimer);
      reject(err);
    });

    child.on("close", (code) => {
      clearInterval(hb);
      clearTimeout(softTimer);
      clearTimeout(hardTimer);

      if (code === 0) {
        return resolve({
          ffmpeg_last: { ...last },
          ffmpeg_samples: samples.slice(-20),
          elapsed_ms: Date.now() - started,
        });
      }

      const e = new Error(`ffmpeg exited with code ${code}`);
      e.code = code;
      e.ffmpeg_last = { ...last };
      e.ffmpeg_samples = samples.slice(-20);
      reject(e);
    });
  });
}

// ---------- POST /render ----------
app.post("/render", upload.single("audio"), async (req, res) => {
  const reqId = crypto.randomBytes(6).toString("hex");
  const startedAt = Date.now();

  if (renderBusy) return res.status(503).json({ ok: false, error: "Renderer busy. Try again." });
  renderBusy = true;

  log(reqId, "[/render] request start");

  // Make timeout is 300s; start with these, and you can override via env vars
  const FFMPEG_TIMEOUT_MS = Number(process.env.FFMPEG_TIMEOUT_MS || 285000); // close to 300s
  const HARD_TIMEOUT_MS = Number(process.env.HARD_TIMEOUT_MS || 295000);

  let audioPath = null;
  let assPath = null;

  try {
    if (!req.file?.path) return res.status(400).json({ ok: false, error: "Missing multipart file field: audio" });
    audioPath = req.file.path;

    const payloadStr = req.body?.payload;
    if (!payloadStr || typeof payloadStr !== "string") {
      return res.status(400).json({ ok: false, error: "Missing text field: payload (JSON string)" });
    }

    const parsed = safeJsonParse(payloadStr);
    if (!parsed.ok) return res.status(400).json({ ok: false, error: "Invalid JSON in payload" });

    const payload = validatePayload(parsed.value);

    // download assets
    const baseBgPath = await downloadToCache(payload.assets.base_background_url);
    const endCardPath = await downloadToCache(payload.assets.end_card_url);

    // audio duration
    const audioMs = await ffprobeDurationMs(audioPath, { reqId });
    const audioSec = audioMs / 1000;
    const videoSec = audioSec + 4.0;
    const videoMs = Math.round(videoSec * 1000);

    // subtitles (scale to audio) + write ASS (unless disabled)
    const jobId = crypto.randomBytes(12).toString("hex");
    assPath = path.join(WORK_DIR, `${jobId}.ass`);

    const scaledLines = scaleSubtitleLinesToAudio(payload.subtitles.lines, audioMs);
    if (!payload.debug.disableSubtitles) {
      await writeAssFile(scaledLines, assPath);
    } else {
      // still create empty file path reference? not needed; filter builder will ignore
      await fsp.writeFile(assPath, "", "utf8");
      log(reqId, "[/render] debug: subtitles DISABLED for A/B test");
    }

    // output
    const outName = `${jobId}.mp4`;
    const outPath = path.join(PUBLIC_DIR, outName);

    const fps = payload.spec.fps;

    const filterComplex = buildFilterComplex({
      assPath,
      title: payload.text.title,
      footer: payload.text.footer,
      audioSec,
      videoSec,
      disableSubtitles: payload.debug.disableSubtitles,
    });

    // IMPORTANT: finite image inputs (no infinite -loop without -t)
    // Audio padded by 4s and trimmed by -t
    //
    // FFmpeg progress:
    //  -progress pipe:2 prints key=value
    //  -nostats reduces noise
    const ffmpegArgs = [
      "-y",
      "-hide_banner",
      "-loglevel", "warning",

      // Progress info (diagnostic)
      "-nostats",
      "-progress", "pipe:2",

      // BG finite
      "-loop", "1",
      "-framerate", String(fps),
      "-t", videoSec.toFixed(3),
      "-i", baseBgPath,

      // Audio
      "-i", audioPath,

      // End card finite
      "-loop", "1",
      "-framerate", String(fps),
      "-t", videoSec.toFixed(3),
      "-i", endCardPath,

      // Compose
      "-filter_complex", filterComplex,

      // Extend audio by 4s
      "-filter:a", "apad=pad_dur=4",

      // Map
      "-map", "[vout]",
      "-map", "1:a",

      // Stop guards
      "-shortest",
      "-t", videoSec.toFixed(3),

      // Encode (low CPU for free plan)
      "-r", String(fps),
      "-fps_mode", "cfr",
      "-c:v", "libx264",
      "-pix_fmt", "yuv420p",
      "-profile:v", "high",
      "-preset", "ultrafast",
      "-tune", "stillimage",
      "-threads", "1",
      "-crf", "24",

      "-c:a", "aac",
      "-b:a", "160k",

      "-movflags", "+faststart",
      outPath,
    ];

    log(reqId, `[/render] ffmpeg start (timeout=${FFMPEG_TIMEOUT_MS} hard=${HARD_TIMEOUT_MS})`);

    const ff = await runFfmpegWithProgress({
      reqId,
      args: ffmpegArgs,
      timeoutMs: FFMPEG_TIMEOUT_MS,
      hardTimeoutMs: HARD_TIMEOUT_MS,
    });

    log(reqId, "[/render] done");

    const downloadUrl = `${req.protocol}://${req.get("host")}/tmp/${outName}`;
    return res.json({
      ok: true,
      download_url: downloadUrl,
      debug: {
        audio_ms: audioMs,
        video_ms: videoMs,
        elapsed_ms: Date.now() - startedAt,
        disable_subtitles: payload.debug.disableSubtitles,
        ffmpeg_last: ff.ffmpeg_last,
        ffmpeg_samples: ff.ffmpeg_samples,
      },
    });
  } catch (err) {
    log(reqId, "[/render] error", err?.message || err);

    if (err?.code === "ETIMEDOUT" || err?.code === "HARD_TIMEOUT") {
      return res.status(504).json({
        ok: false,
        error: "Render timeout",
        debug: {
          ffmpeg_last: err.ffmpeg_last,
          ffmpeg_samples: err.ffmpeg_samples,
        },
      });
    }

    return res.status(500).json({
      ok: false,
      error: err?.message || "Unknown error",
      debug: {
        code: err?.code,
        ffmpeg_last: err?.ffmpeg_last,
        ffmpeg_samples: err?.ffmpeg_samples,
      },
    });
  } finally {
    renderBusy = false;

    // Cleanup
    try { if (audioPath && fs.existsSync(audioPath)) await fsp.unlink(audioPath); } catch {}
    try { if (assPath && fs.existsSync(assPath)) await fsp.unlink(assPath); } catch {}
  }
});

// ---------- Start + graceful shutdown ----------
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`motionalyx-render-endpoint listening on :${PORT}`);
});

function shutdown(signal) {
  console.log(`${nowIso()} [shutdown] received ${signal}`);
  server.close(() => {
    console.log(`${nowIso()} [shutdown] server closed`);
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
