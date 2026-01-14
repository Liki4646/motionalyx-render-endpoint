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

// -------------------- Directories --------------------
const PUBLIC_DIR = path.join(os.tmpdir(), "mxpublic");
const CACHE_DIR = path.join(os.tmpdir(), "mxcache");
fs.mkdirSync(PUBLIC_DIR, { recursive: true });
fs.mkdirSync(CACHE_DIR, { recursive: true });

// serve rendered files
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

// -------------------- Upload --------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 60 * 1024 * 1024 }
});

// -------------------- Settings --------------------
const EXTRA_TAIL_MS = 4000;               // end card length
const FFMPEG_TIMEOUT_MS = 180000;         // keep under Make 300s (3 min)
const DOWNLOAD_TIMEOUT_MS = 20000;

const DEJAVU_TTF = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

// -------------------- Health --------------------
app.get("/", (_, res) => res.status(200).send("ok"));
app.get("/health", (_, res) => res.status(200).json({ ok: true }));

// -------------------- Helpers --------------------
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function downloadToFile(url, outPath, timeoutMs = DOWNLOAD_TIMEOUT_MS) {
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
        return reject(new Error(`Failed to download asset: ${res.statusCode}`));
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
  await downloadToFile(url, tmp, DOWNLOAD_TIMEOUT_MS);
  fs.renameSync(tmp, cached);
  return cached;
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
      try {
        child.kill("SIGKILL");
      } catch {}
      reject(new Error(`ffmpeg timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("exit", () => clearTimeout(t));
  });
}

// ASS helpers
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

function escapeDrawtext(s) {
  let t = String(s ?? "");
  t = t.replace(/\\/g, "\\\\");
  t = t.replace(/:/g, "\\:");
  t = t.replace(/'/g, "\\'");
  t = t.replace(/%/g, "\\%");
  t = t.replace(/[\r\n]+/g, " ").trim();
  return t;
}

// -------------------- Render --------------------
app.post("/render", upload.single("audio"), async (req, res) => {
  let workDir = null;

  try {
    const startedAt = Date.now();
    console.log("[/render] request start");

    if (!req.file) return res.status(400).json({ error: "Missing audio file field 'audio'." });
    if (!req.body?.payload) return res.status(400).json({ error: "Missing text field 'payload'." });

    let payload;
    try {
      payload = JSON.parse(req.body.payload);
    } catch {
      return res.status(400).json({ error: "Payload is not valid JSON." });
    }

    // Required assets for Option 3
    const baseBgUrl =
      payload?.assets?.base_background_url ||
      payload?.assets?.background_template_url; // backward compat

    const endCardUrl = payload?.assets?.end_card_url;

    // Optional (ignored for now if missing / empty)
    const cardUrls = Array.isArray(payload?.assets?.card_image_urls)
      ? payload.assets.card_image_urls.filter((u) => typeof u === "string" && u.trim()).slice(0, 3)
      : [];

    const title = payload?.text?.title ?? "";
    const footer = payload?.text?.footer ?? "";
    const rawLines = Array.isArray(payload?.subtitles?.lines) ? payload.subtitles.lines : [];

    if (!baseBgUrl) return res.status(400).json({ error: "payload.assets.base_background_url is required." });
    if (!endCardUrl) return res.status(400).json({ error: "payload.assets.end_card_url is required." });

    workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mxrender-"));
    const audioPath = path.join(workDir, "audio.mp3");
    fs.writeFileSync(audioPath, req.file.buffer);

    // Download assets (cached)
    const baseBgFile = await getOrDownload(baseBgUrl);
    const endCardFile = await getOrDownload(endCardUrl);

    // (Optional) card downloads — we will not fail if they’re missing
    const cardFiles = [];
    for (const u of cardUrls) {
      try {
        cardFiles.push(await getOrDownload(u));
      } catch (e) {
        console.log("[/render] card download failed:", String(e?.message || e));
      }
    }

    // Audio duration
    let audioMs = await getAudioDurationMs(audioPath);
    if (!audioMs) audioMs = 12000; // fallback

    const audioSec = (audioMs / 1000).toFixed(3);
    const videoMs = audioMs + EXTRA_TAIL_MS;
    const videoSec = (videoMs / 1000).toFixed(3);
    const audioEndSec = (audioMs / 1000).toFixed(3);

    // Build subtitles ASS (only if we got lines)
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

    const assEvents = rawLines
      .filter((l) => typeof l?.start_ms === "number" && typeof l?.end_ms === "number" && l?.text)
      .map((l) => {
        const start = msToAssTime(l.start_ms);
        const end = msToAssTime(l.end_ms);
        const clean = escapeAssText(l.text);
        const wrapped = wrapTwoLines(clean, 26);
        return `Dialogue: 0,${start},${end},Default,,0,0,0,,${wrapped}`;
      })
      .join("\n");

    fs.writeFileSync(assPath, assHeader + (assEvents ? assEvents + "\n" : ""));

    // Build filter_complex (SIMPLE + STABLE)
    const tTitle = escapeDrawtext(title);
    const tFooter = escapeDrawtext(footer);

    // show title/footer only during audio (escape commas with \\,)
    const titleDraw =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=82:fontcolor=white:` +
      `x=(w-text_w)/2:y=220:text='${tTitle}':shadowx=0:shadowy=2:shadowcolor=black@0.35:` +
      `enable=lt(t\\,${audioSec})`;

    const footerDraw =
      `drawtext=fontfile=${DEJAVU_TTF}:fontsize=46:fontcolor=white:` +
      `x=(w-text_w)/2:y=h-260:text='${tFooter}':shadowx=0:shadowy=2:shadowcolor=black@0.35:` +
      `enable=lt(t\\,${audioSec})`;

    // subtitles only during audio (ASS already ends at audio end if your chunks do)
    const subsFilter = `subtitles=${assPath}`;

    // Inputs:
    // 0 = base bg image
    // 1 = end card image
    // 2 = audio
    // (cards are ignored in this “reset” version to guarantee stability; we’ll add them back once it runs)
    const filterParts = [
      `[0:v]scale=1080:1920,format=rgba[bg]`,
      `[1:v]scale=1080:1920,format=rgba[end]`,
      `[bg]${titleDraw},${footerDraw},${subsFilter}[main]`,
      // End card only for last 4s (escape commas)
      `[main][end]overlay=x=0:y=0:enable=between(t\\,${audioEndSec}\\,${videoSec})[vout]`
    ];

    // IMPORTANT: join with ';' and no trailing empties
    const filterComplex = filterParts.join(";");

    // Output path
    const publicName = `mx_${Date.now()}_${Math.random().toString(16).slice(2)}.mp4`;
    const publicOutPath = path.join(PUBLIC_DIR, publicName);

    // Audio trim safety
    const audioFilter = `atrim=0:${audioSec},asetpts=N/SR/TB`;

    const args = [
      "-y",
      "-hide_banner",
      "-loglevel",
      "error",
      "-nostats",

      // base bg
      "-loop",
      "1",
      "-i",
      baseBgFile,

      // end card
      "-loop",
      "1",
      "-i",
      endCardFile,

      // audio
      "-i",
      audioPath,

      "-filter_complex",
      filterComplex,

      "-map",
      "[vout]",
      "-map",
      "2:a",

      "-t",
      videoSec,
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
      "192k",

      publicOutPath
    ];

    console.log("[/render] ffmpeg start", {
      audio_ms: audioMs,
      video_ms: videoMs,
      cards_downloaded: cardFiles.length
    });

    await runFfmpeg(args, FFMPEG_TIMEOUT_MS);

    const proto = req.get("x-forwarded-proto") || req.protocol;
    const baseUrl = `${proto}://${req.get("host")}`;

    console.log("[/render] done", { ms: Date.now() - startedAt });

    return res.status(200).json({
      ok: true,
      download_url: `${baseUrl}/tmp/${publicName}`,
      debug: {
        audio_ms: audioMs,
        video_ms: videoMs,
        cards_downloaded: cardFiles.length
      }
    });
  } catch (e) {
    console.log("[/render] error", String(e?.message || e));
    // If ffmpeg timed out, return 504 so Make stops waiting cleanly
    const msg = String(e?.message || e);
    const isTimeout = msg.toLowerCase().includes("timed out");
    return res.status(isTimeout ? 504 : 500).json({ error: msg });
  } finally {
    try {
      if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    } catch {}
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render endpoint running on :${port}`));
