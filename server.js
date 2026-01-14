import express from "express";
import multer from "multer";
import { execFile } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";
import https from "https";
import http from "http";

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 } // 25MB
});

// Health check
app.get("/health", (_, res) => res.status(200).json({ ok: true }));

function downloadToFile(url, outPath) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    const req = client.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Failed to download background: ${res.statusCode}`));
        res.resume();
        return;
      }
      const file = fs.createWriteStream(outPath);
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
    });
    req.on("error", reject);
  });
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    execFile("ffmpeg", args, { maxBuffer: 1024 * 1024 * 20 }, (err, stdout, stderr) => {
      if (err) reject(new Error(stderr || err.message));
      else resolve({ stdout, stderr });
    });
  });
}

// POST /render
// multipart/form-data:
// - audio: file (mp3)
// - payload: text (JSON string)
app.post("/render", upload.single("audio"), async (req, res) => {
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
    const lines = payload?.subtitles?.lines ?? [];

    if (!bgUrl) return res.status(400).json({ error: "payload.assets.background_template_url is required." });
    if (!Array.isArray(lines)) return res.status(400).json({ error: "payload.subtitles.lines must be an array." });

    // Temp working dir
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mxrender-"));
    const audioPath = path.join(workDir, "audio.mp3");
    const outPath = path.join(workDir, "out.mp4");

    fs.writeFileSync(audioPath, req.file.buffer);

    // Download background (png/jpg/mp4)
    const bgExt = (new URL(bgUrl)).pathname.split(".").pop() || "mp4";
    const bgFile = path.join(workDir, `bg.${bgExt}`);
    await downloadToFile(bgUrl, bgFile);

    // Build ASS subtitle file
    const assPath = path.join(workDir, "subs.ass");
    const safe = (s) => String(s).replace(/[\r\n]+/g, " ").replace(/&/g, "&amp;");

    const assHeader = `[Script Info]
Title: Motionalyx Subtitles
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,DejaVu Sans,54,&H00FFFFFF,&H00FFFFFF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,3,0,2,140,140,180,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
`;

    const msToAssTime = (ms) => {
      const total = Math.max(0, Math.floor(ms));
      const cs = Math.floor((total % 1000) / 10);
      const s = Math.floor(total / 1000) % 60;
      const m = Math.floor(total / 60000) % 60;
      const h = Math.floor(total / 3600000);
      const pad = (n, w = 2) => String(n).padStart(w, "0");
      return `${h}:${pad(m)}:${pad(s)}.${pad(cs)}`;
    };

    const assEvents = lines
      .filter(l => typeof l?.start_ms === "number" && typeof l?.end_ms === "number" && l?.text)
      .map(l => {
        const start = msToAssTime(l.start_ms);
        const end = msToAssTime(l.end_ms);
        const text = safe(l.text);
        return `Dialogue: 0,${start},${end},Default,,0,0,0,,${text}`;
      })
      .join("\n");

    fs.writeFileSync(assPath, assHeader + assEvents + "\n");

    // Title/Footer via drawtext
    const esc = (s) => safe(s).replace(/'/g, "\\'");
    const titleFilter = `drawtext=font=DejaVuSans:fontsize=64:fontcolor=white:x=(w-text_w)/2:y=240:text='${esc(title)}'`;
    const footerFilter = `drawtext=font=DejaVuSans:fontsize=42:fontcolor=white:x=(w-text_w)/2:y=h-220:text='${esc(footer)}'`;

    const vf = `${titleFilter},${footerFilter},subtitles=${assPath}`;
    const isImage = /\.(png|jpg|jpeg)$/i.test(bgFile);

    const args = isImage
      ? [
          "-y",
          "-loop", "1",
          "-i", bgFile,
          "-i", audioPath,
          "-tune", "stillimage",
          "-c:v", "libx264",
          "-pix_fmt", "yuv420p",
          "-vf", vf,
          "-shortest",
          "-r", "30",
          "-s", "1080x1920",
          "-c:a", "aac",
          "-b:a", "192k",
          outPath
        ]
      : [
          "-y",
          "-i", bgFile,
          "-i", audioPath,
          "-c:v", "libx264",
          "-pix_fmt", "yuv420p",
          "-vf", vf,
          "-shortest",
          "-r", "30",
          "-s", "1080x1920",
          "-c:a", "aac",
          "-b:a", "192k",
          outPath
        ];

    await runFfmpeg(args);

    const stat = fs.statSync(outPath);
    res.setHeader("Content-Type", "video/mp4");
    res.setHeader("Content-Length", stat.size);
    res.setHeader("Content-Disposition", "inline; filename=motionalyx.mp4");
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Render endpoint running on :${port}`));
