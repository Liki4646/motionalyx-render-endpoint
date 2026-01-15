'use strict';

const express = require('express');
const multer = require('multer');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');

const app = express();

// --- Multer: multipart/form-data (audio file + payload text) ---
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB
  },
});

const PORT = process.env.PORT || 3000;

// ---- Helpers ----
function run(cmd, args, { timeoutMs = 10 * 60 * 1000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';

    const t = setTimeout(() => {
      try { child.kill('SIGKILL'); } catch {}
      reject(new Error(`Timeout running: ${cmd} ${args.join(' ')}`));
    }, timeoutMs);

    child.stdout.on('data', (d) => (out += d.toString()));
    child.stderr.on('data', (d) => (err += d.toString()));

    child.on('error', (e) => {
      clearTimeout(t);
      reject(e);
    });

    child.on('close', (code) => {
      clearTimeout(t);
      if (code === 0) return resolve({ out, err });
      reject(new Error(`Command failed (${code}): ${cmd} ${args.join(' ')}\n${err}`));
    });
  });
}

async function ffprobeDurationSeconds(filePath) {
  // returns duration as number (seconds)
  const args = [
    '-v', 'error',
    '-show_entries', 'format=duration',
    '-of', 'default=noprint_wrappers=1:nokey=1',
    filePath
  ];
  const { out } = await run('ffprobe', args, { timeoutMs: 60_000 });
  const v = parseFloat((out || '').trim());
  if (!Number.isFinite(v)) throw new Error('ffprobe could not read duration');
  return v;
}

async function downloadToFile(url, destPath) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Download failed ${res.status} for ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  await fsp.writeFile(destPath, buf);
  return destPath;
}

function safeJsonParse(str) {
  try { return JSON.parse(str); } catch { return null; }
}

function tmpName(prefix, ext) {
  return path.join('/tmp', `${prefix}-${crypto.randomBytes(6).toString('hex')}${ext}`);
}

// ---- Routes ----

// Health check
app.get('/', (req, res) => res.status(200).send('ok'));
app.get('/health', (req, res) => res.status(200).json({ ok: true }));

/**
 * POST /
 * multipart/form-data:
 *  - audio: File
 *  - payload: Text (JSON string)
 *
 * Payload supported (optional):
 *  - background_video_url: string  (if provided -> we render mp4)
 *  - subtitles_srt: string         (optional; burned in)
 *  - extra_tail_seconds: number    (default 4)
 *  - response_mode: "json" | "binary" (default "json")
 */
app.post('/', upload.single('audio'), async (req, res) => {
  const startedAt = Date.now();

  try {
    // Validate multipart fields
    if (!req.file) {
      return res.status(400).json({ ok: false, error: 'Missing form-data file field: audio' });
    }
    const payloadRaw = (req.body && req.body.payload) ? String(req.body.payload) : '';
    const payload = safeJsonParse(payloadRaw);

    if (!payload) {
      return res.status(400).json({
        ok: false,
        error: 'Invalid JSON in form-data field: payload',
        hint: 'Make sure payload is a valid JSON string',
      });
    }

    // Always return something useful (so Make can debug)
    const meta = {
      received: {
        audio: { originalname: req.file.originalname, mimetype: req.file.mimetype, size: req.file.size },
        payload_keys: Object.keys(payload || {}),
      },
    };

    // If no background_video_url, we just acknowledge (fixes your current "Cannot POST /" issue)
    if (!payload.background_video_url) {
      return res.status(200).json({
        ok: true,
        mode: 'ack',
        message: 'POST / received. Add payload.background_video_url to enable MP4 rendering.',
        meta,
        ms: Date.now() - startedAt,
      });
    }

    // ---- Render MP4 ----
    const audioExt = (() => {
      const n = (req.file.originalname || '').toLowerCase();
      if (n.endsWith('.mp3')) return '.mp3';
      if (n.endsWith('.wav')) return '.wav';
      if (n.endsWith('.m4a')) return '.m4a';
      if (n.endsWith('.aac')) return '.aac';
      return '.bin';
    })();

    const audioPath = tmpName('audio', audioExt);
    const bgPath = tmpName('bg', '.mp4');
    const outPath = tmpName('out', '.mp4');
    const srtPath = tmpName('subs', '.srt');

    await fsp.writeFile(audioPath, req.file.buffer);

    // download background video
    await downloadToFile(payload.background_video_url, bgPath);

    // durations
    const audioDur = await ffprobeDurationSeconds(audioPath);
    const extraTail = Number.isFinite(Number(payload.extra_tail_seconds))
      ? Math.max(0, Number(payload.extra_tail_seconds))
      : 4;

    const targetDur = audioDur + extraTail;

    // subtitles (optional)
    let vf = [
      // Reels friendly
      "scale=1080:1920:force_original_aspect_ratio=increase",
      "crop=1080:1920",
      "fps=30",
      "format=yuv420p",
      `trim=duration=${targetDur}`,
      "setpts=PTS-STARTPTS",
    ];

    if (typeof payload.subtitles_srt === 'string' && payload.subtitles_srt.trim()) {
      await fsp.writeFile(srtPath, payload.subtitles_srt, 'utf8');
      // Basic styling; you can tweak later
      vf.push(`subtitles=${srtPath}:force_style='FontName=DejaVu Sans,FontSize=48,PrimaryColour=&HFFFFFF&,OutlineColour=&H000000&,BorderStyle=3,Outline=2,Shadow=0,MarginV=140'`);
    } else {
      // remove unused path
      try { await fsp.unlink(srtPath); } catch {}
    }

    const filterComplex = [
      // Video (loop if needed)
      `[0:v]${vf.join(',')}[v]`,
      // Audio: pad silence to reach targetDur, then trim exact length
      `[1:a]apad=pad_dur=${extraTail},atrim=0:${targetDur},asetpts=PTS-STARTPTS[a]`,
    ].join(';');

    const ffmpegArgs = [
      // loop background infinitely, we trim anyway
      '-stream_loop', '-1',
      '-i', bgPath,
      '-i', audioPath,
      '-filter_complex', filterComplex,
      '-map', '[v]',
      '-map', '[a]',
      '-t', String(targetDur),
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-crf', '23',
      '-c:a', 'aac',
      '-b:a', '192k',
      '-movflags', '+faststart',
      outPath,
    ];

    await run('ffmpeg', ffmpegArgs, { timeoutMs: 15 * 60 * 1000 });

    const responseMode = (payload.response_mode || 'json').toLowerCase();

    if (responseMode === 'binary') {
      // Send MP4 directly
      res.setHeader('Content-Type', 'video/mp4');
      res.setHeader('Content-Disposition', 'inline; filename="render.mp4"');
      const stream = fs.createReadStream(outPath);
      stream.on('close', async () => {
        // cleanup
        for (const p of [audioPath, bgPath, outPath]) { try { await fsp.unlink(p); } catch {} }
      });
      return stream.pipe(res);
    }

    // Default: JSON response (works with Make "Parse response = Yes")
    const mp4Buf = await fsp.readFile(outPath);
    const base64 = mp4Buf.toString('base64');

    // cleanup
    for (const p of [audioPath, bgPath, outPath]) { try { await fsp.unlink(p); } catch {} }

    return res.status(200).json({
      ok: true,
      mode: 'rendered',
      ms: Date.now() - startedAt,
      audio_duration_s: audioDur,
      target_duration_s: targetDur,
      file: {
        filename: 'render.mp4',
        mime: 'video/mp4',
        encoding: 'base64',
        data: base64,
      },
      meta,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err && err.message ? err.message : String(err),
    });
  }
});

// ---- Start ----
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
});
