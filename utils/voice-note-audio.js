const { spawn, spawnSync } = require('child_process');
const fs = require('fs');
const {
  getVoiceNotePipePath,
  getVoiceAudioMode,
  getPulseClientEnv,
  isPipeSourceReady,
  pausePipeSilenceFiller,
  resumePipeSilenceFiller,
  VOICE_NOTE_SOURCE_NAME,
} = require('./pulse-pipe-source');

function ffmpegBin() {
  return process.env.FFMPEG_PATH || process.env.FFMPEG_BIN || 'ffmpeg';
}

function ffprobeBin() {
  return process.env.FFPROBE_PATH || process.env.FFPROBE_BIN || 'ffprobe';
}

/** True if ffmpeg + ffprobe are on PATH (or FFMPEG_PATH / FFPROBE_PATH). Required for voice-note pipe feeding. */
function isFfmpegAvailable() {
  try {
    const a = spawnSync(ffmpegBin(), ['-hide_banner', '-version'], { encoding: 'utf8' });
    const b = spawnSync(ffprobeBin(), ['-version'], { encoding: 'utf8' });
    if (a.error || b.error) return false;
    return a.status === 0 && b.status === 0;
  } catch {
    return false;
  }
}

function getAudioDurationSec(audioPath) {
  const probe = spawnSync(
    ffprobeBin(),
    [
      '-v',
      'error',
      '-show_entries',
      'format=duration',
      '-of',
      'default=noprint_wrappers=1:nokey=1',
      audioPath,
    ],
    { encoding: 'utf8' }
  );
  if (probe.status !== 0) return 7;
  const value = parseFloat((probe.stdout || '').trim());
  if (!Number.isFinite(value) || value <= 0) return 7;
  return Math.min(Math.max(value, 1), 60);
}

/**
 * Start ffmpeg feeding the voice-note audio. Uses pipe-source (raw PCM → fifo) or
 * null-sink (ffmpeg -f pulse → sink) depending on getVoiceAudioMode().
 */
function startVoiceNotePlayback(audioPath, _pipePathOrSink, logger, timeoutMs = 90000) {
  if (!audioPath) throw new Error('voice_note_path_missing');
  if (!fs.existsSync(audioPath)) throw new Error('voice_note_file_not_found');
  if (!isPipeSourceReady()) {
    throw new Error(
      'voice_pipe_source_not_ready: PulseAudio setup failed. Voice notes require a VPS with PulseAudio. Install: sudo apt install pulseaudio.'
    );
  }
  const durationSec = getAudioDurationSec(audioPath);
  const mode = getVoiceAudioMode();
  const sinkName = _pipePathOrSink?.sink || VOICE_NOTE_SOURCE_NAME;

  const bin = ffmpegBin();
  let child;

  if (mode === 'nullsink') {
    // ffmpeg writes directly to Pulse sink; Chromium captures from sink.monitor
    const args = [
      '-re', '-stream_loop', '0', '-i', audioPath,
      '-vn', '-ac', '1', '-ar', '48000',
      '-f', 'pulse', sinkName,
    ];
    child = spawn(bin, args, { stdio: ['ignore', 'ignore', 'pipe'], env: getPulseClientEnv() });
  } else {
    // pipe-source: ffmpeg → raw PCM → fifo
    pausePipeSilenceFiller();
    spawnSync('sleep', ['0.05'], { encoding: 'utf8' });
    const resumeIfNeeded = () => resumePipeSilenceFiller(logger);
    const pipePath = getVoiceNotePipePath();

    let pipeFd;
    try {
      pipeFd = fs.openSync(pipePath, 'w');
    } catch (e) {
      resumeIfNeeded();
      throw new Error(`voice_note_pipe_open_failed: ${pipePath}`);
    }

    const args = [
      '-re', '-stream_loop', '0', '-i', audioPath,
      '-vn', '-ac', '2', '-ar', '48000', '-f', 's16le', '-',
    ];
    child = spawn(bin, args, { stdio: ['ignore', pipeFd, 'pipe'] });

    child.on('exit', () => {
      try { fs.closeSync(pipeFd); } catch { /* ignore */ }
      resumeIfNeeded();
    });
    child.on('error', () => { if (!child.killed) resumeIfNeeded(); });
  }

  let stderrBuf = '';
  if (child.stderr) {
    child.stderr.on('data', (d) => {
      if (stderrBuf.length < 2000) stderrBuf += d.toString();
    });
  }
  const timeout = setTimeout(() => child.kill('SIGTERM'), timeoutMs);
  let exited = false;
  child.on('exit', () => {
    exited = true;
    clearTimeout(timeout);
  });
  child.on('error', (err) => {
    if (err && err.code === 'ENOENT' && logger) {
      logger.warn(`ffmpeg not found (${bin}). Install: sudo apt install ffmpeg.`);
    } else if (logger) {
      logger.warn('ffmpeg spawn error: ' + (err && err.message ? err.message : String(err)));
    }
  });

  if (logger) logger.log(`Voice playback started (${durationSec.toFixed(1)}s) ${mode === 'nullsink' ? '→ pulse sink' : '→ pipe'}: ${audioPath}`);
  return {
    durationSec,
    stop: () => {
      if (!exited) child.kill('SIGTERM');
    },
    getStderr: () => stderrBuf.slice(-1000),
  };
}

module.exports = { startVoiceNotePlayback, getAudioDurationSec, isFfmpegAvailable, ffmpegBin, ffprobeBin };
