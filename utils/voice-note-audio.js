const { spawn, spawnSync } = require('child_process');
const fs = require('fs');

function ffmpegBin() {
  return process.env.FFMPEG_PATH || process.env.FFMPEG_BIN || 'ffmpeg';
}

function ffprobeBin() {
  return process.env.FFPROBE_PATH || process.env.FFPROBE_BIN || 'ffprobe';
}

/** True if ffmpeg + ffprobe are on PATH (or FFMPEG_PATH / FFPROBE_PATH). Required for voice-note playback to PulseAudio. */
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

function startVoiceNotePlayback(audioPath, sinkName, logger, timeoutMs = 90000) {
  if (!audioPath) throw new Error('voice_note_path_missing');
  if (!fs.existsSync(audioPath)) throw new Error('voice_note_file_not_found');
  const durationSec = getAudioDurationSec(audioPath);
  const args = [
    '-re',
    '-stream_loop',
    '0',
    '-i',
    audioPath,
    '-vn',
    '-ac',
    '1',
    '-ar',
    '48000',
    '-f',
    'pulse',
    sinkName,
  ];
  const bin = ffmpegBin();
  const child = spawn(bin, args, { stdio: ['ignore', 'ignore', 'pipe'] });
  let stderrBuf = '';
  if (child.stderr) {
    child.stderr.on('data', (d) => {
      if (stderrBuf.length < 2000) stderrBuf += d.toString();
    });
  }
  child.on('error', (err) => {
    if (err && err.code === 'ENOENT') {
      if (logger) {
        logger.warn(
          `ffmpeg not found (${bin}). Install on the VPS: sudo apt install ffmpeg. Or set FFMPEG_PATH to the full binary path.`
        );
      }
    } else if (logger) {
      logger.warn('ffmpeg spawn error: ' + (err && err.message ? err.message : String(err)));
    }
  });
  const timeout = setTimeout(() => {
    child.kill('SIGTERM');
  }, timeoutMs);
  let exited = false;
  child.on('exit', () => {
    exited = true;
    clearTimeout(timeout);
  });
  if (logger) logger.log(`Voice playback started (${durationSec.toFixed(1)}s): ${audioPath}`);
  return {
    durationSec,
    stop: () => {
      if (!exited) child.kill('SIGTERM');
    },
    getStderr: () => stderrBuf.slice(-1000),
  };
}

module.exports = { startVoiceNotePlayback, getAudioDurationSec, isFfmpegAvailable, ffmpegBin, ffprobeBin };
