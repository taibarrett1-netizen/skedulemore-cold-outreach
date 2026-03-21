const { spawn, spawnSync } = require('child_process');
const fs = require('fs');

function getAudioDurationSec(audioPath) {
  const probe = spawnSync(
    'ffprobe',
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
  const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
  let stderrBuf = '';
  if (child.stderr) {
    child.stderr.on('data', (d) => {
      if (stderrBuf.length < 2000) stderrBuf += d.toString();
    });
  }
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

module.exports = { startVoiceNotePlayback, getAudioDurationSec };
