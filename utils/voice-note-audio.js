/**
 * Voice note audio helpers. Chrome fake mic: convert to WAV; no PulseAudio.
 */

const { spawn, spawnSync } = require('child_process');
const fs = require('fs');
const { CHROME_FAKE_MIC_WAV } = require('./chrome-fake-mic');

function ffmpegBin() {
  return process.env.FFMPEG_PATH || process.env.FFMPEG_BIN || 'ffmpeg';
}

function ffprobeBin() {
  return process.env.FFPROBE_PATH || process.env.FFPROBE_BIN || 'ffprobe';
}

/** True if ffmpeg + ffprobe are on PATH. Required for voice-note conversion. */
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

/**
 * Leading / trailing silence (seconds) so the fake-mic loop hits silence, not speech.
 * Defaults: ~0.12s lead-in, ~0.08s tail (short tail = less dead air at end of the sent note).
 * Set `VOICE_FAKE_MIC_PAD_START_SEC` / `VOICE_FAKE_MIC_PAD_END_SEC` to override (use `0` to disable that side).
 */
function getChromeFakeMicPadSec() {
  const start = parseFloat(process.env.VOICE_FAKE_MIC_PAD_START_SEC);
  const end = parseFloat(process.env.VOICE_FAKE_MIC_PAD_END_SEC);
  const defStart = 0.12;
  /** Short tail — long pad + hold = audible silence before loop; tune VOICE_FAKE_MIC_PAD_END_SEC. */
  const defEnd = 0.08;
  const padStart = Number.isFinite(start) && start >= 0 ? Math.min(start, 5) : defStart;
  const padEnd = Number.isFinite(end) && end >= 0 ? Math.min(end, 5) : defEnd;
  return { padStart, padEnd };
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
 * Resample to 48 kHz stereo s16 for Chrome’s fake mic. Uses wider SWR window for cleaner down/up-sampling.
 * Advanced: set `VOICE_FAKE_MIC_ARESAMPLE_FILTER` to a full `aresample=…,aformat=…` chain (no brackets).
 */
function fakeMicInputResampleStereo() {
  const custom = (process.env.VOICE_FAKE_MIC_ARESAMPLE_FILTER || '').trim();
  if (custom) return custom;
  return 'aresample=48000:filter_size=256:cutoff=0.973,aformat=sample_fmts=s16:channel_layouts=stereo';
}

/**
 * Convert audio to Chrome fake mic format and write to /tmp/current-voice-note.wav.
 * Chrome expects: 48kHz, stereo, s16.
 *
 * Chromium loops the WAV forever; we prepend/append silence so when the loop wraps,
 * it plays silence instead of the start of the message again. Hold time should match
 * the returned duration (full padded file) + small jitter.
 *
 * @returns {{ durationSec: number, padStartSec: number, padEndSec: number }}
 */
function convertToChromeFakeMicWav(inputPath, logger = null) {
  if (!inputPath || !fs.existsSync(inputPath)) {
    throw new Error('voice_note_file_not_found');
  }
  const { padStart, padEnd } = getChromeFakeMicPadSec();
  const bin = ffmpegBin();

  const argsSimple = [
    '-y',
    '-i',
    inputPath,
    '-filter:a',
    fakeMicInputResampleStereo(),
    '-f',
    'wav',
    CHROME_FAKE_MIC_WAV,
  ];

  let result;
  if (padStart <= 0 && padEnd <= 0) {
    result = spawnSync(bin, argsSimple, { encoding: 'utf8', timeout: 30000 });
  } else if (padStart <= 0 && padEnd > 0) {
    const fcEndOnly =
      `[0:a]${fakeMicInputResampleStereo()}[main];` + `[main]apad=pad_dur=${padEnd}[out]`;
    result = spawnSync(
      bin,
      [
        '-y',
        '-i',
        inputPath,
        '-filter_complex',
        fcEndOnly,
        '-map',
        '[out]',
        '-ar',
        '48000',
        '-ac',
        '2',
        '-sample_fmt',
        's16',
        '-f',
        'wav',
        CHROME_FAKE_MIC_WAV,
      ],
      { encoding: 'utf8', timeout: 30000 }
    );
  } else {
    const midToOut =
      padEnd > 0
        ? `[mid]apad=pad_dur=${padEnd}[out]`
        : `[mid]aformat=sample_fmts=s16:channel_layouts=stereo[out]`;
    const fc =
      `[0:a]aformat=sample_fmts=s16:channel_layouts=stereo[pre];` +
      `[1:a]${fakeMicInputResampleStereo()}[main];` +
      `[pre][main]concat=n=2:v=0:a=1[mid];` +
      midToOut;
    const paddedArgs = [
      '-y',
      '-f',
      'lavfi',
      '-t',
      String(Math.max(padStart, 0.001)),
      '-i',
      'anullsrc=r=48000:cl=stereo',
      '-i',
      inputPath,
      '-filter_complex',
      fc,
      '-map',
      '[out]',
      '-ar',
      '48000',
      '-ac',
      '2',
      '-sample_fmt',
      's16',
      '-f',
      'wav',
      CHROME_FAKE_MIC_WAV,
    ];
    result = spawnSync(bin, paddedArgs, { encoding: 'utf8', timeout: 30000 });
  }

  if (result.status !== 0) {
    const err = (result.stderr || result.error || '').slice(-500);
    throw new Error(`voice_note_convert_failed: ${err}`);
  }

  const durationOut = fs.existsSync(CHROME_FAKE_MIC_WAV)
    ? getAudioDurationSec(CHROME_FAKE_MIC_WAV)
    : 7;

  if (logger && typeof logger.log === 'function') {
    const padNote =
      padStart > 0 || padEnd > 0 ? ` pad=${padStart}s+${padEnd}s` : '';
    logger.log(
      `[voice] Converted to Chrome fake mic format: ${inputPath} → ${CHROME_FAKE_MIC_WAV} (${durationOut.toFixed(1)}s${padNote})`
    );
  }
  return { durationSec: durationOut, padStartSec: padStart, padEndSec: padEnd };
}

/**
 * Ensure /tmp/current-voice-note.wav exists (silent placeholder) so Chrome can launch.
 * Call before first browser launch when no voice file has been converted yet.
 */
function ensureChromeFakeMicPlaceholder(logger = null) {
  if (fs.existsSync(CHROME_FAKE_MIC_WAV)) return;
  const bin = ffmpegBin();
  const result = spawnSync(
    bin,
    [
      '-y',
      '-f', 'lavfi',
      '-i', 'anullsrc=r=48000:cl=stereo',
      '-t', '1',
      '-ar', '48000',
      '-ac', '2',
      '-sample_fmt', 's16',
      '-f', 'wav',
      CHROME_FAKE_MIC_WAV,
    ],
    { encoding: 'utf8', timeout: 5000 }
  );
  if (result.status !== 0 && logger && typeof logger.warn === 'function') {
    logger.warn(`[voice] Could not create placeholder ${CHROME_FAKE_MIC_WAV}: ${result.stderr || ''}`);
  }
}

module.exports = {
  getAudioDurationSec,
  getChromeFakeMicPadSec,
  fakeMicInputResampleStereo,
  isFfmpegAvailable,
  convertToChromeFakeMicWav,
  ensureChromeFakeMicPlaceholder,
  ffmpegBin,
  ffprobeBin,
};
