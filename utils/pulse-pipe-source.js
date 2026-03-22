/**
 * PulseAudio pipe-source setup for voice notes.
 *
 * Replaces the old null-sink + monitor approach. Uses module-pipe-source so that
 * a virtual microphone exists from the start — getUserMedia sees a valid, live
 * device immediately, and clicking the IG mic icon starts recording right away.
 *
 * At startup: unload any old null-sink, create pipe-source, set as default.
 * Per voice note: ffmpeg writes raw PCM into the pipe (see voice-note-audio.js).
 */

const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const VOICE_NOTE_SOURCE_NAME = (process.env.VOICE_NOTE_SOURCE_NAME || 'ColdDMsVoice').trim();
const VOICE_NOTE_PIPE_PATH = process.env.VOICE_NOTE_PIPE_PATH || '/tmp/cold-dms-voice.pipe';
const VOICE_USE_PIPE_SOURCE = process.env.VOICE_USE_PIPE_SOURCE !== 'false' && process.env.VOICE_USE_PIPE_SOURCE !== '0';

let pipeSourceSetupDone = false;

/** Build env so PulseAudio clients (pactl, Chromium) find the server. PM2 often lacks XDG_RUNTIME_DIR. */
function pactlEnv() {
  const env = { ...process.env };
  if (env.PULSE_SERVER) return env;
  const rt = env.XDG_RUNTIME_DIR || (typeof process.getuid === 'function' && process.getuid() === 0 ? '/run/user/0' : null);
  if (rt) {
    env.XDG_RUNTIME_DIR = rt;
    env.PULSE_RUNTIME_PATH = rt;
  }
  return env;
}

/**
 * Find and unload PulseAudio modules by name and argument match.
 * @param {string} moduleName - e.g. 'module-null-sink' or 'module-pipe-source'
 * @param {string} argMatch - e.g. 'sink_name=ColdDMsVoice' or 'source_name=ColdDMsVoice'
 * @returns {boolean} true if something was unloaded
 */
function unloadPulseModuleIfPresent(moduleName, argMatch) {
  try {
    const list = spawnSync('pactl', ['list', 'modules'], { encoding: 'utf8', env: pactlEnv() });
    if (list.status !== 0 || !list.stdout) return false;
    const blocks = list.stdout.split('\n\n');
    for (const block of blocks) {
      const nameLine = block.match(/Name:\s*(.+)/);
      const argLine = block.match(/Argument:\s*(.+)/);
      if (!nameLine || !argLine) continue;
      const name = nameLine[1].trim();
      const arg = argLine[1].trim();
      if (name === moduleName && arg.includes(argMatch)) {
        const indexMatch = block.match(/Module #(\d+)/);
        if (indexMatch) {
          const idx = indexMatch[1];
          spawnSync('pactl', ['unload-module', idx], { encoding: 'utf8' });
          return true;
        }
      }
    }
  } catch {
    /* ignore */
  }
  return false;
}

/**
 * Ensure the named pipe (fifo) exists. Create it if missing.
 */
function ensurePipeExists(pipePath) {
  try {
    if (fs.existsSync(pipePath)) {
      const stat = fs.statSync(pipePath);
      if (!stat.isFIFO()) {
        fs.unlinkSync(pipePath);
        fs.mkdirSync(path.dirname(pipePath), { recursive: true });
        spawnSync('mkfifo', [pipePath], { encoding: 'utf8' });
      }
    } else {
      fs.mkdirSync(path.dirname(pipePath), { recursive: true });
      spawnSync('mkfifo', [pipePath], { encoding: 'utf8' });
    }
    return fs.existsSync(pipePath);
  } catch {
    return false;
  }
}

/**
 * One-time setup: unload old null-sink, load pipe-source, set default source.
 * Call at bot/process startup before the first browser launch that may use voice.
 *
 * @param {{ log?: Function, warn?: Function } | null} [logger]
 * @returns {{ ok: boolean; pipePath: string; error?: string }}
 */
function ensureVoicePipeSource(logger = null) {
  if (!VOICE_USE_PIPE_SOURCE) {
    return { ok: false, pipePath: VOICE_NOTE_PIPE_PATH, error: 'VOICE_USE_PIPE_SOURCE disabled' };
  }

  if (pipeSourceSetupDone) {
    return { ok: true, pipePath: VOICE_NOTE_PIPE_PATH };
  }

  try {
    // 1. Unload any old module-null-sink (ColdDMsVoice sink from previous setup)
    const unloadedSink = unloadPulseModuleIfPresent('module-null-sink', `sink_name=${VOICE_NOTE_SOURCE_NAME}`);
    if (unloadedSink && logger) {
      logger.log(`[voice] Unloaded old null-sink (${VOICE_NOTE_SOURCE_NAME})`);
    }

    // 2. Unload any existing pipe-source so we get a clean reload
    const unloadedSource = unloadPulseModuleIfPresent('module-pipe-source', `source_name=${VOICE_NOTE_SOURCE_NAME}`);
    if (unloadedSource && logger) {
      logger.log(`[voice] Unloaded old pipe-source (${VOICE_NOTE_SOURCE_NAME})`);
    }

    // 3. Ensure the fifo exists
    if (!ensurePipeExists(VOICE_NOTE_PIPE_PATH)) {
      const err = `[voice] Failed to create fifo: ${VOICE_NOTE_PIPE_PATH}`;
      if (logger) logger.warn(err);
      return { ok: false, pipePath: VOICE_NOTE_PIPE_PATH, error: err };
    }

    // 4. Load module-pipe-source (virtual mic that reads from the pipe)
    const load = spawnSync(
      'pactl',
      [
        'load-module',
        'module-pipe-source',
        `source_name=${VOICE_NOTE_SOURCE_NAME}`,
        `file=${VOICE_NOTE_PIPE_PATH}`,
        'format=s16le',
        'rate=48000',
        'channels=2',
      ],
      { encoding: 'utf8', env: pactlEnv() }
    );

    if (load.status !== 0) {
      const err = `[voice] pactl load-module failed: ${load.stderr || load.error || 'unknown'}`;
      if (logger) logger.warn(err);
      return { ok: false, pipePath: VOICE_NOTE_PIPE_PATH, error: err };
    }

    const moduleIndex = (load.stdout || '').trim();
    if (logger) logger.log(`[voice] Loaded pipe-source ${VOICE_NOTE_SOURCE_NAME} (module ${moduleIndex})`);

    // 5. Set as default source so Chromium's getUserMedia uses it without PULSE_SOURCE
    const setDefault = spawnSync('pactl', ['set-default-source', VOICE_NOTE_SOURCE_NAME], {
      encoding: 'utf8',
      env: pactlEnv(),
    });
    if (setDefault.status !== 0 && logger) {
      logger.warn(`[voice] pactl set-default-source failed: ${setDefault.stderr || ''}`);
    } else if (logger) {
      logger.log(`[voice] Default source set to ${VOICE_NOTE_SOURCE_NAME}`);
    }

    pipeSourceSetupDone = true;
    return { ok: true, pipePath: VOICE_NOTE_PIPE_PATH };
  } catch (e) {
    const err = `[voice] Pipe-source setup error: ${e && e.message ? e.message : String(e)}`;
    if (logger) logger.warn(err);
    return { ok: false, pipePath: VOICE_NOTE_PIPE_PATH, error: err };
  }
}

/**
 * Get the pipe path. Ensure setup has run first (call ensureVoicePipeSource).
 */
function getVoiceNotePipePath() {
  return VOICE_NOTE_PIPE_PATH;
}

/** True if pipe-source setup succeeded (pactl load-module worked). */
function isPipeSourceReady() {
  return pipeSourceSetupDone;
}

/** Env for Chromium so getUserMedia finds PulseAudio (same as pactl). */
function getPulseClientEnv() {
  return pactlEnv();
}

module.exports = {
  ensureVoicePipeSource,
  getVoiceNotePipePath,
  getPulseClientEnv,
  isPipeSourceReady,
  VOICE_NOTE_SOURCE_NAME,
  VOICE_NOTE_PIPE_PATH,
};
