/**
 * Shared Chrome launch helpers: persistent userDataDir, headless modes, baseline args.
 * Used by bot.js (send worker) and scraper.js (platform pool scrapes).
 */
const fs = require('fs');
const path = require('path');

const DEFAULT_PROFILES_ROOT = path.join(process.cwd(), '.browser-profiles');

function getBrowserProfilesRoot() {
  const raw = process.env.PUPPETEER_USER_DATA_ROOT;
  return raw && String(raw).trim() ? path.resolve(String(raw).trim()) : DEFAULT_PROFILES_ROOT;
}

function ensureProfilesRoot() {
  const root = getBrowserProfilesRoot();
  fs.mkdirSync(root, { recursive: true });
  return root;
}

/**
 * @param {string} envValue - raw env string (e.g. process.env.HEADLESS_MODE)
 * @param {boolean} [defaultHeadless=true] - when env unset
 * @returns {boolean | 'new'}
 */
function resolveHeadlessMode(envValue, defaultHeadless = true) {
  if (envValue == null || String(envValue).trim() === '') return defaultHeadless;
  const v = String(envValue).trim().toLowerCase();
  if (v === 'false' || v === '0' || v === 'no') return false;
  if (v === 'new') return 'new';
  return true;
}

/** Baseline flags for server/VPS Chrome; stealth plugin adds its own mitigations. */
function baseChromeArgs() {
  return [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-blink-features=AutomationControlled',
  ];
}

/**
 * Attach a persistent userDataDir under PUPPETEER_USER_DATA_ROOT (or .browser-profiles).
 * @param {import('puppeteer').LaunchOptions} launchOpts
 * @param {string} subdir - unique per IG session / pool row (sanitized)
 */
function assignPersistentUserDataDir(launchOpts, subdir) {
  if (!launchOpts || !subdir) return;
  const safe = String(subdir).replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 120);
  const root = ensureProfilesRoot();
  const dir = path.join(root, safe);
  fs.mkdirSync(dir, { recursive: true });
  launchOpts.userDataDir = dir;
}

function isPidRunning(pid) {
  const n = Number(pid);
  if (!Number.isFinite(n) || n <= 0) return false;
  try {
    process.kill(n, 0);
    return true;
  } catch (e) {
    return e && e.code === 'EPERM';
  }
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const LOCK_NAME = 'skedulemore-send-chrome.lock';

/**
 * Serialize Chrome launches for a persistent userDataDir (PM2 cluster: multiple Node processes
 * must not open the same profile — Chromium errors with "browser is already running").
 * @param {string} profileDir - Chrome userDataDir path
 * @param {{ log?: (msg: string) => void, waitMs?: number, pollMs?: number }} [opts]
 * @returns {Promise<{ release: () => void }>}
 */
async function acquireChromeUserDataDirLock(profileDir, opts = {}) {
  const log = typeof opts.log === 'function' ? opts.log : () => {};
  const waitMs = Math.max(30_000, Number(opts.waitMs) || 240_000);
  const pollMs = Math.max(200, Number(opts.pollMs) || 750);
  if (!profileDir || typeof profileDir !== 'string') {
    return { release: () => {} };
  }
  const lockPath = path.join(profileDir, LOCK_NAME);
  const deadline = Date.now() + waitMs;
  let lockFd = null;
  let loggedWait = false;

  const writePayload = () => {
    const body = JSON.stringify({
      pid: process.pid,
      at: Date.now(),
      worker: process.env.SEND_WORKER_ID || process.env.pm_id || process.env.name || '',
    });
    fs.writeSync(lockFd, body, 0, 'utf8');
    fs.fsyncSync(lockFd);
  };

  while (Date.now() < deadline) {
    try {
      lockFd = fs.openSync(lockPath, 'wx');
      try {
        writePayload();
      } catch (e) {
        try {
          fs.closeSync(lockFd);
        } catch {}
        lockFd = null;
        try {
          fs.unlinkSync(lockPath);
        } catch {}
        throw e;
      }
      return {
        release: () => {
          try {
            if (lockFd != null) {
              try {
                fs.closeSync(lockFd);
              } catch {}
              lockFd = null;
            }
            fs.unlinkSync(lockPath);
          } catch {}
        },
      };
    } catch (e) {
      if (e && e.code !== 'EEXIST') throw e;
      let steal = false;
      try {
        const raw = fs.readFileSync(lockPath, 'utf8');
        const j = JSON.parse(raw);
        if (j && j.pid != null && !isPidRunning(j.pid)) steal = true;
      } catch {
        steal = true;
      }
      if (steal) {
        try {
          fs.unlinkSync(lockPath);
        } catch {}
        continue;
      }
      if (!loggedWait) {
        loggedWait = true;
        log(
          `Chrome profile in use by another process (same Instagram session or PM2 cluster). Waiting for lock on ${path.basename(profileDir)}…`
        );
      }
      await sleepMs(pollMs + Math.floor(Math.random() * 400));
    }
  }
  throw new Error(
    `Timeout waiting for Chrome profile lock (${path.basename(profileDir)}). ` +
      'If no other send worker is running, kill stray Chromium for this profile or set SEND_WORKER_INSTANCES=1.'
  );
}

module.exports = {
  getBrowserProfilesRoot,
  ensureProfilesRoot,
  resolveHeadlessMode,
  baseChromeArgs,
  assignPersistentUserDataDir,
  acquireChromeUserDataDirLock,
};
