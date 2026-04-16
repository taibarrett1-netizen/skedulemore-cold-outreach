/**
 * Shared Chrome launch helpers: persistent userDataDir, headless modes, baseline args.
 * Used by bot.js (send worker) and scraper.js (platform pool scrapes).
 */
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

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

/** True if `dir` is inside our managed .browser-profiles tree (never touch arbitrary paths). */
function isPathUnderBrowserProfilesRoot(absDir) {
  if (!absDir || typeof absDir !== 'string') return false;
  const root = path.resolve(getBrowserProfilesRoot());
  const d = path.resolve(absDir);
  return d === root || d.startsWith(root + path.sep);
}

/** Puppeteer/Chromium failed because the profile singleton is held (stale process after PM2 restart/OOM). */
function isChromeProfileSingletonLockError(err) {
  const msg = String((err && err.message) || err || '');
  return (
    /profile appears to be in use|process_singleton|another Chromium process|SingletonLock/i.test(msg) ||
    (/Failed to launch the browser process/i.test(msg) && /Code:\s*21/i.test(msg))
  );
}

/**
 * SingletonLock names another machine. Headless Chromium will not auto-unlink that (no GUI unlock);
 * deleting lock files is unreliable if the profile was copied from NFS or another droplet.
 */
function isChromeSingletonForeignHostError(err) {
  const msg = String((err && err.message) || err || '');
  return /another computer|on another computer/i.test(msg);
}

const SINGLETON_NAMES = new Set(['SingletonLock', 'SingletonSocket', 'SingletonCookie']);

function listProfileSubdirsForSingleton(profileDir) {
  const out = ['Default'];
  try {
    for (const ent of fs.readdirSync(profileDir, { withFileTypes: true })) {
      if (ent.isDirectory() && /^Profile \d+$/i.test(ent.name)) out.push(ent.name);
    }
  } catch (_) {}
  return out;
}

/** Remove Chromium singleton files (root + Default + Profile N). */
function unlinkSingletonArtifactsUnderProfile(profileDir) {
  const dirs = [profileDir, ...listProfileSubdirsForSingleton(profileDir).map((sd) => path.join(profileDir, sd))];
  for (const d of dirs) {
    for (const name of SINGLETON_NAMES) {
      const p = path.join(d, name);
      try {
        if (fs.existsSync(p)) fs.unlinkSync(p);
      } catch (_) {}
    }
  }
}

/**
 * Kill Linux Chromium/Chrome processes whose argv references this user-data-dir (stray after PM2 restart).
 * Child GPU/renderer processes often omit --user-data-dir; killing the browser parent is enough when we match it.
 */
function killLinuxChromiumProcessesUsingProfileDir(profileDir, log, signal = 'SIGTERM') {
  if (process.platform !== 'linux') return;
  const resolved = path.resolve(profileDir);
  let entries;
  try {
    entries = fs.readdirSync('/proc', { withFileTypes: true });
  } catch (_) {
    return;
  }
  const prefix = '--user-data-dir=';
  for (const ent of entries) {
    if (!ent.isDirectory() || !/^\d+$/.test(ent.name)) continue;
    const pidStr = ent.name;
    const pid = Number(pidStr);
    if (pid === process.pid) continue;
    let raw;
    try {
      raw = fs.readFileSync(`/proc/${pidStr}/cmdline`);
    } catch (_) {
      continue;
    }
    if (!raw || raw.length === 0) continue;
    const argv = String(raw).split('\0').filter(Boolean);
    const joined = argv.join(' ');
    let matches = joined.includes(resolved);
    if (!matches) {
      for (const a of argv) {
        if (a.startsWith(prefix)) {
          try {
            if (path.resolve(a.slice(prefix.length)) === resolved) {
              matches = true;
              break;
            }
          } catch (_) {}
        }
      }
    }
    if (!matches) continue;
    const exe = String(argv[0] || '');
    const exeBase = path.basename(exe);
    const looksLikeChrome =
      /^(chrome|chromium|chromium-browser|google-chrome|google-chrome-stable|google-chrome-beta)$/i.test(exeBase) ||
      /[\\/]chrome-linux[\\/]chrome$/i.test(exe) ||
      /[\\/]\.cache[\\/]puppeteer[\\/].+[\\/]chrome$/i.test(exe);
    if (!looksLikeChrome) continue;
    try {
      process.kill(pid, signal);
      if (log && signal === 'SIGTERM') {
        log(`[send-worker] Sent SIGTERM to Chromium pid ${pidStr} holding ${path.basename(profileDir)}`);
      }
    } catch (_) {}
  }
}

/**
 * After PM2 restart or OOM, orphan Chromium can leave SingletonLock/Socket. Kill processes using the
 * profile (fuser + /proc scan), remove stale lock files, then relaunch can succeed.
 * @param {(msg: string) => void} [log]
 */
function tryRecoverStaleChromeProfileLocks(profileDir, log) {
  if (!profileDir || typeof profileDir !== 'string') return;
  if (!isPathUnderBrowserProfilesRoot(profileDir)) {
    if (log) log('[send-worker] skip Chrome singleton recovery: path not under browser profiles root');
    return;
  }
  if (log) log(`[send-worker] Recovering stale Chromium profile locks for ${path.basename(profileDir)}…`);
  if (process.platform === 'linux') {
    killLinuxChromiumProcessesUsingProfileDir(profileDir, log, 'SIGTERM');
    try {
      spawnSync('fuser', ['-TERM', profileDir], { stdio: 'ignore', timeout: 8000 });
    } catch (_) {
      /* fuser missing or no PIDs */
    }
    killLinuxChromiumProcessesUsingProfileDir(profileDir, log, 'SIGKILL');
    try {
      spawnSync('fuser', ['-k', '-9', profileDir], { stdio: 'ignore', timeout: 8000 });
    } catch (_) {}
  }
  unlinkSingletonArtifactsUnderProfile(profileDir);
}

/**
 * Rename send-* profile away and recreate an empty dir at the same path. Safe for send worker: Instagram
 * state is re-applied from Supabase (cookies + web storage).
 * @returns {boolean}
 */
function quarantineChromePersistentSendProfileDir(profileDir, log) {
  if (!profileDir || typeof profileDir !== 'string') return false;
  if (!isPathUnderBrowserProfilesRoot(profileDir)) {
    if (log) log('[send-worker] skip profile quarantine: path not under browser profiles root');
    return false;
  }
  const resolved = path.resolve(profileDir);
  const base = path.basename(resolved);
  if (!/^send-[a-zA-Z0-9._-]+$/.test(base)) {
    if (log) log('[send-worker] skip profile quarantine: not a send-* profile directory');
    return false;
  }
  const parent = path.dirname(resolved);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const suffix = Math.random().toString(36).slice(2, 8);
  const dest = path.join(parent, `${base}.stale-singleton-${stamp}-${suffix}`);
  try {
    if (!fs.existsSync(resolved)) {
      fs.mkdirSync(resolved, { recursive: true });
      return true;
    }
    fs.renameSync(resolved, dest);
    fs.mkdirSync(resolved, { recursive: true });
    if (log) {
      log(
        `[send-worker] Quarantined Chromium profile → ${path.basename(dest)}; empty ${base} (session reloads from DB).`
      );
    }
    return true;
  } catch (err) {
    if (log) log(`[send-worker] Profile quarantine failed: ${err && err.message ? err.message : err}`);
    return false;
  }
}

module.exports = {
  getBrowserProfilesRoot,
  ensureProfilesRoot,
  resolveHeadlessMode,
  baseChromeArgs,
  assignPersistentUserDataDir,
  acquireChromeUserDataDirLock,
  isPathUnderBrowserProfilesRoot,
  isChromeProfileSingletonLockError,
  isChromeSingletonForeignHostError,
  tryRecoverStaleChromeProfileLocks,
  quarantineChromePersistentSendProfileDir,
};
