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

module.exports = {
  getBrowserProfilesRoot,
  ensureProfilesRoot,
  resolveHeadlessMode,
  baseChromeArgs,
  assignPersistentUserDataDir,
};
