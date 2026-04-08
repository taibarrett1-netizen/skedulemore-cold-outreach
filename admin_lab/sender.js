/**
 * Admin Cold Outreach Lab — isolated sender: Puppeteer + optional Decodo proxy.
 * Does not use cold_dm_* tables. Session stored on disk (ADMIN_LAB_SESSION_PATH).
 */
const fs = require('fs').promises;
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { login, sendDM, completeInstagram2FA } = require('../bot');
const { applyMobileEmulation, applyDesktopEmulation } = require('../utils/mobile-viewport');
const { ensureChromeFakeMicPlaceholder } = require('../utils/voice-note-audio');
const {
  appendChromeFakeMicArgs,
} = require('../utils/chrome-fake-mic');
const logger = require('../utils/logger');

puppeteer.use(StealthPlugin());

const DEFAULT_SESSION_DIR = path.join(__dirname, '.sessions');
const SESSION_FILE = process.env.ADMIN_LAB_SESSION_PATH
  ? path.resolve(process.env.ADMIN_LAB_SESSION_PATH)
  : path.join(DEFAULT_SESSION_DIR, 'sender.json');

/**
 * Parse http(s)://user:pass@host:port for Puppeteer --proxy-server + page.authenticate.
 */
function parseProxyUrl(proxyUrl) {
  if (!proxyUrl || typeof proxyUrl !== 'string') return null;
  const trimmed = proxyUrl.trim();
  if (!trimmed) return null;
  let u;
  try {
    u = new URL(trimmed);
  } catch {
    return null;
  }
  const port = u.port || (u.protocol === 'https:' ? '443' : '80');
  const server = `http://${u.hostname}:${port}`;
  const username = u.username ? decodeURIComponent(u.username) : null;
  const password = u.password ? decodeURIComponent(u.password) : null;
  return { server, username, password, raw: trimmed };
}

function buildLaunchOptions(proxyParsed) {
  ensureChromeFakeMicPlaceholder(logger);
  const launch = {
    headless: process.env.HEADLESS_MODE !== 'false',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
  };
  if (proxyParsed) {
    launch.args.push(`--proxy-server=${proxyParsed.server}`);
  }
  appendChromeFakeMicArgs(launch.args);
  const slowMo = parseInt(process.env.PUPPETEER_SLOW_MO_MS || '0', 10) || 0;
  if (slowMo > 0) launch.slowMo = slowMo;
  return launch;
}

async function ensureSessionDir() {
  await fs.mkdir(path.dirname(SESSION_FILE), { recursive: true });
}

async function saveAdminLabSession(payload) {
  await ensureSessionDir();
  await fs.writeFile(SESSION_FILE, JSON.stringify(payload, null, 2), 'utf8');
}

async function loadLabSession() {
  try {
    const raw = await fs.readFile(SESSION_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function hasSessionIdCookie(cookies) {
  if (!Array.isArray(cookies)) return false;
  return cookies.some((c) => c && c.name === 'sessionid' && c.value);
}

/**
 * bot.js logs "Logged in" when the URL no longer contains /accounts/login — that can still be a
 * challenge/suspension page without sessionid. Load home and wait until sessionid exists or fail clearly.
 */
async function waitForWebSessionOrThrow(page) {
  const deadline = Date.now() + 45000;
  await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 45000 }).catch(() => {});
  while (Date.now() < deadline) {
    const cookies = await page.cookies();
    if (hasSessionIdCookie(cookies)) return cookies;
    const u = page.url();
    if (u.includes('/accounts/login') || u.includes('/challenge') || u.includes('/suspended')) {
      const snippet = await page.evaluate(() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 400)).catch(() => '');
      throw new Error(
        `Instagram did not establish a web session (no sessionid cookie). Final URL: ${u}. ${snippet ? `Page: ${snippet.replace(/\s+/g, ' ').slice(0, 200)}` : ''} Open Instagram in a normal browser, approve "This was me" / complete any check, then Connect again with the same proxy.`,
      );
    }
    await new Promise((r) => setTimeout(r, 1500));
  }
  const cookies = await page.cookies();
  if (hasSessionIdCookie(cookies)) return cookies;
  throw new Error(
    'Timed out waiting for sessionid cookie after login. Instagram may require manual approval in the app or blocked this login — try again after confirming the session on a phone.',
  );
}

/**
 * Minimal adapter so sendDM does not touch Supabase campaign state.
 */
function createAdminLabAdapter(messageText) {
  return {
    dailyLimit: 999999,
    maxPerHour: 999999,
    alreadySent: async () => false,
    logSentMessage: async () => {},
    getDailyStats: async () => ({ total_sent: 0 }),
    getHourlySent: async () => 0,
    getControl: async () => 0,
    setControl: async () => {},
    getRandomMessage: () => messageText || '',
  };
}

/**
 * Connect Instagram through proxy; returns cookies or 2FA pending object shape.
 */
async function adminLabConnect({ username, password, proxyUrl, twoFactorCode }) {
  const useMobile = process.env.DISABLE_MOBILE_LOGIN !== '1' && process.env.DISABLE_MOBILE_LOGIN !== 'true';
  const proxyParsed = parseProxyUrl(proxyUrl || process.env.ADMIN_LAB_DECODO_PROXY_URL || '');
  const launchOpts = buildLaunchOptions(proxyParsed);
  const browser = await puppeteer.launch(launchOpts);
  let keepBrowserOpen = false;
  try {
    const page = await browser.newPage();
    if (proxyParsed && proxyParsed.username && proxyParsed.password) {
      await page.authenticate({ username: proxyParsed.username, password: proxyParsed.password });
    }
    if (useMobile) await applyMobileEmulation(page);
    else await applyDesktopEmulation(page);

    await login(page, {
      username,
      password,
      twoFactorCode: twoFactorCode || undefined,
    });
    const cookies = await waitForWebSessionOrThrow(page);
    await saveAdminLabSession({
      cookies,
      username,
      proxyUrl: proxyParsed ? proxyParsed.raw : null,
      updatedAt: new Date().toISOString(),
    });
    return { ok: true, cookies, username };
  } catch (e) {
    if (e.code === 'TWO_FACTOR_REQUIRED' && e.page) {
      keepBrowserOpen = true;
      return {
        twoFactorRequired: true,
        page: e.page,
        browser,
        username,
        proxyUrl: proxyParsed ? proxyParsed.raw : null,
      };
    }
    throw e;
  } finally {
    if (!keepBrowserOpen) await browser.close().catch(() => {});
  }
}

/**
 * Complete 2FA for admin lab (browser still open). Saves lab session to disk.
 */
async function adminLabComplete2FA(page, browser, twoFactorCode, instagramUsername, proxyUrl) {
  const result = await completeInstagram2FA(page, browser, twoFactorCode, instagramUsername);
  const raw = proxyUrl || process.env.ADMIN_LAB_DECODO_PROXY_URL || '';
  const parsed = parseProxyUrl(raw);
  await saveAdminLabSession({
    cookies: result.cookies,
    username: result.username,
    proxyUrl: parsed ? parsed.raw : null,
    updatedAt: new Date().toISOString(),
  });
  return result;
}

/**
 * Send DMs to usernames (one message) using saved lab session.
 */
async function adminLabSend({ usernames, message }) {
  const session = await loadLabSession();
  if (!session || !session.cookies || !Array.isArray(session.cookies) || session.cookies.length === 0) {
    throw new Error('No saved lab session. Connect first.');
  }
  const proxyRaw = session.proxyUrl || process.env.ADMIN_LAB_DECODO_PROXY_URL;
  const proxyParsed = parseProxyUrl(proxyRaw || '');
  const launchOpts = buildLaunchOptions(proxyParsed);
  const browser = await puppeteer.launch(launchOpts);
  const page = await browser.newPage();
  if (proxyParsed && proxyParsed.username && proxyParsed.password) {
    await page.authenticate({ username: proxyParsed.username, password: proxyParsed.password });
  }
  await applyDesktopEmulation(page);
  await page.setCookie(...session.cookies);
  await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 45000 }).catch(() => {});
  await new Promise((r) => setTimeout(r, 2000));
  if (page.url().includes('/accounts/login')) {
    await browser.close().catch(() => {});
    throw new Error('Session expired. Connect again.');
  }

  const adapter = createAdminLabAdapter(message);
  const list = Array.isArray(usernames)
    ? usernames.map((u) => String(u).trim().replace(/^@/, '')).filter(Boolean)
    : [];
  const results = [];
  for (const u of list) {
    try {
      const res = await sendDM(page, u, adapter, { messageOverride: message });
      results.push({ username: u, ok: res.ok, reason: res.reason || null });
    } catch (err) {
      results.push({ username: u, ok: false, reason: (err && err.message) || String(err) });
    }
    await new Promise((r) => setTimeout(r, 1500 + Math.floor(Math.random() * 2000)));
  }
  await browser.close().catch(() => {});
  return { results, count: results.length };
}

module.exports = {
  adminLabConnect,
  adminLabComplete2FA,
  adminLabSend,
  loadLabSession,
  parseProxyUrl,
  SESSION_FILE,
};
