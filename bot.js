require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
const os = require('os');
const csv = require('csv-parser');
const { getRandomMessage } = require('./config/messages');
const { alreadySent, logSentMessage, getDailyStats, normalizeUsername, getControl, setControl } = require('./database/db');
const sb = require('./database/supabase');
const logger = require('./utils/logger');
const {
  applyMobileEmulation,
  applyDesktopEmulation,
  buildDesktopViewport,
  getDesktopWindowPadding,
} = require('./utils/mobile-viewport');
const { substituteVariables, normalizeName, normalizeFullDisplayName } = require('./utils/message-variables');
const { isFfmpegAvailable, convertToChromeFakeMicWav, ensureChromeFakeMicPlaceholder } = require('./utils/voice-note-audio');
const {
  appendChromeFakeMicArgs,
  DEFAULT_CHROME_FAKE_MIC_WAV,
  buildChromeFakeMicPath,
} = require('./utils/chrome-fake-mic');
const {
  sendVoiceNoteInThread,
  prepareVoiceNoteUi,
  grantMicrophoneForInstagram,
  VOICE_NOTE_STRICT_VERIFY,
} = require('./utils/instagram-voice-note');
const {
  dismissInstagramCookieConsent,
  dismissInstagramHomeModals,
  dismissInstagramPopups,
  detectInstagramPasswordReauthScreen,
} = require('./utils/instagram-modals');
const {
  navigateToDmThread,
  sendPlainTextInThread,
  typeInstagramDmPlainTextInComposer,
  typeInstagramDmPlainTextWithKeyboard,
} = require('./utils/open-dm-thread');
const { clickInstagramDmSearchResult, formatSearchFailurePageSnippet } = require('./utils/instagram-dm-search');
const { attachInstagramSendIdCapture } = require('./utils/instagram-dm-network-ids');
const { applyProxyToLaunchOptions, authenticatePageForProxy } = require('./utils/proxy-puppeteer');
const {
  resolveHeadlessMode,
  baseChromeArgs,
  assignPersistentUserDataDir,
  acquireChromeUserDataDirLock,
  isChromeProfileSingletonLockError,
  isChromeSingletonForeignHostError,
  tryRecoverStaleChromeProfileLocks,
  quarantineChromePersistentSendProfileDir,
} = require('./utils/puppeteer-chrome-launch');
const { gotoInstagramDirectNew } = require('./utils/goto-instagram-direct-new');
const {
  navigateAndCaptureInstagramWebStorage,
  applyInstagramWebStorageFromSessionData,
} = require('./utils/instagram-web-storage');
puppeteer.use(StealthPlugin());

const DAILY_LIMIT = Math.min(parseInt(process.env.DAILY_SEND_LIMIT, 10) || 100, 200);
const MIN_DELAY_MS = (parseInt(process.env.MIN_DELAY_MINUTES, 10) || 5) * 60 * 1000;
const MAX_DELAY_MS = (parseInt(process.env.MAX_DELAY_MINUTES, 10) || 30) * 60 * 1000;
const MAX_PER_HOUR = parseInt(process.env.MAX_SENDS_PER_HOUR, 10) || 20;
/** false | true | 'new' (Chromium new headless; slightly less brittle than legacy true for some sites) */
const HEADLESS = resolveHeadlessMode(process.env.HEADLESS_MODE, true);
/** Per-Instagram-session persistent Chrome profile under .browser-profiles/send-<sessionId> (disable with 0/false). */
const PUPPETEER_PERSIST_SEND_PROFILES =
  process.env.PUPPETEER_PERSIST_SEND_PROFILES == null ||
  String(process.env.PUPPETEER_PERSIST_SEND_PROFILES).trim() === '' ||
  (String(process.env.PUPPETEER_PERSIST_SEND_PROFILES).toLowerCase() !== '0' &&
    String(process.env.PUPPETEER_PERSIST_SEND_PROFILES).toLowerCase() !== 'false');
const SEND_LEASE_SECONDS = Math.max(120, parseInt(process.env.SEND_LEASE_SECONDS || '600', 10) || 600);
const SEND_WORKER_ID = process.env.SEND_WORKER_ID || `send-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
const SEND_WORKER_VERBOSE_LOGS =
  String(process.env.SEND_WORKER_VERBOSE_LOGS || '').trim().toLowerCase() === '1' ||
  String(process.env.SEND_WORKER_VERBOSE_LOGS || '').trim().toLowerCase() === 'true';
const SEND_STAGE_TIMEOUT_MS = Math.max(
  15000,
  parseInt(process.env.SEND_STAGE_TIMEOUT_MS || '30000', 10) || 30000
);
/** When daily cap is hit, push the whole campaign queue forward at least this long (default 1h). Shorter values caused a claim → restore → limit → repeat log storm. */
const SEND_DAILY_LIMIT_DEFER_MS = Math.max(
  5 * 60 * 1000,
  parseInt(process.env.SEND_DAILY_LIMIT_DEFER_MS || String(60 * 60 * 1000), 10) || 60 * 60 * 1000
);
/** Min interval between identical send-limit logs per campaign (default 10m). */
const SEND_LIMIT_LOG_THROTTLE_MS = Math.max(
  30_000,
  parseInt(process.env.SEND_LIMIT_LOG_THROTTLE_MS || String(10 * 60 * 1000), 10) || 10 * 60 * 1000
);
const sendLimitLogLast = new Map();

function throttleSendLimitLog(key, emit) {
  const now = Date.now();
  const prev = sendLimitLogLast.get(key) || 0;
  if (now - prev < SEND_LIMIT_LOG_THROTTLE_MS) return;
  sendLimitLogLast.set(key, now);
  emit();
}

const COLD_DM_CONCURRENCY_DEBUG =
  String(process.env.COLD_DM_CONCURRENCY_DEBUG || '').trim().toLowerCase() === '1' ||
  String(process.env.COLD_DM_CONCURRENCY_DEBUG || '').trim().toLowerCase() === 'true' ||
  String(process.env.COLD_DM_CONCURRENCY_DEBUG || '').trim().toLowerCase() === 'yes';

function logColdDmConcurrencyDebug(message, details = null) {
  if (!COLD_DM_CONCURRENCY_DEBUG) return;
  const prefix = '[cold-dm-concurrency-debug] ';
  if (details == null) {
    logger.log(prefix + message);
    return;
  }
  try {
    logger.log(prefix + message + ' ' + JSON.stringify(details));
  } catch {
    logger.log(prefix + message);
  }
}

/** Avoid wiping non-IG cookies / storage when refreshing Instagram session cookies. */
async function clearInstagramCookiesOnlyOnPage(pg) {
  try {
    const existing = await pg.cookies();
    const ig = existing.filter((c) => {
      const d = (c.domain || '').toLowerCase();
      return d.includes('instagram');
    });
    if (ig.length) await pg.deleteCookie(...ig);
  } catch (e) {
    logger.warn(`clearInstagramCookiesOnlyOnPage: ${e.message || e}`);
  }
}

async function withTimeout(promise, timeoutMs, timeoutMessage) {
  const ms = Math.max(1000, Number(timeoutMs) || 1000);
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(timeoutMessage || `Timed out after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}
/** When set (e.g. 80), slows Puppeteer operations for debugging voice/UI (all launch paths that use applyPuppeteerSlowMo). */
function getPuppeteerSlowMo() {
  const n = parseInt(process.env.PUPPETEER_SLOW_MO_MS, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

/**
 * After voice Send in follow-ups, keep the browser open this long so Instagram can finish upload
 * before we close the session (default 10s). Set VOICE_POST_SEND_BROWSER_WAIT_MS=0 to skip.
 */
const VOICE_POST_SEND_BROWSER_WAIT_MS = Math.min(
  120000,
  Math.max(0, parseInt(process.env.VOICE_POST_SEND_BROWSER_WAIT_MS, 10) || 10000)
);
/**
 * Headed Chromium often opens ~800×600; `page.setViewport()` alone does not resize the X11 window,
 * so Instagram stays visually clipped until the real window matches (especially on VNC + Xvfb).
 *
 * `--window-size` must be **larger** than the layout viewport: browser chrome (tab strip, toolbar,
 * bookmarks) is outside the page. If they match, the composer / right column look "cut off" even
 * when Xvfb is big enough.
 */
function getChromeWindowPositionArg() {
  const raw = (process.env.CHROME_WINDOW_POSITION || '0,0').trim();
  return /^\d+,\d+$/.test(raw) ? raw : '0,0';
}

function applyHeadedChromeWindowToLaunchOpts(launchOpts) {
  /** Only when truly headed (HEADLESS_MODE=false). `new` headless still skips window chrome args. */
  if (HEADLESS !== false || !launchOpts || !Array.isArray(launchOpts.args)) return;
  const vp = buildDesktopViewport();
  const { padX, padY } = getDesktopWindowPadding();
  const outerW = vp.width + padX;
  const outerH = vp.height + padY;
  if (!launchOpts.args.some((a) => typeof a === 'string' && a.startsWith('--window-size='))) {
    launchOpts.args.push(`--window-size=${outerW},${outerH}`);
  }
  if (!launchOpts.args.some((a) => typeof a === 'string' && a.startsWith('--window-position='))) {
    launchOpts.args.push(`--window-position=${getChromeWindowPositionArg()}`);
  }
  launchOpts.defaultViewport = {
    width: vp.width,
    height: vp.height,
    deviceScaleFactor: vp.deviceScaleFactor,
    isMobile: vp.isMobile,
    hasTouch: vp.hasTouch,
  };
}

function applyPuppeteerSlowMo(launchOpts) {
  const sm = getPuppeteerSlowMo();
  if (sm > 0) launchOpts.slowMo = sm;
  return launchOpts;
}

async function applyConnectFingerprint(page) {
  // Force English on connect/login flows for stable selectors and diagnostics.
  const acceptLanguage = 'en-US,en;q=0.9';
  const timezoneId = (process.env.CONNECT_TIMEZONE_ID || 'Europe/Berlin').trim();
  try {
    await page.setExtraHTTPHeaders({ 'Accept-Language': acceptLanguage });
  } catch {}
  try {
    if (timezoneId) await page.emulateTimezone(timezoneId);
  } catch {}
}

const VOICE_NOTE_SOURCE_NAME = (process.env.VOICE_NOTE_SOURCE_NAME || 'ColdDMsVoice').trim();
const BROWSER_PROFILE_DIR = path.join(process.cwd(), '.browser-profile');
const VOICE_NOTE_FILE = (process.env.VOICE_NOTE_FILE || '').trim();
const VOICE_NOTE_MODE = (process.env.VOICE_NOTE_MODE || 'after_text').trim().toLowerCase();
const LOGIN_DEBUG_SCREENSHOT_DIR = path.join(process.cwd(), 'logs', 'login-debug');

function wantsVoiceNotes(sendOpts = {}) {
  return !!((sendOpts.voiceNotePath || '').trim());
}

async function saveLoginDebugScreenshot(page, label) {
  const enabled =
    process.env.LOGIN_DEBUG_SCREENSHOTS === '1' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true';
  if (!enabled || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const safe = String(label || 'step')
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '_')
      .slice(0, 40);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_${safe}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[LOGIN_DEBUG] screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[LOGIN_DEBUG] screenshot failed: ' + (e.message || e));
    return null;
  }
}

function wantsTermsUnblockDebugScreenshot() {
  return (
    process.env.TERMS_UNBLOCK_DEBUG_SCREENSHOTS === '1' ||
    process.env.TERMS_UNBLOCK_DEBUG_SCREENSHOTS === 'true' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === '1' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true'
  );
}

/** Full-page PNG when Instagram shows /terms/unblock (see button labels). Same folder as login-debug. */
async function saveTermsUnblockDebugScreenshot(page, label) {
  if (!wantsTermsUnblockDebugScreenshot() || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const safe = String(label || 'terms')
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '_')
      .slice(0, 48);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_${safe}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[terms/unblock] debug screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[terms/unblock] debug screenshot failed: ' + (e.message || e));
    return null;
  }
}

async function logTermsUnblockVisibleButtons(page) {
  if (!wantsTermsUnblockDebugScreenshot() || !page) return;
  try {
    const rows = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('button, [role="button"], a, div[tabindex="0"]').forEach((el) => {
        try {
          const r = el.getBoundingClientRect();
          if (r.width < 2 || r.height < 2) return;
          const st = window.getComputedStyle(el);
          if (st.visibility === 'hidden' || st.display === 'none') return;
          const t = (el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 120);
          const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
          const aTrim = String(a).trim().slice(0, 120);
          if (!t && !aTrim) return;
          out.push({ tag: el.tagName, text: t, ariaLabel: aTrim });
        } catch {
          // ignore
        }
      });
      return out.slice(0, 40);
    });
    logger.log(`[terms/unblock] visible buttons/links (text + aria-label): ${JSON.stringify(rows)}`);
  } catch (e) {
    logger.warn('[terms/unblock] button dump failed: ' + (e.message || e));
  }
}

function wantsDmSearchDebugScreenshot() {
  return (
    process.env.DM_SEARCH_DEBUG_SCREENSHOTS === '1' ||
    process.env.DM_SEARCH_DEBUG_SCREENSHOTS === 'true' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === '1' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true'
  );
}

/** Full-page PNG before DM search-result click + on failure (pair). Enable with DM_SEARCH_PAIR_SCREENSHOTS=1 or any DM_SEARCH_DEBUG_SCREENSHOTS / LOGIN_DEBUG. */
function wantsDmSearchPairScreenshots() {
  return (
    wantsDmSearchDebugScreenshot() ||
    process.env.DM_SEARCH_PAIR_SCREENSHOTS === '1' ||
    process.env.DM_SEARCH_PAIR_SCREENSHOTS === 'true'
  );
}

/** Full-page PNG when DM /direct/new search result click fails (same folder as login-debug). */
async function saveDmSearchDebugScreenshot(page, label) {
  if (!(wantsDmSearchDebugScreenshot() || wantsDmSearchPairScreenshots()) || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const safe = String(label || 'dm_search')
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '_')
      .slice(0, 48);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_${safe}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[dm-search] debug screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[dm-search] debug screenshot failed: ' + (e.message || e));
    return null;
  }
}

/**
 * Always logs URL + page text snippet + light DOM counts when picking a search result fails.
 * PNG only if DM_SEARCH_DEBUG_SCREENSHOTS or LOGIN_DEBUG* is set.
 */
/**
 * One screenshot immediately before typing into the DM composer (focused, message not yet entered).
 * Use to inspect thread header / layout (e.g. stray "Back" in accessibility text). Independent of LOGIN_DEBUG*.
 */
function wantsComposeTypingScreenshot() {
  return process.env.DM_COMPOSE_TYPING_SCREENSHOT === '1' || process.env.DM_COMPOSE_TYPING_SCREENSHOT === 'true';
}

async function saveComposeTypingDebugScreenshot(page, username) {
  if (!wantsComposeTypingScreenshot() || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const u = String(username || 'lead').replace(/^@/, '').replace(/[^a-z0-9_-]/gi, '_').slice(0, 40);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_compose_before_type_${u}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[compose] typing-view screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[compose] typing-view screenshot failed: ' + (e.message || e));
    return null;
  }
}

function wantsComposePostSendScreenshot() {
  return (
    process.env.DM_COMPOSE_POST_SEND_SCREENSHOT === '1' ||
    process.env.DM_COMPOSE_POST_SEND_SCREENSHOT === 'true'
  );
}

/**
 * One screenshot right after Enter send to capture thread/render state for debugging
 * "message sent but UI looked wrong" cases.
 */
async function saveComposePostSendDebugScreenshot(page, username) {
  if (!wantsComposePostSendScreenshot() || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const u = String(username || 'lead').replace(/^@/, '').replace(/[^a-z0-9_-]/gi, '_').slice(0, 40);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_compose_after_send_${u}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[compose] post-send screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[compose] post-send screenshot failed: ' + (e.message || e));
    return null;
  }
}

function wantsComposeFailureScreenshot() {
  return (
    process.env.DM_COMPOSE_FAILURE_SCREENSHOT === '1' ||
    process.env.DM_COMPOSE_FAILURE_SCREENSHOT === 'true' ||
    // If typing screenshot is enabled, also capture no_compose failures by default.
    wantsComposeTypingScreenshot()
  );
}

/** Screenshot when compose is missing (before returning no_compose). */
async function saveComposeFailureDebugScreenshot(page, username, label = 'compose_missing') {
  if (!wantsComposeFailureScreenshot() || !page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const u = String(username || 'lead').replace(/^@/, '').replace(/[^a-z0-9_-]/gi, '_').slice(0, 40);
    const safeLabel = String(label || 'compose_missing').replace(/[^a-z0-9_-]/gi, '_').slice(0, 24);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_${safeLabel}_${u}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[compose] failure screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[compose] failure screenshot failed: ' + (e.message || e));
    return null;
  }
}

/**
 * Full-page screenshot right after a compose-recovery CTA (Continue, Accept, etc.).
 * Not gated on DM_COMPOSE_FAILURE_SCREENSHOT — always written when recovery runs so
 * account-picker / interstitial flows are visible on the VPS (logs/login-debug).
 */
async function saveAfterComposeRecoveryScreenshot(page, recoveryClicked, leadUsername) {
  if (!page) return null;
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    const u = String(leadUsername || 'lead').replace(/^@/, '').replace(/[^a-z0-9_-]/gi, '_').slice(0, 40);
    const safe = String(recoveryClicked || 'recovery').replace(/[^a-z0-9_-]/gi, '_').slice(0, 80);
    const out = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_after_compose_recovery_${safe}_${u}.png`);
    await page.screenshot({ path: out, fullPage: true });
    logger.log(`[compose-recovery] after-CTA screenshot=${out}`);
    return out;
  } catch (e) {
    logger.warn('[compose-recovery] after-CTA screenshot failed: ' + (e.message || e));
    return null;
  }
}

async function logDmSearchFailureDiagnostics(page, username, searchPick) {
  const u = String(username || '').trim().replace(/^@/, '');
  let url = '';
  let meta = {};
  let snippet = '';
  try {
    url = page.url();
    const data = await page.evaluate(() => {
      const vis = (el) => {
        try {
          if (!el || el.disabled) return false;
          if (el.type === 'hidden') return false;
          return (el.getClientRects && el.getClientRects().length > 0) || el.offsetParent !== null;
        } catch {
          return false;
        }
      };
      const body = (document.body && document.body.innerText ? document.body.innerText : '').replace(/\s+/g, ' ').trim();
      return {
        path: location.pathname,
        title: (document.title || '').slice(0, 200),
        listboxCount: document.querySelectorAll('[role="listbox"]').length,
        dialogCount: document.querySelectorAll('[role="dialog"], [role="alertdialog"]').length,
        visibleInputs: Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]')).filter(vis).length,
        bodyLen: body.length,
        bodySnippet: body.slice(0, 1200),
      };
    });
    meta = {
      path: data.path,
      title: data.title,
      listboxCount: data.listboxCount,
      dialogCount: data.dialogCount,
      visibleInputs: data.visibleInputs,
      bodyLen: data.bodyLen,
    };
    snippet = data.bodySnippet;
  } catch (e) {
    snippet = '(read failed: ' + (e.message || e) + ')';
  }
  const pickLine = searchPick && searchPick.logLine ? searchPick.logLine : JSON.stringify(searchPick || {});
  logger.warn(`[dm-search] failed @${u} pick={${pickLine}}`);
  logger.warn(`[dm-search] url=${url} meta=${JSON.stringify(meta)}`);
  logger.warn(`[dm-search] snippet=${snippet}`);
  await saveDmSearchDebugScreenshot(page, `dm_search_fail_${u.replace(/[^a-z0-9_-]/gi, '_').slice(0, 40)}`);
}

/** Non-login Instagram URLs that mean we do not have a usable session (challenge, checkpoint, etc.). */
function instagramAuthUrlFailureReason(url) {
  try {
    const path = new URL(url).pathname.toLowerCase();
    if (path.includes('/accounts/login')) return null;
    if (path.includes('/challenge')) {
      return 'Instagram opened a security challenge (/challenge). Complete it in a normal browser, then retry Connect.';
    }
    if (path.includes('/checkpoint')) {
      return 'Instagram requires a checkpoint. Open instagram.com in a normal browser, finish the security step, then retry.';
    }
    if (path.includes('/accounts/suspended') || path.includes('/accounts/disabled')) {
      return 'Instagram shows this account as suspended or disabled.';
    }
    if (path.includes('/accounts/recovery')) {
      return 'Instagram is asking for account recovery.';
    }
  } catch (_) {}
  return null;
}

/** Visible copy that often appears when Meta blocks or challenges the login (feed can load URL=/ while this shows). */
async function detectInstagramDomBlockAfterLogin(page) {
  return page.evaluate(() => {
    const chunks = [];
    if (document.body) chunks.push(document.body.innerText || '');
    document.querySelectorAll('[role="dialog"], [role="alertdialog"]').forEach((el) => {
      chunks.push(el.textContent || '');
    });
    const t = chunks.join('\n').toLowerCase();
    const patterns = [
      'we detected an unusual login',
      'unusual login attempt',
      'suspicious login',
      'blocked your attempt',
      'couldn\'t log you in',
      'there was a problem logging you',
      'confirm it\'s you',
      'help us confirm you own',
      'help us confirm',
      'verify this was you',
      'temporarily locked',
      'temporarily blocked',
      'your account has been disabled',
      'we suspended your account',
    ];
    for (const p of patterns) {
      if (t.includes(p)) return p;
    }
    return null;
  });
}

async function pageHasInstagramSessionCookie(page) {
  const cookies = await page.cookies();
  return cookies.some((c) => c.name === 'sessionid' && c.value && String(c.value).length >= 8);
}

/**
 * After navigation away from the login form, ensure we are not on a challenge URL, no block modal text,
 * and Instagram set a web session cookie — otherwise the dashboard would show "connected" incorrectly.
 */
async function assertHealthyInstagramSessionOrThrow(page, contextLabel) {
  const url = page.url();
  const urlReason = instagramAuthUrlFailureReason(url);
  if (urlReason) throw new Error(urlReason);
  const domHit = await detectInstagramDomBlockAfterLogin(page);
  if (domHit) {
    throw new Error(
      `Instagram blocked or challenged this login (${domHit}). Log in once in Chrome/Firefox (same network if possible), complete any prompt, or use 2FA — then retry Connect.`
    );
  }
  const hasSession = await pageHasInstagramSessionCookie(page);
  if (!hasSession) {
    if (envLoginDebugEnabled()) {
      const ck = await page.cookies();
      logger.error(
        `[login] Missing sessionid after ${contextLabel || 'login'}; cookie names=${ck.length ? ck.map((c) => c.name).join(', ') : '(none)'} url=${url}`
      );
      const snippet = await page
        .evaluate(() => ((document.body && document.body.innerText) || '').slice(0, 500))
        .catch(() => '');
      logger.error('[login] Body snippet: ' + String(snippet).replace(/\n/g, ' '));
    }
    throw new Error(
      `No Instagram session cookie after ${contextLabel || 'login'}. Instagram never issued a web session (Meta blocked or left the flow incomplete — not a false positive from our checker). ` +
        `Why VPS often works: same datacenter IP + browser fingerprint may already be trusted. Residential is a new IP/device every time until sticky + warmup. ` +
        `Try: (1) Proxy geo is random by default; set DECODO_GATE_COUNTRY=us/gb/etc. (or none for random). Reconnect so proxy_url updates. ` +
        `(2) Log in once in Chrome/Firefox using the exact proxy_url from Supabase, finish any checkpoint, then Connect on the VPS. ` +
        `(3) Use 2FA on Connect if the account has it.`
    );
  }
}

/** Detect Instagram "check your email" checkpoint and extract masked email if shown. */
async function detectInstagramEmailVerificationState(page) {
  return page.evaluate(() => {
    const body = ((document.body && document.body.innerText) || '').replace(/\s+/g, ' ');
    const lower = body.toLowerCase();
    const needs =
      lower.includes('check your email') ||
      lower.includes('enter the code we sent to') ||
      lower.includes('we sent the code to') ||
      (lower.includes('code') && lower.includes('try another way'));
    if (!needs) return { required: false, maskedEmail: null };
    const m = body.match(/(?:sent to|to)\s+([A-Za-z0-9._%*+\-]+@[A-Za-z0-9.\-*]+\.[A-Za-z]{2,})/i);
    return { required: true, maskedEmail: m ? m[1] : null };
  });
}

/** Detect code-entry challenge pages that should use the existing pending-code UI. */
async function detectInstagramInteractiveChallengeState(page) {
  return page.evaluate(() => {
    const body = ((document.body && document.body.innerText) || '').replace(/\s+/g, ' ');
    const lower = body.toLowerCase();
    const path = location.pathname.toLowerCase();
    const visibleInputs = Array.from(document.querySelectorAll('input')).filter((el) => {
      try {
        if (!el || el.disabled || el.type === 'hidden') return false;
        const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
        if (style && (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0')) return false;
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      } catch {
        return false;
      }
    });
    const hasCodeInput = visibleInputs.length > 0;
    const emailMatch = body.match(/(?:sent to|to)\s+([A-Za-z0-9._%*+\-]+@[A-Za-z0-9.\-*]+\.[A-Za-z]{2,})/i);
    const emailRequired =
      hasCodeInput &&
      (
        lower.includes('check your email') ||
        lower.includes('email verification') ||
        lower.includes('enter the code we sent to') ||
        lower.includes('we sent the code to') ||
        lower.includes('sent to your email')
      );
    if (emailRequired) {
      return { required: true, kind: 'email', maskedEmail: emailMatch ? emailMatch[1] : null };
    }
    const twoFactorRequired =
      hasCodeInput &&
      (
        path.includes('/accounts/login/two_factor') ||
        lower.includes('two-factor') ||
        lower.includes('2fa') ||
        lower.includes('security code') ||
        lower.includes('6-digit code') ||
        lower.includes('authentication app') ||
        lower.includes('authenticator app') ||
        lower.includes('whatsapp') ||
        lower.includes('try another way')
      );
    if (twoFactorRequired) {
      return { required: true, kind: 'two_factor', maskedEmail: null };
    }
    return { required: false, kind: null, maskedEmail: null };
  });
}

// dismissInstagramCookieConsent — imported from utils/instagram-modals.js

function isInstagramTermsUnblockUrl(url) {
  return typeof url === 'string' && url.toLowerCase().includes('/terms/unblock');
}

/**
 * Instagram /terms/unblock: long terms + scroll, then Accept; often a second modal
 * "Review and Agree" / "You're all set!" with OK. Prioritize OK on that modal, then Accept.
 */
async function handleInstagramTermsUnblock(page) {
  if (!isInstagramTermsUnblockUrl(page.url())) return false;

  logger.warn('Instagram terms/unblock interstitial detected. Scrolling and clicking Accept / OK...');
  await saveTermsUnblockDebugScreenshot(page, 'terms_unblock_initial');
  await logTermsUnblockVisibleButtons(page);

  const maxPasses = 40;

  for (let pass = 0; pass < maxPasses; pass++) {
    if (!isInstagramTermsUnblockUrl(page.url())) {
      logger.log(`terms/unblock cleared; url=${page.url()}`);
      return true;
    }

    if (pass === 10) {
      await saveTermsUnblockDebugScreenshot(page, 'terms_unblock_after_scroll');
      await logTermsUnblockVisibleButtons(page);
    }

    // Keyboard scroll backup (some layouts only respond to this).
    if (pass % 3 === 0) {
      await page.keyboard.press('End').catch(() => {});
      await delay(200);
    }

    const step = await page.evaluate(() => {
      const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
      const lower = (s) => norm(s).toLowerCase();

      function visible(el) {
        if (!el) return false;
        try {
          const st = window.getComputedStyle(el);
          if (st.visibility === 'hidden' || st.display === 'none' || parseFloat(st.opacity || '1') === 0) return false;
          const r = el.getBoundingClientRect();
          return r.width >= 2 && r.height >= 2 && r.bottom > 0 && r.top < window.innerHeight + 200;
        } catch {
          return false;
        }
      }

      function clickEl(el) {
        if (!el) return false;
        const btn = el.closest('button, [role="button"], a') || el;
        try {
          btn.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
        } catch {
          btn.click();
        }
        return true;
      }

      // Scroll window and inner scroll regions (terms are often in a nested div).
      const roots = [document.scrollingElement, document.documentElement, document.body].filter(Boolean);
      for (const r of roots) {
        try {
          r.scrollTop = (r.scrollHeight || 0) - (r.clientHeight || 0);
        } catch {
          // ignore
        }
      }
      document.querySelectorAll('div, main, section, article').forEach((el) => {
        try {
          if (el.scrollHeight > (el.clientHeight || 0) + 80) {
            el.scrollTop = el.scrollHeight;
          }
        } catch {
          // ignore
        }
      });

      const bodyText = ((document.body && document.body.innerText) || '').toLowerCase();
      const nodes = Array.from(document.querySelectorAll('button, [role="button"], a, div[tabindex="0"]'));
      const labelOf = (el) => {
        const t = lower(el.textContent);
        const a = lower(el.getAttribute && el.getAttribute('aria-label'));
        return { t, a, combined: `${t} ${a}`.trim() };
      };

      // 1) Success overlay: "Review and Agree" / "You're all set!" → blue OK
      const successHint =
        bodyText.includes("you're all set") ||
        bodyText.includes('you’re all set') ||
        bodyText.includes('review and agree') ||
        bodyText.includes('thank you for reviewing');
      if (successHint) {
        for (const el of nodes) {
          const { t, a } = labelOf(el);
          if (!visible(el)) continue;
          const okWord =
            t === 'ok' ||
            t === 'done' ||
            t === 'got it' ||
            t === 'close' ||
            (t.length > 0 && t.length <= 28 && /^(ok|done)$/i.test(t)) ||
            /^(ok|done|close)\b/i.test((a || '').trim()) ||
            /\b(ok|done)\b/i.test(a);
          if (okWord) {
            return { action: 'ok_modal', label: t || a || 'ok', ok: clickEl(el) };
          }
        }
        // Blue bar button: sometimes only inner span; pick largest visible primary-looking button in dialog
        const dialogs = Array.from(document.querySelectorAll('[role="dialog"], [role="alertdialog"]'));
        for (const d of dialogs) {
          const btns = Array.from(d.querySelectorAll('button, [role="button"]')).filter(visible);
          const primary = btns.find((b) => {
            const { t } = labelOf(b);
            return t === 'ok' || t === 'done' || /^ok$/i.test(t);
          });
          if (primary) return { action: 'ok_modal_dialog', label: norm(primary.textContent), ok: clickEl(primary) };
          if (btns.length === 1) return { action: 'ok_modal_single', label: norm(btns[0].textContent), ok: clickEl(btns[0]) };
        }
      }

      // 2) Primary terms CTAs (exact-ish; avoid matching random "next" in nav)
      const primaryRes = [
        /^accept$/i,
        /^i agree$/i,
        /^agree$/i,
        /^agree and continue$/i,
        /^continue$/i,
        /^review now$/i,
      ];
      for (const el of nodes) {
        const t = norm(el.textContent);
        const aria = norm(el.getAttribute && el.getAttribute('aria-label'));
        const pick = t || aria;
        if (!pick || pick.length > 96) continue;
        if (!visible(el)) continue;
        if (primaryRes.some((re) => re.test(pick))) {
          return { action: 'primary_cta', label: pick, ok: clickEl(el) };
        }
      }

      // 3) Looser: line is mostly an accept phrase
      for (const el of nodes) {
        const t = lower(el.textContent);
        if (!t || t.length > 72) continue;
        if (!visible(el)) continue;
        if (
          /\baccept\b/.test(t) ||
          /\bagree\b/.test(t) ||
          (t.includes('continue') && (t.includes('agree') || t.includes('terms')))
        ) {
          return { action: 'loose_cta', label: t.slice(0, 48), ok: clickEl(el) };
        }
      }

      return { action: 'scroll_only', label: '', ok: false };
    });

    if (step.ok) {
      logger.log(`terms/unblock pass ${pass + 1}: ${step.action} "${step.label || ''}"`);
      await delay(1600);
      continue;
    }

    await page.evaluate(() => {
      window.scrollBy(0, Math.min(900, window.innerHeight));
    });
    await delay(350);
  }

  return !isInstagramTermsUnblockUrl(page.url());
}

function buildVoiceSendConfig(sendOpts = {}) {
  const modeRaw = String(sendOpts.voiceNoteMode || VOICE_NOTE_MODE || 'after_text').toLowerCase();
  const mode = modeRaw === 'voice_only' ? 'voice_only' : 'after_text';
  return {
    voiceNotePath: (sendOpts.voiceNotePath || '').trim(),
    mode,
  };
}

/** Download HTTPS URL to temp file (Supabase Storage signed URLs, follow-up audioUrl, etc.) or pass through local path. */
async function resolveVoiceNotePath(rawPath) {
  const p = (rawPath || '').trim();
  if (!p) return { localPath: '', cleanup: async () => {} };
  if (!/^https?:\/\//i.test(p)) return { localPath: p, cleanup: async () => {} };
  const res = await fetch(p, { signal: AbortSignal.timeout(15000) });
  if (!res.ok) throw new Error('voice_note_download_failed');
  const ab = await res.arrayBuffer();
  const ext = path.extname(new URL(p).pathname || '').toLowerCase() || '.wav';
  const outPath = path.join(os.tmpdir(), `voice-note-${Date.now()}-${Math.random().toString(16).slice(2)}${ext}`);
  await fs.promises.writeFile(outPath, Buffer.from(ab));
  return {
    localPath: outPath,
    cleanup: async () => {
      await fs.promises.unlink(outPath).catch(() => {});
    },
  };
}

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function randomDelay(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
}

async function humanDelay() {
  await delay(500 + Math.floor(Math.random() * 1500));
}

async function tinyHumanMouseMove(page) {
  if (!page || !page.mouse || typeof page.viewport !== 'function') return;
  const vp = page.viewport() || {};
  const width = Math.max(800, vp.width || 1200);
  const height = Math.max(600, vp.height || 900);
  const start = {
    x: Math.max(20, Math.min(width - 20, Math.round(width * (0.18 + Math.random() * 0.18)))),
    y: Math.max(20, Math.min(height - 20, Math.round(height * (0.22 + Math.random() * 0.16)))),
  };
  const end = {
    x: Math.max(20, Math.min(width - 20, Math.round(width * (0.42 + Math.random() * 0.2)))),
    y: Math.max(20, Math.min(height - 20, Math.round(height * (0.34 + Math.random() * 0.18)))),
  };
  try {
    await page.mouse.move(start.x, start.y, { steps: 5 });
    await delay(60 + Math.floor(Math.random() * 100));
    await page.mouse.move(end.x, end.y, { steps: 7 });
    await delay(80 + Math.floor(Math.random() * 140));
  } catch {
    // best-effort only
  }
}

/** Extract Instagram thread id from direct URL (e.g. /direct/t/17843804841623833/). */
function getInstagramThreadIdFromUrl(url) {
  if (!url || typeof url !== 'string') return null;
  const m = url.match(/\/direct\/t\/(\d+)/);
  return m ? m[1] : null;
}

/** Call cold-dm-on-send Edge Function so dashboard creates conversation with tag cold-outreach. */
async function coldDmOnSend(payload) {
  const baseUrl = process.env.SUPABASE_URL;
  const apiKey = process.env.COLD_DM_API_KEY;
  if (!baseUrl || !apiKey) {
    logger.warn('cold-dm-on-send skipped: SUPABASE_URL or COLD_DM_API_KEY not set');
    return;
  }
  const url = `${baseUrl.replace(/\/$/, '')}/functions/v1/cold-dm-on-send`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      signal: AbortSignal.timeout(10000),
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      logger.log('cold-dm-on-send: ' + res.status + ' (conversation created or updated)');
    } else {
      const errText = await res.text();
      if (res.status === 404) {
        logger.warn('cold-dm-on-send 404: Edge Function not deployed. Deploy "cold-dm-on-send" in your Supabase project so the dashboard can create cold-outreach conversations and match GHL contacts.');
      } else {
        logger.warn('cold-dm-on-send failed: ' + res.status + ' ' + errText);
      }
    }
  } catch (e) {
    logger.warn('cold-dm-on-send request error: ' + e.message);
  }
}

function getHourlySent() {
  const { db } = require('./database/db');
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const row = db.prepare("SELECT COUNT(*) as c FROM sent_messages WHERE sent_at >= ? AND status = 'success'").get(oneHourAgo);
  return row ? row.c : 0;
}

function readEnvFromFile() {
  const envPath = path.join(process.cwd(), '.env');
  const out = {};
  if (!fs.existsSync(envPath)) return out;
  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split('\n')) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) out[m[1].trim()] = m[2].trim();
  }
  return out;
}

/** Only explicit truthy strings enable login debug logs (avoids accidental on from PM2/env drift). */
function envLoginDebugEnabled() {
  const v = String(process.env.LOGIN_DEBUG ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

/**
 * Instagram login inputs are React-controlled. Puppeteer typing alone can leave React state stale,
 * which surfaces as "The login information you entered is incorrect" even with valid credentials.
 */
async function setReactLoginInputValue(elementHandle, value) {
  const str = value == null ? '' : String(value);
  await elementHandle.evaluate((el, v) => {
    if (!el || el.tagName !== 'INPUT') return;
    const proto = window.HTMLInputElement.prototype;
    const desc = Object.getOwnPropertyDescriptor(proto, 'value');
    if (desc && desc.set) desc.set.call(el, v);
    else el.value = v;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }, str);
}

async function login(page, credentials) {
  const username = credentials?.username ?? readEnvFromFile().INSTAGRAM_USERNAME ?? process.env.INSTAGRAM_USERNAME;
  const password = credentials?.password ?? readEnvFromFile().INSTAGRAM_PASSWORD ?? process.env.INSTAGRAM_PASSWORD;
  if (!username || !password) {
    throw new Error('Missing INSTAGRAM_USERNAME or INSTAGRAM_PASSWORD. Add them in the dashboard Settings and save.');
  }

  logger.log('Loading Instagram login page...');
  await page.goto('https://www.instagram.com/accounts/login/?hl=en', { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
  const afterGotoUrl = page.url();
  const afterGotoTitle = await page.title().catch(() => '');
  logger.log(`After load: URL=${afterGotoUrl} title=${afterGotoTitle}`);
  await delay(4000);
  const currentUrl = page.url();
  if (!currentUrl.includes('/accounts/login')) {
    logger.log('Already logged in (session restored).');
    return;
  }

  const cookieDismissed = await dismissInstagramCookieConsent(page);
  if (cookieDismissed) {
    logger.log('Dismissed Instagram cookie consent modal.');
    await saveLoginDebugScreenshot(page, 'after_cookie_dismiss');
    await delay(600);
  }

  const findLoginFields = async () => {
    return page.evaluate(() => {
      const visible = (el) => {
        try {
          if (!el || el.disabled) return false;
          const rects = el.getClientRects?.().length || 0;
          const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
          const hiddenByStyle = style && (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0');
          return rects > 0 && !hiddenByStyle;
        } catch {
          return false;
        }
      };
      const norm = (s) => (s || '').toString().toLowerCase().replace(/\s+/g, ' ').trim();
      const inputs = Array.from(document.querySelectorAll('input'));
      const samples = inputs.slice(0, 10).map((el) => ({
        type: norm(el.type || ''),
        name: norm(el.name || ''),
        autocomplete: norm(el.autocomplete || ''),
        placeholder: norm(el.placeholder || ''),
        aria: norm(el.getAttribute('aria-label') || ''),
        visible: visible(el),
      }));
      const pickBy = (...preds) => inputs.find((el) => visible(el) && preds.some((fn) => fn(el)));
      const userEl =
        pickBy(
          (el) => norm(el.name || '') === 'username',
          (el) => norm(el.autocomplete || '') === 'username',
          (el) => norm(el.placeholder || '').includes('username'),
          (el) => norm(el.placeholder || '').includes('email'),
          (el) => norm(el.type || '') === 'text',
          (el) => norm(el.type || '') === 'email',
          (el) => norm(el.type || '') === ''
        ) || null;
      const passEl =
        pickBy(
          (el) => norm(el.name || '') === 'password',
          (el) => norm(el.autocomplete || '') === 'current-password',
          (el) => norm(el.placeholder || '').includes('password'),
          (el) => norm(el.type || '') === 'password'
        ) || null;
      return {
        ok: !!userEl && !!passEl,
        userIndex: userEl ? inputs.indexOf(userEl) : -1,
        passIndex: passEl ? inputs.indexOf(passEl) : -1,
        sample: samples,
        url: location.href,
        title: document.title || '',
        bodySnippet: (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 400),
      };
    });
  };

  let loginDiag = await findLoginFields().catch(() => null);
  const loginReloadAttempts = Math.max(
    0,
    Math.min(2, parseInt(process.env.LOGIN_FORM_RELOAD_ATTEMPTS || '1', 10) || 1)
  );
  for (let attempt = 0; attempt < loginReloadAttempts && (!loginDiag || !loginDiag.ok); attempt++) {
    logger.warn(
      `Login fields not ready yet on attempt ${attempt + 1}/${loginReloadAttempts + 1} for @${username}. Retrying page load before failing.`
    );
    await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
    await delay(4000 + attempt * 1000);
    loginDiag = await findLoginFields().catch(() => null);
  }
  if (!loginDiag || !loginDiag.ok) {
    const failUrl = page.url();
    const failTitle = await page.title().catch(() => '');
    logger.error('Login form fields not found');
    logger.log(`Page at failure: URL=${failUrl} title=${failTitle}`);
    logger.log(`Login field diag: ${JSON.stringify(loginDiag || {}, null, 0).slice(0, 2000)}`);
    throw new Error('Login form fields not found. Instagram may be loading a different shell or security interstitial.');
  }

  const inputs = await page.$$('input');
  const getFieldMeta = async (el) =>
    el.evaluate((node) => ({
      type: node.type,
      visible: (() => {
        const rects = node.getClientRects?.().length || 0;
        const style = window.getComputedStyle ? window.getComputedStyle(node) : null;
        const hiddenByStyle = style && (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0');
        return !node.disabled && rects > 0 && !hiddenByStyle;
      })(),
      disabled: !!node.disabled,
      name: node.name || '',
      autocomplete: node.autocomplete || '',
      placeholder: node.placeholder || '',
      aria: node.getAttribute('aria-label') || '',
      value: node.value || '',
    }));
  const isUsableField = (meta) => meta && !meta.disabled;
  const findField = async (chooser) => {
    for (const el of inputs) {
      const meta = await getFieldMeta(el);
      if (!isUsableField(meta)) continue;
      if (chooser(meta)) return { el, meta };
    }
    return { el: null, meta: null };
  };
  const pickByIndex = async (idx) => {
    if (typeof idx !== 'number' || idx < 0 || idx >= inputs.length) return { el: null, meta: null };
    const el = inputs[idx];
    const meta = await getFieldMeta(el).catch(() => null);
    return isUsableField(meta) ? { el, meta } : { el: null, meta: null };
  };
  const userChoice =
    (await pickByIndex(loginDiag?.userIndex)) ||
    (await findField((p) => p.name === 'username' || p.autocomplete === 'username')) ||
    (await findField((p) => p.placeholder.toLowerCase().includes('username') || p.placeholder.toLowerCase().includes('email') || p.aria.toLowerCase().includes('username') || p.aria.toLowerCase().includes('email'))) ||
    (await findField((p) => p.type === 'text' || p.type === 'email' || p.type === ''));
  const passChoice =
    (await pickByIndex(loginDiag?.passIndex)) ||
    (await findField((p) => p.name === 'password' || p.autocomplete === 'current-password')) ||
    (await findField((p) => p.placeholder.toLowerCase().includes('password') || p.aria.toLowerCase().includes('password') || p.type === 'password'));
  const userEl = userChoice.el;
  const passEl = passChoice.el;
  if (!userEl || !passEl) {
    inputs.forEach((el) => el.dispose());
    const failUrl = page.url();
    const failTitle = await page.title().catch(() => '');
    const bodyText = await page.evaluate(() => document.body ? document.body.innerText.slice(0, 500) : '').catch(() => '');
    logger.error('Login form fields not found after retry');
    logger.log(`Page at failure: URL=${failUrl} title=${failTitle}`);
    logger.log(`Login field diag reused: ${JSON.stringify(loginDiag || {}, null, 0).slice(0, 2000)}`);
    logger.log(`Page body snippet: ${bodyText.replace(/\n/g, ' ').slice(0, 300)}`);
    throw new Error('Login form fields not found. Instagram may have changed the page.');
  }
  for (const el of inputs) {
    if (el !== userEl && el !== passEl) el.dispose();
  }
  const LOGIN_DEBUG = envLoginDebugEnabled();
  const loginResponses = [];
  const allInstagramRequests = [];
  const respHandler = async (response) => {
    if (!LOGIN_DEBUG) return;
    const url = response.url();
    const status = response.status();
    const req = response.request();
    const method = req.method();
    if (url.includes('instagram.com')) {
      if (allInstagramRequests.length < 20) allInstagramRequests.push({ method, url: url.slice(0, 120), status });
    }
    if (url.includes('login') || url.includes('ajax/bz') || (url.includes('accounts') && (url.includes('web') || url.includes('api')))) {
      try {
        let body = '';
        try { body = (await response.text()).slice(0, 500); } catch (e) {}
        loginResponses.push({ url: url.slice(0, 100), status, body: body.slice(0, 300) });
      } catch (e) {}
    }
  };
  page.on('response', respHandler);

  logger.log('Login form found, entering credentials...');
  await userEl.click({ clickCount: 1 }).catch(() => {});
  await setReactLoginInputValue(userEl, username);
  await userEl.dispose();
  await humanDelay();
  await passEl.click({ clickCount: 1 }).catch(() => {});
  await setReactLoginInputValue(passEl, password);
  await passEl.dispose();
  await humanDelay();

  const submitStyle = (process.env.LOGIN_SUBMIT || 'click').toLowerCase();
  let submitMethod = 'click';

  if (submitStyle === 'enter' || submitStyle === 'enterthenclick') {
    await page.keyboard.press('Enter');
    await delay(800);
    submitMethod = submitStyle === 'enter' ? 'enterKey' : 'enterKeyThenClick';
  }
  if (submitStyle !== 'enter') {
    const clicked = await page.evaluate(function () {
      var xpaths = [
        "//button[normalize-space(.)='Log in']",
        "//div[@role='button'][normalize-space(.)='Log in']",
        "//span[normalize-space(.)='Log in']/parent::button",
        "//span[normalize-space(.)='Log in']/parent::div[@role='button']",
        "//button[contains(., 'Log in') and not(contains(., 'Log into'))]",
        "//div[@role='button'][contains(., 'Log in') and not(contains(., 'Log into'))]",
        "//button[normalize-space(.)='Anmelden']",
        "//div[@role='button'][normalize-space(.)='Anmelden']"
      ];
      for (var i = 0; i < xpaths.length; i++) {
        var r = document.evaluate(xpaths[i], document, null, 9, null);
        var el = r.singleNodeValue;
        if (el && el.offsetParent) { el.scrollIntoView({ block: 'center' }); el.click(); return true; }
      }
      return false;
    });
    if (clicked) submitMethod = submitStyle === 'enterthenclick' ? 'enterKeyThenClick' : 'click';
    else {
      // Fallback: submit via Enter if button text/layout prevents click matching.
      await page.keyboard.press('Enter').catch(() => {});
      submitMethod = submitStyle === 'enterthenclick' ? 'enterKeyThenClickFallback' : 'enterKeyFallback';
    }
  }

  if (LOGIN_DEBUG) logger.log('Login submitMethod=' + submitMethod);
  await saveLoginDebugScreenshot(page, 'before_submit');

  logger.log('Submitted login form, waiting for redirect...');
  await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});

  const loginWaitMs = Math.min(parseInt(process.env.LOGIN_WAIT_MS, 10) || 20000, 90000);
  const pollIntervalMs = 1000;
  const deadline = Date.now() + loginWaitMs;
  const startedAt = Date.now();
  while (Date.now() < deadline) {
    await delay(pollIntervalMs);
    const url = page.url();
    if (!url.includes('/accounts/login')) {
      logger.log('Redirect detected after ' + Math.round((Date.now() - startedAt) / 1000) + 's');
      break;
    }
  }
  await delay(1500);

  const interactiveChallenge = await detectInstagramInteractiveChallengeState(page);
  if (interactiveChallenge.required && interactiveChallenge.kind === 'two_factor') {
    page.off('response', respHandler);
    const err = new Error('Two-factor authentication required. Enter the 6-digit code from your authenticator app or WhatsApp.');
    err.code = 'TWO_FACTOR_REQUIRED';
    err.page = page;
    throw err;
  }
  if (interactiveChallenge.required && interactiveChallenge.kind === 'email') {
    await saveLoginDebugScreenshot(page, 'email_checkpoint');
    page.off('response', respHandler);
    const err = new Error(
      `Email verification required.${interactiveChallenge.maskedEmail ? ` Enter the code sent to ${interactiveChallenge.maskedEmail}.` : ''}`
    );
    err.code = 'EMAIL_VERIFICATION_REQUIRED';
    err.page = page;
    err.maskedEmail = interactiveChallenge.maskedEmail || null;
    throw err;
  }

  // Handle two-factor authentication page
  if (page.url().includes('/accounts/login/two_factor')) {
    const twoFactorCode = credentials?.twoFactorCode ? String(credentials.twoFactorCode).replace(/\D/g, '').slice(0, 6) : '';
    if (!twoFactorCode) {
      page.off('response', respHandler);
      const err = new Error('Two-factor authentication required. Enter the 6-digit code from your authenticator app or WhatsApp.');
      err.code = 'TWO_FACTOR_REQUIRED';
      err.page = page;
      throw err;
    }
    await ensure2FACodeEntryPage(page);
    logger.log('On 2FA page, entering security code...');
    const codeInputFocused = await page.evaluate(() => {
      const inputs = Array.from(document.querySelectorAll('input'));
      const visible = inputs.filter((el) => el.offsetParent != null && el.type !== 'hidden');
      const codeInput = visible.find((el) => {
        const p = (el.placeholder || '').toLowerCase();
        const a = (el.getAttribute('aria-label') || '').toLowerCase();
        return p.includes('code') || p.includes('security') || a.includes('code') || a.includes('security') || (el.type !== 'password' && el.type !== 'email');
      }) || visible[0];
      if (!codeInput) return false;
      codeInput.focus();
      codeInput.click();
      return true;
    });
    if (!codeInputFocused) {
      page.off('response', respHandler);
      const err = new Error('Two-factor code input not found on page.');
      err.code = 'TWO_FACTOR_REQUIRED';
      throw err;
    }
    await delay(300);
    await page.keyboard.type(twoFactorCode, { delay: 80 + Math.floor(Math.random() * 40) });
    await humanDelay();
    const confirmClicked = await page.evaluate(function () {
      const labels = ['Confirm', 'Next', 'Submit'];
      const buttons = Array.from(document.querySelectorAll('button, div[role="button"], input[type="submit"]'));
      for (const label of labels) {
        const btn = buttons.find((el) => (el.textContent || el.value || '').trim() === label);
        if (btn && btn.offsetParent) { btn.scrollIntoView({ block: 'center' }); btn.click(); return true; }
      }
      const confirmLike = buttons.find((el) => /confirm|next|submit/i.test((el.textContent || el.value || '').trim()));
      if (confirmLike && confirmLike.offsetParent) { confirmLike.click(); return true; }
      return false;
    });
    if (confirmClicked) {
      await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
      await delay(2000);
    }
    if (page.url().includes('/accounts/login/two_factor')) {
      page.off('response', respHandler);
      const err = new Error('Two-factor code may be wrong or expired. Try again with a fresh code.');
      err.code = 'TWO_FACTOR_REQUIRED';
      throw err;
    }
    logger.log('2FA code accepted.');
  }

  const emailCheckpoint = await detectInstagramEmailVerificationState(page);
  const currentLoginUrl = page.url().toLowerCase();
  if (emailCheckpoint.required || currentLoginUrl.includes('/auth_platform/codeentry')) {
    await saveLoginDebugScreenshot(page, 'email_checkpoint');
    page.off('response', respHandler);
    const err = new Error(
      `Email verification required.${emailCheckpoint.maskedEmail ? ` Enter the code sent to ${emailCheckpoint.maskedEmail}.` : ''}`
    );
    err.code = 'EMAIL_VERIFICATION_REQUIRED';
    err.page = page;
    err.maskedEmail = emailCheckpoint.maskedEmail || null;
    throw err;
  }

  const shouldRetryLoginSubmit = !['0', 'false'].includes(String(process.env.LOGIN_RETRY_SUBMIT || '1').toLowerCase());
  if (shouldRetryLoginSubmit && page.url().includes('/accounts/login')) {
    logger.log('Still on login page; retrying submit (click only)...');
    const retryClick = await page.evaluate(function () {
      var xpaths = [
        "//button[normalize-space(.)='Log in']",
        "//div[@role='button'][normalize-space(.)='Log in']",
        "//span[normalize-space(.)='Log in']/parent::button",
        "//span[normalize-space(.)='Log in']/parent::div[@role='button']"
      ];
      for (var i = 0; i < xpaths.length; i++) {
        var r = document.evaluate(xpaths[i], document, null, 9, null);
        var el = r.singleNodeValue;
        if (el && el.offsetParent) { el.scrollIntoView({ block: 'center' }); el.click(); return true; }
      }
      return false;
    });
    if (retryClick) {
      await delay(1000);
      const retryDeadline = Date.now() + 30000;
      while (Date.now() < retryDeadline) {
        await delay(pollIntervalMs);
        if (!page.url().includes('/accounts/login')) break;
      }
      await delay(1500);
    }
  }

  const interactiveChallengeAfterRetry = await detectInstagramInteractiveChallengeState(page);
  if (interactiveChallengeAfterRetry.required && interactiveChallengeAfterRetry.kind === 'two_factor') {
    page.off('response', respHandler);
    const err = new Error('Two-factor authentication required. Enter the 6-digit code from your authenticator app or WhatsApp.');
    err.code = 'TWO_FACTOR_REQUIRED';
    err.page = page;
    throw err;
  }
  if (interactiveChallengeAfterRetry.required && interactiveChallengeAfterRetry.kind === 'email') {
    await saveLoginDebugScreenshot(page, 'email_checkpoint_after_retry');
    page.off('response', respHandler);
    const err = new Error(
      `Email verification required.${interactiveChallengeAfterRetry.maskedEmail ? ` Enter the code sent to ${interactiveChallengeAfterRetry.maskedEmail}.` : ''}`
    );
    err.code = 'EMAIL_VERIFICATION_REQUIRED';
    err.page = page;
    err.maskedEmail = interactiveChallengeAfterRetry.maskedEmail || null;
    throw err;
  }

  const emailCheckpointAfterRetry = await detectInstagramEmailVerificationState(page);
  const retryLoginUrl = page.url().toLowerCase();
  if (emailCheckpointAfterRetry.required || retryLoginUrl.includes('/auth_platform/codeentry')) {
    await saveLoginDebugScreenshot(page, 'email_checkpoint_after_retry');
    page.off('response', respHandler);
    const err = new Error(
      `Email verification required.${emailCheckpointAfterRetry.maskedEmail ? ` Enter the code sent to ${emailCheckpointAfterRetry.maskedEmail}.` : ''}`
    );
    err.code = 'EMAIL_VERIFICATION_REQUIRED';
    err.page = page;
    err.maskedEmail = emailCheckpointAfterRetry.maskedEmail || null;
    throw err;
  }

  for (let i = 0; i < 3; i++) {
    const dismissed = await page.evaluate(function () {
      const dialogs = document.querySelectorAll('[role="dialog"], [role="alertdialog"]');
      for (let d = 0; d < dialogs.length; d++) {
        const txt = (dialogs[d].textContent || '').toLowerCase();
        if (txt.indexOf('save your login') !== -1 || txt.indexOf('not now') !== -1 || txt.indexOf('turn on notifications') !== -1) {
          const notNow = Array.from(dialogs[d].querySelectorAll('span, button, div[role="button"]')).find(function (el) {
            return (el.textContent || '').trim().toLowerCase() === 'not now';
          });
          if (notNow) {
            const btn = notNow.closest('[role="button"]') || notNow.closest('button') || notNow;
            if (btn) { btn.click(); return true; }
          }
        }
      }
      return false;
    });
    if (dismissed) {
      logger.log('Dismissed post-login popup');
      await delay(2000);
    } else {
      break;
    }
  }

  await delay(2000);
  page.off('response', respHandler);
  const urlAfterLogin = page.url();
  if (urlAfterLogin.includes('/accounts/login')) {
    await saveLoginDebugScreenshot(page, 'still_on_login_failure');
    const bodySnippet = await page.evaluate(() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 600)).catch(() => '');
    let hint = '';
    const lower = bodySnippet.toLowerCase();
    if (
      lower.indexOf('login information you entered is incorrect') !== -1 ||
      lower.indexOf('password was incorrect') !== -1 ||
      lower.indexOf('incorrect password') !== -1
    ) {
      hint = ' Instagram says the username or password is wrong. Double-check both, or reset the password in the Instagram app.';
    } else if (lower.indexOf('username you entered') !== -1 || lower.indexOf("doesn't belong to an account") !== -1) hint = ' Username not found.';
    else if (lower.indexOf('challenge') !== -1 || lower.indexOf('suspicious') !== -1 || lower.indexOf('verify') !== -1 || lower.indexOf('confirm it\'s you') !== -1 || lower.indexOf('security code') !== -1) hint = ' Instagram may require manual verification. Log in once in a normal browser (Chrome/Firefox), complete any challenge, then try again here.';
    else if (lower.indexOf('try again later') !== -1 || lower.indexOf('too many requests') !== -1) hint = ' Rate limited. Try again in 30–60 minutes.';
    else hint = ' If your password is correct, log in once in a normal browser to clear any security check, then retry.';
    logger.error('Login failed. submitMethod=' + submitMethod + ' url=' + urlAfterLogin);
    if (LOGIN_DEBUG) {
      logger.error('Login API responses (count=' + loginResponses.length + '): ' + (loginResponses.length ? JSON.stringify(loginResponses.slice(-5)) : 'none captured'));
      if (allInstagramRequests.length) logger.error('Recent Instagram requests: ' + JSON.stringify(allInstagramRequests.slice(-10)));
    }
    logger.error('Login failed. Page snippet: ' + bodySnippet.replace(/\n/g, ' ').slice(0, 400));
    throw new Error('Login may have failed; still on login page. Check credentials.' + hint);
  }
  await assertHealthyInstagramSessionOrThrow(page, 'login');
  logger.log('Logged in to Instagram.');
}

const MAX_SEND_RETRIES = 3;

async function runComposeDiagnostic(page, usernameForPane = null) {
  const needleArg = (usernameForPane || '').replace(/^@/, '').trim();
  return page.evaluate((needleRaw) => {
    const needleLc = (needleRaw || '').toLowerCase();
    const textareas = document.querySelectorAll('textarea');
    const editables = document.querySelectorAll('div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"]');
    const roleBoxes = document.querySelectorAll('[role="textbox"]');
    const visible = (el) => el.offsetParent !== null;
    const vis = (el) => {
      try {
        return el && el.offsetParent !== null;
      } catch {
        return false;
      }
    };
    const composers = Array.from(
      document.querySelectorAll(
        'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
      )
    ).filter(vis);
    let compose = composers.find((el) => {
      const ph = (el.getAttribute('placeholder') || '').toLowerCase();
      const aria = (el.getAttribute('aria-label') || '').toLowerCase();
      return ph.includes('message') || aria.includes('message');
    });
    if (!compose && composers.length) {
      const vwGuess = document.documentElement.clientWidth || 1200;
      compose =
        [...composers].reverse().find((el) => {
          try {
            const r = el.getBoundingClientRect();
            return r.width > 48 && r.height > 16 && r.left > vwGuess * 0.3;
          } catch {
            return false;
          }
        }) || null;
    }
    let paneScopedSnippet = '';
    if (compose) {
      const vw = document.documentElement.clientWidth || 1200;
      const minLeft = Math.max(0, compose.getBoundingClientRect().left - 48);
      const maxPaneWidth = vw - minLeft + 320;
      let best = compose;
      let el = compose;
      for (let depth = 0; depth < 28 && el; depth++) {
        el = el.parentElement;
        if (!el || el === document.body || el === document.documentElement) break;
        const r = el.getBoundingClientRect();
        if (r.left < minLeft) break;
        if (r.width <= maxPaneWidth) best = el;
        else break;
      }
      let elPane = best;
      for (let step = 0; step < 24; step++) {
        const raw = (elPane.innerText || '').trim();
        const okNeedle = needleLc && raw.length >= 40 && raw.toLowerCase().includes(needleLc);
        const okBulk = !needleLc && raw.length >= 140;
        if (okNeedle || okBulk) {
          best = elPane;
          break;
        }
        const p = elPane.parentElement;
        if (!p || p === document.body || p === document.documentElement) break;
        const r = p.getBoundingClientRect();
        if (r.left < minLeft - 120) break;
        if (r.left <= 20 && r.width >= vw * 0.93) break;
        elPane = p;
        best = p;
      }
      paneScopedSnippet = (best.innerText || '').slice(0, 400).replace(/\n/g, ' ');
    }
    return {
      url: window.location.href,
      textarea: textareas.length,
      textareaVisible: Array.from(textareas).filter(visible).length,
      contenteditable: editables.length,
      contenteditableVisible: Array.from(editables).filter(visible).length,
      roleTextbox: roleBoxes.length,
      roleTextboxVisible: Array.from(roleBoxes).filter(visible).length,
      bodySnippet: document.body ? document.body.innerText.slice(0, 400).replace(/\n/g, ' ') : '',
      paneScopedSnippet,
    };
  }, needleArg);
}

/** Instagram redirects /direct/new → /accounts/login?next=…/direct… (__coig_login) for cookie resume. */
function isInstagramCoigLoginOrDirectGateUrl(pageUrl) {
  if (!pageUrl || typeof pageUrl !== 'string') return false;
  const u = pageUrl.toLowerCase();
  if (!u.includes('/accounts/login')) return false;
  try {
    const parsed = new URL(pageUrl);
    const next = (parsed.searchParams.get('next') || '').toLowerCase();
    if (next.includes('/direct/')) return true;
  } catch {}
  if (u.includes('__coig_login') || u.includes('%2fdirect%2f')) return true;
  return false;
}

/**
 * Screenshot → click Continue → wait off /accounts/login → screenshot.
 * Always writes PNGs under logs/login-debug (not gated on LOGIN_DEBUG_SCREENSHOTS).
 */
async function passInstagramDirectNewCoigLoginGate(page, tag = 'direct_new_gate') {
  if (!page) return { ok: true, skipped: true, reason: 'no_page' };
  const url0 = page.url();
  if (!isInstagramCoigLoginOrDirectGateUrl(url0)) {
    return { ok: true, skipped: true, url: url0 };
  }
  let beforePath = null;
  let afterPath = null;
  const safeTag = String(tag || 'gate').replace(/[^a-z0-9_-]/gi, '_').slice(0, 48);
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    beforePath = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_before_continue_${safeTag}.png`);
    await page.screenshot({ path: beforePath, fullPage: true });
    logger.log(`[ig-direct-gate] before-Continue screenshot=${beforePath} url=${url0}`);
  } catch (e) {
    logger.warn('[ig-direct-gate] before-Continue screenshot failed: ' + (e.message || e));
  }

  /**
   * React/IG often ignores synthetic DOM .click() from page.evaluate. Pick the largest visible
   * primary "Continue" (exact label) and hit it with real pointer events via CDP.
   */
  const pickCenter = await page
    .evaluate(() => {
      const visible = (el) => {
        try {
          if (!el || el.offsetParent === null) return false;
          const r = el.getBoundingClientRect();
          return r.width >= 4 && r.height >= 4;
        } catch {
          return false;
        }
      };
      const label = (el) => (el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
      const scored = [];
      document.querySelectorAll('button, [role="button"]').forEach((el) => {
        if (!visible(el)) return;
        const t = label(el);
        if (t !== 'continue') return;
        const r = el.getBoundingClientRect();
        const area = r.width * r.height;
        if (area < 1200) return;
        scored.push({ el, area });
      });
      if (!scored.length) {
        document.querySelectorAll('button, [role="button"]').forEach((el) => {
          if (!visible(el)) return;
          const t = label(el);
          if (!t.includes('continue') || t.includes('agree')) return;
          const r = el.getBoundingClientRect();
          const area = r.width * r.height;
          if (area < 800) return;
          scored.push({ el, area });
        });
      }
      if (!scored.length) return null;
      scored.sort((a, b) => b.area - a.area);
      const best = scored[0].el;
      try {
        best.scrollIntoView({ block: 'center', inline: 'nearest' });
      } catch {}
      const r = best.getBoundingClientRect();
      return { cx: r.left + r.width / 2, cy: r.top + r.height / 2, area: r.width * r.height };
    })
    .catch(() => null);

  let clicked = false;
  if (pickCenter && Number.isFinite(pickCenter.cx) && Number.isFinite(pickCenter.cy)) {
    try {
      await page.mouse.move(pickCenter.cx, pickCenter.cy, { steps: 8 }).catch(() => {});
      await delay(120);
      await page.mouse.click(pickCenter.cx, pickCenter.cy, { delay: 120, clickCount: 1 });
      clicked = true;
      logger.log(
        `[ig-direct-gate] pointer-click Continue at (${Math.round(pickCenter.cx)},${Math.round(pickCenter.cy)}) area≈${Math.round(pickCenter.area)}`
      );
      await delay(900);
    } catch (e) {
      logger.warn('[ig-direct-gate] pointer-click Continue failed: ' + (e.message || e));
    }
  }

  async function screenshotPasswordReauthGate() {
    let pwShot = null;
    try {
      fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
      pwShot = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_password_reauth_modal_${safeTag}.png`);
      await page.screenshot({ path: pwShot, fullPage: true });
      logger.warn(`[ig-direct-gate] password re-login modal (after Continue) screenshot=${pwShot}`);
    } catch (_) {}
    return pwShot;
  }

  if (clicked) {
    await delay(1600);
    if (await detectInstagramPasswordReauthScreen(page)) {
      const pwShot = await screenshotPasswordReauthGate();
      return {
        ok: false,
        skipped: false,
        url: page.url(),
        beforePath,
        afterPath: pwShot,
        error: 'instagram_password_reauth_required',
      };
    }
  }

  if (!clicked) {
    logger.warn('[ig-direct-gate] Continue button not found or click failed');
    try {
      fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
      const p = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_continue_not_found_${safeTag}.png`);
      await page.screenshot({ path: p, fullPage: true });
      logger.log(`[ig-direct-gate] continue-not-found screenshot=${p}`);
    } catch (_) {}
    return { ok: false, skipped: false, url: page.url(), beforePath, afterPath: null, error: 'continue_not_found' };
  }

  logger.log('[ig-direct-gate] clicked Continue; waiting to leave /accounts/login...');
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    await delay(400);
    if (await detectInstagramPasswordReauthScreen(page)) {
      const pwShot = await screenshotPasswordReauthGate();
      return {
        ok: false,
        skipped: false,
        url: page.url(),
        beforePath,
        afterPath: pwShot,
        error: 'instagram_password_reauth_required',
      };
    }
    const low = (page.url() || '').toLowerCase();
    if (!low.includes('/accounts/login')) {
      await delay(1200);
      try {
        fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
        afterPath = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_after_continue_${safeTag}.png`);
        await page.screenshot({ path: afterPath, fullPage: true });
        logger.log(`[ig-direct-gate] after-Continue screenshot=${afterPath} url=${page.url()}`);
      } catch (e) {
        logger.warn('[ig-direct-gate] after-Continue screenshot failed: ' + (e.message || e));
      }
      return { ok: true, skipped: false, url: page.url(), beforePath, afterPath };
    }
  }

  const finalUrl = page.url();
  try {
    fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
    afterPath = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_after_continue_timeout_${safeTag}.png`);
    await page.screenshot({ path: afterPath, fullPage: true });
    logger.warn(`[ig-direct-gate] still on /accounts/login after Continue (timeout); screenshot=${afterPath} url=${finalUrl}`);
  } catch (_) {}
  return { ok: false, skipped: false, url: finalUrl, beforePath, afterPath, error: 'still_on_login_after_continue' };
}

async function gotoInstagramDirectNewMaybePassCoigLoginGate(page, tag, opts = {}) {
  await gotoInstagramDirectNew(page);
  if (opts.settleMs != null) await delay(Number(opts.settleMs));
  else await humanDelay();
  const gate = await passInstagramDirectNewCoigLoginGate(page, tag);
  if (!gate.ok) {
    logger.error(
      '[ig-direct-gate] failed ' +
        JSON.stringify({ error: gate.error, url: gate.url, beforePath: gate.beforePath, afterPath: gate.afterPath })
    );
    if (gate.error === 'instagram_password_reauth_required') {
      const e = new Error('instagram_password_reauth_required');
      e.code = 'INSTAGRAM_PASSWORD_REAUTH';
      e.gateDetails = gate;
      throw e;
    }
    const err =
      gate.error === 'continue_not_found'
        ? `Instagram login gate before DMs: Continue not found (${gate.url}). before_screenshot=${gate.beforePath || 'n/a'}`
        : `Instagram login gate before DMs: still on login after Continue (${gate.url}). after_screenshot=${gate.afterPath || 'n/a'}`;
    throw new Error(err);
  }
  return gate;
}

async function sendDMOnce(page, u, messageTemplate, nameFallback = {}, sendOpts = {}) {
  const voiceCfg = buildVoiceSendConfig(sendOpts);
  if (wantsVoiceNotes(voiceCfg) && !isFfmpegAvailable()) {
    return { ok: false, reason: 'ffmpeg_missing', pageSnippet: 'Install ffmpeg on the VPS: sudo apt install ffmpeg' };
  }
  const coigTag = `send_${normalizeUsername(u)}`;
  // Desktop layout for all sends: mobile thread header merges back-arrow + name in innerText ("BackTai"); desktop DMs behave better for automation.
  await applyDesktopEmulation(page);
  await gotoInstagramDirectNewMaybePassCoigLoginGate(page, coigTag);
  for (let termsRound = 0; termsRound < 3; termsRound++) {
    if (!isInstagramTermsUnblockUrl(page.url())) break;
    const handled = await handleInstagramTermsUnblock(page).catch(() => false);
    if (handled && !isInstagramTermsUnblockUrl(page.url())) {
      if (!page.url().toLowerCase().includes('/direct/')) {
        await gotoInstagramDirectNewMaybePassCoigLoginGate(page, coigTag, { settleMs: 1200 });
      }
      await delay(1200);
      break;
    }
    if (isInstagramTermsUnblockUrl(page.url())) {
      await gotoInstagramDirectNewMaybePassCoigLoginGate(page, coigTag, { settleMs: 2000 });
    }
  }

  await dismissInstagramHomeModals(page, logger);
  await delay(500);

  // Wait for the direct/new search UI to render (may be an input, textarea, or contenteditable element).
  await page
    .waitForFunction(
      () => {
        const selectors = [
          'input',
          'textarea',
          '[contenteditable="true"]',
          '[role="combobox"]',
          '[role="textbox"]',
        ];
        const els = Array.from(document.querySelectorAll(selectors.join(',')));
        return els.some((el) => {
          try {
            if (!el || el.disabled) return false;
            const t = ((el.getAttribute && el.getAttribute('type')) || '').toLowerCase();
            if (t === 'hidden') return false;
            const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
            const aria = ((el.getAttribute && el.getAttribute('aria-label')) || '').toLowerCase();
            const ph = ((el.getAttribute && el.getAttribute('placeholder')) || '').toLowerCase();
            const role = ((el.getAttribute && el.getAttribute('role')) || '').toLowerCase();
            const looksLikeSearch =
              ph.includes('search') ||
              ph.includes('to:') ||
              aria.includes('search') ||
              aria.includes('to:') ||
              role === 'combobox' ||
              role === 'textbox';
            return looksLikeSearch || (style && style.display !== 'none');
          } catch {
            return false;
          }
        });
      },
      { timeout: 12000 }
    )
    .catch(() => {});

  const searchHandle = await page.evaluateHandle(() => {
    const normalize = (s) => (s || '').toString().toLowerCase();
    const candidates = Array.from(
      document.querySelectorAll('input, textarea, [contenteditable="true"], [role="combobox"], [role="textbox"]')
    ).filter((el) => {
      try {
        if (!el || el.disabled) return false;
        if (el.type === 'hidden') return false;
        const ph = normalize(el.getAttribute && el.getAttribute('placeholder'));
        const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
        const role = normalize(el.getAttribute && el.getAttribute('role'));
        const looksLikeSearch =
          ph.includes('search') ||
          ph.includes('to:') ||
          aria.includes('search') ||
          aria.includes('to:') ||
          role === 'combobox' ||
          role === 'textbox';
        if (looksLikeSearch) return true;
        if (el.getClientRects && el.getClientRects().length > 0) return true;
        if (el.offsetParent !== null) return true;
        return true;
      } catch {
        return false;
      }
    });

    const findWithHints = (predicates) => {
      for (const pred of predicates) {
        const hit = candidates.find((el) => pred(el));
        if (hit) return hit;
      }
      return null;
    };

    const searchOrTo = (el) => {
      const ph = normalize(el.placeholder);
      const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
      return ph.includes('search') || ph.includes('to:') || aria.includes('search') || aria.includes('to:');
    };

    const comboboxRole = (el) => {
      const role = normalize(el.getAttribute && el.getAttribute('role'));
      return role === 'combobox' || role === 'textbox';
    };

    const textInput = (el) => {
      if (!('tagName' in el)) return false;
      if (el.tagName === 'INPUT') return !el.type || el.type === 'text';
      if (el.tagName === 'TEXTAREA') return true;
      return !!el.isContentEditable;
    };

    const hit =
      findWithHints([searchOrTo, comboboxRole]) ||
      findWithHints([textInput]) ||
      candidates[0] ||
      null;

    return hit;
  });

  const searchEl = searchHandle.asElement();
  if (!searchEl) {
    const diag = await page
      .evaluate(() => {
        const normalize = (s) => (s || '').toString().toLowerCase();
        const els = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'));
        const visible = els.filter((el) => {
          try {
            if (!el || el.disabled) return false;
            if (el.type === 'hidden') return false;
            return (el.getClientRects && el.getClientRects().length > 0) || el.offsetParent !== null;
          } catch {
            return false;
          }
        });
        const sample = visible.slice(0, 5).map((el) => ({
          tag: el.tagName,
          type: el.type || '',
          placeholder: el.placeholder || '',
          aria: normalize(el.getAttribute && el.getAttribute('aria-label')),
          contentEditable: !!el.isContentEditable,
        }));
        return { url: location.href, visibleCount: visible.length, sample };
      })
      .catch(() => null);
    await searchHandle.dispose().catch(() => {});
    const diagUrl = (diag?.url || '').toLowerCase();
    if (diagUrl.includes('/terms/unblock') || diagUrl.includes('/challenge/') || diagUrl.includes('/checkpoint/')) {
      if (diagUrl.includes('/terms/unblock')) {
        const handled = await handleInstagramTermsUnblock(page).catch(() => false);
        if (handled) {
          return {
            ok: false,
            reason: 'retry_needed_after_terms_unblock',
            pageSnippet: 'Accepted Instagram terms/unblock. Retrying DM open flow.',
          };
        }
      }
      return {
        ok: false,
        reason: 'account_unblock_required',
        pageSnippet: `Instagram redirected to security/unblock page before DM search (url=${diag?.url || 'unknown'}). Open this account in a normal browser and complete the unblock/checkpoint, then retry.`,
      };
    }
    await delay(1200);
    const retryHandle = await page.evaluateHandle(() => {
      const selectors = [
        'input[placeholder*="search" i]',
        'input[aria-label*="search" i]',
        'input[placeholder*="to:" i]',
        'input[aria-label*="to:" i]',
        'input',
        'textarea',
        '[contenteditable="true"]',
        '[role="combobox"]',
        '[role="textbox"]',
      ];
      const els = Array.from(document.querySelectorAll(selectors.join(',')));
      const norm = (s) => (s || '').toString().toLowerCase();
      const visible = (el) => {
        try {
          if (!el || el.disabled) return false;
          if (el.type === 'hidden') return false;
          const r = el.getClientRects && el.getClientRects();
          if (r && r.length) return true;
          const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
          return !!style && style.display !== 'none' && style.visibility !== 'hidden';
        } catch {
          return false;
        }
      };
      const preferred = els.find((el) => {
        const ph = norm(el.getAttribute && el.getAttribute('placeholder'));
        const aria = norm(el.getAttribute && el.getAttribute('aria-label'));
        const role = norm(el.getAttribute && el.getAttribute('role'));
        return ph.includes('search') || ph.includes('to:') || aria.includes('search') || aria.includes('to:') || role === 'combobox' || role === 'textbox';
      });
      return preferred || els.find(visible) || els[0] || null;
    });
    const retrySearchEl = retryHandle.asElement();
    if (retrySearchEl) {
      const retryMeta = await page.evaluate((el) => ({ tag: el.tagName, type: el.type || '', isCE: !!el.isContentEditable }), retrySearchEl).catch(() => ({}));
      await retrySearchEl.click({ delay: 50 }).catch(() => {});
      if (retryMeta.tag === 'INPUT' || retryMeta.tag === 'TEXTAREA') {
        await retrySearchEl.type(u, { delay: 90 });
      } else {
        await delay(100);
        await page.keyboard.type(u, { delay: 90 });
      }
      await retrySearchEl.dispose().catch(() => {});
      await retryHandle.dispose().catch(() => {});
    } else {
      await retryHandle.dispose().catch(() => {});
      throw new Error(`Search input not found on direct/new page (url=${diag?.url || 'unknown'} visible=${diag?.visibleCount ?? 'n/a'})`);
    }
  }

  const searchMeta = await page.evaluate((el) => ({ tag: el.tagName, type: el.type || '', isCE: !!el.isContentEditable }), searchEl).catch(() => ({}));
  await tinyHumanMouseMove(page);
  await searchEl.click({ delay: 50 }).catch(() => {});

  if (searchMeta.tag === 'INPUT' || searchMeta.tag === 'TEXTAREA') {
    await searchEl.type(u, { delay: 90 });
  } else {
    // contenteditable element: use keyboard typing so React/Instagram gets the right events
    await delay(100);
    await page.keyboard.type(u, { delay: 90 });
  }

  await searchEl.dispose();
  await searchHandle.dispose();
  await delay(2800);
  await dismissInstagramHomeModals(page, logger).catch(() => {});
  await delay(300);

  if (wantsDmSearchPairScreenshots()) {
    await saveDmSearchDebugScreenshot(page, `dm_search_before_pick_${u.replace(/[^a-z0-9_-]/gi, '_').slice(0, 40)}`);
  }

  let searchPick = await clickInstagramDmSearchResult(page, u).catch((e) => ({
    ok: false,
    reason: 'search_result_select_failed',
    logLine: `evaluate_threw: ${e && e.message ? e.message : String(e)}`,
  }));
  if (!searchPick.ok && searchPick.reason === 'search_result_select_failed') {
    // "Turn on Notifications" or similar overlays can appear after typing and block row clicks.
    await dismissInstagramHomeModals(page, logger).catch(() => {});
    await delay(500);
    searchPick = await clickInstagramDmSearchResult(page, u).catch((e) => ({
      ok: false,
      reason: 'search_result_select_failed',
      logLine: `retry_evaluate_threw: ${e && e.message ? e.message : String(e)}`,
    }));
  }
  if (!searchPick.ok) {
    await logDmSearchFailureDiagnostics(page, u, searchPick).catch(() => {});
    return {
      ok: false,
      reason: searchPick.reason || 'search_result_select_failed',
      pageSnippet: formatSearchFailurePageSnippet(u, searchPick),
    };
  }
  // Display name extracted from the search result sidebar row (most reliable source —
  // IG always shows "Display Name / username / bio" in the left panel before you click).
  const sidebarDisplayName = (searchPick.displayName && String(searchPick.displayName).trim()) || null;
  if (sidebarDisplayName) {
    logger.log(`[name-extraction] sidebar display name for @${u}: "${sidebarDisplayName}"`);
  }
  await delay(1500);

  const openedThread = await page.evaluate(() => {
    const targets = ['button', 'div[role="button"]', 'a', 'span[role="button"]'];
    const candidates = [];
    for (const sel of targets) {
      document.querySelectorAll(sel).forEach((el) => {
        if (el.offsetParent !== null && el.offsetWidth > 0 && el.offsetHeight > 0) candidates.push(el);
      });
    }
    const needle = (t) => t.toLowerCase().replace(/\s+/g, ' ').trim();
    const labels = ['send message', 'message', 'next', 'chat', 'send a message', 'start a chat'];
    for (const label of labels) {
      const btn = candidates.find((el) => {
        const t = needle(el.textContent || '');
        return t === label || (t.includes('send') && t.includes('message')) || (t === 'next') || (t === 'chat');
      });
      if (btn) {
        btn.click();
        return true;
      }
    }
    for (const label of labels) {
      const btn = candidates.find((el) => needle(el.textContent || '').includes(label));
      if (btn) {
        btn.click();
        return true;
      }
    }
    return false;
  });
  if (openedThread) await delay(2500);
  await delay(2000);

  try {
    await page.waitForFunction(
      () => !window.location.pathname.includes('/direct/new') && window.location.pathname.includes('/direct/'),
      { timeout: 8000 }
    );
  } catch (e) {
    if (page.url().includes('/direct/new')) {
      await page.evaluate(() => {
        const clickables = Array.from(document.querySelectorAll('button, div[role="button"], a'));
        const nextOrChat = clickables.find((el) => {
          const t = (el.textContent || '').toLowerCase().trim();
          return t === 'next' || t === 'chat' || (t.includes('send') && t.includes('message'));
        });
        if (nextOrChat && nextOrChat.offsetParent) nextOrChat.click();
      });
      await delay(3000);
    }
  }
  await delay(2000);

  await dismissInstagramHomeModals(page, logger);
  await delay(600);

  const composeSelector = 'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]';
  logger.log('Waiting for compose area...');
  let composeFound = false;
  let noComposeReason = 'no_compose';
  let composeRecoveryScreenshotPath = null;
  try {
    await page.waitForSelector(composeSelector, { timeout: 20000 });
    composeFound = true;
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
  } catch (e) {
    // Instagram sometimes leaves us on /direct/t/... but only the inbox list is rendered (no right pane composer yet).
    // Try one recovery pass: open the thread row / accept request / click message-like CTA, then re-check compose.
    const recovery = await page
      .evaluate((usernameRaw) => {
        const username = (usernameRaw || '').replace(/^@/, '').toLowerCase().trim();
        const visible = (el) => {
          try {
            return !!el && el.offsetParent !== null;
          } catch {
            return false;
          }
        };
        const textOf = (el) => ((el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase());
        const clickEl = (el) => {
          if (!el) return false;
          const btn = el.closest('button, a, [role="button"]') || el;
          try {
            btn.scrollIntoView({ block: 'center' });
          } catch {}
          try {
            btn.click();
            return true;
          } catch {
            return false;
          }
        };

        // 1) If a request gate exists, accept/reply first so composer becomes available.
        const ctas = Array.from(document.querySelectorAll('button, [role="button"], a')).filter(visible);
        const acceptLike = ctas.find((el) => {
          const t = textOf(el);
          return (
            t === 'accept' ||
            t === 'allow' ||
            t === 'reply' ||
            t === 'message' ||
            t === 'send message' ||
            t === 'chat' ||
            t === 'continue'
          );
        });
        if (acceptLike && clickEl(acceptLike)) return { clicked: 'cta:' + textOf(acceptLike) };

        // 2) Re-open the currently targeted thread row from list if username is visible.
        if (username) {
          const threadRows = Array.from(
            document.querySelectorAll('a[href*="/direct/t/"], [role="link"][href*="/direct/t/"], div[role="button"]')
          ).filter(visible);
          const row = threadRows.find((el) => textOf(el).includes(username));
          if (row && clickEl(row)) return { clicked: 'thread_row_for_username' };
        }

        // 3) Fallback: click any visible /direct/t/ link to force thread pane render.
        const anyThreadLink = Array.from(document.querySelectorAll('a[href*="/direct/t/"]')).find(visible);
        if (anyThreadLink && clickEl(anyThreadLink)) return { clicked: 'thread_row_generic' };
        return { clicked: null };
      }, u)
      .catch(() => ({ clicked: null }));
    if (recovery?.clicked) {
      logger.log(`Compose recovery click: ${recovery.clicked}`);
      await delay(1800);
      await dismissInstagramHomeModals(page, logger);
      composeRecoveryScreenshotPath = await saveAfterComposeRecoveryScreenshot(page, recovery.clicked, u);
      try {
        await page.waitForSelector(composeSelector, { timeout: 12000 });
        composeFound = true;
        noComposeReason = null;
      } catch {}
    }
    if (!composeFound && page.url().includes('/direct/t/')) {
      // Thread URL is loaded but right pane sometimes fails to mount; one reload often restores composer.
      try {
        logger.warn('Compose still missing on /direct/t/ thread; retrying with one thread reload.');
        await page.reload({ waitUntil: 'domcontentloaded', timeout: 45000 });
        await delay(2500);
        await dismissInstagramHomeModals(page, logger);
        await page.waitForSelector(composeSelector, { timeout: 12000 });
        composeFound = true;
        noComposeReason = null;
      } catch {}
    }
    const diag = await runComposeDiagnostic(page, u).catch(() => ({}));
    const bodySnippet = (diag.bodySnippet || '').toLowerCase();
    if (bodySnippet.includes('this account is private') || bodySnippet.includes('account is private')) noComposeReason = 'account_private';
    else if (bodySnippet.includes("can't message") || bodySnippet.includes("can't send") || bodySnippet.includes('message request') || bodySnippet.includes("don't accept")) noComposeReason = 'messages_restricted';
    else if (bodySnippet.includes('couldn\'t find') || bodySnippet.includes('no results')) noComposeReason = 'user_not_found';
    if (!composeFound) {
      logger.warn('Compose wait failed ' + e.message + (noComposeReason !== 'no_compose' ? ' (page suggests: ' + noComposeReason + ')' : ''));
      logger.log('Compose diagnostic: ' + JSON.stringify(diag));
      await saveComposeFailureDebugScreenshot(page, u, noComposeReason || 'compose_missing');
    } else {
      logger.log('Compose recovery succeeded; composer detected after retry action.');
    }
  }

  // When lead has no display_name/first_name in DB but template uses {{first_name}}/{{full_name}}, get name from thread page (e.g. "AI Setter Test 8 aisettertest8")
  const templateUsesName = /\{\{\s*(first_name|full_name)\s*\}\}/i.test(messageTemplate);
  const preferThreadName = sendOpts.preferThreadName === true || sendOpts.dryRunNames === true;
  // Priority: DB name → sidebar (search result row, most reliable live source) → thread-header extraction.
  // preferThreadName skips DB names but sidebar is still reliable (it's live IG data, not a stale lead record).
  let displayNameForSubst = preferThreadName
    ? (sidebarDisplayName || null)
    : (nameFallback.display_name ?? nameFallback.first_name ?? sidebarDisplayName ?? null);
  let resolvedNameSource = displayNameForSubst
    ? (nameFallback.display_name && !preferThreadName)
      ? 'fallback_display_name'
      : (nameFallback.first_name && !preferThreadName)
      ? 'fallback_first_name'
      : 'sidebar'
    : null;
  let nameExtractionDebugSnapshot = null;
  const nameExtractionDebugLog =
    !!sendOpts.dryRunNames ||
    sendOpts.nameExtractionDebug === true ||
    process.env.NAME_EXTRACTION_DEBUG === '1' ||
    process.env.NAME_EXTRACTION_DEBUG === 'true';

  if (templateUsesName && !displayNameForSubst && (!nameFallback.display_name || !nameFallback.first_name || preferThreadName)) {
    try {
      const settleMs = Math.max(0, parseInt(process.env.NAME_EXTRACTION_SETTLE_MS || '0', 10) || 0);
      if (settleMs) await delay(settleMs);
      const extractionResult = await page.evaluate((username) => {
        const needle = username.replace(/^@/, '').toLowerCase();
        const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
        const tokenRegex = new RegExp(`(^|[^a-z0-9._])@?${needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z0-9._]|$)`, 'i');
        const containsUsernameToken = (s) => tokenRegex.test(clean(s).toLowerCase());
        /** Profile link aria is often navigation copy, not a display name (e.g. "Open the profile page of user"). */
        const isIgProfileNavAria = (s) => {
          const x = clean(s).toLowerCase();
          if (!x) return false;
          if (/\bopen\s+the\s+profile\s+page\s+of\b/.test(x)) return true;
          if (/^go\s+to\s+profile\b/.test(x)) return true;
          if (/^see\s+profile\b/.test(x)) return true;
          if (/^visit\s+profile\b/.test(x)) return true;
          return false;
        };
        const tooGeneric = (s, opts = {}) => {
          const t = clean(s).toLowerCase();
          if (!t) return true;
          if (!opts.allowEqualsUsername && (t === needle || t === `@${needle}`)) return true;
          if (t.length < 2 || t.length > 120) return true;
          if (/^(message|send message|chat|details|info|back|next|cancel)$/i.test(t)) return true;
          // Inbox-only relative time token (not a person name).
          if (/^\d{1,3}\s*[mhdw]$/i.test(t)) return true;
          // IG thread chrome (profile link parent is often only this).
          if (/^view\s*profile$/i.test(t)) return true;
          // After stripping handle from IG nav aria-labels ("Open the profile page of {user}").
          if (/^open\s+the\s+profile\s+page(\s+of)?$/.test(t)) return true;
          if (/^(follow|following|requested|message|share|more|options|report)$/i.test(t)) return true;
          if (/^conversation information$/i.test(t)) return true;
          return false;
        };
        const tooGenericReason = (s, opts = {}) => {
          const t = clean(s).toLowerCase();
          if (!t) return 'empty';
          if (!opts.allowEqualsUsername && (t === needle || t === `@${needle}`)) return 'equals_username';
          if (t.length < 2) return 'too_short';
          if (t.length > 120) return 'too_long';
          if (/^(message|send message|chat|details|info|back|next|cancel)$/i.test(t)) return 'ui_label';
          if (/^\d{1,3}\s*[mhdw]$/i.test(t)) return 'relative_time_token';
          if (/^view\s*profile$/i.test(t)) return 'ig_view_profile_chrome';
          if (/^open\s+the\s+profile\s+page(\s+of)?$/.test(t)) return 'ig_profile_nav_aria';
          if (/^(follow|following|requested|message|share|more|options|report)$/i.test(t)) return 'ig_action_chrome';
          if (/^conversation information$/i.test(t)) return 'ig_conversation_info';
          return 'unknown';
        };

        /** IG often concatenates header actions onto the title without spaces. */
        const stripHeaderUiTail = (s) =>
          clean(s.replace(/\s*(Audio call|Video call|Conversation information|Voice call)\b[\s\S]*$/i, ''));

        /** @type {{ ctx: string, rawPreview: string, splitPieces: string[], nonUserPieces: string[], usedFirstNonUserPiece: boolean, joinedBulletSegments?: boolean, afterSplit: string, afterStripUsername: string, rejected?: string, out: string }[]} */
        const normalizationTraces = [];

        const normalizeCandidateName = (raw, ctx, opts = {}) => {
          const trace = {
            ctx: ctx || 'unnamed',
            rawPreview: String(raw || '').slice(0, 320),
            splitPieces: /** @type {string[]} */ ([]),
            nonUserPieces: /** @type {string[]} */ ([]),
            usedFirstNonUserPiece: false,
            joinedBulletSegments: false,
            afterSplit: '',
            afterStripUsername: '',
            rejected: /** @type {string|undefined} */ (undefined),
            out: '',
          };
          let t = clean(raw);
          if (!t) {
            trace.rejected = 'empty_raw';
            normalizationTraces.push(trace);
            return '';
          }
          const splitPieces = t
            .split(/[|·•]/g)
            .map((x) => clean(x))
            .filter(Boolean);
          trace.splitPieces = splitPieces.slice(0, 12);
          if (splitPieces.length > 1) {
            const nonUserPieces = splitPieces.filter((p) => !/^instagram$/i.test(p));
            trace.nonUserPieces = nonUserPieces.slice(0, 12);
            if (nonUserPieces.length) {
              trace.usedFirstNonUserPiece = true;
              trace.joinedBulletSegments = true;
              t = clean(
                nonUserPieces
                  .map((p) => stripHeaderUiTail(p))
                  .filter(Boolean)
                  .join(' • ')
              );
            }
          }
          trace.afterSplit = t.slice(0, 200);
          t = clean(
            stripHeaderUiTail(
              t
                .replace(/\binstagram\b/gi, '')
            )
          );
          trace.afterStripUsername = t.slice(0, 200);
          if (tooGeneric(t, opts)) {
            trace.rejected = tooGenericReason(t, opts);
            trace.out = '';
            normalizationTraces.push(trace);
            return '';
          }
          trace.out = t;
          normalizationTraces.push(trace);
          return t;
        };

        const isNameLikeCandidate = (raw) => {
          const t = clean(raw);
          if (!t) return false;
          if (containsUsernameToken(t)) return false;
          if (/^view\s*profile$/i.test(t)) return false;
          if (/^message$/i.test(t)) return false;
          if (/^instagram$/i.test(t)) return false;
          if (/^\d{1,3}\s*[mhdw]$/i.test(t)) return false;
          if (/·\s*instagram\s*·?\s*view\s*profile/i.test(t)) return false;
          if (t.length < 2 || t.length > 80) return false;
          return true;
        };

        /**
         * Open conversation column only: anchor minLeft to the Message composer’s X position;
         * stop climbing when a parent spans too wide (inbox + thread row).
         */
        function threadPaneRoot() {
          const vis = (el) => {
            try {
              return el && el.offsetParent !== null;
            } catch {
              return false;
            }
          };
          const composers = Array.from(
            document.querySelectorAll(
              'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
            )
          ).filter(vis);
          let compose = composers.find((el) => {
            const ph = (el.getAttribute('placeholder') || '').toLowerCase();
            const aria = (el.getAttribute('aria-label') || '').toLowerCase();
            return ph.includes('message') || aria.includes('message');
          });
          if (!compose && composers.length) {
            const vwGuess = document.documentElement.clientWidth || 1200;
            compose =
              [...composers].reverse().find((el) => {
                try {
                  const r = el.getBoundingClientRect();
                  return r.width > 48 && r.height > 16 && r.left > vwGuess * 0.3;
                } catch {
                  return false;
                }
              }) || null;
          }
          if (!compose) return { el: document.body, composeFound: false };
          const vw = document.documentElement.clientWidth || 1200;
          const minLeft = Math.max(0, compose.getBoundingClientRect().left - 48);
          const maxPaneWidth = vw - minLeft + 320;
          let best = compose;
          let el = compose;
          let depthUsed = 0;
          for (let depth = 0; depth < 28 && el; depth++) {
            el = el.parentElement;
            if (!el || el === document.body || el === document.documentElement) break;
            const r = el.getBoundingClientRect();
            if (r.left < minLeft) break;
            if (r.width <= maxPaneWidth) {
              best = el;
              depthUsed = depth + 1;
            } else {
              break;
            }
          }
          const n = needle.toLowerCase();
          let elPane = best;
          let paneExpandSteps = 0;
          for (let step = 0; step < 24; step++) {
            const raw = (elPane.innerText || '').trim();
            if (n && raw.length >= 40 && raw.toLowerCase().includes(n)) {
              best = elPane;
              break;
            }
            const p = elPane.parentElement;
            if (!p || p === document.body || p === document.documentElement) break;
            const r = p.getBoundingClientRect();
            if (r.left < minLeft - 120) break;
            if (r.left <= 20 && r.width >= vw * 0.93) break;
            elPane = p;
            best = p;
            paneExpandSteps += 1;
          }
          return {
            el: best,
            composeFound: true,
            composeRect: compose.getBoundingClientRect(),
            paneRect: best.getBoundingClientRect(),
            climbDepth: depthUsed,
            paneExpandSteps,
            minLeft,
            maxPaneWidth,
          };
        }

        const paneInfo = threadPaneRoot();
        const pane = paneInfo.el;
        const hasTargetProfileLink = !!Array.from(
          pane.querySelectorAll('a[href*="instagram.com/"], a[href^="/"]')
        ).find((a) => {
          const href = (a.getAttribute('href') || '').toLowerCase();
          return href.includes(`/${needle}`) || href === `/${needle}/` || href === `/${needle}`;
        });
        const debug = {
          needle,
          hasTargetProfileLink,
          threadPane: {
            composeFound: paneInfo.composeFound,
            climbDepth: paneInfo.climbDepth ?? 0,
            paneExpandSteps: paneInfo.paneExpandSteps ?? 0,
            paneIncludesUsername:
              !!(needle && (pane.innerText || '').toLowerCase().includes(needle.toLowerCase())),
            minLeft: paneInfo.minLeft,
            maxPaneWidth: paneInfo.maxPaneWidth,
            paneTag: pane.tagName,
            paneRole: pane.getAttribute && pane.getAttribute('role'),
            paneClass: (pane.className && String(pane.className).slice(0, 160)) || '',
            paneInnerTextLen: (pane.innerText || '').length,
            paneInnerTextPreview: (pane.innerText || '').slice(0, 700).replace(/\s+/g, ' '),
            fallbackToBody: !paneInfo.composeFound,
          },
          headersTried: /** @type {object[]} */ ([]),
          step2HeadingHits: /** @type {object[]} */ ([]),
          step3Profile: null,
          step4Fallback: null,
          winningPath: /** @type {string|null} */ (null),
          normalizationTraces,
        };
        if (!debug.threadPane.paneIncludesUsername && !hasTargetProfileLink) {
          debug.winningPath = 'thread_identity_mismatch';
          return { extracted: null, debug };
        }

        const extractNameFromHeaderRoot = (root, headerDebug) => {
          const rawLines = (root.innerText || '').split(/\n/);
          const lines = rawLines.map((x) => clean(x)).filter(Boolean);
          headerDebug.linesLabeled = lines.slice(0, 20).map((line, idx) => ({
            lineIndex: idx,
            text: line.slice(0, 200),
            containsUsernameToken: containsUsernameToken(line),
          }));
          for (let i = 0; i < lines.length; i++) {
            if (!containsUsernameToken(lines[i])) continue;
            const lineDebug = {
              handleLineIndex: i,
              lineAbove: i > 0 ? lines[i - 1].slice(0, 200) : null,
              lineWithHandle: lines[i].slice(0, 200),
              lineBelow: i + 1 < lines.length ? lines[i + 1].slice(0, 200) : null,
            };
            headerDebug.handleLineDetail = lineDebug;
            const prev =
              i > 0
                ? normalizeCandidateName(
                    lines[i - 1],
                    `header[${headerDebug.headerIndex}]:lineAboveHandle`,
                    { allowEqualsUsername: true }
                  )
                : '';
            const next =
              i + 1 < lines.length
                ? normalizeCandidateName(
                    lines[i + 1],
                    `header[${headerDebug.headerIndex}]:lineBelowHandle`,
                    { allowEqualsUsername: true }
                  )
                : '';
            lineDebug.normalizedAbove = prev || null;
            lineDebug.normalizedBelow = next || null;
            if (prev) {
              headerDebug.pickedFrom = 'line_above_handle';
              return prev;
            }
            if (next) {
              headerDebug.pickedFrom = 'line_below_handle';
              return next;
            }
          }
          headerDebug.pickedFrom = null;
          return null;
        };

        // Deterministic header parser for:
        // "DisplayName Username · Instagram View profile"
        const extractDisplayNameByProfilePattern = (raw, ctx) => {
          const line = clean(raw);
          if (!line) return '';
          const escapedNeedle = needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          const m = line.match(
            new RegExp(
              `^(.*?)\\s+@?${escapedNeedle}\\s*(?:[·•|]\\s*)?instagram\\b[\\s\\S]*?\\bview\\s*profile\\b`,
              'i'
            )
          );
          if (!m || !m[1]) return '';
          const candidate = clean(m[1]).replace(/[·•|:\\-\\s]+$/g, '');
          return normalizeCandidateName(candidate, `${ctx}:display_before_username_instagram_profile`, {
            allowEqualsUsername: true,
          });
        };

        // 1) DM thread top header bar (pfp + display + username): authoritative source.
        const allHeaderRoots = Array.from(pane.querySelectorAll('header, [role="banner"]'));
        const headerMatchesNeedle = (root) => containsUsernameToken(root.innerText || '');
        const matchingHeaders = allHeaderRoots.filter(headerMatchesNeedle).sort((a, b) => {
          const ra = a.getBoundingClientRect();
          const rb = b.getBoundingClientRect();
          const aa = ra.width * ra.height;
          const ba = rb.width * rb.height;
          if (aa !== ba) return aa - ba;
          return ra.top - rb.top;
        });
        const otherHeaders = allHeaderRoots.filter((h) => !headerMatchesNeedle(h));
        let hi = 0;
        const headerCandidateBuffers = [];
        for (const root of [...matchingHeaders, ...otherHeaders]) {
          const ra = root.getBoundingClientRect();
          const headerDebug = {
            headerIndex: hi,
            group: matchingHeaders.includes(root) ? 'matching_handle' : 'other',
            tag: root.tagName,
            role: root.getAttribute && root.getAttribute('role'),
            area: Math.round(ra.width * ra.height),
            innerTextPreview: (root.innerText || '').slice(0, 500).replace(/\s+/g, ' '),
            matchesHandle: headerMatchesNeedle(root),
          };
          const got = extractNameFromHeaderRoot(root, headerDebug);
          headerDebug.extractNameFromHeaderRoot = got;
          debug.headersTried.push(headerDebug);
          if (!got) {
            const text = clean(root.innerText || '');
            if (text && isNameLikeCandidate(text)) {
              headerCandidateBuffers.push(text);
            }
          }
          hi += 1;
          if (got) {
            debug.winningPath = 'step1_header_banner_line_adjacent_to_handle';
            return { extracted: got, debug };
          }
        }

        // 1b) Top-strip direct parser: first line is display name, second line is username.
        // This mirrors the visible header on desktop and avoids body/list contamination.
        const topStrip = Array.from(pane.querySelectorAll('header, [role="banner"], [role="navigation"]'))
          .filter((el) => {
            try {
              return !!el && el.offsetParent !== null;
            } catch {
              return false;
            }
          })
          .sort((a, b) => {
            const ra = a.getBoundingClientRect();
            const rb = b.getBoundingClientRect();
            return ra.top - rb.top;
          })[0];
        if (topStrip) {
          const lines = (topStrip.innerText || '')
            .split(/\n/)
            .map((x) => clean(x))
            .filter(Boolean);
          const usernameLineIdx = lines.findIndex((ln) => containsUsernameToken(ln));
          if (usernameLineIdx > 0) {
            const rawDisplay = lines[usernameLineIdx - 1];
            const parsed = normalizeCandidateName(
              rawDisplay,
              'step1b:top_header_display_line',
              { allowEqualsUsername: true }
            );
            debug.step1TopBar = {
              lineCount: lines.length,
              usernameLineIdx,
              displayLine: rawDisplay || null,
              parsed: parsed || null,
            };
            if (parsed) {
              debug.winningPath = 'step1b_top_header_display_above_username';
              return { extracted: parsed, debug };
            }
          }
        }

        if (headerCandidateBuffers.length) {
          const sorted = [...new Set(headerCandidateBuffers)].sort((a, b) => b.length - a.length);
          const chosen = sorted[0];
          debug.winningPath = 'step1_header_banner_text_fallback';
          debug.step1Fallback = { candidates: sorted.slice(0, 10), chosen };
          return { extracted: chosen, debug };
        }

        // 2) Prefer thread header title/name when available (pane only).
        const headerCandidates = [];
        const selectors = [
          'header h1',
          'header h2',
          'header [role="heading"]',
          '[role="banner"] h1',
          '[role="banner"] h2',
          '[role="banner"] [role="heading"]',
        ];
        for (const sel of selectors) {
          pane.querySelectorAll(sel).forEach((el) => {
            const raw = el.textContent || '';
            const txt = normalizeCandidateName(raw, `step2:${sel}`);
            debug.step2HeadingHits.push({
              selector: sel,
              rawPreview: raw.slice(0, 240),
              normalized: txt || null,
            });
            if (txt) headerCandidates.push(txt);
          });
        }
        if (headerCandidates.length) {
          headerCandidates.sort((a, b) => b.length - a.length);
          debug.winningPath = 'step2_heading_longest_after_normalize';
          debug.step2Chosen = headerCandidates[0];
          return { extracted: headerCandidates[0], debug };
        }

        // 3) Profile link → parent text (often only "View profile"; tooGeneric rejects so we fall through to step 4).
        const profileLink = Array.from(pane.querySelectorAll('a[href*="instagram.com/"], a[href^="/"]')).find((a) => {
          const href = (a.getAttribute('href') || '').toLowerCase();
          return href.includes(`/${needle}`) || href === `/${needle}/` || href === `/${needle}`;
        });
        if (profileLink) {
          const headerPatternScopes = [];
          const headerLike = profileLink.closest('header');
          if (headerLike) headerPatternScopes.push({ raw: headerLike.innerText || headerLike.textContent || '', ctx: 'step3p:header' });
          let pAnc = profileLink.parentElement;
          for (let d = 0; d < 3 && pAnc; d++) {
            headerPatternScopes.push({ raw: pAnc.innerText || pAnc.textContent || '', ctx: `step3p:ancestor depth=${d}` });
            pAnc = pAnc.parentElement;
          }
          for (const scope of headerPatternScopes) {
            const parsed = extractDisplayNameByProfilePattern(scope.raw, scope.ctx);
            if (parsed) {
              debug.step3Profile = {
                href: (profileLink.getAttribute('href') || '').slice(0, 200),
                parsedByProfilePattern: parsed,
                parsedContext: scope.ctx,
              };
              debug.winningPath = 'step3_profile_pattern_display_username_instagram_view_profile';
              return { extracted: parsed, debug };
            }
          }

          const parent = profileLink.closest('header') || profileLink.parentElement || profileLink;
          const rawParent = parent.textContent || '';
          const txt = normalizeCandidateName(rawParent, 'step3:profile_parent');
          debug.step3Profile = {
            href: (profileLink.getAttribute('href') || '').slice(0, 200),
            parentTag: parent.tagName,
            rawPreview: rawParent.slice(0, 400),
            normalized: txt || null,
          };
          if (txt) {
            debug.winningPath = 'step3_profile_link_parent';
            return { extracted: txt, debug };
          }
          // 3b) IG often puts the display name in a sibling of the profile link (or sibling flex cell), not in the link/parent text.
          // Keep this strictly shallow (header row area) to avoid conversation-body UI text.
          const step3bPeers = [];
          const tryPeerText = (raw, ctx) => {
            const t = normalizeCandidateName(raw, ctx);
            if (t) step3bPeers.push(t);
          };
          let sEl = profileLink.previousElementSibling;
          while (sEl) {
            tryPeerText(sEl.innerText || sEl.textContent || '', 'step3b:prevSibling');
            sEl = sEl.previousElementSibling;
          }
          let anc = profileLink.parentElement;
          for (let depth = 0; depth <= 2 && anc; depth++) {
            for (const k of Array.from(anc.children || [])) {
              if (k === profileLink || k.contains(profileLink)) continue;
              const raw = (k.innerText || k.textContent || '').trim();
              if (!raw || raw.length > 200) continue;
              tryPeerText(raw, `step3b:ancestorChild depth=${depth}`);
            }
            if (step3bPeers.length) break;
            anc = anc.parentElement;
          }
          debug.step3Profile.step3bPeers = step3bPeers.slice(0, 16);
          if (step3bPeers.length) {
            const chosen = [...new Set(step3bPeers)][0];
            debug.winningPath = 'step3b_profile_row_peer';
            debug.step3Profile.step3bChosen = chosen;
            return { extracted: chosen, debug };
          }
          // If we already found the profile link but no reliable display-name candidate, do not widen
          // to global column scans (they can pick inbox chrome like "Request (1)").
          debug.winningPath = 'step3_profile_link_found_no_reliable_name';
          return { extracted: '', debug };
        } else {
          debug.step3Profile = { found: false };
        }

        // 4) Fallback: substring of pane before first username token (handles single-line IG headers).
        const body = pane.innerText || '';
        const idx = body.toLowerCase().indexOf(needle);
        if (idx > 0) {
          let before = body.slice(0, idx).trim();
          before = before.replace(/\s*·\s*Instagram\s*$/i, '').trim();
          const lines = before.split(/\n/);
          const lastPart = (lines[lines.length - 1] || '').trim();
          const maxSeg = 200;
          const candidate =
            lastPart.length > 0 && lastPart.length <= maxSeg && !/^https?:\/\//i.test(lastPart)
              ? lastPart
              : before.length > 0 && before.length <= maxSeg
                ? before
                : lastPart.length > 0 && !/^https?:\/\//i.test(lastPart)
                  ? lastPart.slice(0, maxSeg)
                  : before.length > 0
                    ? before.slice(0, maxSeg)
                    : null;
          const normalized = normalizeCandidateName(candidate || '', 'step4:pane_before_username');
          debug.step4Fallback = {
            needleIndex: idx,
            beforePreview: before.slice(-240),
            lastLineBeforeNeedle: lastPart.slice(0, 160),
            chosenCandidate: candidate,
            normalized: normalized || null,
          };
          if (candidate && !/^\d+$/.test(candidate) && normalized) {
            debug.winningPath = 'step4_pane_text_before_username_token';
            return { extracted: normalized, debug };
          }
        } else {
          debug.step4Fallback = { needleIndex: idx, note: idx <= 0 ? 'username_not_found_in_pane_text' : null };
        }

        // 5) Empty / sparse thread: display name is often in the column *above* the compose-scoped pane
        // (pane.innerText is only "username username · Instagram View profile"; heading row is a sibling branch).
        let wideRoot = pane;
        for (let d = 0; d < 12 && wideRoot && wideRoot.parentElement; d++) {
          const p = wideRoot.parentElement;
          if (!p || p === document.body || p === document.documentElement) break;
          wideRoot = p;
        }
        if (!wideRoot || wideRoot === document.documentElement) wideRoot = pane;

        const vwStep5 = document.documentElement.clientWidth || 1200;
        let paneLeftStep5 = 0;
        try {
          paneLeftStep5 = pane.getBoundingClientRect().left;
        } catch {
          paneLeftStep5 = 0;
        }
        const colLeftStep5 = Math.max(0, paneLeftStep5 - 80);
        const inThreadColumn = (el) => {
          try {
            const r = el.getBoundingClientRect();
            if (r.width < 6 || r.height < 6) return false;
            const cx = r.left + r.width / 2;
            return cx >= colLeftStep5 && r.left < vwStep5 - 4;
          } catch {
            return false;
          }
        };

        const step5Candidates = [];
        const step5Log = {
          wideRootTag: wideRoot.tagName,
          wideRootClass: (wideRoot.className && String(wideRoot.className).slice(0, 120)) || '',
          colLeftStep5,
          fromAria: [],
          fromHeadings: [],
          fromDirAuto: [],
        };

        const profileAnchorsWide = Array.from(wideRoot.querySelectorAll('a[href*="instagram.com/"], a[href^="/"]')).filter(
          (a) => {
            const href = (a.getAttribute('href') || '').toLowerCase();
            return href.includes(`/${needle}`) || href === `/${needle}/` || href === `/${needle}`;
          }
        );
        for (const a of profileAnchorsWide) {
          if (!inThreadColumn(a)) continue;
          const al = (a.getAttribute('aria-label') || '').trim();
          if (!al || /^view\s*profile$/i.test(al) || isIgProfileNavAria(al)) continue;
          const cleanedAria = al.replace(/\s*,?\s*verified\s*$/i, '').trim();
          const t = normalizeCandidateName(cleanedAria, 'step5:profile_aria_label');
          if (t) {
            step5Candidates.push(t);
            step5Log.fromAria.push(t);
          }
        }

        const headingNodes = wideRoot.querySelectorAll('[role="heading"], h1, h2, h3');
        headingNodes.forEach((el) => {
          try {
            if (!el.offsetParent) return;
          } catch {
            return;
          }
          if (!inThreadColumn(el)) return;
          const raw = (el.textContent || '').replace(/\s+/g, ' ').trim();
          if (!raw || clean(raw).toLowerCase() === needle) return;
          const t = normalizeCandidateName(raw, 'step5:column_heading');
          if (t) {
            step5Candidates.push(t);
            step5Log.fromHeadings.push(t);
          }
        });

        const spanNearProfileAnchor = (spanEl, anchors) => {
          try {
            const rs = spanEl.getBoundingClientRect();
            const cy = rs.top + rs.height / 2;
            for (const a of anchors) {
              const ra = a.getBoundingClientRect();
              const ay = ra.top + ra.height / 2;
              if (Math.abs(cy - ay) < 140 && Math.abs(rs.left - ra.left) < 420) return true;
            }
          } catch {
            return false;
          }
          return false;
        };
        if (profileAnchorsWide.length) {
          wideRoot.querySelectorAll('span[dir="auto"]').forEach((el) => {
            try {
              if (!el.offsetParent) return;
            } catch {
              return;
            }
            if (!inThreadColumn(el)) return;
            if (!spanNearProfileAnchor(el, profileAnchorsWide)) return;
            const raw = (el.textContent || '').replace(/\s+/g, ' ').trim();
            if (!raw || raw.length > 120) return;
            const t = normalizeCandidateName(raw, 'step5:span_dir_auto');
            if (t) {
              step5Candidates.push(t);
              step5Log.fromDirAuto.push(t);
            }
          });
        }

        debug.step5WideColumn = {
          ...step5Log,
          candidateCount: step5Candidates.length,
          chosen: null,
        };

        if (step5Candidates.length) {
          const uniq = [...new Set(step5Candidates)].filter((candidate) => !/^view\s*profile$/i.test(candidate));
          uniq.sort((a, b) => b.length - a.length);
          const chosen = uniq[0];
          debug.step5WideColumn.chosen = chosen;
          debug.winningPath = 'step5_wide_column_heading_or_aria';
          return { extracted: chosen, debug };
        }

        debug.winningPath = null;
        return { extracted: null, debug };
      }, u);

      const extracted = extractionResult && extractionResult.extracted;
      const nameDebug = extractionResult && extractionResult.debug;
      if (nameDebug) nameExtractionDebugSnapshot = nameDebug;
      if (nameDebug && nameDebug.winningPath === 'thread_identity_mismatch') {
        const diag = await runComposeDiagnostic(page, u).catch(() => ({}));
        logger.warn(`Thread identity mismatch for @${u}; aborting to avoid wrong-thread extraction/send.`);
        logger.log('Compose diagnostic: ' + JSON.stringify(diag));
        if (diag.paneScopedSnippet) {
          logger.log(
            'Compose pane snippet (thread column, same scope as display-name extraction): ' + diag.paneScopedSnippet
          );
        }
        return {
          ok: false,
          reason: 'thread_mismatch',
          pageSnippet: `Active thread does not appear to be @${u}; aborting to avoid wrong-thread send.`,
          previewNamesOnly: !!sendOpts.dryRunNames,
          username: u,
          url: page.url(),
          pane_scoped_snippet: diag.paneScopedSnippet || null,
          body_snippet: diag.bodySnippet || null,
          name_extraction_debug: nameDebug,
        };
      }

      if (nameExtractionDebugLog && nameDebug) {
        try {
          const payload = JSON.stringify({ username: u, ...nameDebug }, null, 0);
          const max = 24000;
          logger.log(
            `[name-extraction-debug] @${u} ${payload.length > max ? payload.slice(0, max) + '…[truncated]' : payload}`
          );
        } catch (e) {
          logger.warn(`[name-extraction-debug] could not serialize debug for @${u}: ${e.message || e}`);
        }
      }

      if (extracted) {
        const trimmed = extracted.trim();
        if (/^\d{1,3}\s*[mhdw]$/i.test(trimmed)) {
          logger.log(`Display name from thread for @${u} ignored (inbox time token, not a name): "${extracted}"`);
        } else {
          const firstWord = trimmed.split(/\s+/)[0] || '';
          const normalizedFirst = normalizeName(firstWord);
          const blocklist = sendOpts.firstNameBlocklist || new Set();
          if (!normalizedFirst) {
            logger.log(`Display name from thread for @${u} not used: first word normalized to empty`);
          } else if (blocklist.has(normalizedFirst.toLowerCase())) {
            logger.log(`Display name from thread for @${u} not used: first name "${normalizedFirst}" is blocklisted`);
          } else {
            if (preferThreadName || !displayNameForSubst) {
              displayNameForSubst = extracted;
              resolvedNameSource = 'thread';
              logger.log(`Using display name from thread for @${u}: "${extracted}"`);
            }
          }
        }
      }
    } catch (e) {
      if (nameExtractionDebugLog) {
        logger.warn(`[name-extraction-debug] page.evaluate failed @${u}: ${e && e.message ? e.message : String(e)}`);
      }
    }
  }

  const leadFromPage = {
    username: u,
    first_name: nameFallback.first_name ?? null,
    last_name: nameFallback.last_name ?? null,
    display_name: displayNameForSubst ?? nameFallback.display_name ?? null,
  };
  const deriveNamesFromLead = (lead) => {
    const fromDisplay = typeof lead.display_name === 'string' ? lead.display_name.trim() : '';
    if (fromDisplay) {
      const words = fromDisplay.split(/\s+/).filter(Boolean);
      if (words.length > 0) {
        const first = normalizeName(words[0]);
        const last = words.length > 1 ? normalizeName(words.slice(1).join(' ')) : '';
        return { first_name: first || null, last_name: last || null };
      }
    }
    const f = normalizeName((lead.first_name || '').trim());
    const l = normalizeName((lead.last_name || '').trim());
    return { first_name: f || null, last_name: l || null };
  };
  const derivedNames = deriveNamesFromLead(leadFromPage);
  const msg = substituteVariables(messageTemplate, leadFromPage, {
    firstNameBlocklist: sendOpts.firstNameBlocklist || new Set(),
    onFirstNameEmpty: (reason) => logger.warn(`First name empty for @${u}: ${reason}`),
    senderName: sendOpts.senderName || '',
  });
  const threadId = getInstagramThreadIdFromUrl(page.url());
  if (sendOpts.dryRunNames) {
    let fullNameOut = '';
    if (leadFromPage.display_name && String(leadFromPage.display_name).trim()) {
      fullNameOut = normalizeFullDisplayName(leadFromPage.display_name);
    }
    if (!fullNameOut) {
      fullNameOut = [derivedNames.first_name, derivedNames.last_name].filter(Boolean).join(' ');
    }
    const diag = await runComposeDiagnostic(page, u).catch(() => ({}));
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));
    if (diag.paneScopedSnippet) {
      logger.log(
        'Compose pane snippet (thread column, same scope as display-name extraction): ' + diag.paneScopedSnippet
      );
    }
    return {
      ok: composeFound,
      previewNamesOnly: true,
      reason: composeFound ? undefined : noComposeReason,
      username: u,
      url: page.url(),
      instagramThreadId: threadId || undefined,
      display_name: leadFromPage.display_name || null,
      first_name: derivedNames.first_name || null,
      last_name: derivedNames.last_name || null,
      full_name: fullNameOut || null,
      resolved_name_source: resolvedNameSource,
      composeFound,
      pane_scoped_snippet: diag.paneScopedSnippet || null,
      body_snippet: diag.bodySnippet || null,
      name_extraction_debug: nameExtractionDebugSnapshot,
      ...(composeRecoveryScreenshotPath ? { compose_recovery_screenshot: composeRecoveryScreenshotPath } : {}),
    };
  }
  const shouldSendText = voiceCfg.mode !== 'voice_only';
  const shouldSendVoice = wantsVoiceNotes(voiceCfg);
  let textSent = false;
  let voiceSent = false;
  let voiceFailure = null;

  const attemptVoiceSend = async () => {
    if (!shouldSendVoice) return;
    // NEW: Chrome fake mic — durationSec from browser restart (must be set by caller).
    const durationSec = sendOpts.voiceDurationSec;
    if (durationSec == null) {
      voiceFailure = 'voice_duration_missing';
      logger.warn(`Voice note: durationSec not set — browser restart with convert required before sendDM.`);
      return;
    }
    try {
      const prep = await prepareVoiceNoteUi(page, { logger });
      if (!prep.ok) {
        voiceFailure = prep.reason || 'voice_mic_not_found';
        logger.warn(`Voice note UI not ready for @${u}: ${voiceFailure}`);
        return;
      }
      const voiceResult = await sendVoiceNoteInThread(page, {
        logger,
        voiceSource: { durationSec },
      });
      if (!voiceResult.ok) {
        voiceFailure = voiceResult.reason || 'voice_note_failed';
        logger.warn(`Voice note send failed for @${u}: ${voiceFailure}`);
        return;
      }
      voiceSent = true;
      logger.log(`Voice note sent to @${u}.`);
    } catch (e) {
      voiceFailure = e.message || 'voice_note_failed';
      logger.warn(`Voice note send error for @${u}: ${voiceFailure}`);
    }
  };

  if (composeFound) {
    const diag = await runComposeDiagnostic(page, u).catch(() => ({}));
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));
    if (diag.paneScopedSnippet) {
      logger.log('Compose pane snippet (thread column, same scope as display-name extraction): ' + diag.paneScopedSnippet);
    }
    noComposeReason = null;

    const composeEl = await page.evaluateHandle(() => {
      const byPlaceholder = (el) => {
        const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
        const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
        const t = (p + ' ' + a).toLowerCase();
        return t.includes('message') || t.includes('add a message') || t.includes('write a message');
      };
      const all = document.querySelectorAll('textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]');
      for (const el of all) {
        if (el.offsetParent === null) continue;
        if (byPlaceholder(el)) return el;
      }
      for (const el of all) {
        if (el.offsetParent !== null) return el;
      }
      return null;
    });
    const compose = composeEl.asElement();
  if (compose && shouldSendText) {
    await delay(500);
    await tinyHumanMouseMove(page);
    await compose.click();
    await saveComposeTypingDebugScreenshot(page, u);
    await typeInstagramDmPlainTextInComposer(page, compose, msg, {
      delay: 60 + Math.floor(Math.random() * 40),
    });
    await compose.dispose();
    await composeEl.dispose();
    await humanDelay();
    await tinyHumanMouseMove(page);
    await page.keyboard.press('Enter');
    await delay(1500);
    await saveComposePostSendDebugScreenshot(page, u);
    textSent = true;
    } else if (compose) {
      await compose.dispose();
      await composeEl.dispose();
      logger.log(`Skipping text send for @${u} (voice_only mode).`);
    } else {
      await composeEl.dispose();
      logger.warn('Compose element not found after selector matched');
    }

    await attemptVoiceSend();
    if (textSent || voiceSent) {
      return {
        ok: true,
        finalMessage: textSent ? msg : null,
        instagramThreadId: threadId,
        display_name: leadFromPage.display_name || undefined,
        first_name: derivedNames.first_name || undefined,
        last_name: derivedNames.last_name || undefined,
      };
    }
    if (voiceFailure) return { ok: false, reason: voiceFailure };
  }

  const keyboardSent = shouldSendText ? await page.evaluate((text) => {
    const focusable = document.querySelector('textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]');
    if (!focusable || focusable.offsetParent === null) return false;
    focusable.focus();
    focusable.click();
    return true;
  }, msg) : false;
  if (keyboardSent) {
    await delay(300);
    await saveComposeTypingDebugScreenshot(page, u);
    await typeInstagramDmPlainTextWithKeyboard(page, msg, { delay: 60 + Math.floor(Math.random() * 40) });
    await humanDelay();
    await tinyHumanMouseMove(page);
    await page.keyboard.press('Enter');
    await delay(1500);
    await saveComposePostSendDebugScreenshot(page, u);
    textSent = true;
    await attemptVoiceSend();
    return {
      ok: true,
      finalMessage: msg,
      instagramThreadId: threadId,
      display_name: leadFromPage.display_name || undefined,
      first_name: derivedNames.first_name || undefined,
      last_name: derivedNames.last_name || undefined,
    };
  }

  await attemptVoiceSend();
  if (voiceSent) {
    return {
      ok: true,
      finalMessage: null,
      instagramThreadId: threadId,
      display_name: leadFromPage.display_name || undefined,
      first_name: derivedNames.first_name || undefined,
      last_name: derivedNames.last_name || undefined,
    };
  }
  if (voiceFailure) return { ok: false, reason: voiceFailure };

  return { ok: false, reason: noComposeReason || 'no_compose' };
}

/**
 * Opens Instagram with a stored sender session, runs the same DM-open + display-name path as a real send,
 * but does not type or send. For admin "name test" debugging.
 *
 * @param {{ clientId: string, instagramSessionId: string, username?: string, targetUsername?: string, first_name?: string, last_name?: string, display_name?: string }} body
 */
async function previewDmLeadNamesFromSession(body) {
  const clientId = (body.clientId || '').trim();
  const instagramSessionId = (body.instagramSessionId || '').trim();
  const targetUsername = normalizeUsername(body.username || body.targetUsername || '');
  if (!clientId || !instagramSessionId || !targetUsername) {
    return { ok: false, error: 'clientId, instagramSessionId, and username are required' };
  }
  if (!sb.isSupabaseConfigured()) {
    return { ok: false, error: 'Supabase not configured' };
  }
  const session = await sb.getInstagramSessionByIdForClient(clientId, instagramSessionId);
  if (!session) {
    return { ok: false, error: 'Instagram session not found' };
  }
  const cookies = session.session_data?.cookies;
  if (!cookies?.length) {
    return { ok: false, error: 'Session has no cookies; reconnect Instagram' };
  }

  const firstNameBlocklist = new Set();
  if (sb.getFirstNameBlocklist) {
    const list = await sb.getFirstNameBlocklist(clientId).catch(() => []);
    list.forEach((n) => firstNameBlocklist.add(String(n).toLowerCase()));
  }
  let senderAccountName = '';
  if (sb.getUserAccountName) {
    senderAccountName = (await sb.getUserAccountName(clientId).catch(() => null)) || '';
  }
  const nameFallback = {
    first_name: body.first_name,
    last_name: body.last_name,
    display_name: body.display_name,
  };

  const launchOpts = buildFollowUpLaunchOptions(DEFAULT_CHROME_FAKE_MIC_WAV, session.proxy_url);
  let browser;
  try {
    browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url);
    await grantMicrophoneForInstagram(page, logger);
    // Mirror live sender behavior so admin name-test reflects production extraction context.
    if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);
    await page.setCookie(...cookies);
    await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
    await delay(3000);
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
    if (page.url().includes('/accounts/login')) {
      let screenshotPath = null;
      try {
        fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
        screenshotPath = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_preview_session_expired.png`);
        await page.screenshot({ path: screenshotPath, fullPage: true });
      } catch (_) {}
      return { ok: false, error: 'Instagram session expired', screenshotPath };
    }

    const result = await sendDMOnce(page, targetUsername, '{{first_name}}', nameFallback, {
      dryRunNames: true,
      preferThreadName: true,
      firstNameBlocklist,
      senderName: senderAccountName,
    });
    if (result.previewNamesOnly) {
      return {
        ok: result.ok,
        username: result.username,
        url: result.url,
        instagramThreadId: result.instagramThreadId,
        display_name: result.display_name,
        first_name: result.first_name,
        last_name: result.last_name,
        full_name: result.full_name,
        composeFound: result.composeFound,
        reason: result.reason,
        pane_scoped_snippet: result.pane_scoped_snippet,
        body_snippet: result.body_snippet,
        name_extraction_debug: result.name_extraction_debug,
        ...(result.compose_recovery_screenshot
          ? { compose_recovery_screenshot: result.compose_recovery_screenshot }
          : {}),
      };
    }
    return { ok: false, error: 'Unexpected send path (preview only)' };
  } catch (e) {
    if (e && e.code === 'INSTAGRAM_PASSWORD_REAUTH' && clientId && instagramSessionId) {
      await sb.handleInstagramPasswordReauthDisruption(clientId, instagramSessionId).catch(() => {});
      return {
        ok: false,
        error: 'instagram_password_reauth_required',
        message:
          'Instagram asked for your password again. Open Settings → Integrations and tap Reconnect.',
        gateDetails: e.gateDetails || undefined,
      };
    }
    logger.warn(`[preview-dm-names] ${e && e.message ? e.message : String(e)}`);
    let screenshotPath = null;
    try {
      if (browser) {
        const pages = await browser.pages().catch(() => []);
        const page = pages && pages.length > 0 ? pages[pages.length - 1] : null;
        if (page) {
          fs.mkdirSync(LOGIN_DEBUG_SCREENSHOT_DIR, { recursive: true });
          screenshotPath = path.join(LOGIN_DEBUG_SCREENSHOT_DIR, `${Date.now()}_preview_exception.png`);
          await page.screenshot({ path: screenshotPath, fullPage: true });
        }
      }
    } catch (_) {}
    return { ok: false, error: e.message || 'Preview failed', screenshotPath };
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

function buildFollowUpLaunchOptions(fakeMicPath = DEFAULT_CHROME_FAKE_MIC_WAV, proxyUrl = null) {
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger, fakeMicPath);
  const opts = {
    headless: HEADLESS,
    args: [...baseChromeArgs(), '--autoplay-policy=no-user-gesture-required'],
  };
  appendChromeFakeMicArgs(opts.args, fakeMicPath);
  applyPuppeteerSlowMo(opts);
  applyHeadedChromeWindowToLaunchOpts(opts);
  applyProxyToLaunchOptions(opts, proxyUrl);
  return opts;
}

/** Only one manual debug browser at a time (prevents duplicate Chromium on the same DISPLAY). */
let manualDebugFollowUpBrowserActive = false;

/**
 * Opens headed Chromium with stored IG session cookies, then idles for manual testing (VNC).
 * Does not send DMs or voice. Queued via scheduleDebugFollowUpBrowser.
 *
 * Env: HEADLESS_MODE=false + DISPLAY (e.g. :98) to see the window.
 * FOLLOW_UP_DEBUG_BROWSER_MS — if set and > 0, auto-close after N ms; otherwise hold until PM2 restart.
 */
async function debugOpenFollowUpBrowserForManualTest(body) {
  const clientId = (body.clientId || '').trim();
  const instagramSessionId = (body.instagramSessionId || '').trim();
  const recipientUsername = (body.recipientUsername || '').trim().replace(/^@/, '');
  if (!clientId || !instagramSessionId) {
    logger.warn('[debug] follow-up/browser: missing clientId or instagramSessionId');
    manualDebugFollowUpBrowserActive = false;
    return { ok: false, error: 'clientId and instagramSessionId required' };
  }

  let browser;
  let infiniteHold = false;
  try {
    const session = await sb.getInstagramSessionByIdForClient(clientId, instagramSessionId);
    if (!session) {
      logger.warn('[debug] follow-up/browser: session not found');
      manualDebugFollowUpBrowserActive = false;
      return { ok: false, error: 'Instagram session not found' };
    }
    const cookies = session.session_data?.cookies;
    if (!cookies?.length) {
      logger.warn('[debug] follow-up/browser: session has no cookies');
      manualDebugFollowUpBrowserActive = false;
      return { ok: false, error: 'Session has no cookies; reconnect Instagram' };
    }

    const launchOpts = buildFollowUpLaunchOptions(DEFAULT_CHROME_FAKE_MIC_WAV, session.proxy_url);
    browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url);
    await grantMicrophoneForInstagram(page, logger);
    await applyDesktopEmulation(page);
    await page.setCookie(...cookies);
    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
    await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
    await delay(2000);
    await grantMicrophoneForInstagram(page, logger);
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
    if (page.url().includes('/accounts/login')) {
      logger.warn('[debug] follow-up/browser: landed on login — session expired');
      await browser.close().catch(() => {});
      browser = null;
      manualDebugFollowUpBrowserActive = false;
      return { ok: false, error: 'Instagram session expired' };
    }

    if (recipientUsername) {
      const u = normalizeUsername(recipientUsername);
      await tinyHumanMouseMove(page);
      const nav = await navigateToDmThread(page, u);
      if (!nav.ok) {
        logger.warn(
          `[debug] follow-up/browser: could not open DM @${u} (${nav.reason || 'unknown'}) — staying on current page`
        );
      } else {
        await grantMicrophoneForInstagram(page, logger);
      }
    }

    const holdMs = parseInt(process.env.FOLLOW_UP_DEBUG_BROWSER_MS, 10);
    const hasTimedHold = Number.isFinite(holdMs) && holdMs > 0;

    const dvp = buildDesktopViewport();
    const pad = getDesktopWindowPadding();
    logger.log(
      `[debug] follow-up/browser: Chromium open for manual test (user=${session.instagram_username || 'n/a'}). ` +
        `DISPLAY=${process.env.DISPLAY || '(unset)'} viewport=${dvp.width}x${dvp.height} ` +
        `windowSize=${dvp.width + pad.padX}x${dvp.height + pad.padY} (pad +${pad.padX},+${pad.padY} for browser chrome). ` +
        (hasTimedHold
          ? `Auto-close after FOLLOW_UP_DEBUG_BROWSER_MS=${holdMs}ms.`
          : 'Holding until PM2 restart (or set FOLLOW_UP_DEBUG_BROWSER_MS).')
    );

    if (hasTimedHold) {
      await delay(holdMs);
    } else {
      infiniteHold = true;
      await new Promise(() => {});
    }
  } catch (e) {
    logger.error('[debug] follow-up/browser exception', e);
  } finally {
    if (browser) await browser.close().catch(() => {});
    if (!infiniteHold) manualDebugFollowUpBrowserActive = false;
  }
  return { ok: true };
}

/**
 * Queue a manual debug browser session (non-blocking HTTP). Returns immediately.
 * @returns {{ ok: boolean, error?: string }}
 */
function scheduleDebugFollowUpBrowser(body) {
  if (manualDebugFollowUpBrowserActive) {
    return {
      ok: false,
      error:
        'A debug browser session is already running. Restart PM2 or wait for FOLLOW_UP_DEBUG_BROWSER_MS to expire.',
    };
  }
  manualDebugFollowUpBrowserActive = true;
  setImmediate(() => {
    debugOpenFollowUpBrowserForManualTest(body || {}).catch((e) => {
      logger.error('[debug] follow-up/browser async error', e);
      manualDebugFollowUpBrowserActive = false;
    });
  });
  return { ok: true };
}

function followUpReasonToError(reason, pageSnippet) {
  const map = {
    user_not_found: 'Recipient not found in Instagram search',
    search_result_select_failed: 'Search showed results but the bot could not select the correct row (UI/DOM)',
    account_private: 'Instagram account is private or unavailable',
    rate_limited: 'Instagram rate limited. Try again later',
    no_compose: 'Could not open DM compose',
    messages_restricted: 'Messaging restricted for this thread',
    voice_mic_not_found: 'Could not find voice recorder control',
    voice_permission_denied: 'Microphone permission denied',
    voice_send_button_not_found: 'Could not send voice note',
    voice_note_failed: 'Voice note failed',
    voice_recording_ui_not_detected:
      'Voice recording UI did not appear after clicking the mic (no blue bar / 0:00 timer). Wrong control clicked or mic blocked; use VNC / HEADLESS_MODE=false to inspect',
    voice_not_confirmed_in_thread: 'Voice send was not confirmed in the thread (DOM did not update). Try debug screenshots, VNC, or PUPPETEER_SLOW_MO_MS',
    empty_message: 'Empty message',
  };
  let msg = map[reason] || reason || 'Send failed';
  if (pageSnippet) msg += ` (${String(pageSnippet).slice(0, 120)})`;
  return msg;
}

/**
 * SkeduleMore follow-up send (HTTP-triggered). Does not write to Supabase messages tables.
 *
 * Voice: pass `audioUrl` (HTTPS signed URL from Supabase Storage `voice-notes` or similar) and optional `caption`.
 * Follow-up audio is configured in dashboard `bot_config.follow_ups[]`; this handler does not read campaigns or
 * `cold_dm_message_group_messages` / migration 010 columns — only the request body + `cold_dm_instagram_sessions`.
 */
function logFollowUpFailure(clientId, instagramSessionId, recipientUsername, error, statusCode, correlationId) {
  const c = correlationId ? ` correlationId=${correlationId}` : '';
  logger.warn(
    `[follow-up] failed clientId=${clientId || '-'} sessionId=${instagramSessionId || '-'} recipient=@${recipientUsername || '-'} error=${error}${c}`
  );
  return { ok: false, error, statusCode };
}

/** Success payload for dashboard webhook dedupe (GraphQL `item_id` when captured). */
function followUpOkWithInstagramIds(payload) {
  const out = { ok: true };
  if (payload.instagram_message_id) {
    out.instagram_message_id = payload.instagram_message_id;
    out.instagramMessageId = payload.instagram_message_id;
  }
  if (payload.instagram_message_ids && payload.instagram_message_ids.length > 0) {
    out.instagram_message_ids = payload.instagram_message_ids;
    out.instagramMessageIds = payload.instagram_message_ids;
  }
  return out;
}

async function sendFollowUp(body) {
  const correlationId = (body.correlationId || body.requestId || '').trim();
  const cLog = correlationId ? ` correlationId=${correlationId}` : '';
  const clientId = (body.clientId || '').trim();
  const instagramSessionId = (body.instagramSessionId || '').trim();
  const recipientUsername = (body.recipientUsername || '').trim().replace(/^@/, '');
  const fail = (error, statusCode) =>
    logFollowUpFailure(clientId, instagramSessionId, recipientUsername, error, statusCode, correlationId);
  if (!clientId || !instagramSessionId || !recipientUsername) {
    return fail('clientId, instagramSessionId, and recipientUsername are required', 400);
  }

  const captionRaw = body.caption != null ? String(body.caption).trim() : '';
  const hasCaption = captionRaw !== '';

  const textSingle = body.text != null && String(body.text).trim() !== '';
  let messageLines = null;
  if (Array.isArray(body.messages)) {
    messageLines = body.messages.map((m) => String(m).trim()).filter(Boolean);
  }
  const hasMessages = messageLines && messageLines.length > 0;
  const audioUrlRaw = body.audioUrl != null ? String(body.audioUrl).trim() : '';
  const hasAudio = audioUrlRaw !== '';

  if (hasCaption && !hasAudio) {
    return fail('caption is only valid with audioUrl', 400);
  }

  const modeCount = [textSingle, hasMessages, hasAudio].filter(Boolean).length;
  if (modeCount !== 1) {
    return fail('Specify exactly one of: text, messages (non-empty strings), or audioUrl', 400);
  }
  if (hasAudio && !/^https:\/\//i.test(audioUrlRaw)) {
    return fail('audioUrl must be an HTTPS URL', 400);
  }

  const session = await sb.getInstagramSessionByIdForClient(clientId, instagramSessionId);
  if (!session) {
    return fail('Instagram session not found for this client', 404);
  }
  const cookies = session.session_data?.cookies;
  if (!cookies?.length) {
    return fail('Session has no cookies; reconnect Instagram', 400);
  }

  const modeLabel = hasAudio ? (hasCaption ? 'voice+caption' : 'voice') : hasMessages ? `messages(${messageLines.length})` : 'text';
  logger.log(
    `[follow-up] start clientId=${clientId} sessionId=${instagramSessionId} recipient=@${recipientUsername} mode=${modeLabel} sessionUser=${session.instagram_username || 'n/a'}${cLog}`
  );
  if (hasAudio) {
    logger.log(
      `[follow-up] voice debug: VOICE_NOTE_STRICT_VERIFY=${VOICE_NOTE_STRICT_VERIFY} slowMo=${getPuppeteerSlowMo() || 0}ms HEADLESS=${HEADLESS}`
    );
  }

  if (hasAudio && !isFfmpegAvailable()) {
    return fail(
      'ffmpeg/ffprobe not installed on this server (required for voice follow-ups). Run: sudo apt install ffmpeg',
      503
    );
  }

  // NEW: For voice, download + convert to Chrome fake mic format BEFORE launch.
  // We restart browser so the new audio file is loaded via --use-file-for-fake-audio-capture.
  let voiceDurationSec = null;
  let followUpFakeMicPath = DEFAULT_CHROME_FAKE_MIC_WAV;
  if (hasAudio) {
    followUpFakeMicPath = buildChromeFakeMicPath(
      `${clientId}-${instagramSessionId}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`
    );
    const resolved = await resolveVoiceNotePath(audioUrlRaw);
    if (!resolved.localPath) {
      return fail('Could not download audio file', 400);
    }
    try {
      const conv = convertToChromeFakeMicWav(resolved.localPath, logger, followUpFakeMicPath);
      voiceDurationSec = conv.durationSec;
    } catch (e) {
      await resolved.cleanup().catch(() => {});
      return fail(e.message && e.message.includes('convert') ? 'Could not convert audio' : (e.message || 'Audio conversion failed'), 400);
    }
    await resolved.cleanup();
  }

  const launchOpts = buildFollowUpLaunchOptions(followUpFakeMicPath, session.proxy_url);
  let browser;
  let idCapture = null;
  try {
    browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url);
    idCapture = attachInstagramSendIdCapture(page, { logger });
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);
    await page.setCookie(...cookies);
    if (hasAudio) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);

    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
    await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
    await delay(2000);
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
    if (page.url().includes('/accounts/login')) {
      return fail('Instagram session expired', 401);
    }

    const u = normalizeUsername(recipientUsername);
    await tinyHumanMouseMove(page);
    const nav = await navigateToDmThread(page, u);
    if (!nav.ok) {
      const errMsg = followUpReasonToError(nav.reason, nav.pageSnippet);
      return fail(errMsg, 400);
    }
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);

    if (textSingle) {
      await tinyHumanMouseMove(page);
      const sent = await sendPlainTextInThread(page, String(body.text).trim(), { idCapture });
      if (!sent.ok) {
        return fail(followUpReasonToError(sent.reason), 400);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      if (sent.instagramMessageId) {
        logger.log(`[follow-up] instagram_message_id=${sent.instagramMessageId}${cLog}`);
      }
      return followUpOkWithInstagramIds({
        instagram_message_id: sent.instagramMessageId || undefined,
      });
    }

    if (hasMessages) {
      const collectedIds = [];
      for (const line of messageLines) {
        await tinyHumanMouseMove(page);
        const sent = await sendPlainTextInThread(page, line, { idCapture });
        if (!sent.ok) {
          return fail(followUpReasonToError(sent.reason), 400);
        }
        collectedIds.push(sent.instagramMessageId || null);
        await delay(2000);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      const instagram_message_ids = collectedIds.some((x) => x != null) ? collectedIds : undefined;
      if (instagram_message_ids) {
        logger.log(`[follow-up] instagram_message_ids=${JSON.stringify(instagram_message_ids)}${cLog}`);
      }
      return followUpOkWithInstagramIds({ instagram_message_ids });
    }

    if (hasAudio) {
      const captionIds = [];
      if (hasCaption) {
        await tinyHumanMouseMove(page);
        const cap = await sendPlainTextInThread(page, captionRaw, { idCapture });
        if (!cap.ok) {
          return fail(followUpReasonToError(cap.reason), 400);
        }
        captionIds.push(cap.instagramMessageId || null);
        await delay(1200);
      }
      await tinyHumanMouseMove(page);
      const prep = await prepareVoiceNoteUi(page, { logger });
      if (!prep.ok) {
        return fail(followUpReasonToError(prep.reason || 'voice_mic_not_found'), 400);
      }
      const voiceResult = await sendVoiceNoteInThread(page, {
        logger,
        correlationId,
        voiceSource: { durationSec: voiceDurationSec },
        idCapture,
      });
      if (!voiceResult.ok) {
        return fail(followUpReasonToError(voiceResult.reason || 'voice_note_failed'), 400);
      }
      if (VOICE_POST_SEND_BROWSER_WAIT_MS > 0) {
        logger.log(
          `[follow-up] voice: waiting ${VOICE_POST_SEND_BROWSER_WAIT_MS}ms before closing browser (upload settle)`
        );
        await delay(VOICE_POST_SEND_BROWSER_WAIT_MS);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      fs.unlink(followUpFakeMicPath, () => {});
      if (hasCaption) {
        const pair = [...captionIds, voiceResult.instagramMessageId || null];
        if (pair.some((x) => x != null)) {
          logger.log(`[follow-up] instagram_message_ids=${JSON.stringify(pair)}${cLog}`);
        }
        return followUpOkWithInstagramIds({ instagram_message_ids: pair });
      }
      if (voiceResult.instagramMessageId) {
        logger.log(`[follow-up] instagram_message_id=${voiceResult.instagramMessageId}${cLog}`);
      }
      return followUpOkWithInstagramIds({
        instagram_message_id: voiceResult.instagramMessageId || undefined,
      });
    }

    return fail('No delivery mode', 400);
  } catch (e) {
    logger.warn(`[follow-up] exception clientId=${clientId} recipient=@${recipientUsername} error=${e.message}${cLog}`);
    return { ok: false, error: e.message || 'Send failed', statusCode: 500 };
  } finally {
    if (idCapture && typeof idCapture.dispose === 'function') {
      try {
        idCapture.dispose();
      } catch {
        /* ignore */
      }
    }
    if (browser) await browser.close().catch(() => {});
    if (hasAudio && followUpFakeMicPath && followUpFakeMicPath !== DEFAULT_CHROME_FAKE_MIC_WAV) {
      fs.unlink(followUpFakeMicPath, () => {});
    }
  }
}

/** Chrome/Puppeteer errors where retrying the next lead will burn the queue — pause sending instead. */
function isProxyOrNetworkInfrastructureError(message) {
  const m = String(message || '');
  return (
    m.includes('ERR_TUNNEL_CONNECTION_FAILED') ||
    m.includes('ERR_PROXY_CONNECTION_FAILED') ||
    m.includes('ERR_PROXY_CERTIFICATE_INVALID') ||
    m.includes('ERR_CONNECTION_CLOSED') ||
    m.includes('ERR_CONNECTION_RESET') ||
    m.includes('ERR_CONNECTION_REFUSED') ||
    m.includes('ERR_NAME_NOT_RESOLVED') ||
    m.includes('ERR_INTERNET_DISCONNECTED') ||
    m.includes('ERR_ADDRESS_UNREACHABLE') ||
    m.includes('ERR_SSL_PROTOCOL_ERROR')
  );
}

async function sendDM(page, username, adapter, options = {}) {
  const {
    messageOverride,
    campaignId,
    campaignLeadId,
    messageGroupId,
    messageGroupMessageId,
    dailySendLimit,
    hourlySendLimit,
    instagramSessionId,
  } = options;
  const sendWorkerId = options.sendWorkerId || null;
  const u = normalizeUsername(username);
  const sent = await Promise.resolve(adapter.alreadySent(u));
  if (sent) {
    logger.warn(`Already sent to @${u}, skipping.`);
    if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'sent', null, sendWorkerId).catch(() => {});
    return { ok: false, reason: 'already_sent' };
  }

  const freshCampaignLimits =
    campaignId && typeof sb.getCampaignLimitsById === 'function'
      ? await sb.getCampaignLimitsById(campaignId).catch(() => null)
      : null;
  const effectiveDailyLimit = freshCampaignLimits ? freshCampaignLimits.daily_send_limit : dailySendLimit;
  const effectiveHourlyLimit = freshCampaignLimits ? freshCampaignLimits.hourly_send_limit : hourlySendLimit;
  const stats = await Promise.resolve(adapter.getDailyStats(campaignId, effectiveDailyLimit));
  const hourlySent = await Promise.resolve(adapter.getHourlySent());
  const limitState = evaluateCampaignLimitState({
    sentToday: stats.total_sent,
    sentThisHour: hourlySent,
    dailySendLimit: effectiveDailyLimit,
    hourlySendLimit: effectiveHourlyLimit,
  });
  if (limitState.blocked) {
    throttleSendLimitLog(`sendDM:${campaignId || 'no-campaign'}:${limitState.reason}`, () => {
      logger.warn(limitState.statusMessage);
    });
    return { ok: false, reason: limitState.reason, statusMessage: limitState.statusMessage };
  }

  const messageTemplate = messageOverride || adapter.getRandomMessage();
  const resolvedVoicePath = (options.voice_note_path || options.voiceNotePath || VOICE_NOTE_FILE || '').trim();
  const resolvedVoiceMode = (options.voice_note_mode || options.voiceNoteMode || VOICE_NOTE_MODE || 'after_text').trim().toLowerCase();
  const logSent = (status, finalMsg, failureReason = null) =>
    adapter.logSentMessage(u, finalMsg != null ? finalMsg : messageTemplate, status, campaignId, messageGroupId, messageGroupMessageId, failureReason);

  let lastError;
  const nameFallback = {
    first_name: options.first_name,
    last_name: options.last_name,
    display_name: options.display_name,
  };
  let firstNameBlocklist = new Set();
  if (options.clientId && sb.getFirstNameBlocklist) {
    const list = await sb.getFirstNameBlocklist(options.clientId).catch(() => []);
    list.forEach((n) => firstNameBlocklist.add(n.toLowerCase()));
  }
  let senderAccountName = '';
  if (options.clientId && sb.getUserAccountName) {
    senderAccountName = (await sb.getUserAccountName(options.clientId).catch(() => null)) || '';
  }
  const preferThreadName = options.preferThreadName !== false;
  const hasCampaignCooldown =
    options.minDelaySec != null &&
    options.maxDelaySec != null &&
    Number.isFinite(Number(options.minDelaySec)) &&
    Number.isFinite(Number(options.maxDelaySec));
  if (!hasCampaignCooldown) {
    const statusMessage = 'Campaign is missing min/max send delay settings. Set them before starting.';
    logger.warn(statusMessage);
    return { ok: false, reason: 'missing_delay_config', statusMessage };
  }
  const sendCooldownMs = randomDelay(
    Math.max(0, Number(options.minDelaySec)) * 1000,
    Math.max(0, Number(options.maxDelaySec)) * 1000
  );
  for (let attempt = 1; attempt <= MAX_SEND_RETRIES; attempt++) {
    try {
      if (page && instagramSessionId) {
        try {
          const pu = page.url() || '';
          if (pu.includes('/accounts/login') || (await detectInstagramPasswordReauthScreen(page))) {
            await sb.markInstagramSessionWebNeedsRefresh(instagramSessionId).catch(() => {});
            return {
              ok: false,
              reason: 'session_logged_out',
              statusMessage: 'Instagram logged out — reconnect this sender in Cold Outreach.',
            };
          }
        } catch (_) {}
      }
      const result = await sendDMOnce(page, u, messageTemplate, nameFallback, {
        firstNameBlocklist,
        senderName: senderAccountName,
        voiceNotePath: resolvedVoicePath,
        voiceNoteMode: resolvedVoiceMode,
        voiceDurationSec: options.voiceDurationSec,
        preferThreadName,
      });
      if (result.ok) {
        const finalMessage = result.finalMessage != null ? result.finalMessage : (resolvedVoiceMode === 'voice_only' ? '' : messageTemplate);
        await Promise.resolve(logSent('success', finalMessage));
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'sent', null, sendWorkerId).catch(() => {});
        if (options.clientId && (result.display_name || result.first_name || result.last_name) && typeof sb.upsertLeadIdentity === 'function') {
          sb.upsertLeadIdentity(options.clientId, u, {
            display_name: result.display_name,
            first_name: result.first_name,
            last_name: result.last_name,
          }).catch(() => {});
        }
        if (options.clientId && result.instagramThreadId) {
          const payload = {
            client_id: options.clientId,
            instagram_thread_id: result.instagramThreadId,
            username: u,
            message_text: finalMessage || undefined,
            sent_at: new Date().toISOString(),
            message_group_id: messageGroupId || undefined,
            message_group_message_id: messageGroupMessageId || undefined,
          };
          if (result.display_name) payload.display_name = result.display_name;
          if (result.first_name) payload.first_name = result.first_name;
          if (result.last_name) payload.last_name = result.last_name;
          coldDmOnSend(payload).catch(() => {});
        }
        logger.log(`Sent to @${u}: ${(finalMessage || messageTemplate).slice(0, 30)}...`);
        return { ok: true, cooldownMs: sendCooldownMs };
      }
      if (!result.ok && page && instagramSessionId) {
        try {
          const pu = page.url() || '';
          if (pu.includes('/accounts/login') || (await detectInstagramPasswordReauthScreen(page))) {
            await sb.markInstagramSessionWebNeedsRefresh(instagramSessionId).catch(() => {});
            return {
              ok: false,
              reason: 'session_logged_out',
              statusMessage: 'Instagram logged out — reconnect this sender in Cold Outreach.',
            };
          }
        } catch (_) {}
      }
      if (result.reason === 'session_logged_out') {
        return result;
      }
      const terminalReasons = [
        'user_not_found',
        'search_result_select_failed',
        'account_unblock_required',
        'no_compose',
        'account_private',
        'rate_limited',
        'messages_restricted',
        'voice_note_failed',
        'voice_mic_not_found',
        'voice_permission_denied',
        'voice_send_button_not_found',
        'voice_note_file_not_found',
        'voice_note_download_failed',
        'ffmpeg_missing',
      ];
      if (terminalReasons.includes(result.reason)) {
        await Promise.resolve(logSent('failed', result.finalMessage, result.reason));
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed', result.reason, sendWorkerId).catch(() => {});
        const detail = result.pageSnippet ? ` ${result.pageSnippet}` : '';
        logger.warn(`Send failed for @${u}: ${result.reason}.${detail}`.trim());
        return result;
      }
      lastError = new Error(result.reason);
    } catch (err) {
      if (
        err &&
        err.code === 'INSTAGRAM_PASSWORD_REAUTH' &&
        options.clientId &&
        instagramSessionId
      ) {
        await sb.handleInstagramPasswordReauthDisruption(options.clientId, instagramSessionId).catch(() => {});
        await Promise.resolve(logSent('failed', null, 'instagram_password_reauth'));
        if (campaignLeadId) {
          await sb
            .updateCampaignLeadStatus(campaignLeadId, 'failed', 'instagram_password_reauth', sendWorkerId)
            .catch(() => {});
        }
        return {
          ok: false,
          reason: 'session_logged_out',
          statusMessage:
            'Instagram asked for your password again. Campaigns using this sender are paused. Open Settings → Integrations and tap Reconnect.',
        };
      }
      lastError = err;
      logger.warn(`Attempt ${attempt}/${MAX_SEND_RETRIES} for @${u} failed: ${err.message}`);
      if (attempt < MAX_SEND_RETRIES) await delay(2000 + Math.floor(Math.random() * 3000));
    }
  }
  logger.error(`Error sending to @${u} after ${MAX_SEND_RETRIES} retries`, lastError);
  if (isProxyOrNetworkInfrastructureError(lastError?.message)) {
    return {
      ok: false,
      reason: 'proxy_tunnel_failed',
      statusMessage:
        'Instagram unreachable (proxy/VPN tunnel or network failure). Sending paused — fix proxy and click Start.',
    };
  }
  await Promise.resolve(logSent('failed', null));
  if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed', null, sendWorkerId).catch(() => {});
  return { ok: false, reason: lastError.message };
}

function evaluateCampaignLimitState({ sentToday, sentThisHour, dailySendLimit, hourlySendLimit }) {
  if (dailySendLimit != null && sentToday >= dailySendLimit) {
    return {
      blocked: true,
      reason: 'daily_limit',
      statusMessage: `daily limit reached (campaign daily=${dailySendLimit}, sentToday=${sentToday}, counting=successful sends only)`,
    };
  }
  if (hourlySendLimit != null && sentThisHour >= hourlySendLimit) {
    return {
      blocked: true,
      reason: 'hourly_limit',
      statusMessage: `hourly limit reached (campaign hourly=${hourlySendLimit}, sentThisHour=${sentThisHour}, counting=successful sends only)`,
    };
  }
  return { blocked: false, reason: null, statusMessage: null };
}

function loadLeadsFromCSV(csvPath) {
  return new Promise((resolve, reject) => {
    const leads = [];
    const fullPath = path.isAbsolute(csvPath) ? csvPath : path.join(process.cwd(), csvPath);
    if (!fs.existsSync(fullPath)) {
      return reject(new Error(`Leads file not found: ${fullPath}`));
    }
    fs.createReadStream(fullPath)
      .pipe(csv())
      .on('data', (row) => {
        const u = (row.username || row.Username || row.user || row.User || Object.values(row)[0] || '').trim();
        if (u) leads.push(u.replace(/^@/, ''));
      })
      .on('end', () => resolve(leads))
      .on('error', reject);
  });
}

/** Build adapter and delays for a client (Supabase multi-tenant). Returns null if client has no templates (should not happen for campaign work). */
async function buildAdapterForClient(clientId) {
  const settings = await sb.getSettings(clientId);
  const messages = await sb.getMessageTemplates(clientId);
  const minDelayMs = (settings?.min_delay_minutes ?? 5) * 60 * 1000;
  const maxDelayMs = (settings?.max_delay_minutes ?? 30) * 60 * 1000;
  const adapter = {
    dailyLimit: Math.min(settings?.daily_send_limit ?? 100, 200),
    maxPerHour: settings?.max_sends_per_hour ?? 20,
    alreadySent: (u) => sb.alreadySent(clientId, u),
    logSentMessage: (u, msg, status, campaignId, messageGroupId, messageGroupMessageId, failureReason) =>
      sb.logSentMessage(clientId, u, msg, status, campaignId, messageGroupId, messageGroupMessageId, failureReason),
    getDailyStats: async (campaignId = null, campaignDailyLimit = null) => {
      if (campaignId && campaignDailyLimit != null && Number.isFinite(Number(campaignDailyLimit))) {
        const limits = await sb.getCampaignLimitsById(campaignId).catch(() => null);
        const campaignTz = limits?.timezone ?? null;
        if (campaignTz && typeof sb.getDailyStatsForTimezone === 'function') {
          return sb.getDailyStatsForTimezone(clientId, campaignTz);
        }
      }
      return sb.getDailyStats(clientId);
    },
    getHourlySent: () => sb.getHourlySent(clientId),
    getControl: () => sb.getControl(clientId),
    setControl: (v) => sb.setControl(clientId, v),
    getRandomMessage: () =>
      messages?.length ? messages[Math.floor(Math.random() * messages.length)] : '',
  };
  return { adapter, minDelayMs, maxDelayMs };
}

/**
 * PM2 cluster: pin this process to one active campaign's send queue (see NODE_APP_INSTANCE).
 * Off: SEND_WORKER_PIN_CAMPAIGNS=0. On without cluster: SEND_WORKER_PIN_CAMPAIGNS=1 and SEND_WORKER_CAMPAIGN_SLOT=0.
 * Manual override: COLD_DM_SEND_CAMPAIGN_IDS=id1,id2 (this process only claims those campaigns).
 */
function shouldPinSendWorkerToCampaignPool() {
  if (process.env.SEND_WORKER_PIN_CAMPAIGNS === '0' || process.env.SEND_WORKER_PIN_CAMPAIGNS === 'false') return false;
  if (process.env.SEND_WORKER_PIN_CAMPAIGNS === '1' || process.env.SEND_WORKER_PIN_CAMPAIGNS === 'true') return true;
  return process.env.NODE_APP_INSTANCE != null && String(process.env.NODE_APP_INSTANCE).trim() !== '';
}

function resolveSendWorkerCampaignSlot() {
  if (process.env.SEND_WORKER_CAMPAIGN_SLOT != null && String(process.env.SEND_WORKER_CAMPAIGN_SLOT).trim() !== '') {
    return parseInt(process.env.SEND_WORKER_CAMPAIGN_SLOT, 10);
  }
  if (process.env.NODE_APP_INSTANCE != null && String(process.env.NODE_APP_INSTANCE).trim() !== '') {
    return parseInt(process.env.NODE_APP_INSTANCE, 10);
  }
  return null;
}

let sendWorkerPinIdleLogLast = 0;
function throttlePinIdleLog(msg) {
  const now = Date.now();
  if (now - sendWorkerPinIdleLogLast < 5 * 60 * 1000) return;
  sendWorkerPinIdleLogLast = now;
  logger.log(msg);
}

/**
 * Multi-tenant loop: one worker serves all clients with pause=0 and pending work.
 * Exits when there is no work; start again from the dashboard when you have a campaign to run.
 */
async function runBotMultiTenant() {
  logger.log('Starting multi-tenant sender loop (always-on).');
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  let browser = null;
  let page = null;
  /** @type {string|null|undefined} undefined = never launched; '' = no proxy */
  let currentProxyKey = undefined;
  let currentSessionId = null;
  /** Instagram session row id for which `userDataDir` was opened (send worker persistent profile). */
  let currentProfileSessionId = null;
  /** Cross-process lock for persistent Chrome userDataDir (PM2 cluster must not double-open one profile). */
  let chromeProfileDirLock = null;
  /** Retries when cold_dm_control has no pause=0 yet (race right after dashboard Start). */
  let noPauseZeroEmptyRounds = 0;
  /** Set while this worker holds an IG session lease — PM2 stop may not run `finally` before exit. */
  let leasedSessionIdForSignal = null;
  /** Campaign currently leased by this worker (one at a time). */
  let leasedCampaignIdForSignal = null;
  /** Concurrency debug signal: only log claim when client changes. */
  let lastClaimedClientIdForDebug = null;
  process.once('SIGTERM', () => {
    void (async () => {
      const sid = leasedSessionIdForSignal;
      if (sid) {
        logger.warn('[send-worker] SIGTERM: releasing Instagram session lease');
        await sb.releaseInstagramSessionLease(sid, SEND_WORKER_ID).catch(() => {});
        leasedSessionIdForSignal = null;
      }
      const campaignId = leasedCampaignIdForSignal;
      if (campaignId) {
        logger.warn('[send-worker] SIGTERM: releasing campaign send lease');
        await sb.releaseCampaignSendLease(campaignId, SEND_WORKER_ID).catch(() => {});
        leasedCampaignIdForSignal = null;
      }
      await sb.releaseAllCampaignSendLeases(SEND_WORKER_ID).catch(() => {});
      process.exit(0);
    })();
  });
  process.once('SIGINT', () => {
    void (async () => {
      const sid = leasedSessionIdForSignal;
      if (sid) {
        logger.warn('[send-worker] SIGINT: releasing Instagram session lease');
        await sb.releaseInstagramSessionLease(sid, SEND_WORKER_ID).catch(() => {});
        leasedSessionIdForSignal = null;
      }
      const campaignId = leasedCampaignIdForSignal;
      if (campaignId) {
        logger.warn('[send-worker] SIGINT: releasing campaign send lease');
        await sb.releaseCampaignSendLease(campaignId, SEND_WORKER_ID).catch(() => {});
        leasedCampaignIdForSignal = null;
      }
      await sb.releaseAllCampaignSendLeases(SEND_WORKER_ID).catch(() => {});
      process.exit(0);
    })();
  });

  function proxyKeyForSession(session) {
    return session && session.proxy_url ? String(session.proxy_url).trim() : '';
  }

  function isDeadTargetOrCdpError(e) {
    const msg = (e && e.message) || String(e);
    return (
      /session closed/i.test(msg) ||
      /target closed/i.test(msg) ||
      /connection closed/i.test(msg) ||
      /execution context was destroyed/i.test(msg) ||
      /protocol error.*closed/i.test(msg)
    );
  }

  async function invalidateSendWorkerBrowser(reason) {
    if (reason) logger.warn(`[send-worker] Resetting Chrome: ${reason}`);
    await browser?.close?.().catch(() => {});
    if (chromeProfileDirLock && typeof chromeProfileDirLock.release === 'function') {
      try {
        chromeProfileDirLock.release();
      } catch {}
      chromeProfileDirLock = null;
    }
    browser = null;
    page = null;
    currentSessionId = null;
    currentProfileSessionId = null;
  }

  async function ensureBrowserForSession(session) {
    const key = proxyKeyForSession(session);
    const igSessionId = session && session.id != null ? String(session.id) : '';

    if (browser && typeof browser.isConnected === 'function' && !browser.isConnected()) {
      await invalidateSendWorkerBrowser('browser disconnected (Chrome exited or crashed)');
    }

    const profileChanged = igSessionId && currentProfileSessionId != null && currentProfileSessionId !== igSessionId;
    const proxyChanged = browser && currentProxyKey !== key;

    if (browser && (proxyChanged || profileChanged)) {
      logger.log(
        `[send-worker] Relaunching Chrome (${proxyChanged ? 'proxy' : ''}${proxyChanged && profileChanged ? ' + ' : ''}${
          profileChanged ? 'Instagram session' : ''
        } changed) — persistent profile is per sender session.`
      );
      await invalidateSendWorkerBrowser(null);
    }

    if (!browser) {
      currentProxyKey = key;
      const launchOpts = {
        headless: HEADLESS,
        args: [...baseChromeArgs(), '--autoplay-policy=no-user-gesture-required'],
      };
      if (PUPPETEER_PERSIST_SEND_PROFILES && igSessionId) {
        assignPersistentUserDataDir(launchOpts, `send-${igSessionId}`);
        logger.log(`[send-worker] Persistent Chrome profile: send-${igSessionId} (PUPPETEER_USER_DATA_ROOT / .browser-profiles)`);
      } else if (!PUPPETEER_PERSIST_SEND_PROFILES) {
        logger.log('[send-worker] Ephemeral Chrome profile (PUPPETEER_PERSIST_SEND_PROFILES=0)');
      }
      appendChromeFakeMicArgs(launchOpts.args);
      applyPuppeteerSlowMo(launchOpts);
      applyHeadedChromeWindowToLaunchOpts(launchOpts);
      applyProxyToLaunchOptions(launchOpts, session.proxy_url || null);
      if (launchOpts.slowMo) logger.log(`Puppeteer slowMo=${launchOpts.slowMo}ms (PUPPETEER_SLOW_MO_MS)`);
      if (launchOpts.userDataDir) {
        try {
          chromeProfileDirLock = await acquireChromeUserDataDirLock(launchOpts.userDataDir, {
            log: (msg) => logger.warn(`[send-worker] ${msg}`),
          });
        } catch (e) {
          logger.error('Chrome profile lock failed', e);
          throw e;
        }
      }
      try {
        browser = await puppeteer.launch(launchOpts);
      } catch (e) {
        const profileDir = launchOpts.userDataDir;
        if (profileDir && isChromeProfileSingletonLockError(e)) {
          // Keep chromeProfileDirLock: releasing here lets another PM2 worker open the same profile and races singleton recovery.
          logger.warn(
            '[send-worker] Chromium profile singleton locked (often orphan Chrome after PM2 restart). Cleaning while holding Node profile lock, retrying launch once…'
          );
          tryRecoverStaleChromeProfileLocks(profileDir, (m) => logger.warn(m));
          await delay(2000);
          try {
            browser = await puppeteer.launch(launchOpts);
            logger.warn('[send-worker] Chrome launched after singleton recovery');
          } catch (e2) {
            if (chromeProfileDirLock && typeof chromeProfileDirLock.release === 'function') {
              try {
                chromeProfileDirLock.release();
              } catch {}
              chromeProfileDirLock = null;
            }
            const msg2 = String((e2 && e2.message) || e2 || '');
            if (!(profileDir && isChromeProfileSingletonLockError(e2))) {
              logger.error('Browser launch failed (after singleton recovery)', e2);
              throw e2;
            }
            if (isChromeSingletonForeignHostError(e2)) {
              logger.warn(
                '[send-worker] Singleton lock references another hostname (cloned disk, old droplet, or NFS). Headless Chrome will not clear that; quarantining profile.'
              );
            } else {
              logger.warn(
                '[send-worker] Singleton still failing after kill/unlink; quarantining send profile once and retrying.'
              );
            }
            tryRecoverStaleChromeProfileLocks(profileDir, (m) => logger.warn(m));
            await delay(1500);
            if (!quarantineChromePersistentSendProfileDir(profileDir, (m) => logger.warn(m))) {
              logger.error('Browser launch failed (after singleton recovery)', e2);
              throw e2;
            }
            try {
              chromeProfileDirLock = await acquireChromeUserDataDirLock(profileDir, {
                log: (msg) => logger.warn(`[send-worker] ${msg}`),
              });
            } catch (lockErr) {
              logger.error('Chrome profile lock failed (after quarantine)', lockErr);
              throw lockErr;
            }
            try {
              browser = await puppeteer.launch(launchOpts);
              logger.warn('[send-worker] Chrome launched after profile quarantine (fresh userDataDir)');
            } catch (e3) {
              if (chromeProfileDirLock && typeof chromeProfileDirLock.release === 'function') {
                try {
                  chromeProfileDirLock.release();
                } catch {}
                chromeProfileDirLock = null;
              }
              logger.error('Browser launch failed (after profile quarantine)', e3);
              throw e3;
            }
          }
        } else {
          if (chromeProfileDirLock && typeof chromeProfileDirLock.release === 'function') {
            try {
              chromeProfileDirLock.release();
            } catch {}
            chromeProfileDirLock = null;
          }
          logger.error('Browser launch failed', e);
          throw e;
        }
      }
      currentProfileSessionId = igSessionId || null;
      page = await browser.newPage();
      await authenticatePageForProxy(page, session.proxy_url);
      await grantMicrophoneForInstagram(page, logger);
      if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
      else await applyMobileEmulation(page);
      return;
    }

    currentProxyKey = key;

    if (!page || (typeof page.isClosed === 'function' && page.isClosed())) {
      logger.warn('[send-worker] Page was missing or closed; opening a new tab on existing browser.');
      if (page && typeof page.isClosed === 'function' && !page.isClosed()) {
        await page.close().catch(() => {});
      }
      page = null;
      currentSessionId = null;
      page = await browser.newPage();
      await authenticatePageForProxy(page, session.proxy_url);
      await grantMicrophoneForInstagram(page, logger);
      if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
      else await applyMobileEmulation(page);
    }
  }

  async function ensurePageSession(session) {
    await ensureBrowserForSession(session);
    const pg = page;
    if (!pg) return false;
    if (currentSessionId === session.id) return true;
    const sessionLabel = session.instagram_username || session.id;
    try {
      const cookies = session?.session_data?.cookies;
      const hasCookies = Array.isArray(cookies) && cookies.length > 0;
      if (hasCookies) {
        await clearInstagramCookiesOnlyOnPage(pg);
        await pg.setCookie(...cookies);
      }
      let gotoTimedOut = false;
      try {
        await pg.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 45000 });
      } catch (e) {
        gotoTimedOut = e && e.name === 'TimeoutError';
        logger.warn(
          `Session switch navigation ${gotoTimedOut ? 'timed out' : 'failed'} for ${sessionLabel}: ${e.message}. Verifying current page before failing.`
        );
      }
      if (hasCookies && session?.session_data) {
        await applyInstagramWebStorageFromSessionData(pg, session.session_data, logger);
      }
      await delay(3000);
      const reauth = await detectInstagramPasswordReauthScreen(pg).catch(() => false);
      if (pg.url().includes('/accounts/login') || reauth) {
        logger.error(
          `Instagram session expired or security screen for account ${sessionLabel}` +
            (reauth ? ' (password / challenge UI detected)' : ' (login URL)')
        );
        if (session?.id) await sb.markInstagramSessionWebNeedsRefresh(session.id).catch(() => {});
        return false;
      }
      // If DB cookies were missing but the Chrome profile is already authenticated, capture and persist them
      // so future restarts can restore without relying on a warm disk profile.
      if (!hasCookies && session?.id && typeof sb.updateInstagramSessionSessionData === 'function') {
        try {
          const webStorageCap = await navigateAndCaptureInstagramWebStorage(pg, logger).catch(() => null);
          const freshCookies = await pg.cookies().catch(() => []);
          if (Array.isArray(freshCookies) && freshCookies.length > 0) {
            const nextSessionData = {
              ...(session.session_data || {}),
              cookies: freshCookies,
              ...(webStorageCap ? { web_storage: webStorageCap } : {}),
            };
            await sb.updateInstagramSessionSessionData(session.id, nextSessionData).catch(() => {});
          }
        } catch {}
      }
      currentSessionId = session.id;
      return true;
    } catch (e) {
      if (e && e.name === 'TimeoutError') {
        try {
          await delay(2000);
          const reauthT = await detectInstagramPasswordReauthScreen(pg).catch(() => false);
          if (!pg.url().includes('/accounts/login') && !reauthT) {
            logger.warn(`Session switch timeout for ${sessionLabel} but page is not login; continuing.`);
            currentSessionId = session.id;
            return true;
          }
          if (session?.id) await sb.markInstagramSessionWebNeedsRefresh(session.id).catch(() => {});
        } catch {}
      }
      logger.error('Failed to switch session: ' + e.message);
      if (isDeadTargetOrCdpError(e)) {
        await invalidateSendWorkerBrowser('CDP/target died during session switch');
      }
      return false;
    }
  }

  async function releaseClaimedCampaignLease(campaignId) {
    if (!campaignId) return;
    await sb.releaseCampaignSendLease(campaignId, SEND_WORKER_ID).catch(() => {});
    if (leasedCampaignIdForSignal === campaignId) leasedCampaignIdForSignal = null;
  }

  for (;;) {
    await sb.workerHeartbeat(SEND_WORKER_ID, 'send', { pid: process.pid }).catch(() => {});

    let pinnedCampaignIdsForClaim = null;
    /** When true, pinning is on but we must not claim without p_campaign_ids (avoids duplicate workers on one campaign/session). */
    let pinForcedIdle = false;
    let pinForcedIdleDetail = '';
    if (shouldPinSendWorkerToCampaignPool()) {
      const manualIds = (process.env.COLD_DM_SEND_CAMPAIGN_IDS || '').trim();
      if (manualIds) {
        pinnedCampaignIdsForClaim = manualIds.split(',').map((s) => s.trim()).filter(Boolean);
        if (!pinnedCampaignIdsForClaim.length) {
          pinForcedIdle = true;
          pinForcedIdleDetail =
            'COLD_DM_SEND_CAMPAIGN_IDS is empty after parse — idling (no unpinned claims).';
        }
      } else {
        const slot = resolveSendWorkerCampaignSlot();
        if (slot == null || Number.isNaN(slot)) {
          pinForcedIdle = true;
          pinForcedIdleDetail =
            'PIN_CAMPAIGNS on but no slot: set PM2 cluster (NODE_APP_INSTANCE) or SEND_WORKER_CAMPAIGN_SLOT — idling (no unpinned claims).';
        } else {
          const ordered = await sb.getDistinctActiveCampaignIdsWithReadySendJobs().catch(() => []);
          if (!ordered.length) {
            pinForcedIdle = true;
            pinForcedIdleDetail =
              'PIN_CAMPAIGNS: no active campaigns with ready send jobs at this moment — idling (no cross-campaign claims).';
          } else if (slot >= ordered.length) {
            pinForcedIdle = true;
            pinForcedIdleDetail = `PIN_CAMPAIGNS: slot ${process.env.NODE_APP_INSTANCE ?? process.env.SEND_WORKER_CAMPAIGN_SLOT} has no matching active campaign with ready send jobs — idling (no cross-campaign claims).`;
          } else {
            pinnedCampaignIdsForClaim = [ordered[slot]];
          }
        }
      }
    }

    if (pinForcedIdle) {
      throttlePinIdleLog(`[send-worker] ${pinForcedIdleDetail}`);
      await delay(randomDelay(20000, 40000));
      continue;
    }

    let claimedJob = await sb.claimColdDmSendJob(SEND_WORKER_ID, SEND_LEASE_SECONDS, pinnedCampaignIdsForClaim);
    if (!claimedJob) {
      const clientIds = await sb.getClientIdsWithPauseZero();
      logColdDmConcurrencyDebug('claim_miss_syncing_clients', {
        workerId: SEND_WORKER_ID,
        activeClientCount: clientIds.length,
        activeClientIds: clientIds,
      });
      for (const cid of clientIds) {
        const synced = await sb.syncSendJobsForClient(cid).catch((e) => {
          logger.error(`[send-worker] syncSendJobsForClient failed for ${cid}: ${e?.message || e}`);
          return 0;
        });
        if (synced > 0) logger.log(`[send-worker] synced ${synced} send job(s) for client ${cid}`);
        if (synced > 0) {
          logColdDmConcurrencyDebug('sync_jobs_for_client', {
            workerId: SEND_WORKER_ID,
            clientId: cid,
            syncedJobs: synced,
          });
        }
      }
      claimedJob = await sb.claimColdDmSendJob(SEND_WORKER_ID, SEND_LEASE_SECONDS, pinnedCampaignIdsForClaim);
    }
    if (!claimedJob) {
      const clientIds = await sb.getClientIdsWithPauseZero();
      if (clientIds.length === 0) {
        noPauseZeroEmptyRounds += 1;
        if (noPauseZeroEmptyRounds === 1) {
          logger.warn(
            `[send-worker] No cold_dm_control rows with pause=0 (attempt ${noPauseZeroEmptyRounds}). Retrying in 15s (common right after clicking Start).`
          );
        }
        if (noPauseZeroEmptyRounds < 24) {
          await delay(15000);
          continue;
        }
        logger.error('[send-worker] Giving up: still no pause=0 clients after ~6 min.');
        await invalidateSendWorkerBrowser(null);
        process.exit(0);
      }
      noPauseZeroEmptyRounds = 0;
      let earliestResumeAt = null;
      let resumeReason = '';
      const reasonMessageByClient = new Map();
      for (const cid of clientIds) {
        const info = await sb.getClientNoWorkResumeAt(cid).catch(() => ({ message: null, reason: 'no_pending', resumeAt: null }));
        if (info.message) {
          reasonMessageByClient.set(cid, info.message);
          await sb.setClientStatusMessage(cid, info.message).catch(() => {});
        }
        if (info.reason === 'no_pending') continue;
        if (info.reason === 'pending_ready') {
          await sb.setClientStatusMessage(cid, 'Syncing send jobs…').catch(() => {});
          earliestResumeAt = info.resumeAt || new Date(Date.now() + 15_000);
          resumeReason = 'pending_ready';
          continue;
        }
        if (info.resumeAt && (!earliestResumeAt || info.resumeAt.getTime() < earliestResumeAt.getTime())) {
          earliestResumeAt = info.resumeAt;
          resumeReason = info.reason;
        }
      }
      if (!earliestResumeAt) {
        for (const cid of clientIds) {
          const existingReasonMessage = reasonMessageByClient.get(cid);
          if (existingReasonMessage) {
            logger.log('No work: ' + existingReasonMessage);
            await sb.setClientStatusMessage(cid, existingReasonMessage).catch(() => {});
          } else {
            const hint = await sb.getNoWorkHint(cid).catch(() => '');
            if (hint) {
              logger.log('No work: ' + hint);
              await sb.setClientStatusMessage(cid, hint).catch(() => {});
            } else {
              await sb
                .setClientStatusMessage(
                  cid,
                  'No sendable campaign found. Check campaign status, lead groups, message template/group, schedule, and delay settings.'
                )
                .catch(() => {});
            }
          }
          // Keep control flag aligned with worker state so dashboards don't show "running" after an auto-exit.
          await sb.setControl(cid, 1).catch(() => {});
        }
        logger.log('No work. Exiting after surfacing the specific blocker for each client.');
        await invalidateSendWorkerBrowser(null);
        process.exit(0);
      }
      const sleepMs = Math.max(1000, earliestResumeAt.getTime() - Date.now());
      const sleepSec = Math.ceil(sleepMs / 1000);
      const sleepMin = Math.round(sleepMs / 60000);
      const sleepLabel = sleepSec < 60 ? `${sleepSec}s` : `${sleepMin} min`;
      logger.log(`Paused (${resumeReason}). Resuming in ${sleepLabel} at ${earliestResumeAt.toISOString().slice(0, 16)}.`);
      const SCHEDULE_RECHECK_MS = 5 * 60 * 1000;
      const chunkMs = resumeReason === 'outside_schedule' ? Math.min(sleepMs, SCHEDULE_RECHECK_MS) : sleepMs;
      await delay(chunkMs);
      continue;
    }

    if (SEND_WORKER_VERBOSE_LOGS) {
      logger.log(
        `[send-worker] claimed job ${claimedJob.id} campaign=${claimedJob.campaign_id || 'null'} ` +
          `client=${claimedJob.client_id || 'null'} campaignLead=${claimedJob.campaign_lead_id || 'null'} ` +
          `for ${claimedJob.username || '?'}`
      );
    }
    const claimedClientId = claimedJob.client_id || null;
    if (lastClaimedClientIdForDebug !== claimedClientId) {
      logColdDmConcurrencyDebug('claimed_job_client_switch', {
        workerId: SEND_WORKER_ID,
        fromClientId: lastClaimedClientIdForDebug,
        toClientId: claimedClientId,
        jobId: claimedJob.id || null,
        campaignId: claimedJob.campaign_id || null,
        campaignLeadId: claimedJob.campaign_lead_id || null,
        username: claimedJob.username || null,
      });
      lastClaimedClientIdForDebug = claimedClientId;
    }
    if (claimedJob.client_id && claimedJob.campaign_id && typeof sb.getClientSendCampaignTurn === 'function') {
      const campaignTurn = await sb.getClientSendCampaignTurn(claimedJob.client_id).catch(() => null);
      if (campaignTurn?.campaignId && campaignTurn.campaignId !== claimedJob.campaign_id) {
        const turnLabel = campaignTurn.campaignName || campaignTurn.campaignId;
        const turnMessage = `Waiting for "${turnLabel}" to finish sending before this campaign can start.`;
        await sb.updateSendJob(
          claimedJob.id,
          {
            status: 'retry',
            available_at: new Date(Date.now() + randomDelay(30, 60) * 1000).toISOString(),
            last_error_class: 'campaign_turn_locked',
            last_error_message: turnMessage,
          },
          SEND_WORKER_ID
        ).catch(() => {});
        await sb.setClientStatusMessage(claimedJob.client_id, turnMessage).catch(() => {});
        await releaseClaimedCampaignLease(claimedJob.campaign_id);
        await delay(randomDelay(500, 1500));
        continue;
      }
    }
    leasedCampaignIdForSignal = claimedJob.campaign_id || null;
    const resolved = await withTimeout(
      sb.buildSendWorkFromJob(claimedJob.id),
      SEND_STAGE_TIMEOUT_MS,
      `buildSendWorkFromJob timeout after ${SEND_STAGE_TIMEOUT_MS}ms`
    ).catch((e) => {
      logger.error(`[send-worker] buildSendWorkFromJob failed: ${e?.message || e}`);
      return null;
    });
    if (!resolved) {
      logger.error(`[send-worker] job ${claimedJob.id} resolution returned null (job_resolution_failed)`);
      await sb.updateSendJob(claimedJob.id, { status: 'failed', last_error_class: 'job_resolution_failed', last_error_message: 'Could not resolve send job payload.' }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      await delay(randomDelay(1000, 3000));
      continue;
    }
    if (resolved.disposition === 'retry' && resolved.reason === 'outside_schedule') {
      const msg =
        resolved.statusMessage ||
        'Outside sending schedule — waiting for the next send window.';
      await sb.setClientStatusMessage(claimedJob.client_id, msg).catch(() => {});
      throttleSendLimitLog(`outside_schedule:${claimedJob.campaign_id || 'unknown'}`, () => {
        logger.log(
          `[send-worker] outside_schedule campaign=${claimedJob.campaign_id} client=${claimedJob.client_id} available_at=${resolved.availableAt || '?'}`
        );
      });
    } else if (SEND_WORKER_VERBOSE_LOGS || resolved.disposition !== 'ready') {
      logger.log(
        `[send-worker] job ${claimedJob.id} resolved disposition=${resolved.disposition} reason=${resolved.reason || 'none'} ` +
          `client=${claimedJob.client_id || 'null'} campaign=${claimedJob.campaign_id || 'null'}`
      );
    }
    if (resolved.disposition === 'cancelled') {
      await sb.updateSendJob(claimedJob.id, {
        status: 'cancelled',
        last_error_class: resolved.reason || 'cancelled',
        last_error_message: resolved.reason || 'cancelled',
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      await delay(randomDelay(500, 1500));
      continue;
    }
    if (resolved.disposition === 'retry') {
      const retryNote =
        resolved.reason === 'outside_schedule' && resolved.statusMessage
          ? String(resolved.statusMessage).slice(0, 500)
          : resolved.reason || 'retry';
      await sb.updateSendJob(claimedJob.id, {
        status: 'retry',
        available_at: resolved.availableAt || new Date(Date.now() + 5 * 60 * 1000).toISOString(),
        last_error_class: resolved.reason || 'retry',
        last_error_message: retryNote,
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      await delay(randomDelay(500, 1500));
      continue;
    }
    if (resolved.disposition === 'failed') {
      if (resolved.work?.campaignLeadId) {
        await sb.updateCampaignLeadStatus(resolved.work.campaignLeadId, 'failed', resolved.reason || null, SEND_WORKER_ID).catch(() => {});
      } else if (resolved.job?.campaign_lead_id) {
        await sb.updateCampaignLeadStatus(resolved.job.campaign_lead_id, 'failed', resolved.reason || null, SEND_WORKER_ID).catch(() => {});
      }
      await sb.updateSendJob(claimedJob.id, {
        status: 'failed',
        last_error_class: resolved.reason || 'failed',
        last_error_message: resolved.reason || 'failed',
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      await delay(randomDelay(500, 1500));
      continue;
    }

    const work = resolved.work;
    const clientId = work.clientId;
    noPauseZeroEmptyRounds = 0;
    const pause = await withTimeout(
      sb.getControl(clientId),
      SEND_STAGE_TIMEOUT_MS,
      `getControl timeout after ${SEND_STAGE_TIMEOUT_MS}ms`
    ).catch((e) => {
      logger.error(`[send-worker] getControl failed for client ${clientId}: ${e?.message || e}`);
      return 1;
    });
    if (pause === '1' || pause === 1) {
      await sb.updateSendJob(claimedJob.id, {
        status: 'retry',
        available_at: new Date(Date.now() + 30 * 1000).toISOString(),
        last_error_class: 'client_paused',
        last_error_message: 'client_paused',
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      continue;
    }
    const built = await withTimeout(
      buildAdapterForClient(clientId),
      SEND_STAGE_TIMEOUT_MS,
      `buildAdapterForClient timeout after ${SEND_STAGE_TIMEOUT_MS}ms`
    ).catch((e) => {
      logger.error(`[send-worker] buildAdapterForClient failed for client ${clientId}: ${e?.message || e}`);
      return null;
    });
    if (!built) {
      logger.warn('No adapter for client ' + clientId + ', skipping.');
      await sb.updateSendJob(claimedJob.id, {
        status: 'retry',
        available_at: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
        last_error_class: 'missing_adapter',
        last_error_message: 'missing_adapter',
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      continue;
    }
    const { adapter, minDelayMs, maxDelayMs } = built;

    const campaignLimitsEarly =
      work.campaignId && typeof sb.getCampaignLimitsById === 'function'
        ? await sb.getCampaignLimitsById(work.campaignId).catch(() => null)
        : null;
    const effDailyEarly = campaignLimitsEarly?.daily_send_limit ?? work.dailySendLimit;
    const effHourlyEarly = campaignLimitsEarly?.hourly_send_limit ?? work.hourlySendLimit;
    try {
      const statsEarly = await adapter.getDailyStats(work.campaignId, effDailyEarly);
      const hourlyEarly = await adapter.getHourlySent();
      const limEarly = evaluateCampaignLimitState({
        sentToday: statsEarly.total_sent,
        sentThisHour: hourlyEarly,
        dailySendLimit: effDailyEarly,
        hourlySendLimit: effHourlyEarly,
      });
      if (limEarly.blocked) {
        const msg = limEarly.statusMessage || limEarly.reason;
        const isDaily = limEarly.reason === 'daily_limit';
        const deferMs = isDaily
          ? SEND_DAILY_LIMIT_DEFER_MS
          : randomDelay(55 * 60 * 1000, 60 * 60 * 1000);
        const untilIso = new Date(Date.now() + deferMs).toISOString();
        throttleSendLimitLog(`early:${work.campaignId}:${limEarly.reason}`, () => {
          logger.warn(msg);
          logger.log(
            `[send-worker] Deferring campaign ${work.campaignId} queue ~${Math.round(deferMs / 60000)} min (${limEarly.reason}).`
          );
        });
        await sb.setClientStatusMessage(clientId, msg).catch(() => {});
        await sb.updateSendJob(
          claimedJob.id,
          {
            status: 'retry',
            available_at: untilIso,
            last_error_class: limEarly.reason,
            last_error_message: msg,
          },
          SEND_WORKER_ID
        ).catch(() => {});
        await sb.deferCampaignPendingJobs(work.campaignId, claimedJob.id, untilIso).catch(() => {});
        await releaseClaimedCampaignLease(claimedJob.campaign_id);
        await delay(400);
        continue;
      }
    } catch (e) {
      logger.warn(`[send-worker] early send limit check failed (continuing): ${e.message || e}`);
    }

    logger.log(
      `[send-worker] preparing claimed job ${claimedJob.id} client=${clientId} campaign=${work.campaignId} username=@${work.username}`
    );
    sb.setClientStatusMessage(clientId, 'Preparing send…').catch(() => {});

    logger.log(`[send-worker] claiming Instagram session for campaign ${work.campaignId}`);
    const session = await withTimeout(
      sb.claimInstagramSessionForCampaign(clientId, work.campaignId, SEND_WORKER_ID, SEND_LEASE_SECONDS),
      SEND_STAGE_TIMEOUT_MS,
      `claimInstagramSessionForCampaign timeout after ${SEND_STAGE_TIMEOUT_MS}ms`
    ).catch((e) => {
      logger.error(`[send-worker] claimInstagramSessionForCampaign failed: ${e?.message || e}`);
      return null;
    });
    if (!session) {
      logger.warn(`No Instagram session available for campaign ${work.campaignId}, waiting.`);
      const waitingReason =
        (await sb.getWaitingInstagramSessionReason(clientId, work.campaignId).catch(() => null)) ||
        'Waiting for an available Instagram session…';
      await sb.setClientStatusMessage(clientId, waitingReason).catch(() => {});
      await sb.updateSendJob(claimedJob.id, {
        status: 'retry',
        available_at: new Date(Date.now() + randomDelay(15, 45) * 1000).toISOString(),
        last_error_class: 'waiting_for_session',
        last_error_message: waitingReason,
      }, SEND_WORKER_ID).catch(() => {});
      await releaseClaimedCampaignLease(claimedJob.campaign_id);
      await delay(randomDelay(5000, 15000));
      continue;
    }
    leasedSessionIdForSignal = session.id;
    leasedCampaignIdForSignal = work.campaignId;
    await sb.updateSendJob(claimedJob.id, { instagram_session_id: session.id }, SEND_WORKER_ID).catch(() => {});
    const leaseHeartbeatMs = Math.max(30000, Math.min(60000, Math.floor((SEND_LEASE_SECONDS * 1000) / 2)));
    let leaseHeartbeatTimer = null;
    let sendJobHeartbeatTimer = null;
    let campaignLeaseHeartbeatTimer = null;
    const startLeaseHeartbeat = () => {
      if (leaseHeartbeatTimer) return;
      leaseHeartbeatTimer = setInterval(() => {
        sb.heartbeatInstagramSessionLease(session.id, SEND_WORKER_ID, SEND_LEASE_SECONDS).catch(() => {});
      }, leaseHeartbeatMs);
      if (typeof leaseHeartbeatTimer.unref === 'function') leaseHeartbeatTimer.unref();
    };
    const startSendJobHeartbeat = () => {
      if (sendJobHeartbeatTimer) return;
      sendJobHeartbeatTimer = setInterval(() => {
        sb.heartbeatSendJobLease(claimedJob.id, SEND_WORKER_ID, SEND_LEASE_SECONDS, work.campaignId).catch(() => {});
      }, leaseHeartbeatMs);
      if (typeof sendJobHeartbeatTimer.unref === 'function') sendJobHeartbeatTimer.unref();
    };
    const startCampaignLeaseHeartbeat = () => {
      if (campaignLeaseHeartbeatTimer) return;
      campaignLeaseHeartbeatTimer = setInterval(() => {
        sb.heartbeatCampaignSendLease(work.campaignId, SEND_WORKER_ID, SEND_LEASE_SECONDS).catch(() => {});
      }, leaseHeartbeatMs);
      if (typeof campaignLeaseHeartbeatTimer.unref === 'function') campaignLeaseHeartbeatTimer.unref();
    };
    const stopLeaseHeartbeat = () => {
      if (leaseHeartbeatTimer) clearInterval(leaseHeartbeatTimer);
      leaseHeartbeatTimer = null;
    };
    const stopSendJobHeartbeat = () => {
      if (sendJobHeartbeatTimer) clearInterval(sendJobHeartbeatTimer);
      sendJobHeartbeatTimer = null;
    };
    const stopCampaignLeaseHeartbeat = () => {
      if (campaignLeaseHeartbeatTimer) clearInterval(campaignLeaseHeartbeatTimer);
      campaignLeaseHeartbeatTimer = null;
    };

    try {
      startLeaseHeartbeat();
      startSendJobHeartbeat();
      startCampaignLeaseHeartbeat();
      let skipSendAfterVoiceRestart = false;
      logger.log(`[send-worker] restoring Instagram session for @${work.username}`);
      let ensurePageSessionErr = '';
      const ok = await withTimeout(
        ensurePageSession(session),
        SEND_STAGE_TIMEOUT_MS,
        `ensurePageSession timeout after ${SEND_STAGE_TIMEOUT_MS}ms`
      ).catch((e) => {
        ensurePageSessionErr = String((e && e.message) || e || '');
        logger.error(`[send-worker] ensurePageSession failed: ${ensurePageSessionErr}`);
        return false;
      });
      if (!ok) {
        logger.warn('Could not load Instagram session; re-queueing send job (lead not failed).');
        const timedOut = /timeout after|ensurePageSession timeout/i.test(ensurePageSessionErr);
        const likelyLoggedOut = session?.web_session_needs_refresh === true || /security screen|login url|expired/i.test(ensurePageSessionErr || '');
        const statusMsg = timedOut
          ? 'Instagram browser did not become ready in time — often another send worker is waiting on the same Chrome profile. Retrying automatically. If this repeats, reduce concurrent send workers or restart ig-dm-send.'
          : likelyLoggedOut
            ? 'Instagram session needs reconnect — Instagram asked for login again. Open Cold Outreach and reconnect this sender.'
            : 'Instagram browser session was not ready for this send — retrying automatically.';
        await sb.setClientStatusMessage(clientId, statusMsg).catch(() => {});
        await sb.updateSendJob(
          claimedJob.id,
          {
            status: 'retry',
            available_at: new Date(Date.now() + randomDelay(30, 90) * 1000).toISOString(),
            last_error_class: timedOut ? 'session_load_timeout' : 'session_load_failed',
            last_error_message: timedOut
              ? ensurePageSessionErr.slice(0, 400) || 'session_load_timeout'
              : (ensurePageSessionErr || statusMsg).slice(0, 400) || 'session_load_failed',
          },
          SEND_WORKER_ID
        ).catch(() => {});
        await delay(randomDelay(2000, 5000));
        continue;
      }

      const options = {
        clientId,
        sendWorkerId: SEND_WORKER_ID,
        messageOverride: work.messageText,
        campaignId: work.campaignId,
        campaignLeadId: work.campaignLeadId,
        messageGroupId: work.messageGroupId,
        messageGroupMessageId: work.messageGroupMessageId,
        dailySendLimit: work.dailySendLimit,
        hourlySendLimit: work.hourlySendLimit,
        minDelaySec: work.minDelaySec,
        maxDelaySec: work.maxDelaySec,
        first_name: work.first_name,
        last_name: work.last_name,
        display_name: work.display_name,
        voice_note_path: work.voiceNotePath || VOICE_NOTE_FILE || null,
        voice_note_mode: work.voiceNoteMode || VOICE_NOTE_MODE || 'after_text',
        instagramSessionId: session.id,
      };
      logger.log(`[send-worker] sending DM to @${work.username}`);
      sb.setClientStatusMessage(clientId, 'Sending…').catch(() => {});

      // NEW: For voice notes, close browser, convert audio, relaunch so Chrome loads the new file.
      const needsVoice = (options.voice_note_path || '').trim() !== '';
      if (needsVoice && browser) {
        let resolved = null;
        try {
          resolved = await resolveVoiceNotePath(options.voice_note_path);
          if (resolved.localPath) {
            const conv = convertToChromeFakeMicWav(resolved.localPath, logger);
            options.voiceDurationSec = conv.durationSec;
            await invalidateSendWorkerBrowser(null);
            const okVoice = await ensurePageSession(session);
            if (!okVoice) {
              logger.warn('Could not restore session after voice browser restart.');
              skipSendAfterVoiceRestart = true;
            }
          }
        } catch (e) {
          logger.warn('Voice browser restart failed: ' + (e.message || e));
        } finally {
          if (resolved) await resolved.cleanup().catch(() => {});
        }
      }

      const sendResult = skipSendAfterVoiceRestart
        ? {
            ok: false,
            reason: 'session_logged_out',
            statusMessage: 'Instagram session lost after voice prep — reconnect sender in Cold Outreach.',
          }
        : await sendDM(page, work.username, adapter, options);

      let delayMs;
      let sendJobStatus = 'completed';
      let sendJobUpdates = {};
      if (!sendResult.ok && sendResult.reason === 'hourly_limit') {
        delayMs = randomDelay(55 * 60 * 1000, 60 * 60 * 1000);
        const msg = sendResult.statusMessage || 'hourly limit reached';
        throttleSendLimitLog(`postSend:hourly:${work.campaignId}`, () => {
          logger.log(`${msg}. Sleeping ${Math.round(delayMs / 60000)} minutes until window resets.`);
        });
        sb.setClientStatusMessage(clientId, `${msg}. Next send in ~60 min.`).catch(() => {});
        sendJobStatus = 'retry';
        sendJobUpdates = {
          available_at: new Date(Date.now() + delayMs).toISOString(),
          last_error_class: 'hourly_limit',
          last_error_message: msg,
        };
      } else if (!sendResult.ok && sendResult.reason === 'daily_limit') {
        delayMs = SEND_DAILY_LIMIT_DEFER_MS;
        const msg = sendResult.statusMessage || 'daily limit reached';
        throttleSendLimitLog(`postSend:daily:${work.campaignId}`, () => {
          logger.log(`${msg}. Rechecking in ~${Math.round(delayMs / 60000)} minutes.`);
        });
        sb.setClientStatusMessage(clientId, msg).catch(() => {});
        sendJobStatus = 'retry';
        sendJobUpdates = {
          available_at: new Date(Date.now() + delayMs).toISOString(),
          last_error_class: 'daily_limit',
          last_error_message: msg,
        };
      } else if (!sendResult.ok && sendResult.reason === 'missing_delay_config') {
        const msg = sendResult.statusMessage || 'Campaign missing min/max send delay settings.';
        logger.warn(msg);
        if (typeof sb.pauseCampaignMissingSendDelayConfig === 'function') {
          await sb.pauseCampaignMissingSendDelayConfig(clientId, work.campaignId, msg).catch(() => {});
        }
        sendJobStatus = 'cancelled';
        sendJobUpdates = {
          finished_at: new Date().toISOString(),
          last_error_class: 'missing_delay_config',
          last_error_message: msg.slice(0, 500),
        };
      } else if (!sendResult.ok && sendResult.reason === 'already_sent') {
        // Duplicate lead for this client was already messaged earlier; do not consume campaign cooldown.
        delayMs = randomDelay(200, 900);
        sendJobStatus = 'completed';
        sendJobUpdates = {
          last_error_class: 'already_sent',
          last_error_message: 'already_sent',
        };
      } else if (!sendResult.ok && sendResult.reason === 'session_logged_out') {
        delayMs = randomDelay(30, 90) * 1000;
        sendJobStatus = 'retry';
        sendJobUpdates = {
          available_at: new Date(Date.now() + delayMs).toISOString(),
          last_error_class: 'session_logged_out',
          last_error_message: sendResult.statusMessage || 'session_logged_out',
        };
        logger.error(
          `[send-worker] Instagram session logged out — pausing client ${clientId} (lead @${work.username} not marked failed).`
        );
        await sb.setControl(clientId, 1).catch(() => {});
        sb.setClientStatusMessage(
          clientId,
          'Instagram logged out — reconnect your sender in Cold Outreach, then sending resumes.'
        ).catch(() => {});
      } else if (!sendResult.ok && sendResult.reason === 'proxy_tunnel_failed') {
        delayMs = randomDelay(15 * 60 * 1000, 30 * 60 * 1000);
        sendJobStatus = 'retry';
        sendJobUpdates = {
          available_at: new Date(Date.now() + delayMs).toISOString(),
          last_error_class: 'proxy_tunnel_failed',
          last_error_message: sendResult.statusMessage || 'proxy_tunnel_failed',
        };
        logger.error(
          `[send-worker] Proxy/network failure — pausing client ${clientId} (lead @${work.username} not marked failed).`
        );
        await sb.setControl(clientId, 1).catch(() => {});
        sb.setClientStatusMessage(
          clientId,
          sendResult.statusMessage ||
            'Instagram unreachable (proxy/VPN tunnel failed). Sending paused — fix connectivity and press Start.'
        ).catch(() => {});
      } else {
        delayMs = sendResult.cooldownMs != null ? sendResult.cooldownMs : randomDelay(work.minDelaySec * 1000, work.maxDelaySec * 1000);
        const delaySec = Math.max(1, Math.ceil(delayMs / 1000));
        const minSec = Math.max(0, Number(work.minDelaySec) || 0);
        const maxSec = Math.max(0, Number(work.maxDelaySec) || 0);
        logger.log(`Campaign cooldown from settings ${minSec}-${maxSec}s. Next send in ${delaySec} sec.`);
        const cooldownStatus = sendResult.ok
          ? `Campaign cooldown: last DM sent. Next send in ~${delaySec}s (random delay between ${minSec}s and ${maxSec}s from campaign settings).`
          : `Campaign cooldown: spacing sends ~${delaySec}s (${minSec}s–${maxSec}s from campaign settings) before retry.`;
        sb.setClientStatusMessage(clientId, cooldownStatus).catch(() => {});
        if (!sendResult.ok) {
          sendJobStatus = 'failed';
          sendJobUpdates = {
            last_error_class: sendResult.reason || 'send_failed',
            last_error_message: sendResult.reason || 'send_failed',
          };
        }
      }
      await sb.updateSendJob(claimedJob.id, { status: sendJobStatus, ...sendJobUpdates }, SEND_WORKER_ID).catch(() => {});

      // ── Cooldown / limits: stamp available_at on remaining campaign jobs instead of blocking sleep ──
      // This lets this worker immediately claim work from OTHER clients while the campaign waits.
      const limitWholeCampaignDefer =
        sendJobStatus === 'retry' &&
        work?.campaignId &&
        sendJobUpdates.available_at &&
        (sendJobUpdates.last_error_class === 'daily_limit' ||
          sendJobUpdates.last_error_class === 'hourly_limit' ||
          sendJobUpdates.last_error_class === 'proxy_tunnel_failed');
      if (limitWholeCampaignDefer) {
        await sb.deferCampaignPendingJobs(work.campaignId, claimedJob.id, sendJobUpdates.available_at).catch(() => {});
        await delay(500);
      } else {
        const isSendCooldown = sendJobStatus === 'completed' || sendJobStatus === 'failed';
        if (isSendCooldown && delayMs > 1000 && work && work.campaignId) {
          const cooldownUntilIso = new Date(Date.now() + delayMs).toISOString();
          await sb.deferCampaignPendingJobs(work.campaignId, claimedJob.id, cooldownUntilIso).catch(() => {});
          await delay(500);
        } else {
          await delay(Math.min(delayMs, 1500));
        }
      }
    } catch (e) {
      logger.error(`Unexpected send loop error for client ${clientId} campaign ${work.campaignId}: ${e.message}`, e);
      await sb.updateCampaignLeadStatus(work.campaignLeadId, 'failed', null, SEND_WORKER_ID).catch(() => {});
      await sb.updateSendJob(claimedJob.id, {
        status: 'failed',
        last_error_class: 'worker_exception',
        last_error_message: e.message || 'worker_exception',
      }, SEND_WORKER_ID).catch(() => {});
      await delay(randomDelay(2000, 5000));
    } finally {
      stopLeaseHeartbeat();
      stopSendJobHeartbeat();
      stopCampaignLeaseHeartbeat();
      await sb.releaseInstagramSessionLease(session.id, SEND_WORKER_ID).catch(() => {});
      leasedSessionIdForSignal = null;
      await sb.releaseCampaignSendLease(work.campaignId, SEND_WORKER_ID).catch(() => {});
      leasedCampaignIdForSignal = null;
    }
  }
}

async function runBot() {
  const useSupabase = sb.isSupabaseConfigured();

  if (useSupabase) {
    const pm2Name = String(process.env.name || '').trim();
    if (pm2Name === 'ig-dm-bot') {
      logger.error(
        '[send-worker] PM2 app name "ig-dm-bot" is deprecated: it duplicates ig-dm-send and causes Chrome ' +
          '"browser is already running" on the same profile. Run: pm2 delete ig-dm-bot && pm2 save — then use ' +
          'ecosystem.config.cjs (ig-dm-send only). See README.md.'
      );
      process.exit(1);
    }
    await runBotMultiTenant();
    return;
  }

  const clientId = sb.getClientId();
  let leads;
  let adapter;
  let minDelayMs = MIN_DELAY_MS;
  let maxDelayMs = MAX_DELAY_MS;

  const csvPath = process.env.LEADS_CSV || 'leads.csv';
  try {
    leads = await loadLeadsFromCSV(csvPath);
  } catch (e) {
    logger.error('Failed to load leads', e);
    throw e;
  }
  leads = leads.filter((u) => !alreadySent(u));
  adapter = {
    dailyLimit: DAILY_LIMIT,
    maxPerHour: MAX_PER_HOUR,
    alreadySent: (u) => alreadySent(u),
    logSentMessage: (u, msg, status, _campaignId) => logSentMessage(u, msg, status),
    getDailyStats: () => getDailyStats(),
    getHourlySent: () => getHourlySent(),
    getControl: () => getControl('pause'),
    setControl: (v) => setControl('pause', v),
    getRandomMessage: () => getRandomMessage(),
  };

  if (leads.length === 0) {
    logger.log('No leads to send. Done.');
    return;
  }

  logger.log('Starting sender loop (legacy CSV mode).');

  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  const launchOpts = {
    headless: HEADLESS,
    args: [...baseChromeArgs(), '--autoplay-policy=no-user-gesture-required'],
  };
  appendChromeFakeMicArgs(launchOpts.args);
  applyPuppeteerSlowMo(launchOpts);
  applyHeadedChromeWindowToLaunchOpts(launchOpts);
  if (launchOpts.slowMo) logger.log(`Puppeteer slowMo=${launchOpts.slowMo}ms (PUPPETEER_SLOW_MO_MS)`);
  const useSessionCookies = false;
  if (!useSessionCookies) {
    try {
      if (!fs.existsSync(BROWSER_PROFILE_DIR)) fs.mkdirSync(BROWSER_PROFILE_DIR, { recursive: true });
      launchOpts.userDataDir = BROWSER_PROFILE_DIR;
    } catch (e) {
      logger.log('Browser profile dir not used', e.message);
    }
  }
  let browser;
  try {
    browser = await puppeteer.launch(launchOpts);
  } catch (e) {
    if (launchOpts.userDataDir) {
      logger.log('Launch with profile failed, retrying without', e.message);
      delete launchOpts.userDataDir;
      browser = await puppeteer.launch(launchOpts);
    } else throw e;
  }

  let page;
  let currentSessionId = null;
  const campaignRoundRobin = new Map();

  async function ensurePageSession(page, session) {
    const cookies = session?.session_data?.cookies;
    if (!cookies?.length) return false;
    if (currentSessionId === session.id) return true;
    const sessionLabel = session.instagram_username || session.id;
    try {
      await clearInstagramCookiesOnlyOnPage(page);
      await page.setCookie(...cookies);
      let gotoTimedOut = false;
      try {
        await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 45000 });
      } catch (e) {
        gotoTimedOut = e && e.name === 'TimeoutError';
        logger.warn(
          `Session switch navigation ${gotoTimedOut ? 'timed out' : 'failed'} for ${sessionLabel}: ${e.message}. Verifying current page before failing.`
        );
      }
      await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
      await delay(3000);
      const reauthLegacy = await detectInstagramPasswordReauthScreen(page).catch(() => false);
      if (page.url().includes('/accounts/login') || reauthLegacy) {
        logger.error(
          `Instagram session expired or security screen for account ${sessionLabel}` +
            (reauthLegacy ? ' (password / challenge UI)' : '')
        );
        if (session?.id) await sb.markInstagramSessionWebNeedsRefresh(session.id).catch(() => {});
        return false;
      }
      currentSessionId = session.id;
      return true;
    } catch (e) {
      if (e && e.name === 'TimeoutError') {
        try {
          await delay(2000);
          const reauthT = await detectInstagramPasswordReauthScreen(page).catch(() => false);
          if (!page.url().includes('/accounts/login') && !reauthT) {
            logger.warn(`Session switch timeout for ${sessionLabel} but page is not login; continuing.`);
            currentSessionId = session.id;
            return true;
          }
          if (session?.id) await sb.markInstagramSessionWebNeedsRefresh(session.id).catch(() => {});
        } catch {}
      }
      logger.error('Failed to switch session: ' + e.message);
      return false;
    }
  }

  try {
    page = await browser.newPage();
    await grantMicrophoneForInstagram(page, logger);
    if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);
    if (useSessionCookies) {
      const session = await sb.getSession(clientId);
      const cookies = session?.session_data?.cookies;
      if (cookies && cookies.length) {
        await page.setCookie(...cookies);
        if (session.id) currentSessionId = session.id;
      }
      await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
      if (session?.session_data) await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
      await delay(2000);
      const url = page.url();
      if (url.includes('/accounts/login')) {
        throw new Error('Instagram session expired. Reconnect from Cold Outreach.');
      }
      logger.log('Using session from Supabase.');
    } else {
      await login(page);
    }
  } catch (err) {
    logger.error('Setup failed', err);
    if (browser) await browser.close().catch(() => {});
    throw err;
  }

  let index = 0;
  const runOne = async () => {
    const pause = await Promise.resolve(adapter.getControl());
    if (pause === '1' || pause === 1) {
      logger.log('Bot paused via control flag. Rechecking in 30s.');
      setTimeout(runOne, 30000);
      return;
    }
    if (index >= leads.length) {
      logger.log('All leads processed.');
      await browser.close();
      process.exit(0);
    }
    const work = { type: 'lead', username: leads[index] };
    const options = {
      voice_note_path: VOICE_NOTE_FILE || null,
      voice_note_mode: VOICE_NOTE_MODE || 'after_text',
    };

    // NEW: For voice notes, close browser, convert audio, relaunch (userDataDir preserves session).
    const needsVoice = (options.voice_note_path || '').trim() !== '';
    if (needsVoice && browser) {
      let resolved = null;
      try {
        resolved = await resolveVoiceNotePath(options.voice_note_path);
        if (resolved.localPath) {
          const conv = convertToChromeFakeMicWav(resolved.localPath, logger);
          options.voiceDurationSec = conv.durationSec;
          await browser.close().catch(() => {});
          browser = await puppeteer.launch(launchOpts);
          page = await browser.newPage();
          await grantMicrophoneForInstagram(page, logger);
          await applyDesktopEmulation(page);
          await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
          await delay(2000);
          if (page.url().includes('/accounts/login')) {
            throw new Error('Instagram session expired after voice restart.');
          }
        }
      } catch (e) {
        logger.warn('Voice browser restart failed: ' + (e.message || e));
      } finally {
        if (resolved) await resolved.cleanup().catch(() => {});
      }
    }

    const result = await sendDM(page, work.username, adapter, options);
    if (result.ok) index += 1;

    const delayMs =
      work.type === 'campaign' && work.minDelaySec != null && work.maxDelaySec != null
        ? randomDelay(work.minDelaySec * 1000, work.maxDelaySec * 1000)
        : randomDelay(minDelayMs, maxDelayMs);
    logger.log(`Local next send in ${Math.round(delayMs / 60000)} minutes.`);
    await delay(delayMs);
    setImmediate(runOne);
  };

  await Promise.resolve(adapter.setControl(0));

  const scheduleNext = () => {
    const initialDelay = randomDelay(5 * 1000, 60 * 1000);
    logger.log(`First send in ${Math.round(initialDelay / 1000)} seconds.`);
    setTimeout(runOne, initialDelay);
  };

  scheduleNext();
}

/**
 * One-time connect: log in with given credentials and return session (cookies).
 * If the account has 2FA and no code is provided, returns { twoFactorRequired: true, page, browser, username }
 * so the server can keep the session and the user can submit the code to POST /api/instagram/connect/2fa.
 */
async function connectInstagram(instagramUsername, instagramPassword, twoFactorCode = null, options = {}) {
  const proxyUrl = options && options.proxyUrl;
  const enableMobile = process.env.ENABLE_MOBILE_LOGIN === '1' || process.env.ENABLE_MOBILE_LOGIN === 'true';
  const disableMobile = process.env.DISABLE_MOBILE_LOGIN === '1' || process.env.DISABLE_MOBILE_LOGIN === 'true';
  const useMobile = enableMobile && !disableMobile;
  if (useMobile) logger.log('Using mobile view for login (ENABLE_MOBILE_LOGIN is set).');
  else logger.log('Using desktop view for login (default for login stability).');
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  const connectLaunch = {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
      '--lang=en-US',
    ],
  };
  appendChromeFakeMicArgs(connectLaunch.args);
  applyPuppeteerSlowMo(connectLaunch);
  applyProxyToLaunchOptions(connectLaunch, proxyUrl);
  const browser = await puppeteer.launch(connectLaunch);
  let keepBrowserOpen = false;
  let connectContext = null;
  try {
    if (typeof browser.createIncognitoBrowserContext === 'function') {
      connectContext = await browser.createIncognitoBrowserContext();
    } else if (typeof browser.createBrowserContext === 'function') {
      connectContext = await browser.createBrowserContext();
    }
    const page = connectContext ? await connectContext.newPage() : await browser.newPage();
    await authenticatePageForProxy(page, proxyUrl);
    if (useMobile && !VOICE_NOTE_FILE) await applyMobileEmulation(page);
    else await applyDesktopEmulation(page);
    await applyConnectFingerprint(page);
    await login(page, {
      username: instagramUsername,
      password: instagramPassword,
      twoFactorCode: twoFactorCode || undefined,
    });
    const webStorageCap = await navigateAndCaptureInstagramWebStorage(page, logger).catch(() => null);
    const cookies = await page.cookies();
    return { cookies, web_storage: webStorageCap || undefined, username: instagramUsername };
  } catch (e) {
    if (e.code === 'TWO_FACTOR_REQUIRED' && e.page) {
      keepBrowserOpen = true;
      return { twoFactorRequired: true, page: e.page, browser, username: instagramUsername };
    }
    if (e.code === 'EMAIL_VERIFICATION_REQUIRED' && e.page) {
      keepBrowserOpen = true;
      return {
        emailVerificationRequired: true,
        page: e.page,
        browser,
        username: instagramUsername,
        maskedEmail: e.maskedEmail || null,
      };
    }
    throw e;
  } finally {
    if (!keepBrowserOpen && connectContext) {
      await connectContext.close().catch(() => {});
    }
    if (!keepBrowserOpen) await browser.close().catch(() => {});
  }
}

/**
 * If Instagram is showing "choose 2FA method", click through to the code entry screen.
 * Prioritises WhatsApp, then authentication app.
 */
async function ensure2FACodeEntryPage(page) {
  const clicked = await page.evaluate(function () {
    const body = (document.body && document.body.innerText) || '';
    const hasCodeInput = document.querySelectorAll('input[type="text"]:not([type="hidden"]), input[type="tel"], input:not([type="hidden"]):not([type="password"])').length >= 1;
    if (hasCodeInput && (body.includes('Security Code') || body.includes('6-digit') || body.includes('Enter'))) return false;
    const clickables = Array.from(document.querySelectorAll('button, div[role="button"], a, span[role="button"], [role="button"]'));
    const lower = (el) => (el.textContent || '').toLowerCase().trim();
    const whatsApp = clickables.find((el) => {
      const t = lower(el);
      return t.includes('whatsapp') || (t.includes('send') && t.includes('whatsapp')) || (t.includes('get code') && t.includes('whatsapp'));
    });
    if (whatsApp && whatsApp.offsetParent) { whatsApp.scrollIntoView({ block: 'center' }); whatsApp.click(); return true; }
    const app = clickables.find((el) => {
      const t = lower(el);
      return t.includes('authentication app') || t.includes('authenticator') || (t.includes('app') && t.length < 25);
    });
    if (app && app.offsetParent) { app.scrollIntoView({ block: 'center' }); app.click(); return true; }
    const sms = clickables.find((el) => lower(el).includes('text message') || lower(el).includes('sms'));
    if (sms && sms.offsetParent) { sms.scrollIntoView({ block: 'center' }); sms.click(); return true; }
    return false;
  });
  if (clicked) {
    logger.log('Clicked 2FA method (WhatsApp preferred), waiting for code entry...');
    await delay(3000);
  }
}

/**
 * Complete 2FA on an existing page (same browser session as the login that hit 2FA).
 * Enters the code via keyboard (so React/Instagram registers it), clicks Confirm, waits for redirect, dismisses dialogs, returns cookies.
 */
async function completeInstagram2FA(page, browser, twoFactorCode, instagramUsername) {
  const code = String(twoFactorCode).replace(/\D/g, '').slice(0, 6);
  if (!code) throw new Error('Invalid 2FA code.');
  if (page.url().includes('/accounts/login/two_factor')) await ensure2FACodeEntryPage(page);
  logger.log('Entering 2FA code on existing session...');
  const inputCount = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll('input'));
    const visible = inputs.filter((el) => el.offsetParent != null && el.type !== 'hidden' && el.type !== 'password');
    return visible.length;
  });
  const isSixBoxes = inputCount === 6;
  if (isSixBoxes) {
    for (let i = 0; i < 6; i++) {
      const digit = code[i] || '';
      await page.evaluate((index) => {
        const inputs = Array.from(document.querySelectorAll('input'));
        const visible = inputs.filter((el) => el.offsetParent != null && el.type !== 'hidden' && el.type !== 'password');
        const el = visible[index];
        if (el) { el.focus(); el.click(); }
      }, i);
      await delay(80);
      if (digit) await page.keyboard.type(digit, { delay: 50 });
      await delay(60);
    }
  } else {
    const codeInputFocused = await page.evaluate(() => {
      const inputs = Array.from(document.querySelectorAll('input'));
      const visible = inputs.filter((el) => el.offsetParent != null && el.type !== 'hidden');
      const codeInput = visible.find((el) => {
        const p = (el.placeholder || '').toLowerCase();
        const a = (el.getAttribute('aria-label') || '').toLowerCase();
        return p.includes('code') || p.includes('security') || a.includes('code') || a.includes('security') || (el.type !== 'password' && el.type !== 'email');
      }) || visible[0];
      if (!codeInput) return false;
      codeInput.focus();
      codeInput.click();
      return true;
    });
    if (!codeInputFocused) throw new Error('Two-factor code input not found.');
    await delay(300);
    await page.keyboard.type(code, { delay: 80 + Math.floor(Math.random() * 40) });
  }
  await delay(500 + Math.floor(Math.random() * 1000));
  const confirmClicked = await page.evaluate(function () {
    const labels = ['Confirm', 'Next', 'Submit'];
    const buttons = Array.from(document.querySelectorAll('button, div[role="button"], input[type="submit"]'));
    for (const label of labels) {
      const btn = buttons.find((el) => (el.textContent || el.value || '').trim() === label);
      if (btn && btn.offsetParent) { btn.scrollIntoView({ block: 'center' }); btn.click(); return true; }
    }
    const confirmLike = buttons.find((el) => /confirm|next|submit/i.test((el.textContent || el.value || '').trim()));
    if (confirmLike && confirmLike.offsetParent) { confirmLike.click(); return true; }
    return false;
  });
  if (confirmClicked) {
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 20000 }).catch(() => {});
    await delay(3000);
  }
  if (page.url().includes('/accounts/login/two_factor')) {
    await delay(2000);
    if (page.url().includes('/accounts/login/two_factor')) {
      throw new Error('Two-factor code may be wrong or expired. Try again with a fresh code.');
    }
  }
  for (let i = 0; i < 3; i++) {
    const dismissed = await page.evaluate(function () {
      const dialogs = document.querySelectorAll('[role="dialog"], [role="alertdialog"]');
      for (let d = 0; d < dialogs.length; d++) {
        const txt = (dialogs[d].textContent || '').toLowerCase();
        if (txt.indexOf('save your login') !== -1 || txt.indexOf('not now') !== -1 || txt.indexOf('turn on notifications') !== -1) {
          const notNow = Array.from(dialogs[d].querySelectorAll('span, button, div[role="button"]')).find(function (el) {
            return (el.textContent || '').trim().toLowerCase() === 'not now';
          });
          if (notNow) {
            const btn = notNow.closest('[role="button"]') || notNow.closest('button') || notNow;
            if (btn) { btn.click(); return true; }
          }
        }
      }
      return false;
    });
    if (dismissed) await delay(2000);
    else break;
  }
  await assertHealthyInstagramSessionOrThrow(page, '2FA');
  const webStorageCap = await navigateAndCaptureInstagramWebStorage(page, logger).catch(() => null);
  const cookies = await page.cookies();
  await browser.close().catch(() => {});
  logger.log('2FA completed, session saved.');
  return { cookies, web_storage: webStorageCap || undefined, username: instagramUsername };
}

/**
 * Complete Instagram email-code verification on an existing session.
 */
async function completeInstagramEmailVerification(page, browser, emailCode, instagramUsername) {
  const code = String(emailCode || '').replace(/\s+/g, '').slice(0, 12);
  if (!code) throw new Error('Invalid email verification code.');
  logger.log('Entering email verification code on existing session...');
  const focused = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll('input'));
    const visible = inputs.filter((el) => el.offsetParent != null && el.type !== 'hidden' && !el.disabled);
    const codeInput =
      visible.find((el) => {
        const p = (el.placeholder || '').toLowerCase();
        const a = (el.getAttribute('aria-label') || '').toLowerCase();
        return p.includes('code') || a.includes('code') || el.type === 'tel';
      }) || visible[0];
    if (!codeInput) return false;
    codeInput.focus();
    codeInput.click();
    codeInput.value = '';
    return true;
  });
  if (!focused) throw new Error('Email verification code input not found.');
  await delay(250);
  await page.keyboard.type(code, { delay: 70 + Math.floor(Math.random() * 30) });
  await delay(500);
  const continueClicked = await page.evaluate(() => {
    const labels = ['Continue', 'Next', 'Submit', 'Confirm'];
    const buttons = Array.from(document.querySelectorAll('button, div[role="button"], input[type="submit"]'));
    for (const label of labels) {
      const btn = buttons.find((el) => (el.textContent || el.value || '').trim() === label);
      if (btn && btn.offsetParent) {
        btn.scrollIntoView({ block: 'center' });
        btn.click();
        return true;
      }
    }
    const generic = buttons.find((el) => /continue|next|submit|confirm/i.test((el.textContent || el.value || '').trim()));
    if (generic && generic.offsetParent) {
      generic.click();
      return true;
    }
    return false;
  });
  if (continueClicked) {
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 20000 }).catch(() => {});
    await delay(2500);
  }
  const state = await detectInstagramEmailVerificationState(page);
  if (state.required) {
    throw new Error('Email verification code may be wrong or expired. Try again with a fresh code.');
  }
  await assertHealthyInstagramSessionOrThrow(page, 'email verification');
  const webStorageCap = await navigateAndCaptureInstagramWebStorage(page, logger).catch(() => null);
  const cookies = await page.cookies();
  await browser.close().catch(() => {});
  logger.log('Email verification completed, session saved.');
  return { cookies, web_storage: webStorageCap || undefined, username: instagramUsername };
}

module.exports = {
  runBot,
  getDailyStats,
  loadLeadsFromCSV,
  sendDM,
  evaluateCampaignLimitState,
  sendFollowUp,
  login,
  connectInstagram,
  completeInstagram2FA,
  completeInstagramEmailVerification,
  scheduleDebugFollowUpBrowser,
  previewDmLeadNamesFromSession,
};
