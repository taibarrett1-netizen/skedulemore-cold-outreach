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
const { substituteVariables, normalizeName } = require('./utils/message-variables');
const { isFfmpegAvailable, convertToChromeFakeMicWav, ensureChromeFakeMicPlaceholder } = require('./utils/voice-note-audio');
const { appendChromeFakeMicArgs, CHROME_FAKE_MIC_WAV } = require('./utils/chrome-fake-mic');
const {
  sendVoiceNoteInThread,
  prepareVoiceNoteUi,
  grantMicrophoneForInstagram,
  VOICE_NOTE_STRICT_VERIFY,
} = require('./utils/instagram-voice-note');
const { dismissInstagramHomeModals } = require('./utils/instagram-modals');
const { navigateToDmThread, sendPlainTextInThread } = require('./utils/open-dm-thread');
puppeteer.use(StealthPlugin());

const DAILY_LIMIT = Math.min(parseInt(process.env.DAILY_SEND_LIMIT, 10) || 100, 200);
const MIN_DELAY_MS = (parseInt(process.env.MIN_DELAY_MINUTES, 10) || 5) * 60 * 1000;
const MAX_DELAY_MS = (parseInt(process.env.MAX_DELAY_MINUTES, 10) || 30) * 60 * 1000;
const MAX_PER_HOUR = parseInt(process.env.MAX_SENDS_PER_HOUR, 10) || 20;
const HEADLESS = process.env.HEADLESS_MODE !== 'false';
/** When set (e.g. 80), slows Puppeteer operations for debugging voice/UI (all launch paths that use applyPuppeteerSlowMo). */
function getPuppeteerSlowMo() {
  const n = parseInt(process.env.PUPPETEER_SLOW_MO_MS, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}
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
  if (HEADLESS || !launchOpts || !Array.isArray(launchOpts.args)) return;
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

const VOICE_NOTE_SOURCE_NAME = (process.env.VOICE_NOTE_SOURCE_NAME || 'ColdDMsVoice').trim();
const BROWSER_PROFILE_DIR = path.join(process.cwd(), '.browser-profile');
const VOICE_NOTE_FILE = (process.env.VOICE_NOTE_FILE || '').trim();
const VOICE_NOTE_MODE = (process.env.VOICE_NOTE_MODE || 'after_text').trim().toLowerCase();

function wantsVoiceNotes(sendOpts = {}) {
  return !!((sendOpts.voiceNotePath || '').trim());
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
  const res = await fetch(p);
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
        logger.warn('cold-dm-on-send 404: Edge Function not deployed. Deploy "cold-dm-on-send" in your Supabase project so the dashboard can create cold-outreach conversations and match GHL contacts. See COLD_DM_HANDOFF.md §2a.');
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
  const row = db.prepare('SELECT COUNT(*) as c FROM sent_messages WHERE sent_at >= ?').get(oneHourAgo);
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

async function login(page, credentials) {
  const username = credentials?.username ?? readEnvFromFile().INSTAGRAM_USERNAME ?? process.env.INSTAGRAM_USERNAME;
  const password = credentials?.password ?? readEnvFromFile().INSTAGRAM_PASSWORD ?? process.env.INSTAGRAM_PASSWORD;
  if (!username || !password) {
    throw new Error('Missing INSTAGRAM_USERNAME or INSTAGRAM_PASSWORD. Add them in the dashboard Settings and save.');
  }

  logger.log('Loading Instagram login page...');
  await page.goto('https://www.instagram.com/accounts/login/', { waitUntil: 'networkidle2', timeout: 45000 });
  const afterGotoUrl = page.url();
  const afterGotoTitle = await page.title().catch(() => '');
  logger.log(`After load: URL=${afterGotoUrl} title=${afterGotoTitle}`);
  await delay(3000);
  const currentUrl = page.url();
  if (!currentUrl.includes('/accounts/login')) {
    logger.log('Already logged in (session restored).');
    return;
  }

  // Instagram changes input attributes; find by type and order: first visible text input = username, first password = password
  const inputs = await page.$$('input');
  let userEl = null;
  let passEl = null;
  for (const el of inputs) {
    const props = await el.evaluate((node) => ({
      type: node.type,
      visible: node.offsetParent !== null,
    }));
    if (props.visible && (props.type === 'text' || props.type === 'email' || props.type === '')) {
      if (!userEl) userEl = el;
    } else if (props.visible && props.type === 'password') {
      passEl = el;
      break;
    }
  }
  if (!userEl || !passEl) {
    inputs.forEach((el) => el.dispose());
    const failUrl = page.url();
    const failTitle = await page.title().catch(() => '');
    const bodyText = await page.evaluate(() => document.body ? document.body.innerText.slice(0, 500) : '').catch(() => '');
    logger.error('Login form fields not found');
    logger.log(`Page at failure: URL=${failUrl} title=${failTitle}`);
    logger.log(`Page body snippet: ${bodyText.replace(/\n/g, ' ').slice(0, 300)}`);
    throw new Error('Login form fields not found. Instagram may have changed the page.');
  }
  for (const el of inputs) {
    if (el !== userEl && el !== passEl) el.dispose();
  }
  const LOGIN_DEBUG = process.env.LOGIN_DEBUG === '1' || process.env.LOGIN_DEBUG === 'true';
  const loginResponses = [];
  const allInstagramRequests = [];
  const respHandler = async (response) => {
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
  await userEl.click();
  await userEl.type(username, { delay: 80 + Math.floor(Math.random() * 60) });
  await userEl.dispose();
  await humanDelay();
  await passEl.click();
  await passEl.type(password, { delay: 80 + Math.floor(Math.random() * 60) });
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
        "//div[@role='button'][contains(., 'Log in') and not(contains(., 'Log into'))]"
      ];
      for (var i = 0; i < xpaths.length; i++) {
        var r = document.evaluate(xpaths[i], document, null, 9, null);
        var el = r.singleNodeValue;
        if (el && el.offsetParent) { el.scrollIntoView({ block: 'center' }); el.click(); return true; }
      }
      return false;
    });
    if (clicked) submitMethod = submitStyle === 'enterthenclick' ? 'enterKeyThenClick' : 'click';
  }

  if (LOGIN_DEBUG) logger.log('[LOGIN_DEBUG] submitMethod=' + submitMethod);

  logger.log('Submitted login form, waiting for redirect...');
  await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 10000 }).catch(() => {});

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

  if (page.url().includes('/accounts/login')) {
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
      const retryDeadline = Date.now() + 20000;
      while (Date.now() < retryDeadline) {
        await delay(pollIntervalMs);
        if (!page.url().includes('/accounts/login')) break;
      }
      await delay(1500);
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
    const bodySnippet = await page.evaluate(() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 600)).catch(() => '');
    let hint = '';
    const lower = bodySnippet.toLowerCase();
    if (lower.indexOf('password was incorrect') !== -1 || lower.indexOf('incorrect password') !== -1) hint = ' Wrong password.';
    else if (lower.indexOf('username you entered') !== -1 || lower.indexOf("doesn't belong to an account") !== -1) hint = ' Username not found.';
    else if (lower.indexOf('challenge') !== -1 || lower.indexOf('suspicious') !== -1 || lower.indexOf('verify') !== -1 || lower.indexOf('confirm it\'s you') !== -1 || lower.indexOf('security code') !== -1) hint = ' Instagram may require manual verification. Log in once in a normal browser (Chrome/Firefox), complete any challenge, then try again here.';
    else if (lower.indexOf('try again later') !== -1 || lower.indexOf('too many requests') !== -1) hint = ' Rate limited. Try again in 30–60 minutes.';
    else hint = ' If your password is correct, log in once in a normal browser to clear any security check, then retry.';
    logger.error('Login failed. submitMethod=' + submitMethod + ' url=' + urlAfterLogin);
    logger.error('Login API responses (count=' + loginResponses.length + '): ' + (loginResponses.length ? JSON.stringify(loginResponses.slice(-5)) : 'none captured'));
    if (allInstagramRequests.length) logger.error('Recent Instagram requests: ' + JSON.stringify(allInstagramRequests.slice(-10)));
    logger.error('Login failed. Page snippet: ' + bodySnippet.replace(/\n/g, ' ').slice(0, 400));
    throw new Error('Login may have failed; still on login page. Check credentials.' + hint);
  }
  logger.log('Logged in to Instagram.');
}

const MAX_SEND_RETRIES = 3;

async function sendDMOnce(page, u, messageTemplate, nameFallback = {}, sendOpts = {}) {
  const voiceCfg = buildVoiceSendConfig(sendOpts);
  if (wantsVoiceNotes(voiceCfg) && !isFfmpegAvailable()) {
    return { ok: false, reason: 'ffmpeg_missing', pageSnippet: 'Install ffmpeg on the VPS: sudo apt install ffmpeg' };
  }
  if (wantsVoiceNotes(voiceCfg)) await applyDesktopEmulation(page);
  await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 });
  await humanDelay();

  // Instagram sometimes shows "Not now"/notifications prompts even after login.
  // If an overlay is present, our search element may not be "visible" yet, so we retry a bit.
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
            if (btn) {
              btn.click();
              return true;
            }
          }
        }
      }
      return false;
    });
    if (dismissed) {
      logger.log('Dismissed direct/new prompt');
      await delay(1500);
    } else {
      break;
    }
  }

  // Wait for the direct/new search UI to render (may be an input, textarea, or contenteditable element).
  await page
    .waitForFunction(
      () => {
        const els = document.querySelectorAll('input, textarea, [contenteditable="true"]');
        return Array.from(els).some((el) => {
          try {
            if (!el || el.disabled) return false;
            return (el.getClientRects && el.getClientRects().length > 0) || el.offsetParent !== null;
          } catch {
            return false;
          }
        });
      },
      { timeout: 8000 }
    )
    .catch(() => {});

  const searchHandle = await page.evaluateHandle(() => {
    const normalize = (s) => (s || '').toString().toLowerCase();
    const candidates = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]')).filter((el) => {
      try {
        if (!el || el.disabled) return false;
        if (el.type === 'hidden') return false;
        if (el.getClientRects && el.getClientRects().length > 0) return true;
        if (el.offsetParent !== null) return true;
        return false;
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
    throw new Error(`Search input not found on direct/new page (url=${diag?.url || 'unknown'} visible=${diag?.visibleCount ?? 'n/a'})`);
  }

  const searchMeta = await page.evaluate((el) => ({ tag: el.tagName, type: el.type || '', isCE: !!el.isContentEditable }), searchEl).catch(() => ({}));
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

  const userClicked = await page.evaluate((username) => {
    const needle = username.toLowerCase().replace(/^@/, '');
    const buttons = Array.from(document.querySelectorAll('div[role="button"]'));
    const userBtn = buttons.find((b) => {
      const t = (b.textContent || '').toLowerCase();
      return t.includes(needle) && !t.includes('more accounts');
    });
    if (userBtn) {
      userBtn.click();
      return true;
    }
    if (buttons.length) buttons[0].click();
    return false;
  }, u);
  if (!userClicked) {
    const { hint, pageSnippet, searchPreview } = await page.evaluate(() => {
      const body = (document.body && document.body.innerText) ? document.body.innerText : '';
      const lower = body.toLowerCase();
      let hint = 'user_not_found';
      if (lower.includes('this account is private') || lower.includes('account is private') || lower.includes('private account')) hint = 'account_private';
      else if (lower.includes("couldn't find") || lower.includes('could not find') || lower.includes('no results') || lower.includes('no users found')) hint = 'user_not_found';
      else if (lower.includes('try again later') || lower.includes('too many')) hint = 'rate_limited';
      const snippet = body.replace(/\s+/g, ' ').trim().slice(0, 120);
      const buttons = Array.from(document.querySelectorAll('div[role="button"]')).filter((b) => !(b.textContent || '').toLowerCase().includes('more accounts'));
      const preview = buttons.slice(0, 4).map((b) => (b.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40)).filter(Boolean);
      return { hint, pageSnippet: snippet || '(empty)', searchPreview: preview.length ? preview.join(' | ') : '' };
    }).catch(() => ({ hint: 'user_not_found', pageSnippet: '(unable to read page)', searchPreview: '' }));
    const extra = searchPreview ? ' First results: ' + searchPreview : '';
    return { ok: false, reason: hint, pageSnippet: (pageSnippet || '') + extra };
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

  const composeDiagnostic = () =>
    page.evaluate(() => {
      const textareas = document.querySelectorAll('textarea');
      const editables = document.querySelectorAll('div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"]');
      const roleBoxes = document.querySelectorAll('[role="textbox"]');
      const visible = (el) => el.offsetParent !== null;
      return {
        url: window.location.href,
        textarea: textareas.length,
        textareaVisible: Array.from(textareas).filter(visible).length,
        contenteditable: editables.length,
        contenteditableVisible: Array.from(editables).filter(visible).length,
        roleTextbox: roleBoxes.length,
        roleTextboxVisible: Array.from(roleBoxes).filter(visible).length,
        bodySnippet: document.body ? document.body.innerText.slice(0, 400).replace(/\n/g, ' ') : '',
      };
    });

  const composeSelector = 'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]';
  logger.log('Waiting for compose area...');
  let composeFound = false;
  let noComposeReason = 'no_compose';
  try {
    await page.waitForSelector(composeSelector, { timeout: 20000 });
    composeFound = true;
  } catch (e) {
    const diag = await composeDiagnostic().catch(() => ({}));
    const bodySnippet = (diag.bodySnippet || '').toLowerCase();
    if (bodySnippet.includes('this account is private') || bodySnippet.includes('account is private')) noComposeReason = 'account_private';
    else if (bodySnippet.includes("can't message") || bodySnippet.includes("can't send") || bodySnippet.includes('message request') || bodySnippet.includes("don't accept")) noComposeReason = 'messages_restricted';
    else if (bodySnippet.includes('couldn\'t find') || bodySnippet.includes('no results')) noComposeReason = 'user_not_found';
    logger.warn('Compose wait failed ' + e.message + (noComposeReason !== 'no_compose' ? ' (page suggests: ' + noComposeReason + ')' : ''));
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));
  }

  // When lead has no display_name/first_name in DB but template uses {{first_name}}/{{full_name}}, get name from thread page (e.g. "AI Setter Test 8 aisettertest8")
  const templateUsesName = /\{\{\s*(first_name|full_name)\s*\}\}/i.test(messageTemplate);
  let displayNameForSubst = nameFallback.display_name ?? nameFallback.first_name ?? null;
  if (templateUsesName && !displayNameForSubst && (!nameFallback.display_name || !nameFallback.first_name)) {
    try {
      const extracted = await page.evaluate((username) => {
        const body = document.body ? document.body.innerText : '';
        const needle = username.replace(/^@/, '').toLowerCase();
        const idx = body.toLowerCase().indexOf(needle);
        if (idx > 0) {
          const before = body.slice(0, idx).trim();
          const lines = before.split(/\n/);
          const lastPart = (lines[lines.length - 1] || '').trim();
          const candidate = lastPart.length > 0 && lastPart.length <= 80 && !/^https?:\/\//i.test(lastPart) ? lastPart : (before.length > 0 && before.length <= 80 ? before : null);
          if (candidate && !/^\d+$/.test(candidate)) return candidate;
        }
        return null;
      }, u);
      if (extracted) {
        const firstWord = extracted.trim().split(/\s+/)[0] || '';
        const normalizedFirst = normalizeName(firstWord);
        const blocklist = sendOpts.firstNameBlocklist || new Set();
        if (!normalizedFirst) {
          logger.log(`Display name from thread for @${u} not used: first word normalized to empty`);
        } else if (blocklist.has(normalizedFirst.toLowerCase())) {
          logger.log(`Display name from thread for @${u} not used: first name "${normalizedFirst}" is blocklisted`);
        } else {
          displayNameForSubst = extracted;
          logger.log(`Using display name from thread for @${u}: "${extracted}"`);
        }
      }
    } catch (e) {
      // ignore
    }
  }

  const leadFromPage = {
    username: u,
    first_name: nameFallback.first_name ?? null,
    last_name: nameFallback.last_name ?? null,
    display_name: displayNameForSubst ?? nameFallback.display_name ?? null,
  };
  const msg = substituteVariables(messageTemplate, leadFromPage, {
    firstNameBlocklist: sendOpts.firstNameBlocklist || new Set(),
    onFirstNameEmpty: (reason) => logger.warn(`First name empty for @${u}: ${reason}`),
  });
  const shouldSendText = voiceCfg.mode !== 'voice_only';
  const shouldSendVoice = wantsVoiceNotes(voiceCfg);
  let textSent = false;
  let voiceSent = false;
  let voiceFailure = null;
  const threadId = getInstagramThreadIdFromUrl(page.url());

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
    const diag = await composeDiagnostic().catch(() => ({}));
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));
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
      await compose.click();
      await compose.type(msg, { delay: 60 + Math.floor(Math.random() * 40) });
      await compose.dispose();
      await composeEl.dispose();
      await humanDelay();
      await page.keyboard.press('Enter');
      await delay(1500);
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
    await page.keyboard.type(msg, { delay: 60 + Math.floor(Math.random() * 40) });
    await humanDelay();
    await page.keyboard.press('Enter');
    await delay(1500);
    textSent = true;
    await attemptVoiceSend();
    return {
      ok: true,
      finalMessage: msg,
      instagramThreadId: threadId,
      display_name: leadFromPage.display_name || undefined,
    };
  }

  await attemptVoiceSend();
  if (voiceSent) {
    return {
      ok: true,
      finalMessage: null,
      instagramThreadId: threadId,
      display_name: leadFromPage.display_name || undefined,
    };
  }
  if (voiceFailure) return { ok: false, reason: voiceFailure };

  return { ok: false, reason: noComposeReason || 'no_compose' };
}

function buildFollowUpLaunchOptions() {
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  const opts = {
    headless: HEADLESS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
  };
  appendChromeFakeMicArgs(opts.args);
  applyPuppeteerSlowMo(opts);
  applyHeadedChromeWindowToLaunchOpts(opts);
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

    const launchOpts = buildFollowUpLaunchOptions();
    browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    await grantMicrophoneForInstagram(page, logger);
    await applyDesktopEmulation(page);
    await page.setCookie(...cookies);
    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
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
  if (hasAudio) {
    const resolved = await resolveVoiceNotePath(audioUrlRaw);
    if (!resolved.localPath) {
      return fail('Could not download audio file', 400);
    }
    try {
      const conv = convertToChromeFakeMicWav(resolved.localPath, logger);
      voiceDurationSec = conv.durationSec;
    } catch (e) {
      await resolved.cleanup().catch(() => {});
      return fail(e.message && e.message.includes('convert') ? 'Could not convert audio' : (e.message || 'Audio conversion failed'), 400);
    }
    await resolved.cleanup();
  }

  const launchOpts = buildFollowUpLaunchOptions();
  let browser;
  try {
    browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);
    await page.setCookie(...cookies);
    if (hasAudio) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);

    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(2000);
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
    if (page.url().includes('/accounts/login')) {
      return fail('Instagram session expired', 401);
    }

    const u = normalizeUsername(recipientUsername);
    const nav = await navigateToDmThread(page, u);
    if (!nav.ok) {
      const errMsg = followUpReasonToError(nav.reason, nav.pageSnippet);
      return fail(errMsg, 400);
    }
    if (hasAudio) await grantMicrophoneForInstagram(page, logger);

    if (textSingle) {
      const sent = await sendPlainTextInThread(page, String(body.text).trim());
      if (!sent.ok) {
        return fail(followUpReasonToError(sent.reason), 400);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      return { ok: true };
    }

    if (hasMessages) {
      for (const line of messageLines) {
        const sent = await sendPlainTextInThread(page, line);
        if (!sent.ok) {
          return fail(followUpReasonToError(sent.reason), 400);
        }
        await delay(2000);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      return { ok: true };
    }

    if (hasAudio) {
      if (hasCaption) {
        const cap = await sendPlainTextInThread(page, captionRaw);
        if (!cap.ok) {
          return fail(followUpReasonToError(cap.reason), 400);
        }
        await delay(1200);
      }
      const prep = await prepareVoiceNoteUi(page, { logger });
      if (!prep.ok) {
        return fail(followUpReasonToError(prep.reason || 'voice_mic_not_found'), 400);
      }
      const voiceResult = await sendVoiceNoteInThread(page, {
        logger,
        correlationId,
        voiceSource: { durationSec: voiceDurationSec },
      });
      if (!voiceResult.ok) {
        return fail(followUpReasonToError(voiceResult.reason || 'voice_note_failed'), 400);
      }
      logger.log(`[follow-up] sent ok clientId=${clientId} recipient=@${recipientUsername} mode=${modeLabel}${cLog}`);
      fs.unlink(CHROME_FAKE_MIC_WAV, () => {});
      return { ok: true };
    }

    return fail('No delivery mode', 400);
  } catch (e) {
    logger.warn(`[follow-up] exception clientId=${clientId} recipient=@${recipientUsername} error=${e.message}${cLog}`);
    return { ok: false, error: e.message || 'Send failed', statusCode: 500 };
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

async function sendDM(page, username, adapter, options = {}) {
  const { messageOverride, campaignId, campaignLeadId, messageGroupId, messageGroupMessageId, dailySendLimit, hourlySendLimit } = options;
  const u = normalizeUsername(username);
  const sent = await Promise.resolve(adapter.alreadySent(u));
  if (sent) {
    logger.warn(`Already sent to @${u}, skipping.`);
    if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed').catch(() => {});
    return { ok: false, reason: 'already_sent' };
  }

  const stats = await Promise.resolve(adapter.getDailyStats());
  const dailyLimit = dailySendLimit ?? adapter.dailyLimit ?? DAILY_LIMIT;
  if (stats.total_sent >= dailyLimit) {
    logger.warn(`Daily limit reached (${dailyLimit}). Skipping.`);
    return { ok: false, reason: 'daily_limit' };
  }

  const hourlySent = await Promise.resolve(adapter.getHourlySent());
  const maxPerHour = hourlySendLimit ?? adapter.maxPerHour ?? MAX_PER_HOUR;
  if (hourlySent >= maxPerHour) {
    logger.warn(`Hourly limit reached (${maxPerHour}). Skipping.`);
    return { ok: false, reason: 'hourly_limit' };
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
  for (let attempt = 1; attempt <= MAX_SEND_RETRIES; attempt++) {
    try {
      const result = await sendDMOnce(page, u, messageTemplate, nameFallback, {
        firstNameBlocklist,
        voiceNotePath: resolvedVoicePath,
        voiceNoteMode: resolvedVoiceMode,
        voiceDurationSec: options.voiceDurationSec,
      });
      if (result.ok) {
        const finalMessage = result.finalMessage != null ? result.finalMessage : (resolvedVoiceMode === 'voice_only' ? '' : messageTemplate);
        await Promise.resolve(logSent('success', finalMessage));
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'sent').catch(() => {});
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
          coldDmOnSend(payload).catch(() => {});
        }
        logger.log(`Sent to @${u}: ${(finalMessage || messageTemplate).slice(0, 30)}...`);
        return { ok: true };
      }
      const terminalReasons = [
        'user_not_found',
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
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed', result.reason).catch(() => {});
        const snippet = result.pageSnippet ? '. Search result: ' + result.pageSnippet : '';
        logger.warn(`Send failed for @${u}: ${result.reason}${snippet}`);
        return result;
      }
      lastError = new Error(result.reason);
    } catch (err) {
      lastError = err;
      logger.warn(`Attempt ${attempt}/${MAX_SEND_RETRIES} for @${u} failed: ${err.message}`);
      if (attempt < MAX_SEND_RETRIES) await delay(2000 + Math.floor(Math.random() * 3000));
    }
  }
  logger.error(`Error sending to @${u} after ${MAX_SEND_RETRIES} retries`, lastError);
  await Promise.resolve(logSent('failed', null));
  if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed').catch(() => {});
  return { ok: false, reason: lastError.message };
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
    getDailyStats: () => sb.getDailyStats(clientId),
    getHourlySent: () => sb.getHourlySent(clientId),
    getControl: () => sb.getControl(clientId),
    setControl: (v) => sb.setControl(clientId, v),
    getRandomMessage: () =>
      messages?.length ? messages[Math.floor(Math.random() * messages.length)] : '',
  };
  return { adapter, minDelayMs, maxDelayMs };
}

/**
 * Multi-tenant loop: one worker serves all clients with pause=0 and pending work.
 * Exits when there is no work; start again from the dashboard when you have a campaign to run.
 */
async function runBotMultiTenant() {
  logger.log('Starting multi-tenant sender loop (always-on).');
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  const launchOpts = {
    headless: HEADLESS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
  };
  appendChromeFakeMicArgs(launchOpts.args);
  applyPuppeteerSlowMo(launchOpts);
  applyHeadedChromeWindowToLaunchOpts(launchOpts);
  if (launchOpts.slowMo) logger.log(`Puppeteer slowMo=${launchOpts.slowMo}ms (PUPPETEER_SLOW_MO_MS)`);
  let browser;
  try {
    browser = await puppeteer.launch(launchOpts);
  } catch (e) {
    logger.error('Browser launch failed', e);
    throw e;
  }
  let page;
  let currentSessionId = null;
  const campaignRoundRobin = new Map();

  async function ensurePageSession(pg, session) {
    const cookies = session?.session_data?.cookies;
    if (!cookies?.length) return false;
    if (currentSessionId === session.id) return true;
    try {
      const existing = await pg.cookies();
      if (existing.length) await pg.deleteCookie(...existing);
      await pg.setCookie(...cookies);
      await pg.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
      await delay(2000);
      if (pg.url().includes('/accounts/login')) {
        logger.error('Instagram session expired for account ' + (session.instagram_username || session.id));
        return false;
      }
      currentSessionId = session.id;
      return true;
    } catch (e) {
      logger.error('Failed to switch session: ' + e.message);
      return false;
    }
  }

  try {
    page = await browser.newPage();
    await grantMicrophoneForInstagram(page, logger);
    if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);
  } catch (err) {
    logger.error('Page setup failed', err);
    await browser.close().catch(() => {});
    throw err;
  }

  for (;;) {
    // Fresh DB read every iteration (no cache). After PM2 restart, first run sees current cold_dm_control, cold_dm_campaigns.status, and cold_dm_campaign_leads.
    const next = await sb.getNextPendingWorkAnyClient();
    if (!next) {
      const clientIds = await sb.getClientIdsWithPauseZero();
      let earliestResumeAt = null;
      let resumeReason = '';
      for (const cid of clientIds) {
        const info = await sb.getClientNoWorkResumeAt(cid).catch(() => ({ message: null, reason: 'no_pending', resumeAt: null }));
        if (info.message) await sb.setClientStatusMessage(cid, info.message).catch(() => {});
        if (info.reason === 'no_pending') continue;
        if (info.resumeAt && (!earliestResumeAt || info.resumeAt.getTime() < earliestResumeAt.getTime())) {
          earliestResumeAt = info.resumeAt;
          resumeReason = info.reason;
        }
      }
      if (!earliestResumeAt) {
        for (const cid of clientIds) {
          const hint = await sb.getNoWorkHint(cid).catch(() => '');
          if (hint) {
            logger.log('No work: ' + hint);
            await sb.setClientStatusMessage(cid, hint).catch(() => {});
          } else {
            await sb.setClientStatusMessage(cid, 'No work. Start again from the dashboard when you have a campaign to run.').catch(() => {});
          }
        }
        logger.log('No work. Exiting. Start again from the dashboard when you have a campaign to run.');
        await browser.close().catch(() => {});
        process.exit(0);
      }
      const sleepMs = Math.max(1000, earliestResumeAt.getTime() - Date.now());
      const sleepMin = Math.round(sleepMs / 60000);
      logger.log(`Paused (${resumeReason}). Resuming in ${sleepMin} min at ${earliestResumeAt.toISOString().slice(0, 16)}.`);
      const SCHEDULE_RECHECK_MS = 5 * 60 * 1000;
      const chunkMs = resumeReason === 'outside_schedule' ? Math.min(sleepMs, SCHEDULE_RECHECK_MS) : sleepMs;
      await delay(chunkMs);
      continue;
    }
    const { clientId, work } = next;
    const pause = await sb.getControl(clientId);
    if (pause === '1' || pause === 1) {
      continue;
    }
    const built = await buildAdapterForClient(clientId);
    if (!built) {
      logger.warn('No adapter for client ' + clientId + ', skipping.');
      continue;
    }
    const { adapter, minDelayMs, maxDelayMs } = built;

    const sessions = await sb.getSessionsForCampaign(clientId, work.campaignId);
    if (!sessions || sessions.length === 0) {
      logger.warn('No sessions for campaign ' + work.campaignId + ', failing lead.');
      await sb.updateCampaignLeadStatus(work.campaignLeadId, 'failed').catch(() => {});
      await delay(randomDelay(2000, 5000));
      continue;
    }
    let state = campaignRoundRobin.get(work.campaignId);
    if (!state) {
      state = { lastIndex: -1 };
      campaignRoundRobin.set(work.campaignId, state);
    }
    state.lastIndex = (state.lastIndex + 1) % sessions.length;
    const session = sessions[state.lastIndex];
    const ok = await ensurePageSession(page, session);
    if (!ok) {
      logger.warn('Could not load session for campaign, failing lead.');
      await sb.updateCampaignLeadStatus(work.campaignLeadId, 'failed').catch(() => {});
      await delay(randomDelay(2000, 5000));
      continue;
    }

    const options = {
      clientId,
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
    };
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
          await browser.close().catch(() => {});
          browser = await puppeteer.launch(launchOpts);
          page = await browser.newPage();
          await grantMicrophoneForInstagram(page, logger);
          await applyDesktopEmulation(page);
          currentSessionId = null;
          const ok = await ensurePageSession(page, session);
          if (!ok) {
            logger.warn('Could not restore session after voice browser restart.');
          }
        }
      } catch (e) {
        logger.warn('Voice browser restart failed: ' + (e.message || e));
      } finally {
        if (resolved) await resolved.cleanup().catch(() => {});
      }
    }

    const sendResult = await sendDM(page, work.username, adapter, options);

    let delayMs;
    if (!sendResult.ok && sendResult.reason === 'hourly_limit') {
      delayMs = randomDelay(55 * 60 * 1000, 60 * 60 * 1000);
      logger.log(`Hourly limit reached. Sleeping ${Math.round(delayMs / 60000)} minutes until window resets.`);
      sb.setClientStatusMessage(clientId, 'Hourly limit reached. Next send in ~60 min.').catch(() => {});
    } else if (!sendResult.ok && sendResult.reason === 'daily_limit') {
      delayMs = randomDelay(5 * 60 * 1000, 10 * 60 * 1000);
      logger.log(`Daily limit reached. Rechecking in ${Math.round(delayMs / 60000)} minutes.`);
      sb.setClientStatusMessage(clientId, 'Daily limit reached.').catch(() => {});
    } else {
      delayMs =
        work.minDelaySec != null && work.maxDelaySec != null
          ? randomDelay(work.minDelaySec * 1000, work.maxDelaySec * 1000)
          : randomDelay(minDelayMs, maxDelayMs);
      logger.log(`Next send in ${Math.round(delayMs / 60000)} minutes.`);
      sb.setClientStatusMessage(clientId, `Waiting. Next send in ${Math.round(delayMs / 60000)} min.`).catch(() => {});
    }
    await delay(delayMs);
  }
}

async function runBot() {
  const useSupabase = sb.isSupabaseConfigured();

  if (useSupabase) {
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
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
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
    try {
      const existing = await page.cookies();
      if (existing.length) await page.deleteCookie(...existing);
      await page.setCookie(...cookies);
      await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
      await delay(2000);
      if (page.url().includes('/accounts/login')) {
        logger.error('Instagram session expired for account ' + (session.instagram_username || session.id));
        return false;
      }
      currentSessionId = session.id;
      return true;
    } catch (e) {
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
    logger.log(`Next send in ${Math.round(delayMs / 60000)} minutes.`);
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
async function connectInstagram(instagramUsername, instagramPassword, twoFactorCode = null) {
  const useMobile = process.env.DISABLE_MOBILE_LOGIN !== '1' && process.env.DISABLE_MOBILE_LOGIN !== 'true';
  if (!useMobile) logger.log('Using desktop view for login (DISABLE_MOBILE_LOGIN is set).');
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger);
  const connectLaunch = {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
  };
  appendChromeFakeMicArgs(connectLaunch.args);
  applyPuppeteerSlowMo(connectLaunch);
  const browser = await puppeteer.launch(connectLaunch);
  let keepBrowserOpen = false;
  try {
    const page = await browser.newPage();
    if (useMobile && !VOICE_NOTE_FILE) await applyMobileEmulation(page);
    else await applyDesktopEmulation(page);
    await login(page, {
      username: instagramUsername,
      password: instagramPassword,
      twoFactorCode: twoFactorCode || undefined,
    });
    const cookies = await page.cookies();
    return { cookies, username: instagramUsername };
  } catch (e) {
    if (e.code === 'TWO_FACTOR_REQUIRED' && e.page) {
      keepBrowserOpen = true;
      return { twoFactorRequired: true, page: e.page, browser, username: instagramUsername };
    }
    throw e;
  } finally {
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
  const cookies = await page.cookies();
  await browser.close().catch(() => {});
  logger.log('2FA completed, session saved.');
  return { cookies, username: instagramUsername };
}

module.exports = {
  runBot,
  getDailyStats,
  loadLeadsFromCSV,
  sendDM,
  sendFollowUp,
  login,
  connectInstagram,
  completeInstagram2FA,
  scheduleDebugFollowUpBrowser,
};
