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
const { dismissInstagramHomeModals } = require('./utils/instagram-modals');
const {
  navigateToDmThread,
  sendPlainTextInThread,
  typeInstagramDmPlainTextInComposer,
  typeInstagramDmPlainTextWithKeyboard,
} = require('./utils/open-dm-thread');
const { clickInstagramDmSearchResult, formatSearchFailurePageSnippet } = require('./utils/instagram-dm-search');
const { attachInstagramSendIdCapture } = require('./utils/instagram-dm-network-ids');
const { applyProxyToLaunchOptions, authenticatePageForProxy } = require('./utils/proxy-puppeteer');
puppeteer.use(StealthPlugin());

const DAILY_LIMIT = Math.min(parseInt(process.env.DAILY_SEND_LIMIT, 10) || 100, 200);
const MIN_DELAY_MS = (parseInt(process.env.MIN_DELAY_MINUTES, 10) || 5) * 60 * 1000;
const MAX_DELAY_MS = (parseInt(process.env.MAX_DELAY_MINUTES, 10) || 30) * 60 * 1000;
const MAX_PER_HOUR = parseInt(process.env.MAX_SENDS_PER_HOUR, 10) || 20;
const HEADLESS = process.env.HEADLESS_MODE !== 'false';
const SEND_LEASE_SECONDS = Math.max(120, parseInt(process.env.SEND_LEASE_SECONDS || '600', 10) || 600);
const SEND_WORKER_ID = process.env.SEND_WORKER_ID || `send-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
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
const LOGIN_DEBUG_SCREENSHOT_DIR = path.join(process.cwd(), 'logs', 'login-debug');

function wantsVoiceNotes(sendOpts = {}) {
  return !!((sendOpts.voiceNotePath || '').trim());
}

async function saveLoginDebugScreenshot(page, label) {
  const enabled =
    process.env.LOGIN_DEBUG_SCREENSHOTS === '1' ||
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true' ||
    process.env.LOGIN_DEBUG === '1' ||
    process.env.LOGIN_DEBUG === 'true';
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
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true' ||
    process.env.LOGIN_DEBUG === '1' ||
    process.env.LOGIN_DEBUG === 'true'
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
    process.env.LOGIN_DEBUG_SCREENSHOTS === 'true' ||
    process.env.LOGIN_DEBUG === '1' ||
    process.env.LOGIN_DEBUG === 'true'
  );
}

/** Full-page PNG when DM /direct/new search result click fails (same folder as login-debug). */
async function saveDmSearchDebugScreenshot(page, label) {
  if (!wantsDmSearchDebugScreenshot() || !page) return null;
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
    if (process.env.LOGIN_DEBUG === '1' || process.env.LOGIN_DEBUG === 'true') {
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

/** Cookie consent blocks typing/clicks on login form; dismiss it before filling credentials. */
async function dismissInstagramCookieConsent(page) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const clicked = await page.evaluate(() => {
      const roots = Array.from(document.querySelectorAll('[role="dialog"], [role="alertdialog"], div'));
      const targets = roots.filter((el) => {
        const t = (el.textContent || '').toLowerCase();
        return t.includes('allow the use of cookies') || t.includes('allow all cookies') || t.includes('cookie');
      });
      for (const root of targets) {
        const clickables = Array.from(root.querySelectorAll('button, [role="button"], a, span'));
        const preferred =
          clickables.find((el) => /allow all cookies|allow all|accept all/i.test((el.textContent || '').trim())) ||
          clickables.find((el) => /decline optional cookies|only allow essential|essential cookies/i.test((el.textContent || '').trim()));
        if (preferred && preferred.offsetParent) {
          const btn = preferred.closest('[role="button"]') || preferred.closest('button') || preferred;
          btn.click();
          return true;
        }
      }
      return false;
    });
    if (!clicked) return false;
    await delay(900);
  }
  return true;
}

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

  const cookieDismissed = await dismissInstagramCookieConsent(page);
  if (cookieDismissed) {
    logger.log('Dismissed Instagram cookie consent modal.');
    await saveLoginDebugScreenshot(page, 'after_cookie_dismiss');
    await delay(600);
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
  await saveLoginDebugScreenshot(page, 'before_submit');

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

  const emailCheckpoint = await detectInstagramEmailVerificationState(page);
  if (emailCheckpoint.required) {
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

  const emailCheckpointAfterRetry = await detectInstagramEmailVerificationState(page);
  if (emailCheckpointAfterRetry.required) {
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
  await assertHealthyInstagramSessionOrThrow(page, 'login');
  logger.log('Logged in to Instagram.');
}

const MAX_SEND_RETRIES = 3;

async function sendDMOnce(page, u, messageTemplate, nameFallback = {}, sendOpts = {}) {
  const voiceCfg = buildVoiceSendConfig(sendOpts);
  if (wantsVoiceNotes(voiceCfg) && !isFfmpegAvailable()) {
    return { ok: false, reason: 'ffmpeg_missing', pageSnippet: 'Install ffmpeg on the VPS: sudo apt install ffmpeg' };
  }
  // Desktop layout for all sends: mobile thread header merges back-arrow + name in innerText ("BackTai"); desktop DMs behave better for automation.
  await applyDesktopEmulation(page);
  await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 });
  await humanDelay();
  for (let termsRound = 0; termsRound < 3; termsRound++) {
    if (!isInstagramTermsUnblockUrl(page.url())) break;
    const handled = await handleInstagramTermsUnblock(page).catch(() => false);
    if (handled && !isInstagramTermsUnblockUrl(page.url())) {
      if (!page.url().toLowerCase().includes('/direct/')) {
        await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 }).catch(() => {});
      }
      await delay(1200);
      break;
    }
    if (isInstagramTermsUnblockUrl(page.url())) {
      await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 }).catch(() => {});
      await delay(2000);
    }
  }

  await dismissInstagramHomeModals(page, logger);
  await delay(500);

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

  const searchPick = await clickInstagramDmSearchResult(page, u).catch((e) => ({
    ok: false,
    reason: 'search_result_select_failed',
    logLine: `evaluate_threw: ${e && e.message ? e.message : String(e)}`,
  }));
  if (!searchPick.ok) {
    await logDmSearchFailureDiagnostics(page, u, searchPick).catch(() => {});
    return {
      ok: false,
      reason: searchPick.reason || 'search_result_select_failed',
      pageSnippet: formatSearchFailurePageSnippet(u, searchPick),
    };
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
    await dismissInstagramHomeModals(page, logger);
    await delay(500);
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
        const needle = username.replace(/^@/, '').toLowerCase();
        const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
        const tokenRegex = new RegExp(`(^|[^a-z0-9._])@?${needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z0-9._]|$)`, 'i');
        const containsUsernameToken = (s) => tokenRegex.test(clean(s).toLowerCase());
        const tooGeneric = (s) => {
          const t = clean(s).toLowerCase();
          if (!t) return true;
          if (t === needle || t === `@${needle}` || containsUsernameToken(t)) return true;
          if (t.length < 2 || t.length > 80) return true;
          if (/^(message|send message|chat|details|info|back|next|cancel)$/i.test(t)) return true;
          return false;
        };
        const normalizeCandidateName = (raw) => {
          let t = clean(raw);
          if (!t) return '';
          const splitPieces = t
            .split(/[|·•]/g)
            .map((x) => clean(x))
            .filter(Boolean);
          if (splitPieces.length > 1) {
            const nonUserPieces = splitPieces.filter((p) => !containsUsernameToken(p) && !/^instagram$/i.test(p));
            if (nonUserPieces.length) t = nonUserPieces[0];
          }
          t = clean(t.replace(/\binstagram\b/gi, '').replace(new RegExp(`@?${needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'gi'), ''));
          if (tooGeneric(t)) return '';
          return t;
        };

        /** Open thread column only (anchored from Message composer — not inbox list). */
        function threadPaneRoot() {
          const vis = (el) => {
            try {
              return el && el.offsetParent !== null;
            } catch {
              return false;
            }
          };
          const composers = Array.from(document.querySelectorAll('textarea, div[contenteditable="true"]')).filter(vis);
          const compose = composers.find((el) => {
            const ph = (el.getAttribute('placeholder') || '').toLowerCase();
            const aria = (el.getAttribute('aria-label') || '').toLowerCase();
            return ph.includes('message') || aria.includes('message');
          });
          if (!compose) return document.body;
          let main = compose.closest('[role="main"]') || compose.closest('main');
          if (main) return main;
          let el = compose;
          const vw = document.documentElement.clientWidth || 1200;
          for (let depth = 0; depth < 14 && el; depth++) {
            el = el.parentElement;
            if (!el) break;
            const r = el.getBoundingClientRect();
            if (r.width >= vw * 0.28 && r.height >= 180) return el;
          }
          return compose.parentElement || document.body;
        }

        const pane = threadPaneRoot();

        // 1) Prefer DM thread header relation (within open thread pane only).
        const headerRoots = Array.from(pane.querySelectorAll('header, [role="banner"]'));
        for (const root of headerRoots) {
          const lines = (root.innerText || '')
            .split(/\n/)
            .map((x) => clean(x))
            .filter(Boolean);
          for (let i = 0; i < lines.length; i++) {
            if (!containsUsernameToken(lines[i])) continue;
            const prev = i > 0 ? normalizeCandidateName(lines[i - 1]) : '';
            if (prev) return prev;
            const next = i + 1 < lines.length ? normalizeCandidateName(lines[i + 1]) : '';
            if (next) return next;
          }
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
            const txt = normalizeCandidateName(el.textContent || '');
            if (txt) headerCandidates.push(txt);
          });
        }
        if (headerCandidates.length) {
          headerCandidates.sort((a, b) => b.length - a.length);
          return headerCandidates[0];
        }

        // 3) Visible profile link in pane → nearby text.
        const profileLink = Array.from(pane.querySelectorAll('a[href*="instagram.com/"], a[href^="/"]')).find((a) => {
          const href = (a.getAttribute('href') || '').toLowerCase();
          return href.includes(`/${needle}`) || href === `/${needle}/` || href === `/${needle}`;
        });
        if (profileLink) {
          const parent = profileLink.closest('header') || profileLink.parentElement || profileLink;
          const txt = normalizeCandidateName(parent.textContent || '');
          if (txt) return txt;
        }

        // 4) Fallback: text in pane only.
        const body = pane.innerText || '';
        const idx = body.toLowerCase().indexOf(needle);
        if (idx > 0) {
          const before = body.slice(0, idx).trim();
          const lines = before.split(/\n/);
          const lastPart = (lines[lines.length - 1] || '').trim();
          const candidate = lastPart.length > 0 && lastPart.length <= 80 && !/^https?:\/\//i.test(lastPart) ? lastPart : (before.length > 0 && before.length <= 80 ? before : null);
          const normalized = normalizeCandidateName(candidate || '');
          if (candidate && !/^\d+$/.test(candidate) && normalized) return normalized;
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
      await saveComposeTypingDebugScreenshot(page, u);
      await typeInstagramDmPlainTextInComposer(page, compose, msg, {
        delay: 60 + Math.floor(Math.random() * 40),
      });
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
    await page.keyboard.press('Enter');
    await delay(1500);
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

function buildFollowUpLaunchOptions(fakeMicPath = DEFAULT_CHROME_FAKE_MIC_WAV, proxyUrl = null) {
  // NEW: Chrome fake mic with file injection (no PulseAudio needed).
  ensureChromeFakeMicPlaceholder(logger, fakeMicPath);
  const opts = {
    headless: HEADLESS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--autoplay-policy=no-user-gesture-required',
    ],
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
        const cap = await sendPlainTextInThread(page, captionRaw, { idCapture });
        if (!cap.ok) {
          return fail(followUpReasonToError(cap.reason), 400);
        }
        captionIds.push(cap.instagramMessageId || null);
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

async function sendDM(page, username, adapter, options = {}) {
  const { messageOverride, campaignId, campaignLeadId, messageGroupId, messageGroupMessageId, dailySendLimit, hourlySendLimit } = options;
  const sendWorkerId = options.sendWorkerId || null;
  const u = normalizeUsername(username);
  const sent = await Promise.resolve(adapter.alreadySent(u));
  if (sent) {
    logger.warn(`Already sent to @${u}, skipping.`);
    if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed', null, sendWorkerId).catch(() => {});
    return { ok: false, reason: 'already_sent' };
  }

  const freshCampaignLimits =
    campaignId && typeof sb.getCampaignLimitsById === 'function'
      ? await sb.getCampaignLimitsById(campaignId).catch(() => null)
      : null;
  const effectiveDailyLimit = freshCampaignLimits ? freshCampaignLimits.daily_send_limit : dailySendLimit;
  const effectiveHourlyLimit = freshCampaignLimits ? freshCampaignLimits.hourly_send_limit : hourlySendLimit;
  const stats = await Promise.resolve(adapter.getDailyStats());
  const hourlySent = await Promise.resolve(adapter.getHourlySent());
  const limitState = evaluateCampaignLimitState({
    sentToday: stats.total_sent,
    sentThisHour: hourlySent,
    dailySendLimit: effectiveDailyLimit,
    hourlySendLimit: effectiveHourlyLimit,
  });
  if (limitState.blocked) {
    logger.warn(limitState.statusMessage);
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
  for (let attempt = 1; attempt <= MAX_SEND_RETRIES; attempt++) {
    try {
      const result = await sendDMOnce(page, u, messageTemplate, nameFallback, {
        firstNameBlocklist,
        senderName: senderAccountName,
        voiceNotePath: resolvedVoicePath,
        voiceNoteMode: resolvedVoiceMode,
        voiceDurationSec: options.voiceDurationSec,
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
        return { ok: true };
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
      lastError = err;
      logger.warn(`Attempt ${attempt}/${MAX_SEND_RETRIES} for @${u} failed: ${err.message}`);
      if (attempt < MAX_SEND_RETRIES) await delay(2000 + Math.floor(Math.random() * 3000));
    }
  }
  logger.error(`Error sending to @${u} after ${MAX_SEND_RETRIES} retries`, lastError);
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
  let browser = null;
  let page = null;
  /** @type {string|null|undefined} undefined = never launched; '' = no proxy */
  let currentProxyKey = undefined;
  let currentSessionId = null;
  const campaignRoundRobin = new Map();
  /** Retries when cold_dm_control has no pause=0 yet (race right after dashboard Start). */
  let noPauseZeroEmptyRounds = 0;

  function proxyKeyForSession(session) {
    return session && session.proxy_url ? String(session.proxy_url).trim() : '';
  }

  async function ensureBrowserForSession(session) {
    const key = proxyKeyForSession(session);
    const needLaunch = !browser || currentProxyKey !== key;
    if (!needLaunch) return;
    if (browser) {
      await browser.close().catch(() => {});
      browser = null;
      page = null;
      currentSessionId = null;
    }
    currentProxyKey = key;
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
    applyProxyToLaunchOptions(launchOpts, session.proxy_url || null);
    if (launchOpts.slowMo) logger.log(`Puppeteer slowMo=${launchOpts.slowMo}ms (PUPPETEER_SLOW_MO_MS)`);
    try {
      browser = await puppeteer.launch(launchOpts);
    } catch (e) {
      logger.error('Browser launch failed', e);
      throw e;
    }
    page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url);
    await grantMicrophoneForInstagram(page, logger);
    if (VOICE_NOTE_FILE) await applyDesktopEmulation(page);
    else await applyMobileEmulation(page);
  }

  async function ensurePageSession(session) {
    const cookies = session?.session_data?.cookies;
    if (!cookies?.length) return false;
    await ensureBrowserForSession(session);
    const pg = page;
    if (!pg) return false;
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

  for (;;) {
    // Fresh DB read every iteration (no cache). After PM2 restart, first run sees current cold_dm_control, cold_dm_campaigns.status, and cold_dm_campaign_leads.
    const next = await sb.getNextPendingWorkAnyClient(SEND_WORKER_ID, SEND_LEASE_SECONDS);
    if (!next) {
      const clientIds = await sb.getClientIdsWithPauseZero();
      if (clientIds.length === 0) {
        noPauseZeroEmptyRounds += 1;
        if (noPauseZeroEmptyRounds <= 24) {
          logger.warn(
            `[send-worker] No cold_dm_control rows with pause=0 (attempt ${noPauseZeroEmptyRounds}/24). Retrying in 15s (common right after clicking Start).`
          );
          await delay(15000);
          continue;
        }
        logger.error('[send-worker] Giving up: still no pause=0 clients after ~6 min.');
        await browser?.close().catch(() => {});
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
            await sb.setClientStatusMessage(cid, 'No work. Start again from the dashboard when you have a campaign to run.').catch(() => {});
          }
          }
          // Keep control flag aligned with worker state so dashboards don't show "running" after an auto-exit.
          await sb.setControl(cid, 1).catch(() => {});
        }
        logger.log('No work. Exiting. Start again from the dashboard when you have a campaign to run.');
        await browser?.close().catch(() => {});
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
    noPauseZeroEmptyRounds = 0;
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
      await sb
        .setClientStatusMessage(
          clientId,
          'No Instagram sender linked to this campaign. Open the campaign → Settings → Instagram accounts, attach an account, then Start again.'
        )
        .catch(() => {});
      await sb.updateCampaignLeadStatus(work.campaignLeadId, 'failed', 'no_instagram_session', SEND_WORKER_ID).catch(() => {});
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
    const ok = await ensurePageSession(session);
    if (!ok) {
      logger.warn('Could not load session for campaign, failing lead.');
      await sb.updateCampaignLeadStatus(work.campaignLeadId, 'failed', null, SEND_WORKER_ID).catch(() => {});
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
          browser = null;
          page = null;
          currentSessionId = null;
          const okVoice = await ensurePageSession(session);
          if (!okVoice) {
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
      const msg = sendResult.statusMessage || 'hourly limit reached';
      logger.log(`${msg}. Sleeping ${Math.round(delayMs / 60000)} minutes until window resets.`);
      sb.setClientStatusMessage(clientId, `${msg}. Next send in ~60 min.`).catch(() => {});
    } else if (!sendResult.ok && sendResult.reason === 'daily_limit') {
      delayMs = randomDelay(5 * 60 * 1000, 10 * 60 * 1000);
      const msg = sendResult.statusMessage || 'daily limit reached';
      logger.log(`${msg}. Rechecking in ${Math.round(delayMs / 60000)} minutes.`);
      sb.setClientStatusMessage(clientId, msg).catch(() => {});
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
async function connectInstagram(instagramUsername, instagramPassword, twoFactorCode = null, options = {}) {
  const proxyUrl = options && options.proxyUrl;
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
  applyProxyToLaunchOptions(connectLaunch, proxyUrl);
  const browser = await puppeteer.launch(connectLaunch);
  let keepBrowserOpen = false;
  try {
    const page = await browser.newPage();
    await authenticatePageForProxy(page, proxyUrl);
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
  const cookies = await page.cookies();
  await browser.close().catch(() => {});
  logger.log('2FA completed, session saved.');
  return { cookies, username: instagramUsername };
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
  const cookies = await page.cookies();
  await browser.close().catch(() => {});
  logger.log('Email verification completed, session saved.');
  return { cookies, username: instagramUsername };
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
};
