require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { getRandomMessage } = require('./config/messages');
const { alreadySent, logSentMessage, getDailyStats, normalizeUsername, getControl, setControl } = require('./database/db');
const sb = require('./database/supabase');
const logger = require('./utils/logger');
const { applyMobileEmulation } = require('./utils/mobile-viewport');
const { substituteVariables } = require('./utils/message-variables');

puppeteer.use(StealthPlugin());

const DAILY_LIMIT = Math.min(parseInt(process.env.DAILY_SEND_LIMIT, 10) || 100, 200);
const MIN_DELAY_MS = (parseInt(process.env.MIN_DELAY_MINUTES, 10) || 5) * 60 * 1000;
const MAX_DELAY_MS = (parseInt(process.env.MAX_DELAY_MINUTES, 10) || 30) * 60 * 1000;
const MAX_PER_HOUR = parseInt(process.env.MAX_SENDS_PER_HOUR, 10) || 20;
const HEADLESS = process.env.HEADLESS_MODE !== 'false';
const BROWSER_PROFILE_DIR = path.join(process.cwd(), '.browser-profile');

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function randomDelay(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
}

async function humanDelay() {
  await delay(500 + Math.floor(Math.random() * 1500));
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

async function sendDMOnce(page, u, messageTemplate, nameFallback = {}) {
  await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 });
  await humanDelay();

  const searchInput = await page.evaluateHandle(() => {
    const inputs = Array.from(document.querySelectorAll('input'));
    const visible = inputs.filter((el) => el.offsetParent !== null && el.type !== 'hidden');
    const search = visible.find(
      (el) =>
        el.placeholder && (el.placeholder.toLowerCase().includes('search') || el.placeholder.toLowerCase().includes('to:'))
    );
    if (search) return search;
    const firstText = visible.find((el) => el.type === 'text' || el.type === '' || !el.type);
    return firstText || null;
  });
  const searchEl = searchInput.asElement();
  if (!searchEl) {
    await searchInput.dispose();
    throw new Error('Search input not found on direct/new page');
  }
  await searchEl.type(u, { delay: 100 });
  await searchEl.dispose();
  await searchInput.dispose();
  await delay(1500);

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
    return { ok: false, reason: 'user_not_found' };
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
  try {
    await page.waitForSelector(composeSelector, { timeout: 20000 });
    composeFound = true;
  } catch (e) {
    const diag = await composeDiagnostic().catch(() => ({}));
    logger.warn('Compose wait failed ' + e.message);
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));
  }

  const displayNameFromPage = await page.evaluate(() => {
    const body = document.body && document.body.innerText;
    if (!body) return null;
    const words = body.split(/\s+/).filter(Boolean);
    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      if (w.length >= 2 && w.length <= 30 && w === w.toLowerCase() && /^[a-z0-9._]+$/.test(w) && !/^https?:\/\//.test(w)) {
        if (i >= 2) return { first_name: words[i - 2], last_name: words[i - 1] };
        if (i >= 1) return { first_name: words[i - 1], last_name: '' };
        return null;
      }
    }
    return null;
  });

  const leadFromPage = {
    username: u,
    first_name: displayNameFromPage?.first_name ?? nameFallback.first_name ?? null,
    last_name: displayNameFromPage?.last_name ?? nameFallback.last_name ?? null,
  };
  const msg = substituteVariables(messageTemplate, leadFromPage);

  if (composeFound) {
    const diag = await composeDiagnostic().catch(() => ({}));
    logger.log('Compose diagnostic: ' + JSON.stringify(diag));

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
    if (compose) {
      await delay(500);
      await compose.click();
      await compose.type(msg, { delay: 60 + Math.floor(Math.random() * 40) });
      await compose.dispose();
      await composeEl.dispose();
      await humanDelay();
      await page.keyboard.press('Enter');
      await delay(1500);
      return { ok: true, finalMessage: msg };
    }
    await composeEl.dispose();
    logger.warn('Compose element not found after selector matched');
  }

  const keyboardSent = await page.evaluate((text) => {
    const focusable = document.querySelector('textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]');
    if (!focusable || focusable.offsetParent === null) return false;
    focusable.focus();
    focusable.click();
    return true;
  }, msg);
  if (keyboardSent) {
    await delay(300);
    await page.keyboard.type(msg, { delay: 60 + Math.floor(Math.random() * 40) });
    await humanDelay();
    await page.keyboard.press('Enter');
    await delay(1500);
    return { ok: true, finalMessage: msg };
  }

  return { ok: false, reason: 'no_compose' };
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
  const logSent = (status, finalMsg) => adapter.logSentMessage(u, finalMsg != null ? finalMsg : messageTemplate, status, campaignId, messageGroupId, messageGroupMessageId);

  let lastError;
  const nameFallback = { first_name: options.first_name, last_name: options.last_name };
  for (let attempt = 1; attempt <= MAX_SEND_RETRIES; attempt++) {
    try {
      const result = await sendDMOnce(page, u, messageTemplate, nameFallback);
      if (result.ok) {
        const finalMessage = result.finalMessage != null ? result.finalMessage : messageTemplate;
        await Promise.resolve(logSent('success', finalMessage));
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'sent').catch(() => {});
        logger.log(`Sent to @${u}: ${(finalMessage || messageTemplate).slice(0, 30)}...`);
        return { ok: true };
      }
      if (result.reason === 'user_not_found' || result.reason === 'no_compose') {
        await Promise.resolve(logSent('failed', result.finalMessage));
        if (campaignLeadId) await sb.updateCampaignLeadStatus(campaignLeadId, 'failed').catch(() => {});
        logger.warn(`Send failed for @${u}: ${result.reason}`);
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
    logSentMessage: (u, msg, status, campaignId, messageGroupId, messageGroupMessageId) =>
      sb.logSentMessage(clientId, u, msg, status, campaignId, messageGroupId, messageGroupMessageId),
    getDailyStats: () => sb.getDailyStats(clientId),
    getHourlySent: () => sb.getHourlySent(clientId),
    getControl: () => sb.getControl(clientId),
    setControl: (v) => sb.setControl(clientId, v),
    getRandomMessage: () =>
      messages?.length ? messages[Math.floor(Math.random() * messages.length)] : '',
  };
  return { adapter, minDelayMs, maxDelayMs };
}

// When there's no work we exit; user starts the bot again from the dashboard when they want to run.

/**
 * Multi-tenant always-on loop: one worker serves all clients with pause=0 and pending work.
 * Never exits; sleeps and re-checks when no work.
 */
async function runBotMultiTenant() {
  logger.log('Starting multi-tenant sender loop (always-on).');
  const launchOpts = {
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  };
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
    await applyMobileEmulation(page);
  } catch (err) {
    logger.error('Page setup failed', err);
    await browser.close().catch(() => {});
    throw err;
  }

  for (;;) {
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
        logger.log('No work. Exiting. Start again from the dashboard when you have a campaign to run.');
        await browser.close().catch(() => {});
        process.exit(0);
      }
      const sleepMs = Math.max(1000, earliestResumeAt.getTime() - Date.now());
      const sleepMin = Math.round(sleepMs / 60000);
      logger.log(`Paused (${resumeReason}). Resuming in ${sleepMin} min at ${earliestResumeAt.toISOString().slice(0, 16)}.`);
      await delay(sleepMs);
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
    };
    sb.setClientStatusMessage(clientId, 'Sending…').catch(() => {});
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

  const launchOpts = {
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  };
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
    await applyMobileEmulation(page);
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
    const options = {};

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
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });
  let keepBrowserOpen = false;
  try {
    const page = await browser.newPage();
    if (useMobile) await applyMobileEmulation(page);
    else await page.setViewport({ width: 1280, height: 800 });
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

module.exports = { runBot, getDailyStats, loadLeadsFromCSV, sendDM, login, connectInstagram, completeInstagram2FA };
