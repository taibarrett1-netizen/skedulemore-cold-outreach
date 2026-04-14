/**
 * Reliable navigation to Instagram /direct/new/.
 * Swallowing page.goto errors (e.g. .catch(() => {})) leaves the main frame on
 * chrome-error://chromewebdata/ with no DOM — send flow then fails with
 * "Search input not found on direct/new page".
 */
const logger = require('./logger');

const DIRECT_NEW_HTTPS = 'https://www.instagram.com/direct/new/';

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isFailedFrameUrl(url) {
  if (!url || typeof url !== 'string') return true;
  const u = url.trim().toLowerCase();
  if (!u) return true;
  if (u.startsWith('chrome-error://')) return true;
  if (u.startsWith('chrome://')) return true;
  if (u === 'about:blank') return true;
  if (u.startsWith('devtools://')) return true;
  return false;
}

/**
 * @param {import('puppeteer').Page} page
 * @param {{ retries?: number; timeoutMs?: number; log?: (msg: string) => void }} [opts]
 */
async function gotoInstagramDirectNew(page, opts = {}) {
  const retries = Math.max(1, opts.retries ?? 5);
  const timeout = Math.max(8000, opts.timeoutMs ?? 55000);
  const log = typeof opts.log === 'function' ? opts.log : (msg) => logger.warn(msg);
  const waitStrategies = ['domcontentloaded', 'load', 'domcontentloaded', 'load', 'load'];

  let lastErr = null;
  for (let attempt = 0; attempt < retries; attempt++) {
    const waitUntil = waitStrategies[Math.min(attempt, waitStrategies.length - 1)];
    try {
      await page.goto(DIRECT_NEW_HTTPS, { waitUntil, timeout });
      await delay(350);
      const url = page.url();
      if (!isFailedFrameUrl(url)) {
        return;
      }
      lastErr = new Error(`navigation landed on error document: ${url}`);
      log(`[ig-nav] direct/new attempt ${attempt + 1}/${retries} bad frame url (${waitUntil}): ${url}`);
    } catch (e) {
      lastErr = e;
      log(
        `[ig-nav] direct/new attempt ${attempt + 1}/${retries} goto failed (${waitUntil}): ${e && e.message ? e.message : String(e)}`
      );
    }
    await delay(700 + attempt * 550);
  }
  const finalUrl = page.url();
  throw lastErr || new Error(`instagram direct/new failed after ${retries} attempts (last url=${finalUrl})`);
}

module.exports = {
  DIRECT_NEW_HTTPS,
  isFailedFrameUrl,
  gotoInstagramDirectNew,
};
