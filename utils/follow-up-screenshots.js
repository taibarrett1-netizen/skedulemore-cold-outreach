/**
 * Optional PNG screenshots during follow-up sends for debugging (e.g. "success" in logs but nothing on IG).
 * Enable with FOLLOW_UP_DEBUG_SCREENSHOTS=true in .env
 */
const fs = require('fs');
const path = require('path');

const DIR = path.join(__dirname, '..', 'follow-up-screenshots');

function isFollowUpScreenshotsEnabled() {
  const v = (process.env.FOLLOW_UP_DEBUG_SCREENSHOTS || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

function fullPageScreenshots() {
  const v = (process.env.FOLLOW_UP_SCREENSHOTS_FULL_PAGE || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

/**
 * @param {import('puppeteer').Page} page
 * @param {string} step - short label (e.g. thread, voice-before-send-click)
 * @param {{ correlationId?: string, logger?: { log: Function, warn: Function } }} meta
 * @returns {Promise<string|null>} absolute file path or null
 */
async function captureFollowUpScreenshot(page, step, meta = {}) {
  if (!isFollowUpScreenshotsEnabled() || !page) return null;
  const { correlationId, logger } = meta;
  try {
    if (!fs.existsSync(DIR)) fs.mkdirSync(DIR, { recursive: true });
    const safeCorr = (correlationId || 'no-corr').replace(/[^a-zA-Z0-9-]/g, '_').slice(0, 80);
    const safeStep = String(step).replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50);
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const fname = `${ts}_${safeCorr}_${safeStep}.png`;
    const fpath = path.join(DIR, fname);
    await page.screenshot({
      path: fpath,
      type: 'png',
      fullPage: fullPageScreenshots(),
    });
    const rel = path.join('follow-up-screenshots', fname);
    if (logger) logger.log(`[follow-up] debug screenshot saved ${rel}`);
    return fpath;
  } catch (e) {
    if (logger) logger.warn(`[follow-up] debug screenshot failed: ${e.message}`);
    return null;
  }
}

module.exports = {
  DIR,
  isFollowUpScreenshotsEnabled,
  captureFollowUpScreenshot,
};
