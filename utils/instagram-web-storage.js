'use strict';

/**
 * Serialize instagram.com localStorage + sessionStorage alongside Puppeteer cookies.
 * Reduces "fresh browser" churn when IG relies on web storage beyond document.cookie.
 */

const DEFAULT_MAX_JSON = 450000;

function maxWebStorageJsonChars() {
  const n = parseInt(process.env.INSTAGRAM_WEB_STORAGE_MAX_JSON || String(DEFAULT_MAX_JSON), 10);
  return Number.isFinite(n) && n > 10_000 ? n : DEFAULT_MAX_JSON;
}

/**
 * @param {import('puppeteer').Page} page
 * @param {{ warn?: (s: string) => void }} [logger]
 * @returns {Promise<{ localStorage: Record<string, string>, sessionStorage: Record<string, string>, v: 1 } | null>}
 */
async function captureInstagramWebStorageFromPage(page, logger) {
  if (!page || typeof page.evaluate !== 'function') return null;
  const warn = logger && typeof logger.warn === 'function' ? (s) => logger.warn(s) : () => {};
  try {
    const raw = await page.evaluate(() => {
      const read = (store) => {
        const out = {};
        try {
          for (let i = 0; i < store.length; i++) {
            const k = store.key(i);
            if (k) out[k] = store.getItem(k) ?? '';
          }
        } catch (_) {}
        return out;
      };
      return {
        localStorage: read(window.localStorage),
        sessionStorage: read(window.sessionStorage),
        v: 1,
      };
    });
    if (!raw || typeof raw !== 'object') return null;
    const payload = { localStorage: raw.localStorage || {}, sessionStorage: raw.sessionStorage || {}, v: 1 };
    const json = JSON.stringify(payload);
    const max = maxWebStorageJsonChars();
    if (json.length > max) {
      warn(`[ig-web-storage] snapshot ${json.length} chars exceeds INSTAGRAM_WEB_STORAGE_MAX_JSON=${max}; omitting web_storage`);
      return null;
    }
    return payload;
  } catch (e) {
    warn(`[ig-web-storage] capture failed: ${e && e.message ? e.message : String(e)}`);
    return null;
  }
}

/**
 * Navigate to www home (stable origin) then capture storage.
 * @param {import('puppeteer').Page} page
 * @param {{ warn?: (s: string) => void, log?: (s: string) => void }} [logger]
 */
async function navigateAndCaptureInstagramWebStorage(page, logger) {
  const warn = logger && typeof logger.warn === 'function' ? (s) => logger.warn(s) : () => {};
  try {
    await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 45000 });
  } catch (e) {
    warn(`[ig-web-storage] pre-capture goto: ${e && e.message ? e.message : String(e)}`);
  }
  await new Promise((r) => setTimeout(r, 1500));
  return captureInstagramWebStorageFromPage(page, logger);
}

/**
 * Apply saved storage on a page that is already on an instagram.com document (typically after setCookie + goto /).
 * @param {import('puppeteer').Page} page
 * @param {unknown} webStorage
 * @param {{ warn?: (s: string) => void }} [logger]
 */
async function applyInstagramWebStorageToPage(page, webStorage, logger) {
  if (!page || !webStorage || typeof webStorage !== 'object') return;
  const warn = logger && typeof logger.warn === 'function' ? (s) => logger.warn(s) : () => {};
  const u = String(page.url() || '').toLowerCase();
  if (!u.includes('instagram.com')) {
    warn('[ig-web-storage] apply skipped: not on instagram.com');
    return;
  }
  if (u.includes('/accounts/login')) {
    warn('[ig-web-storage] apply skipped: on login URL');
    return;
  }
  const ls = webStorage.localStorage && typeof webStorage.localStorage === 'object' ? webStorage.localStorage : {};
  const ss = webStorage.sessionStorage && typeof webStorage.sessionStorage === 'object' ? webStorage.sessionStorage : {};
  if (!Object.keys(ls).length && !Object.keys(ss).length) return;
  try {
    await page.evaluate(
      (payload) => {
        const apply = (store, map) => {
          for (const k of Object.keys(map || {})) {
            try {
              store.setItem(k, map[k] == null ? '' : String(map[k]));
            } catch (_) {}
          }
        };
        apply(window.localStorage, payload.localStorage || {});
        apply(window.sessionStorage, payload.sessionStorage || {});
      },
      { localStorage: ls, sessionStorage: ss }
    );
  } catch (e) {
    warn(`[ig-web-storage] apply failed: ${e && e.message ? e.message : String(e)}`);
  }
}

/**
 * @param {unknown} sessionData
 */
async function applyInstagramWebStorageFromSessionData(page, sessionData, logger) {
  if (!sessionData || typeof sessionData !== 'object') return;
  const ws = /** @type {{ web_storage?: unknown }} */ (sessionData).web_storage;
  await applyInstagramWebStorageToPage(page, ws, logger);
}

/**
 * @param {unknown[]} cookies
 * @param {unknown} webStorageSnapshot
 * @returns {{ cookies: unknown[], web_storage?: unknown }}
 */
function mergeInstagramSessionData(cookies, webStorageSnapshot) {
  const out = { cookies: Array.isArray(cookies) ? cookies : [] };
  if (webStorageSnapshot && typeof webStorageSnapshot === 'object' && !Array.isArray(webStorageSnapshot)) {
    out.web_storage = webStorageSnapshot;
  }
  return out;
}

module.exports = {
  captureInstagramWebStorageFromPage,
  navigateAndCaptureInstagramWebStorage,
  applyInstagramWebStorageToPage,
  applyInstagramWebStorageFromSessionData,
  mergeInstagramSessionData,
  maxWebStorageJsonChars,
};
