/**
 * Instagram scraper module – legacy Puppeteer implementation.
 *
 * This is the browser-based follower/following scraper used for the per-client
 * flow. Keep it conservative and keep the session/proxy tied to the client row.
 */
require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const fs = require('fs');
const {
  updateScrapeJob,
  retryScrapeJob,
  getScrapeJob,
  upsertLeadsBatch,
  getScrapeQuotaStatus,
  getConversationParticipantUsernames,
  getSentUsernames,
  getScrapeBlocklistUsernames,
  getMostRecentInstagramSessionForClient,
  normalizeSessionDataForPuppeteer,
  recordScraperActions,
  markInstagramSessionWebNeedsRefresh,
} = require('./database/supabase');
const logger = require('./utils/logger');
const { applyInstagramWebStorageFromSessionData } = require('./utils/instagram-web-storage');
const { applyMobileEmulation } = require('./utils/mobile-viewport');
const { applyProxyToLaunchOptions, authenticatePageForProxy } = require('./utils/proxy-puppeteer');
const {
  resolveHeadlessMode,
  baseChromeArgs,
  assignPersistentUserDataDir,
} = require('./utils/puppeteer-chrome-launch');
const {
  dismissInstagramPopups,
  detectInstagramPasswordReauthScreen,
} = require('./utils/instagram-modals');
const {
  clickElementNaturally,
  chance,
  idleMouseDrift,
  maybeLightStoryInteraction,
  moveMouseToElement,
  naturalScrollPage,
  organicPause,
  randomMouseDrift,
  viewStoriesNaturally,
} = require('./utils/human-interaction');

/** Log + persist failure (early returns used to only update the DB, so PM2 showed nothing after "claimed job"). */
async function failScrapeJob(jobId, errorMessage) {
  logger.error(`[Scraper] Job ${jobId} failed: ${errorMessage}`);
  await updateScrapeJob(jobId, { status: 'failed', error_message: errorMessage });
}

async function scraperPageLooksLoggedOut(page) {
  if (!page) return false;
  const u = page.url() || '';
  if (u.includes('/accounts/login')) return true;
  return detectInstagramPasswordReauthScreen(page);
}

/** Mark the per-client Instagram session as needing refresh and requeue the scrape job. */
async function requeueScrapeJobForLoggedOutInstagramSession(jobId, instagramSessionId, logPrefix) {
  const msg = 'Instagram web session logged out. Reconnect the sender session and retry scraping.';
  logger.warn(`[Scraper] ${logPrefix} ${msg}`);
  if (instagramSessionId) await markInstagramSessionWebNeedsRefresh(instagramSessionId).catch(() => {});
  await updateScrapeJob(jobId, {
    status: 'retry',
    available_at: new Date().toISOString(),
    error_message: msg,
    last_error_class: 'session_logged_out',
    instagram_session_id: null,
  });
}

async function completeScrapeJobForQuota(jobId, clientId, scrapedCount) {
  const quota = await getScrapeQuotaStatus(clientId);
  const msg = quota?.message || '1000 leads maximum reached, please wait for your scraping usage to reset.';
  logger.warn(`[Scraper] Job ${jobId} quota stop: ${msg}`);
  await updateScrapeJob(jobId, {
    status: 'completed',
    scraped_count: Math.max(0, Number(scrapedCount) || 0),
    error_message: msg,
  });
}

/** After a normal follower/following/comment run: do not overwrite user-cancelled status. */
async function finalizeScrapeJobNormalExit(jobId, scrapedCount) {
  const n = Math.max(0, Number(scrapedCount) || 0);
  const latest = await getScrapeJob(jobId);
  if (latest?.status === 'cancelled') {
    await updateScrapeJob(jobId, { scraped_count: n });
    logger.log(`[Scraper] Job ${jobId} ended while cancelled; updated scraped_count=${n}`);
    return;
  }
  await updateScrapeJob(jobId, { status: 'completed', scraped_count: n });
}

/**
 * Use the client's current Instagram session row. This is the same session the sender uses.
 */
async function resolvePuppeteerSessionForScrapeJob(clientId, jobId) {
  const job = await getScrapeJob(jobId);
  const igSession = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
  const normalized = normalizeSessionDataForPuppeteer(igSession?.session_data);
  let session = null;
  if (igSession && normalized) {
    session = {
      id: igSession.id,
      client_id: igSession.client_id,
      instagram_username: igSession.instagram_username,
      proxy_url: igSession.proxy_url || null,
      proxy_assignment_id: igSession.proxy_assignment_id || null,
      web_session_needs_refresh: igSession.web_session_needs_refresh ?? null,
      session_data: normalized,
    };
  }

  return { job, session, instagramSessionId: session?.id || null };
}

puppeteer.use(StealthPlugin());

const SCRAPE_DELAY_MIN_MS = 900;
const SCRAPE_DELAY_MAX_MS = 2200;
const LOAD_WAIT_MS = 1800;
const LOAD_WAIT_RETRIES = 3;
const SCROLL_CHUNKS_PER_ITER = 8;
const SCRAPE_SESSION_COOLDOWN_MS = Math.max(
  60 * 1000,
  parseInt(process.env.SCRAPE_SESSION_COOLDOWN_MS || String(5 * 60 * 1000), 10) || 5 * 60 * 1000
);
/** false | true | 'new' — set SCRAPER_HEADLESS=new for Chromium new headless. */
const HEADLESS = resolveHeadlessMode(process.env.SCRAPER_HEADLESS, true);
const SCRAPER_PERSIST_PROFILES =
  process.env.PUPPETEER_PERSIST_SCRAPER_PROFILES == null ||
  String(process.env.PUPPETEER_PERSIST_SCRAPER_PROFILES).trim() === '' ||
  (String(process.env.PUPPETEER_PERSIST_SCRAPER_PROFILES).toLowerCase() !== '0' &&
    String(process.env.PUPPETEER_PERSIST_SCRAPER_PROFILES).toLowerCase() !== 'false');

function buildScraperBrowserLaunchOptions(instagramSessionId, proxyUrl) {
  const launchOpts = {
    headless: HEADLESS,
    args: [...baseChromeArgs()],
  };
  if (SCRAPER_PERSIST_PROFILES && instagramSessionId) {
    assignPersistentUserDataDir(launchOpts, `scrape-client-${instagramSessionId}`);
    logger.log(`[Scraper] Persistent Chrome profile: scrape-client-${instagramSessionId}`);
  }
  applyProxyToLaunchOptions(launchOpts, proxyUrl || null);
  return launchOpts;
}
/** page.goto timeout; 30s is often too tight for IG + 2+ concurrent Chromium on a small VPS. */
const SCRAPER_NAV_TIMEOUT_MS = Math.max(15000, parseInt(process.env.SCRAPER_NAV_TIMEOUT_MS || '60000', 10) || 60000);
const LIST_SCRAPE_NAV_WAIT_UNTIL = (() => {
  const w = String(process.env.SCRAPER_LIST_NAV_WAIT_UNTIL || 'domcontentloaded').trim().toLowerCase();
  if (w === 'networkidle2' || w === 'networkidle0' || w === 'load' || w === 'domcontentloaded') return w;
  return 'domcontentloaded';
})();

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function randomDelay(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
}

/**
 * Instagram sometimes paints an almost-empty profile (only the Messages pill / chrome) while the URL
 * still looks like a profile — common when the tab is starved next to another Chromium instance.
 */
async function instagramListProfilePageLooksBroken(page) {
  try {
    return await page.evaluate(() => {
      const links = document.querySelectorAll('a[href*="/followers"], a[href*="/following"]');
      if (links.length > 0) return false;
      const t = ((document.body && document.body.innerText) || '').replace(/\s+/g, ' ').trim();
      if (t.length > 120) return false;
      return true;
    });
  } catch (_) {
    return false;
  }
}

async function recoverBlankInstagramProfilePage(page, cleanTarget, logger, reasonLabel) {
  if (logger) {
    logger.log(
      `[Scraper] ${reasonLabel}: page has no follower/following links and little text — ` +
        'Escape + hard profile reload'
    );
  }
  for (let i = 0; i < 4; i++) {
    await page.keyboard.press('Escape').catch(() => {});
    await delay(150);
  }
  await delay(randomDelay(400, 1000));
  const profileUrl = `https://www.instagram.com/${encodeURIComponent(cleanTarget)}/`;
  const nav = {
    waitUntil: LIST_SCRAPE_NAV_WAIT_UNTIL,
    timeout: SCRAPER_NAV_TIMEOUT_MS,
  };
  try {
    await page.goto(profileUrl, nav);
  } catch (e) {
    if (logger) logger.warn('[Scraper] Profile recovery goto failed: ' + (e.message || e));
    await page.reload(nav).catch(() => {});
  }
  await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
  await dismissInstagramPopups(page, logger).catch(() => {});
}

/**
 * Log in to Instagram with credentials, return session (cookies only).
 * Password is never stored.
 */
async function connectScraper(instagramUsername, instagramPassword) {
  const browser = await puppeteer.launch({
    headless: HEADLESS,
    args: baseChromeArgs(),
  });
  try {
    const page = await browser.newPage();
    await applyMobileEmulation(page);
    const { login } = require('./bot');
    await login(page, { username: instagramUsername, password: instagramPassword });
    const cookies = await page.cookies();
    return { cookies, username: instagramUsername };
  } finally {
    await browser.close().catch(() => {});
  }
}

async function getInstagramListScrollTargetHandle(page) {
  return page.evaluateHandle(() => {
    function countProfileLinks(el) {
      let c = 0;
      for (const a of el.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]')) {
        const href = (a.getAttribute('href') || '').trim();
        const m = href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/) || href.match(/instagram\.com\/([A-Za-z0-9._]{2,30})(?:\/|\?|#|$)/i);
        if (m && /^[a-z0-9._]{2,30}$/i.test(m[1])) c++;
      }
      return c;
    }

    let dialog = null;
    let bestCount = 0;
    for (const d of document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]')) {
      const count = countProfileLinks(d);
      if (count > bestCount && count >= 5) {
        bestCount = count;
        dialog = d;
      }
    }
    if (bestCount < 5) {
      for (const d of document.querySelectorAll('div')) {
        if (d.clientHeight < 80) continue;
        const count = countProfileLinks(d);
        if (count > bestCount && count >= 10) {
          bestCount = count;
          dialog = d;
        }
      }
    }
    if (!dialog) return null;

    let scrollTarget = null;
    let bestScore = -1;
    for (const el of [dialog, ...dialog.querySelectorAll('div')]) {
      const hasOverflow = el.scrollHeight > el.clientHeight + 16;
      if (!hasOverflow || el.clientHeight <= 80) continue;
      const score = countProfileLinks(el) * 1000 + (el.scrollHeight - el.clientHeight);
      if (score > bestScore) {
        bestScore = score;
        scrollTarget = el;
      }
    }
    return scrollTarget || dialog;
  });
}

async function tryOpenProfileList(page, target, kind) {
  return page.evaluate((targetUsername, listKind) => {
    const lower = (s) => (s || '').toLowerCase();
    const wantFollowing = listKind === 'following';

    const links = Array.from(document.querySelectorAll('a[href*="/followers"], a[href*="/following"]'));
    const picked = links.find((a) => {
      const href = lower(a.getAttribute('href') || '');
      if (wantFollowing) {
        return href.includes(`/${targetUsername}/following`) || (href.includes('/following') && !href.includes('/followers'));
      }
      return href.includes(`/${targetUsername}/followers`) || (href.includes('/followers') && !href.includes('/following'));
    });
    if (picked) {
      picked.click();
      return true;
    }

    const candidates = Array.from(document.querySelectorAll('a, span, div, button, [role="button"]'));
    const needle = wantFollowing ? 'following' : 'followers';
    const statsLike = candidates.find((el) => {
      const text = (el.textContent || '').trim();
      const l = lower(text);
      return l.includes(needle) && /\d/.test(text);
    });
    if (statsLike) {
      const clickable = statsLike.closest('a, button, [role="button"]') || statsLike;
      if (clickable && clickable instanceof HTMLElement) {
        clickable.click();
        return true;
      }
    }

    const roleButtons = Array.from(document.querySelectorAll('[role="button"], button, a'));
    for (const btn of roleButtons) {
      const t = lower(btn.textContent || '');
      if (wantFollowing) {
        if (t.includes('following') || /\d+\s*following/.test(t)) {
          btn.click();
          return true;
        }
      } else if (t.includes('followers') || /\d+\s*followers/.test(t)) {
        btn.click();
        return true;
      }
    }

    return false;
  }, target, kind);
}

async function getInstagramListModalSnapshot(page) {
  return page.evaluate(() => {
    function countProfileLinks(el) {
      let c = 0;
      for (const a of el.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]')) {
        const href = (a.getAttribute('href') || '').trim();
        const m = href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/) || href.match(/instagram\.com\/([A-Za-z0-9._]{2,30})(?:\/|\?|#|$)/i);
        if (m && /^[a-z0-9._]{2,30}$/i.test(m[1])) c++;
      }
      return c;
    }

    let dialog = null;
    let bestCount = 0;
    for (const d of document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]')) {
      const count = countProfileLinks(d);
      if (count > bestCount && count >= 5) {
        bestCount = count;
        dialog = d;
      }
    }
    if (bestCount < 5) {
      for (const d of document.querySelectorAll('div')) {
        if (d.clientHeight < 80) continue;
        const count = countProfileLinks(d);
        if (count > bestCount && count >= 10) {
          bestCount = count;
          dialog = d;
        }
      }
    }
    if (!dialog) return null;

    let scrollTarget = null;
    let bestScore = -1;
    for (const el of [dialog, ...dialog.querySelectorAll('div')]) {
      const hasOverflow = el.scrollHeight > el.clientHeight + 16;
      if (!hasOverflow || el.clientHeight <= 80) continue;
      const score = countProfileLinks(el) * 1000 + (el.scrollHeight - el.clientHeight);
      if (score > bestScore) {
        bestScore = score;
        scrollTarget = el;
      }
    }

    const target = scrollTarget || dialog;
    return {
      scrollTop: Math.round(target.scrollTop || 0),
      remaining: Math.max(0, Math.round((target.scrollHeight || 0) - (target.clientHeight || 0) - (target.scrollTop || 0))),
      linkCount: countProfileLinks(target),
    };
  }).catch(() => null);
}

async function getVisibleInstagramListUsernameHandles(page, scrollTargetHandle, limit = 4) {
  if (!scrollTargetHandle) return [];
  const anchors = await scrollTargetHandle.$$('a[href^="/"], a[href*="instagram.com/"]').catch(() => []);
  const picked = [];
  const seen = new Set();
  try {
    for (const anchor of anchors) {
      const href = await anchor.evaluate((el) => el.getAttribute('href') || '').catch(() => '');
      const match =
        href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/) ||
        href.match(/instagram\.com\/([A-Za-z0-9._]{2,30})(?:\/|\?|#|$)/i);
      const username = (match && match[1] ? match[1] : '').toLowerCase();
      if (!username || seen.has(username)) {
        await anchor.dispose().catch(() => {});
        continue;
      }
      const box = await anchor.boundingBox().catch(() => null);
      if (!box || box.width < 8 || box.height < 8 || box.y < 0 || box.y > 760) {
        await anchor.dispose().catch(() => {});
        continue;
      }
      seen.add(username);
      picked.push(anchor);
      if (picked.length >= limit) break;
    }
  } finally {
    for (const anchor of anchors) {
      if (!picked.includes(anchor)) await anchor.dispose().catch(() => {});
    }
  }
  return picked;
}

async function maybeHoverVisibleInstagramUsernames(page, scrollTargetHandle, opts = {}) {
  if (!scrollTargetHandle || Math.random() >= (opts.probability ?? 0.35)) return false;
  const handles = await getVisibleInstagramListUsernameHandles(page, scrollTargetHandle, randomDelay(2, 4));
  if (!handles.length) return false;
  try {
    for (const handle of handles) {
      await moveMouseToElement(page, handle, { totalDurationMs: randomDelay(320, 900) }).catch(() => {});
      await delay(randomDelay(1200, 3600));
    }
    return true;
  } finally {
    for (const handle of handles) await handle.dispose().catch(() => {});
  }
}

async function humanScrollInstagramListModal(page, opts = {}) {
  if (!page) return false;
  const targetHandleRef = await getInstagramListScrollTargetHandle(page).catch(() => null);
  const targetHandle = targetHandleRef?.asElement ? targetHandleRef.asElement() : null;
  if (!targetHandle) {
    await targetHandleRef?.dispose?.().catch(() => {});
    return false;
  }
  try {
    const beforeTop = await targetHandle.evaluate((el) => el.scrollTop).catch(() => 0);
    await moveMouseToElement(page, targetHandle, { totalDurationMs: randomDelay(220, 560) }).catch(() => {});
    if (Math.random() < 0.3) {
      await clickElementNaturally(page, targetHandle, { totalDurationMs: randomDelay(180, 360) }).catch(() => {});
    }
    if (Math.random() < 0.45) {
      await randomMouseDrift(page, { totalDurationMs: randomDelay(320, 880) }).catch(() => {});
    }
    await maybeHoverVisibleInstagramUsernames(page, targetHandle, { probability: 0.42 }).catch(() => {});
    const rounds = Math.max(1, opts.rounds || randomDelay(2, 4));
    let moved = false;
    for (let i = 0; i < rounds; i++) {
      const stepResult = await targetHandle.evaluate(
        async (el, config) => {
          const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
          let didMove = false;
          for (let step = 0; step < config.steps; step++) {
            const maxScroll = Math.max(0, el.scrollHeight - el.clientHeight);
            if (maxScroll <= 0) break;
            const remaining = Math.max(0, maxScroll - el.scrollTop);
            if (remaining <= 3) break;
            const prev = el.scrollTop;
            const delta = Math.min(remaining, config.baseStep + Math.round(Math.random() * config.stepVariance));
            el.scrollTop = Math.min(maxScroll, prev + delta);
            if (Math.abs(el.scrollTop - prev) > 1) didMove = true;
            await sleep(config.stepPauseMin + Math.round(Math.random() * (config.stepPauseMax - config.stepPauseMin)));
          }
          return { didMove, top: Math.round(el.scrollTop || 0) };
        },
        {
          steps: randomDelay(5, 10),
          baseStep: randomDelay(90, 180),
          stepVariance: randomDelay(30, 110),
          stepPauseMin: 140,
          stepPauseMax: 360,
        }
      ).catch(() => ({ didMove: false, top: beforeTop }));
      if (stepResult.didMove) moved = true;
      if (Math.random() < 0.25) {
        await randomMouseDrift(page, { totalDurationMs: randomDelay(260, 720) }).catch(() => {});
      }
      await maybeHoverVisibleInstagramUsernames(page, targetHandle, { probability: 0.25 }).catch(() => {});
      await organicPause('scroll', 0.8);
    }
    const afterTop = await targetHandle.evaluate((el) => el.scrollTop).catch(() => beforeTop);
    return moved || Math.abs(afterTop - beforeTop) > 2;
  } finally {
    await targetHandle.dispose().catch(() => {});
    await targetHandleRef.dispose().catch(() => {});
  }
}

async function pushInstagramListModalAndWaitForLoad(page, opts = {}) {
  if (!page) return { moved: false, loadedMore: false, exhausted: false };

  const maxWaitMs = opts.maxWaitMs ?? randomDelay(2500, 5000);
  const bottomOffsetPx = opts.bottomOffsetPx ?? randomDelay(120, 260);

  const before = await page.evaluate(() => {
    function countProfileLinks(el) {
      let c = 0;
      for (const a of el.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]')) {
        const href = (a.getAttribute('href') || '').trim();
        const m =
          href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/) ||
          href.match(/instagram\.com\/([A-Za-z0-9._]{2,30})(?:\/|\?|#|$)/i);
        if (m && /^[a-z0-9._]{2,30}$/i.test(m[1])) c++;
      }
      return c;
    }

    function rootHasSpinner(root) {
      return !!root.querySelector('svg[aria-label*="Loading" i], [role="progressbar"]');
    }

    let dialog = null;
    let bestCount = 0;
    for (const d of document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]')) {
      const count = countProfileLinks(d);
      if (count > bestCount && count >= 5) {
        bestCount = count;
        dialog = d;
      }
    }
    if (!dialog) return null;

    let target = null;
    let bestScore = -1;
    for (const el of [dialog, ...dialog.querySelectorAll('div')]) {
      const hasOverflow = el.scrollHeight > el.clientHeight + 16;
      if (!hasOverflow || el.clientHeight <= 80) continue;
      const score = countProfileLinks(el) * 1000 + (el.scrollHeight - el.clientHeight);
      if (score > bestScore) {
        bestScore = score;
        target = el;
      }
    }
    target = target || dialog;

    return {
      scrollTop: target.scrollTop || 0,
      scrollHeight: target.scrollHeight || 0,
      clientHeight: target.clientHeight || 0,
      linkCount: countProfileLinks(target),
      spinner: rootHasSpinner(target),
    };
  }).catch(() => null);

  if (!before) return { moved: false, loadedMore: false, exhausted: true };

  const targetHandleRef = await getInstagramListScrollTargetHandle(page).catch(() => null);
  const targetHandle = targetHandleRef?.asElement ? targetHandleRef.asElement() : null;
  if (!targetHandle) {
    await targetHandleRef?.dispose?.().catch(() => {});
    return { moved: false, loadedMore: false, exhausted: true };
  }

  try {
    await moveMouseToElement(page, targetHandle, { totalDurationMs: randomDelay(180, 420) }).catch(() => {});

    const pushResult = await targetHandle.evaluate((el, offsetPx) => {
      const maxScroll = Math.max(0, el.scrollHeight - el.clientHeight);
      const prev = el.scrollTop || 0;
      const target = Math.max(0, maxScroll - offsetPx);
      el.scrollTop = target;
      return {
        prev,
        now: el.scrollTop || 0,
        maxScroll,
      };
    }, bottomOffsetPx).catch(() => null);

    const moved = !!pushResult && Math.abs((pushResult.now || 0) - (pushResult.prev || 0)) > 2;

    const start = Date.now();
    let loadedMore = false;
    let exhausted = false;

    while (Date.now() - start < maxWaitMs) {
      await delay(randomDelay(400, 850));

      const after = await targetHandle.evaluate((el) => {
        function countProfileLinks(root) {
          let c = 0;
          for (const a of root.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]')) {
            const href = (a.getAttribute('href') || '').trim();
            const m =
              href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/) ||
              href.match(/instagram\.com\/([A-Za-z0-9._]{2,30})(?:\/|\?|#|$)/i);
            if (m && /^[a-z0-9._]{2,30}$/i.test(m[1])) c++;
          }
          return c;
        }

        function rootHasSpinner(root) {
          return !!root.querySelector('svg[aria-label*="Loading" i], [role="progressbar"]');
        }

        return {
          scrollTop: el.scrollTop || 0,
          scrollHeight: el.scrollHeight || 0,
          clientHeight: el.clientHeight || 0,
          linkCount: countProfileLinks(el),
          spinner: rootHasSpinner(el),
        };
      }).catch(() => null);

      if (!after) break;

      if (after.scrollHeight > before.scrollHeight + 20 || after.linkCount > before.linkCount) {
        loadedMore = true;
        break;
      }

      const remaining = Math.max(0, after.scrollHeight - after.clientHeight - after.scrollTop);
      if (remaining <= 5 && !after.spinner) {
        exhausted = true;
      }
    }

    return { moved, loadedMore, exhausted };
  } finally {
    await targetHandle.dispose().catch(() => {});
    await targetHandleRef.dispose().catch(() => {});
  }
}

async function closeInstagramListModal(page) {
  const closeHandle = await page
    .$([
      '[role="dialog"] button[aria-label="Close"]',
      '[role="dialog"] svg[aria-label="Close"]',
      '[role="dialog"] [role="button"][aria-label*="close" i]',
    ].join(', '))
    .catch(() => null);
  if (closeHandle) {
    try {
      await clickElementNaturally(page, closeHandle, { totalDurationMs: randomDelay(220, 520) }).catch(() => {});
      await delay(randomDelay(900, 2200));
      return true;
    } finally {
      await closeHandle.dispose().catch(() => {});
    }
  }
  await page.keyboard.press('Escape').catch(() => {});
  await delay(randomDelay(900, 2200));
  return true;
}

async function maybeRefreshInstagramListModal(page, cleanTarget, listKind, logger, reason) {
  if (!chance(0.6)) return false;
  logger.log(`[Scraper] Refreshing ${listKind} modal after stall: ${reason}`);
  await closeInstagramListModal(page).catch(() => {});
  await delay(randomDelay(5000, 11000));
  await dismissInstagramPopups(page, logger).catch(() => {});
  const reopened = await tryOpenProfileList(page, cleanTarget, listKind).catch(() => false);
  if (!reopened) return false;
  await delay(randomDelay(4000, 9000));
  await humanScrollInstagramListModal(page, { rounds: randomDelay(1, 2) }).catch(() => false);
  return true;
}

async function runScrapeWarmupPattern(page, patternName) {
  if (patternName === 'stories-first') {
    await randomMouseDrift(page, { totalDurationMs: randomDelay(260, 620) });
    await maybeLightStoryInteraction(page, { openChance: 0.18 }).catch(() => {});
    await organicPause('between_actions', 1.9);
    await naturalScrollPage(page, { rounds: randomDelay(2, 4), pauseMultiplier: 0.85 });
    await organicPause('between_actions', 1.4);
    await randomMouseDrift(page, { totalDurationMs: randomDelay(260, 540) });
    return;
  }

  if (patternName === 'idle-heavy') {
    await idleMouseDrift(page, { durationMs: randomDelay(9000, 18000) }).catch(() => {});
    await naturalScrollPage(page, { rounds: randomDelay(1, 3), pauseMultiplier: 0.75 });
    if (Math.random() < 0.45) {
      await naturalScrollPage(page, { rounds: 1, direction: 'up', pauseMultiplier: 0.55 });
    }
    await maybeLightStoryInteraction(page, { openChance: 0.1 }).catch(() => {});
    return;
  }

  await organicPause('between_actions', 1.6);
  for (let i = 0; i < 2 + Math.floor(Math.random() * 2); i++) {
    await randomMouseDrift(page, { totalDurationMs: randomDelay(180, 420) });
    await naturalScrollPage(page, { rounds: randomDelay(2, 4), pauseMultiplier: 0.9 });
    if (Math.random() < 0.35) {
      await naturalScrollPage(page, { rounds: 1, direction: 'up', pauseMultiplier: 0.6 });
    }
    await maybeLightStoryInteraction(page, { openChance: 0.12 }).catch(() => {});
    await organicPause('between_actions', 1.8);
  }
}

async function getVisibleHandles(page, selector, limit) {
  const handles = await page.$$(selector).catch(() => []);
  const picked = [];
  try {
    for (const handle of handles) {
      const box = await handle.boundingBox().catch(() => null);
      if (!box || box.width < 12 || box.height < 12 || box.y < 0 || box.y > 760) {
        await handle.dispose().catch(() => {});
        continue;
      }
      picked.push(handle);
      if (picked.length >= limit) break;
    }
  } finally {
    for (const handle of handles) {
      if (!picked.includes(handle)) await handle.dispose().catch(() => {});
    }
  }
  return picked;
}

async function maybeBrowseProfilePosts(page) {
  if (!chance(randomDelay(15, 25) / 100)) return;
  await naturalScrollPage(page, { rounds: randomDelay(2, 4), pauseMultiplier: 0.8 }).catch(() => {});
  await delay(randomDelay(4000, 9000));
  const postHandles = await getVisibleHandles(page, 'a[href*="/p/"], a[href*="/reel/"]', randomDelay(1, 2));
  try {
    for (const postHandle of postHandles) {
      await moveMouseToElement(page, postHandle, { totalDurationMs: randomDelay(420, 1100) }).catch(() => {});
      await delay(randomDelay(5000, 12000));
    }
  } finally {
    for (const handle of postHandles) await handle.dispose().catch(() => {});
  }
}

async function maybeBrowseProfileComments(page) {
  if (!chance(0.18)) return;
  const postHandles = await getVisibleHandles(page, 'a[href*="/p/"], a[href*="/reel/"]', 1);
  if (!postHandles.length) return;
  try {
    await clickElementNaturally(page, postHandles[0], { totalDurationMs: randomDelay(260, 620) }).catch(() => {});
    await delay(randomDelay(2500, 5000));
    const commentHandles = await getVisibleHandles(
      page,
      '[role="dialog"] ul ul span, [role="dialog"] h1 ~ div span',
      randomDelay(1, 2)
    );
    try {
      for (const commentHandle of commentHandles) {
        await moveMouseToElement(page, commentHandle, { totalDurationMs: randomDelay(420, 980) }).catch(() => {});
        await delay(randomDelay(3500, 9000));
      }
    } finally {
      for (const handle of commentHandles) await handle.dispose().catch(() => {});
    }
    await closeInstagramListModal(page).catch(() => {});
  } finally {
    for (const handle of postHandles) await handle.dispose().catch(() => {});
  }
}

async function maybeIdleOnProfile(page) {
  if (!chance(0.28)) return;
  await idleMouseDrift(page, {
    durationMs: randomDelay(20000, 60000),
    segmentDurationMs: randomDelay(700, 2200),
  }).catch(() => {});
}

async function maybeBrowseTargetProfile(page) {
  if (chance(0.42)) {
    await viewStoriesNaturally(page, {
      minStories: 2,
      maxStories: 4,
      minViewMs: 8000,
      maxViewMs: 20000,
    }).catch(() => {});
    await delay(randomDelay(3000, 7000));
  }
  await maybeBrowseProfilePosts(page).catch(() => {});
  await maybeBrowseProfileComments(page).catch(() => {});
  await maybeIdleOnProfile(page).catch(() => {});
}

/**
 * Run follower or following list scrape in the background. Call from API without awaiting.
 * Loads scraper session, navigates to profile, paginates the modal list, upserts leads.
 * @param {number} [options.maxLeads] - Optional. Stop when this many NEW leads have been added. Omit for no limit.
 * @param {'followers'|'following'} [options.listKind] - Default 'followers'. Use 'following' for accounts the target follows.
 */
async function runFollowerScrape(clientId, jobId, targetUsername, options = {}) {
  const maxLeads = options.maxLeads != null ? Math.max(1, parseInt(options.maxLeads, 10) || 0) : null;
  const listKind = options.listKind === 'following' ? 'following' : 'followers';
  const leadGroupId = options.leadGroupId || null;
  const leaseOptions = options.leaseOptions || null;
  const sbMod = require('./database/supabase');
  const sb = sbMod.getSupabase();
  if (!sb || !clientId || !jobId) {
    logger.error('[Scraper] Missing clientId or jobId');
    return;
  }

  let leaseHbTimer = null;
  if (leaseOptions?.jobId && leaseOptions?.workerId) {
    const sec = Math.max(60, parseInt(leaseOptions.leaseSec || process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
    leaseHbTimer = setInterval(() => {
      sbMod.heartbeatScrapeJobLease(leaseOptions.jobId, leaseOptions.workerId, sec).catch(() => {});
    }, Math.min(120000, Math.max(30000, sec * 250)));
  }

  let browser;
  let page = null;
  let scrapedSessionId = null;
  try {
    const { job, session, instagramSessionId } = await resolvePuppeteerSessionForScrapeJob(clientId, jobId);
    scrapedSessionId = instagramSessionId || null;
    if (!session?.session_data?.cookies?.length) {
      const n = Array.isArray(session?.session_data?.cookies) ? session.session_data.cookies.length : 0;
      logger.error(
        `[Scraper] Job ${jobId} no Puppeteer cookies on current Instagram session: clientId=${clientId} ` +
          `session_row=${session ? 'yes' : 'no'} cookie_count=${n}`
      );
      await retryScrapeJob(
        jobId,
        'No current Instagram session with usable Puppeteer cookies.',
        60
      ).catch(async () => {
        await failScrapeJob(
          jobId,
          'No current Instagram session with usable Puppeteer cookies.'
        );
      });
      return;
    }

    logger.log(`[Scraper] Job ${jobId} session OK; launching browser for ${listKind} scrape`);

    browser = await puppeteer.launch(buildScraperBrowserLaunchOptions(instagramSessionId, session.proxy_url));
    page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url || null);
    const useMobile = process.env.SCRAPER_USE_MOBILE === '1' || process.env.SCRAPER_USE_MOBILE === 'true';
    if (useMobile) {
      await applyMobileEmulation(page);
      logger.log('[Scraper] Using mobile viewport (SCRAPER_USE_MOBILE=1)');
    } else {
      await page.setViewport({ width: 1280, height: 800 });
      logger.log('[Scraper] Using desktop viewport for profile list scrape (mobile scroll fails)');
    }
    await page.setCookie(...session.session_data.cookies);
    await page.goto('https://www.instagram.com/', {
      waitUntil: LIST_SCRAPE_NAV_WAIT_UNTIL,
      timeout: SCRAPER_NAV_TIMEOUT_MS,
    });
    await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
    await delay(randomDelay(1500, 3500));

    if (await scraperPageLooksLoggedOut(page)) {
      await requeueScrapeJobForLoggedOutInstagramSession(jobId, instagramSessionId, 'follower scrape home');
      return;
    }

    // Dismiss any blocking popups that appear on home page load.
    await dismissInstagramPopups(page, logger).catch(() => {});

    logger.log('[Scraper] Warming session before scrape...');
    const warmupPatterns = ['baseline', 'stories-first', 'idle-heavy'];
    const chosenWarmup = warmupPatterns[randomDelay(0, warmupPatterns.length - 1)];
    logger.log(`[Scraper] Warmup pattern: ${chosenWarmup}`);
    await runScrapeWarmupPattern(page, chosenWarmup).catch(() => {});
    await randomMouseDrift(page, { totalDurationMs: randomDelay(180, 380) });
    await organicPause('between_actions', 1.2);
    logger.log('[Scraper] Warm behaviour done.');

    const source = `${listKind}:${targetUsername}`;
    const cleanTarget = targetUsername.replace(/^@/, '').trim().toLowerCase();
    logger.log(`[Scraper] Starting ${listKind} scrape for @${cleanTarget}${maxLeads ? ` (max ${maxLeads})` : ''}`);

    await page.goto(`https://www.instagram.com/${encodeURIComponent(cleanTarget)}/`, {
      waitUntil: LIST_SCRAPE_NAV_WAIT_UNTIL,
      timeout: SCRAPER_NAV_TIMEOUT_MS,
    });
    await organicPause('between_actions', 1.7);

    // Dismiss cookies, account-switcher "Continue", notifications, and terms dialogs.
    await dismissInstagramPopups(page, logger).catch(() => {});
    await naturalScrollPage(page, { rounds: randomDelay(1, 2), pauseMultiplier: 0.8 }).catch(() => {});
    if (Math.random() < 0.28) {
      await naturalScrollPage(page, { rounds: 1, direction: 'up', pauseMultiplier: 0.55 }).catch(() => {});
    }
    await maybeBrowseTargetProfile(page).catch(() => {});
    await delay(randomDelay(8000, 18000));

    if (await instagramListProfilePageLooksBroken(page)) {
      await recoverBlankInstagramProfilePage(
        page,
        cleanTarget,
        logger,
        'After profile navigation'
      );
    }

    const jobCheck = await getScrapeJob(jobId);
    if (jobCheck?.status === 'cancelled') return;

    const profileStatCount = await page.evaluate((target, kind) => {
      function parseCount(str) {
        if (!str || typeof str !== 'string') return null;
        const raw = str.replace(/,/g, '').trim();
        const m = raw.match(/([\d.]+)\s*(k|m)?/i);
        if (!m) return null;
        let n = parseFloat(m[1], 10);
        if (m[2] === 'k' || m[2] === 'K') n *= 1000;
        else if (m[2] === 'm' || m[2] === 'M') n *= 1000000;
        return Math.floor(n);
      }
      const following = kind === 'following';
      const links = Array.from(
        document.querySelectorAll(following ? 'a[href*="/following"]' : 'a[href*="/followers"]')
      );
      const statLink = links.find(function (a) {
        const href = (a.getAttribute('href') || '').toLowerCase();
        if (following) {
          return href.indexOf('/' + target + '/following') !== -1;
        }
        return href.indexOf('/' + target + '/followers') !== -1;
      });
      if (!statLink) return null;
      const container = statLink.closest('li') || statLink.parentElement;
      if (!container) return null;
      const titleEl = container.querySelector('[title]');
      const span = container.querySelector('span');
      let n = parseCount((titleEl && titleEl.getAttribute('title')) || (span && span.getAttribute('title')));
      if (n != null) return n;
      const txt = (container.textContent || '').replace(/,/g, '');
      n = parseCount(txt);
      if (n != null) return n;
      n = parseCount(statLink.getAttribute('aria-label'));
      if (n != null) return n;
      return parseCount(statLink.textContent);
    }, cleanTarget, listKind);

    const effectiveMax =
      profileStatCount != null && profileStatCount > 0
        ? (maxLeads ? Math.min(maxLeads, profileStatCount) : profileStatCount)
        : maxLeads;
    if (profileStatCount != null) {
      logger.log(
        `[Scraper] Profile has ${profileStatCount} ${listKind === 'following' ? 'following' : 'followers'}; capping at ${effectiveMax}`
      );
    } else {
      logger.log('[Scraper] Could not parse stat count from profile; using max_leads only');
    }

    let profileListOpened = await tryOpenProfileList(page, cleanTarget, listKind);
    if (!profileListOpened) {
      if (await instagramListProfilePageLooksBroken(page)) {
        await recoverBlankInstagramProfilePage(
          page,
          cleanTarget,
          logger,
          'Before follower/following link retry'
        );
        profileListOpened = await tryOpenProfileList(page, cleanTarget, listKind);
      }
    }
    if (!profileListOpened) {
      try {
        await dismissInstagramPopups(page, logger);
        await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
      } catch (e) {
        logger.warn('[Scraper] Failed to dismiss popups before retry: ' + e.message);
      }
      profileListOpened = await tryOpenProfileList(page, cleanTarget, listKind);
    }
    if (!profileListOpened) {
      await recoverBlankInstagramProfilePage(
        page,
        cleanTarget,
        logger,
        'Final attempt before modal failure'
      );
      profileListOpened = await tryOpenProfileList(page, cleanTarget, listKind);
    }

    if (!profileListOpened) {
      // Diagnose what Instagram actually showed so we can give a precise error.
      let diagUrl = '';
      let diagText = '';
      try {
        diagUrl = page.url();
        diagText = await page.evaluate(() =>
          ((document.body && document.body.innerText) || '').slice(0, 400).replace(/\s+/g, ' ').trim()
        ).catch(() => '');
        logger.error(`[Scraper] Modal open failed. url=${diagUrl} page_text="${diagText}"`);
      } catch (_) {}

      try {
        const debugDir = path.join(process.cwd(), 'scraper-debug');
        if (!fs.existsSync(debugDir)) fs.mkdirSync(debugDir, { recursive: true });
        const tag = listKind === 'following' ? 'following' : 'followers';
        const screenshotPath = path.join(debugDir, `${tag}_modal_fail_${String(jobId)}.png`);
        await page.screenshot({ path: screenshotPath, fullPage: true });
        logger.error(`[Scraper] Screenshot saved: ${screenshotPath}`);
      } catch (screenshotErr) {
        logger.error(`[Scraper] Failed to capture screenshot: ${screenshotErr.message}`);
      }

      // Detect rate limiting (429) specifically.
      const is429 =
        diagUrl.includes('chrome-error://') ||
        /429|rate.?limit|too many request/i.test(diagText);

      if (is429) {
        // Re-queue the job with a fresh session slot so it retries automatically
        // once another (non-rate-limited) session is available.  Clear the
        // platform_scraper_session_id so the worker reserves a different one.
        logger.error(
          `[Scraper] Job ${jobId} rate-limited (429) by Instagram — re-queuing with fresh session`
        );
        await updateScrapeJob(jobId, {
          status: 'retry',
          available_at: new Date().toISOString(), // available immediately
          error_message: 'Instagram rate limit (429). Re-queued for a different session.',
          last_error_class: 'rate_limited_429',
          platform_scraper_session_id: null, // force fresh session on next claim
        });
      } else {
        const modalFailMsg =
          listKind === 'following'
            ? 'Could not open following list. Profile may be private, or the link was not found.'
            : 'Could not open followers list. Profile may be private, or the link was not found.';
        await updateScrapeJob(jobId, {
          status: 'failed',
          error_message: modalFailMsg,
          last_error_class: 'modal_open_failed',
        });
        logger.error(`[Scraper] Job ${jobId} failed: ${modalFailMsg}`);
      }
      return;
    }

    logger.log(`[Scraper] ${listKind === 'following' ? 'Following' : 'Followers'} modal opened, extracting...`);
    await delay(randomDelay(2500, 5000));
    await humanScrollInstagramListModal(page, { rounds: randomDelay(1, 2) }).catch(() => false);

    const saveLoginHandled = await page.evaluate(function () {
      const dialogs = document.querySelectorAll('[role="dialog"]');
      let saveLoginDialog = null;
      for (let i = 0; i < dialogs.length; i++) {
        const d = dialogs[i];
        const txt = (d.textContent || '').toLowerCase();
        if (txt.indexOf('save your login info') !== -1 && txt.indexOf('not now') !== -1) {
          saveLoginDialog = d;
          break;
        }
      }
      if (saveLoginDialog) {
        const saveInfo = Array.from(saveLoginDialog.querySelectorAll('span, div[role="button"], button')).find(function (el) {
          return (el.textContent || '').trim().toLowerCase() === 'save info';
        });
        if (saveInfo) {
          const btn = saveInfo.closest('[role="button"]') || saveInfo.closest('button') || saveInfo;
          if (btn) { btn.click(); return true; }
        }
      }
      return false;
    });
    if (saveLoginHandled) {
      logger.log('[Scraper] Clicked Save info on login dialog');
      await delay(randomDelay(1000, 2000));
    }

    /** Rows newly inserted into cold_dm_leads this job (not DOM usernames seen). */
    let newInsertsTotal = 0;
    const seenUsernames = new Set();
    let noNewCount = 0;
    // To avoid hammering one account with huge follower scrapes, insert a long cooldown
    // after scraping a large chunk. Defaults: pause after each 1000 new leads.
    const COOLDOWN_CHUNK = parseInt(process.env.SCRAPER_FOLLOWER_COOLDOWN_CHUNK || '1000', 10);
    const COOLDOWN_MIN_MS = 45 * 60 * 1000;
    const COOLDOWN_MAX_MS = 70 * 60 * 1000;
    let scrapedSinceCooldown = 0;
    const MAX_NO_NEW = profileStatCount != null && profileStatCount > 100 ? 12 : 6;
    const [inConvos, sentUsernames, blocklistUsernames] = await Promise.all([
      getConversationParticipantUsernames(clientId),
      getSentUsernames(clientId),
      getScrapeBlocklistUsernames(clientId),
    ]);
    const BLACKLIST = new Set([
      'explore', 'direct', 'accounts', 'reels', 'stories', 'p', 'tv', 'tags',
      'developer', 'about', 'blog', 'jobs', 'help', 'api', 'privacy', 'terms',
    ]);
    let scrollCount = 0;
    let lastModalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
    let stuckModalCount = 0;
    let loopIter = 0;
    let exhaustedConfirmCount = 0;
    let detachedFrameRetryUsed = false;

    while (true) {
      loopIter++;
      logger.log(
        `[Scraper][Loop ${loopIter}] Start: total=${newInsertsTotal} noNew=${noNewCount}/${MAX_NO_NEW} scrollCount=${scrollCount}`
      );
      const jobCheck = await getScrapeJob(jobId);
      if (jobCheck && jobCheck.status === 'cancelled') {
        logger.log('[Scraper] Job cancelled');
        break;
      }

      let batchResult;
      try {
        batchResult = await page.evaluate(() => {
        const leads = [];
        let root = document.body;
        let bestCount = 0;
        function countProfileLinks(el) {
          let c = 0;
          for (const a of el.querySelectorAll('a[href^="/"]')) {
            const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
            if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
          }
          return c;
        }
        const candidates = document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]');
        for (const d of candidates) {
          const count = countProfileLinks(d);
          if (count > bestCount && count >= 5) {
            bestCount = count;
            root = d;
          }
        }
        if (bestCount < 5) {
          for (const d of document.querySelectorAll('div')) {
            if (d.clientHeight < 80) continue;
            const count = countProfileLinks(d);
            if (count > bestCount && count >= 10) {
              bestCount = count;
              root = d;
            }
          }
        }

        function isInSuggestedRow(el) {
          var p = el.parentElement;
          for (var up = 0; up < 12 && p; up++) {
            var t = (p.textContent || '').toLowerCase();
            if (t.indexOf('suggested for you') !== -1 || t.indexOf('people you may know') !== -1 || t.indexOf('similar accounts') !== -1) return true;
            p = p.parentElement;
          }
          return false;
        }

        function isInRestrictedMessage(el) {
          var p = el.parentElement;
          for (var up = 0; up < 8 && p; up++) {
            var t = (p.textContent || '').toLowerCase();
            if (t.indexOf('can see all followers') !== -1 || t.indexOf('can see all following') !== -1) return true;
            p = p.parentElement;
          }
          return false;
        }

        function parseUsernameFromHref(href) {
          if (!href || typeof href !== 'string') return null;
          var path = href.trim();
          if (path.indexOf('http') === 0) {
            try { path = new URL(path).pathname; } catch (e) { return null; }
          }
          // IG often uses /user/?hl=en — require first segment only, allow ?/# after it.
          var m = path.match(/^\/([^/?#]+)(?:\/|\?|#|$)/);
          if (!m) return null;
          var u = m[1].toLowerCase();
          if (u.length < 2 || u.length > 30 || !/^[a-z0-9._]+$/.test(u)) return null;
          return u;
        }

        var anchors = root.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]');

        for (var i = 0; i < anchors.length; i++) {
          var a = anchors[i];
          var u = parseUsernameFromHref(a.getAttribute('href'));
          if (!u) continue;

          if (isInSuggestedRow(a)) continue;

          leads.push({ username: u });
        }

        if (leads.length === 0) {
          var spans = root.querySelectorAll('span[dir="auto"]');
          for (var si = 0; si < spans.length; si++) {
            var sp = spans[si];
            var txt = (sp.textContent || '').trim();
            if (txt.length < 2 || txt.length > 30 || !/^[a-z0-9._]+$/.test(txt)) continue;
            if (isInSuggestedRow(sp) || isInRestrictedMessage(sp)) continue;
            var parentLink = sp.closest('a');
            if (!parentLink) continue;
            var u = txt.toLowerCase();
            leads.push({ username: u });
          }
        }

        var seen = {};
        var deduped = [];
        for (var di = 0; di < leads.length; di++) {
          var L = leads[di];
          if (!seen[L.username]) {
            seen[L.username] = true;
            deduped.push(L);
          }
        }
        return { leads: deduped };
        });
      } catch (batchErr) {
        const msg = String(batchErr?.message || batchErr || "");
        if (/detached frame/i.test(msg) && !detachedFrameRetryUsed) {
          detachedFrameRetryUsed = true;
          logger.warn(`[Scraper] Detached frame in scrape loop; attempting one recovery retry: ${msg}`);
          const refreshed = await maybeRefreshInstagramListModal(
            page,
            cleanTarget,
            listKind,
            logger,
            "detached_frame_recover"
          ).catch(() => false);
          if (refreshed) {
            lastModalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
            await delay(randomDelay(1800, 3600));
            continue;
          }
          await closeInstagramListModal(page).catch(() => {});
          await delay(randomDelay(1200, 2600));
          const reopened = await tryOpenProfileList(page, cleanTarget, listKind).catch(() => false);
          if (reopened) {
            lastModalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
            await delay(randomDelay(1500, 3200));
            continue;
          }
        }
        throw batchErr;
      }

      const batch = batchResult.leads || [];

      let newLeads = batch.filter((lead) => {
        const u = (typeof lead === 'string' ? lead : lead.username).trim().replace(/^@/, '').toLowerCase();
        return (
          !seenUsernames.has(u) &&
          !BLACKLIST.has(u) &&
          u !== cleanTarget &&
          !inConvos.has(u) &&
          !sentUsernames.has(u) &&
          !blocklistUsernames.has(u)
        );
      });
      const seenBatch = new Set();
      newLeads = newLeads.filter((lead) => {
        const u = typeof lead === 'string' ? lead : lead.username;
        if (seenBatch.has(u)) return false;
        seenBatch.add(u);
        return true;
      });
      if (effectiveMax && newInsertsTotal + newLeads.length > effectiveMax) {
        newLeads = newLeads.slice(0, effectiveMax - newInsertsTotal);
      }
      const quotaStatus = await getScrapeQuotaStatus(clientId).catch(() => null);
      if (quotaStatus && quotaStatus.remaining <= 0) {
        await completeScrapeJobForQuota(jobId, clientId, newInsertsTotal);
        return;
      }
      if (quotaStatus && newLeads.length > quotaStatus.remaining) {
        newLeads = newLeads.slice(0, quotaStatus.remaining);
      }
      logger.log(
        `[Scraper][Loop ${loopIter}] Extracted=${batch.length} candidates; after filters=${newLeads.length}; quotaRemaining=${quotaStatus?.remaining ?? 'n/a'}`
      );
      for (const lead of newLeads) {
        const u = (typeof lead === 'string' ? lead : lead.username).trim().replace(/^@/, '').toLowerCase();
        seenUsernames.add(u);
      }

      if (newLeads.length > 0) {
        const batchInserted = await upsertLeadsBatch(clientId, newLeads, source, leadGroupId);
        newInsertsTotal += batchInserted;
        await updateScrapeJob(jobId, { scraped_count: newInsertsTotal });
        noNewCount = 0;
        logger.log(
          `[Scraper] Batch: +${batchInserted} new row(s) (${newLeads.length} passed filters), job total ${newInsertsTotal}`
        );
        scrapedSinceCooldown += batchInserted;
        if (chance(0.06)) {
          const batchPauseMs = randomDelay(45000, 90000);
          const driftMs = Math.min(batchPauseMs, randomDelay(12000, 26000));
          logger.log(`[Scraper] Taking a long post-batch pause for ${Math.round(batchPauseMs / 1000)}s.`);
          await idleMouseDrift(page, {
            durationMs: driftMs,
            segmentDurationMs: randomDelay(700, 1800),
          }).catch(() => {});
          await delay(Math.max(0, batchPauseMs - driftMs));
        }
        if (!effectiveMax && COOLDOWN_CHUNK > 0 && scrapedSinceCooldown >= COOLDOWN_CHUNK) {
          const pauseMs = randomDelay(COOLDOWN_MIN_MS, COOLDOWN_MAX_MS);
          logger.log(
            `[Scraper] Long cooldown after ${scrapedSinceCooldown} new leads (total ${newInsertsTotal}). Pausing for ${Math.round(
              pauseMs / 60000
            )} minutes before continuing.`
          );
          scrapedSinceCooldown = 0;
          await delay(pauseMs);
        }
        if (effectiveMax && newInsertsTotal >= effectiveMax) {
          logger.log(`[Scraper] Reached limit (${effectiveMax}). Stopping.`);
          break;
        }
        const quotaAfterInsert = await getScrapeQuotaStatus(clientId).catch(() => null);
        if (quotaAfterInsert && quotaAfterInsert.remaining <= 0) {
          await completeScrapeJobForQuota(jobId, clientId, newInsertsTotal);
          return;
        }
      } else {
        if (noNewCount >= MAX_NO_NEW) {
          logger.log(
            `[Scraper] MAX_NO_NEW exit: noNewCount=${noNewCount} scrollCount=${scrollCount} newInsertsTotal=${newInsertsTotal}`
          );
          break;
        }
      }

      const hadNoNewThisIter = newLeads.length === 0;
      scrollCount++;
      let weGotMoreFromWaitRetry = false;

      let anyScrollThisIter = false;
      let loadedMoreThisIter = false;
      let exhaustedThisIter = false;

      for (let batchIndex = 0; batchIndex < 3; batchIndex++) {
        const scrolled = await humanScrollInstagramListModal(page, { rounds: randomDelay(1, 2) }).catch(() => false);
        if (scrolled) anyScrollThisIter = true;

        const loadResult = await pushInstagramListModalAndWaitForLoad(page, {
          maxWaitMs: randomDelay(2500, 4500),
          bottomOffsetPx: randomDelay(120, 240),
        }).catch(() => ({ moved: false, loadedMore: false, exhausted: false }));

        if (loadResult.moved) anyScrollThisIter = true;
        if (loadResult.loadedMore) {
          loadedMoreThisIter = true;
          break;
        }
        if (loadResult.exhausted) {
          exhaustedThisIter = true;
        }
        logger.log(
          `[Scraper][Loop ${loopIter}] Scroll chunk ${batchIndex + 1}/3: scrolled=${!!scrolled} moved=${!!loadResult.moved} loadedMore=${!!loadResult.loadedMore} exhausted=${!!loadResult.exhausted}`
        );

        if (batchIndex < 2 && Math.random() < 0.35) {
          await randomMouseDrift(page, { totalDurationMs: randomDelay(250, 700) }).catch(() => {});
        }

        await delay(randomDelay(1200, 2800));
      }

      if (!loadedMoreThisIter && exhaustedThisIter) {
        exhaustedConfirmCount++;
        const belowTarget = !!effectiveMax && newInsertsTotal < effectiveMax;
        logger.log(
          `[Scraper][Loop ${loopIter}] Exhaustion signal ${exhaustedConfirmCount}/3 (belowTarget=${belowTarget} total=${newInsertsTotal}${effectiveMax ? `/${effectiveMax}` : ''})`
        );
        if (belowTarget && exhaustedConfirmCount < 3) {
          const refreshed = await maybeRefreshInstagramListModal(
            page,
            cleanTarget,
            listKind,
            logger,
            `exhausted_confirm=${exhaustedConfirmCount}`
          );
          if (refreshed) {
            lastModalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
            await delay(randomDelay(3500, 8000));
            continue;
          }
          await delay(randomDelay(1800, 3500));
          continue;
        }
        logger.log('[Scraper] Followers/following modal appears exhausted after bottom-push load checks.');
        if (hadNoNewThisIter) noNewCount++;
        break;
      }

      if (!anyScrollThisIter && !loadedMoreThisIter) {
        let loadRetries = 0;
        while (loadRetries < LOAD_WAIT_RETRIES) {
          const retryResult = await pushInstagramListModalAndWaitForLoad(page, {
            maxWaitMs: randomDelay(3000, 5000),
            bottomOffsetPx: randomDelay(100, 220),
          }).catch(() => ({ moved: false, loadedMore: false, exhausted: false }));

          if (retryResult.moved) anyScrollThisIter = true;
          if (retryResult.loadedMore) {
            weGotMoreFromWaitRetry = true;
            loadedMoreThisIter = true;
            break;
          }
          if (retryResult.exhausted) {
            exhaustedThisIter = true;
            break;
          }
          logger.log(
            `[Scraper][Loop ${loopIter}] Retry ${loadRetries + 1}/${LOAD_WAIT_RETRIES}: moved=${!!retryResult.moved} loadedMore=${!!retryResult.loadedMore} exhausted=${!!retryResult.exhausted}`
          );

          loadRetries++;
          await delay(randomDelay(800, 1800));
        }

        if (!anyScrollThisIter && !loadedMoreThisIter && exhaustedThisIter) {
          logger.log(`[Scraper] No more scrollable content after ${LOAD_WAIT_RETRIES} bottom-load retries.`);
          if (hadNoNewThisIter) noNewCount++;
          break;
        }
      }

      const modalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
      const snapshotLooksStuck =
        hadNoNewThisIter &&
        modalSnapshot &&
        lastModalSnapshot &&
        modalSnapshot.scrollTop === lastModalSnapshot.scrollTop &&
        modalSnapshot.linkCount === lastModalSnapshot.linkCount &&
        modalSnapshot.remaining === lastModalSnapshot.remaining;
      if (snapshotLooksStuck) stuckModalCount++;
      else if (anyScrollThisIter || !hadNoNewThisIter) stuckModalCount = 0;
      lastModalSnapshot = modalSnapshot;

      if (
        (stuckModalCount >= 2 || (!anyScrollThisIter && hadNoNewThisIter)) &&
        (await maybeRefreshInstagramListModal(page, cleanTarget, listKind, logger, `stuck=${stuckModalCount}`))
      ) {
        logger.log(
          `[Scraper][Loop ${loopIter}] Modal refresh triggered (stuckModalCount=${stuckModalCount}, hadNoNew=${hadNoNewThisIter})`
        );
        stuckModalCount = 0;
        lastModalSnapshot = await getInstagramListModalSnapshot(page).catch(() => null);
        await delay(randomDelay(12000, 35000));
        continue;
      }

      if (hadNoNewThisIter && !weGotMoreFromWaitRetry && scrollCount >= 3) {
        noNewCount++;
      }
      if (noNewCount >= MAX_NO_NEW) {
        logger.log(
          `[Scraper] MAX_NO_NEW exit: noNewCount=${noNewCount} scrollCount=${scrollCount} newInsertsTotal=${newInsertsTotal}`
        );
        break;
      }
      if (loadedMoreThisIter || anyScrollThisIter) exhaustedConfirmCount = 0;
      logger.log(
        `[Scraper][Loop ${loopIter}] End: hadNoNew=${hadNoNewThisIter} noNew=${noNewCount}/${MAX_NO_NEW} anyScroll=${anyScrollThisIter} loadedMore=${loadedMoreThisIter} exhausted=${exhaustedThisIter} stuckModalCount=${stuckModalCount} retryLoadHit=${weGotMoreFromWaitRetry}`
      );

      await organicPause('between_actions', 1.6);
    }

    await finalizeScrapeJobNormalExit(jobId, newInsertsTotal);
    logger.log(
      `[Scraper] Job ${jobId} finished. ${newInsertsTotal} new lead row(s) (${listKind === 'following' ? 'following' : 'followers'}) from @${cleanTarget}`
    );

    try {
      await page.goto('https://www.instagram.com/', {
        waitUntil: 'domcontentloaded',
        timeout: SCRAPER_NAV_TIMEOUT_MS,
      });
      await organicPause('between_actions', 1.5);
      await randomMouseDrift(page, { totalDurationMs: randomDelay(160, 360) });
      await naturalScrollPage(page, { rounds: randomDelay(2, 4), pauseMultiplier: 0.85 });
      await maybeLightStoryInteraction(page, { openChance: 0.1 }).catch(() => {});
      await organicPause('between_actions', 1.5);
      logger.log('[Scraper] Post-scrape warm done.');
    } catch (e) {
      const msg = String(e?.message || e || '');
      if (/detached frame/i.test(msg)) logger.log('[Scraper] Post-scrape warm skipped: frame detached after job completion.');
      else logger.warn('[Scraper] Post-scrape warm skipped: ' + msg);
    }
    if (instagramSessionId && newInsertsTotal > 0) {
      await recordScraperActions(instagramSessionId, newInsertsTotal).catch(() => {});
    }
  } catch (err) {
    logger.error(`[Scraper] ${listKind === 'following' ? 'Following' : 'Follower'} scrape failed`, err);
    await saveScraperFailureScreenshot(page, jobId, listKind + '_scrape');
    try {
      const { updateScrapeJob: updateJob } = require('./database/supabase');
      await updateJob(jobId, {
        status: 'failed',
        error_message: (err && err.message) || String(err),
      });
    } catch (e) {
      logger.error('[Scraper] Failed to update job status', e);
    }
  } finally {
    if (leaseHbTimer) clearInterval(leaseHbTimer);
    if (scrapedSessionId) {
      const cooldownUntilIso = new Date(Date.now() + SCRAPE_SESSION_COOLDOWN_MS).toISOString();
      await require('./database/supabase')
        .updateInstagramSessionScrapeCooldown(scrapedSessionId, cooldownUntilIso)
        .catch((e) => logger.warn('[Scraper] failed setting scrape cooldown: ' + (e.message || e)));
    }
    if (browser) await browser.close().catch(() => {});
  }
}

async function runFollowingScrape(clientId, jobId, targetUsername, options = {}) {
  return runFollowerScrape(clientId, jobId, targetUsername, { ...options, listKind: 'following' });
}

/**
 * Extract shortcode from Instagram post URL.
 * e.g. https://www.instagram.com/p/ABC123/ -> ABC123
 */
function getShortcodeFromPostUrl(url) {
  const m = String(url || '').match(/instagram\.com\/p\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

/**
 * Puppeteer requires a valid absolute URL. DB/API often store `www.instagram.com/...` without scheme
 * or a bare shortcode — both throw Protocol error (Page.navigate): Cannot navigate to invalid URL.
 */
function normalizeInstagramPostUrlForNavigation(postUrl) {
  let s = String(postUrl ?? '')
    .trim()
    .replace(/\u200b/g, '');
  if (!s) return null;

  if (!/^https?:\/\//i.test(s)) {
    if (/instagram\.com/i.test(s)) {
      s = s.replace(/^\/+/, '');
      s = 'https://' + s.replace(/^https?:\/\//i, '');
    } else {
      const code = s.replace(/^\/+|\/+$/g, '');
      if (!/^[A-Za-z0-9_-]+$/.test(code)) return null;
      s = 'https://www.instagram.com/p/' + code + '/';
    }
  }

  try {
    const u = new URL(s);
    const h = u.hostname.toLowerCase();
    if (h !== 'instagram.com' && !h.endsWith('.instagram.com')) return null;
    if (!/\/p\/[A-Za-z0-9_-]+/i.test(u.pathname)) return null;
    return u.toString();
  } catch (_) {
    return null;
  }
}

/** Stable /p/{code}/comments/ URL for comment-thread scraping (hl=en when missing). */
function buildInstagramPostCommentsUrl(postUrl) {
  const raw = String(postUrl || '').trim();
  if (!raw) return 'https://www.instagram.com/';
  try {
    const u = new URL(raw.includes('://') ? raw : 'https://www.instagram.com' + (raw.startsWith('/') ? raw : '/' + raw));
    let path = u.pathname.replace(/\/+$/, '');
    path = path.replace(/\/comments\/?$/i, '');
    const m = path.match(/^(.*\/p\/[^/]+)$/i);
    if (!m) return raw;
    u.pathname = m[1] + '/comments/';
    if (!u.searchParams.get('hl')) u.searchParams.set('hl', 'en');
    return u.toString();
  } catch (_) {
    const m = raw.match(/\/p\/([A-Za-z0-9_-]+)/);
    return m ? `https://www.instagram.com/p/${m[1]}/comments/?hl=en` : raw;
  }
}

function scraperDebugEnabled() {
  const v = String(process.env.SCRAPER_DEBUG || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

/** Full-page PNG on scrape exception (comment / follower / following). On if SCRAPER_FAILURE_SCREENSHOT=1 or SCRAPER_DEBUG=1. */
function scraperFailureScreenshotEnabled() {
  const v = String(process.env.SCRAPER_FAILURE_SCREENSHOT || '').trim().toLowerCase();
  if (v === '1' || v === 'true' || v === 'yes') return true;
  return scraperDebugEnabled();
}

async function saveScraperFailureScreenshot(page, jobId, tag) {
  if (!scraperFailureScreenshotEnabled() || !page) return;
  try {
    if (typeof page.isClosed === 'function' && page.isClosed()) return;
  } catch {
    return;
  }
  try {
    const dir = path.join(__dirname, 'logs', 'scrape-failure-debug');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const safeTag = String(tag || 'fail').replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 56);
    const jid = String(jobId || 'unknown').replace(/[^a-zA-Z0-9.-]/g, '_').slice(0, 40);
    const out = path.join(dir, `${jid}_${safeTag}_${Date.now()}.png`);
    await page.screenshot({ path: out, type: 'png', fullPage: true });
    logger.log('[Scraper] failure screenshot -> ' + out);
  } catch (e) {
    logger.warn('[Scraper] failure screenshot failed: ' + (e.message || e));
  }
}

/**
 * IG mobile comments often live in divs without inline overflow:* styles — detect scrollHeight > clientHeight.
 * Incremental scroll so lazy-loaded nodes can attach (jumping to scrollHeight often loads nothing new).
 */
async function scrollInstagramCommentsViewport(page) {
  try {
    return await page.evaluate(() => {
      const step = 420;
      let moved = false;
      const candidates = [];
      document.querySelectorAll('div, section, main, article, ul, ol, [role="main"], [role="dialog"]').forEach((el) => {
        if (!el || !el.offsetParent) return;
        const sh = el.scrollHeight;
        const ch = el.clientHeight;
        if (sh > ch + 24) candidates.push({ el, gap: sh - ch });
      });
      candidates.sort((a, b) => b.gap - a.gap);
      for (let i = 0; i < Math.min(6, candidates.length); i++) {
        const { el } = candidates[i];
        const sh = el.scrollHeight;
        const ch = el.clientHeight;
        const prev = el.scrollTop;
        el.scrollTop = Math.min(Math.max(0, el.scrollTop + step), Math.max(0, sh - ch));
        if (Math.abs(el.scrollTop - prev) > 2) moved = true;
      }
      const se = document.scrollingElement || document.documentElement;
      if (se) {
        const prev = se.scrollTop;
        const maxScroll = Math.max(0, se.scrollHeight - se.clientHeight);
        se.scrollTop = Math.min(se.scrollTop + step, maxScroll);
        if (Math.abs(se.scrollTop - prev) > 2) moved = true;
      }
      const prevY = window.scrollY;
      window.scrollBy(0, step);
      if (Math.abs(window.scrollY - prevY) > 2) moved = true;
      return moved;
    });
  } catch (_) {
    return false;
  }
}

async function expandInstagramCommentRepliesBatch(page, maxClicks) {
  const limit = Math.max(0, Math.min(12, maxClicks));
  if (!limit) return 0;
  try {
    return await page.evaluate((lim) => {
      const hits = [];
      document.querySelectorAll('span, button, div[role="button"], a, [role="button"]').forEach((el) => {
        if (!el.offsetParent) return;
        const t = (el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
        if (/^view all \d+ repl/.test(t)) hits.push(el);
      });
      let c = 0;
      for (const el of hits.slice(0, lim)) {
        const clickEl =
          el.tagName === 'A' ? el : el.closest('a') || el.closest('[role="button"]') || el.closest('button') || el;
        try {
          clickEl.scrollIntoView({ block: 'center', inline: 'nearest' });
          clickEl.click();
          c++;
        } catch (_) {}
      }
      return c;
    }, limit);
  } catch (_) {
    return 0;
  }
}

async function getInstagramCommentsScrollDebug(page) {
  try {
    return await page.evaluate(() => {
      let overflowish = 0;
      let best = { gap: 0, sh: 0, ch: 0, top: 0 };
      document.querySelectorAll('div, section, main, article, ul, [role="main"]').forEach((el) => {
        if (!el.offsetParent) return;
        const sh = el.scrollHeight;
        const ch = el.clientHeight;
        if (sh > ch + 24) {
          overflowish++;
          const gap = sh - ch;
          if (gap > best.gap) best = { gap, sh, ch, top: el.scrollTop };
        }
      });
      const se = document.scrollingElement || document.documentElement;
      const docGap =
        se && se.scrollHeight > se.clientHeight + 10 ? se.scrollHeight - se.clientHeight : 0;
      const bestSample =
        best.gap > 0
          ? 'el:' + best.sh + '/' + best.ch + ' top=' + Math.round(best.top)
          : docGap > 0
            ? 'doc:' + se.scrollHeight + '/' + se.clientHeight + ' top=' + Math.round(se.scrollTop)
            : 'none';
      return {
        overflowish,
        bestSample,
        anchors: document.querySelectorAll('a[href^="/"]').length,
      };
    });
  } catch (_) {
    return { overflowish: 0, bestSample: 'err', anchors: 0 };
  }
}

/**
 * In-browser snapshot for comment-scrape debugging (href shapes, dialogs, "view all" text).
 * Helps when extract uses a[href^="/"] but IG serves full URLs or a different shell.
 */
async function logCommentScrapeDomDebug(page, logger, label, extra = null) {
  const d = await page.evaluate(function () {
    const hrefsRelSample = [];
    document.querySelectorAll('a[href^="/"]').forEach(function (a, i) {
      if (i < 22) hrefsRelSample.push((a.getAttribute('href') || '').slice(0, 140));
    });
    const hrefsAnySample = [];
    document.querySelectorAll('a[href]').forEach(function (a, i) {
      if (i < 32) hrefsAnySample.push((a.getAttribute('href') || '').slice(0, 140));
    });
    const skip = {
      p: 1,
      reel: 1,
      reels: 1,
      stories: 1,
      explore: 1,
      accounts: 1,
      direct: 1,
      tv: 1,
      tags: 1,
      graphql: 1,
      legal: 1,
      privacy: 1,
    };
    const igProfileFromAbs = [];
    document.querySelectorAll('a[href*="instagram.com"]').forEach(function (a) {
      const h = a.getAttribute('href') || '';
      const m = h.match(/instagram\.com\/([A-Za-z0-9._]{1,30})(?:\/|\?|#|$)/i);
      if (m && !skip[m[1].toLowerCase()]) igProfileFromAbs.push(m[1].toLowerCase());
    });
    const viewAllLike = [];
    document.querySelectorAll('a, span, div[role="button"], button, [role="button"]').forEach(function (el) {
      const t = (el.textContent || '').trim().replace(/\s+/g, ' ');
      if (!t || t.length > 100) return;
      const low = t.toLowerCase();
      if (low.indexOf('view all') !== -1 || low.indexOf('comment') !== -1 || /^\d+\s*comments?$/i.test(t)) {
        if (viewAllLike.length < 14) viewAllLike.push(t);
      }
    });
    const usernameFromRel = [];
    document.querySelectorAll('a[href^="/"]').forEach(function (a) {
      const href = (a.getAttribute('href') || '').trim();
      const m = href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/);
      if (!m) return;
      const u = m[1].toLowerCase();
      if (u && u.length >= 2 && u.length <= 30 && /^[a-z0-9._]+$/.test(u)) usernameFromRel.push(u);
    });
    return {
      hrefRelCount: document.querySelectorAll('a[href^="/"]').length,
      hrefAnyCount: document.querySelectorAll('a[href]').length,
      hrefIgHostCount: document.querySelectorAll('a[href*="instagram.com"]').length,
      hrefsRelSample: hrefsRelSample,
      hrefsAnySample: hrefsAnySample,
      extractStyleUsernames: [...new Set(usernameFromRel)],
      igAbsProfileHints: [...new Set(igProfileFromAbs)].slice(0, 28),
      roleDialog: document.querySelectorAll('[role="dialog"]').length,
      rolePresentation: document.querySelectorAll('[role="presentation"]').length,
      articles: document.querySelectorAll('article').length,
      viewAllLikeTexts: viewAllLike,
    };
  });
  const tail = extra ? ' ' + JSON.stringify(extra) : '';
  logger.log('[Scraper] comment DOM debug [' + label + '] url=' + page.url() + tail + ' ' + JSON.stringify(d));
}

function safeCommentDebugFilePart(s) {
  return String(s || 'x').replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 64);
}

/** Viewport PNG when SCRAPER_DEBUG is on — see logs/comment-scrape-debug/ */
/** Mobile web guest shell: signup strip + login — real logged-in feed usually lacks this on posts. */
async function instagramMobilePostLooksLikeLoggedOutGuest(page) {
  try {
    return await page.evaluate(() => {
      const b = ((document.body && document.body.innerText) || '').toLowerCase();
      if (b.indexOf('sign up for instagram') !== -1) return true;
      if (
        b.indexOf('join ') !== -1 &&
        b.indexOf('on instagram') !== -1 &&
        document.querySelector('a[href*="/accounts/login"]')
      ) {
        return true;
      }
      return false;
    });
  } catch (_) {
    return false;
  }
}

/** Comment scrape cannot run until the page is a real logged-in post/profile view. */
async function scraperWebSessionBlocksWork(page, usernameHint) {
  if (!page) return true;
  if (await scraperPageLooksLoggedOut(page)) return true;
  if (await instagramMobilePostLooksLikeLoggedOutGuest(page)) return true;
  return false;
}

async function commentScrapeDebugScreenshot(page, logger, enabled, jobId, shortcode, phase) {
  if (!enabled) return;
  try {
    const dir = path.join(__dirname, 'logs', 'comment-scrape-debug');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const ts = Date.now();
    const out = path.join(
      dir,
      `${safeCommentDebugFilePart(jobId)}_${safeCommentDebugFilePart(shortcode || 'post')}_${phase}_${ts}.png`
    );
    await page.screenshot({ path: out, type: 'png', fullPage: false });
    logger.log('[Scraper] comment scrape debug screenshot ' + phase + ' -> ' + out);
  } catch (e) {
    logger.warn('[Scraper] comment scrape debug screenshot failed ' + phase + ': ' + (e.message || e));
  }
}

/**
 * Run comment scrape: navigate to post URLs, extract commenter usernames.
 * @param {string} clientId
 * @param {string} jobId
 * @param {string[]} postUrls - Instagram post URLs (e.g. https://www.instagram.com/p/ABC123/)
 * @param {object} options - { maxLeads, leadGroupId }
 */
async function runCommentScrape(clientId, jobId, postUrls, options = {}) {
  const maxLeads = options.maxLeads != null ? Math.max(1, parseInt(options.maxLeads, 10) || 0) : null;
  const leadGroupId = options.leadGroupId || null;
  const leaseOptions = options.leaseOptions || null;
  const sbMod = require('./database/supabase');
  const sb = sbMod.getSupabase();
  if (!sb || !clientId || !jobId || !postUrls || !Array.isArray(postUrls)) {
    logger.error('[Scraper] Comment scrape: missing clientId, jobId, or postUrls');
    return;
  }
  postUrls = postUrls.map((u) => String(u ?? '').trim().replace(/\u200b/g, '')).filter(Boolean);
  if (postUrls.length === 0) {
    logger.error('[Scraper] Comment scrape: post_urls is empty after trimming');
    await failScrapeJob(jobId, 'No valid post URLs on comment scrape job (empty or whitespace).').catch(() => {});
    return;
  }

  let leaseHbTimer = null;
  if (leaseOptions?.jobId && leaseOptions?.workerId) {
    const sec = Math.max(60, parseInt(leaseOptions.leaseSec || process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
    leaseHbTimer = setInterval(() => {
      sbMod.heartbeatScrapeJobLease(leaseOptions.jobId, leaseOptions.workerId, sec).catch(() => {});
    }, Math.min(120000, Math.max(30000, sec * 250)));
  }

  const BLACKLIST = new Set([
    'explore', 'direct', 'accounts', 'reels', 'stories', 'p', 'tv', 'tags',
    'developer', 'about', 'blog', 'jobs', 'help', 'api', 'privacy', 'terms',
  ]);

  let browser;
  let page = null;
  try {
    const { session, instagramSessionId } = await resolvePuppeteerSessionForScrapeJob(clientId, jobId);
    if (!session?.session_data?.cookies?.length) {
      const n = Array.isArray(session?.session_data?.cookies) ? session.session_data.cookies.length : 0;
      logger.error(
        `[Scraper] Job ${jobId} no Puppeteer cookies on current Instagram session: clientId=${clientId} ` +
          `session_row=${session ? 'yes' : 'no'} cookie_count=${n}`
      );
      await retryScrapeJob(
        jobId,
        'No current Instagram session with usable Puppeteer cookies.',
        60
      ).catch(async () => {
        await failScrapeJob(
          jobId,
          'No current Instagram session with usable Puppeteer cookies.'
        );
      });
      return;
    }

    logger.log(`[Scraper] Job ${jobId} session OK; launching browser for comment scrape`);

    browser = await puppeteer.launch(buildScraperBrowserLaunchOptions(instagramSessionId, session.proxy_url));
    page = await browser.newPage();
    await authenticatePageForProxy(page, session.proxy_url || null);
    await applyMobileEmulation(page);
    await page.setCookie(...session.session_data.cookies);

    const preferredIgUser = (session?.instagram_username || '').trim().replace(/^@/, '').toLowerCase();
    const scraperDebug = scraperDebugEnabled();

    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
    await applyInstagramWebStorageFromSessionData(page, session.session_data, logger);
    await delay(randomDelay(1500, 3500));

    const sessionEstablishedHome = await ensurePoolScraperInstagramWebSession(page, logger, preferredIgUser, jobId);

    await commentScrapeDebugScreenshot(
      page,
      logger,
      scraperDebug && String(process.env.SCRAPER_COMMENT_HOME_SCREENSHOT || '').trim() === '1',
      jobId,
      'home',
      'after-home-session-check'
    );

    if (!sessionEstablishedHome || (await scraperWebSessionBlocksWork(page, preferredIgUser))) {
      await requeueScrapeJobForLoggedOutInstagramSession(jobId, instagramSessionId, 'comment scrape home');
      return;
    }

    logger.log('[Scraper] Warming session before comment scrape...');
    await delay(3000 + Math.floor(Math.random() * 5000));
    await page.evaluate(() => window.scrollTo(0, 200 + Math.random() * 500));
    await delay(2000 + Math.floor(Math.random() * 3000));

    logger.log('[Scraper] Comment scrape: ' + postUrls.length + ' post(s)');
    let leadsInsertedTotal = 0;
    const seenUsernames = new Set();
    const [inConvos, sentUsernames, blocklistUsernames] = await Promise.all([
      getConversationParticipantUsernames(clientId),
      getSentUsernames(clientId),
      getScrapeBlocklistUsernames(clientId),
    ]);
    const scraperUsername = (session?.instagram_username || '').trim().replace(/^@/, '').toLowerCase();
    if (scraperUsername) seenUsernames.add(scraperUsername);

    for (const postUrl of postUrls) {
      if (maxLeads && leadsInsertedTotal >= maxLeads) break;

      const jobCheck = await getScrapeJob(jobId);
      if (jobCheck && jobCheck.status === 'cancelled') {
        logger.log('[Scraper] Job cancelled');
        break;
      }

      const normalizedUrl = normalizeInstagramPostUrlForNavigation(postUrl);
      if (!normalizedUrl) {
        logger.error(
          '[Scraper] Comment scrape: invalid post URL (need https://www.instagram.com/p/CODE/ or shortcode): ' +
            JSON.stringify(postUrl)
        );
        await failScrapeJob(
          jobId,
          'Invalid post URL for comment scrape. Use a full https://www.instagram.com/p/… link or a post shortcode.'
        );
        return;
      }
      const shortcode = getShortcodeFromPostUrl(normalizedUrl);
      const commentsListUrl = buildInstagramPostCommentsUrl(normalizedUrl);
      await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
      await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
      await dismissInstagramPopups(page, logger).catch(() => {});

      for (let guestRetry = 0; guestRetry < 2; guestRetry++) {
        if (!(await instagramMobilePostLooksLikeLoggedOutGuest(page))) break;
        logger.warn(
          '[Scraper] Post page looks like logged-out guest UI; re-establishing session from home' +
            (guestRetry ? ' (retry)' : '')
        );
        await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
        await delay(1500 + Math.floor(Math.random() * 900));
        const sessionOkGuest = await ensurePoolScraperInstagramWebSession(page, logger, scraperUsername, jobId);
        if (!sessionOkGuest || (await scraperWebSessionBlocksWork(page, scraperUsername))) {
          await requeueScrapeJobForLoggedOutInstagramSession(jobId, instagramSessionId, 'comment scrape after guest retry');
          return;
        }
        await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
        await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
        await dismissInstagramPopups(page, logger).catch(() => {});
      }

      if (await instagramMobilePostLooksLikeLoggedOutGuest(page)) {
        await requeueScrapeJobForLoggedOutInstagramSession(jobId, instagramSessionId, 'comment scrape post still guest');
        return;
      }

      if (scraperDebug) await logCommentScrapeDomDebug(page, logger, 'post-loaded', { shortcode: shortcode || null });

      const candidateAuthors = await page.evaluate(function () {
        const blacklist = ['explore', 'direct', 'accounts', 'reels', 'stories', 'p', 'tv', 'tags'];
        const out = [];
        const anchors = document.querySelectorAll('a[href^="/"]');
        for (let i = 0; i < Math.min(anchors.length, 20); i++) {
          const href = (anchors[i].getAttribute('href') || '').trim();
          const m = href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/);
          if (!m) continue;
          const u = m[1].toLowerCase();
          if (u && u.length >= 2 && u.length <= 30 && /^[a-z0-9._]+$/.test(u) && blacklist.indexOf(u) === -1) {
            out.push(u);
          }
        }
        return out;
      });

      const postAuthor = candidateAuthors.find((u) => u !== scraperUsername) || null;

      if (postAuthor) {
        await updateScrapeJob(jobId, { target_username: postAuthor });
        logger.log('[Scraper] Post author: @' + postAuthor);
        seenUsernames.add(postAuthor);
      }

      const source = postAuthor ? 'comments:' + postAuthor : (shortcode ? 'comments:' + shortcode : 'comments:' + postUrl);

      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await delay(randomDelay(1000, 2000));

      let commentsThreadUrl = null;
      let commentsOpened = false;
      for (let attempt = 0; attempt < 5; attempt++) {
        commentsOpened = await page.evaluate(function () {
          const candidates = Array.from(document.querySelectorAll('a, span, div[role="button"], [role="button"]'));
          const viewAll = candidates.find(function (el) {
            const t = (el.textContent || '').toLowerCase();
            return /view all \d+ comments?/.test(t) || (t.includes('view all') && t.includes('comment'));
          });
          if (viewAll) {
            const clickable = viewAll.tagName === 'A' ? viewAll : viewAll.closest('a') || viewAll;
            clickable.scrollIntoView({ block: 'center' });
            clickable.click();
            return true;
          }
          return false;
        });
        if (commentsOpened) {
          logger.log('[Scraper] Clicked View all comments');
          await delay(randomDelay(2000, 4000));
          let u = page.url() || '';
          if (!/\/p\/[^/]+\/comments\//i.test(u)) {
            logger.warn('[Scraper] Did not land on /comments/ after click — opening comments URL');
            await page.goto(commentsListUrl, { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
            await delay(randomDelay(1200, 2200));
            await dismissInstagramPopups(page, logger).catch(() => {});
            u = page.url() || '';
          }
          commentsThreadUrl = u || commentsListUrl;
          break;
        }
        await page.evaluate(() => window.scrollBy(0, 200));
        await delay(randomDelay(800, 1500));
      }

      if (scraperDebug) {
        await logCommentScrapeDomDebug(page, logger, 'after-view-all-attempts', {
          commentsOpened,
          attemptsUsed: commentsOpened ? 'ok' : 'none',
        });
      }
      if (!commentsThreadUrl) {
        logger.warn('[Scraper] Comment scrape: opening thread via /comments/ URL (no in-page "View all" or still on post view)');
        await page.goto(commentsListUrl, { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
        await delay(randomDelay(1500, 2800));
        await dismissInstagramPopups(page, logger).catch(() => {});
        commentsThreadUrl = page.url() || commentsListUrl;
      }

      await delay(randomDelay(2000, 4000));

      let anchorCount = await page.evaluate(() => document.querySelectorAll('a[href^="/"]').length);
      if (scraperDebug) logger.log('[Scraper] After open: anchors=' + anchorCount);

      const warmScrollRounds = Math.min(
        8,
        Math.max(2, parseInt(process.env.SCRAPER_COMMENT_WARM_SCROLLS || '3', 10) || 3)
      );
      for (let s = 0; s < warmScrollRounds; s++) {
        await scrollInstagramCommentsViewport(page);
        await delay(randomDelay(1200, 2200));
        const prev = anchorCount;
        anchorCount = await page.evaluate(() => document.querySelectorAll('a[href^="/"]').length);
        if (scraperDebug) logger.log('[Scraper] Warm scroll ' + s + ': anchors=' + anchorCount);
        if (anchorCount > 15 && anchorCount === prev) break;
      }

      await commentScrapeDebugScreenshot(page, logger, scraperDebug, jobId, shortcode, 'warm-scroll-end');

      if (scraperDebug) await logCommentScrapeDomDebug(page, logger, 'after-warm-scrolls', { anchorCount });

      let noNewCount = 0;
      let scrollCount = 0;
      const noNewGiveUp = Math.min(
        8,
        Math.max(2, parseInt(process.env.SCRAPER_COMMENT_NO_NEW_GIVEUP || '3', 10) || 3)
      );
      const maxScrollIters = Math.min(
        40,
        Math.max(
          6,
          parseInt(
            process.env.SCRAPER_COMMENT_MAX_SCROLL_ITERS ||
              String(maxLeads != null && maxLeads > 30 ? 20 : 12),
            10
          ) || 12
        )
      );

      while (true) {
        if (commentsThreadUrl && !/\/p\/[^/]+\/comments\//i.test(page.url() || '')) {
          const cur = page.url() || '';
          logger.warn('[Scraper] Left comments thread (' + cur.slice(0, 96) + ') — restoring');
          await page.goto(commentsThreadUrl, { waitUntil: 'networkidle2', timeout: SCRAPER_NAV_TIMEOUT_MS });
          await delay(randomDelay(1200, 2200));
          await dismissInstagramPopups(page, logger).catch(() => {});
          noNewCount = Math.max(0, noNewCount - 2);
        }

        const usernames = await page.evaluate(function () {
          const out = [];
          const anchors = document.querySelectorAll('a[href^="/"]');
          for (let i = 0; i < anchors.length; i++) {
            const href = (anchors[i].getAttribute('href') || '').trim();
            const m = href.match(/^\/([^/?#]+)(?:\/|\?|#|$)/);
            if (!m) continue;
            const u = m[1].toLowerCase();
            if (u && u.length >= 2 && u.length <= 30 && /^[a-z0-9._]+$/.test(u)) out.push(u);
          }
          return [...new Set(out)];
        });

        if (scraperDebug) {
          const why = usernames.map((u) => {
            if (seenUsernames.has(u)) return u + ':seen';
            if (BLACKLIST.has(u)) return u + ':blacklist';
            if (inConvos.has(u)) return u + ':inConvos';
            if (sentUsernames.has(u)) return u + ':sent';
            if (blocklistUsernames.has(u)) return u + ':blocklist';
            if (postAuthor && u === postAuthor) return u + ':postAuthor';
            return u + ':new';
          });
          logger.log('[Scraper] Comment extract: raw=' + usernames.length + ' [' + usernames.join(',') + '] seen=' + Array.from(seenUsernames).join(',') + ' why=' + why.join(' '));
        }
        if (scraperDebug && usernames.length === 0 && noNewCount === 0) {
          await logCommentScrapeDomDebug(page, logger, 'extract-iteration-raw-0', { scrollCount, noNewCount });
        }

        let newUsernames = usernames.filter(
          (u) =>
            !seenUsernames.has(u) &&
            !BLACKLIST.has(u) &&
            !inConvos.has(u) &&
            !sentUsernames.has(u) &&
            !blocklistUsernames.has(u) &&
            (!postAuthor || u !== postAuthor)
        );
        newUsernames = [...new Set(newUsernames)];
        const quotaStatus = await getScrapeQuotaStatus(clientId).catch(() => null);
        if (quotaStatus && quotaStatus.remaining <= 0) {
          await completeScrapeJobForQuota(jobId, clientId, leadsInsertedTotal);
          return;
        }
        if (quotaStatus && newUsernames.length > quotaStatus.remaining) {
          newUsernames = newUsernames.slice(0, quotaStatus.remaining);
        }
        if (maxLeads && leadsInsertedTotal + newUsernames.length > maxLeads) {
          newUsernames = newUsernames.slice(0, maxLeads - leadsInsertedTotal);
        }
        for (const u of newUsernames) seenUsernames.add(u);

        if (newUsernames.length > 0) {
          const batchInserted = await upsertLeadsBatch(clientId, newUsernames, source, leadGroupId);
          leadsInsertedTotal += batchInserted;
          await updateScrapeJob(jobId, { scraped_count: leadsInsertedTotal });
          noNewCount = 0;
          logger.log(
            '[Scraper] Comments: +' +
              batchInserted +
              ' new row(s) (' +
              newUsernames.length +
              ' passed filters), job total ' +
              leadsInsertedTotal
          );
          const quotaAfterInsert = await getScrapeQuotaStatus(clientId).catch(() => null);
          if (quotaAfterInsert && quotaAfterInsert.remaining <= 0) {
            await completeScrapeJobForQuota(jobId, clientId, leadsInsertedTotal);
            return;
          }
          if (maxLeads && leadsInsertedTotal >= maxLeads) break;
        } else {
          noNewCount++;
          if (!scraperDebug && usernames.length > 0 && noNewCount === 1) {
            logger.log('[Scraper] Comment extract: raw=' + usernames.length + ', new=0 (set SCRAPER_DEBUG=1 for details)');
          }
          if (noNewCount >= noNewGiveUp) {
            if (scraperDebug && usernames.length === 0) {
              await logCommentScrapeDomDebug(page, logger, 'giving-up-raw-0-after-empty-rounds', {
                scrollCount,
                noNewGiveUp,
              });
            }
            break;
          }
        }

        const onCommentsPath = /\/p\/[^/]+\/comments\//i.test(page.url() || '');
        let nudgedViewAll = false;
        if (!onCommentsPath) {
          nudgedViewAll = await page.evaluate(function () {
            const candidates = Array.from(document.querySelectorAll('a, span, div[role="button"], [role="button"]'));
            const viewAll = candidates.find(function (b) {
              const t = (b.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
              return /view all \d+ comments?/.test(t);
            });
            if (viewAll) {
              const clickable = viewAll.tagName === 'A' ? viewAll : viewAll.closest('a') || viewAll;
              clickable.scrollIntoView({ block: 'center' });
              clickable.click();
              return true;
            }
            return false;
          });
        }
        if (nudgedViewAll) await delay(randomDelay(1500, 3500));

        if (onCommentsPath) {
          const expanded = await expandInstagramCommentRepliesBatch(page, 5);
          if (expanded > 0) await delay(randomDelay(900, 1800));
        }

        const scrollDebug = await getInstagramCommentsScrollDebug(page);
        if (scraperDebug) {
          logger.log(
            '[Scraper] Scroll: overflowish=' +
              scrollDebug.overflowish +
              ' best=' +
              scrollDebug.bestSample +
              ' anchors=' +
              scrollDebug.anchors
          );
        }

        const scrolled = await scrollInstagramCommentsViewport(page);
        if (!scrolled && scraperDebug) {
          logger.log('[Scraper] Comment scroll: no scrollable overflow moved (try replies expanded or UI change)');
        }
        await delay(randomDelay(2000, 4000));
        scrollCount++;
        if (scrollCount > maxScrollIters) break;
      }

      await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
    }

    try {
      await page.goto('https://www.instagram.com/', {
        waitUntil: 'domcontentloaded',
        timeout: SCRAPER_NAV_TIMEOUT_MS,
      });
      await delay(3000 + Math.floor(Math.random() * 5000));
      await page.evaluate(() => window.scrollTo(0, 200));
      await delay(5000 + Math.floor(Math.random() * 8000));
      logger.log('[Scraper] Post-scrape warm done.');
    } catch (e) {
      const msg = String(e?.message || e || '');
      if (/detached frame/i.test(msg)) logger.log('[Scraper] Post-scrape warm skipped: frame detached after job completion.');
      else logger.warn('[Scraper] Post-scrape warm skipped: ' + msg);
    }

    if (instagramSessionId && leadsInsertedTotal > 0) {
      await recordScraperActions(instagramSessionId, leadsInsertedTotal).catch(() => {});
    }

    await finalizeScrapeJobNormalExit(jobId, leadsInsertedTotal);
    logger.log('[Scraper] Comment job ' + jobId + ' finished. Scraped ' + leadsInsertedTotal + ' leads.');
  } catch (err) {
    logger.error('[Scraper] Comment scrape failed', err);
    await saveScraperFailureScreenshot(page, jobId, 'comment_scrape');
    try {
      const { updateScrapeJob: updateJob } = require('./database/supabase');
      await updateJob(jobId, {
        status: 'failed',
        error_message: (err && err.message) || String(err),
      });
    } catch (e) {
      logger.error('[Scraper] Failed to update job status', e);
    }
  } finally {
    if (leaseHbTimer) clearInterval(leaseHbTimer);
    if (browser) await browser.close().catch(() => {});
  }
}

/**
 * Optional fallback: fetch followers via Instagram GraphQL instead of UI scrolling.
 *
 * WARNING: The query_hash/doc_id values used by Instagram's private GraphQL API
 * change frequently (every few weeks). You MUST open DevTools → Network tab on
 * a live Instagram session and copy the current values from a real
 * followers/comments request.
 *
 * Example:
 *   // CHECK NETWORK TAB FOR LATEST followers query_hash/doc_id; this value changes frequently.
 */
async function fetchFollowersViaGraphQL(page, userId, afterCursor = null, first = 50) {
  const variables = {
    id: userId,
    include_reel: true,
    fetch_mutual: false,
    first,
  };
  if (afterCursor) {
    variables.after = afterCursor;
  }

  // IMPORTANT: Replace <FOLLOWERS_QUERY_HASH> with the latest value from DevTools.
  const queryHash = '<FOLLOWERS_QUERY_HASH>'; // CHECK NETWORK TAB FOR LATEST DOC_ID

  return page.evaluate(
    async ({ qh, vars }) => {
      const res = await fetch('https://www.instagram.com/graphql/query/', {
        method: 'GET',
        credentials: 'include',
        headers: {
          'x-ig-app-id': '936619743392459', // public web app id; may change
        },
        body: null,
      });
      // This evaluate body is intentionally minimal; real implementation should:
      // - Use ?query_hash=<qh>&variables=<JSON_ENCODED> in the URL
      // - Parse JSON and return { users, page_info }
      // This stub documents the pattern and reminds you to pull fresh hashes.
      return res.status;
    },
    { qh: queryHash, vars: variables }
  );
}

/**
 * Optional fallback: fetch comments via Instagram GraphQL/private API.
 *
 * As with followers, doc_id/query_hash rotates often.
 *   // CHECK NETWORK TAB FOR LATEST comments doc_id/query_hash.
 */
async function fetchCommentsViaGraphQL(page, shortcode, afterCursor = null, first = 50) {
  const variables = {
    shortcode,
    first,
  };
  if (afterCursor) {
    variables.after = afterCursor;
  }

  const docId = '<COMMENTS_DOC_ID>'; // CHECK NETWORK TAB FOR LATEST DOC_ID

  return page.evaluate(
    async ({ docId, vars }) => {
      // Real implementation should mirror the request Instagram sends for comments:
      // POST https://www.instagram.com/graphql/query/ with form data:
      //   doc_id=<docId>&variables=<JSON_ENCODED>
      const res = await fetch('https://www.instagram.com/graphql/query/', {
        method: 'POST',
        credentials: 'include',
        headers: {
          'content-type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          doc_id: docId,
          variables: JSON.stringify(vars),
        }),
      });
      // Parse JSON and return comments/page_info in a real implementation.
      return res.status;
    },
    { docId, vars: variables }
  );
}

module.exports = {
  connectScraper,
  runFollowerScrape,
  runFollowingScrape,
  runCommentScrape,
  // Optional GraphQL-based fallbacks (not wired into the API by default)
  fetchFollowersViaGraphQL,
  fetchCommentsViaGraphQL,
};
