/**
 * Instagram scraper module – legacy Puppeteer implementation.
 *
 * NOTE: The preferred path for high-volume, stable scraping is now the Python
 * instagrapi worker (spawned from /api/scraper/start). This file remains as
 * an optional fallback and for platform scraper login.
 *
 * If you re-enable JS scraping, avoid UI scrolling and use the GraphQL/private
 * APIs below. ALWAYS verify the latest query_hash/doc_id values via DevTools:
 *   // CHECK NETWORK TAB FOR LATEST DOC_ID / query_hash
 */
require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const fs = require('fs');
const {
  saveScraperSession,
  createScrapeJob,
  updateScrapeJob,
  getScrapeJob,
  upsertLeadsBatch,
  getConversationParticipantUsernames,
  getSentUsernames,
  getScrapeBlocklistUsernames,
  getPlatformScraperSessionById,
  reservePlatformScraperSessionForWorker,
  describePlatformScraperPoolForLogs,
  recordScraperActions,
} = require('./database/supabase');
const logger = require('./utils/logger');
const { applyMobileEmulation } = require('./utils/mobile-viewport');

/** Log + persist failure (early returns used to only update the DB, so PM2 showed nothing after "claimed job"). */
async function failScrapeJob(jobId, errorMessage) {
  logger.error(`[Scraper] Job ${jobId} failed: ${errorMessage}`);
  await updateScrapeJob(jobId, { status: 'failed', error_message: errorMessage });
}

/**
 * Scrapes use only the shared platform pool (cold_dm_platform_scraper_sessions), not per-client cold_dm_scraper_sessions.
 * Optionally re-reserves from the pool when the job points at a row with no Puppeteer cookies.
 */
async function resolvePuppeteerSessionForScrapeJob(jobId, leaseOptions) {
  const job = await getScrapeJob(jobId);
  let session = null;
  let platformSessionId = null;

  if (job?.platform_scraper_session_id) {
    const platformSession = await getPlatformScraperSessionById(job.platform_scraper_session_id);
    if (platformSession && Array.isArray(platformSession.session_data?.cookies) && platformSession.session_data.cookies.length > 0) {
      session = platformSession;
      platformSessionId = job.platform_scraper_session_id;
    }
  }

  if (!session && leaseOptions?.workerId) {
    const sec = Math.max(60, parseInt(leaseOptions.leaseSec || process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
    const reserved = await reservePlatformScraperSessionForWorker(leaseOptions.workerId, sec);
    if (reserved && Array.isArray(reserved.session_data?.cookies) && reserved.session_data.cookies.length > 0) {
      session = {
        session_data: reserved.session_data,
        instagram_username: reserved.instagram_username,
      };
      platformSessionId = reserved.id;
      await updateScrapeJob(jobId, { platform_scraper_session_id: reserved.id });
    }
  }

  if (leaseOptions && platformSessionId) {
    leaseOptions.platformSessionId = platformSessionId;
  }

  return { job, session, platformSessionId };
}

puppeteer.use(StealthPlugin());

const SCRAPE_DELAY_MIN_MS = 2000;
const SCRAPE_DELAY_MAX_MS = 5000;
const SCROLL_PAUSE_MS = 1500;
const LOAD_WAIT_MS = 4000;
const LOAD_WAIT_RETRIES = 3;
const SCROLL_CHUNK_PX = 300;
const SCROLL_CHUNKS_PER_ITER = 8;
const SCROLL_CHUNK_DELAY_MS = 600;
const HEADLESS = process.env.SCRAPER_HEADLESS !== 'false';

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function randomDelay(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
}

/**
 * Log in to Instagram with credentials, return session (cookies only).
 * Password is never stored.
 */
async function connectScraper(instagramUsername, instagramPassword) {
  const browser = await puppeteer.launch({
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
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

/**
 * Run follower scrape in the background. Call from API without awaiting.
 * Loads scraper session, navigates to profile, paginates followers, upserts leads.
 * @param {number} [options.maxLeads] - Optional. Stop when this many NEW leads have been added. Omit for no limit.
 */
async function runFollowerScrape(clientId, jobId, targetUsername, options = {}) {
  const maxLeads = options.maxLeads != null ? Math.max(1, parseInt(options.maxLeads, 10) || 0) : null;
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
      if (leaseOptions.platformSessionId) {
        sbMod.heartbeatPlatformScraperSessionLease(leaseOptions.platformSessionId, leaseOptions.workerId, sec).catch(() => {});
      }
    }, Math.min(120000, Math.max(30000, sec * 250)));
  }

  let browser;
  try {
    const { job, session, platformSessionId } = await resolvePuppeteerSessionForScrapeJob(jobId, leaseOptions);
    if (!session?.session_data?.cookies?.length) {
      const n = Array.isArray(session?.session_data?.cookies) ? session.session_data.cookies.length : 0;
      const poolHint = await describePlatformScraperPoolForLogs().catch(() => '');
      logger.error(
        `[Scraper] Job ${jobId} no Puppeteer cookies: clientId=${clientId} ` +
          `job.platform_scraper_session_id=${job?.platform_scraper_session_id ?? 'null'} ` +
          `session_row=${session ? 'yes' : 'no'} cookie_count=${n}. ${poolHint}`
      );
      await failScrapeJob(
        jobId,
        'No platform scraper with a valid session (cold_dm_platform_scraper_sessions). Per-client cold_dm_scraper_sessions is not used.'
      );
      return;
    }

    logger.log(`[Scraper] Job ${jobId} session OK; launching browser for follower scrape`);

    browser = await puppeteer.launch({
      headless: HEADLESS,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    const useMobile = process.env.SCRAPER_USE_MOBILE === '1' || process.env.SCRAPER_USE_MOBILE === 'true';
    if (useMobile) {
      await applyMobileEmulation(page);
      logger.log('[Scraper] Using mobile viewport (SCRAPER_USE_MOBILE=1)');
    } else {
      await page.setViewport({ width: 1280, height: 800 });
      logger.log('[Scraper] Using desktop viewport for follower scrape (mobile scroll fails)');
    }
    await page.setCookie(...session.session_data.cookies);
    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(randomDelay(1500, 3500));

    if (page.url().includes('/accounts/login')) {
      await failScrapeJob(jobId, 'Scraper session expired. Reconnect scraper.');
      return;
    }
    logger.log('[Scraper] Warming session before scrape...');
    await delay(3000 + Math.floor(Math.random() * 5000));
    for (let i = 0; i < 2 + Math.floor(Math.random() * 2); i++) {
      await page.evaluate(() => window.scrollTo(0, 200 + Math.random() * 500));
      await delay(8000 + Math.floor(Math.random() * 15000));
    }
    const liked = await page.evaluate(() => {
      const likeBtns = Array.from(document.querySelectorAll('[aria-label="Like"], svg[aria-label="Like"]')).slice(0, 2);
      for (const btn of likeBtns) {
        const el = btn.closest('button') || btn.closest('[role="button"]') || btn;
        if (el && el.offsetParent) {
          el.click();
          return true;
        }
      }
      return false;
    });
    if (liked) await delay(5000 + Math.floor(Math.random() * 10000));
    logger.log('[Scraper] Warm behaviour done.');

    const source = `followers:${targetUsername}`;
    const cleanTarget = targetUsername.replace(/^@/, '').trim().toLowerCase();
    logger.log(`[Scraper] Starting follower scrape for @${cleanTarget}${maxLeads ? ` (max ${maxLeads})` : ''}`);

    await page.goto(`https://www.instagram.com/${encodeURIComponent(cleanTarget)}/`, {
      waitUntil: 'networkidle2',
      timeout: 30000,
    });
    await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));

    // Handle "Review and Agree" / terms/privacy dialogs that can block the page.
    async function dismissReviewDialogs(page) {
      return page.evaluate(() => {
        const bodyText = (document.body && document.body.innerText) || '';
        if (
          !/review and agree/i.test(bodyText) &&
          !/changes to how we manage data/i.test(bodyText) &&
          !/updates to our terms/i.test(bodyText)
        ) {
          return false;
        }
        const labels = ['Agree to Terms', 'Agree', 'Next', 'OK', 'Accept', 'Continue'];
        const buttons = Array.from(
          document.querySelectorAll('button, div[role="button"], [role="button"]')
        );
        for (const label of labels) {
          const btn = buttons.find(
            (el) => (el.textContent || '').trim().toLowerCase() === label.toLowerCase()
          );
          if (btn && btn.offsetParent) {
            btn.click();
            return true;
          }
        }
        // Fallback: click the primary button in any dialog.
        const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'));
        for (const d of dialogs) {
          const primary = Array.from(
            d.querySelectorAll('button, div[role="button"], [role="button"]')
          ).find((el) => {
            const style = window.getComputedStyle(el);
            const bg = style.backgroundColor || '';
            // Heuristic: bright/blue or main CTA.
            return /rgb\(0,\s*149,\s*246\)/.test(bg) || /rgb\(0,\s*55,\s*107\)/.test(bg);
          });
          if (primary && primary.offsetParent) {
            primary.click();
            return true;
          }
        }
        return false;
      });
    }

    try {
      // Some accounts see multiple stacked dialogs; loop a few times.
      for (let i = 0; i < 3; i++) {
        const handled = await dismissReviewDialogs(page);
        if (!handled) break;
        logger.log('[Scraper] Dismissed Review/Terms dialog (%d)', i + 1);
        await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
      }
    } catch (e) {
      logger.warn('[Scraper] Failed to handle Review/Terms dialog: ' + e.message);
    }

    const jobCheck = await getScrapeJob(jobId);
    if (jobCheck?.status === 'cancelled') return;

    const profileFollowerCount = await page.evaluate((target) => {
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
      const links = Array.from(document.querySelectorAll('a[href*="/followers"]'));
      const followersLink = links.find(function (a) {
        const href = (a.getAttribute('href') || '').toLowerCase();
        return href.indexOf('/' + target + '/followers') !== -1;
      });
      if (!followersLink) return null;
      const container = followersLink.closest('li') || followersLink.parentElement;
      if (!container) return null;
      const titleEl = container.querySelector('[title]');
      const span = container.querySelector('span');
      let n = parseCount((titleEl && titleEl.getAttribute('title')) || (span && span.getAttribute('title')));
      if (n != null) return n;
      const txt = (container.textContent || '').replace(/,/g, '');
      n = parseCount(txt);
      if (n != null) return n;
      n = parseCount(followersLink.getAttribute('aria-label'));
      if (n != null) return n;
      return parseCount(followersLink.textContent);
    }, cleanTarget);

    const effectiveMax =
      profileFollowerCount != null && profileFollowerCount > 0
        ? (maxLeads ? Math.min(maxLeads, profileFollowerCount) : profileFollowerCount)
        : maxLeads;
    if (profileFollowerCount != null) {
      logger.log('[Scraper] Profile has ' + profileFollowerCount + ' followers; capping at ' + effectiveMax);
    } else {
      logger.log('[Scraper] Could not parse follower count from profile; using max_leads only');
    }

    async function tryOpenFollowers(page, target) {
      return page.evaluate((targetUsername) => {
        const lower = (s) => (s || '').toLowerCase();

        // 1) Original href-based logic.
        const links = Array.from(document.querySelectorAll('a[href*="/followers"], a[href*="/following"]'));
        const followersLink = links.find((a) => {
          const href = lower(a.getAttribute('href') || '');
          return (
            href.includes(`/${targetUsername}/followers`) ||
            (href.includes('/followers') && !href.includes('/following'))
          );
        });
        if (followersLink) {
          followersLink.click();
          return true;
        }

        // 2) Fallback: stats row text (e.g. "1,128 followers").
        const candidates = Array.from(
          document.querySelectorAll('a, span, div, button, [role="button"]')
        );
        const statsLike = candidates.find((el) => {
          const text = (el.textContent || '').trim();
          const l = lower(text);
          return l.includes('followers') && /\d/.test(text);
        });
        if (statsLike) {
          const clickable =
            statsLike.closest('a, button, [role="button"]') || statsLike;
          if (clickable && clickable instanceof HTMLElement) {
            clickable.click();
            return true;
          }
        }

        // 3) Last resort: any button-like element that mentions followers.
        const roleButtons = Array.from(
          document.querySelectorAll('[role="button"], button, a')
        );
        for (const btn of roleButtons) {
          const t = lower(btn.textContent || '');
          if (t.includes('followers') || /\d+\s*followers/.test(t)) {
            btn.click();
            return true;
          }
        }

        return false;
      }, target);
    }

    let followersLinkClicked = await tryOpenFollowers(page, cleanTarget);
    if (!followersLinkClicked) {
      // If first attempt fails, try to clear any remaining dialogs and retry once.
      try {
        const handled = await dismissReviewDialogs(page);
        if (handled) {
          logger.log('[Scraper] Dismissed additional Review/Terms dialog before retrying followers modal');
          await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
        }
      } catch (e) {
        logger.warn('[Scraper] Failed to handle Review/Terms dialog on retry: ' + e.message);
      }
      followersLinkClicked = await tryOpenFollowers(page, cleanTarget);
    }

    if (!followersLinkClicked) {
      // Capture a screenshot to debug new layouts/popups blocking the followers modal.
      try {
        const debugDir = path.join(process.cwd(), 'scraper-debug');
        if (!fs.existsSync(debugDir)) {
          fs.mkdirSync(debugDir, { recursive: true });
        }
        const screenshotPath = path.join(
          debugDir,
          `followers_modal_fail_${String(jobId)}.png`
        );
        await page.screenshot({ path: screenshotPath, fullPage: true });
        logger.error('[Scraper] Could not open followers modal – screenshot saved at %s', screenshotPath);
      } catch (screenshotErr) {
        logger.error('[Scraper] Failed to capture screenshot on followers modal error: %s', screenshotErr.message);
      }
      const modalFailMsg =
        'Could not open followers list. Profile may be private or link not found.';
      await failScrapeJob(jobId, modalFailMsg);
      return;
    }

    logger.log('[Scraper] Followers modal opened, extracting...');
    await delay(randomDelay(2500, 5000));
    await page.evaluate(() => {
      function countProfileLinks(el) {
        let c = 0;
        for (const a of el.querySelectorAll('a[href^="/"]')) {
          const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
          if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
        }
        return c;
      }
      let best = null;
      let bestCount = 0;
      for (const d of document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]')) {
        const count = countProfileLinks(d);
        if (count > bestCount && count >= 5) {
          bestCount = count;
          best = d;
        }
      }
          if (bestCount < 5) {
            for (const d of document.querySelectorAll('div')) {
              if (d.clientHeight < 80) continue;
              const count = countProfileLinks(d);
              if (count > bestCount && count >= 10) {
                bestCount = count;
                best = d;
              }
            }
          }
      if (best) {
        best.focus();
        for (let i = 0; i < 3; i++) {
          best.dispatchEvent(new WheelEvent('wheel', { deltaY: 200, bubbles: true }));
        }
      }
    });
    await delay(1500);

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

    let totalScraped = 0;
    const seenUsernames = new Set();
    let noNewCount = 0;
    // To avoid hammering one account with huge follower scrapes, insert a long cooldown
    // after scraping a large chunk. Defaults: pause after each 1000 new leads.
    const COOLDOWN_CHUNK = parseInt(process.env.SCRAPER_FOLLOWER_COOLDOWN_CHUNK || '1000', 10);
    const COOLDOWN_MIN_MS = 45 * 60 * 1000;
    const COOLDOWN_MAX_MS = 70 * 60 * 1000;
    let scrapedSinceCooldown = 0;
    const MAX_NO_NEW = profileFollowerCount != null && profileFollowerCount > 100 ? 12 : 6;
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

    while (true) {
      const jobCheck = await getScrapeJob(jobId);
      if (jobCheck && jobCheck.status === 'cancelled') {
        logger.log('[Scraper] Job cancelled');
        break;
      }

      const batchResult = await page.evaluate(() => {
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
          var m = path.match(/^\/([^/?#]+)\/?$/);
          if (!m) return null;
          var u = m[1].toLowerCase();
          if (u.length < 2 || u.length > 30 || !/^[a-z0-9._]+$/.test(u)) return null;
          return u;
        }

        function getDisplayNameFromAnchor(anchor, username) {
          var spans = anchor.querySelectorAll('span[dir="auto"], span');
          var withSpace = null;
          var fallback = null;
          for (var i = 0; i < spans.length; i++) {
            var txt = (spans[i].textContent || '').trim();
            if (!txt || txt.toLowerCase() === username) continue;
            if (txt.length > 50) continue;
            if (txt.indexOf(' ') !== -1) withSpace = txt;
            else if (!fallback) fallback = txt;
          }
          return withSpace || fallback || null;
        }

        var anchors = root.querySelectorAll('a[href^="/"], a[href*="instagram.com/"]');

        for (var i = 0; i < anchors.length; i++) {
          var a = anchors[i];
          var u = parseUsernameFromHref(a.getAttribute('href'));
          if (!u) continue;

          if (isInSuggestedRow(a)) continue;

          var displayName = getDisplayNameFromAnchor(a, u);
          leads.push({ username: u, display_name: displayName });
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
            var displayName = parentLink ? getDisplayNameFromAnchor(parentLink, u) : null;
            leads.push({ username: u, display_name: displayName });
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
      if (effectiveMax && totalScraped + newLeads.length > effectiveMax) {
        newLeads = newLeads.slice(0, effectiveMax - totalScraped);
      }
      for (const lead of newLeads) {
        const u = (typeof lead === 'string' ? lead : lead.username).trim().replace(/^@/, '').toLowerCase();
        seenUsernames.add(u);
      }

      if (newLeads.length > 0) {
        await upsertLeadsBatch(clientId, newLeads, source, leadGroupId);
        totalScraped = seenUsernames.size;
        await updateScrapeJob(jobId, { scraped_count: totalScraped });
        noNewCount = 0;
        logger.log(`[Scraper] Batch: +${newLeads.length} new, total ${totalScraped}`);
        scrapedSinceCooldown += newLeads.length;
        if (!effectiveMax && COOLDOWN_CHUNK > 0 && scrapedSinceCooldown >= COOLDOWN_CHUNK) {
          const pauseMs = randomDelay(COOLDOWN_MIN_MS, COOLDOWN_MAX_MS);
          logger.log(
            `[Scraper] Long cooldown after ${scrapedSinceCooldown} new leads (total ${totalScraped}). Pausing for ${Math.round(
              pauseMs / 60000
            )} minutes before continuing.`
          );
          scrapedSinceCooldown = 0;
          await delay(pauseMs);
        }
        if (effectiveMax && totalScraped >= effectiveMax) {
          logger.log(`[Scraper] Reached limit (${effectiveMax}). Stopping.`);
          break;
        }
      } else {
        if (noNewCount >= MAX_NO_NEW) {
          logger.log(
            `[Scraper] MAX_NO_NEW exit: noNewCount=${noNewCount} scrollCount=${scrollCount} totalScraped=${totalScraped}`
          );
          break;
        }
      }

      const hadNoNewThisIter = newLeads.length === 0;
      scrollCount++;
      let weGotMoreFromWaitRetry = false;

      const scrollIncrementally = () =>
        page.evaluate((chunkPx) => {
          function countProfileLinks(el) {
            let c = 0;
            for (const a of el.querySelectorAll('a[href^="/"]')) {
              const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
              if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
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
          if (!dialog) return { scrolled: false };
          let scrollTarget = null;
          let maxLinks = 0;
          for (const div of dialog.querySelectorAll('div')) {
            const links = div.querySelectorAll('a[href^="/"]');
            const hasOverflow = div.scrollHeight > div.clientHeight;
            const isTallEnough = div.clientHeight > 80;
            if (links.length > maxLinks && hasOverflow && isTallEnough) {
              maxLinks = links.length;
              scrollTarget = div;
            }
          }
          if (!scrollTarget && dialog.scrollHeight > dialog.clientHeight) scrollTarget = dialog;
          if (!scrollTarget) {
            for (const div of dialog.querySelectorAll('div')) {
              if (div.scrollHeight > div.clientHeight && div.clientHeight > 80) {
                scrollTarget = div;
                break;
              }
            }
          }
          if (!scrollTarget) {
            maxLinks = 0;
            for (const div of dialog.querySelectorAll('div')) {
              const links = div.querySelectorAll('a[href^="/"]');
              if (links.length > maxLinks && div.clientHeight > 80) {
                maxLinks = links.length;
                scrollTarget = div;
              }
            }
          }
          const scrollables = [];
          if (scrollTarget) scrollables.push(scrollTarget);
          for (const div of dialog.querySelectorAll('div')) {
            if (div.scrollHeight > div.clientHeight && div.clientHeight > 80 && !scrollables.includes(div)) {
              scrollables.push(div);
            }
          }
          let didScroll = false;
          for (const el of scrollables) {
            if (!el) continue;
            const prev = el.scrollTop;
            el.scrollBy(0, chunkPx);
            if (el.scrollTop !== prev) {
              didScroll = true;
              break;
            }
          }
          if (!didScroll && scrollTarget) {
            const prevTop = scrollTarget.scrollTop;
            scrollTarget.focus();
            scrollTarget.dispatchEvent(new WheelEvent('wheel', { deltaY: chunkPx, bubbles: true, cancelable: true }));
            if (scrollTarget.scrollTop !== prevTop) didScroll = true;
          }
          return { scrolled: didScroll };
        }, SCROLL_CHUNK_PX);

      let anyScrollThisIter = false;
      for (let c = 0; c < SCROLL_CHUNKS_PER_ITER; c++) {
        const result = await scrollIncrementally();
        if (result.scrolled) anyScrollThisIter = true;
        await delay(SCROLL_CHUNK_DELAY_MS);
      }

      if (!anyScrollThisIter) {
        const scrollIntoViewResult = await page.evaluate(() => {
          function countProfileLinks(el) {
            let c = 0;
            for (const a of el.querySelectorAll('a[href^="/"]')) {
              const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
              if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
            }
            return c;
          }
          let root = null;
          let bestCount = 0;
          for (const d of document.querySelectorAll('[role="dialog"], div[role="presentation"], div[role="menu"]')) {
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
          if (!root) return false;
          const links = root.querySelectorAll('a[href^="/"]');
          const lastLink = links[links.length - 1];
          if (lastLink) {
            lastLink.scrollIntoView({ block: 'end', behavior: 'instant' });
            return true;
          }
          return false;
        });
        if (scrollIntoViewResult) await delay(1500);
        const wheelScrolled = await page.evaluate(() => {
          function countProfileLinks(el) {
            let c = 0;
            for (const a of el.querySelectorAll('a[href^="/"]')) {
              const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
              if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
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
          if (!dialog) return false;
          dialog.focus();
          const targets = [dialog, ...dialog.querySelectorAll('div')].filter((el) => el.clientHeight > 80);
          for (const el of targets) {
            el.dispatchEvent(new WheelEvent('wheel', { deltaY: 400, bubbles: true }));
          }
          return true;
        });
        if (wheelScrolled) await delay(800);
        await page.evaluate(() => {
          function countProfileLinks(el) {
            let c = 0;
            for (const a of el.querySelectorAll('a[href^="/"]')) {
              const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
              if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
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
          if (dialog) dialog.focus();
        });
        for (let k = 0; k < 4; k++) {
          await page.keyboard.press('PageDown');
          await delay(400);
        }
        const { scrolled: kbdScrolled } = await page.evaluate(() => {
          function countProfileLinks(el) {
            let c = 0;
            for (const a of el.querySelectorAll('a[href^="/"]')) {
              const m = (a.getAttribute('href') || '').match(/^\/([^/?#]+)/);
              if (m && m[1].length >= 2 && m[1].length <= 30 && /^[a-z0-9._]+$/.test(m[1].toLowerCase())) c++;
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
          if (!dialog) return { scrolled: false };
          let anyScroll = false;
          for (const el of [dialog, ...dialog.querySelectorAll('div')]) {
            if (el.scrollHeight > el.clientHeight && el.clientHeight > 80) {
              const prev = el.scrollTop;
              el.scrollTop = el.scrollHeight;
              if (el.scrollTop !== prev) anyScroll = true;
            }
          }
          return { scrolled: anyScroll };
        });
        if (kbdScrolled) anyScrollThisIter = true;
      }

      if (!anyScrollThisIter) {
        let loadRetries = 0;
        while (loadRetries < LOAD_WAIT_RETRIES) {
          await delay(LOAD_WAIT_MS);
          for (let c = 0; c < SCROLL_CHUNKS_PER_ITER; c++) {
            const { scrolled } = await scrollIncrementally();
            if (scrolled) anyScrollThisIter = true;
            await delay(SCROLL_CHUNK_DELAY_MS);
          }
          if (anyScrollThisIter) {
            weGotMoreFromWaitRetry = true;
            break;
          }
          loadRetries++;
        }
        if (!anyScrollThisIter) {
          logger.log(`[Scraper] No more scrollable content after ${LOAD_WAIT_RETRIES} retries.`);
          if (hadNoNewThisIter) noNewCount++;
          break;
        }
      }

      if (hadNoNewThisIter && !weGotMoreFromWaitRetry && scrollCount >= 3) {
        noNewCount++;
      }
      if (noNewCount >= MAX_NO_NEW) {
        logger.log(
          `[Scraper] MAX_NO_NEW exit: noNewCount=${noNewCount} scrollCount=${scrollCount} totalScraped=${totalScraped}`
        );
        break;
      }

      await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
    }

    await updateScrapeJob(jobId, { status: 'completed', scraped_count: totalScraped });
    logger.log(`[Scraper] Job ${jobId} completed. Scraped ${totalScraped} followers from @${cleanTarget}`);

    try {
      await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await delay(3000 + Math.floor(Math.random() * 5000));
      await page.evaluate(() => window.scrollTo(0, 200));
      await delay(5000 + Math.floor(Math.random() * 8000));
      logger.log('[Scraper] Post-scrape warm done.');
    } catch (e) {
      logger.warn('[Scraper] Post-scrape warm skipped: ' + e.message);
    }
    if (platformSessionId && totalScraped > 0) {
      const actionCount = Math.max(20, totalScraped + 10);
      await recordScraperActions(platformSessionId, actionCount).catch(() => {});
    }
  } catch (err) {
    logger.error('[Scraper] Follower scrape failed', err);
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
 * Extract shortcode from Instagram post URL.
 * e.g. https://www.instagram.com/p/ABC123/ -> ABC123
 */
function getShortcodeFromPostUrl(url) {
  const m = String(url || '').match(/instagram\.com\/p\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
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
  if (!sb || !clientId || !jobId || !postUrls || !Array.isArray(postUrls) || postUrls.length === 0) {
    logger.error('[Scraper] Comment scrape: missing clientId, jobId, or postUrls');
    return;
  }

  let leaseHbTimer = null;
  if (leaseOptions?.jobId && leaseOptions?.workerId) {
    const sec = Math.max(60, parseInt(leaseOptions.leaseSec || process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
    leaseHbTimer = setInterval(() => {
      sbMod.heartbeatScrapeJobLease(leaseOptions.jobId, leaseOptions.workerId, sec).catch(() => {});
      if (leaseOptions.platformSessionId) {
        sbMod.heartbeatPlatformScraperSessionLease(leaseOptions.platformSessionId, leaseOptions.workerId, sec).catch(() => {});
      }
    }, Math.min(120000, Math.max(30000, sec * 250)));
  }

  const BLACKLIST = new Set([
    'explore', 'direct', 'accounts', 'reels', 'stories', 'p', 'tv', 'tags',
    'developer', 'about', 'blog', 'jobs', 'help', 'api', 'privacy', 'terms',
  ]);

  let browser;
  let platformSessionId = null;
  try {
    const { session, platformSessionId: resolvedPlatformId } = await resolvePuppeteerSessionForScrapeJob(
      jobId,
      leaseOptions
    );
    platformSessionId = resolvedPlatformId;
    if (!session?.session_data?.cookies?.length) {
      const n = Array.isArray(session?.session_data?.cookies) ? session.session_data.cookies.length : 0;
      const poolHint = await describePlatformScraperPoolForLogs().catch(() => '');
      logger.error(
        `[Scraper] Job ${jobId} no Puppeteer cookies: clientId=${clientId} ` +
          `job.platform_scraper_session_id=${job?.platform_scraper_session_id ?? 'null'} ` +
          `session_row=${session ? 'yes' : 'no'} cookie_count=${n}. ${poolHint}`
      );
      await failScrapeJob(
        jobId,
        'No platform scraper with a valid session (cold_dm_platform_scraper_sessions). Per-client cold_dm_scraper_sessions is not used.'
      );
      return;
    }

    logger.log(`[Scraper] Job ${jobId} session OK; launching browser for comment scrape`);

    browser = await puppeteer.launch({
      headless: HEADLESS,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    await applyMobileEmulation(page);
    await page.setCookie(...session.session_data.cookies);
    await page.goto('https://www.instagram.com/', { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(randomDelay(1500, 3500));

    if (page.url().includes('/accounts/login')) {
      await failScrapeJob(jobId, 'Scraper session expired. Reconnect scraper.');
      return;
    }

    logger.log('[Scraper] Warming session before comment scrape...');
    await delay(3000 + Math.floor(Math.random() * 5000));
    await page.evaluate(() => window.scrollTo(0, 200 + Math.random() * 500));
    await delay(2000 + Math.floor(Math.random() * 3000));

    logger.log('[Scraper] Comment scrape: ' + postUrls.length + ' post(s)');
    let totalScraped = 0;
    const seenUsernames = new Set();
    const [inConvos, sentUsernames, blocklistUsernames] = await Promise.all([
      getConversationParticipantUsernames(clientId),
      getSentUsernames(clientId),
      getScrapeBlocklistUsernames(clientId),
    ]);
    const scraperUsername = (session?.instagram_username || '').trim().replace(/^@/, '').toLowerCase();
    if (scraperUsername) seenUsernames.add(scraperUsername);

    for (const postUrl of postUrls) {
      const jobCheck = await getScrapeJob(jobId);
      if (jobCheck && jobCheck.status === 'cancelled') {
        logger.log('[Scraper] Job cancelled');
        break;
      }

      const shortcode = getShortcodeFromPostUrl(postUrl);
      const normalizedUrl = postUrl.includes('instagram.com') ? postUrl : 'https://www.instagram.com/p/' + postUrl + '/';
      await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));

      const candidateAuthors = await page.evaluate(function () {
        const blacklist = ['explore', 'direct', 'accounts', 'reels', 'stories', 'p', 'tv', 'tags'];
        const out = [];
        const anchors = document.querySelectorAll('a[href^="/"]');
        for (let i = 0; i < Math.min(anchors.length, 20); i++) {
          const href = (anchors[i].getAttribute('href') || '').trim();
          const m = href.match(/^\/([^/?#]+)\/?$/);
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
          break;
        }
        await page.evaluate(() => window.scrollBy(0, 200));
        await delay(randomDelay(800, 1500));
      }

      await delay(randomDelay(3000, 5000));

      const SCRAPER_DEBUG = process.env.SCRAPER_DEBUG === '1' || process.env.SCRAPER_DEBUG === 'true';
      let anchorCount = await page.evaluate(() => document.querySelectorAll('a[href^="/"]').length);
      if (SCRAPER_DEBUG) logger.log('[Scraper] After open: anchors=' + anchorCount);

      for (let s = 0; s < 8; s++) {
        await page.evaluate(function () {
          const all = document.querySelectorAll('div, section, main, [role="main"]');
          for (let i = 0; i < all.length; i++) {
            const el = all[i];
            if (el.scrollHeight > el.clientHeight && el.offsetParent) {
              el.scrollTop = Math.min(el.scrollTop + 400, el.scrollHeight);
            }
          }
          window.scrollBy(0, 300);
        });
        await delay(randomDelay(1500, 3000));
        const prev = anchorCount;
        anchorCount = await page.evaluate(() => document.querySelectorAll('a[href^="/"]').length);
        if (SCRAPER_DEBUG) logger.log('[Scraper] Scroll ' + s + ': anchors=' + anchorCount);
        if (anchorCount > 15 && anchorCount === prev) break;
      }

      let noNewCount = 0;
      let scrollCount = 0;

      while (true) {
        const usernames = await page.evaluate(function () {
          const out = [];
          const anchors = document.querySelectorAll('a[href^="/"]');
          for (let i = 0; i < anchors.length; i++) {
            const href = (anchors[i].getAttribute('href') || '').trim();
            const m = href.match(/^\/([^/?#]+)\/?$/);
            if (!m) continue;
            const u = m[1].toLowerCase();
            if (u && u.length >= 2 && u.length <= 30 && /^[a-z0-9._]+$/.test(u)) out.push(u);
          }
          return [...new Set(out)];
        });

        const SCRAPER_DEBUG = process.env.SCRAPER_DEBUG === '1' || process.env.SCRAPER_DEBUG === 'true';
        if (SCRAPER_DEBUG) {
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
        if (maxLeads && totalScraped + newUsernames.length > maxLeads) {
          newUsernames = newUsernames.slice(0, maxLeads - totalScraped);
        }
        for (const u of newUsernames) seenUsernames.add(u);

        if (newUsernames.length > 0) {
          await upsertLeadsBatch(clientId, newUsernames, source, leadGroupId);
          totalScraped = seenUsernames.size;
          await updateScrapeJob(jobId, { scraped_count: totalScraped });
          noNewCount = 0;
          logger.log('[Scraper] Comments: +' + newUsernames.length + ' new, total ' + totalScraped);
          if (maxLeads && totalScraped >= maxLeads) break;
        } else {
          noNewCount++;
          if (!SCRAPER_DEBUG && usernames.length > 0 && noNewCount === 1) {
            logger.log('[Scraper] Comment extract: raw=' + usernames.length + ', new=0 (set SCRAPER_DEBUG=1 for details)');
          }
          if (noNewCount >= 3) break;
        }

        const commentsOpened = await page.evaluate(function () {
          const btns = Array.from(document.querySelectorAll('span, a, [role="button"]'));
          const commentBtn = btns.find(function (b) {
            const t = (b.textContent || '').toLowerCase();
            return t.includes('comment') || t === 'view all' || /^\d+\s*comment/.test(t);
          });
          if (commentBtn) {
            commentBtn.click();
            return true;
          }
          return false;
        });
        if (commentsOpened) await delay(randomDelay(1500, 3500));

        const scrollDebug = await page.evaluate(function () {
          const sel = 'div[style*="overflow"], [role="dialog"], section, article, div[style*="overflow-y"]';
          const scrollables = Array.from(document.querySelectorAll(sel));
          const info = scrollables.slice(0, 8).map(function (s, i) {
            return '#' + i + ':' + s.scrollHeight + '/' + s.clientHeight + ' top=' + s.scrollTop;
          });
          return { count: scrollables.length, items: info, anchors: document.querySelectorAll('a[href^="/"]').length };
        });
        if (SCRAPER_DEBUG) {
          logger.log('[Scraper] Scroll: containers=' + scrollDebug.count + ' anchors=' + scrollDebug.anchors + ' ' + scrollDebug.items.join(' '));
        }

        const scrolled = await page.evaluate(function () {
          const sel = 'div[style*="overflow"], [role="dialog"], section, article, div[style*="overflow-y"]';
          const scrollables = Array.from(document.querySelectorAll(sel));
          let didScroll = false;
          for (let i = 0; i < scrollables.length; i++) {
            const s = scrollables[i];
            if (!s.offsetParent) continue;
            const sh = s.scrollHeight;
            const ch = s.clientHeight;
            if (sh > ch) {
              const prev = s.scrollTop;
              s.scrollTop = s.scrollHeight;
              if (s.scrollTop !== prev) didScroll = true;
            }
          }
          window.scrollBy(0, 400);
          return true;
        });
        if (!scrolled) break;
        await delay(randomDelay(2000, 4000));
        scrollCount++;
        if (scrollCount > 20) break;
      }

      await delay(randomDelay(SCRAPE_DELAY_MIN_MS, SCRAPE_DELAY_MAX_MS));
    }

    try {
      await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await delay(3000 + Math.floor(Math.random() * 5000));
      await page.evaluate(() => window.scrollTo(0, 200));
      await delay(5000 + Math.floor(Math.random() * 8000));
      logger.log('[Scraper] Post-scrape warm done.');
    } catch (e) {
      logger.warn('[Scraper] Post-scrape warm skipped: ' + e.message);
    }

    if (platformSessionId && totalScraped > 0) {
      const actionCount = Math.max(20, totalScraped + 10);
      await recordScraperActions(platformSessionId, actionCount).catch(() => {});
    }

    await updateScrapeJob(jobId, { status: 'completed', scraped_count: totalScraped });
    logger.log('[Scraper] Comment job ' + jobId + ' completed. Scraped ' + totalScraped + ' leads.');
  } catch (err) {
    logger.error('[Scraper] Comment scrape failed', err);
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
  runCommentScrape,
  // Optional GraphQL-based fallbacks (not wired into the API by default)
  fetchFollowersViaGraphQL,
  fetchCommentsViaGraphQL,
};
