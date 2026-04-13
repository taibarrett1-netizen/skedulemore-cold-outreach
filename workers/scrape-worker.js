#!/usr/bin/env node
/**
 * Polls Supabase for pending scrape jobs and runs them concurrently — one job
 * per available platform scraper session.  Concurrency is auto-detected from
 * the pool: with 4 active sessions you get 4 parallel Puppeteer scrapes, with
 * 1 session you get 1.  The pool is re-checked every SCRAPE_POOL_RECHECK_MS
 * so adding or removing sessions takes effect without a restart.
 *
 * Run under PM2 as `ig-dm-scrape` (a single instance handles all concurrency).
 * Set SCRAPE_DEFER_TO_WORKER=1 on the API so the dashboard enqueues jobs
 * without running scrapes in the same process.
 *
 * Key env vars:
 *   SCRAPE_MAX_CONCURRENT          — optional hard cap on parallel jobs.
 *                                    Leave unset/0 to match pool size exactly.
 *   SCRAPE_POOL_RECHECK_MS         — how often to re-count active sessions
 *                                    (default: 120000 = 2 min)
 *   SCRAPE_FAILURE_COOLDOWN_SEC    — seconds to cool a session after any job
 *                                    failure (default: 300 = 5 min)
 *   SCRAPER_WORKER_POLL_MS         — idle poll when no jobs queued (default: 2000 ms)
 *   SCRAPER_SESSION_LEASE_SEC      — session lease duration (default: 240 s)
 *   SCRAPER_DEBUG=1                — verbose scrape logs; comment jobs also log DOM snapshots
 *                                    (href samples, igAbsProfileHints, dialogs, view-all text)
 *                                    and write viewport PNGs under logs/comment-scrape-debug/
 *                                    (after-home-session-check, warm-scroll-start / -end per post)
 *   SCRAPER_SESSION_DEBUG=1        — like SCRAPER_DEBUG but only extra session-ensure logs/PNGs
 *                                    if you want session diagnostics without full comment DOM debug
 *   SCRAPER_SESSION_FULL_SCREENSHOTS=1 — with session debug: capture every session_ensure PNG (default: round 0+4 only)
 *   SCRAPER_COMMENT_HOME_SCREENSHOT=1 — with SCRAPER_DEBUG: also save home after-home-session-check PNG
 *   SCRAPER_COMMENT_WARM_SCROLLS       — comment thread warm-up scroll rounds (default 3, max 8)
 *   SCRAPER_COMMENT_NO_NEW_GIVEUP      — stop after this many extract rounds with zero new leads (default 3, max 8)
 *   SCRAPER_COMMENT_MAX_SCROLL_ITERS   — cap on main comment scroll loop iterations (default 12 or 20 if max_leads>30)
 */
require('dotenv').config();
const os = require('os');
const logger = require('../utils/logger');
const sb = require('../database/supabase');
const { runFollowerScrape, runCommentScrape } = require('../scraper');

const SCRAPER_POLL_MS = Math.max(1000, parseInt(process.env.SCRAPER_WORKER_POLL_MS || '2000', 10) || 2000);
const LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
const FAILURE_COOLDOWN_SEC = Math.max(0, parseInt(process.env.SCRAPE_FAILURE_COOLDOWN_SEC || '300', 10) || 300);
// Rate-limit (429) gets a much longer cooldown so the session can recover.
const RATE_LIMIT_COOLDOWN_SEC = Math.max(0, parseInt(process.env.SCRAPE_RATE_LIMIT_COOLDOWN_SEC || '3600', 10) || 3600);
// Optional hard cap. When unset (0), concurrency is auto-derived from the pool.
const MAX_CONCURRENT_CAP = Math.max(0, parseInt(process.env.SCRAPE_MAX_CONCURRENT || '0', 10) || 0);
// Re-check pool size every this many ms so concurrency adjusts when sessions are added/removed.
const POOL_RECHECK_MS = Math.max(30_000, parseInt(process.env.SCRAPE_POOL_RECHECK_MS || '120000', 10) || 120_000);

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normalizeTarget(job) {
  const t = (job.target_username || '').trim().replace(/^@/, '');
  if (job.scrape_type === 'comments') return '_comment_scrape';
  return t;
}

/**
 * Run a single scrape job, reserve/release the platform session around it.
 * Returns true on success, false on any failure.
 * Always releases the session lease in the finally block; applies a cooldown
 * on failure so the same session isn't immediately re-used.
 */
async function processOneJob(workerId, job) {
  let reservedPlatformId = null;
  let jobFailed = false;
  let finalErrorClass = null;
  try {
    if (!job.platform_scraper_session_id) {
      const reserved = await sb.reservePlatformScraperSessionForWorker(workerId, LEASE_SEC);
      if (reserved?.id) {
        reservedPlatformId = reserved.id;
        await sb.updateScrapeJob(job.id, { platform_scraper_session_id: reserved.id });
        job.platform_scraper_session_id = reserved.id;
        logger.log(
          `[scrape-worker] reserved platform session ${reserved.id} for job ${job.id}`
        );
      } else {
        const hint = await sb.describePlatformScraperPoolForLogs().catch(() => '');
        logger.error(
          `[scrape-worker] could not reserve a platform scraper for job ${job.id} (need Puppeteer cookies on pool rows). ${hint} ` +
          `Check PM2 stderr for [platform-scraper-reserve] (set PLATFORM_SCRAPER_RESERVE_DEBUG=1 for per-attempt success logs).`
        );
        await sb.retryScrapeJob(job.id, 'waiting_for_platform_scraper_session', 60, workerId).catch(() => {});
        return false;
      }
    }

    const leaseOpts = {
      jobId: job.id,
      workerId,
      leaseSec: LEASE_SEC,
      platformSessionId: job.platform_scraper_session_id || null,
    };

    const scrapeType =
      job.scrape_type === 'comments'
        ? 'comments'
        : job.scrape_type === 'following'
        ? 'following'
        : 'followers';

    logger.log(
      `[scrape-worker] begin scrape job=${job.id} type=${scrapeType} max_leads=${job.max_leads ?? '—'} ` +
        (scrapeType === 'comments'
          ? `posts=${Array.isArray(job.post_urls) ? job.post_urls.length : 0}`
          : `target=@${String(job.target_username || '').replace(/^@/, '')}`)
    );

    if (scrapeType === 'followers' || scrapeType === 'following') {
      await runFollowerScrape(String(job.client_id), String(job.id), normalizeTarget(job), {
        maxLeads: job.max_leads,
        leadGroupId: job.lead_group_id,
        leaseOptions: leaseOpts,
        listKind: scrapeType === 'following' ? 'following' : 'followers',
      });
    } else {
      const urls = Array.isArray(job.post_urls) ? job.post_urls : [];
      await runCommentScrape(String(job.client_id), String(job.id), urls, {
        maxLeads: job.max_leads,
        leadGroupId: job.lead_group_id,
        leaseOptions: leaseOpts,
      });
    }

    logger.log(`[scrape-worker] scrape routine returned job=${job.id} (status updated in DB by scraper)`);

    // The scraper updates the DB itself — check the final status so we know
    // whether to apply a session cooldown even when no exception was thrown.
    try {
      const finalJob = await sb.getScrapeJob(job.id);
      if (finalJob) {
        finalErrorClass = finalJob.last_error_class || null;
        if (finalJob.status === 'failed') {
          jobFailed = true;
          logger.log(
            `[scrape-worker] job ${job.id} failed by scraper (error_class=${finalErrorClass || 'none'})`
          );
        } else if (finalJob.status === 'retry') {
          // 429 re-queue: the session still needs its cooldown applied but the
          // job itself is not permanently failed.
          jobFailed = true;
          logger.log(
            `[scrape-worker] job ${job.id} re-queued by scraper (error_class=${finalErrorClass || 'none'}) — cooldown will apply to session`
          );
        }
      }
    } catch (_) {}

    return !jobFailed;
  } catch (e) {
    jobFailed = true;
    logger.error(`[scrape-worker] job ${job.id} error`, e);
    await sb.updateScrapeJob(job.id, {
      status: 'failed',
      error_message: (e && e.message) || String(e),
      last_error_class: 'worker_exception',
    }).catch(() => {});
    return false;
  } finally {
    if (reservedPlatformId) {
      let skipRelease = false;
      if (jobFailed) {
        let errClass = finalErrorClass || 'scrape_failure';
        let errMsg = '';
        try {
          const em = await sb.getScrapeJob(job.id);
          errMsg = (em && (em.last_error_message || em.error_message)) || '';
          if (!finalErrorClass && em?.last_error_class) errClass = em.last_error_class;
        } catch (_) {}
        const cooldownSec =
          errClass === 'rate_limited_429' ? RATE_LIMIT_COOLDOWN_SEC : FAILURE_COOLDOWN_SEC;
        const reported = await sb
          .reportPlatformScraperScrapeFailure(job.id, reservedPlatformId, workerId, errClass, errMsg, {
            cooldownSec,
          })
          .catch(() => ({ ok: false }));
        if (reported && reported.ok) {
          skipRelease = true;
          logger.log(
            `[scrape-worker] session ${reservedPlatformId} failure reported via RPC ` +
              `(requeued=${reported.requeued} final_failure=${reported.final_failure})`
          );
        }
      }
      if (!skipRelease) {
        let cooldownSec = 0;
        if (jobFailed) {
          cooldownSec =
            finalErrorClass === 'rate_limited_429' ? RATE_LIMIT_COOLDOWN_SEC : FAILURE_COOLDOWN_SEC;
        }
        if (cooldownSec > 0) {
          logger.log(
            `[scrape-worker] session ${reservedPlatformId} cooldown=${cooldownSec}s` +
            (finalErrorClass ? ` (${finalErrorClass})` : '')
          );
        }
        await sb.releasePlatformScraperSessionLease(reservedPlatformId, workerId, { cooldownSec }).catch(() => {});
      }
    }
  }
}

async function main() {
  if (!sb.isSupabaseConfigured()) {
    logger.error('[scrape-worker] Supabase not configured (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY)');
    process.exit(1);
  }

  const workerId = `scrape-${os.hostname()}-${process.pid}-${Date.now().toString(36)}`;

  // Auto-detect concurrency from pool size and re-check periodically.
  let effectiveConcurrent = 1;
  let lastPoolCheckAt = 0;

  async function refreshPoolConcurrency() {
    try {
      const poolCount = await sb.countActivePlatformScraperSessions();
      const next = MAX_CONCURRENT_CAP > 0 ? Math.min(poolCount, MAX_CONCURRENT_CAP) : poolCount;
      effectiveConcurrent = Math.max(1, next);
      lastPoolCheckAt = Date.now();
      logger.log(
        `[scrape-worker] pool has ${poolCount} active session(s) → ` +
        `concurrency=${effectiveConcurrent}` +
        (MAX_CONCURRENT_CAP > 0 ? ` (cap=${MAX_CONCURRENT_CAP})` : '')
      );
    } catch (e) {
      logger.error('[scrape-worker] could not count scraper pool sessions', e);
    }
  }

  await refreshPoolConcurrency();
  logger.log(
    `[scrape-worker] started id=${workerId} poll=${SCRAPER_POLL_MS}ms ` +
    `concurrency=${effectiveConcurrent} failure_cooldown=${FAILURE_COOLDOWN_SEC}s`
  );

  // activeJobs: Map<jobId, Promise<boolean>>
  // Each entry is a running processOneJob promise. When the promise settles it
  // removes itself from the map so the slot becomes available.
  const activeJobs = new Map();

  for (;;) {
    // Periodically re-check pool size so concurrency adjusts when sessions are added/removed.
    if (Date.now() - lastPoolCheckAt > POOL_RECHECK_MS) {
      await refreshPoolConcurrency();
    }

    await sb.workerHeartbeat(workerId, 'scrape', { pid: process.pid, activeJobs: activeJobs.size });

    // Fill every available slot up to effectiveConcurrent.
    while (activeJobs.size < effectiveConcurrent) {
      const job = await sb.claimColdDmScrapeJob(workerId, LEASE_SEC);
      if (!job) break; // No pending jobs right now.

      const postsN = Array.isArray(job.post_urls) ? job.post_urls.length : 0;
      const tgt =
        job.scrape_type === 'comments'
          ? `${postsN} post(s)`
          : `@${String(job.target_username || '').replace(/^@/, '') || '—'}`;
      logger.log(
        `[scrape-worker] claimed job ${job.id} client=${job.client_id} type=${job.scrape_type} ` +
        `target=${tgt} max_leads=${job.max_leads ?? '—'} active=${activeJobs.size + 1}/${effectiveConcurrent}`
      );

      const p = processOneJob(workerId, job).finally(() => {
        activeJobs.delete(job.id);
      });
      activeJobs.set(job.id, p);
    }

    if (activeJobs.size === 0) {
      // Nothing running and no jobs in queue — idle poll.
      await delay(SCRAPER_POLL_MS);
    } else if (activeJobs.size >= effectiveConcurrent) {
      // All slots full — wait for at least one to finish before claiming more.
      await Promise.race(activeJobs.values());
    } else {
      // Some slots free but queue is empty — short poll in case new jobs arrive.
      await delay(SCRAPER_POLL_MS);
    }
  }
}

main().catch((e) => {
  logger.error('[scrape-worker] fatal', e);
  process.exit(1);
});
