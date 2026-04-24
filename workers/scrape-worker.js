#!/usr/bin/env node
/**
 * Per-client scrape worker using the legacy Puppeteer scraper.
 *
 * This drains cold_dm_scrape_jobs and runs the older browser-based follower/following
 * scraper against the per-client Instagram session row.
 */
require('dotenv').config();

const os = require('os');
const logger = require('../utils/logger');
const sb = require('../database/supabase');
const legacyScraper = require('../scraper');

const SCRAPER_POLL_MS = Math.max(1000, parseInt(process.env.SCRAPER_WORKER_POLL_MS || '2000', 10) || 2000);
const SCRAPER_SLOT_POLL_MS = Math.max(50, parseInt(process.env.SCRAPER_SLOT_POLL_MS || '400', 10) || 400);
const LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
const MAX_CONCURRENT = Math.max(1, parseInt(process.env.SCRAPE_MAX_CONCURRENT || '1', 10) || 1);
const SEND_SCRAPE_COOLDOWN_MS = Math.max(
  60 * 1000,
  parseInt(process.env.SEND_SCRAPE_COOLDOWN_MS || String(5 * 60 * 1000), 10) || 5 * 60 * 1000
);
const WORKER_HEARTBEAT_MS = Math.max(
  15000,
  parseInt(process.env.SCRAPER_WORKER_HEARTBEAT_MS || '30000', 10) || 30000
);
const CLAIM_ERROR_BACKOFF_MIN_MS = Math.max(
  1000,
  parseInt(process.env.SCRAPER_CLAIM_ERROR_BACKOFF_MIN_MS || '5000', 10) || 5000
);
const CLAIM_ERROR_BACKOFF_MAX_MS = Math.max(
  CLAIM_ERROR_BACKOFF_MIN_MS,
  parseInt(process.env.SCRAPER_CLAIM_ERROR_BACKOFF_MAX_MS || '120000', 10) || 120000
);

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function backoffMs(attempt, minMs = CLAIM_ERROR_BACKOFF_MIN_MS, maxMs = CLAIM_ERROR_BACKOFF_MAX_MS) {
  const exp = Math.min(maxMs, minMs * Math.pow(2, Math.min(Math.max(0, attempt), 6)));
  return Math.min(maxMs, exp + Math.floor(exp * (0.2 + Math.random() * 0.3)));
}

async function processOneJob(workerId, job) {
  const jobId = job.id;
  let leaseHbTimer = null;
  let instagramSessionLeaseHbTimer = null;
  let leasedInstagramSessionId = null;
  const hbIntervalMs = Math.min(120000, Math.max(30000, LEASE_SEC * 250));
  leaseHbTimer = setInterval(() => {
    sb.heartbeatScrapeJobLease(jobId, workerId, LEASE_SEC).catch(() => {});
  }, hbIntervalMs);

  try {
    const activelySendableNow = await sb.canClientActivelySendNow(String(job.client_id)).catch(() => false);
    if (activelySendableNow) {
      await sb.retryScrapeJob(job.id, 'campaigns_active', 3600, workerId).catch(() => {});
      logger.warn(
        `[scrape-worker] client has an actively sendable campaign; deferring scrape job=${job.id} client=${job.client_id}`
      );
      return false;
    }

    const latestSentAtIso = await sb.getLatestSuccessfulColdDmSentAt(String(job.client_id)).catch(() => null);
    if (latestSentAtIso) {
      const latestSentAtMs = new Date(latestSentAtIso).getTime();
      const cooldownRemainingMs = latestSentAtMs + SEND_SCRAPE_COOLDOWN_MS - Date.now();
      if (Number.isFinite(cooldownRemainingMs) && cooldownRemainingMs > 0) {
        await sb.retryScrapeJob(job.id, 'recent_send_cooldown', Math.ceil(cooldownRemainingMs / 1000), workerId).catch(() => {});
        logger.warn(
          `[scrape-worker] recent send cooldown; deferring scrape job=${job.id} client=${job.client_id} wait=${Math.ceil(
            cooldownRemainingMs / 1000
          )}s`
        );
        return false;
      }
    }

    const jobSessionId = String(job.instagram_session_id || '').trim() || null;
    const sessionRow = jobSessionId
      ? await sb.getInstagramSessionByIdForClient(String(job.client_id), jobSessionId).catch(() => null)
      : await sb.getMostRecentInstagramSessionForClient(String(job.client_id)).catch(() => null);
    if (sessionRow?.scrape_cooldown_until) {
      const scrapeCooldownUntilMs = new Date(sessionRow.scrape_cooldown_until).getTime();
      const cooldownRemainingMs = scrapeCooldownUntilMs - Date.now();
      // claim_cold_dm_scrape_job sets a 5-minute session cooldown at claim time to prevent
      // parallel claims. The job that just won the claim must still run.
      const claimedThisRun = job.status === 'running' && job.leased_by_worker === workerId;
      if (Number.isFinite(cooldownRemainingMs) && cooldownRemainingMs > 0 && !claimedThisRun) {
        await sb
          .retryScrapeJob(job.id, 'scrape_cooldown', Math.ceil(cooldownRemainingMs / 1000), workerId)
          .catch(() => {});
        logger.warn(
          `[scrape-worker] session ${sessionRow.id} in scrape cooldown; deferring job=${job.id} client=${job.client_id} wait=${Math.ceil(
            cooldownRemainingMs / 1000
          )}s`
        );
        return false;
      }
    } else if (!sessionRow?.id) {
      await sb.retryScrapeJob(job.id, 'missing_instagram_session', 300, workerId).catch(() => {});
      logger.warn(`[scrape-worker] no instagram session found; deferring scrape job=${job.id} client=${job.client_id}`);
      return false;
    }

    const sessionLeaseWorkerId = `scrape-session-${workerId}`;
    const leaseOk = await sb
      .claimInstagramSessionLease(sessionRow.id, sessionLeaseWorkerId, LEASE_SEC)
      .catch(() => false);
    if (!leaseOk) {
      await sb.retryScrapeJob(job.id, 'waiting_for_session', 60, workerId).catch(() => {});
      logger.warn(
        `[scrape-worker] instagram session busy; deferring scrape job=${job.id} client=${job.client_id} session=${sessionRow.id}`
      );
      return false;
    }
    leasedInstagramSessionId = sessionRow.id;
    instagramSessionLeaseHbTimer = setInterval(() => {
      sb.heartbeatInstagramSessionLease(sessionRow.id, sessionLeaseWorkerId, LEASE_SEC).catch(() => {});
    }, hbIntervalMs);

    const scrapeType = (job.scrape_type || 'followers').toLowerCase();
    if (scrapeType !== 'followers' && scrapeType !== 'following') {
      await sb
        .updateScrapeJob(jobId, {
          status: 'failed',
          last_error_class: 'unsupported_scrape_type',
          error_message: 'Only follower and following scraping are supported right now.',
        })
        .catch(() => {});
      return false;
    }

    logger.log(
      `[scrape-worker] run job=${jobId} client=${job.client_id} type=${scrapeType} ` +
        `target=@${job.target_username || '—'} max_leads=${job.max_leads ?? '—'}`
    );

    const runner = scrapeType === 'following' ? legacyScraper.runFollowingScrape : legacyScraper.runFollowerScrape;
    await runner(job.client_id, job.id, job.target_username || '', {
      maxLeads: job.max_leads,
      leadGroupId: job.lead_group_id,
      leaseOptions: {
        jobId,
        workerId,
        leaseSec: LEASE_SEC,
      },
    });

    const finalJob = await sb.getScrapeJob(jobId).catch(() => null);
    if (finalJob?.status === 'failed') return false;
    if (finalJob?.status === 'retry') return false;
    return true;
  } catch (e) {
    logger.error(`[scrape-worker] job=${jobId} exception`, e);
    await sb
      .updateScrapeJob(jobId, {
        status: 'failed',
        last_error_class: 'worker_exception',
        error_message: (e && e.message) || String(e),
      })
      .catch(() => {});
    return false;
  } finally {
    if (leaseHbTimer) clearInterval(leaseHbTimer);
    if (instagramSessionLeaseHbTimer) clearInterval(instagramSessionLeaseHbTimer);
    if (leasedInstagramSessionId) {
      await sb.releaseInstagramSessionLease(leasedInstagramSessionId, `scrape-session-${workerId}`).catch(() => {});
    }
  }
}

async function main() {
  if (!sb.isSupabaseConfigured()) {
    logger.error('[scrape-worker] Supabase not configured (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY)');
    process.exit(1);
  }

  const workerId = `scrape-${os.hostname()}-${process.pid}-${Date.now().toString(36)}`;
  logger.log(
    `[scrape-worker] started id=${workerId} idle_poll=${SCRAPER_POLL_MS}ms slot_poll=${SCRAPER_SLOT_POLL_MS}ms ` +
      `concurrency=${MAX_CONCURRENT} lease=${LEASE_SEC}s`
  );

  const activeJobs = new Map();
  let lastHeartbeatAt = 0;
  let claimErrorCount = 0;

  for (;;) {
    const now = Date.now();
    if (now - lastHeartbeatAt >= WORKER_HEARTBEAT_MS) {
      lastHeartbeatAt = now;
      await sb.workerHeartbeat(workerId, 'scrape', { pid: process.pid, activeJobs: activeJobs.size }).catch(() => {});
    }

    while (activeJobs.size < MAX_CONCURRENT) {
      let job = null;
      try {
        job = await sb.claimColdDmScrapeJob(workerId, LEASE_SEC);
        claimErrorCount = 0;
      } catch (e) {
        const waitMs = backoffMs(claimErrorCount++);
        const cause = e?.cause
          ? ` cause=${e.cause.code || e.cause.name || e.cause.message || String(e.cause)}`
          : '';
        logger.warn(
          `[scrape-worker] claim failed; backing off ${Math.ceil(waitMs / 1000)}s (${e?.message || e}${cause})`
        );
        await delay(waitMs);
        break;
      }
      if (!job) break;

      logger.log(
        `[scrape-worker] claimed job=${job.id} client=${job.client_id} type=${job.scrape_type || 'followers'} ` +
          `target=@${String(job.target_username || '').replace(/^@/, '') || '—'} active=${activeJobs.size + 1}/${MAX_CONCURRENT}`
      );

      const p = processOneJob(workerId, job).finally(() => {
        activeJobs.delete(job.id);
      });
      activeJobs.set(job.id, p);
    }

    if (activeJobs.size === 0) {
      await delay(SCRAPER_POLL_MS);
    } else if (activeJobs.size >= MAX_CONCURRENT) {
      await Promise.race(activeJobs.values());
    } else {
      await Promise.race([...activeJobs.values(), delay(SCRAPER_SLOT_POLL_MS)]);
    }
  }
}

main().catch((e) => {
  logger.error('[scrape-worker] fatal', e);
  process.exit(1);
});
