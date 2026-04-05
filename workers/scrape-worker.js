#!/usr/bin/env node
/**
 * Polls Supabase for pending scrape jobs, claims one at a time, runs Puppeteer scraper.
 * Run under PM2 as `ig-dm-scrape`. Set SCRAPE_DEFER_TO_WORKER=1 on the API so
 * dashboard enqueue jobs without running scrapes in the same process.
 */
require('dotenv').config();
const os = require('os');
const logger = require('../utils/logger');
const sb = require('../database/supabase');
const { runFollowerScrape, runCommentScrape } = require('../scraper');

const SCRAPER_POLL_MS = Math.max(2000, parseInt(process.env.SCRAPER_WORKER_POLL_MS || '5000', 10) || 5000);
const LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normalizeTarget(job) {
  const t = (job.target_username || '').trim().replace(/^@/, '');
  if (job.scrape_type === 'comments') return '_comment_scrape';
  return t;
}

async function processOneJob(workerId, job) {
  let reservedPlatformId = null;
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
          `[scrape-worker] could not reserve a platform scraper for job ${job.id} (need Puppeteer cookies on pool rows). ${hint}`
        );
      }
    }

    const leaseOpts = {
      jobId: job.id,
      workerId,
      leaseSec: LEASE_SEC,
      platformSessionId: job.platform_scraper_session_id || null,
    };

    const scrapeType = job.scrape_type === 'comments' ? 'comments' : 'followers';
    logger.log(
      `[scrape-worker] begin scrape job=${job.id} type=${scrapeType} max_leads=${job.max_leads ?? '—'} ` +
        (scrapeType === 'followers'
          ? `target=@${String(job.target_username || '').replace(/^@/, '')}`
          : `posts=${Array.isArray(job.post_urls) ? job.post_urls.length : 0}`)
    );
    if (scrapeType === 'followers') {
      await runFollowerScrape(String(job.client_id), String(job.id), normalizeTarget(job), {
        maxLeads: job.max_leads,
        leadGroupId: job.lead_group_id,
        leaseOptions: leaseOpts,
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
  } finally {
    if (reservedPlatformId) {
      await sb.releasePlatformScraperSessionLease(reservedPlatformId, workerId).catch(() => {});
    }
  }
}

async function main() {
  if (!sb.isSupabaseConfigured()) {
    logger.error('[scrape-worker] Supabase not configured (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY)');
    process.exit(1);
  }

  const workerId = `scrape-${os.hostname()}-${process.pid}-${Date.now().toString(36)}`;
  logger.log(`[scrape-worker] started id=${workerId} poll=${SCRAPER_POLL_MS}ms`);

  for (;;) {
    await sb.workerHeartbeat(workerId, 'scrape', { pid: process.pid });

    const job = await sb.claimColdDmScrapeJob(workerId, LEASE_SEC);
    if (!job) {
      await delay(SCRAPER_POLL_MS);
      continue;
    }

    const postsN = Array.isArray(job.post_urls) ? job.post_urls.length : 0;
    const tgt =
      job.scrape_type === 'comments'
        ? `${postsN} post(s)`
        : `@${String(job.target_username || '').replace(/^@/, '') || '—'}`;
    logger.log(
      `[scrape-worker] claimed job ${job.id} client=${job.client_id} type=${job.scrape_type} target=${tgt} max_leads=${job.max_leads ?? '—'}`
    );
    try {
      await processOneJob(workerId, job);
    } catch (e) {
      logger.error(`[scrape-worker] job ${job.id} error`, e);
      await sb.updateScrapeJob(job.id, {
        status: 'failed',
        error_message: (e && e.message) || String(e),
        last_error_class: 'worker_exception',
      }).catch(() => {});
    }
  }
}

main().catch((e) => {
  logger.error('[scrape-worker] fatal', e);
  process.exit(1);
});
