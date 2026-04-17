#!/usr/bin/env node
/**
 * Per-client scrape worker (instagrapi).
 *
 * This worker drains cold_dm_scrape_jobs via the service-role claim RPC and runs a
 * conservative Python scraper per job. Execution is intentionally NOT on Vercel.
 *
 * Concurrency:
 *   SCRAPE_MAX_CONCURRENT (default 1). We do not auto-scale from any shared pool.
 *
 * Lease heartbeat:
 *   Keeps cold_dm_scrape_jobs leased_until fresh while Python is running so the job
 *   is not re-claimed mid-scrape.
 */
require('dotenv').config();

const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const logger = require('../utils/logger');
const sb = require('../database/supabase');

const SCRAPER_POLL_MS = Math.max(1000, parseInt(process.env.SCRAPER_WORKER_POLL_MS || '2000', 10) || 2000);
const SCRAPER_SLOT_POLL_MS = Math.max(50, parseInt(process.env.SCRAPER_SLOT_POLL_MS || '400', 10) || 400);
const LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
const MAX_CONCURRENT = Math.max(1, parseInt(process.env.SCRAPE_MAX_CONCURRENT || '1', 10) || 1);

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function pythonCmd() {
  return process.env.SCRAPER_PYTHON || process.env.ADMIN_LAB_PYTHON || 'python3';
}

function safeFollowersScriptPath() {
  return path.join(__dirname, '..', 'scraper_worker', 'safe_followers_scrape.py');
}

async function runPythonFollowerScrape(jobId) {
  const py = pythonCmd();
  const script = safeFollowersScriptPath();
  const args = [script, '--job_id', String(jobId)];

  return await new Promise((resolve) => {
    const child = spawn(py, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    child.stdout.on('data', (d) => (out += d.toString('utf8')));
    child.stderr.on('data', (d) => (err += d.toString('utf8')));
    child.on('close', (code) => resolve({ code: code ?? 0, out, err }));
  });
}

async function processOneJob(workerId, job) {
  const jobId = job.id;
  let leaseHbTimer = null;
  const hbIntervalMs = Math.min(120000, Math.max(30000, LEASE_SEC * 250));
  leaseHbTimer = setInterval(() => {
    sb.heartbeatScrapeJobLease(jobId, workerId, LEASE_SEC).catch(() => {});
  }, hbIntervalMs);

  try {
    // Respect global pause: when sending is paused for support, do not continue scraping for this client.
    const pause = await sb.getControl(String(job.client_id)).catch(() => null);
    if (pause === '1' || pause === 1) {
      await sb
        .retryScrapeJob(job.id, 'client_paused', 3600, workerId)
        .catch(() => {});
      logger.warn(`[scrape-worker] client paused; deferring scrape job=${job.id} client=${job.client_id}`);
      return false;
    }
    if ((job.scrape_type || 'followers') !== 'followers') {
      await sb
        .updateScrapeJob(jobId, {
          status: 'failed',
          last_error_class: 'unsupported_scrape_type',
          error_message: 'Only follower scraping is supported right now.',
        })
        .catch(() => {});
      return false;
    }

    logger.log(`[scrape-worker] run job=${jobId} client=${job.client_id} target=@${job.target_username || '—'} max_leads=${job.max_leads ?? '—'}`);

    const r = await runPythonFollowerScrape(jobId);
    if (r && r.err && String(r.err).trim()) {
      // Avoid spamming logs with huge traces.
      logger.warn(`[scrape-worker] python stderr job=${jobId}: ${String(r.err).slice(-1200)}`);
    }

    // The Python runner updates the job row. Treat non-zero exit as an infrastructure error.
    if (r && Number(r.code) !== 0) {
      const finalJob = await sb.getScrapeJob(jobId).catch(() => null);
      if (!finalJob || (finalJob.status !== 'failed' && finalJob.status !== 'cancelled' && finalJob.status !== 'completed')) {
        await sb
          .updateScrapeJob(jobId, {
            status: 'failed',
            last_error_class: 'worker_exit',
            error_message: `Scrape worker exited with code ${r.code}. Check VPS logs.`,
          })
          .catch(() => {});
      }
      return false;
    }

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

  for (;;) {
    await sb.workerHeartbeat(workerId, 'scrape', { pid: process.pid, activeJobs: activeJobs.size }).catch(() => {});

    while (activeJobs.size < MAX_CONCURRENT) {
      const job = await sb.claimColdDmScrapeJob(workerId, LEASE_SEC);
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
