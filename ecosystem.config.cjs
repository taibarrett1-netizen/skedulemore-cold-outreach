/**
 * PM2: dashboard (API + static UI), send worker (DMs), scrape worker (optional queue).
 * Usage: pm2 start ecosystem.config.cjs
 *
 * Recommended VPS env:
 *   SCRAPE_DEFER_TO_WORKER=1   — enqueue scrapes only; ig-dm-scrape runs workers/scrape-worker.js
 *   SEND_WORKER_ENTRY=workers/send-worker.js — used by dashboard "Start" to launch the sender
 *   SEND_WORKER_MIN / SEND_WORKER_MAX — bounds for getRecommendedSendWorkerInstanceCount + scripts/scale-send-workers.js
 *   PM2 cluster: NODE_APP_INSTANCE pins each ig-dm-send process to one active campaign’s queue (see SEND_WORKER_PIN_CAMPAIGNS in .env.example).
 *   npm run scale:send-workers — optional; ig-dm-dashboard auto-scales by default when Supabase is set (SCALE_SEND_WORKERS_AUTO=0 to disable)
 *
 * ig-dm-send kill_timeout: bounded by COLD_DM_INTERLEAVE_FOLLOW_UP_MAX_WAIT_MS (same-session shortcut only).
 * Follow-ups scheduled farther out (minutes–days) stay on cold_dm_follow_up_queue until pg_cron runs
 * process-scheduled-responses → Edge cold DM follow-ups → VPS /api/follow-up/send — the worker does not wait for those.
 */
const coldDmInterleaveFollowUpMaxWaitMs = Math.max(
  60_000,
  parseInt(process.env.COLD_DM_INTERLEAVE_FOLLOW_UP_MAX_WAIT_MS || '', 10) || 15 * 60 * 1000
);
/** PM2 must not SIGKILL during interleave wait + a short buffer for send/browser teardown. */
const igDmSendKillTimeoutMs = Math.max(180_000, coldDmInterleaveFollowUpMaxWaitMs + 120_000);

module.exports = {
  apps: [
    {
      name: 'ig-dm-dashboard',
      script: 'server.js',
      // Dashboard runs Puppeteer for Instagram connect; 512M causes PM2 restart loops under real load.
      max_memory_restart: process.env.PM2_DASHBOARD_MAX_MEMORY || '2048M',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    },
  ],
};
