/**
 * PM2: dashboard (API + static UI), send worker (DMs), scrape worker (optional queue).
 * Usage: pm2 start ecosystem.config.cjs
 *
 * Recommended VPS env:
 *   SCRAPE_DEFER_TO_WORKER=1   — enqueue scrapes only; ig-dm-scrape runs workers/scrape-worker.js
 *   SEND_WORKER_ENTRY=workers/send-worker.js — used by dashboard "Start" to launch the sender
 */
module.exports = {
  apps: [
    {
      name: 'ig-dm-dashboard',
      script: 'server.js',
      max_memory_restart: '512M',
    },
    {
      name: 'ig-dm-send',
      script: 'workers/send-worker.js',
      autorestart: false,
      max_restarts: 20,
      min_uptime: 5000,
    },
    {
      name: 'ig-dm-scrape',
      script: 'workers/scrape-worker.js',
      autorestart: true,
      max_memory_restart: '1G',
      // Set in env or .env — do not commit secrets
      env: {},
    },
  ],
};
