#!/usr/bin/env node
/**
 * Dedicated send worker: same behavior as `node cli.js --start` / multi-tenant runBot().
 * Run under PM2 as `ig-dm-send` (see ecosystem.config.cjs). Do not use deprecated `ig-dm-bot`.
 */
require('dotenv').config({ quiet: true });
const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const sb = require('../database/supabase');

const csvPath = process.env.LEADS_CSV || path.join(process.cwd(), 'leads.csv');
const clientId = String(process.env.COLD_DM_CLIENT_ID || '').trim();
const logPrefix = clientId ? `[${clientId.slice(0, 8)}]` : '[unscoped]';

['log', 'warn', 'error'].forEach((method) => {
  const original = logger[method];
  if (typeof original !== 'function') return;
  logger[method] = (msg, ...rest) => original(`${logPrefix} ${msg}`, ...rest);
});

// database/supabase.js (and some deps) use console.* — prefix those too so worker logs grep cleanly.
['log', 'warn', 'error', 'info', 'debug'].forEach((method) => {
  const orig = console[method];
  if (typeof orig !== 'function') return;
  console[method] = (...args) => {
    if (args.length && typeof args[0] === 'string' && args[0].startsWith(logPrefix)) {
      return orig.apply(console, args);
    }
    if (args.length && typeof args[0] === 'string') {
      args[0] = `${logPrefix} ${args[0]}`;
    } else {
      args.unshift(logPrefix);
    }
    return orig.apply(console, args);
  };
});

async function main() {
  if (!sb.isSupabaseConfigured() && !fs.existsSync(csvPath)) {
    logger.error(`Leads file not found: ${csvPath}. Create leads.csv or set LEADS_CSV (or use Supabase).`);
    process.exit(1);
  }
  const { runBot } = require('../bot');
  await runBot();
}

main().catch((err) => {
  logger.error('send-worker exited with error', err);
  process.exit(1);
});
