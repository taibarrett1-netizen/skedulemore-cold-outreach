#!/usr/bin/env node
/**
 * Dedicated send worker: same behavior as `node cli.js --start` / multi-tenant runBot().
 * Run under PM2 as `ig-dm-send` or `ig-dm-bot` so the dashboard API stays responsive.
 */
require('dotenv').config();
const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const sb = require('../database/supabase');

const csvPath = process.env.LEADS_CSV || path.join(process.cwd(), 'leads.csv');

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
