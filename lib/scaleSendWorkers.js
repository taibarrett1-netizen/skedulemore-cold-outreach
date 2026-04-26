const { execSync } = require('child_process');

/**
 * @param {string} pm2AppName
 * @returns {number | null} instance count, or null if pm2 jlist failed
 */
function getPm2SendWorkerCount(pm2AppName) {
  try {
    const out = execSync('pm2 jlist', { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 });
    const list = JSON.parse(out);
    const exact = list.filter((p) => p.name === pm2AppName).length;
    const dynamic = list.filter((p) => String(p.name || '').startsWith('ig-dm-send-')).length;
    return { exact, dynamic, total: exact > 0 ? exact : dynamic };
  } catch {
    return null;
  }
}

/**
 * Reads Supabase-backed load signal and runs `pm2 scale <app> N` when N differs.
 * @param {object} [options]
 * @param {boolean} [options.dryRun]
 * @param {string} [options.pm2AppName]
 * @param {object} [options.supabase] — supabase module (defaults to ../database/supabase)
 */
async function runScaleSendWorkers(options = {}) {
  const dryRun = Boolean(options.dryRun);
  const pm2AppName = options.pm2AppName || process.env.SEND_WORKER_PM2_NAME || 'ig-dm-send';
  const sb = options.supabase || require('../database/supabase');
  const snap = await sb.getRecommendedSendWorkerInstanceCount();
  const target = snap.recommended;
  const current = getPm2SendWorkerCount(pm2AppName);

  const result = {
    ...snap,
    pm2AppName,
    currentPm2: current?.total ?? null,
    currentPm2Exact: current?.exact ?? null,
    currentPm2Dynamic: current?.dynamic ?? null,
    target,
    scaled: false,
    dryRun,
  };

  if (current === null) {
    result.error = 'pm2_jlist_failed';
    return result;
  }
  if (current.total === 0) {
    result.error = 'send_worker_not_in_pm2';
    result.hint = `Start once with: pm2 start ecosystem.config.cjs --only ${pm2AppName}`;
    return result;
  }
  if (current.total === target) {
    return result;
  }
  if (current.exact === 0 && current.dynamic > 0) {
    result.mode = 'per_client_workers';
    result.hint =
      'Detected dynamic per-client send workers (ig-dm-send-<clientId>). Direct pm2 scale is skipped in this mode.';
    return result;
  }
  if (dryRun) {
    result.wouldScale = true;
    return result;
  }
  execSync(`pm2 scale ${pm2AppName} ${target}`, { encoding: 'utf8', stdio: 'inherit' });
  result.scaled = true;
  return result;
}

module.exports = {
  getPm2SendWorkerCount,
  runScaleSendWorkers,
};
