require('dotenv').config({ quiet: true });
const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { exec, execFile, spawn } = require('child_process');
const multer = require('multer');
const { rateLimit, ipKeyGenerator } = require('express-rate-limit');
const { getDailyStats, getRecentSent, getControl, setControl, alreadySent, clearFailedAttempts } = require('./database/db');
const {
  isSupabaseConfigured,
  getClientId,
  getControl: getControlSupabase,
  setControl: setControlSupabase,
  getClientStatusMessage: getClientStatusMessageSupabase,
  getDailyStats: getDailyStatsSupabase,
  getRecentSent: getRecentSentSupabase,
  clearFailedAttempts: clearFailedAttemptsSupabase,
  getLeads: getLeadsSupabase,
  getLeadsTotalAndRemaining,
  setClientStatusMessage,
  saveSession,
  isAdminUser,
  countActiveVpsInstagramSessions,
  countActiveGraphInstagramAccounts,
  updateSettingsInstagramUsername,
  getLatestScrapeJob,
  getScrapeQuotaStatus,
  createScrapeJob,
  cancelScrapeJob,
  savePlatformScraperSession,
  addCampaignLeadsFromGroups,
  syncSendJobsForClient,
  getNoWorkHint,
  getClientNoWorkResumeAt,
  canClientActivelySendNow,
  getCampaignsMissingSendDelays,
  getSessionsForCampaign,
  reactivateCampaignsWithPendingLeads,
  tryVpsIdempotencyOnce,
  getOrResolveColdDmProxyUrl,
  getMostRecentInstagramSessionForClient,
  updateInstagramSessionProxy,
  releaseAllInstagramSessionLeases,
  releaseAllCampaignSendLeases,
  getClientIdsWithPauseZero,
  getLatestSuccessfulColdDmSentAt,
  getClientsOnCurrentVps,
} = require('./database/supabase');
const {
  loadLeadsFromCSV,
  connectInstagram,
  completeInstagram2FA,
  completeInstagramEmailVerification,
  sendFollowUp,
  scheduleDebugFollowUpBrowser,
  previewDmLeadNamesFromSession,
} = require('./bot');
const { MESSAGES } = require('./config/messages');
const logger = require('./utils/logger');
const { mergeInstagramSessionData } = require('./utils/instagram-web-storage');

const app = express();
const PORT = process.env.DASHBOARD_PORT || 3000;
const projectRoot = path.join(__dirname);
const envPath = path.join(projectRoot, '.env');
const leadsPath = path.join(projectRoot, process.env.LEADS_CSV || 'leads.csv');
const voiceNotesDir = path.join(projectRoot, 'voice-notes');
const followUpScreenshotsDir = path.join(projectRoot, 'follow-up-screenshots');
const loginDebugScreenshotsDir = path.join(projectRoot, 'logs', 'login-debug');
const PROCESS_BOOT_ID = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex');
const dashboardAuditPath = path.join(projectRoot, 'logs', 'dashboard-restart-audit.log');
const dashboardStartedAt = Date.now();
const dashboardDebugState = {
  recentRequests: [],
  lastAdminUpdate: null,
  lastControlStart: null,
  lastPm2Command: null,
  lastWorkerEnsure: null,
  signalCounts: {},
};

function safeJson(value) {
  return JSON.stringify(value, (_key, val) => {
    if (val instanceof Error) {
      return { name: val.name, message: val.message, stack: val.stack };
    }
    if (typeof val === 'bigint') return val.toString();
    if (typeof val === 'function') return `[Function ${val.name || 'anonymous'}]`;
    return val;
  });
}

function redactForAudit(value) {
  const secretKeyPattern = /(SECRET|PASSWORD|TOKEN|API_KEY|SERVICE_ROLE|SUPABASE_DB_URL|DATABASE_URL|DB_URL|JWT|BEARER|COOKIE)/i;
  const redactString = (s) => {
    if (/eyJ[a-zA-Z0-9_-]+\./.test(s)) return '[REDACTED_JWT]';
    if (s.length > 120 && /[A-Za-z0-9+/=_-]{80,}/.test(s)) return '[REDACTED_LONG_SECRET]';
    return s;
  };
  const walk = (val, key = '') => {
    if (val == null) return val;
    if (secretKeyPattern.test(key)) return '[REDACTED]';
    if (typeof val === 'string') return redactString(val);
    if (Array.isArray(val)) return val.map((item) => walk(item));
    if (typeof val === 'object') {
      const out = {};
      for (const [childKey, childValue] of Object.entries(val)) {
        out[childKey] = walk(childValue, childKey);
      }
      return out;
    }
    return val;
  };
  return walk(value);
}

function appendDashboardAudit(event, details = {}) {
  const entry = {
    at: new Date().toISOString(),
    event,
    bootId: PROCESS_BOOT_ID,
    pid: process.pid,
    ppid: process.ppid,
    pmId: process.env.pm_id || null,
    uptimeMs: Math.round(process.uptime() * 1000),
    ...redactForAudit(details),
  };
  const line = `${safeJson(entry)}\n`;
  try {
    fs.mkdirSync(path.dirname(dashboardAuditPath), { recursive: true });
    fs.appendFileSync(dashboardAuditPath, line);
  } catch (_) {}
  try {
    if (process.env.DASHBOARD_AUDIT_CONSOLE === '1' || process.env.DASHBOARD_AUDIT_CONSOLE === 'true') {
      console.log(`[dashboard:audit] ${event} ${safeJson(redactForAudit(details))}`);
    }
  } catch (_) {}
}

function dashboardDebugSnapshot(extra = {}) {
  return {
    uptimeMs: Date.now() - dashboardStartedAt,
    memory: process.memoryUsage(),
    activeHandles: typeof process._getActiveHandles === 'function' ? process._getActiveHandles().length : null,
    activeRequests: typeof process._getActiveRequests === 'function' ? process._getActiveRequests().length : null,
    state: dashboardDebugState,
    ...extra,
  };
}

process.on('exit', (code) => {
  appendDashboardAudit('process_exit', dashboardDebugSnapshot({ code }));
});
process.on('SIGINT', () => {
  dashboardDebugState.signalCounts.SIGINT = (dashboardDebugState.signalCounts.SIGINT || 0) + 1;
  appendDashboardAudit('signal_SIGINT', dashboardDebugSnapshot());
});
process.on('SIGTERM', () => {
  dashboardDebugState.signalCounts.SIGTERM = (dashboardDebugState.signalCounts.SIGTERM || 0) + 1;
  appendDashboardAudit('signal_SIGTERM', dashboardDebugSnapshot());
});
process.on('uncaughtException', (err) => {
  appendDashboardAudit('uncaught_exception', dashboardDebugSnapshot({ err }));
  process.exitCode = 1;
});
process.on('unhandledRejection', (reason) => {
  appendDashboardAudit('unhandled_rejection', dashboardDebugSnapshot({ reason }));
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use((req, res, next) => {
  const startedAt = Date.now();
  const entry = {
    id: crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(8).toString('hex'),
    method: req.method,
    path: req.path,
    action: req.body?.action || null,
    clientId: req.body?.clientId || req.query?.clientId || null,
    ip: req.ip,
    userAgent: req.get('user-agent') || null,
    startedAt: new Date(startedAt).toISOString(),
  };
  dashboardDebugState.recentRequests.push(entry);
  dashboardDebugState.recentRequests = dashboardDebugState.recentRequests.slice(-30);
  if (
    req.path === '/api/admin/update' ||
    req.path === '/api/control/start' ||
    req.path === '/api/admin/assign-client' ||
    process.env.DASHBOARD_RESTART_DEBUG_VERBOSE === '1'
  ) {
    appendDashboardAudit('request_start', entry);
  }
  res.on('finish', () => {
    const done = { ...entry, statusCode: res.statusCode, durationMs: Date.now() - startedAt };
    const idx = dashboardDebugState.recentRequests.findIndex((r) => r.id === entry.id);
    if (idx >= 0) dashboardDebugState.recentRequests[idx] = done;
    if (
      req.path === '/api/admin/update' ||
      req.path === '/api/control/start' ||
      req.path === '/api/admin/assign-client' ||
      process.env.DASHBOARD_RESTART_DEBUG_VERBOSE === '1'
    ) {
      appendDashboardAudit('request_finish', done);
    }
  });
  next();
});

const API_KEY = (process.env.COLD_DM_API_KEY || '').trim();
const API_KEY_CLIENT_MAP = (process.env.COLD_DM_API_KEYS || '')
  .split(',')
  .map((x) => x.trim())
  .filter(Boolean)
  .reduce((acc, pair) => {
    const idx = pair.indexOf(':');
    if (idx <= 0 || idx === pair.length - 1) return acc;
    const key = pair.slice(0, idx).trim();
    const clientId = pair.slice(idx + 1).trim();
    if (key && clientId) acc[key] = clientId;
    return acc;
  }, {});

const COOKIE_NAME = 'cold_dm_api';
const cookieSecure =
  process.env.COLD_DM_COOKIE_SECURE === '1' || process.env.COLD_DM_COOKIE_SECURE === 'true';
const PUPPETEER_APT_PACKAGES = [
  'libgbm1',
  'libasound2',
  'libnss3',
  'libatk1.0-0',
  'libatk-bridge2.0-0',
  'libcups2',
  'libdrm2',
  'libxkbcommon0',
  'libxcomposite1',
  'libxdamage1',
  'libxfixes3',
  'libxrandr2',
  'libpango-1.0-0',
  'libcairo2',
  'libgtk-3-0',
  'libdbus-1-3',
  'libnspr4',
  'libx11-xcb1',
  'libxshmfence1',
  'fonts-liberation',
  'xdg-utils',
];

function getPuppeteerDepsInstallCommand() {
  return `apt-get update && apt-get install -y ${PUPPETEER_APT_PACKAGES.join(' ')}`;
}

function shQuote(s) {
  // Minimal single-quote escaping for embedding values into a `bash -lc` string.
  // Example: abc'd -> 'abc'\''d'
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
}

function envCheckSnapshot(req) {
  const auth = req.headers.authorization;
  const bearer = typeof auth === 'string' ? auth.replace(/^Bearer\s+/i, '').trim() : '';
  const xApiKey = (req.headers['x-api-key'] || '').toString().trim();
  const cookieKey = req.cookies && req.cookies[COOKIE_NAME] ? String(req.cookies[COOKIE_NAME]).trim() : '';
  const presentedKey = getPresentedApiKey(req);

  const supabaseUrl = (process.env.SUPABASE_URL || '').trim();
  const serviceRoleLen = (process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim().length;
  const edgeSecretLen = (process.env.EDGE_INTERNAL_FUNCTION_SECRET || '').trim().length;
  const coldDmApiKeyLen = (process.env.COLD_DM_API_KEY || '').trim().length;

  return {
    presented: {
      hasKey: Boolean(presentedKey),
      keyLen: presentedKey ? String(presentedKey).length : 0,
      sources: {
        bearer: bearer.length > 0,
        xApiKey: xApiKey.length > 0,
        cookie: cookieKey.length > 0,
      },
    },
    server: {
      apiKeyConfigured: API_KEY.length > 0,
      apiKeyLen: API_KEY.length,
      apiKeysMapCount: Object.keys(API_KEY_CLIENT_MAP).length,
      pinnedClientId: Boolean((process.env.COLD_DM_CLIENT_ID || '').trim()),
    },
    env: {
      supabaseUrlPresent: supabaseUrl.length > 0,
      supabaseUrl: supabaseUrl ? supabaseUrl.replace(/\/$/, '') : '',
      supabaseServiceRoleKeyLen: serviceRoleLen,
      edgeInternalSecretLen: edgeSecretLen,
      coldDmApiKeyLen,
    },
    process: {
      cwd: process.cwd(),
      node: process.version,
      pm2: process.env.pm_id != null,
    },
  };
}

// Dashboard HTML: set HttpOnly cookie from server env so the browser never types or sees the API key in JS/repo.
app.get('/', (req, res) => {
  if (API_KEY) {
    res.cookie(COOKIE_NAME, API_KEY, {
      httpOnly: true,
      sameSite: 'lax',
      secure: cookieSecure,
      path: '/',
      maxAge: 60 * 24 * 60 * 60 * 1000, // 60 days
    });
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/index.html', (req, res) => {
  res.redirect(302, '/');
});

app.use(express.static(path.join(__dirname, 'public'), { index: false }));
if (!fs.existsSync(voiceNotesDir)) fs.mkdirSync(voiceNotesDir, { recursive: true });
app.use('/voice-notes', express.static(voiceNotesDir));

const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: Math.max(10, parseInt(process.env.API_RATE_LIMIT_PER_MIN || '120', 10) || 120),
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api', apiLimiter);

// Debug helper: confirms what env/headers the running VPS process sees at runtime.
// Enable with COLD_DM_DEBUG_AUTH=1 and hit GET /api/internal/env-check.
app.get('/api/internal/env-check', (req, res) => {
  if ((process.env.COLD_DM_DEBUG_AUTH || '').trim() !== '1') {
    return res.status(404).json({ ok: false });
  }
  return res.json({ ok: true, ...envCheckSnapshot(req) });
});

const followUpLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: Math.max(10, parseInt(process.env.FOLLOW_UP_RATE_LIMIT_PER_MIN || '60', 10) || 60),
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req, res) => {
    const id = (req.body?.clientId || '').toString().trim();
    return id ? `fu:${id}` : `fu:ip:${ipKeyGenerator(req, res)}`;
  },
});

const connectLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: Math.max(5, parseInt(process.env.IG_CONNECT_RATE_LIMIT_PER_15MIN || '20', 10) || 20),
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req, res) => {
    const id = (req.body?.clientId || '').toString().trim();
    return id ? `ig:${id}` : `ig:ip:${ipKeyGenerator(req, res)}`;
  },
});

function getPresentedApiKey(req) {
  const auth = req.headers.authorization;
  const bearer = typeof auth === 'string' ? auth.replace(/^Bearer\s+/i, '').trim() : '';
  const xApiKey = (req.headers['x-api-key'] || '').toString().trim();
  const cookieKey = req.cookies && req.cookies[COOKIE_NAME] ? String(req.cookies[COOKIE_NAME]).trim() : '';
  return bearer || xApiKey || cookieKey;
}

function resolveRequestedClientId(req) {
  const bodyClientId = req.body?.clientId;
  const queryClientId = req.query?.clientId;
  return (bodyClientId || queryClientId || '').toString().trim();
}

function requireScopedClientId(req, res) {
  const clientId = resolveRequestedClientId(req);
  if (!clientId) {
    res.status(400).json({ ok: false, error: 'clientId is required' });
    return null;
  }
  if (req.authClientId && req.authClientId !== clientId) {
    res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
    return null;
  }
  return clientId;
}

function getHealthPayload() {
  const assignedClientId = getClientId();
  return {
    ok: true,
    bootId: PROCESS_BOOT_ID,
    poolMode: process.env.COLD_DM_POOL_MODE === '1' || process.env.COLD_DM_POOL_MODE === 'true',
    assignedClientIdPresent: Boolean(assignedClientId),
  };
}

async function fetchTextWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) return '';
    return String(await res.text().catch(() => '')).trim();
  } catch {
    return '';
  } finally {
    clearTimeout(timeout);
  }
}

async function notifyPoolWorkerReady(reason = 'startup') {
  const isPoolMode = process.env.COLD_DM_POOL_MODE === '1' || process.env.COLD_DM_POOL_MODE === 'true';
  if (!isPoolMode) return true;
  if (getClientId()) return true;

  const supabaseUrl = (process.env.SUPABASE_URL || '').replace(/\/$/, '');
  const bearer =
    (process.env.EDGE_INTERNAL_FUNCTION_SECRET || '').trim() ||
    (process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  if (!supabaseUrl || !bearer) {
    console.warn(
      `[pool-worker-ready] skip reason=${reason} missing env (SUPABASE_URL=${supabaseUrl ? 'set' : 'missing'} bearer=${
        bearer ? 'set' : 'missing'
      })`
    );
    return false;
  }

  const [dropletIdRaw, publicIp] = await Promise.all([
    fetchTextWithTimeout('http://169.254.169.254/metadata/v1/id', 3000),
    fetchTextWithTimeout('http://169.254.169.254/metadata/v1/interfaces/public/0/ipv4/address', 3000),
  ]);
  const dropletId = Number(dropletIdRaw);
  if (!Number.isFinite(dropletId) || dropletId <= 0 || !publicIp) {
    console.warn(
      `[pool-worker-ready] metadata unavailable reason=${reason} dropletId=${dropletIdRaw || 'missing'} publicIp=${
        publicIp || 'missing'
      }`
    );
    return false;
  }

  try {
    const res = await fetch(`${supabaseUrl}/functions/v1/cold-dm-vps-proxy`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${bearer}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        action: 'poolWorkerReady',
        dropletId,
        publicIp,
      }),
    });
    if (!res.ok) {
      const bodyPreview = String(await res.text().catch(() => '')).replace(/\s+/g, ' ').trim().slice(0, 300);
      console.warn(
        `[pool-worker-ready] callback failed reason=${reason} status=${res.status} body=${bodyPreview || 'n/a'}`
      );
      return false;
    }
    console.log(`[pool-worker-ready] callback ok reason=${reason} dropletId=${dropletId} publicIp=${publicIp}`);
    return true;
  } catch (e) {
    console.warn(`[pool-worker-ready] callback exception reason=${reason} error=${e?.message || e}`);
    return false;
  }
}

function schedulePoolWorkerReadyRegistration() {
  const isPoolMode = process.env.COLD_DM_POOL_MODE === '1' || process.env.COLD_DM_POOL_MODE === 'true';
  if (!isPoolMode) return;

  const runAfter = (reason, delayMs) => {
    setTimeout(async () => {
      if (getClientId()) return;
      const ok = await notifyPoolWorkerReady(reason);
      if (!ok && !getClientId()) runAfter('retry', 30000);
    }, delayMs);
  };

  runAfter('startup', 5000);
}

app.use('/api', (req, res, next) => {
  if (req.path === '/health') return next();
  if (req.path === '/internal/env-check') return next();
  if (req.path === '/internal/scale-send-workers') return next();
  const key = getPresentedApiKey(req);
  if (!key) {
    return res.status(401).json({ error: 'Unauthorized: missing API key' });
  }
  if (!API_KEY && Object.keys(API_KEY_CLIENT_MAP).length === 0) {
    return res.status(503).json({ error: 'API key auth is required but not configured on the server' });
  }
  if (Object.prototype.hasOwnProperty.call(API_KEY_CLIENT_MAP, key)) {
    req.authClientId = API_KEY_CLIENT_MAP[key];
    return next();
  }
  if (API_KEY && key === API_KEY) {
    return next();
  }
  return res.status(401).json({ error: 'Unauthorized' });
});

// --- API: admin maintenance (fixed actions only; no arbitrary command execution) ---
app.post('/api/admin/update', (req, res) => {
  // Safe "pull + restart" endpoint for per-client droplets so Edge can deploy updates without SSH.
  // Protected by the same Bearer COLD_DM_API_KEY middleware above.
  const branch = String(process.env.COLD_DM_WORKER_BRANCH || process.env.GIT_BRANCH || 'main')
    .trim() || 'main';
  const updateId = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(12).toString('hex');
  const updateLogPath = `/tmp/cold-dm-update-${updateId}.log`;
  dashboardDebugState.lastAdminUpdate = {
    updateId,
    branch,
    acceptedAt: new Date().toISOString(),
    ip: req.ip,
    userAgent: req.get('user-agent') || null,
    bodyKeys: Object.keys(req.body || {}),
  };

  logger.log(`[admin:update] accepted updateId=${updateId} branch=${branch}`);
  appendDashboardAudit('admin_update_accepted', dashboardDebugState.lastAdminUpdate);
  res.json({
    ok: true,
    accepted: true,
    updateId,
    branch,
    restarting: true,
    bootIdBeforeRestart: PROCESS_BOOT_ID,
    updateLogPath,
  });

  setTimeout(() => {
    // IMPORTANT: avoid `pm2 restart all` from inside the dashboard process.
    // When PM2 restarts `ig-dm-dashboard`, it can kill this background runner (process-tree kill),
    // which leads to partial updates where `ig-dm-send` never restarts.
    //
    // Fix: restart workers first, `pm2 save`, then restart the dashboard last.
    const script = [
      'set -euo pipefail',
      `cd ${shQuote(projectRoot)}`,
      `echo "[admin:update] start updateId=${updateId} branch=${branch} at=$(date -Is)"`,
      `git pull origin ${shQuote(branch)}`,
      'npm install',
      `node - <<'NODE'
const { execFileSync } = require('child_process');
const list = JSON.parse(execFileSync('pm2', ['jlist'], { encoding: 'utf8' }) || '[]');
for (const proc of list) {
  const name = proc && proc.name;
  if (typeof name === 'string' && /^ig-dm-(send|scrape)-/.test(name)) {
    execFileSync('pm2', ['restart', name, '--update-env'], { stdio: 'inherit' });
  }
}
NODE`,
      // Best-effort: load ecosystem if missing (no-op if already started).
      '(pm2 start ecosystem.config.cjs --update-env >/dev/null 2>&1 || true)',
      // Save before restarting the dashboard (the command below may kill this runner).
      '(pm2 save || true)',
      '(pm2 restart ig-dm-dashboard --update-env || pm2 start ecosystem.config.cjs --only ig-dm-dashboard --update-env)',
      'echo "[admin:update] done at=$(date -Is)"',
    ].join('\n');
    const runner = `(${script}) > ${shQuote(updateLogPath)} 2>&1`;
    appendDashboardAudit('admin_update_runner_spawn', {
      updateId,
      branch,
      updateLogPath,
      willRestartDashboard: true,
    });
    try {
      const child = spawn('bash', ['-lc', runner], {
        cwd: projectRoot,
        detached: true,
        stdio: 'ignore',
      });
      child.unref();
    } catch (err) {
      logger.error(`[admin:update] failed to launch updateId=${updateId} error=${String(err?.message || err)}`);
    }
  }, 25);
});

app.post('/api/admin/assign-client', async (req, res) => {
  const clientId = (req.body?.clientId || '').toString().trim();
  if (!clientId) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  appendDashboardAudit('admin_assign_client_accepted', {
    clientId,
    ip: req.ip,
    userAgent: req.get('user-agent') || null,
  });
  const syncTimeoutMs = Math.max(1000, parseInt(process.env.ASSIGN_CLIENT_SYNC_TIMEOUT_MS || '15000', 10) || 15000);
  let timedOut = false;
  const ensurePromise = ensureClientWorkerStack(clientId);
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => {
      timedOut = true;
      resolve({ ok: false, timeout: true });
    }, syncTimeoutMs);
  });
  const result = await Promise.race([ensurePromise, timeoutPromise]);
  if (timedOut) {
    ensurePromise
      .then((lateResult) => {
        if (!lateResult.ok) {
          console.error(`[API] assign-client failed to spawn workers clientId=${clientId.slice(0, 8)} error=${lateResult.error || 'unknown'}`);
        }
      })
      .catch((e) => {
        console.error(`[API] assign-client worker spawn exception clientId=${clientId.slice(0, 8)}`, e);
      });
    return res.status(202).json({ ok: true, clientId, accepted: true, workersReady: false, timeoutMs: syncTimeoutMs });
  }
  if (!result.ok) {
    console.error(`[API] assign-client failed to spawn workers clientId=${clientId.slice(0, 8)} error=${result.error || 'unknown'}`);
    return res.status(500).json({ ok: false, clientId, error: result.error || 'worker_spawn_failed' });
  }
  res.json({
    ok: true,
    clientId,
    accepted: true,
    workersReady: true,
    sendName: result.sendName,
    scrapeName: result.scrapeName,
  });
});

app.get('/api/admin/clients', (_req, res) => {
  listActiveClientIdsFromPm2()
    .then((clientIds) => res.json({ ok: true, clientIds }))
    .catch((e) => res.status(500).json({ ok: false, error: e?.message || String(e) }));
});

const { registerAdminLabRoutes } = require('./admin_lab/http');
const { runScaleSendWorkers } = require('./lib/scaleSendWorkers');
registerAdminLabRoutes(app);

const upload = multer({ dest: projectRoot, limits: { fileSize: 1024 * 1024 } });
const uploadVoice = multer({ dest: voiceNotesDir, limits: { fileSize: 25 * 1024 * 1024 } });

const BOT_PM2_NAME = 'ig-dm-send';
const SCRAPE_PM2_NAME = 'ig-dm-scrape';
const SEND_PM2_PREFIX = 'ig-dm-send-';
const SCRAPE_PM2_PREFIX = 'ig-dm-scrape-';
const SEND_WORKER_ENTRY = process.env.SEND_WORKER_ENTRY || 'workers/send-worker.js';
const SCRAPE_WORKER_ENTRY = process.env.SCRAPE_WORKER_ENTRY || 'workers/scrape-worker.js';
const PER_CLIENT_PM2_WORKERS_ENABLED =
  process.env.COLD_DM_PER_CLIENT_PM2_WORKERS !== '0' &&
  process.env.COLD_DM_PER_CLIENT_PM2_WORKERS !== 'false';
const LEGACY_SHARED_SEND_WORKER_ENABLED =
  process.env.COLD_DM_ALLOW_LEGACY_SHARED_SEND_WORKER === '1' ||
  process.env.COLD_DM_ALLOW_LEGACY_SHARED_SEND_WORKER === 'true';
const AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START =
  process.env.COLD_DM_AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START === '1' ||
  process.env.COLD_DM_AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START === 'true';
const SCRAPER_SESSION_LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);
const SEND_SCRAPE_COOLDOWN_MS = Math.max(
  60 * 1000,
  parseInt(process.env.SEND_SCRAPE_COOLDOWN_MS || String(5 * 60 * 1000), 10) || 5 * 60 * 1000
);
const PROCESS_SCHEDULED_RESPONSES_FALLBACK_ENABLED =
  process.env.PROCESS_SCHEDULED_RESPONSES_FALLBACK === '1' ||
  process.env.PROCESS_SCHEDULED_RESPONSES_FALLBACK === 'true';
const PROCESS_SCHEDULED_RESPONSES_FALLBACK_INTERVAL_MS = Math.max(
  30 * 1000,
  parseInt(process.env.PROCESS_SCHEDULED_RESPONSES_FALLBACK_INTERVAL_MS || '60000', 10) || 60000
);
const PROCESS_SCHEDULED_RESPONSES_FALLBACK_TIMEOUT_MS = Math.max(
  30 * 1000,
  parseInt(process.env.PROCESS_SCHEDULED_RESPONSES_FALLBACK_TIMEOUT_MS || '300000', 10) || 300000
);
const PROCESS_SCHEDULED_RESPONSES_FALLBACK_WARN_EVERY_MS = Math.max(
  60 * 1000,
  parseInt(process.env.PROCESS_SCHEDULED_RESPONSES_FALLBACK_WARN_EVERY_MS || String(15 * 60 * 1000), 10) ||
    15 * 60 * 1000
);

let processScheduledResponsesFallbackInFlight = false;
let processScheduledResponsesFallbackLastWarn = null;

function formatDurationShort(ms) {
  const safeMs = Math.max(0, Number(ms) || 0);
  const totalSec = Math.max(1, Math.ceil(safeMs / 1000));
  if (totalSec < 60) return `${totalSec} second${totalSec === 1 ? '' : 's'}`;
  const min = Math.max(1, Math.round(totalSec / 60));
  if (min < 60) return `${min} minute${min === 1 ? '' : 's'}`;
  const hours = Math.floor(min / 60);
  const mins = min % 60;
  if (mins === 0) return `${hours} hour${hours === 1 ? '' : 's'}`;
  return `${hours} hour${hours === 1 ? '' : 's'} ${mins} minute${mins === 1 ? '' : 's'}`;
}

function normalizeProxyUrl(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  // Decodo colon format: host:port:user:pass
  if (!/^[a-z]+:\/\//i.test(s)) {
    const parts = s.split(':');
    if (parts.length >= 4 && parts[0] && parts[1] && parts[2]) {
      const host = parts[0];
      const port = parts[1];
      const user = parts[2];
      const pass = parts.slice(3).join(':');
      return normalizeProxyUrl(`http://${user}:${pass}@${host}:${port}`);
    }
    return s;
  }
  try {
    const u = new URL(s);
    if (u.username || u.password) {
      // Percent-encode userinfo; keep unreserved per RFC3986.
      const enc = (v) =>
        encodeURIComponent(v)
          .replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase())
          .replace(/%7E/g, '~');
      const user = enc(u.username || '');
      const pass = enc(u.password || '');
      const host = u.host;
      const auth = user || pass ? `${user}:${pass}@` : '';
      return `${u.protocol}//${auth}${host}${u.pathname || ''}${u.search || ''}${u.hash || ''}`;
    }
    return s;
  } catch {
    return s;
  }
}

function getProcessScheduledResponsesBearer() {
  const edgeInternal = (process.env.EDGE_INTERNAL_FUNCTION_SECRET || '').trim();
  if (edgeInternal) return edgeInternal;
  return (process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
}

function warnProcessScheduledResponsesFallbackOnce(signature, message) {
  const now = Date.now();
  const shouldWarn =
    !processScheduledResponsesFallbackLastWarn ||
    processScheduledResponsesFallbackLastWarn.signature !== signature ||
    now - processScheduledResponsesFallbackLastWarn.at >= PROCESS_SCHEDULED_RESPONSES_FALLBACK_WARN_EVERY_MS;
  if (!shouldWarn) return;
  processScheduledResponsesFallbackLastWarn = { signature, at: now };
  logger.warn(message);
}

async function triggerProcessScheduledResponsesFallback(reason = 'interval') {
  if (processScheduledResponsesFallbackInFlight) {
    logger.log(`[process-scheduled-responses:fallback] skip overlapping tick reason=${reason}`);
    return;
  }
  const supabaseUrl = (process.env.SUPABASE_URL || '').replace(/\/$/, '');
  const bearer = getProcessScheduledResponsesBearer();
  if (!supabaseUrl || !bearer) {
    logger.warn(
      `[process-scheduled-responses:fallback] disabled at runtime: missing env (SUPABASE_URL=${
        supabaseUrl ? 'set' : 'missing'
      } auth_secret_len=${String(bearer || '').length})`
    );
    return;
  }

  processScheduledResponsesFallbackInFlight = true;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), PROCESS_SCHEDULED_RESPONSES_FALLBACK_TIMEOUT_MS);
  try {
    const url = `${supabaseUrl}/functions/v1/process-scheduled-responses`;
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${bearer}`,
        'Content-Type': 'application/json',
        'X-Triggered-By': 'cold-dm-vps-fallback',
      },
      body: JSON.stringify({ timestamp: new Date().toISOString(), reason }),
      signal: controller.signal,
    });
    const text = await res.text().catch(() => '');
    const snippet = String(text || '').replace(/\s+/g, ' ').trim().slice(0, 400);
    if (!res.ok) {
      warnProcessScheduledResponsesFallbackOnce(
        `status:${res.status}:${snippet}`,
        `[process-scheduled-responses:fallback] tick failed status=${res.status} reason=${reason} body=${snippet || 'n/a'}`
      );
      return;
    }
  } catch (e) {
    if (e?.name === 'AbortError' || String(e?.message || e) === 'This operation was aborted') {
      logger.log(
        `[process-scheduled-responses:fallback] tick timed out after ${Math.ceil(
          PROCESS_SCHEDULED_RESPONSES_FALLBACK_TIMEOUT_MS / 1000
        )}s reason=${reason}`
      );
      return;
    }
    warnProcessScheduledResponsesFallbackOnce(
      `exception:${e?.message || e}`,
      `[process-scheduled-responses:fallback] tick exception reason=${reason} error=${e?.message || e}`
    );
  } finally {
    clearTimeout(timeout);
    processScheduledResponsesFallbackInFlight = false;
  }
}

/** Hands-free PM2 scaling: on by default when Supabase is configured. Set SCALE_SEND_WORKERS_AUTO=0 to disable (e.g. laptop without PM2). */
function shouldAutoScaleSendWorkers() {
  const v = (process.env.SCALE_SEND_WORKERS_AUTO || '').trim().toLowerCase();
  if (v === '0' || v === 'false' || v === 'off' || v === 'no') return false;
  if (v === '1' || v === 'true' || v === 'on' || v === 'yes') return true;
  return isSupabaseConfigured();
}

const SCALE_SEND_WORKERS_AUTO_INTERVAL_MS = Math.max(
  60 * 1000,
  (Math.max(1, parseInt(process.env.SCALE_SEND_WORKERS_AUTO_INTERVAL_MINUTES || '5', 10) || 5)) * 60 * 1000
);

let scaleSendWorkersInFlight = false;
let scaleSendWorkersDebounce = null;

function runAutoScaleSendWorkersTick(reason) {
  if (!shouldAutoScaleSendWorkers()) return;
  if (scaleSendWorkersInFlight) return;
  scaleSendWorkersInFlight = true;
  runScaleSendWorkers({ dryRun: false, pm2AppName: BOT_PM2_NAME })
    .then((r) => {
      if (r.scaled) {
        console.log(
          `[scale-send-workers:auto:${reason}] scaled to ${r.target} (pauseZero=${r.pauseZeroClients}, sessions=${r.instagramSessionsForActiveClients})`
        );
      }
    })
    .catch((e) => console.warn(`[scale-send-workers:auto:${reason}]`, e.message))
    .finally(() => {
      scaleSendWorkersInFlight = false;
    });
}

/** After Start or on a timer; debounced so many clients starting at once trigger one pm2 scale. */
function scheduleAutoScaleSendWorkers(reason) {
  if (!shouldAutoScaleSendWorkers()) return;
  clearTimeout(scaleSendWorkersDebounce);
  scaleSendWorkersDebounce = setTimeout(() => {
    scaleSendWorkersDebounce = null;
    runAutoScaleSendWorkersTick(reason);
  }, 8000);
}

// Internal: scale send cluster from Supabase (pause=0 clients + their IG session count). Set SCALE_SEND_WORKERS_SECRET; call with header x-scale-send-workers-secret.
app.post('/api/internal/scale-send-workers', async (req, res) => {
  const secret = (process.env.SCALE_SEND_WORKERS_SECRET || '').trim();
  if (!secret) {
    return res.status(503).json({ ok: false, error: 'SCALE_SEND_WORKERS_SECRET not set' });
  }
  const hdr = (req.headers['x-scale-send-workers-secret'] || '').toString().trim();
  if (hdr !== secret) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const dryRun =
    req.body?.dryRun === true ||
    String(req.query?.dryRun || '').toLowerCase() === '1' ||
    String(req.query?.dryRun || '').toLowerCase() === 'true';
  try {
    const result = await runScaleSendWorkers({ dryRun, pm2AppName: BOT_PM2_NAME });
    if (result.error) {
      const code = result.error === 'send_worker_not_in_pm2' ? 409 : 502;
      return res.status(code).json({ ok: false, ...result });
    }
    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error('[API] scale-send-workers', e);
    return res.status(500).json({ ok: false, error: e.message || 'scale failed' });
  }
});

const PM2_JLIST_TIMEOUT_MS = Math.max(
  800,
  parseInt(process.env.PM2_JLIST_TIMEOUT_MS || '1500', 10) || 1500
);

function getBotProcessRunning(cb) {
  exec(
    'pm2 jlist',
    { maxBuffer: 1024 * 1024, timeout: PM2_JLIST_TIMEOUT_MS },
    (err, stdout) => {
      if (err) {
        const timedOut =
          err.killed === true || err.signal === 'SIGKILL' || /timed out|TIMEOUT/i.test(String(err.message || ''));
        if (timedOut) {
          console.warn(
            `[API] pm2 jlist exceeded ${PM2_JLIST_TIMEOUT_MS}ms (PM2 daemon busy). Treating send worker as online so /api/status still returns.`
          );
          // Optimistic: avoids 503 spam and false "stopped" when ig-dm-send is actually running.
          return cb(true);
        }
        return cb(false);
      }
      try {
        const list = JSON.parse(stdout);
        const proc = list.find((p) => p.name === BOT_PM2_NAME);
        cb(proc && proc.pm2_env && proc.pm2_env.status === 'online');
      } catch (e) {
        cb(false);
      }
    }
  );
}

function execPm2(command) {
  const isNoisyStatusCommand = /^pm2\s+jlist\b/.test(command);
  const startedAt = Date.now();
  const commandId = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(8).toString('hex');
  const stack = new Error('pm2 command caller').stack;
  dashboardDebugState.lastPm2Command = {
    commandId,
    command,
    startedAt: new Date(startedAt).toISOString(),
    stack,
  };
  if (!isNoisyStatusCommand || process.env.DASHBOARD_RESTART_DEBUG_VERBOSE === '1') {
    appendDashboardAudit('pm2_command_start', dashboardDebugState.lastPm2Command);
  }
  return new Promise((resolve) => {
    exec(command, { cwd: projectRoot }, (err, stdout, stderr) => {
      const done = {
        commandId,
        command,
        ok: !err,
        durationMs: Date.now() - startedAt,
        err: err ? { message: err.message, code: err.code, signal: err.signal, killed: err.killed } : null,
        stdoutPreview: String(stdout || '').slice(0, 1000),
        stderrPreview: String(stderr || '').slice(0, 1000),
      };
      dashboardDebugState.lastPm2Command = {
        ...dashboardDebugState.lastPm2Command,
        ...done,
        finishedAt: new Date().toISOString(),
      };
      if (!isNoisyStatusCommand || err || process.env.DASHBOARD_RESTART_DEBUG_VERBOSE === '1') {
        appendDashboardAudit('pm2_command_finish', done);
      }
      resolve({
        ok: !err,
        err,
        stdout: (stdout || '').toString(),
        stderr: (stderr || '').toString(),
        out: (((stdout || '') + (stderr || '')).toString() || '').trim(),
      });
    });
  });
}

async function getPm2AppStatusByName(appName) {
  const res = await execPm2('pm2 jlist');
  if (!res.ok) return { exists: false, online: false, status: null, error: res.err || new Error(res.stderr || 'pm2 jlist failed') };
  try {
    const list = JSON.parse(res.stdout || '[]');
    const proc = Array.isArray(list) ? list.find((p) => p?.name === appName) : null;
    const status = proc?.pm2_env?.status || null;
    return {
      exists: !!proc,
      online: status === 'online',
      status,
      error: null,
    };
  } catch (e) {
    return { exists: false, online: false, status: null, error: e };
  }
}

function sanitizeClientIdForPm2(clientId) {
  return String(clientId || '').trim().replace(/[^a-zA-Z0-9_-]+/g, '-');
}

function getSendWorkerPm2Name(clientId) {
  return `${SEND_PM2_PREFIX}${sanitizeClientIdForPm2(clientId)}`;
}

function getScrapeWorkerPm2Name(clientId) {
  return `${SCRAPE_PM2_PREFIX}${sanitizeClientIdForPm2(clientId)}`;
}

async function listPm2Processes() {
  const res = await execPm2('pm2 jlist');
  if (!res.ok) throw res.err || new Error(res.stderr || 'pm2 jlist failed');
  const list = JSON.parse(res.stdout || '[]');
  return Array.isArray(list) ? list : [];
}

async function listActiveClientIdsFromPm2() {
  const list = await listPm2Processes();
  return [
    ...new Set(
      list
        .map((proc) => String(proc?.name || ''))
        .filter((name) => name.startsWith(SEND_PM2_PREFIX))
        .map((name) => name.slice(SEND_PM2_PREFIX.length))
        .filter(Boolean),
    ),
  ];
}

function execPm2File(args, env, auditCommand) {
  const startedAt = Date.now();
  const commandId = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(8).toString('hex');
  dashboardDebugState.lastPm2Command = {
    commandId,
    command: auditCommand,
    startedAt: new Date(startedAt).toISOString(),
  };
  appendDashboardAudit('pm2_command_start', dashboardDebugState.lastPm2Command);
  return new Promise((resolve) => {
    execFile('pm2', args, { cwd: projectRoot, env }, (err, stdout, stderr) => {
      const done = {
        commandId,
        command: auditCommand,
        ok: !err,
        durationMs: Date.now() - startedAt,
        err: err ? { message: err.message, code: err.code, signal: err.signal, killed: err.killed } : null,
        stdoutPreview: String(stdout || '').slice(0, 1000),
        stderrPreview: String(stderr || '').slice(0, 1000),
      };
      dashboardDebugState.lastPm2Command = {
        ...dashboardDebugState.lastPm2Command,
        ...done,
        finishedAt: new Date().toISOString(),
      };
      appendDashboardAudit('pm2_command_finish', done);
      resolve({
        ok: !err,
        err,
        stdout: (stdout || '').toString(),
        stderr: (stderr || '').toString(),
        out: (((stdout || '') + (stderr || '')).toString() || '').trim(),
      });
    });
  });
}

function cleanEnvForPm2Child(extra = {}) {
  const forbiddenExact = new Set([
    'NODE_APP_INSTANCE',
    'PM2_HOME',
    'PM2_JSON_PROCESSING',
    'PM2_PROGRAMMATIC',
    'PM2_USAGE',
    'OLDPWD',
    '_',
    'axm_actions',
    'axm_dynamic',
    'axm_monitor',
    'axm_options',
    'automation',
    'autostart',
    'autorestart',
    'created_at',
    'cwd',
    'env',
    'exec_interpreter',
    'exec_mode',
    'filter_env',
    'instance_var',
    'kill_retry_time',
    'log_date_format',
    'max_memory_restart',
    'max_restarts',
    'merge_logs',
    'min_uptime',
    'name',
    'namespace',
    'node_args',
    'pm_cwd',
    'pm_err_log_path',
    'pm_exec_path',
    'pm_id',
    'pm_out_log_path',
    'pm_pid_path',
    'pm_uptime',
    'pmx',
    'restart_time',
    'status',
    'treekill',
    'unstable_restarts',
    'updateEnv',
    'username',
    'version',
    'vizion',
    'watch',
    'windowsHide',
  ]);
  const forbiddenPrefixes = ['pm_', 'axm_'];
  const forbiddenAppNamePattern = /^ig-dm-(dashboard|send|scrape)(-|$)/;
  const env = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (forbiddenExact.has(key)) continue;
    if (forbiddenPrefixes.some((prefix) => key.startsWith(prefix))) continue;
    if (forbiddenAppNamePattern.test(key)) continue;
    env[key] = value;
  }
  return { ...env, ...extra };
}

async function startClientProcessIfMissing(processName, script, env, outFile, errorFile) {
  const status = await getPm2AppStatusByName(processName);
  if (!status.error && status.online) {
    appendDashboardAudit('pm2_child_start_skipped_existing_online', {
      processName,
      script,
      statusBeforeStart: status,
    });
    return { ok: true, action: 'noop_online' };
  }
  appendDashboardAudit('pm2_child_start_request', {
    processName,
    script,
    outFile,
    errorFile,
    statusBeforeStart: status,
    leakedPm2KeysPresent: Object.keys(env || {}).filter((key) =>
      key === 'name' ||
      key === 'pm_exec_path' ||
      key === 'pm_cwd' ||
      key === 'pm_id' ||
      key === 'NODE_APP_INSTANCE' ||
      key.startsWith('pm_') ||
      key.startsWith('axm_') ||
      /^ig-dm-(dashboard|send|scrape)(-|$)/.test(key)
    ),
  });
  const args = status.exists
    ? ['restart', processName, '--update-env']
    : [
        'start',
        script,
        '--name',
        processName,
        '--cwd',
        projectRoot,
        '--output',
        outFile,
        '--error',
        errorFile,
        '--log-date-format',
        'YYYY-MM-DD HH:mm:ss Z',
        '--max-restarts',
        '20',
        '--update-env',
      ];
  const auditCommand = status.exists
    ? `pm2 restart ${processName} --update-env`
    : `pm2 start ${script} --name ${processName} --cwd ${projectRoot} --update-env`;
  const result = await execPm2File(args, env, auditCommand);
  if (!result.ok) {
    throw result.err || new Error(result.stderr || result.stdout || `pm2 failed for ${processName}`);
  }
  return { ok: true, action: status.exists ? 'restart_existing' : 'create_missing' };
}

async function ensureClientWorkerStack(clientId) {
  const normalizedClientId = String(clientId || '').trim();
  if (!normalizedClientId) return { ok: false, error: 'Missing clientId' };
  dashboardDebugState.lastWorkerEnsure = {
    kind: 'client_stack',
    clientId: normalizedClientId,
    startedAt: new Date().toISOString(),
  };
  appendDashboardAudit('ensure_client_worker_stack_start', dashboardDebugState.lastWorkerEnsure);
  if (!String(process.env.COLD_DM_VPS_IP || '').trim()) {
    await detectLocalPublicIp().catch(() => '');
  }
  const sendName = getSendWorkerPm2Name(normalizedClientId);
  const scrapeName = getScrapeWorkerPm2Name(normalizedClientId);
  const mergedEnv = cleanEnvForPm2Child({ COLD_DM_CLIENT_ID: normalizedClientId });
  const sendOut = path.join(projectRoot, 'logs', `send-${normalizedClientId}.out.log`);
  const sendErr = path.join(projectRoot, 'logs', `send-${normalizedClientId}.err.log`);
  const scrapeOut = path.join(projectRoot, 'logs', `scrape-${normalizedClientId}.out.log`);
  const scrapeErr = path.join(projectRoot, 'logs', `scrape-${normalizedClientId}.err.log`);

  try {
    await startClientProcessIfMissing(sendName, SEND_WORKER_ENTRY, mergedEnv, sendOut, sendErr);
    await startClientProcessIfMissing(scrapeName, SCRAPE_WORKER_ENTRY, mergedEnv, scrapeOut, scrapeErr);
    dashboardDebugState.lastWorkerEnsure = {
      ...dashboardDebugState.lastWorkerEnsure,
      sendName,
      scrapeName,
      finishedAt: new Date().toISOString(),
      ok: true,
    };
    appendDashboardAudit('ensure_client_worker_stack_finish', dashboardDebugState.lastWorkerEnsure);
    return { ok: true, sendName, scrapeName };
  } catch (e) {
    dashboardDebugState.lastWorkerEnsure = {
      ...dashboardDebugState.lastWorkerEnsure,
      finishedAt: new Date().toISOString(),
      ok: false,
      error: e?.message || String(e),
    };
    appendDashboardAudit('ensure_client_worker_stack_error', dashboardDebugState.lastWorkerEnsure);
    return { ok: false, error: e?.message || String(e) };
  }
}

async function detectLocalPublicIp() {
  if ((process.env.COLD_DM_VPS_IP || '').trim()) return process.env.COLD_DM_VPS_IP.trim();
  try {
    const ip = await fetchTextWithTimeout('http://169.254.169.254/metadata/v1/interfaces/public/0/ipv4/address', 2500);
    if (ip) process.env.COLD_DM_VPS_IP = ip;
    return ip;
  } catch {
    return '';
  }
}

async function ensureAssignedClientWorkerStacksOnStartup() {
  await detectLocalPublicIp().catch(() => '');
  const clientIds = await getClientsOnCurrentVps().catch(() => []);
  for (const clientId of clientIds) {
    const result = await ensureClientWorkerStack(clientId);
    if (!result.ok) {
      console.error(`[pm2:auto-ensure] client stack failed on startup clientId=${String(clientId).slice(0, 8)} error=${result.error || 'unknown'}`);
    }
  }
}

/**
 * Matches ecosystem.config.cjs `ig-dm-send` instance count. Dashboard `pm2 start` must use `-i N`
 * (cluster mode) so each process gets NODE_APP_INSTANCE and pins to distinct campaign queues.
 */
function sendWorkerPm2ClusterInstances() {
  if ((getClientId() || '').trim()) {
    return 1;
  }
  return Math.max(
    1,
    parseInt(
      process.env.SEND_WORKER_INSTANCES ||
        String(
          Math.max(
            1,
            parseInt(process.env.COLD_DM_MAX_CONCURRENT_SENDERS || process.env.COLD_DM_ACTIVE_SESSION_COUNT || '1', 10) || 1,
          ),
        ),
      10,
    ) || 1,
  );
}

/**
 * Ensure send worker is running without bouncing an already-online process.
 * - online: no-op
 * - exists but stopped: start by name
 * - missing: start by script+name (cluster `-i N`, same as ecosystem.config.cjs)
 */
async function ensureSendWorkerProcess() {
  if (PER_CLIENT_PM2_WORKERS_ENABLED && !LEGACY_SHARED_SEND_WORKER_ENABLED) {
    return {
      ok: false,
      action: 'legacy_shared_send_worker_disabled',
      out:
        `Refusing to start shared ${BOT_PM2_NAME}; per-client PM2 workers are enabled. ` +
        `Use ensureClientWorkerStack(clientId), or set COLD_DM_ALLOW_LEGACY_SHARED_SEND_WORKER=1 to opt in.`,
    };
  }
  const status = await getPm2AppStatusByName(BOT_PM2_NAME);
  if (!status.error && status.online) {
    return { ok: true, action: 'noop_online', out: `already online (${BOT_PM2_NAME})` };
  }
  if (!status.error && status.exists) {
    const startByName = await execPm2(`pm2 start ${BOT_PM2_NAME} --update-env`);
    if (startByName.ok || /online|already\s+running|successfully/i.test(startByName.out)) {
      return { ok: true, action: 'start_existing', out: startByName.out };
    }
    const restartByName = await execPm2(`pm2 restart ${BOT_PM2_NAME} --update-env`);
    if (restartByName.ok || /online|already\s+running|successfully/i.test(restartByName.out)) {
      return { ok: true, action: 'restart_existing', out: restartByName.out };
    }
    return { ok: false, action: 'start_existing_failed', out: `${startByName.out}\n${restartByName.out}`.trim(), err: restartByName.err || startByName.err };
  }
  const instances = sendWorkerPm2ClusterInstances();
  const create = await execPm2(
    `pm2 start ${SEND_WORKER_ENTRY} --name ${BOT_PM2_NAME} --no-autorestart -i ${instances}`,
  );
  if (create.ok || /online|already\s+running|successfully/i.test(create.out)) {
    return { ok: true, action: 'create_missing', out: create.out };
  }
  return { ok: false, action: 'create_missing_failed', out: create.out, err: create.err };
}

async function ensureScrapeWorkerProcess() {
  const status = await getPm2AppStatusByName(SCRAPE_PM2_NAME);
  if (!status.error && status.online) {
    return { ok: true, action: 'noop_online', out: `already online (${SCRAPE_PM2_NAME})` };
  }
  if (!status.error && status.exists) {
    const startByName = await execPm2(`pm2 start ${SCRAPE_PM2_NAME} --update-env`);
    if (startByName.ok || /online|already\s+running|successfully/i.test(startByName.out)) {
      return { ok: true, action: 'start_existing', out: startByName.out };
    }
    const restartByName = await execPm2(`pm2 restart ${SCRAPE_PM2_NAME} --update-env`);
    if (restartByName.ok || /online|already\s+running|successfully/i.test(restartByName.out)) {
      return { ok: true, action: 'restart_existing', out: restartByName.out };
    }
    return { ok: false, action: 'start_existing_failed', out: `${startByName.out}\n${restartByName.out}`.trim(), err: restartByName.err || startByName.err };
  }
  const create = await execPm2(`pm2 start ${SCRAPE_WORKER_ENTRY} --name ${SCRAPE_PM2_NAME}`);
  if (create.ok || /online|already\s+running|successfully/i.test(create.out)) {
    return { ok: true, action: 'create_missing', out: create.out };
  }
  return { ok: false, action: 'create_missing_failed', out: create.out, err: create.err };
}

async function ensureAssignedClientWorkerStack(reason) {
  const assignedClientId = (getClientId() || '').trim();
  if (!assignedClientId) {
    return;
  }

  const result = await ensureClientWorkerStack(assignedClientId);
  if (!result.ok) {
    console.error(
      `[pm2:auto-ensure] client worker stack failed reason=${reason} clientId=${assignedClientId.slice(0, 8)} error=${
        result.error || 'unknown'
      }`
    );
    return;
  }
  console.log(
    `[pm2:auto-ensure] client worker stack ready reason=${reason} clientId=${assignedClientId.slice(0, 8)} send=${result.sendName}`
  );
}

const STATUS_TIMEOUT_MS = 8000; // respond before typical Edge Function timeouts (~10–15s); status uses fast queries
const STATUS_COMPONENT_TIMEOUT_MS = Math.max(
  1200,
  parseInt(process.env.STATUS_COMPONENT_TIMEOUT_MS || '3200', 10) || 3200
);
const STATUS_SLOW_COMPONENT_LOG_MS = Math.max(
  250,
  parseInt(process.env.STATUS_SLOW_COMPONENT_LOG_MS || '1200', 10) || 1200
);
const STATUS_CACHE_TTL_MS = Math.max(
  1000,
  parseInt(process.env.STATUS_CACHE_TTL_MS || '30000', 10) || 30000
);
const STATUS_CACHE_STALE_MAX_MS = Math.max(
  STATUS_CACHE_TTL_MS,
  parseInt(process.env.STATUS_CACHE_STALE_MAX_MS || '120000', 10) || 120000
);
const PM2_STATUS_CACHE_TTL_MS = Math.max(
  1000,
  parseInt(process.env.PM2_STATUS_CACHE_TTL_MS || '60000', 10) || 60000
);
const STATUS_FIRST_RESPONSE_TIMEOUT_MS = Math.max(
  500,
  parseInt(process.env.STATUS_FIRST_RESPONSE_TIMEOUT_MS || '1200', 10) || 1200
);

const statusCacheByKey = new Map();
const pm2RunningCache = {
  value: false,
  updatedAt: 0,
  inFlight: null,
};

function timeoutAfter(ms, label) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
}

async function settleStatusComponent(label, promise, fallbackValue) {
  const startedAt = Date.now();
  try {
    const value = await Promise.race([
      Promise.resolve(promise),
      timeoutAfter(STATUS_COMPONENT_TIMEOUT_MS, label),
    ]);
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= STATUS_SLOW_COMPONENT_LOG_MS) {
      console.warn(`[API] /api/status slow component ${label}: ${elapsedMs}ms`);
    }
    return { ok: true, value, elapsedMs };
  } catch (err) {
    const elapsedMs = Date.now() - startedAt;
    console.warn(
      `[API] /api/status component fallback ${label}: ${elapsedMs}ms (${err?.message || err})`
    );
    return { ok: false, value: fallbackValue, elapsedMs, error: err };
  }
}

function getStatusCacheKey(useSupabase, clientId) {
  if (useSupabase && clientId) return `sb:${clientId}`;
  return 'local';
}

async function getBotProcessRunningCached() {
  const now = Date.now();
  if (now - pm2RunningCache.updatedAt <= PM2_STATUS_CACHE_TTL_MS) {
    return pm2RunningCache.value;
  }
  if (pm2RunningCache.inFlight) {
    return pm2RunningCache.updatedAt ? pm2RunningCache.value : true;
  }
  pm2RunningCache.inFlight = new Promise((resolve) => getBotProcessRunning(resolve))
    .then((v) => {
      pm2RunningCache.value = !!v;
      pm2RunningCache.updatedAt = Date.now();
      return pm2RunningCache.value;
    })
    .catch(() => {
      pm2RunningCache.updatedAt = Date.now();
      return pm2RunningCache.value;
    })
    .finally(() => {
      pm2RunningCache.inFlight = null;
    });
  if (pm2RunningCache.updatedAt) return pm2RunningCache.value;
  // First status after boot should not block on PM2 when Chrome/PM2 is busy.
  return true;
}

function optimisticStatusPayload({ useSupabase }) {
  const stats = useSupabase ? { total_sent: 0, total_failed: 0 } : getDailyStats();
  return {
    processRunning: true,
    statusMessage: null,
    todaySent: stats.total_sent ?? 0,
    todayFailed: stats.total_failed ?? 0,
    leadsTotal: 0,
    leadsRemaining: 0,
    statusDegraded: true,
  };
}

async function buildStatusPayload({ useSupabase, clientId, requestStartedAt }) {
  if (useSupabase) {
    const [processRunningPm2Res, statsRes, statusMessageRes, leadsCountsRes, pauseFlagRes] =
      await Promise.all([
        settleStatusComponent('pm2_running', getBotProcessRunningCached(), false),
        settleStatusComponent('daily_stats', getDailyStatsSupabase(clientId), {
          total_sent: 0,
          total_failed: 0,
        }),
        settleStatusComponent('status_message', getClientStatusMessageSupabase(clientId), null),
        settleStatusComponent('lead_counts', getLeadsTotalAndRemaining(clientId), {
          total: 0,
          remaining: 0,
        }),
        settleStatusComponent('control_pause', getControlSupabase(clientId), '1'),
      ]);

    const paused = pauseFlagRes.value === '1' || pauseFlagRes.value === 1;
    const processRunning = processRunningPm2Res.value && !paused;
    const degraded =
      !processRunningPm2Res.ok ||
      !statsRes.ok ||
      !statusMessageRes.ok ||
      !leadsCountsRes.ok ||
      !pauseFlagRes.ok;

    if (degraded) {
      const failedLabels = [
        !processRunningPm2Res.ok ? 'pm2_running' : null,
        !statsRes.ok ? 'daily_stats' : null,
        !statusMessageRes.ok ? 'status_message' : null,
        !leadsCountsRes.ok ? 'lead_counts' : null,
        !pauseFlagRes.ok ? 'control_pause' : null,
      ]
        .filter(Boolean)
        .join(', ');
      console.warn(
        `[API] /api/status degraded response for client=${clientId} after ${Date.now() - requestStartedAt}ms; failed=${failedLabels}`
      );
    } else if (Date.now() - requestStartedAt >= STATUS_TIMEOUT_MS) {
      console.warn(
        `[API] /api/status exceeded target timeout (${Date.now() - requestStartedAt}ms) without failure`
      );
    }

    return {
      processRunning,
      statusMessage: statusMessageRes.value ?? (processRunning ? null : 'Stopped'),
      todaySent: statsRes.value.total_sent ?? 0,
      todayFailed: statsRes.value.total_failed ?? 0,
      leadsTotal: leadsCountsRes.value.total ?? 0,
      leadsRemaining: leadsCountsRes.value.remaining ?? 0,
      statusDegraded: degraded,
    };
  }

  const processRunningRes = await settleStatusComponent('pm2_running', getBotProcessRunningCached(), false);
  const stats = getDailyStats();
  const leadsRes = await settleStatusComponent('csv_leads', loadLeadsFromCSV(leadsPath), []);
  const leads = Array.isArray(leadsRes.value) ? leadsRes.value : [];
  const leadsRemaining = leads.filter((u) => !alreadySent(u)).length;
  const degraded = !processRunningRes.ok || !leadsRes.ok;
  return {
    processRunning: processRunningRes.value,
    todaySent: stats.total_sent,
    todayFailed: stats.total_failed,
    leadsTotal: leads.length,
    leadsRemaining,
    statusDegraded: degraded,
  };
}

function readCachedStatus(cacheKey) {
  const entry = statusCacheByKey.get(cacheKey);
  if (!entry || !entry.payload || !entry.updatedAt) return null;
  const ageMs = Date.now() - entry.updatedAt;
  return {
    entry,
    ageMs,
    isFresh: ageMs <= STATUS_CACHE_TTL_MS,
    isWithinStaleWindow: ageMs <= STATUS_CACHE_STALE_MAX_MS,
  };
}

// --- API: health (for proxy/dashboard connectivity check; no DB or pm2) ---
app.get('/api/health', (req, res) => {
  res.json(getHealthPayload());
});

// --- API: status & stats ---
// Returns immediately using only fast queries and stored status (set by the sender loop). No schedule recomputation.
app.get('/api/status', async (req, res) => {
  const clientId = resolveRequestedClientId(req);
  if (req.authClientId && clientId && req.authClientId !== clientId) {
    return res.status(403).json({ error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const useSupabase = isSupabaseConfigured() && clientId;
  const requestStartedAt = Date.now();
  const cacheKey = getStatusCacheKey(useSupabase, clientId);
  const cached = readCachedStatus(cacheKey);

  if (cached?.isFresh) {
    return res.status(200).json({
      ...cached.entry.payload,
      statusCached: true,
      statusCacheAgeMs: cached.ageMs,
    });
  }

  try {
    let cacheEntry = statusCacheByKey.get(cacheKey);
    if (!cacheEntry) {
      cacheEntry = { payload: null, updatedAt: 0, inFlight: null };
      statusCacheByKey.set(cacheKey, cacheEntry);
    }

    if (!cacheEntry.inFlight) {
      cacheEntry.inFlight = buildStatusPayload({ useSupabase, clientId, requestStartedAt })
        .catch((err) => {
          console.warn(`[API] /api/status background refresh failed: ${err?.message || err}`);
          return {
            ...(cacheEntry.payload || optimisticStatusPayload({ useSupabase })),
            statusDegraded: true,
          };
        })
        .then((payload) => {
          cacheEntry.payload = payload;
          cacheEntry.updatedAt = Date.now();
          return payload;
        })
        .finally(() => {
          cacheEntry.inFlight = null;
        });
    }

    // Serve stale quickly while a refresh is happening; avoids hammering pm2/supabase under load.
    if (cached?.isWithinStaleWindow && cacheEntry.inFlight) {
      return res.status(200).json({
        ...cached.entry.payload,
        statusCached: true,
        statusStale: true,
        statusCacheAgeMs: cached.ageMs,
      });
    }

    const payload = await Promise.race([
      cacheEntry.inFlight,
      timeoutAfter(STATUS_FIRST_RESPONSE_TIMEOUT_MS, 'status first response'),
    ]).catch(() => optimisticStatusPayload({ useSupabase }));
    return res.status(200).json({
      ...payload,
      statusCached: false,
      statusPendingRefresh: payload.statusDegraded === true,
      statusCacheAgeMs: 0,
    });
  } catch (e) {
    const fallback = readCachedStatus(cacheKey);
    if (fallback?.isWithinStaleWindow) {
      console.warn(
        `[API] /api/status using stale cache after refresh error (${e?.message || e}); age=${fallback.ageMs}ms`
      );
      return res.status(200).json({
        ...fallback.entry.payload,
        statusCached: true,
        statusStale: true,
        statusCacheAgeMs: fallback.ageMs,
        statusDegraded: true,
      });
    }
    console.error('[API] Status error', e);
    return res.status(500).json({ error: e.message });
  }
});

app.get('/api/stats', (req, res) => {
  const clientId = requireScopedClientId(req, res);
  if (!clientId) return;
  if (isSupabaseConfigured()) {
    getDailyStatsSupabase(clientId)
      .then((stats) => res.json(stats))
      .catch((e) => res.status(500).json({ error: e.message }));
    return;
  }
  res.json(getDailyStats());
});

app.get('/api/sent', (req, res) => {
  const clientId = requireScopedClientId(req, res);
  if (!clientId) return;
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);
  if (isSupabaseConfigured()) {
    getRecentSentSupabase(clientId, limit)
      .then((rows) => res.json(rows))
      .catch((e) => res.status(500).json({ error: e.message }));
    return;
  }
  res.json(getRecentSent(limit));
});

// --- API: settings (.env) ---
const ENV_KEYS = [
  'INSTAGRAM_USERNAME',
  'INSTAGRAM_PASSWORD',
  'DAILY_SEND_LIMIT',
  'MIN_DELAY_MINUTES',
  'MAX_DELAY_MINUTES',
  'MAX_SENDS_PER_HOUR',
  'HEADLESS_MODE',
  'DESKTOP_VIEWPORT_WIDTH',
  'DESKTOP_VIEWPORT_HEIGHT',
  'DESKTOP_WINDOW_PAD_X',
  'DESKTOP_WINDOW_PAD_Y',
  'CHROME_WINDOW_POSITION',
  'CHROMIUM_USE_FAKE_MEDIA_DEVICE',
  'LEADS_CSV',
  'VOICE_NOTE_FILE',
  'VOICE_NOTE_MODE',
  'VOICE_NOTE_SOURCE_NAME',
  'VOICE_NOTE_PIPE_PATH',
  'VOICE_USE_PIPE_SOURCE',
  'VOICE_USE_NULL_SINK',
  'VOICE_FFMPEG_HEAD_START_MS',
  'XDG_RUNTIME_DIR',
  'PULSE_SERVER',
  'VOICE_ASSUME_RECORDING_AFTER_MIC',
  'VOICE_RECORDING_UI_CONFIRM_STREAK',
  'VOICE_LATE_RECORDING_UI_MS',
  'VOICE_DESKTOP_MIC_METHOD',
  'VOICE_SEND_CLICK_NUDGE_X',
  'FOLLOW_UP_DEBUG_BROWSER_MS',
  'FOLLOW_UP_DEBUG_SCREENSHOTS',
  'FOLLOW_UP_SCREENSHOTS_FULL_PAGE',
  'FOLLOW_UP_MESSAGE_ID_DEBUG',
  'VOICE_POST_SEND_BROWSER_WAIT_MS',
];

function readEnv() {
  const out = {};
  if (!fs.existsSync(envPath)) return out;
  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split('\n')) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (!m) continue;
    const k = m[1].trim();
    // Only expose/edit non-secret dashboard settings keys.
    if (ENV_KEYS.includes(k)) {
      out[k] = m[2].trim();
    }
  }
  return out;
}

function upsertEnvKey(key, value) {
  const k = String(key || '').trim();
  if (!k) return;
  const v = value == null ? '' : String(value).trim();
  const nextLine = `${k}=${v}`;
  const existing = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
  const lines = existing.split('\n');
  let replaced = false;
  const out = lines.map((line) => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (!m) return line;
    const lk = m[1].trim();
    if (lk !== k) return line;
    replaced = true;
    return nextLine;
  });
  if (!replaced) {
    if (out.length > 0 && out[out.length - 1] !== '') out.push('');
    out.push(nextLine);
  }
  fs.writeFileSync(envPath, out.join('\n').replace(/\n+$/g, '\n') + '\n', 'utf8');
}

function deleteEnvKey(key) {
  const k = String(key || '').trim();
  if (!k) return;
  if (!fs.existsSync(envPath)) return;
  const existing = fs.readFileSync(envPath, 'utf8');
  const lines = existing.split('\n');
  const out = lines.filter((line) => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (!m) return true;
    return m[1].trim() !== k;
  });
  fs.writeFileSync(envPath, out.join('\n').replace(/\n+$/g, '\n') + '\n', 'utf8');
}

function writeEnv(obj) {
  // IMPORTANT: do not overwrite the entire .env. It contains secrets used by the worker
  // (SUPABASE_URL, service keys, proxy creds, API key), and we only allow editing a safe subset.
  for (const key of ENV_KEYS) {
    if (obj[key] === undefined) continue;
    const v = String(obj[key] ?? '').trim();
    if (!v) deleteEnvKey(key);
    else upsertEnvKey(key, v);
  }
}

app.get('/api/settings', (req, res) => {
  const env = readEnv();
  const safe = { ...env };
  if (safe.INSTAGRAM_PASSWORD) safe.INSTAGRAM_PASSWORD = '********';
  res.json(safe);
});

app.post('/api/settings', (req, res) => {
  const env = readEnv();
  const body = req.body || {};
  for (const key of ENV_KEYS) {
    if (body[key] === undefined) continue;
    if (key === 'INSTAGRAM_PASSWORD' && body[key] === '********') continue;
    env[key] = body[key];
  }
  writeEnv(env);
  res.json({ ok: true });
});

// --- API: messages (templates) ---
app.get('/api/messages', (req, res) => {
  res.json({ messages: MESSAGES });
});

app.post('/api/messages', (req, res) => {
  const { messages } = req.body || {};
  if (!Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages must be a non-empty array' });
  }
  const configPath = path.join(__dirname, 'config', 'messages.js');
  const content = `const MESSAGES = ${JSON.stringify(messages, null, 2)};\n\nfunction getRandomMessage() {\n  return MESSAGES[Math.floor(Math.random() * MESSAGES.length)];\n}\n\nmodule.exports = { MESSAGES, getRandomMessage };\n`;
  fs.writeFileSync(configPath, content, 'utf8');
  res.json({ ok: true });
});

// --- API: leads ---
app.get('/api/leads', (req, res) => {
  if (!fs.existsSync(leadsPath)) {
    return res.json({ usernames: [], raw: '' });
  }
  const raw = fs.readFileSync(leadsPath, 'utf8');
  loadLeadsFromCSV(leadsPath).then((usernames) => {
    res.json({ usernames, raw });
  }).catch(() => res.json({ usernames: [], raw }));
});

app.post('/api/leads', (req, res) => {
  const { raw } = req.body || {};
  const lines = (raw || '').split('\n').map((l) => l.trim()).filter(Boolean);
  const usernames = lines.map((u) => u.replace(/^@/, ''));
  const header = 'username\n';
  const body = usernames.join('\n') + (usernames.length ? '\n' : '');
  fs.writeFileSync(leadsPath, header + body, 'utf8');
  res.json({ ok: true, count: usernames.length });
});

app.post('/api/leads/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }
  const raw = fs.readFileSync(req.file.path, 'utf8');
  fs.unlinkSync(req.file.path);
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const first = lines[0].toLowerCase();
  const start = first === 'username' || first === 'user' ? 1 : 0;
  const usernames = lines.slice(start).map((u) => u.replace(/^@/, '')).filter(Boolean);
  const header = 'username\n';
  const body = usernames.join('\n') + (usernames.length ? '\n' : '');
  fs.writeFileSync(leadsPath, header + body, 'utf8');
  res.json({ ok: true, count: usernames.length });
});

/**
 * SkeduleMore follow-up send (browser session). Auth: Bearer COLD_DM_API_KEY (same as other /api routes when set).
 * Body: { clientId, instagramSessionId, recipientUsername, text? | messages? | audioUrl?, caption? }
 * Voice: `audioUrl` = HTTPS URL the worker GETs; optional `caption` = text in-thread before voice. Correlation: X-Correlation-ID / X-Request-ID / body correlationId | requestId.
 */
app.post('/api/follow-up/send', followUpLimiter, async (req, res) => {
  const body = req.body || {};
  const cid = (body.clientId || '').trim();
  if (req.authClientId && cid !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!cid) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  if (!isSupabaseConfigured()) {
    logger.warn('[API] follow-up/send 503 Supabase not configured');
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const correlationId = (
    req.get('x-correlation-id') ||
    req.get('x-request-id') ||
    (body.correlationId && String(body.correlationId).trim()) ||
    (body.requestId && String(body.requestId).trim()) ||
    ''
  ).trim();
  if (correlationId) {
    try {
      const proceed = await tryVpsIdempotencyOnce(cid, 'follow-up/send', correlationId);
      if (!proceed) {
        logger.log(`[API] follow-up/send idempotent duplicate correlationId=${correlationId}`);
        return res.json({ ok: true, duplicate: true, correlationId });
      }
    } catch (e) {
      logger.warn('[API] follow-up/send idempotency error (continuing)', e.message || e);
    }
  }
  const sid = (body.instagramSessionId || '').trim();
  const recip = (body.recipientUsername || '').trim().replace(/^@/, '');
  let mode = 'unknown';
  if (body.text != null && String(body.text).trim() !== '') mode = 'text';
  else if (Array.isArray(body.messages) && body.messages.some((m) => String(m).trim())) {
    mode = `messages(${body.messages.filter((m) => String(m).trim()).length})`;
  } else if (body.audioUrl) mode = body.caption != null && String(body.caption).trim() !== '' ? 'voice+caption' : 'voice';
  const corrPart = correlationId ? ` correlationId=${correlationId}` : '';
  logger.log(
    `[API] follow-up/send request clientId=${cid || '-'} sessionId=${sid || '-'} recipient=@${recip || '-'} mode=${mode}${corrPart}`
  );
  try {
    const payload = correlationId ? { ...body, correlationId } : body;
    const result = await sendFollowUp(payload);
    if (result.ok) {
      const hasIds =
        !!result.instagram_message_id ||
        (Array.isArray(result.instagram_message_ids) && result.instagram_message_ids.some((x) => x != null));
      const idPart = hasIds
        ? ` instagram_message_id=${result.instagram_message_id || '-'} instagram_message_ids=${result.instagram_message_ids ? JSON.stringify(result.instagram_message_ids) : '-'}`
        : '';
      logger.log(
        `[API] follow-up/send response ok=true clientId=${cid || '-'} recipient=@${recip || '-'}${corrPart}${idPart}`
      );
      const body = { ok: true };
      if (result.instagram_message_id) {
        body.instagram_message_id = result.instagram_message_id;
        body.instagramMessageId = result.instagramMessageId ?? result.instagram_message_id;
      }
      if (result.instagram_message_ids && result.instagram_message_ids.length > 0) {
        body.instagram_message_ids = result.instagram_message_ids;
        body.instagramMessageIds = result.instagramMessageIds ?? result.instagram_message_ids;
      }
      return res.json(body);
    }
    const status = result.statusCode && result.statusCode >= 400 && result.statusCode < 600 ? result.statusCode : 400;
    logger.warn(
      `[API] follow-up/send response ok=false status=${status} error=${result.error || 'Send failed'}${corrPart}`
    );
    return res.status(status).json({
      ok: false,
      error: result.error || 'Send failed',
      ...(result.code ? { code: result.code } : {}),
      ...(result.retryable != null ? { retryable: Boolean(result.retryable) } : {}),
      ...(result.retryAfter ? { retryAfter: result.retryAfter } : {}),
      ...(result.retryAfterIso ? { retryAfterIso: result.retryAfterIso } : {}),
      ...(result.dailyLimit != null ? { dailyLimit: result.dailyLimit } : {}),
      ...(result.totalSent != null ? { totalSent: result.totalSent } : {}),
    });
  } catch (e) {
    logger.error('[API] follow-up/send exception', e);
    return res.status(500).json({ ok: false, error: e.message || 'Internal error' });
  }
});

/**
 * Open headed Chromium with session cookies for manual testing (VNC) — does not send messages or voice.
 * Body: { clientId, instagramSessionId, recipientUsername? }
 * Server needs HEADLESS_MODE=false and DISPLAY (e.g. :98 with Xvfb). Returns 202 immediately; browser starts in background.
 */
/**
 * Admin / debug: same DM navigation + thread display-name extraction as a real send; does not type or send.
 * Body: { clientId, instagramSessionId, username, first_name?, last_name?, display_name? }
 * `username` is the **recipient** lead handle to open a thread with (not the sender). After a compose-recovery
 * CTA (e.g. Continue), JSON may include `compose_recovery_screenshot` (VPS path under logs/login-debug).
 */
app.post('/api/debug/preview-dm-names', async (req, res) => {
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const body = req.body || {};
  const cid = (body.clientId || '').trim();
  if (req.authClientId && cid && cid !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!cid) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  try {
    const result = await previewDmLeadNamesFromSession(body);
    let status = 200;
    if (result.error === 'Instagram session expired') status = 401;
    else if (
      typeof result.error === 'string' &&
      /required|not configured|not found|no cookies/i.test(result.error)
    ) {
      status = 400;
    }
    return res.status(status).json(result);
  } catch (e) {
    logger.error('[API] debug/preview-dm-names exception', e);
    return res.status(500).json({ ok: false, error: e.message || 'Internal error' });
  }
});

app.post('/api/debug/follow-up/browser', (req, res) => {
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const body = req.body || {};
  const cid = (body.clientId || '').trim();
  if (req.authClientId && cid && cid !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const sid = (body.instagramSessionId || '').trim();
  if (!cid || !sid) {
    return res.status(400).json({ ok: false, error: 'clientId and instagramSessionId are required' });
  }
  const scheduled = scheduleDebugFollowUpBrowser(body);
  if (!scheduled.ok) {
    return res.status(409).json({ ok: false, error: scheduled.error || 'Could not start debug browser' });
  }
  const recip = (body.recipientUsername || '').trim().replace(/^@/, '') || '-';
  logger.log(`[API] debug/follow-up/browser queued clientId=${cid} sessionId=${sid} recipient=@${recip}`);
  return res.status(202).json({
    ok: true,
    accepted: true,
    hint: 'Use VNC on the same DISPLAY as this process. Chromium should appear in a few seconds. No DMs are sent.',
    env: {
      HEADLESS_MODE: process.env.HEADLESS_MODE || '(unset)',
      DISPLAY: process.env.DISPLAY || '(unset)',
      FOLLOW_UP_DEBUG_BROWSER_MS:
        process.env.FOLLOW_UP_DEBUG_BROWSER_MS || '(unset — window stays until PM2 restart)',
    },
  });
});

/** List PNGs from follow-up debug runs (Bearer COLD_DM_API_KEY when set). */
app.get('/api/debug/follow-up-screenshots', (req, res) => {
  try {
    if (!fs.existsSync(followUpScreenshotsDir)) {
      return res.json({
        ok: true,
        files: [],
        directory: 'follow-up-screenshots',
        hint: 'Set FOLLOW_UP_DEBUG_SCREENSHOTS=true, restart PM2, run a voice follow-up',
      });
    }
    const names = fs
      .readdirSync(followUpScreenshotsDir)
      .filter((f) => /\.png$/i.test(f) && !f.startsWith('.'));
    const files = names
      .map((name) => {
        const fp = path.join(followUpScreenshotsDir, name);
        try {
          const st = fs.statSync(fp);
          return { name, size: st.size, mtime: st.mtime.toISOString() };
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => (a.mtime < b.mtime ? 1 : -1));
    res.json({
      ok: true,
      files,
      directory: 'follow-up-screenshots',
      downloadUrl: '/api/debug/follow-up-screenshots/file?name=FILENAME.png',
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'list failed' });
  }
});

/** Download one screenshot PNG (query: name=). */
app.get('/api/debug/follow-up-screenshots/file', (req, res) => {
  const raw = req.query.name;
  const name =
    raw && typeof raw === 'string' && /^[a-zA-Z0-9._-]+\.png$/i.test(path.basename(raw))
      ? path.basename(raw)
      : null;
  if (!name) return res.status(400).json({ ok: false, error: 'Invalid or missing ?name=filename.png' });
  const fp = path.join(followUpScreenshotsDir, name);
  const resolved = path.resolve(fp);
  const resolvedDir = path.resolve(followUpScreenshotsDir);
  if (!resolved.startsWith(resolvedDir + path.sep)) {
    return res.status(400).json({ ok: false, error: 'Invalid path' });
  }
  if (!fs.existsSync(resolved)) return res.status(404).json({ ok: false, error: 'Not found' });
  res.setHeader('Content-Type', 'image/png');
  res.setHeader('Content-Disposition', `inline; filename="${name}"`);
  res.sendFile(resolved);
});

/** List PNGs from logs/login-debug (DM search / login / compose debug captures). */
app.get('/api/debug/login-screenshots', (req, res) => {
  try {
    if (!fs.existsSync(loginDebugScreenshotsDir)) {
      return res.json({
        ok: true,
        files: [],
        directory: 'logs/login-debug',
        hint: 'Enable DM_SEARCH_DEBUG_SCREENSHOTS=true, then run preview/connect/send to generate screenshots.',
      });
    }
    const names = fs
      .readdirSync(loginDebugScreenshotsDir)
      .filter((f) => /\.png$/i.test(f) && !f.startsWith('.'));
    const files = names
      .map((name) => {
        const fp = path.join(loginDebugScreenshotsDir, name);
        try {
          const st = fs.statSync(fp);
          return { name, size: st.size, mtime: st.mtime.toISOString() };
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => (a.mtime < b.mtime ? 1 : -1));
    return res.json({
      ok: true,
      files,
      directory: 'logs/login-debug',
      downloadUrl: '/api/debug/login-screenshots/file?name=FILENAME.png',
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message || 'list failed' });
  }
});

/** Download one screenshot PNG from logs/login-debug (query: name=). */
app.get('/api/debug/login-screenshots/file', (req, res) => {
  const raw = req.query.name;
  const name =
    raw && typeof raw === 'string' && /^[a-zA-Z0-9._-]+\.png$/i.test(path.basename(raw))
      ? path.basename(raw)
      : null;
  if (!name) return res.status(400).json({ ok: false, error: 'Invalid or missing ?name=filename.png' });
  const fp = path.join(loginDebugScreenshotsDir, name);
  const resolved = path.resolve(fp);
  const resolvedDir = path.resolve(loginDebugScreenshotsDir);
  if (!resolved.startsWith(resolvedDir + path.sep)) {
    return res.status(400).json({ ok: false, error: 'Invalid path' });
  }
  if (!fs.existsSync(resolved)) return res.status(404).json({ ok: false, error: 'Not found' });
  res.setHeader('Content-Type', 'image/png');
  res.setHeader('Content-Disposition', `inline; filename="${name}"`);
  res.sendFile(resolved);
});

app.post('/api/voice/upload', uploadVoice.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: 'No file uploaded' });
  const ext = path.extname(req.file.originalname || '').toLowerCase();
  const allowed = new Set(['.wav', '.mp3', '.m4a', '.ogg', '.webm']);
  if (!allowed.has(ext)) {
    fs.unlinkSync(req.file.path);
    return res.status(400).json({ ok: false, error: 'Only wav/mp3/m4a/ogg/webm audio is allowed' });
  }
  const safeBase = path.basename(req.file.originalname, ext).replace(/[^a-z0-9_-]+/gi, '_').slice(0, 60) || 'voice_note';
  const finalName = `${Date.now()}_${safeBase}${ext}`;
  const finalPath = path.join(voiceNotesDir, finalName);
  fs.renameSync(req.file.path, finalPath);
  const env = readEnv();
  env.VOICE_NOTE_FILE = finalPath;
  writeEnv(env);
  res.json({ ok: true, path: finalPath, publicUrl: `/voice-notes/${finalName}` });
});

// Pending 2FA sessions: id -> { page, browser, username, clientId, createdAt }. Cleared when code is submitted or after TTL.
const pending2FAMap = new Map();
const pendingScraper2FAMap = new Map();
const pendingEmailVerifyMap = new Map();
const reconnectLocksByClientId = new Map();
const PENDING_2FA_TTL_MS = 2 * 60 * 1000;

function cleanupExpired2FA() {
  const now = Date.now();
  for (const [id, data] of pending2FAMap.entries()) {
    if (now - data.createdAt > PENDING_2FA_TTL_MS) {
      pending2FAMap.delete(id);
      if (data.browser) data.browser.close().catch(() => {});
    }
  }
}

function cleanupExpiredEmailVerify() {
  const now = Date.now();
  for (const [id, data] of pendingEmailVerifyMap.entries()) {
    if (now - data.createdAt > PENDING_2FA_TTL_MS) {
      pendingEmailVerifyMap.delete(id);
      if (data.browser) data.browser.close().catch(() => {});
    }
  }
}

function cleanupExpiredScraper2FA() {
  const now = Date.now();
  for (const [id, data] of pendingScraper2FAMap.entries()) {
    if (now - data.createdAt > PENDING_2FA_TTL_MS) {
      pendingScraper2FAMap.delete(id);
      if (data.browser) data.browser.close().catch(() => {});
    }
  }
}

// --- API: Instagram connect (one-time; password never stored) ---
// Policy: 2FA is required for connect. If the account reaches 2FA challenge, returns
// { ok: false, code: 'two_factor_required', pending2FAId } for POST /api/instagram/connect/2fa.
// If it does not reach 2FA (e.g. straight login or email-code checkpoint), connect is rejected.
app.post('/api/instagram/connect', connectLimiter, async (req, res) => {
  const { username, password, clientId } = req.body || {};
  const reqId = require('crypto').randomBytes(6).toString('hex');
  const startedAt = Date.now();
  const safeUser = String(username || '').trim().replace(/^@/, '').toLowerCase();
  const safeClient = String(clientId || '').trim();
  console.log(
    `[API] instagram_connect:start id=${reqId} clientId=${safeClient ? safeClient.slice(0, 8) : 'missing'} user=${
      safeUser || 'missing'
    }`
  );
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!username || !password || !clientId) {
    return res.status(400).json({ ok: false, error: 'username, password, and clientId are required' });
  }
  if (reconnectLocksByClientId.get(String(clientId))) {
    return res.status(409).json({ ok: false, error: 'A reconnect is already in progress for this client. Please wait and retry.' });
  }
  reconnectLocksByClientId.set(String(clientId), true);
  if (!isSupabaseConfigured()) {
    reconnectLocksByClientId.delete(String(clientId));
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const isAdmin = await isAdminUser(clientId).catch(() => false);
    const igKey = String(username)
      .trim()
      .replace(/^@/, '')
      .toLowerCase();
    if (!isAdmin) {
      // Non-admins can have only one automation account. Reconnect is allowed only when it's the same handle.
      const existing = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
      const existingKey =
        existing?.instagram_username != null ? String(existing.instagram_username).trim().toLowerCase() : null;
      if (existingKey && existingKey !== igKey) {
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    let proxyMeta = { proxyUrl: null, proxyAssignmentId: null };
    try {
      proxyMeta = await getOrResolveColdDmProxyUrl(clientId, igKey);
    } catch (pe) {
      console.error(
        `[API] instagram_connect:proxy_failed id=${reqId} afterMs=${Date.now() - startedAt}`,
        pe
      );
      return res.status(503).json({
        ok: false,
        error: pe instanceof Error ? pe.message : String(pe) || 'Could not allocate proxy (check Decodo API and credits)',
      });
    }
    console.log(
      `[API] instagram_connect:proxy_ok id=${reqId} afterMs=${Date.now() - startedAt} proxy=${
        proxyMeta.proxyUrl ? 'set' : 'missing'
      } assignmentId=${proxyMeta.proxyAssignmentId ? String(proxyMeta.proxyAssignmentId).slice(0, 8) : 'n/a'}`
    );
    const allowNoProxy =
      process.env.COLD_DM_ALLOW_NO_PROXY === '1' || process.env.COLD_DM_ALLOW_NO_PROXY === 'true';
    if (!allowNoProxy && !proxyMeta.proxyUrl) {
      return res.status(503).json({
        ok: false,
        error:
          'Proxy is not configured for this VPS worker. Set DECODO_SHARED_USERNAME/DECODO_SHARED_PASSWORD (or DECODO_API_KEY) and retry Connect.',
        code: 'proxy_not_configured',
      });
    }
    console.log(
      `[API] instagram_connect:puppeteer_start id=${reqId} afterMs=${Date.now() - startedAt}`
    );
    const result = await connectInstagram(username, password, null, { proxyUrl: proxyMeta.proxyUrl });
    console.log(
      `[API] instagram_connect:puppeteer_done id=${reqId} afterMs=${Date.now() - startedAt} twoFactor=${
        result?.twoFactorRequired ? '1' : '0'
      } emailVerify=${result?.emailVerificationRequired ? '1' : '0'}`
    );
    if (result.twoFactorRequired) {
      cleanupExpired2FA();
      const pendingId = require('crypto').randomBytes(16).toString('hex');
      pending2FAMap.set(pendingId, {
        page: result.page,
        browser: result.browser,
        username: result.username,
        clientId,
        createdAt: Date.now(),
        proxyUrl: proxyMeta.proxyUrl,
        proxyAssignmentId: proxyMeta.proxyAssignmentId,
      });
      console.log(
        `[API] instagram_connect:two_factor_required id=${reqId} afterMs=${Date.now() - startedAt} pending2FAId=${pendingId.slice(
          0,
          8
        )}`
      );
      return res.status(200).json({
        ok: false,
        code: 'two_factor_required',
        message: 'Enter the 6-digit code from your app or WhatsApp.',
        pending2FAId: pendingId,
      });
    }
    if (result.emailVerificationRequired) {
      if (result.browser) result.browser.close().catch(() => {});
      console.log(
        `[API] instagram_connect:email_verification_required id=${reqId} afterMs=${Date.now() - startedAt}`
      );
      return res.status(400).json({
        ok: false,
        code: 'two_factor_required_for_connect',
        error:
          'Two-factor authentication is required for connect. This account prompted email verification instead of app/WhatsApp 2FA. Enable 2FA in Instagram Security settings, then reconnect.',
      });
    }
    // Connect succeeded without requiring a challenge (e.g. valid existing browser session/cookies).
    await saveSession(clientId, mergeInstagramSessionData(result.cookies, result.web_storage), result.username, {
      proxyUrl: proxyMeta.proxyUrl,
      proxyAssignmentId: proxyMeta.proxyAssignmentId,
    });
    await updateSettingsInstagramUsername(clientId, result.username).catch(() => {});
    console.log(
      `[API] instagram_connect:success id=${reqId} afterMs=${Date.now() - startedAt} user=${String(
        result.username || ''
      )
        .trim()
        .replace(/^@/, '')
        .toLowerCase()}`
    );
    return res.status(200).json({
      ok: true,
      username: result.username,
      message: 'Connected. Existing Instagram session was restored.',
    });
  } catch (e) {
    console.error(`[API] instagram_connect:failed id=${reqId} afterMs=${Date.now() - startedAt}`, e);
    if (e.code === 'TWO_FACTOR_REQUIRED') {
      return res.status(200).json({ ok: false, code: 'two_factor_required', message: e.message || 'Enter the 6-digit code from your app or WhatsApp.' });
    }
    if (e.code === 'EMAIL_VERIFICATION_REQUIRED') {
      return res.status(200).json({
        ok: false,
        code: 'email_verification_required',
        message: e.message || 'Enter the verification code sent to your email.',
        maskedEmail: e.maskedEmail || null,
      });
    }
    res.status(500).json({ ok: false, error: e.message || 'Login failed' });
  } finally {
    reconnectLocksByClientId.delete(String(clientId));
  }
});

app.post('/api/instagram/connect/2fa', connectLimiter, async (req, res) => {
  const { pending2FAId, twoFactorCode, clientId } = req.body || {};
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!pending2FAId || !twoFactorCode || !clientId) {
    return res.status(400).json({ ok: false, error: 'pending2FAId, twoFactorCode, and clientId are required' });
  }
  const pending = pending2FAMap.get(pending2FAId);
  if (!pending) {
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when the popup appears.' });
  }
  if (String(pending.clientId) !== String(clientId)) {
    return res.status(403).json({ ok: false, error: 'Forbidden: pending 2FA session does not belong to this clientId' });
  }
  if (Date.now() - pending.createdAt > PENDING_2FA_TTL_MS) {
    pending2FAMap.delete(pending2FAId);
    if (pending.browser) pending.browser.close().catch(() => {});
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when the popup appears.' });
  }
  pending2FAMap.delete(pending2FAId);
  try {
    const result = await completeInstagram2FA(pending.page, pending.browser, twoFactorCode, pending.username);
    const isAdmin = await isAdminUser(clientId).catch(() => false);
    if (!isAdmin) {
      const existing = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
      const existingKey =
        existing?.instagram_username != null ? String(existing.instagram_username).trim().toLowerCase() : null;
      const nextKey = result?.username != null ? String(result.username).trim().replace(/^@/, '').toLowerCase() : null;
      if (existingKey && nextKey && existingKey !== nextKey) {
        if (pending.browser) pending.browser.close().catch(() => {});
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    await saveSession(clientId, mergeInstagramSessionData(result.cookies, result.web_storage), result.username, {
      proxyUrl: pending.proxyUrl,
      proxyAssignmentId: pending.proxyAssignmentId,
    });
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram 2FA complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || '2FA failed' });
  }
});

app.post('/api/instagram/connect/email-code', connectLimiter, async (req, res) => {
  const { pendingEmailId, emailCode, clientId } = req.body || {};
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!pendingEmailId || !emailCode || !clientId) {
    return res.status(400).json({ ok: false, error: 'pendingEmailId, emailCode, and clientId are required' });
  }
  const pending = pendingEmailVerifyMap.get(pendingEmailId);
  if (!pending) {
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when prompted.' });
  }
  if (String(pending.clientId) !== String(clientId)) {
    return res.status(403).json({ ok: false, error: 'Forbidden: pending verification session does not belong to this clientId' });
  }
  if (Date.now() - pending.createdAt > PENDING_2FA_TTL_MS) {
    pendingEmailVerifyMap.delete(pendingEmailId);
    if (pending.browser) pending.browser.close().catch(() => {});
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when prompted.' });
  }
  pendingEmailVerifyMap.delete(pendingEmailId);
  try {
    const result = await completeInstagramEmailVerification(pending.page, pending.browser, emailCode, pending.username);
    const isAdmin = await isAdminUser(clientId).catch(() => false);
    if (!isAdmin) {
      const existing = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
      const existingKey =
        existing?.instagram_username != null ? String(existing.instagram_username).trim().toLowerCase() : null;
      const nextKey = result?.username != null ? String(result.username).trim().replace(/^@/, '').toLowerCase() : null;
      if (existingKey && nextKey && existingKey !== nextKey) {
        if (pending.browser) pending.browser.close().catch(() => {});
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    await saveSession(clientId, mergeInstagramSessionData(result.cookies, result.web_storage), result.username, {
      proxyUrl: pending.proxyUrl,
      proxyAssignmentId: pending.proxyAssignmentId,
    });
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram email verification complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || 'Email verification failed' });
  }
});

app.post('/api/instagram/instagrapi/connect', (_req, res) => {
  return res.status(410).json({
    ok: false,
    error: 'Instagrapi scraping has been removed. Use the legacy Puppeteer connect and scrape flow.',
  });
});

// --- API: bot control (PM2 start/stop) ---
// Return 200 immediately; pm2 start/stop runs in background. Sender loop does the actual wait (schedule, limits).
app.post('/api/control/start', async (req, res) => {
  const clientId = req.body?.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const campaignId = req.body?.campaignId || null;
  dashboardDebugState.lastControlStart = {
    clientId: clientId || null,
    campaignId,
    receivedAt: new Date().toISOString(),
    ip: req.ip,
    userAgent: req.get('user-agent') || null,
    authClientId: req.authClientId || null,
    perClientWorkersEnabled: PER_CLIENT_PM2_WORKERS_ENABLED,
  };
  appendDashboardAudit('control_start_received', dashboardDebugState.lastControlStart);
  if (isSupabaseConfigured()) {
    if (!clientId) return res.status(400).json({ ok: false, error: 'clientId required when using Supabase' });
    try {
      const reactivated = await reactivateCampaignsWithPendingLeads(clientId, campaignId);
      if (reactivated > 0) {
        console.log(
          `[API] Reactivated ${reactivated} campaign(s) with pending leads for clientId=${clientId}` +
            (campaignId ? ` campaignId=${campaignId}` : '')
        );
      }
      const delayProblems = await getCampaignsMissingSendDelays(clientId, campaignId).catch(() => []);
      if (delayProblems.length > 0) {
        const labels = delayProblems
          .slice(0, 3)
          .map((c) => `"${c.name || c.id}"`)
          .join(', ');
        const extra = delayProblems.length > 3 ? ` (+${delayProblems.length - 3} more)` : '';
        const errorMessage =
          `Campaign ${labels}${extra} missing min/max send delay settings. Set those in campaign settings before pressing Start.`;
        await setClientStatusMessage(clientId, errorMessage).catch(() => {});
        return res.status(400).json({ ok: false, error: errorMessage, problems: delayProblems });
      }
      const sessionsForCampaign = await getSessionsForCampaign(clientId, campaignId).catch(() => []);
      const staleSessions = (sessionsForCampaign || []).filter((s) => s?.web_session_needs_refresh === true);
      if (staleSessions.length > 0) {
        const reconnectMessage =
          'Please reconnect your account in Settings > Integrations > Automation session (outbound).';
        await setClientStatusMessage(clientId, reconnectMessage).catch(() => {});
        return res.status(400).json({
          ok: false,
          code: 'session_reconnect_required',
          error: reconnectMessage,
        });
      }
      const sessionRow = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
      if (sessionRow?.scrape_cooldown_until) {
        const untilMs = new Date(sessionRow.scrape_cooldown_until).getTime();
        if (Number.isFinite(untilMs) && untilMs > Date.now()) {
          const waitMs = untilMs - Date.now();
          const cooldownMessage = `Scraping cooldown active for account safety. Sending can resume in ${formatDurationShort(waitMs)}.`;
          await setClientStatusMessage(clientId, cooldownMessage).catch(() => {});
          return res.status(400).json({
            ok: false,
            code: 'scrape_cooldown',
            error: cooldownMessage,
            scrapeCooldownUntil: sessionRow.scrape_cooldown_until,
          });
        }
      }
      const noWorkHint = await getNoWorkHint(clientId).catch(() => '');
      if (noWorkHint) {
        await setClientStatusMessage(clientId, noWorkHint).catch(() => {});
      } else {
        await setClientStatusMessage(clientId, 'Starting…').catch(() => {});
      }
    } catch (e) {
      console.error('[API] reactivateCampaignsWithPendingLeads', e);
    }
    await setControlSupabase(clientId, 0).catch((e) => console.error('[API] setControlSupabase', e));
    await syncSendJobsForClient(clientId, campaignId || null, { force: true }).catch((e) => {
      console.error(
        '[API] syncSendJobsForClient',
        JSON.stringify({
          code: e?.code || null,
          message: e?.message || String(e),
          details: e?.details || null,
          hint: e?.hint || null,
        })
      );
    });
    try {
      const noWorkHint = await getNoWorkHint(clientId).catch(() => '');
      let statusToSet = '';
      if (noWorkHint) {
        statusToSet = noWorkHint;
      } else {
        const resume = await getClientNoWorkResumeAt(clientId).catch(() => ({
          message: null,
          reason: 'pending_ready',
          resumeAt: null,
        }));
        const rMsg = resume?.message != null ? String(resume.message).trim() : '';
        const reason = resume?.reason || 'pending_ready';
        if (reason === 'no_pending' && !rMsg) {
          statusToSet = 'No pending leads to send.';
        } else if (reason === 'pending_ready' && !rMsg) {
          statusToSet = 'Ready to send.';
        } else if (rMsg) {
          statusToSet = rMsg.slice(0, 500);
        } else {
          statusToSet = `Nothing to send right now (${reason}).`;
        }
      }
      await setClientStatusMessage(clientId, statusToSet).catch(() => {});
    } catch (e) {
      console.error('[API] control/start status message', e);
    }
    console.log('[API] Start (pause=0) for clientId=', clientId);
    appendDashboardAudit('control_start_responding', {
      clientId,
      campaignId,
      processRunning: true,
      perClientWorkersEnabled: PER_CLIENT_PM2_WORKERS_ENABLED,
    });
    res.json({ ok: true, processRunning: true });
    const ensureWorkers = PER_CLIENT_PM2_WORKERS_ENABLED
      ? ensureClientWorkerStack(clientId)
      : ensureSendWorkerProcess();
    ensureWorkers
      .then((r) => {
        appendDashboardAudit('control_start_worker_ensure_result', {
          clientId,
          campaignId,
          result: r,
        });
        if (!r.ok) {
          const detail = String(r.out || r.err?.message || r.error || 'pm2 ensure failed').slice(0, 220);
          console.error('[API] pm2 ensure client worker stack failed', r.err || detail);
          setClientStatusMessage(clientId, `Worker stack did not start: ${detail}`).catch(() => {});
          return;
        }
        if (PER_CLIENT_PM2_WORKERS_ENABLED) {
          console.log(
            `[API] pm2 ensure client worker stack ready send=${r.sendName || 'n/a'} scrape=${r.scrapeName || 'n/a'}`
          );
        } else {
          if (r.out) console.log(`[API] pm2 ensure send worker (${r.action}):`, r.out.slice(0, 800));
          else console.log(`[API] pm2 ensure send worker (${r.action}) done.`);
          scheduleAutoScaleSendWorkers('after_start');
        }
      })
      .catch((err) => {
        appendDashboardAudit('control_start_worker_ensure_exception', {
          clientId,
          campaignId,
          err,
        });
        console.error('[API] pm2 ensure client worker stack failed', err);
        const detail = String(err?.message || 'pm2 ensure failed').slice(0, 220);
        setClientStatusMessage(clientId, `Worker stack did not start: ${detail}`).catch(() => {});
      });
    return;
  }
  setControl('pause', '0');
  console.log('[API] Start bot requested (legacy)');
  res.json({ ok: true, processRunning: true });
  ensureSendWorkerProcess()
    .then((r) => {
      if (!r.ok) {
        console.error('[API] pm2 ensure send worker failed (legacy)', r.err || r.out);
        return;
      }
      if (r.out) console.log(`[API] pm2 ensure send worker (legacy:${r.action}):`, r.out.slice(0, 800));
      else console.log('[API] Bot start command finished (legacy).');
      scheduleAutoScaleSendWorkers('after_start');
    })
    .catch((err) => console.error('[API] pm2 ensure send worker failed (legacy)', err));
});

app.post('/api/reset-failed', async (req, res) => {
  const clientId = req.body?.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  try {
    if (isSupabaseConfigured() && clientId) {
      const cleared = await clearFailedAttemptsSupabase(clientId);
      return res.json({ ok: true, cleared });
    }
    const cleared = clearFailedAttempts();
    res.json({ ok: true, cleared });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Add all leads from the campaign's lead groups into cold_dm_campaign_leads (pending). Dashboard can call this when user clicks "Add all leads from groups".
app.post('/api/campaigns/add-leads-from-groups', async (req, res) => {
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const { campaignId, clientId } = req.body || {};
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!campaignId || !clientId) {
    return res.status(400).json({ ok: false, error: 'campaignId and clientId are required' });
  }
  try {
    const added = await addCampaignLeadsFromGroups(clientId, campaignId);
    const queued = await syncSendJobsForClient(clientId, campaignId, { force: true }).catch(() => 0);
    return res.json({ ok: true, added, queued_send_jobs: queued });
  } catch (e) {
    console.error('[API] add-leads-from-groups', e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Scraper connect is deprecated; scraping now uses the same sender session. ---
app.post('/api/scraper/connect', connectLimiter, async (_req, res) => {
  return res.status(410).json({
    ok: false,
    error: 'Deprecated endpoint. Scraping now uses the same Instagram session as sending. Reconnect it in Settings > Integrations.',
  });
});

app.post('/api/scraper/connect/2fa', connectLimiter, async (_req, res) => {
  return res.status(410).json({
    ok: false,
    error: 'Deprecated endpoint. Scraping now uses the same Instagram session as sending. Reconnect it in Settings > Integrations.',
  });
});

app.get('/api/scraper/status', async (req, res) => {
  const clientId = req.query.clientId || req.body?.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!clientId) {
    return res.status(400).json({ error: 'clientId is required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ error: 'Supabase not configured' });
  }
  try {
    const session = await getMostRecentInstagramSessionForClient(clientId);
    const connected = !!(session?.session_data?.cookies?.length);
    const response = {
      connected,
      instagram_username: session?.instagram_username || null,
    };
    const job = await getLatestScrapeJob(clientId);
    if (job) {
      response.currentJob = {
        id: job.id,
        target_username: job.target_username,
        status: job.status,
        scraped_count: job.scraped_count,
      };
    }
    res.json(response);
  } catch (e) {
    console.error('[API] Scraper status error', e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/scraper/status', async (req, res) => {
  const clientId = req.body?.clientId || req.query.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!clientId) {
    return res.status(400).json({ error: 'clientId is required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ error: 'Supabase not configured' });
  }
  try {
    const session = await getMostRecentInstagramSessionForClient(clientId);
    const connected = !!(session?.session_data?.cookies?.length);
    const response = {
      connected,
      instagram_username: session?.instagram_username || null,
    };
    const job = await getLatestScrapeJob(clientId);
    if (job) {
      response.currentJob = {
        id: job.id,
        target_username: job.target_username,
        status: job.status,
        scraped_count: job.scraped_count,
      };
    }
    res.json(response);
  } catch (e) {
    console.error('[API] Scraper status error', e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/scraper/start', async (req, res) => {
  const { clientId, target_username, max_leads, lead_group_id, scrape_type, post_urls } = req.body || {};
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const rawType = String(scrape_type || 'followers')
    .trim()
    .toLowerCase();
  const scrapeType = rawType === 'followers' ? 'followers' : rawType;

  if (!clientId) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  if (scrapeType !== 'followers' && scrapeType !== 'following') {
    return res.status(400).json({
      ok: false,
      error: 'Only follower and following scraping are supported right now.',
      code: 'scrape_type_not_supported',
    });
  }
  if (!target_username) {
    return res
      .status(400)
      .json({ ok: false, error: 'target_username is required for follower or following scrape' });
  }

  try {
    // Scraping may run whenever nothing can actively send right now.
    const activelySendableNow = await canClientActivelySendNow(clientId).catch(() => false);
    if (activelySendableNow) {
      return res.status(400).json({
        ok: false,
        code: 'campaigns_active',
        error: 'Scraping is blocked while a campaign can actively send right now. Pause it or wait until it is out of schedule / capped.',
      });
    }

    const latestSentAtIso = await getLatestSuccessfulColdDmSentAt(clientId).catch(() => null);
    if (latestSentAtIso) {
      const latestSentAtMs = new Date(latestSentAtIso).getTime();
      const cooldownRemainingMs = latestSentAtMs + SEND_SCRAPE_COOLDOWN_MS - Date.now();
      if (Number.isFinite(cooldownRemainingMs) && cooldownRemainingMs > 0) {
        return res.status(400).json({
          ok: false,
          code: 'recent_send_cooldown',
          error: `Account safety cooldown active after sending. Try scraping again in ${formatDurationShort(cooldownRemainingMs)}.`,
          cooldownUntil: new Date(latestSentAtMs + SEND_SCRAPE_COOLDOWN_MS).toISOString(),
        });
      }
    }

    // Bind scrape job to the client's most-recent IG session (client view: only one account).
    const sessionRow = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
    if (!sessionRow?.id) {
      return res.status(400).json({
        ok: false,
        code: 'missing_instagram_session',
        error: 'Connect your Instagram automation session first (Settings → Integrations).',
      });
    }
    if (sessionRow.scrape_cooldown_until) {
      const untilMs = new Date(sessionRow.scrape_cooldown_until).getTime();
      if (Number.isFinite(untilMs) && untilMs > Date.now()) {
        const waitMs = untilMs - Date.now();
        return res.status(400).json({
          ok: false,
          code: 'scrape_cooldown',
          error: `Scraping cooldown active for account safety. Try again in ${formatDurationShort(waitMs)}.`,
          scrapeCooldownUntil: sessionRow.scrape_cooldown_until,
        });
      }
    }
    if (sessionRow.leased_until) {
      const leaseUntilMs = new Date(sessionRow.leased_until).getTime();
      if (Number.isFinite(leaseUntilMs) && leaseUntilMs > Date.now()) {
        return res.status(409).json({
          ok: false,
          code: 'instagram_session_busy',
          error:
            'This Instagram session is busy sending follow-ups or messages right now. Try the scrape again in a few minutes.',
          leasedUntil: sessionRow.leased_until,
        });
      }
    }

    const quota = await getScrapeQuotaStatus(clientId);
    if (quota.remaining <= 0) {
      return res.status(400).json({
        ok: false,
        error: quota.message,
        scrapeQuota: quota,
      });
    }
    const targetForJob = target_username.trim().replace(/^@/, '');
    const requestedMaxLeads = max_leads != null && max_leads > 0 ? max_leads : null;
    const boundedMax = requestedMaxLeads != null && requestedMaxLeads > 0 ? Math.min(requestedMaxLeads, quota.remaining) : quota.remaining;
    const effectiveMaxLeads = boundedMax > 0 ? boundedMax : null;

    // Final cooldown re-check right before queueing — closes the race window where a DM
    // finished (and set scrape_cooldown_until on the session) between our earlier read and
    // this call. If still in cooldown, refuse loudly so the UI can toast.
    const freshSessionRow = await getMostRecentInstagramSessionForClient(clientId).catch(() => null);
    if (freshSessionRow?.scrape_cooldown_until) {
      const untilMs = new Date(freshSessionRow.scrape_cooldown_until).getTime();
      if (Number.isFinite(untilMs) && untilMs > Date.now()) {
        const waitMs = untilMs - Date.now();
        return res.status(400).json({
          ok: false,
          code: 'scrape_cooldown',
          error: `Scraping cooldown active for account safety. Try again in ${formatDurationShort(waitMs)}.`,
          scrapeCooldownUntil: freshSessionRow.scrape_cooldown_until,
        });
      }
    }
    const freshLatestSentAtIso = await getLatestSuccessfulColdDmSentAt(clientId).catch(() => null);
    if (freshLatestSentAtIso) {
      const freshLatestSentAtMs = new Date(freshLatestSentAtIso).getTime();
      const freshCooldownRemainingMs = freshLatestSentAtMs + SEND_SCRAPE_COOLDOWN_MS - Date.now();
      if (Number.isFinite(freshCooldownRemainingMs) && freshCooldownRemainingMs > 0) {
        return res.status(400).json({
          ok: false,
          code: 'recent_send_cooldown',
          error: `Account safety cooldown active after sending. Try scraping again in ${formatDurationShort(freshCooldownRemainingMs)}.`,
          cooldownUntil: new Date(freshLatestSentAtMs + SEND_SCRAPE_COOLDOWN_MS).toISOString(),
        });
      }
    }

    const jobId = await createScrapeJob(
      clientId,
      targetForJob,
      lead_group_id || null,
      scrapeType,
      null,
      null, // legacy platformScraperSessionId (unused for per-client puppeteer scrape)
      (freshSessionRow && freshSessionRow.id) || sessionRow.id,
      effectiveMaxLeads != null && effectiveMaxLeads > 0 ? effectiveMaxLeads : null,
      'puppeteer'
    );
    res.json({
      ok: true,
      jobId,
      mode: 'queued',
      deferred: true,
      scrapeQuota: {
        used: quota.used,
        remaining: quota.remaining,
        limit: quota.limit,
      },
      hint: 'Scrape worker will be started automatically if needed.',
    });
    const ensureScrapeWorker = PER_CLIENT_PM2_WORKERS_ENABLED
      ? ensureClientWorkerStack(clientId)
      : ensureScrapeWorkerProcess();
    ensureScrapeWorker
      .then((r) => {
        if (!r.ok) {
          const detail = String(r.out || r.err?.message || 'pm2 ensure failed').slice(0, 220);
          console.error('[API] pm2 ensure scrape worker failed', r.err || detail);
          return;
        }
        if (r.action !== 'noop_online') {
          console.log(`[API] pm2 ensure scrape worker (${r.action || 'client_stack'}) done.`);
        }
      })
      .catch((err) => console.error('[API] pm2 ensure scrape worker failed', err));
  } catch (e) {
    console.error('[API] Scraper start error', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/scraper/stop', async (req, res) => {
  const { clientId, jobId } = req.body || {};
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!clientId) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const cancelled = await cancelScrapeJob(clientId, jobId || null);
    res.json({ ok: true, cancelled });
  } catch (e) {
    console.error('[API] Scraper stop error', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/control/stop', (req, res) => {
  const clientId = req.body?.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (isSupabaseConfigured()) {
    if (!clientId) return res.status(400).json({ ok: false, error: 'clientId required when using Supabase' });
    (async () => {
      await setControlSupabase(clientId, 1);
      const [sessionRelease, campaignRelease] = await Promise.all([
        releaseAllInstagramSessionLeases(clientId),
        releaseAllCampaignSendLeases(null, clientId),
      ]);
      if (sessionRelease.released > 0) {
        console.log(`[API] Cleared ${sessionRelease.released} Instagram session lease(s) after stop for client=${clientId}`);
      }
      if (campaignRelease.released > 0) {
        console.log(`[API] Cleared ${campaignRelease.released} campaign send lease(s) after stop for client=${clientId}`);
      }
      const activeClientIds = await getClientIdsWithPauseZero();
      const shouldStopSharedWorkers = activeClientIds.length === 0;
      console.log(
        `[API] Stop (pause=1) for clientId=${clientId}; active_clients_after_stop=${activeClientIds.length}; pm2_stop=${shouldStopSharedWorkers}`
      );
      res.json({ ok: true, processRunning: !shouldStopSharedWorkers });
      if (shouldStopSharedWorkers) {
        exec(`pm2 stop ${BOT_PM2_NAME}`, () => {});
      }
    })().catch((e) => {
      console.error('[API] stop failed', e);
      res.status(500).json({ ok: false, error: e.message || 'Failed to stop sending for client' });
    });
    return;
  }
  setControl('pause', '1');
  console.log('[API] Stop bot requested (legacy)');
  res.json({ ok: true, processRunning: false });
  exec(`pm2 stop ${BOT_PM2_NAME}`, () => {});
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard at http://0.0.0.0:${PORT}`);
  appendDashboardAudit('dashboard_listen', {
    port: PORT,
    processScheduledResponsesFallbackEnabled: PROCESS_SCHEDULED_RESPONSES_FALLBACK_ENABLED,
    autoScaleSendWorkers: shouldAutoScaleSendWorkers(),
    perClientPm2WorkersEnabled: PER_CLIENT_PM2_WORKERS_ENABLED,
    autoEnsureClientWorkersOnDashboardStart: AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START,
    legacySharedSendWorkerEnabled: LEGACY_SHARED_SEND_WORKER_ENABLED,
    remoteUpdateCanRestartDashboard: true,
  });
  schedulePoolWorkerReadyRegistration();
  if (AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START) {
    setTimeout(() => {
      ensureAssignedClientWorkerStacksOnStartup().catch((err) => {
        console.error('[pm2:auto-ensure] assigned client worker stack failed on startup', err);
      });
    }, 1500);
  } else {
    appendDashboardAudit('startup_worker_auto_ensure_skipped', {
      reason: 'disabled_by_default',
      enableWith: 'COLD_DM_AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START=1',
    });
  }
  if (shouldAutoScaleSendWorkers()) {
    const min = SCALE_SEND_WORKERS_AUTO_INTERVAL_MS / 60000;
    console.log(
      `[scale-send-workers] auto: every ${min}m + after Start (set SCALE_SEND_WORKERS_AUTO=0 to disable)`
    );
    setInterval(() => runAutoScaleSendWorkersTick('interval'), SCALE_SEND_WORKERS_AUTO_INTERVAL_MS);
    setTimeout(() => runAutoScaleSendWorkersTick('startup'), 60_000);
  }
  if (PROCESS_SCHEDULED_RESPONSES_FALLBACK_ENABLED) {
    const min = Math.round(PROCESS_SCHEDULED_RESPONSES_FALLBACK_INTERVAL_MS / 1000);
    console.log(
      `[process-scheduled-responses:fallback] enabled: every ${min}s from VPS dashboard process (backup for missing/broken Supabase cron)`
    );
    setInterval(
      () => triggerProcessScheduledResponsesFallback('interval'),
      PROCESS_SCHEDULED_RESPONSES_FALLBACK_INTERVAL_MS
    );
    setTimeout(() => triggerProcessScheduledResponsesFallback('startup'), 20_000);
  }
});
