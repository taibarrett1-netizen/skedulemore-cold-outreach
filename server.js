require('dotenv').config();
const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
const fs = require('fs');
const { exec, spawn } = require('child_process');
const multer = require('multer');
const { rateLimit, ipKeyGenerator } = require('express-rate-limit');
const { getDailyStats, getRecentSent, getControl, setControl, alreadySent, clearFailedAttempts } = require('./database/db');
const {
  isSupabaseConfigured,
  getClientId,
  setClientId,
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
  getScraperSession,
  saveScraperSession,
  getLatestScrapeJob,
  getScrapeQuotaStatus,
  createScrapeJob,
  cancelScrapeJob,
  savePlatformScraperSession,
  addCampaignLeadsFromGroups,
  syncSendJobsForClient,
  getNoWorkHint,
  getCampaignsMissingSendDelays,
  getSessionsForCampaign,
  reactivateCampaignsWithPendingLeads,
  tryVpsIdempotencyOnce,
  getOrResolveColdDmProxyUrl,
  releaseAllInstagramSessionLeases,
  releaseAllCampaignSendLeases,
  getClientIdsWithPauseZero,
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
const { connectScraper } = require('./scraper');
const { MESSAGES } = require('./config/messages');
const logger = require('./utils/logger');

const app = express();
const PORT = process.env.DASHBOARD_PORT || 3000;
const projectRoot = path.join(__dirname);
const envPath = path.join(projectRoot, '.env');
const leadsPath = path.join(projectRoot, process.env.LEADS_CSV || 'leads.csv');
const voiceNotesDir = path.join(projectRoot, 'voice-notes');
const followUpScreenshotsDir = path.join(projectRoot, 'follow-up-screenshots');
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

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

app.use('/api', (req, res, next) => {
  if (req.path === '/health') return next();
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

const { registerAdminLabRoutes } = require('./admin_lab/http');
const { runScaleSendWorkers } = require('./lib/scaleSendWorkers');
registerAdminLabRoutes(app);

const upload = multer({ dest: projectRoot, limits: { fileSize: 1024 * 1024 } });
const uploadVoice = multer({ dest: voiceNotesDir, limits: { fileSize: 25 * 1024 * 1024 } });

const BOT_PM2_NAME = 'ig-dm-send';
const SEND_WORKER_ENTRY = process.env.SEND_WORKER_ENTRY || 'workers/send-worker.js';
const SCRAPER_SESSION_LEASE_SEC = Math.max(60, parseInt(process.env.SCRAPER_SESSION_LEASE_SEC || '240', 10) || 240);

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

function getBotProcessRunning(cb) {
  exec('pm2 jlist', { maxBuffer: 1024 * 1024 }, (err, stdout) => {
    if (err) return cb(false);
    try {
      const list = JSON.parse(stdout);
      const proc = list.find((p) => p.name === BOT_PM2_NAME);
      cb(proc && proc.pm2_env && proc.pm2_env.status === 'online');
    } catch (e) {
      cb(false);
    }
  });
}

function execPm2(command) {
  return new Promise((resolve) => {
    exec(command, { cwd: projectRoot }, (err, stdout, stderr) => {
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

/**
 * Matches ecosystem.config.cjs `ig-dm-send` instance count. Dashboard `pm2 start` must use `-i N`
 * (cluster mode) so each process gets NODE_APP_INSTANCE and pins to distinct campaign queues.
 */
function sendWorkerPm2ClusterInstances() {
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

const STATUS_TIMEOUT_MS = 8000; // respond before typical Edge Function timeouts (~10–15s); status uses fast queries

// --- API: health (for proxy/dashboard connectivity check; no DB or pm2) ---
app.get('/api/health', (req, res) => {
  res.json({ ok: true });
});

// --- API: status & stats ---
// Returns immediately using only fast queries and stored status (set by the sender loop). No schedule recomputation.
app.get('/api/status', (req, res) => {
  const clientId = resolveRequestedClientId(req);
  if (req.authClientId && clientId && req.authClientId !== clientId) {
    return res.status(403).json({ error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const useSupabase = isSupabaseConfigured() && clientId;
  let responded = false;
  const send = (status, body) => {
    if (responded) return;
    responded = true;
    clearTimeout(timer);
    res.status(status).json(body);
  };
  const timer = setTimeout(() => {
    if (responded) return;
    console.warn('[API] /api/status timeout – responding 503');
    send(503, {
      error: 'Status request timed out. Try again.',
      processRunning: false,
      statusMessage: null,
      todaySent: 0,
      todayFailed: 0,
      leadsTotal: 0,
      leadsRemaining: 0,
    });
  }, STATUS_TIMEOUT_MS);

  const processRunningPromise = new Promise((resolve) => getBotProcessRunning(resolve));

  (async () => {
    try {
      if (useSupabase) {
        const [processRunningPm2, stats, statusMessage, leadsCounts, pauseFlag] = await Promise.all([
          processRunningPromise,
          getDailyStatsSupabase(clientId),
          getClientStatusMessageSupabase(clientId),
          getLeadsTotalAndRemaining(clientId),
          getControlSupabase(clientId),
        ]);
        const paused = pauseFlag === '1' || pauseFlag === 1;
        const processRunning = processRunningPm2 && !paused;
        send(200, {
          processRunning,
          statusMessage: statusMessage ?? (processRunning ? null : 'Stopped'),
          todaySent: stats.total_sent,
          todayFailed: stats.total_failed,
          leadsTotal: leadsCounts.total,
          leadsRemaining: leadsCounts.remaining,
        });
      } else {
        const processRunning = await processRunningPromise;
        const stats = getDailyStats();
        loadLeadsFromCSV(leadsPath)
          .then((leads) => {
            const leadsRemaining = leads.filter((u) => !alreadySent(u)).length;
            send(200, {
              processRunning,
              todaySent: stats.total_sent,
              todayFailed: stats.total_failed,
              leadsTotal: leads.length,
              leadsRemaining,
            });
          })
          .catch(() => {
            send(200, {
              processRunning,
              todaySent: stats.total_sent,
              todayFailed: stats.total_failed,
              leadsTotal: 0,
              leadsRemaining: 0,
            });
          });
      }
    } catch (e) {
      console.error('[API] Status error', e);
      send(500, { error: e.message });
    }
  })();
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
    if (m) out[m[1].trim()] = m[2].trim();
  }
  return out;
}

function writeEnv(obj) {
  const lines = [];
  for (const key of ENV_KEYS) {
    if (obj[key] !== undefined && obj[key] !== '') {
      lines.push(`${key}=${String(obj[key]).trim()}`);
    }
  }
  fs.writeFileSync(envPath, lines.join('\n') + '\n', 'utf8');
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
    if (body[key] !== undefined) {
      if (key === 'INSTAGRAM_PASSWORD' && body[key] === '********') continue;
      env[key] = body[key];
    }
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
    return res.status(status).json({ ok: false, error: result.error || 'Send failed' });
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
  const { username, password, clientId, platformScraperPool, platformScraperBackup } = req.body || {};
  const isPlatformPool = platformScraperPool === true || platformScraperPool === 'true' || platformScraperPool === 1;
  const isPlatformBackup = platformScraperBackup === true || platformScraperBackup === 'true' || platformScraperBackup === 1;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  if (!username || !password || !clientId) {
    return res.status(400).json({ ok: false, error: 'username, password, and clientId are required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const isAdmin = await isAdminUser(clientId).catch(() => false);
    // Platform pool uses a shared client_id with many cold_dm_instagram_sessions + platform rows (primary + backup).
    // isAdminUser(clientId) is keyed by user id, not client id — do not block pool connects after the first session.
    if (!isAdmin && !isPlatformPool) {
      const activeCount = await countActiveVpsInstagramSessions(clientId).catch(() => 0);
      if (activeCount > 0) {
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    const igKey = String(username)
      .trim()
      .replace(/^@/, '')
      .toLowerCase();
    let proxyMeta = { proxyUrl: null, proxyAssignmentId: null };
    try {
      proxyMeta = await getOrResolveColdDmProxyUrl(clientId, igKey);
    } catch (pe) {
      return res.status(503).json({
        ok: false,
        error: pe instanceof Error ? pe.message : String(pe) || 'Could not allocate proxy (check Decodo API and credits)',
      });
    }
    const result = await connectInstagram(username, password, null, { proxyUrl: proxyMeta.proxyUrl });
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
        platformScraperPool: isPlatformPool,
        platformScraperBackup: isPlatformBackup,
      });
      return res.status(200).json({
        ok: false,
        code: 'two_factor_required',
        message: 'Enter the 6-digit code from your app or WhatsApp.',
        pending2FAId: pendingId,
      });
    }
    if (result.emailVerificationRequired) {
      if (result.browser) result.browser.close().catch(() => {});
      return res.status(400).json({
        ok: false,
        code: 'two_factor_required_for_connect',
        error:
          'Two-factor authentication is required for connect. This account prompted email verification instead of app/WhatsApp 2FA. Enable 2FA in Instagram Security settings, then reconnect.',
      });
    }
    // Connect succeeded without requiring a challenge (e.g. valid existing browser session/cookies).
    await saveSession(clientId, { cookies: result.cookies }, result.username, {
      proxyUrl: proxyMeta.proxyUrl,
      proxyAssignmentId: proxyMeta.proxyAssignmentId,
    });
    if (isPlatformPool) {
      await savePlatformScraperSession(
        { cookies: result.cookies },
        result.username,
        req.body?.daily_actions_limit != null ? req.body.daily_actions_limit : 500,
        { forceInsert: isPlatformBackup }
      );
    }
    return res.status(200).json({
      ok: true,
      username: result.username,
      message: 'Connected. Existing Instagram session was restored.',
    });
  } catch (e) {
    console.error('[API] Instagram connect failed', e);
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
  }
});

app.post('/api/instagram/connect/2fa', connectLimiter, async (req, res) => {
  const { pending2FAId, twoFactorCode, clientId, platformScraperPool, platformScraperBackup, daily_actions_limit } = req.body || {};
  const isPlatformPool = platformScraperPool === true || platformScraperPool === 'true' || platformScraperPool === 1;
  const isPlatformBackup = platformScraperBackup === true || platformScraperBackup === 'true' || platformScraperBackup === 1;
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
    const poolConnect = isPlatformPool || pending.platformScraperPool === true;
    if (!isAdmin && !poolConnect) {
      const activeCount = await countActiveVpsInstagramSessions(clientId).catch(() => 0);
      if (activeCount > 0) {
        if (pending.browser) pending.browser.close().catch(() => {});
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    await saveSession(clientId, { cookies: result.cookies }, result.username, {
      proxyUrl: pending.proxyUrl,
      proxyAssignmentId: pending.proxyAssignmentId,
    });
    if (isPlatformPool || pending.platformScraperPool) {
      await savePlatformScraperSession(
        { cookies: result.cookies },
        result.username,
        daily_actions_limit != null ? daily_actions_limit : 500,
        { forceInsert: isPlatformBackup || pending.platformScraperBackup === true }
      ).catch(() => {});
      res.json({ ok: true, cookies: result.cookies, username: result.username, instagram_username: result.username });
      return;
    }
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram 2FA complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || '2FA failed' });
  }
});

app.post('/api/instagram/connect/email-code', connectLimiter, async (req, res) => {
  const { pendingEmailId, emailCode, clientId, platformScraperPool, platformScraperBackup, daily_actions_limit } = req.body || {};
  const isPlatformPool = platformScraperPool === true || platformScraperPool === 'true' || platformScraperPool === 1;
  const isPlatformBackup = platformScraperBackup === true || platformScraperBackup === 'true' || platformScraperBackup === 1;
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
  if (!!pending.platformScraperPool !== !!isPlatformPool) {
    return res.status(403).json({ ok: false, error: 'Forbidden: pending verification session type mismatch' });
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
    const poolConnectEmail = isPlatformPool || pending.platformScraperPool === true;
    if (!isAdmin && !poolConnectEmail) {
      const activeCount = await countActiveVpsInstagramSessions(clientId).catch(() => 0);
      if (activeCount > 0) {
        if (pending.browser) pending.browser.close().catch(() => {});
        return res.status(400).json({
          ok: false,
          error: 'Only one automation Instagram account is allowed for this account. Remove the current session before connecting another.',
        });
      }
    }
    await saveSession(clientId, { cookies: result.cookies }, result.username, {
      proxyUrl: pending.proxyUrl,
      proxyAssignmentId: pending.proxyAssignmentId,
    });
    if (isPlatformPool) {
      await savePlatformScraperSession(
        { cookies: result.cookies },
        result.username,
        daily_actions_limit || 500,
        { forceInsert: isPlatformBackup || pending.platformScraperBackup === true }
      ).catch(() => {});
      return res.json({ ok: true, cookies: result.cookies, username: result.username, instagram_username: result.username });
    }
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram email verification complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || 'Email verification failed' });
  }
});

// --- API: bot control (PM2 start/stop) ---
// Return 200 immediately; pm2 start/stop runs in background. Sender loop does the actual wait (schedule, limits).
app.post('/api/control/start', async (req, res) => {
  const clientId = req.body?.clientId;
  if (req.authClientId && clientId && String(clientId) !== req.authClientId) {
    return res.status(403).json({ ok: false, error: 'Forbidden: clientId mismatch for provided API key' });
  }
  const campaignId = req.body?.campaignId || null;
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
    await syncSendJobsForClient(clientId, campaignId || null).catch((e) => {
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
    console.log('[API] Start (pause=0) for clientId=', clientId);
    res.json({ ok: true, processRunning: true });
    ensureSendWorkerProcess()
      .then((r) => {
        if (!r.ok) {
          const detail = String(r.out || r.err?.message || 'pm2 ensure failed').slice(0, 220);
          console.error('[API] pm2 ensure send worker failed', r.err || detail);
          setClientStatusMessage(clientId, `Send worker did not start: ${detail}`).catch(() => {});
          return;
        }
        if (r.out) console.log(`[API] pm2 ensure send worker (${r.action}):`, r.out.slice(0, 800));
        else console.log(`[API] pm2 ensure send worker (${r.action}) done.`);
        scheduleAutoScaleSendWorkers('after_start');
      })
      .catch((err) => {
        console.error('[API] pm2 ensure send worker failed', err);
        const detail = String(err?.message || 'pm2 ensure failed').slice(0, 220);
        setClientStatusMessage(clientId, `Send worker did not start: ${detail}`).catch(() => {});
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
    const queued = await syncSendJobsForClient(clientId, campaignId).catch(() => 0);
    return res.json({ ok: true, added, queued_send_jobs: queued });
  } catch (e) {
    console.error('[API] add-leads-from-groups', e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Scraper account connect has moved to Admin Scraper Pool (platform accounts only). ---
app.post('/api/scraper/connect', connectLimiter, async (_req, res) => {
  return res.status(410).json({
    ok: false,
    error: 'Deprecated endpoint. Scraper accounts are now managed from Admin Panel -> Scraper Pool.',
  });
});

app.post('/api/scraper/connect/2fa', connectLimiter, async (_req, res) => {
  return res.status(410).json({
    ok: false,
    error: 'Deprecated endpoint. Scraper accounts are now managed from Admin Panel -> Scraper Pool.',
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
    const session = await getScraperSession(clientId);
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
    const session = await getScraperSession(clientId);
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
  const scrapeType =
    rawType === 'comments' ? 'comments' : rawType === 'following' ? 'following' : 'followers';

  if (!clientId) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  if ((scrapeType === 'followers' || scrapeType === 'following') && !target_username) {
    return res
      .status(400)
      .json({ ok: false, error: 'target_username is required for follower or following scrape' });
  }
  if (scrapeType === 'comments') {
    if (!post_urls || !Array.isArray(post_urls) || post_urls.length === 0) {
      return res.status(400).json({ ok: false, error: 'post_urls (array of Instagram post URLs) is required for comment scrape' });
    }
    if (post_urls.some((u) => typeof u !== 'string')) {
      return res.status(400).json({ ok: false, error: 'post_urls must be an array of strings' });
    }
  }

  try {
    const quota = await getScrapeQuotaStatus(clientId);
    if (quota.remaining <= 0) {
      return res.status(400).json({
        ok: false,
        error: quota.message,
        scrapeQuota: quota,
      });
    }
    const targetForJob =
      scrapeType === 'comments' ? '_comment_scrape' : target_username.trim().replace(/^@/, '');
    const requestedMaxLeads = max_leads != null && max_leads > 0 ? max_leads : null;
    const boundedMax = requestedMaxLeads != null && requestedMaxLeads > 0 ? Math.min(requestedMaxLeads, quota.remaining) : quota.remaining;
    const effectiveMaxLeads = boundedMax > 0 ? boundedMax : null;
    const jobId = await createScrapeJob(
      clientId,
      targetForJob,
      lead_group_id || null,
      scrapeType,
      scrapeType === 'comments' ? post_urls : null,
      null,
      effectiveMaxLeads != null && effectiveMaxLeads > 0 ? effectiveMaxLeads : null,
      'instagrapi'
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
      hint: 'Run PM2 process ig-dm-scrape (workers/scrape-worker.js) to drain scrape jobs.',
    });
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

app.post('/api/scraper/connect-platform', connectLimiter, async (req, res) => {
  const { username, password, daily_actions_limit, platformScraperBackup } = req.body || {};
  const isPlatformBackup = platformScraperBackup === true || platformScraperBackup === 'true' || platformScraperBackup === 1;
  if (!username || !password) {
    return res.status(400).json({ ok: false, error: 'username and password are required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const { connectScraper } = require('./scraper');
    const { cookies, username: instagramUsername } = await connectScraper(username, password);
    await savePlatformScraperSession(
      { cookies },
      instagramUsername,
      daily_actions_limit != null ? daily_actions_limit : 500,
      { forceInsert: isPlatformBackup }
    );
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Platform scraper connect error', e);
    res.status(500).json({ ok: false, error: e.message || 'Login failed' });
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
  if (shouldAutoScaleSendWorkers()) {
    const min = SCALE_SEND_WORKERS_AUTO_INTERVAL_MS / 60000;
    console.log(
      `[scale-send-workers] auto: every ${min}m + after Start (set SCALE_SEND_WORKERS_AUTO=0 to disable)`
    );
    setInterval(() => runAutoScaleSendWorkersTick('interval'), SCALE_SEND_WORKERS_AUTO_INTERVAL_MS);
    setTimeout(() => runAutoScaleSendWorkersTick('startup'), 60_000);
  }
});
