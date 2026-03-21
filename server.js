require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const { exec, spawn } = require('child_process');
const multer = require('multer');
const { getDailyStats, getRecentSent, getControl, setControl, alreadySent, clearFailedAttempts } = require('./database/db');
const {
  isSupabaseConfigured,
  getClientId,
  setClientId,
  setControl: setControlSupabase,
  getClientStatusMessage: getClientStatusMessageSupabase,
  getDailyStats: getDailyStatsSupabase,
  getRecentSent: getRecentSentSupabase,
  clearFailedAttempts: clearFailedAttemptsSupabase,
  getLeads: getLeadsSupabase,
  getLeadsTotalAndRemaining,
  saveSession,
  updateSettingsInstagramUsername,
  getScraperSession,
  saveScraperSession,
  getLatestScrapeJob,
  createScrapeJob,
  updateScrapeJob,
  cancelScrapeJob,
  pickScraperSessionForJob,
  savePlatformScraperSession,
  addCampaignLeadsFromGroups,
} = require('./database/supabase');
const { loadLeadsFromCSV, connectInstagram, completeInstagram2FA, sendFollowUp } = require('./bot');
const { connectScraper, runFollowerScrape, runCommentScrape } = require('./scraper');
const { MESSAGES } = require('./config/messages');
const logger = require('./utils/logger');

const app = express();
const PORT = process.env.DASHBOARD_PORT || 3000;
const projectRoot = path.join(__dirname);
const envPath = path.join(projectRoot, '.env');
const leadsPath = path.join(projectRoot, process.env.LEADS_CSV || 'leads.csv');
const voiceNotesDir = path.join(projectRoot, 'voice-notes');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
if (!fs.existsSync(voiceNotesDir)) fs.mkdirSync(voiceNotesDir, { recursive: true });
app.use('/voice-notes', express.static(voiceNotesDir));

// Optional API key for external clients (e.g. Lovable). Set COLD_DM_API_KEY in .env to enable.
const API_KEY = process.env.COLD_DM_API_KEY;
if (API_KEY) {
  app.use('/api', (req, res, next) => {
    const key = req.headers.authorization?.replace(/^Bearer\s+/i, '') || req.headers['x-api-key'];
    if (key !== API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
  });
}

const upload = multer({ dest: projectRoot, limits: { fileSize: 1024 * 1024 } });
const uploadVoice = multer({ dest: voiceNotesDir, limits: { fileSize: 25 * 1024 * 1024 } });

const BOT_PM2_NAME = 'ig-dm-bot';

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

const STATUS_TIMEOUT_MS = 8000; // respond before typical Edge Function timeouts (~10–15s); status uses fast queries

// --- API: health (for proxy/dashboard connectivity check; no DB or pm2) ---
app.get('/api/health', (req, res) => {
  res.json({ ok: true });
});

// --- API: status & stats ---
// Returns immediately using only fast queries and stored status (set by the sender loop). No schedule recomputation.
app.get('/api/status', (req, res) => {
  const clientId = req.query.clientId;
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
        const [processRunning, stats, statusMessage, leadsCounts] = await Promise.all([
          processRunningPromise,
          getDailyStatsSupabase(clientId),
          getClientStatusMessageSupabase(clientId),
          getLeadsTotalAndRemaining(clientId),
        ]);
        send(200, {
          processRunning,
          statusMessage: processRunning ? (statusMessage ?? null) : 'Stopped',
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
  res.json(getDailyStats());
});

app.get('/api/sent', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);
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
  'LEADS_CSV',
  'VOICE_NOTE_FILE',
  'VOICE_NOTE_MODE',
  'VOICE_NOTE_SINK',
  'VOICE_NOTE_PULSE_SOURCE',
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
 * Follow-up voice: `audioUrl` = signed HTTPS URL (e.g. Storage voice-notes); no cold_dm_campaigns / message_group columns.
 */
app.post('/api/follow-up/send', async (req, res) => {
  if (!isSupabaseConfigured()) {
    logger.warn('[API] follow-up/send 503 Supabase not configured');
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const body = req.body || {};
  const cid = (body.clientId || '').trim();
  const sid = (body.instagramSessionId || '').trim();
  const recip = (body.recipientUsername || '').trim().replace(/^@/, '');
  const correlationId = (
    req.get('x-correlation-id') ||
    req.get('x-request-id') ||
    (body.correlationId && String(body.correlationId).trim()) ||
    (body.requestId && String(body.requestId).trim()) ||
    ''
  ).trim();
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
      logger.log(
        `[API] follow-up/send response ok=true clientId=${cid || '-'} recipient=@${recip || '-'}${corrPart}`
      );
      return res.json({ ok: true });
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
// If account has 2FA, returns { ok: false, code: 'two_factor_required', pending2FAId }. Submit code to POST /api/instagram/connect/2fa with same clientId.
app.post('/api/instagram/connect', async (req, res) => {
  const { username, password, clientId } = req.body || {};
  if (!username || !password || !clientId) {
    return res.status(400).json({ ok: false, error: 'username, password, and clientId are required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const result = await connectInstagram(username, password, null);
    if (result.twoFactorRequired) {
      cleanupExpired2FA();
      const pendingId = require('crypto').randomBytes(16).toString('hex');
      pending2FAMap.set(pendingId, {
        page: result.page,
        browser: result.browser,
        username: result.username,
        clientId,
        createdAt: Date.now(),
      });
      return res.status(200).json({
        ok: false,
        code: 'two_factor_required',
        message: 'Enter the 6-digit code from your app or WhatsApp.',
        pending2FAId: pendingId,
      });
    }
    await saveSession(clientId, { cookies: result.cookies }, result.username);
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram connect failed', e);
    if (e.code === 'TWO_FACTOR_REQUIRED') {
      return res.status(200).json({ ok: false, code: 'two_factor_required', message: e.message || 'Enter the 6-digit code from your app or WhatsApp.' });
    }
    res.status(500).json({ ok: false, error: e.message || 'Login failed' });
  }
});

app.post('/api/instagram/connect/2fa', async (req, res) => {
  const { pending2FAId, twoFactorCode, clientId } = req.body || {};
  if (!pending2FAId || !twoFactorCode || !clientId) {
    return res.status(400).json({ ok: false, error: 'pending2FAId, twoFactorCode, and clientId are required' });
  }
  const pending = pending2FAMap.get(pending2FAId);
  if (!pending) {
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when the popup appears.' });
  }
  if (Date.now() - pending.createdAt > PENDING_2FA_TTL_MS) {
    pending2FAMap.delete(pending2FAId);
    if (pending.browser) pending.browser.close().catch(() => {});
    return res.status(400).json({ ok: false, error: 'Session expired. Start Connect again and enter the new code when the popup appears.' });
  }
  pending2FAMap.delete(pending2FAId);
  try {
    const result = await completeInstagram2FA(pending.page, pending.browser, twoFactorCode, pending.username);
    await saveSession(clientId, { cookies: result.cookies }, result.username);
    await updateSettingsInstagramUsername(clientId, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Instagram 2FA complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || '2FA failed' });
  }
});

// --- API: bot control (PM2 start/stop) ---
// Return 200 immediately; pm2 start/stop runs in background. Sender loop does the actual wait (schedule, limits).
app.post('/api/control/start', (req, res) => {
  const clientId = req.body?.clientId;
  if (isSupabaseConfigured()) {
    if (!clientId) return res.status(400).json({ ok: false, error: 'clientId required when using Supabase' });
    setControlSupabase(clientId, 0).catch((e) => console.error('[API] setControlSupabase', e));
    console.log('[API] Start (pause=0) for clientId=', clientId);
    res.json({ ok: true, processRunning: true });
    exec(`pm2 start cli.js --name ${BOT_PM2_NAME} --no-autorestart -- --start`, { cwd: projectRoot }, (err, stdout, stderr) => {
      const out = (stdout || '') + (stderr || '');
      const alreadyRunning = /already (running|launched)|online/i.test(out);
      if (err && !alreadyRunning) console.error('[API] pm2 start failed', err, stderr);
      else if (!alreadyRunning) console.log('[API] Worker started.');
    });
    return;
  }
  setControl('pause', '0');
  console.log('[API] Start bot requested (legacy)');
  res.json({ ok: true, processRunning: true });
  exec(`pm2 start cli.js --name ${BOT_PM2_NAME} --no-autorestart -- --start`, { cwd: projectRoot }, (err, stdout, stderr) => {
    if (err) console.error('[API] pm2 start failed', err, stderr);
    else console.log('[API] Bot start command executed.');
  });
});

app.post('/api/reset-failed', async (req, res) => {
  const clientId = req.body?.clientId;
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
  if (!campaignId || !clientId) {
    return res.status(400).json({ ok: false, error: 'campaignId and clientId are required' });
  }
  try {
    const added = await addCampaignLeadsFromGroups(clientId, campaignId);
    return res.json({ ok: true, added });
  } catch (e) {
    console.error('[API] add-leads-from-groups', e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Scraper API (same login + 2FA flow as Instagram connect) ---
app.post('/api/scraper/connect', async (req, res) => {
  const { username, password, clientId } = req.body || {};
  if (!username || !password || !clientId) {
    return res.status(400).json({ ok: false, error: 'username, password, and clientId are required' });
  }
  if (!isSupabaseConfigured()) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  try {
    const result = await connectInstagram(username, password, null);
    if (result.twoFactorRequired) {
      cleanupExpiredScraper2FA();
      const pendingId = require('crypto').randomBytes(16).toString('hex');
      pendingScraper2FAMap.set(pendingId, {
        page: result.page,
        browser: result.browser,
        username: result.username,
        clientId,
        createdAt: Date.now(),
      });
      return res.status(200).json({
        ok: false,
        code: 'two_factor_required',
        message: 'Enter the 6-digit code from your app or WhatsApp.',
        pending2FAId: pendingId,
      });
    }
    await saveScraperSession(clientId, { cookies: result.cookies }, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Scraper connect failed', e);
    if (e.code === 'TWO_FACTOR_REQUIRED') {
      return res.status(200).json({ ok: false, code: 'two_factor_required', message: e.message || 'Enter the 6-digit code from your app or WhatsApp.' });
    }
    res.status(500).json({ ok: false, error: e.message || 'Login failed' });
  }
});

app.post('/api/scraper/connect/2fa', async (req, res) => {
  const { pending2FAId, twoFactorCode, clientId } = req.body || {};
  if (!pending2FAId || !twoFactorCode || !clientId) {
    return res.status(400).json({ ok: false, error: 'pending2FAId, twoFactorCode, and clientId are required' });
  }
  const pending = pendingScraper2FAMap.get(pending2FAId);
  if (!pending) {
    return res.status(400).json({ ok: false, error: 'Session expired. Start Scraper Connect again and enter the new code when the popup appears.' });
  }
  if (Date.now() - pending.createdAt > PENDING_2FA_TTL_MS) {
    pendingScraper2FAMap.delete(pending2FAId);
    if (pending.browser) pending.browser.close().catch(() => {});
    return res.status(400).json({ ok: false, error: 'Session expired. Start Scraper Connect again and enter the new code when the popup appears.' });
  }
  pendingScraper2FAMap.delete(pending2FAId);
  try {
    const result = await completeInstagram2FA(pending.page, pending.browser, twoFactorCode, pending.username);
    await saveScraperSession(clientId, { cookies: result.cookies }, result.username);
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Scraper 2FA complete failed', e);
    if (pending.browser) pending.browser.close().catch(() => {});
    res.status(500).json({ ok: false, error: e.message || '2FA failed' });
  }
});

app.get('/api/scraper/status', async (req, res) => {
  const clientId = req.query.clientId || req.body?.clientId;
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
  const scrapeType = scrape_type === 'comments' ? 'comments' : 'followers';

  if (!clientId) {
    return res.status(400).json({ ok: false, error: 'clientId is required' });
  }
  if (scrapeType === 'followers' && !target_username) {
    return res.status(400).json({ ok: false, error: 'target_username is required for follower scrape' });
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
    const targetForJob = scrapeType === 'followers' ? target_username.trim().replace(/^@/, '') : '_comment_scrape';

    const requestedMaxLeads = max_leads != null && max_leads > 0 ? max_leads : null;
    const effectiveMaxLeads = requestedMaxLeads != null && requestedMaxLeads > 0 ? requestedMaxLeads : null;

    const jobId = await createScrapeJob(
      clientId,
      targetForJob,
      lead_group_id || null,
      scrapeType,
      scrapeType === 'comments' ? post_urls : null,
      null, // platformScraperSessionId not used in legacy Puppeteer mode
      effectiveMaxLeads != null && effectiveMaxLeads > 0 ? effectiveMaxLeads : null,
      'instagrapi' // legacy value; worker is not used when Puppeteer path is active
    );

    // Kick off legacy Puppeteer scraper in the background (do not await).
    if (scrapeType === 'followers') {
      runFollowerScrape(String(clientId), String(jobId), targetForJob, {
        maxLeads: effectiveMaxLeads,
        leadGroupId: lead_group_id || null,
      }).catch((err) => {
        console.error('[API] runFollowerScrape error', err);
      });
    } else {
      runCommentScrape(String(clientId), String(jobId), post_urls || [], {
        maxLeads: effectiveMaxLeads,
        leadGroupId: lead_group_id || null,
      }).catch((err) => {
        console.error('[API] runCommentScrape error', err);
      });
    }

    res.json({ ok: true, jobId, mode: 'puppeteer_legacy' });
  } catch (e) {
    console.error('[API] Scraper start error', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/scraper/stop', async (req, res) => {
  const { clientId, jobId } = req.body || {};
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

app.post('/api/scraper/connect-platform', async (req, res) => {
  const { username, password, daily_actions_limit } = req.body || {};
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
      daily_actions_limit != null ? daily_actions_limit : 500
    );
    res.json({ ok: true });
  } catch (e) {
    console.error('[API] Platform scraper connect error', e);
    res.status(500).json({ ok: false, error: e.message || 'Login failed' });
  }
});

app.post('/api/control/stop', (req, res) => {
  const clientId = req.body?.clientId;
  if (isSupabaseConfigured()) {
    if (!clientId) return res.status(400).json({ ok: false, error: 'clientId required when using Supabase' });
    setControlSupabase(clientId, 1).catch((e) => console.error('[API] setControlSupabase', e));
    console.log('[API] Stop (pause=1) for clientId=', clientId);
    res.json({ ok: true, processRunning: false });
    exec(`pm2 stop ${BOT_PM2_NAME}`, () => {});
    return;
  }
  setControl('pause', '1');
  console.log('[API] Stop bot requested (legacy)');
  res.json({ ok: true, processRunning: false });
  exec(`pm2 stop ${BOT_PM2_NAME}`, () => {});
});

// --- serve dashboard ---
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard at http://0.0.0.0:${PORT}`);
});
