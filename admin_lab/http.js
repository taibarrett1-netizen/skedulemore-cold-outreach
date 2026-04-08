/**
 * Admin Cold Outreach Lab HTTP routes (isolated from production Cold DM).
 * Mounted under /api/admin-lab/* — requires global API key + X-Admin-Lab-Secret.
 */
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { rateLimit, ipKeyGenerator } = require('express-rate-limit');
const { adminLabConnect, adminLabComplete2FA, adminLabSend } = require('./sender');

const projectRoot = path.join(__dirname, '..');
const OUT_DIR = path.join(__dirname, '.out');
const PYTHON_SCRIPT = path.join(__dirname, 'ig_public_followers_scrape.py');

const pendingAdminLab2FAMap = new Map();
const PENDING_ADMIN_LAB_2FA_TTL_MS = 10 * 60 * 1000;

/** jobId -> { status, rowCount?, error?, downloadToken?, stderrTail?, createdAt, startedAt?, finishedAt? } */
const scrapeJobs = new Map();
/** jobId -> { status, userId?, cached?, expiresAt?, error?, stderrTail?, targetUsername?, createdAt, startedAt?, finishedAt? } */
const resolveJobs = new Map();
const labDownloadTokens = new Map(); // token -> { filePath, expiresAt }

let adminLabSenderBusy = false;
let adminLabScrapeRunning = false;
let adminLabResolveBusy = false;

/** Max time for username→user_id resolve (spawn); default 20m. */
const ADMIN_LAB_RESOLVE_SPAWN_MS = Math.min(
  Math.max(60000, parseInt(process.env.ADMIN_LAB_RESOLVE_HTTP_TIMEOUT_MS || '1200000', 10) || 1200000),
  3600000,
);

function parseAdminLabResolveStdout(stdout) {
  const s = String(stdout || '').trim();
  if (!s) return null;
  const lines = s.split(/\r?\n/).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    try {
      const j = JSON.parse(lines[i]);
      if (j && typeof j === 'object') return j;
    } catch (_) {
      /* continue */
    }
  }
  try {
    return JSON.parse(s);
  } catch (_) {
    return null;
  }
}

const adminLabConnectLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: Math.max(5, parseInt(process.env.ADMIN_LAB_CONNECT_RATE_LIMIT_PER_15MIN || '20', 10) || 20),
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req, res) => `admin-lab-ig:${ipKeyGenerator(req, res)}`,
});

function cleanupExpiredAdminLab2FA() {
  const now = Date.now();
  for (const [id, data] of pendingAdminLab2FAMap.entries()) {
    if (now - data.createdAt > PENDING_ADMIN_LAB_2FA_TTL_MS) {
      pendingAdminLab2FAMap.delete(id);
      if (data.browser) data.browser.close().catch(() => {});
    }
  }
}

function cleanupExpiredDownloadTokens() {
  const now = Date.now();
  for (const [t, data] of labDownloadTokens.entries()) {
    if (data.expiresAt < now) {
      labDownloadTokens.delete(t);
      if (data.filePath && fs.existsSync(data.filePath)) {
        fs.unlink(data.filePath, () => {});
      }
    }
  }
}

function requireAdminLabSecret(req, res, next) {
  const secret = (process.env.ADMIN_LAB_SECRET || '').trim();
  if (!secret) {
    return res.status(503).json({ ok: false, error: 'ADMIN_LAB_SECRET not configured on VPS' });
  }
  const h = String(req.headers['x-admin-lab-secret'] || '').trim();
  if (h !== secret) {
    return res.status(403).json({ ok: false, error: 'Forbidden' });
  }
  next();
}

function registerAdminLabRoutes(app) {
  app.post(
    '/api/admin-lab/sender/connect',
    adminLabConnectLimiter,
    requireAdminLabSecret,
    async (req, res) => {
      const { username, password, proxyUrl, twoFactorCode } = req.body || {};
      if (!username || !password) {
        return res.status(400).json({ ok: false, error: 'username and password are required' });
      }
      if (adminLabSenderBusy) {
        return res.status(503).json({ ok: false, error: 'Another admin lab sender operation is in progress. Try again shortly.' });
      }
      adminLabSenderBusy = true;
      try {
        const result = await adminLabConnect({
          username: String(username).trim(),
          password: String(password),
          proxyUrl: typeof proxyUrl === 'string' ? proxyUrl.trim() : undefined,
          twoFactorCode: twoFactorCode != null ? String(twoFactorCode).trim() : undefined,
        });
        if (result.twoFactorRequired) {
          cleanupExpiredAdminLab2FA();
          const pendingId = crypto.randomBytes(16).toString('hex');
          pendingAdminLab2FAMap.set(pendingId, {
            page: result.page,
            browser: result.browser,
            username: result.username,
            proxyUrl: result.proxyUrl || null,
            createdAt: Date.now(),
          });
          return res.json({
            ok: false,
            code: 'two_factor_required',
            message: 'Enter the 6-digit code from your app or WhatsApp.',
            pending2FAId: pendingId,
          });
        }
        return res.json({ ok: true });
      } catch (e) {
        console.error('[admin-lab] sender connect failed', e);
        return res.status(500).json({ ok: false, error: e.message || 'Login failed' });
      } finally {
        adminLabSenderBusy = false;
      }
    },
  );

  app.post('/api/admin-lab/sender/2fa', adminLabConnectLimiter, requireAdminLabSecret, async (req, res) => {
    const { pending2FAId, twoFactorCode } = req.body || {};
    if (!pending2FAId || !twoFactorCode) {
      return res.status(400).json({ ok: false, error: 'pending2FAId and twoFactorCode are required' });
    }
    cleanupExpiredAdminLab2FA();
    const pending = pendingAdminLab2FAMap.get(pending2FAId);
    if (!pending) {
      return res.status(400).json({
        ok: false,
        error: 'Session expired. Start Connect again and enter the new code when prompted.',
      });
    }
    if (Date.now() - pending.createdAt > PENDING_ADMIN_LAB_2FA_TTL_MS) {
      pendingAdminLab2FAMap.delete(pending2FAId);
      if (pending.browser) pending.browser.close().catch(() => {});
      return res.status(400).json({
        ok: false,
        error: 'Session expired. Start Connect again and enter the new code when prompted.',
      });
    }
    pendingAdminLab2FAMap.delete(pending2FAId);
    if (adminLabSenderBusy) {
      if (pending.browser) pending.browser.close().catch(() => {});
      return res.status(503).json({ ok: false, error: 'Another admin lab sender operation is in progress.' });
    }
    adminLabSenderBusy = true;
    try {
      await adminLabComplete2FA(
        pending.page,
        pending.browser,
        twoFactorCode,
        pending.username,
        pending.proxyUrl,
      );
      return res.json({ ok: true });
    } catch (e) {
      console.error('[admin-lab] sender 2fa failed', e);
      if (pending.browser) pending.browser.close().catch(() => {});
      return res.status(500).json({ ok: false, error: e.message || '2FA failed' });
    } finally {
      adminLabSenderBusy = false;
    }
  });

  app.post('/api/admin-lab/sender/send', requireAdminLabSecret, async (req, res) => {
    const { usernames, message, targetsText } = req.body || {};
    let list = [];
    if (Array.isArray(usernames)) list = usernames;
    else if (typeof targetsText === 'string' && targetsText.trim()) {
      list = targetsText
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter(Boolean);
    }
    if (!message || typeof message !== 'string' || !message.trim()) {
      return res.status(400).json({ ok: false, error: 'message is required' });
    }
    if (!list.length) {
      return res.status(400).json({ ok: false, error: 'Provide usernames (array) or targetsText (one per line)' });
    }
    if (adminLabSenderBusy) {
      return res.status(503).json({ ok: false, error: 'Another admin lab sender operation is in progress.' });
    }
    adminLabSenderBusy = true;
    try {
      const out = await adminLabSend({ usernames: list, message: message.trim() });
      return res.json({ ok: true, ...out });
    } catch (e) {
      console.error('[admin-lab] sender send failed', e);
      return res.status(500).json({ ok: false, error: e.message || 'Send failed' });
    } finally {
      adminLabSenderBusy = false;
    }
  });

  app.post('/api/admin-lab/scrape/followers', requireAdminLabSecret, (req, res) => {
    const { targetUsername, targetUserId, maxUsers, proxyUrl } = req.body || {};
    const un = typeof targetUsername === 'string' ? targetUsername.trim().replace(/^@/, '') : '';
    if (!un) {
      return res.status(400).json({ ok: false, error: 'targetUsername is required' });
    }
    const scrapeMode = (process.env.ADMIN_LAB_SCRAPE_MODE || 'graphql').trim().toLowerCase();
    const docId = (process.env.ADMIN_LAB_IG_DOC_ID_FOLLOWERS || '').trim();
    if (!docId && scrapeMode !== 'api_v1') {
      return res.status(503).json({
        ok: false,
        error:
          'ADMIN_LAB_IG_DOC_ID_FOLLOWERS is not set (needed for graphql/auto scrape modes). For api_v1-only, set ADMIN_LAB_SCRAPE_MODE=api_v1.',
      });
    }
    const proxy =
      (typeof proxyUrl === 'string' && proxyUrl.trim()) || (process.env.ADMIN_LAB_DECODO_PROXY_URL || '').trim();
    if (!proxy) {
      return res.status(503).json({
        ok: false,
        error: 'Set ADMIN_LAB_DECODO_PROXY_URL on the VPS or pass proxyUrl in the request body.',
      });
    }
    if (adminLabScrapeRunning) {
      return res.status(503).json({
        ok: false,
        error: 'A follower scrape job is already running. Wait for it to finish.',
      });
    }

    cleanupExpiredDownloadTokens();
    try {
      fs.mkdirSync(OUT_DIR, { recursive: true });
    } catch (e) {
      return res.status(500).json({ ok: false, error: 'Could not create output directory' });
    }

    const jobId = crypto.randomBytes(12).toString('hex');
    const outFile = path.join(OUT_DIR, `followers-${jobId}.csv`);
    const max = Math.min(
      50000,
      Math.max(1, parseInt(String(maxUsers != null ? maxUsers : 500), 10) || 500),
    );

    const py = process.env.ADMIN_LAB_PYTHON || 'python3';
    const args = [PYTHON_SCRIPT, '--username', un, '--proxy', proxy, '--max_users', String(max), '--output', outFile];
    if (targetUserId != null && String(targetUserId).trim()) {
      args.push('--user_id', String(targetUserId).trim());
    }

    adminLabScrapeRunning = true;
    const startedAt = Date.now();
    scrapeJobs.set(jobId, {
      status: 'running',
      targetUsername: un,
      createdAt: startedAt,
      startedAt,
    });

    const child = spawn(py, args, {
      cwd: projectRoot,
      env: { ...process.env, ...(docId ? { ADMIN_LAB_IG_DOC_ID_FOLLOWERS: docId } : {}) },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stderrBuf = '';
    child.stderr.on('data', (d) => {
      stderrBuf += d.toString();
      if (stderrBuf.length > 12000) stderrBuf = stderrBuf.slice(-8000);
    });
    child.stdout.on('data', (d) => {
      const s = d.toString();
      stderrBuf += s;
      if (stderrBuf.length > 12000) stderrBuf = stderrBuf.slice(-8000);
    });

    child.on('error', (err) => {
      adminLabScrapeRunning = false;
      const tail = stderrBuf.slice(-2000);
      console.error('[admin-lab] scrape spawn error', jobId, err.message || err, tail ? `\nstderr: ${tail}` : '');
      scrapeJobs.set(jobId, {
        status: 'failed',
        error: err.message || String(err),
        stderrTail: tail,
        finishedAt: Date.now(),
        targetUsername: un,
        createdAt: startedAt,
        startedAt,
      });
    });

    child.on('close', (code) => {
      adminLabScrapeRunning = false;
      const finishedAt = Date.now();
      if (code !== 0) {
        const tail = stderrBuf.slice(-2000);
        console.error(
          `[admin-lab] scrape python exit ${code} job=${jobId} user=${un}`,
          tail ? `\n${tail}` : '',
        );
        scrapeJobs.set(jobId, {
          status: 'failed',
          error: `Python exited with code ${code}`,
          stderrTail: tail,
          finishedAt,
          targetUsername: un,
          createdAt: startedAt,
          startedAt,
        });
        return;
      }
      let rowCount = 0;
      try {
        if (fs.existsSync(outFile)) {
          const text = fs.readFileSync(outFile, 'utf8');
          const lines = text.split(/\r?\n/).filter((l) => l.length);
          rowCount = Math.max(0, lines.length - 1);
        }
      } catch (e) {
        console.error('[admin-lab] count rows', e);
      }

      const token = crypto.randomBytes(24).toString('hex');
      labDownloadTokens.set(token, {
        filePath: outFile,
        expiresAt: Date.now() + 60 * 60 * 1000,
      });

      let smallBase64 = null;
      try {
        if (fs.existsSync(outFile)) {
          const st = fs.statSync(outFile);
          if (st.size <= 900 * 1024) {
            smallBase64 = fs.readFileSync(outFile).toString('base64');
          }
        }
      } catch (e) {
        console.error('[admin-lab] read csv for inline', e);
      }

      scrapeJobs.set(jobId, {
        status: 'done',
        rowCount,
        downloadToken: token,
        csvBase64: smallBase64,
        stderrTail: stderrBuf.slice(-2000),
        finishedAt,
        targetUsername: un,
        createdAt: startedAt,
        startedAt,
      });
    });

    res.json({ ok: true, jobId });
  });

  /**
   * Start username→user_id resolve in the background (returns jobId immediately).
   * Long-running work must not block HTTP handlers: Supabase Edge has a short wall-clock limit, so the
   * dashboard polls GET /api/admin-lab/scrape/resolve/status instead of waiting on this POST.
   */
  app.post('/api/admin-lab/scrape/resolve', requireAdminLabSecret, (req, res) => {
    const { targetUsername, proxyUrl } = req.body || {};
    const un = typeof targetUsername === 'string' ? targetUsername.trim().replace(/^@/, '') : '';
    if (!un) {
      return res.status(400).json({ ok: false, error: 'targetUsername is required' });
    }
    const proxy =
      (typeof proxyUrl === 'string' && proxyUrl.trim()) || (process.env.ADMIN_LAB_DECODO_PROXY_URL || '').trim();
    if (!proxy) {
      return res.status(503).json({
        ok: false,
        error: 'Set ADMIN_LAB_DECODO_PROXY_URL on the VPS or pass proxyUrl in the request body.',
      });
    }
    if (adminLabResolveBusy) {
      return res.status(503).json({
        ok: false,
        error: 'A resolve operation is already running. Wait or try again.',
      });
    }

    const jobId = crypto.randomBytes(12).toString('hex');
    const startedAt = Date.now();
    const py = process.env.ADMIN_LAB_PYTHON || 'python3';
    const args = [PYTHON_SCRIPT, '--resolve_only', '--username', un, '--proxy', proxy];

    resolveJobs.set(jobId, {
      status: 'running',
      targetUsername: un,
      createdAt: startedAt,
      startedAt,
    });
    adminLabResolveBusy = true;

    const child = spawn(py, args, {
      cwd: projectRoot,
      env: { ...process.env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdoutBuf = '';
    let stderrBuf = '';
    child.stdout.on('data', (d) => {
      stdoutBuf += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderrBuf += d.toString();
      if (stderrBuf.length > 12000) stderrBuf = stderrBuf.slice(-8000);
    });

    const killTimer = setTimeout(() => {
      try {
        child.kill('SIGTERM');
      } catch (_) {
        /* ignore */
      }
    }, ADMIN_LAB_RESOLVE_SPAWN_MS);

    child.on('error', (err) => {
      clearTimeout(killTimer);
      adminLabResolveBusy = false;
      console.error('[admin-lab] resolve spawn error', jobId, err);
      resolveJobs.set(jobId, {
        status: 'failed',
        error: err.message || String(err),
        stderrTail: stderrBuf.slice(-2000),
        finishedAt: Date.now(),
        targetUsername: un,
        createdAt: startedAt,
        startedAt,
      });
    });

    child.on('close', (code) => {
      clearTimeout(killTimer);
      adminLabResolveBusy = false;
      const finishedAt = Date.now();
      const parsed = parseAdminLabResolveStdout(stdoutBuf);
      if (parsed && parsed.ok && parsed.userId) {
        resolveJobs.set(jobId, {
          status: 'done',
          userId: String(parsed.userId),
          cached: !!parsed.cached,
          expiresAt: typeof parsed.expiresAt === 'string' ? parsed.expiresAt : undefined,
          stderrTail: stderrBuf.slice(-2000),
          finishedAt,
          targetUsername: un,
          createdAt: startedAt,
          startedAt,
        });
        return;
      }
      const errMsg =
        (parsed && parsed.error) ||
        stderrBuf.slice(-1500) ||
        `Python exited with code ${code}`;
      if (code !== 0) {
        console.error(
          `[admin-lab] resolve python exit ${code} job=${jobId} user=${un}`,
          stderrBuf ? `\n${stderrBuf.slice(-2000)}` : '',
        );
      }
      resolveJobs.set(jobId, {
        status: 'failed',
        error: errMsg,
        stderrTail: stderrBuf.slice(-2000),
        finishedAt,
        targetUsername: un,
        createdAt: startedAt,
        startedAt,
      });
    });

    res.json({ ok: true, jobId });
  });

  app.get('/api/admin-lab/scrape/resolve/status', requireAdminLabSecret, (req, res) => {
    const jobId = String(req.query.jobId || '').trim();
    if (!jobId) return res.status(400).json({ ok: false, error: 'jobId query parameter required' });
    const job = resolveJobs.get(jobId);
    if (!job) return res.status(404).json({ ok: false, error: 'Unknown jobId' });
    return res.json({
      ok: true,
      status: job.status,
      userId: job.userId,
      cached: job.cached,
      expiresAt: job.expiresAt,
      error: job.error,
      stderrTail: job.stderrTail,
      targetUsername: job.targetUsername,
    });
  });

  app.get('/api/admin-lab/scrape/followers/status', requireAdminLabSecret, (req, res) => {
    const jobId = String(req.query.jobId || '').trim();
    if (!jobId) return res.status(400).json({ ok: false, error: 'jobId query parameter required' });
    const job = scrapeJobs.get(jobId);
    if (!job) return res.status(404).json({ ok: false, error: 'Unknown jobId' });
    const payload = {
      ok: true,
      status: job.status,
      rowCount: job.rowCount,
      error: job.error,
      downloadToken: job.downloadToken,
      csvBase64: job.csvBase64,
      stderrTail: job.stderrTail,
      targetUsername: job.targetUsername,
    };
    return res.json(payload);
  });

  app.post('/api/admin-lab/scrape/followers/download', requireAdminLabSecret, (req, res) => {
    cleanupExpiredDownloadTokens();
    const { token } = req.body || {};
    const t = typeof token === 'string' ? token.trim() : '';
    if (!t) return res.status(400).json({ ok: false, error: 'token is required' });
    const rec = labDownloadTokens.get(t);
    if (!rec || rec.expiresAt < Date.now()) {
      return res.status(404).json({ ok: false, error: 'Invalid or expired download token' });
    }
    if (!rec.filePath || !fs.existsSync(rec.filePath)) {
      return res.status(404).json({ ok: false, error: 'CSV file no longer available' });
    }
    try {
      const buf = fs.readFileSync(rec.filePath);
      labDownloadTokens.delete(t);
      return res.json({
        ok: true,
        filename: path.basename(rec.filePath),
        csvBase64: buf.toString('base64'),
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: e.message || 'Read failed' });
    }
  });
}

module.exports = { registerAdminLabRoutes };
