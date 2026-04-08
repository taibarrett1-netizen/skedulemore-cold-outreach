/**
 * Decodo Public API v2: create sub-users with dashboard API key (Authorization header).
 * OpenAPI: https://help.decodo.com/reference — POST /v2/sub-users (no /v1/auth; that flow was removed).
 */
const crypto = require('crypto');
const https = require('https');

const API_BASE = (process.env.DECODO_API_BASE || 'https://api.decodo.com').replace(/\/$/, '');

function httpsJson(method, path, headers, bodyObj) {
  return new Promise((resolve, reject) => {
    const u = new URL(path.startsWith('http') ? path : `${API_BASE}${path}`);
    const body = bodyObj != null ? JSON.stringify(bodyObj) : null;
    const hdr = {
      Accept: 'application/json',
      ...headers,
    };
    if (body !== null) {
      hdr['Content-Type'] = 'application/json';
      hdr['Content-Length'] = Buffer.byteLength(body);
    } else if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
      hdr['Content-Length'] = '0';
    }
    const opts = {
      method,
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      headers: hdr,
    };
    const req = https.request(opts, (res) => {
      let data = '';
      res.on('data', (c) => {
        data += c;
      });
      res.on('end', () => {
        let parsed = null;
        try {
          parsed = data ? JSON.parse(data) : null;
        } catch {
          parsed = { _raw: data };
        }
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ status: res.statusCode, body: parsed });
        } else {
          const err = new Error(
            `Decodo HTTP ${res.statusCode}: ${typeof parsed === 'object' ? JSON.stringify(parsed).slice(0, 400) : data.slice(0, 400)}`
          );
          err.statusCode = res.statusCode;
          err.body = parsed;
          reject(err);
        }
      });
    });
    req.on('error', reject);
    if (body !== null) req.write(body);
    req.end();
  });
}

/** Trim and strip accidental surrounding quotes from .env / PM2 values */
function normalizeSecret(val) {
  let v = String(val || '').trim();
  if (
    v.length >= 2 &&
    ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'")))
  ) {
    v = v.slice(1, -1).trim();
  }
  return v;
}

/**
 * Decodo OpenAPI uses `apiKey` in header `Authorization` (not necessarily RFC Bearer).
 * Sending `Bearer <key>` often yields 401 "Invalid Api key" — default is **raw key only**.
 *
 * @returns {Record<string, string>}
 */
function getDecodoAuthHeaders() {
  const full = normalizeSecret(process.env.DECODO_AUTHORIZATION);
  if (full) {
    return { Authorization: full };
  }
  const key = normalizeSecret(process.env.DECODO_API_KEY || process.env.DECODO_API_TOKEN);
  if (!key) {
    throw new Error(
      'Decodo: set DECODO_API_KEY (dashboard → API / Public API key). Legacy POST /v1/auth was removed; see https://help.decodo.com/reference/public-api-key-authentication'
    );
  }
  const scheme = (process.env.DECODO_AUTH_SCHEME || 'raw').toLowerCase();
  if (scheme === 'raw') {
    return { Authorization: key };
  }
  if (scheme === 'bearer') {
    if (key.toLowerCase().startsWith('bearer ')) return { Authorization: key };
    return { Authorization: `Bearer ${key}` };
  }
  if (scheme === 'token') {
    return { Authorization: `Token ${key}` };
  }
  return { Authorization: key };
}

function stableSubuserUsername(clientId, instagramUsername) {
  const ig = String(instagramUsername || '')
    .trim()
    .replace(/^@/, '')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 24);
  const h = crypto.createHash('sha256').update(`${clientId}:${ig}`).digest('hex').slice(0, 18);
  return `skm_${h}`;
}

/** Decodo: 9+ chars, ≥1 upper, ≥1 digit; no @ or : */
function randomSubuserPassword() {
  const lower = 'abcdefghijklmnopqrstuvwxyz';
  const upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const digits = '0123456789';
  const safe = lower + upper + digits;
  let s = '';
  s += upper[crypto.randomInt(upper.length)];
  s += digits[crypto.randomInt(digits.length)];
  const extra = 14 + crypto.randomInt(10);
  for (let i = 0; i < extra; i++) s += safe[crypto.randomInt(safe.length)];
  const arr = s.split('');
  for (let i = arr.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr.join('').slice(0, 64);
}

function buildProxyUrlFromCredentials(username, password) {
  const host = (process.env.DECODO_GATE_HOST || 'gate.decodo.com').trim();
  const port = String(process.env.DECODO_GATE_PORT || '10001').trim();
  const u = encodeURIComponent(username);
  const p = encodeURIComponent(password);
  return `http://${u}:${p}@${host}:${port}`;
}

/**
 * Create a new Decodo sub-user and return proxy URL + provider_ref for storage.
 */
async function provisionDecodoSubuserProxy(clientId, instagramUsername) {
  const authHeaders = getDecodoAuthHeaders();
  const subUsername = stableSubuserUsername(clientId, instagramUsername);
  const subPassword = randomSubuserPassword();
  const serviceType = (process.env.DECODO_SUBUSER_SERVICE_TYPE || 'residential_proxies').trim();

  try {
    await httpsJson('POST', `${API_BASE}/v2/sub-users`, authHeaders, {
      username: subUsername,
      password: subPassword,
      service_type: serviceType,
    });
  } catch (e) {
    if (e && e.statusCode === 401) {
      const extra =
        ' Decodo expects the raw key in Authorization by default. If you still see 401, try DECODO_AUTH_SCHEME=bearer, or paste the exact header into DECODO_AUTHORIZATION. Confirm the key is the Proxy Public API key (Settings → API keys), not a scraper-only key.';
      e.message = (e.message || 'Decodo 401') + extra;
    }
    throw e;
  }

  const proxyUrl = buildProxyUrlFromCredentials(subUsername, subPassword);
  const providerRef = {
    decodo_subuser: subUsername,
    gate_host: process.env.DECODO_GATE_HOST || 'gate.decodo.com',
    gate_port: process.env.DECODO_GATE_PORT || '10001',
    service_type: serviceType,
    api: 'v2',
  };
  return { proxyUrl, providerRef };
}

function isDecodoAutoConfigured() {
  if (process.env.DECODO_DISABLE_AUTO === '1' || process.env.DECODO_DISABLE_AUTO === 'true') return false;
  const key = normalizeSecret(process.env.DECODO_API_KEY || process.env.DECODO_API_TOKEN);
  const auth = normalizeSecret(process.env.DECODO_AUTHORIZATION);
  return !!(key || auth);
}

module.exports = {
  provisionDecodoSubuserProxy,
  isDecodoAutoConfigured,
  stableSubuserUsername,
};
