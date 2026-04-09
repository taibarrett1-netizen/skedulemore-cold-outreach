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
      'User-Agent': 'SkeduleMore-cold-dm/1',
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
          if (process.env.DECODO_DEBUG === '1') {
            const debugPath = u.pathname + u.search;
            const payload =
              parsed && typeof parsed === 'object'
                ? parsed
                : { _nonJson: String(data ?? '') };
            console.error('[decodoProvision] HTTP response (error)', {
              method,
              path: debugPath,
              statusCode: res.statusCode,
              headers: {
                'content-type': res.headers['content-type'],
                'content-length': res.headers['content-length'],
              },
              body: payload,
            });
            if (parsed && typeof parsed === 'object' && parsed._raw != null) {
              console.error('[decodoProvision] HTTP response (non-JSON)', String(parsed._raw));
            }
          }
          const detail =
            parsed && typeof parsed === 'object' ? JSON.stringify(parsed) : String(data || '');
          const err = new Error(`Decodo HTTP ${res.statusCode}: ${detail.slice(0, 2000)}`);
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

/** @returns {number} Total max length for compact sub-user usernames (6–64). Default 32 — avoids Decodo generic 400 on long names. */
function getCompactSubuserUsernameMaxTotal() {
  const maxTotalRaw = (process.env.DECODO_SUBUSER_USERNAME_MAX_LEN || '32').trim();
  let maxTotal = parseInt(maxTotalRaw, 10);
  if (!Number.isFinite(maxTotal)) maxTotal = 32;
  return Math.min(64, Math.max(6, maxTotal));
}

/**
 * Stable per (clientId, ig). Default `compact`: one letter prefix + hex — avoids `skm…` / `skm_…` shapes that some
 * Decodo accounts reject on POST /v2/sub-users.
 * Default max length **32** (not 64): Decodo often returns generic 400 for 64-char usernames despite docs saying ≤64.
 * Set DECODO_SUBUSER_USERNAME_MAX_LEN (6–64), DECODO_SUBUSER_USERNAME_PREFIX (1–3 letters), or legacy mode for `skm_` + 18 hex.
 */
function stableSubuserUsername(clientId, instagramUsername) {
  const ig = String(instagramUsername || '')
    .trim()
    .replace(/^@/, '')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 24);
  const digest = crypto.createHash('sha256').update(`${clientId}:${ig}`).digest('hex');
  const legacy = (process.env.DECODO_SUBUSER_USERNAME_MODE || 'compact').toLowerCase() === 'legacy';
  if (legacy) {
    return `skm_${digest.slice(0, 18)}`;
  }
  let prefix = (process.env.DECODO_SUBUSER_USERNAME_PREFIX || 'u').trim().toLowerCase();
  prefix = prefix.replace(/[^a-z]/g, '').slice(0, 3);
  if (!prefix) prefix = 'u';
  const maxTotal = getCompactSubuserUsernameMaxTotal();
  const maxTail = Math.min(digest.length, maxTotal - prefix.length);
  return `${prefix}${digest.slice(0, Math.max(0, maxTail))}`;
}

/**
 * Decodo v2: ≥12 chars; ≥1 upper, ≥1 lower, ≥1 digit; ≥1 symbol from `_~+=` only.
 * Other symbols (e.g. !) or alphanumeric-only passwords tend to yield generic 400 "Can not process request".
 * Password is passed through encodeURIComponent in the proxy URL.
 */
function randomSubuserPassword() {
  const lower = 'abcdefghijklmnopqrstuvwxyz';
  const upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const digits = '0123456789';
  const symbols = '_~+=';
  const all = lower + upper + digits + symbols;
  const len = 12 + crypto.randomInt(17);
  const chars = [];
  chars.push(upper[crypto.randomInt(upper.length)]);
  chars.push(lower[crypto.randomInt(lower.length)]);
  chars.push(digits[crypto.randomInt(digits.length)]);
  chars.push(symbols[crypto.randomInt(symbols.length)]);
  for (let i = chars.length; i < len; i++) {
    chars.push(all[crypto.randomInt(all.length)]);
  }
  for (let i = chars.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);
    [chars[i], chars[j]] = [chars[j], chars[i]];
  }
  const out = chars.join('').slice(0, 64);
  const valid =
    out.length >= 12 &&
    /[A-Z]/.test(out) &&
    /[a-z]/.test(out) &&
    /[0-9]/.test(out) &&
    /[_~+=]/.test(out);
  if (!valid) {
    const sym = symbols[crypto.randomInt(symbols.length)];
    let filler = '';
    for (let k = 0; k < 8; k++) {
      filler += all[crypto.randomInt(all.length)];
    }
    return (`Aa9${sym}${filler}`).slice(0, 64);
  }
  return out;
}

/**
 * Decodo residential gate auth: username line should start with `user-` + sub-user name (see help.decodo.com residential user:pass).
 * Set DECODO_GATE_USERNAME_PREFIX= (empty) only if your plan uses raw sub-user names without the prefix.
 */
function getDecodoGateUsernamePrefix() {
  const raw = process.env.DECODO_GATE_USERNAME_PREFIX;
  if (raw === undefined || raw === null) return 'user-';
  return String(raw).trim();
}

/** Stable alphanumeric id for sticky session (same client + IG → same egress IP while session lives). */
function stickySessionKeyForAssignment(clientId, instagramUsername) {
  const ig = String(instagramUsername || '')
    .trim()
    .replace(/^@/, '')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 24);
  return crypto.createHash('sha256').update(`${clientId}:${ig}`).digest('hex').slice(0, 12);
}

/** ISO 3166-1 alpha-2 for Decodo `-country-xx` (must come before `-session-` in username). */
function normalizeDecodoGateCountryCode(code) {
  const c = String(code || '')
    .trim()
    .toLowerCase()
    .replace(/^country-/, '');
  return /^[a-z]{2}$/.test(c) ? c : '';
}

/**
 * Fixed geo default: **UK (`gb`)** for residential (1 IG account → 1 sticky session; align exit with account locale).
 * Override: `DECODO_GATE_COUNTRY=us` (any ISO2). Disable country pin: `DECODO_GATE_COUNTRY=none`.
 */
function getDecodoGateCountryCodeEffective() {
  const raw = process.env.DECODO_GATE_COUNTRY;
  if (raw === undefined || raw === null) return 'gb';
  const t = String(raw).trim().toLowerCase();
  if (t === '' || t === 'none' || t === 'off' || t === '-' || t === 'any') return '';
  return normalizeDecodoGateCountryCode(t);
}

/** Decodo `-city-slug` (with `country` set). E.g. `london`, `new_york`. */
function getDecodoGateCitySlug() {
  const t = (process.env.DECODO_GATE_CITY || '').trim().toLowerCase().replace(/\s+/g, '_');
  return t.replace(/[^a-z0-9_]/g, '').slice(0, 64) || '';
}

function parseDecodoGateUserFromProxyUrl(proxyUrl) {
  try {
    const u = new URL(String(proxyUrl).trim());
    if (!u.username) return null;
    return decodeURIComponent(u.username);
  } catch (_) {
    return null;
  }
}

/**
 * True if stored assignment URL was built under old rules (e.g. no `-country-gb`) and must be rebuilt
 * so Decodo actually applies UK/sticky — otherwise Supabase keeps a random-world exit (e.g. Kuwait).
 */
function decodoStoredProxyUrlNeedsRefresh(clientId, instagramUsername, storedProxyUrl, providerRef) {
  if (!storedProxyUrl || typeof storedProxyUrl !== 'string') return false;
  if (providerRef && providerRef.service_type === 'shared_proxies') return false;

  const isResidential = !providerRef?.service_type || providerRef.service_type === 'residential_proxies';
  if (!isResidential) return false;

  const gateUser = parseDecodoGateUserFromProxyUrl(storedProxyUrl);
  if (!gateUser) return false;

  const wantCountry = getDecodoGateCountryCodeEffective();
  const mCountry = gateUser.match(/-country-([a-z]{2})(?=-|$)/);
  const hasCountry = mCountry ? mCountry[1] : null;
  if (wantCountry) {
    if (hasCountry !== wantCountry) return true;
  } else if (hasCountry) {
    return true;
  }

  const wantCity = getDecodoGateCitySlug();
  if (wantCountry && wantCity) {
    const mc = gateUser.match(/-city-([a-z0-9_]+)(?=-|$)/);
    const hasCity = mc ? mc[1] : null;
    if (hasCity !== wantCity) return true;
  } else if (gateUser.match(/-city-[a-z0-9_]+(?=-|$)/) && !wantCity) {
    return true;
  }

  const stickyOff = process.env.DECODO_STICKY_SESSION === '0' || process.env.DECODO_STICKY_SESSION === 'false';
  if (stickyOff) {
    if (gateUser.includes('-session-')) return true;
  } else {
    const key = stickySessionKeyForAssignment(clientId, instagramUsername);
    let stickyMins = parseInt(process.env.DECODO_STICKY_SESSION_DURATION_MINUTES || '60', 10);
    if (!Number.isFinite(stickyMins)) stickyMins = 60;
    stickyMins = Math.min(1440, Math.max(30, stickyMins));
    const needle = `-session-${key}-sessionduration-${stickyMins}`;
    if (!gateUser.includes(needle)) return true;
  }

  const pref = getDecodoGateUsernamePrefix();
  if (pref) {
    const expectedStart = pref.endsWith('-') ? pref : `${pref}-`;
    if (!gateUser.startsWith(expectedStart)) return true;
  }

  return false;
}

/**
 * @param {string} username - Decodo sub-user name (API), without gate prefix
 * @param {object} [opts]
 * @param {string} [opts.gateUsernamePrefix] - override env prefix
 * @param {string} [opts.countryCode] - optional alpha-2, inserts -country-xx before sticky params (Decodo order)
 * @param {boolean} [opts.useResidentialDefaultCountry] - when true, unset env defaults to UK (gb); when false (e.g. shared), only env/explicit country applies
 * @param {string|null} [opts.stickySessionId] - if set with stickyDurationMinutes, appends -session-{id}-sessionduration-{m}
 * @param {number|null} [opts.stickyDurationMinutes] - 1–1440
 */
function buildProxyUrlFromCredentials(username, password, opts = {}) {
  const host = (process.env.DECODO_GATE_HOST || 'gate.decodo.com').trim();
  const port = String(process.env.DECODO_GATE_PORT || '10001').trim();
  let user = String(username || '').trim();
  const prefixRaw = opts.gateUsernamePrefix !== undefined ? opts.gateUsernamePrefix : getDecodoGateUsernamePrefix();
  if (prefixRaw) {
    const pref = prefixRaw.endsWith('-') ? prefixRaw : `${prefixRaw}-`;
    if (!user.startsWith(pref)) user = `${pref}${user}`;
  }
  const explicitCc = normalizeDecodoGateCountryCode(opts.countryCode);
  const cc =
    explicitCc ||
    (opts.useResidentialDefaultCountry !== false
      ? getDecodoGateCountryCodeEffective()
      : normalizeDecodoGateCountryCode(process.env.DECODO_GATE_COUNTRY));
  if (cc) user = `${user}-country-${cc}`;
  const citySlug = opts.citySlug !== undefined ? String(opts.citySlug || '').trim() : getDecodoGateCitySlug();
  const cityNorm = citySlug.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '').slice(0, 64);
  if (cc && cityNorm) user = `${user}-city-${cityNorm}`;
  const sid = opts.stickySessionId;
  let mins = opts.stickyDurationMinutes;
  if (mins != null) mins = parseInt(String(mins), 10);
  if (sid && Number.isFinite(mins) && mins >= 1 && mins <= 1440) {
    const safe = String(sid).replace(/[^a-zA-Z0-9]/g, '').slice(0, 32) || 'skm';
    user = `${user}-session-${safe}-sessionduration-${Math.floor(mins)}`;
  }
  const u = encodeURIComponent(user);
  const p = encodeURIComponent(password);
  return `http://${u}:${p}@${host}:${port}`;
}

function unwrapArray(body) {
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.data)) return body.data;
  if (body && Array.isArray(body.results)) return body.results;
  return [];
}

/** @returns {Promise<Array<{ id?: number, username?: string }>>} */
async function listDecodoSubUsers(authHeaders, serviceType) {
  const q = new URLSearchParams({ service_type: serviceType }).toString();
  const { body } = await httpsJson('GET', `${API_BASE}/v2/sub-users?${q}`, authHeaders, null);
  return unwrapArray(body);
}

function pickSubscriptionPayload(body) {
  if (!body || typeof body !== 'object') return null;
  if (Array.isArray(body)) return body[0] || null;
  if (body.data != null) {
    const d = body.data;
    return Array.isArray(d) ? d[0] || null : d;
  }
  if (body.subscription != null && typeof body.subscription === 'object') return body.subscription;
  return body;
}

/** Normalize Decodo subscription shapes (top-level vs data[] vs camelCase). */
function extractSubscriptionFields(body) {
  const p = pickSubscriptionPayload(body);
  if (!p || typeof p !== 'object') {
    return {
      service_type: undefined,
      users_limit: undefined,
      traffic: undefined,
      traffic_limit: undefined,
      _payloadKeys: [],
    };
  }
  const st = p.service_type ?? p.serviceType ?? p.type;
  /** Residential trials use `proxy_users_limit`; older docs used `users_limit`. */
  const ul =
    p.users_limit ?? p.usersLimit ?? p.user_limit ?? p.proxy_users_limit ?? p.proxyUsersLimit ?? p.proxy_user_limit;
  const n = ul != null && ul !== '' ? parseInt(String(ul), 10) : NaN;
  const tr = p.traffic ?? p.used_traffic ?? p.usedTraffic;
  const tlim = p.traffic_limit ?? p.trafficLimit;
  const traffic = tr != null && tr !== '' ? parseFloat(String(tr)) : NaN;
  const trafficLimit = tlim != null && tlim !== '' ? parseFloat(String(tlim)) : NaN;
  return {
    service_type: st != null && st !== '' ? String(st).trim() : undefined,
    users_limit: Number.isFinite(n) ? n : undefined,
    traffic: Number.isFinite(traffic) ? traffic : undefined,
    traffic_limit: Number.isFinite(trafficLimit) ? trafficLimit : undefined,
    _payloadKeys: Object.keys(p),
  };
}

/** @returns {Promise<object>} */
async function fetchDecodoSubscription(authHeaders) {
  try {
    const { body } = await httpsJson('GET', `${API_BASE}/v2/subscriptions`, authHeaders, null);
    const base = body && typeof body === 'object' ? body : {};
    const extracted = extractSubscriptionFields(body);
    return {
      ...base,
      service_type: extracted.service_type,
      users_limit: Number.isFinite(extracted.users_limit) ? extracted.users_limit : undefined,
      traffic: extracted.traffic,
      traffic_limit: extracted.traffic_limit,
      _subscriptionPayloadKeys: extracted._payloadKeys,
    };
  } catch (e) {
    return { _fetchFailed: true, _statusCode: e.statusCode, _message: e.message || String(e) };
  }
}

/**
 * If GET /v2/subscriptions returns no service_type, infer from list: whichever service_type filter returns rows.
 */
async function inferServiceTypeFromSubUserLists(authHeaders) {
  for (const st of ['residential_proxies', 'shared_proxies']) {
    const rows = await listDecodoSubUsers(authHeaders, st);
    if (rows.length > 0) {
      const rowSt = rows[0] && rows[0].service_type;
      return rowSt ? String(rowSt) : st;
    }
  }
  return null;
}

/**
 * Explicit DECODO_SUBUSER_SERVICE_TYPE wins. Else subscription fields. Else infer from existing sub-users. Else residential.
 */
async function resolveServiceType(envOverride, subscription, authHeaders) {
  const explicit = (envOverride || '').trim();
  if (explicit) return explicit;
  if (subscription && !subscription._fetchFailed && subscription.service_type) {
    const st = String(subscription.service_type).trim();
    if (st) return st;
  }
  const inferred = await inferServiceTypeFromSubUserLists(authHeaders);
  if (inferred) return inferred;
  return 'residential_proxies';
}

async function appendDecodoDiagnostics(err, authHeaders, serviceType, subscription) {
  const parts = [];
  if (subscription && subscription._fetchFailed) {
    parts.push(`subscription=unavailable(${(subscription._message || '').slice(0, 120)})`);
  } else if (subscription) {
    parts.push(`subscription.service_type=${subscription.service_type}`);
    parts.push(`users_limit=${subscription.users_limit}`);
    parts.push(`proxy_users_limit=${subscription.proxy_users_limit != null ? subscription.proxy_users_limit : 'n/a'}`);
    if (subscription._subscriptionPayloadKeys && subscription._subscriptionPayloadKeys.length) {
      parts.push(`subscription_payload_keys=${subscription._subscriptionPayloadKeys.join(',')}`);
    }
  }
  try {
    const n = (await listDecodoSubUsers(authHeaders, serviceType)).length;
    parts.push(`sub_users_for_service_type=${n}`);
  } catch (le) {
    parts.push(`list_sub_users_failed=${(le.message || '').slice(0, 80)}`);
  }
  err.message = `${err.message} | ${parts.join('; ')}`;
}

async function putDecodoSubUserPassword(authHeaders, subUserId, password) {
  await httpsJson('PUT', `${API_BASE}/v2/sub-users/${encodeURIComponent(subUserId)}`, authHeaders, {
    password,
  });
}

/**
 * POST create; optionally retry with the other service_type when DECODO_SUBUSER_SERVICE_TYPE is unset (mis-matched type → generic 400).
 * @returns {Promise<string>} service_type that succeeded
 */
async function createDecodoSubUser(authHeaders, username, password, primaryServiceType) {
  const explicit = (process.env.DECODO_SUBUSER_SERVICE_TYPE || '').trim();
  const tryAlt =
    !explicit &&
    process.env.DECODO_TRY_ALT_SERVICE_TYPE !== '0' &&
    process.env.DECODO_TRY_ALT_SERVICE_TYPE !== 'false';
  const types = [primaryServiceType];
  if (tryAlt) {
    const other = primaryServiceType === 'residential_proxies' ? 'shared_proxies' : 'residential_proxies';
    types.push(other);
  }
  let lastErr = null;
  for (let i = 0; i < types.length; i++) {
    const st = types[i];
    if (process.env.DECODO_DEBUG === '1') {
      console.error('[decodoProvision] POST /v2/sub-users', {
        username,
        usernameLen: username.length,
        service_type: st,
        passwordLen: password.length,
      });
    }
    try {
      await httpsJson('POST', `${API_BASE}/v2/sub-users`, authHeaders, {
        username,
        password,
        service_type: st,
      });
      return st;
    } catch (e) {
      lastErr = e;
      const canRetry = e && e.statusCode === 400 && i < types.length - 1;
      if (!canRetry) throw e;
    }
  }
  throw lastErr;
}

/**
 * Create or reuse a Decodo sub-user (stable username per client+IG) and return proxy URL + provider_ref.
 * If the sub-user already exists (e.g. orphaned from a failed DB write), we rotate password via PUT.
 */
async function provisionDecodoSubuserProxy(clientId, instagramUsername) {
  const authHeaders = getDecodoAuthHeaders();
  const subUsername = stableSubuserUsername(clientId, instagramUsername);
  const subPassword = randomSubuserPassword();

  const subscription = await fetchDecodoSubscription(authHeaders);
  let serviceType = await resolveServiceType(process.env.DECODO_SUBUSER_SERVICE_TYPE, subscription, authHeaders);

  if (process.env.DECODO_DEBUG === '1') {
    console.error('[decodoProvision] subscription snapshot', {
      service_type: subscription.service_type,
      users_limit: subscription.users_limit,
      traffic: subscription.traffic,
      traffic_limit: subscription.traffic_limit,
      fetchFailed: subscription._fetchFailed,
    });
  }

  const enrichError = async (e) => {
    if (!e || e.decodoEnriched) return e;
    e.decodoEnriched = true;
    if (e.statusCode === 401) {
      const extra =
        ' Decodo expects the raw key in Authorization by default. If you still see 401, try DECODO_AUTH_SCHEME=bearer, or paste the exact header into DECODO_AUTHORIZATION. Confirm the key is the Proxy Public API key (Settings → API keys), not a scraper-only key.';
      e.message = (e.message || 'Decodo 401') + extra;
    }
    if (e.statusCode === 400) {
      e.message =
        (e.message || 'Decodo 400') +
        ' Common causes: wrong service_type (we retry alternate when env service type is unset), username length (set DECODO_SUBUSER_USERNAME_MAX_LEN), or password rules (Decodo: ≥12 chars, upper+lower+digit, and one of _ ~ + = only). With DECODO_DEBUG=1, error response bodies and POST lengths are logged.';
      await appendDecodoDiagnostics(e, authHeaders, serviceType, subscription);
    }
    return e;
  };

  const usersLimit =
    subscription && subscription.users_limit != null && subscription.users_limit !== ''
      ? parseInt(String(subscription.users_limit), 10)
      : NaN;

  const tr = subscription && subscription.traffic;
  const trLim = subscription && subscription.traffic_limit;
  if (
    subscription &&
    !subscription._fetchFailed &&
    typeof tr === 'number' &&
    Number.isFinite(tr) &&
    typeof trLim === 'number' &&
    Number.isFinite(trLim) &&
    trLim > 0 &&
    tr >= trLim
  ) {
    const err = new Error(
      'Decodo: traffic quota appears exhausted (traffic >= traffic_limit). Add bandwidth or wait for reset, then retry.'
    );
    err.statusCode = 400;
    throw await enrichError(err);
  }

  try {
    let rows = [];
    try {
      rows = await listDecodoSubUsers(authHeaders, serviceType);
    } catch (_) {
      rows = [];
    }

    const existing = rows.find((r) => r && String(r.username) === subUsername);

    if (existing && existing.id != null) {
      await putDecodoSubUserPassword(authHeaders, existing.id, subPassword);
    } else {
      if (Number.isFinite(usersLimit) && usersLimit === 0) {
        const err = new Error(
          'Decodo: subscription reports 0 proxy users allowed (proxy_users_limit / users_limit). Auto-create is blocked. Upgrade the plan, or create one user in the dashboard (Residential → Authentication → Users) and set DECODO_DISABLE_AUTO=1 with a manual proxy flow.'
        );
        err.statusCode = 400;
        throw await enrichError(err);
      }
      if (Number.isFinite(usersLimit) && usersLimit > 0 && rows.length >= usersLimit) {
        const err = new Error(
          `Decodo: sub-user limit reached (${rows.length}/${usersLimit}) for ${serviceType}. Remove a sub-user in the Decodo dashboard (Proxy → Sub-users) or upgrade the plan, then retry.`
        );
        err.statusCode = 400;
        throw await enrichError(err);
      }
      try {
        serviceType = await createDecodoSubUser(authHeaders, subUsername, subPassword, serviceType);
      } catch (postErr) {
        if (postErr && postErr.statusCode === 400) {
          let rows2 = [];
          try {
            rows2 = await listDecodoSubUsers(authHeaders, serviceType);
          } catch (_) {
            rows2 = [];
          }
          const found = rows2.find((r) => r && String(r.username) === subUsername);
          if (found && found.id != null) {
            await putDecodoSubUserPassword(authHeaders, found.id, subPassword);
          } else if (rows2.length >= 1) {
            const limitHint = new Error(
              `Decodo rejected creating sub-user "${subUsername}". This account already has ${rows2.length} sub-user(s) for ${serviceType}; many plans allow only one. In Decodo: Proxy → Sub-users → delete the user you do not need (or upgrade), then connect again. Original: ${postErr.message}`
            );
            limitHint.statusCode = 400;
            limitHint.body = postErr.body;
            throw await enrichError(limitHint);
          } else {
            throw await enrichError(postErr);
          }
        } else {
          throw await enrichError(postErr);
        }
      }
    }
  } catch (e) {
    if (e && !e.decodoEnriched) await enrichError(e);
    throw e;
  }

  const stickyOff = process.env.DECODO_STICKY_SESSION === '0' || process.env.DECODO_STICKY_SESSION === 'false';
  const useSticky = !stickyOff && serviceType === 'residential_proxies';
  let stickyMins = parseInt(process.env.DECODO_STICKY_SESSION_DURATION_MINUTES || '60', 10);
  if (!Number.isFinite(stickyMins)) stickyMins = 60;
  stickyMins = Math.min(1440, Math.max(30, stickyMins));
  const stickyKey = useSticky ? stickySessionKeyForAssignment(clientId, instagramUsername) : null;
  const proxyUrl = buildProxyUrlFromCredentials(subUsername, subPassword, {
    useResidentialDefaultCountry: serviceType === 'residential_proxies',
    stickySessionId: stickyKey,
    stickyDurationMinutes: stickyKey ? stickyMins : null,
  });
  const legacyMode = (process.env.DECODO_SUBUSER_USERNAME_MODE || 'compact').toLowerCase() === 'legacy';
  const gateCountry =
    serviceType === 'residential_proxies'
      ? getDecodoGateCountryCodeEffective()
      : normalizeDecodoGateCountryCode(process.env.DECODO_GATE_COUNTRY) || undefined;
  const gateCity = serviceType === 'residential_proxies' && gateCountry ? getDecodoGateCitySlug() || undefined : undefined;
  const providerRef = {
    decodo_subuser: subUsername,
    gate_username_prefix: getDecodoGateUsernamePrefix() || undefined,
    gate_country: gateCountry || undefined,
    gate_city: gateCity,
    sticky_session: stickyKey ? stickyKey : undefined,
    sticky_session_duration_minutes: stickyKey ? stickyMins : undefined,
    gate_host: process.env.DECODO_GATE_HOST || 'gate.decodo.com',
    gate_port: process.env.DECODO_GATE_PORT || '10001',
    service_type: serviceType,
    username_mode: (process.env.DECODO_SUBUSER_USERNAME_MODE || 'compact').toLowerCase(),
    username_max_total: legacyMode ? undefined : getCompactSubuserUsernameMaxTotal(),
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
  getCompactSubuserUsernameMaxTotal,
  buildProxyUrlFromCredentials,
  getDecodoGateUsernamePrefix,
  stickySessionKeyForAssignment,
  normalizeDecodoGateCountryCode,
  getDecodoGateCountryCodeEffective,
  getDecodoGateCitySlug,
  parseDecodoGateUserFromProxyUrl,
  decodoStoredProxyUrlNeedsRefresh,
};
