/**
 * Supabase layer for Cold DM (handoff from setter dashboard).
 * All tables use client_id (UUID). Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.
 */
const { createClient } = require('@supabase/supabase-js');
const path = require('path');
const fs = require('fs');

const decodoProvision = require('../proxy/decodoProvision');

const CLIENT_ID_FILE = path.join(process.cwd(), '.cold_dm_client_id');

let _client = null;
let _coldDmCampaignsSupportsVoiceNoteColumns = null;
let _loggedMissingColdDmCampaignVoiceColumns = false;

function noWorkDebugEnabled() {
  return process.env.NO_WORK_DEBUG === '1' || process.env.NO_WORK_DEBUG === 'true';
}

function logNoWorkDebug(message, details = null) {
  if (!noWorkDebugEnabled()) return;
  const prefix = '[no-work-debug] ';
  if (details == null) {
    console.log(prefix + message);
    return;
  }
  try {
    console.log(prefix + message + ' ' + JSON.stringify(details));
  } catch {
    console.log(prefix + message);
  }
}

function platformScraperReserveDebugEnabled() {
  return (
    process.env.PLATFORM_SCRAPER_RESERVE_DEBUG === '1' ||
    process.env.PLATFORM_SCRAPER_RESERVE_DEBUG === 'true'
  );
}

function coldDmMetricsDebugEnabled() {
  const v = String(process.env.COLD_DM_METRICS_DEBUG || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

function logColdDmMetricsDebug(message, details = null) {
  if (!coldDmMetricsDebugEnabled()) return;
  const prefix = '[cold-dm-metrics-debug] ';
  if (details == null) {
    console.log(prefix + message);
    return;
  }
  try {
    console.log(prefix + message + ' ' + JSON.stringify(details));
  } catch {
    console.log(prefix + message);
  }
}

function coldDmConcurrencyDebugEnabled() {
  const v = String(process.env.COLD_DM_CONCURRENCY_DEBUG || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

function logColdDmConcurrencyDebug(message, details = null) {
  if (!coldDmConcurrencyDebugEnabled()) return;
  const prefix = '[cold-dm-concurrency-debug] ';
  if (details == null) {
    console.log(prefix + message);
    return;
  }
  try {
    console.log(prefix + message + ' ' + JSON.stringify(details));
  } catch {
    console.log(prefix + message);
  }
}

async function withTimeout(promise, timeoutMs, timeoutMessage) {
  const ms = Math.max(1000, Number(timeoutMs) || 1000);
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(timeoutMessage || `Timed out after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

/** Logs why a compare-and-swap reserve did not return a row (always) or verbose success (when PLATFORM_SCRAPER_RESERVE_DEBUG=1). */
function logPlatformScraperReserve(message, details = null, opts = {}) {
  const { always = false } = opts;
  if (!always && !platformScraperReserveDebugEnabled()) return;
  const prefix = '[platform-scraper-reserve] ';
  if (details == null) {
    console.error(prefix + message);
    return;
  }
  try {
    console.error(prefix + message + ' ' + JSON.stringify(details));
  } catch {
    console.error(prefix + message);
  }
}

function getSupabase() {
  if (_client) return _client;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) return null;
  _client = createClient(url, key);
  return _client;
}

function isSupabaseConfigured() {
  return !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
}

function getClientId() {
  if (process.env.COLD_DM_CLIENT_ID) return process.env.COLD_DM_CLIENT_ID;
  try {
    if (fs.existsSync(CLIENT_ID_FILE)) {
      return fs.readFileSync(CLIENT_ID_FILE, 'utf8').trim();
    }
  } catch (e) {}
  return null;
}

function setClientId(clientId) {
  if (!clientId) return;
  try {
    fs.writeFileSync(CLIENT_ID_FILE, String(clientId).trim(), 'utf8');
  } catch (e) {}
}

async function isAdminUser(userId) {
  const sb = getSupabase();
  if (!sb || !userId) return false;
  try {
    const { data, error } = await sb.rpc('is_admin', { _user_id: userId });
    if (error) return false;
    return data === true;
  } catch {
    return false;
  }
}

async function countActiveVpsInstagramSessions(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const { count, error } = await sb
    .from('cold_dm_instagram_sessions')
    .select('*', { count: 'exact', head: true })
    .eq('client_id', clientId);
  if (error) throw error;
  return count ?? 0;
}

async function countActiveGraphInstagramAccounts(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const { count, error } = await sb
    .from('instagram_accounts')
    .select('*', { count: 'exact', head: true })
    .eq('client_id', clientId)
    .eq('is_active', true);
  if (error) throw error;
  return count ?? 0;
}

function getToday() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Merge campaign seconds with client cold_dm_settings minute defaults (see migrations on min_delay_minutes).
 * @param {Record<string, unknown>} campaign - cold_dm_campaigns row
 * @param {Record<string, unknown> | null} settings - cold_dm_settings row or null
 */
function computeEffectiveSendDelaySeconds(campaign, settings) {
  let minS = campaign?.min_delay_sec;
  let maxS = campaign?.max_delay_sec;
  if (minS == null && settings && settings.min_delay_minutes != null) {
    const mm = Number(settings.min_delay_minutes);
    if (Number.isFinite(mm) && mm >= 0) minS = Math.round(mm * 60);
  }
  if (maxS == null && settings && settings.max_delay_minutes != null) {
    const mm = Number(settings.max_delay_minutes);
    if (Number.isFinite(mm) && mm >= 0) maxS = Math.round(mm * 60);
  }
  return { minDelaySec: minS, maxDelaySec: maxS };
}

function hasValidResolvedSendDelays(minS, maxS) {
  if (minS == null || maxS == null) return false;
  const min = Number(minS);
  const max = Number(maxS);
  return Number.isFinite(min) && Number.isFinite(max) && min >= 1 && max >= min;
}

function hasValidCampaignSendDelayConfig(campaign, settings = null) {
  if (!campaign) return false;
  const { minDelaySec, maxDelaySec } = computeEffectiveSendDelaySeconds(campaign, settings);
  return hasValidResolvedSendDelays(minDelaySec, maxDelaySec);
}

function describeCampaignSendDelayConfigProblem(campaign, settings = null) {
  const { minDelaySec: min, maxDelaySec: max } = computeEffectiveSendDelaySeconds(campaign, settings);
  if (min == null && max == null) {
    return 'missing min/max send delay (set seconds on the campaign or min/max delay minutes in Cold DM client settings)';
  }
  if (min == null) return 'missing min send delay (campaign min_delay_sec or client min_delay_minutes)';
  if (max == null) return 'missing max send delay (campaign max_delay_sec or client max_delay_minutes)';
  if (Number(max) < Number(min)) return `max_delay (${max}) is lower than min_delay (${min})`;
  return 'invalid send delay settings';
}

/**
 * Normalize schedule time to HH:mm:ss. Handles DB TIME ("03:00:00"), ISO datetimes, fractional seconds + offsets,
 * and Date objects (some drivers map TIME to 1970-01-01 UTC wall clock).
 */
function normalizeScheduleTime(value) {
  if (value == null || value === '') return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    const h = value.getUTCHours();
    const m = value.getUTCMinutes();
    const sec = value.getUTCSeconds();
    return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`;
  }
  const s = String(value).trim();
  if (!s) return null;
  if (s.includes('T')) {
    const afterT = s.split('T')[1] || '';
    const mch = afterT.match(/^(\d{1,2}:\d{2}(?::\d{2})?)/);
    if (!mch) return null;
    const parts = mch[1].split(':');
    const hh = String(Number(parts[0])).padStart(2, '0');
    const mm = String(Number(parts[1])).padStart(2, '0');
    const ss = parts[2] != null ? String(Number(parts[2])).padStart(2, '0') : '00';
    return `${hh}:${mm}:${ss}`;
  }
  const plain = s.match(/^(\d{1,2}:\d{2}(?::\d{2})?)/);
  if (plain) {
    const parts = plain[1].split(':');
    const hh = String(Number(parts[0])).padStart(2, '0');
    const mm = String(Number(parts[1])).padStart(2, '0');
    const ss = parts[2] != null ? String(Number(parts[2])).padStart(2, '0') : '00';
    return `${hh}:${mm}:${ss}`;
  }
  return null;
}

/**
 * Parse "HH:mm" or "HH:mm:ss" into seconds since midnight.
 * Returns null if invalid.
 */
function parseClockTimeToSeconds(hhmmss) {
  if (!hhmmss) return null;
  const raw = String(hhmmss).trim();
  if (!raw) return null;
  const mch = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!mch) return null;
  const h = Number(mch[1]);
  const m = Number(mch[2]);
  const s = mch[3] != null ? Number(mch[3]) : 0;
  if (!Number.isFinite(h) || !Number.isFinite(m) || !Number.isFinite(s)) return null;
  if (h < 0 || h > 23) return null;
  if (m < 0 || m > 59) return null;
  if (s < 0 || s > 59) return null;
  return h * 3600 + m * 60 + s;
}

/**
 * Current local time in `timezone` as seconds since midnight (0..86399).
 * Uses Intl.formatToParts to avoid locale formatting edge cases.
 * Falls back to UTC wall clock if tz is missing/invalid.
 */
function getClockSecondsInTimezone(now, timezone) {
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) {
    return now.getUTCHours() * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds();
  }
  try {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
      hourCycle: 'h23',
    }).formatToParts(now);
    const hour = Number(parts.find((p) => p.type === 'hour')?.value);
    const minute = Number(parts.find((p) => p.type === 'minute')?.value);
    const second = Number(parts.find((p) => p.type === 'second')?.value);
    if (!Number.isFinite(hour) || !Number.isFinite(minute) || !Number.isFinite(second)) {
      // Fallback: try parsing the formatted string.
      const str = getClockTimeHHMMSSInTimezone(now, tz);
      const sec = parseClockTimeToSeconds(str);
      return sec == null ? now.getUTCHours() * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds() : sec;
    }
    return hour * 3600 + minute * 60 + second;
  } catch {
    return now.getUTCHours() * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds();
  }
}

/**
 * Current local time in `timezone` as "HH:mm:ss" (24h). Trims IANA ids (trailing spaces break Intl).
 * Falls back to UTC wall clock if tz is missing/invalid.
 */
function getClockTimeHHMMSSInTimezone(now, timezone) {
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) {
    return `${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
  }
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    hourCycle: 'h23',
  }).format(now);
}

/** Returns YYYY-MM-DD in the given IANA timezone; falls back to UTC if invalid/missing. */
function getTodayInTimezone(timezone) {
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) return getToday();
  try {
    return new Date().toLocaleDateString('en-CA', { timeZone: tz });
  } catch (e) {
    return getToday();
  }
}

/** Returns "today" (YYYY-MM-DD) in the client's configured timezone for daily stats/limits. */
async function getTodayForClient(clientId) {
  const settings = await getSettings(clientId);
  const tz = settings?.timezone;
  return getTodayInTimezone(tz);
}

/** Returns the UTC Date for the start of the next calendar day (midnight) in the given IANA timezone. */
function getNextMidnightInTimezone(timezone) {
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) return new Date(Date.now() + 24 * 60 * 60 * 1000);
  try {
    const now = new Date();
    const todayStr = getTodayInTimezone(tz);
    const [y, m, d] = todayStr.split('-').map(Number);
    const tomorrowDate = new Date(Date.UTC(y, m - 1, d + 1));
    const tomorrowStr = tomorrowDate.toLocaleDateString('en-CA', { timeZone: tz });
    let low = now.getTime();
    let high = now.getTime() + 48 * 60 * 60 * 1000;
    while (high - low > 60000) {
      const mid = Math.floor((low + high) / 2);
      const d2 = new Date(mid);
      const datePart = d2.toLocaleDateString('en-CA', { timeZone: tz });
      const timePart = d2.toLocaleTimeString('en-CA', { timeZone: tz, hour12: false });
      if (datePart < tomorrowStr || (datePart === tomorrowStr && timePart.slice(0, 5) < '00:00')) low = mid + 1;
      else high = mid;
    }
    return new Date(high);
  } catch (e) {
    return new Date(Date.now() + 24 * 60 * 60 * 1000);
  }
}

/** Returns the UTC Date for the start of the next hour in the given IANA timezone. */
function getNextHourStartInTimezone(timezone) {
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) {
    const d = new Date();
    return new Date(d.getTime() + (60 - d.getUTCMinutes()) * 60 * 1000 - d.getUTCSeconds() * 1000);
  }
  try {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-CA', { timeZone: tz, hour12: false });
    const dateStr = now.toLocaleDateString('en-CA', { timeZone: tz });
    const [h] = timeStr.split(':').map(Number);
    const nextHour = (h + 1) % 24;
    const targetDateStr = nextHour === 0 ? new Date(now.getTime() + 24 * 60 * 60 * 1000).toLocaleDateString('en-CA', { timeZone: tz }) : dateStr;
    const targetTime = `${String(nextHour).padStart(2, '0')}:00`;
    let low = now.getTime();
    let high = now.getTime() + 2 * 60 * 60 * 1000;
    while (high - low > 60000) {
      const mid = Math.floor((low + high) / 2);
      const d = new Date(mid);
      const dStr = d.toLocaleDateString('en-CA', { timeZone: tz });
      const tStr = d.toLocaleTimeString('en-CA', { timeZone: tz, hour12: false }).slice(0, 5);
      if (dStr < targetDateStr || (dStr === targetDateStr && tStr < targetTime)) low = mid + 1;
      else high = mid;
    }
    return new Date(high);
  } catch (e) {
    const d = new Date();
    return new Date(d.getTime() + (60 - d.getUTCMinutes()) * 60 * 1000 - d.getUTCSeconds() * 1000);
  }
}

/**
 * UTC instant when `timezone` shows calendar `dateStr` (YYYY-MM-DD) at local wall time `hm` ("HH:mm").
 */
function utcAtLocalDateAndHM(dateStr, hm, tz) {
  const [y, m, d] = dateStr.split('-').map(Number);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return new Date(NaN);
  let low = Date.UTC(y, m - 1, d - 1, 8, 0, 0);
  let high = Date.UTC(y, m - 1, d + 1, 16, 0, 0);
  while (high - low > 1000) {
    const mid = Math.floor((low + high) / 2);
    const inst = new Date(mid);
    const ds = inst.toLocaleDateString('en-CA', { timeZone: tz });
    const ts = inst.toLocaleTimeString('en-CA', { timeZone: tz, hour12: false }).slice(0, 5);
    if (ds < dateStr || (ds === dateStr && ts < hm)) low = mid + 1;
    else high = mid;
  }
  return new Date(high);
}

/** Next calendar YYYY-MM-DD after `prevYmd` in `tz` (walks forward from end of previous local day). */
function nextCalendarDateStrInTz(prevYmd, tz) {
  const late = utcAtLocalDateAndHM(prevYmd, '23:59', tz);
  let t = new Date(late.getTime() + 120_000);
  for (let i = 0; i < 96; i++) {
    const ds = t.toLocaleDateString('en-CA', { timeZone: tz });
    if (ds !== prevYmd) return ds;
    t = new Date(t.getTime() + 15 * 60 * 1000);
  }
  return null;
}

/** Returns the UTC Date for the next time the daily send window *opens* (schedule start), strictly after `now` in TZ. */
function getNextScheduleStartInTimezone(scheduleStartTime, timezone) {
  // Match isWithinSchedule: missing start in DB still means "business morning" for cold DMs, not midnight.
  const startStr = normalizeScheduleTime(scheduleStartTime) || '09:00:00';
  const [sh, sm] = startStr.split(':').map(Number);
  const startTime = `${String(sh).padStart(2, '0')}:${String(sm || 0).padStart(2, '0')}`;
  const tz = normalizeTimezoneInput(timezone);
  if (!tz) {
    const now = new Date();
    let next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), sh, sm || 0, 0));
    if (next.getTime() <= now.getTime()) next = new Date(next.getTime() + 24 * 60 * 60 * 1000);
    return next;
  }
  try {
    const now = new Date();
    const todayStr = getTodayInTimezone(tz);
    const todayOpening = utcAtLocalDateAndHM(todayStr, startTime, tz);
    if (todayOpening.getTime() > now.getTime()) return todayOpening;
    let ymd = todayStr;
    for (let k = 0; k < 14; k++) {
      ymd = nextCalendarDateStrInTz(ymd, tz);
      if (!ymd) break;
      const opening = utcAtLocalDateAndHM(ymd, startTime, tz);
      if (opening.getTime() > now.getTime()) return opening;
    }
    return null;
  } catch (e) {
    return null;
  }
}

/** cold_dm_control / UI: next send window in campaign timezone. */
function formatOutsideScheduleResumeMessage(timezone, nextStartDate, availableAtIso) {
  const tz = normalizeTimezoneInput(timezone);
  const tzLabel = tz || 'UTC';
  try {
    const d =
      nextStartDate instanceof Date && !Number.isNaN(nextStartDate.getTime())
        ? nextStartDate
        : availableAtIso
          ? new Date(availableAtIso)
          : null;
    if (d && !Number.isNaN(d.getTime())) {
      const localPart = d.toLocaleString('en-US', {
        timeZone: tzLabel,
        weekday: 'short',
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
      });
      return `Outside sending schedule — resumes ${localPart} (${tzLabel}).`;
    }
  } catch (_) {}
  const fallback = (availableAtIso || '').slice(0, 19).replace('T', ' ');
  return `Outside sending schedule — resumes at ${fallback || 'next window'} UTC.`;
}

/**
 * Explains cold_dm_send_jobs.available_at when it is in the future (queue_wait).
 * Same instant in UTC often looks like "evening" UTC while it is midnight in Europe/Berlin (CEST).
 * `jobMeta` comes from the earliest row: that stamp may be from an earlier defer (not "today's cap" right now).
 */
function formatQueueWaitDeferMessage(availableAtIso, clientTimezone, jobMeta = null) {
  const resumeAt = availableAtIso ? new Date(availableAtIso) : null;
  if (!resumeAt || Number.isNaN(resumeAt.getTime())) {
    return 'Queued send jobs are deferred (invalid available_at).';
  }
  const cls = String(jobMeta?.lastErrorClass || '').trim();
  const clsHint =
    cls === 'hourly_limit'
      ? 'Oldest job: hourly_limit — client hourly cap counts all campaigns (rolling last 60 minutes of sends).'
      : cls === 'daily_limit'
        ? 'Oldest job: daily_limit — wait was stamped when the cap was hit; it can still sit in the future after the calendar day rolls over.'
      : cls === 'outside_schedule'
        ? 'Oldest job: outside_schedule — wait matches send-window defer, not random UTC.'
        : cls === 'session_load_timeout' || cls === 'session_load_failed'
          ? `Oldest job: ${cls} — browser/session hold; other workers may have deferred the queue.`
          : cls
            ? `Oldest job last marked: ${cls}.`
            : 'Oldest job has no last_error_class (older row) — often send spacing cooldown.';

  const tz = normalizeTimezoneInput(clientTimezone);
  const utcShort = `${resumeAt.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
  if (!tz) {
    return (
      `Queued send jobs are deferred until ${utcShort}. ${clsHint} ` +
      'Set Cold DM timezone in Settings for local time. This is not the same rule as campaign send hours.'
    );
  }
  try {
    const localPart = resumeAt.toLocaleString('en-US', {
      timeZone: tz,
      weekday: 'short',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    });
    return (
      `Queued send jobs are deferred until ${localPart} (${tz}) — ${utcShort}. ${clsHint} ` +
      'Not the same rule as campaign send hours.'
    );
  } catch (_) {
    return `Queued send jobs are deferred until ${utcShort}. ${clsHint}`;
  }
}

function normalizeUsername(username) {
  const u = String(username).trim();
  return u.startsWith('@') ? u.slice(1) : u;
}

function normalizeTimezoneInput(timezone) {
  if (!timezone || typeof timezone !== 'string') return null;
  const raw = timezone.trim();
  if (!raw) return null;
  const compact = raw.replace(/\s+/g, ' ');
  const slashNormalized = compact.replace(/\s*\/\s*/g, '/');
  const aliasKey = slashNormalized.toLowerCase().replace(/\s+/g, '');
  const aliases = {
    singapore: 'Asia/Singapore',
    asiasingapore: 'Asia/Singapore',
    'asia/singapore': 'Asia/Singapore',
  };
  const candidate = aliases[aliasKey] || slashNormalized;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: candidate });
    return candidate;
  } catch {
    return null;
  }
}

function isMissingColumnError(error, expectedColumnName = '') {
  if (!error) return false;
  const code = error?.code ? String(error.code) : '';
  const msg = error?.message ? String(error.message).toLowerCase() : '';
  const col = String(expectedColumnName || '').toLowerCase();
  if (code !== '42703') return false;
  if (!col) return true;
  return msg.includes(col);
}

async function getSettings(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb
    .from('cold_dm_settings')
    .select('*')
    .eq('client_id', clientId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

/** Returns list of first names (lowercase) that should be treated as empty in message templates. */
async function getFirstNameBlocklist(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_first_name_blocklist')
    .select('first_name_lower')
    .eq('client_id', clientId);
  if (error) throw error;
  return (data || []).map((r) => (r.first_name_lower || '').toLowerCase()).filter(Boolean);
}

/** Display name on the SkeduleMore account (public.users.name) for {{sender_name}} / {{sender_first_name}} in templates. */
async function getUserAccountName(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb.from('users').select('name').eq('id', clientId).maybeSingle();
  if (error || !data) return null;
  const n = typeof data.name === 'string' ? data.name.trim() : '';
  return n || null;
}

async function getMessageTemplates(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_message_templates')
    .select('message_text')
    .eq('client_id', clientId)
    .order('sort_order', { ascending: true });
  if (error) throw error;
  return (data || []).map((r) => r.message_text).filter(Boolean);
}

async function getLeads(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_leads')
    .select('username')
    .eq('client_id', clientId);
  if (error) throw error;
  return (data || []).map((r) => normalizeUsername(r.username)).filter(Boolean);
}

/** Fast counts for status: total leads and remaining (not yet sent). Uses RPC when available; falls back to full fetch if migration 005 not run. */
async function getLeadsTotalAndRemaining(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return { total: 0, remaining: 0 };
  const { data, error } = await sb.rpc('get_cold_dm_leads_counts', { p_client_id: clientId });
  if (!error && data != null) {
    return { total: data.total ?? 0, remaining: data.remaining ?? 0 };
  }
  const [leads, sentSet] = await Promise.all([getLeads(clientId), getSentUsernames(clientId)]);
  const remaining = leads.filter((u) => !sentSet.has(u)).length;
  return { total: leads.length, remaining };
}

async function getSession(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  try {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id, leased_until, leased_by_worker, lease_heartbeat_at')
      .eq('client_id', clientId)
      .maybeSingle();
    if (error) throw error;
    return data;
  } catch (e) {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id')
      .eq('client_id', clientId)
      .maybeSingle();
    if (error) throw error;
    return data;
  }
}

/** Instagram session row for a specific id; must belong to clientId (follow-up send). */
async function getInstagramSessionByIdForClient(clientId, sessionId) {
  const sb = getSupabase();
  if (!sb || !clientId || !sessionId) return null;
  try {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, client_id, session_data, instagram_username, proxy_url, proxy_assignment_id, leased_until, leased_by_worker, lease_heartbeat_at')
      .eq('id', sessionId)
      .eq('client_id', clientId)
      .maybeSingle();
    if (error) throw error;
    return data;
  } catch (e) {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, client_id, session_data, instagram_username, proxy_url, proxy_assignment_id')
      .eq('id', sessionId)
      .eq('client_id', clientId)
      .maybeSingle();
    if (error) throw error;
    return data;
  }
}

/** All sessions for a client. Used when campaign has no assigned sessions. */
async function getSessions(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  try {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id, leased_until, leased_by_worker, lease_heartbeat_at, web_session_needs_refresh')
      .eq('client_id', clientId)
      .order('id', { ascending: true });
    if (error) throw error;
    return data || [];
  } catch (e) {
    const { data, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id, web_session_needs_refresh')
      .eq('client_id', clientId)
      .order('id', { ascending: true });
    if (error) throw error;
    return data || [];
  }
}

/**
 * Sessions to use for a campaign. If campaign has rows in cold_dm_campaign_instagram_sessions,
 * returns only those sessions; otherwise returns all client sessions.
 */
async function getSessionsForCampaign(clientId, campaignId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  if (!campaignId) return getSessions(clientId);
  try {
    const { data: assigned, error } = await sb
      .from('cold_dm_campaign_instagram_sessions')
      .select('instagram_session_id')
      .eq('campaign_id', campaignId);
    if (error || !assigned || assigned.length === 0) {
      return getSessions(clientId);
    }
    const ids = assigned.map((r) => r.instagram_session_id).filter(Boolean);
    if (ids.length === 0) return getSessions(clientId);
    try {
      const { data: sessions, error: sessErr } = await sb
        .from('cold_dm_instagram_sessions')
        .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id, leased_until, leased_by_worker, lease_heartbeat_at, web_session_needs_refresh')
        .eq('client_id', clientId)
        .in('id', ids)
        .order('id', { ascending: true });
      if (sessErr || !sessions?.length) return getSessions(clientId);
      return sessions;
    } catch (e) {
      const { data: sessions, error: sessErr } = await sb
        .from('cold_dm_instagram_sessions')
        .select('id, session_data, instagram_username, proxy_url, proxy_assignment_id, web_session_needs_refresh')
        .eq('client_id', clientId)
        .in('id', ids)
        .order('id', { ascending: true });
      if (sessErr || !sessions?.length) return getSessions(clientId);
      return sessions;
    }
  } catch (e) {
    return getSessions(clientId);
  }
}

function computeSendSessionLeaseUntil(leaseSec = 600) {
  const sec = Math.max(60, parseInt(leaseSec, 10) || 600);
  return new Date(Date.now() + sec * 1000).toISOString();
}

function computeAvailableAtIso(delaySeconds = 0) {
  const sec = Math.max(0, parseInt(delaySeconds, 10) || 0);
  return new Date(Date.now() + sec * 1000).toISOString();
}

function instagramLeaseStaleBeforeIso(leaseSeconds = 600) {
  const staleSec = Math.max(
    45,
    parseInt(process.env.INSTAGRAM_SESSION_LEASE_STALE_SEC || '120', 10) || 120
  );
  return new Date(Date.now() - staleSec * 1000).toISOString();
}

function campaignSendLeaseStaleBeforeIso() {
  const staleSec = Math.max(
    45,
    parseInt(process.env.CAMPAIGN_SEND_LEASE_STALE_SEC || '120', 10) || 120
  );
  return new Date(Date.now() - staleSec * 1000).toISOString();
}

/** Postgres undefined_column or PostgREST "column does not exist" style errors. */
function isLikelyPgMissingColumnError(error) {
  if (!error) return false;
  const code = String(error.code || '').trim();
  if (code === '42703') return true;
  const msg = String(error.message || '').toLowerCase();
  return msg.includes('does not exist') && (msg.includes('column') || msg.includes('field'));
}

/**
 * cold_dm_instagram_sessions + cold_dm_send_jobs use leased_until / leased_by_worker / lease_heartbeat_at.
 * Do not match campaign send_lease_* errors (substring "leased_until" inside "send_leased_until" was bypassing the campaign mutex).
 */
function isMissingStandardLeaseColumnsError(error) {
  if (!isLikelyPgMissingColumnError(error)) return false;
  const msg = String(error.message || '').toLowerCase();
  if (msg.includes('send_leased') || msg.includes('send_lease_heartbeat')) return false;
  return (
    msg.includes('leased_until') || msg.includes('leased_by_worker') || msg.includes('lease_heartbeat_at')
  );
}

/** cold_dm_campaigns campaign-level send worker lease columns. */
function isMissingCampaignSendLeaseColumnsError(error) {
  if (!isLikelyPgMissingColumnError(error)) return false;
  const msg = String(error.message || '').toLowerCase();
  return (
    msg.includes('send_leased_until') ||
    msg.includes('send_leased_by_worker') ||
    msg.includes('send_lease_heartbeat_at')
  );
}

/**
 * After PM2 stop / crash, leased_until can stay in the future for many minutes while no process heartbeats.
 * Allow reclaim when heartbeat is missing or older than INSTAGRAM_SESSION_LEASE_STALE_SEC (default 120s).
 */
async function claimInstagramSessionLease(sessionId, workerId, leaseSeconds = 600) {
  const sb = getSupabase();
  if (!sb || !sessionId || !workerId) return false;
  const nowIso = new Date().toISOString();
  const leaseUntil = computeSendSessionLeaseUntil(leaseSeconds);
  const staleBefore = instagramLeaseStaleBeforeIso(leaseSeconds);
  const updatePayload = {
    leased_by_worker: workerId,
    leased_until: leaseUntil,
    lease_heartbeat_at: nowIso,
  };
  const attempts = [
    (q) => q.is('leased_until', null),
    (q) => q.lte('leased_until', nowIso),
    (q) =>
      q
        .gt('leased_until', nowIso)
        .or(`lease_heartbeat_at.lt.${staleBefore},lease_heartbeat_at.is.null`),
  ];
  for (const build of attempts) {
    let query = sb.from('cold_dm_instagram_sessions').update(updatePayload).eq('id', sessionId);
    query = build(query);
    const { data, error } = await query.select('id').limit(1);
    if (!error && data && data.length > 0) return true;
    if (isMissingStandardLeaseColumnsError(error)) {
      // Backward compatibility: schema missing lease columns.
      // Treat as claimable so single-worker mode still functions until migration runs.
      const { data: exists, error: existsErr } = await sb
        .from('cold_dm_instagram_sessions')
        .select('id')
        .eq('id', sessionId)
        .limit(1);
      if (!existsErr && exists && exists.length > 0) return true;
    }
  }
  return false;
}

async function claimInstagramSessionForCampaign(clientId, campaignId, workerId, leaseSeconds = 600) {
  const sessions = await getSessionsForCampaign(clientId, campaignId);
  if (!sessions?.length) return null;
  const now = Date.now();
  const staleMs =
    Math.max(45, parseInt(process.env.INSTAGRAM_SESSION_LEASE_STALE_SEC || '120', 10) || 120) * 1000;
  const heartbeatStale = (s) => {
    if (s.lease_heartbeat_at == null || s.lease_heartbeat_at === '') return true;
    return new Date(s.lease_heartbeat_at).getTime() < now - staleMs;
  };
  const eligible = sessions.filter((s) => {
    if (s?.web_session_needs_refresh === true) return false;
    if (!s.leased_until || new Date(s.leased_until).getTime() <= now) return true;
    return heartbeatStale(s);
  });
  for (const candidate of eligible) {
    const ok = await claimInstagramSessionLease(candidate.id, workerId, leaseSeconds);
    if (ok) {
      return candidate;
    }
  }
  return null;
}

/**
 * Best-effort human-readable reason when a campaign is waiting for a leased Instagram session.
 * Uses the same campaign/session assignment scope as claimInstagramSessionForCampaign.
 */
async function getWaitingInstagramSessionReason(clientId, campaignId) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignId) return null;
  const sessions = await getSessionsForCampaign(clientId, campaignId);
  if (!sessions?.length) return null;
  if (sessions.some((s) => s?.web_session_needs_refresh === true)) {
    return 'Please reconnect your account in Settings > Integrations > Automation session (outbound).';
  }
  const now = Date.now();
  const leased = sessions
    .filter((s) => s?.leased_until && new Date(s.leased_until).getTime() > now)
    .sort((a, b) => new Date(a.leased_until).getTime() - new Date(b.leased_until).getTime());
  if (leased.length === 0) return null;

  const locked = leased[0];
  const username = locked.instagram_username ? `@${String(locked.instagram_username).trim().replace(/^@/, '')}` : 'an Instagram account';
  const workerId = locked.leased_by_worker ? String(locked.leased_by_worker).trim() : '';
  if (!workerId) return `Waiting for ${username} to become available.`;

  const { data: blockingCampaign } = await sb
    .from('cold_dm_campaigns')
    .select('id, name')
    .eq('client_id', clientId)
    .eq('status', 'active')
    .neq('id', campaignId)
    .eq('send_leased_by_worker', workerId)
    .order('updated_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (blockingCampaign?.name) {
    return `Waiting for ${username} to finish "${String(blockingCampaign.name)}".`;
  }
  return `Waiting for ${username} to become available.`;
}

async function heartbeatInstagramSessionLease(sessionId, workerId, leaseSeconds = 600) {
  const sb = getSupabase();
  if (!sb || !sessionId || !workerId) return false;
  const nowIso = new Date().toISOString();
  const leaseUntil = computeSendSessionLeaseUntil(leaseSeconds);
  const { data, error } = await sb
    .from('cold_dm_instagram_sessions')
    .update({ leased_until: leaseUntil, lease_heartbeat_at: nowIso, updated_at: nowIso })
    .eq('id', sessionId)
    .eq('leased_by_worker', workerId)
    .select('id')
    .limit(1);
  if (isMissingStandardLeaseColumnsError(error)) return true;
  return !error && !!(data && data.length > 0);
}

async function releaseInstagramSessionLease(sessionId, workerId) {
  const sb = getSupabase();
  if (!sb || !sessionId) return;
  const nowIso = new Date().toISOString();
  let q = sb
    .from('cold_dm_instagram_sessions')
    .update({ leased_until: null, leased_by_worker: null, lease_heartbeat_at: nowIso, updated_at: nowIso })
    .eq('id', sessionId);
  if (workerId) q = q.eq('leased_by_worker', workerId);
  const { error } = await q;
  if (isMissingStandardLeaseColumnsError(error)) return;
}

/**
 * Clear all IG session leases. Call when send workers are stopped (e.g. dashboard Stop) so rows are not
 * stuck until leased_until expires; PM2 often kills processes before bot.js finally runs.
 */
async function releaseAllInstagramSessionLeases(clientId = null) {
  const sb = getSupabase();
  if (!sb) return { released: 0 };
  const nowIso = new Date().toISOString();
  let query = sb
    .from('cold_dm_instagram_sessions')
    .update({
      leased_until: null,
      leased_by_worker: null,
      lease_heartbeat_at: nowIso,
      updated_at: nowIso,
    })
    .or('leased_by_worker.not.is.null,leased_until.not.is.null');
  if (clientId) query = query.eq('client_id', clientId);
  const primary = await query.select('id');
  if (!primary.error) return { released: (primary.data || []).length };
  // Backward compatibility: older DBs may not have leased_* columns yet.
  const msg = String(primary.error?.message || '').toLowerCase();
  if (primary.error?.code === '42703' || msg.includes('does not exist')) {
    return { released: 0, skipped: true, reason: 'lease_columns_missing' };
  }
  throw primary.error;
}

async function claimCampaignSendLease(campaignId, workerId, leaseSeconds = 240) {
  const sb = getSupabase();
  if (!sb || !campaignId || !workerId) return false;
  const nowIso = new Date().toISOString();
  const leaseUntil = computeSendSessionLeaseUntil(leaseSeconds);
  const staleBefore = campaignSendLeaseStaleBeforeIso();
  const updatePayload = {
    send_leased_by_worker: workerId,
    send_leased_until: leaseUntil,
    send_lease_heartbeat_at: nowIso,
    updated_at: nowIso,
  };
  const attempts = [
    (q) => q.is('send_leased_until', null),
    (q) => q.lte('send_leased_until', nowIso),
    (q) => q.eq('send_leased_by_worker', workerId),
    (q) =>
      q
        .gt('send_leased_until', nowIso)
        .or(`send_lease_heartbeat_at.lt.${staleBefore},send_lease_heartbeat_at.is.null`),
  ];
  for (const build of attempts) {
    let query = sb.from('cold_dm_campaigns').update(updatePayload).eq('id', campaignId);
    query = build(query);
    const { data, error } = await query.select('id').limit(1);
    if (!error && data && data.length > 0) return true;
    if (isMissingCampaignSendLeaseColumnsError(error)) {
      const { data: exists, error: existsErr } = await sb.from('cold_dm_campaigns').select('id').eq('id', campaignId).limit(1);
      if (!existsErr && exists && exists.length > 0) return true;
    }
  }
  return false;
}

async function heartbeatCampaignSendLease(campaignId, workerId, leaseSeconds = 240) {
  const sb = getSupabase();
  if (!sb || !campaignId || !workerId) return false;
  const nowIso = new Date().toISOString();
  const leaseUntil = computeSendSessionLeaseUntil(leaseSeconds);
  const { data, error } = await sb
    .from('cold_dm_campaigns')
    .update({
      send_leased_until: leaseUntil,
      send_lease_heartbeat_at: nowIso,
      updated_at: nowIso,
    })
    .eq('id', campaignId)
    .eq('send_leased_by_worker', workerId)
    .select('id')
    .limit(1);
  if (isMissingCampaignSendLeaseColumnsError(error)) return true;
  return !error && !!(data && data.length > 0);
}

async function releaseCampaignSendLease(campaignId, workerId) {
  const sb = getSupabase();
  if (!sb || !campaignId) return;
  const nowIso = new Date().toISOString();
  let q = sb
    .from('cold_dm_campaigns')
    .update({
      send_leased_until: null,
      send_leased_by_worker: null,
      send_lease_heartbeat_at: nowIso,
      updated_at: nowIso,
    })
    .eq('id', campaignId);
  if (workerId) q = q.eq('send_leased_by_worker', workerId);
  const { error } = await q;
  if (isMissingCampaignSendLeaseColumnsError(error)) return;
}

async function releaseAllCampaignSendLeases(workerId = null, clientId = null) {
  const sb = getSupabase();
  if (!sb) return { released: 0 };
  const nowIso = new Date().toISOString();
  let q = sb
    .from('cold_dm_campaigns')
    .update({
      send_leased_until: null,
      send_leased_by_worker: null,
      send_lease_heartbeat_at: nowIso,
      updated_at: nowIso,
    })
    .or('send_leased_by_worker.not.is.null,send_leased_until.not.is.null');
  if (workerId) q = q.eq('send_leased_by_worker', workerId);
  if (clientId) q = q.eq('client_id', clientId);
  const primary = await q.select('id');
  if (!primary.error) return { released: (primary.data || []).length };
  const msg = String(primary.error?.message || '').toLowerCase();
  if (primary.error?.code === '42703' || msg.includes('does not exist')) {
    return { released: 0, skipped: true, reason: 'lease_columns_missing' };
  }
  throw primary.error;
}

function normalizeInstagramKey(instagramUsername) {
  return (instagramUsername || '').trim().replace(/^@/, '').toLowerCase() || null;
}

/**
 * Resolve or create Decodo sub-user proxy for this client + IG handle. Reuses cold_dm_proxy_assignments after session delete.
 * @returns {{ proxyUrl: string|null, proxyAssignmentId: string|null }}
 */
async function getOrResolveColdDmProxyUrl(clientId, instagramUsername) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const ig = normalizeInstagramKey(instagramUsername);
  if (!ig) throw new Error('instagram username required for proxy resolution');
  const forceGermany = (process.env.DECODO_GATE_COUNTRY || '').trim().toLowerCase() === 'de' || !(process.env.DECODO_GATE_COUNTRY || '').trim();

  const { data: existing, error: selErr } = await sb
    .from('cold_dm_proxy_assignments')
    .select('id, proxy_url, provider_ref')
    .eq('client_id', clientId)
    .eq('instagram_username', ig)
    .maybeSingle();
  if (selErr) throw selErr;
  if (existing?.proxy_url) {
    const needsRefresh = decodoProvision.decodoStoredProxyUrlNeedsRefresh(clientId, ig, existing.proxy_url, existing.provider_ref);
    const missingGermanyPin = forceGermany && !String(existing.proxy_url).includes('-country-de');
    if (needsRefresh || missingGermanyPin) {
      const { proxyUrl, providerRef } = await decodoProvision.provisionDecodoSubuserProxy(clientId, ig);
      const { error: upErr } = await sb
        .from('cold_dm_proxy_assignments')
        .update({
          proxy_url: proxyUrl,
          provider_ref: providerRef,
          updated_at: new Date().toISOString(),
        })
        .eq('id', existing.id);
      if (upErr) throw upErr;
      return { proxyUrl, proxyAssignmentId: existing.id };
    }
    return { proxyUrl: existing.proxy_url, proxyAssignmentId: existing.id };
  }

  if (!decodoProvision.isDecodoAutoConfigured()) {
    return { proxyUrl: null, proxyAssignmentId: null };
  }

  const { proxyUrl, providerRef } = await decodoProvision.provisionDecodoSubuserProxy(clientId, ig);
  const { data: inserted, error: insErr } = await sb
    .from('cold_dm_proxy_assignments')
    .insert({
      client_id: clientId,
      instagram_username: ig,
      proxy_url: proxyUrl,
      provider: 'decodo',
      provider_ref: providerRef,
      updated_at: new Date().toISOString(),
    })
    .select('id')
    .maybeSingle();

  if (insErr && insErr.code === '23505') {
    const { data: again } = await sb
      .from('cold_dm_proxy_assignments')
      .select('id, proxy_url')
      .eq('client_id', clientId)
      .eq('instagram_username', ig)
      .maybeSingle();
    if (again?.proxy_url) {
      return { proxyUrl: again.proxy_url, proxyAssignmentId: again.id };
    }
    throw insErr;
  }
  if (insErr) throw insErr;
  return { proxyUrl, proxyAssignmentId: inserted?.id || null };
}

/**
 * @param {object} proxyOpts
 * @param {string|null} [proxyOpts.proxyUrl]
 * @param {string|null} [proxyOpts.proxyAssignmentId]
 */
async function saveSession(clientId, sessionData, instagramUsername, proxyOpts = {}) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const username = normalizeInstagramKey(instagramUsername);
  const row = {
    client_id: clientId,
    session_data: sessionData,
    instagram_username: username || null,
    updated_at: new Date().toISOString(),
  };
  if (proxyOpts.proxyUrl) row.proxy_url = proxyOpts.proxyUrl;
  if (proxyOpts.proxyAssignmentId) row.proxy_assignment_id = proxyOpts.proxyAssignmentId;

  const { error } = await sb.from('cold_dm_instagram_sessions').upsert(row, {
    onConflict: 'client_id,instagram_username',
  });
  if (error) throw error;
}

async function alreadySent(clientId, username) {
  const sb = getSupabase();
  if (!sb || !clientId) return false;
  const u = normalizeUsername(username);
  const { data, error } = await sb
    .from('cold_dm_sent_messages')
    .select('id')
    .eq('client_id', clientId)
    .eq('username', u)
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return !!data;
}

/**
 * @param {{ skipDailyStats?: boolean }} [options] - If skipDailyStats, insert cold_dm_sent_messages only (e.g. pre-send blocklist skip).
 */
async function logSentMessage(
  clientId,
  username,
  message,
  status = 'success',
  campaignId = null,
  messageGroupId = null,
  messageGroupMessageId = null,
  failureReason = null,
  options = {}
) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const skipDailyStats = options && options.skipDailyStats === true;
  const u = normalizeUsername(username);
  const date = await getTodayForClient(clientId);
  let settingsTimezoneForDebug = null;
  if (coldDmMetricsDebugEnabled()) {
    try {
      settingsTimezoneForDebug = (await getSettings(clientId))?.timezone ?? null;
    } catch (e) {
      settingsTimezoneForDebug = { error: e && e.message ? String(e.message) : 'getSettings failed' };
    }
  }
  const insertPayload = {
    client_id: clientId,
    username: u,
    message: message || null,
    status,
  };
  if (campaignId) insertPayload.campaign_id = campaignId;
  if (messageGroupId) insertPayload.message_group_id = messageGroupId;
  if (messageGroupMessageId) insertPayload.message_group_message_id = messageGroupMessageId;
  if (status === 'failed' && failureReason) insertPayload.failure_reason = failureReason;
  const { error: insertErr } = await sb.from('cold_dm_sent_messages').insert(insertPayload);
  if (insertErr) throw insertErr;
  logColdDmMetricsDebug('insert cold_dm_sent_messages ok', {
    clientId,
    username: u,
    status,
    campaignId: campaignId || null,
    messageGroupId: messageGroupId || null,
    messageGroupMessageId: messageGroupMessageId || null,
    skipDailyStats,
    dailyStatsDate: skipDailyStats ? null : date,
    settingsTimezone: settingsTimezoneForDebug,
  });
  if (skipDailyStats) return;

  // Optimistic CAS retry to avoid lost updates under concurrent writes.
  for (let attempt = 0; attempt < 5; attempt++) {
    const { data: existing, error: getErr } = await sb
      .from('cold_dm_daily_stats')
      .select('total_sent, total_failed')
      .eq('client_id', clientId)
      .eq('date', date)
      .maybeSingle();
    if (getErr) throw getErr;

    if (!existing) {
      const { error: insertStatErr } = await sb.from('cold_dm_daily_stats').upsert(
        {
          client_id: clientId,
          date,
          total_sent: status === 'success' ? 1 : 0,
          total_failed: status === 'failed' ? 1 : 0,
        },
        { onConflict: 'client_id,date', ignoreDuplicates: true }
      );
      if (insertStatErr) throw insertStatErr;
      logColdDmMetricsDebug('daily_stats upsert new row (retry read)', {
        clientId,
        date,
        attempt,
        status,
      });
      continue;
    }

    const nextTotalSent = status === 'success' ? existing.total_sent + 1 : existing.total_sent;
    const nextTotalFailed = status === 'failed' ? existing.total_failed + 1 : existing.total_failed;
    const { data: updated, error: updateErr } = await sb
      .from('cold_dm_daily_stats')
      .update({ total_sent: nextTotalSent, total_failed: nextTotalFailed })
      .eq('client_id', clientId)
      .eq('date', date)
      .eq('total_sent', existing.total_sent)
      .eq('total_failed', existing.total_failed)
      .select('client_id')
      .limit(1);
    if (updateErr) throw updateErr;
    if (updated && updated.length > 0) {
      logColdDmMetricsDebug('daily_stats CAS update ok', {
        clientId,
        date,
        attempt,
        status,
        prev: { total_sent: existing.total_sent, total_failed: existing.total_failed },
        next: { total_sent: nextTotalSent, total_failed: nextTotalFailed },
      });
      return;
    }
    logColdDmMetricsDebug('daily_stats CAS miss (retry)', {
      clientId,
      date,
      attempt,
      expected: { total_sent: existing.total_sent, total_failed: existing.total_failed },
    });
  }
  throw new Error('Failed to atomically update daily stats after retries');
}

async function getDailyStats(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return { date: getToday(), total_sent: 0, total_failed: 0 };
  const date = await getTodayForClient(clientId);
  const { data, error } = await sb
    .from('cold_dm_daily_stats')
    .select('total_sent, total_failed')
    .eq('client_id', clientId)
    .eq('date', date)
    .maybeSingle();
  if (error) throw error;
  return data
    ? { date, total_sent: data.total_sent, total_failed: data.total_failed }
    : { date, total_sent: 0, total_failed: 0 };
}

async function getDailyStatsForTimezone(clientId, timezone) {
  const sb = getSupabase();
  if (!sb || !clientId) return { date: getToday(), total_sent: 0, total_failed: 0 };
  const tz = normalizeTimezoneInput(timezone) || 'UTC';
  const date = getTodayInTimezone(tz);
  const sinceIso = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
  const { data, error } = await sb
    .from('cold_dm_sent_messages')
    .select('status, sent_at')
    .eq('client_id', clientId)
    .gte('sent_at', sinceIso)
    .in('status', ['success', 'failed']);
  if (error) throw error;
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  let total_sent = 0;
  let total_failed = 0;
  for (const row of data || []) {
    if (!row?.sent_at) continue;
    const rowDate = fmt.format(new Date(row.sent_at));
    if (rowDate !== date) continue;
    if (row.status === 'success') total_sent += 1;
    else if (row.status === 'failed') total_failed += 1;
  }
  return { date, total_sent, total_failed };
}

async function getHourlySent(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const { count, error } = await sb
    .from('cold_dm_sent_messages')
    .select('*', { count: 'exact', head: true })
    .eq('client_id', clientId)
    .eq('status', 'success')
    .gte('sent_at', oneHourAgo);
  if (error) throw error;
  return count || 0;
}

async function getCampaignLimitsById(campaignId) {
  const sb = getSupabase();
  if (!sb || !campaignId) return null;
  const { data, error } = await sb
    .from('cold_dm_campaigns')
    .select('id, daily_send_limit, hourly_send_limit, timezone')
    .eq('id', campaignId)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getControl(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb
    .from('cold_dm_control')
    .select('pause')
    .eq('client_id', clientId)
    .maybeSingle();
  if (error) throw error;
  return data ? String(data.pause) : null;
}

async function setControl(clientId, pause) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const { error } = await sb
    .from('cold_dm_control')
    .upsert(
      {
        client_id: clientId,
        pause: pause === 1 || pause === '1' ? 1 : 0,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'client_id' }
    );
  if (error) throw error;
}

/**
 * Set a human-readable status message for this client (e.g. "Sending…", "Hourly limit reached. Next send in ~60 min.").
 * Dashboard can display this for running campaigns.
 */
async function setClientStatusMessage(clientId, message) {
  const sb = getSupabase();
  if (!sb || !clientId) return;
  const now = new Date().toISOString();
  await sb
    .from('cold_dm_control')
    .update({ status_message: message == null ? null : String(message).slice(0, 500), status_updated_at: now, updated_at: now })
    .eq('client_id', clientId);
}

/**
 * Get the current status message for a client (set by the bot).
 */
async function getClientStatusMessage(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb
    .from('cold_dm_control')
    .select('status_message')
    .eq('client_id', clientId)
    .maybeSingle();
  if (error) return null;
  return data?.status_message ?? null;
}

/**
 * Returns client_ids that have pause = 0 (sending allowed).
 * Used by multi-tenant worker to find clients that may have work.
 */
async function getClientIdsWithPauseZero() {
  const sb = getSupabase();
  if (!sb) return [];
  const { data, error } = await sb
    .from('cold_dm_control')
    .select('client_id')
    .eq('pause', 0)
    .order('client_id', { ascending: true });
  if (error) throw error;
  return (data || []).map((r) => r.client_id).filter(Boolean);
}

/**
 * Distinct active campaigns (pause=0 clients only) that have at least one send job ready to claim now.
 * Sorted by campaign id for stable PM2 slot → campaign assignment.
 */
async function getDistinctActiveCampaignIdsWithReadySendJobs() {
  const sb = getSupabase();
  if (!sb) return [];
  const clientIds = await getClientIdsWithPauseZero();
  if (!clientIds.length) return [];
  const nowIso = new Date().toISOString();
  const { data: jobRows, error: jobErr } = await sb
    .from('cold_dm_send_jobs')
    .select('campaign_id, client_id')
    .in('status', ['pending', 'retry'])
    .lte('available_at', nowIso);
  if (jobErr) throw jobErr;
  const byCampaign = new Map();
  for (const row of jobRows || []) {
    if (!row.campaign_id || !row.client_id) continue;
    if (!clientIds.includes(row.client_id)) continue;
    byCampaign.set(row.campaign_id, row.client_id);
  }
  if (byCampaign.size === 0) return [];
  const campaignIds = [...byCampaign.keys()];
  const { data: camps, error: campErr } = await sb
    .from('cold_dm_campaigns')
    .select('id, client_id, status')
    .in('id', campaignIds)
    .eq('status', 'active');
  if (campErr) throw campErr;
  const clientSet = new Set(clientIds);
  const active = (camps || [])
    .filter((c) => c.id && c.client_id && clientSet.has(c.client_id))
    .map((c) => c.id);
  active.sort();
  return active;
}

/**
 * Enforce single active send campaign per client by selecting the first active
 * campaign (ordered by oldest ready job) that currently has claimable work.
 */
async function getClientSendCampaignTurn(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const nowIso = new Date().toISOString();
  const { data: jobs, error: jobsErr } = await sb
    .from('cold_dm_send_jobs')
    .select('campaign_id, available_at, priority, created_at')
    .eq('client_id', clientId)
    .in('status', ['pending', 'retry'])
    .not('campaign_id', 'is', null)
    .lte('available_at', nowIso)
    .order('available_at', { ascending: true })
    .order('priority', { ascending: true })
    .order('created_at', { ascending: true })
    .limit(200);
  if (jobsErr || !jobs?.length) return null;
  const orderedCampaignIds = [];
  const seen = new Set();
  for (const row of jobs) {
    const id = row?.campaign_id || null;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    orderedCampaignIds.push(id);
  }
  if (!orderedCampaignIds.length) return null;
  const { data: campaigns, error: campErr } = await sb
    .from('cold_dm_campaigns')
    .select('id, name, status')
    .in('id', orderedCampaignIds);
  if (campErr || !campaigns?.length) return null;
  const campaignById = new Map(campaigns.map((c) => [c.id, c]));
  for (const id of orderedCampaignIds) {
    const c = campaignById.get(id);
    if (!c || c.status !== 'active') continue;
    return { campaignId: id, campaignName: c.name || id };
  }
  return null;
}

/**
 * Suggested PM2 cluster size for `ig-dm-send`: max of (clients with sending on),
 * (Instagram sessions on those clients), and (distinct active campaigns with ready send jobs),
 * clamped to SEND_WORKER_MIN / SEND_WORKER_MAX.
 * When every client is paused, returns 1 so one worker can still claim jobs if state changes.
 */
async function getRecommendedSendWorkerInstanceCount() {
  const minN = Math.max(1, parseInt(process.env.SEND_WORKER_MIN || '1', 10) || 1);
  const maxN = Math.max(minN, parseInt(process.env.SEND_WORKER_MAX || '64', 10) || 64);
  const sb = getSupabase();
  if (!sb) {
    return {
      recommended: minN,
      pauseZeroClients: 0,
      instagramSessionsForActiveClients: 0,
      campaignsWithReadyJobs: 0,
      minN,
      maxN,
      reason: 'no_supabase',
    };
  }
  const clientIds = await getClientIdsWithPauseZero();
  const pauseZero = clientIds.length;
  let instagramSessionsForActiveClients = 0;
  if (clientIds.length > 0) {
    const { count, error } = await sb
      .from('cold_dm_instagram_sessions')
      .select('*', { count: 'exact', head: true })
      .in('client_id', clientIds);
    if (error) throw error;
    instagramSessionsForActiveClients = count ?? 0;
  }
  let campaignsWithReadyJobs = 0;
  try {
    const cids = await getDistinctActiveCampaignIdsWithReadySendJobs();
    campaignsWithReadyJobs = cids.length;
  } catch (e) {
    console.warn('[getRecommendedSendWorkerInstanceCount] campaign ready count failed:', e?.message || e);
  }
  const raw =
    pauseZero === 0 ? 1 : Math.max(1, pauseZero, instagramSessionsForActiveClients, campaignsWithReadyJobs);
  const recommended = Math.max(minN, Math.min(maxN, raw));
  return {
    recommended,
    pauseZeroClients: pauseZero,
    instagramSessionsForActiveClients,
    campaignsWithReadyJobs,
    minN,
    maxN,
  };
}

/**
 * Returns next pending work from any client with pause = 0.
 * Fresh read every call (no cache). Only campaigns with status = 'active' are considered;
 * then cold_dm_campaign_leads with status = 'pending', schedule/timezone/limits applied.
 * @returns {Promise<{ clientId: string, work: object } | null>}
 */
async function getNextPendingWorkAnyClient(workerId = null, leaseSeconds = 600) {
  const clientIds = await getClientIdsWithPauseZero();
  if (clientIds.length === 0) {
    logNoWorkDebug('No clients with pause=0.');
    return null;
  }
  for (const clientId of clientIds) {
    const work = await getNextPendingCampaignLead(clientId, workerId, leaseSeconds);
    if (work) {
      logNoWorkDebug('Selected work for client.', { clientId, campaignId: work.campaignId, campaignLeadId: work.campaignLeadId, username: work.username });
      return { clientId, work };
    }
    logNoWorkDebug('Client has no sendable work this iteration.', { clientId });
  }
  return null;
}

/**
 * If the client has pending campaign leads but is currently outside all campaign schedule windows (per campaign timezone), returns a status message. Otherwise null.
 */
async function getClientOutsideScheduleStatus(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const campaigns = await getActiveCampaigns(clientId);
  let firstWindow = null;
  let tzLabel = 'UTC';
  let hasPending = false;
  let allOutside = true;
  for (const camp of campaigns) {
    const { count, error: err } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', camp.id)
      .eq('status', 'pending');
    if (err || (count ?? 0) === 0) continue;
    hasPending = true;
    const campaignTz = camp.timezone ?? null;
    const inSchedule = isWithinSchedule(camp.schedule_start_time, camp.schedule_end_time, campaignTz);
    if (inSchedule) {
      allOutside = false;
      break;
    }
    if (!firstWindow && (camp.schedule_start_time || camp.schedule_end_time)) {
      const start = (normalizeScheduleTime(camp.schedule_start_time) || '00:00:00').slice(0, 5);
      const end = (normalizeScheduleTime(camp.schedule_end_time) || '24:00:00').slice(0, 5);
      firstWindow = `${start}–${end}`;
      tzLabel = campaignTz || 'UTC';
    }
  }
  if (!hasPending || !allOutside || !firstWindow) return null;
  return `Outside schedule. Sends between ${firstWindow} (${tzLabel}).`;
}

/**
 * Returns the status message and reason when there is no sendable work for this client
 * (outside schedule, daily limit, hourly limit, or no pending leads).
 * @returns {{ message: string | null, reason: 'outside_schedule' | 'daily_limit' | 'hourly_limit' | 'no_pending' }}
 */
async function getClientNoWorkReason(clientId) {
  const r = await getClientNoWorkResumeAt(clientId);
  return { message: r.message, reason: r.reason };
}

/**
 * Returns message, reason, and resumeAt (UTC Date) when we should wake and retry.
 * resumeAt is null only when reason is 'no_pending' (campaign completed).
 */
async function getClientNoWorkResumeAt(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return { message: null, reason: 'no_pending', resumeAt: null };

  const settings = await getSettings(clientId).catch(() => null);
  const { data: allCampaigns } = await sb
    .from('cold_dm_campaigns')
    .select('id, name, status')
    .eq('client_id', clientId);
  if (!allCampaigns || allCampaigns.length === 0) {
    return { message: 'No campaigns.', reason: 'no_campaigns', resumeAt: null };
  }
  const allCampaignIds = allCampaigns.map((c) => c.id).filter(Boolean);
  const { count: totalPendingAllCampaigns } =
    allCampaignIds.length > 0
      ? await sb
          .from('cold_dm_campaign_leads')
          .select('*', { count: 'exact', head: true })
          .in('campaign_id', allCampaignIds)
          .eq('status', 'pending')
      : { count: 0 };
  if ((totalPendingAllCampaigns ?? 0) === 0) {
    return { message: null, reason: 'no_pending', resumeAt: null };
  }

  const campaigns = await getActiveCampaigns(clientId);
  const activeCampaignIds = (campaigns || []).map((c) => c.id).filter(Boolean);
  const { count: pendingOnActiveCampaigns } =
    activeCampaignIds.length > 0
      ? await sb
          .from('cold_dm_campaign_leads')
          .select('*', { count: 'exact', head: true })
          .in('campaign_id', activeCampaignIds)
          .eq('status', 'pending')
      : { count: 0 };
  const pendingTotal = pendingOnActiveCampaigns ?? 0;

  if (pendingTotal === 0) {
    return {
      message:
        `You have ${totalPendingAllCampaigns} pending lead(s), but none on an active campaign. ` +
        `Active campaigns: ${allCampaigns.map((c) => `${c.name || c.id}:${c.status || 'unknown'}`).join(', ')}. ` +
        'Open the campaign and set status to active (or press Start in the dashboard) so the bot can send.',
      reason: 'no_sendable_work',
      resumeAt: null,
    };
  }

  const delayProblems = await getCampaignsMissingSendDelays(clientId).catch(() => []);
  if (delayProblems.length > 0) {
    const first = delayProblems[0];
    const extra = delayProblems.length > 1 ? ` (+${delayProblems.length - 1} more)` : '';
    return {
      message: `Campaign "${first.name || first.id}" is missing send delay settings (${first.reason}). Set min/max delay in campaign settings before starting${extra}.`,
      reason: 'missing_delay_config',
      resumeAt: null,
    };
  }

  let earliestScheduleResume = null;
  let firstWindow = null;
  let tzLabel = 'UTC';
  let allOutside = true;
  for (const camp of campaigns) {
    if (!hasValidCampaignSendDelayConfig(camp, settings)) continue;
    const { count: campPending } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', camp.id)
      .eq('status', 'pending');
    if ((campPending ?? 0) === 0) continue;
    const campaignTz = camp.timezone ?? null;
    const inSchedule = isWithinSchedule(camp.schedule_start_time, camp.schedule_end_time, campaignTz);
    if (inSchedule) {
      allOutside = false;
      break;
    }
    if (!firstWindow && (camp.schedule_start_time || camp.schedule_end_time)) {
      const start = (normalizeScheduleTime(camp.schedule_start_time) || '00:00:00').slice(0, 5);
      const end = (normalizeScheduleTime(camp.schedule_end_time) || '24:00:00').slice(0, 5);
      firstWindow = `${start}–${end}`;
      tzLabel = campaignTz || 'UTC';
    }
    const nextStart = getNextScheduleStartInTimezone(camp.schedule_start_time, campaignTz);
    if (nextStart && (!earliestScheduleResume || nextStart.getTime() < earliestScheduleResume.getTime())) {
      earliestScheduleResume = nextStart;
    }
  }
  if (allOutside && earliestScheduleResume && firstWindow) {
    return {
      message: `Outside schedule. Sends between ${firstWindow} (${tzLabel}).`,
      reason: 'outside_schedule',
      resumeAt: earliestScheduleResume,
    };
  }

  const clientTz = settings?.timezone ?? null;
  const [stats, hourlySent] = await Promise.all([getDailyStats(clientId), getHourlySent(clientId)]);
  const pendingCampaignsInSchedule = [];
  for (const camp of campaigns) {
    const { count: campPending } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', camp.id)
      .eq('status', 'pending');
    if ((campPending ?? 0) === 0) continue;
    const campaignTz = camp.timezone ?? null;
    if (!isWithinSchedule(camp.schedule_start_time, camp.schedule_end_time, campaignTz)) continue;
    pendingCampaignsInSchedule.push(camp);
  }
  const blockedDaily = pendingCampaignsInSchedule.find((camp) => camp.daily_send_limit != null && stats.total_sent >= camp.daily_send_limit);
  if (blockedDaily) {
    return {
      message: `daily limit reached (campaign daily=${blockedDaily.daily_send_limit}, sentToday=${stats.total_sent}, counting=successful sends only)`,
      reason: 'daily_limit',
      resumeAt: getNextMidnightInTimezone(clientTz),
    };
  }
  const blockedHourly = pendingCampaignsInSchedule.find((camp) => camp.hourly_send_limit != null && hourlySent >= camp.hourly_send_limit);
  if (blockedHourly) {
    return {
      message: `hourly limit reached (campaign hourly=${blockedHourly.hourly_send_limit}, sentThisHour=${hourlySent}, counting=successful sends only)`,
      reason: 'hourly_limit',
      resumeAt: getNextHourStartInTimezone(clientTz),
    };
  }
  // Queue-aware wakeup: if all pending/retry jobs are deferred to the future (e.g. outside schedule),
  // do not spin on "pending_ready" every few seconds.
  const nowMs = Date.now();
  const { data: earliestQueuedJob } = await sb
    .from('cold_dm_send_jobs')
    .select('available_at, status, last_error_class, last_error_message')
    .eq('client_id', clientId)
    .in('status', ['pending', 'retry'])
    .order('available_at', { ascending: true })
    .limit(1)
    .maybeSingle();
  const earliestQueuedAtRaw = earliestQueuedJob?.available_at || null;
  const earliestQueuedAtMs = earliestQueuedAtRaw ? Date.parse(earliestQueuedAtRaw) : NaN;
  if (Number.isFinite(earliestQueuedAtMs) && earliestQueuedAtMs > nowMs + 1000) {
    // Self-heal: if we have pending work that is *currently* in schedule, but the queue is deferred
    // due to outside_schedule, pull those jobs forward so we don't "wait until tomorrow" incorrectly.
    // This also recovers from any past schedule math bugs that stamped a too-far-future available_at.
    if (
      pendingCampaignsInSchedule.length > 0 &&
      String(earliestQueuedJob?.last_error_class || '').trim() === 'outside_schedule'
    ) {
      try {
        const nowIso = new Date().toISOString();
        const inScheduleCampaignIds = pendingCampaignsInSchedule.map((c) => c.id).filter(Boolean);
        if (inScheduleCampaignIds.length > 0) {
          await sb
            .from('cold_dm_send_jobs')
            .update({ available_at: nowIso, updated_at: nowIso })
            .eq('client_id', clientId)
            .in('campaign_id', inScheduleCampaignIds)
            .in('status', ['pending', 'retry'])
            .eq('last_error_class', 'outside_schedule')
            .gt('available_at', nowIso);
          return { message: null, reason: 'pending_ready', resumeAt: new Date(Date.now() + 15_000) };
        }
      } catch {}
    }
    const resumeAt = new Date(earliestQueuedAtMs);
    const jobMeta = {
      lastErrorClass: earliestQueuedJob?.last_error_class || '',
      lastErrorMessage: earliestQueuedJob?.last_error_message || '',
    };
    return {
      message: formatQueueWaitDeferMessage(earliestQueuedAtRaw, settings?.timezone ?? null, jobMeta),
      reason: 'queue_wait',
      resumeAt,
    };
  }
  const unsendableHint = await getMostSpecificNoWorkHint(clientId).catch(() => '');
  if (unsendableHint) {
    return { message: unsendableHint, reason: 'no_sendable_work', resumeAt: null };
  }
  return {
    message: null,
    reason: 'pending_ready',
    resumeAt: new Date(Date.now() + 15_000),
  };
}

async function getRecentSent(clientId, limit = 50) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_sent_messages')
    .select('username, message, sent_at, status')
    .eq('client_id', clientId)
    .order('sent_at', { ascending: false })
    .limit(Math.min(limit, 200));
  if (error) throw error;
  return data || [];
}

async function getSentUsernames(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return new Set();
  const { data, error } = await sb
    .from('cold_dm_sent_messages')
    .select('username')
    .eq('client_id', clientId);
  if (error) throw error;
  const set = new Set();
  for (const row of data || []) {
    const u = normalizeUsername(row.username).toLowerCase();
    if (u) set.add(u);
  }
  return set;
}

async function clearFailedAttempts(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const date = await getTodayForClient(clientId);
  const { data: deleted } = await sb
    .from('cold_dm_sent_messages')
    .delete()
    .eq('client_id', clientId)
    .eq('status', 'failed')
    .select('id');
  const count = deleted?.length ?? 0;
  const { data: row } = await sb
    .from('cold_dm_daily_stats')
    .select('id')
    .eq('client_id', clientId)
    .eq('date', date)
    .maybeSingle();
  if (row) {
    await sb.from('cold_dm_daily_stats').update({ total_failed: 0 }).eq('client_id', clientId).eq('date', date);
  }
  return count;
}

async function updateSettingsInstagramUsername(clientId, instagramUsername) {
  const sb = getSupabase();
  if (!sb || !clientId) return;
  await sb.from('cold_dm_settings').update({ instagram_username: instagramUsername, updated_at: new Date().toISOString() }).eq('client_id', clientId);
}

// --- Scraper session ---
async function getScraperSession(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb
    .from('cold_dm_scraper_sessions')
    .select('session_data, instagram_username')
    .eq('client_id', clientId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function saveScraperSession(clientId, sessionData, instagramUsername) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const { error } = await sb
    .from('cold_dm_scraper_sessions')
    .upsert(
      {
        client_id: clientId,
        session_data: sessionData,
        instagram_username: instagramUsername || null,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'client_id' }
    );
  if (error) throw error;
}

// --- Platform scraper sessions (rotation pool) ---
async function getPlatformScraperSessions() {
  const sb = getSupabase();
  if (!sb) return [];
  const { data, error } = await sb
    .from('cold_dm_platform_scraper_sessions')
    .select(
      'id, session_data, instagram_username, daily_actions_limit, account_state, cooldown_until, leased_until, leased_by_worker, risk_score, created_at, updated_at'
    )
    .order('id', { ascending: true });
  if (error) {
    const { data: fallback, error: fallbackErr } = await sb
      .from('cold_dm_platform_scraper_sessions')
      .select('id, session_data, instagram_username, daily_actions_limit')
      .order('id', { ascending: true });
    if (fallbackErr) return [];
    return (fallback || []).map((row) => ({
      ...row,
      account_state: 'active',
      cooldown_until: null,
      leased_until: null,
      leased_by_worker: null,
      risk_score: 0,
      created_at: row.created_at || null,
      updated_at: row.updated_at || null,
    }));
  }
  return data || [];
}

async function getPlatformScraperSessionById(id) {
  if (!id) return null;
  const sb = getSupabase();
  if (!sb) return null;
  const { data, error } = await sb
    .from('cold_dm_platform_scraper_sessions')
    .select('id, session_data, instagram_username, created_at, updated_at')
    .eq('id', id)
    .maybeSingle();
  if (error) return null;
  return data;
}

function computeLeaseUntil(leaseSec = 180) {
  const sec = Math.max(30, parseInt(leaseSec, 10) || 180);
  return new Date(Date.now() + sec * 1000).toISOString();
}

/**
 * Extract Puppeteer-style cookie array from JSONB (shape varies by VPS / proxy).
 * Supports: { cookies: [...] }, nested session.cookies, stringified JSON, raw array.
 */
function getPuppeteerCookiesFromSessionData(sessionData) {
  if (sessionData == null) return null;
  let sd = sessionData;
  if (typeof sd === 'string') {
    try {
      sd = JSON.parse(sd);
    } catch {
      return null;
    }
  }
  if (Array.isArray(sd)) return sd.length ? sd : null;
  if (typeof sd !== 'object') return null;
  if (Array.isArray(sd.cookies) && sd.cookies.length) return sd.cookies;
  if (sd.session && typeof sd.session === 'object' && Array.isArray(sd.session.cookies) && sd.session.cookies.length) {
    return sd.session.cookies;
  }
  return null;
}

/** Pool rows must have at least one usable cookie for Puppeteer (see getPuppeteerCookiesFromSessionData). */
function platformSessionHasPuppeteerCookies(s) {
  const c = getPuppeteerCookiesFromSessionData(s && s.session_data);
  return Array.isArray(c) && c.length > 0;
}

/** Normalize to { cookies } for page.setCookie / scraper checks. */
function normalizeSessionDataForPuppeteer(sessionData) {
  const cookies = getPuppeteerCookiesFromSessionData(sessionData);
  if (!cookies) return null;
  return { cookies };
}

function normalizePlatformSessionRowForPuppeteer(row) {
  if (!row) return null;
  const norm = normalizeSessionDataForPuppeteer(row.session_data);
  if (!norm) return null;
  return { ...row, session_data: norm };
}

/**
 * Count how many platform scraper sessions exist in the pool that are active
 * and have Puppeteer cookies.  Ignores transient lease / cooldown state so
 * the number reflects the true pool size (both primary and backup/overflow
 * sessions), not just what is free right now.  Used by the scrape-worker to
 * auto-set its concurrency limit.
 */
async function countActivePlatformScraperSessions() {
  const sessions = await getPlatformScraperSessions();
  return sessions.filter(
    (s) =>
      (s.account_state || 'active').toLowerCase() === 'active' &&
      platformSessionHasPuppeteerCookies(s)
  ).length;
}

/**
 * One-line summary for logs when reserve fails (empty pool, no cookies, all leased, etc.).
 */
async function describePlatformScraperPoolForLogs() {
  const sessions = await getPlatformScraperSessions();
  if (!sessions.length) {
    return 'pool: 0 rows in cold_dm_platform_scraper_sessions — add platform scrapers in admin';
  }
  const usage = await getPlatformScraperUsageToday(sessions.map((s) => s.id));
  const now = Date.now();
  const groups = new Map();
  let withCookies = 0;
  let eligible = 0;
  for (const s of sessions) {
    const key = normalizeInstagramKey(s.instagram_username) || String(s.id);
    const list = groups.get(key) || [];
    list.push(s);
    groups.set(key, list);
    if (platformSessionHasPuppeteerCookies(s)) withCookies += 1;
    const state = (s.account_state || 'active').toLowerCase();
    const okState = state === 'active';
    const okCd = !s.cooldown_until || new Date(s.cooldown_until).getTime() <= now;
    const okLease = !s.leased_until || new Date(s.leased_until).getTime() <= now;
    if (okState && platformSessionHasPuppeteerCookies(s) && okCd && okLease) eligible += 1;
  }
  const uniqueAccounts = groups.size;
  const overflowRows = Math.max(0, sessions.length - uniqueAccounts);
  return (
    `pool: ${sessions.length} row(s) across ${uniqueAccounts} account key(s), ${overflowRows} overflow row(s), ` +
      `${withCookies} with session_data.cookies (Puppeteer login), ${eligible} eligible for scrape ` +
      `(active + cookies + not leased). ` +
      (withCookies === 0
        ? 'Reconnect Instagram in admin Platform scrapers so Puppeteer saves cookies.'
        : eligible === 0
          ? 'All accounts may be leased, on cooldown, or not active.'
          : '')
  );
}

/**
 * Reserve one platform scraper account for a worker.
 * Best-effort atomic reservation via compare-and-swap update.
 */
async function reservePlatformScraperSessionForWorker(workerId, leaseSec = 180) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const fromRpc = await tryLeasePlatformScraperSessionViaRpc(workerId, leaseSec);
  if (fromRpc) return fromRpc;

  const nowIso = new Date().toISOString();
  const leaseUntil = computeLeaseUntil(leaseSec);
  const sessions = await getPlatformScraperSessions();
  if (!sessions.length) return null;
  const usage = await getPlatformScraperUsageToday(sessions.map((s) => s.id));
  const updatePayload = {
    leased_until: leaseUntil,
    leased_by_worker: workerId,
    lease_heartbeat_at: nowIso,
    updated_at: nowIso,
  };

  function sessionSortKey(s) {
    const createdAt = s.created_at ? new Date(s.created_at).getTime() : 0;
    return [createdAt, String(s.id || '')];
  }

  const groups = new Map();
  for (const s of sessions) {
    const key = normalizeInstagramKey(s.instagram_username) || String(s.id);
    const list = groups.get(key) || [];
    list.push(s);
    groups.set(key, list);
  }

  const sortRows = (rows) =>
    [...rows].sort((a, b) => {
      const aRisk = a.risk_score || 0;
      const bRisk = b.risk_score || 0;
      if (aRisk !== bRisk) return aRisk - bRisk;
      const aUsage = usage[a.id] || 0;
      const bUsage = usage[b.id] || 0;
      if (aUsage !== bUsage) return aUsage - bUsage;
      const [aCreated, aId] = sessionSortKey(a);
      const [bCreated, bId] = sessionSortKey(b);
      if (aCreated !== bCreated) return aCreated - bCreated;
      return String(aId).localeCompare(String(bId));
    });

  const eligibleRows = (rows) =>
    sortRows(
      rows.filter((s) => {
        const state = (s.account_state || 'active').toLowerCase();
        if (state !== 'active') return false;
        if (!platformSessionHasPuppeteerCookies(s)) return false;
        if (s.cooldown_until && new Date(s.cooldown_until).getTime() > Date.now()) return false;
        if (s.leased_until && new Date(s.leased_until).getTime() > Date.now()) return false;
        return true;
      })
    );

  const primaries = [];
  const overflow = [];
  for (const rows of groups.values()) {
    const sorted = sortRows(rows);
    if (sorted.length > 0) primaries.push(sorted[0]);
    if (sorted.length > 1) overflow.push(...sorted.slice(1));
  }

  const stageCandidates = [eligibleRows(primaries), eligibleRows(overflow)];

  for (const [stageIndex, stage] of stageCandidates.entries()) {
    for (const candidate of stage) {
      // Do not use a single .or(`leased_until.is.null,leased_until.lte.${nowIso}`): raw ISO in the filter
      // breaks PostgREST parsing (colons/dots), so the UPDATE matches 0 rows while JS still shows "eligible".
      const attempts = [
        { name: 'leased_until_is_null', build: (q) => q.is('leased_until', null) },
        { name: 'leased_until_lte_now', build: (q) => q.lte('leased_until', nowIso) },
      ];

      for (const attempt of attempts) {
        let query = sb
          .from('cold_dm_platform_scraper_sessions')
          .update(updatePayload)
          .eq('id', candidate.id);
        query = attempt.build(query);
        const { data, error } = await query.select('id, session_data, instagram_username, created_at').limit(1);

        if (error) {
          logPlatformScraperReserve('update failed', {
            workerId,
            candidateId: candidate.id,
            attempt: attempt.name,
            message: error.message,
            code: error.code,
            details: error.details,
            hint: error.hint,
          }, { always: true });
          continue;
        }

        const rowCount = data?.length ?? 0;
        if (rowCount > 0) {
          logPlatformScraperReserve('reserved session', {
            workerId,
            candidateId: candidate.id,
            attempt: attempt.name,
            stage: stageIndex === 0 ? 'primary' : 'overflow',
          });
          return {
            id: data[0].id,
            session_data: data[0].session_data,
            instagram_username: data[0].instagram_username,
          };
        }

        logPlatformScraperReserve('update matched 0 rows (lost race or stale eligible list)', {
          workerId,
          candidateId: candidate.id,
          attempt: attempt.name,
          nowIso,
        }, { always: true });
      }
    }
  }

  logPlatformScraperReserve('no session reserved after trying all eligible candidates', {
    workerId,
    primaryCount: stageCandidates[0].length,
    overflowCount: stageCandidates[1].length,
    candidateIds: [...stageCandidates[0], ...stageCandidates[1]].map((c) => c.id),
  }, { always: true });
  return null;
}

async function heartbeatPlatformScraperSessionLease(sessionId, workerId, leaseSec = 180) {
  const sb = getSupabase();
  if (!sb || !sessionId || !workerId) return false;
  const { data: rpcOk, error: rpcErr } = await sb.rpc('heartbeat_platform_scraper_session', {
    p_session_id: sessionId,
    p_worker_id: workerId,
    p_lease_seconds: leaseSec,
  });
  if (!rpcErr && rpcOk === true) return true;

  const nowIso = new Date().toISOString();
  const leaseUntil = computeLeaseUntil(leaseSec);
  const { data, error } = await sb
    .from('cold_dm_platform_scraper_sessions')
    .update({ leased_until: leaseUntil, lease_heartbeat_at: nowIso, updated_at: nowIso })
    .eq('id', sessionId)
    .eq('leased_by_worker', workerId)
    .select('id')
    .limit(1);
  return !error && !!(data && data.length > 0);
}

/**
 * Mid-scrape failure: cooldown session, error_events, requeue job (RPC). Best-effort.
 * Falls back to releasePlatformScraperSessionLease in scrape-worker when this returns ok:false.
 */
async function reportPlatformScraperScrapeFailure(
  jobId,
  platformSessionId,
  workerId,
  errorClass,
  errorMessage,
  opts = {}
) {
  const sb = getSupabase();
  if (!sb || !jobId || !platformSessionId || !workerId) {
    return { ok: false, error: 'missing args' };
  }
  const cooldownSec = opts.cooldownSec != null ? Number(opts.cooldownSec) : null;
  const cooldownMinutes =
    opts.cooldownMinutes != null
      ? Math.max(5, parseInt(opts.cooldownMinutes, 10) || 45)
      : cooldownSec != null && cooldownSec > 0
        ? Math.max(5, Math.ceil(cooldownSec / 60))
        : 45;
  const { data, error } = await sb.rpc('report_platform_scraper_scrape_failure', {
    p_scrape_job_id: jobId,
    p_platform_session_id: platformSessionId,
    p_worker_id: workerId,
    p_error_class: (errorClass && String(errorClass).slice(0, 120)) || 'unknown',
    p_error_message: (errorMessage && String(errorMessage).slice(0, 2000)) || '',
    p_cooldown_minutes: cooldownMinutes,
    p_quarantine_session: !!opts.quarantine,
  });
  if (error) return { ok: false, error: error.message };
  const row = data && typeof data === 'object' ? data : null;
  if (row && row.ok === true) return { ok: true, requeued: row.requeued, final_failure: row.final_failure };
  return { ok: false, error: (row && row.error) || 'rpc not ok' };
}

async function releasePlatformScraperSessionLease(sessionId, workerId, { cooldownSec = 0 } = {}) {
  const sb = getSupabase();
  if (!sb || !sessionId) return;
  const nowIso = new Date().toISOString();
  const cooldownUntil =
    cooldownSec > 0
      ? new Date(Date.now() + cooldownSec * 1000).toISOString()
      : null;
  const updatePayload = {
    leased_until: null,
    leased_by_worker: null,
    lease_heartbeat_at: nowIso,
    updated_at: nowIso,
    ...(cooldownUntil ? { cooldown_until: cooldownUntil } : {}),
  };
  let q = sb
    .from('cold_dm_platform_scraper_sessions')
    .update(updatePayload)
    .eq('id', sessionId);
  if (workerId) q = q.eq('leased_by_worker', workerId);
  await q;
}

async function markInstagramSessionWebNeedsRefresh(sessionId) {
  const sb = getSupabase();
  if (!sb || !sessionId) return;
  const nowIso = new Date().toISOString();
  await sb
    .from('cold_dm_instagram_sessions')
    .update({ web_session_needs_refresh: true, updated_at: nowIso })
    .eq('id', sessionId);
}

/**
 * Save refreshed Puppeteer session_data captured from a live browser (cookies + local/session storage).
 * Also clears web_session_needs_refresh because we just verified the session is currently usable.
 */
async function updateInstagramSessionSessionData(sessionId, sessionData) {
  const sb = getSupabase();
  if (!sb || !sessionId || !sessionData) return false;
  const nowIso = new Date().toISOString();
  const { error } = await sb
    .from('cold_dm_instagram_sessions')
    .update({
      session_data: sessionData,
      web_session_needs_refresh: false,
      updated_at: nowIso,
    })
    .eq('id', sessionId);
  return !error;
}

/**
 * Pause active campaigns that use this Instagram session (junction cold_dm_campaign_instagram_sessions).
 */
async function pauseActiveCampaignsForInstagramSession(clientId, instagramSessionId) {
  const sb = getSupabase();
  if (!sb || !clientId || !instagramSessionId) return { ok: false, paused: 0 };
  const { data: rows, error } = await sb
    .from('cold_dm_campaign_instagram_sessions')
    .select('campaign_id')
    .eq('instagram_session_id', instagramSessionId);
  if (error) {
    console.error('[pauseActiveCampaignsForInstagramSession] select', error.message);
    return { ok: false, paused: 0 };
  }
  const campaignIds = [...new Set((rows || []).map((r) => r.campaign_id).filter(Boolean))];
  if (campaignIds.length === 0) return { ok: true, paused: 0 };
  const { data: updatedRows, error: upErr } = await sb
    .from('cold_dm_campaigns')
    .update({ status: 'paused', updated_at: new Date().toISOString() })
    .eq('client_id', clientId)
    .eq('status', 'active')
    .in('id', campaignIds)
    .select('id');
  if (upErr) {
    console.error('[pauseActiveCampaignsForInstagramSession] update', upErr.message);
    return { ok: false, paused: 0 };
  }
  return { ok: true, paused: (updatedRows || []).length };
}

/** After Instagram shows password re-login (post-Continue), flag session, pause affected campaigns, surface status. */
async function handleInstagramPasswordReauthDisruption(clientId, instagramSessionId) {
  if (!clientId || !instagramSessionId) return;
  await markInstagramSessionWebNeedsRefresh(instagramSessionId).catch(() => {});
  const r = await pauseActiveCampaignsForInstagramSession(clientId, instagramSessionId).catch(() => ({
    paused: 0,
  }));
  const msg =
    'Automation session needs reconnect — Instagram asked for your password again. Open Settings → Integrations and tap Reconnect.' +
    (r && r.paused ? ` Paused ${r.paused} active campaign(s) tied to this account.` : '');
  await setClientStatusMessage(clientId, msg).catch(() => {});
}

async function markPlatformScraperWebNeedsRefresh(sessionId) {
  const sb = getSupabase();
  if (!sb || !sessionId) return;
  const nowIso = new Date().toISOString();
  await sb
    .from('cold_dm_platform_scraper_sessions')
    .update({ web_session_needs_refresh: true, updated_at: nowIso })
    .eq('id', sessionId);
}

/**
 * Prefer Postgres lease_platform_scraper_session (lowest today's usage first, SKIP LOCKED).
 * Validates Puppeteer cookies in JS (daily caps are not enforced).
 */
async function tryLeasePlatformScraperSessionViaRpc(workerId, leaseSec = 180) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const { data, error } = await sb.rpc('lease_platform_scraper_session', {
    p_worker_id: workerId,
    p_lease_seconds: leaseSec,
  });
  if (error) return null;
  const rows = Array.isArray(data) ? data : data != null ? [data] : [];
  const row = rows[0];
  if (!row?.id) return null;
  if (!platformSessionHasPuppeteerCookies(row)) {
    await releasePlatformScraperSessionLease(row.id, workerId, { cooldownSec: 0 });
    return null;
  }
  return {
    id: row.id,
    session_data: row.session_data,
    instagram_username: row.instagram_username,
  };
}

async function getPlatformScraperUsageToday(sessionIds) {
  if (!sessionIds || sessionIds.length === 0) return {};
  const sb = getSupabase();
  if (!sb) return {};
  const today = new Date().toISOString().slice(0, 10);
  const { data, error } = await sb
    .from('cold_dm_scraper_daily_usage')
    .select('platform_scraper_session_id, actions_count')
    .in('platform_scraper_session_id', sessionIds)
    .eq('usage_date', today);
  if (error) return {};
  const map = {};
  for (const row of data || []) {
    map[row.platform_scraper_session_id] = row.actions_count || 0;
  }
  return map;
}

async function pickScraperSessionForJob(clientId) {
  void clientId;
  const sessions = await getPlatformScraperSessions();
  if (sessions.length === 0) return null;
  const sessionIds = sessions.map((s) => s.id);
  const usage = await getPlatformScraperUsageToday(sessionIds);
  const candidates = sessions.filter((s) => platformSessionHasPuppeteerCookies(s));
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => (usage[a.id] || 0) - (usage[b.id] || 0));
  const picked = candidates[0];
  return {
    source: 'platform',
    session: { session_data: picked.session_data, instagram_username: picked.instagram_username },
    platformSessionId: picked.id,
  };
}

/** Adds `count` to today's cold_dm_scraper_daily_usage for this platform session. Count is leads actually scraped (job completion), not abstract "actions". */
async function recordScraperActions(platformSessionId, count) {
  if (!platformSessionId || count <= 0) return;
  const sb = getSupabase();
  if (!sb) return;
  const today = new Date().toISOString().slice(0, 10);
  for (let attempt = 0; attempt < 5; attempt++) {
    const { data: existing, error: getErr } = await sb
      .from('cold_dm_scraper_daily_usage')
      .select('id, actions_count')
      .eq('platform_scraper_session_id', platformSessionId)
      .eq('usage_date', today)
      .maybeSingle();
    if (getErr) throw getErr;

    if (!existing) {
      const { error: insertErr } = await sb.from('cold_dm_scraper_daily_usage').upsert(
        {
          platform_scraper_session_id: platformSessionId,
          usage_date: today,
          actions_count: count,
        },
        { onConflict: 'platform_scraper_session_id,usage_date', ignoreDuplicates: true }
      );
      if (insertErr) throw insertErr;
      continue;
    }

    const { data: updated, error: updateErr } = await sb
      .from('cold_dm_scraper_daily_usage')
      .update({ actions_count: existing.actions_count + count, updated_at: new Date().toISOString() })
      .eq('id', existing.id)
      .eq('actions_count', existing.actions_count)
      .select('id')
      .limit(1);
    if (updateErr) throw updateErr;
    if (updated && updated.length > 0) return;
  }
  throw new Error('Failed to atomically update scraper daily usage after retries');
}

async function savePlatformScraperSession(sessionData, instagramUsername, dailyActionsLimit = 500, opts = {}) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');
  const username = (instagramUsername || '').trim().replace(/^@/, '').toLowerCase();
  if (!username) throw new Error('Instagram username required');
  const limit = Math.max(1, parseInt(dailyActionsLimit, 10) || 500);
  const forceInsert = opts && (opts.forceInsert === true || opts.allowDuplicateUsername === true);
  const { data, error } = await sb.rpc('service_upsert_platform_scraper_from_connect', {
    p_username: username,
    p_session_data: sessionData,
    p_daily_limit: limit,
    p_backup_slot: !!forceInsert,
  });
  if (error) throw error;
  const row = data && typeof data === 'object' ? data : null;
  return row && row.id ? row.id : null;
}

// --- Scrape jobs ---
async function createScrapeJob(
  clientId,
  targetUsername,
  leadGroupId = null,
  scrapeType = 'followers',
  postUrls = null,
  platformScraperSessionId = null,
  maxLeads = null,
  scrapeMethod = 'instagrapi'
) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const payload = {
    client_id: clientId,
    target_username: targetUsername,
    status: 'pending',
    scraped_count: 0,
    started_at: new Date().toISOString(),
  };
  if (leadGroupId) payload.lead_group_id = leadGroupId;
  if (scrapeType) payload.scrape_type = scrapeType;
  if (postUrls && Array.isArray(postUrls) && postUrls.length) payload.post_urls = postUrls;
  if (platformScraperSessionId) payload.platform_scraper_session_id = platformScraperSessionId;
   // max_leads is an optional column on cold_dm_scrape_jobs used by the Python worker
  if (maxLeads != null && !Number.isNaN(Number(maxLeads))) {
    payload.max_leads = Number(maxLeads);
  }
  if (scrapeMethod) {
    payload.scrape_method = scrapeMethod;
  }
  const { data, error } = await sb
    .from('cold_dm_scrape_jobs')
    .insert(payload)
    .select('id')
    .single();
  if (error) throw error;
  return data?.id;
}

function formatRelativeDuration(ms) {
  const safeMs = Math.max(0, Number(ms) || 0);
  const totalMinutes = Math.ceil(safeMs / 60000);
  if (totalMinutes <= 1) return 'less than 1 minute';
  const days = Math.floor(totalMinutes / (60 * 24));
  const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
  const minutes = totalMinutes % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 && days === 0) parts.push(`${minutes}m`);
  return parts.join(' ');
}

function scrapeQuotaReachedMessage(resetInText) {
  return `1000 leads maximum reached, please wait for your scraping usage to reset in ${resetInText}.`;
}

function applyScrapedLeadSourceFilter(query) {
  return query.or('source.ilike.followers:%,source.ilike.following:%,source.ilike.comments:%');
}

async function getScrapeQuotaStatus(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const limit = 1000;
  const windowMs = 7 * 24 * 60 * 60 * 1000;
  const nowMs = Date.now();
  const windowStartIso = new Date(nowMs - windowMs).toISOString();

  let countQuery = sb
    .from('cold_dm_leads')
    .select('id', { count: 'exact', head: true })
    .eq('client_id', clientId)
    .gte('added_at', windowStartIso);
  countQuery = applyScrapedLeadSourceFilter(countQuery);
  const { count, error: countErr } = await countQuery;
  if (countErr) throw countErr;
  const used = Math.max(0, Number(count || 0));
  const remaining = Math.max(0, limit - used);

  let resetAtIso = null;
  if (used > 0) {
    let oldestQuery = sb
      .from('cold_dm_leads')
      .select('added_at')
      .eq('client_id', clientId)
      .gte('added_at', windowStartIso)
      .order('added_at', { ascending: true })
      .limit(1)
      .maybeSingle();
    oldestQuery = applyScrapedLeadSourceFilter(oldestQuery);
    const { data: oldestRow, error: oldestErr } = await oldestQuery;
    if (oldestErr) throw oldestErr;
    const oldestAddedAt = oldestRow?.added_at ? new Date(oldestRow.added_at).getTime() : NaN;
    if (Number.isFinite(oldestAddedAt)) {
      resetAtIso = new Date(oldestAddedAt + windowMs).toISOString();
    }
  }
  const resetInMs = resetAtIso ? Math.max(0, new Date(resetAtIso).getTime() - nowMs) : 0;
  const resetInText = formatRelativeDuration(resetInMs);
  return {
    limit,
    used,
    remaining,
    resetAtIso,
    resetInMs,
    resetInText,
    message: scrapeQuotaReachedMessage(resetInText),
  };
}

async function updateScrapeJob(jobId, updates) {
  const sb = getSupabase();
  if (!sb || !jobId) throw new Error('Supabase or jobId missing');
  const payload = { ...updates };
  if (payload.status === 'completed' || payload.status === 'failed' || payload.status === 'cancelled') {
    payload.finished_at = new Date().toISOString();
    payload.leased_until = null;
    payload.leased_by_worker = null;
    payload.available_at = payload.available_at || new Date().toISOString();
  }
  if (payload.status === 'retry' || payload.status === 'pending') {
    payload.leased_until = null;
    payload.leased_by_worker = null;
    payload.finished_at = null;
    payload.available_at = payload.available_at || new Date().toISOString();
  }
  const { error } = await sb.from('cold_dm_scrape_jobs').update(payload).eq('id', jobId);
  if (error) throw error;
}

async function getScrapeJob(jobId) {
  const sb = getSupabase();
  if (!sb || !jobId) return null;
  const { data, error } = await sb.from('cold_dm_scrape_jobs').select('*').eq('id', jobId).maybeSingle();
  if (error) throw error;
  return data;
}

async function getLatestScrapeJob(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const { data, error } = await sb
    .from('cold_dm_scrape_jobs')
    .select('id, target_username, status, scraped_count')
    .eq('client_id', clientId)
    .order('started_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function cancelScrapeJob(clientId, jobId) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  if (jobId) {
    const { error } = await sb
      .from('cold_dm_scrape_jobs')
      .update({
        status: 'cancelled',
        finished_at: new Date().toISOString(),
        leased_until: null,
        leased_by_worker: null,
      })
      .eq('id', jobId)
      .eq('client_id', clientId);
    if (error) throw error;
    return true;
  }
  const { data: running } = await sb
    .from('cold_dm_scrape_jobs')
    .select('id')
    .eq('client_id', clientId)
    .in('status', ['running', 'pending', 'leased', 'retry'])
    .order('started_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (running?.id) {
    const { error } = await sb
      .from('cold_dm_scrape_jobs')
      .update({ status: 'cancelled', finished_at: new Date().toISOString(), leased_until: null, leased_by_worker: null })
      .eq('id', running.id);
    if (error) throw error;
    return true;
  }
  return false;
}

/**
 * Atomically claim next pending scrape job (Postgres SKIP LOCKED via RPC when deployed).
 */
async function claimColdDmScrapeJob(workerId, leaseSeconds = 240) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const { data, error } = await sb.rpc('claim_cold_dm_scrape_job', {
    p_worker_id: workerId,
    p_lease_seconds: leaseSeconds,
  });
  if (!error) {
    const rows = Array.isArray(data) ? data : data != null ? [data] : [];
    if (rows.length > 0) return rows[0];
    return null;
  }
  return claimColdDmScrapeJobFallback(workerId, leaseSeconds);
}

async function retryScrapeJob(jobId, errorMessage = null, delaySeconds = 60, workerId = null) {
  const sb = getSupabase();
  if (!sb || !jobId) throw new Error('Supabase or jobId missing');
  const payload = {
    status: 'retry',
    error_message: errorMessage || 'retry_scheduled',
    available_at: computeAvailableAtIso(delaySeconds),
    leased_until: null,
    leased_by_worker: null,
    lease_heartbeat_at: new Date().toISOString(),
    finished_at: null,
  };
  let q = sb.from('cold_dm_scrape_jobs').update(payload).eq('id', jobId);
  if (workerId) q = q.eq('leased_by_worker', workerId);
  const { error } = await q;
  if (error) throw error;
}

/** Best-effort claim when RPC is missing or races (no SKIP LOCKED). */
async function claimColdDmScrapeJobFallback(workerId, leaseSeconds = 240) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const { data: pending } = await sb
    .from('cold_dm_scrape_jobs')
    .select('id, attempt_count')
    .eq('status', 'pending')
    .order('started_at', { ascending: true })
    .limit(1)
    .maybeSingle();
  if (!pending?.id) return null;
  const leaseUntil = new Date(Date.now() + Math.max(30, leaseSeconds) * 1000).toISOString();
  const nowIso = new Date().toISOString();
  const nextAttempt = (pending.attempt_count || 0) + 1;
  const { data: updated, error } = await sb
    .from('cold_dm_scrape_jobs')
    .update({
      status: 'running',
      leased_by_worker: workerId,
      leased_until: leaseUntil,
      lease_heartbeat_at: nowIso,
      attempt_count: nextAttempt,
    })
    .eq('id', pending.id)
    .eq('status', 'pending')
    .select('*')
    .maybeSingle();
  if (error || !updated) return null;
  return updated;
}

async function heartbeatScrapeJobLease(jobId, workerId, leaseSeconds = 240) {
  const sb = getSupabase();
  if (!sb || !jobId || !workerId) return false;
  const { data, error } = await sb.rpc('heartbeat_cold_dm_scrape_job', {
    p_job_id: jobId,
    p_worker_id: workerId,
    p_lease_seconds: leaseSeconds,
  });
  if (!error && data === true) return true;
  if (!error && data === false) return false;
  const leaseUntil = new Date(Date.now() + Math.max(30, leaseSeconds) * 1000).toISOString();
  const { data: rows, error: uerr } = await sb
    .from('cold_dm_scrape_jobs')
    .update({ leased_until: leaseUntil, lease_heartbeat_at: new Date().toISOString() })
    .eq('id', jobId)
    .eq('leased_by_worker', workerId)
    .eq('status', 'running')
    .select('id');
  return !uerr && rows && rows.length > 0;
}

/**
 * Insert idempotency key for VPS routes (follow-up dedupe on Edge retries).
 * @returns {Promise<boolean>} true if this is the first time (caller should proceed), false if duplicate.
 */
async function tryVpsIdempotencyOnce(clientId, route, idempotencyKey) {
  const sb = getSupabase();
  if (!sb || !clientId || !route || !idempotencyKey) return true;
  const key = String(idempotencyKey).trim().slice(0, 256);
  if (!key) return true;
  const { error } = await sb.from('cold_dm_vps_idempotency').insert({
    client_id: clientId,
    route: String(route).slice(0, 120),
    idempotency_key: key,
  });
  if (error && error.code === '23505') return false;
  if (error) {
    if (String(error.message || '').includes('cold_dm_vps_idempotency') || error.code === '42P01') {
      return true;
    }
    throw error;
  }
  return true;
}

async function workerHeartbeat(workerId, workerType, meta = {}) {
  const sb = getSupabase();
  if (!sb || !workerId || !workerType) return;
  try {
    const os = require('os');
    await sb.from('cold_dm_worker_heartbeats').upsert(
      {
        worker_id: workerId,
        worker_type: workerType,
        host: os.hostname(),
        meta: meta || {},
        last_seen_at: new Date().toISOString(),
      },
      { onConflict: 'worker_id' }
    );
  } catch (e) {
    /* table may not exist until migration */
  }
}

async function createSendJob({
  clientId,
  campaignId,
  campaignLeadId,
  instagramSessionId = null,
  username,
  payload = {},
  priority = 100,
  availableAt = null,
  idempotencyKey = null,
}) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignLeadId || !username) throw new Error('Missing createSendJob params');
  const row = {
    client_id: clientId,
    campaign_id: campaignId || null,
    campaign_lead_id: campaignLeadId,
    instagram_session_id: instagramSessionId,
    username: normalizeUsername(username),
    payload: payload || {},
    priority: Number.isFinite(Number(priority)) ? Number(priority) : 100,
    available_at: availableAt || new Date().toISOString(),
    idempotency_key: idempotencyKey || `campaign-lead:${campaignLeadId}`,
    status: 'pending',
  };
  const { data, error } = await sb
    .from('cold_dm_send_jobs')
    .upsert(row, {
      onConflict: 'client_id,idempotency_key',
      ignoreDuplicates: true,
    })
    .select('id')
    .limit(1);
  if (error) throw error;
  return Array.isArray(data) && data[0]?.id ? data[0].id : null;
}

async function updateSendJob(jobId, updates, workerId = null) {
  const sb = getSupabase();
  if (!sb || !jobId) throw new Error('Supabase or jobId missing');
  const payload = { ...updates, updated_at: new Date().toISOString() };
  if (payload.status === 'completed' || payload.status === 'failed' || payload.status === 'cancelled') {
    payload.finished_at = new Date().toISOString();
    payload.leased_until = null;
    payload.leased_by_worker = null;
  }
  if (payload.status === 'retry' || payload.status === 'pending') {
    payload.finished_at = null;
    payload.leased_until = null;
    payload.leased_by_worker = null;
    payload.available_at = payload.available_at || new Date().toISOString();
  }
  let q = sb.from('cold_dm_send_jobs').update(payload).eq('id', jobId);
  if (workerId) q = q.eq('leased_by_worker', workerId);
  const { error } = await q;
  if (error) throw error;
}

/**
 * Stamp available_at = cooldownUntilIso on all pending/retry jobs for a campaign
 * (excluding the just-completed job). This lets workers immediately claim work
 * from other clients rather than sleeping — the per-campaign cooldown is enforced
 * via the DB field rather than a blocking sleep in the worker process.
 */
async function deferCampaignPendingJobs(campaignId, excludeJobId, cooldownUntilIso) {
  const sb = getSupabase();
  if (!sb || !campaignId) return;
  try {
    let q = sb
      .from('cold_dm_send_jobs')
      .update({ available_at: cooldownUntilIso, updated_at: new Date().toISOString() })
      .eq('campaign_id', campaignId)
      .in('status', ['pending', 'retry'])
      .lt('available_at', cooldownUntilIso); // only push forward, never pull back
    if (excludeJobId) q = q.neq('id', excludeJobId);
    const { error } = await q;
    if (error) console.warn('[deferCampaignPendingJobs] update error:', error.message || error);
  } catch (e) {
    console.warn('[deferCampaignPendingJobs] exception:', e.message || e);
  }
}

async function getSendJob(jobId) {
  const sb = getSupabase();
  if (!sb || !jobId) return null;
  const { data, error } = await sb.from('cold_dm_send_jobs').select('*').eq('id', jobId).maybeSingle();
  if (error) throw error;
  return data;
}

async function claimColdDmSendJob(workerId, leaseSeconds = 240, campaignIds = null) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const rpcTimeoutMs = Math.max(3000, parseInt(process.env.CLAIM_SEND_JOB_RPC_TIMEOUT_MS || '12000', 10) || 12000);
  const rpcPayload = {
    p_worker_id: workerId,
    p_lease_seconds: leaseSeconds,
  };
  if (Array.isArray(campaignIds) && campaignIds.length > 0) {
    rpcPayload.p_campaign_ids = campaignIds;
  }
  for (let attempt = 0; attempt < 8; attempt++) {
    let data;
    let error;
    try {
      const rpcResult = await withTimeout(
        sb.rpc('claim_cold_dm_send_job', rpcPayload),
        rpcTimeoutMs,
        `claim_cold_dm_send_job timed out after ${rpcTimeoutMs}ms`
      );
      data = rpcResult?.data;
      error = rpcResult?.error;
    } catch (rpcTimeoutErr) {
      console.error('[claimColdDmSendJob] RPC timeout, using fallback:', rpcTimeoutErr.message || rpcTimeoutErr);
      logColdDmConcurrencyDebug('rpc_claim_timeout_fallback', {
        workerId,
        leaseSeconds,
        attempt,
        timeoutMs: rpcTimeoutMs,
        error: String(rpcTimeoutErr?.message || rpcTimeoutErr || 'timeout'),
      });
      return claimColdDmSendJobFallback(workerId, leaseSeconds, campaignIds);
    }
    if (error) {
      console.error('[claimColdDmSendJob] RPC error, using fallback:', error.message || error);
      return claimColdDmSendJobFallback(workerId, leaseSeconds, campaignIds);
    }
    const rows = Array.isArray(data) ? data : data != null ? [data] : [];
    const claimed = rows.length > 0 ? rows[0] : null;
    if (!claimed) {
      const nowIso = new Date().toISOString();
      const { data: pendingCheck } = await sb
        .from('cold_dm_send_jobs')
        .select('id, status, available_at')
        .in('status', ['pending', 'retry'])
        .lte('available_at', nowIso)
        .order('created_at', { ascending: true })
        .limit(5);
      if (pendingCheck?.length) {
        console.error(
          `[claimColdDmSendJob] RPC returned 0 rows but ${pendingCheck.length} ready pending/retry jobs exist. ` +
            `First: id=${pendingCheck[0].id} status=${pendingCheck[0].status} available_at=${pendingCheck[0].available_at} now=${nowIso}`
        );
        logColdDmConcurrencyDebug('rpc_claim_empty_with_ready_jobs', {
          workerId,
          leaseSeconds,
          readyJobs: pendingCheck.length,
          firstJobId: pendingCheck[0]?.id || null,
          firstJobStatus: pendingCheck[0]?.status || null,
          firstJobAvailableAt: pendingCheck[0]?.available_at || null,
          nowIso,
        });
        return claimColdDmSendJobFallback(workerId, leaseSeconds, campaignIds);
      }
      return null;
    }
    if (!claimed.campaign_id) return claimed;
    const lockOk = await claimCampaignSendLease(claimed.campaign_id, workerId, leaseSeconds);
    if (lockOk) return claimed;
    const retryAt = new Date(Date.now() + (5 + Math.floor(Math.random() * 10)) * 1000).toISOString();
    await updateSendJob(
      claimed.id,
      {
        status: 'retry',
        available_at: retryAt,
        last_error_class: 'campaign_locked',
        last_error_message: 'campaign_locked',
      },
      workerId
    ).catch(() => {});
  }
  return null;
}

async function claimColdDmSendJobFallback(workerId, leaseSeconds = 240, campaignIds = null) {
  const sb = getSupabase();
  if (!sb || !workerId) return null;
  const nowIso = new Date().toISOString();
  let pendingQuery = sb
    .from('cold_dm_send_jobs')
    .select('id, client_id, campaign_id, campaign_lead_id, username, attempt_count')
    .in('status', ['pending', 'retry'])
    .lte('available_at', nowIso);
  if (Array.isArray(campaignIds) && campaignIds.length > 0) {
    pendingQuery = pendingQuery.in('campaign_id', campaignIds);
  }
  const { data: pendingRows } = await pendingQuery
    .order('available_at', { ascending: true })
    .order('priority', { ascending: true })
    .order('created_at', { ascending: true })
    .limit(20);
  const pending = Array.isArray(pendingRows) ? pendingRows : [];
  if (!pending.length) return null;
  const buckets = new Map();
  for (const row of pending) {
    const key = row.client_id || '__null__';
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(row);
  }
  const interleaved = [];
  while (true) {
    let progressed = false;
    for (const queue of buckets.values()) {
      if (!queue.length) continue;
      interleaved.push(queue.shift());
      progressed = true;
    }
    if (!progressed) break;
  }
  logColdDmConcurrencyDebug('fallback_candidates_ordered', {
    workerId,
    leaseSeconds,
    fetched: pending.length,
    uniqueClients: [...new Set(pending.map((r) => r.client_id).filter(Boolean))].length,
    firstCandidates: interleaved.slice(0, 10).map((r) => ({
      id: r.id,
      clientId: r.client_id || null,
      campaignId: r.campaign_id || null,
    })),
  });
  for (const candidate of interleaved) {
    const campaignId = candidate.campaign_id || null;
    if (campaignId) {
      const lockOk = await claimCampaignSendLease(campaignId, workerId, leaseSeconds);
      if (!lockOk) continue;
    }
    const leaseUntil = new Date(Date.now() + Math.max(30, leaseSeconds) * 1000).toISOString();
    const nextAttempt = (candidate.attempt_count || 0) + 1;
    const { data: updated, error } = await sb
      .from('cold_dm_send_jobs')
      .update({
        status: 'running',
        leased_by_worker: workerId,
        leased_until: leaseUntil,
        lease_heartbeat_at: nowIso,
        attempt_count: nextAttempt,
        updated_at: nowIso,
      })
      .eq('id', candidate.id)
      .in('status', ['pending', 'retry'])
      .select('*')
      .maybeSingle();
    if (!error && updated) {
      logColdDmConcurrencyDebug('fallback_claim_ok', {
        workerId,
        leaseSeconds,
        jobId: updated.id || candidate.id,
        clientId: updated.client_id || candidate.client_id || null,
        campaignId: updated.campaign_id || candidate.campaign_id || null,
        campaignLeadId: updated.campaign_lead_id || candidate.campaign_lead_id || null,
        username: updated.username || candidate.username || null,
      });
      return updated;
    }
    if (campaignId) await releaseCampaignSendLease(campaignId, workerId).catch(() => {});
  }
  return null;
}

async function heartbeatSendJobLease(jobId, workerId, leaseSeconds = 240, campaignId = null) {
  const sb = getSupabase();
  if (!sb || !jobId || !workerId) return false;
  const { data, error } = await sb.rpc('heartbeat_cold_dm_send_job', {
    p_job_id: jobId,
    p_worker_id: workerId,
    p_lease_seconds: leaseSeconds,
  });
  if (!error && data === true) {
    if (campaignId) await heartbeatCampaignSendLease(campaignId, workerId, leaseSeconds).catch(() => {});
    return true;
  }
  if (!error && data === false) return false;
  const leaseUntil = new Date(Date.now() + Math.max(30, leaseSeconds) * 1000).toISOString();
  const { data: rows, error: uerr } = await sb
    .from('cold_dm_send_jobs')
    .update({ leased_until: leaseUntil, lease_heartbeat_at: new Date().toISOString(), updated_at: new Date().toISOString() })
    .eq('id', jobId)
    .eq('leased_by_worker', workerId)
    .eq('status', 'running')
    .select('id');
  const ok = !uerr && rows && rows.length > 0;
  if (ok && campaignId) await heartbeatCampaignSendLease(campaignId, workerId, leaseSeconds).catch(() => {});
  return ok;
}

async function syncSendJobsForCampaign(clientId, campaignId) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignId) return 0;
  await addCampaignLeadsFromGroups(clientId, campaignId).catch(() => 0);
  const { data: campaign } = await sb
    .from('cold_dm_campaigns')
    .select('id, status, timezone, schedule_start_time, schedule_end_time')
    .eq('client_id', clientId)
    .eq('id', campaignId)
    .maybeSingle();
  if (!campaign) return 0;
  if (campaign.status !== 'active') {
    const { count: stillPending } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', campaignId)
      .eq('status', 'pending');
    if ((stillPending ?? 0) > 0) {
      await sb.from('cold_dm_campaigns').update({ status: 'active', updated_at: new Date().toISOString() }).eq('id', campaignId);
      console.log(`[syncSendJobsForCampaign] reactivated campaign ${campaignId} (was ${campaign.status}, ${stillPending} pending leads)`);
    } else {
      return 0;
    }
  }

  const { data: leadRows, error: leadErr } = await sb
    .from('cold_dm_campaign_leads')
    .select('id, lead_id, status')
    .eq('campaign_id', campaignId)
    .eq('status', 'pending');
  if (leadErr) throw leadErr;
  if (!leadRows?.length) return 0;

  const leadIds = leadRows.map((r) => r.lead_id).filter(Boolean);
  if (leadIds.length === 0) return 0;
  // Chunk large IN filters to avoid PostgREST "Bad Request" on long query strings.
  const leads = [];
  // Keep URL/query length below PostgREST header/url limits when using .in('id', [...uuid]).
  // UUID filters can overflow around a few hundred ids depending on URL/base headers.
  const leadChunkSize = Math.max(
    20,
    Math.min(120, parseInt(process.env.SEND_SYNC_LEAD_ID_CHUNK || '80', 10) || 80)
  );
  for (let i = 0; i < leadIds.length; i += leadChunkSize) {
    const chunk = leadIds.slice(i, i + leadChunkSize);
    const { data: part, error: leadsErr } = await sb
      .from('cold_dm_leads')
      .select('id, username')
      .eq('client_id', clientId)
      .in('id', chunk);
    if (leadsErr) {
      const wrapped = new Error(`[syncSendJobsForCampaign:load_leads_chunk] ${leadsErr.message || leadsErr}`);
      wrapped.code = leadsErr.code || null;
      wrapped.details = leadsErr.details || null;
      wrapped.hint = leadsErr.hint || null;
      throw wrapped;
    }
    if (part?.length) leads.push(...part);
  }
  const usernameByLeadId = new Map((leads || []).map((r) => [r.id, r.username]));

  let existingJobs = [];
  const { data: existingWithLease, error: existingErr } = await sb
    .from('cold_dm_send_jobs')
    .select('id, campaign_lead_id, status, last_error_class, leased_until, lease_heartbeat_at')
    .eq('client_id', clientId)
    .eq('campaign_id', campaignId);
  if (!existingErr) {
    existingJobs = existingWithLease || [];
  } else if (isMissingStandardLeaseColumnsError(existingErr) || String(existingErr.message || '').toLowerCase() === 'bad request') {
    // Backward compatibility if lease columns are missing in this DB.
    const { data: existingLegacy, error: legacyErr } = await sb
      .from('cold_dm_send_jobs')
      .select('id, campaign_lead_id, status, last_error_class')
      .eq('client_id', clientId)
      .eq('campaign_id', campaignId);
    if (legacyErr) throw legacyErr;
    existingJobs = existingLegacy || [];
  } else {
    throw existingErr;
  }

  const { data: campaignLeadIdRows, error: campaignLeadIdsErr } = await sb
    .from('cold_dm_campaign_leads')
    .select('id')
    .eq('campaign_id', campaignId);
  if (campaignLeadIdsErr) throw campaignLeadIdsErr;
  const validCampaignLeadIds = new Set((campaignLeadIdRows || []).map((r) => r.id).filter(Boolean));
  const deadJobIds = [];
  for (const j of existingJobs || []) {
    if (!['pending', 'retry', 'running'].includes(j.status)) continue;
    if (!j.campaign_lead_id || !validCampaignLeadIds.has(j.campaign_lead_id)) deadJobIds.push(j.id);
  }
  if (deadJobIds.length > 0) {
    for (let i = 0; i < deadJobIds.length; i += 200) {
      const batch = deadJobIds.slice(i, i + 200);
      await sb.from('cold_dm_send_jobs').delete().in('id', batch);
    }
    console.log(
      `[syncSendJobsForCampaign] removed ${deadJobIds.length} orphan/unresolvable send job(s) for campaign ${campaignId} (null campaign_lead_id or deleted campaign_lead)`
    );
    existingJobs = (existingJobs || []).filter((j) => !deadJobIds.includes(j.id));
  }

  const activeLeadIds = new Set();
  const staleJobIds = [];
  const staleRunningJobIds = [];
  const nowMs = Date.now();
  const staleRunningHeartbeatMs =
    Math.max(45, parseInt(process.env.SEND_JOB_RUNNING_STALE_SEC || '180', 10) || 180) * 1000;
  const scheduleTz = campaign.timezone ?? null;
  const nextAvailableAt = isWithinSchedule(campaign.schedule_start_time, campaign.schedule_end_time, scheduleTz)
    ? new Date().toISOString()
    : (getNextScheduleStartInTimezone(campaign.schedule_start_time, scheduleTz)?.toISOString() ?? new Date(Date.now() + 15 * 60 * 1000).toISOString());
  for (const j of existingJobs || []) {
    if (!j.campaign_lead_id) continue;
    if (j.status === 'running') {
      const leaseUntilMs = j.leased_until ? Date.parse(j.leased_until) : NaN;
      const hbMs = j.lease_heartbeat_at ? Date.parse(j.lease_heartbeat_at) : NaN;
      const leaseExpired = Number.isFinite(leaseUntilMs) ? leaseUntilMs <= nowMs : true;
      const heartbeatStale = Number.isFinite(hbMs) ? hbMs < nowMs - staleRunningHeartbeatMs : true;
      if (leaseExpired || heartbeatStale) {
        staleRunningJobIds.push(j.id);
        continue;
      }
    }
    if (['pending', 'running', 'retry'].includes(j.status)) {
      activeLeadIds.add(j.campaign_lead_id);
    } else {
      staleJobIds.push(j.id);
    }
  }

  if (staleRunningJobIds.length > 0) {
    const nowIso = new Date().toISOString();
    const fullReset = await sb
      .from('cold_dm_send_jobs')
      .update({
        status: 'retry',
        available_at: nowIso,
        leased_until: null,
        leased_by_worker: null,
        lease_heartbeat_at: nowIso,
        last_error_class: 'stale_running_requeued',
        last_error_message: 'stale_running_requeued',
        updated_at: nowIso,
      })
      .in('id', staleRunningJobIds);
    if (fullReset?.error && isMissingStandardLeaseColumnsError(fullReset.error)) {
      await sb
        .from('cold_dm_send_jobs')
        .update({
          status: 'retry',
          available_at: nowIso,
          last_error_class: 'stale_running_requeued',
          last_error_message: 'stale_running_requeued',
          updated_at: nowIso,
        })
        .in('id', staleRunningJobIds);
    }
    console.log(
      `[syncSendJobsForCampaign] requeued ${staleRunningJobIds.length} stale running send job(s) for campaign ${campaignId}`
    );
  }

  if (staleJobIds.length > 0) {
    for (let i = 0; i < staleJobIds.length; i += 200) {
      const batch = staleJobIds.slice(i, i + 200);
      await sb.from('cold_dm_send_jobs').delete().in('id', batch);
    }
    console.log(`[syncSendJobsForCampaign] cleaned ${staleJobIds.length} stale send job(s) for campaign ${campaignId}`);
  }

  const requeueableJobIds = (existingJobs || [])
    .filter((j) => j.status === 'retry' && j.last_error_class === 'outside_schedule')
    .map((j) => j.id)
    .filter(Boolean);
  if (requeueableJobIds.length > 0) {
    await sb
      .from('cold_dm_send_jobs')
      .update({
        available_at: nextAvailableAt,
        updated_at: new Date().toISOString(),
      })
      .in('id', requeueableJobIds);
    console.log(
      `[syncSendJobsForCampaign] rescheduled ${requeueableJobIds.length} outside_schedule job(s) for campaign ${campaignId} to ${nextAvailableAt}`
    );
  }

  const rows = [];
  for (const row of leadRows) {
    if (!row?.id || activeLeadIds.has(row.id)) continue;
    const username = usernameByLeadId.get(row.lead_id);
    if (!username) continue;
    rows.push({
      client_id: clientId,
      campaign_id: campaignId,
      campaign_lead_id: row.id,
      username: normalizeUsername(username),
      payload: {},
      status: 'pending',
      priority: 100,
      available_at: new Date().toISOString(),
      idempotency_key: `campaign-lead:${row.id}`,
    });
  }
  if (rows.length === 0) return 0;
  let inserted = 0;
  // 1) Preferred path: idempotent upsert on unique index.
  const upsertRes = await sb.from('cold_dm_send_jobs').upsert(rows, {
    onConflict: 'client_id,idempotency_key',
    ignoreDuplicates: false,
  });
  if (!upsertRes.error) {
    inserted = rows.length;
  } else {
    const upMsg = String(upsertRes.error?.message || '').toLowerCase();
    const canFallbackInsert =
      upsertRes.error?.code === '42P10' || // conflict index mismatch
      upsertRes.error?.code === '42703' || // missing column
      upMsg === 'bad request' ||
      upMsg.includes('does not exist');
    if (!canFallbackInsert) {
      throw upsertRes.error;
    }
    console.error(
      '[syncSendJobsForCampaign] upsert failed; falling back to insert strategies',
      JSON.stringify({
        code: upsertRes.error?.code || null,
        message: upsertRes.error?.message || String(upsertRes.error),
        details: upsertRes.error?.details || null,
      })
    );

    // 2) Insert with idempotency_key (ignore duplicates manually).
    let insertedViaIdempotency = 0;
    for (const row of rows) {
      const r = await sb.from('cold_dm_send_jobs').insert(row);
      if (!r.error) {
        insertedViaIdempotency += 1;
        continue;
      }
      const msg = String(r.error?.message || '').toLowerCase();
      const duplicate = r.error?.code === '23505' || msg.includes('duplicate key');
      if (duplicate) continue;
      // 3) Older schema fallback: minimal row shape (omit payload/priority/idempotency_key).
      const minimal = {
        client_id: row.client_id,
        campaign_id: row.campaign_id,
        campaign_lead_id: row.campaign_lead_id,
        username: row.username,
        status: 'pending',
        available_at: row.available_at,
      };
      const r2 = await sb.from('cold_dm_send_jobs').insert(minimal);
      if (!r2.error) {
        insertedViaIdempotency += 1;
        continue;
      }
      const msg2 = String(r2.error?.message || '').toLowerCase();
      const duplicate2 = r2.error?.code === '23505' || msg2.includes('duplicate key');
      if (duplicate2) continue;
      throw r2.error;
    }
    inserted = insertedViaIdempotency;
  }
  console.log(`[syncSendJobsForCampaign] created ${inserted} send job(s) for campaign ${campaignId}`);
  return inserted;
}

async function syncSendJobsForClient(clientId, campaignId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  if (campaignId) return syncSendJobsForCampaign(clientId, campaignId);
  const campaigns = await getActiveCampaigns(clientId);
  let total = 0;
  for (const campaign of campaigns) {
    total += await syncSendJobsForCampaign(clientId, campaign.id).catch((e) => {
      console.error(
        '[syncSendJobsForClient] campaign sync failed',
        JSON.stringify({
          clientId,
          campaignId: campaign.id,
          code: e?.code || null,
          message: e?.message || String(e),
          details: e?.details || null,
          hint: e?.hint || null,
        })
      );
      return 0;
    });
  }
  return total;
}

/**
 * Pause one campaign and cancel all queued/running send jobs for it (stops hammering every lead).
 * IG sessions stay connected; user fixes delays and sets campaign active again.
 */
async function pauseCampaignMissingSendDelayConfig(clientId, campaignId, message) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignId) return;
  const nowIso = new Date().toISOString();
  const msg = String(message || 'missing_delay_config').slice(0, 500);
  try {
    await sb
      .from('cold_dm_campaigns')
      .update({ status: 'paused', updated_at: nowIso })
      .eq('id', campaignId)
      .eq('client_id', clientId);
  } catch (e) {
    console.error('[pauseCampaignMissingSendDelayConfig] campaign update failed', e);
  }
  try {
    await sb
      .from('cold_dm_send_jobs')
      .update({
        status: 'cancelled',
        finished_at: nowIso,
        leased_until: null,
        leased_by_worker: null,
        last_error_class: 'missing_delay_config',
        last_error_message: msg,
        updated_at: nowIso,
      })
      .eq('campaign_id', campaignId)
      .eq('client_id', clientId)
      .in('status', ['pending', 'retry', 'running']);
  } catch (e) {
    console.error('[pauseCampaignMissingSendDelayConfig] send_jobs cancel failed', e);
  }
  await setClientStatusMessage(clientId, msg).catch(() => {});
}

async function buildSendWorkFromJob(jobId) {
  const sb = getSupabase();
  if (!sb || !jobId) return null;
  const job = await getSendJob(jobId);
  if (!job) {
    console.warn(`[buildSendWorkFromJob] send job row missing jobId=${jobId}`);
    return { job: { id: jobId }, disposition: 'cancelled', reason: 'send_job_not_found' };
  }
  if (!job.client_id || !job.campaign_id || !job.campaign_lead_id) {
    console.warn(
      '[buildSendWorkFromJob] invalid send job row (missing client_id, campaign_id, or campaign_lead_id)',
      JSON.stringify({
        jobId: job.id,
        hasClientId: !!job.client_id,
        hasCampaignId: !!job.campaign_id,
        hasCampaignLeadId: !!job.campaign_lead_id,
      })
    );
    return { job, disposition: 'cancelled', reason: 'invalid_send_job_row' };
  }
  const campaignSelectWithVoice =
    'id, client_id, name, status, message_template_id, message_group_id, schedule_start_time, schedule_end_time, timezone, daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec, send_voice_note, voice_note_storage_path, voice_note_mode';
  const campaignSelectLegacy =
    'id, client_id, name, status, message_template_id, message_group_id, schedule_start_time, schedule_end_time, timezone, daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec';
  const prefersVoiceColumns = _coldDmCampaignsSupportsVoiceNoteColumns !== false;
  const selectColumns = prefersVoiceColumns ? campaignSelectWithVoice : campaignSelectLegacy;
  let campaign = null;
  let campaignLookupError = null;
  try {
    const { data, error } = await sb
      .from('cold_dm_campaigns')
      .select(selectColumns)
      .eq('id', job.campaign_id)
      .eq('client_id', job.client_id)
      .maybeSingle();
    if (error) throw error;
    if (_coldDmCampaignsSupportsVoiceNoteColumns == null && prefersVoiceColumns) {
      _coldDmCampaignsSupportsVoiceNoteColumns = true;
    }
    campaign = data || null;
  } catch (e) {
    campaignLookupError = e;
    const missingVoiceColumn = prefersVoiceColumns && isMissingColumnError(e, 'cold_dm_campaigns.send_voice_note');
    if (missingVoiceColumn) {
      _coldDmCampaignsSupportsVoiceNoteColumns = false;
      if (!_loggedMissingColdDmCampaignVoiceColumns) {
        _loggedMissingColdDmCampaignVoiceColumns = true;
        console.error(
          '[buildSendWorkFromJob] cold_dm_campaigns voice-note columns missing; using legacy campaign select until restart. ' +
            'Apply migration 010_voice_notes.sql to add send_voice_note/voice_note_storage_path/voice_note_mode.',
          JSON.stringify({
            jobId: job.id,
            campaignId: job.campaign_id,
            clientId: job.client_id,
            errorCode: e?.code || null,
            errorMessage: e?.message || String(e),
          })
        );
      }
    } else {
      console.error(
        '[buildSendWorkFromJob] campaign lookup failed',
        JSON.stringify({
          jobId: job.id,
          campaignId: job.campaign_id,
          clientId: job.client_id,
          errorCode: e?.code || null,
          errorMessage: e?.message || String(e),
          errorDetails: e?.details || null,
        })
      );
    }
    const { data: fallbackCampaign, error: fallbackError } = await sb
      .from('cold_dm_campaigns')
      .select(campaignSelectLegacy)
      .eq('id', job.campaign_id)
      .eq('client_id', job.client_id)
      .maybeSingle();
    if (fallbackError) {
      console.error(
        '[buildSendWorkFromJob] campaign fallback lookup failed',
        JSON.stringify({
          jobId: job.id,
          campaignId: job.campaign_id,
          clientId: job.client_id,
          errorCode: fallbackError?.code || null,
          errorMessage: fallbackError?.message || String(fallbackError),
          errorDetails: fallbackError?.details || null,
        })
      );
    } else if (fallbackCampaign) {
      campaign = {
        ...fallbackCampaign,
        send_voice_note: false,
        voice_note_storage_path: null,
        voice_note_mode: 'after_text',
      };
      campaignLookupError = null;
      if (!missingVoiceColumn) {
        console.warn(
          `[buildSendWorkFromJob] campaign fallback lookup succeeded for campaign=${job.campaign_id} client=${job.client_id}.`
        );
      }
    }
  }
  if (!campaign) {
    if (campaignLookupError) {
      return { job, disposition: 'retry', reason: 'campaign_lookup_error', availableAt: computeAvailableAtIso(2 * 60) };
    }
    console.warn(
      `[buildSendWorkFromJob] campaign not found for job=${job.id} campaign=${job.campaign_id} client=${job.client_id}. ` +
        'If this repeats, verify cold_dm_send_jobs.client_id/campaign_id values and campaign ownership.'
    );
    return { job, disposition: 'cancelled', reason: 'campaign_not_found' };
  }
  if (campaign.status !== 'active') {
    // Drain stale queued jobs for inactive/paused/completed campaigns so workers
    // do not repeatedly claim+cancel them one-by-one.
    try {
      const nowIso = new Date().toISOString();
      await sb
        .from('cold_dm_send_jobs')
        .update({
          status: 'cancelled',
          finished_at: nowIso,
          leased_until: null,
          leased_by_worker: null,
          last_error_class: 'campaign_inactive',
          last_error_message: `campaign_inactive:${campaign.status}`,
          updated_at: nowIso,
        })
        .eq('client_id', job.client_id)
        .eq('campaign_id', job.campaign_id)
        .in('status', ['pending', 'retry', 'running'])
        .neq('id', job.id);
    } catch {}
    return { job, disposition: 'cancelled', reason: 'campaign_inactive' };
  }
  const { data: leadLink } = await sb
    .from('cold_dm_campaign_leads')
    .select('id, lead_id, status')
    .eq('id', job.campaign_lead_id)
    .maybeSingle();
  if (!leadLink || leadLink.status !== 'pending') {
    return { job, disposition: 'cancelled', reason: 'campaign_lead_not_pending' };
  }
  const { data: lead } = await sb
    .from('cold_dm_leads')
    .select('id, username, first_name, last_name, display_name')
    .eq('id', leadLink.lead_id)
    .eq('client_id', job.client_id)
    .maybeSingle();
  if (!lead?.username) {
    return { job, disposition: 'failed', reason: 'missing_lead_row' };
  }

  let messageText = null;
  let messageGroupMessageId = null;
  let voiceNotePath = null;
  let voiceNoteMode = campaign.voice_note_mode || 'after_text';
  if (campaign.message_group_id) {
    const groupMsg = await getRandomMessageFromGroup(campaign.message_group_id);
    if (groupMsg) {
      messageText = groupMsg.message_text;
      messageGroupMessageId = groupMsg.id;
      if (groupMsg.send_voice_note && groupMsg.voice_note_storage_path) {
        voiceNotePath = groupMsg.voice_note_storage_path;
      }
    }
  }
  if (!messageText && campaign.message_template_id) {
    messageText = await getMessageTemplateById(campaign.message_template_id);
  }
  if (!voiceNotePath && campaign.send_voice_note && campaign.voice_note_storage_path) {
    voiceNotePath = campaign.voice_note_storage_path;
  }
  if (!messageText) {
    return { job, disposition: 'failed', reason: 'no_message_text' };
  }
  const settingsForDelays =
    campaign.min_delay_sec == null || campaign.max_delay_sec == null
      ? await getSettings(job.client_id).catch(() => null)
      : null;
  if (!hasValidCampaignSendDelayConfig(campaign, settingsForDelays)) {
    const detail = describeCampaignSendDelayConfigProblem(campaign, settingsForDelays);
    const statusMessage = `Campaign "${campaign.name || campaign.id}" paused: ${detail}. Fix delays, then set the campaign to Active again.`;
    await pauseCampaignMissingSendDelayConfig(job.client_id, campaign.id, statusMessage).catch((e) =>
      console.error('[buildSendWorkFromJob] pauseCampaignMissingSendDelayConfig failed', e)
    );
    return {
      job,
      disposition: 'cancelled',
      reason: 'missing_delay_config',
      statusMessage,
    };
  }
  const effectiveDelays = computeEffectiveSendDelaySeconds(campaign, settingsForDelays);
  if (!isWithinSchedule(campaign.schedule_start_time, campaign.schedule_end_time, campaign.timezone ?? null)) {
    const nextStart = getNextScheduleStartInTimezone(campaign.schedule_start_time, campaign.timezone ?? null);
    const availableAt = nextStart ? nextStart.toISOString() : computeAvailableAtIso(15 * 60);
    const statusMessage = formatOutsideScheduleResumeMessage(
      campaign.timezone ?? null,
      nextStart,
      availableAt
    );
    return {
      job,
      disposition: 'retry',
      reason: 'outside_schedule',
      availableAt,
      statusMessage,
    };
  }
  const scrapeBlocklistSet = await getScrapeBlocklistUsernames(job.client_id);
  const unameNorm = normalizeUsername(lead.username).toLowerCase();
  if (scrapeBlocklistSet.has(unameNorm)) {
    return { job, disposition: 'failed', reason: 'blocklist', lead, campaign, messageText, messageGroupMessageId, voiceNotePath, voiceNoteMode };
  }
  return {
    job,
    disposition: 'ready',
    work: {
      clientId: job.client_id,
      campaignLeadId: job.campaign_lead_id,
      campaignId: job.campaign_id,
      leadId: leadLink.lead_id,
      username: normalizeUsername(lead.username),
      first_name: lead.first_name ?? null,
      last_name: lead.last_name ?? null,
      display_name: lead.display_name ?? null,
      messageText,
      messageGroupId: campaign.message_group_id || null,
      messageGroupMessageId: messageGroupMessageId || null,
      dailySendLimit: campaign.daily_send_limit,
      hourlySendLimit: campaign.hourly_send_limit,
      minDelaySec: effectiveDelays.minDelaySec,
      maxDelaySec: effectiveDelays.maxDelaySec,
      voiceNotePath,
      voiceNoteMode,
    },
  };
}

async function getColdDmQueueHealthSnapshot() {
  const sb = getSupabase();
  if (!sb) return null;
  const nowIso = new Date().toISOString();
  const [sendRes, scrapeRes, followUpRes, workerRes] = await Promise.all([
    sb.from('cold_dm_send_jobs').select('status, available_at, created_at'),
    sb.from('cold_dm_scrape_jobs').select('status, available_at, started_at'),
    sb.from('cold_dm_follow_up_queue').select('status, scheduled_for, created_at'),
    sb.from('cold_dm_worker_heartbeats').select('worker_id, worker_type, last_seen_at'),
  ]);

  const sendRows = sendRes.data || [];
  const scrapeRows = scrapeRes.data || [];
  const followUpRows = followUpRes.data || [];
  const workerRows = workerRes.data || [];

  const pendingSend = sendRows.filter((r) => ['pending', 'retry', 'running'].includes(r.status)).length;
  const pendingScrape = scrapeRows.filter((r) => ['pending', 'retry', 'running'].includes(r.status)).length;
  const pendingFollowUps = followUpRows.filter((r) => ['pending', 'processing'].includes(r.status)).length;
  const oldestSend = sendRows
    .filter((r) => ['pending', 'retry'].includes(r.status))
    .map((r) => r.available_at || r.created_at)
    .filter(Boolean)
    .sort()[0] || null;
  const oldestScrape = scrapeRows
    .filter((r) => ['pending', 'retry'].includes(r.status))
    .map((r) => r.available_at || r.started_at)
    .filter(Boolean)
    .sort()[0] || null;
  const oldestFollowUp = followUpRows
    .filter((r) => r.status === 'pending')
    .map((r) => r.scheduled_for || r.created_at)
    .filter(Boolean)
    .sort()[0] || null;
  const activeWorkers = workerRows.filter((r) => r.last_seen_at && r.last_seen_at >= new Date(Date.now() - 2 * 60 * 1000).toISOString());
  return {
    now: nowIso,
    send_jobs: { pending: pendingSend, oldest_available_at: oldestSend },
    scrape_jobs: { pending: pendingScrape, oldest_available_at: oldestScrape },
    follow_up_queue: { pending: pendingFollowUps, oldest_scheduled_for: oldestFollowUp },
    workers: {
      send: activeWorkers.filter((r) => r.worker_type === 'send').length,
      scrape: activeWorkers.filter((r) => r.worker_type === 'scrape').length,
      scheduler: activeWorkers.filter((r) => r.worker_type === 'scheduler').length,
    },
  };
}

/** Returns Set of normalised usernames in cold_dm_scrape_blocklist (do not scrape as leads or cold-DM). */
async function getScrapeBlocklistUsernames(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return new Set();
  try {
    const { data, error } = await sb
      .from('cold_dm_scrape_blocklist')
      .select('username')
      .eq('client_id', clientId);
    if (error) return new Set();
    const set = new Set();
    for (const row of data || []) {
      const u = (row.username || '').trim().replace(/^@/, '').toLowerCase();
      if (u) set.add(u);
    }
    return set;
  } catch (e) {
    return new Set();
  }
}

/** Returns Set of normalised usernames that have active conversations (do not scrape as leads). */
async function getConversationParticipantUsernames(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return new Set();
  try {
    const { data, error } = await sb
      .from('conversations')
      .select('participant_username')
      .eq('client_id', clientId);
    if (error) return new Set();
    const set = new Set();
    for (const row of data || []) {
      const raw = (row.participant_username || '').trim().replace(/^@/, '');
      const u = raw.toLowerCase();
      if (u) set.add(u);
    }
    return set;
  } catch (e) {
    return new Set();
  }
}

// --- Leads upsert (for scraper) ---
async function upsertLead(clientId, username, source) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const u = normalizeUsername(username);
  const { error } = await sb
    .from('cold_dm_leads')
    .upsert(
      {
        client_id: clientId,
        username: u,
        source: source || null,
        added_at: new Date().toISOString(),
      },
      { onConflict: 'client_id,username', ignoreDuplicates: true }
    );
  if (error) throw error;
}

/**
 * Scrape-only: insert truly new (client_id, username) rows; never overwrite source, group,
 * names, or added_at on existing leads (avoids "replacing" template/bulk leads and bad counts).
 * If leadGroupId is set, assigns that group only when the existing row has no lead_group_id.
 *
 * @param {string} clientId
 * @param {string[]|{ username: string }[]} leadsOrUsernames - Usernames only; display names are not persisted from scrape.
 * @returns {Promise<number>} Number of newly inserted lead rows.
 */
async function upsertLeadsBatch(clientId, leadsOrUsernames, source, leadGroupId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const normalized = [];
  for (const item of leadsOrUsernames || []) {
    const u = normalizeUsername(typeof item === 'string' ? item : item.username);
    if (u) normalized.push(u);
  }
  const uniqueIncoming = [...new Set(normalized)];
  if (uniqueIncoming.length === 0) return 0;

  const existingByUsername = new Map();
  const chunkSize = 150;
  for (let i = 0; i < uniqueIncoming.length; i += chunkSize) {
    const chunk = uniqueIncoming.slice(i, i + chunkSize);
    const { data: existingRows, error: existingErr } = await sb
      .from('cold_dm_leads')
      .select('id, username, lead_group_id')
      .eq('client_id', clientId)
      .in('username', chunk);
    if (existingErr) throw existingErr;
    for (const r of existingRows || []) {
      existingByUsername.set(normalizeUsername(r.username), r);
    }
  }

  const isoNow = new Date().toISOString();
  const inserts = [];
  const idsToAssignGroup = [];
  for (const username of uniqueIncoming) {
    const ex = existingByUsername.get(username);
    if (!ex) {
      const row = {
        client_id: clientId,
        username,
        source: source || null,
        added_at: isoNow,
      };
      if (leadGroupId) row.lead_group_id = leadGroupId;
      inserts.push(row);
    } else if (leadGroupId && (ex.lead_group_id == null || ex.lead_group_id === '')) {
      idsToAssignGroup.push(ex.id);
    }
  }

  let inserted = 0;
  const insChunk = 200;
  for (let i = 0; i < inserts.length; i += insChunk) {
    const slice = inserts.slice(i, i + insChunk);
    const { data: insData, error: insErr } = await sb.from('cold_dm_leads').insert(slice).select('id');
    if (insErr) throw insErr;
    inserted += (insData || []).length;
  }

  const assignIds = [...new Set(idsToAssignGroup)];
  if (assignIds.length && leadGroupId) {
    for (let i = 0; i < assignIds.length; i += insChunk) {
      const slice = assignIds.slice(i, i + insChunk);
      const { error: upErr } = await sb
        .from('cold_dm_leads')
        .update({ lead_group_id: leadGroupId })
        .eq('client_id', clientId)
        .in('id', slice);
      if (upErr) throw upErr;
    }
  }

  return inserted;
}

/**
 * Upsert lead identity fields discovered during DM flow.
 * Saves canonical display_name and optional first/last names for future templating.
 */
async function upsertLeadIdentity(clientId, username, identity = {}) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const u = normalizeUsername(username);
  if (!u) return false;

  const displayName = typeof identity.display_name === 'string' ? identity.display_name.trim() : '';
  const firstName = typeof identity.first_name === 'string' ? identity.first_name.trim() : '';
  const lastName = typeof identity.last_name === 'string' ? identity.last_name.trim() : '';
  if (!displayName && !firstName && !lastName) return false;

  const row = {
    client_id: clientId,
    username: u,
    added_at: new Date().toISOString(),
  };
  if (displayName) row.display_name = displayName;
  if (firstName) row.first_name = firstName;
  if (lastName) row.last_name = lastName;

  const { error } = await sb.from('cold_dm_leads').upsert(row, {
    onConflict: 'client_id,username',
    ignoreDuplicates: false,
  });
  if (error) throw error;
  return true;
}

// --- Campaigns ---
// Campaign config (timezone, schedule, limits, delays) is read from DB on every get-next-work / can-run check. Do not cache; re-evaluate when user changes campaign in dashboard.
/** Campaigns with status = active only (sending / queue materialization). */
async function getActiveCampaigns(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  try {
    const { data, error } = await sb
      .from('cold_dm_campaigns')
      .select(
        'id, name, status, message_template_id, message_group_id, schedule_start_time, schedule_end_time, timezone, daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec, send_voice_note, voice_note_storage_path, voice_note_mode'
      )
      .eq('client_id', clientId)
      .eq('status', 'active')
      .order('created_at', { ascending: true });
    if (error) throw error;
    return data || [];
  } catch (e) {
    const { data, error } = await sb
      .from('cold_dm_campaigns')
      .select('id, name, status, message_template_id, message_group_id, schedule_start_time, schedule_end_time, timezone, daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec')
      .eq('client_id', clientId)
      .eq('status', 'active')
      .order('created_at', { ascending: true });
    if (error) throw error;
    return (data || []).map((r) => ({ ...r, send_voice_note: false, voice_note_storage_path: null, voice_note_mode: 'after_text' }));
  }
}

async function getCampaignsMissingSendDelays(clientId, campaignId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const settings = await getSettings(clientId).catch(() => null);
  let q = sb
    .from('cold_dm_campaigns')
    .select('id, name, status, min_delay_sec, max_delay_sec')
    .eq('client_id', clientId);
  if (campaignId) q = q.eq('id', campaignId);
  const { data: campaigns, error } = await q.order('created_at', { ascending: true });
  if (error) throw error;
  if (!campaigns?.length) return [];

  const problems = [];
  for (const camp of campaigns) {
    if (hasValidCampaignSendDelayConfig(camp, settings)) continue;

    let shouldBlock = camp.status === 'active';
    if (!shouldBlock) {
      const { count: pendingCount, error: pendingErr } = await sb
        .from('cold_dm_campaign_leads')
        .select('*', { count: 'exact', head: true })
        .eq('campaign_id', camp.id)
        .eq('status', 'pending');
      if (pendingErr) throw pendingErr;
      shouldBlock = (pendingCount ?? 0) > 0;
    }

    if (!shouldBlock) {
      const { data: leadGroupRows, error: leadGroupErr } = await sb
        .from('cold_dm_campaign_lead_groups')
        .select('lead_group_id')
        .eq('campaign_id', camp.id);
      if (leadGroupErr) throw leadGroupErr;
      const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
      if (leadGroupIds.length > 0) {
        const { data: mappedLeads, error: mappedLeadsErr } = await sb
          .from('cold_dm_leads')
          .select('username')
          .eq('client_id', clientId)
          .in('lead_group_id', leadGroupIds);
        if (mappedLeadsErr) throw mappedLeadsErr;
        const usernames = (mappedLeads || [])
          .map((r) => normalizeUsername(r.username || '').toLowerCase())
          .filter(Boolean);
        if (usernames.length > 0) {
          const { data: sentRows, error: sentErr } = await sb
            .from('cold_dm_sent_messages')
            .select('username')
            .eq('client_id', clientId)
            .in('username', usernames);
          if (sentErr) throw sentErr;
          const sentSet = new Set((sentRows || []).map((r) => normalizeUsername(r.username || '').toLowerCase()));
          shouldBlock = usernames.some((u) => !sentSet.has(u));
        }
      }
    }

    if (shouldBlock) {
      problems.push({
        id: camp.id,
        name: camp.name || null,
        status: camp.status,
        reason: describeCampaignSendDelayConfigProblem(camp, settings),
      });
    }
  }

  return problems;
}

/**
 * Returns a short hint for why there is no sendable work for this client.
 */
async function getNoWorkHint(clientId) {
  return getMostSpecificNoWorkHint(clientId);
}

async function getMostSpecificNoWorkHint(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return '';

  const campaigns = await getActiveCampaigns(clientId);
  if (!campaigns?.length) return '';
  const settings = await getSettings(clientId).catch(() => null);

  let anyWithPending = false;

  for (const camp of campaigns) {
    const { count: campPending } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', camp.id)
      .eq('status', 'pending');
    if ((campPending ?? 0) === 0) continue;

    anyWithPending = true;

    if (!hasValidCampaignSendDelayConfig(camp, settings)) {
      return `Campaign "${camp.name || camp.id}" is missing send delay settings (${describeCampaignSendDelayConfigProblem(camp, settings)}). Set min/max delay on the campaign or client Cold DM defaults before starting.`;
    }

    let messageText = null;
    if (camp.message_group_id) {
      const groupMsg = await getRandomMessageFromGroup(camp.message_group_id).catch(() => null);
      if (groupMsg?.message_text) messageText = groupMsg.message_text;
    }
    if (!messageText && camp.message_template_id) {
      messageText = await getMessageTemplateById(camp.message_template_id).catch(() => null);
    }
    if (!messageText) {
      return `Campaign "${camp.name || camp.id}" has no usable message text. Add a message template or messages to the selected message group before starting.`;
    }

    const { data: leadGroupRows, error: leadGroupErr } = await sb
      .from('cold_dm_campaign_lead_groups')
      .select('lead_group_id')
      .eq('campaign_id', camp.id);
    if (leadGroupErr) throw leadGroupErr;
    const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
    if (leadGroupIds.length === 0) {
      return `Campaign "${camp.name || camp.id}" has no lead groups assigned. Select at least one lead group before starting.`;
    }

    const { data: leadRows, error: leadErr } = await sb
      .from('cold_dm_leads')
      .select('id')
      .eq('client_id', clientId)
      .in('lead_group_id', leadGroupIds);
    if (leadErr) throw leadErr;
    if (!leadRows || leadRows.length === 0) {
      return `Campaign "${camp.name || camp.id}" has no leads in the selected lead groups. Add leads to those groups before starting.`;
    }
  }

  if (anyWithPending) {
    // Start API uses this hint instead of "Starting…"; previously we returned '' here even when every
    // active campaign with pending leads was outside its send window (schedule is checked in the worker only).
    const outsideMsg = await getClientOutsideScheduleStatus(clientId);
    if (outsideMsg) return outsideMsg;
    return '';
  }
  return 'No campaigns with pending leads.';
}

/**
 * Schedule window uses campaign timezone (cold_dm_campaigns.timezone). Not cold_dm_settings.timezone.
 * @param {string} [timezone] - IANA timezone from campaign row (e.g. America/New_York). If omitted/null, uses UTC.
 */
function isWithinSchedule(scheduleStart, scheduleEnd, timezone) {
  const normStart = normalizeScheduleTime(scheduleStart);
  const normEnd = normalizeScheduleTime(scheduleEnd);
  if (!normStart && !normEnd) return true;
  const now = new Date();
  const currentSec = getClockSecondsInTimezone(now, timezone);
  // Cold DM campaigns: implicit 09:00–17:00 when one side was cleared in the UI (saved as null) but the other remains.
  const start = normStart || '09:00:00';
  const end = normEnd || '23:59:59';
  const startSec = parseClockTimeToSeconds(start) ?? 9 * 3600;
  const endSec = parseClockTimeToSeconds(end) ?? 23 * 3600 + 59 * 60 + 59;
  if (startSec <= endSec) return currentSec >= startSec && currentSec <= endSec;
  return currentSec >= startSec || currentSec <= endSec;
}

async function getRandomMessageFromGroup(messageGroupId) {
  const sb = getSupabase();
  if (!sb || !messageGroupId) return null;
  try {
    const { data, error } = await sb
      .from('cold_dm_message_group_messages')
      .select('id, message_text, send_voice_note, voice_note_storage_path')
      .eq('message_group_id', messageGroupId)
      .order('sort_order', { ascending: true });
    if (error || !data || data.length === 0) return null;
    const row = data[Math.floor(Math.random() * data.length)];
    return {
      id: row.id,
      message_text: row.message_text,
      send_voice_note: row.send_voice_note === true,
      voice_note_storage_path: row.voice_note_storage_path || null,
    };
  } catch (e) {
    const { data, error } = await sb
      .from('cold_dm_message_group_messages')
      .select('id, message_text')
      .eq('message_group_id', messageGroupId)
      .order('sort_order', { ascending: true });
    if (error || !data || data.length === 0) return null;
    const row = data[Math.floor(Math.random() * data.length)];
    return { id: row.id, message_text: row.message_text, send_voice_note: false, voice_note_storage_path: null };
  }
}

async function getMessageTemplateById(templateId) {
  const sb = getSupabase();
  if (!sb || !templateId) return null;
  const { data, error } = await sb
    .from('cold_dm_message_templates')
    .select('message_text')
    .eq('id', templateId)
    .maybeSingle();
  if (error) throw error;
  return data?.message_text || null;
}

async function claimCampaignLeadLease(campaignLeadId, workerId, leaseSeconds = 600) {
  const sb = getSupabase();
  if (!sb || !campaignLeadId || !workerId) return false;
  const nowIso = new Date().toISOString();
  const leaseUntil = new Date(Date.now() + Math.max(60, parseInt(leaseSeconds, 10) || 600) * 1000).toISOString();
  const updatePayload = {
    leased_by_worker: workerId,
    leased_until: leaseUntil,
    lease_heartbeat_at: nowIso,
  };
  const attempts = [
    (q) => q.is('leased_until', null),
    (q) => q.lte('leased_until', nowIso),
  ];
  for (const build of attempts) {
    let query = sb.from('cold_dm_campaign_leads').update(updatePayload).eq('id', campaignLeadId).eq('status', 'pending');
    query = build(query);
    const { data, error } = await query.select('id').limit(1);
    if (!error && data && data.length > 0) return true;
  }
  return false;
}

async function getNextPendingCampaignLead(clientId, workerId = null, leaseSeconds = 600) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const campaigns = await getActiveCampaigns(clientId);
  const scrapeBlocklistSet = await getScrapeBlocklistUsernames(clientId);
  const clientSettings = await getSettings(clientId).catch(() => null);
  const campaignDebug = [];
  for (const camp of campaigns) {
    const dbg = {
      campaignId: camp.id,
      campaignName: camp.name || null,
      inSchedule: true,
      hasMessageText: true,
      leadGroupCount: 0,
      leadRowsCount: 0,
      pendingCount: 0,
      skippedMissingLeadRow: 0,
      skippedAlreadySent: 0,
      skippedBlocklist: 0,
      blockedBy: null,
      reason: null,
    };
    const campaignTz = camp.timezone ?? null;
    if (!isWithinSchedule(camp.schedule_start_time, camp.schedule_end_time, campaignTz)) {
      dbg.inSchedule = false;
      dbg.reason = 'outside_schedule';
      campaignDebug.push(dbg);
      continue;
    }
    if (!hasValidCampaignSendDelayConfig(camp, clientSettings)) {
      dbg.reason = 'missing_delay_config';
      dbg.blockedBy = 'missing_delay_config';
      campaignDebug.push(dbg);
      continue;
    }
    let messageText = null;
    let messageGroupMessageId = null;
    let voiceNotePath = null;
    let voiceNoteMode = camp.voice_note_mode || 'after_text';
    if (camp.message_group_id) {
      const groupMsg = await getRandomMessageFromGroup(camp.message_group_id);
      if (groupMsg) {
        messageText = groupMsg.message_text;
        messageGroupMessageId = groupMsg.id;
        if (groupMsg.send_voice_note && groupMsg.voice_note_storage_path) {
          voiceNotePath = groupMsg.voice_note_storage_path;
        }
      }
    }
    if (!messageText && camp.message_template_id) {
      messageText = await getMessageTemplateById(camp.message_template_id);
    }
    if (!voiceNotePath && camp.send_voice_note && camp.voice_note_storage_path) {
      voiceNotePath = camp.voice_note_storage_path;
    }
    if (!messageText) {
      dbg.hasMessageText = false;
      dbg.reason = 'no_message_text';
      campaignDebug.push(dbg);
      continue;
    }

    const { data: leadGroupRows } = await sb
      .from('cold_dm_campaign_lead_groups')
      .select('lead_group_id')
      .eq('campaign_id', camp.id);
    const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
    dbg.leadGroupCount = leadGroupIds.length;
    if (leadGroupIds.length === 0) {
      dbg.reason = 'no_lead_groups';
      campaignDebug.push(dbg);
      continue;
    }

    const { data: leadRows } = await sb
      .from('cold_dm_leads')
      .select('id, username')
      .eq('client_id', clientId)
      .in('lead_group_id', leadGroupIds);
    dbg.leadRowsCount = leadRows?.length || 0;
    if (!leadRows || leadRows.length === 0) {
      dbg.reason = 'no_leads_in_mapped_groups';
      campaignDebug.push(dbg);
      continue;
    }

    for (const lead of leadRows) {
      const { data: existing } = await sb
        .from('cold_dm_campaign_leads')
        .select('id')
        .eq('campaign_id', camp.id)
        .eq('lead_id', lead.id)
        .maybeSingle();
      if (!existing) {
        await sb.from('cold_dm_campaign_leads').upsert(
          { campaign_id: camp.id, lead_id: lead.id, status: 'pending' },
          { onConflict: 'campaign_id,lead_id', ignoreDuplicates: true }
        );
      }
    }

    let clRow = null;
    let leadRow = null;
    for (;;) {
      const nowIso = new Date().toISOString();
      const basePending = () =>
        sb.from('cold_dm_campaign_leads').select('id, lead_id').eq('campaign_id', camp.id).eq('status', 'pending');
      let { data: pendingRow, error } = await basePending()
        .is('leased_until', null)
        .order('id', { ascending: true })
        .limit(1)
        .maybeSingle();
      if (!error && !pendingRow?.lead_id) {
        ({ data: pendingRow, error } = await basePending()
          .lte('leased_until', nowIso)
          .order('id', { ascending: true })
          .limit(1)
          .maybeSingle());
      }
      if (error || !pendingRow?.lead_id) break;
      dbg.pendingCount += 1;
      const { data: leadData } = await sb
        .from('cold_dm_leads')
        .select('username, first_name, last_name, display_name')
        .eq('id', pendingRow.lead_id)
        .eq('client_id', clientId)
        .maybeSingle();
      if (!leadData?.username) {
        dbg.skippedMissingLeadRow += 1;
        await updateCampaignLeadStatus(pendingRow.id, 'failed').catch(() => {});
        continue;
      }
      const unameNorm = normalizeUsername(leadData.username).toLowerCase();
      if (scrapeBlocklistSet.has(unameNorm)) {
        dbg.skippedBlocklist += 1;
        await updateCampaignLeadStatus(pendingRow.id, 'failed', 'blocklist').catch(() => {});
        await logSentMessage(
          clientId,
          leadData.username,
          messageText || null,
          'failed',
          camp.id,
          camp.message_group_id || null,
          messageGroupMessageId || null,
          'blocklist',
          { skipDailyStats: true }
        ).catch((e) => console.error('[getNextPendingCampaignLead] logSentMessage blocklist', e && e.message));
        continue;
      }
      const sent = await alreadySent(clientId, leadData.username);
      if (sent) {
        dbg.skippedAlreadySent += 1;
        await updateCampaignLeadStatus(pendingRow.id, 'sent').catch(() => {});
        continue;
      }
      clRow = pendingRow;
      leadRow = leadData;
      break;
    }
    if (!clRow || !leadRow) {
      dbg.reason = 'no_pending_row_survived_filters';
      campaignDebug.push(dbg);
      continue;
    }

    const [stats, hourlySent] = await Promise.all([getDailyStats(clientId), getHourlySent(clientId)]);
    if (camp.daily_send_limit != null && stats.total_sent >= camp.daily_send_limit) {
      dbg.blockedBy = 'daily_limit';
      dbg.reason = 'daily_limit_reached';
      campaignDebug.push(dbg);
      continue;
    }
    if (camp.hourly_send_limit != null && hourlySent >= camp.hourly_send_limit) {
      dbg.blockedBy = 'hourly_limit';
      dbg.reason = 'hourly_limit_reached';
      campaignDebug.push(dbg);
      continue;
    }

    if (workerId) {
      const leased = await claimCampaignLeadLease(clRow.id, workerId, leaseSeconds);
      if (!leased) {
        dbg.reason = 'lease_race_lost';
        campaignDebug.push(dbg);
        continue;
      }
    }

    logNoWorkDebug('Campaign selected for send.', {
      clientId,
      campaignId: camp.id,
      campaignLeadId: clRow.id,
      username: normalizeUsername(leadRow.username),
    });
    const effDelays = computeEffectiveSendDelaySeconds(camp, clientSettings);
    return {
      campaignLeadId: clRow.id,
      campaignId: camp.id,
      leadId: clRow.lead_id,
      username: normalizeUsername(leadRow.username),
      first_name: leadRow.first_name ?? null,
      last_name: leadRow.last_name ?? null,
      display_name: leadRow.display_name ?? null,
      messageText,
      messageGroupId: camp.message_group_id || null,
      messageGroupMessageId: messageGroupMessageId || null,
      dailySendLimit: camp.daily_send_limit,
      hourlySendLimit: camp.hourly_send_limit,
      minDelaySec: effDelays.minDelaySec,
      maxDelaySec: effDelays.maxDelaySec,
      voiceNotePath,
      voiceNoteMode,
    };
  }
  logNoWorkDebug('No sendable campaign lead found.', { clientId, campaignsChecked: campaignDebug });
  return null;
}

/**
 * Add all leads from the campaign's lead groups into cold_dm_campaign_leads (status pending).
 * Only inserts when no row exists (ignores existing sent/failed). Returns count of rows inserted.
 *
 * PostgREST returns at most ~1000 rows per request; we page lead ids and batch upserts.
 */
async function addCampaignLeadsFromGroups(clientId, campaignId) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignId) return 0;

  const { data: camp } = await sb
    .from('cold_dm_campaigns')
    .select('id')
    .eq('id', campaignId)
    .eq('client_id', clientId)
    .maybeSingle();
  if (!camp) return 0;

  const { data: leadGroupRows } = await sb
    .from('cold_dm_campaign_lead_groups')
    .select('lead_group_id')
    .eq('campaign_id', campaignId);
  const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
  if (leadGroupIds.length === 0) return 0;

  const pageSize = 1000;
  const leadIds = [];
  let from = 0;
  for (;;) {
    const { data: chunk, error: leErr } = await sb
      .from('cold_dm_leads')
      .select('id')
      .eq('client_id', clientId)
      .in('lead_group_id', leadGroupIds)
      .order('id', { ascending: true })
      .range(from, from + pageSize - 1);
    if (leErr) throw leErr;
    const rows = chunk || [];
    for (const r of rows) leadIds.push(r.id);
    if (rows.length < pageSize) break;
    from += pageSize;
  }
  if (leadIds.length === 0) return 0;

  const existing = new Set();
  from = 0;
  for (;;) {
    const { data: exChunk, error: exErr } = await sb
      .from('cold_dm_campaign_leads')
      .select('lead_id')
      .eq('campaign_id', campaignId)
      .order('lead_id', { ascending: true })
      .range(from, from + pageSize - 1);
    if (exErr) throw exErr;
    const rows = exChunk || [];
    for (const r of rows) existing.add(r.lead_id);
    if (rows.length < pageSize) break;
    from += pageSize;
  }

  const toAdd = leadIds.filter((id) => !existing.has(id));
  if (toAdd.length === 0) return 0;

  const sentSet = await getSentUsernames(clientId).catch(() => new Set());
  const usernameByLeadId = new Map();
  const lookupChunk = 500;
  for (let i = 0; i < toAdd.length; i += lookupChunk) {
    const batch = toAdd.slice(i, i + lookupChunk);
    const { data: leadRows, error: leadErr } = await sb
      .from('cold_dm_leads')
      .select('id, username')
      .eq('client_id', clientId)
      .in('id', batch);
    if (leadErr) throw leadErr;
    for (const row of leadRows || []) {
      usernameByLeadId.set(row.id, normalizeUsername(row.username || '').toLowerCase());
    }
  }

  const insertChunk = 500;
  let added = 0;
  for (let i = 0; i < toAdd.length; i += insertChunk) {
    const batch = toAdd.slice(i, i + insertChunk);
    const { error } = await sb.from('cold_dm_campaign_leads').upsert(
      batch.map((lead_id) => {
        const u = usernameByLeadId.get(lead_id) || '';
        const wasSent = !!u && sentSet.has(u);
        return {
          campaign_id: campaignId,
          lead_id,
          status: wasSent ? 'sent' : 'pending',
          sent_at: wasSent ? new Date().toISOString() : null,
        };
      }),
      { onConflict: 'campaign_id,lead_id', ignoreDuplicates: true }
    );
    if (error) throw error;
    added += batch.length;
  }
  return added;
}

/**
 * On Start, reactivate campaigns that still have pending leads but are not active.
 * Returns number of campaigns switched to status='active'.
 */
async function reactivateCampaignsWithPendingLeads(clientId, campaignId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const settings = await getSettings(clientId).catch(() => null);
  let q = sb
    .from('cold_dm_campaigns')
    .select('id, status, min_delay_sec, max_delay_sec')
    .eq('client_id', clientId);
  if (campaignId) q = q.eq('id', campaignId);
  const { data: campaigns } = await q;
  if (!campaigns || campaigns.length === 0) return 0;

  let reactivated = 0;
  for (const camp of campaigns) {
    if (camp.status === 'active') continue;
    if (!hasValidCampaignSendDelayConfig(camp, settings)) continue;
    const { count: pendingCount } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', camp.id)
      .eq('status', 'pending');
    let shouldActivate = (pendingCount ?? 0) > 0;
    if (!shouldActivate) {
      // Fallback: some flows show leads as pending in UI before campaign_leads rows are materialized.
      // If campaign has mapped lead groups with leads, activate so worker can materialize pending rows.
      const { data: leadGroupRows } = await sb
        .from('cold_dm_campaign_lead_groups')
        .select('lead_group_id')
        .eq('campaign_id', camp.id);
      const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
      if (leadGroupIds.length > 0) {
        const { data: mappedLeads } = await sb
          .from('cold_dm_leads')
          .select('username')
          .eq('client_id', clientId)
          .in('lead_group_id', leadGroupIds);
        const usernames = (mappedLeads || [])
          .map((r) => normalizeUsername(r.username || '').toLowerCase())
          .filter(Boolean);
        if (usernames.length > 0) {
          const { data: sentRows } = await sb
            .from('cold_dm_sent_messages')
            .select('username')
            .eq('client_id', clientId)
            .in('username', usernames);
          const sentSet = new Set((sentRows || []).map((r) => normalizeUsername(r.username || '').toLowerCase()));
          shouldActivate = usernames.some((u) => !sentSet.has(u));
        }
      }
    }
    if (!shouldActivate) continue;
    const { error } = await sb
      .from('cold_dm_campaigns')
      .update({ status: 'active', updated_at: new Date().toISOString() })
      .eq('id', camp.id);
    if (!error) reactivated += 1;
  }
  return reactivated;
}

async function updateCampaignLeadStatus(campaignLeadId, status, failureReason = null, workerId = null) {
  const sb = getSupabase();
  if (!sb || !campaignLeadId) throw new Error('Supabase or campaignLeadId missing');
  const { data: row } = await sb
    .from('cold_dm_campaign_leads')
    .select('campaign_id')
    .eq('id', campaignLeadId)
    .maybeSingle();
  const payload = { status };
  if (status === 'sent' || status === 'failed') payload.sent_at = new Date().toISOString();
  if (status === 'failed' && failureReason) payload.failure_reason = failureReason;
  payload.leased_by_worker = null;
  payload.leased_until = null;
  payload.lease_heartbeat_at = new Date().toISOString();
  let q = sb.from('cold_dm_campaign_leads').update(payload).eq('id', campaignLeadId).select('id');
  if (workerId) q = q.eq('leased_by_worker', workerId);
  let { data: updatedRows, error } = await q;
  if (error) throw error;
  if (workerId && (!updatedRows || updatedRows.length === 0)) {
    const retry = await sb.from('cold_dm_campaign_leads').update(payload).eq('id', campaignLeadId).select('id');
    if (retry.error) throw retry.error;
    updatedRows = retry.data;
  }
  if (!updatedRows || updatedRows.length === 0) return;

  if (row && row.campaign_id && (status === 'sent' || status === 'failed')) {
    const { count } = await sb
      .from('cold_dm_campaign_leads')
      .select('*', { count: 'exact', head: true })
      .eq('campaign_id', row.campaign_id)
      .eq('status', 'pending');
    if (count === 0) {
      await sb.from('cold_dm_campaigns').update({ status: 'completed', updated_at: new Date().toISOString() }).eq('id', row.campaign_id);
      const { data: camp } = await sb.from('cold_dm_campaigns').select('client_id').eq('id', row.campaign_id).maybeSingle();
      if (camp?.client_id) await setControl(camp.client_id, 1);
    }
  }
}

module.exports = {
  getSupabase,
  isSupabaseConfigured,
  getClientId,
  setClientId,
  getToday,
  normalizeUsername,
  getSettings,
  getMessageTemplates,
  getLeads,
  getLeadsTotalAndRemaining,
  getSession,
  getInstagramSessionByIdForClient,
  getSessions,
  getSessionsForCampaign,
  getWaitingInstagramSessionReason,
  claimInstagramSessionLease,
  claimInstagramSessionForCampaign,
  heartbeatInstagramSessionLease,
  releaseInstagramSessionLease,
  releaseAllInstagramSessionLeases,
  claimCampaignSendLease,
  heartbeatCampaignSendLease,
  releaseCampaignSendLease,
  releaseAllCampaignSendLeases,
  saveSession,
  isAdminUser,
  countActiveVpsInstagramSessions,
  countActiveGraphInstagramAccounts,
  alreadySent,
  logSentMessage,
  getDailyStats,
  getDailyStatsForTimezone,
  getHourlySent,
  getCampaignLimitsById,
  getControl,
  setControl,
  setClientStatusMessage,
  getClientStatusMessage,
  getRecentSent,
  getSentUsernames,
  clearFailedAttempts,
  updateSettingsInstagramUsername,
  getScraperSession,
  saveScraperSession,
  getPlatformScraperSessions,
  getPlatformScraperSessionById,
  getPuppeteerCookiesFromSessionData,
  normalizePlatformSessionRowForPuppeteer,
  pickScraperSessionForJob,
  countActivePlatformScraperSessions,
  describePlatformScraperPoolForLogs,
  reservePlatformScraperSessionForWorker,
  heartbeatPlatformScraperSessionLease,
  releasePlatformScraperSessionLease,
  markInstagramSessionWebNeedsRefresh,
  updateInstagramSessionSessionData,
  pauseActiveCampaignsForInstagramSession,
  handleInstagramPasswordReauthDisruption,
  markPlatformScraperWebNeedsRefresh,
  reportPlatformScraperScrapeFailure,
  recordScraperActions,
  savePlatformScraperSession,
  getConversationParticipantUsernames,
  getScrapeBlocklistUsernames,
  getSentUsernames,
  createScrapeJob,
  getScrapeQuotaStatus,
  updateScrapeJob,
  retryScrapeJob,
  getScrapeJob,
  getLatestScrapeJob,
  cancelScrapeJob,
  claimColdDmScrapeJob,
  heartbeatScrapeJobLease,
  createSendJob,
  updateSendJob,
  deferCampaignPendingJobs,
  getSendJob,
  claimColdDmSendJob,
  heartbeatSendJobLease,
  workerHeartbeat,
  tryVpsIdempotencyOnce,
  upsertLead,
  upsertLeadsBatch,
  upsertLeadIdentity,
  getActiveCampaigns,
  getMessageTemplateById,
  getNextPendingCampaignLead,
  updateCampaignLeadStatus,
  getClientIdsWithPauseZero,
  getDistinctActiveCampaignIdsWithReadySendJobs,
  getClientSendCampaignTurn,
  getRecommendedSendWorkerInstanceCount,
  getNextPendingWorkAnyClient,
  getOrResolveColdDmProxyUrl,
  getClientOutsideScheduleStatus,
  getClientNoWorkReason,
  getClientNoWorkResumeAt,
  getNoWorkHint,
  getCampaignsMissingSendDelays,
  pauseCampaignMissingSendDelayConfig,
  getFirstNameBlocklist,
  getUserAccountName,
  addCampaignLeadsFromGroups,
  reactivateCampaignsWithPendingLeads,
  syncSendJobsForCampaign,
  syncSendJobsForClient,
  buildSendWorkFromJob,
  getColdDmQueueHealthSnapshot,
};
