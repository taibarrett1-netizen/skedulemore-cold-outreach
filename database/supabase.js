/**
 * Supabase layer for Cold DM (handoff from setter dashboard).
 * All tables use client_id (UUID). Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.
 */
const { createClient } = require('@supabase/supabase-js');
const path = require('path');
const fs = require('fs');

const CLIENT_ID_FILE = path.join(process.cwd(), '.cold_dm_client_id');

let _client = null;

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

function getToday() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Normalize schedule time to HH:mm:ss. Handles DB TIME (e.g. "03:00:00") and ISO timestamps (e.g. "2026-01-01T03:00:00.000Z").
 */
function normalizeScheduleTime(value) {
  if (value == null || value === '') return null;
  const s = String(value).trim();
  if (s.includes('T')) {
    const timePart = s.split('T')[1];
    return timePart ? timePart.replace(/\.\d+Z?$/i, '').slice(0, 8) : null;
  }
  return s.slice(0, 8) || null;
}

/** Returns YYYY-MM-DD in the given IANA timezone; falls back to UTC if invalid/missing. */
function getTodayInTimezone(timezone) {
  if (!timezone || typeof timezone !== 'string') return getToday();
  try {
    return new Date().toLocaleDateString('en-CA', { timeZone: timezone.trim() });
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
  if (!timezone || typeof timezone !== 'string') return new Date(Date.now() + 24 * 60 * 60 * 1000);
  try {
    const now = new Date();
    const todayStr = getTodayInTimezone(timezone);
    const [y, m, d] = todayStr.split('-').map(Number);
    const tomorrowDate = new Date(Date.UTC(y, m - 1, d + 1));
    const tomorrowStr = tomorrowDate.toLocaleDateString('en-CA', { timeZone: timezone.trim() });
    let low = now.getTime();
    let high = now.getTime() + 48 * 60 * 60 * 1000;
    while (high - low > 60000) {
      const mid = Math.floor((low + high) / 2);
      const d2 = new Date(mid);
      const datePart = d2.toLocaleDateString('en-CA', { timeZone: timezone.trim() });
      const timePart = d2.toLocaleTimeString('en-CA', { timeZone: timezone.trim(), hour12: false });
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
  if (!timezone || typeof timezone !== 'string') {
    const d = new Date();
    return new Date(d.getTime() + (60 - d.getUTCMinutes()) * 60 * 1000 - d.getUTCSeconds() * 1000);
  }
  try {
    const now = new Date();
    const tz = timezone.trim();
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

/** Returns the UTC Date for the next time the schedule window opens (scheduleStart today or tomorrow in TZ). */
function getNextScheduleStartInTimezone(scheduleStartTime, timezone) {
  const startStr = normalizeScheduleTime(scheduleStartTime);
  if (!startStr) return null;
  const [sh, sm] = startStr.split(':').map(Number);
  const startTime = `${String(sh).padStart(2, '0')}:${String(sm || 0).padStart(2, '0')}`;
  if (!timezone || typeof timezone !== 'string') {
    const now = new Date();
    let next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), sh, sm || 0, 0));
    if (next.getTime() <= now.getTime()) next = new Date(next.getTime() + 24 * 60 * 60 * 1000);
    return next;
  }
  try {
    const now = new Date();
    const tz = timezone.trim();
    const todayStr = getTodayInTimezone(tz);
    const tomorrowStr = new Date(now.getTime() + 24 * 60 * 60 * 1000).toLocaleDateString('en-CA', { timeZone: tz });
    const findMoment = (dateStr) => {
      let low = now.getTime();
      let high = now.getTime() + 48 * 60 * 60 * 1000;
      while (high - low > 60000) {
        const mid = Math.floor((low + high) / 2);
        const d = new Date(mid);
        const dStr = d.toLocaleDateString('en-CA', { timeZone: tz });
        const tStr = d.toLocaleTimeString('en-CA', { timeZone: tz, hour12: false }).slice(0, 5);
        if (dStr < dateStr || (dStr === dateStr && tStr < startTime)) low = mid + 1;
        else high = mid;
      }
      return new Date(high);
    };
    const todayStart = findMoment(todayStr);
    const tomorrowStart = findMoment(tomorrowStr);
    if (todayStart.getTime() > now.getTime()) return todayStart;
    return tomorrowStart;
  } catch (e) {
    return null;
  }
}

function normalizeUsername(username) {
  const u = String(username).trim();
  return u.startsWith('@') ? u.slice(1) : u;
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
  const { data, error } = await sb
    .from('cold_dm_instagram_sessions')
    .select('id, session_data, instagram_username')
    .eq('client_id', clientId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

/** All sessions for a client. Used when campaign has no assigned sessions. */
async function getSessions(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_instagram_sessions')
    .select('id, session_data, instagram_username')
    .eq('client_id', clientId)
    .order('id', { ascending: true });
  if (error) throw error;
  return data || [];
}

/**
 * Sessions to use for a campaign. If campaign has rows in cold_dm_campaign_instagram_sessions,
 * returns only those sessions; otherwise returns all client sessions.
 */
async function getSessionsForCampaign(clientId, campaignId) {
  const sb = getSupabase();
  if (!sb || !clientId || !campaignId) return [];
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
    const { data: sessions, error: sessErr } = await sb
      .from('cold_dm_instagram_sessions')
      .select('id, session_data, instagram_username')
      .eq('client_id', clientId)
      .in('id', ids)
      .order('id', { ascending: true });
    if (sessErr || !sessions?.length) return getSessions(clientId);
    return sessions;
  } catch (e) {
    return getSessions(clientId);
  }
}

async function saveSession(clientId, sessionData, instagramUsername) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const { error } = await sb
    .from('cold_dm_instagram_sessions')
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

async function logSentMessage(clientId, username, message, status = 'success', campaignId = null, messageGroupId = null, messageGroupMessageId = null, failureReason = null) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const u = normalizeUsername(username);
  const date = await getTodayForClient(clientId);
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

  const { data: existing } = await sb
    .from('cold_dm_daily_stats')
    .select('total_sent, total_failed')
    .eq('client_id', clientId)
    .eq('date', date)
    .maybeSingle();

  if (existing) {
    const { error: updateErr } = await sb
      .from('cold_dm_daily_stats')
      .update(
        status === 'success'
          ? { total_sent: existing.total_sent + 1 }
          : { total_failed: existing.total_failed + 1 }
      )
      .eq('client_id', clientId)
      .eq('date', date);
    if (updateErr) throw updateErr;
  } else {
    const { error: insertStatErr } = await sb.from('cold_dm_daily_stats').insert({
      client_id: clientId,
      date,
      total_sent: status === 'success' ? 1 : 0,
      total_failed: status === 'failed' ? 1 : 0,
    });
    if (insertStatErr) throw insertStatErr;
  }
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

async function getHourlySent(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return 0;
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const { count, error } = await sb
    .from('cold_dm_sent_messages')
    .select('*', { count: 'exact', head: true })
    .eq('client_id', clientId)
    .gte('sent_at', oneHourAgo);
  if (error) throw error;
  return count || 0;
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
 * Returns next pending work from any client with pause = 0.
 * Fresh read every call (no cache). Only campaigns with status = 'active' are considered;
 * then cold_dm_campaign_leads with status = 'pending', schedule/timezone/limits applied.
 * @returns {Promise<{ clientId: string, work: object } | null>}
 */
async function getNextPendingWorkAnyClient() {
  const clientIds = await getClientIdsWithPauseZero();
  if (clientIds.length === 0) return null;
  for (const clientId of clientIds) {
    const work = await getNextPendingCampaignLead(clientId);
    if (work) return { clientId, work };
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

  const settings = await getSettings(clientId);
  const campaigns = await getActiveCampaigns(clientId);
  const campaignIds = (campaigns || []).map((c) => c.id).filter(Boolean);

  const { count: pendingCount } =
    campaignIds.length > 0
      ? await sb
          .from('cold_dm_campaign_leads')
          .select('*', { count: 'exact', head: true })
          .in('campaign_id', campaignIds)
          .eq('status', 'pending')
      : { count: 0 };
  if ((pendingCount ?? 0) === 0) return { message: null, reason: 'no_pending', resumeAt: null };

  let earliestScheduleResume = null;
  let firstWindow = null;
  let tzLabel = 'UTC';
  let allOutside = true;
  for (const camp of campaigns) {
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
  const dailyLimit = settings?.daily_send_limit ?? 100;
  const hourlyLimit = settings?.max_sends_per_hour ?? 20;
  if (stats.total_sent >= dailyLimit) {
    return {
      message: 'Daily limit reached.',
      reason: 'daily_limit',
      resumeAt: getNextMidnightInTimezone(clientTz),
    };
  }
  if (hourlySent >= hourlyLimit) {
    return {
      message: 'Hourly limit reached.',
      reason: 'hourly_limit',
      resumeAt: getNextHourStartInTimezone(clientTz),
    };
  }
  return { message: null, reason: 'no_pending', resumeAt: null };
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
    .select('id, session_data, instagram_username, daily_actions_limit')
    .order('id', { ascending: true });
  if (error) return [];
  return data || [];
}

async function getPlatformScraperSessionById(id) {
  if (!id) return null;
  const sb = getSupabase();
  if (!sb) return null;
  const { data, error } = await sb
    .from('cold_dm_platform_scraper_sessions')
    .select('id, session_data, instagram_username')
    .eq('id', id)
    .maybeSingle();
  if (error) return null;
  return data;
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
  const sessions = await getPlatformScraperSessions();
  if (sessions.length === 0) {
    const clientSession = await getScraperSession(clientId);
    if (clientSession) return { source: 'client', session: clientSession, platformSessionId: null };
    return null;
  }
  const sessionIds = sessions.map((s) => s.id);
  const usage = await getPlatformScraperUsageToday(sessionIds);
  const candidates = sessions.filter((s) => (usage[s.id] || 0) < (s.daily_actions_limit || 500));
  if (candidates.length === 0) {
    const clientSession = await getScraperSession(clientId);
    if (clientSession) return { source: 'client', session: clientSession, platformSessionId: null };
    return null;
  }
  candidates.sort((a, b) => (usage[a.id] || 0) - (usage[b.id] || 0));
  const picked = candidates[0];
  return {
    source: 'platform',
    session: { session_data: picked.session_data, instagram_username: picked.instagram_username },
    platformSessionId: picked.id,
  };
}

async function recordScraperActions(platformSessionId, count) {
  if (!platformSessionId || count <= 0) return;
  const sb = getSupabase();
  if (!sb) return;
  const today = new Date().toISOString().slice(0, 10);
  const { data: existing } = await sb
    .from('cold_dm_scraper_daily_usage')
    .select('id, actions_count')
    .eq('platform_scraper_session_id', platformSessionId)
    .eq('usage_date', today)
    .maybeSingle();
  const newCount = (existing?.actions_count || 0) + count;
  if (existing) {
    await sb
      .from('cold_dm_scraper_daily_usage')
      .update({ actions_count: newCount, updated_at: new Date().toISOString() })
      .eq('id', existing.id);
  } else {
    await sb.from('cold_dm_scraper_daily_usage').insert({
      platform_scraper_session_id: platformSessionId,
      usage_date: today,
      actions_count: newCount,
    });
  }
}

async function savePlatformScraperSession(sessionData, instagramUsername, dailyActionsLimit = 500) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');
  const username = (instagramUsername || '').trim().replace(/^@/, '');
  if (!username) throw new Error('Instagram username required');
  const { data, error } = await sb
    .from('cold_dm_platform_scraper_sessions')
    .upsert(
      {
        session_data: sessionData,
        instagram_username: username,
        daily_actions_limit: Math.max(1, parseInt(dailyActionsLimit, 10) || 500),
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'instagram_username' }
    )
    .select('id')
    .single();
  if (error) {
    const { error: insertErr } = await sb.from('cold_dm_platform_scraper_sessions').insert({
      session_data: sessionData,
      instagram_username: username,
      daily_actions_limit: Math.max(1, parseInt(dailyActionsLimit, 10) || 500),
    });
    if (insertErr) throw insertErr;
    return;
  }
  return data?.id;
}

// --- Scrape jobs ---
async function createScrapeJob(clientId, targetUsername, leadGroupId = null, scrapeType = 'followers', postUrls = null, platformScraperSessionId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const payload = {
    client_id: clientId,
    target_username: targetUsername,
    status: 'running',
    scraped_count: 0,
    started_at: new Date().toISOString(),
  };
  if (leadGroupId) payload.lead_group_id = leadGroupId;
  if (scrapeType) payload.scrape_type = scrapeType;
  if (postUrls && Array.isArray(postUrls) && postUrls.length) payload.post_urls = postUrls;
  if (platformScraperSessionId) payload.platform_scraper_session_id = platformScraperSessionId;
  const { data, error } = await sb
    .from('cold_dm_scrape_jobs')
    .insert(payload)
    .select('id')
    .single();
  if (error) throw error;
  return data?.id;
}

async function updateScrapeJob(jobId, updates) {
  const sb = getSupabase();
  if (!sb || !jobId) throw new Error('Supabase or jobId missing');
  const payload = { ...updates };
  if (payload.status === 'completed' || payload.status === 'failed' || payload.status === 'cancelled') {
    payload.finished_at = new Date().toISOString();
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
    const { error } = await sb.from('cold_dm_scrape_jobs').update({ status: 'cancelled', finished_at: new Date().toISOString() }).eq('id', jobId).eq('client_id', clientId);
    if (error) throw error;
    return true;
  }
  const { data: running } = await sb
    .from('cold_dm_scrape_jobs')
    .select('id')
    .eq('client_id', clientId)
    .eq('status', 'running')
    .order('started_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (running?.id) {
    const { error } = await sb
      .from('cold_dm_scrape_jobs')
      .update({ status: 'cancelled', finished_at: new Date().toISOString() })
      .eq('id', running.id);
    if (error) throw error;
    return true;
  }
  return false;
}

/** Returns Set of normalised usernames in cold_dm_scrape_blocklist (do not scrape as leads). */
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
 * @param {string} clientId
 * @param {string[]|{ username: string, display_name?: string }[]} leadsOrUsernames - Usernames only (e.g. comment scrape) or { username, display_name } from follower scrape.
 */
async function upsertLeadsBatch(clientId, leadsOrUsernames, source, leadGroupId = null) {
  const sb = getSupabase();
  if (!sb || !clientId) throw new Error('Supabase or clientId missing');
  const rows = leadsOrUsernames.map((item) => {
    const username = typeof item === 'string' ? item : item.username;
    const displayName = typeof item === 'string' ? null : (item.display_name || null);
    let first_name = null;
    let last_name = null;
    if (displayName && typeof displayName === 'string') {
      const trimmed = displayName.trim();
      const firstWord = trimmed.split(/\s+/)[0] || null;
      const spaceIdx = trimmed.indexOf(' ');
      if (spaceIdx > 0) {
        first_name = firstWord;
        last_name = trimmed.slice(spaceIdx + 1).trim();
      } else {
        first_name = firstWord;
      }
    }
    const row = {
      client_id: clientId,
      username: normalizeUsername(username),
      source: source || null,
      added_at: new Date().toISOString(),
    };
    if (leadGroupId) row.lead_group_id = leadGroupId;
    if (displayName && typeof displayName === 'string') row.display_name = displayName;
    if (first_name != null) row.first_name = first_name;
    if (last_name != null) row.last_name = last_name;
    return row;
  });
  if (rows.length === 0) return 0;
  const { error } = await sb
    .from('cold_dm_leads')
    .upsert(rows, {
      onConflict: 'client_id,username',
      ignoreDuplicates: !leadGroupId,
    });
  if (error) throw error;
  return rows.length;
}

// --- Campaigns ---
// Campaign config (timezone, schedule, limits, delays) is read from DB on every get-next-work / can-run check. Do not cache; re-evaluate when user changes campaign in dashboard.
async function getActiveCampaigns(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return [];
  const { data, error } = await sb
    .from('cold_dm_campaigns')
    .select(
      'id, name, message_template_id, message_group_id, schedule_start_time, schedule_end_time, timezone, daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec'
    )
    .eq('client_id', clientId)
    .eq('status', 'active')
    .order('created_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

/**
 * Returns a short hint for why there is no sendable work for this client (e.g. no active campaigns, or campaigns are stopped).
 */
async function getNoWorkHint(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return '';
  const { data: campaigns } = await sb
    .from('cold_dm_campaigns')
    .select('id, name, status')
    .eq('client_id', clientId);
  if (!campaigns?.length) return 'No campaigns.';
  const active = campaigns.filter((c) => c.status === 'active');
  if (active.length > 0) return '';
  const withPending = await Promise.all(
    campaigns.map(async (c) => {
      const { count } = await sb
        .from('cold_dm_campaign_leads')
        .select('*', { count: 'exact', head: true })
        .eq('campaign_id', c.id)
        .eq('status', 'pending');
      return { name: c.name, status: c.status, pending: count ?? 0 };
    })
  );
  const stoppedWithPending = withPending.filter((c) => c.pending > 0 && c.status !== 'active');
  if (stoppedWithPending.length > 0) {
    const names = stoppedWithPending.map((c) => `"${c.name}" (${c.status})`).join(', ');
    return `Campaign(s) have pending leads but status is not active: ${names}. Set campaign to Active in the dashboard.`;
  }
  return 'No campaigns with status=active and pending leads.';
}

/**
 * Schedule window uses campaign timezone (cold_dm_campaigns.timezone). Not cold_dm_settings.timezone.
 * @param {string} [timezone] - IANA timezone from campaign row (e.g. America/New_York). If omitted/null, uses UTC.
 */
function isWithinSchedule(scheduleStart, scheduleEnd, timezone) {
  if (!scheduleStart && !scheduleEnd) return true;
  const now = new Date();
  let current;
  if (timezone) {
    try {
      current = now.toLocaleTimeString('en-CA', { timeZone: timezone, hour12: false });
      if (current.length === 7) current = '0' + current;
    } catch (e) {
      current = `${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
    }
  } else {
    current = `${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
  }
  const start = normalizeScheduleTime(scheduleStart) || '00:00:00';
  const end = normalizeScheduleTime(scheduleEnd) || '23:59:59';
  if (start <= end) return current >= start && current <= end;
  return current >= start || current <= end;
}

async function getRandomMessageFromGroup(messageGroupId) {
  const sb = getSupabase();
  if (!sb || !messageGroupId) return null;
  const { data, error } = await sb
    .from('cold_dm_message_group_messages')
    .select('id, message_text')
    .eq('message_group_id', messageGroupId)
    .order('sort_order', { ascending: true });
  if (error || !data || data.length === 0) return null;
  const row = data[Math.floor(Math.random() * data.length)];
  return { id: row.id, message_text: row.message_text };
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

async function getNextPendingCampaignLead(clientId) {
  const sb = getSupabase();
  if (!sb || !clientId) return null;
  const settings = await getSettings(clientId);
  const campaigns = await getActiveCampaigns(clientId);
  for (const camp of campaigns) {
    const campaignTz = camp.timezone ?? null;
    if (!isWithinSchedule(camp.schedule_start_time, camp.schedule_end_time, campaignTz)) continue;
    let messageText = null;
    let messageGroupMessageId = null;
    if (camp.message_group_id) {
      const groupMsg = await getRandomMessageFromGroup(camp.message_group_id);
      if (groupMsg) {
        messageText = groupMsg.message_text;
        messageGroupMessageId = groupMsg.id;
      }
    }
    if (!messageText && camp.message_template_id) {
      messageText = await getMessageTemplateById(camp.message_template_id);
    }
    if (!messageText) continue;

    const { data: leadGroupRows } = await sb
      .from('cold_dm_campaign_lead_groups')
      .select('lead_group_id')
      .eq('campaign_id', camp.id);
    const leadGroupIds = (leadGroupRows || []).map((r) => r.lead_group_id).filter(Boolean);
    if (leadGroupIds.length === 0) continue;

    const { data: leadRows } = await sb
      .from('cold_dm_leads')
      .select('id, username')
      .eq('client_id', clientId)
      .in('lead_group_id', leadGroupIds);
    if (!leadRows || leadRows.length === 0) continue;

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
      const { data: pendingRow, error } = await sb
        .from('cold_dm_campaign_leads')
        .select('id, lead_id')
        .eq('campaign_id', camp.id)
        .eq('status', 'pending')
        .order('id', { ascending: true })
        .limit(1)
        .maybeSingle();
      if (error || !pendingRow?.lead_id) break;
      const { data: leadData } = await sb
        .from('cold_dm_leads')
        .select('username, first_name, last_name, display_name')
        .eq('id', pendingRow.lead_id)
        .eq('client_id', clientId)
        .maybeSingle();
      if (!leadData?.username) {
        await updateCampaignLeadStatus(pendingRow.id, 'failed').catch(() => {});
        continue;
      }
      const sent = await alreadySent(clientId, leadData.username);
      if (sent) {
        await updateCampaignLeadStatus(pendingRow.id, 'sent').catch(() => {});
        continue;
      }
      clRow = pendingRow;
      leadRow = leadData;
      break;
    }
    if (!clRow || !leadRow) continue;

    const [stats, hourlySent] = await Promise.all([getDailyStats(clientId), getHourlySent(clientId)]);
    const dailyLimit = camp.daily_send_limit ?? settings?.daily_send_limit ?? 100;
    const hourlyLimit = camp.hourly_send_limit ?? settings?.max_sends_per_hour ?? 20;
    if (stats.total_sent >= dailyLimit) continue;
    if (hourlySent >= hourlyLimit) continue;

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
      minDelaySec: camp.min_delay_sec,
      maxDelaySec: camp.max_delay_sec,
    };
  }
  return null;
}

/**
 * Add all leads from the campaign's lead groups into cold_dm_campaign_leads (status pending).
 * Only inserts when no row exists (ignores existing sent/failed). Returns count of rows inserted.
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

  const { data: leadRows } = await sb
    .from('cold_dm_leads')
    .select('id')
    .eq('client_id', clientId)
    .in('lead_group_id', leadGroupIds);
  if (!leadRows || leadRows.length === 0) return 0;

  let added = 0;
  for (const lead of leadRows) {
    const { data: existing } = await sb
      .from('cold_dm_campaign_leads')
      .select('id')
      .eq('campaign_id', campaignId)
      .eq('lead_id', lead.id)
      .maybeSingle();
    if (!existing) {
      const { error } = await sb.from('cold_dm_campaign_leads').upsert(
        { campaign_id: campaignId, lead_id: lead.id, status: 'pending' },
        { onConflict: 'campaign_id,lead_id', ignoreDuplicates: true }
      );
      if (!error) added += 1;
    }
  }
  return added;
}

async function updateCampaignLeadStatus(campaignLeadId, status, failureReason = null) {
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
  const { error } = await sb.from('cold_dm_campaign_leads').update(payload).eq('id', campaignLeadId);
  if (error) throw error;

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
  getSessions,
  getSessionsForCampaign,
  saveSession,
  alreadySent,
  logSentMessage,
  getDailyStats,
  getHourlySent,
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
  pickScraperSessionForJob,
  recordScraperActions,
  savePlatformScraperSession,
  getConversationParticipantUsernames,
  getScrapeBlocklistUsernames,
  getSentUsernames,
  createScrapeJob,
  updateScrapeJob,
  getScrapeJob,
  getLatestScrapeJob,
  cancelScrapeJob,
  upsertLead,
  upsertLeadsBatch,
  getActiveCampaigns,
  getMessageTemplateById,
  getNextPendingCampaignLead,
  updateCampaignLeadStatus,
  getClientIdsWithPauseZero,
  getNextPendingWorkAnyClient,
  getClientOutsideScheduleStatus,
  getClientNoWorkReason,
  getClientNoWorkResumeAt,
  getNoWorkHint,
  getFirstNameBlocklist,
  addCampaignLeadsFromGroups,
};
