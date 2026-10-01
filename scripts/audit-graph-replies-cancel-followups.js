#!/usr/bin/env node
'use strict';

/**
 * Audit Instagram Graph API conversations and cancel pending follow-ups
 * for accounts that have already replied.
 *
 * Usage:
 *   node scripts/audit-graph-replies-cancel-followups.js           # dry-run
 *   node scripts/audit-graph-replies-cancel-followups.js --confirm # actually cancel
 *
 * Required env vars:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *   CLIENT_ID  (optional — defaults to the hardcoded value below)
 */

const { createClient } = require('@supabase/supabase-js');

const CLIENT_ID = process.env.CLIENT_ID || 'f8973b06-c52d-42e3-bfcd-dacb64cd18cd';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CONFIRM = process.argv.includes('--confirm');
const GRAPH_API_BASE = 'https://graph.instagram.com/v20.0';

function normalizeUsername(u) {
  return String(u || '').trim().toLowerCase().replace(/^@/, '');
}

async function fetchJson(url) {
  const res = await fetch(url);
  const data = await res.json();
  if (!res.ok) throw new Error(`Graph API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

async function main() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  // 1. Get Instagram account credentials for this client
  console.log(`\nLoading Instagram account for client ${CLIENT_ID}...`);
  const { data: accounts, error: accErr } = await sb
    .from('instagram_accounts')
    .select('instagram_account_id, access_token, username')
    .eq('client_id', CLIENT_ID)
    .eq('is_active', true);

  if (accErr) { console.error('Failed to load instagram_accounts:', accErr.message); process.exit(1); }
  if (!accounts?.length) { console.error('No active instagram_accounts found for this client'); process.exit(1); }

  const account = accounts[0];
  const { instagram_account_id, access_token, username: igUsername } = account;
  console.log(`  IG account: @${igUsername || '?'} (id=${instagram_account_id})`);

  // 2. Load all pending follow-ups for this client
  console.log('\nLoading pending follow-ups...');
  const { data: pendingRows, error: fuErr } = await sb
    .from('cold_dm_follow_up_queue')
    .select('id, username, follow_up_index, scheduled_for, source_sent_at')
    .eq('client_id', CLIENT_ID)
    .eq('status', 'pending');

  if (fuErr) { console.error('Failed to load follow-up queue:', fuErr.message); process.exit(1); }
  if (!pendingRows?.length) { console.log('No pending follow-ups found — nothing to do.'); return; }

  console.log(`  ${pendingRows.length} pending follow-up rows`);

  // Build username → [row ids] map
  const usernameToIds = new Map();
  for (const row of pendingRows) {
    const u = normalizeUsername(row.username);
    if (!u) continue;
    if (!usernameToIds.has(u)) usernameToIds.set(u, []);
    usernameToIds.get(u).push(row.id);
  }
  console.log(`  ${usernameToIds.size} unique usernames with pending follow-ups`);

  // 3. Paginate Graph API conversations
  console.log('\nPaginating Instagram Graph API conversations...');

  const toCancel = new Map(); // username → [row ids] that should be cancelled
  let pageCount = 0;
  let conversationsChecked = 0;
  let repliesFound = 0;

  // Request participants and just the last 10 messages per conversation to check for replies
  let url = `${GRAPH_API_BASE}/me/conversations?platform=instagram` +
    `&fields=participants%7Bid%2Cusername%7D%2Cmessages.limit(10)%7Bfrom%2Ccreated_time%7D` +
    `&limit=25` +
    `&access_token=${access_token}`;

  while (url) {
    pageCount++;
    process.stdout.write(`  Page ${pageCount} (${conversationsChecked} convos checked, ${repliesFound} with replies)...\r`);

    let data;
    try {
      data = await fetchJson(url);
    } catch (e) {
      console.error(`\nGraph API fetch failed on page ${pageCount}:`, e.message);
      break;
    }

    const convos = data.data || [];

    for (const convo of convos) {
      conversationsChecked++;

      // Find the brand participant (the one that is NOT our account)
      const participants = convo.participants?.data || [];
      const brandParticipant = participants.find(p => p.id !== instagram_account_id);
      if (!brandParticipant) continue;

      const brandUsername = normalizeUsername(brandParticipant.username || '');
      if (!brandUsername) continue;

      // Only bother checking messages if this username has pending follow-ups
      if (!usernameToIds.has(brandUsername)) continue;

      // Check if the brand has sent any message (from.id == brandParticipant.id)
      const messages = convo.messages?.data || [];
      const theyReplied = messages.some(msg => msg.from?.id === brandParticipant.id);

      if (theyReplied) {
        repliesFound++;
        toCancel.set(brandUsername, usernameToIds.get(brandUsername));
      }
    }

    url = data.paging?.next || null;

    // Small delay to be polite to the API
    if (url) await new Promise(r => setTimeout(r, 100));
  }

  console.log(`\n  Done: ${pageCount} pages, ${conversationsChecked} conversations checked, ${repliesFound} with inbound replies`);

  // 4. Report
  console.log(`\n--- Results ---`);
  if (toCancel.size === 0) {
    console.log('No pending follow-ups match accounts that have replied. Nothing to cancel.');
    return;
  }

  const allIdsToCanel = [];
  for (const [username, ids] of toCancel) {
    allIdsToCanel.push(...ids);
    console.log(`  @${username} — cancelling ${ids.length} follow-up(s)`);
  }

  console.log(`\nTotal: ${toCancel.size} accounts, ${allIdsToCanel.length} follow-up rows to cancel`);

  if (!CONFIRM) {
    console.log('\nDry-run mode — no changes made. Re-run with --confirm to cancel.');
    return;
  }

  // 5. Cancel in batches of 500 (Supabase IN clause limit)
  console.log('\nCancelling...');
  const BATCH = 500;
  const nowIso = new Date().toISOString();
  let cancelled = 0;

  for (let i = 0; i < allIdsToCanel.length; i += BATCH) {
    const batch = allIdsToCanel.slice(i, i + BATCH);
    const { error: updErr } = await sb
      .from('cold_dm_follow_up_queue')
      .update({
        status: 'cancelled',
        cancel_reason: 'graph_api_reply_audit',
        cancelled_at: nowIso,
        updated_at: nowIso,
      })
      .in('id', batch)
      .eq('status', 'pending'); // guard: only touch pending rows

    if (updErr) {
      console.error(`Batch ${i / BATCH + 1} update failed:`, updErr.message);
    } else {
      cancelled += batch.length;
    }
  }

  console.log(`\nDone. Cancelled ${cancelled} follow-up rows.`);
}

main().catch(e => {
  console.error('\nFatal error:', e.message || e);
  process.exit(1);
});
