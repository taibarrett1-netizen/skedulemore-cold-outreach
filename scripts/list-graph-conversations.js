#!/usr/bin/env node
'use strict';

/**
 * Pull conversations from Instagram Graph API and list participant usernames
 * with inbound reply counts.
 *
 * Usage:
 *   node scripts/list-graph-conversations.js
 *
 * Required env vars:
 *   ACCESS_TOKEN  — Instagram Graph API access token
 *   IG_ACCOUNT_ID — Instagram account ID (numeric)
 *   MAX_CONVOS    — (optional) max conversations to fetch, default 1300
 */

const ACCESS_TOKEN = process.env.ACCESS_TOKEN;
const IG_ACCOUNT_ID = process.env.IG_ACCOUNT_ID;
const MAX_CONVOS = parseInt(process.env.MAX_CONVOS || '1300', 10);
const GRAPH_BASE = 'https://graph.instagram.com/v20.0';

async function fetchJson(url) {
  const res = await fetch(url);
  const data = await res.json();
  if (data.error) throw new Error(`Graph API: ${data.error.message} (code ${data.error.code})`);
  return data;
}

function normalizeUsername(u) {
  return String(u || '').trim().toLowerCase().replace(/^@/, '');
}

async function main() {
  if (!ACCESS_TOKEN || !IG_ACCOUNT_ID) {
    console.error('Usage: ACCESS_TOKEN=... IG_ACCOUNT_ID=... node scripts/list-graph-conversations.js');
    process.exit(1);
  }

  console.log(`Fetching up to ${MAX_CONVOS} conversations for account ${IG_ACCOUNT_ID}...\n`);

  const results = []; // { username, inboundCount, conversationId }
  let pageCount = 0;
  let url = `${GRAPH_BASE}/${IG_ACCOUNT_ID}/conversations?platform=instagram&fields=participants%7Bid%2Cusername%7D%2Cmessages%7Bfrom%7D&limit=100&access_token=${ACCESS_TOKEN}`;

  while (url && results.length < MAX_CONVOS) {
    pageCount++;
    process.stdout.write(`  Page ${pageCount} — ${results.length} conversations so far...\r`);

    let data;
    try {
      data = await fetchJson(url);
    } catch (e) {
      console.error(`\nFailed on page ${pageCount}:`, e.message);
      break;
    }

    const convos = data.data || [];
    if (!convos.length) break;

    // Log raw participants from first convo on first page to debug ID/username structure
    if (pageCount === 1 && results.length === 0 && convos.length > 0) {
      console.log('\nRAW first convo participants:', JSON.stringify(convos[0].participants?.data, null, 2));
      console.log('RAW first convo messages sample:', JSON.stringify((convos[0].messages?.data || []).slice(0, 3), null, 2));
      console.log('');
    }

    for (const convo of convos) {
      if (results.length >= MAX_CONVOS) break;

      const participants = convo.participants?.data || [];
      // Filter by username, not ID — the account's participant ID differs from IG_ACCOUNT_ID
      const other = participants.find(p => normalizeUsername(p.username) !== 'ninja.ugc');
      if (!other) continue;

      const username = normalizeUsername(other.username || other.name || '');
      const messages = convo.messages?.data || [];
      const inboundCount = messages.filter(m => m.from?.id === other.id).length;

      results.push({ username, inboundCount, participantId: other.id, convoId: convo.id });
    }

    url = data.paging?.next || null;
    if (url) await new Promise(r => setTimeout(r, 150));
  }

  console.log(`\nDone: ${pageCount} page(s), ${results.length} conversations fetched.\n`);

  // Summary
  const withReplies = results.filter(r => r.inboundCount > 0);
  const noReplies = results.filter(r => r.inboundCount === 0);
  const noUsername = results.filter(r => !r.username);

  console.log(`Total conversations:  ${results.length}`);
  console.log(`With inbound replies: ${withReplies.length}`);
  console.log(`No replies:           ${noReplies.length}`);
  console.log(`No username resolved: ${noUsername.length}`);

  if (withReplies.length) {
    console.log('\n--- Accounts with replies ---');
    for (const r of withReplies) {
      console.log(`  @${r.username || '(unknown)'}  —  ${r.inboundCount} inbound message(s)`);
    }
  }

  // Also dump full list to a file so you can cross-reference
  const fs = require('fs');
  const outPath = require('path').join(__dirname, 'graph-conversations-dump.json');
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(`\nFull list written to scripts/graph-conversations-dump.json`);
}

main().catch(e => {
  console.error('Fatal:', e.message || e);
  process.exit(1);
});
