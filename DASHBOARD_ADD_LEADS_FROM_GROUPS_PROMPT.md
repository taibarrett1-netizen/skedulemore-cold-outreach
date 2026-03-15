# Dashboard: Fix "Add all leads from groups" and pending count

## Problem
User clicks "Add all leads from groups" and sees "✔ 1 lead(s) from groups added to campaign", but the campaign still shows **pending 0** and starting the campaign says "add leads before running campaign".

## Cause
Either (1) the dashboard is writing to `cold_dm_campaign_leads` in a way that fails (e.g. RLS/trigger) or doesn’t match what the VPS reads, or (2) the pending count / "add leads before running" check reads from a different source or is cached.

## VPS change (done in Cold DMs V1 repo)
- **New endpoint:** `POST /api/campaigns/add-leads-from-groups`  
  Body: `{ campaignId: string, clientId: string }`  
  Response: `{ ok: true, added: number }` (number of rows inserted into `cold_dm_campaign_leads` with status `pending`).  
  Uses same logic as the bot: campaign’s lead groups → leads in those groups → upsert into `cold_dm_campaign_leads` (insert only when no row exists).  
  Requires Supabase and (if set) `COLD_DM_API_KEY` (Bearer or `x-api-key`).

## Dashboard changes to make

1. **"Add all leads from groups" button**  
   When the user clicks it, call the VPS:
   - `POST <VPS_BASE_URL>/api/campaigns/add-leads-from-groups`  
   - Body: `{ campaignId: <campaign.id>, clientId: <campaign.client_id or current client> }`  
   - Headers: if the dashboard uses an API key for the VPS, send `Authorization: Bearer <key>` or `x-api-key: <key>`.  
   Use the returned `added` count for the toast (e.g. "✔ {added} lead(s) from groups added to campaign").  
   After success, refetch campaign stats so the UI shows the updated pending count.

2. **Pending count**  
   Ensure the number shown as "Pending" (and used for "add leads before running campaign") comes from the **same** source: count of rows in `cold_dm_campaign_leads` for that campaign with `status = 'pending'`.  
   If you use the VPS for stats, add or use an endpoint that returns this count; if you read from Supabase in the dashboard, query `cold_dm_campaign_leads` (with RLS so only the client’s data is visible) and count where `campaign_id = <id>` and `status = 'pending'`.  
   Do not infer pending from "leads in groups" or a different table; only `cold_dm_campaign_leads.status = 'pending'` is authoritative.

3. **"Add leads before running campaign"**  
   Before allowing Start, check the same pending count (e.g. `pendingCount > 0`). If the dashboard calls the VPS to add leads and then refetches, the updated count will unblock Start.

## Summary
- Wire "Add all leads from groups" to `POST /api/campaigns/add-leads-from-groups` on the VPS; show toast from `added` and refetch.
- Derive pending and "can start" from `cold_dm_campaign_leads` where `status = 'pending'` for that campaign.
