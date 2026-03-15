# Cold DM VPS – Handoff

**Single handoff for the Cold DM Cursor project.** The setter dashboard (this repo) has created the Cold DM tables in the **same** Supabase project and integrated the Cold Outreach tab. The VPS must read/write these tables and expose the API (connect, status, start, stop, scraper, and campaign-aware sending).

---

## 1. Supabase connection

- **Env vars:** `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` (same project as the setter app).
- Use the **service role** key so the bot can read/write all Cold DM tables without RLS. The setter UI uses anon key + RLS; the VPS does not impersonate a user.

---

## 2. Tables (all by `client_id`)

### Base tables

**cold_dm_message_templates**  
`id`, `client_id`, `message_text`, `name`, `sort_order`, `created_at`, `updated_at`

**cold_dm_leads**  
`id`, `client_id`, `username`, `source`, `added_at`, **`lead_group_id`** (nullable, FK → cold_dm_lead_groups), **`first_name`** (TEXT, nullable), **`last_name`** (TEXT, nullable). Unique: `(client_id, username)`.  
- Use `first_name` / `last_name` when substituting message variables (see **Message variables** below). If null, VPS may derive from username (e.g. `john_doe` → First: John, Last: Doe).

**cold_dm_settings** (one row per client)

| Column            | Type      | Nullable |
|-------------------|-----------|----------|
| id                | UUID      | NO (PK)  |
| client_id         | UUID      | NO (UNIQUE) |
| daily_send_limit  | INT       | NO (default 50) |
| min_delay_minutes | INT       | NO (default 2) |
| max_delay_minutes | INT       | NO (default 5) |
| max_sends_per_hour| INT       | NO (default 20) |
| instagram_username| TEXT      | YES      |
| **timezone**      | TEXT      | YES      |
| created_at        | TIMESTAMPTZ | YES    |
| updated_at        | TIMESTAMPTZ | YES    |

- **timezone:** IANA (e.g. `America/New_York`) for schedule windows and daily reset. When null, use UTC.
- **Limits:** daily_send_limit and max_sends_per_hour are **per client** (all accounts combined).

**cold_dm_instagram_sessions**  
One row per client. `client_id`, `session_data` (JSONB), `instagram_username`, `updated_at`. Session only; no password.

**cold_dm_sent_messages**  
`id`, `client_id`, `username`, `message`, `sent_at`, `status` ('success'|'failed'), **`campaign_id`**, **`message_group_id`** (nullable), **`message_group_message_id`** (nullable).  
- **`message`** must be the **actual sent text after variable substitution** (see **Message variables**). This is what the lead received. The bot/conversation system uses this (or the linked outreach_message) so the first message in conversation history matches what was sent and routing works correctly.
- **`message_group_message_id`** – **REQUIRED** when sending from a message group. Record the `id` of the specific `cold_dm_message_group_messages` row that was sent. This enables per-message Outreach Start routing (each message in the group can route to a different script path).

**cold_dm_daily_stats**  
`client_id`, `date`, `total_sent`, `total_failed`. Unique: `(client_id, date)`.

**cold_dm_control**  
One row per client. `pause` (0 = may send, 1 = stopped). Start/Stop in the dashboard only update this; they do **not** start or stop the worker process. When a campaign completes (no pending leads), the VPS sets `pause = 1` for that client so sending stops.

### Scraper

**cold_dm_scraper_sessions**  
One row per client. Same format as `cold_dm_instagram_sessions`; separate account for scraping only.

**cold_dm_scrape_jobs**  
`id`, `client_id`, `target_username`, `status` ('running'|'completed'|'failed'|'cancelled'), `scraped_count`, `error_message`, `lead_group_id`, `started_at`, `finished_at`, `scrape_type`, `post_urls` (for comment scrape). Index: `(client_id, started_at DESC)`.

**cold_dm_scrape_blocklist**  
`id`, `client_id`, `username`, `added_at`. UNIQUE `(client_id, username)`. Usernames in this table must never be scraped or added as leads; the scraper must skip them when writing to `cold_dm_leads`.

### Lead groups and message groups

**cold_dm_lead_groups**  
`id`, `client_id`, `name`, `created_at`.

**cold_dm_campaign_lead_groups**  
`(campaign_id, lead_group_id)` – which lead groups a campaign targets.

**cold_dm_message_groups**  
`id`, `client_id`, `name`, `outreach_message_id`, `created_at`, `updated_at`.

**cold_dm_message_group_messages**  
`id`, `message_group_id`, `message_text`, `sort_order`, `created_at`, **`outreach_message_id`** (nullable). Pick a **random** message from the group per send. When the dashboard links a message group to the script, each message gets its own `outreach_message_id` so each can route to a different Outreach Start path. The VPS must set `message_group_message_id` on `cold_dm_sent_messages` when sending.

### Campaigns

**cold_dm_campaigns**

| Column               | Type      | Nullable |
|----------------------|-----------|----------|
| id                   | UUID      | NO (PK)  |
| client_id            | UUID      | NO       |
| name                 | TEXT      | NO       |
| message_template_id  | UUID      | YES (legacy) |
| message_group_id     | UUID      | YES      |
| status               | TEXT      | NO       |
| schedule_start_time  | TIME      | YES      |
| schedule_end_time    | TIME      | YES      |
| **timezone**         | TEXT      | YES      |
| daily_send_limit     | INT       | YES      |
| hourly_send_limit    | INT       | YES      |
| min_delay_sec        | INT       | YES      |
| max_delay_sec        | INT       | YES      |
| created_at           | TIMESTAMPTZ | YES    |
| updated_at           | TIMESTAMPTZ | YES    |

- **status:** `draft` | `active` | `paused` | `completed`. Only `active` campaigns are sent.
- **timezone:** IANA (e.g. `America/New_York`) for **this campaign only**. Schedule start/end are interpreted in this timezone. When null, use UTC. **Per campaign;** the dashboard no longer has global timezone (removed).
- **schedule_start_time / schedule_end_time:** Send only when current time **in this campaign's timezone** (`cold_dm_campaigns.timezone` or UTC) is within this range.
- **daily_send_limit, hourly_send_limit, min_delay_sec, max_delay_sec:** **Per campaign.** When set, override `cold_dm_settings`; when null, use global. Delays are **between messages** (seconds).
- **Recheck on change:** When the user changes timezone, schedule times, DM limits, or delays for a campaign, the sender must re-evaluate on the next cycle whether that campaign can run (read current row each cycle; do not cache).

**cold_dm_campaign_instagram_sessions**  
`(campaign_id, instagram_session_id)`. Which accounts a campaign uses. Empty = all sessions for client.

**cold_dm_campaign_leads**  
`id`, `campaign_id`, `lead_id`, `status` ('pending'|'sent'|'failed'), `sent_at`. UNIQUE `(campaign_id, lead_id)`.

- **Trigger:** `cold_dm_campaign_leads_lead_in_group_trigger` – only allows INSERT/UPDATE if the lead's `lead_group_id` is one of the campaign's selected lead groups. Ensures we never send to unassigned leads.

---

### Message variables (personalization)

Templates and message group messages can use these placeholders. The VPS must **substitute** them when building the DM text and store the **resulting text** in `cold_dm_sent_messages.message` (so the bot has the real first message in conversation history).

| Placeholder | Meaning |
|-------------|--------|
| `{{username}}` or `{{instagram_username}}` | Lead's Instagram handle (no @) |
| `{{first_name}}` | From `cold_dm_leads.first_name` or derived from username |
| `{{last_name}}` | From `cold_dm_leads.last_name` or derived from username |
| `{{full_name}}` | `first_name + " " + last_name`, or username if neither set |

If `first_name` / `last_name` are not stored for a lead, the VPS derives them from the username (e.g. `john_doe` → First: John, Last: Doe). The dashboard stores optional `first_name` and `last_name` on leads and shows variable help in the message composer.

**Bot sync:** Conversations are linked to an outreach message via `outreach_message_id` (from the message group's linked `outreach_messages` row). The **substituted** message (what was actually sent) must appear as the first message in conversation history so the AI and script routing see the real outreach. The dashboard/backend uses `cold_dm_sent_messages.message` and/or `outreach_sends` to build that history; ensure the VPS writes the final substituted text into `cold_dm_sent_messages.message` and **`message_group_message_id`** (the id of the `cold_dm_message_group_messages` row that was sent). When `message_group_message_id` is set, the bot uses per-message routing (each message can route to its own Outreach Start path).

**Connect SkeduleMore + GHL when sending:** When you send a cold DM, call the dashboard's **cold-dm-on-send** Edge Function after each successful send. The dashboard **creates the conversation** with tag `cold-outreach` and backfills the first message so the thread appears in SkeduleMore. GHL auto-creates the contact; add a **Contact Created** trigger in GHL that fires the **same** webhook URL you use for Customer Replied (`ghl-inbound-webhook`). The dashboard then matches the new contact to that conversation (by client + contact name + time) and sets `ghl_contact_id`. When the lead replies, the Instagram webhook removes the `cold-outreach` tag so it becomes a normal conversation. See **§2a. Cold DM on-send** and **§2b. GHL Contact Created** below.

---

## 2a. Cold DM on-send (cold-dm-on-send)

**Purpose:** Create the SkeduleMore conversation with tag `cold-outreach` and backfill the first "us" message so the thread appears in the dashboard. No `ghl_contact_id` is required; the GHL link is set when GHL fires the Contact Created webhook to the same ghl-inbound-webhook URL.

**When to call:** Immediately after each successful cold DM send (after inserting `cold_dm_sent_messages`).

**Endpoint:** `POST https://<SUPABASE_PROJECT_REF>.supabase.co/functions/v1/cold-dm-on-send`

**Auth:** `Authorization: Bearer <COLD_DM_API_KEY>`. Use the same API key the dashboard uses to talk to the VPS (set in dashboard env as `COLD_DM_API_KEY`; the VPS must have this value to call the Edge Function).

**Body (JSON):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `client_id` | UUID | Yes | Client UUID. |
| `instagram_thread_id` | string | Yes | Instagram thread / recipient user ID (the ID you used to send the DM). |
| `username` | string | Yes | Lead username (with or without @). |
| `message_text` | string | No | The substituted message that was sent; used to backfill the first message in the thread. |
| `sent_at` | string (ISO) | No | When the message was sent; default = now. |
| `message_group_id` | UUID | No | `cold_dm_message_groups.id` for the group that was used. |
| `message_group_message_id` | UUID | No | `cold_dm_message_group_messages.id` of the specific message sent (for outreach routing). |
| `ghl_contact_id` | string | No | If the VPS ever has the GHL contact ID at send time, pass it and the dashboard will store it in the mapping table as well. |

**Behaviour:** The function gets or creates a conversation for `(client_id, instagram_thread_id)` with `tags = ['cold-outreach']`, resolves `outreach_message_id` from the message group when provided, and backfills the first "us" message if missing. If `ghl_contact_id` is passed, it also upserts into `cold_dm_ghl_contact_mapping`. When the lead replies, the Instagram webhook removes the `cold-outreach` tag so the conversation becomes a normal one.

**Response:** `200` with `{ "ok": true, "conversation_id": "<uuid>", "created": true|false }`. On error, `4xx/5xx` with `{ "error": "..." }`.

---

## 2b. GHL Contact Created (same ghl-inbound-webhook URL)

**Purpose:** Link the GHL contact to the cold-outreach conversation. GHL auto-creates the contact when you send the DM; you do **not** create the contact from the VPS to avoid duplicates.

**Setup:** In GHL, add a **second** trigger to the same automation (or a second automation) that uses the **same** webhook URL as Customer Replied: **Contact Created** → Fire webhook POST to your existing `ghl-inbound-webhook` Supabase function URL. No new endpoint is required.

**Behaviour:** The webhook detects Contact Created (no message text, has contact name/dateAdded). It finds a conversation for that client with tag `cold-outreach` whose participant name matches the contact name and whose `created_at` is within ±30s of the contact's `dateAdded`, then sets `ghl_contact_id` on that conversation. Tags and booking then work for that thread.

---

## 3. Session storage

- **Table:** `cold_dm_instagram_sessions` (sending) and `cold_dm_scraper_sessions` (scraping).
- Store **session only** (e.g. cookies JSON) in `session_data`; never store password.
- One row per client per table. Load by `client_id` when running the bot or scraper.

---

## 4. Connect endpoint (including 2FA)

**Step 1 – Connect**

- **POST /api/instagram/connect**  
  Body: `{ "username", "password", "clientId" }` (all required when Supabase is used). Log in with Puppeteer; keep the same browser session when 2FA is required. Never store password.

**Responses from step 1:**

1. **Success (no 2FA):** `{ "ok": true }` → Dashboard shows success and clears the password from the form.
2. **2FA required:** `{ "ok": false, "code": "two_factor_required", "message": "...", "pending2FAId": "<id>" }` → Dashboard must **not** show a generic error. It stores `pending2FAId` and `clientId`, and shows a modal for the 6-digit code. Do **not** call connect again with username/password + code; that would start a new login and trigger a second code.
3. **Other errors:** `{ "ok": false, "error": "..." }` or HTTP 4xx/5xx → Dashboard shows `error`; no 2FA modal.

**Step 2 – Submit 2FA code**

- **POST /api/instagram/connect/2fa**  
  Body: `{ "pending2FAId", "twoFactorCode", "clientId" }` (all required). Use the **same** browser session identified by `pending2FAId`; submit the user's code (digits only, first 6 chars). Instagram does not send a second code.

**Responses from step 2:**

1. **Success:** `{ "ok": true }` → Dashboard closes the modal, shows success, clears password and code.
2. **Error:** `{ "ok": false, "error": "..." }` (wrong/expired code or expired session) → Dashboard shows the error and leaves the modal open so the user can try again or cancel.

**Cancel** in the modal discards the pending step; the user can click Connect again from scratch (they will get a new code from Instagram on the next attempt).

---

## 5. Control and status (always-on worker)

- **Worker process:** One long-running process (e.g. `pm2 start cli.js --name ig-dm-bot -- --start`). It runs forever and serves all clients with `pause = 0` and pending work. When there is no work for any client it sleeps 30–60s and re-checks; it does **not** exit.
- **cold_dm_control.pause:** 0 = may send, 1 = stopped. **POST /api/control/start** sets `pause = 0` for that client and optionally ensures the worker process is running (e.g. start it only if not already running). **POST /api/control/stop** sets `pause = 1` for that client only; it does **not** stop the worker process.
- **Campaign completed:** When a campaign has no remaining pending leads (last lead sent or failed), the VPS must set `cold_dm_campaigns.status = 'completed'` and set `cold_dm_control.pause = 1` for that campaign's `client_id` so that client stops sending until the user starts again.
- **GET /api/status:** Return `{ "processRunning": true | false }`; optionally todaySent, todayFailed, leadsTotal, leadsRemaining from Supabase. Per-client "is sending" = `pause === 0` and has pending work.

---

## 6. Limits and schedule (important)

- **Read current value every run.** The sender must read each campaign's **timezone**, schedule, and limits **on each send cycle**, not cache them. If the user changes timezone, schedule times, daily/hourly limits, or delays, the next cycle must recheck whether the campaign can run (e.g. new timezone may put current time outside the window). No auto start/stop—the campaign stays active; the sender just applies the current config.
- **Scope:** Global limits (when campaign override is null) = **per client** from `cold_dm_settings`. Campaign limits = **per campaign** from `cold_dm_campaigns`.
- **Schedule timezone:** Use **per campaign** `cold_dm_campaigns.timezone` (IANA). When null, use UTC. For each campaign, compare current time in that campaign's timezone to `schedule_start_time`–`schedule_end_time`. Do not use `cold_dm_settings.timezone` for schedule (dashboard no longer exposes global timezone).

---

## 7. Scraper API

- **POST /api/scraper/connect** – Body: `{ username, password, clientId }`. Save session to `cold_dm_scraper_sessions`.
- **GET /api/scraper/status** – Query: `clientId`. Return connected, instagram_username, optional current job.
- **POST /api/scraper/start** – Body: `{ clientId, target_username?, post_urls?, max_leads?, lead_group_id?, scrape_type? }`. For followers: `target_username`; for comments: `post_urls`, `scrape_type: 'comments'`. Insert job, run scrape in background, assign leads to `lead_group_id`.

**Scraper filter (do not add as leads):** When writing scraped usernames to `cold_dm_leads`, **do not insert** any username that:
1. **Already cold-messaged:** exists in `cold_dm_sent_messages` for this `client_id` (same username, normalise e.g. lower/trim/remove @).
2. **Already has a conversation:** exists in `conversations` for this `client_id` (match `conversations.participant_username` to the scraped username, normalised).
3. **Blocklisted:** exists in `cold_dm_scrape_blocklist` for this `client_id` (same username, normalised).

Filter **during** the scrape before inserting into `cold_dm_leads` so lead groups are not filled with dead leads. The VPS should query these three sources (e.g. distinct usernames for the client) and skip any scraped handle that appears in any of them.
- **POST /api/scraper/stop** – Body: `{ clientId, jobId? }`. Set job `status = 'cancelled'`.

---

## 8. Campaign-aware sender loop (multi-tenant)

1. **Single worker:** One process loops: get next pending work from **any** client with `pause = 0` (e.g. `getNextPendingWorkAnyClient()`). If none, sleep 30–60s and re-check. If work, resolve `client_id`, load that client's adapter and settings, then do one send for that client.
2. **Pause:** Only consider clients where `cold_dm_control.pause = 0`. Optionally re-check pause before send for that client.
3. **Schedule:** For each active campaign, send only when current time **in that campaign's timezone** (`cold_dm_campaigns.timezone` or UTC) is inside `schedule_start_time`–`schedule_end_time`.
4. **Per send:** For the chosen work item (campaign lead): get sessions from `cold_dm_campaign_instagram_sessions` for that campaign or all sessions for client if empty; pick message from campaign's message group (or legacy template); apply campaign or global limits and delays.
5. **After send:** Update `cold_dm_campaign_leads` to `sent`/`failed` and `sent_at`; insert `cold_dm_sent_messages` with `campaign_id`, `message_group_id`, **`message_group_message_id`** (the id of the specific message row that was sent); update `cold_dm_daily_stats`. Then call the **cold-dm-on-send** Edge Function (§2a) with `client_id`, `instagram_thread_id`, `username`, and `message_text` so the dashboard creates the conversation with tag `cold-outreach`. GHL will auto-create the contact; add a **Contact Created** trigger in GHL to the same ghl-inbound-webhook URL (§2b) so the contact is linked by name + time.
6. **Campaign completed:** When a campaign has no remaining pending leads, set `cold_dm_campaigns.status = 'completed'` and set `cold_dm_control.pause = 1` for that campaign's `client_id` so the client stops. The worker keeps running and will simply not pick that client again until the user hits Start.

---

## 9. Optional: warm behaviour

Between sends or on a timer: light activity (scroll feed, like 1–3 posts, view comments) with random delays to look less robotic. VPS only; no new endpoints.

---

## 10. Summary

1. **Supabase:** Same project; service role; read/write all cold_dm_* tables by `client_id`.
2. **Connect:** POST /api/instagram/connect; save session only to `cold_dm_instagram_sessions`.
3. **Control:** `cold_dm_control.pause`; Start/Stop only set pause (and Start optionally ensures worker is running). Worker is always-on; when a campaign has no pending leads, set campaign status to `completed` and set `pause = 1` for that client so it stops.
4. **Limits and timezone:** Read **current** limits and **per-campaign timezone** every send cycle; no auto start/stop when they change; global = per client, campaign = per campaign; schedule uses **each campaign's** `cold_dm_campaigns.timezone` (or UTC when null).
5. **Campaign leads:** Only leads in the campaign's selected lead groups (enforced by trigger on `cold_dm_campaign_leads`).
6. **Scraper:** Separate session, jobs with `lead_group_id`. When adding leads: do **not** add usernames that are in `cold_dm_sent_messages`, in `conversations` (by `participant_username`), or in `cold_dm_scrape_blocklist` for that client (filter during scrape to avoid dead leads).
7. **Sender:** Single always-on worker; multi-tenant loop (get next work any client with pause=0); schedule window; per-campaign or global limits and delays; record `campaign_id` and `message_group_id` on sent messages; on campaign completion set status and pause client.
8. **Cold outreach + GHL:** After each send, call **cold-dm-on-send** (§2a) with `client_id`, `instagram_thread_id`, `username`, `message_text` so the dashboard creates the conversation with tag `cold-outreach`. In GHL, add **Contact Created** as a trigger to the same ghl-inbound-webhook URL (§2b) so the new contact is linked to that conversation by name + time. When the lead replies, the cold-outreach tag is removed and the conversation is normal with GHL already linked.

Use this handoff as the single reference for the Cold DM VPS implementation.
