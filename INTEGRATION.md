# Cold DM Module – Integration Guide (Setter + Lovable + Supabase)

This document is for integrating **Cold DM / Cold Outreach** into your **AI setter app** (Lovable). The Cold Outreach UI lives as a **tab** in the same webpage as the rest of your setter. All Cold DM data lives in your **existing Supabase project** (same as setter settings and conversations). The **VPS** only runs the bot process and stays **in sync** with Lovable via Supabase. **We do not store Instagram passwords**—only a session (cookies) after a one-time connect.

**Audience:** Developers or an AI model integrating this module into the setter (Lovable).

---

## 1. What This Module Does

- **Instagram cold DMs:** Sends DMs to a list of leads using configurable message templates, with random delays and daily/hourly limits.
- **Runs 24/7 on a VPS** (Puppeteer/Chromium). Your setter app does **not** run the browser; it only shows the UI and stores data in Supabase.
- **Single source of truth:** Your **Supabase project** (the same one used for setter settings and conversations) stores all Cold DM data: message templates, leads, settings (limits, delays), Instagram **session only** (no password), sent log, and stats. Lovable and the VPS both read/write this same database so they stay in sync.

---

## 2. Architecture: What Runs Where

| Component | Where | Notes |
|-----------|--------|--------|
| **Cold Outreach UI** | Lovable (same app as setter) | A **tab** (e.g. "Cold Outreach") in your setter UI. Same webpage, same domain. No separate Cold DM app. |
| **Cold DM data** | **Supabase** (same project as setter) | Message templates, leads, settings, Instagram session (cookies only), sent_messages, daily_stats, control (pause). |
| **Bot process** | VPS only | Puppeteer/Chromium, PM2. Reads config and leads from **Supabase**, writes sent log and stats to **Supabase**. Does **not** use SQLite or CSV in production. |
| **VPS API** | VPS only | Minimal: **start**, **stop**, **status** (is bot process running?), and **connect** (one-time: receive password, log in, save session to Supabase, discard password). |
| **Instagram password** | **Never stored** | User enters it once in the Cold Outreach tab → sent to your backend → backend calls VPS `/api/instagram/connect` → VPS logs in, saves **session (cookies)** to Supabase, returns. Password is never written to Supabase or to disk. You can truthfully say: **"We do not store your Instagram password."** |

**No local/standalone mode:** There is no separate “Cold DM dashboard” to host; everything is under your setter UI in a Cold Outreach tab.

---

## 3. Keeping Lovable and VPS in Sync

- **Lovable:** Users edit message templates, leads, and settings in the Cold Outreach tab. Your backend writes these to **Supabase** (same project as setter).
- **VPS bot:** On start, it reads message templates, leads, settings, and Instagram session from **Supabase**. It writes each sent DM and daily stats to **Supabase**. It reads the “pause” control from **Supabase**.
- **Result:** Both sides use the same Supabase project. No need to “sync” via the VPS API for data—only for **start/stop** and **status** (and one-time **connect**). The UI always shows up-to-date data by reading from Supabase (e.g. via your Lovable backend or direct Supabase client).

---

## 4. Instagram: Session Only, Password Never Stored

**Goal:** You never store the user’s Instagram password, and you can say “We do not store your Instagram password.”

**Flow:**

1. In the Cold Outreach tab, user clicks **“Connect Instagram”** (or “Reconnect” if session expired).
2. User enters **Instagram username** and **password** in a form (only when connecting).
3. Your **Lovable backend** sends these to the VPS once: e.g. `POST /api/instagram/connect` with `{ "username": "...", "password": "..." }` over HTTPS (with API key).
4. **VPS** receives the request, runs Puppeteer to log into Instagram, then:
   - Saves the **session** (cookies, or serialized browser profile) to **Supabase** (e.g. table `cold_dm_instagram_sessions` or a row in settings), associated with the user/app.
   - **Does not** store the password anywhere (not in Supabase, not in .env, not in a file). Returns success/failure.
5. From then on, the **bot** loads the session from Supabase and uses it to send DMs. When the session expires (e.g. Instagram logs them out), the user clicks “Reconnect” and enters the password again (same one-time flow).

**Implementation note:** The Cold DM repo (VPS) must be adapted to: (a) accept the one-time connect endpoint, (b) persist only session data to Supabase, and (c) read session from Supabase when running the bot. The current repo uses .env for credentials; for this integration that is replaced by “session in Supabase, password only in memory during connect.”

### 4a. Residential proxy, sticky IP, and improving password-only (no 2FA) success

- **Why VPS “just worked” but residential is harder:** A **stable datacenter IP** you already used can already be “familiar” to Meta. **Residential** is better for **one IP per account**, but **rotating** exits look like a new network every request — that raises risk. **2FA** is a strong trust signal; not every Instagram account has it enabled.
- **Sticky session + UK by default:** For Decodo residential, a **sticky** session keeps the **same egress IP** for N minutes (default **60**, minimum **30** for auto-provision; see [Decodo sticky sessions](https://help.decodo.com/docs/residential-proxy-custom-sticky-sessions)). The gate username gets `-country-gb` **by default** (UK exit), optional `-city-london` via `DECODO_GATE_CITY`, then `-session-{stableId}-sessionduration-{minutes}` — **one stable session id per (client, IG handle)**. **Stale rows** in `cold_dm_proxy_assignments` (saved before country/sticky existed) are **auto-refreshed** on the next Connect so you do not keep a random-world IP. Override country with `DECODO_GATE_COUNTRY` or `none`. The gate username uses Decodo’s **`user-` prefix** by default.
- **Browser warmup (do this):** On a normal machine, configure **Chrome or Firefox** to use the **same HTTP proxy URL** the bot will use for that Instagram account (copy `proxy_url` from `cold_dm_instagram_sessions` or your `cold_dm_proxy_assignments` row after provision). Open **https://www.instagram.com**, sign in, and complete any **suspicious activity / checkpoint** UI. That ties **human + that IP** once; then run **Connect** on the VPS (same sticky assignment) so Puppeteer reuses a consistent path. Optional: use a privacy/incognito window with only that proxy profile so you do not leak your home IP in the same session.
- **Tuning:** Raise `DECODO_STICKY_SESSION_DURATION_MINUTES` (up to 1440) if you need longer sticky IP hold. Default geo is already **UK (`gb`)**; change if your accounts are not UK-based. If Meta still blocks automation-only logins, **2FA at connect time** remains the most reliable fallback.

---

## 5. Supabase Schema (Same Project as Setter)

Create these in your **existing** Supabase project. Use a prefix like `cold_dm_` if you want to keep Cold DM tables grouped.

| Table | Purpose |
|-------|--------|
| **cold_dm_message_templates** | Outreach message templates (one per row or a single row with JSON array, depending on your preference). |
| **cold_dm_leads** | Usernames to DM (e.g. `username` text, optional `added_at`, `source`). |
| **cold_dm_settings** | One row: `daily_send_limit`, `min_delay_minutes`, `max_delay_minutes`, `max_sends_per_hour`, `instagram_username` (display only). No password column. |
| **cold_dm_instagram_sessions** | Session data only: e.g. `id`, `session_data` (cookies or encrypted blob), `instagram_username`, `updated_at`. Bot reads this to stay logged in. |
| **cold_dm_sent_messages** | Log of sent DMs: `username`, `message`, `sent_at`, `status` ('success' / 'failed'). |
| **cold_dm_daily_stats** | `date`, `total_sent`, `total_failed` (for today’s stats). |
| **cold_dm_control** | Key-value: e.g. `key = 'pause'`, `value = '0'` or `'1'`. Bot checks this before each send. |

Your setter may already have a “user” or “tenant” concept; if so, add a `user_id` or `tenant_id` to these tables so multiple users/tenants can have separate Cold DM data. The VPS bot can be single-tenant (one Supabase project = one setter customer) or multi-tenant (bot reads which tenant is “active” from control or settings).

---

## 6. VPS API (Minimal – Start, Stop, Status, Connect)

The VPS exposes a **small API** so Lovable can control the bot and perform the one-time connect. All other data lives in Supabase; Lovable reads/writes Supabase directly (or via your backend).

**Base URL:** e.g. `https://colddm-api.yourdomain.com`  
**Auth:** Every request must include `Authorization: Bearer YOUR_API_KEY` or `X-API-Key: YOUR_API_KEY` (set `COLD_DM_API_KEY` on the VPS).

---

### 6.1 Status (is the bot process running?)

**GET** `/api/status`

**Response:**

```json
{
  "processRunning": true
}
```

Optional: you can also return `todaySent`, `todayFailed`, `leadsTotal`, `leadsRemaining` by reading from **Supabase** on the VPS and including them here, so the UI can show them without querying Supabase from Lovable. Or the UI can read those from Supabase itself.

---

### 6.2 Start bot

**POST** `/api/control/start`

Sets `cold_dm_control.pause` to `0` in Supabase (if using Supabase for control) and runs `pm2 start cli.js --name ig-dm-bot -- --start` (or equivalent).

**Response:** `{ "ok": true, "processRunning": true }`

---

### 6.3 Stop bot

**POST** `/api/control/stop`

Sets `cold_dm_control.pause` to `1` in Supabase and runs `pm2 stop ig-dm-bot`.

**Response:** `{ "ok": true, "processRunning": false }`

---

### 6.4 Connect Instagram (one-time; password never stored)

**POST** `/api/instagram/connect`  
**Body:**

```json
{
  "username": "instagram_username",
  "password": "instagram_password"
}
```

**Behavior:** VPS runs Puppeteer, logs into Instagram with the provided credentials, then saves **only the session** (cookies or profile) to Supabase (e.g. `cold_dm_instagram_sessions`). Does **not** persist the password. Returns success or error.

**Response:** `{ "ok": true }` or `{ "ok": false, "error": "..." }`

**Security:** This endpoint must be called over HTTPS and protected by the same API key. Only your Lovable backend should call it, and only when the user explicitly clicks “Connect” or “Reconnect.”

---

## 7. Lovable Integration Checklist

1. **Cold Outreach tab**
   - Add a tab (e.g. “Cold Outreach”) in the same setter app UI. All Cold DM UI lives here; no separate site or “local mode.”

2. **Supabase**
   - In your **existing** setter Supabase project, create the Cold DM tables (message templates, leads, settings, instagram_sessions, sent_messages, daily_stats, control). Use RLS so only the right user/tenant can read/write.

3. **Backend (Lovable)**
   - Store **VPS base URL** and **API key** in env (e.g. `COLD_DM_VPS_URL`, `COLD_DM_API_KEY`).
   - Implement server-side calls to the VPS **only** for: **status** (GET), **start** (POST), **stop** (POST), **connect** (POST with username + password in body). Do not put the API key or VPS URL in the frontend.
   - All other Cold DM data (templates, leads, settings, sent list, stats): read/write **Supabase** from your backend (or from the frontend via Supabase client with RLS). No need to proxy these through the VPS.

4. **Frontend (Cold Outreach tab)**
   - **Connect Instagram:** Form (username + password) → backend → VPS `POST /api/instagram/connect`. Show “Connected as @username” when session exists; “Reconnect” when session is missing or expired. Never show or store the password after connect.
   - **Settings:** Form for daily limit, min/max delay, max per hour. Save to **Supabase** (`cold_dm_settings`).
   - **Message templates:** List/edit templates. Save to **Supabase** (`cold_dm_message_templates`).
   - **Leads:** List/edit/paste/upload usernames. Save to **Supabase** (`cold_dm_leads`).
   - **Sent:** Table/list from **Supabase** (`cold_dm_sent_messages`).
   - **Dashboard:** Today sent/failed, leads total/remaining (from Supabase), Start / Stop buttons (via backend → VPS), Reset failed (e.g. delete failed rows in `cold_dm_sent_messages` and update `cold_dm_daily_stats` in Supabase).

5. **Security**
   - HTTPS for VPS and Lovable. API key for all VPS calls. Instagram password only sent once to VPS for connect; never stored. You can say: **“We do not store your Instagram password.”**

6. **No bot code in Lovable**
   - Lovable never runs Puppeteer or Chromium. It only talks to Supabase and to the VPS for start/stop/status/connect.

---

## 8. Current Cold DM Repo vs This Integration

The **current** Cold DM repo (this codebase) uses:

- **SQLite** for sent_messages, daily_stats, control  
- **.env** and **leads.csv** for credentials and leads  
- **config/messages.js** for templates  

For this integration, the **VPS side** must be **adapted** to:

- Read/write **Supabase** instead of SQLite and CSV (same project as setter).
- Implement **session-only** Instagram: add `POST /api/instagram/connect`, persist only session to Supabase, and have the bot load session from Supabase (no password in .env for production).
- Keep the **minimal VPS API**: status, start, stop, connect. All other data is in Supabase so Lovable and VPS stay in sync.

That adaptation can be done in this repo (Cold DMs V1) and then deployed to the same VPS. INTEGRATION.md describes the **target** architecture; the code changes to use Supabase and the connect endpoint are a separate step (migration in this repo).

---

## 9. Repo Structure (This Module)

```
Cold DMs V1/
├── INTEGRATION.md     ← This file (give to the setter/Lovable project)
├── DEPLOYMENT.md      ← VPS setup (Node, Chromium, PM2)
├── README.md
├── server.js          ← Add /api/instagram/connect; optional: read/write Supabase
├── bot.js             ← Puppeteer send flow; adapt to read session + config from Supabase
├── cli.js             ← PM2 entry
├── database/          ← Current: SQLite. Target: Supabase client + same schema in Supabase
├── config/
├── public/
└── ...
```

The setter/Lovable project needs this **INTEGRATION.md** and the **Supabase schema** (section 5). The VPS repo (this one) needs to be updated to use Supabase and session-only login as described in sections 4 and 8.

---

## 10. Summary

- **UI:** Cold Outreach is a **tab** in your setter app (same webpage). No local/standalone mode.
- **Data:** All Cold DM data (messages, leads, settings, session only, sent log, stats, control) lives in your **existing Supabase project** so you can share data with the setter (e.g. leads, conversations) and stay in sync.
- **Password:** **Never stored.** One-time connect sends password to VPS; VPS saves only session to Supabase. You can say: **“We do not store your Instagram password.”**
- **Lovable ↔ VPS in sync:** Both use Supabase. Lovable calls the VPS only for **start**, **stop**, **status**, and **connect**.

---

## 11. Handoff for the Cold DM Repo (Setter Cursor → Cold DM Cursor)

**If you are the setter/Lovable Cursor:** After you have created the Supabase schema and integrated the Cold Outreach tab, you **must** produce a **handoff document** that the product owner can copy and paste into the **Cold DM Cursor project** (the VPS repo). That document tells the Cold DM repo exactly how to adapt its code to use Supabase and the connect flow.

**The handoff document should include:**

1. **Supabase connection**
   - Which env vars the VPS should use (e.g. `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_ANON_KEY`), and whether the Cold DM bot should use the service role (to bypass RLS) or a dedicated key.

2. **Exact table names and column names**
   - For every Cold DM table you created (e.g. `cold_dm_message_templates`, `cold_dm_leads`, `cold_dm_settings`, `cold_dm_instagram_sessions`, `cold_dm_sent_messages`, `cold_dm_daily_stats`, `cold_dm_control`), list the **exact** table name and each column (name, type, nullable). If you use a `user_id` or `tenant_id` for multi-tenancy, say so and how the VPS should know which row to use (e.g. single row per app, or env var `COLD_DM_USER_ID`).

3. **Session storage**
   - Which table and columns store the Instagram session (e.g. `cold_dm_instagram_sessions.session_data`, `instagram_username`, `updated_at`). In what format should the VPS persist the session? (e.g. JSON-serialized cookies array, or base64 blob of Puppeteer profile.) How does the bot read it back (e.g. single row, or by `user_id`)?

4. **Connect endpoint**
   - Confirm: `POST /api/instagram/connect` body is `{ "username", "password" }`. After Puppeteer logs in, the VPS must write the session to the table/columns you specified above and must **not** store the password anywhere. Any extra steps (e.g. set `instagram_username` in `cold_dm_settings`)?

5. **Control and status**
   - Where is the “pause” flag stored (table + key/column)? How should the VPS return `todaySent`, `todayFailed`, `leadsTotal`, `leadsRemaining` for `GET /api/status`—by reading from Supabase, or leave that to Lovable?

6. **Migrations or SQL**
   - If you have run any migrations or SQL to create the Cold DM tables, paste the relevant SQL (or a link to it) so the Cold DM Cursor can align with your schema if needed.

**Output the handoff as a single markdown or text block** that the product owner can paste into the Cold DM Cursor and say: “Use this to implement the Supabase changes and the connect endpoint in the Cold DM repo.”
