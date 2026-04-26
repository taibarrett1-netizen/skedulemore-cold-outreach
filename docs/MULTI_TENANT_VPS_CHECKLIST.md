# Multi-tenant cold DM VPS — test & deploy checklist

Use this on **staging first**, then repeat on **production** after merge to `main`. Check boxes as you go.

**Repos involved:** `skedulemore-cold-outreach` (VPS), `skedulemore` (app + Supabase migrations + Edge `cold-dm-vps-proxy`).

**Naming:** Per-client PM2 apps are typically `ig-dm-send-<clientId>` and `ig-dm-scrape-<clientId>` (full UUID in the name is fine). Dashboard stays `ig-dm-dashboard`.

---

## Environment & VPS baseline

- [ ] VPS has Node LTS aligned with repo (e.g. 20.x), `npm`, and PM2 (`pm2 list` works).
- [ ] App lives in a fixed directory (e.g. `~/cold-outreach`), **not** a moving copy.
- [ ] `git status` clean; branch matches what you intend to run (staging / `main` / release tag).
- [ ] `npm ci` or `npm install` completed in that directory (`node_modules` present, including `pm2` dependency for programmatic spawn if used).
- [ ] `.env` exists at repo root (same folder as `server.js`). Compared against `.env.example` for new keys.
- [ ] `COLD_DM_VPS_IP` is set to this droplet’s **public** IPv4 (used for DB-scoped “clients on this VPS” behavior).
- [ ] Supabase URL + service role / keys in `.env` match the **same** project (staging vs prod — no cross-wiring).
- [ ] `pm2 start ecosystem.config.cjs` (or your standard start) brings up **`ig-dm-dashboard` only** if using the slim ecosystem; workers are started per client (below).
- [ ] `pm2 save` and `pm2 startup` configured if this host should survive reboot.

---

## Manual per-client workers (reliable default)

From repo root (`cd ~/cold-outreach` or equivalent), with `.env` loaded via worker `dotenv` (**`--cwd` must be repo root**):

```bash
CLIENT_ID='<paste-cold-dm-client-uuid>'

COLD_DM_CLIENT_ID="$CLIENT_ID" pm2 start workers/send-worker.js \
  --name "ig-dm-send-${CLIENT_ID}" --cwd "$PWD"

COLD_DM_CLIENT_ID="$CLIENT_ID" pm2 start workers/scrape-worker.js \
  --name "ig-dm-scrape-${CLIENT_ID}" --cwd "$PWD"

pm2 save
```

- [ ] Client A: send + scrape processes **online** (`pm2 list`).
- [ ] Logs show client prefix / name so you can tell A vs B apart (`pm2 logs <name>`).

---

## Phase 1 — One client: Instagram, scrape, send

- [ ] **Attach Instagram** for client A via your normal product flow (SkeduleMore → cold DM / connect). No secrets pasted into tickets or chat.
- [ ] Supabase: session row exists for client A; cookies / session payload present where your schema expects them.
- [ ] Worker logs: no permanent “no session” / “reconnect” loop after a successful connect.
- [ ] **Scrape:** trigger a small job you can verify (queue row, lead field, or log line).
- [ ] **Send:** one test send to an account you control; confirm delivery.
- [ ] Optional: one **follow-up** or scheduled path you rely on in prod; confirm VPS fallback or cron path if applicable.

---

## Phase 2 — Second client on the same VPS

- [ ] Client B exists in SkeduleMore / Supabase (distinct `COLD_DM_CLIENT_ID`).
- [ ] DB: client B is assigned to **this** VPS IP / fleet row (same model as client A — no duplicate “one client per box” conflicts).
- [ ] Start **second** pair of PM2 apps with **B’s** `COLD_DM_CLIENT_ID` and **different** process names (same commands as above, `CLIENT_ID` = B).
- [ ] `pm2 list` shows dashboard + **two** sends + **two** scrapes (or your intended counts).
- [ ] Attach IG for client B; confirm sessions are **scoped to B** in DB.
- [ ] Smoke: A still scrape + send; B scrape + send. Logs show correct prefixes and no mixed clientId in errors.

---

## Phase 3 — Concurrency (same box)

- [ ] Overlap: both clients **scraping** at once (watch CPU/RAM).
- [ ] Overlap: both **sending** (or send + scrape on both). Watch for OOM / PM2 restart storms.
- [ ] If unstable: note peak RAM; plan **larger droplet** or lower concurrency envs (`COLD_DM_MAX_CONCURRENT_SENDERS`, campaign limits, etc.) before adding many clients.

---

## Phase 4 — API assign path (optional, after manual proof)

When you want Edge / `POST /api/admin/assign-client` to spawn workers without SSH:

- [ ] Same code revision on VPS as branch where `assign-client` + `ensureClientWorkerStack` were fixed.
- [ ] `POST /api/admin/assign-client` with Bearer `COLD_DM_API_KEY` returns `{ ok: true, clientId, accepted: true }`.
- [ ] Within ~30s, `pm2 list` shows new `ig-dm-send-*` / `ig-dm-scrape-*` **or** dashboard error log explains failure (spawn bug, missing `logs/`, PM2 API, etc.).
- [ ] `GET /api/admin/clients` lists expected client IDs when implemented.

---

## Deploy to production (`main`) — order matters

### 1. Database (Supabase production)

- [ ] Migrations merged to `main` include fleet / multi-tenant changes you depend on (`cold_dm_vps_fleet`, `vps_fleet_id`, etc.).
- [ ] Migrations applied to **production** project (no drift vs `main`).
- [ ] Post-migration sanity: fleet rows, client worker rows, RLS/policies if any were touched.

### 2. Edge

- [ ] Deploy `cold-dm-vps-proxy` (and any shared function updates) to **production** Supabase.
- [ ] Secrets / env for Edge: prod Supabase URL, service role if needed, VPS base URLs, **rotated** API keys if old ones leaked.

### 3. Skedulemore application

- [ ] Frontend + API on `main` deployed to prod; points at **prod** Supabase and **prod** Edge URLs.

### 4. Each production VPS

- [ ] `git fetch && git checkout main && git pull` (or deploy tag).
- [ ] `npm ci` or `npm install`.
- [ ] Merge `.env.example` → update `.env` (new keys, `COLD_DM_VPS_IP`, keys rotated).
- [ ] Restart dashboard: `pm2 reload ecosystem.config.cjs` or `pm2 restart ig-dm-dashboard`.
- [ ] Start or respawn **per-client** send/scrape processes (manual commands above or assign-client once trusted).
- [ ] `pm2 save`.

### 5. Security

- [ ] Rotate **`COLD_DM_API_KEY`** (and any other keys ever pasted in Slack/chat); update VPS `.env`, SkeduleMore secrets, and Edge secrets.
- [ ] Firewall: only necessary ports open (e.g. dashboard/API if not behind VPN; prefer SSH tunnel or allowlist if possible).

### 6. Production smoke (short)

- [ ] `GET /api/health` (or your health URL) OK with expected shape.
- [ ] One client: connect IG → scrape → send on prod.
- [ ] Second client on same VPS if multi-tenant prod is live.

---

## Rollback hints

- [ ] Know previous **git ref** on VPS before pull (`git rev-parse HEAD`).
- [ ] Know previous **Edge** deployment version if Supabase supports rollback.
- [ ] DB: avoid destructive migrations without backup; test migrations on staging clone first.

---

## Done criteria (staging)

- [ ] Two clients on one VPS: stable IG sessions, scrape + send for both, acceptable resource use under concurrent load.
- [ ] Deploy path to `main` documented for your team (this list + any org-specific URLs).

---

## Done criteria (production)

- [ ] Migrations + Edge + app + all VPS nodes updated without mixed prod/staging config.
- [ ] Keys rotated post-deploy if there was any exposure.
- [ ] Spot-check N clients (however many you initially onboard) over 24h for PM2 restarts and memory.
