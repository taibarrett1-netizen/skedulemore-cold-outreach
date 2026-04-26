# Production Cold DM Rollout Runbook

Use this when promoting the tested multi-tenant Cold DM flow from staging to production.

Repos involved:

- `skedulemore`: app, Supabase migrations, Edge functions
- `skedulemore-cold-outreach`: VPS dashboard and workers

## What Staging Proved

- Per-client PM2 workers start correctly: `ig-dm-send-<clientId>` and `ig-dm-scrape-<clientId>`.
- Queued scraping works when `COLD_DM_MAX_CONCURRENT_SCRAPES_PER_VPS=1`.
- Concurrent scraping works when `COLD_DM_MAX_CONCURRENT_SCRAPES_PER_VPS=2`.
- Concurrent sending works across two clients without cross-client claims.
- Different Instagram sessions use their own stored `proxy_url` / `proxy_assignment_id`.
- CPU was low in the 50-lead scrape test; run a separate high-volume test before raising scrape concurrency further.

## Code Fixes Included

- Failed sends no longer duplicate in the campaign sent table.
- Per-client scraper mode no longer recreates the legacy shared `ig-dm-scrape`.
- `updateWorker` / Admin "Update VPS" now restarts per-client `ig-dm-send-*` and `ig-dm-scrape-*` workers before restarting `ig-dm-dashboard`.
- Stripe checkout and checkout-finalize Edge functions avoid the Stripe SDK Deno crash path.
- VPS assignment uses the atomic `service_assign_cold_dm_client_to_least_loaded_vps` RPC.

## Pre-Merge Checks

Run these locally before merging:

```bash
cd "/Users/taibarrett/Documents/SkeduleMore/Skedulemore All/skedulemore-cold-outreach"
node --check server.js
```

```bash
cd "/Users/taibarrett/Documents/SkeduleMore/Skedulemore All/skedulemore"
npx eslint src/components/admin/ColdOutreachTab.tsx
deno check supabase/functions/finalize-checkout-session/index.ts
deno check supabase/functions/create-stripe-checkout/index.ts
deno check supabase/functions/cold-dm-vps-proxy/index.ts
```

`ColdOutreachTab.tsx` may still report existing hook warnings; there should be no lint errors.

## Merge

```bash
cd "/Users/taibarrett/Documents/SkeduleMore/Skedulemore All/skedulemore"
git status
git add src/components/admin/ColdOutreachTab.tsx \
  supabase/functions/_shared/pending_checkout_provision.ts \
  supabase/functions/_shared/subscription_access.ts \
  supabase/functions/cold-dm-vps-proxy/index.ts \
  supabase/functions/create-stripe-checkout/index.ts
git commit -m "Stabilize cold outreach checkout and multi-tenant workers"
git push origin main
```

```bash
cd "/Users/taibarrett/Documents/SkeduleMore/Skedulemore All/skedulemore-cold-outreach"
git status
git add server.js docs/PROD_COLD_DM_ROLLOUT.md
git commit -m "Update cold outreach production rollout flow"
git push origin main
```

## Production Supabase DB

Apply production migrations first. Then verify the required RPC and columns exist:

```sql
select proname
from pg_proc
where proname = 'service_assign_cold_dm_client_to_least_loaded_vps';

select column_name
from information_schema.columns
where table_schema = 'public'
  and table_name = 'cold_dm_client_workers'
  and column_name = 'vps_fleet_id';
```

## Fleet Rows

`cold_dm_vps_fleet` will not fill itself just because code is merged. Add or confirm one active fleet row for each production VPS.

Replace the placeholders before running:

```sql
with input(ip, base_url, droplet_id) as (
  values
    ('PROD_VPS_1_IP', 'https://PROD_VPS_1_BASE_URL', null::bigint),
    ('PROD_VPS_2_IP', 'https://PROD_VPS_2_BASE_URL', null::bigint)
)
update cold_dm_vps_fleet f
set base_url = i.base_url,
    droplet_id = coalesce(i.droplet_id, f.droplet_id),
    status = 'active'
from input i
where f.ip = i.ip;

with input(ip, base_url, droplet_id) as (
  values
    ('PROD_VPS_1_IP', 'https://PROD_VPS_1_BASE_URL', null::bigint),
    ('PROD_VPS_2_IP', 'https://PROD_VPS_2_BASE_URL', null::bigint)
)
insert into cold_dm_vps_fleet (ip, base_url, droplet_id, status)
select i.ip, i.base_url, i.droplet_id, 'active'
from input i
where not exists (
  select 1 from cold_dm_vps_fleet f where f.ip = i.ip
);
```

Verify distribution:

```sql
select
  f.id as fleet_id,
  f.ip,
  f.base_url,
  f.status,
  count(cw.client_id) as assigned_clients
from cold_dm_vps_fleet f
left join cold_dm_client_workers cw on cw.vps_fleet_id = f.id
where f.status = 'active'
group by f.id, f.ip, f.base_url, f.status
order by assigned_clients asc, f.created_at asc;
```

## Existing Client Worker Rows

Important: canonical client routing must use `users.id`. Some older rows may have `cold_dm_client_workers.client_id = users.user_id`; those are legacy auth-user-id rows and should not be the main routing rows.

Inspect current assignment state and identify canonical vs legacy rows:

```sql
select
  u.id as canonical_client_id,
  u.user_id as auth_user_id,
  u.name,
  u.primary_email,
  cw.client_id as worker_client_id,
  case
    when cw.client_id = u.id then 'canonical'
    when cw.client_id = u.user_id then 'legacy_auth_user_id'
    when cw.client_id is null then 'missing_worker_row'
    else 'other_mismatch'
  end as worker_id_type,
  cw.vps_fleet_id,
  cw.droplet_ipv4,
  cw.base_url,
  cw.status,
  cw.last_error
from users u
left join cold_dm_client_workers cw
  on cw.client_id = u.id
  or cw.client_id = u.user_id
where u.service_category = 'cold_outreach_only'
   or u.plan_tier in ('cold_outreach', 'both')
   or exists (
     select 1
     from cold_dm_instagram_sessions s
     where s.client_id = u.id or s.client_id = u.user_id
   )
order by u.created_at desc;
```

For a single existing VPS where all current Cold Outreach users should live on that box, update existing canonical rows first:

```sql
update cold_dm_client_workers cw
set
  vps_fleet_id = '<FLEET_ID>'::uuid,
  provider = 'digitalocean',
  droplet_id = null,
  droplet_ipv4 = '<VPS_PUBLIC_IP>',
  base_url = 'http://<VPS_PUBLIC_IP>:3000',
  status = 'ready',
  last_error = null,
  updated_at = now()
from users u
where cw.client_id = u.id
  and (
    u.service_category = 'cold_outreach_only'
    or u.plan_tier in ('cold_outreach', 'both')
    or exists (
      select 1
      from cold_dm_instagram_sessions s
      where s.client_id = u.id or s.client_id = u.user_id
    )
  );
```

Then insert missing canonical rows. This avoids `ON CONFLICT`, because older DBs may not have a unique constraint on `cold_dm_client_workers.client_id`.

```sql
insert into cold_dm_client_workers (
  client_id,
  vps_fleet_id,
  provider,
  droplet_id,
  droplet_ipv4,
  base_url,
  status,
  last_error,
  updated_at
)
select
  u.id,
  '<FLEET_ID>'::uuid,
  'digitalocean',
  null,
  '<VPS_PUBLIC_IP>',
  'http://<VPS_PUBLIC_IP>:3000',
  'ready',
  null,
  now()
from users u
where (
    u.service_category = 'cold_outreach_only'
    or u.plan_tier in ('cold_outreach', 'both')
    or exists (
      select 1
      from cold_dm_instagram_sessions s
      where s.client_id = u.id or s.client_id = u.user_id
    )
  )
  and not exists (
    select 1
    from cold_dm_client_workers cw
    where cw.client_id = u.id
  );
```

For multiple existing VPSs, backfill by current IP/base URL instead of assigning everything to one box:

```sql
update cold_dm_client_workers cw
set
  vps_fleet_id = f.id,
  droplet_ipv4 = coalesce(cw.droplet_ipv4, f.ip),
  base_url = regexp_replace(coalesce(cw.base_url, f.base_url), '/+$', ''),
  status = case when cw.status in ('ready', 'provisioning') then 'ready' else cw.status end,
  last_error = case when cw.status in ('ready', 'provisioning') then null else cw.last_error end,
  updated_at = now()
from cold_dm_vps_fleet f
where cw.vps_fleet_id is null
  and (
    cw.droplet_ipv4 = f.ip
    or regexp_replace(cw.base_url, '/+$', '') = regexp_replace(f.base_url, '/+$', '')
  );
```

Verify canonical worker rows are ready:

```sql
select
  u.id as canonical_client_id,
  u.name,
  u.primary_email,
  cw.client_id as worker_client_id,
  cw.vps_fleet_id,
  cw.droplet_ipv4,
  cw.base_url,
  cw.status,
  cw.last_error
from users u
left join cold_dm_client_workers cw on cw.client_id = u.id
where u.service_category = 'cold_outreach_only'
   or u.plan_tier in ('cold_outreach', 'both')
   or exists (
     select 1
     from cold_dm_instagram_sessions s
     where s.client_id = u.id or s.client_id = u.user_id
   )
order by u.created_at desc;
```

Verify no routed canonical clients are left without a fleet assignment:

```sql
select
  u.id as canonical_client_id,
  u.name,
  u.primary_email,
  cw.client_id as worker_client_id,
  cw.vps_fleet_id,
  cw.droplet_ipv4,
  cw.base_url,
  cw.status,
  cw.last_error
from users u
left join cold_dm_client_workers cw on cw.client_id = u.id
where (
    u.service_category = 'cold_outreach_only'
    or u.plan_tier in ('cold_outreach', 'both')
    or exists (
      select 1
      from cold_dm_instagram_sessions s
      where s.client_id = u.id or s.client_id = u.user_id
    )
  )
  and (cw.client_id is null or cw.vps_fleet_id is null or cw.status <> 'ready');
```

After canonical rows are verified and live traffic works, inspect legacy auth-user-id worker rows. Delete only rows where `client_id = users.user_id` and a canonical `client_id = users.id` row already exists:

```sql
select
  legacy.client_id as legacy_worker_client_id,
  u.id as canonical_client_id,
  u.user_id as auth_user_id,
  u.name,
  u.primary_email,
  legacy.status,
  legacy.base_url
from users u
join cold_dm_client_workers legacy on legacy.client_id = u.user_id
where exists (
  select 1
  from cold_dm_client_workers canonical
  where canonical.client_id = u.id
);
```

## Edge Deploy

Set production Edge secrets first:

- `COLD_DM_API_KEY`
- `COLD_DM_MAX_CLIENTS_PER_VPS`
- `DIGITALOCEAN_API_TOKEN` and DigitalOcean image/region/size/SSH key envs if production should auto-create pool droplets
- `DECODO_SHARED_USERNAME` / `DECODO_SHARED_PASSWORD`
- Stripe secrets
- Production Supabase URL/service role values

Deploy:

```bash
cd "/Users/taibarrett/Documents/SkeduleMore/Skedulemore All/skedulemore"
supabase functions deploy cold-dm-vps-proxy --project-ref PROD_REF
supabase functions deploy create-stripe-checkout --project-ref PROD_REF
supabase functions deploy finalize-checkout-session --project-ref PROD_REF
```

## VPS Rollout

Run this on every production VPS:

```bash
cd /root/cold-outreach
git fetch origin
git checkout main
git pull origin main
npm install
node --check server.js
```

Set or confirm `.env`:

```bash
COLD_DM_VPS_IP=<this VPS public IP>
COLD_DM_API_KEY=<same key Edge uses>
COLD_DM_PER_CLIENT_PM2_WORKERS=1
COLD_DM_MAX_CONCURRENT_SCRAPES_PER_VPS=2
COLD_DM_AUTO_ENSURE_CLIENT_WORKERS_ON_DASHBOARD_START=0
ASSIGN_CLIENT_SYNC_TIMEOUT_MS=15000
DASHBOARD_AUDIT_CONSOLE=0
SEND_WORKER_VERBOSE_LOGS=0
LOG_LEVEL=info
```

Also confirm Supabase and Decodo envs point to production.

Restart cleanly:

```bash
pm2 restart ig-dm-dashboard --update-env
pm2 delete ig-dm-send || true
pm2 delete ig-dm-scrape || true
pm2 save
```

For existing clients assigned to that VPS, start or confirm per-client stacks:

```bash
CLIENT_ID='<client uuid>'
COLD_DM_CLIENT_ID="$CLIENT_ID" pm2 start workers/send-worker.js --name "ig-dm-send-${CLIENT_ID}" --cwd "$PWD" --update-env
COLD_DM_CLIENT_ID="$CLIENT_ID" pm2 start workers/scrape-worker.js --name "ig-dm-scrape-${CLIENT_ID}" --cwd "$PWD" --update-env
pm2 save
```

## Admin Update VPS Button

The admin UI button calls:

```text
ClientManagementTab -> cold-dm-vps-proxy action=updateWorker -> /api/admin/update on the assigned VPS
```

Expected response from the button is quick success because `/api/admin/update` accepts the update and runs the pull/restart in the background.

After clicking it, SSH into the VPS and check:

```bash
pm2 logs ig-dm-dashboard --lines 100
ls -t /tmp/cold-dm-update-*.log | head -1
LATEST="$(ls -t /tmp/cold-dm-update-*.log | head -1)"
sed -n '1,160p' "$LATEST"
pm2 status
```

Expected:

- Log contains `[admin:update] start`.
- `git pull origin <branch>` succeeds.
- `npm install` succeeds.
- Existing per-client `ig-dm-send-*` and `ig-dm-scrape-*` workers restart.
- `ig-dm-dashboard` restarts last.
- No shared `ig-dm-send` or shared `ig-dm-scrape` remains.

## Production Smoke

After rollout:

```bash
pm2 status
```

Expected process shape:

- `ig-dm-dashboard`
- `ig-dm-send-<clientId>` per assigned client
- `ig-dm-scrape-<clientId>` per assigned client
- no shared `ig-dm-send`
- no shared `ig-dm-scrape`

Then test:

1. Existing client loads Cold Outreach.
2. Existing client can start/stop sending without dashboard restarts.
3. One scrape starts and finishes.
4. A second assigned client can run a tiny scrape/send without cross-client logs.
5. New signup creates/reuses `cold_dm_client_workers` and lands on the least-loaded active VPS.

## Rollback

Before pulling main on each VPS:

```bash
git rev-parse HEAD
pm2 list
```

If VPS code needs rollback:

```bash
git checkout <previous_sha>
npm install
pm2 restart ig-dm-dashboard --update-env
pm2 restart 'ig-dm-send-*' --update-env || true
pm2 restart 'ig-dm-scrape-*' --update-env || true
pm2 save
```

If Edge routing breaks, redeploy the previous known-good `cold-dm-vps-proxy` and keep existing `cold_dm_client_workers.base_url` rows pointed at the known-good VPS.
