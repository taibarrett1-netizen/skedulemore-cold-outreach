# Admin Cold Outreach Lab (VPS)

Isolated experimental endpoints under `POST/GET /api/admin-lab/*` (see `admin_lab/http.js`). They require the normal Cold DM API key **plus** header `X-Admin-Lab-Secret` matching `ADMIN_LAB_SECRET` on the server. The same secret must be set in Supabase Edge (`ADMIN_LAB_SECRET`) for `cold-dm-vps-proxy` so the dashboard can call the lab.

## Decodo proxy

- Format: `http://USER:PASS@gate.decodo.com:10001` (set as `ADMIN_LAB_DECODO_PROXY_URL` on the VPS only).
- Verify egress: `curl -x "$ADMIN_LAB_DECODO_PROXY_URL" https://ip.decodo.com/json`
- Use one sticky proxy URL per connect session or per scrape run; trial bandwidth is small (~100MB)—keep GraphQL `first` modest and avoid redundant requests.

## Instagram GraphQL `doc_id`

Public follower pagination depends on a persisted query id. When the Python scraper errors with “Could not find follower edges” or GraphQL errors, open Instagram in a browser, DevTools → Network, filter `graphql/query`, trigger a followers load, and copy the `doc_id` for the followers request into `ADMIN_LAB_IG_DOC_ID_FOLLOWERS`. Expect to refresh this periodically as Instagram changes web clients.

## Python

Install once on the VPS: `pip install -r admin_lab/requirements-lab.txt` (see `requirements-lab.txt`).
