import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Iterable, List

from instagrapi.exceptions import ClientError

logger = logging.getLogger("scraper_worker")

from .db import (
  get_connection,
  fetch_scraper_session,
  fetch_scrape_job,
  update_scrape_job,
  get_status_for_job,
  load_filter_sets,
  insert_lead_if_new,
)
from .instagram_client import build_client_from_session


def _float_env(name: str, default: float) -> float:
  try:
    v = os.getenv(name)
    return float(v) if v not in (None, "") else default
  except (TypeError, ValueError):
    return default


def _sleep_between_batches():
  lo = _float_env("SCRAPER_DELAY_BATCH_MIN", 15.0)
  hi = _float_env("SCRAPER_DELAY_BATCH_MAX", 35.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_before_first():
  lo = _float_env("SCRAPER_DELAY_BEFORE_FIRST_MIN", 10.0)
  hi = _float_env("SCRAPER_DELAY_BEFORE_FIRST_MAX", 25.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_between_calls():
  lo = _float_env("SCRAPER_DELAY_BETWEEN_CALLS_MIN", 12.0)
  hi = _float_env("SCRAPER_DELAY_BETWEEN_CALLS_MAX", 28.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_per_item():
  lo = _float_env("SCRAPER_DELAY_PER_ITEM_MIN", 0.8)
  hi = _float_env("SCRAPER_DELAY_PER_ITEM_MAX", 2.2)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_warmup():
  """Long delay before any API call to avoid burst-on-connect detection."""
  lo = _float_env("SCRAPER_WARMUP_MIN", 45.0)
  hi = _float_env("SCRAPER_WARMUP_MAX", 90.0)
  if hi < lo:
    hi = lo
  t = random.uniform(lo, hi)
  logger.info("Warm-up delay %.0fs before first request (SCRAPER_WARMUP_*)", t)
  time.sleep(t)


def _sleep_chunk_cooldown():
  """Long pause every N items to mimic human session breaks (avoids sustained automation)."""
  every = int(_float_env("SCRAPER_CHUNK_COOLDOWN_EVERY", 200.0))
  if every <= 0:
    return
  lo = _float_env("SCRAPER_CHUNK_COOLDOWN_MIN", 120.0)
  hi = _float_env("SCRAPER_CHUNK_COOLDOWN_MAX", 300.0)
  if hi < lo:
    hi = lo
  t = random.uniform(lo, hi)
  logger.info("Chunk cooldown %.0fs (every %s items)", t, every)
  time.sleep(t)


def _sleep_jitter():
  """Random extra delay occasionally to break predictable patterns."""
  lo = _float_env("SCRAPER_JITTER_EXTRA_MIN", 15.0)
  hi = _float_env("SCRAPER_JITTER_EXTRA_MAX", 45.0)
  if hi < lo:
    hi = lo
  t = random.uniform(lo, hi)
  logger.info("Jitter delay +%.0fs", t)
  time.sleep(t)


def _should_cancel(conn, job_id: str) -> bool:
  status = get_status_for_job(conn, job_id)
  return status and status != "running"


def scrape_followers(conn, job: dict):
  client_id = job["client_id"]
  target_username = (job.get("target_username") or "").strip().lstrip("@").lower()
  if not target_username:
    raise RuntimeError("target_username missing on scrape job.")

  logger.info("Follower scrape started: target=@%s job_id=%s", target_username, job.get("id"))

   # Respect cancellation immediately, before warm-up or any API calls.
  if _should_cancel(conn, job["id"]):
    logger.info("Job cancelled before start. Exiting without API calls.")
    update_scrape_job(conn, job["id"], status="cancelled", scraped_count=int(job.get("scraped_count") or 0))
    return

  _sleep_warmup()

  platform_session_id = job.get("platform_scraper_session_id")
  session_row = fetch_scraper_session(conn, client_id, platform_session_id)
  if not session_row or not (session_row.get("session_data") or {}).get("cookies"):
    raise RuntimeError("Scraper session not found or expired for this job.")

  logger.info("Session loaded (platform_session_id=%s), building instagrapi client", platform_session_id)
  cl = build_client_from_session(
    session_row["session_data"], session_row.get("instagram_username")
  )
  logger.info("Client ready, resolving user_id for @%s", target_username)

  lead_group_id = job.get("lead_group_id")
  source = f"followers:{target_username}"

  (
    in_conversations,
    sent_usernames,
    blocklist_usernames,
    existing_leads,
  ) = load_filter_sets(conn, client_id)

  scraped_new = int(job.get("scraped_count") or 0)

  max_leads = job.get("max_leads") or None
  if max_leads is not None:
    try:
      max_leads = int(max_leads)
    except (TypeError, ValueError):
      max_leads = None
  if max_leads is not None:
    logger.info(
      "Job has max_leads=%s (already scraped=%d). Will stop when new leads reach this count.",
      max_leads,
      scraped_new,
    )
  else:
    logger.info("Job has no max_leads; will scrape all available followers.")

  _sleep_before_first()
  try:
    user_id = cl.user_id_from_username(target_username)
  except ClientError as e:
    raise RuntimeError(f"Failed to resolve user_id for @{target_username}: {e}") from e

  _sleep_between_calls()

  # Fetch only a conservative slice of followers per job, not the whole account.
  # - For small jobs, use a small multiple of max_leads.
  # - For large jobs, cap the API load per run so we don't hammer Instagram.
  SAFE_FETCH_CAP = int(_float_env("SCRAPER_SAFE_FETCH_CAP", 800.0))
  if SAFE_FETCH_CAP <= 0:
    SAFE_FETCH_CAP = 800
  if max_leads is not None and max_leads > 0:
    # Up to 2x max_leads, but never more than SAFE_FETCH_CAP.
    amount = min(int(max_leads * 2), SAFE_FETCH_CAP)
  else:
    # No max_leads: still respect a cap so we don't pull everything.
    amount = SAFE_FETCH_CAP

  logger.info(
    "Requesting up to %d followers for @%s this job (SAFE_FETCH_CAP=%d, max_leads=%s)",
    amount,
    target_username,
    SAFE_FETCH_CAP,
    max_leads,
  )
  followers_dict = cl.user_followers(user_id, amount=amount)
  followers = list(followers_dict.values())
  logger.info("Fetched %d followers for @%s", len(followers), target_username)

  batch_new = 0
  for idx, user in enumerate(followers, start=1):
    username = (getattr(user, "username", None) or "").strip().lstrip("@").lower()
    if not username:
      continue

    if (
      username in existing_leads
      or username in in_conversations
      or username in sent_usernames
      or username in blocklist_usernames
      or username == target_username
    ):
      continue

    is_new = insert_lead_if_new(conn, client_id, username, source, lead_group_id)
    if is_new:
      existing_leads.add(username)
      scraped_new += 1
      batch_new += 1

    if max_leads is not None and scraped_new >= max_leads:
      logger.info("Reached max_leads=%s, completing job. Total new leads: %d", max_leads, scraped_new)
      update_scrape_job(conn, job["id"], status="completed", scraped_count=scraped_new)
      return

    _sleep_per_item()

    chunk_every = max(1, int(_float_env("SCRAPER_CHUNK_COOLDOWN_EVERY", 200.0)))
    jitter_every = max(1, int(_float_env("SCRAPER_JITTER_EVERY", 500.0)))
    if idx > 0 and idx % chunk_every == 0:
      if _should_cancel(conn, job["id"]):
        logger.info("Job cancelled during cooldown. Scraped %d new leads", scraped_new)
        update_scrape_job(conn, job["id"], status="cancelled", scraped_count=scraped_new)
        return
      _sleep_chunk_cooldown()
    if idx > 0 and idx % jitter_every == 0:
      _sleep_jitter()

    if idx % 50 == 0:
      update_scrape_job(conn, job["id"], scraped_count=scraped_new)
      logger.info("Progress: %d/%d followers processed, %d new leads so far", idx, len(followers), scraped_new)
      if _should_cancel(conn, job["id"]):
        logger.info("Job cancelled, stopping. Scraped %d new leads", scraped_new)
        update_scrape_job(conn, job["id"], status="cancelled", scraped_count=scraped_new)
        return
      _sleep_between_batches()

  logger.info("Follower scrape completed: %d new leads for @%s", scraped_new, target_username)
  update_scrape_job(conn, job["id"], status="completed", scraped_count=scraped_new)


def _sleep_graphql_page_delay():
  """
  Long delay between GraphQL page requests.
  Defaults are higher than instagrapi's internal delays to reduce rate-limit risk.
  """
  lo = _float_env("SCRAPER_GRAPHQL_PAGE_DELAY_MIN", 45.0)
  hi = _float_env("SCRAPER_GRAPHQL_PAGE_DELAY_MAX", 120.0)
  if hi < lo:
    hi = lo
  t = random.uniform(lo, hi)
  logger.info("GraphQL page delay %.0fs", t)
  time.sleep(t)


def scrape_followers_via_graphql(conn, job: dict):
  """
  Follower scrape using Instagram's web GraphQL endpoint (edge_followed_by)
  instead of instagrapi.user_followers().

  This is an additive fallback path. It keeps:
    - The same platform scraper rotation / session loading
    - The same lead filtering + upsert logic
    - Similar cancellation and progress updates

  IMPORTANT: Instagram rotates the followers query_hash / doc_id periodically.
  To update:
    1. Open a real browser, log into the same account used for scraping.
    2. Go to https://www.instagram.com/<username>/ and click "followers".
    3. In DevTools → Network, find the GraphQL request whose response contains "edge_followed_by".
    4. Copy its "query_hash" (or "doc_id") value and update SCRAPER_GRAPHQL_FOLLOWERS_QUERY_HASH
       in the environment, or hard-code it below if needed.
  """
  import requests

  client_id = job["client_id"]
  target_username = (job.get("target_username") or "").strip().lstrip("@").lower()
  if not target_username:
    raise RuntimeError("target_username missing on scrape job.")

  logger.info(
    "GraphQL follower scrape started: target=@%s job_id=%s",
    target_username,
    job.get("id"),
  )

  if _should_cancel(conn, job["id"]):
    logger.info("[GraphQL] Job cancelled before start. Exiting without API calls.")
    update_scrape_job(conn, job["id"], status="cancelled", scraped_count=int(job.get("scraped_count") or 0))
    return

  _sleep_warmup()

  platform_session_id = job.get("platform_scraper_session_id")
  session_row = fetch_scraper_session(conn, client_id, platform_session_id)
  if not session_row or not (session_row.get("session_data") or {}).get("cookies"):
    raise RuntimeError("Scraper session not found or expired for this job.")

  session_data = session_row["session_data"] or {}
  cookies = session_data.get("cookies") or []

  logger.info(
    "Session loaded (platform_session_id=%s), building instagrapi client for user id resolution",
    platform_session_id,
  )
  cl = build_client_from_session(session_data, session_row.get("instagram_username"))

  lead_group_id = job.get("lead_group_id")
  source = f"followers:{target_username}"

  (
    in_conversations,
    sent_usernames,
    blocklist_usernames,
    existing_leads,
  ) = load_filter_sets(conn, client_id)

  scraped_new = int(job.get("scraped_count") or 0)

  max_leads = job.get("max_leads") or None
  if max_leads is not None:
    try:
      max_leads = int(max_leads)
    except (TypeError, ValueError):
      max_leads = None
  if max_leads is not None:
    logger.info(
      "[GraphQL] Job has max_leads=%s (already scraped=%d). Will stop when new leads reach this count.",
      max_leads,
      scraped_new,
    )
  else:
    logger.info("[GraphQL] Job has no max_leads; will scrape all available followers.")

  _sleep_before_first()
  try:
    user_id = cl.user_id_from_username(target_username)
  except ClientError as e:
    raise RuntimeError(
      f"[GraphQL] Failed to resolve user_id for @{target_username}: {e}"
    ) from e

  # Build a requests session with the same cookies and proxy settings as the platform scraper.
  session = requests.Session()
  for c in cookies:
    name = c.get("name")
    value = c.get("value")
    if not name or value is None:
      continue
    domain = c.get("domain") or ".instagram.com"
    session.cookies.set(name, value, domain=domain)

  # Environment variable SCRAPER_GRAPHQL_PROXY overrides proxy for GraphQL mode; otherwise, rely on system/requests defaults.
  proxy_url = os.getenv("SCRAPER_GRAPHQL_PROXY") or None
  if proxy_url:
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    logger.info("[GraphQL] Using proxy %s", proxy_url)

  # Web headers to mimic a normal logged-in browser session.
  session.headers.update(
    {
      "User-Agent": os.getenv(
        "SCRAPER_GRAPHQL_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
      ),
      "Accept": "*/*",
      "Referer": f"https://www.instagram.com/{target_username}/",
      "X-Requested-With": "XMLHttpRequest",
    }
  )

  # Followers GraphQL query hash.
  # NOTE: Instagram may rotate this over time.
  # The default below is a known followers edge_followed_by hash as of early 2026.
  # You can override it via SCRAPER_GRAPHQL_FOLLOWERS_QUERY_HASH in the environment if it stops working.
  default_query_hash = "c76146de99bb02f6415203be841dd25a"
  query_hash = os.getenv("SCRAPER_GRAPHQL_FOLLOWERS_QUERY_HASH") or default_query_hash

  page_size_env = os.getenv("SCRAPER_GRAPHQL_PAGE_SIZE") or ""
  try:
    page_size = int(page_size_env) if page_size_env else 40
  except ValueError:
    page_size = 40
  if page_size <= 0:
    page_size = 40

  end_cursor = None
  no_new_pages = 0
  MAX_NO_NEW_PAGES = 5

  while True:
    if _should_cancel(conn, job["id"]):
      logger.info("[GraphQL] Job cancelled before page fetch. Scraped %d new leads", scraped_new)
      update_scrape_job(conn, job["id"], status="cancelled", scraped_count=scraped_new)
      return

    variables = {
      "id": str(user_id),
      "include_reel": True,
      "fetch_mutual": False,
      "first": page_size,
    }
    if end_cursor:
      variables["after"] = end_cursor

    params = {
      "query_hash": query_hash,
      "variables": json.dumps(variables, separators=(",", ":")),
    }

    logger.info(
      "[GraphQL] Fetching followers page (first=%d, after=%s)",
      page_size,
      end_cursor or "None",
    )
    try:
      resp = session.get(
        "https://www.instagram.com/graphql/query/",
        params=params,
        timeout=30,
      )
    except Exception as e:
      raise RuntimeError(f"[GraphQL] Request error: {e}") from e

    if resp.status_code == 429 or "Please wait a few minutes" in (resp.text or ""):
      raise RuntimeError(
        "[GraphQL] Rate limited (429 / 'Please wait a few minutes'). Back off this account."
      )
    if resp.status_code != 200:
      raise RuntimeError(
        f"[GraphQL] Unexpected status {resp.status_code}: {resp.text[:300]}"
      )

    try:
      data = resp.json()
    except ValueError as e:
      raise RuntimeError(f"[GraphQL] Failed to decode JSON: {e}") from e

    user_data = (
      (data.get("data") or {})
      .get("user")
      or {}
    )
    edge = user_data.get("edge_followed_by") or {}
    edges = edge.get("edges") or []
    page_info = edge.get("page_info") or {}
    has_next_page = bool(page_info.get("has_next_page"))
    end_cursor = page_info.get("end_cursor") or None

    logger.info("[GraphQL] Got %d edges (has_next_page=%s)", len(edges), has_next_page)

    batch_new = 0
    for node_wrapper in edges:
      node = node_wrapper.get("node") or {}
      username = (node.get("username") or "").strip().lstrip("@").lower()
      if not username:
        continue

      if (
        username in existing_leads
        or username in in_conversations
        or username in sent_usernames
        or username in blocklist_usernames
        or username == target_username
      ):
        continue

      is_new = insert_lead_if_new(conn, client_id, username, source, lead_group_id)
      if is_new:
        existing_leads.add(username)
        scraped_new += 1
        batch_new += 1

      if max_leads is not None and scraped_new >= max_leads:
        logger.info(
          "[GraphQL] Reached max_leads=%s, completing job. Total new leads: %d",
          max_leads,
          scraped_new,
        )
        update_scrape_job(
          conn, job["id"], status="completed", scraped_count=scraped_new
        )
        return

      _sleep_per_item()

    if batch_new == 0:
      no_new_pages += 1
    else:
      no_new_pages = 0

    update_scrape_job(conn, job["id"], scraped_count=scraped_new)
    logger.info(
      "[GraphQL] Page processed: +%d new, total %d", batch_new, scraped_new
    )

    if not has_next_page or (max_leads is not None and scraped_new >= max_leads):
      break
    if no_new_pages >= MAX_NO_NEW_PAGES:
      logger.info(
        "[GraphQL] Stopping after %d pages with no new leads.", no_new_pages
      )
      break

    _sleep_graphql_page_delay()

  logger.info(
    "[GraphQL] Follower scrape completed: %d new leads for @%s",
    scraped_new,
    target_username,
  )
  update_scrape_job(conn, job["id"], status="completed", scraped_count=scraped_new)


def _extract_shortcode_from_url(url: str) -> str:
  url = (url or "").strip()
  if not url:
    return ""
  # Simple regex-free parse: look for "/p/<shortcode>/" segment.
  parts = url.split("/")
  try:
    p_index = parts.index("p")
  except ValueError:
    # fallback: try to find segment 'p' within the path
    for i, seg in enumerate(parts):
      if seg.endswith("instagram.com") and i + 2 < len(parts) and parts[i + 1] == "p":
        return parts[i + 2]
    return ""
  if p_index + 1 < len(parts):
    return parts[p_index + 1]
  return ""


def scrape_comments(conn, job: dict):
  client_id = job["client_id"]
  post_urls = job.get("post_urls") or []
  if isinstance(post_urls, str):
    try:
      post_urls = json.loads(post_urls)
    except json.JSONDecodeError:
      post_urls = []
  if not isinstance(post_urls, list) or not post_urls:
    raise RuntimeError("post_urls must be a non-empty array on comment scrape jobs.")

  logger.info("Comment scrape started: %d post(s) job_id=%s", len(post_urls), job.get("id"))

  _sleep_warmup()

  platform_session_id = job.get("platform_scraper_session_id")
  session_row = fetch_scraper_session(conn, client_id, platform_session_id)
  if not session_row or not (session_row.get("session_data") or {}).get("cookies"):
    raise RuntimeError("Scraper session not found or expired for this job.")

  logger.info("Session loaded, building instagrapi client")
  cl = build_client_from_session(
    session_row["session_data"], session_row.get("instagram_username")
  )

  _sleep_before_first()

  max_leads = job.get("max_leads") or None
  if max_leads is not None:
    try:
      max_leads = int(max_leads)
    except (TypeError, ValueError):
      max_leads = None

  lead_group_id = job.get("lead_group_id")

  (
    in_conversations,
    sent_usernames,
    blocklist_usernames,
    existing_leads,
  ) = load_filter_sets(conn, client_id)

  scraped_new = int(job.get("scraped_count") or 0)

  for raw_url in post_urls:
    if max_leads is not None and scraped_new >= max_leads:
      break
    if _should_cancel(conn, job["id"]):
      update_scrape_job(conn, job["id"], status="cancelled", scraped_count=scraped_new)
      return

    url = str(raw_url).strip()
    if not url:
      continue
    _sleep_between_calls()
    shortcode = _extract_shortcode_from_url(url)
    logger.info("Processing post: %s", url[:60] + "..." if len(url) > 60 else url)
    try:
      media_pk = cl.media_pk_from_url(url)
    except Exception:
      if shortcode:
        try:
          media_pk = cl.media_pk_from_shortcode(shortcode)
        except Exception as e:
          raise RuntimeError(f"Failed to resolve media for URL {url}: {e}") from e
      else:
        raise RuntimeError(f"Failed to resolve media for URL {url}") from None

    try:
      comments = cl.media_comments(media_pk, amount=0)
    except ClientError as e:
      raise RuntimeError(f"Failed to fetch comments for media {media_pk}: {e}") from e

    logger.info("Fetched %d comments for media %s", len(comments), media_pk)
    source = f"comments:{shortcode or media_pk}"

    for idx, comment in enumerate(comments, start=1):
      username = (getattr(comment.user, "username", None) or "").strip().lstrip("@").lower()
      if not username:
        continue

      if (
        username in existing_leads
        or username in in_conversations
        or username in sent_usernames
        or username in blocklist_usernames
      ):
        continue

      is_new = insert_lead_if_new(conn, client_id, username, source, lead_group_id)
      if is_new:
        existing_leads.add(username)
        scraped_new += 1

      if max_leads is not None and scraped_new >= max_leads:
        logger.info("Reached max_leads=%s, completing job. Total new leads: %d", max_leads, scraped_new)
        update_scrape_job(conn, job["id"], status="completed", scraped_count=scraped_new)
        return

      _sleep_per_item()

      chunk_every = max(1, int(_float_env("SCRAPER_CHUNK_COOLDOWN_EVERY", 200.0)))
      jitter_every = max(1, int(_float_env("SCRAPER_JITTER_EVERY", 500.0)))
      if idx > 0 and idx % chunk_every == 0:
        if _should_cancel(conn, job["id"]):
          logger.info("Job cancelled during cooldown. Scraped %d new leads", scraped_new)
          update_scrape_job(conn, job["id"], status="cancelled", scraped_count=scraped_new)
          return
        _sleep_chunk_cooldown()
      if idx > 0 and idx % jitter_every == 0:
        _sleep_jitter()

      if idx % 50 == 0:
        update_scrape_job(conn, job["id"], scraped_count=scraped_new)
        if _should_cancel(conn, job["id"]):
          logger.info("Job cancelled, stopping. Scraped %d new leads", scraped_new)
          update_scrape_job(
            conn, job["id"], status="cancelled", scraped_count=scraped_new
          )
          return
        _sleep_between_batches()

  logger.info("Comment scrape completed: %d new leads", scraped_new)
  update_scrape_job(conn, job["id"], status="completed", scraped_count=scraped_new)


def main(argv: List[str]) -> int:
  parser = argparse.ArgumentParser(
    description=(
      "Cold DM scraper worker using instagrapi. "
      "Reads jobs and sessions from Supabase Postgres and writes leads + job status back."
    )
  )
  parser.add_argument("--job-id", required=True, help="ID of cold_dm_scrape_jobs row")
  parser.add_argument("--client-id", required=True, help="Tenant client_id")
  parser.add_argument(
    "--scrape-type",
    required=True,
    choices=["followers", "comments"],
    help="Type of scrape to run",
  )
  # Optional CLI overrides; normally the worker uses job row fields.
  parser.add_argument("--target-username", help="Target username for follower scrape")
  parser.add_argument(
    "--post-urls",
    help="JSON array of post URLs for comment scrape (fallback if job.post_urls missing)",
  )
  parser.add_argument(
    "--max-leads",
    type=int,
    help="Optional max leads override. If omitted, uses cold_dm_scrape_jobs.max_leads.",
  )
  parser.add_argument(
    "--lead-group-id",
    help="Optional lead_group_id override. If omitted, uses job.lead_group_id.",
  )

  args = parser.parse_args(argv)

  logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
  )
  # Silence noisy third-party loggers (instagrapi/urllib3 dump URLs and sometimes response bodies)
  for name in ("instagrapi", "urllib3", "requests", "instagrapi.mixins.private", "private_request", "public_request"):
    logging.getLogger(name).setLevel(logging.WARNING)

  logger.info("Starting job_id=%s scrape_type=%s", args.job_id, args.scrape_type)
  with get_connection() as conn:
    job = fetch_scrape_job(conn, args.job_id)
    if not job:
      logger.error("Job %s not found", args.job_id)
      return 1

    # Allow CLI overrides to fill in missing job data, but do not change DB schema here.
    if args.max_leads is not None:
      job["max_leads"] = args.max_leads
    if args.lead_group_id is not None:
      job["lead_group_id"] = args.lead_group_id
    if args.scrape_type == "followers" and args.target_username:
      job["target_username"] = args.target_username
    if args.scrape_type == "comments" and args.post_urls and not job.get("post_urls"):
      try:
        job["post_urls"] = json.loads(args.post_urls)
      except json.JSONDecodeError:
        job["post_urls"] = []

    try:
      if args.scrape_type == "followers":
        method = (job.get("scrape_method") or "instagrapi").lower()
        logger.info("Using scrape_method=%s for job %s", method, job["id"])
        if method == "graphql":
          scrape_followers_via_graphql(conn, job)
        else:
          scrape_followers(conn, job)
      else:
        scrape_comments(conn, job)
      logger.info("Job %s finished successfully", job["id"])
      return 0
    except Exception as e:
      msg = str(e)
      # Log a single, grep-friendly line that always includes the job id and reason,
      # then let logger.exception print the full traceback on the next lines.
      logger.error("Job %s failed: %s", job["id"], msg)
      logger.exception("Traceback for failed job %s", job["id"])
      try:
        update_scrape_job(conn, job["id"], status="failed", error_message=msg[:500])
      except Exception as update_err:
        logger.error("Failed to update job status after error: %s", update_err)
      return 1


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))

