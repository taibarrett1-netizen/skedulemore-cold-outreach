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
  lo = _float_env("SCRAPER_DELAY_BATCH_MIN", 6.0)
  hi = _float_env("SCRAPER_DELAY_BATCH_MAX", 14.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_before_first():
  lo = _float_env("SCRAPER_DELAY_BEFORE_FIRST_MIN", 3.0)
  hi = _float_env("SCRAPER_DELAY_BEFORE_FIRST_MAX", 8.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_between_calls():
  lo = _float_env("SCRAPER_DELAY_BETWEEN_CALLS_MIN", 4.0)
  hi = _float_env("SCRAPER_DELAY_BETWEEN_CALLS_MAX", 10.0)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _sleep_per_item():
  lo = _float_env("SCRAPER_DELAY_PER_ITEM_MIN", 0.4)
  hi = _float_env("SCRAPER_DELAY_PER_ITEM_MAX", 1.2)
  if hi < lo:
    hi = lo
  time.sleep(random.uniform(lo, hi))


def _should_cancel(conn, job_id: str) -> bool:
  status = get_status_for_job(conn, job_id)
  return status and status != "running"


def scrape_followers(conn, job: dict):
  client_id = job["client_id"]
  target_username = (job.get("target_username") or "").strip().lstrip("@").lower()
  if not target_username:
    raise RuntimeError("target_username missing on scrape job.")

  logger.info("Follower scrape started: target=@%s job_id=%s", target_username, job.get("id"))

  platform_session_id = job.get("platform_scraper_session_id")
  session_row = fetch_scraper_session(conn, client_id, platform_session_id)
  if not session_row or not (session_row.get("session_data") or {}).get("cookies"):
    raise RuntimeError("Scraper session not found or expired for this job.")

  logger.info("Session loaded (platform_session_id=%s), building instagrapi client", platform_session_id)
  cl = build_client_from_session(
    session_row["session_data"], session_row.get("instagram_username")
  )
  logger.info("Client ready, resolving user_id for @%s", target_username)

  max_leads = job.get("max_leads") or None
  if max_leads is not None:
    try:
      max_leads = int(max_leads)
    except (TypeError, ValueError):
      max_leads = None

  lead_group_id = job.get("lead_group_id")
  source = f"followers:{target_username}"

  (
    in_conversations,
    sent_usernames,
    blocklist_usernames,
    existing_leads,
  ) = load_filter_sets(conn, client_id)

  scraped_new = int(job.get("scraped_count") or 0)

  _sleep_before_first()
  try:
    user_id = cl.user_id_from_username(target_username)
  except ClientError as e:
    raise RuntimeError(f"Failed to resolve user_id for @{target_username}: {e}") from e

  _sleep_between_calls()
  followers_dict = cl.user_followers(user_id, amount=0)
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
  for name in ("instagrapi", "urllib3", "requests", "instagrapi.mixins.private", "private_request"):
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
        scrape_followers(conn, job)
      else:
        scrape_comments(conn, job)
      logger.info("Job %s finished successfully", job["id"])
      return 0
    except Exception as e:
      msg = str(e)
      logger.exception("Job %s failed: %s", job["id"], msg)
      try:
        update_scrape_job(conn, job["id"], status="failed", error_message=msg[:500])
      except Exception as update_err:
        logger.error("Failed to update job status after error: %s", update_err)
      return 1


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))

