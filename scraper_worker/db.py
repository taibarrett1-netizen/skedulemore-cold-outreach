import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras


def _get_dsn() -> str:
  """
  Returns the Postgres connection string.

  Prefer SUPABASE_DB_URL if set; fall back to DATABASE_URL.
  You must configure one of these in the VPS environment.
  """
  dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
  if not dsn:
    raise RuntimeError(
      "SUPABASE_DB_URL or DATABASE_URL must be set for scraper_worker DB access."
    )
  return dsn


@contextmanager
def get_connection():
  conn = psycopg2.connect(_get_dsn())
  try:
    yield conn
  finally:
    conn.close()


def fetch_scraper_session(conn, client_id, platform_session_id=None):
  """
  Load scraper session JSON and username.

  Mirrors Node pickScraperSessionForJob logic in a simplified form:
  - If platform_session_id is provided, use cold_dm_platform_scraper_sessions.
  - Otherwise, fall back to cold_dm_scraper_sessions for this client.
  """
  with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    if platform_session_id:
      cur.execute(
        """
        SELECT session_data, instagram_username
        FROM cold_dm_platform_scraper_sessions
        WHERE id = %s
        """,
        (platform_session_id,),
      )
      row = cur.fetchone()
      if row:
        return row

    cur.execute(
      """
      SELECT session_data, instagram_username
      FROM cold_dm_scraper_sessions
      WHERE client_id = %s
      """,
      (client_id,),
    )
    return cur.fetchone()


def fetch_scrape_job(conn, job_id):
  with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute(
      """
      SELECT
        id,
        client_id,
        target_username,
        status,
        scraped_count,
        scrape_type,
        post_urls,
        lead_group_id,
        platform_scraper_session_id,
        instagram_session_id,
        max_leads
      FROM cold_dm_scrape_jobs
      WHERE id = %s
      """,
      (job_id,),
    )
    return cur.fetchone()


def fetch_instagram_session_for_scrape(conn, instagram_session_id):
  """
  Load per-client IG session fields needed for instagrapi scraping.
  Returns:
    - proxy_url (current proxy)
    - instagrapi_proxy_url (proxy used when settings were created)
    - instagrapi_state
    - settings_json (decrypted text)
    - instagram_username
  """
  with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute(
      """
      SELECT
        id,
        client_id,
        instagram_username,
        proxy_url,
        instagrapi_proxy_url,
        instagrapi_state,
        decrypt_credential(instagrapi_settings_encrypted) AS settings_json
      FROM cold_dm_instagram_sessions
      WHERE id = %s
      """,
      (instagram_session_id,),
    )
    return cur.fetchone()


def update_instagrapi_state(conn, instagram_session_id, state, error_class=None, error_message=None):
  with conn.cursor() as cur:
    cur.execute(
      """
      UPDATE cold_dm_instagram_sessions
      SET
        instagrapi_state = %s,
        instagrapi_last_error_class = %s,
        instagrapi_last_error_message = %s,
        updated_at = NOW()
      WHERE id = %s
      """,
      (
        state,
        (str(error_class)[:120] if error_class else None),
        (str(error_message)[:2000] if error_message else None),
        instagram_session_id,
      ),
    )
  conn.commit()


def update_scrape_job(conn, job_id, **fields):
  if not fields:
    return

  # If status is terminal, also set finished_at.
  status = fields.get("status")
  if status in ("completed", "failed", "cancelled") and "finished_at" not in fields:
    # Use a simple sentinel; the actual value is always set to NOW() in the SQL.
    fields["finished_at"] = True

  set_clauses = []
  values = []
  for idx, (key, value) in enumerate(fields.items(), start=1):
    if key == "finished_at" and value is not None:
      set_clauses.append("finished_at = NOW()")
      continue
    set_clauses.append(f"{key} = %s")
    values.append(value)
  values.append(job_id)

  sql = f"UPDATE cold_dm_scrape_jobs SET {', '.join(set_clauses)} WHERE id = %s"
  with conn.cursor() as cur:
    cur.execute(sql, values)
  conn.commit()


def get_status_for_job(conn, job_id):
  with conn.cursor() as cur:
    cur.execute(
      "SELECT status FROM cold_dm_scrape_jobs WHERE id = %s",
      (job_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def load_filter_sets(conn, client_id):
  """
  Returns four sets of usernames (lowercase, no @):
  - in_conversations
  - sent_usernames
  - blocklist_usernames
  - existing_leads
  """
  in_conversations = set()
  sent_usernames = set()
  blocklist_usernames = set()
  existing_leads = set()

  with conn.cursor() as cur:
    # conversations
    cur.execute(
      "SELECT participant_username FROM conversations WHERE client_id = %s",
      (client_id,),
    )
    for (username,) in cur.fetchall() or []:
      if not username:
        continue
      u = str(username).strip().lstrip("@").lower()
      if u:
        in_conversations.add(u)

    # sent messages
    cur.execute(
      "SELECT username FROM cold_dm_sent_messages WHERE client_id = %s",
      (client_id,),
    )
    for (username,) in cur.fetchall() or []:
      if not username:
        continue
      u = str(username).strip().lstrip("@").lower()
      if u:
        sent_usernames.add(u)

    # blocklist
    cur.execute(
      "SELECT username FROM cold_dm_scrape_blocklist WHERE client_id = %s",
      (client_id,),
    )
    for (username,) in cur.fetchall() or []:
      if not username:
        continue
      u = str(username).strip().lstrip("@").lower()
      if u:
        blocklist_usernames.add(u)

    # existing leads
    cur.execute(
      "SELECT username FROM cold_dm_leads WHERE client_id = %s",
      (client_id,),
    )
    for (username,) in cur.fetchall() or []:
      if not username:
        continue
      u = str(username).strip().lstrip("@").lower()
      if u:
        existing_leads.add(u)

  return in_conversations, sent_usernames, blocklist_usernames, existing_leads


def insert_lead_if_new(conn, client_id, username, source, lead_group_id=None):
  """
  Inserts a lead row if it does not already exist.
  Returns True if a new row was inserted, False on conflict.
  """
  clean = str(username).strip().lstrip("@").lower()
  if not clean:
    return False

  with conn.cursor() as cur:
    cur.execute(
      """
      INSERT INTO cold_dm_leads (client_id, username, source, lead_group_id, added_at)
      VALUES (%s, %s, %s, %s, NOW())
      ON CONFLICT (client_id, username) DO NOTHING
      RETURNING 1
      """,
      (client_id, clean, source, lead_group_id),
    )
    row = cur.fetchone()
  conn.commit()
  return bool(row)


def _format_reset_in_text(delta: timedelta) -> str:
  total_minutes = max(0, int((delta.total_seconds() + 59) // 60))
  if total_minutes <= 1:
    return "less than 1 minute"
  days = total_minutes // (60 * 24)
  hours = (total_minutes % (60 * 24)) // 60
  minutes = total_minutes % 60
  parts = []
  if days > 0:
    parts.append(f"{days}d")
  if hours > 0:
    parts.append(f"{hours}h")
  if minutes > 0 and days == 0:
    parts.append(f"{minutes}m")
  return " ".join(parts)


def get_scrape_quota_status(conn, client_id):
  limit = 1000
  now = datetime.now(timezone.utc)
  window_start = now - timedelta(days=7)
  with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute(
      """
      SELECT COUNT(*)::int AS used
      FROM cold_dm_leads
      WHERE client_id = %s
        AND added_at >= %s
        AND (
          source ILIKE 'followers:%%'
          OR source ILIKE 'following:%%'
          OR source ILIKE 'comments:%%'
        )
      """,
      (client_id, window_start),
    )
    row = cur.fetchone() or {}
    used = int(row.get("used") or 0)
    remaining = max(0, limit - used)
    reset_at = None
    if used > 0:
      cur.execute(
        """
        SELECT added_at
        FROM cold_dm_leads
        WHERE client_id = %s
          AND added_at >= %s
          AND (
            source ILIKE 'followers:%%'
            OR source ILIKE 'following:%%'
            OR source ILIKE 'comments:%%'
          )
        ORDER BY added_at ASC
        LIMIT 1
        """,
        (client_id, window_start),
      )
      oldest = cur.fetchone()
      oldest_added_at = oldest.get("added_at") if oldest else None
      if oldest_added_at is not None:
        reset_at = oldest_added_at + timedelta(days=7)
    reset_delta = (reset_at - now) if reset_at is not None else timedelta()
    reset_in_text = _format_reset_in_text(reset_delta)
    return {
      "limit": limit,
      "used": used,
      "remaining": remaining,
      "reset_at": reset_at,
      "reset_in_text": reset_in_text,
      "message": f"1000 leads maximum reached, please wait for your scraping usage to reset in {reset_in_text}.",
    }
