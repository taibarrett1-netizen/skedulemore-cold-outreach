import json
import os
from contextlib import contextmanager

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
        max_leads
      FROM cold_dm_scrape_jobs
      WHERE id = %s
      """,
      (job_id,),
    )
    return cur.fetchone()


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

