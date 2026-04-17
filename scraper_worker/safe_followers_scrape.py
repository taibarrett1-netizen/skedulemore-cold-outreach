#!/usr/bin/env python3
"""
Per-client safe follower scraper (instagrapi private mobile API).

Hard-coded pacing rules (not configurable via UI):
- 30–60s sleep between follower-list API pages (one network request per paced step).
- Extra cooldown 3–5 minutes every ~120 newly-inserted followers (threshold jitter 110–130).
- Fixed page size = 30 (within 20–40).

Safety:
- No retries on Instagram/API errors: fail fast with a clear error class.
- Global PK de-dupe + duplicate/loop circuit breaker.
- Cursor stall detection (cursor repeats; all-duplicate chunk streak).
- Proxy mismatch guard: if proxy differs from instagrapi_proxy_url saved at login, refuse reuse.
- Interruptible sleeps with cancellation semantics: stop when scrape job status != 'running'.
"""

import argparse
import json
import random
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from instagrapi import Client
from instagrapi.extractors import extract_user_short
from instagrapi.exceptions import (
    ChallengeRequired,
    ClientError,
    FeedbackRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    RateLimitError,
)

from db import (
    fetch_instagram_session_for_scrape,
    fetch_scrape_job,
    get_connection,
    get_scrape_quota_status,
    get_status_for_job,
    insert_lead_if_new,
    load_filter_sets,
    update_instagrapi_state,
    update_scrape_job,
)


PAGE_SIZE = 30
PAGE_SLEEP_MIN_S = 30
PAGE_SLEEP_MAX_S = 60
COOLDOWN_SLEEP_MIN_S = 180
COOLDOWN_SLEEP_MAX_S = 300
COOLDOWN_THRESHOLD_MIN = 110
COOLDOWN_THRESHOLD_MAX = 130


def normalize_proxy_url(proxy_url: str) -> str:
    raw = (proxy_url or "").strip()
    if not raw or "@" not in raw:
        return raw
    parts = urlsplit(raw)
    if not parts.scheme or not parts.hostname:
        return raw
    netloc = parts.netloc
    if "@" not in netloc:
        return raw
    userinfo, hostport = netloc.rsplit("@", 1)
    if ":" not in userinfo:
        return raw
    user, sep, password = userinfo.partition(":")
    if sep != ":":
        return raw
    u = quote(unquote(user), safe="-._~")
    p = quote(unquote(password), safe="-._~")
    new_netloc = f"{u}:{p}@{hostport}"
    return urlunsplit((parts.scheme, new_netloc, parts.path or "", parts.query, parts.fragment))


def _interruptible_sleep(conn, job_id: str, seconds: float, label: str) -> bool:
    """
    Sleep in short chunks and stop early if job is cancelled.
    Returns True if cancelled.
    """
    end = time.time() + max(0.0, float(seconds or 0.0))
    while time.time() < end:
        status = get_status_for_job(conn, job_id)
        if status and status != "running":
            return True
        # 2s chunks keeps DB load modest but still responsive.
        time.sleep(min(2.0, max(0.0, end - time.time())))
    return False


def _device_settings() -> Dict[str, Any]:
    return {
        "app_version": "269.0.0.0.0",
        "android_version": "31",
        "android_release": "12",
        "dpi": "480dpi",
        "resolution": "1080x1920",
        "manufacturer": "samsung",
        "device": "SM-G973U",
        "model": "samsung",
        "cpu": "exynos",
        "version_code": "269000000",
    }


def _load_settings_into_client(cl: Client, settings: Dict[str, Any]) -> None:
    if hasattr(cl, "set_settings"):
        cl.set_settings(settings)  # type: ignore[call-arg]
        return
    # Fallback: many versions store on .settings
    setattr(cl, "settings", settings)


def _user_followers_v1_single_page(cl: Client, user_id: str, count: int, max_id: str) -> Tuple[List[Any], str]:
    """
    Exactly one friendships/{id}/followers/ request (one paced step == one network call).
    """
    result = cl.private_request(
        f"friendships/{user_id}/followers/",
        params={
            "max_id": max_id or "",
            "count": max(1, int(count)),
            "rank_token": cl.rank_token,
            "search_surface": "follow_list_page",
            "query": "",
            "enable_groups": "true",
        },
    )
    users: List[Any] = []
    seen: set = set()
    for raw in result.get("users") or []:
        u = extract_user_short(raw)
        if u.pk in seen:
            continue
        seen.add(u.pk)
        users.append(u)
    next_raw = result.get("next_max_id")
    next_max_id = "" if next_raw is None else str(next_raw)
    return users, next_max_id


def _fail_job(conn, job_id: str, scraped_count: int, error_class: str, message: str) -> None:
    update_scrape_job(
        conn,
        job_id,
        status="failed",
        scraped_count=int(scraped_count or 0),
        last_error_class=str(error_class)[:120],
        last_error_message=str(message or "")[:2000],
        error_message=str(message or "")[:500],
    )


def run_job(conn, job_id: str) -> int:
    job = fetch_scrape_job(conn, job_id)
    if not job:
        return 2

    if (job.get("status") or "") != "running":
        return 0

    if (job.get("scrape_type") or "followers") != "followers":
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "unsupported_scrape_type", "Only follower scraping is supported.")
        return 0

    instagram_session_id = job.get("instagram_session_id")
    if not instagram_session_id:
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "missing_instagram_session", "No instagram_session_id attached to scrape job.")
        return 0

    sess = fetch_instagram_session_for_scrape(conn, instagram_session_id)
    if not sess:
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "instagram_session_not_found", "Instagram session row not found.")
        return 0

    state = (sess.get("instagrapi_state") or "").strip().lower()
    if state != "ready":
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "instagrapi_not_ready", "Scraping login not enabled. Reconnect scraping login and try again.")
        return 0

    proxy_current = normalize_proxy_url(str(sess.get("proxy_url") or "").strip())
    proxy_saved = normalize_proxy_url(str(sess.get("instagrapi_proxy_url") or "").strip())
    if proxy_saved and proxy_current and proxy_saved != proxy_current:
        update_instagrapi_state(conn, instagram_session_id, "reauth_required", "proxy_mismatch", "Proxy changed; instagrapi session must be recreated.")
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "proxy_mismatch", "Proxy changed for this Instagram account. Reconnect scraping login to continue.")
        return 0
    if not proxy_current:
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "missing_proxy", "Proxy URL missing for this Instagram account.")
        return 0

    settings_json = (sess.get("settings_json") or "").strip()
    if not settings_json:
        update_instagrapi_state(conn, instagram_session_id, "reauth_required", "missing_settings", "No instagrapi settings stored.")
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "missing_settings", "Scraping login missing or expired. Reconnect scraping login.")
        return 0

    try:
        settings = json.loads(settings_json)
        if not isinstance(settings, dict):
            raise ValueError("settings is not an object")
    except Exception:
        update_instagrapi_state(conn, instagram_session_id, "reauth_required", "settings_parse_failed", "Could not parse instagrapi settings JSON.")
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "settings_parse_failed", "Could not read scraping session settings. Reconnect scraping login.")
        return 0

    target_username = (job.get("target_username") or "").strip().lstrip("@")
    if not target_username:
        _fail_job(conn, job_id, int(job.get("scraped_count") or 0), "missing_target", "target_username missing on scrape job.")
        return 0

    # Quota is rolling 7 days / 1000 unique leads.
    quota = get_scrape_quota_status(conn, job.get("client_id"))
    if quota and int(quota.get("remaining") or 0) <= 0:
        msg = quota.get("message") or "1000 leads maximum reached, please wait for your scraping usage to reset."
        update_scrape_job(conn, job_id, status="completed", scraped_count=int(job.get("scraped_count") or 0), error_message=str(msg)[:500])
        return 0

    max_leads = job.get("max_leads")
    try:
        max_leads_n = int(max_leads) if max_leads is not None else None
    except Exception:
        max_leads_n = None
    if max_leads_n is not None and max_leads_n > 0 and quota:
        max_leads_n = min(max_leads_n, int(quota.get("remaining") or max_leads_n))

    # Filters: in conversations, already sent, blocklist, existing leads
    in_convos, sent_usernames, blocklist_usernames, existing_leads = load_filter_sets(conn, job.get("client_id"))
    client_id = job.get("client_id")
    lead_group_id = job.get("lead_group_id")
    source = f"followers:{target_username.strip().lower()}"

    scraped_new = int(job.get("scraped_count") or 0)
    inserted_since_cooldown = 0
    cooldown_threshold = random.randint(COOLDOWN_THRESHOLD_MIN, COOLDOWN_THRESHOLD_MAX)

    cl = Client()
    cl.set_proxy(proxy_current)
    try:
        cl.set_device(_device_settings())
    except Exception:
        pass
    _load_settings_into_client(cl, settings)

    # Validate quickly via private API; avoids public endpoints.
    try:
        cl.account_info()
    except Exception:
        # Some sessions fail account_info but still can do private requests; proceed and let errors surface.
        pass

    try:
        # Resolve target via private API (avoids web_profile_info heavy 429).
        target_pk = str(cl.user_info_by_username_v1(target_username).pk)
    except Exception as e:
        _fail_job(conn, job_id, scraped_new, "resolve_target_failed", f"Failed to resolve @{target_username}: {type(e).__name__}")
        return 0

    max_id: str = ""
    pending_users: List[Any] = []
    pending_next_max_id: Optional[str] = None

    seen_pks: set[str] = set()
    duplicate_pk_hits = 0
    all_dup_chunk_streak = 0
    cursor_loop_streak = 0
    DUPLICATE_PK_CIRCUIT_BREAKER = 40
    ALL_DUP_CHUNK_STREAK_MAX = 3
    CURSOR_LOOP_STREAK_MAX = 2

    try:
        while True:
            if max_leads_n is not None and scraped_new >= max_leads_n:
                update_scrape_job(conn, job_id, status="completed", scraped_count=scraped_new)
                return 0

            status = get_status_for_job(conn, job_id)
            if status and status != "running":
                update_scrape_job(conn, job_id, status="cancelled", scraped_count=scraped_new)
                return 0

            # One paced step == one request. Buffer slices are not requests.
            if pending_users:
                chunk_users = pending_users[:PAGE_SIZE]
                pending_users = pending_users[PAGE_SIZE:]
                if not pending_users and pending_next_max_id is not None:
                    max_id = pending_next_max_id
                    pending_next_max_id = None
                did_network = False
            else:
                cursor_sent = max_id or ""
                raw_users, next_max_id = _user_followers_v1_single_page(cl, target_pk, PAGE_SIZE, max_id)
                next_s = next_max_id or ""
                if raw_users and cursor_sent and next_s == cursor_sent:
                    cursor_loop_streak += 1
                    if cursor_loop_streak >= CURSOR_LOOP_STREAK_MAX:
                        _fail_job(conn, job_id, scraped_new, "cursor_stall", "Cursor stall: next cursor repeats request cursor.")
                        return 0
                else:
                    cursor_loop_streak = 0

                chunk_users = raw_users[:PAGE_SIZE]
                pending_users = raw_users[PAGE_SIZE:]
                if pending_users:
                    pending_next_max_id = next_max_id or ""
                else:
                    max_id = next_max_id or ""
                did_network = True

            if not chunk_users:
                update_scrape_job(conn, job_id, status="completed", scraped_count=scraped_new)
                return 0

            new_pk_in_chunk = 0
            for u in chunk_users:
                pk = str(getattr(u, "pk", "") or "")
                if not pk:
                    continue
                if pk in seen_pks:
                    duplicate_pk_hits += 1
                    if duplicate_pk_hits >= DUPLICATE_PK_CIRCUIT_BREAKER:
                        _fail_job(conn, job_id, scraped_new, "duplicate_pk_circuit_breaker", "Too many repeated follower PKs (loop).")
                        return 0
                    continue
                seen_pks.add(pk)
                new_pk_in_chunk += 1

                username = (getattr(u, "username", None) or "").strip().lstrip("@").lower()
                if not username:
                    continue
                if (
                    username in existing_leads
                    or username in in_convos
                    or username in sent_usernames
                    or username in blocklist_usernames
                    or username == target_username.strip().lower()
                ):
                    continue

                if quota and int(quota.get("remaining") or 0) <= 0:
                    msg = quota.get("message") or "1000 leads maximum reached, please wait for your scraping usage to reset."
                    update_scrape_job(conn, job_id, status="completed", scraped_count=scraped_new, error_message=str(msg)[:500])
                    return 0

                is_new = insert_lead_if_new(conn, client_id, username, source, lead_group_id)
                if is_new:
                    existing_leads.add(username)
                    scraped_new += 1
                    inserted_since_cooldown += 1
                    if max_leads_n is not None and scraped_new >= max_leads_n:
                        update_scrape_job(conn, job_id, status="completed", scraped_count=scraped_new)
                        return 0

            if new_pk_in_chunk == 0:
                all_dup_chunk_streak += 1
                if all_dup_chunk_streak >= ALL_DUP_CHUNK_STREAK_MAX:
                    _fail_job(conn, job_id, scraped_new, "all_duplicate_streak", "Cursor stall: consecutive chunks had no new follower PKs.")
                    return 0
            else:
                all_dup_chunk_streak = 0

            update_scrape_job(conn, job_id, scraped_count=scraped_new)

            # Extra cooldown every ~120 inserted leads.
            if inserted_since_cooldown >= cooldown_threshold and (max_leads_n is None or scraped_new < max_leads_n):
                cooldown = random.uniform(COOLDOWN_SLEEP_MIN_S, COOLDOWN_SLEEP_MAX_S)
                inserted_since_cooldown = 0
                cooldown_threshold = random.randint(COOLDOWN_THRESHOLD_MIN, COOLDOWN_THRESHOLD_MAX)
                if _interruptible_sleep(conn, job_id, cooldown, "extra_cooldown"):
                    update_scrape_job(conn, job_id, status="cancelled", scraped_count=scraped_new)
                    return 0

            # Page pacing only after a network request.
            if did_network and (max_leads_n is None or scraped_new < max_leads_n):
                delay = random.uniform(PAGE_SLEEP_MIN_S, PAGE_SLEEP_MAX_S)
                if _interruptible_sleep(conn, job_id, delay, "page_delay"):
                    update_scrape_job(conn, job_id, status="cancelled", scraped_count=scraped_new)
                    return 0

    except ChallengeRequired as e:
        update_instagrapi_state(conn, instagram_session_id, "challenge_required", "challenge_required", str(e))
        _fail_job(conn, job_id, scraped_new, "challenge_required", "Instagram security checkpoint/challenge. Reconnect scraping login.")
        return 0
    except LoginRequired as e:
        update_instagrapi_state(conn, instagram_session_id, "reauth_required", "login_required", str(e))
        _fail_job(conn, job_id, scraped_new, "login_required", "Instagram login required. Reconnect scraping login.")
        return 0
    except (RateLimitError, PleaseWaitFewMinutes, FeedbackRequired) as e:
        update_instagrapi_state(conn, instagram_session_id, "cooldown", "rate_limited", str(e))
        _fail_job(conn, job_id, scraped_new, "rate_limited", "Instagram rate limit. Wait 15–60 minutes and try again.")
        return 0
    except ClientError as e:
        _fail_job(conn, job_id, scraped_new, "client_error", f"Instagram client error: {str(e)[:180]}")
        return 0
    except Exception as e:
        _fail_job(conn, job_id, scraped_new, "unknown_error", f"{type(e).__name__}: {str(e)[:180]}")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job_id", required=True)
    args = ap.parse_args()
    job_id = str(args.job_id).strip()
    if not job_id:
        return 2
    with get_connection() as conn:
        return run_job(conn, job_id)


if __name__ == "__main__":
    raise SystemExit(main())
