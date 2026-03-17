import base64
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from instagrapi import Client
from instagrapi.exceptions import ClientError
from requests.cookies import create_cookie
from requests.exceptions import RetryError

logger = logging.getLogger("instagram_client")


def build_client_from_session(session_data: Dict[str, Any], instagram_username: Optional[str] = None) -> Client:
  """
  Initialize an instagrapi Client using cookies stored from Puppeteer.

  WARNING: This relies on mapping Puppeteer-style cookies into the instagrapi client.
  If login starts failing, you may need to:
  - Re-connect the scraper account so fresh cookies are stored, OR
  - Perform a direct instagrapi login once and persist cl.get_settings() instead.
  """
  cl = Client()

  proxy_url = (os.getenv("SCRAPER_PROXY_URL") or "").strip()
  if proxy_url:
    cl.set_proxy(proxy_url)
    # Set proxy on both sessions and add explicit Proxy-Authorization header (requests/urllib3
    # sometimes drop auth from the URL on HTTPS CONNECT, causing 407).
    try:
      parsed = urlparse(proxy_url)
      user, passwd = parsed.username or "", parsed.password or ""
      auth_header = None
      if user or passwd:
        token = base64.b64encode(f"{user}:{passwd}".encode()).decode("ascii")
        auth_header = f"Basic {token}"
      for session_attr in ("private", "public"):
        sess = getattr(cl, session_attr, None)
        if sess is not None and hasattr(sess, "proxies"):
          sess.proxies = {"http": proxy_url, "https": proxy_url}
          if auth_header and hasattr(sess, "headers"):
            sess.headers["Proxy-Authorization"] = auth_header
          logger.info("Set proxy on %s (auth in URL + header)", session_attr)
    except Exception as e:
      logger.warning("Proxy auth setup failed: %s", e)

  cookies = (session_data or {}).get("cookies") or []
  if not cookies:
    raise RuntimeError("No cookies found in session_data for scraper session.")
  logger.info("Loading session: %d cookies", len(cookies))

  # Find the requests session's cookie jar. Instagrapi 2.x uses different internal
  # structure (e.g. private.session) so try several possible paths.
  def _get_jar(obj, *attrs):
    for attr in attrs:
      obj = getattr(obj, attr, None) if obj is not None else None
    return obj

  jar = None
  for base, path in (
    (getattr(cl, "private", None), ("cookies",)),  # instagrapi 2.x: private is Session with .cookies
    (getattr(cl, "public", None), ("cookies",)),
    (cl, ("http", "cookies")),
    (getattr(cl, "private", None), ("session", "cookies")),
    (getattr(cl, "private", None), ("_session", "cookies")),
    (getattr(cl, "public", None), ("session", "cookies")),
    (cl, ("_session", "cookies")),
    (cl, ("session", "cookies")),
  ):
    if base is None:
      continue
    candidate = _get_jar(base, *path)
    if candidate is not None and getattr(candidate, "set_cookie", None) is not None:
      jar = candidate
      break
  if jar is None:
    raise RuntimeError(
      "Could not find requests session on instagrapi Client. "
      "Your instagrapi version may use a different structure; try upgrading: pip install -U instagrapi"
    )

  # Collect all cookie jars we should update (instagrapi 2.x has private + public).
  jars = [jar]
  other = _get_jar(getattr(cl, "public", None), "cookies") if getattr(cl, "private", None) is not None else None
  if other is not None and other is not jar and getattr(other, "set_cookie", None) is not None:
    jars.append(other)

  # Map basic cookies into the underlying requests session(s).
  set_count = 0
  for c in cookies:
    name = c.get("name")
    value = c.get("value")
    if not name or value is None:
      continue
    domain = c.get("domain") or ".instagram.com"
    cookie = create_cookie(name=name, value=value, domain=domain)
    for j in jars:
      j.set_cookie(cookie)
    set_count += 1
  logger.info("Set %d cookies on client", set_count)

  # Rate-limit: random delay between every API request (reduces ban/challenge risk).
  def _float_env(name: str, default: float) -> float:
    try:
      v = os.getenv(name)
      return float(v) if v not in (None, "") else default
    except (TypeError, ValueError):
      return default

  delay_min = _float_env("SCRAPER_DELAY_MIN", 2.0)
  delay_max = _float_env("SCRAPER_DELAY_MAX", 6.0)
  if delay_max < delay_min:
    delay_max = delay_min
  cl.delay_range = [delay_min, delay_max]
  logger.info("Request delay_range=[%.1f, %.1f]s (set SCRAPER_DELAY_MIN/MAX to override)", delay_min, delay_max)

  # Validation: use private API only (account_info) to avoid public web_profile_info which is heavily rate-limited (429).
  # Set SKIP_SESSION_VALIDATION=1 to skip this and go straight to scrape (session checked on first real request).
  if os.getenv("SKIP_SESSION_VALIDATION", "").strip().lower() in ("1", "true", "yes"):
    logger.info("Skipping session validation (SKIP_SESSION_VALIDATION=1)")
    return cl

  try:
    logger.info("Validating session via account_info() (private API)")
    cl.account_info()
    logger.info("Session validated successfully")
  except ClientError as e:
    raise RuntimeError(
      f"Instagram session invalid or expired for scraper account. "
      f"Reconnect via /api/scraper/connect. Underlying error: {e}"
    ) from e
  except RetryError as e:
    logger.warning("Session validation hit rate limit (429)")
    raise RuntimeError(
      "Instagram rate limit (429). Wait 15–60 minutes and retry, or set SCRAPER_PROXY_URL to use a proxy."
    ) from e

  return cl

