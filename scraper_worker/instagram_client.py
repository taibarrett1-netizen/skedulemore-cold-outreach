import os
from typing import Any, Dict, Optional

from instagrapi import Client
from instagrapi.exceptions import ClientError
from requests.cookies import create_cookie


def build_client_from_session(session_data: Dict[str, Any], instagram_username: Optional[str] = None) -> Client:
  """
  Initialize an instagrapi Client using cookies stored from Puppeteer.

  WARNING: This relies on mapping Puppeteer-style cookies into the instagrapi client.
  If login starts failing, you may need to:
  - Re-connect the scraper account so fresh cookies are stored, OR
  - Perform a direct instagrapi login once and persist cl.get_settings() instead.
  """
  cl = Client()

  proxy_url = os.getenv("SCRAPER_PROXY_URL")
  if proxy_url:
    # Optional proxy support stub
    cl.set_proxy(proxy_url)

  cookies = (session_data or {}).get("cookies") or []
  if not cookies:
    raise RuntimeError("No cookies found in session_data for scraper session.")

  # Map basic cookies into the underlying requests session. This is a best-effort
  # mapping; if IG changes cookie requirements you may need to adjust which cookies
  # are set here.
  jar = cl.http.cookies
  for c in cookies:
    name = c.get("name")
    value = c.get("value")
    if not name or value is None:
      continue
    domain = c.get("domain") or ".instagram.com"
    cookie = create_cookie(name=name, value=value, domain=domain)
    jar.set_cookie(cookie)

  # Lightweight validation – ensure session is still valid.
  try:
    if instagram_username:
      cl.user_info_by_username(instagram_username)
    else:
      # Fallback: fetch current user info; may fail if session is invalid.
      cl.account_info()
  except ClientError as e:
    raise RuntimeError(
      f"Instagram session invalid or expired for scraper account. "
      f"Reconnect via /api/scraper/connect. Underlying error: {e}"
    ) from e

  return cl

