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
  for c in cookies:
    name = c.get("name")
    value = c.get("value")
    if not name or value is None:
      continue
    domain = c.get("domain") or ".instagram.com"
    cookie = create_cookie(name=name, value=value, domain=domain)
    for j in jars:
      j.set_cookie(cookie)

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

