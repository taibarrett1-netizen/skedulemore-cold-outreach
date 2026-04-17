#!/usr/bin/env python3
"""
One-time instagrapi login helper (mobile private API).

This script is intentionally stateless:
- It does NOT write a session file to disk.
- It does NOT store the password.
- If Instagram requires a 2FA/TOTP code, the caller should re-run this script with --verification_code.

Output: a single JSON line to stdout.
"""

import argparse
import json
import sys
from typing import Any, Dict, Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from instagrapi import Client
from instagrapi.exceptions import (
    BadPassword,
    ChallengeRequired,
    FeedbackRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    RateLimitError,
    TwoFactorRequired as IGTwoFactorRequired,
)


def _normalize_proxy_url(proxy_url: str) -> str:
    """
    Normalize proxy URL by percent-encoding userinfo.

    Decodo (and others) may use characters like +, :, @ in passwords; leaving them raw can break
    CONNECT auth. This mirrors new-ig-folllower-scrape's normalization behavior.
    """
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


def _decodo_colon_format_to_proxy_url(line: str) -> str:
    """
    Convert Decodo dashboard line `host:port:username:password` into normalized http URL.
    """
    raw = (line or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://") or raw.startswith("socks5://"):
        return _normalize_proxy_url(raw)
    parts = raw.split(":")
    if len(parts) < 4:
        return raw
    host, port, user = parts[0], parts[1], parts[2]
    password = ":".join(parts[3:])
    if not host or not port or not user:
        return raw
    built = f"http://{user}:{password}@{host}:{port}"
    return _normalize_proxy_url(built)


def _device_settings() -> Dict[str, Any]:
    # Mirror the reference repo's stable Samsung-like Android profile.
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


def _emit(payload: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--proxy", default="")
    ap.add_argument("--verification_code", default="")
    args = ap.parse_args()

    username = (args.username or "").strip().lstrip("@")
    password = args.password or ""
    proxy_url = _decodo_colon_format_to_proxy_url(args.proxy)
    code = (args.verification_code or "").strip()

    if not username or not password:
        _emit({"ok": False, "error": "username/password required"})
        return 2

    cl = Client()
    if proxy_url:
        cl.set_proxy(proxy_url)

    try:
        cl.set_device(_device_settings())
    except Exception:
        # Older instagrapi versions might not support set_device; ignore quietly.
        pass

    try:
        if code:
            # Some instagrapi versions accept verification_code kwarg.
            try:
                cl.login(username, password, verification_code=code)
            except TypeError:
                # Fallback: try without kwarg; caller can rely on web connect if this version can't handle it.
                cl.login(username, password)
        else:
            cl.login(username, password)

        settings: Optional[Dict[str, Any]] = None
        try:
            settings = cl.get_settings()  # type: ignore[assignment]
        except Exception:
            settings = None

        if not settings or not isinstance(settings, dict):
            _emit({"ok": False, "error": "instagrapi did not return settings"})
            return 1

        _emit(
            {
                "ok": True,
                "instagram_username": username,
                "proxy_url": proxy_url or None,
                "settings_json": json.dumps(settings),
            }
        )
        return 0

    except IGTwoFactorRequired:
        _emit(
            {
                "ok": False,
                "code": "two_factor_required",
                "error": "Instagram 2FA code required",
            }
        )
        return 0
    except ChallengeRequired:
        _emit(
            {
                "ok": False,
                "code": "challenge_required",
                "error": "Instagram security challenge required (checkpoint/suspicious login).",
            }
        )
        return 0
    except (RateLimitError, PleaseWaitFewMinutes, FeedbackRequired):
        _emit(
            {
                "ok": False,
                "code": "rate_limited",
                "error": "Instagram rate limit during login. Wait and try again later.",
            }
        )
        return 0
    except (BadPassword, LoginRequired):
        _emit({"ok": False, "code": "bad_credentials", "error": "Invalid Instagram credentials"})
        return 0
    except Exception as e:
        _emit({"ok": False, "code": "unknown", "error": f"{type(e).__name__}: {e}"})
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
