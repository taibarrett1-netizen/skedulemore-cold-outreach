#!/usr/bin/env python3
"""
Login-less public follower scrape via Instagram web_profile_info + GraphQL (doc_id from env).
Uses a single sticky proxy for the whole run. Rotate ADMIN_LAB_IG_DOC_ID_FOLLOWERS from DevTools
(Network tab -> graphql/query -> doc_id) every few weeks when pagination breaks.

Bandwidth: keep --max_users modest; trial proxies (~100MB) burn quickly on retries.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Rotating desktop / mobile UAs (Instagram web)
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

IG_APP_ID = "936619743392459"
CHECKPOINT_EVERY = max(10, int(os.getenv("ADMIN_LAB_CHECKPOINT_EVERY", "50")))


def pick_ua() -> str:
    return random.choice(USER_AGENTS)


def base_headers() -> Dict[str, str]:
    return {
        "User-Agent": pick_ua(),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-IG-App-ID": IG_APP_ID,
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.instagram.com",
        "Referer": "https://www.instagram.com/",
    }


def walk_find_edges(obj: Any, depth: int = 0) -> Optional[List[Dict[str, Any]]]:
    if depth > 14:
        return None
    if isinstance(obj, dict):
        edges = obj.get("edges")
        if isinstance(edges, list) and edges:
            first = edges[0]
            if isinstance(first, dict) and "node" in first:
                node = first.get("node")
                if isinstance(node, dict) and "username" in node:
                    return edges
        for v in obj.values():
            r = walk_find_edges(v, depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = walk_find_edges(it, depth + 1)
            if r is not None:
                return r
    return None


def walk_find_page_info(obj: Any, depth: int = 0) -> Optional[Dict[str, Any]]:
    if depth > 14:
        return None
    if isinstance(obj, dict):
        if "page_info" in obj and isinstance(obj["page_info"], dict):
            return obj["page_info"]
        for v in obj.values():
            r = walk_find_page_info(v, depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = walk_find_page_info(it, depth + 1)
            if r is not None:
                return r
    return None


async def fetch_profile_user_id(client: httpx.AsyncClient, username: str) -> str:
    urls = [
        f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}",
        f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}",
    ]
    last_err: Optional[Exception] = None
    for url in urls:
        for attempt in range(4):
            try:
                headers = base_headers()
                headers["Referer"] = f"https://www.instagram.com/{username}/"
                r = await client.get(url, headers=headers, timeout=45.0)
                if r.status_code == 429:
                    last_err = RuntimeError(f"HTTP 429 from web_profile_info ({url})")
                    await asyncio.sleep(2 ** attempt + random.uniform(1, 4))
                    continue
                if r.status_code >= 400:
                    body_preview = r.text[:240].replace("\n", " ")
                    raise RuntimeError(
                        f"HTTP {r.status_code} from web_profile_info ({url}) body={body_preview}"
                    )
                data = r.json()
                uid = (
                    data.get("data", {})
                    .get("user", {})
                    .get("id")
                )
                if not uid:
                    raise RuntimeError(
                        f"Unexpected web_profile_info shape from {url}: {json.dumps(data)[:500]}"
                    )
                return str(uid)
            except Exception as e:
                last_err = e
                await asyncio.sleep(1.5 ** attempt + random.uniform(0.2, 1))
    raise RuntimeError(f"web_profile_info failed: {last_err}")


async def graphql_followers_page(
    client: httpx.AsyncClient,
    doc_id: str,
    user_id: str,
    first: int,
    after: Optional[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    variables: Dict[str, Any] = {"id": user_id, "first": first}
    if after:
        variables["after"] = after

    body = {
        "doc_id": doc_id,
        "variables": json.dumps(variables, separators=(",", ":")),
    }
    url = "https://www.instagram.com/graphql/query/"
    last_err: Optional[Exception] = None
    for attempt in range(5):
        try:
            headers = base_headers()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            r = await client.post(url, data=body, headers=headers, timeout=60.0)
            if r.status_code == 429:
                wait = min(90.0, 2 ** attempt + random.uniform(2, 8))
                sys.stderr.write(f"[admin-lab] rate limited, sleeping {wait:.1f}s\n")
                await asyncio.sleep(wait)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text[:500]}")
            data = r.json()
            if "errors" in data and data["errors"]:
                raise RuntimeError(f"GraphQL errors: {data['errors'][:3]}")
            edges = walk_find_edges(data)
            if edges is None:
                raise RuntimeError(
                    "Could not find follower edges in GraphQL response. "
                    "Update ADMIN_LAB_IG_DOC_ID_FOLLOWERS from DevTools (followers query)."
                )
            page_info = walk_find_page_info(data) or {}
            return edges, page_info
        except Exception as e:
            last_err = e
            await asyncio.sleep(1.2 ** attempt + random.uniform(0.3, 1.2))
    raise RuntimeError(f"graphql followers failed: {last_err}")


def append_rows_csv(path: str, rows: List[Dict[str, str]], write_header: bool) -> None:
    fieldnames = ["username", "full_name", "id", "is_private"]
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def node_to_row(node: Dict[str, Any]) -> Dict[str, str]:
    return {
        "username": str(node.get("username") or ""),
        "full_name": str(node.get("full_name") or ""),
        "id": str(node.get("id") or ""),
        "is_private": str(node.get("is_private", "")).lower()
        if node.get("is_private") is not None
        else "",
    }


async def run_scrape(username: str, proxy: str, max_users: int, output: str, user_id: Optional[str] = None) -> int:
    doc_id = (os.getenv("ADMIN_LAB_IG_DOC_ID_FOLLOWERS") or "").strip()
    if not doc_id:
        print("ADMIN_LAB_IG_DOC_ID_FOLLOWERS is required", file=sys.stderr)
        sys.exit(2)

    first = min(40, max(12, int(os.getenv("ADMIN_LAB_GRAPHQL_FIRST", "40"))))

    transport = httpx.AsyncHTTPTransport(retries=1)
    async with httpx.AsyncClient(
        proxy=proxy,
        transport=transport,
        follow_redirects=True,
        timeout=httpx.Timeout(90.0),
    ) as client:
        uid = user_id.strip() if isinstance(user_id, str) and user_id.strip() else None
        if not uid:
            uid = await fetch_profile_user_id(client, username)
        sys.stderr.write(f"[admin-lab] Resolved @{username} -> id={uid}\n")

        after: Optional[str] = None
        collected = 0
        buffer: List[Dict[str, str]] = []
        header_written = bool(os.path.exists(output) and os.path.getsize(output) > 0)

        while collected < max_users:
            edges, page_info = await graphql_followers_page(client, doc_id, uid, first, after)
            await asyncio.sleep(random.uniform(0.8, 2.2))

            for edge in edges:
                if collected >= max_users:
                    break
                node = edge.get("node") if isinstance(edge, dict) else None
                if not isinstance(node, dict):
                    continue
                buffer.append(node_to_row(node))
                collected += 1
                if len(buffer) >= CHECKPOINT_EVERY:
                    append_rows_csv(output, buffer, write_header=not header_written)
                    header_written = True
                    buffer.clear()
                    sys.stderr.write(f"[admin-lab] checkpoint {collected} rows\n")

            if buffer:
                append_rows_csv(output, buffer, write_header=not header_written)
                header_written = True
                buffer.clear()

            has_next = bool(page_info.get("has_next_page"))
            after = page_info.get("end_cursor")
            if not has_next or not after:
                break

        if buffer:
            append_rows_csv(output, buffer, write_header=not header_written)
            header_written = True

    return collected


def main() -> None:
    p = argparse.ArgumentParser(description="Instagram public followers scrape (login-less, lab)")
    p.add_argument("--username", required=True)
    p.add_argument("--user_id", required=False, help="Optional: bypass web_profile_info lookup (use numeric IG user id)")
    p.add_argument("--proxy", required=True, help="HTTP proxy URL, e.g. http://user:pass@host:port")
    p.add_argument("--max_users", type=int, default=500)
    p.add_argument("--output", required=True, help="Output CSV path")
    args = p.parse_args()

    t0 = time.time()
    n = asyncio.run(
        run_scrape(
            args.username.strip().lstrip("@"),
            args.proxy.strip(),
            int(args.max_users),
            args.output,
            args.user_id,
        )
    )
    sys.stderr.write(f"[admin-lab] Done: {n} users in {time.time() - t0:.1f}s -> {args.output}\n")
    print(f"ROWCOUNT={n}")


if __name__ == "__main__":
    main()
