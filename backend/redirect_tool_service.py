"""Redirect Tool — talks to redirect.api.beslist.nl and persists runs.

Key behavior:
- The redirect API's `url_redirect` table has a UNIQUE index across both fromUrl and
  toUrl, so a URL can't be both at once. Preflight rewrites each input row's `new`
  value to the terminal target if the URL is already a fromUrl in the DB (i.e.,
  "flattens" the chain client-side so the POST doesn't 500).
- URL variants (literal space, underscore, %20) are treated as equivalent for
  matching purposes — many real URLs exist under multiple forms.
- The homepage (`/`, empty, `/index`, `/index.html`) is hard-blocked as a fromUrl.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
from typing import Any

import requests

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

REDIRECT_API = "https://redirect.api.beslist.nl"
HTTP_TIMEOUT = 30
LIST_PAGE_SIZE = 50

DEFAULT_COUNTRY = "nl"
ALLOWED_STATUS_CODES = {301, 302, 303, 307, 308}
DEFAULT_STATUS_CODE = 301

# Paths that resolve to the homepage and must never be redirected
HOMEPAGE_PATHS = {"", "/", "/index", "/index.html"}


# ---------------------------------------------------------------------------
# URL handling
# ---------------------------------------------------------------------------

def strip_domain(url: str) -> str:
    """Return a /-prefixed path for any URL form (full URL, bare hostname, path)."""
    if not url:
        return ""
    s = url.strip()
    if s.startswith("/"):
        return s
    if "://" in s:
        parsed = urllib.parse.urlparse(s)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        return path
    # bare hostname like www.beslist.nl/foo
    if "/" in s and ("." in s.split("/")[0]):
        return "/" + s.split("/", 1)[1]
    return "/" + s


def normalize_path(path: str) -> str:
    """Decode %-escapes and strip whitespace, but keep spaces/underscores as-is."""
    if not path:
        return ""
    return urllib.parse.unquote(path.strip())


def equiv_key(path: str) -> str:
    """Canonical comparison key — treats space/underscore/%20 as identical."""
    return normalize_path(path).replace("_", " ")


def url_variants(path: str) -> list[str]:
    """Generate matching variants (space-form, underscore-form, %20-form)."""
    p = normalize_path(path)
    if not p:
        return []
    space = p.replace("_", " ")
    underscore = p.replace(" ", "_")
    percent = p.replace(" ", "%20")
    # Order matters: prefer the original-decoded form first so resolver hits the
    # exact stored value when possible.
    seen, out = set(), []
    for v in (p, space, underscore, percent):
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def is_homepage(path: str) -> bool:
    # Accept either path or full URL — strip domain first so callers can't bypass
    # the safety block by passing 'https://www.beslist.nl'.
    p = normalize_path(strip_domain(path))
    return p in HOMEPAGE_PATHS or p.rstrip("/") == ""


# ---------------------------------------------------------------------------
# Redirect API client
# ---------------------------------------------------------------------------

def _resolve_one(url: str, country: str = DEFAULT_COUNTRY) -> dict | None:
    try:
        r = requests.get(
            f"{REDIRECT_API}/api/redirect",
            params={"searchterm": url, "country": country},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("totalRecords", 0) > 0 and data.get("data"):
            return data["data"][0]
    except Exception as exc:
        logger.warning("resolver call failed for %s: %s", url, exc)
    return None


def check_url_is_fromUrl(path: str, country: str = DEFAULT_COUNTRY) -> dict | None:
    """Return {url, statusCode, matched_variant} if `path` is a fromUrl in the DB."""
    for variant in url_variants(path):
        hit = _resolve_one(variant, country)
        if hit:
            return {**hit, "matched_variant": variant}
    return None


def check_url_incoming(path: str, max_pages: int = 5) -> list[dict]:
    """Find redirects whose toUrl matches any variant of `path`."""
    variants = url_variants(path)
    if not variants:
        return []
    # Use the most-distinctive substring for urlContains — strip leading/trailing
    # slashes and pick a no-space variant to avoid query-string ambiguity.
    search = next((v for v in variants if " " not in v), variants[0]).strip("/")
    target_keys = {equiv_key(v) for v in variants}

    seen_ids: set[int] = set()
    matches: list[dict] = []
    for page in range(max_pages):
        try:
            r = requests.get(
                f"{REDIRECT_API}/api/redirects",
                params={
                    "limit": LIST_PAGE_SIZE,
                    "offset": page * LIST_PAGE_SIZE,
                    "urlContains": search,
                },
                timeout=HTTP_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json().get("data", [])
        except Exception as exc:
            logger.warning("incoming list call failed: %s", exc)
            break
        if not data:
            break
        for row in data:
            rid = row.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            if equiv_key(row.get("toUrl", "")) in target_keys:
                matches.append(row)
        if len(data) < LIST_PAGE_SIZE:
            break
    return matches


def post_redirect(from_url: str, to_url: str, country: str, status_code: int) -> tuple[int, Any]:
    r = requests.post(
        f"{REDIRECT_API}/api/redirect",
        json=[{
            "fromUrl": from_url,
            "toUrl": to_url,
            "country": country,
            "statusCode": status_code,
        }],
        timeout=HTTP_TIMEOUT,
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:500]}
    return r.status_code, body


# ---------------------------------------------------------------------------
# Preflight + submit
# ---------------------------------------------------------------------------

def preflight_rows(rows: list[dict]) -> dict:
    """For each row, normalize, flatten chains, mark skips. Pure read-only."""
    processed: list[dict] = []
    flattened = 0
    skipped_home = 0

    for raw in rows:
        old = strip_domain(str(raw.get("old", "")))
        new = strip_domain(str(raw.get("new", "")))
        country = (str(raw.get("country") or "").strip() or DEFAULT_COUNTRY).lower()
        if country not in {"nl", "be"}:
            country = DEFAULT_COUNTRY
        try:
            sc = int(str(raw.get("statuscode") or DEFAULT_STATUS_CODE).strip())
        except (ValueError, TypeError):
            sc = DEFAULT_STATUS_CODE
        if sc not in ALLOWED_STATUS_CODES:
            sc = DEFAULT_STATUS_CODE
        label = str(raw.get("label") or "").strip()

        item = {
            "input_old": old,
            "input_new": new,
            "final_new": new,
            "country": country,
            "statusCode": sc,
            "label": label,
            "skip_reason": None,
            "flatten_from": None,
        }

        if not old or not new:
            item["skip_reason"] = "missing old or new URL"
            processed.append(item)
            continue

        if is_homepage(old):
            item["skip_reason"] = "old URL is the homepage (safety block)"
            skipped_home += 1
            processed.append(item)
            continue

        # Flatten: if `new` is itself a fromUrl in the DB, swap to its target
        hit = check_url_is_fromUrl(new, country)
        if hit:
            item["final_new"] = hit["url"]
            item["flatten_from"] = new
            flattened += 1

        # Reject self-redirects after flatten (any variant of old == final_new)
        if equiv_key(item["final_new"]) == equiv_key(old):
            item["skip_reason"] = "would create self-redirect after flatten"
            processed.append(item)
            continue

        processed.append(item)

    return {
        "processed": processed,
        "stats": {
            "total": len(rows),
            "flattened": flattened,
            "skipped_home": skipped_home,
            "submittable": sum(1 for p in processed if not p["skip_reason"]),
        },
    }


def submit_rows(processed: list[dict]) -> dict:
    """POST one row at a time so we get per-row pass/fail."""
    success = 0
    failed = 0
    per_row: list[dict] = []

    for item in processed:
        if item.get("skip_reason"):
            per_row.append({**item, "status": "skipped", "api_response": None})
            continue
        try:
            code, body = post_redirect(
                item["input_old"], item["final_new"],
                item["country"], item["statusCode"],
            )
        except Exception as exc:
            failed += 1
            per_row.append({**item, "status": "fail", "api_response": {"error": str(exc)}})
            continue
        if 200 <= code < 300:
            success += 1
            per_row.append({**item, "status": "ok", "api_response": body})
        else:
            failed += 1
            per_row.append({**item, "status": "fail", "api_response": body})

    return {"success": success, "failed": failed, "per_row": per_row}


# ---------------------------------------------------------------------------
# Run persistence
# ---------------------------------------------------------------------------

def save_run(label: str, input_method: str, preflight: dict, result: dict) -> int:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO redirect_tool_runs
               (label, input_method, total_rows, flattened, skipped_home, success, failed, results)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            (
                label or None,
                input_method,
                preflight["stats"]["total"],
                preflight["stats"]["flattened"],
                preflight["stats"]["skipped_home"],
                result["success"],
                result["failed"],
                json.dumps(result["per_row"]),
            ),
        )
        new_id = cur.fetchone()["id"]
        conn.commit()
        return new_id
    finally:
        return_db_connection(conn)


def list_runs(limit: int = 100) -> list[dict]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, input_method, total_rows, flattened,
                      skipped_home, success, failed
               FROM redirect_tool_runs ORDER BY created_at DESC LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        for r in rows:
            r["created_at"] = r["created_at"].isoformat()
        return rows
    finally:
        return_db_connection(conn)


def get_run(run_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, input_method, total_rows, flattened,
                      skipped_home, success, failed, results
               FROM redirect_tool_runs WHERE id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            row["created_at"] = row["created_at"].isoformat()
        return row
    finally:
        return_db_connection(conn)


def delete_run(run_id: int) -> bool:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM redirect_tool_runs WHERE id = %s", (run_id,))
        deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    finally:
        return_db_connection(conn)
