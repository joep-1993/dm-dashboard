"""
Canonical URL Generator Service

Generates canonical URLs by applying transformation rules to URLs fetched from Redshift.
Replaces GA4 data source with Redshift visits data.

Transformation types:
- CAT-CAT: Replace category slug with another
- FACET-FACET: Replace facet value with another
- CAT+FACET: Change category for URLs containing specific facet (keep facet)
- CAT+FACET1: Change category for URLs containing specific facet (remove facet)
- BUCKET+BUCKET: Replace bucket value with another
- REMOVEBUCKET: Remove bucket from URL
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from psycopg2.extras import Json
from backend.database import (
    get_redshift_connection,
    return_redshift_connection,
    get_db_connection,
    return_db_connection,
)

# Compiled once (was re.search'd per URL in _extract_maincat).
_PRODUCTS_RE = re.compile(r'/products/([^/]+)/')


def _like_escape(s: str) -> str:
    """Escape LIKE wildcards in a user pattern so a literal % or _ in a URL
    fragment isn't treated as a wildcard. Uses '!' as the escape char (paired
    with ``ESCAPE '!'``) — a backslash would need doubling and collides with
    Redshift string-literal escaping. Escape the escape-char first."""
    return s.replace("!", "!!").replace("%", "!%").replace("_", "!_")


def _validate_yyyymmdd(value: str, label: str) -> None:
    """Raise ValueError with a clear message if value isn't a YYYYMMDD date."""
    if not re.fullmatch(r"\d{8}", str(value or "")):
        raise ValueError(f"{label} must be an 8-digit YYYYMMDD date, got {value!r}")
    try:
        datetime.strptime(str(value), "%Y%m%d")
    except ValueError:
        raise ValueError(f"{label} is not a valid calendar date: {value!r}")


@dataclass
class CatCatRule:
    """Category to category replacement rule"""
    old_cat: str
    new_cat: str
    new_maincat: Optional[str] = None


@dataclass
class FacetFacetRule:
    """Facet to facet replacement rule"""
    old_facet: str
    new_facet: str
    cat: Optional[str] = None  # Optional category filter


@dataclass
class CatFacetRule:
    """Category + Facet rule (keep facet)"""
    facet: str
    canon_cat: str
    cat: Optional[str] = None


@dataclass
class CatFacetRemoveRule:
    """Category + Facet rule (remove facet)"""
    facet: str
    canon_cat: str
    cat: Optional[str] = None


@dataclass
class BucketRule:
    """Bucket to bucket replacement rule"""
    old_bucket: str
    new_bucket: str


@dataclass
class RemoveBucketRule:
    """Remove bucket rule"""
    bucket: str
    cat: Optional[str] = None


@dataclass
class TransformationRules:
    """All transformation rules"""
    cat_cat: List[CatCatRule]
    facet_facet: List[FacetFacetRule]
    cat_facet: List[CatFacetRule]
    cat_facet_remove: List[CatFacetRemoveRule]
    bucket_bucket: List[BucketRule]
    remove_bucket: List[RemoveBucketRule]


def fetch_urls_from_redshift(
    contains: Optional[str] = None,
    not_contains: Optional[List[str]] = None,
    contains_all: Optional[List[str]] = None,
    start_date: str = "20240101",
    end_date: str = "20261231",
    limit: int = 10000
) -> List[Dict]:
    """
    Fetch URLs from Redshift based on filter criteria.

    Args:
        contains: URL must contain this string
        not_contains: URL must NOT contain these strings
        contains_all: URL must contain ALL of these strings
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        limit: Maximum number of URLs to return

    Returns:
        List of dicts with 'url' and 'visits' keys
    """
    # #6: validate dates up front (was `int(date)` mid-query → opaque 500 on a
    # non-numeric value; also catches an inverted range).
    _validate_yyyymmdd(start_date, "start_date")
    _validate_yyyymmdd(end_date, "end_date")
    if int(start_date) > int(end_date):
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    conn = None
    cur = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Build the query
        query = """
            SELECT
                SPLIT_PART(dv.url, '?', 1) as url,
                COUNT(*) as visits
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
                ON fcv.dim_visit_key = dv.dim_visit_key
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN %s AND %s
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url LIKE '%%/products/%%'
              AND dv.url NOT LIKE '%%/r/%%'
              AND dv.url NOT LIKE '%%/l/%%'
              AND dv.url NOT LIKE '%%/page_%%'
              AND dv.url NOT LIKE '%%#%%'
              AND dv.url NOT LIKE '%%device=%%'
              AND dv.url NOT LIKE '%%sortby=%%'
              AND dv.url NOT LIKE '%%shop_id=%%'
              AND dv.url NOT LIKE '%%/sitemap/%%'
              AND dv.url NOT LIKE '%%/filters/%%'
              AND dv.url NOT LIKE '%%+%%'
        """

        params = [int(start_date), int(end_date)]

        # #4: escape LIKE wildcards in user patterns and skip empty entries (an
        # empty pattern became `LIKE '%%'` = match-everything).
        if contains:
            query += " AND dv.url LIKE %s ESCAPE '!'"
            params.append(f"%{_like_escape(contains)}%")

        if contains_all:
            for ca in contains_all:
                if not ca:
                    continue
                query += " AND dv.url LIKE %s ESCAPE '!'"
                params.append(f"%{_like_escape(ca)}%")

        if not_contains:
            for nc in not_contains:
                if not nc:
                    continue
                query += " AND dv.url NOT LIKE %s ESCAPE '!'"
                params.append(f"%{_like_escape(nc)}%")

        query += """
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT %s
        """
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        # Pools use RealDictCursor, so rows are dict-like.
        return [{"url": row["url"], "visits": row["visits"]} for row in rows]

    except Exception as e:
        print(f"[ERROR] Failed to fetch URLs from Redshift: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_redshift_connection(conn)


def fetch_urls_for_rules(rules: TransformationRules, start_date: str, end_date: str) -> List[str]:
    """
    Fetch all URLs from Redshift that match any of the transformation rules.
    """
    all_urls = set()

    # Fetch URLs for CAT-CAT rules
    for rule in rules.cat_cat:
        urls = fetch_urls_from_redshift(
            contains=f"{rule.old_cat}c/",
            start_date=start_date,
            end_date=end_date
        )
        for u in urls:
            all_urls.add(u["url"])

    # Fetch URLs for FACET+FACET rules (URLs must contain BOTH old and new facet)
    for rule in rules.facet_facet:
        facet_filters = [rule.old_facet, rule.new_facet]
        if rule.cat:
            facet_filters.append(rule.cat)
        urls = fetch_urls_from_redshift(
            contains_all=facet_filters,
            start_date=start_date,
            end_date=end_date
        )
        for u in urls:
            all_urls.add(u["url"])

    # Fetch URLs for CAT+FACET rules
    for rule in rules.cat_facet:
        if rule.cat:
            # Fetch URLs containing the facet, then filter by category
            urls = fetch_urls_from_redshift(
                contains=rule.facet,
                start_date=start_date,
                end_date=end_date
            )
            # Only include URLs that also contain the category filter
            for u in urls:
                if rule.cat in u["url"]:
                    all_urls.add(u["url"])
        else:
            urls = fetch_urls_from_redshift(
                contains=rule.facet,
                not_contains=[f"{rule.canon_cat}c/"],
                start_date=start_date,
                end_date=end_date
            )
            for u in urls:
                all_urls.add(u["url"])

    # Fetch URLs for CAT+FACET1 rules (remove facet)
    for rule in rules.cat_facet_remove:
        if rule.cat:
            # Fetch URLs containing the facet, then filter by category
            urls = fetch_urls_from_redshift(
                contains=rule.facet,
                start_date=start_date,
                end_date=end_date
            )
            # Only include URLs that also contain the category filter
            for u in urls:
                if rule.cat in u["url"]:
                    all_urls.add(u["url"])
        else:
            urls = fetch_urls_from_redshift(
                contains=rule.facet,
                not_contains=[f"{rule.canon_cat}c/"],
                start_date=start_date,
                end_date=end_date
            )
            for u in urls:
                all_urls.add(u["url"])

    # Fetch URLs for BUCKET+BUCKET rules
    for rule in rules.bucket_bucket:
        urls = fetch_urls_from_redshift(
            contains=rule.old_bucket,
            start_date=start_date,
            end_date=end_date
        )
        for u in urls:
            all_urls.add(u["url"])

    # Fetch URLs for REMOVEBUCKET rules
    for rule in rules.remove_bucket:
        urls = fetch_urls_from_redshift(
            contains=rule.bucket,
            start_date=start_date,
            end_date=end_date
        )
        for u in urls:
            all_urls.add(u["url"])

    return list(all_urls)


def _normalize_path(path: str) -> str:
    """Ensure path starts and ends with /"""
    path = path.strip("/")
    return f"/{path}/"


def _extract_maincat(url: str) -> str:
    """Extract maincat from URL like /products/maincat_123/..."""
    match = _PRODUCTS_RE.search(url)
    if match:
        return f"/{match.group(1)}/"
    return ""


def _extract_cat(url: str, maincat: str) -> str:
    """Extract category slug after maincat"""
    if maincat and maincat in url:
        after_maincat = url.split(maincat)[1]
        parts = after_maincat.split("/")
        if parts and parts[0] and parts[0] != "c":
            return f"/{parts[0]}/"
    return ""


def _sort_facets(url: str) -> str:
    """Normalize the /c/ facet group: sort multi-facet groups alphabetically and
    drop the trailing slash after /c/ (the project canonical rule). Previously a
    SINGLE-facet URL was returned untouched, so it kept its trailing slash while
    multi-facet URLs lost theirs — inconsistent canonical output."""
    if "/c/" not in url:
        return url

    parts = url.split("/c/")
    if len(parts) < 2:
        return url

    base = parts[0]
    facet_str = parts[1].rstrip("/")

    if "~~" in facet_str:
        facets = [f.lower() for f in facet_str.split("~~") if f]
        # Sort by facet name only (part before ~), not full string,
        # because ~ (ASCII 126) > letters, causing e.g. kleurtint~... < kleur~...
        facets.sort(key=lambda f: f.split("~")[0] if "~" in f else f)
        facet_str = "~~".join(facets)
    else:
        facet_str = facet_str.lower()

    return f"{base}/c/{facet_str}"


def _contains_any(url: str, patterns: List[str]) -> bool:
    """Check if URL contains any of the patterns"""
    for pattern in patterns:
        if pattern and pattern in url:
            return True
    return False


def _determine_tasks(url: str, rules: TransformationRules) -> List[str]:
    """Determine which transformation tasks apply to this URL"""
    tasks = []

    # Check CAT-CAT
    old_cats = [r.old_cat for r in rules.cat_cat]
    if _contains_any(url, old_cats):
        tasks.append("CAT-CAT")

    # Check FACET+FACET (URL must contain BOTH facets, and the old one in the
    # ~~-delimited form that _apply_facet_facet actually removes — matching its
    # predicate so the task isn't reported when application would no-op).
    for rule in rules.facet_facet:
        old_delimited = (f"{rule.old_facet}~~" in url or f"~~{rule.old_facet}" in url)
        if old_delimited and rule.new_facet in url:
            if not rule.cat or rule.cat in url:
                tasks.append("FACET-FACET")
                break

    # Check CAT+FACET (respecting per-rule category filter)
    for rule in rules.cat_facet:
        if rule.facet in url:
            # If rule has category filter, URL must contain it
            if not rule.cat or rule.cat in url:
                tasks.append("CAT+FACET")
                break

    # Check CAT+FACET1 (respecting per-rule category filter)
    for rule in rules.cat_facet_remove:
        if rule.facet in url:
            # If rule has category filter, URL must contain it
            if not rule.cat or rule.cat in url:
                tasks.append("CAT+FACET1")
                break

    # Check BUCKET+BUCKET
    old_buckets = [r.old_bucket for r in rules.bucket_bucket]
    if _contains_any(url, old_buckets):
        tasks.append("BUCKET+BUCKET")

    # Check REMOVEBUCKET
    remove_buckets = [r.bucket for r in rules.remove_bucket]
    if _contains_any(url, remove_buckets):
        tasks.append("REMOVEBUCKET")

    return tasks


def _apply_cat_cat(url: str, rules: List[CatCatRule]) -> str:
    """Apply category-to-category replacement"""
    maincat = _extract_maincat(url)

    for rule in rules:
        old_cat = _normalize_path(rule.old_cat)
        new_cat = _normalize_path(rule.new_cat)

        if old_cat in url:
            url = url.replace(old_cat, new_cat)

            # Also replace maincat if specified
            if rule.new_maincat and maincat:
                new_maincat = _normalize_path(rule.new_maincat)
                if new_maincat != new_cat:
                    url = url.replace(maincat, new_maincat)

    return url


def _apply_facet_facet(url: str, rules: List[FacetFacetRule]) -> str:
    """Apply facet+facet canonicalization: remove old facet from URLs that have both old and new facet."""
    for rule in rules:
        # Skip if category filter is specified and URL doesn't contain it
        if rule.cat and rule.cat not in url:
            continue

        # Only apply when URL contains BOTH facets
        if rule.old_facet not in url or rule.new_facet not in url:
            continue

        # Remove the old facet from the URL (keep the new facet)
        # Handle three positions: start (facet~~...), middle (~~facet~~), end (~~facet)
        if f"{rule.old_facet}~~" in url:
            # Old facet is at start or middle: remove it and its trailing ~~
            url = url.replace(f"{rule.old_facet}~~", "")
        elif f"~~{rule.old_facet}" in url:
            # Old facet is at end: remove the leading ~~ and the facet
            url = url.replace(f"~~{rule.old_facet}", "")

    return url


def _dedupe_maincat(url: str, maincat: str) -> str:
    """Collapse an accidental doubled maincat (e.g. /mode/mode/) back to one."""
    if maincat:
        double_main = (maincat + maincat).replace("//", "/")
        if double_main in url:
            url = url.replace(double_main, maincat)
    return url


def _apply_cat_facet(url: str, rules: List[CatFacetRule]) -> str:
    """Apply category+facet rule (keep facet)"""
    maincat = _extract_maincat(url)
    cat = _extract_cat(url, maincat)

    for rule in rules:
        if rule.facet not in url:
            continue

        # Skip if category filter is specified and URL doesn't contain it
        if rule.cat and rule.cat not in url:
            continue

        canon_cat = _normalize_path(rule.canon_cat)

        # _extract_cat never returns "/c/" (it excludes the "c" segment), so the
        # old `if cat == "/c/"` branch was dead and identical to this one.
        if cat:
            url = url.replace(cat, canon_cat)

    url = _dedupe_maincat(url, maincat)
    return url


def _apply_cat_facet_remove(url: str, rules: List[CatFacetRemoveRule]) -> str:
    """Apply category+facet rule (remove facet)"""
    maincat = _extract_maincat(url)
    cat = _extract_cat(url, maincat)

    for rule in rules:
        if rule.facet not in url:
            continue

        # Skip if category filter is specified and URL doesn't contain it
        if rule.cat and rule.cat not in url:
            continue

        canon_cat = _normalize_path(rule.canon_cat)

        # Replace category (cat is never "/c/" — see _apply_cat_facet).
        if cat:
            url = url.replace(cat, canon_cat)

        # Remove the full facet (facet_name~value) using regex. The value is
        # usually a numeric id ("type_spelcomputer~480840") but allow a named
        # value too ([^~/]+) so a non-numeric value isn't left orphaned.
        facet = rule.facet
        facet_pattern = re.escape(facet) + r'(?:~[^~/]+)?'

        # Try removal patterns in order of specificity
        regex_patterns = [
            (rf'~~{facet_pattern}~~', '~~'),             # facet in the middle
            (rf'{facet_pattern}~~', ''),                 # facet at the start
            (rf'~~{facet_pattern}', ''),                 # facet at the end
            (rf'/c/{facet_pattern}(?:/|$)', ''),         # single facet after /c/ (with or without trailing slash)
        ]

        for regex, replacement in regex_patterns:
            # IGNORECASE because facet keys/values in the URL may carry mixed
            # casing from the source system; the canonicalized output is
            # lowercased later.
            new_url = re.sub(regex, replacement, url, flags=re.IGNORECASE)
            if new_url != url:
                url = new_url
                break

    url = _dedupe_maincat(url, maincat)
    return url


def _apply_bucket_bucket(url: str, rules: List[BucketRule]) -> str:
    """Apply bucket-to-bucket replacement"""
    for rule in rules:
        old = rule.old_bucket
        new = rule.new_bucket

        pattern_between = f"{old}~~"
        pattern_end = f"~~{old}"

        if pattern_between in url:
            url = url.replace(pattern_between, f"{new}~~" if new else "")
        elif pattern_end in url:
            url = url.replace(pattern_end, f"~~{new}" if new else "")
        elif old in url and "~~" not in url:
            url = url.replace(f"/c/{old}", f"/c/{new}" if new else "")

    return url


def _split_letters_numbers(s: str) -> str:
    """Split letters and numbers with tilde: 'abc123' -> 'abc~123'"""
    return re.sub(r'([a-zA-Z]+)(\d+)', r'\1~\2', s)


def _apply_remove_bucket(url: str, rules: List[RemoveBucketRule]) -> str:
    """Apply remove bucket transformation"""
    for rule in rules:
        bucket = rule.bucket

        if bucket not in url:
            continue

        # Try to extract full facet with number if not already has tilde
        if "~" not in bucket:
            match = re.search(rf'{re.escape(bucket)}~(\d+)', url)
            if match:
                bucket = match.group(0)
            else:
                bucket = _split_letters_numbers(bucket)

        pattern_between = f"{bucket}~~"
        pattern_end = f"~~{bucket}"

        if pattern_between in url:
            url = url.replace(pattern_between, "")
        elif pattern_end in url:
            url = url.replace(pattern_end, "")
        elif bucket in url and "~~" not in url:
            url = url.replace(f"/c/{bucket}", "/")

    return url


def transform_url(url: str, rules: TransformationRules) -> str:
    """
    Apply all applicable transformation rules to a URL.

    Args:
        url: The original URL
        rules: All transformation rules

    Returns:
        The transformed canonical URL
    """
    # Normalize URL ending
    if "/r/" in url and "/c/" not in url:
        if not url.endswith("/"):
            url = url + "/"

    # Determine which tasks apply
    tasks = _determine_tasks(url, rules)

    if not tasks:
        return url

    # Apply transformations in order
    for task in tasks:
        if task == "FACET-FACET":
            url = _apply_facet_facet(url, rules.facet_facet)
        elif task == "CAT-CAT":
            url = _apply_cat_cat(url, rules.cat_cat)
        elif task == "CAT+FACET":
            url = _apply_cat_facet(url, rules.cat_facet)
        elif task == "CAT+FACET1":
            url = _apply_cat_facet_remove(url, rules.cat_facet_remove)
        elif task == "BUCKET+BUCKET":
            url = _apply_bucket_bucket(url, rules.bucket_bucket)
        elif task == "REMOVEBUCKET":
            url = _apply_remove_bucket(url, rules.remove_bucket)

        # Ensure /products/ prefix
        if "/products/" not in url:
            url = "/products" + url

    # Clean up URL
    url = url.lower()
    url = url.replace("/products/products/", "/products/")
    # Remove duplicate slashes but preserve protocol (https://)
    url = re.sub(r'(?<!:)//+', '/', url)

    # Sort facets alphabetically
    url = _sort_facets(url)

    # Ensure trailing slash for URLs without facets (no /c/ and no ~)
    if '/c/' not in url and '~' not in url and not url.endswith('/'):
        url = url + '/'

    return url


def generate_canonicals(
    urls: List[str],
    rules: TransformationRules
) -> List[Dict[str, str]]:
    """
    Generate canonical URLs for a list of URLs.

    Args:
        urls: List of original URLs
        rules: Transformation rules

    Returns:
        List of dicts with 'original' and 'canonical' keys
    """
    results = []

    for url in urls:
        canonical = transform_url(url, rules)
        results.append({
            "original": url,
            "canonical": canonical
        })

    return results


def parse_rules_from_json(data: dict) -> TransformationRules:
    """
    Parse transformation rules from JSON input.

    Expected format:
    {
        "cat_cat": [{"old_cat": "...", "new_cat": "...", "new_maincat": "..."}],
        "facet_facet": [{"old_facet": "...", "new_facet": "...", "cat": "..."}],
        "cat_facet": [{"facet": "...", "canon_cat": "...", "cat": "..."}],
        "cat_facet_remove": [{"facet": "...", "canon_cat": "...", "cat": "..."}],
        "bucket_bucket": [{"old_bucket": "...", "new_bucket": "..."}],
        "remove_bucket": [{"bucket": "...", "cat": "..."}]
    }
    """
    return TransformationRules(
        cat_cat=[CatCatRule(**r) for r in data.get("cat_cat", []) if r.get("old_cat") and r.get("new_cat")],
        facet_facet=[FacetFacetRule(**r) for r in data.get("facet_facet", []) if r.get("old_facet") and r.get("new_facet")],
        cat_facet=[CatFacetRule(**r) for r in data.get("cat_facet", []) if r.get("facet") and r.get("canon_cat")],
        cat_facet_remove=[CatFacetRemoveRule(**r) for r in data.get("cat_facet_remove", []) if r.get("facet") and r.get("canon_cat")],
        bucket_bucket=[BucketRule(**r) for r in data.get("bucket_bucket", []) if r.get("old_bucket")],
        remove_bucket=[RemoveBucketRule(**r) for r in data.get("remove_bucket", []) if r.get("bucket")]
    )


# =============================================================================
# Run persistence — saved canonical generations survive page refreshes
# (mirrors the Redirect tool's run history; lazy CREATE TABLE so no migration
#  or backend restart is needed to start saving).
# =============================================================================

_table_ensured = False


def _ensure_canonical_runs_table() -> None:
    # Run the DDL once per process instead of on every runs request.
    global _table_ensured
    if _table_ensured:
        return
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS canonical_runs (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                label TEXT,
                total INTEGER NOT NULL DEFAULT 0,
                changed INTEGER NOT NULL DEFAULT 0,
                rules JSONB,
                results JSONB
            )
            """
        )
        conn.commit()
        _table_ensured = True
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        return_db_connection(conn)


def save_canonical_run(label: Optional[str], rules: dict, results: List[Dict],
                       total: int) -> int:
    """Persist one canonical generation. `results` is the list of changed
    {original, canonical} pairs; `total` is the count of URLs processed."""
    _ensure_canonical_runs_table()
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO canonical_runs (label, total, changed, rules, results)
               VALUES (%s, %s, %s, %s, %s) RETURNING id""",
            (label or None, total, len(results), Json(rules), Json(results)),
        )
        new_id = cur.fetchone()["id"]
        conn.commit()
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        return_db_connection(conn)


def list_canonical_runs(limit: int = 100) -> List[Dict]:
    _ensure_canonical_runs_table()
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, total, changed
               FROM canonical_runs ORDER BY created_at DESC LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        for r in rows:
            r["created_at"] = r["created_at"].isoformat()
        return rows
    finally:
        if cur:
            cur.close()
        conn.rollback()  # end read txn before returning to pool
        return_db_connection(conn)


def get_canonical_run(run_id: int) -> Optional[Dict]:
    _ensure_canonical_runs_table()
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, total, changed, rules, results
               FROM canonical_runs WHERE id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            row["created_at"] = row["created_at"].isoformat()
        return row
    finally:
        if cur:
            cur.close()
        conn.rollback()  # end read txn before returning to pool
        return_db_connection(conn)


def delete_canonical_run(run_id: int) -> bool:
    _ensure_canonical_runs_table()
    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM canonical_runs WHERE id = %s", (run_id,))
        deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        return_db_connection(conn)
