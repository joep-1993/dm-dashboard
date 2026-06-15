"""
SEO Rulings Service

Runs a small fixed set of sanity checks against live beslist.nl category pages
and the unique-titles database, then posts a summary to Slack.

Checks:
  1. No-script categories      — every sampled category renders the
                                 "Kies categorie" noScript header
  2. No-script facet-links     — sampled category/facet combos where the facet
                                 has seoPriority=true render a noScript header
                                 carrying the facet name
  3. Basement links            — sampled category pages have a basement-link
                                 group AND at least one link inside it
  4. Title variables           — !!DISCOUNT!! / !!NR!! / !!JAAR!! placeholders
                                 stored in pa.unique_titles_content are
                                 properly substituted on the rendered page
  5. XML-Sitemaps             — landing (PLP) + browse sitemap XML files are
                                 reachable (200)
  6. HTML-Sitemaps             — HTML sitemap pages are reachable (200) and
                                 list at least one item
"""
import csv
import json
import logging
import os
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

USER_AGENT = "Beslist script voor SEO"
SITE_BASE = "https://www.beslist.nl"
TAX_BASE = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
TAX_HEADERS = {"X-User-Name": "SEO_JOEP", "Accept": "application/json"}
TIMEOUT = 30
FACET_LOOKUP_MAX_TRIES = 60

MAINCAT_CSV = Path(__file__).parent / "maincat_mapping.csv"
CAT_URLS_CSV = Path(__file__).parent / "data" / "cat_urls.csv"

# XML sitemap availability check — a fixed set of landing (PLP) and browse
# sitemap URLs that must each return HTTP 200 with non-empty XML.
SITEMAP_XML_URLS = [
    f"{SITE_BASE}/sitemapxml/nl/current/sitemap-landing-cs-elektronica.xml",
    f"{SITE_BASE}/sitemapxml/nl/current/sitemap-landing-cs-mode.xml",
    f"{SITE_BASE}/sitemapxml/nl/current/sitemap-browse-cs-huis_tuin.xml",
    f"{SITE_BASE}/sitemapxml/nl/current/sitemap-browse-cs-meubilair.xml",
]

# HTML sitemap pages must be reachable (200) AND list at least one item.
HTML_SITEMAP_URLS = [
    f"{SITE_BASE}/sitemap/schoenen/schoenen_430884/",
    f"{SITE_BASE}/sitemap/parfum_aftershave/",
]
# An item is a <li class="sitemap__item--XXXXX"> — the suffix after "--" is a
# per-build CSS-module hash that differs between pages (e.g. R6p3e on
# parfum_aftershave, xkQIJ on schoenen), so we match the stable prefix.
SITEMAP_ITEM_CLASS = "sitemap__item--"

NOSCRIPT_TITLE_CLASS = "noScript__title--LfAWg"
BASEMENT_GROUP_CLASS = "basementlinks__group--igXdw"
BASEMENT_LINK_CLASS = "basementlinks__link--2awhY"

_SESSION = requests.Session()

# Per-run cache of taxv2 /api/Categories/{id}.isEnabled lookups.
_ACTIVE_CACHE: Dict[str, bool] = {}

# Per-run cache of (html, status) for category URLs so the sampler's 404 check
# and the check phase don't refetch the same page twice.
_HTML_CACHE: Dict[str, Tuple[Optional[str], int]] = {}


def _clear_run_caches() -> None:
    _ACTIVE_CACHE.clear()
    _HTML_CACHE.clear()


def _is_category_active(cat_id: str) -> bool:
    """Return True when taxv2 reports isEnabled=true for this category id.

    Cached per-process; cache miss costs one HTTP roundtrip. On any error
    (404, network blip, malformed response) returns False so the caller
    skips this category rather than crashing the run.
    """
    if cat_id in _ACTIVE_CACHE:
        return _ACTIVE_CACHE[cat_id]
    try:
        r = _SESSION.get(
            f"{TAX_BASE}/api/Categories/{cat_id}",
            params={"locale": "nl-NL", "includeSubCategories": "false", "includeFacets": "false"},
            headers=TAX_HEADERS,
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            _ACTIVE_CACHE[cat_id] = False
            return False
        active = bool(r.json().get("isEnabled", False))
    except Exception as e:
        logger.warning(f"[SEO_RULINGS] isEnabled lookup failed for cat {cat_id}: {e}")
        active = False
    _ACTIVE_CACHE[cat_id] = active
    return active


# ---------------------------------------------------------------------------
# Category sampling — uses the pre-built taxv2 snapshots in
# backend/maincat_mapping.csv + backend/data/cat_urls.csv (kept in sync with
# the Taxonomy API). Each candidate is verified live against the taxv2
# isEnabled flag so disabled / disabled-pending categories are skipped.
# ---------------------------------------------------------------------------
def _load_maincats() -> List[Dict]:
    rows: List[Dict] = []
    if not MAINCAT_CSV.exists():
        return rows
    with open(MAINCAT_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            slug = (row.get("maincat_url") or "").strip("/").lower()
            if not slug:
                continue
            rows.append({
                "name": (row.get("maincat") or "").strip(),
                "slug": slug,
                "maincat_url": row.get("maincat_url", ""),
                "cat_id": (row.get("maincat_id") or "").strip(),
            })
    return rows


def _load_cat_urls() -> List[Dict]:
    rows: List[Dict] = []
    if not CAT_URLS_CSV.exists():
        return rows
    with open(CAT_URLS_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter=";"):
            url_name = (row.get("url_name") or "").strip()
            cat_id = (row.get("cat_id") or "").strip()
            if not url_name or not cat_id:
                continue
            # Depth = number of numeric ids in the slug (e.g. /a_1_b_2/ -> 2)
            id_count = len(re.findall(r"_(\d+)(?=_|$)", url_name.strip("/")))
            rows.append({
                "maincat": (row.get("maincat") or "").strip(),
                "deepest_cat": (row.get("deepest_cat") or "").strip(),
                "url_name": url_name,
                "cat_id": cat_id,
                "depth": id_count,
            })
    return rows


SAMPLE_MAX_TRIES = 50


def _pick_one_live(
    pool: List[Dict],
    build_url,
    cat_id_key: str = "cat_id",
) -> Optional[Tuple[Dict, str]]:
    """Shuffle the pool and return the first (row, url) where the category
    is isEnabled=true in taxv2 AND the URL doesn't return 404. Caches the
    fetched HTML so the check phase doesn't refetch. Tries up to
    SAMPLE_MAX_TRIES candidates from the shuffled pool before giving up."""
    if not pool:
        return None
    shuffled = list(pool)
    random.shuffle(shuffled)
    for row in shuffled[:SAMPLE_MAX_TRIES]:
        cat_id = str(row[cat_id_key])
        if not _is_category_active(cat_id):
            continue
        url = build_url(row)
        _, status = _fetch(url)
        if status == 404 or status == 0:
            logger.info(f"[SEO_RULINGS] skipping cat {cat_id} ({url}) — status {status}")
            continue
        return row, url
    return None


def _pick_sample_categories() -> List[Dict]:
    """Pick 1 main, 1 subcat (depth=1) and 1 deepest (max depth), each one
    verified isEnabled=true in taxv2 AND returning a non-404 status."""
    maincats = _load_maincats()
    cat_urls = _load_cat_urls()
    if not maincats:
        return []
    maincat_by_name = {m["name"]: m for m in maincats}

    out: List[Dict] = []

    main_pick = _pick_one_live(
        maincats,
        build_url=lambda r: f"{SITE_BASE}/products{r['maincat_url']}",
    )
    if main_pick:
        row, url = main_pick
        out.append({
            "label": "Main category",
            "name": row["name"],
            "url": url,
            "depth": 0,
            "cat_id": row["cat_id"],
        })

    subs = [r for r in cat_urls if r["depth"] == 1 and r["maincat"] in maincat_by_name]
    sub_pick = _pick_one_live(
        subs,
        build_url=lambda r: f"{SITE_BASE}/products/{maincat_by_name[r['maincat']]['slug']}{r['url_name']}",
    )
    if sub_pick:
        row, url = sub_pick
        out.append({
            "label": "Subcategory",
            "name": row["deepest_cat"],
            "url": url,
            "depth": row["depth"],
            "cat_id": row["cat_id"],
        })

    # Walk depths from deepest down to 2, taking the first level that yields
    # a live (isEnabled + non-404) candidate. Whole-pool stale slugs at the
    # max depth (e.g. when a leaf branch got pruned from the live site but
    # the CSV snapshot still lists it) then fall through to a shallower —
    # but still "deeper than the subcategory" — pick.
    max_depth = max((r["depth"] for r in cat_urls), default=1)
    for d in range(max_depth, 1, -1):
        deepest_pool = [
            r for r in cat_urls
            if r["depth"] == d and r["maincat"] in maincat_by_name
        ]
        deepest_pick = _pick_one_live(
            deepest_pool,
            build_url=lambda r: f"{SITE_BASE}/products/{maincat_by_name[r['maincat']]['slug']}{r['url_name']}",
        )
        if deepest_pick:
            row, url = deepest_pick
            out.append({
                "label": "Deepest category",
                "name": row["deepest_cat"],
                "url": url,
                "depth": row["depth"],
                "cat_id": row["cat_id"],
            })
            break
    return out


# ---------------------------------------------------------------------------
# HTTP fetch (SEO user-agent)
# ---------------------------------------------------------------------------
def _fetch(url: str) -> Tuple[Optional[str], int]:
    if url in _HTML_CACHE:
        return _HTML_CACHE[url]
    try:
        r = _SESSION.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        # beslist.nl serves UTF-8 but doesn't always set the charset in the
        # Content-Type header, so requests falls back to ISO-8859-1 and
        # mangles characters like the warning emoji (⚠️ → â ï¸). Force UTF-8.
        r.encoding = "utf-8"
        result = (r.text, r.status_code)
    except Exception as e:
        logger.warning(f"[SEO_RULINGS] fetch failed {url}: {e}")
        result = (None, 0)
    _HTML_CACHE[url] = result
    return result


# ---------------------------------------------------------------------------
# HTML probes
# ---------------------------------------------------------------------------
def _has_noscript_title(html: str, text: str) -> bool:
    needle = f'<div class="{NOSCRIPT_TITLE_CLASS}">{text}</div>'
    return needle in html


def _check_sitemaps(urls: List[str]) -> Tuple[bool, List[Dict]]:
    """Fetch each sitemap URL and confirm it returns HTTP 200 with non-empty
    XML. Returns (failed, details) where failed is True if any URL is
    unreachable."""
    failed = False
    details: List[Dict] = []
    for url in urls:
        body, http_status = _fetch(url)
        present = http_status == 200 and bool(body and body.strip())
        details.append({"url": url, "present": present, "http_status": http_status})
        if not present:
            failed = True
    return failed, details


def _check_html_sitemaps(urls: List[str]) -> Tuple[bool, List[Dict]]:
    """Fetch each HTML sitemap page and confirm it returns HTTP 200 AND lists
    at least one sitemap item (<li class="sitemap__item--R6p3e">). Returns
    (failed, details) where failed is True if any page is unreachable or has
    no items."""
    failed = False
    details: List[Dict] = []
    for url in urls:
        body, http_status = _fetch(url)
        reachable = http_status == 200 and bool(body)
        item_count = body.count(SITEMAP_ITEM_CLASS) if body else 0
        present = reachable and item_count > 0
        details.append({
            "url": url,
            "present": present,
            "http_status": http_status,
            "item_count": item_count,
        })
        if not present:
            failed = True
    return failed, details


def _check_basement_links(html: str) -> Tuple[bool, str]:
    if f'class="{BASEMENT_GROUP_CLASS}"' not in html:
        return False, "basementlinks group div not found"
    if BASEMENT_LINK_CLASS not in html:
        return False, "basementlinks group present but no links inside"
    return True, ""


# ---------------------------------------------------------------------------
# Taxv2 — find category/facet combos where the facet has seoPriority=true
# ---------------------------------------------------------------------------
def _get_priority_facet_combos(n: int = 3) -> List[Dict]:
    cat_urls = _load_cat_urls()
    maincats = _load_maincats()
    if not cat_urls or not maincats:
        return []
    maincat_by_name = {m["name"]: m for m in maincats}

    eligible = [r for r in cat_urls if r["maincat"] in maincat_by_name]
    random.shuffle(eligible)

    found: List[Dict] = []
    tries = 0
    for row in eligible:
        if len(found) >= n or tries >= FACET_LOOKUP_MAX_TRIES:
            break
        tries += 1
        cat_id = row["cat_id"]
        if not _is_category_active(str(cat_id)):
            continue
        try:
            settings_resp = _SESSION.get(
                f"{TAX_BASE}/api/CategoryFacetSettings",
                params={"categoryId": cat_id},
                headers=TAX_HEADERS,
                timeout=TIMEOUT,
            )
            if settings_resp.status_code != 200:
                continue
            settings_data = settings_resp.json()
            items = settings_data if isinstance(settings_data, list) else settings_data.get("items", [])
            prio_facet_ids = [
                s.get("facetId") or s.get("FacetId")
                for s in items
                if s.get("seoPriority") is True
            ]
            prio_facet_ids = [fid for fid in prio_facet_ids if fid is not None]
            if not prio_facet_ids:
                continue

            facets_resp = _SESSION.get(
                f"{TAX_BASE}/api/CategoryFacets",
                params={"categoryId": cat_id, "locale": "nl-NL"},
                headers=TAX_HEADERS,
                timeout=TIMEOUT,
            )
            if facets_resp.status_code != 200:
                continue
            facets_data = facets_resp.json()
            facet_items = facets_data if isinstance(facets_data, list) else facets_data.get("items", [])
            for cf in facet_items:
                facet = cf.get("facet") or cf
                fid = facet.get("id")
                if fid not in prio_facet_ids:
                    continue
                labels = facet.get("labels") or []
                nl = next((l for l in labels if l.get("locale") == "nl-NL"), {})
                facet_name = (nl.get("name") or facet.get("name") or "").strip()
                if not facet_name:
                    continue
                root_slug = maincat_by_name[row["maincat"]]["slug"]
                cat_url = f"{SITE_BASE}/products/{root_slug}{row['url_name']}"
                _, cat_status = _fetch(cat_url)
                if cat_status == 404 or cat_status == 0:
                    logger.info(f"[SEO_RULINGS] skipping facet-combo cat {cat_id} ({cat_url}) — status {cat_status}")
                    break
                found.append({
                    "cat_id": cat_id,
                    "cat_name": row["deepest_cat"],
                    "cat_url": cat_url,
                    "facet_id": fid,
                    "facet_name": facet_name,
                })
                break  # only one combo per category
        except Exception as e:
            logger.warning(f"[SEO_RULINGS] facet lookup failed for cat {cat_id}: {e}")
            continue
    return found[:n]


# ---------------------------------------------------------------------------
# Check 4 — title/description variables
# ---------------------------------------------------------------------------
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_DESC_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']',
    re.IGNORECASE,
)


def _extract_title(html: str) -> str:
    m = _TITLE_RE.search(html)
    return (m.group(1).strip() if m else "")


def _extract_description(html: str) -> str:
    m = _META_DESC_RE.search(html)
    return (m.group(1).strip() if m else "")


def _check_variable(
    column: str,
    placeholder: str,
    limit: int,
    extract: str,
    success_pattern: re.Pattern,
) -> List[Dict]:
    """Pick `limit` URLs where pa.unique_titles_content.<column> contains the
    placeholder, fetch each and confirm the rendered title/description has
    `placeholder` substituted (i.e. placeholder gone + `success_pattern` present)."""
    findings: List[Dict] = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        sql = f"""
            SELECT u.url, c.{column} AS template
            FROM pa.unique_titles_content c
            JOIN pa.urls u ON c.url_id = u.url_id
            WHERE c.{column} LIKE %s
            ORDER BY random()
            LIMIT %s
        """
        # Fetch extra rows so we can skip 404s and still have enough results
        cur.execute(sql, (f"%{placeholder}%", limit * 3))
        rows = cur.fetchall()
    finally:
        cur.close()
        return_db_connection(conn)

    if not rows:
        findings.append({
            "variable": placeholder,
            "status": "no_rows",
            "detail": f"No URLs with {placeholder} in {column}",
        })
        return findings

    checked = 0
    for row in rows:
        if checked >= limit:
            break
        # Unique-titles DB stores paths as `/products/...`; absolutize before
        # fetching and before returning so the frontend can link to them.
        raw_url = row["url"]
        url = raw_url if raw_url.startswith("http") else f"{SITE_BASE}{raw_url}"
        html, http_status = _fetch(url)
        if not html:
            findings.append({
                "variable": placeholder,
                "url": url,
                "status": "fetch_error",
                "detail": f"HTTP {http_status}",
            })
            continue
        # Skip non-200 pages (404, 503, etc.) — the URL is dead, not a
        # variable-substitution failure.  Try the next candidate instead.
        if http_status != 200:
            findings.append({
                "variable": placeholder,
                "url": url,
                "status": "skipped",
                "detail": f"HTTP {http_status}",
            })
            continue
        checked += 1
        rendered = _extract_title(html) if extract == "title" else _extract_description(html)
        if placeholder in rendered or not success_pattern.search(rendered):
            findings.append({
                "variable": placeholder,
                "url": url,
                "status": "failed",
                "rendered": rendered[:240],
                "template": (row["template"] or "")[:240],
            })
        else:
            findings.append({
                "variable": placeholder,
                "url": url,
                "status": "ok",
                "rendered": rendered[:240],
            })
    return findings


def _check_title_variables() -> Dict:
    findings: List[Dict] = []
    findings += _check_variable(
        column="title", placeholder="!!DISCOUNT!!", limit=3,
        extract="title", success_pattern=re.compile(r"\d+\s*%"),
    )
    findings += _check_variable(
        column="description", placeholder="!!NR!!", limit=3,
        extract="description", success_pattern=re.compile(r"\d"),
    )
    findings += _check_variable(
        column="title", placeholder="!!JAAR!!", limit=3,
        extract="title", success_pattern=re.compile(r"(?:19|20)\d{2}"),
    )

    failed = any(f["status"] in ("failed", "no_rows") for f in findings)
    return {"findings": findings, "failed": failed}


# ---------------------------------------------------------------------------
# Slack — DM to the SEO_USER_ID via SLACK_BOT_TOKEN (same pattern as
# backend/daily_automation.py)
# ---------------------------------------------------------------------------
_SITEMAP_CHECK_KEYS = ("xml_sitemaps", "html_sitemaps")


def _sitemap_slack_lines(check_key: str, details: List[Dict]) -> List[str]:
    """Per-URL breakdown lines for the sitemap checks, indented under the
    check's summary line. Returns [] for any non-sitemap check. HTML sitemaps
    also report the item count; XML/HTML failures report the HTTP status."""
    if check_key not in _SITEMAP_CHECK_KEYS:
        return []
    lines: List[str] = []
    for d in details:
        url = (d.get("url") or "").replace(SITE_BASE, "")
        ok = d.get("present") is True
        mark = ":white_check_mark:" if ok else ":x:"
        if check_key == "html_sitemaps":
            suffix = f" ({d.get('item_count', 0)} items)"
        elif not ok:
            suffix = f" (HTTP {d.get('http_status')})"
        else:
            suffix = ""
        lines.append(f"        {mark} {url}{suffix}")
    return lines


def _send_slack(text: str) -> Dict:
    token = os.getenv("SLACK_BOT_TOKEN", "")
    user_id = os.getenv("SLACK_USER_ID", "")
    if not token or not user_id:
        logger.warning("[SEO_RULINGS] SLACK_BOT_TOKEN or SLACK_USER_ID not set; skipping notification")
        return {"sent": False, "reason": "missing_env"}
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": user_id, "text": text},
            timeout=15,
        )
        data = resp.json()
        if data.get("ok"):
            return {"sent": True}
        return {"sent": False, "reason": data.get("error", "unknown")}
    except Exception as e:
        logger.warning(f"[SEO_RULINGS] Slack send failed: {e}")
        return {"sent": False, "reason": str(e)}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
CHECK_LABELS = {
    "no_script_categories": "No-script categories",
    "no_script_facet_links": "No-script facet-links",
    "basement_links": "Basement links",
    "title_variables": "Title variables",
    "xml_sitemaps": "XML-Sitemaps",
    "html_sitemaps": "HTML-Sitemaps",
}


def run_all_checks() -> Dict:
    """Run every SEO check and return the full summary."""
    _clear_run_caches()
    started_at = datetime.utcnow()
    results: Dict = {"checks": {}, "details": {}}

    cats = _pick_sample_categories()
    results["details"]["sampled_categories"] = cats

    # --- Check 1 (noScript "Kies categorie") + Check 3 (basement links) ---
    no_script_failed = False
    basement_failed = False
    no_script_details: List[Dict] = []
    basement_details: List[Dict] = []
    if not cats:
        no_script_failed = True
        basement_failed = True
        no_script_details.append({"status": "no_sample"})
        basement_details.append({"status": "no_sample"})
    for c in cats:
        html, http_status = _fetch(c["url"])
        if not html:
            no_script_failed = True
            basement_failed = True
            err = {**c, "status": "fetch_error", "http_status": http_status}
            no_script_details.append(err)
            basement_details.append(err)
            continue
        present = _has_noscript_title(html, "Kies categorie")
        no_script_details.append({**c, "present": present, "http_status": http_status})
        if not present:
            no_script_failed = True

        b_ok, b_msg = _check_basement_links(html)
        basement_details.append({
            **c, "present": b_ok, "detail": b_msg, "http_status": http_status,
        })
        if not b_ok:
            basement_failed = True

    results["checks"]["no_script_categories"] = "failed" if no_script_failed else "passed"
    results["details"]["no_script_categories"] = no_script_details
    results["checks"]["basement_links"] = "failed" if basement_failed else "passed"
    results["details"]["basement_links"] = basement_details

    # --- Check 2 (noScript facet-links) ---
    combos = _get_priority_facet_combos(3)
    facet_failed = False
    facet_details: List[Dict] = []
    if not combos:
        facet_failed = True
        facet_details.append({"status": "no_priority_facets_found"})
    for combo in combos:
        html, http_status = _fetch(combo["cat_url"])
        if not html:
            facet_failed = True
            facet_details.append({**combo, "status": "fetch_error", "http_status": http_status})
            continue
        present = _has_noscript_title(html, combo["facet_name"])
        facet_details.append({**combo, "present": present, "http_status": http_status})
        if not present:
            facet_failed = True
    results["checks"]["no_script_facet_links"] = "failed" if facet_failed else "passed"
    results["details"]["no_script_facet_links"] = facet_details

    # --- Check 4 (title/description variables) ---
    tv = _check_title_variables()
    results["checks"]["title_variables"] = "failed" if tv["failed"] else "passed"
    results["details"]["title_variables"] = tv["findings"]

    # --- Check 5 (XML sitemaps — landing/PLP + browse) ---
    xml_sm_failed, xml_sm_details = _check_sitemaps(SITEMAP_XML_URLS)
    results["checks"]["xml_sitemaps"] = "failed" if xml_sm_failed else "passed"
    results["details"]["xml_sitemaps"] = xml_sm_details

    # --- Check 6 (HTML sitemaps — reachable + has items) ---
    html_sm_failed, html_sm_details = _check_html_sitemaps(HTML_SITEMAP_URLS)
    results["checks"]["html_sitemaps"] = "failed" if html_sm_failed else "passed"
    results["details"]["html_sitemaps"] = html_sm_details

    # --- Summary + Slack ---
    passed = [k for k, v in results["checks"].items() if v == "passed"]
    failed = [k for k, v in results["checks"].items() if v == "failed"]
    icon = ":x:" if failed else ":white_check_mark:"
    # Passed first, then failed — but keep each check's own per-URL breakdown
    # (sitemap checks only) indented under its line so a failure shows exactly
    # which URL is down.
    summary_lines: List[str] = []
    for k in passed + failed:
        mark = ":white_check_mark:" if k in passed else ":x:"
        summary_lines.append(f"{mark} {CHECK_LABELS[k]}")
        summary_lines.extend(_sitemap_slack_lines(k, results["details"].get(k, [])))
    slack_text = (
        f"{icon} *SEO Rulings — {len(failed)} failed, {len(passed)} passed*\n"
        + "\n".join(summary_lines)
    )
    slack_result = _send_slack(slack_text)

    finished_at = datetime.utcnow()
    results["summary"] = {
        "passed": passed,
        "failed": failed,
        "slack": slack_result,
        "slack_text": slack_text,
    }
    results["started_at"] = started_at.isoformat() + "Z"
    results["finished_at"] = finished_at.isoformat() + "Z"

    try:
        run_id = _persist_run(started_at, finished_at, results)
        if run_id is not None:
            results["run_id"] = run_id
    except Exception as e:
        logger.warning(f"[SEO_RULINGS] persist failed: {e}")

    return results


# ---------------------------------------------------------------------------
# Persistence — pa.seo_rulings_runs stores every completed run so the page
# can rehydrate the last result on refresh.
# ---------------------------------------------------------------------------
def init_seo_rulings_tables() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_rulings_runs (
                run_id        SERIAL PRIMARY KEY,
                started_at    TIMESTAMP NOT NULL,
                finished_at   TIMESTAMP NOT NULL DEFAULT NOW(),
                passed_count  INT       NOT NULL,
                failed_count  INT       NOT NULL,
                result        JSONB     NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS seo_rulings_runs_finished_at_idx
            ON pa.seo_rulings_runs (finished_at DESC)
        """)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def _persist_run(started_at: datetime, finished_at: datetime, result: Dict) -> Optional[int]:
    summary = result.get("summary") or {}
    passed = len(summary.get("passed") or [])
    failed = len(summary.get("failed") or [])
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO pa.seo_rulings_runs
                (started_at, finished_at, passed_count, failed_count, result)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            RETURNING run_id
            """,
            (started_at, finished_at, passed, failed, json.dumps(result)),
        )
        row = cur.fetchone()
        conn.commit()
        return row["run_id"] if row else None
    finally:
        cur.close()
        return_db_connection(conn)


def _row_to_run(row: Dict, include_result: bool = True) -> Dict:
    out = {
        "run_id": row["run_id"],
        "started_at": row["started_at"].isoformat() + "Z",
        "finished_at": row["finished_at"].isoformat() + "Z",
        "passed_count": row["passed_count"],
        "failed_count": row["failed_count"],
    }
    if include_result:
        out["result"] = row["result"]
    return out


def get_last_run() -> Optional[Dict]:
    """Return the most-recently-completed run, or None if none exists yet."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT run_id, started_at, finished_at, passed_count, failed_count, result
            FROM pa.seo_rulings_runs
            ORDER BY finished_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return _row_to_run(row) if row else None
    finally:
        cur.close()
        return_db_connection(conn)


def get_recent_runs(limit: int = 20) -> List[Dict]:
    """Return up to `limit` recent runs (newest first), without the full
    result JSONB so the list response stays small."""
    limit = max(1, min(int(limit), 200))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT run_id, started_at, finished_at, passed_count, failed_count
            FROM pa.seo_rulings_runs
            ORDER BY finished_at DESC
            LIMIT %s
        """, (limit,))
        return [_row_to_run(r, include_result=False) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_run_by_id(run_id: int) -> Optional[Dict]:
    """Return one run with full result payload, for export / re-render."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT run_id, started_at, finished_at, passed_count, failed_count, result
            FROM pa.seo_rulings_runs
            WHERE run_id = %s
        """, (int(run_id),))
        row = cur.fetchone()
        return _row_to_run(row) if row else None
    finally:
        cur.close()
        return_db_connection(conn)


def delete_run(run_id: int) -> bool:
    """Delete one run. Returns True if a row was deleted, False otherwise."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM pa.seo_rulings_runs WHERE run_id = %s",
            (int(run_id),),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
        return_db_connection(conn)
