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
"""
import csv
import logging
import os
import random
import re
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

NOSCRIPT_TITLE_CLASS = "noScript__title--LfAWg"
BASEMENT_GROUP_CLASS = "basementlinks__group--igXdw"
BASEMENT_LINK_CLASS = "basementlinks__link--2awhY"

_SESSION = requests.Session()


# ---------------------------------------------------------------------------
# Category sampling — uses the pre-built taxv2 snapshots in
# backend/maincat_mapping.csv + backend/data/cat_urls.csv (kept in sync with
# the Taxonomy API).
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


def _pick_sample_categories() -> List[Dict]:
    """Pick 1 main, 1 subcat (depth=1) and 1 deepest (max depth)."""
    maincats = _load_maincats()
    cat_urls = _load_cat_urls()
    if not maincats:
        return []
    maincat_by_name = {m["name"]: m for m in maincats}

    main = random.choice(maincats)
    out: List[Dict] = [{
        "label": "Main category",
        "name": main["name"],
        "url": f"{SITE_BASE}/products{main['maincat_url']}",
        "depth": 0,
        "cat_id": main["cat_id"],
    }]

    subs = [r for r in cat_urls if r["depth"] == 1 and r["maincat"] in maincat_by_name]
    if subs:
        sub = random.choice(subs)
        root_slug = maincat_by_name[sub["maincat"]]["slug"]
        out.append({
            "label": "Subcategory",
            "name": sub["deepest_cat"],
            "url": f"{SITE_BASE}/products/{root_slug}{sub['url_name']}",
            "depth": sub["depth"],
            "cat_id": sub["cat_id"],
        })

    max_depth = max((r["depth"] for r in cat_urls), default=1)
    if max_depth > 1:
        deepest_pool = [
            r for r in cat_urls
            if r["depth"] == max_depth and r["maincat"] in maincat_by_name
        ]
        if deepest_pool:
            deepest = random.choice(deepest_pool)
            root_slug = maincat_by_name[deepest["maincat"]]["slug"]
            out.append({
                "label": "Deepest category",
                "name": deepest["deepest_cat"],
                "url": f"{SITE_BASE}/products/{root_slug}{deepest['url_name']}",
                "depth": deepest["depth"],
                "cat_id": deepest["cat_id"],
            })
    return out


# ---------------------------------------------------------------------------
# HTTP fetch (SEO user-agent)
# ---------------------------------------------------------------------------
def _fetch(url: str) -> Tuple[Optional[str], int]:
    try:
        r = _SESSION.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        return r.text, r.status_code
    except Exception as e:
        logger.warning(f"[SEO_RULINGS] fetch failed {url}: {e}")
        return None, 0


# ---------------------------------------------------------------------------
# HTML probes
# ---------------------------------------------------------------------------
def _has_noscript_title(html: str, text: str) -> bool:
    needle = f'<div class="{NOSCRIPT_TITLE_CLASS}">{text}</div>'
    return needle in html


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
        cur.execute(sql, (f"%{placeholder}%", limit))
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

    for row in rows:
        url = row["url"]
        html, http_status = _fetch(url)
        if not html:
            findings.append({
                "variable": placeholder,
                "url": url,
                "status": "fetch_error",
                "detail": f"HTTP {http_status}",
            })
            continue
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
        column="description", placeholder="!!NR!!", limit=1,
        extract="description", success_pattern=re.compile(r"\d"),
    )
    findings += _check_variable(
        column="title", placeholder="!!JAAR!!", limit=1,
        extract="title", success_pattern=re.compile(r"(?:19|20)\d{2}"),
    )

    failed = any(f["status"] in ("failed", "fetch_error", "no_rows") for f in findings)
    return {"findings": findings, "failed": failed}


# ---------------------------------------------------------------------------
# Slack — DM to the SEO_USER_ID via SLACK_BOT_TOKEN (same pattern as
# backend/daily_automation.py)
# ---------------------------------------------------------------------------
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
}


def run_all_checks() -> Dict:
    """Run every SEO check and return the full summary."""
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

    # --- Summary + Slack ---
    passed = [k for k, v in results["checks"].items() if v == "passed"]
    failed = [k for k, v in results["checks"].items() if v == "failed"]
    icon = ":x:" if failed else ":white_check_mark:"
    summary_lines = [f":white_check_mark: {CHECK_LABELS[k]}" for k in passed]
    summary_lines += [f":x: {CHECK_LABELS[k]}" for k in failed]
    slack_text = (
        f"{icon} *SEO Rulings — {len(failed)} failed, {len(passed)} passed*\n"
        + "\n".join(summary_lines)
    )
    slack_result = _send_slack(slack_text)

    results["summary"] = {
        "passed": passed,
        "failed": failed,
        "slack": slack_result,
        "slack_text": slack_text,
    }
    return results
