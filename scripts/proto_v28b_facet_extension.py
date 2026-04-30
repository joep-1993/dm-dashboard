"""V28b prototype: extend the search-derived rescue to also propose a facet.

For each row in a recent Auto-Redirects export, hits the Beslist Search API
and on top of the dominant deepest_cat (current V28 logic) also inspects
the response's `facets[]` array. If any single value covers >=
FACET_DOMINANCE of the products in the result set, propose appending it
as ~~<facet_name>~<value_id>.

Output CSV per row:
  old url, current redirect, dom_cat_name, total_products,
  picked_facet_name, picked_facet_value, picked_value_share,
  proposed_redirect (with facet appended), would_change
"""

import re
import sys
import time
import urllib.parse
from pathlib import Path

import pandas as pd
import requests

SEARCH_BASE_URL = "https://productsearch-v2.api.beslist.nl"
LIMIT = 50
TIMEOUT = 12
SLEEP = 0.5  # 2 QPS (matches IT's ask)
AND_MODE_TOTAL_THRESHOLD = 10000
DOMINANCE_THRESHOLD = 0.60
FACET_DOMINANCE_THRESHOLD = 0.50  # value's share of the result set


def parse_rurl(url: str) -> dict:
    m = re.search(r"/products/([^/]+)/([^/]+)/r/([^/?]+)", url)
    if m:
        return {"maincat": m.group(1), "subcat": m.group(2),
                "keyword": m.group(3).replace("_", " ").replace("-", " ")}
    m = re.search(r"/products/([^/]+)/r/([^/?]+)", url)
    if m:
        return {"maincat": m.group(1), "subcat": None,
                "keyword": m.group(2).replace("_", " ").replace("-", " ")}
    return {}


def fetch(maincat, keyword):
    params = {
        "category": maincat,
        "query": keyword,
        "countryLanguage": "nl-nl",
        "isBot": "true",
        "limit": str(LIMIT),
        "trackTotalHits": "true",
    }
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)[:80]}


def deepest_cat(data, _facets_meta):
    cats = data.get("categories") or []
    if not cats:
        return None
    max_depth = max((c.get("depth") or 0) for c in cats)
    leaf = sorted([c for c in cats if (c.get("depth") or 0) == max_depth],
                  key=lambda c: -(c.get("count") or 0))
    if not leaf:
        return None
    sum_at_leaf = sum((c.get("count") or 0) for c in leaf) or 1
    top = leaf[0]
    share = (top.get("count") or 0) / sum_at_leaf
    return {"name": top.get("name", ""), "url_slug": top.get("urlName", ""),
            "share": round(share, 2), "count": top.get("count") or 0}


def pick_dominant_facet(data, total, facet_meta):
    """Return (facet_name, value_name, value_id, share) or None.

    facet_meta: dict facet_id -> facet_name (slug), built from the cached
    facets.csv since the API response has facet_id but no slug.
    """
    if not total or total <= 0:
        return None
    best = None
    for f in (data.get("facets") or []):
        fid = f.get("id")
        # Skip "Winkel" (shop, facet_id=1) — never useful as a routing facet.
        if fid == 1:
            continue
        facet_slug = facet_meta.get(fid)
        if not facet_slug:
            continue
        for v in (f.get("values") or []):
            count = v.get("count") or 0
            share = count / total
            if share < FACET_DOMINANCE_THRESHOLD:
                continue
            cand = (share, count, facet_slug, v.get("facetValue") or "", v.get("id"))
            if best is None or cand > best:
                best = cand
    if best is None:
        return None
    share, count, facet_slug, value_name, value_id = best
    return {"facet_name": facet_slug, "value_name": value_name,
            "value_id": value_id, "share": round(share, 2), "count": count}


def main(xlsx_path: Path, out_csv: Path):
    df = pd.read_excel(xlsx_path)
    print(f"Loaded {len(df)} rows from {xlsx_path.name}")
    print(f"Columns: {list(df.columns)}")

    # Build facet_id -> facet_name slug map from the cache.
    facets_df = pd.read_csv("backend/rurl_optimizer_v2/data/cache/facets.csv")
    facet_meta = dict(zip(facets_df["facet_id"], facets_df["facet_name"]))

    out_rows = []
    for i, row in df.iterrows():
        old = str(row.get("old url") or "").strip()
        current = str(row.get("new url") or "").strip()
        rurl = parse_rurl(old)
        if not rurl:
            out_rows.append({"old_url": old, "error": "could not parse"})
            continue

        data = fetch(rurl["maincat"], rurl["keyword"])
        time.sleep(SLEEP)
        if "error" in data:
            out_rows.append({"old_url": old, "current_redirect": current,
                             "error": data["error"]})
            continue

        total = data.get("total") or 0
        mode = "fallback" if total >= AND_MODE_TOTAL_THRESHOLD else "and"

        dc = deepest_cat(data, facet_meta) if mode == "and" else None
        pf = pick_dominant_facet(data, total, facet_meta) if mode == "and" else None

        proposed = ""
        if dc and dc["share"] >= DOMINANCE_THRESHOLD and dc["url_slug"]:
            base = f"https://www.beslist.nl/products/{rurl['maincat']}/{dc['url_slug']}/"
            if pf:
                proposed = f"{base.rstrip('/')}/c/{pf['facet_name']}~{pf['value_id']}"
            else:
                proposed = base

        out_rows.append({
            "old_url": old,
            "keyword": rurl["keyword"],
            "current_redirect": current,
            "mode": mode,
            "total": total,
            "dom_cat_name": dc["name"] if dc else "",
            "dom_cat_share": dc["share"] if dc else "",
            "picked_facet_name": pf["facet_name"] if pf else "",
            "picked_value_name": pf["value_name"] if pf else "",
            "picked_value_id": pf["value_id"] if pf else "",
            "picked_value_share": pf["share"] if pf else "",
            "proposed_redirect": proposed,
            "would_change": (proposed and proposed != current and proposed.rstrip("/") != current.rstrip("/")),
        })
        marker = "≠" if out_rows[-1]["would_change"] else "="
        print(f"  [{i:3}] {marker} {mode:8} t={total:>7}  {rurl['keyword']!r:46} "
              f"dom={dc['name'] if dc else '?':<25}  facet={pf['facet_name'] + '~' + pf['value_name'] if pf else '-'}")

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(out_csv, index=False)
    print(f"\nWrote {out_csv} ({len(out_df)} rows)")

    if "would_change" in out_df.columns:
        n_change = out_df["would_change"].fillna(False).sum()
        n_with_facet = (out_df["picked_facet_name"].fillna("").astype(bool)).sum()
        print(f"\nrows with a facet pick: {n_with_facet}")
        print(f"rows where the proposal differs from current: {n_change}")


if __name__ == "__main__":
    xlsx = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        "/mnt/c/Users/JoepvanSchagen/Downloads/redirects_b064d07a_20260430_102811.xlsx"
    )
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(
        "/home/joepvanschagen/projects/dm-tools/scripts/v28b_facet_extension.csv"
    )
    main(xlsx, out)
