"""V28 prototype: search-derived redirect.

For each row in a recent Auto-Redirects export, hits the Search API once
with the R-URL's keyword + maincat filter, then derives a redirect
proposal from what the returned products have in common (specifically,
their dominant deepest_cat). Compares the proposal side-by-side with the
existing V14/V27 redirect.

The idea: instead of fuzzy-matching the keyword against facet values
(forward), let the search engine tell us which products satisfy the
keyword and route to whatever they share (backward). This sidesteps the
generic-adjective / head-noun / coverage-floor heuristics that V21–V27
keep adding as patches.

Output CSV columns:
    old url, keyword, maincat,
    legacy redirect (from current engine),
    legacy score,
    api_total, api_mode (and / fallback / wide),
    dom_cat_name, dom_cat_share, dom_cat_id, dom_cat_url_slug,
    derived redirect (proposal),
    agree (legacy_deepest_cat == derived_deepest_cat ?)
"""

import re
import sys
import time
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

SEARCH_BASE_URL = "https://productsearch-v2.api.beslist.nl"
COUNTRY_LANG = "nl-nl"
LIMIT = 50
TIMEOUT = 12
SLEEP = 0.15

# V28: With trackTotalHits=true the API returns the real total count (not
# capped at 10000), which makes AND vs fallback bimodal. AND-mode queries
# return ≤ a few thousand; fallback returns 100k–1.4M (the entire indexed
# catalog of the maincat). 10000 is a safe cap with plenty of headroom.
AND_MODE_TOTAL_THRESHOLD = 10000
DOMINANCE_THRESHOLD = 0.60       # fraction of products that must share the deepest_cat


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


def parse_redirect_url(url: str) -> dict:
    """Extract deepest_cat slug from the legacy redirect URL for comparison."""
    if not url or pd.isna(url):
        return {}
    m = re.search(r"/products/(?:[^/]+/)?([^/]+)/c/", url)
    if m:
        return {"deepest_cat_slug": m.group(1)}
    m = re.search(r"/products/(?:[^/]+/)?([^/]+)/?$", url)
    if m:
        return {"deepest_cat_slug": m.group(1)}
    return {}


def fetch_products(maincat: str, query: str) -> Optional[dict]:
    # V28: isBot=true skips A/B experiments and personalisation, giving the
    # clean default ranking. trackTotalHits=true uncaps `total` so we can
    # tell real AND-mode (small total) from fallback (catalog-sized total).
    params = [
        ("category", maincat),
        ("query", query),
        ("countryLanguage", COUNTRY_LANG),
        ("isBot", "true"),
        ("limit", str(LIMIT)),
        ("trackTotalHits", "true"),
    ]
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)[:120]}


def classify_mode(api_resp: dict, maincat: str) -> str:
    """and / fallback based on the real total count (trackTotalHits=true)."""
    if "error" in api_resp:
        return "error"
    total = api_resp.get("total") or 0
    products = api_resp.get("products") or []
    if not products:
        return "empty"
    if total < AND_MODE_TOTAL_THRESHOLD:
        return "and"
    return "fallback"


def dominant_deepest_cat(api_resp: dict) -> Optional[dict]:
    """Find the deepest_cat shared by ≥ DOMINANCE_THRESHOLD of returned products."""
    products = api_resp.get("products") or []
    if not products:
        return None
    rows = []
    for p in products:
        cats = p.get("categories") or []
        if cats:
            c = cats[-1]
            rows.append((c.get("id"), c.get("name", ""), c.get("urlName", "")))
    if not rows:
        return None
    counter = Counter(rows)
    (cat_id, cat_name, cat_slug), count = counter.most_common(1)[0]
    share = count / len(rows)
    if share < DOMINANCE_THRESHOLD:
        return None
    return {"id": cat_id, "name": cat_name, "url_slug": cat_slug, "share": round(share, 2)}


def derive_redirect(maincat: str, dom_cat: Optional[dict]) -> Optional[str]:
    """Build the search-derived redirect URL from the dominant deepest_cat."""
    if not dom_cat or not dom_cat.get("url_slug"):
        return None
    return f"https://www.beslist.nl/products/{maincat}/{dom_cat['url_slug']}/"


def main(xlsx_path: Path, out_csv: Path, limit_rows: Optional[int] = None):
    df = pd.read_excel(xlsx_path)
    if limit_rows:
        df = df.head(limit_rows)
    print(f"Processing {len(df)} rows from {xlsx_path.name}")

    out_rows = []
    for i, row in df.iterrows():
        old = str(row.get("old url") or "").strip()
        legacy_new = str(row.get("new url") or "").strip()
        legacy_score = row.get("score")
        rurl = parse_rurl(old)
        legacy_dst = parse_redirect_url(legacy_new)
        if not rurl:
            out_rows.append({"old_url": old, "error": "could not parse R-URL"})
            continue
        api = fetch_products(rurl["maincat"], rurl["keyword"])
        time.sleep(SLEEP)
        if not api or "error" in api:
            out_rows.append({
                "old_url": old, "keyword": rurl["keyword"], "maincat": rurl["maincat"],
                "legacy_redirect": legacy_new, "legacy_score": legacy_score,
                "error": (api or {}).get("error", "no response"),
            })
            continue
        mode = classify_mode(api, rurl["maincat"])
        # Only derive a redirect when we're in real AND-mode. Fallback mode
        # by definition has no signal — products are popular generics.
        dom = dominant_deepest_cat(api) if mode == "and" else None
        derived = derive_redirect(rurl["maincat"], dom)

        legacy_slug = (legacy_dst or {}).get("deepest_cat_slug", "")
        derived_slug = (dom or {}).get("url_slug", "")
        agree = bool(legacy_slug and derived_slug and legacy_slug == derived_slug)

        out_rows.append({
            "old_url": old,
            "keyword": rurl["keyword"],
            "maincat": rurl["maincat"],
            "legacy_redirect": legacy_new,
            "legacy_score": legacy_score,
            "api_total": api.get("total"),
            "api_mode": mode,
            "dom_cat_name": (dom or {}).get("name", ""),
            "dom_cat_share": (dom or {}).get("share", ""),
            "dom_cat_id": (dom or {}).get("id", ""),
            "dom_cat_slug": derived_slug,
            "derived_redirect": derived or "",
            "legacy_slug": legacy_slug,
            "agree_on_deepest_cat": agree,
            "error": "",
        })
        marker = "✓" if agree else ("≠" if derived else "·")
        print(f"  [{i:3}] {marker} {mode:8} t={api.get('total'):>6} "
              f"{rurl['keyword']!r:42} → {(dom or {}).get('name', '(no dominant)')!r}")

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(out_csv, index=False)
    print(f"\nWrote {out_csv} ({len(out_df)} rows)")

    # Summary
    print("\n=== Mode distribution ===")
    print(out_df["api_mode"].value_counts(dropna=False).to_string())

    have_derived = out_df[out_df["derived_redirect"] != ""]
    print(f"\nDerived redirect produced: {len(have_derived)} / {len(out_df)}")

    have_both = have_derived[have_derived["legacy_slug"] != ""]
    if not have_both.empty:
        agree_n = have_both["agree_on_deepest_cat"].sum()
        print(f"Of those with legacy + derived: agree on deepest_cat = {agree_n}/{len(have_both)} "
              f"({100*agree_n/len(have_both):.0f}%)")

    legacy_zero_derived_yes = out_df[
        (pd.to_numeric(out_df["legacy_score"], errors="coerce") == 0)
        & (out_df["derived_redirect"] != "")
    ]
    print(f"\nRescue candidates (legacy score=0 → derived has a proposal): "
          f"{len(legacy_zero_derived_yes)}")

    legacy_high_no_derived = out_df[
        (pd.to_numeric(out_df["legacy_score"], errors="coerce") >= 70)
        & (out_df["derived_redirect"] == "")
    ]
    print(f"Disagree-warnings (legacy high score but derived has no proposal): "
          f"{len(legacy_high_no_derived)}")


if __name__ == "__main__":
    xlsx = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        "/mnt/c/Users/JoepvanSchagen/Downloads/redirects_global_5ce534e0_20260429_134255.xlsx"
    )
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(
        "/home/joepvanschagen/projects/dm-tools/scripts/search_derived_proto.csv"
    )
    main(xlsx, out)
