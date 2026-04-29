"""V27 prototype: product-overlap check for Auto-Redirects.

Reads a recent Auto-Redirects export, samples a mix of high/medium/low/zero
tier rows, calls the Beslist Search API once for the original R-URL and once
for the redirect URL, and computes the Jaccard / hit-rate overlap of the
returned product IDs. Output: a CSV the user can eyeball to decide whether
product overlap is a strong enough signal to fold into the reliability
score.
"""

import csv
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

SEARCH_BASE_URL = "https://productsearch-v2.api.beslist.nl"
COUNTRY_LANG = "nl-nl"
LIMIT = 50  # how many products to compare per side
TIMEOUT = 12
SLEEP_BETWEEN = 0.15


def fetch_products(category: str, query: Optional[str] = None,
                   filters: Optional[list] = None,
                   limit: int = LIMIT) -> Optional[dict]:
    """Hit the Search API; return dict with total + ids, or None on failure."""
    params = [
        ("category", category),
        ("countryLanguage", COUNTRY_LANG),
        ("isBot", "false"),
        ("limit", str(limit)),
    ]
    if query:
        params.append(("query", query))
    if filters:
        for fname, fvalue in filters:
            params.append((f"filters[{fname}][0]", str(fvalue)))
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"error": str(e)[:120], "url": url}
    products = data.get("products") or []
    return {
        "total": data.get("total"),
        "ids": [p.get("id") for p in products if p.get("id")],
        "url": url,
    }


def parse_rurl(url: str) -> dict:
    """Extract (category_slug, keyword) from an R-URL.

    Supports:
      /products/<maincat>/<subcat>/r/<keyword>/...   → subcat slug
      /products/<maincat>/r/<keyword>/...            → maincat slug
    """
    m = re.search(r"/products/([^/]+)/([^/]+)/r/([^/?]+)", url)
    if m:
        return {"category": m.group(2), "keyword": m.group(3).replace("_", " ").replace("-", " ")}
    m = re.search(r"/products/([^/]+)/r/([^/?]+)", url)
    if m:
        return {"category": m.group(1), "keyword": m.group(2).replace("_", " ").replace("-", " ")}
    return {}


def parse_redirect_url(url: str) -> dict:
    """Extract (category_slug, [(facet_name, facet_value_id), ...]) from a redirect URL.

    Examples:
      /products/schoenen/schoenen_430892/c/populaire_serie~23811073
      /products/x/y/c/f1~v1~~f2~v2
    """
    m = re.search(r"/products/(?:[^/]+/)?([^/]+)/c/([^/?#]+)", url)
    if not m:
        # No /c/ — bare category page
        m2 = re.search(r"/products/(?:[^/]+/)?([^/]+)/?$", url)
        if m2:
            return {"category": m2.group(1), "filters": []}
        return {}
    cat = m.group(1)
    fragment = m.group(2)
    pairs = []
    for piece in fragment.split("~~"):
        if "~" in piece:
            name, vid = piece.split("~", 1)
            pairs.append((name, vid))
    return {"category": cat, "filters": pairs}


def overlap_metrics(a: list, b: list) -> dict:
    sa, sb = set(a), set(b)
    inter = sa & sb
    union = sa | sb
    return {
        "n_left": len(sa),
        "n_right": len(sb),
        "intersection": len(inter),
        "jaccard": round(100 * len(inter) / len(union), 1) if union else 0,
        "hit_rate_left": round(100 * len(inter) / len(sa), 1) if sa else 0,
    }


def sample_rows(df: pd.DataFrame, n_per_bucket: Optional[int] = 12) -> pd.DataFrame:
    """Mix of buckets so we can see whether overlap correlates with score.

    Pass n_per_bucket=None to take everything (no sampling).
    """
    df = df.copy()
    df["score_num"] = pd.to_numeric(df.get("score"), errors="coerce")
    buckets = {
        "high":   df[df["score_num"] >= 75],
        "medium": df[(df["score_num"] >= 50) & (df["score_num"] < 75)],
        "low":    df[(df["score_num"] >= 1)  & (df["score_num"] < 50)],
        "zero":   df[df["score_num"].fillna(-1) == 0],
    }
    parts = []
    for name, sub in buckets.items():
        if sub.empty:
            continue
        if n_per_bucket is None:
            s = sub.copy()
        else:
            take = min(n_per_bucket, len(sub))
            s = sub.sample(n=take, random_state=42).copy()
        s["bucket"] = name
        parts.append(s)
    return pd.concat(parts, ignore_index=True) if parts else df.head(0)


def main(xlsx_path: Path, out_csv: Path, n_per_bucket: Optional[int] = 12):
    df = pd.read_excel(xlsx_path)
    print(f"Loaded {len(df)} rows from {xlsx_path.name}")
    print(f"Columns: {list(df.columns)}")

    sample = sample_rows(df, n_per_bucket=n_per_bucket)
    print(f"Sampled {len(sample)} rows across buckets:",
          sample["bucket"].value_counts().to_dict())

    rows_out = []
    for i, row in sample.iterrows():
        old = str(row.get("old url") or row.get("original_url") or "").strip()
        new = str(row.get("new url") or row.get("redirect_url") or "").strip()
        score = row.get("score")
        bucket = row.get("bucket")
        rurl = parse_rurl(old)
        redir = parse_redirect_url(new)
        if not rurl or not redir or not new or new.lower() == "nan":
            rows_out.append({
                "old_url": old, "new_url": new, "score": score, "bucket": bucket,
                "error": "could not parse one side", "rurl_total": None,
                "redirect_total": None, "intersection": None, "jaccard": None,
                "hit_rate_left": None,
            })
            continue
        # R-URL side: keyword search inside its category.
        a = fetch_products(rurl["category"], query=rurl.get("keyword"))
        time.sleep(SLEEP_BETWEEN)
        # Redirect side: category + facet filters.
        b = fetch_products(redir["category"], filters=redir.get("filters") or [])
        time.sleep(SLEEP_BETWEEN)
        # Redirect-without-facet baseline. Used to detect "facet did not
        # narrow the result set" cases — typical of OR-fallback / popular
        # product pages where the comparison metric is meaningless.
        c = fetch_products(redir["category"]) if redir.get("filters") else b
        time.sleep(SLEEP_BETWEEN)

        if (a is None or b is None or c is None
                or "error" in (a or {}) or "error" in (b or {}) or "error" in (c or {})):
            rows_out.append({
                "old_url": old, "new_url": new, "score": score, "bucket": bucket,
                "error": (a or {}).get("error") or (b or {}).get("error")
                         or (c or {}).get("error") or "fetch failed",
                "rurl_total": (a or {}).get("total"),
                "redirect_total": (b or {}).get("total"),
                "redirect_no_facet_total": (c or {}).get("total"),
                "facet_narrowed": None,
                "intersection": None, "jaccard": None, "hit_rate_left": None,
            })
            continue

        # "Facet narrowed" = the facet filter cut at least 5% off the
        # category-only result count. When the redirect URL has no facet,
        # we still treat the comparison as meaningful (b == c).
        b_total = b.get("total") or 0
        c_total = c.get("total") or 0
        if not redir.get("filters"):
            facet_narrowed = True
        elif c_total <= 0:
            facet_narrowed = False
        else:
            facet_narrowed = b_total < c_total * 0.95

        m = overlap_metrics(a["ids"], b["ids"])
        rows_out.append({
            "old_url": old,
            "new_url": new,
            "score": score,
            "bucket": bucket,
            "rurl_keyword": rurl.get("keyword"),
            "rurl_category": rurl.get("category"),
            "redirect_category": redir.get("category"),
            "redirect_filters": ";".join(f"{n}={v}" for n, v in redir.get("filters") or []),
            "rurl_total": a.get("total"),
            "redirect_total": b_total,
            "redirect_no_facet_total": c_total,
            "facet_narrowed": facet_narrowed,
            "intersection": m["intersection"],
            "jaccard": m["jaccard"],
            "hit_rate_left": m["hit_rate_left"],
            "error": "",
        })
        marker = "" if facet_narrowed else " [SKIP: facet did not narrow]"
        print(f"  [{bucket} score={score}] {rurl.get('keyword')!r:40} "
              f"jaccard={m['jaccard']}  hit_rate={m['hit_rate_left']}  "
              f"({m['intersection']}/{m['n_left']}∩{m['n_right']}){marker}")

    out_df = pd.DataFrame(rows_out)
    out_df.to_csv(out_csv, index=False)
    print(f"\nWrote {out_csv} ({len(out_df)} rows)")

    # Summary by bucket — both unfiltered and only-facet-narrowed rows.
    print("\n=== Median jaccard / hit_rate by bucket (all rows) ===")
    grp = out_df.groupby("bucket")[["jaccard", "hit_rate_left"]].median().round(1)
    print(grp.to_string())

    if "facet_narrowed" in out_df.columns:
        narrowed = out_df[out_df["facet_narrowed"] == True]
        skipped = out_df[out_df["facet_narrowed"] == False]
        print(f"\nFacet-narrowed rows: {len(narrowed)} / {len(out_df)} "
              f"(skipped {len(skipped)} as inconclusive)")
        if not narrowed.empty:
            print("=== Median jaccard / hit_rate by bucket (facet narrowed only) ===")
            grp2 = narrowed.groupby("bucket")[["jaccard", "hit_rate_left"]].median().round(1)
            print(grp2.to_string())


if __name__ == "__main__":
    xlsx = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        "/mnt/c/Users/JoepvanSchagen/Downloads/redirects_global_63a90fd1_20260429_122533.xlsx"
    )
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(
        "/home/joepvanschagen/projects/dm-tools/scripts/product_overlap_prototype.csv"
    )
    # Pass --full as 3rd arg to skip per-bucket sampling.
    n_per_bucket = None if len(sys.argv) > 3 and sys.argv[3] == "--full" else 12
    main(xlsx, out, n_per_bucket=n_per_bucket)
