#!/usr/bin/env python3
"""
Fix D policy dry-run / comparison harness.

For every existing `search_derived_samecat` redirect (the "Fix D" path), replay
several candidate policies against the REAL cached search + facet-probe data and
the REAL lexical bridging logic, then export a side-by-side comparison so we can
decide the wrong-category behaviour on actual results.

Policies compared (per source URL):
  current        - what the optimizer actually emitted (from rurl_processed)
  P1_guard       - append matched facets when head noun is represented; else
                   keep bare when keyword names the category; else SUPPRESS->origin
  P2_keep_append - always keep the dominant category, append any matched facets
  P3_gate075     - same as P2 but only fire when dom_cat_share >= 0.75 (else origin)

Q1 decision baked in: brand facets (merk/winkel) are appended ONLY when a keyword
token literally names the brand value.

Run from backend/:  python3 rurl_optimizer_v2/test_fixd_policies.py
"""
import os, sys, re, json, sqlite3
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # so `import src.*` works like the optimizer does

from src.reliability_scorer import _keyword_bridges_value
from src.validation_rules import STOPWORDS, SHOP_NAMES

import psycopg2

CACHE = os.path.join(HERE, "data", "cache", "search_derived.sqlite")
PG = dict(host="10.1.32.9", user="dbadmin", dbname="n8n-vector-db",
          password="Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6")
BRAND_FACETS = ("merk", "winkel")
OUT_DIR = "/mnt/c/Users/JoepvanSchagen/Downloads/claude"
GATE_075 = 0.75
COUNT_FLOOR = 15  # informational column only

# --- parsing helpers ---------------------------------------------------------
def parse_source(url):
    """Return (maincat, subcat_slug_or_None, keyword) from an R-URL."""
    m = re.search(r"/products/([^/]+)/(?:([^/]+)/)?r/([^/]+?)(?:/c/.*)?/?$", url)
    if not m:
        return None, None, None
    maincat, subcat, kw = m.group(1), m.group(2), m.group(3)
    if subcat == "r":   # /products/{maincat}/r/... (no subcat)
        subcat = None
    return maincat, subcat, kw.replace("_", " ").replace("-", " ")

def existing_facet(url):
    m = re.search(r"/c/(.+?)/?$", url)
    return m.group(1) if m else ""

def kw_tokens(keyword):
    return [t for t in re.split(r"[\s\-_]+", (keyword or "").lower())
            if len(t) >= 3 and t not in STOPWORDS and t not in SHOP_NAMES]

def head_token(keyword):
    toks = kw_tokens(keyword)
    return toks[-1] if toks else ""

# --- cache lookups -----------------------------------------------------------
_conn = sqlite3.connect(f"file:{CACHE}?mode=ro", uri=True)

def search_cache(maincat, kw_norm):
    r = _conn.execute("select payload from search_cache where maincat=? and keyword=?",
                      (maincat, kw_norm)).fetchone()
    return json.loads(r[0]) if r else None

def probe_cache(maincat, kw_norm):
    r = _conn.execute("select payload from facet_probe_cache where maincat=? and keyword=?",
                      (maincat, kw_norm)).fetchone()
    return json.loads(r[0]) if r else None

# --- core: derive usable facets ---------------------------------------------
def usable_facets(probe, keyword):
    """List of (facet_name, value_id, value_name, count, is_brand) that the
    keyword lexically matches. Brand facets only when a token names the brand."""
    if not probe:
        return []
    cands = list(probe.get("multi_facets") or [])
    # include the top single pick if it isn't already in multi_facets
    if probe.get("facet_name") and probe.get("value_id") is not None:
        if not any(c.get("value_id") == probe["value_id"] for c in cands):
            cands.append({"facet_name": probe["facet_name"],
                          "value_id": probe["value_id"],
                          "value_name": probe.get("value_name", ""),
                          "count": probe.get("value_count")})
    out = []
    for c in cands:
        fn = (c.get("facet_name") or "")
        vn = c.get("value_name") or ""
        vid = c.get("value_id")
        cnt = c.get("count")
        is_brand = fn.lower() in BRAND_FACETS
        bridges = _keyword_bridges_value(keyword, vn)
        if is_brand:
            # Q1: append brand only when a keyword token literally names it
            if bridges:
                out.append((fn, vid, vn, cnt, True))
        else:
            if bridges:
                out.append((fn, vid, vn, cnt, False))
    return out

def build_url(maincat, slug, facets, ef=""):
    base = f"https://www.beslist.nl/products/{maincat}/{slug}"
    frags = []
    if ef:
        frags.append(ef)
    for fn, vid, *_ in facets:
        frags.append(f"{fn}~{vid}")
    if not frags:
        return base + "/"
    return base + "/c/" + "~~".join(sorted(set(frags)))

def origin_url(maincat, subcat, ef=""):
    slug = subcat if subcat else maincat
    base = f"https://www.beslist.nl/products/{maincat}/{slug}" if subcat \
        else f"https://www.beslist.nl/products/{maincat}"
    if ef:
        return base + "/c/" + ef
    return base + "/"

# --- evaluate one row --------------------------------------------------------
def evaluate(orig_url, current_redirect):
    maincat, subcat, keyword = parse_source(orig_url)
    if not maincat or not keyword:
        return None
    kw_norm = keyword  # cache keys are space-normalized lower (already done)
    sc = search_cache(maincat, kw_norm)
    pr = probe_cache(maincat, kw_norm)
    dom_name = (sc or {}).get("dom_cat_name", "") or ""
    dom_slug = (sc or {}).get("dom_cat_url_slug", "") or ""
    share = (sc or {}).get("dom_cat_share") or 0
    ef = existing_facet(orig_url)

    head = head_token(keyword)
    facets = usable_facets(pr, keyword)
    nonbrand = [f for f in facets if not f[4]]
    head_in_cat = _keyword_bridges_value(head, dom_name) if head else False
    head_in_facet = any(_keyword_bridges_value(head, f[2]) for f in facets) if head else False
    head_repr = head_in_cat or head_in_facet
    name_link = _keyword_bridges_value(keyword, dom_name)

    # P1 guard
    if facets and head_repr:
        p1 = ("APPEND", build_url(maincat, dom_slug, facets, ef))
    elif name_link or head_in_cat:
        p1 = ("KEEP_BARE", build_url(maincat, dom_slug, [], ef))
    else:
        p1 = ("SUPPRESS", origin_url(maincat, subcat, ef))

    # P2 keep+append
    if facets:
        p2 = ("KEEP_APPEND", build_url(maincat, dom_slug, facets, ef))
    else:
        p2 = ("KEEP_BARE", build_url(maincat, dom_slug, [], ef))

    # P3 gate 0.75
    if share < GATE_075:
        p3 = ("SUPPRESS_LOWSHARE", origin_url(maincat, subcat, ef))
    else:
        p3 = (p2[0], p2[1])

    # P4 combined (recommended): suppress unanchored or weak-and-wrong; else keep+append
    if not facets and not name_link:
        p4 = ("SUPPRESS_NOANCHOR", origin_url(maincat, subcat, ef))
    elif share < GATE_075 and not head_in_cat:
        p4 = ("SUPPRESS_WEAK", origin_url(maincat, subcat, ef))
    elif facets:
        p4 = ("KEEP_APPEND", build_url(maincat, dom_slug, facets, ef))
    else:
        p4 = ("KEEP_BARE", build_url(maincat, dom_slug, [], ef))

    facet_str = ", ".join(f"{f[0]}~{f[1]}({f[2]}{'*BRAND' if f[4] else ''}"
                          f"{',n='+str(f[3]) if f[3] is not None else ''})" for f in facets)
    return {
        "original_url": orig_url,
        "current_redirect": current_redirect,
        "maincat": maincat,
        "keyword": keyword,
        "head_token": head,
        "dom_cat": dom_name,
        "share": share,
        "probe_mode": (pr or {}).get("mode", "NO_CACHE"),
        "matched_facets": facet_str,
        "head_represented": head_repr,
        "name_link": name_link,
        "P1_guard_action": p1[0],
        "P1_guard_url": p1[1],
        "P2_keepappend_action": p2[0],
        "P2_keepappend_url": p2[1],
        "P3_gate075_action": p3[0],
        "P3_gate075_url": p3[1],
        "P4_combined_action": p4[0],
        "P4_combined_url": p4[1],
        "current_vs_P1_changed": (current_redirect or "").rstrip("/") != (p1[1] or "").rstrip("/"),
        "current_vs_P4_changed": (current_redirect or "").rstrip("/") != (p4[1] or "").rstrip("/"),
    }

# --- main --------------------------------------------------------------------
def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute("select original_url, redirect_url from rurl_processed "
                "where match_type='search_derived_samecat'")
    rows = cur.fetchall()
    results = [r for r in (evaluate(o, rd) for o, rd in rows) if r]
    print(f"evaluated {len(results)} / {len(rows)} search_derived_samecat rows")

    # summaries
    for pol in ("P1_guard_action", "P2_keepappend_action", "P3_gate075_action", "P4_combined_action"):
        print(f"\n{pol}:", dict(Counter(r[pol] for r in results)))
    print(f"\nP1 would CHANGE {sum(1 for r in results if r['current_vs_P1_changed'])} of {len(results)}")
    print(f"P4 would CHANGE {sum(1 for r in results if r['current_vs_P4_changed'])} of {len(results)}")

    # the four worked examples
    print("\n=== worked examples ===")
    needles = ["lichtgewicht", "inklapbaar-droogrek-muur", "waxinelicht_groot", "intex_opblaas_bank"]
    for n in needles:
        for r in results:
            if n in r["original_url"]:
                print(f"\n[{n}]  kw={r['keyword']!r}  dom={r['dom_cat']!r} ({r['share']}) "
                      f"probe={r['probe_mode']}")
                print(f"   facets: {r['matched_facets'] or '(none)'}  head_repr={r['head_represented']}")
                print(f"   current : {r['current_redirect']}")
                print(f"   P1 guard: [{r['P1_guard_action']}] {r['P1_guard_url']}")
                print(f"   P2 keep : [{r['P2_keepappend_action']}] {r['P2_keepappend_url']}")
                print(f"   P3 .75  : [{r['P3_gate075_action']}] {r['P3_gate075_url']}")
                print(f"   P4 comb : [{r['P4_combined_action']}] {r['P4_combined_url']}")
                break

    # export
    os.makedirs(OUT_DIR, exist_ok=True)
    cols = ["original_url", "current_redirect", "maincat", "keyword", "head_token",
            "dom_cat", "share", "probe_mode", "matched_facets", "head_represented",
            "name_link", "P1_guard_action", "P1_guard_url", "P2_keepappend_action",
            "P2_keepappend_url", "P3_gate075_action", "P3_gate075_url",
            "P4_combined_action", "P4_combined_url",
            "current_vs_P1_changed", "current_vs_P4_changed"]
    try:
        import pandas as pd
        df = pd.DataFrame(results)[cols]
        out = os.path.join(OUT_DIR, "fixd_policy_comparison.xlsx")
        df.to_excel(out, index=False)
    except Exception as e:
        import csv
        out = os.path.join(OUT_DIR, "fixd_policy_comparison.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in results:
                w.writerow({k: r[k] for k in cols})
        print(f"(pandas unavailable: {e}; wrote CSV)")
    print(f"\nwrote {out}")

if __name__ == "__main__":
    main()
