"""HS2.0 selection -> Keywords API (`POST /sitemap`) payload builder.

The Keywords API (http://keywords.api.beslist.nl, Swagger on `/`) is what the
website reads for HTML-sitemap and footer links, so it — not Redshift
`bt.new_hs_data` — is the surface a HS2.0 rollout has to land on. Contract as
verified live against production 2026-08-03:

    GET  /sitemap/{cc}/{catId}/{limit}/{offset}
           -> {success, keywords:[{url, keywords}], totalCount}
           limit may be the full totalCount (7,348 in one call was fine)
    POST /sitemap   body = {deepestCategoryId, countryCodes:[cc], keywords:[…]}
           keyword item = {url, keywords, order}
           -> {success, before, after}
    GET  /health -> {"name":"Keywords API","debug":false,"env":"production"}

SIX CONTRACT FACTS THAT SHAPE THIS MODULE

1. `categoryId` accepts ids from **both taxonomy id spaces**, because the
   taxonomy genuinely uses both: the 32 top-level categories have SMALL ids
   (`parentId IS NULL` — 6 /computers/, 137 /mode/, 165 /huis_tuin/, 32000
   /schoenen/) and everything below them is 9xxxxxx (9000066 -> 854 records).
   **Corrected 2026-08-04**: this note previously said a "32000-style id returns
   an empty set — ignore them". It does not. 32000 is Schoenen and returns
   27,883 records; 137 returns 53,411 and 165 returns 68,209. Reading those
   maincat buckets as invalid is precisely what would block a maincat push.
   `bt.new_hs_data.deepest_category_id` mixing the two spaces is therefore the
   taxonomy being itself, not corruption — but it is still not a safe payload key,
   because it does not tell you WHICH level a row belongs to.
   Top-level buckets also already contain URLs from deeper categories (bucket 6's
   first record is /products/computers/computers_19664320/c/merk~24100313), so a
   maincat rollup matches what the channel already renders.
2. The grain is **(url, keyword)**, not url: one URL may carry several rows with
   different anchor text (Airco: 1,308 records over 603 distinct URLs). We emit
   exactly one row per URL, so a push shrinks record counts even where it grows
   URL coverage. That is deliberate — see `keywords` below.
3. `keywords` is the **anchor text** rendered on the sitemap page, not a tag.
   Live values are a mix of real page titles ("Heren Sneakers met rits") and raw
   lowercase GSC queries ("nike air max roze"); 15.3% are the latter. We use the
   page's own H1 (`dim_visit.page_heading`, most-visited variant per URL), which
   for URLs already live is the identical string 91.4% of the time.
4. POST is a **replace per (category, country)** — `before`/`after` in the
   response are set sizes, and there is NO DELETE endpoint. So the first push
   for a category discards whatever it holds now, and the only repair is another
   push. Hence `push()` is gated behind an explicit confirmation token and every
   other function here is read-only. Verified live on Grasmaaiers 9003581
   (2026-08-03): posting its 409 records back unchanged returned
   {"success":true,"before":409,"after":409} and the content compared identical,
   then the HS2.0 payload returned before=409 after=752. **POST needs no auth**
   — no security scheme in the Swagger and no challenge on the write, so the
   confirmation token is the only thing standing between a typo and a live
   category.
5. The channel carries **no `/p/` product pages** (0 of 3,000 sampled in each of
   Sneakers / Stoelen / Voer), while ~27% of the HS2.0 selection is PLP. Those
   rows are excluded by default; `include_plp=True` exists only so the decision
   is explicit rather than hidden.
6. Trailing slash follows the house canonicalization rule, confirmed against
   14,785 live records: a URL containing `/c/` has **no** trailing slash
   (14,673/14,673), anything else (bare category, `/r/`-only) **has** one
   (112/112). `pa.hs2_sitemap.npath` strips all trailing slashes, so it must be
   re-canonicalised before it goes over the wire.

WHAT THIS MODULE DOES NOT DO
The zero-traffic "new-URL bucket" rows (`source='new'`) are not pushable yet:
they are written with `deepest_category_id = NULL` and have no page_heading, so
they need a taxonomy-composed anchor and a category attribution first. They are
reported as skipped, never silently dropped.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

import requests

from backend.database import (get_db_connection, get_redshift_connection,
                              return_db_connection, return_redshift_connection)

KEYWORDS_API = os.getenv("KEYWORDS_API_URL", "http://keywords.api.beslist.nl")
SITEMAP_TABLE = "pa.hs2_sitemap"

# Maincat rollup (top-level sitemaps only) — see healthscore_service's MAINCAT
# section. Correction to contract fact 1 below: the small-id space is NOT invalid.
MAINCAT_SITEMAP_TABLE = "pa.hs2_sitemap_maincat"
MAINCAT_MAP_TABLE = "pa.hs2_cat_maincat"

# The 10 validation categories HS2.0 is being rolled out to first.
TEST_CATEGORIES = {
    9000047: "Stoelen",
    9000066: "Eetkamerstoelen",
    9000608: "Sneakers",
    9000953: "Voer",
    9002072: "Douchewanden",
    9005282: "Mobiele telefoons",
    9005317: "Airconditionings",
    9001646: "Dekbedovertrekken",
    9003581: "Grasmaaiers",
    9000668: "Shirts",
}

# Same normalisation healthscore_service._norm() applies, so headings join to
# pa.hs2_sitemap.npath without a second canonical form floating around.
_NORM_RS = ("rtrim(split_part(split_part("
            "lower(regexp_replace(dv.url,'^https?://[^/]+','')),'?',1),'#',1),'/')")

# page_heading lives on the visit fact, so a URL only has one if it has had SEO
# traffic. A year-wide window maximises the hit rate; the most-visited variant
# wins per URL (per Joep, 2026-08-03) because competing variants are casing /
# wording noise and the busiest one is what searchers actually landed on.
HEADING_WINDOW_DAYS = 365

_session = requests.Session()


# --------------------------------------------------------------------------- #
# URL canonicalisation
# --------------------------------------------------------------------------- #
def is_product_path(npath: str) -> bool:
    """True for product pages, which this channel carries none of (fact 5).

    Matching the PATH, not `type_url`: filtering on type_url == 'PLP' alone is not
    enough, because /p/ URLs also arrive typed as R-url or with no type at all.
    That hole let 14 product pages into the maincat payloads on 2026-08-04
    (Napapijri jackets under /mode/, EAN-style /p/ pages under /huis_tuin/); they
    were caught only by validate_payload, i.e. after the build. The deepest-category
    builder has the same hole — it just has not been hit yet, because those pages
    have to reach a category's top-N first, and a maincat pool is far wider.

    '/products/' is not a false positive: the substring is exactly slash-p-slash.
    """
    return "/p/" in (npath or "")


def canonical_url(npath: str) -> str:
    """Re-apply the trailing-slash rule npath strips (contract fact 6)."""
    p = (npath or "").strip()
    if not p:
        return p
    if "/c/" in p:
        return p.rstrip("/")
    return p if p.endswith("/") else p + "/"


# --------------------------------------------------------------------------- #
# Anchor text
# --------------------------------------------------------------------------- #
def fetch_page_headings(cats, as_of: date, window_days: int = HEADING_WINDOW_DAYS) -> dict:
    """{npath: (heading, visits)} for `cats`, most-visited heading per URL.

    One query for every category at once: the per-URL grain means a per-category
    loop would rescan the same fact partitions ten times.
    """
    lo = int((as_of - timedelta(days=window_days)).strftime("%Y%m%d"))
    hi = int(as_of.strftime("%Y%m%d"))
    sql = f"""
        SELECT {_NORM_RS} AS npath, dv.page_heading AS heading, count(*) AS visits
          FROM datamart.fct_visits fcv
          JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
          JOIN chan_deriv.ref_channel_derivation_stats chan
            ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
         WHERE dv.is_real_visit = 1
           AND chan.marketing_channel = 'SEO'
           AND fcv.dim_date_key BETWEEN %(lo)s AND %(hi)s
           AND dv.url ~ '^https?://www\\.beslist\\.nl/'
           AND dv.page_heading IS NOT NULL AND dv.page_heading <> ''
           AND dv.deepest_subcat_id IN %(cats)s
         GROUP BY 1, 2
    """
    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"lo": lo, "hi": hi, "cats": tuple(cats)})
            best: dict = {}
            # Both pools are built with RealDictCursor (backend/database.py), so
            # rows are mappings — tuple-unpacking them silently iterates the KEY
            # NAMES and yields exactly one bogus entry. Index by name.
            for row in cur:
                npath, h, visits = row["npath"], (row["heading"] or "").strip(), row["visits"]
                if not h:
                    continue
                cur_best = best.get(npath)
                # Strict >, so ties keep the first row and the result is stable
                # across runs rather than depending on scan order.
                if cur_best is None or visits > cur_best[1]:
                    best[npath] = (h, visits)
            return best
    finally:
        return_redshift_connection(conn)


# --------------------------------------------------------------------------- #
# Payload
# --------------------------------------------------------------------------- #
def _selected_under_other_category(live_urls, as_of: date, cat: int) -> set:
    """Of `live_urls`, the ones HS2.0 still selects — but under a DIFFERENT
    category. Returned in the caller's (live, slash-canonical) spelling.

    `deepest_category_id IS NOT NULL` excludes the new-URL bucket: those rows
    carry no category, so they are not evidence that a URL is wanted anywhere.
    """
    if not live_urls:
        return set()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            WITH d(u) AS (SELECT unnest(%s::text[]))
            SELECT DISTINCT d.u AS u
              FROM d
              JOIN {SITEMAP_TABLE} s
                ON rtrim(s.npath, '/') = rtrim(d.u, '/')
             WHERE s.as_of_date = %s
               AND s.deepest_category_id IS NOT NULL
               AND s.deepest_category_id <> %s
        """, (list(live_urls), as_of, cat))
        out = {r["u"] for r in cur.fetchall()}
        cur.close()
        return out
    finally:
        return_db_connection(conn)


def build_payload(cat: int, as_of: date, headings: dict, country: str = "nl",
                  include_plp: bool = False, preserve_cross_category: bool = False) -> dict:
    """Build one `POST /sitemap` body for a category, plus a report of what was
    left out. Order follows our score rank, so the sitemap renders best-first.

    preserve_cross_category — keep live URLs that HS2.0 still selects, but under
    a category outside this push. Needed because the Keywords API's category
    buckets do NOT match `dim_visit.deepest_subcat_id`: a URL sitting in the
    API's Stoelen set can be attributed to another category in Redshift, so a
    partial (e.g. 10-category) rollout would delete URLs the model wants to
    keep, purely because their own category was not pushed. Measured on the 10
    test categories: 2,935 URLs carrying 35,866 90-day SEO visits, i.e. 87% of
    all dropped traffic. Left unpreserved that reads as "SEO fell after the
    HS2.0 test" and gets blamed on the model instead of on the pilot's scope.

    Preserved URLs keep their **live anchor text verbatim** rather than being
    re-anchored to page_heading: we are not piloting those categories, so the
    conservative move is to leave their rendering exactly as it is. They are
    appended after the scored rows, so they never displace a ranked URL — which
    does mean the pushed set exceeds this category's cap by that many rows.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT npath, type_url, rank_in_cat, source
              FROM {SITEMAP_TABLE}
             WHERE as_of_date = %s AND deepest_category_id = %s
             ORDER BY rank_in_cat NULLS LAST, npath
        """, (as_of, cat))
        rows = cur.fetchall()
        cur.close()
    finally:
        return_db_connection(conn)

    keywords, skipped = [], {"plp": 0, "product": 0, "no_heading": 0, "no_rank": 0}
    for r in rows:
        npath, type_url, rank, source = r["npath"], r["type_url"], r["rank_in_cat"], r["source"]
        if type_url == "PLP" and not include_plp:
            skipped["plp"] += 1
            continue
        # See is_product_path: type_url == 'PLP' does not catch every /p/ page.
        if is_product_path(npath):
            skipped["product"] += 1
            continue
        hit = headings.get(npath)
        if not hit:
            skipped["no_heading"] += 1
            continue
        if rank is None:
            # Only the new-URL bucket lacks a rank, and that bucket has no
            # category either, so this should be unreachable for a cat query.
            skipped["no_rank"] += 1
            continue
        keywords.append({"url": canonical_url(npath), "keywords": hit[0], "order": int(rank)})

    preserved = 0
    if preserve_cross_category:
        live_anchors: dict = {}
        for k in get_live(cat, country):
            live_anchors.setdefault(k["url"], []).append(k["keywords"])
        ours = {k["url"] for k in keywords}
        would_drop = [u for u in live_anchors if u not in ours]
        keepers = _selected_under_other_category(would_drop, as_of, cat)
        order = max((k["order"] for k in keywords), default=0)
        for u in sorted(keepers):                      # sorted: stable payloads
            order += 1
            # page_heading when we happen to have it (the URL may belong to a
            # category we ARE pushing), otherwise the live anchor unchanged.
            hit = headings.get(u.rstrip("/"))
            anchor = hit[0] if hit else sorted(live_anchors[u])[0]
            keywords.append({"url": u, "keywords": anchor, "order": order})
            preserved += 1

    payload = {"deepestCategoryId": int(cat), "countryCodes": [country], "keywords": keywords}
    return {"payload": payload, "skipped": skipped, "preserved": preserved,
            "rows_considered": len(rows)}


def fetch_maincat_headings(maincats, as_of: date,
                           window_days: int = HEADING_WINDOW_DAYS) -> dict:
    """{npath: (heading, visits)} for whole maincat subtrees.

    Filters on dim_category.main_category_id rather than an IN-list of the ~3,500
    descendant deepest categories, which is both faster and immune to the list
    going stale.
    """
    lo = int((as_of - timedelta(days=window_days)).strftime("%Y%m%d"))
    hi = int(as_of.strftime("%Y%m%d"))
    sql = f"""
        SELECT {_NORM_RS} AS npath, dv.page_heading AS heading, count(*) AS visits
          FROM datamart.fct_visits fcv
          JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
          JOIN chan_deriv.ref_channel_derivation_stats chan
            ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
          JOIN datamart.dim_category dc
            ON dc.deepest_category_id = dv.deepest_subcat_id AND dc.deleted_ind = 0
         WHERE dv.is_real_visit = 1
           AND chan.marketing_channel = 'SEO'
           AND fcv.dim_date_key BETWEEN %(lo)s AND %(hi)s
           AND dv.url ~ '^https?://www\\.beslist\\.nl/'
           AND dv.page_heading IS NOT NULL AND dv.page_heading <> ''
           AND dc.main_category_id IN %(maincats)s
         GROUP BY 1, 2
    """
    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"lo": lo, "hi": hi, "maincats": tuple(maincats)})
            best: dict = {}
            for row in cur:
                npath, h, visits = row["npath"], (row["heading"] or "").strip(), row["visits"]
                if not h:
                    continue
                cur_best = best.get(npath)
                if cur_best is None or visits > cur_best[1]:
                    best[npath] = (h, visits)
            return best
    finally:
        return_redshift_connection(conn)


def seo_visits_in_maincats(maincats, as_of: date, days: int = 90) -> dict:
    """{npath: seo_visits} over the trailing `days` for these maincats' subtrees.

    Feeds the pre-flight bar the rollout is held to: Grasmaaiers went live with 0
    SEO visits dropped, and a maincat push can only be judged the same way if the
    drop list is priced. Fetched per maincat set rather than per URL so a 50k drop
    list costs one query, not 50k parameters.
    """
    lo = int((as_of - timedelta(days=days)).strftime("%Y%m%d"))
    hi = int(as_of.strftime("%Y%m%d"))
    sql = f"""
        SELECT {_NORM_RS} AS npath, count(*) AS visits
          FROM datamart.fct_visits fcv
          JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
          JOIN chan_deriv.ref_channel_derivation_stats chan
            ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
          JOIN datamart.dim_category dc
            ON dc.deepest_category_id = dv.deepest_subcat_id AND dc.deleted_ind = 0
         WHERE dv.is_real_visit = 1
           AND chan.marketing_channel = 'SEO'
           AND fcv.dim_date_key BETWEEN %(lo)s AND %(hi)s
           AND dv.url ~ '^https?://www\\.beslist\\.nl/'
           AND dc.main_category_id IN %(maincats)s
         GROUP BY 1
    """
    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"lo": lo, "hi": hi, "maincats": tuple(maincats)})
            return {r["npath"]: int(r["visits"] or 0) for r in cur}
    finally:
        return_redshift_connection(conn)


def build_maincat_payload(maincat: int, as_of: date, headings: dict,
                          country: str = "nl", include_plp: bool = False) -> dict:
    """One `POST /sitemap` body for a top-level category, from the rollup table.

    Same rules as build_payload — PLP excluded by default, a URL needs a heading
    to get an anchor, order follows our rank — but the pool is the whole subtree
    and the rank is rank_in_maincat.

    No preserve_cross_category here, and none needed: the bucket already spans the
    entire subtree, so a maincat push cannot strand a URL whose own category was
    left out of the batch. The one residual risk is a deepest category missing
    from dim_category, which `unmapped_live` reports rather than hides.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT npath, type_url, rank_in_maincat AS rank_in_cat, source
              FROM {MAINCAT_SITEMAP_TABLE}
             WHERE as_of_date = %s AND maincat = %s
             ORDER BY rank_in_maincat NULLS LAST, npath
        """, (as_of, maincat))
        rows = cur.fetchall()
        cur.close()
    finally:
        return_db_connection(conn)

    keywords, skipped = [], {"plp": 0, "product": 0, "no_heading": 0, "no_rank": 0}
    for r in rows:
        npath, type_url, rank = r["npath"], r["type_url"], r["rank_in_cat"]
        if type_url == "PLP" and not include_plp:
            skipped["plp"] += 1
            continue
        # Unconditional, unlike PLP: include_plp is a deliberate choice about
        # listing pages, never a licence to publish product pages.
        if is_product_path(npath):
            skipped["product"] += 1
            continue
        hit = headings.get(npath)
        if not hit:
            skipped["no_heading"] += 1
            continue
        if rank is None:
            skipped["no_rank"] += 1
            continue
        keywords.append({"url": canonical_url(npath), "keywords": hit[0], "order": int(rank)})

    payload = {"deepestCategoryId": int(maincat), "countryCodes": [country],
               "keywords": keywords}
    return {"payload": payload, "skipped": skipped, "preserved": 0,
            "rows_considered": len(rows)}


def snapshot_live(cat: int, country: str = "nl", out_dir: str = None) -> dict:
    """Capture a category's current live set verbatim, as a ready-to-POST body.

    This is the ONLY undo that exists: POST replaces and there is no DELETE, so
    the way back from a bad push is to push what was there before. Take this
    BEFORE any write. Note the round-trip is not perfectly lossless — the GET
    does not return `order`, so restoring re-numbers rows in the returned
    sequence; URLs and anchor text come back exactly.
    """
    live = get_live(cat, country)
    payload = {"deepestCategoryId": int(cat), "countryCodes": [country],
               "keywords": [{"url": k["url"], "keywords": k["keywords"], "order": i + 1}
                            for i, k in enumerate(live)]}
    out = {"category": cat, "records": len(live), "payload": payload}
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"live_snapshot_{cat}_{country}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        out["file"] = path
    return out


def validate_payload(payload: dict, allow_duplicate_pairs: bool = False) -> list:
    """Contract checks against the Swagger schema + the facts above. Returns a
    list of problems; empty means the body is shaped correctly.

    allow_duplicate_pairs — the LIVE data genuinely contains repeated
    (url, keywords) rows (4 of Grasmaaiers' 409, e.g. the same URL twice under
    'Husqvarna Grasmaaiers'), so a faithful restore from snapshot_live() has to
    be allowed to reproduce them. For a freshly BUILT payload a duplicate pair is
    a bug — we emit one row per URL — so it stays an error by default.
    """
    problems = []
    if not isinstance(payload.get("deepestCategoryId"), int):
        problems.append("deepestCategoryId must be an int")
    ccs = payload.get("countryCodes")
    if not isinstance(ccs, list) or not ccs:
        problems.append("countryCodes must be a non-empty array")
    else:
        for cc in ccs:
            if not isinstance(cc, str) or len(cc) != 2:
                problems.append(f"countryCode {cc!r} must be 2 letters")
    kws = payload.get("keywords")
    if not isinstance(kws, list) or not kws:
        # A POST with an empty array would replace the category with nothing.
        problems.append("keywords must be a NON-EMPTY array (empty = wipe the category)")
        return problems
    seen = set()
    for i, k in enumerate(kws):
        url, kw, order = k.get("url"), k.get("keywords"), k.get("order")
        if not isinstance(url, str) or not url.startswith("/"):
            problems.append(f"[{i}] url must be a site-relative path, got {url!r}")
        if not isinstance(kw, str) or not kw.strip():
            problems.append(f"[{i}] keywords (anchor text) must be a non-empty string")
        if not isinstance(order, int):
            problems.append(f"[{i}] order must be an int, got {order!r}")
        if url and "/p/" in url:
            problems.append(f"[{i}] {url} is a product page; this channel carries none")
        if (url, kw) in seen and not allow_duplicate_pairs:
            problems.append(f"[{i}] duplicate (url, keywords) pair: {url} / {kw!r}")
        seen.add((url, kw))
        if set(k) - {"url", "keywords", "order"}:
            problems.append(f"[{i}] unexpected field(s): {sorted(set(k) - {'url','keywords','order'})}")
    return problems


# --------------------------------------------------------------------------- #
# API client — reads are free, the write is gated
# --------------------------------------------------------------------------- #
def health() -> dict:
    r = _session.get(f"{KEYWORDS_API}/health", timeout=30)
    r.raise_for_status()
    return r.json()


def get_live(cat: int, country: str = "nl", limit: int = None) -> list:
    """Current records for a category. Two calls: totalCount, then the full set
    (the API happily returns limit == totalCount in one page)."""
    base = f"{KEYWORDS_API}/sitemap/{country}/{int(cat)}"
    r = _session.get(f"{base}/1/0", timeout=60)
    r.raise_for_status()
    total = r.json().get("totalCount") or 0
    if not total:
        return []
    n = min(total, limit) if limit else total
    r = _session.get(f"{base}/{n}/0", timeout=300)
    r.raise_for_status()
    return r.json().get("keywords") or []


def diff_against_live(payload: dict, country: str = "nl", live: list = None) -> dict:
    """What a push would actually change. Because POST replaces the category,
    every live URL absent from the payload disappears — so `dropped` is a real
    consequence, not a rounding error.

    `live` lets a caller that already holds the set pass it in. That matters at
    maincat scale: /huis_tuin/ is 68,209 records, so re-fetching it per report
    would move ~2M records over the wire for one dry run.
    """
    cat = payload["deepestCategoryId"]
    live = get_live(cat, country) if live is None else live
    live_anchors: dict = {}
    for k in live:
        live_anchors.setdefault(k["url"], set()).add(k["keywords"])
    ours = {k["url"]: k["keywords"] for k in payload["keywords"]}

    kept = [u for u in ours if u in live_anchors]
    anchor_same = [u for u in kept if ours[u].lower() in {a.lower() for a in live_anchors[u]}]
    return {
        "category": cat,
        "live_records": len(live),
        "live_urls": len(live_anchors),
        "payload_records": len(ours),
        "added_urls": len([u for u in ours if u not in live_anchors]),
        "kept_urls": len(kept),
        "anchor_unchanged": len(anchor_same),
        "anchor_changed": len(kept) - len(anchor_same),
        "dropped_urls": len([u for u in live_anchors if u not in ours]),
        "dropped_records": len([k for k in live if k["url"] not in ours]),
    }


def push(payload: dict, confirm_token: str = "", allow_duplicate_pairs: bool = False) -> dict:
    """POST the payload. REFUSES unless `confirm_token` is exactly
    "REPLACE <deepestCategoryId>".

    The gate is not ceremony: POST replaces the whole category and there is no
    DELETE, so a mistaken call is only repairable by another push, and the
    category's live links are gone in between.
    """
    problems = validate_payload(payload, allow_duplicate_pairs=allow_duplicate_pairs)
    if problems:
        raise ValueError(f"payload invalid, refusing to push: {problems[:5]}")
    expected = f"REPLACE {payload['deepestCategoryId']}"
    if confirm_token != expected:
        raise PermissionError(
            f"refusing to push: pass confirm_token={expected!r} to actually replace "
            f"category {payload['deepestCategoryId']}'s live sitemap set "
            f"({len(payload['keywords'])} records)")
    r = _session.post(f"{KEYWORDS_API}/sitemap", json=payload, timeout=300)
    out = {"status_code": r.status_code, "body": (r.text or "")[:1000]}
    try:
        out["json"] = r.json()
    except Exception:
        pass
    return out


def describe_request(payload: dict) -> dict:
    """The exact request `push()` would send — for eyeballing before anything
    goes over the wire."""
    body = json.dumps(payload, ensure_ascii=False)
    return {
        "method": "POST",
        "url": f"{KEYWORDS_API}/sitemap",
        "headers": {"Content-Type": "application/json"},
        "body_bytes": len(body.encode("utf-8")),
        "records": len(payload.get("keywords") or []),
        "body_head": body[:400],
    }


# --------------------------------------------------------------------------- #
# Dry run over the test categories
# --------------------------------------------------------------------------- #
def dry_run(as_of: date, cats: dict = None, country: str = "nl",
            include_plp: bool = False, out_dir: str = None,
            preserve_cross_category: bool = False) -> dict:
    """Build + validate + diff every category, write the payloads to disk, and
    send NOTHING. This is the whole rollout rehearsal."""
    cats = cats or TEST_CATEGORIES
    headings = fetch_page_headings(list(cats), as_of)
    results = []
    for cat, name in cats.items():
        built = build_payload(cat, as_of, headings, country, include_plp,
                              preserve_cross_category)
        payload = built["payload"]
        res = {
            "cat": cat, "name": name,
            "records": len(payload["keywords"]),
            "skipped": built["skipped"],
            "preserved": built["preserved"],
            "problems": validate_payload(payload),
            "request": describe_request(payload),
        }
        if payload["keywords"]:
            res["diff"] = diff_against_live(payload, country)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"hs2_sitemap_payload_{cat}_{name.replace(' ','_')}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            res["file"] = path
        results.append(res)
    return {"as_of": str(as_of), "country": country, "headings_available": len(headings),
            "categories": results}


# --------------------------------------------------------------------------- #
# Dry run over the MAINCATS (top-level sitemaps)
# --------------------------------------------------------------------------- #
def maincat_dry_run(as_of: date, maincats: dict = None, country: str = "nl",
                    include_plp: bool = False, out_dir: str = None,
                    visits_days: int = 90) -> dict:
    """Build + validate + diff every maincat, price the drop list, send NOTHING.

    Two numbers decide whether the cap clamp is right, and both are printed per
    maincat: `proposed_records` against `live_records`, and what the drop would
    cost in SEO visits. With MAINCAT_CAP_MAX at its default the cap should not be
    what binds — if proposed collapses far below live, the clamp is the cause and
    wants raising before any push.

    Coverage is reported on DISTINCT URLs (per Joep, 2026-08-04): a URL sitting in
    both its own deepest-category bucket and its maincat bucket is one URL.
    """
    from backend.healthscore_service import get_maincats

    maincats = maincats or get_maincats()
    ids = list(maincats)
    headings = fetch_maincat_headings(ids, as_of)
    visits = seo_visits_in_maincats(ids, as_of, visits_days)

    results, all_urls = [], set()
    for mc in ids:
        name = maincats[mc]
        built = build_maincat_payload(mc, as_of, headings, country, include_plp)
        payload = built["payload"]
        ours = {k["url"] for k in payload["keywords"]}
        all_urls |= ours

        res = {
            "maincat": mc, "name": name,
            "proposed_records": len(payload["keywords"]),
            "skipped": built["skipped"],
            "problems": validate_payload(payload),
        }
        if payload["keywords"]:
            # One fetch, reused for both the diff and the drop pricing.
            live_records = get_live(mc, country)
            res["diff"] = diff_against_live(payload, country, live=live_records)
            dropped = {k["url"] for k in live_records} - ours
            # Price the drop list. npath has no trailing slash; visits keys are
            # normalised the same way, so compare on the stripped form.
            dropped_v = {u: visits.get(u.rstrip("/"), 0) for u in dropped}
            with_traffic = {u: v for u, v in dropped_v.items() if v > 0}
            res["drop_cost"] = {
                "dropped_urls": len(dropped),
                "dropped_urls_with_seo_visits": len(with_traffic),
                f"dropped_seo_visits_{visits_days}d": sum(with_traffic.values()),
                "worst": sorted(with_traffic.items(), key=lambda kv: -kv[1])[:10],
            }
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            safe = str(name).replace(" ", "_").replace("/", "_").replace("&", "en")
            path = os.path.join(out_dir, f"hs2_maincat_payload_{mc}_{safe}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            res["file"] = path
        results.append(res)

    return {"as_of": str(as_of), "country": country,
            "headings_available": len(headings),
            "distinct_urls_across_maincats": len(all_urls),
            "maincats": results}
