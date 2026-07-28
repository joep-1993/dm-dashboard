#!/usr/bin/env python3
"""
suggestions.txt item 48.

Top 5 facets per category by visits **across all channels**, then a page-title
blueprint for every non-empty combination of those 5.

Default is a DRY RUN: writes one Excel workbook and touches nothing else.
`--write` additionally CREATES the new combos in `pa.seo_titles_blueprints` with
status='built', i.e. staged in the SEO Titles tool and publishable from its UI.

**Neither mode ever calls /page-titles.** Publishing stays a deliberate click.

⚠️ `--write` changes what the SEO Titles "Publish" button does: with no rows
selected it pushes ALL status='built' blueprints, and before this ran there were
none (43,874 rows, every one already 'pushed'). The run prints the exact DELETE
to undo itself.

Why this is not scripts/pagetitles_topn_combinations.py
------------------------------------------------------
That script ranks facets by **SEO** visits, and reads them from a stale pickle
(/tmp/seo_traffic_rows.pkl). Item 48 asks for **all channels**, so this queries
Redshift directly with no marketing_channel filter. It also reuses
backend.seo_titles_service.build_blueprint — the live logic the tool actually
pushes with — rather than the archival scripts/pagetitles_blueprint_from_urls.py
copy, so what you review is byte-identical to what a push would send.

Combination count
-----------------
canon_key sorts the facet types, so ORDER CREATES NO DISTINCT KEY: "all combos
of 5" is 2**5-1 = **31 per category**, not 325. Anything that says 325 is
double-counting permutations.

Usage
-----
    python3 scripts/pagetitles_top5_allchannel_combos.py [--top-n 5]
            [--from 2025-01-01] [--to 2026-07-27] [--url-limit 400000]
            [--max-cats N] [--out /path/file.xlsx]

Output columns per row: cat_id, cat_name, key, n_facets, combo_facets,
title, h1_title, description, country_code, already_exists, facet_visits,
cat_visits, top5_rank_of_facets.
"""
import argparse
import itertools
import os
import sys
from collections import defaultdict, Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (
    get_db_connection, return_db_connection,
    get_redshift_connection, return_redshift_connection,
)
from backend.seo_titles_service import (
    build_blueprint, canon_key, load_rules, load_existing_combos,
    parse_url, _resolve_cat, _upsert_blueprint, _yyyymmdd,
    init_seo_titles_table,
)

DEFAULT_OUT_DIR = '/mnt/c/Users/JoepvanSchagen/Downloads/claude'


def fetch_all_channel_faceted_urls(date_from, date_to, url_limit):
    """Faceted /c/ URLs with visits summed over EVERY channel.

    Deliberately no `chan.marketing_channel = 'SEO'` predicate and no join to
    ref_channel_derivation_stats at all — item 48 wants all-channel visits, and
    dropping the join also avoids inflating counts if a visit ever matched more
    than one derivation row.
    """
    sql = """
        SELECT SPLIT_PART(dv.url, '?', 1) AS url,
               count(*) AS visits
        FROM datamart.fct_visits fcv
        JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
        WHERE dv.is_real_visit = 1
          AND fcv.dim_date_key BETWEEN %s AND %s
          AND dv.url LIKE '%%beslist.nl%%'
          AND dv.url LIKE '%%/c/%%'
          AND dv.url NOT LIKE '%%/r/%%'
          AND dv.url NOT LIKE '%%+%%'
          AND dv.url NOT LIKE '%%/l/%%'
          AND dv.url NOT LIKE '%%/page_%%'
          AND dv.url NOT LIKE '%%#%%'
        GROUP BY 1
        ORDER BY visits DESC
        LIMIT %s
    """
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, (date_from, date_to, int(url_limit)))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_redshift_connection(conn)


def rank_facets_per_category(rows, taxonomy_cache, top_n):
    """-> {cat_id: {'cat_name', 'ranked': [(facet, visits), ...], 'cat_visits': n}}

    A facet's score is the summed visits of every URL that uses it, so a facet
    that only ever appears in deep combos still scores.
    """
    per_cat = {}
    stats = Counter()
    for r in rows:
        url = (r.get('url') or '').lower()
        p = parse_url(url)
        if not p:
            stats['not_faceted'] += 1
            continue
        leaf, types = p
        if not types:
            stats['no_facets'] += 1
            continue
        cat = _resolve_cat(taxonomy_cache, leaf)
        if not cat:
            stats['no_cat'] += 1
            continue
        cid = cat['cat_id']
        slot = per_cat.setdefault(cid, {
            'cat_name': cat.get('cat_name', ''),
            'counter': Counter(),
            'cat_visits': 0,
        })
        v = int(r.get('visits') or 0)
        slot['cat_visits'] += v
        for t in types:
            slot['counter'][t] += v
        stats['used'] += 1

    out = {}
    for cid, slot in per_cat.items():
        # Deterministic: visits desc, then facet name, so re-runs are comparable.
        ranked = sorted(slot['counter'].items(), key=lambda kv: (-kv[1], kv[0]))
        out[cid] = {
            'cat_name': slot['cat_name'],
            'ranked': ranked[:top_n],
            'cat_visits': slot['cat_visits'],
        }
    return out, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top-n', type=int, default=5)
    ap.add_argument('--from', dest='date_from', default='2025-01-01')
    ap.add_argument('--to', dest='date_to', default=None,
                    help='default: today')
    ap.add_argument('--url-limit', type=int, default=400000,
                    help='faceted /c/ urls pulled from Redshift (visits desc)')
    ap.add_argument('--max-cats', type=int, default=0,
                    help='0 = every category; otherwise the N with most visits')
    ap.add_argument('--out', default=None)
    ap.add_argument('--write', action='store_true',
                    help="create the NEW combos in pa.seo_titles_blueprints as "
                         "status='built'. Never pushes to /page-titles.")
    args = ap.parse_args()

    dto_default = int(datetime.now().strftime('%Y%m%d'))
    dfrom = _yyyymmdd(args.date_from, 20250101)
    dto = _yyyymmdd(args.date_to, dto_default)

    print(f'[1/5] Redshift: faceted /c/ urls, ALL channels, {dfrom}..{dto} '
          f'(limit {args.url_limit:,})')
    rows = fetch_all_channel_faceted_urls(dfrom, dto, args.url_limit)
    print(f'      {len(rows):,} urls, {sum(int(r["visits"] or 0) for r in rows):,} visits')

    print('[2/5] resolving categories + ranking facets by visits')
    from backend.url_validator_service import _cache as taxonomy_cache
    per_cat, stats = rank_facets_per_category(rows, taxonomy_cache, args.top_n)
    print(f'      {len(per_cat):,} categories | url outcomes: {dict(stats)}')

    cats = sorted(per_cat.items(), key=lambda kv: -kv[1]['cat_visits'])
    if args.max_cats:
        cats = cats[:args.max_cats]
        print(f'      limited to the top {len(cats):,} categories by visits')

    print('[3/5] loading facet position rules + existing combos')
    rules = load_rules()
    existing = load_existing_combos(force=True)
    print(f'      {len(rules):,} facet rules | {len(existing):,} existing (cat_id, key) combos')

    print(f'[4/5] building blueprints for every non-empty subset of the top '
          f'{args.top_n} ({2 ** args.top_n - 1} per category)')
    out_rows = []
    new_bps = []          # (blueprint, facet_visits) for the --write step
    n_new = n_existing = 0
    for cid, info in cats:
        ranked = info['ranked']
        facets = [t for t, _ in ranked]
        visits_by_facet = dict(ranked)
        rank_of = {t: i + 1 for i, t in enumerate(facets)}
        for size in range(1, len(facets) + 1):
            for combo in itertools.combinations(facets, size):
                types = set(combo)
                bp = build_blueprint(cid, info['cat_name'], types, rules)
                ck = (cid, canon_key(bp['key']))
                is_existing = ck in existing
                n_existing += is_existing
                n_new += (not is_existing)
                if not is_existing:
                    new_bps.append((bp, sum(visits_by_facet.get(t, 0) for t in combo)))
                out_rows.append({
                    'cat_id': cid,
                    'cat_name': info['cat_name'],
                    'key': bp['key'],
                    'n_facets': size,
                    'combo_facets': ' + '.join(sorted(combo)),
                    'title': bp['title'],
                    'h1_title': bp['h1_title'],
                    'description': bp['description'],
                    'country_code': bp['country_code'],
                    'already_exists': 'yes' if is_existing else 'no',
                    'facet_visits': sum(visits_by_facet.get(t, 0) for t in combo),
                    'cat_visits': info['cat_visits'],
                    'top5_rank_of_facets': ' + '.join(str(rank_of[t]) for t in sorted(combo)),
                })

    print(f'      {len(out_rows):,} blueprint rows | {n_new:,} new, {n_existing:,} already exist')

    # Excel caps a sheet at 1,048,576 rows; say so rather than truncating quietly.
    XLSX_MAX = 1048575
    if len(out_rows) > XLSX_MAX:
        print(f'      !! {len(out_rows):,} rows exceeds the Excel sheet limit '
              f'({XLSX_MAX:,}). Re-run with --max-cats to narrow.')
        return 1

    print('[5/5] writing the workbook (nothing is pushed)')
    import pandas as pd
    df = pd.DataFrame(out_rows)
    out = args.out or os.path.join(
        DEFAULT_OUT_DIR,
        f'top{args.top_n}_facet_combos_allchannel_{datetime.now():%Y-%m-%d}.xlsx')
    os.makedirs(os.path.dirname(out), exist_ok=True)

    # Per-category summary so the ranking itself can be sanity-checked without
    # reading 100k blueprint rows.
    summary = pd.DataFrame([{
        'cat_id': cid,
        'cat_name': info['cat_name'],
        'cat_visits': info['cat_visits'],
        'n_facets_found': len(info['ranked']),
        'top_facets': ' > '.join(f'{t} ({v:,})' for t, v in info['ranked']),
        'combos_generated': 2 ** len(info['ranked']) - 1,
    } for cid, info in cats])

    with pd.ExcelWriter(out, engine='openpyxl') as xw:
        df.to_excel(xw, sheet_name=f'top{args.top_n}_combos', index=False)
        summary.to_excel(xw, sheet_name='per_category', index=False)
    print(f'      -> {out}')

    if not args.write:
        print('\nDRY RUN — nothing was written and nothing was pushed.')
        print(f'Of {len(out_rows):,} rows, {n_new:,} would be new blueprints.')
        print('Re-run with --write to create them as status=\'built\'.')
        return 0

    # ---- --write: create the new combos as 'built' -------------------------
    print(f"\n[write] creating {len(new_bps):,} new blueprints as status='built'")
    init_seo_titles_table()
    conn = get_db_connection()
    cur = conn.cursor()
    written = 0
    try:
        # Stamp from the DB's clock, NOT datetime.now(). created_at is a naive
        # TIMESTAMP filled by now() on a server running TimeZone=Etc/UTC, so a
        # local (CEST) stamp is 2h ahead and every created_at >= stamp comparison
        # matches nothing — which silently broke both the verification count and
        # the printed undo statement on the first run.
        cur.execute("SELECT now() AT TIME ZONE 'UTC' AS t")
        stamp = cur.fetchone()['t']
        for bp, fvisits in new_bps:
            # source_url is NULL on purpose: these combos are synthesised from the
            # top-5 ranking, not scraped from one URL. publish_built() only reads
            # cat_id/key/title/h1_title/description/country_code, so that is safe;
            # it does mean the optional per-URL AI-title push has nothing to do.
            _upsert_blueprint(cur, bp, None, fvisits, None)
            written += 1
            if written % 2000 == 0:
                conn.commit()
                print(f'        {written:,}/{len(new_bps):,}')
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)

    # Verify against the DB rather than trusting the loop counter.
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT status, count(*) AS c FROM pa.seo_titles_blueprints GROUP BY 1 ORDER BY 2 DESC")
        by_status = {r['status']: r['c'] for r in cur.fetchall()}
        cur.execute("SELECT count(*) AS c FROM pa.seo_titles_blueprints WHERE created_at >= %s", (stamp,))
        fresh = cur.fetchone()['c']
    finally:
        cur.close()
        return_db_connection(conn)

    print(f'      wrote {written:,}; {fresh:,} rows carry this run\'s created_at '
          f'(stamp {stamp:%Y-%m-%d %H:%M:%S} UTC)')
    if fresh != written:
        print(f"      !! expected {written:,} — check the created_at timezone")
    print(f'      pa.seo_titles_blueprints now: '
          + ' | '.join(f'{k}={v:,}' for k, v in by_status.items()))
    print('\nNOTHING was pushed to /page-titles — publishing stays a deliberate click.')
    print('⚠️  SEO Titles → Publish with NO rows selected pushes ALL \'built\' rows,')
    print(f'    which is now {by_status.get("built", 0):,}. Select rows to push a subset.')
    print('\nTo undo this run exactly (created_at is UTC — the server is Etc/UTC):')
    print(f"    DELETE FROM pa.seo_titles_blueprints"
          f" WHERE status='built' AND created_at >= '{stamp:%Y-%m-%d %H:%M:%S}';")
    return 0


if __name__ == '__main__':
    sys.exit(main())
