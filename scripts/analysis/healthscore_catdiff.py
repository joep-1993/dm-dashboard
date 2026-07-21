"""
HS2.0 vs HS1.0 per-category diff for a specific list of deepest_category_id.

Reuses the locked model + windows from healthscore_service.py:
  - HS2.0 set = top-N/cat by score = 0.889*pct(log visits)+0.111*pct(log revenue),
    scored on a leakage-free 90d predictor window before the holdout month.
  - HS1.0 set = live bt.new_hs_data URLs for the holdout month (per deepest_category_id).
  - Coverage = share of the holdout month's real SEO visits/revenue each set captures,
    denominator = the category's holdout traffic (by dim_visit.deepest_subcat_id).

Output: per-category summary table + full kept/added/dropped URL lists to CSV.
"""
from __future__ import annotations
import os, sys, csv, argparse
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from backend.healthscore_service import (  # noqa: E402
    _load_env, _redshift, _month_bounds, _predictor_window, _norm,
    _SEO_JOIN, _SEO_WHERE, _REV, W_VISITS_DEFAULT, W_REV_DEFAULT, CAP_N_DEFAULT,
)

CAT_IDS = [9000047, 9000066, 9000608, 9000953, 9002072,
           9005282, 9005317, 9001646, 9003581, 9000668]


def _seasonal_caps(cat_ids, month, default_cap):
    """Per-cat cap for a calendar month from pa.hs2_cat_cap (fallback default)."""
    import psycopg2 as _pg
    conn = _pg.connect(dsn=os.environ["DATABASE_URL"], connect_timeout=20)
    try:
        with conn.cursor() as c:
            c.execute("SELECT cat, cap FROM pa.hs2_cat_cap WHERE calendar_month=%s "
                      "AND cat = ANY(%s)", (month, list(cat_ids)))
            m = dict(c.fetchall())
    finally:
        conn.close()
    return {cid: int(m.get(cid, default_cap)) for cid in cat_ids}


def run(cat_ids, holdout_month, cap_n, w_visits, w_rev, out_csv, cap_mode="flat"):
    lo, hi, nl_label = _month_bounds(holdout_month)
    pred_lo, pred_hi = _predictor_window(holdout_month, 90)
    nv, nn = _norm("dv.url"), _norm("nh.url")
    ids = ",".join(str(int(x)) for x in cat_ids)

    if cap_mode == "seasonal":
        caps = _seasonal_caps(cat_ids, int(holdout_month.split("-")[1]), cap_n)
        cap_case = "CASE r.cat " + \
            " ".join(f"WHEN {int(c)} THEN {int(v)}" for c, v in caps.items()) + \
            f" ELSE {int(cap_n)} END"
        cap_filter = f"r.rnk <= {cap_case}"
    else:
        cap_filter = "r.rnk <= %(cap)s"
    cap_join = ""

    sql = f"""
        WITH pred_raw AS (
            SELECT {nv} AS npath, MAX(dv.deepest_subcat_id) AS cat,
                   MIN(dv.url) AS sample_url, COUNT(*) AS visits, SUM({_REV}) AS revenue
            FROM datamart.fct_visits fv {_SEO_JOIN}
            WHERE fv.dim_date_key BETWEEN %(pred_lo)s AND %(pred_hi)s AND {_SEO_WHERE}
              AND dv.deepest_subcat_id IN ({ids})
            GROUP BY 1
        ),
        pred AS (
            SELECT npath, cat, sample_url, visits,
                   %(wv)s * percent_rank() OVER (PARTITION BY cat ORDER BY ln(1 + visits::float8))
                 + %(wr)s * percent_rank() OVER (PARTITION BY cat ORDER BY ln(1 + GREATEST(revenue,0)::float8)) AS score
            FROM pred_raw WHERE cat IS NOT NULL AND visits > 0
        ),
        hs2 AS (
            SELECT r.npath, r.cat, r.sample_url FROM (
                SELECT npath, cat, sample_url,
                       row_number() OVER (PARTITION BY cat ORDER BY score DESC, visits DESC) AS rnk
                FROM pred
            ) r {cap_join} WHERE {cap_filter}
        ),
        cur AS (
            SELECT {nn} AS npath, MAX(nh.deepest_category_id) AS cat, MIN(nh.url) AS sample_url
            FROM bt.new_hs_data nh
            WHERE regexp_replace(lower(trim(nh.current_month_year)), '[[:space:]]+', ' ') = %(label)s
              AND nh.country = 'nl' AND nh.url IS NOT NULL AND nh.url <> ''
              AND nh.deepest_category_id IN ({ids})
            GROUP BY 1
        ),
        hold AS (
            SELECT {nv} AS npath, MAX(dv.deepest_subcat_id) AS cat,
                   MIN(dv.url) AS sample_url, COUNT(*) AS jv, SUM({_REV}) AS jr
            FROM datamart.fct_visits fv {_SEO_JOIN}
            WHERE fv.dim_date_key BETWEEN %(lo)s AND %(hi)s AND {_SEO_WHERE}
              AND dv.deepest_subcat_id IN ({ids})
            GROUP BY 1
        )
        SELECT COALESCE(h.cat, hs2.cat, cur.cat)                      AS cat,
               COALESCE(h.npath, hs2.npath, cur.npath)                AS npath,
               COALESCE(h.sample_url, cur.sample_url, hs2.sample_url) AS sample_url,
               CASE WHEN hs2.npath IS NOT NULL THEN 1 ELSE 0 END      AS in_hs2,
               CASE WHEN cur.npath IS NOT NULL THEN 1 ELSE 0 END      AS in_cur,
               COALESCE(h.jv, 0)                                      AS jv,
               COALESCE(h.jr, 0)                                      AS jr
        FROM hs2
        FULL OUTER JOIN cur  ON hs2.npath = cur.npath
        FULL OUTER JOIN hold h ON COALESCE(hs2.npath, cur.npath) = h.npath
    """
    params = {"pred_lo": pred_lo, "pred_hi": pred_hi, "lo": lo, "hi": hi,
              "label": nl_label, "wv": w_visits, "wr": w_rev, "cap": cap_n}

    with _redshift() as rs, rs.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(sql, params)
        rows = c.fetchall()
        # category names
        c.execute(f"""SELECT deepest_category_id, MAX(deepest_category_name) nm,
                             MAX(main_category_name) mnm
                      FROM bt.new_hs_data WHERE country='nl'
                        AND deepest_category_id IN ({ids}) GROUP BY 1""")
        names = {r["deepest_category_id"]: (r["nm"], r["mnm"]) for r in c.fetchall()}

    # ---- aggregate per category ----
    per = {}
    for r in rows:
        cat = r["cat"]
        d = per.setdefault(cat, dict(tot_v=0, tot_r=0.0, hs2_v=0, hs2_r=0.0, cur_v=0, cur_r=0.0,
                                     kept=0, added=0, dropped=0, hs2_n=0, cur_n=0,
                                     added_rows=[], dropped_rows=[]))
        jv, jr = int(r["jv"] or 0), float(r["jr"] or 0.0)
        d["tot_v"] += jv; d["tot_r"] += jr
        if r["in_hs2"]:
            d["hs2_n"] += 1; d["hs2_v"] += jv; d["hs2_r"] += jr
        if r["in_cur"]:
            d["cur_n"] += 1; d["cur_v"] += jv; d["cur_r"] += jr
        if r["in_hs2"] and r["in_cur"]:
            d["kept"] += 1
        elif r["in_hs2"] and not r["in_cur"]:
            d["added"] += 1; d["added_rows"].append((jv, jr, r["sample_url"]))
        elif r["in_cur"] and not r["in_hs2"]:
            d["dropped"] += 1; d["dropped_rows"].append((jv, jr, r["sample_url"]))

    def pct(a, b):
        return (100.0 * a / b) if b else None

    # ---- print summary ----
    order = [c for c in cat_ids if c in per] + [c for c in per if c not in cat_ids]
    print(f"\nHS2.0 vs HS1.0 per-category diff — holdout {holdout_month} "
          f"(HS1.0='{nl_label}'), cap N={cap_n}, predictor {pred_lo}-{pred_hi}\n")
    h = (f"{'category':<40}{'HS1.0 n':>9}{'HS2.0 n':>9}{'kept':>7}{'add':>7}{'drop':>7}"
         f"{'  cur_vis%':>10}{'  hs2_vis%':>10}{'  cur_rev%':>10}{'  hs2_rev%':>10}")
    print(h); print("-" * len(h))
    tot = dict(tot_v=0, cur_v=0, hs2_v=0, tot_r=0.0, cur_r=0.0, hs2_r=0.0)
    for cat in order:
        d = per[cat]
        nm = names.get(cat, ("?", "?"))[0] or "?"
        label = f"{cat} {nm}"[:39]
        for k in tot: tot[k] += d[k]
        print(f"{label:<40}{d['cur_n']:>9,}{d['hs2_n']:>9,}{d['kept']:>7,}{d['added']:>7,}"
              f"{d['dropped']:>7,}"
              f"{_p(pct(d['cur_v'],d['tot_v'])):>10}{_p(pct(d['hs2_v'],d['tot_v'])):>10}"
              f"{_p(pct(d['cur_r'],d['tot_r'])):>10}{_p(pct(d['hs2_r'],d['tot_r'])):>10}")
    print("-" * len(h))
    print(f"{'TOTAL (10 cats)':<40}{'':>9}{'':>9}{'':>7}{'':>7}{'':>7}"
          f"{_p(pct(tot['cur_v'],tot['tot_v'])):>10}{_p(pct(tot['hs2_v'],tot['tot_v'])):>10}"
          f"{_p(pct(tot['cur_r'],tot['tot_r'])):>10}{_p(pct(tot['hs2_r'],tot['tot_r'])):>10}")

    # ---- sample added / dropped (top by holdout visits) ----
    print("\nTop URLs HS2.0 ADDS (in HS2.0, not in live set) — by June SEO visits:")
    for cat in order:
        d = per[cat]; nm = names.get(cat, ("?", "?"))[0] or "?"
        top = sorted(d["added_rows"], reverse=True)[:5]
        if not top: continue
        print(f"  [{cat} {nm}]")
        for jv, jr, url in top:
            print(f"     +{jv:>6,} vis  €{jr:>8,.0f}  {url}")
    print("\nTop URLs HS2.0 DROPS (in live set, not in HS2.0) — by June SEO visits:")
    for cat in order:
        d = per[cat]; nm = names.get(cat, ("?", "?"))[0] or "?"
        top = sorted(d["dropped_rows"], reverse=True)[:5]
        if not top: continue
        print(f"  [{cat} {nm}]")
        for jv, jr, url in top:
            print(f"     -{jv:>6,} vis  €{jr:>8,.0f}  {url}")

    # ---- full CSV ----
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cat_id", "cat_name", "npath", "sample_url", "in_hs1", "in_hs2",
                    "status", "june_visits", "june_revenue"])
        for r in rows:
            cat = r["cat"]
            st = ("kept" if r["in_hs2"] and r["in_cur"]
                  else "added" if r["in_hs2"]
                  else "dropped" if r["in_cur"]
                  else "uncovered")
            w.writerow([cat, (names.get(cat, ("", ""))[0] or ""), r["npath"], r["sample_url"],
                        r["in_cur"], r["in_hs2"], st, r["jv"], round(float(r["jr"] or 0), 2)])
    print(f"\nFull per-URL diff written to: {out_csv}")


def _p(x):
    return f"{x:.1f}" if x is not None else "-"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default="2026-06")
    ap.add_argument("--cap-n", type=int, default=CAP_N_DEFAULT,
                    help="flat cap, or fallback cap for cats missing from pa.hs2_cat_cap")
    ap.add_argument("--cap-mode", choices=["flat", "seasonal"], default="flat")
    ap.add_argument("--out", default="/mnt/c/Users/JoepvanSchagen/Downloads/claude/hs2_catdiff.csv")
    a = ap.parse_args()
    _load_env()
    run(CAT_IDS, a.month, a.cap_n, W_VISITS_DEFAULT, W_REV_DEFAULT, a.out, a.cap_mode)
