"""
HS2.0 seasonal per-category cap builder.

Combines two data sources into a per-category, per-calendar-month cap:

  base_cap_c   = coverage-knee: # URLs to cover P% of category c's own SEO visits
                 over the trailing 12 months (clamped to [MIN, MAX]).
  season_mult  = climatology multiplier from pa.hs2_cat_month (24-month calendar
                 average), dampened by `alpha` and clamped to [mmin, mmax].

  cap_c(month) = clamp( round(base_cap_c * season_mult_c,month), MIN, MAX )

Writes:
  pa.hs2_cat_knee (cat, yearly, knee80, knee90, knee95, n_urls)   -- heavy, Redshift
  pa.hs2_cat_cap  (cat, calendar_month, base_cap, season_index, cap, yearly)

Run once end-to-end; then iterate clamps/alpha fast with --skip-knee (reads the
persisted knee table, no Redshift round-trip).
"""
from __future__ import annotations
import os, sys, argparse
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from backend.healthscore_service import (  # noqa: E402
    _load_env, _redshift, _postgres, _norm, _SEO_JOIN, _SEO_WHERE,
)

YEAR_LO, YEAR_HI = 20250701, 20260630        # trailing 12 complete months
KNEE_TABLE = "pa.hs2_cat_knee"
CAP_TABLE = "pa.hs2_cat_cap"
CAT_MONTH_TABLE = "pa.hs2_cat_month"

DEMO_CATS = [9000047, 9000066, 9000608, 9000953, 9002072,
             9005282, 9005317, 9001646, 9003581, 9000668]
DEMO_NAME_LIKE = ["slee", "vaz", "airco", "sneaker", "winterjas", "zwembad"]
_NL = {1:"jan",2:"feb",3:"mrt",4:"apr",5:"mei",6:"jun",7:"jul",8:"aug",9:"sep",10:"okt",11:"nov",12:"dec"}


def build_knee():
    nv = _norm("dv.url")
    sql = f"""
        WITH u AS (
            SELECT dv.deepest_subcat_id AS cat, {nv} AS npath, COUNT(*) AS v
            FROM datamart.fct_visits fv {_SEO_JOIN}
            WHERE fv.dim_date_key BETWEEN {YEAR_LO} AND {YEAR_HI} AND {_SEO_WHERE}
              AND dv.deepest_subcat_id IS NOT NULL
            GROUP BY 1, 2
        ),
        r AS (
            SELECT cat,
                   SUM(v) OVER (PARTITION BY cat ORDER BY v DESC
                                ROWS UNBOUNDED PRECEDING) AS cum,
                   SUM(v) OVER (PARTITION BY cat)          AS tot,
                   ROW_NUMBER() OVER (PARTITION BY cat ORDER BY v DESC) AS rn
            FROM u
        )
        SELECT cat, MAX(tot) AS yearly,
               MIN(CASE WHEN cum >= 0.80*tot THEN rn END) AS knee80,
               MIN(CASE WHEN cum >= 0.90*tot THEN rn END) AS knee90,
               MIN(CASE WHEN cum >= 0.95*tot THEN rn END) AS knee95,
               COUNT(*) AS n_urls
        FROM r GROUP BY cat
    """
    print(f"[caps] computing coverage-knee per category {YEAR_LO}-{YEAR_HI} (heavy) ...",
          file=sys.stderr)
    with _redshift() as rs, rs.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(sql)
        rows = c.fetchall()
    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {KNEE_TABLE} (
                cat BIGINT PRIMARY KEY, yearly BIGINT, knee80 INT, knee90 INT,
                knee95 INT, n_urls INT)""")
            c.execute(f"TRUNCATE {KNEE_TABLE}")
            execute_values(c, f"INSERT INTO {KNEE_TABLE} "
                              f"(cat,yearly,knee80,knee90,knee95,n_urls) VALUES %s",
                           [(r["cat"], int(r["yearly"] or 0), r["knee80"], r["knee90"],
                             r["knee95"], r["n_urls"]) for r in rows], page_size=10000)
        pg.commit()
    finally:
        pg.close()
    print(f"[caps] wrote {len(rows):,} rows to {KNEE_TABLE}", file=sys.stderr)


def build_caps(p, cap_min, cap_max, alpha, mmin, mmax):
    knee_col = {80: "knee80", 90: "knee90", 95: "knee95"}[p]
    pg = _postgres()
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"SELECT cat, yearly, knee80, knee90, knee95, n_urls FROM {KNEE_TABLE}")
            knee = {r["cat"]: r for r in c.fetchall()}
            c.execute(f"SELECT cat, yyyymm, visits FROM {CAT_MONTH_TABLE}")
            cm = defaultdict(dict)
            for r in c.fetchall():
                cm[r["cat"]][r["yyyymm"]] = int(r["visits"] or 0)
    finally:
        pg.close()

    # names from Redshift
    with _redshift() as rs, rs.cursor() as c:
        c.execute("""SELECT deepest_category_id, MAX(deepest_category_name)
                     FROM bt.new_hs_data WHERE country='nl'
                       AND deepest_category_id IS NOT NULL GROUP BY 1""")
        names = {r[0]: r[1] for r in c.fetchall()}

    def season_index(cat):
        mv = cm.get(cat, {})
        clim = defaultdict(list)
        for ym, v in mv.items():
            clim[ym % 100].append(v)
        cl = {m: sum(vs)/len(vs) for m, vs in clim.items()}
        base = (sum(cl.values())/len(cl)) if cl else 0
        return {m: (cl[m]/base if base else 1.0) for m in range(1, 13) if m in cl}, cl

    def clamp(x, lo, hi):
        return max(lo, min(hi, x))

    out_rows = []      # (cat, month, base_cap, season_index, cap, yearly)
    caps_by_cat = {}
    for cat, k in knee.items():
        kv = k[knee_col]
        if kv is None:
            continue
        base = int(clamp(kv, cap_min, cap_max))
        idx, _ = season_index(cat)
        monthly = {}
        for m in range(1, 13):
            si = idx.get(m, 1.0)
            mult = clamp(si ** alpha, mmin, mmax)
            cap = int(clamp(round(base * mult), cap_min, cap_max))
            monthly[m] = (cap, si)
            out_rows.append((cat, m, base, round(si, 3), cap, int(k["yearly"] or 0)))
        caps_by_cat[cat] = {"base": base, "yearly": int(k["yearly"] or 0),
                            "knee": k, "monthly": monthly}

    # persist
    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {CAP_TABLE} (
                cat BIGINT, calendar_month INT, base_cap INT, season_index DOUBLE PRECISION,
                cap INT, yearly BIGINT, PRIMARY KEY (cat, calendar_month))""")
            c.execute(f"TRUNCATE {CAP_TABLE}")
            execute_values(c, f"INSERT INTO {CAP_TABLE} "
                              f"(cat,calendar_month,base_cap,season_index,cap,yearly) VALUES %s",
                           out_rows, page_size=10000)
        pg.commit()
    finally:
        pg.close()

    # ---- report ----
    allknee = sorted(k[knee_col] for k in knee.values() if k[knee_col] is not None)
    def pctl(a, q): return a[min(len(a)-1, int(q*len(a)))]
    print(f"\nParams: P={p}%  base_clamp=[{cap_min},{cap_max}]  alpha={alpha}  "
          f"season_mult_clamp=[{mmin},{mmax}]")
    print(f"knee{p} distribution across {len(allknee):,} cats: "
          f"p50={pctl(allknee,.5):,}  p90={pctl(allknee,.9):,}  "
          f"p99={pctl(allknee,.99):,}  max={allknee[-1]:,}")

    demo = list(DEMO_CATS)
    for cat, nm in names.items():
        if nm and any(x in nm.lower() for x in DEMO_NAME_LIKE) and cat in caps_by_cat \
           and caps_by_cat[cat]["yearly"] > 500 and cat not in demo:
            demo.append(cat)
    demo = [c for c in demo if c in caps_by_cat]
    demo.sort(key=lambda c: -caps_by_cat[c]["yearly"])

    print(f"\n{'category':<34}{'yearly':>10}{'k80':>6}{'k90':>6}{'k95':>7}{'base':>6}"
          f"{'  low→peak cap':>16}{' (peak mo)':>10}")
    print("-"*100)
    for cat in demo:
        d = caps_by_cat[cat]; nm = names.get(cat) or "?"
        caps = {m: d["monthly"][m][0] for m in range(1, 13)}
        lo_m = min(caps, key=caps.get); hi_m = max(caps, key=caps.get)
        print(f"{cat} {nm[:27]:<28}{d['yearly']:>10,}{_n(d['knee']['knee80']):>6}"
              f"{_n(d['knee']['knee90']):>6}{_n(d['knee']['knee95']):>7}{d['base']:>6}"
              f"{caps[lo_m]:>7,} →{caps[hi_m]:>6,}{('  '+_NL[hi_m]):>10}")
    print(f"\nWrote {len(out_rows):,} cap rows ({len(caps_by_cat):,} cats x 12 months) to {CAP_TABLE}.")


def _n(x): return x if x is not None else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-knee", action="store_true", help="reuse persisted pa.hs2_cat_knee")
    ap.add_argument("--p", type=int, choices=[80, 90, 95], default=90)
    ap.add_argument("--cap-min", type=int, default=100)
    ap.add_argument("--cap-max", type=int, default=6000)
    ap.add_argument("--alpha", type=float, default=1.0)
    ap.add_argument("--mult-min", type=float, default=0.4)
    ap.add_argument("--mult-max", type=float, default=2.5)
    a = ap.parse_args()
    _load_env()
    if not a.skip_knee:
        build_knee()
    build_caps(a.p, a.cap_min, a.cap_max, a.alpha, a.mult_min, a.mult_max)
