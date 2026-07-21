"""
HS2.0 per-category yearly-volume + seasonality analysis.

Aggregates real SEO visits per deepest_category_id per calendar month over the
trailing 24 complete months, then derives:
  - yearly_visits   : trailing-12-month SEO visits per category
  - season_index[M] : category's avg visits in calendar month M / its avg month
                      (climatology over up to 2 years -> stable, not noise)

Writes pa.hs2_cat_month (cat, yyyymm, visits, revenue) for reuse and prints a
demo for a handful of categories (the 10 diff cats + a few seasonal examples).
"""
from __future__ import annotations
import os, sys
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from backend.healthscore_service import (  # noqa: E402
    _load_env, _redshift, _postgres, _SEO_JOIN, _SEO_WHERE, _REV,
)

WIN_LO, WIN_HI = 20240701, 20260630  # 24 complete months
CAT_MONTH_TABLE = "pa.hs2_cat_month"

DEMO_CATS = [9000047, 9000066, 9000608, 9000953, 9002072,
             9005282, 9005317, 9001646, 9003581, 9000668]
DEMO_NAME_LIKE = ["slee", "vaz", "airco", "sneaker", "winterjas", "zwembad", "kerst"]

_NL_MONTHS = {1:"jan",2:"feb",3:"mrt",4:"apr",5:"mei",6:"jun",
              7:"jul",8:"aug",9:"sep",10:"okt",11:"nov",12:"dec"}


def main():
    _load_env()
    sql = f"""
        SELECT dv.deepest_subcat_id AS cat,
               fv.dim_date_key / 100 AS yyyymm,
               COUNT(*)              AS visits,
               SUM({_REV})           AS revenue
        FROM datamart.fct_visits fv {_SEO_JOIN}
        WHERE fv.dim_date_key BETWEEN {WIN_LO} AND {WIN_HI} AND {_SEO_WHERE}
          AND dv.deepest_subcat_id IS NOT NULL
        GROUP BY 1, 2
    """
    print(f"[seasonality] aggregating SEO visits per cat x month {WIN_LO}-{WIN_HI} ...",
          file=sys.stderr)
    with _redshift() as rs, rs.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(sql)
        rows = c.fetchall()
        c.execute("""SELECT deepest_category_id, MAX(deepest_category_name) nm
                     FROM bt.new_hs_data WHERE country='nl'
                       AND deepest_category_id IS NOT NULL GROUP BY 1""")
        names = {r["deepest_category_id"]: r["nm"] for r in c.fetchall()}
    print(f"[seasonality] {len(rows):,} cat-month rows, {len(names):,} named cats",
          file=sys.stderr)

    # ---- persist for reuse ----
    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {CAT_MONTH_TABLE} (
                            cat BIGINT, yyyymm INT, visits BIGINT, revenue DOUBLE PRECISION,
                            PRIMARY KEY (cat, yyyymm))""")
            c.execute(f"TRUNCATE {CAT_MONTH_TABLE}")
            execute_values(c, f"INSERT INTO {CAT_MONTH_TABLE} (cat,yyyymm,visits,revenue) VALUES %s",
                           [(r["cat"], r["yyyymm"], int(r["visits"] or 0), float(r["revenue"] or 0))
                            for r in rows], page_size=10000)
        pg.commit()
    finally:
        pg.close()

    # ---- derive per-cat yearly volume + monthly climatology ----
    by_cat = defaultdict(dict)          # cat -> {yyyymm: visits}
    for r in rows:
        by_cat[r["cat"]][r["yyyymm"]] = int(r["visits"] or 0)

    last12 = {ym for ym in _month_range(20250701, 20260630)}
    stats = {}
    for cat, mv in by_cat.items():
        yearly = sum(v for ym, v in mv.items() if ym in last12)
        # climatology: mean visits per calendar month across the up-to-2 years present
        cm = defaultdict(list)
        for ym, v in mv.items():
            cm[ym % 100].append(v)
        clim = {m: (sum(vs) / len(vs)) for m, vs in cm.items()}
        baseline = (sum(clim.values()) / len(clim)) if clim else 0
        idx = {m: (clim[m] / baseline if baseline else 1.0) for m in clim}
        stats[cat] = {"yearly": yearly, "clim": clim, "idx": idx, "baseline": baseline}

    # ---- pick demo cats ----
    demo = list(DEMO_CATS)
    for cat, nm in names.items():
        if nm and any(k in nm.lower() for k in DEMO_NAME_LIKE) and cat in stats:
            if stats[cat]["yearly"] > 500 and cat not in demo:
                demo.append(cat)

    print("\n=== Per-category yearly SEO visits + seasonality index (1.0 = avg month) ===\n")
    hdr = f"{'category':<34}{'yearly':>10} | " + " ".join(f"{_NL_MONTHS[m]:>4}" for m in range(1,13)) + f" | {'peak':>5}"
    print(hdr); print("-"*len(hdr))
    demo = [c for c in demo if c in stats]
    demo.sort(key=lambda c: -stats[c]["yearly"])
    for cat in demo:
        s = stats[cat]; nm = (names.get(cat) or "?")
        label = f"{cat} {nm}"[:33]
        cells = []
        for m in range(1,13):
            v = s["idx"].get(m)
            cells.append(f"{v:>4.1f}" if v is not None else "   -")
        peak = max(s["idx"], key=s["idx"].get) if s["idx"] else 0
        print(f"{label:<34}{s['yearly']:>10,} | " + " ".join(cells) + f" | {_NL_MONTHS.get(peak,'-'):>5}")
    print(f"\nWrote {len(rows):,} rows to {CAT_MONTH_TABLE}. Total cats with data: {len(stats):,}")


def _month_range(lo, hi):
    y, m = lo // 100, lo % 100
    ey, em = hi // 100, hi % 100
    out = []
    while (y, m) <= (ey, em):
        out.append(y*100+m)
        m += 1
        if m > 12: m = 1; y += 1
    return out


if __name__ == "__main__":
    main()
