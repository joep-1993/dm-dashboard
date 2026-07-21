"""
HS2.0 Phase 5 — shadow run: HS2.0 method vs the live system, out-of-sample.
Predictor <= May 2026 -> select scored top-N/cat. Compare coverage of June SEO
visits/revenue against the CURRENT live set (bt.new_hs_data 'Juni 2026').
Plus overlap: what HS2.0 ADDS (only-hs2) vs DROPS (only-current).
"""
import sys
import numpy as np, pandas as pd, psycopg2

CAP_N, W_V, W_R = 1000, 0.889, 0.111
def g(k):
    for line in open('/home/joepvanschagen/projects/dm-dashboard/.env'):
        if line.startswith(k + '='):
            return line.split('=', 1)[1].strip().strip('"').strip("'")

NV = "rtrim(split_part(split_part(lower(regexp_replace(dv.url,'^https?://[^/]+','')),'?',1),'#',1),'/')"
NN = "rtrim(split_part(split_part(lower(regexp_replace(nh.url,'^https?://[^/]+','')),'?',1),'#',1),'/')"
REV = "COALESCE(fv.ww_revenue,0)+COALESCE(fv.cpc_revenue,0)+COALESCE(fv.affiliate_revenue,0)"
SEO = ("JOIN datamart.dim_visit dv ON fv.dim_visit_key=dv.dim_visit_key "
       "JOIN chan_deriv.ref_channel_derivation_stats c ON dv.aff_id=c.aff_id AND dv.channel_id=c.channel_id "
       "WHERE dv.is_real_visit=1 AND c.marketing_channel='SEO' AND dv.url ~ '^https?://www\\.beslist\\.nl/'")
PRED = f"""SELECT {NV} npath, MAX(dv.deepest_subcat_id) cat, COUNT(*) visits, SUM({REV}) revenue
  FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN 20260303 AND 20260531 GROUP BY 1"""
HOLD = f"""SELECT {NV} npath, COUNT(*) jv, SUM({REV}) jr
  FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN 20260601 AND 20260630 GROUP BY 1"""
CUR = f"""SELECT DISTINCT {NN} npath FROM bt.new_hs_data nh
  WHERE regexp_replace(lower(trim(nh.current_month_year)),'[[:space:]]+',' ')='juni 2026'
    AND nh.country='nl' AND nh.url IS NOT NULL AND nh.url<>''"""

def pull(sql, lbl):
    rs = psycopg2.connect(host=g('REDSHIFT_HOST'), port=g('REDSHIFT_PORT'), dbname=g('REDSHIFT_DB'),
                          user=g('REDSHIFT_USER'), password=g('REDSHIFT_PASSWORD'), connect_timeout=60)
    try: df = pd.read_sql(sql, rs)
    finally: rs.close()
    print(f"[pull] {lbl}: {len(df):,}", file=sys.stderr); return df

pred = pull(PRED, "predictor <=May"); hold = pull(HOLD, "holdout June"); cur = pull(CUR, "current live set")
TOT_V, TOT_R = hold.jv.sum(), hold.jr.sum()

# HS2.0 scored selection (top-N/cat)
pred = pred[pred.visits > 0].copy()
pred['fv'] = np.log1p(pred.visits); pred['fr'] = np.log1p(pred.revenue.clip(lower=0))
pred['score'] = W_V * pred.groupby('cat')['fv'].rank(pct=True) + W_R * pred.groupby('cat')['fr'].rank(pct=True)
pred['rnk'] = pred.groupby('cat')['score'].rank(method='first', ascending=False)
hs2 = set(pred.loc[pred.rnk <= CAP_N, 'npath'])
curs = set(cur.npath)
print(f"\nSet sizes — HS2.0 scored: {len(hs2):,} | current live: {len(curs):,}")

def cover(S):
    m = hold.npath.isin(S)
    return hold.loc[m, 'jv'].sum() / TOT_V, hold.loc[m, 'jr'].sum() / TOT_R

v_hs2, r_hs2 = cover(hs2); v_cur, r_cur = cover(curs)
print(f"\nJune coverage (out-of-sample):")
print(f"  Current live set : visits {100*v_cur:5.1f}%   revenue {100*r_cur:5.1f}%")
print(f"  HS2.0 scored     : visits {100*v_hs2:5.1f}%   revenue {100*r_hs2:5.1f}%")
print(f"  Delta            : visits {100*(v_hs2-v_cur):+5.1f}pp  revenue {100*(r_hs2-r_cur):+5.1f}pp")

# overlap: traffic ADDED vs DROPPED
only_hs2 = hs2 - curs; only_cur = curs - hs2; both = hs2 & curs
print(f"\nOverlap: both {len(both):,} | only-HS2.0 {len(only_hs2):,} | only-current {len(only_cur):,}")
add_v = hold.loc[hold.npath.isin(only_hs2), 'jv'].sum()
drop_v = hold.loc[hold.npath.isin(only_cur), 'jv'].sum()
add_r = hold.loc[hold.npath.isin(only_hs2), 'jr'].sum()
drop_r = hold.loc[hold.npath.isin(only_cur), 'jr'].sum()
print(f"  HS2.0 ADDS (only-HS2.0)   : {add_v:,.0f} June visits ({100*add_v/TOT_V:.1f}pp), rev {add_r:,.0f}")
print(f"  HS2.0 DROPS (only-current): {drop_v:,.0f} June visits ({100*drop_v/TOT_V:.1f}pp), rev {drop_r:,.0f}")
