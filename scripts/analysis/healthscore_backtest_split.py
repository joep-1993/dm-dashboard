"""
HS2.0 Phase 3 — parameterized blend backtest for robustness across splits.
Usage: hs_backtest_split.py --as-of YYYY-MM-DD --holdout YYYY-MM
"""
import sys, argparse
from datetime import date, timedelta
import numpy as np, pandas as pd, psycopg2

BLEND = 0.5

def g(k):
    for line in open('/home/joepvanschagen/projects/dm-tools/.env'):
        if line.startswith(k + '='):
            return line.split('=', 1)[1].strip().strip('"').strip("'")

ap = argparse.ArgumentParser()
ap.add_argument('--as-of', required=True)
ap.add_argument('--holdout', required=True)
a = ap.parse_args()

def key(d): return d.year * 10000 + d.month * 100 + d.day
as_of = date.fromisoformat(a.__dict__['as_of'])
WIN_LO, AS_OF = key(as_of - timedelta(days=89)), key(as_of)
REC_LO = key(as_of - timedelta(days=13))
PRI_HI = key(as_of - timedelta(days=14)); PRI_LO = key(as_of - timedelta(days=27))
hy, hm = (int(x) for x in a.holdout.split('-'))
H_LO = hy * 10000 + hm * 100 + 1; H_HI = hy * 10000 + hm * 100 + 31
NORM = "rtrim(split_part(split_part(lower(regexp_replace(dv.url,'^https?://[^/]+','')),'?',1),'#',1),'/')"
REV = "COALESCE(fv.ww_revenue,0)+COALESCE(fv.cpc_revenue,0)+COALESCE(fv.affiliate_revenue,0)"
SEO = ("JOIN datamart.dim_visit dv ON fv.dim_visit_key=dv.dim_visit_key "
       "JOIN chan_deriv.ref_channel_derivation_stats c ON dv.aff_id=c.aff_id AND dv.channel_id=c.channel_id "
       "WHERE dv.is_real_visit=1 AND c.marketing_channel='SEO' AND dv.url ~ '^https?://www\\.beslist\\.nl/'")

PRED_SQL = f"""
  SELECT {NORM} AS npath, MAX(dv.deepest_subcat_id) AS cat, COUNT(*) AS visits,
         SUM(COALESCE(fv.number_of_bvb_clicks,0)+COALESCE(fv.number_of_outclicks,0)) AS clicks,
         SUM(CASE WHEN COALESCE(fv.number_of_cpc_productclicks,0)=0 AND COALESCE(fv.number_of_ww_productclicks,0)=0 THEN 1 ELSE 0 END) AS noprod,
         SUM({REV}) AS revenue,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {REC_LO} AND {AS_OF} THEN 1 ELSE 0 END) AS rec,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {PRI_LO} AND {PRI_HI} THEN 1 ELSE 0 END) AS pri
  FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN {WIN_LO} AND {AS_OF} GROUP BY 1
"""
TGT_SQL = f"SELECT {NORM} AS npath, COUNT(*) AS jv, SUM({REV}) AS jr FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN {H_LO} AND {H_HI} GROUP BY 1"

def pull(sql, lbl):
    rs = psycopg2.connect(host=g('REDSHIFT_HOST'), port=g('REDSHIFT_PORT'), dbname=g('REDSHIFT_DB'),
                          user=g('REDSHIFT_USER'), password=g('REDSHIFT_PASSWORD'), connect_timeout=40)
    try: df = pd.read_sql(sql, rs)
    finally: rs.close()
    print(f"[pull] {lbl}: {len(df):,}", file=sys.stderr); return df

print(f"SPLIT: predictor as-of {as_of} (90d)  ->  holdout {a.holdout}", file=sys.stderr)
pred = pull(PRED_SQL, "predictor"); tgt = pull(TGT_SQL, "target")
TOT_V, TOT_R = tgt.jv.sum(), tgt.jr.sum()
pred = pred[pred.visits > 0].copy()
pred['f_visits'] = np.log1p(pred.visits)
pred['f_ctr'] = pred.clicks / pred.visits
pred['f_eng'] = 1 - pred.noprod / pred.visits
pred['f_rev'] = np.log1p(pred.revenue.clip(lower=0))
pred['f_mom'] = np.log((pred['rec'] + 1) / (pred['pri'] + 1))
FEATS = ['f_visits', 'f_ctr', 'f_eng', 'f_rev', 'f_mom']
for f in FEATS: pred[f'p_{f}'] = pred.groupby('cat')[f].rank(pct=True)
P = pred[[f'p_{f}' for f in FEATS]].fillna(0.5).to_numpy()
pred = pred.merge(tgt, on='npath', how='left'); pred['jv'] = pred.jv.fillna(0.0); pred['jr'] = pred.jr.fillna(0.0)
jv, jr, cat = pred.jv.to_numpy(), pred.jr.to_numpy(), pred.cat.to_numpy()

def cov(w, N):
    s = P @ np.asarray(w, float)
    d = pd.DataFrame({'cat': cat, 's': s})
    m = (d.groupby('cat')['s'].rank(method='first', ascending=False) <= N).to_numpy()
    return jv[m].sum() / TOT_V, jr[m].sum() / TOT_R
def obj(w, N):
    vc, rc = cov(w, N); return BLEND * vc + (1 - BLEND) * rc

WN, cand = 300, [0.0, 0.5, 1.0, 2.0, 4.0]; w = [1.0] * 5; best = obj(w, WN)
for _ in range(4):
    for i in range(5):
        bv = w[i]
        for v in cand:
            w[i] = v; o = obj(w, WN)
            if o > best: best, bv = o, v
        w[i] = bv
wn = np.array(w) / (sum(w) or 1)
vc, rc = cov(w, WN)
print(f"\n=== {a.holdout} holdout ===  ceiling vis {100*jv.sum()/TOT_V:.1f}% rev {100*jr.sum()/TOT_R:.1f}%")
print("weights:", {f: round(x, 3) for f, x in zip(FEATS, wn)})
print(f"@N=300: visit {100*vc:.1f}%  revenue {100*rc:.1f}%")
