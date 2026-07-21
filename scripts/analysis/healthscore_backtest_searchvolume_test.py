"""
HS2.0 Phase 3 step 2 test — does SEARCH VOLUME earn weight in the blend?
Adds f_vol (R-url r_term -> keyword cache volume) as a 6th feature and refits
the blend (June split). If f_vol ~ 0, search volume doesn't help coverage.
"""
import sys
import numpy as np, pandas as pd, psycopg2
sys.path.insert(0, '/home/joepvanschagen/projects/dm-dashboard/backend')
from keyword_planner_service import clean_keyword

BLEND = 0.5
def g(k):
    for line in open('/home/joepvanschagen/projects/dm-dashboard/.env'):
        if line.startswith(k + '='):
            return line.split('=', 1)[1].strip().strip('"').strip("'")

NORM = "rtrim(split_part(split_part(lower(regexp_replace(dv.url,'^https?://[^/]+','')),'?',1),'#',1),'/')"
REV = "COALESCE(fv.ww_revenue,0)+COALESCE(fv.cpc_revenue,0)+COALESCE(fv.affiliate_revenue,0)"
WIN_LO, AS_OF, REC_LO, PRI_LO, PRI_HI = 20260303, 20260531, 20260518, 20260504, 20260517
JUN_LO, JUN_HI = 20260601, 20260630
SEO = ("JOIN datamart.dim_visit dv ON fv.dim_visit_key=dv.dim_visit_key "
       "JOIN chan_deriv.ref_channel_derivation_stats c ON dv.aff_id=c.aff_id AND dv.channel_id=c.channel_id "
       "WHERE dv.is_real_visit=1 AND c.marketing_channel='SEO' AND dv.url ~ '^https?://www\\.beslist\\.nl/'")
PRED_SQL = f"""
  SELECT {NORM} AS npath, MAX(COALESCE(dv.type_url,'x')) AS type_url, MAX(dv.deepest_subcat_id) AS cat,
         MAX(CASE WHEN dv.type_url='R-url' THEN dv.r_terms END) AS rterm,
         COUNT(*) AS visits,
         SUM(COALESCE(fv.number_of_bvb_clicks,0)+COALESCE(fv.number_of_outclicks,0)) AS clicks,
         SUM(CASE WHEN COALESCE(fv.number_of_cpc_productclicks,0)=0 AND COALESCE(fv.number_of_ww_productclicks,0)=0 THEN 1 ELSE 0 END) AS noprod,
         SUM({REV}) AS revenue,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {REC_LO} AND {AS_OF} THEN 1 ELSE 0 END) AS rec,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {PRI_LO} AND {PRI_HI} THEN 1 ELSE 0 END) AS pri
  FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN {WIN_LO} AND {AS_OF} GROUP BY 1
"""
TGT_SQL = f"SELECT {NORM} AS npath, COUNT(*) AS jv, SUM({REV}) AS jr FROM datamart.fct_visits fv {SEO} AND fv.dim_date_key BETWEEN {JUN_LO} AND {JUN_HI} GROUP BY 1"

def pull(sql, lbl, dsn=None):
    if dsn:
        conn = psycopg2.connect(dsn=dsn, connect_timeout=30)
    else:
        conn = psycopg2.connect(host=g('REDSHIFT_HOST'), port=g('REDSHIFT_PORT'), dbname=g('REDSHIFT_DB'),
                                user=g('REDSHIFT_USER'), password=g('REDSHIFT_PASSWORD'), connect_timeout=40)
    try: df = pd.read_sql(sql, conn)
    finally: conn.close()
    print(f"[pull] {lbl}: {len(df):,}", file=sys.stderr); return df

pred = pull(PRED_SQL, "predictor")
tgt = pull(TGT_SQL, "target")
kw = pull("SELECT keyword_norm, search_volume FROM pa.hs_keyword_search_volume", "kw cache", dsn=g('DATABASE_URL'))
volmap = dict(zip(kw.keyword_norm, kw.search_volume))
TOT_V, TOT_R = tgt.jv.sum(), tgt.jr.sum()

pred = pred[pred.visits > 0].copy()
pred['f_visits'] = np.log1p(pred.visits)
pred['f_ctr'] = pred.clicks / pred.visits
pred['f_eng'] = 1 - pred.noprod / pred.visits
pred['f_rev'] = np.log1p(pred.revenue.clip(lower=0))
pred['f_mom'] = np.log((pred['rec'] + 1) / (pred['pri'] + 1))
def vol_of(rt):
    if not rt: return 0
    return volmap.get(clean_keyword(rt), 0)
pred['sv'] = pred['rterm'].map(vol_of)
pred['f_vol'] = np.log1p(pred['sv'].clip(lower=0))
cov_r = (pred[pred.type_url == 'R-url']['sv'] > 0).mean()
print(f"\nR-url rows with a matched search volume: {100*cov_r:.1f}%", file=sys.stderr)

FEATS = ['f_visits', 'f_ctr', 'f_eng', 'f_rev', 'f_mom', 'f_vol']
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

WN, cand = 300, [0.0, 0.5, 1.0, 2.0, 4.0]; w = [1.0] * 6; best = obj(w, WN)
for _ in range(4):
    for i in range(6):
        bv = w[i]
        for v in cand:
            w[i] = v; o = obj(w, WN)
            if o > best: best, bv = o, v
        w[i] = bv
wn = np.array(w) / (sum(w) or 1)
vc, rc = cov(w, WN)
print("\n=== 6-feature blend (with search volume), June split ===")
print("weights:", {f: round(x, 3) for f, x in zip(FEATS, wn)})
print(f"@N=300: visit {100*vc:.1f}%  revenue {100*rc:.1f}%   (5-feat was 60.7/65.4)")
