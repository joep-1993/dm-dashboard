"""
HS2.0 Phase 3 (blended objective) — maximize 0.5*visit_cov + 0.5*revenue_cov.
Predictor: features as-of 2026-05-31 (90d). Holdout: complete June 2026 (visits + revenue).
"""
import sys
import numpy as np
import pandas as pd
import psycopg2

BLEND = 0.5  # weight on visit coverage; (1-BLEND) on revenue coverage

def g(k):
    for line in open('/home/joepvanschagen/projects/dm-dashboard/.env'):
        if line.startswith(k + '='):
            return line.split('=', 1)[1].strip().strip('"').strip("'")

NORM = "rtrim(split_part(split_part(lower(regexp_replace(dv.url,'^https?://[^/]+','')),'?',1),'#',1),'/')"
WIN_LO, AS_OF = 20260303, 20260531
REC_LO, PRI_LO, PRI_HI = 20260518, 20260504, 20260517
JUN_LO, JUN_HI = 20260601, 20260630
REV = "COALESCE(fv.ww_revenue,0)+COALESCE(fv.cpc_revenue,0)+COALESCE(fv.affiliate_revenue,0)"

PRED_SQL = f"""
  SELECT {NORM} AS npath, MAX(COALESCE(dv.type_url,'(none)')) AS type_url,
         MAX(dv.deepest_subcat_id) AS cat, COUNT(*) AS visits,
         SUM(COALESCE(fv.number_of_bvb_clicks,0)+COALESCE(fv.number_of_outclicks,0)) AS clicks,
         SUM(CASE WHEN COALESCE(fv.number_of_cpc_productclicks,0)=0
                   AND COALESCE(fv.number_of_ww_productclicks,0)=0 THEN 1 ELSE 0 END) AS noprod,
         SUM({REV}) AS revenue,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {REC_LO} AND {AS_OF} THEN 1 ELSE 0 END) AS rec,
         SUM(CASE WHEN fv.dim_date_key BETWEEN {PRI_LO} AND {PRI_HI} THEN 1 ELSE 0 END) AS pri
  FROM datamart.fct_visits fv
  JOIN datamart.dim_visit dv ON fv.dim_visit_key=dv.dim_visit_key
  JOIN chan_deriv.ref_channel_derivation_stats c ON dv.aff_id=c.aff_id AND dv.channel_id=c.channel_id
  WHERE fv.dim_date_key BETWEEN {WIN_LO} AND {AS_OF} AND dv.is_real_visit=1
    AND c.marketing_channel='SEO' AND dv.url ~ '^https?://www\\.beslist\\.nl/'
  GROUP BY 1
"""
TGT_SQL = f"""
  SELECT {NORM} AS npath, COUNT(*) AS jvisits, SUM({REV}) AS jrev
  FROM datamart.fct_visits fv
  JOIN datamart.dim_visit dv ON fv.dim_visit_key=dv.dim_visit_key
  JOIN chan_deriv.ref_channel_derivation_stats c ON dv.aff_id=c.aff_id AND dv.channel_id=c.channel_id
  WHERE fv.dim_date_key BETWEEN {JUN_LO} AND {JUN_HI} AND dv.is_real_visit=1
    AND c.marketing_channel='SEO' AND dv.url ~ '^https?://www\\.beslist\\.nl/'
  GROUP BY 1
"""

def pull(sql, label):
    print(f"[pull] {label} ...", file=sys.stderr)
    rs = psycopg2.connect(host=g('REDSHIFT_HOST'), port=g('REDSHIFT_PORT'), dbname=g('REDSHIFT_DB'),
                          user=g('REDSHIFT_USER'), password=g('REDSHIFT_PASSWORD'), connect_timeout=40)
    try:
        df = pd.read_sql(sql, rs)
    finally:
        rs.close()
    print(f"[pull] {label}: {len(df):,} rows", file=sys.stderr)
    return df

pred = pull(PRED_SQL, "predictor")
tgt = pull(TGT_SQL, "target June")
TOT_V, TOT_R = tgt.jvisits.sum(), tgt.jrev.sum()
print(f"\nJune totals — visits: {TOT_V:,}  revenue: {TOT_R:,.0f}")

pred = pred[pred.visits > 0].copy()
pred['ctr'] = pred.clicks / pred.visits
pred['bounce'] = pred.noprod / pred.visits
pred['f_visits'] = np.log1p(pred.visits)
pred['f_ctr'] = pred.ctr
pred['f_eng'] = 1 - pred.bounce
pred['f_rev'] = np.log1p(pred.revenue.clip(lower=0))
pred['f_mom'] = np.log((pred['rec'] + 1) / (pred['pri'] + 1))
FEATS = ['f_visits', 'f_ctr', 'f_eng', 'f_rev', 'f_mom']
for f in FEATS:
    pred[f'p_{f}'] = pred.groupby('cat')[f].rank(pct=True)
P = pred[[f'p_{f}' for f in FEATS]].fillna(0.5).to_numpy()

pred = pred.merge(tgt, on='npath', how='left')
pred['jvisits'] = pred.jvisits.fillna(0.0); pred['jrev'] = pred.jrev.fillna(0.0)
jv, jr, cat = pred.jvisits.to_numpy(), pred.jrev.to_numpy(), pred.cat.to_numpy()

print(f"Reachable ceiling — visits {100*jv.sum()/TOT_V:.1f}% | revenue {100*jr.sum()/TOT_R:.1f}%\n")

def cov_at(weights, N):
    score = P @ np.asarray(weights, float)
    d = pd.DataFrame({'cat': cat, 's': score})
    rank = d.groupby('cat')['s'].rank(method='first', ascending=False).to_numpy()
    m = rank <= N
    return jv[m].sum() / TOT_V, jr[m].sum() / TOT_R, int(m.sum())

def obj(weights, N):
    vc, rc, _ = cov_at(weights, N)
    return BLEND * vc + (1 - BLEND) * rc

WORK_N, cand = 300, [0.0, 0.5, 1.0, 2.0, 4.0]
w = [1.0] * len(FEATS)
best = obj(w, WORK_N)
for rnd in range(4):
    for i in range(len(FEATS)):
        bv = w[i]
        for v in cand:
            w[i] = v
            o = obj(w, WORK_N)
            if o > best:
                best, bv = o, v
        w[i] = bv
    vc, rc, _ = cov_at(w, WORK_N)
    print(f"[round {rnd+1}] w={['%.1f'%x for x in w]} blend={100*best:.1f}% (vis {100*vc:.1f}% rev {100*rc:.1f}%)",
          file=sys.stderr)

wn = np.array(w) / (sum(w) or 1)
print("Backtested BLEND weights (normalized):")
for f, x in zip(FEATS, wn):
    print(f"   {f:<10} {x:.3f}")

print(f"\nCoverage vs per-category cap N (blend obj; baseline current June vis=45.1%):")
print(f"{'N':>6}{'set':>11}{'visit%':>9}{'rev%':>9}")
for N in [50, 100, 200, 300, 500, 1000, 3000, 10000]:
    vc, rc, sz = cov_at(w, N)
    print(f"{N:>6}{sz:>11,}{100*vc:>9.1f}{100*rc:>9.1f}")

CH = 300
score = P @ np.array(w)
d = pd.DataFrame({'cat': cat, 's': score, 'jv': jv, 'jr': jr, 't': pred.type_url.to_numpy()})
d['sel'] = d.groupby('cat')['s'].rank(method='first', ascending=False) <= CH
t2 = tgt.merge(pred[['npath', 'type_url']], on='npath', how='left')
t2['type_url'] = t2.type_url.fillna('(cold-start)')
dv_ = t2.groupby('type_url')[['jvisits', 'jrev']].sum()
nv_ = d[d.sel].groupby('t')[['jv', 'jr']].sum()
print(f"\nPer-type at N={CH}:")
print(f"{'type_url':<26}{'vis%':>8}{'rev%':>8}")
for t in dv_.sort_values('jvisits', ascending=False).index:
    dvis, drev = dv_.loc[t, 'jvisits'], dv_.loc[t, 'jrev']
    nvis = nv_.loc[t, 'jv'] if t in nv_.index else 0
    nrev = nv_.loc[t, 'jr'] if t in nv_.index else 0
    print(f"{str(t):<26}{(100*nvis/dvis if dvis else 0):>8.1f}{(100*nrev/drev if drev else 0):>8.1f}")
