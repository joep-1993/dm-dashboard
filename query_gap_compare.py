import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-tools/.env')
import psycopg2
from psycopg2.extras import execute_values

# Base = query.txt body, date range 20260301-20260603, parameterized threshold
def build(thr):
    return f"""
select dv.main_cat_name, dv.deepest_subcat_name,
       SPLIT_PART(dv.url, '?', 1) as url,
       count(*) as visits,
       sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) as revenue
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key = dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key = dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id = chan.aff_id and dv.channel_id = chan.channel_id
where dv.is_real_visit = 1 and chan.marketing_channel = 'SEO'
  and fcv.dim_date_key between 20260301 and 20260603
  and dv.url like '%beslist.nl%' and dv.url like '%/c/%'
  and dv.url not like '%/r/%' and dv.url not like '%/l/%'
  and dv.url not like '%/page_%' and dv.url not like '%#%'
group by 3,2,1
having visits > {thr}
"""

KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"

for thr in (4, 3):
    rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                          dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                          password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
    rc = rs.cursor(); rc.execute(build(thr)); rows = rc.fetchall(); rc.close(); rs.close()
    tv = sum(int(r[3]) for r in rows); tr = sum(float(r[4] or 0) for r in rows)
    pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'))
    c = pg.cursor()
    c.execute("DROP TABLE IF EXISTS q; CREATE TEMP TABLE q (raw_url text, visits bigint, revenue double precision)")
    execute_values(c, "INSERT INTO q VALUES %s", [(r[2], int(r[3]), float(r[4] or 0)) for r in rows], page_size=5000)
    c.execute("DROP TABLE IF EXISTS qm; CREATE TEMP TABLE qm AS SELECT q.visits, q.revenue, u.url_id FROM q LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)")
    c.execute(f"SELECT count(*), coalesce(sum(visits),0), coalesce(sum(revenue),0) FROM qm WHERE url_id IS NOT NULL AND url_id NOT IN {KOP} AND url_id NOT IN {FAQ}")
    ng, vg, rg = c.fetchone(); vg = int(vg); rg = float(rg)
    c.execute("SELECT count(*) FROM qm WHERE url_id IS NULL"); nun = c.fetchone()[0]
    c.close()
    print(f"\n=== visits > {thr}  (>= {thr+1} visits) | dates 2026-03-01 .. 2026-06-03 ===")
    print(f"  query rows (distinct URLs): {len(rows):,} | visits {tv:,} | revenue EUR {tr:,.2f}")
    print(f"  NOT in Kopteksten AND NOT in FAQ: {ng:,} urls "
          f"({100*ng/len(rows):.1f}%) | {vg:,} visits ({100*vg/tv:.1f}%) | EUR {rg:,.2f} ({100*rg/tr:.1f}%)")
    print(f"  (+ {nun:,} URLs not in pa.urls at all -> also content-less)")
    pg.close()
