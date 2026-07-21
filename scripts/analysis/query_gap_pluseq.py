import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')
import psycopg2
from psycopg2.extras import execute_values

SQL = """
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
having visits > 3
"""

rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SQL); rows = rc.fetchall(); rs.close()
print(f"query rows: {len(rows):,}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL')); c = pg.cursor()
c.execute("CREATE TEMP TABLE q (raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO q VALUES %s", [(r[2], int(r[3]), float(r[4] or 0)) for r in rows], page_size=5000)
KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"
# gap set = matched to pa.urls but in neither content table
c.execute(f"""CREATE TEMP TABLE gap AS
  SELECT q.raw_url, q.visits, q.revenue
  FROM q JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)
  WHERE u.url_id NOT IN {KOP} AND u.url_id NOT IN {FAQ}""")
c.execute("SELECT count(*) FROM gap"); print("gap urls (in pa.urls, no content):", f"{c.fetchone()[0]:,}")

for sym,label in [('+','PLUS'),('=','EQUALS')]:
    c.execute("SELECT count(*), coalesce(sum(visits),0) FROM gap WHERE raw_url LIKE %s", (f'%{sym}%',))
    n,v = c.fetchone()
    print(f"\n=== gap URLs containing '{sym}' ({label}): {n:,} urls, {int(v):,} visits ===")
    c.execute("SELECT raw_url, visits FROM gap WHERE raw_url LIKE %s ORDER BY visits DESC LIMIT 15", (f'%{sym}%',))
    for u,vis in c.fetchall(): print(f"   {vis:>5}  {u}")
# either
c.execute("SELECT count(*) FROM gap WHERE raw_url LIKE '%+%' OR raw_url LIKE '%=%'")
print(f"\nTOTAL gap URLs with '+' OR '=': {c.fetchone()[0]:,}")
pg.close()
