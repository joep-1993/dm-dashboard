import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')
import psycopg2
from psycopg2.extras import execute_values

# Total May 2026 SEO performance on /c/ URLs (no visit threshold) — aggregated per URL
SQL = """
select SPLIT_PART(dv.url,'?',1) as url, count(*) as visits,
       sum(fcv.cpc_revenue)+sum(fcv.ww_revenue) as revenue
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key=dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key=dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id=chan.aff_id and dv.channel_id=chan.channel_id
where dv.is_real_visit=1 and chan.marketing_channel='SEO'
  and fcv.dim_date_key between 20260501 and 20260531
  and dv.url like '%beslist.nl%' and dv.url like '%/c/%'
  and dv.url not like '%/r/%' and dv.url not like '%/l/%'
  and dv.url not like '%/page_%' and dv.url not like '%#%'
group by 1
"""
print("Querying Redshift: ALL May 2026 SEO /c/ performance...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SQL); rows = rc.fetchall(); rs.close()
total_v = sum(int(r[1]) for r in rows); total_r = sum(float(r[2] or 0) for r in rows)
print(f"  /c/ URLs: {len(rows):,} | TOTAL visits {total_v:,} | TOTAL revenue EUR {total_r:,.2f}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'), keepalives=1, keepalives_idle=10,
                      keepalives_interval=5, keepalives_count=5)
pg.autocommit = True
c = pg.cursor()
c.execute("CREATE TEMP TABLE q (raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO q VALUES %s", [(r[0], int(r[1]), float(r[2] or 0)) for r in rows], page_size=5000)
c.execute("ANALYZE q")

# Numerator: on URLs present in kopteksten_jobs (the 397,652 universe)
c.execute("""
  SELECT count(*), coalesce(sum(q.visits),0), coalesce(sum(q.revenue),0)
  FROM q
  JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)
  WHERE u.url_id IN (SELECT url_id FROM pa.kopteksten_jobs)
""")
k_urls, k_v, k_r = c.fetchone(); k_v=int(k_v); k_r=float(k_r)

# For context: URLs matched to pa.urls at all
c.execute("""SELECT coalesce(sum(q.visits),0), coalesce(sum(q.revenue),0)
             FROM q JOIN pa.urls u ON u.url=pa.canonicalize_url(q.raw_url)""")
m_v, m_r = c.fetchone(); m_v=int(m_v); m_r=float(m_r)

print("\n" + "="*64)
print("MAY 2026 — Kopteksten universe (397,652 URLs) share of SEO /c/")
print("="*64)
print(f"  Denominator (ALL SEO /c/):  {total_v:,} visits | EUR {total_r:,.2f}")
print(f"  On Kopteksten URLs:         {k_v:,} visits | EUR {k_r:,.2f}   ({k_urls:,} urls)")
print(f"  --> SHARE:                  {100*k_v/total_v:.1f}% of visits | {100*k_r/total_r:.1f}% of revenue")
print(f"\n  (matched to pa.urls at all: {m_v:,} visits = {100*m_v/total_v:.1f}% | EUR {m_r:,.2f} = {100*m_r/total_r:.1f}%)")
pg.close()
