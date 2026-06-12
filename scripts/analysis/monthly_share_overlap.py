import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'))
c = pg.cursor()

UT  = "(SELECT url_id FROM pa.unique_titles_content WHERE coalesce(title,'')<>'' OR coalesce(h1_title,'')<>'')"
KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"

# ---- Catalog-wide ----
c.execute(f"SELECT count(*) FROM pa.urls WHERE url_id IN {UT}")
ut_all = c.fetchone()[0]
c.execute(f"SELECT count(*) FROM pa.urls WHERE url_id IN {UT} AND (url_id IN {KOP} OR url_id IN {FAQ})")
ut_overlap_all = c.fetchone()[0]
print("CATALOG-WIDE (all pa.urls):")
print(f"  Unique-titles URLs:                              {ut_all:,}")
print(f"  ...also in Kopteksten OR FAQ (kept):             {ut_overlap_all:,}")
print(f"  ...NOT in either (dropped):                      {ut_all-ut_overlap_all:,}")

# ---- Within May SEO matched set ----
print("\nRebuilding May SEO matched set...")
import psycopg2 as pp
rs = pp.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor()
rc.execute("""
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
group by 1 having count(*)>4
""")
rows = rc.fetchall(); rs.close()
total_visits = sum(r[1] for r in rows); total_rev = sum(float(r[2] or 0) for r in rows)
from psycopg2.extras import execute_values
c.execute("CREATE TEMP TABLE seo_may (raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO seo_may VALUES %s", [(r[0],int(r[1]),float(r[2] or 0)) for r in rows], page_size=5000)
c.execute("""CREATE TEMP TABLE m AS
  SELECT s.visits, s.revenue, u.url_id FROM seo_may s
  LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(s.raw_url) WHERE u.url_id IS NOT NULL""")

def stat(where):
    c.execute(f"SELECT count(*), coalesce(sum(visits),0), coalesce(sum(revenue),0) FROM m WHERE {where}")
    n,v,r = c.fetchone(); return n,int(v),float(r)

n_ut,v_ut,r_ut       = stat(f"url_id IN {UT}")
n_ov,v_ov,r_ov       = stat(f"url_id IN {UT} AND (url_id IN {KOP} OR url_id IN {FAQ})")
n_dr,v_dr,r_dr       = n_ut-n_ov, v_ut-v_ov, r_ut-r_ov

print("\nWITHIN MAY SEO TRAFFIC (denominator: %s visits / EUR %s):" % (f"{total_visits:,}", f"{total_rev:,.2f}"))
print(f"  Unique-titles URLs:                  {n_ut:,} urls | {v_ut:,} visits ({100*v_ut/total_visits:.1f}%) | EUR {r_ut:,.2f} ({100*r_ut/total_rev:.1f}%)")
print(f"  KEPT (also in Kopteksten OR FAQ):    {n_ov:,} urls | {v_ov:,} visits ({100*v_ov/total_visits:.1f}%) | EUR {r_ov:,.2f} ({100*r_ov/total_rev:.1f}%)")
print(f"  DROPPED (in neither):                {n_dr:,} urls | {v_dr:,} visits ({100*v_dr/total_visits:.1f}%) | EUR {r_dr:,.2f} ({100*r_dr/total_rev:.1f}%)")
pg.close()
