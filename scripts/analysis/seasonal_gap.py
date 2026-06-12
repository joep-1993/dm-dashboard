import os, csv
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-tools/.env')
import psycopg2
from psycopg2.extras import execute_values

SQL = """
with boom as (
  select dv.deepest_subcat_name,
         sum(case when fcv.dim_date_key between 20260521 and 20260603 then 1 else 0 end) as recent_v,
         sum(case when fcv.dim_date_key between 20260507 and 20260520 then 1 else 0 end) as prior_v
  from datamart.fct_visits fcv
  join datamart.dim_visit dv on fcv.dim_visit_key=dv.dim_visit_key
  join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id=chan.aff_id and dv.channel_id=chan.channel_id
  where dv.is_real_visit=1 and chan.marketing_channel='SEO'
    and fcv.dim_date_key between 20260507 and 20260603
    and dv.deepest_subcat_name is not null
  group by 1
),
seasonal as (
  select deepest_subcat_name from boom
  where recent_v >= 100 and prior_v > 0 and recent_v::numeric/prior_v >= 3.0
)
select dv.main_cat_name, dv.deepest_subcat_name, SPLIT_PART(dv.url,'?',1) as url,
       count(*) as visits, sum(fcv.cpc_revenue)+sum(fcv.ww_revenue) as revenue
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key=dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key=dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id=chan.aff_id and dv.channel_id=chan.channel_id
where dv.is_real_visit=1 and chan.marketing_channel='SEO'
  and fcv.dim_date_key between 20250401 and 20250901
  and dv.url like '%beslist.nl%' and dv.url like '%/c/%'
  and dv.url not like '%/r/%' and dv.url not like '%/l/%'
  and dv.url not like '%/page_%' and dv.url not like '%#%'
  and dv.deepest_subcat_name in (select deepest_subcat_name from seasonal)
  and SPLIT_PART(dv.url,'?',1) not like '%+%'
  and SPLIT_PART(dv.url,'?',1) not like '%=%'
group by 3,2,1
having count(*) > 3
"""
print("Querying Redshift: seasonal-category popular URLs, 2025-04-01..2025-09-01 ...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SQL); rows = rc.fetchall(); rs.close()
tv = sum(int(r[3]) for r in rows); tr = sum(float(r[4] or 0) for r in rows)
print(f"  popular URLs (visits>3, no '+'/'='): {len(rows):,} | visits {tv:,} | revenue EUR {tr:,.2f}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'), keepalives=1, keepalives_idle=10,
                      keepalives_interval=5, keepalives_count=5)
pg.autocommit = True
c = pg.cursor()
c.execute("CREATE TEMP TABLE q (main text, subcat text, raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO q VALUES %s",
               [(r[0], r[1], r[2], int(r[3]), float(r[4] or 0)) for r in rows], page_size=5000)
c.execute("ANALYZE q")

c.execute("""CREATE TEMP TABLE gap AS
  SELECT u.url_id, u.url, q.main, q.subcat, q.visits, q.revenue
  FROM q
  JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)
  LEFT JOIN pa.kopteksten_content kc ON kc.url_id=u.url_id AND coalesce(kc.content,'')<>''
  LEFT JOIN pa.faq_content_v2 fc ON fc.url_id=u.url_id AND coalesce(fc.faq_json,'')<>''
  WHERE kc.url_id IS NULL AND fc.url_id IS NULL""")
c.execute("ANALYZE gap")
c.execute("SELECT count(*), coalesce(sum(visits),0), coalesce(sum(revenue),0) FROM gap")
g_n,g_v,g_r = c.fetchone()
c.execute("""SELECT count(*) FROM q LEFT JOIN pa.urls u ON u.url=pa.canonicalize_url(q.raw_url) WHERE u.url_id IS NULL""")
not_in_urls = c.fetchone()[0]

print(f"\n=== RESULT ===")
print(f"  popular seasonal URLs total: {len(rows):,}")
print(f"  NOT in Kopteksten AND NOT in FAQ: {g_n:,} ({100*g_n/len(rows):.1f}%) | {int(g_v):,} visits | EUR {float(g_r):,.2f}")
print(f"  (of which not even in pa.urls: {not_in_urls:,})")

print("\n  validation breakdown of the gap URLs:")
c.execute("""SELECT coalesce(v.is_valid::text,'never-validated'), count(*)
             FROM gap g LEFT JOIN pa.url_validation v ON v.url_id=g.url_id GROUP BY 1 ORDER BY 2 DESC""")
for iv,n in c.fetchall(): print(f"     {iv:<16} {n:,}")

print("\n  gap URLs per seasonal subcategory (top 25):")
c.execute("SELECT subcat, count(*) FROM gap GROUP BY 1 ORDER BY 2 DESC LIMIT 25")
for sc,n in c.fetchall(): print(f"     {sc:<32} {n:,}")

out = "/mnt/c/Users/JoepvanSchagen/Downloads/claude/seasonal_gap_urls_20260603.csv"
c.execute("SELECT url_id, main, subcat, url, visits, revenue FROM gap ORDER BY visits DESC")
allrows = c.fetchall()
with open(out, "w", newline="") as f:
    w = csv.writer(f); w.writerow(["url_id","main_cat","subcat","url","visits","revenue"]); w.writerows(allrows)
print(f"\n  saved gap list -> {out}")
pg.close()
