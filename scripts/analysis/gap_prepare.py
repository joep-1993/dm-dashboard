import os, csv
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-tools/.env')
import psycopg2
from psycopg2.extras import execute_values

SQL = """
select SPLIT_PART(dv.url, '?', 1) as url, count(*) as visits,
       sum(fcv.cpc_revenue)+sum(fcv.ww_revenue) as revenue
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key = dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key = dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id = chan.aff_id and dv.channel_id = chan.channel_id
where dv.is_real_visit = 1 and chan.marketing_channel = 'SEO'
  and fcv.dim_date_key between 20260301 and 20260603
  and dv.url like '%beslist.nl%' and dv.url like '%/c/%'
  and dv.url not like '%/r/%' and dv.url not like '%/l/%'
  and dv.url not like '%/page_%' and dv.url not like '%#%'
group by 1 having count(*) > 3
"""
print("Querying Redshift...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SQL); rows = rc.fetchall(); rs.close()
print(f"  rows: {len(rows):,}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'), keepalives=1, keepalives_idle=10,
                      keepalives_interval=5, keepalives_count=5)
pg.autocommit = True
c = pg.cursor()
c.execute("CREATE TEMP TABLE q (raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO q VALUES %s", [(r[0], int(r[1]), float(r[2] or 0)) for r in rows], page_size=5000)
c.execute("ANALYZE q")

KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"
c.execute(f"""CREATE TEMP TABLE gap AS
  SELECT u.url_id, u.url, q.visits, q.revenue
  FROM q
  JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)
  LEFT JOIN pa.kopteksten_content kc ON kc.url_id = u.url_id AND coalesce(kc.content,'')<>''
  LEFT JOIN pa.faq_content_v2 fc ON fc.url_id = u.url_id AND coalesce(fc.faq_json,'')<>''
  WHERE q.raw_url NOT LIKE '%+%' AND kc.url_id IS NULL AND fc.url_id IS NULL""")
c.execute("ANALYZE gap")
c.execute("SELECT count(*) FROM gap"); total = c.fetchone()[0]
print(f"\nGAP url_ids (no content, no '+'): {total:,}")

# dump to disk
out = "/mnt/c/Users/JoepvanSchagen/Downloads/claude/gap_urls_20260603.csv"
os.makedirs(os.path.dirname(out), exist_ok=True)
c.execute("SELECT url_id, url, visits, revenue FROM gap ORDER BY visits DESC")
allrows = c.fetchall()
with open(out, "w", newline="") as f:
    w = csv.writer(f); w.writerow(["url_id","url","visits","revenue"]); w.writerows(allrows)
print(f"  saved -> {out}")

for tool in ['kopteksten','faq']:
    print(f"\n--- pa.{tool}_jobs status for gap URLs ---")
    c.execute(f"""SELECT coalesce(j.status,'(not in jobs)') st, count(*)
                  FROM gap g LEFT JOIN pa.{tool}_jobs j ON j.url_id=g.url_id
                  GROUP BY 1 ORDER BY 2 DESC""")
    for st,n in c.fetchall(): print(f"   {st:<16} {n:,}")

print("\n--- pa.url_validation for gap URLs ---")
c.execute("""SELECT coalesce(v.is_valid::text,'(unknown)') iv, count(*)
             FROM gap g LEFT JOIN pa.url_validation v ON v.url_id=g.url_id
             GROUP BY 1 ORDER BY 2 DESC""")
for iv,n in c.fetchall(): print(f"   is_valid={iv:<10} {n:,}")
pg.close()
