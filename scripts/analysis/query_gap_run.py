import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import execute_values

# query.txt verbatim
SQL = open('/home/joepvanschagen/projects/dm-tools/query.txt').read()

print("Running query.txt against Redshift (date range as written in the file)...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor()
rc.execute(SQL)
rows = rc.fetchall()  # main_cat, deepest_subcat, url, visits, revenue
rs.close()
# url is col index 2, visits 3, revenue 4
total_visits = sum(int(r[3]) for r in rows)
total_rev = sum(float(r[4] or 0) for r in rows)
print(f"  rows: {len(rows):,} | visits: {total_visits:,} | revenue: EUR {total_rev:,.2f}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'))
c = pg.cursor()
c.execute("CREATE TEMP TABLE q (raw_url text, visits bigint, revenue double precision)")
execute_values(c, "INSERT INTO q VALUES %s",
               [(r[2], int(r[3]), float(r[4] or 0)) for r in rows], page_size=5000)
c.execute("""CREATE TEMP TABLE qm AS
  SELECT q.raw_url, q.visits, q.revenue, u.url_id
  FROM q LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)""")

KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"

def stat(where):
    c.execute(f"SELECT count(*), coalesce(sum(visits),0), coalesce(sum(revenue),0) FROM qm WHERE {where}")
    n,v,r = c.fetchone(); return n, int(v), float(r)

n_match,v_match,r_match = stat("url_id IS NOT NULL")
n_gap,v_gap,r_gap       = stat(f"url_id IS NOT NULL AND url_id NOT IN {KOP} AND url_id NOT IN {FAQ}")
n_unmatched = len(rows) - n_match

print(f"\n  matched to pa.urls:  {n_match:,} urls | {v_match:,} visits | EUR {r_match:,.2f}")
print(f"  not in pa.urls:      {n_unmatched:,} urls")
print("\n" + "="*60)
print("URLs from query.txt NOT yet in Kopteksten AND NOT in FAQ:")
print("="*60)
print(f"  URLs:    {n_gap:,}   ({100*n_gap/len(rows):.1f}% of query rows)")
print(f"  Visits:  {v_gap:,}   ({100*v_gap/total_visits:.1f}% of total)")
print(f"  Revenue: EUR {r_gap:,.2f}   ({100*r_gap/total_rev:.1f}% of total)")
pg.close()
