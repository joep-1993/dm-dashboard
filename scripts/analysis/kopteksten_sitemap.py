import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')
import psycopg2
from psycopg2.extras import execute_values

SITEMAP_SQL = """
select distinct url
from bt.new_hs_data
where country = 'nl'
  and current_month_year in ('Mei 2026')
  and url like '%/c/%'
"""
print("Pulling May sitemap /c/ URLs from Redshift...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SITEMAP_SQL); sm = [r[0] for r in rc.fetchall()]; rs.close()
print(f"  sitemap URLs: {len(sm):,}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'), keepalives=1, keepalives_idle=10,
                      keepalives_interval=5, keepalives_count=5)
pg.autocommit = True
c = pg.cursor()
c.execute("CREATE TEMP TABLE sm (raw_url text)")
execute_values(c, "INSERT INTO sm VALUES %s", [(u,) for u in sm], page_size=5000)
# map sitemap urls to url_id
c.execute("""CREATE TEMP TABLE sm_ids AS
  SELECT DISTINCT u.url_id FROM sm
  JOIN pa.urls u ON u.url = pa.canonicalize_url(sm.raw_url)""")
c.execute("CREATE INDEX ON sm_ids(url_id); ANALYZE sm_ids")
c.execute("SELECT count(*) FROM sm_ids")
print(f"  sitemap URLs matched to pa.urls: {c.fetchone()[0]:,}")

cases = [
    ("Kopteksten universe (kopteksten_jobs)",
     "SELECT count(*) FROM pa.kopteksten_jobs",
     "SELECT count(*) FROM pa.kopteksten_jobs k WHERE EXISTS (SELECT 1 FROM sm_ids s WHERE s.url_id=k.url_id)"),
    ("Kopteksten WITH content (kopteksten_content)",
     "SELECT count(*) FROM pa.kopteksten_content WHERE coalesce(content,'')<>''",
     "SELECT count(*) FROM pa.kopteksten_content k WHERE coalesce(k.content,'')<>'' AND EXISTS (SELECT 1 FROM sm_ids s WHERE s.url_id=k.url_id)"),
]
for label, q_total, q_in in cases:
    c.execute(q_total); total = c.fetchone()[0]
    c.execute(q_in); insite = c.fetchone()[0]
    print(f"\n{label}: {total:,} URLs")
    print(f"   in May sitemap: {insite:,}  -> {100*insite/total:.1f}%")
    print(f"   NOT in sitemap: {total-insite:,}  -> {100*(total-insite)/total:.1f}%")
pg.close()
