import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')
import psycopg2
from psycopg2.extras import execute_values

SQL = """
select SPLIT_PART(dv.url, '?', 1) as url, count(*) as visits
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key = dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key = dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id = chan.aff_id and dv.channel_id = chan.channel_id
where dv.is_real_visit = 1 and chan.marketing_channel = 'SEO'
  and fcv.dim_date_key between 20260301 and 20260603
  and dv.url like '%beslist.nl%' and dv.url like '%/c/%'
  and dv.url not like '%/r/%' and dv.url not like '%/l/%'
  and dv.url not like '%/page_%' and dv.url not like '%#%'
group by 1
having count(*) > 3
"""
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor(); rc.execute(SQL); rows = rc.fetchall(); rs.close()

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL')); c = pg.cursor()
c.execute("CREATE TEMP TABLE q (raw_url text, visits bigint)")
execute_values(c, "INSERT INTO q VALUES %s", [(r[0], int(r[1])) for r in rows], page_size=5000)

KOP = "(SELECT url_id FROM pa.kopteksten_content WHERE coalesce(content,'')<>'')"
FAQ = "(SELECT url_id FROM pa.faq_content_v2 WHERE coalesce(faq_json,'')<>'')"
# gap, excluding URLs containing '+'
c.execute(f"""CREATE TEMP TABLE gap AS
  SELECT DISTINCT u.url_id
  FROM q JOIN pa.urls u ON u.url = pa.canonicalize_url(q.raw_url)
  WHERE q.raw_url NOT LIKE '%+%'
    AND u.url_id NOT IN {KOP} AND u.url_id NOT IN {FAQ}""")
c.execute("SELECT count(*) FROM gap"); total = c.fetchone()[0]
print(f"GAP url_ids (no content, no '+'): {total:,}\n")

for tool in ['kopteksten','faq']:
    print(f"--- pa.{tool}_jobs status for these gap URLs ---")
    c.execute(f"""
        SELECT coalesce(j.status,'(not in jobs)') st, count(*)
        FROM gap g LEFT JOIN pa.{tool}_jobs j ON j.url_id = g.url_id
        GROUP BY 1 ORDER BY 2 DESC""")
    for st,n in c.fetchall(): print(f"   {st:<16} {n:,}")
    # how many would be newly inserted (not currently in jobs)
    c.execute(f"SELECT count(*) FROM gap g WHERE g.url_id NOT IN (SELECT url_id FROM pa.{tool}_jobs)")
    print(f"   => not in jobs at all (would be NEW pending rows): {c.fetchone()[0]:,}\n")

# url_validation: how many are known-invalid (skipped) -> generation will skip them
c.execute("""SELECT count(*) FROM gap g JOIN pa.url_validation v ON v.url_id=g.url_id WHERE v.is_valid=FALSE""")
print(f"of the gap URLs, known-invalid in pa.url_validation (would be skipped by pipeline): {c.fetchone()[0]:,}")
pg.close()
