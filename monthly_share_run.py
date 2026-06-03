import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import execute_values

REDSHIFT_SQL = """
select
    SPLIT_PART(dv.url, '?', 1) as url,
    count(*) as visits,
    sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) as revenue
from datamart.fct_visits fcv
join datamart.dim_visit dv on fcv.dim_visit_key = dv.dim_visit_key
join datamart.dim_date dat on fcv.dim_date_key = dat.dim_date_key
join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id = chan.aff_id and dv.channel_id = chan.channel_id
where dv.is_real_visit = 1
    and chan.marketing_channel = 'SEO'
    and fcv.dim_date_key between 20260501 and 20260531
    and dv.url like '%beslist.nl%'
    and dv.url like '%/c/%'
    and dv.url not like '%/r/%'
    and dv.url not like '%/l/%'
    and dv.url not like '%/page_%'
    and dv.url not like '%#%'
group by 1
having count(*) > 4
"""

print("Querying Redshift for May 2026 SEO /c/ visits...")
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
rc = rs.cursor()
rc.execute(REDSHIFT_SQL)
rows = rc.fetchall()  # (url, visits, revenue)
print(f"  rows: {len(rows):,}")
total_visits = sum(r[1] for r in rows)
total_rev = sum(float(r[2] or 0) for r in rows)
print(f"  total visits: {total_visits:,}  total revenue: EUR {total_rev:,.2f}")
rs.close()

print("\nLoading into app DB temp table + joining via pa.canonicalize_url ...")
pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'))
pc = pg.cursor()
pc.execute("CREATE TEMP TABLE seo_may (raw_url text, visits bigint, revenue double precision)")
execute_values(pc, "INSERT INTO seo_may (raw_url, visits, revenue) VALUES %s",
               [(r[0], int(r[1]), float(r[2] or 0)) for r in rows], page_size=5000)

# Map to canonical url_id
pc.execute("""
    CREATE TEMP TABLE seo_may_mapped AS
    SELECT s.raw_url, s.visits, s.revenue, u.url_id
    FROM seo_may s
    LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(s.raw_url)
""")
pc.execute("SELECT count(*), sum(visits), sum(revenue) FROM seo_may_mapped WHERE url_id IS NOT NULL")
m_rows, m_visits, m_rev = pc.fetchone()
print(f"  matched to pa.urls: {m_rows:,} urls  | {m_visits:,} visits  | EUR {float(m_rev):,.2f}")
unmatched = len(rows) - m_rows
print(f"  unmatched (no row in pa.urls): {unmatched:,} urls "
      f"({(total_visits-m_visits):,} visits, EUR {total_rev-float(m_rev):,.2f})")

content_tables = {
    'Kopteksten':    "pa.kopteksten_content WHERE coalesce(content,'')<>''",
    'FAQ':           "pa.faq_content_v2 WHERE coalesce(faq_json,'')<>''",
    'Unique titles': "pa.unique_titles_content WHERE coalesce(title,'')<>'' OR coalesce(h1_title,'')<>''",
}

print("\n" + "="*72)
print(f"{'DB':<16}{'urls w/ content':>16}{'% visits':>12}{'% revenue':>14}")
print("="*72)
results = {}
for name, frm in content_tables.items():
    pc.execute(f"""
        SELECT count(*) , coalesce(sum(m.visits),0), coalesce(sum(m.revenue),0)
        FROM seo_may_mapped m
        WHERE m.url_id IN (SELECT url_id FROM {frm})
    """)
    c_urls, c_visits, c_rev = pc.fetchone()
    c_visits = int(c_visits); c_rev = float(c_rev)
    pv = 100.0 * c_visits / total_visits if total_visits else 0
    pr = 100.0 * c_rev / total_rev if total_rev else 0
    results[name] = (c_urls, c_visits, c_rev, pv, pr)
    print(f"{name:<16}{c_urls:>16,}{pv:>11.1f}%{pr:>13.1f}%")
print("="*72)
print(f"\nDenominator (all May SEO /c/ urls, visits>4): {len(rows):,} urls | "
      f"{total_visits:,} visits | EUR {total_rev:,.2f} revenue")
print("\nDetail (absolute visits / revenue per DB):")
for name,(cu,cv,cr,pv,pr) in results.items():
    print(f"  {name:<16} {cv:>12,} visits   EUR {cr:>14,.2f}")
pg.close()
