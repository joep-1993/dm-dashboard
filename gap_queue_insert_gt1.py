import os, csv
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-tools/.env')
import psycopg2
from psycopg2.extras import execute_values

CSV = "/mnt/c/Users/JoepvanSchagen/Downloads/claude/gap_urls_gt1_20260603.csv"
with open(CSV) as f:
    rows = list(csv.DictReader(f))
url_ids = [int(r["url_id"]) for r in rows]
# CSV already excludes '+' (built with NOT LIKE '%+%'); assert to be safe
assert not any("+" in r["url"] for r in rows), "found '+' url in CSV!"
print(f"gap url_ids from CSV (visits>1, no '+'): {len(url_ids):,}")

pg = psycopg2.connect(dsn=os.getenv('DATABASE_URL'))
c = pg.cursor()
try:
    c.execute("CREATE TEMP TABLE gapids (url_id bigint primary key)")
    execute_values(c, "INSERT INTO gapids VALUES %s ON CONFLICT DO NOTHING",
                   [(u,) for u in url_ids], page_size=5000)
    # never-validated = no row in pa.url_validation
    c.execute("""CREATE TEMP TABLE target AS
        SELECT g.url_id FROM gapids g
        LEFT JOIN pa.url_validation v ON v.url_id = g.url_id
        WHERE v.url_id IS NULL""")
    c.execute("SELECT count(*) FROM target"); n_target = c.fetchone()[0]
    print(f"never-validated target set: {n_target:,}")
    for tool in ['kopteksten','faq']:
        c.execute(f"SELECT count(*) FROM target t WHERE EXISTS (SELECT 1 FROM pa.{tool}_jobs j WHERE j.url_id=t.url_id)")
        print(f"  {tool}_jobs already present: {c.fetchone()[0]:,}")

    print("\nInserting (status='pending', ON CONFLICT DO NOTHING)...")
    for tool in ['kopteksten','faq']:
        c.execute(f"""INSERT INTO pa.{tool}_jobs (url_id, status, created_at, updated_at)
                      SELECT url_id, 'pending', now(), now() FROM target
                      ON CONFLICT (url_id) DO NOTHING""")
        print(f"  pa.{tool}_jobs: inserted {c.rowcount:,} new pending rows")
    pg.commit()
    print("\nCOMMITTED.")
    for tool in ['kopteksten','faq']:
        c.execute(f"SELECT count(*) FROM target t JOIN pa.{tool}_jobs j ON j.url_id=t.url_id WHERE j.status='pending'")
        print(f"  verify pa.{tool}_jobs pending for target set: {c.fetchone()[0]:,}")
except Exception as e:
    pg.rollback(); print("ROLLED BACK:", e); raise
finally:
    pg.close()
