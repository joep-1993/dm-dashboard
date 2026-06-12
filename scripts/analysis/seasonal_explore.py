import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-tools/.env')
import psycopg2
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
c = rs.cursor()
# columns of dim_visit
c.execute("""select column_name from information_schema.columns
             where table_schema='datamart' and table_name='dim_visit' order by ordinal_position""")
print("dim_visit columns:", [r[0] for r in c.fetchall()])
# where do the example categories live?
for term in ['airco','parasol','zwembad']:
    print(f"\n--- '{term}' matches (main_cat_name / deepest_subcat_name) ---")
    c.execute("""select main_cat_name, deepest_subcat_name, count(*) cnt
                 from datamart.dim_visit
                 where (lower(main_cat_name) like %s or lower(deepest_subcat_name) like %s)
                 group by 1,2 order by 3 desc limit 8""", (f'%{term}%', f'%{term}%'))
    for r in c.fetchall(): print(f"   main={r[0]!r:<28} sub={r[1]!r:<35} rows={r[2]:,}")
rs.close()
