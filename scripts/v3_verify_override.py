"""Targeted check: run v3 against URLs in categories that exposed the
redundancy class ('Wanten Handschoenen' etc.). Confirms category-override
patch suppresses the appended canonical category."""
import os, sys
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from backend.database import get_db_connection, return_db_connection
from backend.ai_titles_service import generate_title_v3

conn = get_db_connection()
cur = conn.cursor()
cur.execute(r"""
    SELECT u.url, c.h1_title, u.deepest_subcat_name
    FROM pa.unique_titles_content c
    JOIN pa.urls u ON u.url_id = c.url_id
    WHERE c.h1_title IS NOT NULL
      AND u.url ~ '~(t_|soort_)'
    ORDER BY random()
    LIMIT 15
""")
rows = cur.fetchall()
return_db_connection(conn)

print(f"got {len(rows)} candidates\n")
for r in rows:
    cat = r['deepest_subcat_name']
    url = r['url']
    v1 = r['h1_title']
    res = generate_title_v3(url) or {}
    composed = res.get('composed_h1', '')
    v3 = res.get('h1_title', '')
    redundant = cat and (cat.lower() in v3.lower())
    print(f"[{cat}] {url[:110]}")
    print(f"   v1:       {v1}")
    print(f"   composed: {composed}")
    print(f"   v3:       {v3}{'   <-- still contains category' if redundant else ''}")
    print()
