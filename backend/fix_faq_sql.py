"""Pure SQL fix: add 'name' field from page_title into schema_org JSON."""
import psycopg2

conn = psycopg2.connect(
    host="10.1.32.9", port=5432, dbname="n8n-vector-db",
    user="dbadmin", password="Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6"
)
cur = conn.cursor()

# Simple string replace: inject "name": "title", after "FAQPage",
# Escape double quotes in page_title for valid JSON
sql = """
    UPDATE pa.faq_content
    SET schema_org = replace(
        schema_org,
        '"@type": "FAQPage", "mainEntity"',
        '"@type": "FAQPage", "name": "' || replace(page_title, '"', '\\"') || '", "mainEntity"'
    )
    WHERE schema_org IS NOT NULL
      AND schema_org != ''
      AND page_title IS NOT NULL
      AND page_title != ''
      AND schema_org NOT LIKE '%"name": "%, "mainEntity"%'
"""

cur.execute(sql)
print(f"Updated: {cur.rowcount}")
conn.commit()
cur.close()
conn.close()
