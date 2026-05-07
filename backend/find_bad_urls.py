"""Find URLs with invalid facets/categories by testing against the Product Search API.
Writes bad URLs to /tmp/bad_urls.txt for removal."""

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/myapp")
API_BASE = "https://productsearch-v2.api.beslist.nl/search/products"
BATCH_SIZE = 5000
WORKERS = 20

def check_url(url):
    """Check if a URL returns 200 from the product API. Returns url if bad, None if ok."""
    try:
        parts = url.split('/c/')
        if len(parts) != 2:
            return url  # malformed

        path_part = parts[0]
        filter_part = parts[1].rstrip('/')

        # Strip /page_N/ suffix if present
        if '/page_' in filter_part:
            filter_part = filter_part.split('/page_')[0]

        segments = path_part.strip('/').split('/')
        if len(segments) < 2:
            return url

        main_category = segments[1]
        category = segments[2] if len(segments) > 2 else None

        api_url = f"{API_BASE}?mainCategory={main_category}&countryLanguage=nl-nl&isBot=false&limit=1"
        if category and category != 'c':
            api_url += f"&category={category}"

        facets = filter_part.split('~~')
        for facet in facets:
            if '~' in facet:
                name, value = facet.split('~', 1)
                api_url += f"&filters[{name}][0]={value}"

        resp = requests.get(api_url, timeout=10)
        if resp.status_code == 400:
            return url
        return None
    except Exception:
        return None  # don't flag on network errors


def main():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()

    # Count total — pending unique_titles jobs whose URL is a faceted /products/.../c/... path
    cur.execute("""
        SELECT count(*) AS cnt
        FROM pa.unique_titles_jobs j
        JOIN pa.urls u ON j.url_id = u.url_id
        WHERE j.status = 'pending'
          AND u.url LIKE '/products/%%/c/%%'
    """)
    total = cur.fetchone()['cnt']
    print(f"Total pending URLs to check: {total}")

    bad_urls = []
    checked = 0
    offset = 0

    while offset < total:
        cur.execute("""
            SELECT u.url
            FROM pa.unique_titles_jobs j
            JOIN pa.urls u ON j.url_id = u.url_id
            WHERE j.status = 'pending'
              AND u.url LIKE '/products/%%/c/%%'
            ORDER BY u.url
            LIMIT %s OFFSET %s
        """, (BATCH_SIZE, offset))

        urls = [r['url'] for r in cur.fetchall()]
        if not urls:
            break

        batch_bad = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(check_url, url): url for url in urls}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    bad_urls.append(result)
                    batch_bad += 1
                checked += 1

        offset += BATCH_SIZE
        print(f"  Checked {checked}/{total} — found {len(bad_urls)} bad so far (batch: {batch_bad}/{len(urls)})")
        sys.stdout.flush()

    cur.close()
    conn.close()

    # Write results
    with open('/tmp/bad_urls.txt', 'w') as f:
        for url in bad_urls:
            f.write(url + '\n')

    print(f"\nDone! Found {len(bad_urls)} bad URLs out of {checked} checked ({len(bad_urls)*100/max(checked,1):.1f}%)")
    print(f"Written to /tmp/bad_urls.txt")


if __name__ == "__main__":
    main()
