"""
R-Finder Service

Finds /r/ URLs from Redshift visits data with filtering capabilities.
Replaces the Google Apps Script that queried GA4.

Filters applied:
- Must contain /r/
- Excludes: device=, /sitemap/, sortby=, /filters/, /page_, shop_id=, (other), (not set)
- Excludes certain category combinations (cadeaus/meubilair, kantoor/mode, etc.)
"""

from typing import List, Dict, Optional
from backend.database import get_redshift_connection, return_redshift_connection


def fetch_r_urls(
    filters: Optional[List[str]] = None,
    min_visits: int = 0,
    start_date: str = "20210101",
    end_date: str = "20261231",
    limit: int = 4000
) -> List[Dict]:
    """
    Fetch /r/ URLs from Redshift based on filter criteria.

    Args:
        filters: Optional list of strings that URL must contain (e.g., category segments)
        min_visits: Minimum number of visits required
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        limit: Maximum number of URLs to return

    Returns:
        List of dicts with 'url', 'visits' keys, sorted by visits desc
    """
    conn = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Build the query with all the exclusions from the original GA4 script
        query = """
            SELECT
                SPLIT_PART(dv.url, '?', 1) as url,
                COUNT(*) as visits
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
                ON fcv.dim_visit_key = dv.dim_visit_key
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN %s AND %s
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url LIKE '%%/r/%%'
              -- Exclude filters from original script
              AND dv.url NOT LIKE '%%device=%%'
              AND dv.url NOT LIKE '%%/sitemap/%%'
              AND dv.url NOT LIKE '%%sortby=%%'
              AND dv.url NOT LIKE '%%/filters/%%'
              AND dv.url NOT LIKE '%%/page_%%'
              AND dv.url NOT LIKE '%%shop_id=%%'
              AND dv.url NOT LIKE '%%+%%'
              -- Exclude mismatched category combinations
              AND dv.url NOT LIKE '%%/cadeaus_gadgets_culinair/meubilair_%%'
              AND dv.url NOT LIKE '%%/kantoorartikelen/mode_%%'
              AND dv.url NOT LIKE '%%/meubilair/mode_%%'
              AND dv.url NOT LIKE '%%/klussen/huis_tuin%%'
        """

        params = [int(start_date), int(end_date)]

        # Add optional filters (URL must contain ALL of these strings - AND logic)
        if filters:
            for f in filters:
                if f and f.strip():
                    # Replace spaces with underscores to match URL format
                    filter_value = f.strip().replace(' ', '_')
                    query += " AND dv.url LIKE %s"
                    params.append(f"%{filter_value}%")

        query += """
            GROUP BY 1
            HAVING COUNT(*) > %s
            ORDER BY 2 DESC
            LIMIT %s
        """
        params.append(min_visits)
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        # Handle both dict and tuple cursor results
        results = []
        for row in rows:
            if isinstance(row, dict):
                results.append({
                    "url": row.get("url"),
                    "visits": row.get("visits")
                })
            else:
                results.append({
                    "url": row[0],
                    "visits": row[1]
                })
        return results

    except Exception as e:
        print(f"[ERROR] Failed to fetch R URLs from Redshift: {e}")
        raise
    finally:
        if conn:
            return_redshift_connection(conn)


def get_r_url_stats(
    start_date: str = "20210101",
    end_date: str = "20261231"
) -> Dict:
    """
    Get statistics about /r/ URLs in the database.

    Returns:
        Dict with total_urls, total_visits
    """
    conn = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        query = """
            SELECT
                COUNT(DISTINCT SPLIT_PART(dv.url, '?', 1)) as total_urls,
                COUNT(*) as total_visits
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
                ON fcv.dim_visit_key = dv.dim_visit_key
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN %s AND %s
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url LIKE '%%/r/%%'
              AND dv.url NOT LIKE '%%device=%%'
              AND dv.url NOT LIKE '%%/sitemap/%%'
              AND dv.url NOT LIKE '%%sortby=%%'
              AND dv.url NOT LIKE '%%/filters/%%'
              AND dv.url NOT LIKE '%%/page_%%'
              AND dv.url NOT LIKE '%%shop_id=%%'
              AND dv.url NOT LIKE '%%+%%'
        """

        cur.execute(query, [int(start_date), int(end_date)])
        row = cur.fetchone()

        if isinstance(row, dict):
            return {
                "total_urls": row.get("total_urls", 0),
                "total_visits": row.get("total_visits", 0)
            }
        else:
            return {
                "total_urls": row[0] if row else 0,
                "total_visits": row[1] if row else 0
            }

    except Exception as e:
        print(f"[ERROR] Failed to get R URL stats: {e}")
        raise
    finally:
        if conn:
            return_redshift_connection(conn)
