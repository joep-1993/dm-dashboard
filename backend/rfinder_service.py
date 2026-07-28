"""
R-Finder Service

Finds /r/ URLs from Redshift visits data with filtering capabilities.
Replaces the Google Apps Script that queried GA4.

Filters applied:
- Must contain /r/
- Excludes: device=, /sitemap/, sortby=, /filters/, /page_, shop_id=, (other), (not set)
- Excludes certain category combinations (cadeaus/meubilair, kantoor/mode, etc.)
"""

from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from backend.database import get_redshift_connection, return_redshift_connection

# One filter row costs a full ~17s /r/ scan, so rows run concurrently. Capped
# well under the Redshift pool's maxconn=10 so R-Finder can never starve the
# other tools sharing that pool.
MAX_PARALLEL_ROWS = 4


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


def fetch_r_urls_by_row(
    filter_rows: List[List[str]],
    min_visits: int = 0,
    start_date: str = "20210101",
    end_date: str = "20261231",
    limit: int = 4000
) -> List[Dict]:
    """One INDEPENDENT result set per filter row.

    Each row keeps the existing AND semantics between its own boxes; the rows
    themselves are separate queries, so row 2's results never compete with row
    1's for the LIMIT. That is the whole point — a shared query with the rows
    OR'd together would let a broad row swallow the entire limit and return
    nothing for a narrow one, which is exactly the "row 2 outputs a separate set"
    behaviour this is supposed to provide.

    A URL that matches several rows appears under each of them, by design.

    Returns [{'index', 'filters', 'total', 'urls', 'error'}] in input row order.
    """
    # Drop empty boxes per row, then drop rows left with nothing. An all-empty
    # row would otherwise re-run the unfiltered query and duplicate a sibling.
    cleaned = []
    for i, row in enumerate(filter_rows or []):
        vals = [f.strip() for f in (row or []) if f and f.strip()]
        cleaned.append({"index": i, "filters": vals})

    def _one(spec):
        try:
            urls = fetch_r_urls(
                filters=spec["filters"] or None,
                min_visits=min_visits,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            return {**spec, "total": len(urls), "urls": urls, "error": None}
        except Exception as e:
            # One bad row must not lose the other rows' work.
            print(f"[ERROR] R-Finder row {spec['index']} failed: {e}")
            return {**spec, "total": 0, "urls": [], "error": str(e)}

    if not cleaned:
        return []
    if len(cleaned) == 1:
        return [_one(cleaned[0])]
    with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_ROWS, len(cleaned))) as ex:
        return list(ex.map(_one, cleaned))


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
