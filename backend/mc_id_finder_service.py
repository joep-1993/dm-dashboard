"""
MC ID Finder Service - Looks up Merchant Center IDs by shop name or MC ID.
"""

import logging
from typing import Optional, List, Dict, Any
from backend.database import get_redshift_connection, return_redshift_connection

logger = logging.getLogger(__name__)


def search_mc_ids(
    shop_names: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Search for MC IDs by shop names. countries controls which MC ID columns to return."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()

        # Use ROW_NUMBER to pick only the most recent efficy record per shop
        inner_where = "WHERE 1=1"
        params = []

        if shop_names:
            conditions = []
            for name in shop_names:
                conditions.append("LOWER(r.shop_name) LIKE LOWER(%s)")
                params.append(f"%{name}%")
            inner_where += " AND (" + " OR ".join(conditions) + ")"

        # Require at least one checked country MC ID to be non-empty (columns are strings)
        # Also filter by shop TLD to avoid e.g. x2o.be showing up when only NL is checked
        tld_map = {'nl': '.nl', 'be': '.be', 'de': '.de'}
        if countries:
            mc_conditions = []
            for c in countries:
                col = f"m.f_mc_id_{c.lower()}"
                tld = tld_map[c.lower()]
                mc_conditions.append(
                    f"({col} NOT IN ('','0','1') AND LOWER(r.shop_name) LIKE %s)"
                )
                params.append(f"%{tld}")
            inner_where += " AND (" + " OR ".join(mc_conditions) + ")"
        else:
            inner_where += " AND (m.f_mc_id_nl NOT IN ('','0','1') OR m.f_mc_id_be NOT IN ('','0','1') OR m.f_mc_id_de NOT IN ('','0','1'))"

        query = f"""
            SELECT shop_name, f_mc_id_nl, f_mc_id_be, f_mc_id_de, d_change
            FROM (
                SELECT DISTINCT
                    r.shop_name,
                    m.f_mc_id_nl,
                    m.f_mc_id_be,
                    m.f_mc_id_de,
                    m.d_change,
                    ROW_NUMBER() OVER (PARTITION BY r.shop_name ORDER BY m.d_change DESC) AS rn
                FROM beslistbi.hda.efficy_shop_dm m
                JOIN bt.shop_main_attributes_by_day r ON m.k_shop = r.efficy_k_shop
                {inner_where}
            ) sub
            WHERE rn = 1
            ORDER BY d_change DESC
            LIMIT 500
        """

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        results = []
        for row in rows:
            d = row["d_change"]
            results.append({
                "shop_name": row["shop_name"],
                "mc_id_nl": row["f_mc_id_nl"],
                "mc_id_be": row["f_mc_id_be"],
                "mc_id_de": row["f_mc_id_de"],
                "last_changed": d.strftime("%Y-%m-%d") if d else None,
            })

        return {"status": "success", "results": results, "total": len(results)}
    except Exception as e:
        logger.error(f"Error searching MC IDs: {e}")
        return {"status": "error", "error": str(e), "results": [], "total": 0}
    finally:
        return_redshift_connection(conn)
