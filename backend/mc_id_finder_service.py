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

        # Always select all MC ID columns; frontend picks which to show
        query = """
            SELECT DISTINCT
                r.shop_name,
                m.f_mc_id_nl,
                m.f_mc_id_be,
                m.f_mc_id_de
            FROM beslistbi.hda.efficy_shop_dm m
            JOIN bt.shop_main_attributes_by_day r ON m.k_shop = r.efficy_k_shop
            WHERE 1=1
        """
        params = []

        if shop_names:
            conditions = []
            for name in shop_names:
                conditions.append("LOWER(r.shop_name) LIKE LOWER(%s)")
                params.append(f"%{name}%")
            query += " AND (" + " OR ".join(conditions) + ")"

        # Require at least one checked country MC ID to be non-empty (columns are strings)
        if countries:
            mc_conditions = []
            for c in countries:
                col = f"m.f_mc_id_{c.lower()}"
                mc_conditions.append(f"{col} != '' AND {col} != '0' AND {col} != '1'")
            query += " AND (" + " OR ".join(mc_conditions) + ")"
        else:
            query += " AND (m.f_mc_id_nl NOT IN ('','0','1') OR m.f_mc_id_be NOT IN ('','0','1') OR m.f_mc_id_de NOT IN ('','0','1'))"

        query += " ORDER BY r.shop_name LIMIT 500"

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        results = []
        for row in rows:
            results.append({
                "shop_name": row["shop_name"],
                "mc_id_nl": row["f_mc_id_nl"],
                "mc_id_be": row["f_mc_id_be"],
                "mc_id_de": row["f_mc_id_de"],
            })

        return {"status": "success", "results": results, "total": len(results)}
    except Exception as e:
        logger.error(f"Error searching MC IDs: {e}")
        return {"status": "error", "error": str(e), "results": [], "total": 0}
    finally:
        return_redshift_connection(conn)
