"""
GSD Check Service - Looks up GSD shop flags + shop metadata as of yesterday.
"""

import logging
from typing import Optional, List, Dict, Any
from backend.database import get_redshift_connection, return_redshift_connection

logger = logging.getLogger(__name__)


def search_gsd(
    shop_names: Optional[List[str]] = None,
    shop_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Look up GSD flags + shop metadata for shops as of yesterday.

    Pass shop_names for partial-match LIKE search, or shop_ids for exact-match
    lookup. If both are given, both apply (OR'd). If neither, returns nothing.
    """
    if not shop_names and not shop_ids:
        return {"status": "success", "results": [], "total": 0}

    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            params: list = []
            conditions: list = []

            if shop_names:
                for name in shop_names:
                    conditions.append("LOWER(a.shop_name) LIKE LOWER(%s)")
                    params.append(f"%{name}%")

            if shop_ids:
                placeholders = ",".join(["%s"] * len(shop_ids))
                conditions.append(f"a.shop_id IN ({placeholders})")
                params.extend(shop_ids)

            shop_filter = "AND (" + " OR ".join(conditions) + ")"

            query = f"""
                WITH yesterday_attrs AS (
                    SELECT shop_id,
                           shop_name,
                           is_gsd_nl_shop,
                           is_gsd_be_shop,
                           is_gsd_de_shop
                    FROM beslistbi.bt.shop_main_attributes_by_day
                    WHERE date = CURRENT_DATE - 1
                      AND deleted_ind = 0
                ),
                latest_list AS (
                    SELECT shop_id,
                           accountmanager_name,
                           shop_phase,
                           hide_online,
                           is_disabled,
                           ROW_NUMBER() OVER (
                               PARTITION BY shop_id
                               ORDER BY dim_date_key DESC
                           ) AS rn
                    FROM beslistbi.bt.shop_list
                    WHERE deleted_ind = 0
                      AND dim_date_key <= CAST(TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD') AS BIGINT)
                )
                SELECT a.shop_id,
                       a.shop_name,
                       a.is_gsd_nl_shop,
                       a.is_gsd_be_shop,
                       a.is_gsd_de_shop,
                       l.shop_phase,
                       l.hide_online,
                       l.is_disabled,
                       l.accountmanager_name
                FROM yesterday_attrs a
                LEFT JOIN latest_list l
                       ON l.shop_id = a.shop_id AND l.rn = 1
                WHERE 1=1 {shop_filter}
                ORDER BY a.shop_name
                LIMIT 5000
            """

            cur.execute(query, params)
            rows = cur.fetchall()

            results = [
                {
                    "shop_id": row["shop_id"],
                    "shop_name": row["shop_name"],
                    "is_gsd_nl_shop": row["is_gsd_nl_shop"],
                    "is_gsd_be_shop": row["is_gsd_be_shop"],
                    "is_gsd_de_shop": row["is_gsd_de_shop"],
                    "shop_phase": row["shop_phase"],
                    "hide_online": row["hide_online"],
                    "is_disabled": row["is_disabled"],
                    "accountmanager_name": row["accountmanager_name"],
                }
                for row in rows
            ]

            return {"status": "success", "results": results, "total": len(results)}
    except Exception as e:
        logger.error(f"Error searching GSD: {e}")
        return {"status": "error", "error": str(e), "results": [], "total": 0}
    finally:
        return_redshift_connection(conn)
