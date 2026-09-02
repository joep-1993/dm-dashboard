"""
GSD Check Service - Looks up GSD shop flags + shop metadata as of yesterday.
"""

import logging
from typing import Optional, List, Dict, Any
from backend.database import get_redshift_connection, return_redshift_connection

logger = logging.getLogger(__name__)

# Maximaal aantal rijen dat de UI terugkrijgt. Er wordt er één extra opgehaald om
# te kunnen zien of er is afgekapt.
LIMIT = 5000


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

            # TWEETRAPS. De latest_list-CTE hieronder draait
            # ROW_NUMBER() OVER (PARTITION BY shop_id ...) over bt.shop_list, en dat
            # zijn ~87,8 miljoen rijen. Het shopfilter stond alleen in de buitenste
            # WHERE op alias `a` en is niet door de LEFT JOIN heen te duwen, dus het
            # window draaide altijd over de hele tabel: gemeten 156 s voor 9 rijen.
            # Eerst de shop_ids resolven uit de kleine attributentabel en die dan in
            # BEIDE CTE's binden brengt dat terug naar ~1 s.
            cur.execute(f"""
                SELECT DISTINCT shop_id
                FROM beslistbi.bt.shop_main_attributes_by_day
                WHERE date = CURRENT_DATE - 1
                  AND deleted_ind = 0
                  {shop_filter.replace('a.', '')}
            """, params)
            matched_ids = [r["shop_id"] for r in cur.fetchall()]
            if not matched_ids:
                return {"status": "success", "results": [], "total": 0,
                        "returned": 0, "truncated": False}
            id_list = ",".join(str(int(i)) for i in matched_ids)

            query = f"""
                WITH yesterday_attrs AS (
                    SELECT shop_id,
                           shop_name,
                           is_gsd_nl_shop,
                           is_gsd_be_shop,
                           is_gsd_de_shop,
                           -- Same snapshot as the GSD flags on purpose. is_pixel_shop
                           -- is what decides a shop's derived model in GSD Campaigns
                           -- (CPR when is_wecantrack_shop OR is_pixel_shop, else CPC),
                           -- and it drops in the SAME feed update as the GSD flag — so
                           -- reading it from a different as-of date would hide exactly
                           -- the case you look this up for.
                           is_pixel_shop
                    FROM beslistbi.bt.shop_main_attributes_by_day
                    WHERE date = CURRENT_DATE - 1
                      AND deleted_ind = 0
                      AND shop_id IN ({id_list})
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
                      -- Zonder deze regel draait het window over alle ~87,8M rijen.
                      AND shop_id IN ({id_list})
                )
                SELECT a.shop_id,
                       a.shop_name,
                       a.is_gsd_nl_shop,
                       a.is_gsd_be_shop,
                       a.is_gsd_de_shop,
                       a.is_pixel_shop,
                       l.shop_phase,
                       l.hide_online,
                       l.is_disabled,
                       l.accountmanager_name
                FROM yesterday_attrs a
                LEFT JOIN latest_list l
                       ON l.shop_id = a.shop_id AND l.rn = 1
                ORDER BY a.shop_name
                LIMIT {LIMIT + 1}
            """

            cur.execute(query)
            rows = cur.fetchall()

            results = [
                {
                    "shop_id": row["shop_id"],
                    "shop_name": row["shop_name"],
                    "is_gsd_nl_shop": row["is_gsd_nl_shop"],
                    "is_gsd_be_shop": row["is_gsd_be_shop"],
                    "is_gsd_de_shop": row["is_gsd_de_shop"],
                    "is_pixel_shop": row["is_pixel_shop"],
                    "shop_phase": row["shop_phase"],
                    "hide_online": row["hide_online"],
                    "is_disabled": row["is_disabled"],
                    "accountmanager_name": row["accountmanager_name"],
                }
                for row in rows
            ]

            truncated = len(results) > LIMIT
            if truncated:
                # LIMIT 5000 kapte stil af en rapporteerde die 5000 als `total`, dus
                # een brede zoekterm gaf een willekeurig alfabetisch voorloopje dat
                # als "alles" las — en keyword_redirect_service kiest daar zijn
                # beste match uit.
                results = results[:LIMIT]
            return {"status": "success", "results": results,
                    "total": len(matched_ids), "returned": len(results),
                    "truncated": truncated}
    except Exception as e:
        logger.error(f"Error searching GSD: {e}")
        return {"status": "error", "error": str(e), "results": [], "total": 0}
    finally:
        return_redshift_connection(conn)
