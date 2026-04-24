"""
Persistent URL tracking for the Auto-Redirects tool.

Stores processed R-URLs in the shared n8n-vector-db so repeat runs
don't re-process URLs that already have a valid redirect assigned.
"""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS rurl_processed (
    original_url     TEXT PRIMARY KEY,
    redirect_url     TEXT,
    reliability_tier TEXT,
    reliability_score INT,
    match_type       TEXT,
    processed_at     TIMESTAMPTZ DEFAULT now()
)
"""

UPSERT_SQL = """
INSERT INTO rurl_processed
    (original_url, redirect_url, reliability_tier, reliability_score, match_type, processed_at)
VALUES %s
ON CONFLICT (original_url) DO UPDATE SET
    redirect_url      = EXCLUDED.redirect_url,
    reliability_tier  = EXCLUDED.reliability_tier,
    reliability_score = EXCLUDED.reliability_score,
    match_type        = EXCLUDED.match_type,
    processed_at      = now()
"""

_TABLE_READY = False


def ensure_table() -> None:
    """Create the table on first use. Idempotent."""
    global _TABLE_READY
    if _TABLE_READY:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(TABLE_DDL)
        conn.commit()
        _TABLE_READY = True
    finally:
        return_db_connection(conn)


def already_processed(urls: Iterable[str]) -> set[str]:
    """Return the subset of `urls` already present in rurl_processed."""
    ensure_table()
    url_list = [u for u in urls if u]
    if not url_list:
        return set()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT original_url FROM rurl_processed WHERE original_url = ANY(%s)",
                (url_list,),
            )
            return {r["original_url"] for r in cur.fetchall()}
    finally:
        return_db_connection(conn)


def upsert_results(df: pd.DataFrame) -> int:
    """Upsert optimizer output rows. Returns number of rows written."""
    if df.empty:
        return 0
    ensure_table()
    from psycopg2.extras import execute_values

    cols = ["original_url", "redirect_url", "reliability_tier", "reliability_score", "match_type"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"results CSV missing required columns: {missing}")

    def _int_or_none(v):
        try:
            return int(v) if pd.notna(v) else None
        except (ValueError, TypeError):
            return None

    rows = [
        (
            str(r["original_url"]),
            None if pd.isna(r["redirect_url"]) else str(r["redirect_url"]),
            None if pd.isna(r["reliability_tier"]) else str(r["reliability_tier"]),
            _int_or_none(r["reliability_score"]),
            None if pd.isna(r["match_type"]) else str(r["match_type"]),
            pd.Timestamp.now(tz="UTC"),
        )
        for _, r in df.iterrows()
        if pd.notna(r["original_url"])
    ]
    if not rows:
        return 0

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, template="(%s,%s,%s,%s,%s,%s)")
        conn.commit()
        return len(rows)
    finally:
        return_db_connection(conn)


def load_previous(urls: Iterable[str]) -> pd.DataFrame:
    """Fetch cached rows for the given URLs as a DataFrame."""
    ensure_table()
    url_list = [u for u in urls if u]
    if not url_list:
        return pd.DataFrame(columns=["original_url", "redirect_url", "reliability_tier",
                                     "reliability_score", "match_type", "processed_at"])
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT original_url, redirect_url, reliability_tier,
                          reliability_score, match_type, processed_at
                   FROM rurl_processed
                   WHERE original_url = ANY(%s)""",
                (url_list,),
            )
            return pd.DataFrame(cur.fetchall())
    finally:
        return_db_connection(conn)
