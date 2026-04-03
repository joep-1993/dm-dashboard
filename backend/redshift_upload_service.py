"""
Redshift Upload Service - Uploads tabular data to Redshift pa.* tables.
"""

import logging
import re
from typing import List, Dict, Any
from io import BytesIO

import pandas as pd
from psycopg2.extras import execute_values
from backend.database import get_redshift_connection, return_redshift_connection

logger = logging.getLogger(__name__)

# Allowed table name pattern: alphanumeric + underscores only
TABLE_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
MAX_CHUNK_SIZE = 10_000


def validate_table_name(name: str) -> str:
    """Validate and return the full pa.<name> table name."""
    name = name.strip()
    if not TABLE_NAME_RE.match(name):
        raise ValueError(f"Invalid table name: '{name}'. Use only letters, numbers, and underscores.")
    return f"pa.{name}"


def parse_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """Parse an xlsx file into a DataFrame."""
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=0)
    df = df.dropna(how="all")
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def parse_pasted_data(headers: List[str], rows: List[List[str]]) -> pd.DataFrame:
    """Build a DataFrame from pasted headers + rows."""
    df = pd.DataFrame(rows, columns=headers)
    df = df.dropna(how="all")
    return df


def upload_to_redshift(
    df: pd.DataFrame,
    table_name: str,
    chunk_size: int = 5000,
) -> Dict[str, Any]:
    """Create table (if needed) and insert data in chunks."""
    full_table = validate_table_name(table_name)
    chunk_size = min(max(1, chunk_size), MAX_CHUNK_SIZE)

    if df.empty:
        return {"status": "error", "error": "No data to upload."}

    # Sanitise column names: lowercase, strip whitespace, replace spaces with underscores
    df.columns = [
        re.sub(r'[^a-z0-9_]', '_', col.strip().lower().replace(' ', '_'))
        for col in df.columns
    ]

    conn = get_redshift_connection()
    try:
        cur = conn.cursor()

        # Create table – all columns as VARCHAR(1000)
        col_defs = ',\n'.join([f'"{col}" VARCHAR(1000)' for col in df.columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS {full_table} (\n{col_defs}\n);'
        cur.execute(create_sql)
        conn.commit()

        # Prepare insert
        cols_formatted = ', '.join([f'"{col}"' for col in df.columns])
        insert_sql = f'INSERT INTO {full_table} ({cols_formatted}) VALUES %s'

        df = df.astype(object)
        total = len(df)
        uploaded = 0

        for start in range(0, total, chunk_size):
            end = min(start + chunk_size, total)
            chunk = df.iloc[start:end]
            values = [
                tuple(None if pd.isna(v) else str(v) for v in row)
                for row in chunk.to_numpy()
            ]
            execute_values(cur, insert_sql, values, page_size=chunk_size)
            conn.commit()
            uploaded += len(values)

        cur.close()
        return {
            "status": "success",
            "table": full_table,
            "rows_uploaded": uploaded,
            "columns": list(df.columns),
        }
    except Exception as e:
        conn.rollback()
        logger.error(f"Redshift upload error: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        return_redshift_connection(conn)
