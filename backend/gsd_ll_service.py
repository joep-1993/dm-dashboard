"""
GSD Low-Linkage Pause/Enable Service
=====================================

Reads the pixel-monitor GSD feed and pauses or re-enables GSD Shopping
campaigns based on each shop's linkage status:

- Feed rows with GSD = 0  -> shop dropped below the linkage threshold.
  If the shop is still a GSD shop (is_gsd_<country>_shop = 1 in
  beslistbi.bt.shop_list for the most recent date), every ENABLED campaign
  in that country's GSD account(s) whose name contains the ShopNaam is
  PAUSED and tagged with the label 'GSD_LL_PAUSED'.

- Feed rows with GSD = 1  -> shop recovered its linkage.
  If the shop is still a GSD shop, every campaign carrying the
  'GSD_LL_PAUSED' label (for that ShopNaam, in that country's account(s))
  is re-ENABLED and the label is removed.

Every pause / enable action is appended to pa.jvs_gsd_ll_campaigns in the
n8n-vector-db PostgreSQL DB so the frontend can show an audit trail.

Country -> account mapping and the Google Ads client are reused from
gsd_campaigns_service so this stays in sync with the rest of GSD Campaigns.
"""
import csv
import glob as glob_mod
import io
import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2

from backend.database import get_db_connection, return_db_connection, get_redshift_connection, return_redshift_connection
from backend.gsd_campaigns_service import _get_client, ACCOUNTS, _name_contains_regexp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEED_URL = "https://pixel-monitor.aks.beslist.nl/api/gsd/feed.csv"

LL_LABEL = "GSD_LL_PAUSED"

ADMIN_TABLE = "pa.jvs_gsd_ll_campaigns"

# Per-(shop, country) pause/enable cycle counters — how often a shop has been
# paused vs re-enabled by this tool. One "event" == one run that actually paused
# (or enabled) >=1 campaign for that shop+country, so a run touching a shop's 5
# campaigns bumps the counter once, not five times.
SHOP_CYCLES_TABLE = "pa.jvs_gsd_ll_shop_cycles"

# Map the shop_list GSD flag columns to a country code.
FLAG_TO_COUNTRY = {
    "is_gsd_nl_shop": "NL",
    "is_gsd_be_shop": "BE",
    "is_gsd_de_shop": "DE",
}

# ---------------------------------------------------------------------------
# Excel data source (daily scheduled runs)
# ---------------------------------------------------------------------------

EXCEL_DIR = r"C:\Users\l.davidowski\Documents\Schelduled scripts 2023\script_bc_signalering_gsd_nl_be_efficy"
EXCEL_SHEET = "Pixel linkage"
SCHEDULE_HOUR = 9
SCHEDULE_MINUTE = 50
AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")


def _country_customer_ids() -> Dict[str, Set[str]]:
    """Build {country: {customer_id, ...}} from the shared ACCOUNTS map.

    NL_CPR and NL_CPC share one customer_id; BE has two distinct accounts;
    DE has one. De-duplicated via a set so each account is touched once.
    """
    mapping: Dict[str, Set[str]] = {}
    for info in ACCOUNTS.values():
        mapping.setdefault(info["country"], set()).add(info["customer_id"])
    return mapping


COUNTRY_CUSTOMER_IDS = _country_customer_ids()


# ---------------------------------------------------------------------------
# Progress state (single in-process run at a time, polled by the frontend)
# ---------------------------------------------------------------------------

_LL_LOCK = threading.Lock()
_LL_PROGRESS: Dict[str, Any] = {
    "running": False, "phase": "idle", "total": 0, "processed": 0,
    "paused": 0, "enabled": 0, "skipped": 0, "errors": 0,
    "dry_run": False, "done": False, "result": None, "error": None,
    "started_at": None, "finished_at": None,
}


# ---------------------------------------------------------------------------
# Excel schedule state (daily auto-run at SCHEDULE_HOUR:SCHEDULE_MINUTE CET)
# ---------------------------------------------------------------------------

_EXCEL_TIMER: Optional[threading.Timer] = None
_EXCEL_LOCK = threading.Lock()
_EXCEL_STATE: Dict[str, Any] = {
    "enabled": True,
    "next_run_at": None,
    "last_run_at": None,
    "last_file": None,
    "last_error": None,
}

# Cached Excel data — loaded daily at SCHEDULE_HOUR:SCHEDULE_MINUTE or on
# demand via /ll/excel-load. The Preview/Run flow consumes this cache when
# source='excel', so the actual campaigns are only mutated when the user
# explicitly clicks "Run" in the dashboard.
_EXCEL_DATA: Dict[str, Any] = {
    "feed": None,
    "flags": None,
    "file": None,
    "loaded_at": None,
    "shop_count": 0,
    "pause_count": 0,
    "enable_count": 0,
}


def _progress_set(**kw: Any) -> None:
    with _LL_LOCK:
        _LL_PROGRESS.update(kw)


def get_ll_progress() -> Dict[str, Any]:
    """Snapshot of the current/last low-linkage run for the UI to poll."""
    with _LL_LOCK:
        return dict(_LL_PROGRESS)


def start_ll_run(
    dry_run: bool = False,
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
    source: str = "feed",
) -> Dict[str, Any]:
    """Kick off a low-linkage run in a background thread and return immediately.

    Returns {"started": True} or {"started": False, "busy": True} if a run is
    already in flight (only one at a time).
    """
    with _LL_LOCK:
        if _LL_PROGRESS["running"]:
            return {"started": False, "busy": True}
        _LL_PROGRESS.update({
            "running": True, "phase": "Starting…", "total": 0, "processed": 0,
            "paused": 0, "enabled": 0, "skipped": 0, "errors": 0,
            "dry_run": dry_run, "done": False, "result": None, "error": None,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": None,
        })

    def _worker() -> None:
        try:
            res = run_low_linkage(dry_run, date_str, shop_names, included, source)
            _progress_set(
                result=res, done=True, phase="Done",
                processed=_LL_PROGRESS.get("total", 0),
                paused=res.get("paused_count", len(res.get("paused", []))),
                enabled=res.get("enabled_count", len(res.get("enabled", []))),
                skipped=len(res.get("skipped", [])),
                errors=len(res.get("errors", [])),
            )
        except Exception as ex:  # pragma: no cover - defensive
            logger.exception("GSD LL run crashed")
            _progress_set(error=str(ex), done=True, phase="Error")
        finally:
            _progress_set(running=False, finished_at=datetime.now().isoformat(timespec="seconds"))

    threading.Thread(target=_worker, daemon=True).start()
    return {"started": True, "dry_run": dry_run}


def start_ll_apply(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Kick off a background run that applies ONLY the given preview entries.

    Used by the "Run selected" button: the frontend sends back the exact rows
    the user left checked in a dry-run preview, and this applies just those
    pause / enable mutations. Shares the single-run lock + progress state with
    start_ll_run, so a normal run and a selection apply can't overlap.
    """
    with _LL_LOCK:
        if _LL_PROGRESS["running"]:
            return {"started": False, "busy": True}
        _LL_PROGRESS.update({
            "running": True, "phase": "Starting…", "total": len(entries), "processed": 0,
            "paused": 0, "enabled": 0, "skipped": 0, "errors": 0,
            "dry_run": False, "done": False, "result": None, "error": None,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": None,
        })

    def _worker() -> None:
        try:
            res = apply_selected(entries)
            _progress_set(
                result=res, done=True, phase="Done",
                processed=_LL_PROGRESS.get("total", 0),
                paused=res.get("paused_count", len(res.get("paused", []))),
                enabled=res.get("enabled_count", len(res.get("enabled", []))),
                skipped=len(res.get("skipped", [])),
                errors=len(res.get("errors", [])),
            )
        except Exception as ex:  # pragma: no cover - defensive
            logger.exception("GSD LL apply crashed")
            _progress_set(error=str(ex), done=True, phase="Error")
        finally:
            _progress_set(running=False, finished_at=datetime.now().isoformat(timespec="seconds"))

    threading.Thread(target=_worker, daemon=True).start()
    return {"started": True, "dry_run": False}


# ---------------------------------------------------------------------------
# Feed
# ---------------------------------------------------------------------------


def fetch_feed(url: str = FEED_URL) -> List[Dict[str, Any]]:
    """Fetch and parse the GSD feed CSV.

    Returns a list of dicts with keys: shop_id (int), shop_name (str),
    linkage (float|None), gsd (int 0/1). Malformed rows are skipped.
    """
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # utf-8-sig strips the leading BOM the feed ships with.
    text = resp.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")

    rows: List[Dict[str, Any]] = []
    for raw in reader:
        shop_id_raw = (raw.get("ShopId") or "").strip()
        shop_name = (raw.get("ShopNaam") or "").strip()
        gsd_raw = (raw.get("GSD") or "").strip()
        if not shop_id_raw or not shop_name or gsd_raw not in ("0", "1"):
            continue
        try:
            shop_id = int(shop_id_raw)
        except ValueError:
            continue
        linkage_raw = (raw.get("LinkagePercentage") or "").strip().replace(",", ".")
        try:
            linkage = float(linkage_raw) if linkage_raw else None
        except ValueError:
            linkage = None
        rows.append({
            "shop_id": shop_id,
            "shop_name": shop_name,
            "linkage": linkage,
            "gsd": int(gsd_raw),
        })
    logger.info("GSD LL: fetched %d usable feed rows", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Excel data source
# ---------------------------------------------------------------------------


def _newest_excel(directory: str = EXCEL_DIR) -> Optional[str]:
    """Return the path to the newest gsd_shops_nl_be_*.xlsx file."""
    pattern = os.path.join(directory, "gsd_shops_nl_be_*.xlsx")
    files = glob_mod.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def fetch_feed_from_excel(
    filepath: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, int]], str]:
    """Read the 'Pixel linkage' sheet from an Excel file.

    Returns (feed_rows, flags_by_shop, filepath) where:
    - feed_rows: [{shop_id, shop_name, linkage, gsd}, ...] — same shape as fetch_feed()
    - flags_by_shop: {shop_id: {is_gsd_nl_shop, is_gsd_be_shop, is_gsd_de_shop}}
    - filepath: the actual file that was read
    """
    import pandas as pd

    if filepath is None:
        filepath = _newest_excel()
    if filepath is None:
        raise FileNotFoundError(f"No gsd_shops_nl_be_*.xlsx files found in {EXCEL_DIR}")

    df = pd.read_excel(filepath, sheet_name=EXCEL_SHEET, engine="openpyxl")

    feed: List[Dict[str, Any]] = []
    flags: Dict[int, Dict[str, int]] = {}
    for _, row in df.iterrows():
        shop_id = int(row["shop_id"])
        linkage_val = row.get("LinkagePercentage")
        feed.append({
            "shop_id": shop_id,
            "shop_name": str(row["ShopNaam"]),
            "linkage": float(linkage_val) if pd.notna(linkage_val) else None,
            "gsd": int(row["linkage_gsd"]),
        })
        flags[shop_id] = {
            "is_gsd_nl_shop": int(row.get("is_gsd_nl", 0) or 0),
            "is_gsd_be_shop": int(row.get("is_gsd_be", 0) or 0),
            "is_gsd_de_shop": int(row.get("is_gsd_de", 0) or 0),
        }

    logger.info("GSD LL Excel: read %d rows from %s", len(feed), os.path.basename(filepath))
    return feed, flags, filepath


def _send_slack(text: str) -> None:
    """Best-effort Slack DM using the shared bot token."""
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    user_id = os.environ.get("SLACK_USER_ID", "")
    if not token or not user_id:
        return
    try:
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": user_id, "text": text},
            timeout=15,
        )
    except Exception:
        logger.warning("GSD LL: Slack notification failed", exc_info=True)


def load_excel_data(filepath: Optional[str] = None) -> Dict[str, Any]:
    """Read the newest Excel file and store in the in-memory cache.

    Called daily by the scheduler and on-demand via POST /ll/excel-load.
    Does NOT pause/enable any campaigns — that only happens when the user
    clicks Preview or Run in the dashboard with source='excel'.
    """
    feed, flags, path = fetch_feed_from_excel(filepath)
    fname = os.path.basename(path)
    pause_n = sum(1 for r in feed if r["gsd"] == 0)
    enable_n = sum(1 for r in feed if r["gsd"] == 1)
    status = {
        "feed": feed,
        "flags": flags,
        "file": fname,
        "loaded_at": datetime.now(AMSTERDAM_TZ).isoformat(timespec="seconds"),
        "shop_count": len(feed),
        "pause_count": pause_n,
        "enable_count": enable_n,
    }
    with _EXCEL_LOCK:
        _EXCEL_DATA.update(status)
    logger.info(
        "GSD LL Excel cache loaded: %d shops (%d pause, %d enable) from %s",
        status["shop_count"], pause_n, enable_n, fname,
    )
    _send_slack(
        f":white_check_mark: *GSD Low Linkage — Excel data loaded*\n"
        f"File: {fname}\n"
        f"Shops: {len(feed)} ({pause_n} to pause, {enable_n} to enable)\n"
        f"Ready for Preview / Run in the dashboard."
    )
    return get_excel_data_status()


def get_excel_data_status() -> Dict[str, Any]:
    """Return the cached Excel data status (without the raw data itself)."""
    with _EXCEL_LOCK:
        return {
            "file": _EXCEL_DATA["file"],
            "loaded_at": _EXCEL_DATA["loaded_at"],
            "shop_count": _EXCEL_DATA["shop_count"],
            "pause_count": _EXCEL_DATA["pause_count"],
            "enable_count": _EXCEL_DATA["enable_count"],
            "has_data": _EXCEL_DATA["feed"] is not None,
        }


# ---------------------------------------------------------------------------
# Redshift: GSD shop flags for the most recent date
# ---------------------------------------------------------------------------


def get_shop_flags(shop_ids: List[int], date_str: Optional[str] = None) -> Dict[int, Dict[str, int]]:
    """Return {shop_id: {is_gsd_nl_shop, is_gsd_be_shop, is_gsd_de_shop}}.

    Uses the most recent row per shop in beslistbi.bt.shop_list, matching the
    ROW_NUMBER()-over-dim_date_key-DESC pattern used elsewhere in GSD tooling.
    If date_str (YYYY-MM-DD) is given, evaluates flags as of that date (most
    recent row on or before it) instead of the absolute latest.
    """
    if not shop_ids:
        return {}

    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(shop_ids))
            params = list(shop_ids)
            date_filter = ""
            if date_str:
                date_filter = " AND dim_date_key <= CAST(TO_CHAR(CAST(%s AS DATE), 'YYYYMMDD') AS BIGINT)"
                params.append(date_str)
            cur.execute(f"""
                WITH latest AS (
                    SELECT shop_id,
                           is_gsd_nl_shop,
                           is_gsd_be_shop,
                           is_gsd_de_shop,
                           ROW_NUMBER() OVER (
                               PARTITION BY shop_id ORDER BY dim_date_key DESC
                           ) AS rn
                    FROM beslistbi.bt.shop_list
                    WHERE deleted_ind = 0
                      AND shop_id IN ({placeholders})
                      {date_filter}
                )
                SELECT shop_id, is_gsd_nl_shop, is_gsd_be_shop, is_gsd_de_shop
                FROM latest
                WHERE rn = 1
            """, params)
            rows = cur.fetchall()

        flags: Dict[int, Dict[str, int]] = {}
        for row in rows:
            flags[int(row["shop_id"])] = {
                "is_gsd_nl_shop": int(row["is_gsd_nl_shop"] or 0),
                "is_gsd_be_shop": int(row["is_gsd_be_shop"] or 0),
                "is_gsd_de_shop": int(row["is_gsd_de_shop"] or 0),
            }
        return flags
    finally:
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Admin table (n8n-vector-db PostgreSQL)
# ---------------------------------------------------------------------------


def ensure_admin_table() -> None:
    """Create the audit table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ADMIN_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    shop_id       BIGINT,
                    shop_name     TEXT,
                    country       VARCHAR(4),
                    action        VARCHAR(16),   -- 'Paused' | 'Enabled'
                    campaign_id   TEXT,
                    campaign_name TEXT,
                    customer_id   TEXT,
                    linkage       NUMERIC,
                    created_at    TIMESTAMPTZ DEFAULT now()
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def _record_action(
    conn,
    shop_id: int,
    shop_name: str,
    country: str,
    action: str,
    campaign_id: str,
    campaign_name: str,
    customer_id: str,
    linkage: Optional[float],
) -> None:
    """Insert one audit row (caller owns the transaction / commit)."""
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {ADMIN_TABLE}
                (shop_id, shop_name, country, action, campaign_id,
                 campaign_name, customer_id, linkage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (shop_id, shop_name, country, action, campaign_id,
              campaign_name, customer_id, linkage))


def get_history(limit: int = 500) -> List[Dict[str, Any]]:
    """Return the most recent audit rows for the frontend."""
    ensure_admin_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT shop_id, shop_name, country, action, campaign_id,
                       campaign_name, customer_id, linkage, created_at
                FROM {ADMIN_TABLE}
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Shop-level pause/enable cycle counters (n8n-vector-db PostgreSQL)
# ---------------------------------------------------------------------------


def ensure_shop_cycles_table() -> None:
    """Create the per-(shop, country) cycle-counter table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SHOP_CYCLES_TABLE} (
                    shop_id          BIGINT      NOT NULL,
                    shop_name        TEXT,
                    country          VARCHAR(4)  NOT NULL,
                    pause_count      INTEGER     NOT NULL DEFAULT 0,
                    enable_count     INTEGER     NOT NULL DEFAULT 0,
                    last_paused_at   TIMESTAMPTZ,
                    last_enabled_at  TIMESTAMPTZ,
                    currently_paused BOOLEAN,
                    updated_at       TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (shop_id, country)
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def _bump_shop_cycles(conn, shop_id: int, shop_name: str, country: str, action: str) -> None:
    """
    Increment the pause/enable counter for one (shop, country) by exactly one
    event (the caller must call this at most once per run per shop+country+action,
    NOT once per campaign). Also stamps the last-action time, refreshes shop_name,
    and sets currently_paused. Caller owns the transaction / commit.
    """
    if action == "Paused":
        cnt_col, ts_col, now_paused = "pause_count", "last_paused_at", True
    elif action == "Enabled":
        cnt_col, ts_col, now_paused = "enable_count", "last_enabled_at", False
    else:
        return  # unknown action — nothing to record
    with conn.cursor() as cur:
        # Column names come from the fixed action branch above (never user input).
        cur.execute(f"""
            INSERT INTO {SHOP_CYCLES_TABLE}
                (shop_id, shop_name, country, {cnt_col}, {ts_col},
                 currently_paused, updated_at)
            VALUES (%s, %s, %s, 1, now(), %s, now())
            ON CONFLICT (shop_id, country) DO UPDATE SET
                shop_name        = EXCLUDED.shop_name,
                {cnt_col}        = {SHOP_CYCLES_TABLE}.{cnt_col} + 1,
                {ts_col}         = now(),
                currently_paused = EXCLUDED.currently_paused,
                updated_at       = now()
        """, (shop_id, shop_name, country, now_paused))


def get_shop_cycles(limit: int = 1000) -> List[Dict[str, Any]]:
    """Return the per-(shop, country) cycle counters for the frontend."""
    ensure_shop_cycles_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT shop_id, shop_name, country, pause_count, enable_count,
                       last_paused_at, last_enabled_at, currently_paused, updated_at
                FROM {SHOP_CYCLES_TABLE}
                ORDER BY (pause_count + enable_count) DESC, shop_name, country
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        return_db_connection(conn)


def backfill_shop_cycles(gap_minutes: int = 30, dry_run: bool = True) -> Dict[str, Any]:
    """
    Seed the cycle counters from the existing per-campaign action log
    (pa.jvs_gsd_ll_campaigns). That log has no run_id, so rows are grouped into
    "events" by time: consecutive same-(shop, country, action) rows more than
    ``gap_minutes`` apart count as separate events. Counts are therefore
    APPROXIMATE for history; from the next run onward they are exact.

    dry_run=True only reports the counts it would write. dry_run=False replaces
    the table's counters with the backfilled values (idempotent).
    """
    ensure_shop_cycles_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # One row per action per (shop, country), collapsing bursts within
            # gap_minutes into a single event via a gap-and-islands count.
            cur.execute(f"""
                WITH ordered AS (
                    SELECT shop_id, shop_name, country, action, created_at,
                           LAG(created_at) OVER (
                               PARTITION BY shop_id, country, action
                               ORDER BY created_at
                           ) AS prev_at
                    FROM {ADMIN_TABLE}
                    WHERE shop_id IS NOT NULL AND country IS NOT NULL
                      AND action IN ('Paused', 'Enabled')
                ),
                events AS (
                    SELECT shop_id, shop_name, country, action, created_at,
                           CASE WHEN prev_at IS NULL
                                     OR created_at - prev_at > interval '%s minutes'
                                THEN 1 ELSE 0 END AS is_new_event
                    FROM ordered
                )
                SELECT shop_id, country,
                       MAX(shop_name) AS shop_name,
                       SUM(CASE WHEN action='Paused'  THEN is_new_event ELSE 0 END) AS pause_events,
                       SUM(CASE WHEN action='Enabled' THEN is_new_event ELSE 0 END) AS enable_events,
                       MAX(created_at) FILTER (WHERE action='Paused')  AS last_paused_at,
                       MAX(created_at) FILTER (WHERE action='Enabled') AS last_enabled_at
                FROM events
                GROUP BY shop_id, country
            """, (gap_minutes,))
            rows = [dict(r) for r in cur.fetchall()]

        for r in rows:
            lp, le = r["last_paused_at"], r["last_enabled_at"]
            # Currently paused if the most recent event was a pause.
            r["currently_paused"] = (
                lp is not None and (le is None or lp >= le)
            )

        summary = {
            "dry_run": dry_run,
            "gap_minutes": gap_minutes,
            "shops_country_rows": len(rows),
            "total_pause_events": sum(r["pause_events"] for r in rows),
            "total_enable_events": sum(r["enable_events"] for r in rows),
        }

        if not dry_run and rows:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(f"""
                        INSERT INTO {SHOP_CYCLES_TABLE}
                            (shop_id, shop_name, country, pause_count, enable_count,
                             last_paused_at, last_enabled_at, currently_paused, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (shop_id, country) DO UPDATE SET
                            shop_name        = EXCLUDED.shop_name,
                            pause_count      = EXCLUDED.pause_count,
                            enable_count     = EXCLUDED.enable_count,
                            last_paused_at   = EXCLUDED.last_paused_at,
                            last_enabled_at  = EXCLUDED.last_enabled_at,
                            currently_paused = EXCLUDED.currently_paused,
                            updated_at       = now()
                    """, (r["shop_id"], r["shop_name"], r["country"],
                          int(r["pause_events"]), int(r["enable_events"]),
                          r["last_paused_at"], r["last_enabled_at"],
                          r["currently_paused"]))
            conn.commit()
            summary["written"] = len(rows)

        summary["rows"] = rows
        return summary
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Google Ads label helpers (shared client)
# ---------------------------------------------------------------------------


def _escape_gaql(value: str) -> str:
    """Escape a string literal for a GAQL query."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _ensure_label(client, customer_id: str, label_name: str) -> str:
    """Return the resource name of label_name in the account, creating it if
    absent."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT label.resource_name
        FROM label
        WHERE label.name = '{_escape_gaql(label_name)}'
    """
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            return row.label.resource_name
    except GoogleAdsException as ex:
        logger.warning("Label lookup failed for %s in %s: %s", label_name, customer_id, ex)

    label_service = client.get_service("LabelService")
    op = client.get_type("LabelOperation")
    label = op.create
    label.name = label_name
    label.text_label.background_color = "#E0A800"
    label.text_label.description = "Paused by GSD low-linkage automation"
    response = label_service.mutate_labels(customer_id=customer_id, operations=[op])
    return response.results[0].resource_name


def _apply_label(client, customer_id: str, campaign_resource: str, label_resource: str) -> bool:
    """Attach a label to a campaign. Returns True on success, False on failure.

    The return value matters: if a campaign is PAUSED but the GSD_LL_PAUSED
    label fails to attach, it becomes invisible to the re-enable lookup forever,
    so callers must be able to detect and compensate for the failure.
    """
    service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.create.campaign = campaign_resource
    op.create.label = label_resource
    try:
        service.mutate_campaign_labels(customer_id=customer_id, operations=[op])
        return True
    except GoogleAdsException as ex:
        logger.warning("Could not apply label to %s: %s", campaign_resource, ex)
        return False


def _remove_campaign_label(client, customer_id: str, campaign_label_resource: str) -> bool:
    """Detach a label from a campaign given the campaign_label resource name.
    Returns True on success, False on failure."""
    service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.remove = campaign_label_resource
    try:
        service.mutate_campaign_labels(customer_id=customer_id, operations=[op])
        return True
    except GoogleAdsException as ex:
        logger.warning("Could not remove label %s: %s", campaign_label_resource, ex)
        return False


def _set_status(client, customer_id: str, campaign_id: str, status: str) -> None:
    """Set a campaign's status (ENABLED / PAUSED) using the shared client."""
    campaign_service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    campaign = op.update
    campaign.resource_name = campaign_service.campaign_path(customer_id, campaign_id)
    campaign.status = getattr(client.enums.CampaignStatusEnum, status)
    op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
    campaign_service.mutate_campaigns(customer_id=customer_id, operations=[op])


# ---------------------------------------------------------------------------
# Campaign lookups
# ---------------------------------------------------------------------------


def _find_enabled_campaigns(client, customer_id: str, shop_id: int) -> List[Dict[str, str]]:
    """ENABLED GSD Shopping campaigns in the account for this shop.

    Matches on the exact ``[shop_id:{id}]`` token that every GSD campaign name
    carries (verified across the full audit history) — a numeric, delimited
    match that avoids the false positives of a bare ``LIKE '%shopname%'``
    substring (e.g. "Bol" hitting "Bol.com"/"Carbol") — and restricts to
    ``SHOPPING`` so no Search/PMax/Display campaign is ever paused.
    """
    ga_service = client.get_service("GoogleAdsService")
    # REGEXP_MATCH, not LIKE: brackets make LIKE match the whole account
    # (see _name_contains_regexp).
    name_pattern = _name_contains_regexp(f"[shop_id:{int(shop_id)}]")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND campaign.advertising_channel_type = 'SHOPPING'
          AND campaign.name REGEXP_MATCH '{name_pattern}'
    """
    out: List[Dict[str, str]] = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "resource_name": row.campaign.resource_name,
            })
    except GoogleAdsException as ex:
        logger.error("Enabled-campaign lookup failed (%s, shop_id=%s): %s", customer_id, shop_id, ex)
        raise
    return out


def _find_labeled_campaigns(client, customer_id: str, shop_id: int) -> List[Dict[str, str]]:
    """Non-removed GSD Shopping campaigns carrying the GSD_LL_PAUSED label for
    this shop. Returns campaign + campaign_label resource names.

    Same exact ``[shop_id:{id}]`` token + ``SHOPPING`` guard as
    _find_enabled_campaigns (see its docstring).
    """
    ga_service = client.get_service("GoogleAdsService")
    # REGEXP_MATCH, not LIKE: brackets make LIKE match the whole account
    # (see _name_contains_regexp).
    name_pattern = _name_contains_regexp(f"[shop_id:{int(shop_id)}]")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name,
               campaign.status, campaign_label.resource_name
        FROM campaign_label
        WHERE label.name = '{LL_LABEL}'
          AND campaign.status != 'REMOVED'
          AND campaign.advertising_channel_type = 'SHOPPING'
          AND campaign.name REGEXP_MATCH '{name_pattern}'
    """
    out: List[Dict[str, str]] = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "resource_name": row.campaign.resource_name,
                "campaign_label_resource": row.campaign_label.resource_name,
            })
    except GoogleAdsException as ex:
        logger.error("Labeled-campaign lookup failed (%s, shop_id=%s): %s", customer_id, shop_id, ex)
        raise
    return out


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------


def _countries_for_shop(flags: Dict[str, int]) -> List[str]:
    """Countries where the shop is flagged as a GSD shop (flag == 1)."""
    return [country for col, country in FLAG_TO_COUNTRY.items() if flags.get(col) == 1]


def run_low_linkage(
    dry_run: bool = False,
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
    source: str = "feed",
) -> Dict[str, Any]:
    """Fetch the feed and pause / re-enable low-linkage GSD campaigns.

    Parameters
    ----------
    dry_run : if True, no Google Ads mutations or DB writes happen; the return
        value lists exactly what *would* be paused / enabled.
    date_str : optional YYYY-MM-DD; evaluate the shop_list GSD flags as of this
        date (most recent row on or before it). Defaults to the absolute latest.
        Ignored when source='excel' (the Excel already contains the flags).
    shop_names : optional list of feed shop names to scope the run to.
    included : with shop_names, True = process ONLY those shops, False = process
        all EXCEPT those. Ignored when shop_names is empty.
    source : 'feed' (pixel-monitor CSV + Redshift flags) or 'excel' (local
        Excel file — uses the newest gsd_shops_nl_be_*.xlsx from EXCEL_DIR,
        which already contains the country flags so no Redshift query is needed).
    """
    started = datetime.now()
    result: Dict[str, Any] = {
        "started_at": started.isoformat(timespec="seconds"),
        "dry_run": dry_run,
        "date": date_str,
        "source": source,
        "feed_rows": 0,
        "paused": [],
        "enabled": [],
        "skipped": [],
        "errors": [],
    }

    # 1. Feed — from cached Excel data, fresh Excel read, or pixel-monitor CSV
    excel_flags: Optional[Dict[int, Dict[str, int]]] = None
    if source == "excel":
        # Prefer the in-memory cache (populated daily by the scheduler or
        # manually via /ll/excel-load). Fall back to a direct file read if
        # the cache is empty (first start or after a server restart).
        with _EXCEL_LOCK:
            cached_feed = _EXCEL_DATA["feed"]
            cached_flags = _EXCEL_DATA["flags"]
            cached_file = _EXCEL_DATA["file"]
        if cached_feed is not None:
            _progress_set(phase=f"Using cached Excel data ({cached_file})…")
            feed = list(cached_feed)          # shallow copy — safe to filter
            excel_flags = cached_flags
            result["excel_file"] = cached_file
        else:
            _progress_set(phase="Reading Excel (no cache yet)…")
            try:
                feed, excel_flags, excel_path = fetch_feed_from_excel()
                result["excel_file"] = os.path.basename(excel_path)
            except Exception as ex:
                logger.error("GSD LL: failed to read Excel: %s", ex)
                result["errors"].append({"step": "read_excel", "error": str(ex)})
                return result
    else:
        _progress_set(phase="Fetching linkage feed…")
        try:
            feed = fetch_feed()
        except Exception as ex:
            logger.error("GSD LL: failed to fetch feed: %s", ex)
            result["errors"].append({"step": "fetch_feed", "error": str(ex)})
            return result

    # Optional shop-name include/exclude filter (case-insensitive on ShopNaam).
    if shop_names:
        wanted = {s.strip().lower() for s in shop_names if s.strip()}
        if wanted:
            feed = [r for r in feed
                    if (r["shop_name"].lower() in wanted) == bool(included)]

    result["feed_rows"] = len(feed)
    if not feed:
        _progress_set(total=0, phase="No shops to process")
        return result

    # 2. Flags — from Excel (already loaded) or from Redshift
    if excel_flags is not None:
        flags_by_shop = excel_flags
        _progress_set(total=len(feed), phase="Processing shops…")
    else:
        _progress_set(total=len(feed), phase="Reading shop GSD flags…")
        try:
            flags_by_shop = get_shop_flags([r["shop_id"] for r in feed], date_str)
        except Exception as ex:
            logger.error("GSD LL: failed to fetch shop flags: %s", ex)
            result["errors"].append({"step": "shop_flags", "error": str(ex)})
            return result

    _progress_set(phase="Processing shops…")

    # 3. Shared Google Ads client + per-account label cache
    client = _get_client()
    label_cache: Dict[str, str] = {}

    def label_resource(customer_id: str) -> str:
        if customer_id not in label_cache:
            label_cache[customer_id] = _ensure_label(client, customer_id, LL_LABEL)
        return label_cache[customer_id]

    if not dry_run:
        ensure_admin_table()
        ensure_shop_cycles_table()
    db_conn = None if dry_run else get_db_connection()

    # (shop_id, country, action) -> shop_name for every shop+country actually
    # mutated this run, so the shop-cycle counter is bumped once per event (not
    # once per campaign) after the feed loop.
    cycle_events: Dict[tuple, str] = {}

    try:
        for idx, row in enumerate(feed):
            _progress_set(
                processed=idx,
                paused=len(result["paused"]), enabled=len(result["enabled"]),
                skipped=len(result["skipped"]), errors=len(result["errors"]),
            )
            shop_id = row["shop_id"]
            shop_name = row["shop_name"]
            gsd = row["gsd"]
            linkage = row["linkage"]

            flags = flags_by_shop.get(shop_id)
            if flags is None:
                result["skipped"].append({
                    "shop_id": shop_id, "shop_name": shop_name,
                    "reason": "not_found_in_shop_list",
                })
                continue

            countries = _countries_for_shop(flags)
            if not countries:
                result["skipped"].append({
                    "shop_id": shop_id, "shop_name": shop_name,
                    "reason": "not_a_gsd_shop",
                })
                continue

            action = "Paused" if gsd == 0 else "Enabled"

            for country in countries:
                for customer_id in sorted(COUNTRY_CUSTOMER_IDS.get(country, set())):
                    try:
                        if gsd == 0:
                            campaigns = _find_enabled_campaigns(client, customer_id, shop_id)
                        else:
                            campaigns = _find_labeled_campaigns(client, customer_id, shop_id)
                    except Exception as ex:
                        result["errors"].append({
                            "shop_id": shop_id, "shop_name": shop_name,
                            "country": country, "customer_id": customer_id,
                            "step": "lookup", "error": str(ex),
                        })
                        continue

                    for camp in campaigns:
                        entry = {
                            "shop_id": shop_id,
                            "shop_name": shop_name,
                            "country": country,
                            "customer_id": customer_id,
                            "campaign_id": camp["campaign_id"],
                            "campaign_name": camp["campaign_name"],
                            "linkage": linkage,
                        }
                        # Carry the campaign_label resource on enable rows so a
                        # later "Run selected" can detach the label without a
                        # re-lookup (falls back to re-query if absent).
                        if camp.get("campaign_label_resource"):
                            entry["campaign_label_resource"] = camp["campaign_label_resource"]

                        if dry_run:
                            (result["paused"] if gsd == 0 else result["enabled"]).append(entry)
                            continue

                        try:
                            if gsd == 0:
                                _set_status(client, customer_id, camp["campaign_id"], "PAUSED")
                                if not _apply_label(client, customer_id, camp["resource_name"],
                                                    label_resource(customer_id)):
                                    # Paused-but-untagged is invisible to the re-enable
                                    # lookup forever — roll the pause back and error out.
                                    _set_status(client, customer_id, camp["campaign_id"], "ENABLED")
                                    raise RuntimeError("label apply failed after pause; pause rolled back")
                            else:
                                _set_status(client, customer_id, camp["campaign_id"], "ENABLED")
                                _remove_campaign_label(client, customer_id,
                                                       camp["campaign_label_resource"])
                        except Exception as ex:
                            logger.error("GSD LL: %s failed for shop_id=%s / %s: %s",
                                         action, shop_id, camp["campaign_id"], ex)
                            result["errors"].append({**entry, "step": action.lower(),
                                                     "error": str(ex)})
                            continue

                        # Ads mutation succeeded. A failed audit write must NOT
                        # reclassify a real live mutation as an error — the
                        # campaign IS changed. Count it, then best-effort audit.
                        (result["paused"] if gsd == 0 else result["enabled"]).append(entry)
                        cycle_events[(shop_id, country, action)] = shop_name
                        try:
                            _record_action(
                                db_conn, shop_id, shop_name, country, action,
                                camp["campaign_id"], camp["campaign_name"],
                                customer_id, linkage,
                            )
                            db_conn.commit()
                        except Exception as ex:
                            db_conn.rollback()
                            logger.error("GSD LL: audit-write failed for %s / %s "
                                         "(mutation already applied): %s",
                                         action, camp["campaign_id"], ex)
                            result.setdefault("audit_failures", []).append(
                                {**entry, "error": str(ex)})

        # One shop-cycle bump per (shop, country) event actually mutated this run
        # (best-effort; a counter miss must never fail a real mutation).
        for (s_id, ctry, act), s_name in cycle_events.items():
            try:
                _bump_shop_cycles(db_conn, s_id, s_name, ctry, act)
                db_conn.commit()
            except Exception as ex:
                db_conn.rollback()
                logger.error("GSD LL: shop-cycle bump failed for shop_id=%s / %s: %s",
                             s_id, ctry, ex)
    finally:
        if db_conn is not None:
            return_db_connection(db_conn)

    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["paused_count"] = len(result["paused"])
    result["enabled_count"] = len(result["enabled"])
    logger.info("GSD LL done (dry_run=%s): %d paused, %d enabled, %d skipped, %d errors",
                dry_run, result["paused_count"], result["enabled_count"],
                len(result["skipped"]), len(result["errors"]))
    return result


def apply_selected(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply pause / enable for an explicit list of preview entries.

    Each entry is a row the user left checked in a dry-run preview and carries:
    action ('Paused' | 'Enabled'), customer_id, campaign_id, shop_id, shop_name,
    country, campaign_name, linkage, and — for 'Enabled' rows — optionally
    campaign_label_resource (re-queried if missing). No feed fetch happens; only
    the selected campaigns are touched. Every applied action is audited exactly
    like run_low_linkage.
    """
    started = datetime.now()
    result: Dict[str, Any] = {
        "started_at": started.isoformat(timespec="seconds"),
        "dry_run": False,
        "date": None,
        "feed_rows": len(entries),
        "paused": [],
        "enabled": [],
        "skipped": [],
        "errors": [],
    }

    if not entries:
        result["finished_at"] = datetime.now().isoformat(timespec="seconds")
        result["paused_count"] = 0
        result["enabled_count"] = 0
        return result

    _progress_set(phase="Applying selection…", total=len(entries))

    client = _get_client()
    campaign_service = client.get_service("CampaignService")
    label_cache: Dict[str, str] = {}
    # Cache of labeled-campaign lookups per (customer_id, shop_id) so the
    # enable fallback doesn't re-query the same account/shop repeatedly.
    labeled_cache: Dict[tuple, Dict[str, str]] = {}

    def label_resource(customer_id: str) -> str:
        if customer_id not in label_cache:
            label_cache[customer_id] = _ensure_label(client, customer_id, LL_LABEL)
        return label_cache[customer_id]

    def campaign_label_for(customer_id: str, shop_id: Any, campaign_id: str) -> Optional[str]:
        key = (customer_id, str(shop_id))
        if key not in labeled_cache:
            try:
                found = {c["campaign_id"]: c.get("campaign_label_resource")
                         for c in _find_labeled_campaigns(client, customer_id, int(shop_id))}
            except (ValueError, TypeError):
                found = {}  # non-numeric shop_id — can't run the fallback lookup
            labeled_cache[key] = found
        return labeled_cache[key].get(campaign_id)

    ensure_admin_table()
    ensure_shop_cycles_table()
    db_conn = get_db_connection()

    # (shop_id, country, action) -> shop_name, bumped once per event after the loop.
    cycle_events: Dict[tuple, str] = {}

    try:
        for idx, e in enumerate(entries):
            _progress_set(
                processed=idx,
                paused=len(result["paused"]), enabled=len(result["enabled"]),
                skipped=len(result["skipped"]), errors=len(result["errors"]),
            )

            action = (e.get("action") or "").strip()
            customer_id = str(e.get("customer_id") or "").strip()
            campaign_id = str(e.get("campaign_id") or "").strip()
            shop_id = e.get("shop_id")
            shop_name = e.get("shop_name") or ""
            country = e.get("country") or ""
            campaign_name = e.get("campaign_name") or ""
            linkage = e.get("linkage")

            if action not in ("Paused", "Enabled") or not customer_id or not campaign_id:
                result["skipped"].append({**e, "reason": "invalid_entry"})
                continue

            try:
                if action == "Paused":
                    _set_status(client, customer_id, campaign_id, "PAUSED")
                    if not _apply_label(
                        client, customer_id,
                        campaign_service.campaign_path(customer_id, campaign_id),
                        label_resource(customer_id),
                    ):
                        # Paused-but-untagged is invisible to re-enable forever —
                        # roll the pause back and error out (see run_low_linkage).
                        _set_status(client, customer_id, campaign_id, "ENABLED")
                        raise RuntimeError("label apply failed after pause; pause rolled back")
                else:
                    _set_status(client, customer_id, campaign_id, "ENABLED")
                    label_link = e.get("campaign_label_resource") or \
                        campaign_label_for(customer_id, shop_id, campaign_id)
                    if label_link:
                        _remove_campaign_label(client, customer_id, label_link)
            except Exception as ex:
                logger.error("GSD LL apply: %s failed for shop_id=%s / %s: %s",
                             action, shop_id, campaign_id, ex)
                result["errors"].append({**e, "step": action.lower(), "error": str(ex)})
                continue

            # Ads mutation succeeded — count it, then best-effort audit so a DB
            # failure doesn't misreport a real live mutation as an error.
            (result["paused"] if action == "Paused" else result["enabled"]).append(e)
            if shop_id is not None and country:
                cycle_events[(shop_id, country, action)] = shop_name
            try:
                _record_action(
                    db_conn, shop_id, shop_name, country, action,
                    campaign_id, campaign_name, customer_id, linkage,
                )
                db_conn.commit()
            except Exception as ex:
                db_conn.rollback()
                logger.error("GSD LL apply: audit-write failed for %s / %s "
                             "(mutation already applied): %s", action, campaign_id, ex)
                result.setdefault("audit_failures", []).append({**e, "error": str(ex)})

        # One shop-cycle bump per (shop, country) event actually mutated (best-effort).
        for (s_id, ctry, act), s_name in cycle_events.items():
            try:
                _bump_shop_cycles(db_conn, s_id, s_name, ctry, act)
                db_conn.commit()
            except Exception as ex:
                db_conn.rollback()
                logger.error("GSD LL apply: shop-cycle bump failed for shop_id=%s / %s: %s",
                             s_id, ctry, ex)
    finally:
        return_db_connection(db_conn)

    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["paused_count"] = len(result["paused"])
    result["enabled_count"] = len(result["enabled"])
    logger.info("GSD LL apply done: %d paused, %d enabled, %d skipped, %d errors",
                result["paused_count"], result["enabled_count"],
                len(result["skipped"]), len(result["errors"]))
    return result


# ---------------------------------------------------------------------------
# Daily Excel scheduler
# ---------------------------------------------------------------------------


def _seconds_until(hour: int, minute: int) -> Tuple[float, datetime]:
    """Seconds from now until the next occurrence of hour:minute in Amsterdam time."""
    now = datetime.now(AMSTERDAM_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds(), target


def _excel_scheduled_run() -> None:
    """Called by the timer: load (cache) the newest Excel data and reschedule.

    Does NOT pause/enable any campaigns — only refreshes the in-memory cache
    so the user can Preview/Run from the dashboard using the latest data.
    """
    try:
        logger.info("GSD LL Excel scheduler: loading daily data")
        with _EXCEL_LOCK:
            _EXCEL_STATE["last_run_at"] = datetime.now(AMSTERDAM_TZ).isoformat(timespec="seconds")
            _EXCEL_STATE["last_error"] = None

        status = load_excel_data()

        with _EXCEL_LOCK:
            _EXCEL_STATE["last_file"] = status.get("file")
    except Exception as ex:
        logger.exception("GSD LL Excel scheduler: data load failed")
        with _EXCEL_LOCK:
            _EXCEL_STATE["last_error"] = str(ex)
        _send_slack(
            f":x: *GSD Low Linkage — Excel data load failed*\n"
            f"Error: {ex}"
        )
    finally:
        _schedule_next_excel_run()


def _schedule_next_excel_run() -> None:
    """Set a timer for the next SCHEDULE_HOUR:SCHEDULE_MINUTE CET run."""
    global _EXCEL_TIMER
    with _EXCEL_LOCK:
        if not _EXCEL_STATE["enabled"]:
            _EXCEL_STATE["next_run_at"] = None
            return

    secs, target = _seconds_until(SCHEDULE_HOUR, SCHEDULE_MINUTE)
    with _EXCEL_LOCK:
        _EXCEL_STATE["next_run_at"] = target.isoformat(timespec="seconds")

    _EXCEL_TIMER = threading.Timer(secs, _excel_scheduled_run)
    _EXCEL_TIMER.daemon = True
    _EXCEL_TIMER.start()
    logger.info("GSD LL Excel scheduler: next run at %s (in %.0f seconds)", target, secs)


def start_excel_scheduler() -> None:
    """Initialize the daily Excel scheduler. Call on app startup."""
    _schedule_next_excel_run()


def stop_excel_scheduler() -> None:
    """Cancel the pending timer. Call on app shutdown."""
    global _EXCEL_TIMER
    if _EXCEL_TIMER:
        _EXCEL_TIMER.cancel()
        _EXCEL_TIMER = None


def toggle_excel_schedule(enabled: bool) -> Dict[str, Any]:
    """Enable or disable the daily Excel schedule."""
    global _EXCEL_TIMER
    with _EXCEL_LOCK:
        _EXCEL_STATE["enabled"] = enabled

    if enabled:
        _schedule_next_excel_run()
    else:
        if _EXCEL_TIMER:
            _EXCEL_TIMER.cancel()
            _EXCEL_TIMER = None
        with _EXCEL_LOCK:
            _EXCEL_STATE["next_run_at"] = None

    return get_excel_schedule_status()


def get_excel_schedule_status() -> Dict[str, Any]:
    """Return the current schedule state for the UI."""
    with _EXCEL_LOCK:
        return dict(_EXCEL_STATE)
