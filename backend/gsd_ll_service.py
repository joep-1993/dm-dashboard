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
import json
import logging
import os
import re
import threading
import time
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

# Singleton row recording the last successful Excel load, so the "last
# successful data load" date the tooltip shows survives server restarts
# (the in-memory cache resets on every restart; this table does not).
EXCEL_LOAD_TABLE = "pa.jvs_gsd_ll_excel_load"

# One row per (data_date, shop) for each daily Excel load, so the Date picker
# can replay an earlier day instead of always getting the newest file. Pruned
# to a rolling SNAPSHOT_RETENTION_DAYS calendar-day window on every save.
SNAPSHOT_TABLE = "pa.jvs_gsd_ll_excel_snapshots"
SNAPSHOT_RETENTION_DAYS = 7

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

# Persist the last-load timestamp so it survives server restarts.
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

# Dedup guard: skip the Slack notification when the same file was already
# notified within SLACK_COOLDOWN_SECONDS (prevents double messages when the
# scheduler fires twice, e.g. after a near-schedule-time server restart).
SLACK_COOLDOWN_SECONDS = 600  # 10 minutes
_LAST_SLACK_NOTIFY: Dict[str, Any] = {"file": None, "at": None}

# ---------------------------------------------------------------------------
# Kill switch — safety net while we trace the mysterious daily 09:50 run.
# When active, run_low_linkage / apply_selected refuse to mutate campaigns
# (forced dry-run) and log the blocked attempt loudly with port/pid, so an
# unexpected caller is caught instead of executed. Initialised from env
# GSD_LL_KILL_SWITCH; flip at runtime via POST /ll/kill-switch. See
# cc1/GSD_LL_MYSTERY_RUN.md.
# ---------------------------------------------------------------------------
_KILL_SWITCH: Dict[str, Any] = {
    "active": os.getenv("GSD_LL_KILL_SWITCH", "false").lower() in ("1", "true", "yes", "on"),
}


def kill_switch_status() -> Dict[str, Any]:
    """Return whether the GSD-LL kill switch is currently active."""
    return {"active": _KILL_SWITCH["active"]}


def set_kill_switch(active: bool) -> Dict[str, Any]:
    """Enable/disable the kill switch at runtime (no restart needed)."""
    _KILL_SWITCH["active"] = active
    logger.warning("GSD LL kill switch %s", "ENABLED" if active else "DISABLED")
    return {"active": active}


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
            # Log before flipping to done, so the frontend's reload-on-done
            # always finds the entry (and it lands even if nobody is watching).
            log_run_activity("LL Run", res=res)
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
            if not dry_run:
                log_run_activity("LL Run", error=str(ex))
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
            log_run_activity("LL Run selected", res=res)
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
            log_run_activity("LL Run selected", error=str(ex))
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


def load_excel_data(filepath: Optional[str] = None, *, notify: bool = True, max_retries: int = 3, retry_delay: float = 10.0) -> Dict[str, Any]:
    """Read the newest Excel file and store in the in-memory cache.

    Called daily by the scheduler and on-demand via POST /ll/excel-load.
    Does NOT pause/enable any campaigns — that only happens when the user
    clicks Preview or Run in the dashboard with source='excel'.

    Retries up to ``max_retries`` times (with ``retry_delay`` seconds between
    attempts) when reading the Excel file fails — e.g. the file is still being
    written by the scheduled script or a transient I/O error occurs.
    """
    for attempt in range(1, max_retries + 1):
        try:
            feed, flags, path = fetch_feed_from_excel(filepath)
            break
        except FileNotFoundError:
            # NOT transient — sleeping does not make a file appear, and the retry
            # above is for a file that exists but is still being written. Retrying
            # it cost 20s of dead time on EVERY startup on any machine without the
            # file (EXCEL_DIR is a Windows path that only resolves on the prod
            # host), because start_excel_scheduler() calls this synchronously
            # inside the FastAPI startup event. That caller already treats a
            # missing file as an expected condition and logs it at INFO — so the
            # two layers disagreed: one fought the error for 20s, the other
            # shrugged at it. Hand it straight to the caller and let it decide.
            raise
        except Exception as exc:
            if attempt < max_retries:
                logger.warning(
                    "GSD LL Excel load attempt %d/%d failed: %s — retrying in %.0fs",
                    attempt, max_retries, exc, retry_delay,
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    "GSD LL Excel load failed after %d attempts: %s", max_retries, exc,
                )
                raise
    fname = os.path.basename(path)
    pause_n = sum(1 for r in feed if r["gsd"] == 0)
    enable_n = sum(1 for r in feed if r["gsd"] == 1)

    # Use the Excel FILE's modification time as the load date, NOT the wall-clock
    # time of this call. The scheduled script writes a fresh file ~09:50 daily;
    # its mtime is the true "data date". Reading now() instead meant every
    # restart's startup pre-load re-stamped the current time, so the tooltip
    # showed the last SERVER RESTART instead of the last data load. mtime is
    # stable: re-reading the same file yields the same date across restarts.
    try:
        loaded_at = datetime.fromtimestamp(
            os.path.getmtime(path), AMSTERDAM_TZ
        ).isoformat(timespec="seconds")
    except OSError:
        loaded_at = datetime.now(AMSTERDAM_TZ).isoformat(timespec="seconds")

    status = {
        "feed": feed,
        "flags": flags,
        "file": fname,
        "loaded_at": loaded_at,
        "shop_count": len(feed),
        "pause_count": pause_n,
        "enable_count": enable_n,
    }
    with _EXCEL_LOCK:
        _EXCEL_DATA.update(status)
    # Persist so the date survives restarts and is retrievable even before this
    # process has (re)loaded the file. Best-effort — never breaks the load.
    _record_excel_load(fname, loaded_at, len(feed), pause_n, enable_n)
    # Keep the rows themselves for a week so the Date picker can replay this
    # day later. Also best-effort: today's Preview/Run reads the in-memory
    # cache above and must not depend on this write.
    _save_excel_snapshot(feed, flags, fname, loaded_at)
    logger.info(
        "GSD LL Excel cache loaded: %d shops (%d pause, %d enable) from %s",
        status["shop_count"], pause_n, enable_n, fname,
    )
    if notify and _get_server_port() == "3003":
        now = datetime.now(AMSTERDAM_TZ)
        dup = (
            _LAST_SLACK_NOTIFY["file"] == fname
            and _LAST_SLACK_NOTIFY["at"] is not None
            and (now - _LAST_SLACK_NOTIFY["at"]).total_seconds() < SLACK_COOLDOWN_SECONDS
        )
        if dup:
            logger.info("GSD LL Excel: skipping duplicate Slack notification for %s", fname)
        else:
            _send_slack(
                f":white_check_mark: *GSD Low Linkage — Excel data loaded*\n"
                f"File: {fname}\n"
                f"Shops: {len(feed)} ({pause_n} to pause, {enable_n} to enable)\n"
                f"Ready for Preview / Run in the dashboard."
            )
            _LAST_SLACK_NOTIFY["file"] = fname
            _LAST_SLACK_NOTIFY["at"] = now
    return get_excel_data_status()


def ensure_excel_load_table() -> None:
    """Create the singleton last-load table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {EXCEL_LOAD_TABLE} (
                    id           INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    file         TEXT,
                    loaded_at    TIMESTAMPTZ,
                    shop_count   INTEGER,
                    pause_count  INTEGER,
                    enable_count INTEGER,
                    updated_at   TIMESTAMPTZ DEFAULT now()
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def _record_excel_load(file: str, loaded_at: str, shop_count: int,
                       pause_count: int, enable_count: int) -> None:
    """Persist the latest successful Excel load (singleton row). Best-effort:
    a DB hiccup must never break the load itself."""
    try:
        ensure_excel_load_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {EXCEL_LOAD_TABLE}
                        (id, file, loaded_at, shop_count, pause_count, enable_count, updated_at)
                    VALUES (1, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (id) DO UPDATE SET
                        file         = EXCLUDED.file,
                        loaded_at    = EXCLUDED.loaded_at,
                        shop_count   = EXCLUDED.shop_count,
                        pause_count  = EXCLUDED.pause_count,
                        enable_count = EXCLUDED.enable_count,
                        updated_at   = now()
                """, (file, loaded_at, shop_count, pause_count, enable_count))
            conn.commit()
        finally:
            return_db_connection(conn)
    except Exception:
        logger.warning("GSD LL: failed to persist Excel load timestamp", exc_info=True)


def get_last_excel_load() -> Optional[Dict[str, Any]]:
    """Return the persisted last-successful-load metadata (or None). loaded_at is
    an ISO string to match the in-memory cache's shape for the frontend."""
    try:
        ensure_excel_load_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT file, loaded_at, shop_count, pause_count, enable_count
                    FROM {EXCEL_LOAD_TABLE} WHERE id = 1
                """)
                row = cur.fetchone()
            if not row:
                return None
            r = dict(row)
            if r.get("loaded_at") is not None:
                r["loaded_at"] = r["loaded_at"].isoformat(timespec="seconds")
            return r
        finally:
            return_db_connection(conn)
    except Exception:
        logger.warning("GSD LL: failed to read persisted Excel load timestamp", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Excel snapshots — the rows themselves, one day per data_date
# ---------------------------------------------------------------------------
# EXCEL_LOAD_TABLE above stores only *metadata* about the last load, and the
# in-memory _EXCEL_DATA cache holds exactly one day. That left the Date picker
# in the "Pause / Enable low linkage shops" card doing nothing once the daily
# 09:50 Excel load became the only data source: run_low_linkage(source='excel')
# had no earlier day to read, so it ignored date_str entirely. Persisting each
# load's rows for a week gives the picker something real to select.


def _snapshot_date(loaded_at: str) -> str:
    """The Excel file's own date (its mtime, Amsterdam) — what the picker offers.

    Deliberately NOT the wall-clock date of the load: a restart re-reads the
    same file, and it must land on the same data_date rather than creating a
    second snapshot under today's date. Same reasoning as ``loaded_at`` itself.
    """
    return datetime.fromisoformat(loaded_at).date().isoformat()


def ensure_excel_snapshot_table() -> None:
    """Create the per-day Excel snapshot table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
                    data_date  DATE    NOT NULL,
                    shop_id    INTEGER NOT NULL,
                    shop_name  TEXT,
                    linkage    DOUBLE PRECISION,
                    gsd        SMALLINT,
                    is_gsd_nl  SMALLINT,
                    is_gsd_be  SMALLINT,
                    is_gsd_de  SMALLINT,
                    file       TEXT,
                    loaded_at  TIMESTAMPTZ,
                    PRIMARY KEY (data_date, shop_id)
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def _save_excel_snapshot(feed: List[Dict[str, Any]],
                         flags: Optional[Dict[int, Dict[str, int]]],
                         file: Optional[str],
                         loaded_at: str) -> Optional[str]:
    """Store one day's Excel rows, then prune outside the retention window.

    Best-effort, exactly like _record_excel_load: a DB hiccup must never break
    the load itself, since the in-memory cache (and therefore today's Run) does
    not depend on this succeeding. Returns the data_date written, else None.
    """
    from psycopg2.extras import execute_values

    try:
        data_date = _snapshot_date(loaded_at)
        rows = []
        for r in feed:
            f = (flags or {}).get(r["shop_id"], {})
            rows.append((
                data_date, r["shop_id"], r["shop_name"], r["linkage"], r["gsd"],
                f.get("is_gsd_nl_shop", 0), f.get("is_gsd_be_shop", 0),
                f.get("is_gsd_de_shop", 0), file, loaded_at,
            ))
        if not rows:
            return None
        ensure_excel_snapshot_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, f"""
                    INSERT INTO {SNAPSHOT_TABLE}
                        (data_date, shop_id, shop_name, linkage, gsd,
                         is_gsd_nl, is_gsd_be, is_gsd_de, file, loaded_at)
                    VALUES %s
                    ON CONFLICT (data_date, shop_id) DO UPDATE SET
                        shop_name = EXCLUDED.shop_name,
                        linkage   = EXCLUDED.linkage,
                        gsd       = EXCLUDED.gsd,
                        is_gsd_nl = EXCLUDED.is_gsd_nl,
                        is_gsd_be = EXCLUDED.is_gsd_be,
                        is_gsd_de = EXCLUDED.is_gsd_de,
                        file      = EXCLUDED.file,
                        loaded_at = EXCLUDED.loaded_at
                """, rows, page_size=1000)
                # Retention relative to the date just written, not current_date:
                # replaying an older file then prunes LESS, never more.
                cur.execute(
                    f"DELETE FROM {SNAPSHOT_TABLE} "
                    f"WHERE data_date < %s::date - %s",
                    (data_date, SNAPSHOT_RETENTION_DAYS - 1),
                )
                pruned = cur.rowcount
            conn.commit()
        finally:
            return_db_connection(conn)
        logger.info(
            "GSD LL Excel snapshot: stored %d rows for %s (pruned %d row(s) "
            "older than %d days)", len(rows), data_date, pruned,
            SNAPSHOT_RETENTION_DAYS,
        )
        return data_date
    except Exception:
        logger.warning("GSD LL: failed to store Excel snapshot", exc_info=True)
        return None


def list_excel_snapshot_dates() -> List[Dict[str, Any]]:
    """Stored snapshot dates, newest first — what the Date picker may offer."""
    try:
        ensure_excel_snapshot_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT data_date,
                           min(file)      AS file,
                           max(loaded_at) AS loaded_at,
                           count(*)                          AS shop_count,
                           count(*) FILTER (WHERE gsd = 0)    AS pause_count,
                           count(*) FILTER (WHERE gsd = 1)    AS enable_count
                    FROM {SNAPSHOT_TABLE}
                    GROUP BY data_date
                    ORDER BY data_date DESC
                """)
                out = []
                for row in cur.fetchall():
                    r = dict(row)
                    r["data_date"] = r["data_date"].isoformat()
                    if r.get("loaded_at"):
                        r["loaded_at"] = r["loaded_at"].isoformat(timespec="seconds")
                    out.append(r)
                return out
        finally:
            return_db_connection(conn)
    except Exception:
        logger.warning("GSD LL: failed to list Excel snapshot dates", exc_info=True)
        return []


def get_excel_snapshot(date_str: str) -> Optional[Dict[str, Any]]:
    """Return one stored day in the same shape the in-memory cache has.

    Resolves to the most recent snapshot **on or before** ``date_str`` — the
    same "as of" semantics get_shop_flags() already uses for the Redshift
    flags, so picking a weekend day falls back to Friday's data instead of
    failing. The resolved ``data_date`` is returned so the caller can say which
    day it actually used rather than quietly substituting one.
    """
    ensure_excel_snapshot_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT max(data_date) AS d FROM {SNAPSHOT_TABLE} "
                f"WHERE data_date <= %s", (date_str,),
            )
            row = cur.fetchone()
            resolved = row["d"] if row else None
            if not resolved:
                return None
            cur.execute(f"""
                SELECT shop_id, shop_name, linkage, gsd,
                       is_gsd_nl, is_gsd_be, is_gsd_de, file, loaded_at
                FROM {SNAPSHOT_TABLE}
                WHERE data_date = %s
                ORDER BY shop_id
            """, (resolved,))
            rows = cur.fetchall()
    finally:
        return_db_connection(conn)

    if not rows:
        return None
    feed = [{
        "shop_id": r["shop_id"],
        "shop_name": r["shop_name"],
        "linkage": r["linkage"],
        "gsd": r["gsd"],
    } for r in rows]
    flags = {r["shop_id"]: {
        "is_gsd_nl_shop": r["is_gsd_nl"] or 0,
        "is_gsd_be_shop": r["is_gsd_be"] or 0,
        "is_gsd_de_shop": r["is_gsd_de"] or 0,
    } for r in rows}
    loaded_at = rows[0]["loaded_at"]
    return {
        "feed": feed,
        "flags": flags,
        "file": rows[0]["file"],
        "loaded_at": loaded_at.isoformat(timespec="seconds") if loaded_at else None,
        "data_date": resolved.isoformat(),
        "shop_count": len(feed),
        "pause_count": sum(1 for r in feed if r["gsd"] == 0),
        "enable_count": sum(1 for r in feed if r["gsd"] == 1),
    }


def backfill_excel_snapshots(max_files: int = SNAPSHOT_RETENTION_DAYS) -> Dict[str, Any]:
    """Store snapshots for Excel files still on disk whose date is missing.

    Without this the retention window only starts filling from the next daily
    load, so the picker would have a single selectable day for a week. The
    scheduled script leaves its earlier files in EXCEL_DIR, so the recent ones
    can be replayed once. Reads at most ``max_files`` files oldest-first (so a
    newer mtime wins any same-date upsert), skips dates already stored and
    anything past the retention window, and never raises.
    """
    result: Dict[str, Any] = {"stored": [], "skipped": [], "errors": []}
    try:
        files = sorted(
            glob_mod.glob(os.path.join(EXCEL_DIR, "gsd_shops_nl_be_*.xlsx")),
            key=os.path.getmtime,
        )[-max_files:]
        if not files:
            return result
        have = {d["data_date"] for d in list_excel_snapshot_dates()}
        cutoff = (datetime.now(AMSTERDAM_TZ).date()
                  - timedelta(days=SNAPSHOT_RETENTION_DAYS - 1))
        for path in files:
            name = os.path.basename(path)
            try:
                loaded_at = datetime.fromtimestamp(
                    os.path.getmtime(path), AMSTERDAM_TZ
                ).isoformat(timespec="seconds")
                data_date = _snapshot_date(loaded_at)
                if data_date in have or datetime.fromisoformat(loaded_at).date() < cutoff:
                    result["skipped"].append(data_date)
                    continue
                feed, flags, _ = fetch_feed_from_excel(path)
                if _save_excel_snapshot(feed, flags, name, loaded_at):
                    result["stored"].append(data_date)
                    have.add(data_date)
            except Exception as exc:
                result["errors"].append({"file": name, "error": str(exc)})
        if result["stored"]:
            logger.info("GSD LL Excel snapshot backfill: stored %s",
                        ", ".join(result["stored"]))
    except Exception:
        logger.warning("GSD LL: Excel snapshot backfill failed", exc_info=True)
    return result


def get_excel_data_status() -> Dict[str, Any]:
    """Return the cached Excel data status (without the raw data itself).

    ``loaded_at`` falls back to the persisted last-load row when this process
    has not (yet) loaded the file in memory — so the "last successful data load"
    date the tooltip shows survives restarts instead of resetting.
    """
    with _EXCEL_LOCK:
        file = _EXCEL_DATA["file"]
        loaded_at = _EXCEL_DATA["loaded_at"]
        shop_count = _EXCEL_DATA["shop_count"]
        pause_count = _EXCEL_DATA["pause_count"]
        enable_count = _EXCEL_DATA["enable_count"]
        has_data = _EXCEL_DATA["feed"] is not None

    if not loaded_at:
        persisted = get_last_excel_load()
        if persisted and persisted.get("loaded_at"):
            file = file or persisted.get("file")
            loaded_at = persisted["loaded_at"]
            shop_count = shop_count or persisted.get("shop_count") or 0
            pause_count = pause_count or persisted.get("pause_count") or 0
            enable_count = enable_count or persisted.get("enable_count") or 0

    return {
        "file": file,
        "loaded_at": loaded_at,
        "shop_count": shop_count,
        "pause_count": pause_count,
        "enable_count": enable_count,
        "has_data": has_data,
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
# Activity Log (server-side, replaces the old localStorage log)
# ---------------------------------------------------------------------------

ACTIVITY_TABLE = "pa.jvs_gsd_activity_log"


def ensure_activity_table() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ACTIVITY_TABLE} (
                    id         SERIAL PRIMARY KEY,
                    entry_id   TEXT UNIQUE NOT NULL,
                    time       TIMESTAMPTZ NOT NULL,
                    action     TEXT NOT NULL,
                    details    TEXT,
                    success    BOOLEAN DEFAULT TRUE,
                    undo       JSONB,
                    reset      BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def log_run_activity(kind: str, res: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None,
                     run_key: Optional[str] = None) -> Optional[str]:
    """Write a finished LL run's Activity Log entry. Best effort.

    This entry used to be written by the browser after polling reported done,
    which made the log depend on the tab staying open: a run whose page closed
    mutated campaigns and logged nothing. Two confirmed cases — the 21 Jul
    zombie-APScheduler batch and 28 Jul 08:28 — existed only in ADMIN_TABLE, and
    any reconstruction keyed off the log mis-attributed their rows. Writing it
    here, beside the progress state and the audit rows this module already owns,
    makes the log independent of whoever started the run.

    Call it BEFORE flipping progress to done, so a frontend that reloads the log
    as soon as it sees done=True finds the entry already there.

    ``kind`` is the action string the UI groups on ('LL Run' / 'LL Run
    selected'). Previews are not logged. Returns the entry_id, or None.
    """
    try:
        # Aware on purpose. The column is TIMESTAMPTZ and the shared Postgres
        # session runs Etc/UTC, so a naive datetime.now() (which is Amsterdam
        # local on this host) would be read as UTC and land 2h late in summer —
        # the same trap as the DMA Exclusions timestamp bug.
        now = datetime.now(AMSTERDAM_TZ)
        slug = "selected" if "selected" in kind.lower() else "run"
        # Key the id on the RUN (its start), not on the moment we log: that makes
        # one entry per run and a repeated write an update instead of a second
        # row. Keying it on "now" collided whenever two writes shared a second.
        if not run_key:
            with _LL_LOCK:
                run_key = _LL_PROGRESS.get("started_at")
        entry: Dict[str, Any] = {
            "id": f"ll-{slug}-{run_key or now.isoformat(timespec='seconds')}",
            "time": now,
            "action": kind,
            "reset": False,
        }

        if error is not None:
            entry.update({"details": f"Error: {error}", "success": False, "undo": None})
        else:
            res = res or {}
            # A preview mutates nothing, so it is not activity. A run blocked by
            # the kill switch also reports dry_run, but IS worth recording — an
            # attempt that got stopped is exactly what someone would look for.
            if res.get("dry_run") and not res.get("kill_switch_blocked"):
                return None
            paused = res.get("paused") or []
            enabled = res.get("enabled") or []
            details = (f"{res.get('paused_count', len(paused))} paused / "
                       f"{res.get('enabled_count', len(enabled))} enabled")
            if res.get("kill_switch_blocked"):
                details += " — blocked by the kill switch"
            if res.get("snapshot_date"):
                details += f" (Excel data for {res['snapshot_date']})"
            entry.update({
                "details": details,
                "success": not (res.get("errors") or []),
                # Same inversion as backfill_activity_from_ll; 'll' makes
                # POST /undo reverse through the label-aware path.
                "undo": ({"created": enabled, "paused": paused, "ll": True}
                         if (paused or enabled) else None),
            })

        save_activity(entry)
        logger.info("GSD LL: logged activity entry %s (%s)", entry["id"], entry["details"])
        return entry["id"]
    except Exception:
        logger.warning("GSD LL: failed to write the run's activity entry", exc_info=True)
        return None


def save_activity(entry: Dict[str, Any]) -> None:
    """Insert one activity log entry (idempotent on entry_id)."""
    ensure_activity_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {ACTIVITY_TABLE} (entry_id, time, action, details, success, undo, reset)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entry_id) DO UPDATE SET
                    details = EXCLUDED.details,
                    success = EXCLUDED.success,
                    undo    = EXCLUDED.undo,
                    reset   = EXCLUDED.reset
            """, (
                entry["id"],
                entry["time"],
                entry["action"],
                entry.get("details"),
                entry.get("success", True),
                json.dumps(entry["undo"]) if entry.get("undo") else None,
                entry.get("reset", False),
            ))
        conn.commit()
    finally:
        return_db_connection(conn)


def get_activity_log(limit: int = 100) -> List[Dict[str, Any]]:
    """Return recent activity entries for the frontend."""
    ensure_activity_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT entry_id, time, action, details, success, undo, reset
                FROM {ACTIVITY_TABLE}
                ORDER BY time DESC, id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [
            {
                "id": r["entry_id"],
                "time": r["time"].isoformat() if r["time"] else None,
                "action": r["action"],
                "details": r["details"],
                "success": r["success"],
                "undo": r["undo"],
                "reset": r["reset"],
            }
            for r in rows
        ]
    finally:
        return_db_connection(conn)


def mark_activity_reset(entry_id: str) -> bool:
    """Mark an activity entry as reset. Returns True if found."""
    ensure_activity_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {ACTIVITY_TABLE} SET reset = TRUE WHERE entry_id = %s
            """, (entry_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        return_db_connection(conn)


def backfill_activity_from_ll(gap_minutes: int = 10) -> Dict[str, Any]:
    """
    Reconstruct Activity Log entries from the per-campaign audit table
    (pa.jvs_gsd_ll_campaigns) by grouping rows into runs.

    Rows within ``gap_minutes`` of each other with the same action are treated
    as one run.  Includes undo payloads so reset buttons appear.
    """
    ensure_activity_table()
    ensure_admin_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Gap-and-islands: assign a run_group to consecutive rows within
            # gap_minutes of each other.  Return per-campaign detail too.
            cur.execute(f"""
                WITH ordered AS (
                    SELECT id, action, campaign_id, campaign_name, customer_id, created_at,
                           LAG(created_at) OVER (PARTITION BY action ORDER BY created_at) AS prev_at
                    FROM {ADMIN_TABLE}
                ),
                grouped AS (
                    SELECT *,
                           SUM(CASE WHEN prev_at IS NULL
                                         OR created_at - prev_at > interval '{int(gap_minutes)} minutes'
                                    THEN 1 ELSE 0 END)
                               OVER (PARTITION BY action ORDER BY created_at) AS run_group
                    FROM ordered
                )
                SELECT action, run_group, campaign_id, campaign_name, customer_id,
                       MIN(created_at) OVER (PARTITION BY action, run_group) AS run_time
                FROM grouped
                ORDER BY run_time, action, id
            """)
            all_rows = cur.fetchall()

            # Group into runs.
            runs: Dict[str, Dict] = {}   # keyed by entry_id
            for r in all_rows:
                action = r["action"]     # 'Paused' or 'Enabled'
                run_time = r["run_time"]
                entry_id = f"backfill-{action}-{run_time.isoformat()}"
                if entry_id not in runs:
                    runs[entry_id] = {
                        "action": action,
                        "run_time": run_time,
                        "campaigns": [],
                    }
                runs[entry_id]["campaigns"].append({
                    "customer_id": str(r["customer_id"]),
                    "campaign_id": str(r["campaign_id"]),
                    "campaign_name": r["campaign_name"] or "",
                })

            inserted = 0
            for entry_id, run in runs.items():
                camps = run["campaigns"]
                action = run["action"]
                cnt = len(camps)
                details = f"{cnt} campaign(s) {action.lower()}"
                # For undo: paused campaigns -> undo re-enables them (and vice versa).
                if action == "Paused":
                    undo = {"created": [], "paused": camps}
                else:
                    undo = {"created": camps, "paused": []}
                cur.execute(f"""
                    INSERT INTO {ACTIVITY_TABLE}
                        (entry_id, time, action, details, success, undo, reset)
                    VALUES (%s, %s, %s, %s, TRUE, %s, FALSE)
                    ON CONFLICT (entry_id) DO UPDATE SET
                        undo = EXCLUDED.undo,
                        details = EXCLUDED.details
                """, (entry_id, run["run_time"], "LL Run", details, json.dumps(undo)))
                inserted += cur.rowcount
        conn.commit()
        return {"backfilled": inserted, "total_runs": len(runs)}
    finally:
        return_db_connection(conn)


def undo_ll_run(undo: Dict[str, Any]) -> Dict[str, Any]:
    """Reverse an LL run: re-enable what it paused, re-pause what it enabled.

    Takes an activity entry's undo payload — ``{"created": [...], "paused":
    [...]}`` where *created* holds the campaigns the run ENABLED — and replays
    it through apply_selected() with every action flipped.

    Going through apply_selected rather than the generic undo_run() is the whole
    point: undo_run only writes campaign status, which for LL is half a
    reversal. A re-enabled campaign would keep its GSD_LL_PAUSED label, and — far
    worse — a re-paused one would have none, and a paused-but-untagged campaign
    is invisible to every future enable run, since that lookup finds candidates
    BY the label (see run_low_linkage, which rolls a pause back rather than
    leave a campaign in that state). apply_selected maintains the label in both
    directions, honours the kill switch, and audits each action.

    Returns the {paused_created, enabled_paused, errors} shape the /undo
    endpoint already had, so a frontend that knows nothing about this still
    reports it correctly, with the full apply result under "detail".
    """
    created = undo.get("created") or []     # run enabled them -> pause them back
    paused = undo.get("paused") or []       # run paused them  -> re-enable them

    def _entry(item: Dict[str, Any], action: str) -> Dict[str, Any]:
        # campaign_label_resource is deliberately dropped: on a re-pause the
        # stored link is the one the run detached (dead), and on a re-enable
        # apply_selected re-looks it up from shop_id.
        return {k: v for k, v in item.items()
                if k != "campaign_label_resource"} | {"action": action}

    entries = ([_entry(i, "Enabled") for i in paused]
               + [_entry(i, "Paused") for i in created])
    if not entries:
        return {"paused_created": 0, "enabled_paused": 0, "errors": [], "ll": True}

    # apply_selected has no lock of its own (start_ll_apply holds it), so guard
    # here — a reversal racing a live run would fight over the same campaigns.
    with _LL_LOCK:
        if _LL_PROGRESS["running"]:
            return {"busy": True, "paused_created": 0, "enabled_paused": 0, "ll": True,
                    "errors": [{"error": "a low-linkage run is in progress — "
                                         "wait for it to finish"}]}

    logger.warning("GSD LL undo: reversing %d paused + %d enabled campaign(s)",
                   len(paused), len(created))
    res = apply_selected(entries)
    return {
        # We paused what the run had enabled, and enabled what it had paused.
        "paused_created": res.get("paused_count", len(res.get("paused", []))),
        "enabled_paused": res.get("enabled_count", len(res.get("enabled", []))),
        "errors": res.get("errors", []),
        "kill_switch_blocked": res.get("kill_switch_blocked", False),
        "ll": True,
        "detail": res,
    }


_LL_DETAILS_RE = re.compile(r"(\d+)\s+paused\s*/\s*(\d+)\s+enabled", re.I)


def backfill_ll_undo(dry_run: bool = True, gap_seconds: int = 60,
                     match_minutes: int = 30) -> Dict[str, Any]:
    """Fill in the undo payload on live-logged LL entries that have none.

    Runs logged straight from the browser never stored an undo payload (only
    the GSD 'Run Script' call site passed one), so their Reset button never
    appeared — every Reset visible in the log came from
    backfill_activity_from_ll(). Re-running that is NOT a fix: it keys on
    ``backfill-{action}-{run_time}`` while a live entry carries a browser UUID,
    so ON CONFLICT never matches and it inserts duplicates beside them.

    Attribution groups the audit table into runs (rows within ``gap_seconds``
    of each other, ignoring action so a run's pause and enable rows stay
    together) and matches each entry to the run finishing at most
    ``match_minutes`` before it. Bounding by "the previous activity entry"
    instead does NOT work: the audit table also holds runs that never logged an
    entry at all — the 21 Jul zombie-APScheduler batch, and any run whose
    browser tab closed before the frontend wrote its entry, since the entry is
    written client-side after polling finishes. Those rows would silently be
    attributed to the next entry.

    The 60s default comes from the data: rows inside one run land 1-5s apart
    (worst observed 46s), while separate runs sit 109s+ apart. There is no wide
    cliff between the two, so treat it as a tuning knob, not a constant —
    which is safe because of the check below. Splitting one run in two, or
    merging two, both change the counts and are therefore rejected rather than
    written.

    Nothing is written unless the reconstructed counts equal the entry's own
    details text ("N paused / M enabled"); a mismatch is reported instead of
    guessed at, because a wrong undo payload resets the wrong campaigns.
    """
    ensure_activity_table()
    ensure_admin_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT entry_id, time, details
                FROM {ACTIVITY_TABLE}
                WHERE action LIKE 'LL Run%%' AND undo IS NULL
                ORDER BY time
            """)
            targets = cur.fetchall()
            if not targets:
                return {"dry_run": dry_run, "updated": 0, "matched": [], "skipped": []}

            # Islands of audit rows = runs. Ignoring action keeps a run's pause
            # and enable rows in one group (unlike backfill_activity_from_ll,
            # which partitions per action because it emits one entry each).
            cur.execute(f"""
                WITH ordered AS (
                    SELECT id, action, customer_id, campaign_id, campaign_name,
                           shop_id, shop_name, country, linkage, created_at,
                           LAG(created_at) OVER (ORDER BY created_at, id) AS prev_at
                    FROM {ADMIN_TABLE}
                ), grouped AS (
                    SELECT *, SUM(CASE WHEN prev_at IS NULL
                                         OR created_at - prev_at > interval '{int(gap_seconds)} seconds'
                                       THEN 1 ELSE 0 END)
                                  OVER (ORDER BY created_at, id) AS run_group
                    FROM ordered
                )
                SELECT * FROM grouped ORDER BY run_group, id
            """)
            groups: Dict[int, Dict[str, Any]] = {}
            for r in cur.fetchall():
                g = groups.setdefault(r["run_group"], {
                    "ended": r["created_at"], "paused": [], "enabled": [],
                })
                g["ended"] = max(g["ended"], r["created_at"])
                g["paused" if r["action"] == "Paused" else "enabled"].append({
                    "customer_id": str(r["customer_id"]),
                    "campaign_id": str(r["campaign_id"]),
                    "campaign_name": r["campaign_name"] or "",
                    "shop_id": r["shop_id"],
                    "shop_name": r["shop_name"] or "",
                    "country": r["country"] or "",
                    "linkage": float(r["linkage"]) if r["linkage"] is not None else None,
                })

            matched: List[Dict[str, Any]] = []
            skipped: List[Dict[str, Any]] = []
            used: Set[int] = set()
            updated = 0

            for t in targets:
                when, details = t["time"], (t["details"] or "")
                info = {"entry_id": t["entry_id"],
                        "time": when.isoformat(timespec="seconds"),
                        "details": details}

                m = _LL_DETAILS_RE.search(details)
                if not m:
                    skipped.append({**info, "reason": "details carry no counts "
                                                      "(error entry?) — nothing to verify against"})
                    continue
                want = (int(m.group(1)), int(m.group(2)))

                cands = [(gid, g) for gid, g in groups.items()
                         if gid not in used and g["ended"] <= when
                         and (when - g["ended"]).total_seconds() <= match_minutes * 60]
                if not cands:
                    skipped.append({**info, "reason": f"no audit run ended within "
                                                      f"{match_minutes} min before this entry"})
                    continue
                gid, g = max(cands, key=lambda kv: kv[1]["ended"])
                got = (len(g["paused"]), len(g["enabled"]))
                if got != want:
                    skipped.append({**info, "reason": (
                        f"count mismatch — audit run ending "
                        f"{g['ended'].isoformat(timespec='seconds')} has "
                        f"{got[0]} paused / {got[1]} enabled, entry says "
                        f"{want[0]} / {want[1]}; not guessing which rows are its")})
                    continue

                # Same inversion as backfill_activity_from_ll: undoing a pause
                # re-enables, undoing an enable pauses. 'll' routes the Reset
                # through /ll/apply so the GSD_LL_PAUSED label is fixed too,
                # which the status-only /undo endpoint cannot do.
                undo = {"created": g["enabled"], "paused": g["paused"], "ll": True}
                used.add(gid)
                matched.append({**info,
                                "rebuilt": f"{got[0]} paused / {got[1]} enabled",
                                "run_ended": g["ended"].isoformat(timespec="seconds")})
                if not dry_run:
                    cur.execute(
                        f"UPDATE {ACTIVITY_TABLE} SET undo = %s "
                        f"WHERE entry_id = %s AND undo IS NULL",
                        (json.dumps(undo), t["entry_id"]),
                    )
                    updated += cur.rowcount
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        logger.info("GSD LL undo backfill (dry_run=%s): %d matched, %d skipped, %d updated",
                    dry_run, len(matched), len(skipped), updated)
        return {"dry_run": dry_run, "updated": updated,
                "matched": matched, "skipped": skipped}
    finally:
        return_db_connection(conn)


def backfill_activity_from_gsd(days: int = 29, gap_minutes: int = 30) -> Dict[str, Any]:
    """
    Reconstruct "Run Script" Activity Log entries from Google Ads change_event.

    Queries campaign CREATE events (and status-change-to-PAUSED events for
    non-created campaigns) across all GSD accounts for the last ``days`` days,
    groups them into runs by time proximity, and upserts activity entries with
    undo payloads.

    Google Ads retains change_event for ~30 days, so older runs are lost.
    """
    ensure_activity_table()
    tz = ZoneInfo("Europe/Amsterdam")
    now = datetime.now(tz)
    start = now - timedelta(days=days)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = now.strftime("%Y-%m-%d %H:%M:%S")

    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    # Collect all CREATE + status-change events across accounts.
    events: List[Dict] = []
    errors: List[Dict] = []

    for cid in customer_ids:
        query = f"""
            SELECT change_event.change_date_time,
                   change_event.resource_change_operation,
                   change_event.changed_fields,
                   change_event.new_resource,
                   campaign.id, campaign.name
            FROM change_event
            WHERE change_event.change_date_time BETWEEN '{start_s}' AND '{end_s}'
              AND change_event.change_resource_type = 'CAMPAIGN'
            ORDER BY change_event.change_date_time
            LIMIT 10000
        """
        try:
            rows = list(ga.search(customer_id=cid, query=query))
        except GoogleAdsException as ex:
            errors.append({"customer_id": cid, "error": str(ex)[:400]})
            continue

        for row in rows:
            ce = row.change_event
            name = row.campaign.name or ""
            if "[channel:directshopping]" not in name:
                continue
            op = ce.resource_change_operation.name
            is_create = (op == "CREATE")
            is_pause = ("status" in list(ce.changed_fields.paths)
                        and ce.new_resource.campaign.status.name == "PAUSED")
            if not is_create and not is_pause:
                continue
            events.append({
                "time": ce.change_date_time,
                "customer_id": cid,
                "campaign_id": str(row.campaign.id),
                "campaign_name": name,
                "is_create": is_create,
                "is_pause": is_pause,
            })

    if not events:
        return {"backfilled": 0, "total_runs": 0, "errors": errors}

    # Sort by time and group into runs (gap_minutes apart = new run).
    events.sort(key=lambda e: e["time"])
    runs: List[List[Dict]] = []
    current_run: List[Dict] = [events[0]]
    for ev in events[1:]:
        prev_time = datetime.fromisoformat(current_run[-1]["time"])
        cur_time = datetime.fromisoformat(ev["time"])
        if (cur_time - prev_time).total_seconds() > gap_minutes * 60:
            runs.append(current_run)
            current_run = [ev]
        else:
            current_run.append(ev)
    runs.append(current_run)

    # Insert activity entries.
    conn = get_db_connection()
    try:
        inserted = 0
        with conn.cursor() as cur:
            for run_events in runs:
                run_time = datetime.fromisoformat(run_events[0]["time"])
                created_keys = set()
                created = []
                paused = []
                for ev in run_events:
                    key = (ev["customer_id"], ev["campaign_id"])
                    if ev["is_create"]:
                        created_keys.add(key)
                        created.append({
                            "customer_id": ev["customer_id"],
                            "campaign_id": ev["campaign_id"],
                            "campaign_name": ev["campaign_name"],
                        })
                    elif ev["is_pause"] and key not in created_keys:
                        paused.append({
                            "customer_id": ev["customer_id"],
                            "campaign_id": ev["campaign_id"],
                            "campaign_name": ev["campaign_name"],
                        })

                nC, nP = len(created), len(paused)
                if nC + nP == 0:
                    continue
                entry_id = f"backfill-gsd-{run_time.isoformat()}"
                details = f"{nC} created / {nP} paused"
                undo = {"created": created, "paused": paused}
                cur.execute(f"""
                    INSERT INTO {ACTIVITY_TABLE}
                        (entry_id, time, action, details, success, undo, reset)
                    VALUES (%s, %s, 'Run Script', %s, TRUE, %s, FALSE)
                    ON CONFLICT (entry_id) DO UPDATE SET
                        undo = EXCLUDED.undo,
                        details = EXCLUDED.details
                """, (entry_id, run_time, details, json.dumps(undo)))
                inserted += cur.rowcount
        conn.commit()
        return {"backfilled": inserted, "total_runs": len(runs), "errors": errors}
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


def _find_all_shopping_campaigns(client, customer_id: str, shop_id: int) -> List[Dict[str, str]]:
    """All non-REMOVED Shopping campaigns for this shop, regardless of status or label.

    Used for diagnostics when the primary lookup returns no results, to explain
    *why* (already paused, already enabled, no campaigns at all, etc.).
    """
    ga_service = client.get_service("GoogleAdsService")
    name_pattern = _name_contains_regexp(f"[shop_id:{int(shop_id)}]")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND campaign.advertising_channel_type = 'SHOPPING'
          AND campaign.name REGEXP_MATCH '{name_pattern}'
    """
    out: List[Dict[str, str]] = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "status": row.campaign.status.name,
            })
    except GoogleAdsException as ex:
        logger.error("All-campaign lookup failed (%s, shop_id=%s): %s", customer_id, shop_id, ex)
        raise
    return out


def _diagnose_no_campaigns(client, country: str, shop_id: int, gsd: int) -> Tuple[str, List[Dict[str, str]]]:
    """Determine why no actionable campaigns were found for a shop+country.

    Returns (reason, campaigns) where *campaigns* is a list of
    ``{"campaign_name": ..., "status": ...}`` dicts so the frontend can
    show them in the expandable detail row.
    """
    all_camps: List[Dict[str, str]] = []
    for cid in sorted(COUNTRY_CUSTOMER_IDS.get(country, set())):
        try:
            for camp in _find_all_shopping_campaigns(client, cid, shop_id):
                all_camps.append(camp)
        except Exception:
            pass

    all_statuses = {c["status"] for c in all_camps}
    if not all_statuses:
        return "geen shopping campagnes", []
    if gsd == 0:
        reason = "al gepauzeerd" if all_statuses <= {"PAUSED"} else "geen actieve campagnes"
    else:
        reason = "al geactiveerd" if all_statuses <= {"ENABLED"} else "geen LL label"
    return reason, [{"campaign_name": c["campaign_name"], "status": c["status"]} for c in all_camps]


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
    date_str : optional YYYY-MM-DD; which day's data to run against (most recent
        available on or before it). Defaults to the latest.
        With source='feed' this scopes the shop_list GSD flags from Redshift.
        With source='excel' it replays that day's stored snapshot from
        SNAPSHOT_TABLE — feed rows AND flags, since the Excel carries both —
        and fails rather than silently using the newest file when the date has
        no snapshot.
    shop_names : optional list of feed shop names to scope the run to.
    included : with shop_names, True = process ONLY those shops, False = process
        all EXCEPT those. Ignored when shop_names is empty.
    source : 'feed' (pixel-monitor CSV + Redshift flags) or 'excel' (local
        Excel file — uses the newest gsd_shops_nl_be_*.xlsx from EXCEL_DIR,
        which already contains the country flags so no Redshift query is needed).
    """
    import traceback
    logger.warning(
        "run_low_linkage STARTED — dry_run=%s  source=%s  date=%s  "
        "shop_names=%s  included=%s  server_port=%s  pid=%s\n  Call stack:\n%s",
        dry_run, source, date_str, shop_names, included,
        _get_server_port(), os.getpid(),
        "".join(traceback.format_stack()[-6:-1]).strip(),
    )
    kill_switched = False
    if _KILL_SWITCH["active"] and not dry_run:
        logger.warning(
            "GSD LL KILL SWITCH ACTIVE — blocking real run, forcing dry_run. "
            "source=%s  date=%s  shop_names=%s  included=%s  server_port=%s  pid=%s",
            source, date_str, shop_names, included, _get_server_port(), os.getpid(),
        )
        dry_run = True
        kill_switched = True
    started = datetime.now()
    result: Dict[str, Any] = {
        "started_at": started.isoformat(timespec="seconds"),
        "dry_run": dry_run,
        "kill_switch_blocked": kill_switched,
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
    if source == "excel" and date_str:
        # A picked date replays a stored snapshot. Deliberately does NOT fall
        # back to today's file when the date has no data: silently running the
        # newest Excel for a date the user chose is the behaviour this replaced.
        _progress_set(phase=f"Loading stored Excel data for {date_str}…")
        try:
            snap = get_excel_snapshot(date_str)
        except Exception as ex:
            logger.error("GSD LL: failed to read Excel snapshot: %s", ex)
            result["errors"].append({"step": "excel_snapshot", "error": str(ex)})
            return result
        if not snap:
            available = [d["data_date"] for d in list_excel_snapshot_dates()]
            result["errors"].append({"step": "excel_snapshot", "error": (
                f"no stored Excel data on or before {date_str}"
                + (f" — available: {', '.join(available)}" if available else
                   " — no snapshots stored yet; they accumulate from the next "
                   f"daily {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} load")
            )})
            return result
        feed = list(snap["feed"])              # shallow copy — safe to filter
        excel_flags = snap["flags"]
        result["excel_file"] = snap["file"]
        result["snapshot_date"] = snap["data_date"]
        if snap["data_date"] != date_str:
            result["snapshot_note"] = (
                f"no Excel data for {date_str}; used the most recent snapshot "
                f"on or before it ({snap['data_date']})"
            )
        _progress_set(phase=(f"Using stored Excel data for {snap['data_date']} "
                             f"({snap['file']})…"))
    elif source == "excel":
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
                continue

            action = "Paused" if gsd == 0 else "Enabled"

            for country in countries:
                found_in_country = 0
                had_error = False
                for customer_id in sorted(COUNTRY_CUSTOMER_IDS.get(country, set())):
                    try:
                        if gsd == 0:
                            campaigns = _find_enabled_campaigns(client, customer_id, shop_id)
                        else:
                            campaigns = _find_labeled_campaigns(client, customer_id, shop_id)
                    except Exception as ex:
                        had_error = True
                        result["errors"].append({
                            "shop_id": shop_id, "shop_name": shop_name,
                            "country": country, "customer_id": customer_id,
                            "step": "lookup", "error": str(ex),
                        })
                        continue

                    found_in_country += len(campaigns)

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

                # After all customer_ids for this country: if nothing was found,
                # diagnose WHY so the preview shows a useful reason.
                if found_in_country == 0 and not had_error:
                    try:
                        reason, diag_camps = _diagnose_no_campaigns(client, country, shop_id, gsd)
                    except Exception:
                        reason, diag_camps = "lookup_failed", []
                    result["skipped"].append({
                        "shop_id": shop_id,
                        "shop_name": shop_name,
                        "country": country,
                        "linkage": linkage,
                        "reason": reason,
                        "campaigns": diag_camps,
                    })

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
    logger.warning(
        "apply_selected STARTED — %d entries  server_port=%s  pid=%s",
        len(entries), _get_server_port(), os.getpid(),
    )
    if _KILL_SWITCH["active"]:
        logger.warning(
            "GSD LL KILL SWITCH ACTIVE — blocking apply_selected of %d entries. "
            "server_port=%s  pid=%s",
            len(entries), _get_server_port(), os.getpid(),
        )
        now_iso = datetime.now().isoformat(timespec="seconds")
        return {
            "started_at": now_iso,
            "finished_at": now_iso,
            "dry_run": True,
            "kill_switch_blocked": True,
            "feed_rows": len(entries),
            "paused": [], "enabled": [], "skipped": [], "errors": [],
            "paused_count": 0, "enabled_count": 0,
        }
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


def _get_server_port() -> str:
    """Best-effort: return the port this uvicorn instance listens on."""
    import sys
    for i, arg in enumerate(sys.argv):
        if arg == "--port" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith("--port="):
            return arg.split("=", 1)[1]
    return "unknown"


def _excel_scheduled_run() -> None:
    """Called by the timer: load (cache) the newest Excel data and reschedule.

    Does NOT pause/enable any campaigns — only refreshes the in-memory cache
    so the user can Preview/Run from the dashboard using the latest data.
    """
    try:
        logger.warning("GSD LL Excel scheduler: loading daily data (server port=%s, pid=%s)",
                       _get_server_port(), os.getpid())
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
        if _get_server_port() == "3003":
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

    # Cancel any existing timer to prevent duplicate firings
    if _EXCEL_TIMER:
        _EXCEL_TIMER.cancel()

    secs, target = _seconds_until(SCHEDULE_HOUR, SCHEDULE_MINUTE)
    with _EXCEL_LOCK:
        _EXCEL_STATE["next_run_at"] = target.isoformat(timespec="seconds")

    _EXCEL_TIMER = threading.Timer(secs, _excel_scheduled_run)
    _EXCEL_TIMER.daemon = True
    _EXCEL_TIMER.start()
    logger.info("GSD LL Excel scheduler: next run at %s (in %.0f seconds)", target, secs)


def start_excel_scheduler() -> None:
    """Initialize the daily Excel scheduler. Call on app startup.

    Also eagerly loads the newest Excel file into the in-memory cache so
    the "last successful data load" indicator is correct immediately after
    a server restart (the cache is volatile).
    """
    try:
        load_excel_data(notify=False)
        logger.info("GSD LL Excel scheduler: pre-loaded cache on startup")
    except FileNotFoundError:
        logger.info("GSD LL Excel scheduler: no Excel file found yet, skipping startup pre-load")
    except Exception:
        logger.warning("GSD LL Excel scheduler: startup pre-load failed", exc_info=True)
    # Fill the Date picker's window from the earlier files still on disk. A
    # no-op once every date in the window is stored, so it costs one glob and
    # one query per restart after the first.
    backfill_excel_snapshots()
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
