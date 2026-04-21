"""
GSD Budgets Service

Adjusts Google Shopping Direct (GSD) per-shop campaign budgets based on the
7-day shop-level margin and rev/click from Redshift, cross-checked with
SA360 per-campaign margin and a Google Sheets "BUDGET_CONSTRAINED" flag.

Port of GSD_verhogingen_verlagingen.py (NL) and _BE.py, refactored into
pure functions around a country-parameterized config. E-mail dispatch from
the source scripts is deliberately not ported — the dashboard renders
results in the UI and offers CSV export, same pattern as DMA Bidding.
"""
import base64
import csv as _csv
import io
import json as _json
import logging
import os
import random
import re
import sys
import textwrap
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import exceptions as gax_exceptions
from google.oauth2 import service_account
from psycopg2.extras import execute_values

from backend.database import (
    get_redshift_connection,
    return_redshift_connection,
)

# The vendored helper imports siblings relatively; placing backend/vendor/ on
# sys.path lets us import it as `util_searchads360` exactly like the source script.
_VENDOR_PATH = str(Path(__file__).parent / "vendor")
if _VENDOR_PATH not in sys.path:
    sys.path.insert(0, _VENDOR_PATH)

from util_searchads360 import SearchAds360Client  # noqa: E402
from google.ads.searchads360.v0.services.types.search_ads360_service import (  # noqa: E402
    SearchSearchAds360Request,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — per-country config captured from the two source scripts
# ---------------------------------------------------------------------------

MCC_CUSTOMER_ID = "3011145605"
SA360_LOGIN_CUSTOMER_ID = "9816507046"
SA360_CUSTOM_COLUMN_ID = 29126930
SA360_PAGE_SIZE = 10_000

EXCLUSIONS_SPREADSHEET_ID = "1y7kZmo9O7KO4uaG9wwq_wOtovsDMas07TAFDb0cyGAE"
EXCLUSIONS_TABLE = "pa.gsd_shop_exclusions_joep"
MISSED_SHOPS_TABLE = "pa.jvs_gsd_missed_shops"

# Anything the two scripts branch on by country lives here. Everything else is
# shared logic and stays country-agnostic.
COUNTRY_CONFIG: Dict[str, Dict[str, Any]] = {
    "NL": {
        "customer_id": "7938980174",
        "domain": 1,
        "sa360_account": "Direct Shopping",
        "campaign_limited_sheet": "1qMu0PEXKE_hbB0IfPEIzhjRYyAamCo1YfqpsS8jFbJw",
    },
    "BE": {
        "customer_id": "2454295509",
        "domain": 2,
        "sa360_account": "Beslist.be: Direct Shopping",
        "campaign_limited_sheet": "1f5Nwzk09lh0Efr8Ii6DH2umywqdfzmlpgCae2_jkZ-I",
    },
}
DEFAULT_COUNTRY = "NL"

# Decision thresholds (verbatim from the source scripts)
MARGIN_HARD_DROP = -25        # marge < -25 → verlagen-25 (and top-25 list for NL)
MARGIN_SOFT_DROP_HIGH = -5    # -25 < marge < -5 → verlagen-20
MARGIN_POSITIVE_FLOOR = 0     # marge > 0 branches into rev_click analysis
REV_CLICK_THRESHOLD = 1.38
LINKAGE_THRESHOLD = 0.5
TRANSACTIONS_THRESHOLD = 7
DELTA_UPPER = -15             # delta > -15 → verhogen-20
DELTA_LOWER = -30             # delta < -30 → c-verlagen-20

BUDGET_PCT_BY_ACTION = {
    "verlagen-25": -25,
    "verlagen-20": -20,
    "c-verlagen-20": -20,
    "verhogen-20": 20,
}

# ---------------------------------------------------------------------------
# Run history — persist to disk, same pattern as DMA Bidding
# ---------------------------------------------------------------------------

_DATA_DIR: Path = Path(__file__).parent / "data"
_HISTORY_FILE: Path = _DATA_DIR / "gsd_budgets_history.json"
_HISTORY_MAX = 50
_history_lock = threading.Lock()

# Serialize full runs across HTTP requests so two clicks can't race on budget mutations.
_run_lock = threading.Lock()


def _load_run_history_from_disk() -> List[Dict[str, Any]]:
    if _HISTORY_FILE.exists():
        try:
            data = _json.loads(_HISTORY_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data[:_HISTORY_MAX]
        except Exception as e:
            logger.warning(f"Failed to load GSD budgets history from {_HISTORY_FILE}: {e}")
    return []


def _save_run_history_to_disk():
    try:
        _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        _HISTORY_FILE.write_text(
            _json.dumps(_run_history, default=str, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"Failed to save GSD budgets history to {_HISTORY_FILE}: {e}")


_run_history: List[Dict[str, Any]] = _load_run_history_from_disk()


def _history_prepend(entry: Dict[str, Any]):
    with _history_lock:
        _run_history.insert(0, entry)
        while len(_run_history) > _HISTORY_MAX:
            _run_history.pop()
        _save_run_history_to_disk()


def _history_clear():
    with _history_lock:
        n = len(_run_history)
        _run_history.clear()
        _save_run_history_to_disk()
        return n


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _resolve_country(country: str) -> Dict[str, Any]:
    cfg = COUNTRY_CONFIG.get((country or "").upper())
    if not cfg:
        raise ValueError(
            f"Unknown country '{country}'. Expected one of: {list(COUNTRY_CONFIG.keys())}"
        )
    return cfg


def _sheets_service_account_file() -> str:
    """Resolve the path to the Google Sheets service-account JSON.

    Relative paths are resolved against the repo root (one level above `backend/`)
    so the same env value works under Docker and the local venv runner.
    """
    raw = os.environ.get(
        "GSD_SHEETS_SERVICE_ACCOUNT_FILE",
        "backend/data/gsd-campaign-creation.json",
    )
    p = Path(raw)
    if not p.is_absolute():
        p = Path(__file__).parent.parent / p
    return str(p)


def _get_client() -> GoogleAdsClient:
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


# ---------------------------------------------------------------------------
# SA360 client helper (one shared client per process)
# ---------------------------------------------------------------------------

_sa360_yaml_path: Path = _DATA_DIR / "search-ads-360.yaml"
_sa360_client: Optional[SearchAds360Client] = None
_sa360_lock = threading.Lock()


def _write_sa360_yaml(path: Path, login_customer_id: str) -> None:
    yaml = textwrap.dedent(
        f"""\
        developer_token: "{os.environ.get('GOOGLE_DEVELOPER_TOKEN', '')}"
        client_id: "{os.environ.get('GOOGLE_CLIENT_ID', '')}"
        client_secret: "{os.environ.get('GOOGLE_CLIENT_SECRET', '')}"
        refresh_token: "{os.environ.get('GOOGLE_REFRESH_TOKEN', '')}"
        use_proto_plus: True
        login_customer_id: "{login_customer_id}"
        """
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml, encoding="utf-8")


def _ensure_sa360_client(customer_id: str) -> SearchAds360Client:
    global _sa360_client
    with _sa360_lock:
        if _sa360_client is None:
            _write_sa360_yaml(_sa360_yaml_path, SA360_LOGIN_CUSTOMER_ID)
            _sa360_client = SearchAds360Client.load_from_file(str(_sa360_yaml_path))
        # set_ids is safe to re-call; we still pin the account each time in case
        # a later run switches country within the same process.
        _sa360_client.set_ids(customer_id, SA360_LOGIN_CUSTOMER_ID)
        return _sa360_client


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------


def _gspread_client():
    # Local import so the rest of the module still loads if gspread is missing
    # (e.g. during unit tests that mock sheet access).
    import gspread

    creds = service_account.Credentials.from_service_account_file(
        _sheets_service_account_file(),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def preload_budget_constrained_cache() -> Dict[str, Dict[str, str]]:
    """Pre-read the per-country BUDGET_CONSTRAINED sheets once so the main loop
    can look up each campaign in O(1) instead of hitting the Sheets API per hit.
    Matches the source script's `preload_campaign_limited_data`.
    Returns {country_code: {campaign_name: column_d_value}}.
    """
    client = _gspread_client()
    cache: Dict[str, Dict[str, str]] = {}
    for country, cfg in COUNTRY_CONFIG.items():
        try:
            sheet = client.open_by_key(cfg["campaign_limited_sheet"]).sheet1
            data = sheet.get_all_values()
            if data and len(data) >= 2:
                records = data[1:]
                cache[country] = {row[1]: row[3] for row in records if len(row) >= 4}
            else:
                cache[country] = {}
        except Exception as e:
            logger.error(f"Failed to read BUDGET_CONSTRAINED sheet for {country}: {e}")
            cache[country] = {}
    return cache


def _is_budget_constrained(campaign_name: str, country: str, cache: Dict[str, Dict[str, str]]) -> int:
    value = cache.get(country.upper(), {}).get(campaign_name)
    if value is None:
        return 0
    return 1 if "BUDGET_CONSTRAINED" in value.upper() else 0


def sync_shop_exclusions() -> int:
    """Mirror the exclusions Google Sheet into Redshift.

    The sheet is the user-maintained source of truth; the Redshift table is
    referenced by the main performance query's NOT IN sub-select. Always
    truncate-and-reinsert to match the source script's behaviour. Returns
    the row count that was written.
    """
    client = _gspread_client()
    sheet = client.open_by_key(EXCLUSIONS_SPREADSHEET_ID).sheet1
    data = sheet.get_all_values()
    if not data or len(data) < 2:
        logger.warning("Exclusions sheet returned no data; skipping sync.")
        return 0

    headers = data[0]
    records = [row for row in data[1:] if any((cell or "").strip() for cell in row)]

    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        column_defs = ",\n  ".join([f'"{col}" VARCHAR(1000)' for col in headers])
        cur.execute(f'CREATE TABLE IF NOT EXISTS {EXCLUSIONS_TABLE} (\n  {column_defs}\n);')
        cur.execute(f"DELETE FROM {EXCLUSIONS_TABLE};")
        if records:
            insert_cols = ", ".join([f'"{col}"' for col in headers])
            insert_sql = f"INSERT INTO {EXCLUSIONS_TABLE} ({insert_cols}) VALUES %s"
            execute_values(cur, insert_sql, [tuple(row) for row in records])
        conn.commit()
        cur.close()
        return len(records)
    finally:
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Redshift helpers
# ---------------------------------------------------------------------------


def get_redshift_shop_data(
    country: str,
    shop_names: Optional[List[str]] = None,
    excluded: bool = False,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return 7-day shop-level performance for the given country.

    Mirrors `getRedShiftData` in the source scripts: last-7-day omzet,
    last-day cost, transactions, linkage (uuid-linked clicks / total clicks),
    and rev/click. Excludes shops listed in `EXCLUSIONS_TABLE`.
    """
    cfg = _resolve_country(country)
    today_date = datetime.today().date()
    yesterday_sql = (today_date - timedelta(days=1)).strftime("%Y-%m-%d")
    last_week_sql = (today_date - timedelta(days=7)).strftime("%Y-%m-%d")

    limit_statement = f"LIMIT {int(limit)}" if limit else ""

    name_filter = ""
    if shop_names:
        escaped = ["'" + n.replace("'", "''") + "'" for n in shop_names]
        if not excluded:
            name_filter = f"AND tac.shop_name IN ({', '.join(escaped)})"
        else:
            name_filter = f"AND tac.shop_name NOT IN ({', '.join(escaped)})"

    query = f"""
        WITH omzet_kosten AS (
          SELECT
            tac.outclick_shop_id AS shop_id,
            tac.shop_name,
            SUM(tac.click_revenue) AS omzet,
            MAX(sa.kosten) AS kosten,
            SUM(tac.transactions) AS transactions,
            COUNT(DISTINCT CASE WHEN tac.uuid_linked = 1 THEN tac.stats_id_stat END) * 1.0
              / NULLIF(COUNT(DISTINCT tac.stats_id_stat), 0) AS linkage,
            SUM(tac.revenue_excl) / NULLIF(COUNT(DISTINCT tac.stats_id_stat), 0) AS rev_click
          FROM bt.cpa_outclicks_transactional tac
          LEFT JOIN (
            SELECT
              shop_id,
              SUM(cost) AS kosten
            FROM hda.sa360_adgroup
            WHERE
              date(date) >= '{yesterday_sql}'
              AND deleted_ind = 0
              AND account = '{cfg['sa360_account']}'
            GROUP BY shop_id
          ) sa ON sa.shop_id = tac.outclick_shop_id
          WHERE
            tac.actual_ind = 1
            AND tac.deleted_ind = 0
            AND tac.label IN (
              'cpa', 'cpa_cpc', 't3_fallback',
              'affiliate_linked_revenue', 'affiliate_unlinked_click'
            )
            AND date(tac.date) >= '{last_week_sql}'
            AND tac.marketing_channel_aff_id_name = 'Google Shopping Direct'
            AND tac.outclick_shop_id IS NOT NULL
            AND tac.domain = {cfg['domain']}
            AND tac.outclick_shop_id NOT IN (
              SELECT DISTINCT "shop id"
              FROM {EXCLUSIONS_TABLE}
              WHERE "shop id" IS NOT NULL
            )
            {name_filter}
          GROUP BY tac.outclick_shop_id, tac.shop_name
        )
        SELECT
          shop_id,
          shop_name,
          ROUND(omzet) AS omzet,
          ROUND(COALESCE(kosten, 0)) AS kosten,
          ROUND(omzet - COALESCE(kosten, 0)) AS marge,
          transactions,
          ROUND(linkage, 4) AS linkage,
          ROUND(rev_click, 2) AS rev_click
        FROM omzet_kosten
        WHERE omzet IS NOT NULL
        ORDER BY shop_id
        {limit_statement}
    """

    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        # RealDictCursor returns dict rows already; coerce to plain dicts for safety.
        result = [dict(r) for r in rows]
        cur.close()
        return result
    finally:
        return_redshift_connection(conn)


def get_rev_click_old(country: str, shop_ids: List[int], daterange: str) -> Optional[float]:
    cfg = _resolve_country(country)
    today_date = datetime.today().date()
    old_sql = (today_date - timedelta(days=28)).strftime("%Y-%m-%d")

    if daterange == "28":
        date_filter = f"AND date(tac.date) = '{old_sql}'"
    elif daterange == "35-28":
        old_sql1 = (today_date - timedelta(days=35)).strftime("%Y-%m-%d")
        date_filter = f"AND date(tac.date) BETWEEN '{old_sql1}' AND '{old_sql}'"
    else:
        raise ValueError(f"Unknown daterange {daterange!r}")

    if shop_ids:
        ids_sql = ",".join(str(int(s)) for s in shop_ids)
        shop_filter = f"AND tac.outclick_shop_id IN ({ids_sql})"
    else:
        shop_filter = ""

    query = f"""
        WITH omzet_kosten AS (
          SELECT
            tac.outclick_shop_id AS shop_id,
            SUM(tac.revenue_excl) / NULLIF(COUNT(DISTINCT tac.stats_id_stat), 0) AS rev_click
          FROM bt.cpa_outclicks_transactional tac
          WHERE
            tac.actual_ind = 1
            AND tac.deleted_ind = 0
            AND tac.label IN (
              'cpa', 'cpa_cpc', 't3_fallback',
              'affiliate_linked_revenue', 'affiliate_unlinked_click'
            )
            {date_filter}
            AND tac.marketing_channel_aff_id_name = 'Google Shopping Direct'
            AND tac.outclick_shop_id IS NOT NULL
            AND tac.domain = {cfg['domain']}
            {shop_filter}
          GROUP BY tac.outclick_shop_id
        )
        SELECT ROUND(rev_click, 2) AS rev_click
        FROM omzet_kosten
        LIMIT 1;
    """

    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
    finally:
        return_redshift_connection(conn)

    if not row:
        return None
    # RealDictCursor → dict; plain cursor → tuple. Support both.
    val = row.get("rev_click") if isinstance(row, dict) else row[0]
    return float(val) if val is not None else None


def _count_shops_for_country(country: str) -> int:
    """Cheap shop-count over the last 7 days — used by get_stats to avoid fetching full rows.
    Does not apply the shop-name filter; that's only relevant at run time.
    """
    cfg = _resolve_country(country)
    today_date = datetime.today().date()
    last_week_sql = (today_date - timedelta(days=7)).strftime("%Y-%m-%d")
    query = f"""
        SELECT COUNT(DISTINCT tac.outclick_shop_id) AS n
        FROM bt.cpa_outclicks_transactional tac
        WHERE tac.actual_ind = 1
          AND tac.deleted_ind = 0
          AND tac.label IN (
            'cpa', 'cpa_cpc', 't3_fallback',
            'affiliate_linked_revenue', 'affiliate_unlinked_click'
          )
          AND date(tac.date) >= '{last_week_sql}'
          AND tac.marketing_channel_aff_id_name = 'Google Shopping Direct'
          AND tac.outclick_shop_id IS NOT NULL
          AND tac.domain = {cfg['domain']}
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
    finally:
        return_redshift_connection(conn)
    if not row:
        return 0
    val = row.get("n") if isinstance(row, dict) else row[0]
    return int(val or 0)


def upload_missed_shops(country: str, missed_shops: List[str], dates_missed: List[str]) -> int:
    if not missed_shops or not dates_missed:
        return 0
    if len(missed_shops) != len(dates_missed):
        raise ValueError("missed_shops and dates_missed must have equal length")

    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MISSED_SHOPS_TABLE} (
                shop_name VARCHAR(500),
                "date" DATE,
                country VARCHAR(10)
            );
        """)
        conn.commit()
        parsed_dates = [_coerce_date(d) for d in dates_missed]
        values = [(s, d, country.upper()) for s, d in zip(missed_shops, parsed_dates)]
        execute_values(
            cur,
            f'INSERT INTO {MISSED_SHOPS_TABLE} (shop_name, "date", country) VALUES %s',
            values,
        )
        conn.commit()
        cur.close()
        return len(values)
    finally:
        return_redshift_connection(conn)


def _coerce_date(x: Any) -> date:
    if isinstance(x, date):
        return x
    s = str(x).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%d%m%Y").date()
    raise ValueError(f"Unknown date format: {x!r}")


# ---------------------------------------------------------------------------
# Google Ads per-shop helpers
# ---------------------------------------------------------------------------


def get_campaigns_for_shop(
    country: str,
    shop_name: str,
    shop_id: int,
) -> Tuple[List[str], List[str]]:
    cfg = _resolve_country(country)
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    campaign_service = client.get_service("CampaignService")

    escaped_name = str(shop_name).replace("'", "\\'")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign
        WHERE campaign.name LIKE '%shop_id:{shop_id}]%'
          AND campaign.name LIKE '%shop:{escaped_name}]%'
          AND campaign.status = ENABLED
    """
    resource_names: List[str] = []
    names: List[str] = []
    try:
        response = ga_service.search(customer_id=cfg["customer_id"], query=query)
        for row in response:
            resource_names.append(
                campaign_service.campaign_path(cfg["customer_id"], row.campaign.id)
            )
            names.append(row.campaign.name)
    except GoogleAdsException as ex:
        logger.warning(f"Google Ads API error (get_campaigns_for_shop {shop_name}): {ex.failure}")
    return resource_names, names


def get_total_marge_sa360(
    country: str,
    campaign_name: str,
    start_days_ago: int = 9,
    end_days_ago: int = 3,
) -> float:
    """SA360 per-campaign marge over a trailing window, summing custom column 29126930."""
    cfg = _resolve_country(country)
    client = _ensure_sa360_client(cfg["customer_id"])
    service = client.get_service()

    now = datetime.now(pytz.timezone("Europe/Amsterdam"))
    end_date = (now - timedelta(days=end_days_ago)).strftime("%Y%m%d")
    start_date = (now - timedelta(days=start_days_ago)).strftime("%Y%m%d")

    query = f"""
        SELECT
          segments.date,
          custom_columns.id[{SA360_CUSTOM_COLUMN_ID}]
        FROM campaign
        WHERE segments.date BETWEEN {start_date} AND {end_date}
          AND campaign.name = '{campaign_name.replace("'", "\\'")}'
    """

    request = SearchSearchAds360Request()
    request.customer_id = cfg["customer_id"]
    request.query = query
    request.page_size = SA360_PAGE_SIZE

    # Retry with exponential backoff on 429/503 — SA360 often rate-limits per-campaign loops.
    max_retries = 6
    base_delay = 1.0
    results = None
    for attempt in range(1, max_retries + 1):
        try:
            results = service.search(request=request)
            break
        except gax_exceptions.ResourceExhausted as ex:
            m = re.search(r"Retry in\s+(\d+)\s+seconds", str(ex))
            retry_after = int(m.group(1)) if m else 0
            delay = max(retry_after, base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.5 * base_delay)
            if attempt >= max_retries:
                raise
            time.sleep(delay)
        except (gax_exceptions.Aborted, gax_exceptions.ServiceUnavailable, gax_exceptions.DeadlineExceeded):
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5 * base_delay)
            if attempt >= max_retries:
                raise
            time.sleep(delay)

    if results is None:
        raise RuntimeError("SA360 search returned no results after retries.")

    headers = results.custom_column_headers
    id_to_idx = {h.id: i for i, h in enumerate(headers)}
    idx = id_to_idx.get(SA360_CUSTOM_COLUMN_ID)
    if idx is None:
        raise RuntimeError(
            f"Custom column id {SA360_CUSTOM_COLUMN_ID} not found. "
            f"Available: {[h.id for h in headers]}"
        )

    total = 0.0
    for row in results:
        cc = row.custom_columns[idx]
        if hasattr(cc, "double_value") and cc.double_value:
            total += float(cc.double_value)
        elif hasattr(cc, "long_value") and cc.long_value:
            total += float(cc.long_value)
    return round(total, 2)


def adjust_campaign_budget(
    country: str,
    campaign_resource_name: str,
    percentage_change: float,
    dry_run: bool,
) -> Dict[str, Any]:
    """Multiply the campaign's daily budget by (1 + percentage_change / 100).

    Returns a dict with the old/new EUR amounts and a status field.
    On dry_run, queries the current budget but skips the mutation.
    """
    cfg = _resolve_country(country)
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            campaign.name,
            campaign_budget.amount_micros,
            campaign.campaign_budget
        FROM campaign
        WHERE campaign.resource_name = '{campaign_resource_name}'
    """
    try:
        response = ga_service.search(customer_id=cfg["customer_id"], query=query)
        result = next(iter(response), None)
    except GoogleAdsException as ex:
        logger.warning(f"Google Ads API error reading budget: {ex.failure}")
        return {"status": "error", "error": str(ex)}

    if not result:
        return {"status": "not_found"}

    budget_resource_name = result.campaign.campaign_budget
    current_micros = result.campaign_budget.amount_micros
    current_eur = round(current_micros / 1_000_000, 2)
    new_eur = round(current_eur * (1 + percentage_change / 100.0), 2)

    if new_eur < 0.01:
        return {
            "status": "skipped_too_low",
            "current_eur": current_eur,
            "new_eur": new_eur,
        }

    # Round to the nearest whole cent-thousand (i.e. keep micro granularity at 1000).
    new_micros = int(round(new_eur * 1_000_000 / 1000)) * 1000

    if dry_run:
        return {
            "status": "dry_run",
            "current_eur": current_eur,
            "new_eur": new_eur,
        }

    campaign_budget_service = client.get_service("CampaignBudgetService")
    operation = client.get_type("CampaignBudgetOperation")
    update = operation.update
    update.resource_name = budget_resource_name
    update.amount_micros = new_micros
    operation.update_mask.paths.append("amount_micros")

    try:
        campaign_budget_service.mutate_campaign_budgets(
            customer_id=cfg["customer_id"], operations=[operation]
        )
    except GoogleAdsException as ex:
        logger.warning(f"Google Ads API error mutating budget: {ex.failure}")
        return {
            "status": "error",
            "current_eur": current_eur,
            "new_eur": new_eur,
            "error": str(ex),
        }

    return {"status": "success", "current_eur": current_eur, "new_eur": new_eur}


def get_rev_click_delta(nieuw: Optional[float], oud: Optional[float]) -> Optional[float]:
    if nieuw is None or oud is None or oud == 0:
        return None
    return ((nieuw - oud) / oud) * 100


# ---------------------------------------------------------------------------
# Pure decision logic
# ---------------------------------------------------------------------------


def decide_shop_action(
    *,
    marge: float,
    rev_click: Optional[float],
    rev_click_delta: Optional[float],
    linkage: Optional[float],
    transactions: Optional[int],
) -> Tuple[Optional[str], Optional[str]]:
    """Return (action, skip_reason) for a shop given its 7-day metrics.

    The skip_reason is only populated for the paths that would otherwise
    silently drop off (e.g. the "middle delta band" → missed). See the
    table comment near the bottom of the source scripts for the spec.
    """
    if marge is None:
        return None, "no_marge"
    if marge < MARGIN_HARD_DROP:
        return "verlagen-25", None
    if MARGIN_HARD_DROP < marge < MARGIN_SOFT_DROP_HIGH:
        return "verlagen-20", None
    if marge > MARGIN_POSITIVE_FLOOR:
        linkage = linkage or 0
        transactions = transactions or 0
        if linkage > LINKAGE_THRESHOLD and transactions >= TRANSACTIONS_THRESHOLD:
            if rev_click is not None and rev_click > REV_CLICK_THRESHOLD:
                if rev_click_delta is None:
                    return None, "delta_unknown"
                if rev_click_delta > DELTA_UPPER:
                    return "verhogen-20", None
                if rev_click_delta < DELTA_LOWER:
                    return "c-verlagen-20", None
                return None, "missed"
            if rev_click is not None and rev_click < REV_CLICK_THRESHOLD:
                return "verlagen-20", None
            return None, "rev_click_unknown"
        if transactions < TRANSACTIONS_THRESHOLD:
            return None, "low_transactions"
        if linkage < LINKAGE_THRESHOLD:
            return None, "low_linkage"
    return None, "no_action"


def _should_apply_to_campaign(
    action: str,
    campaign_marge: float,
    campaign_name: str,
    is_budget_constrained: int,
) -> Tuple[bool, Optional[str]]:
    """Return (apply, skip_reason) for the per-campaign gate."""
    if action == "verlagen-25":
        if campaign_marge < 0:
            return True, None
        return False, "campaign_marge_non_negative"
    if action == "verlagen-20":
        if campaign_marge < 0:
            return True, None
        return False, "campaign_marge_non_negative"
    if action == "c-verlagen-20":
        if "[label:c]" in campaign_name:
            return True, None
        return False, "not_label_c"
    if action == "verhogen-20":
        if campaign_marge > 0 and is_budget_constrained == 1:
            return True, None
        if is_budget_constrained != 1:
            return False, "not_budget_constrained"
        return False, "campaign_marge_not_positive"
    return False, "unknown_action"


# ---------------------------------------------------------------------------
# Public entry points for the router
# ---------------------------------------------------------------------------


def get_stats(country: str) -> Dict[str, Any]:
    """Cheap summary for the UI stat cards: how many shops will be evaluated,
    plus a count of enabled GSD campaigns in the target account."""
    cfg = _resolve_country(country)
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    campaign_count = 0
    try:
        response = ga_service.search(
            customer_id=cfg["customer_id"],
            query="""
                SELECT campaign.id
                FROM campaign
                WHERE campaign.name LIKE '%shop_id:%'
                  AND campaign.name LIKE '%shop:%'
                  AND campaign.status = 'ENABLED'
            """,
        )
        campaign_count = sum(1 for _ in response)
    except GoogleAdsException as ex:
        logger.warning(f"Google Ads API error (get_stats campaigns): {ex.failure}")

    shop_count = 0
    try:
        shop_count = _count_shops_for_country(country)
    except Exception as e:
        logger.warning(f"Failed to count shops in get_stats: {e}")

    return {
        "country": country.upper(),
        "customer_id": cfg["customer_id"],
        "campaign_count": campaign_count,
        "shop_count": shop_count,
    }


def run_gsd_budgets(
    *,
    country: str = DEFAULT_COUNTRY,
    dry_run: bool = True,
    start_days_ago: int = 9,
    end_days_ago: int = 3,
    limit_shops: Optional[int] = None,
    shop_names: Optional[List[str]] = None,
    shop_names_excluded: bool = False,
    skip_missed_upload: bool = False,
) -> Dict[str, Any]:
    """Main flow: fetch shops → decide action → per-campaign gate → mutate → collect results."""
    country = country.upper()
    _resolve_country(country)  # raises if unsupported

    run_id = len(_run_history) + 1
    start_time = datetime.now()

    with _run_lock:
        logger.info(
            f"GSD Budgets run #{run_id} start (country={country} dry_run={dry_run} "
            f"start={start_days_ago} end={end_days_ago} limit={limit_shops})"
        )

        exclusions_synced = 0
        exclusions_sync_status = "synced"
        if dry_run:
            exclusions_sync_status = "skipped_dry_run"
            logger.info("Dry-run: skipping pa.gsd_shop_exclusions_joep sync; using whatever is currently in the table")
        else:
            try:
                exclusions_synced = sync_shop_exclusions()
            except Exception as e:
                logger.warning(f"sync_shop_exclusions failed: {e}")
                exclusions_sync_status = f"failed: {e}"

        try:
            limited_cache = preload_budget_constrained_cache()
        except Exception as e:
            logger.warning(f"preload_budget_constrained_cache failed: {e}")
            limited_cache = {}

        shop_rows = get_redshift_shop_data(
            country=country,
            shop_names=shop_names,
            excluded=shop_names_excluded,
            limit=limit_shops,
        )

        today_str = datetime.today().strftime("%d-%m-%Y")
        results: List[Dict[str, Any]] = []
        over_loss_25: List[Dict[str, Any]] = []
        missed_shops: List[str] = []
        missed_dates: List[str] = []
        summary_counts = {
            "verlagen-25": 0,
            "verlagen-20": 0,
            "c-verlagen-20": 0,
            "verhogen-20": 0,
            "no_action": 0,
            "missed": 0,
            "budget_changed": 0,
            "shops_over_loss_25": 0,
        }

        for margin_data in shop_rows:
            shop_name = margin_data.get("shop_name")
            shop_id = margin_data.get("shop_id")
            marge = margin_data.get("marge")
            try:
                marge_f = float(marge) if marge is not None else None
            except (TypeError, ValueError):
                marge_f = None
            rev_click = margin_data.get("rev_click")
            try:
                rev_click_f = float(rev_click) if rev_click is not None else None
            except (TypeError, ValueError):
                rev_click_f = None
            linkage = margin_data.get("linkage")
            try:
                linkage_f = float(linkage) if linkage is not None else 0.0
            except (TypeError, ValueError):
                linkage_f = 0.0
            transactions = int(margin_data.get("transactions") or 0)

            rev_click_old: Optional[float] = None
            rev_click_delta: Optional[float] = None

            # Collect top-25 (NL-only display; data always available)
            if marge_f is not None and marge_f < MARGIN_HARD_DROP:
                over_loss_25.append({
                    "shop_name": shop_name,
                    "shop_id": shop_id,
                    "marge": round(marge_f, 2),
                })

            # In the rev_click-> delta branch, the source script fetches the 4-week
            # baseline *before* calling decide_shop_action. We mirror that order so
            # decide_shop_action gets a real delta, not None.
            if (
                marge_f is not None
                and marge_f > MARGIN_POSITIVE_FLOOR
                and linkage_f > LINKAGE_THRESHOLD
                and transactions >= TRANSACTIONS_THRESHOLD
                and rev_click_f is not None
                and rev_click_f > REV_CLICK_THRESHOLD
            ):
                try:
                    rev_click_old = get_rev_click_old(country, [int(shop_id)], "28")
                    if not rev_click_old:
                        rev_click_old = get_rev_click_old(country, [int(shop_id)], "35-28")
                    rev_click_delta = get_rev_click_delta(rev_click_f, rev_click_old)
                except Exception as e:
                    logger.warning(f"rev_click_old fetch failed for {shop_name}: {e}")

            action, skip_reason = decide_shop_action(
                marge=marge_f,
                rev_click=rev_click_f,
                rev_click_delta=rev_click_delta,
                linkage=linkage_f,
                transactions=transactions,
            )

            shop_record = {
                "shop_name": shop_name,
                "shop_id": shop_id,
                "marge": round(marge_f, 2) if marge_f is not None else None,
                "rev_click": round(rev_click_f, 2) if rev_click_f is not None else None,
                "rev_click_old": round(rev_click_old, 2) if rev_click_old is not None else None,
                "rev_click_delta": round(rev_click_delta, 2) if rev_click_delta is not None else None,
                "linkage": round(linkage_f, 4),
                "transactions": transactions,
                "action": action,
                "skip_reason": skip_reason,
                "campaigns": [],
            }

            if skip_reason == "missed":
                missed_shops.append(shop_name)
                missed_dates.append(today_str)
                summary_counts["missed"] += 1

            if marge_f is not None and marge_f < MARGIN_HARD_DROP:
                summary_counts["shops_over_loss_25"] += 1

            if action is None:
                summary_counts["no_action"] += 1
                results.append(shop_record)
                continue

            summary_counts[action] = summary_counts.get(action, 0) + 1

            campaign_resource_names, campaign_names = get_campaigns_for_shop(
                country, shop_name, shop_id
            )

            for campaign_resource_name, campaign_name in zip(campaign_resource_names, campaign_names):
                try:
                    campaign_marge = get_total_marge_sa360(
                        country,
                        campaign_name,
                        start_days_ago=start_days_ago,
                        end_days_ago=end_days_ago,
                    )
                except Exception as e:
                    logger.warning(f"SA360 marge failed for {campaign_name}: {e}")
                    campaign_marge = 0.0

                constrained = _is_budget_constrained(campaign_name, country, limited_cache)
                apply, per_cg_skip = _should_apply_to_campaign(
                    action, campaign_marge, campaign_name, constrained
                )

                campaign_record = {
                    "campaign_name": campaign_name,
                    "campaign_resource_name": campaign_resource_name,
                    "campaign_marge": campaign_marge,
                    "is_budget_constrained": bool(constrained),
                    "applied": False,
                    "budget_old": None,
                    "budget_new": None,
                    "mutation_status": None,
                    "skip_reason": per_cg_skip,
                }

                if apply:
                    pct = BUDGET_PCT_BY_ACTION[action]
                    mutation = adjust_campaign_budget(
                        country, campaign_resource_name, pct, dry_run=dry_run
                    )
                    campaign_record["mutation_status"] = mutation.get("status")
                    campaign_record["budget_old"] = mutation.get("current_eur")
                    campaign_record["budget_new"] = mutation.get("new_eur")
                    if mutation.get("status") in ("success", "dry_run"):
                        campaign_record["applied"] = True
                        summary_counts["budget_changed"] += 1

                shop_record["campaigns"].append(campaign_record)

            results.append(shop_record)

        missed_uploaded = 0
        missed_upload_status = "skipped"
        if missed_shops and not skip_missed_upload and not dry_run:
            try:
                missed_uploaded = upload_missed_shops(country, missed_shops, missed_dates)
                missed_upload_status = "uploaded"
            except Exception as e:
                logger.warning(f"upload_missed_shops failed: {e}")
                missed_upload_status = f"error: {e}"
        elif dry_run:
            missed_upload_status = "skipped_dry_run"
        elif skip_missed_upload:
            missed_upload_status = "skipped_by_flag"

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        run_result = {
            "run_id": run_id,
            "country": country,
            "dry_run": dry_run,
            "start_days_ago": start_days_ago,
            "end_days_ago": end_days_ago,
            "date_range": {
                "start": (datetime.now() - timedelta(days=start_days_ago)).strftime("%Y-%m-%d"),
                "end": (datetime.now() - timedelta(days=end_days_ago)).strftime("%Y-%m-%d"),
            },
            "limit_shops": limit_shops,
            "shop_names_filter": shop_names,
            "shop_names_excluded": shop_names_excluded,
            "skip_missed_upload": skip_missed_upload,
            "timestamp": start_time.isoformat(),
            "duration_seconds": round(duration, 1),
            "exclusions_synced": exclusions_synced,
            "exclusions_sync_status": exclusions_sync_status,
            "summary": summary_counts,
            "shops_evaluated": len(shop_rows),
            "results": results,
            "over_loss_25": over_loss_25,
            "missed_shops": missed_shops,
            "missed_shops_uploaded": missed_uploaded,
            "missed_upload_status": missed_upload_status,
            "status": "completed",
        }

        _history_prepend(run_result)

        logger.info(
            f"GSD Budgets run #{run_id} done in {duration:.1f}s summary={summary_counts}"
        )
        return run_result
