"""
GSD Campaigns Service

Manages Google Shopping Direct (GSD) campaigns across multiple Google Ads accounts.
Handles campaign creation, pausing, enabling, and removal. Integrates with Merchant
Center for account linking and Redshift for shop change data.
"""
import os
import re
import time
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - stdlib on py3.9+
    ZoneInfo = None

import psycopg2
from psycopg2.extras import execute_values
from backend.database import get_db_connection, return_db_connection
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2
from googleapiclient.discovery import build
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACCOUNTS = {
    "NL_CPR": {"customer_id": "7938980174", "mc_id": "5592708765", "country": "NL", "type": "CPR"},
    "BE_CPR": {"customer_id": "2454295509", "mc_id": "5588879919", "country": "BE", "type": "CPR"},
    "DE_CPR": {"customer_id": "4192567576", "mc_id": "5342886105", "country": "DE", "type": "CPR"},
    "NL_CPC": {"customer_id": "7938980174", "mc_id": "5592708765", "country": "NL", "type": "CPC"},
    # Every country now has ONE account serving both models (Joep, 2026-08-04), so the
    # derived model can no longer send a lookup to the wrong account. That mattered
    # because a shop's model flips on the very day it goes off — is_wecantrack_shop /
    # is_pixel_shop drop in the same feed update as the GSD flag, so an 'uit' row for a
    # CPR shop reads as CPC (see get_redshift_shop_changes). Previously:
    #   * BE_CPC pointed at its own account 7565255758 → the pause searched an account
    #     holding 4 long-paused MANUAL_CPC campaigns, found nothing, and reported
    #     "no_live_campaigns_to_pause" while Emob.be kept 7 ENABLED campaigns in BE_CPR.
    #   * DE_CPC did not exist at all → "no_account_config", and Emob-moebel.de kept 7.
    # 7565255758 is no longer queried by any flow; its 4 campaigns are PAUSED with zero
    # spend and both shops (599127, 27143) are already managed in 2454295509.
    "BE_CPC": {"customer_id": "2454295509", "mc_id": "5588879919", "country": "BE", "type": "CPC"},
    "DE_CPC": {"customer_id": "4192567576", "mc_id": "5342886105", "country": "DE", "type": "CPC"},
}

# Accounts no longer used for creating, but still swept by the PAUSE path (Joep,
# 2026-08-04: "there are only paused campaigns in that account so it wouldn't hurt
# checking it"). 7565255758 was BE_CPC until BE was consolidated into 2454295509; it
# still holds GSD_SCRIPT campaigns, so a shop going off for BE should go dark there too
# rather than leaving them behind where no flow can ever reach them again.
# Cost of sweeping is one read per shop, and pausing skips anything not ENABLED — so an
# account that only holds paused campaigns produces zero mutations. Pause-only by design:
# these never receive creations, and _find_account_info() does not consult this map.
PAUSE_EXTRA_CUSTOMER_IDS: Dict[str, List[str]] = {
    "BE": ["7565255758"],
}

MCC_CUSTOMER_ID = "3011145605"

SCRIPT_LABEL = "GSD_SCRIPT"

# Mirrors gsd_ll_service.LL_LABEL — importing it would be circular (that module imports
# from this one). A campaign carrying it was paused by the low-linkage flow on purpose,
# so the create-run must not switch it back on; the LL run re-enables it when linkage
# recovers. Keep the two spellings in sync.
LL_PAUSED_LABEL = "GSD_LL_PAUSED"

# How far back the post-run reconcile looks for unlogged creations. change_event keeps
# ~30 days (hard limit), and a week covers "it broke on Friday, someone ran it on Monday"
# without re-reading a month of history on every run.
RECONCILE_WINDOW_DAYS = 7

# A freshly created GSD campaign is MANUAL_CPC. Going live requires a manual step in
# SA360 (a colleague pairs the target-ROAS bid strategy), and only after that is the
# campaign enabled. Measured 2026-07-31 across all GSD_SCRIPT campaigns:
#   ENABLED  TARGET_ROAS 2.354 · PAUSED TARGET_ROAS 354 · PAUSED MANUAL_CPC 105
# so bidding_strategy_type IS the "has the SA360 pairing happened" flag. (Note: the
# portfolio field campaign.bidding_strategy is empty on all of them — SA360 sets a
# STANDARD target-ROAS strategy, so testing that field would reject everything.)
# The run must never enable a campaign still in a pending state: that would put a
# campaign live without its bid strategy — exactly what a second run of the day would
# have done to its own creations.
BID_STRATEGY_PENDING = {"MANUAL_CPC", "UNSPECIFIED", "UNKNOWN"}


def _bid_strategy_ready(strategy_type: Optional[str]) -> bool:
    """True when a campaign's bid strategy has been paired in SA360."""
    return bool(strategy_type) and strategy_type.upper() not in BID_STRATEGY_PENDING

# Persistent per-(shop, country) creation dates (n8n-vector-db PostgreSQL). The
# Google Ads API exposes no campaign creation-date field (campaign.start_date was
# removed in v24) and change_event only retains ~30 days, so we log each shop's
# creation date at create time and grow coverage over time (seeded from a
# spreadsheet backfill). The Campaigns-created "Date" column reads from here.
CAMPAIGN_CREATED_TABLE = "pa.jvs_gsd_campaign_created"

LABELS_CPR = ["a", "b", "c", "no_data", "no_ean"]
LABELS_CPC = ["a,b", "c,no_data,no_ean"]

# --- CPC price-bucket structure (newly connected CPC shops) -----------------
# From "create GSD-campaigns CPR CPC split.py" (Joep, 2026-08-17). ONE campaign per
# shop holding 14 ad groups — one per custom_label_4 price bucket, each with its own
# max CPC and a listing tree that serves only its own bucket. Deliberately no a/b/c
# score split: a CPC shop shares no conversion data, so there is no score to split on.
#
# Adopted for NEW CPC shops only (Joep, 2026-08-17). A CPC shop that already carries
# the legacy LABELS_CPC pair keeps it — see _labels_for_shop().
#
# The bucket VALUES are the ones that actually exist in the feed, verified 2026-08-17
# against dra.gmc_products_issues (~40M rows): all 14 present, and the top two are
# spelled '1597-2594' and '2594+'. PRICE_BUCKETS below ends in '1597-2584' /
# '2584-Onbeperkt', which match NO product at all — the two lists disagree on purpose,
# do not "align" them. (PRICE_BUCKETS is wrong and inherited from GSD-CPC.py; fixing it
# touches the live legacy CPC estate and is a separate decision.)
CPC_BUCKET_BIDS = [
    ("0-8", 0.09), ("8-13", 0.10), ("13-21", 0.12), ("21-34", 0.14),
    ("34-55", 0.16), ("55-89", 0.17), ("89-144", 0.20), ("144-233", 0.22),
    ("233-377", 0.26), ("377-610", 0.30), ("610-987", 0.34), ("987-1597", 0.35),
    ("1597-2594", 0.16), ("2594+", 0.11),
]

# The source script names this campaign '… [new cpc structure]', carrying no [label:]
# token at all. Every identity test in this module keys on that token: the pause
# fallback (find_pausable_campaigns source b), _match_existing_campaign, and the label
# shown in Campaigns created. A campaign without one is invisible to all three — the
# way Emob.nl kept five ENABLED campaigns after leaving GSD. So it gets a real token.
#
# '[label:cpc]' (Joep, 2026-08-17). It cannot collide with any other token in
# PAUSE_LABELS: the tokens are matched WITH their closing bracket, so '[label:c]' is not
# a substring of '[label:cpc]' (after '[label:c' comes 'p', not ']') and vice versa.
CPC_BUCKETS_LABEL = "cpc"
LABELS_CPC_BUCKETS = [CPC_BUCKETS_LABEL]

# The listing tree partitions on custom_label_4, NOT custom_label_0. custom_label_0
# holds the score (A/B/C/No data/No EAN); custom_label_4 holds the price bucket.
CPC_BUCKET_ATTRIBUTE_INDEX = "INDEX4"

# The MODEL a campaign belongs to, read off its [label:X] token — the Model column in
# the UI. Two values only, CPC and CPR (Joep, 2026-08-17): the column answers "which
# commercial model is this shop on", not "which structure was built". Both CPC
# structures therefore map to plain "CPC"; the campaign NAME still tells them apart
# ('[label:a,b]' / '[label:c,no_data,no_ean]' for the legacy pair, '[label:cpc]' for the
# price-bucket campaign), and _labels_for_shop() is what decides between them.
#
# Read off the campaign rather than the shop's current Redshift flag, so a shop whose
# model flipped still shows what its campaigns actually are. promo/tag_toppers campaigns
# come from other flows and have no GSD model; they map to None and render as '-'.
_MODEL_BY_LABEL: Dict[str, str] = {
    **{lbl: "CPR" for lbl in LABELS_CPR},
    **{lbl: "CPC" for lbl in LABELS_CPC},
    CPC_BUCKETS_LABEL: "CPC",
}

# Campaigns a switched-off shop must ALSO lose, though the create path never makes them.
# promo and tag_toppers campaigns are GSD campaigns by identity ([shop:N] [shop_id:N]
# [channel:directshopping]) but are built by other flows, so they carry neither a create
# label token nor GSD_SCRIPT — both pause tests rejected them and they kept spending after
# the shop left GSD (Joep, 2026-08-04). Note the tag_toppers flow omits [domein:XX] even
# on DE/BE campaigns, so the account — never the name — is what scopes a pause to a country.
PAUSE_EXTRA_LABELS = ["promo", "tag_toppers"]

# The pause path matches BOTH model vocabularies plus the extras, never just the labels
# belonging to this run's derived model. A shop's model can flip on the very day it goes
# off (is_wecantrack_shop / is_pixel_shop drop together with the GSD flag), and going dark
# must not depend on having derived the model right: Emob.nl kept five ENABLED
# [label:a…no_ean] campaigns because the run derived CPC and therefore looked only for
# [label:a,b] and [label:c,no_data,no_ean]. Over-matching here is safe — identity is
# already pinned by account + [shop_id:N] + shop-name variant + SHOPPING channel.
PAUSE_LABELS = LABELS_CPR + LABELS_CPC + LABELS_CPC_BUCKETS + PAUSE_EXTRA_LABELS

# A Redshift shop-change row is per-country: `kolom` is the GSD flag that flipped,
# so it names the ONE country to act on. A shop flagged for NL only must not
# create/pause BE/DE campaigns (e.g. Calcuso.com|NL -> NL only).
KOLOM_COUNTRY = {"is_gsd_nl_shop": "NL", "is_gsd_be_shop": "BE", "is_gsd_de_shop": "DE"}

# --- Google Sheets run-logging ---------------------------------------------
# Mirrors the original create GSD-campaigns.py: each real run appends one row per
# processed shop to the "campaigns_created" tab of the "Data: Direct Shopping"
# sheet. Best-effort — never fails a run.
LOG_SPREADSHEET_ID = os.environ.get(
    "GSD_LOG_SPREADSHEET_ID", "1m4k8kxhfU7oLIAH3DJOyYx_PKSv4luPyX97j45Wa6s4"
)
LOG_WORKSHEET = os.environ.get("GSD_LOG_WORKSHEET", "campaigns_created")
# The sheet is shared with the dedicated sheets service account
# (gsd-campaign-creator@cla-campaign-creation) — NOT the Content-API accounts in
# backend/service_accounts/. Kept as a separate file/env so it doesn't disturb
# the MC service-account auto-detect (_get_content_service).
SHEETS_SA_FILE = os.environ.get(
    "GSD_SHEETS_SERVICE_ACCOUNT_FILE",
    r"C:\Users\l.davidowski\Downloads\dm_dashboard\gsd-campaign-creation.json",
)
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Cooperative cancel for an in-flight run_gsd_script — checked between shops, so
# a cancel stops further creates/pauses (already-processed shops stay done).
_run_cancel = {"cancel": False}

# Per-shop progress of an in-flight run_gsd_script, polled by the frontend bar.
_run_progress = {"current": 0, "total": 0, "running": False}


def cancel_run() -> None:
    """Request the active GSD run to stop at the next shop boundary."""
    _run_cancel["cancel"] = True


def get_run_progress() -> Dict[str, Any]:
    return dict(_run_progress)


# --- One run at a time, across processes ------------------------------------
#
# On 2026-09-01 two runs went through the SAME 18-shop change list at the same time and
# tore each other apart. `pa.jvs_gsd_campaign_created` recorded two 6-shop batches 21
# seconds apart (10:37:01 and 10:37:22) — that table is written once per run, so two
# batches is two runs — and every symptom of the day follows from it:
#   * DUPLICATE_CAMPAIGN_NAME: run B built a name run A had created seconds earlier, after
#     B had already snapshotted its candidates (_fetch_shop_campaign_candidates).
#   * "The request conflicted with existing data" (CONCURRENT_MODIFICATION): both runs
#     mutating one resource at once.
#   * "Listing group cannot be added ... already exists" + "referenced ... was not found":
#     both runs building a tree in the same ad group.
#   * Two Merchant Center sub-accounts per shop (Bouwlampkoning.nl, Vergewallet.nl):
#     _get_or_create_mc_account is a check-then-create and both checks said "absent".
#   * Duplicate rows in pa.mc_ids_efficy (fixed separately in push_mc_ids_to_redshift).
# The globals above cannot prevent any of that: they are per-process, so they see one
# run even when two are live, one shared progress bar counts for both, and cancel_run()
# stops BOTH. Worse, the Activity Log entry is written by the BROWSER, so the second run
# left no trace at all — which is why the day looks like it had one run, not two.
#
# The lock therefore lives in the shared PostgreSQL DB, not in this process: the two runs
# that collided may well have been two backends (WSL :8003 and prod :3003) pointed at the
# same Google Ads accounts, and an in-process flag is blind to that. It is SESSION-scoped
# (pg_try_advisory_lock on a dedicated connection held for the run) rather than
# transaction-scoped, because a run is minutes long and does its own Redshift/Postgres
# transactions in between. Held by the connection, so a crashed or killed run releases it
# — there is no stale lock to clear by hand.
GSD_RUN_LOCK_KEY = 0x67736472            # "gsdr"; 32-bit so pg_locks.objid holds it whole


class GsdRunInProgress(RuntimeError):
    """Raised instead of starting a second concurrent GSD run. The router turns this
    into HTTP 409 so the caller is told, rather than silently racing the run in
    progress."""


def _run_lock_identity() -> str:
    """Who holds the lock, for the message the second caller gets.

    Hostname first: the case worth naming is two MACHINES (a WSL :8003 and prod
    win-htz-006:3003) pointed at the same Google Ads accounts. No port — uvicorn takes it
    as a flag, not an env var, so anything read here would be a guess.
    """
    import socket
    return (f"{socket.gethostname()} pid {os.getpid()} "
            f"gestart {datetime.now().strftime('%H:%M:%S')}")


@contextmanager
def _session_lock_connection():
    """A pooled connection fit to hold a SESSION-level advisory lock for minutes.

    autocommit, so a run that takes 20 minutes does not sit `idle in transaction` on a
    database we share with n8n (that blocks VACUUM cleanup for as long as it lasts). Any
    session GUC set on it is reset on the way out, because the connection goes back into
    the pool for someone else — leaking `lock_timeout` in particular would make an
    unrelated query start failing.
    """
    conn = get_db_connection()
    previous_autocommit = conn.autocommit
    try:
        # A pooled connection can come back with a transaction still open, and psycopg2
        # refuses to switch autocommit on one ("set_session cannot be used inside a
        # transaction"). Ending it first is safe: nothing of ours is in it.
        try:
            conn.rollback()
        except Exception:
            pass
        conn.autocommit = True
        yield conn
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("RESET application_name")
                cur.execute("RESET lock_timeout")
        except Exception:
            pass
        try:
            conn.autocommit = previous_autocommit
        except Exception:
            pass
        return_db_connection(conn)


@contextmanager
def _gsd_run_lock():
    """Hold the GSD run lock for the duration of a run, or raise GsdRunInProgress.

    Best-effort in one direction only: if PostgreSQL is unreachable the run proceeds
    UNLOCKED (a database hiccup must not block the day's campaigns) and says so in the
    log. It never proceeds when the lock is genuinely held by someone else.
    """
    with _session_lock_connection() as conn:
        holding = False
        try:
            try:
                with conn.cursor() as cur:
                    # application_name is how the *other* process learns who is running.
                    cur.execute("SET application_name = %s", (f"gsd_run {_run_lock_identity()}",))
                    cur.execute("SELECT pg_try_advisory_lock(%s) AS got", (GSD_RUN_LOCK_KEY,))
                    holding = bool(cur.fetchone()["got"])
                if not holding:
                    raise GsdRunInProgress(
                        f"Er loopt al een GSD-run: {_lock_holder_description(conn)}. "
                        "Wacht tot die klaar is (of gebruik Cancel) voor je opnieuw start — "
                        "twee runs tegelijk maken dubbele campagnes en dubbele MC-accounts."
                    )
            except GsdRunInProgress:
                raise
            except Exception as ex:
                logger.warning("GSD run lock unavailable (%s) — running UNLOCKED; a second "
                               "concurrent run is not blocked right now", ex)
            yield holding
        finally:
            if holding:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (GSD_RUN_LOCK_KEY,))
                except Exception as ex:
                    # Not fatal: the lock dies with the connection, and the connection is
                    # closed or reset right after this.
                    logger.warning("Could not release the GSD run lock explicitly: %s", ex)


def _lock_holder_description(conn) -> str:
    """Describe the backend currently holding the run lock, from pg_locks."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.application_name
                FROM pg_locks l
                JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE l.locktype = 'advisory' AND l.objid = %s AND l.granted
                  AND a.pid <> pg_backend_pid()
                LIMIT 1
                """,
                (GSD_RUN_LOCK_KEY,),
            )
            row = cur.fetchone()
        if row and row.get("application_name"):
            return str(row["application_name"]).replace("gsd_run ", "")
        return "een andere sessie"
    except Exception:
        return "een andere sessie"


# Last Google Ads error captured by a create/enable helper, so
# _create_campaigns_for_shop can surface the real reason (not just a code) in
# the run result instead of a bare "—".
_last_gads_error = {"msg": None}


def _gads_err(ex) -> str:
    """Concise message from a GoogleAdsException (joins the per-error messages)."""
    try:
        msgs = [e.message for e in ex.failure.errors if e.message]
        joined = "; ".join(msgs)
        return (joined or str(ex))[:400]
    except Exception:
        return str(ex)[:400]


# Merchant Center (Content API) errors are plain HttpErrors, not
# GoogleAdsExceptions. Stash the real reason so a Merchant-Center failure
# surfaces in the run result instead of a bare "failed_to_get_or_create_mc_account".
_last_mc_error = {"msg": None}


# Which service-account key the Content API is actually using, filled in by
# _get_mc_service(). Purely for error messages: "the caller does not have access" is
# useless without knowing WHICH caller.
_mc_service_account = {"file": None, "email": None}


def _mc_err(ex) -> str:
    """Concise message from a Content API HttpError (prefers the API reason+message).

    Access errors get the service-account email appended, because the API's own wording
    ("The caller does not have access to the accounts: [5342886105]") never says who the
    caller is — which cost a DE run a confusing error on 2026-07-31.
    """
    out = str(ex)[:400]
    try:
        details = ex.error_details  # googleapiclient HttpError, list of dicts
        if details:
            d = details[0]
            reason = d.get("reason", "")
            msg = d.get("message", "")
            out = (f"{reason}: {msg}" if reason else msg or str(ex))[:400]
    except Exception:
        pass
    low = out.lower()
    if _mc_service_account.get("email") and ("access" in low or "denied" in low or "401" in low):
        out = f"{out} [caller: {_mc_service_account['email']}]"
    return out[:600]

TRACKING_TEMPLATES = {
    "NL": (
        "https://www.beslist.nl/outclick/redirect?aff_id=900"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
    "BE": (
        "https://www.beslist.be/outclick/redirect?aff_id=901"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
    "DE": (
        "https://www.shopcaddy.de/outclick/redirect?aff_id=910"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
}

PRICE_BUCKETS = [
    "0-8", "8-13", "13-21", "21-34", "34-55", "55-89",
    "89-144", "144-233", "233-377", "377-610", "610-987",
    "987-1597", "1597-2584", "2584-Onbeperkt",
]

BIDS_AB = [0.12, 0.12, 0.15, 0.17, 0.19, 0.20, 0.23, 0.25, 0.31, 0.35, 0.40, 0.41, 0.35, 0.25]
BIDS_C = [0.08, 0.09, 0.11, 0.12, 0.14, 0.14, 0.17, 0.18, 0.22, 0.26, 0.29, 0.29, 0.26, 0.18]

GEO_TARGETS = {"NL": "2528", "BE": "2056", "DE": "2276"}

# One regex PER FIELD, deliberately not one combined all-or-nothing pattern. The
# old `\[shop:…\].*?\[shop_id:(\d+)\].*?\[label:…\]` returned shop_name = shop_id =
# label = None for any name that deviated even slightly — e.g.
# "[sWalkworld.nl] [shop_id:664923] … [label:a]" (mangled shop tag) or
# "[shop:Petgamma.com] [shop_id:655526] … [merk:royal_canin]" (no [label:]) — which
# dropped those campaigns out of every shop_id-keyed join, including the
# Campaigns-created Date column, whose date was in the table all along.
SHOP_NAME_REGEX = re.compile(r"\[shop:([^\]]+)\]")
SHOP_ID_REGEX = re.compile(r"\[shop_id:(\d+)\]")
LABEL_REGEX = re.compile(r"\[label:([^\]]+)\]")
COUNTRY_REGEX = re.compile(r"\[domein:(\w+)\]")

# Default daily budget in micros. 10 EUR = 10_000_000 micros, matching the
# original create GSD-campaigns.py (campaign_budget.amount_micros = 10000000).
DEFAULT_BUDGET_MICROS = 10_000_000

# Content API scopes for Merchant Center
MC_SCOPES = ["https://www.googleapis.com/auth/content"]

# ---------------------------------------------------------------------------
# Temporary ID counter for mutate operations
# ---------------------------------------------------------------------------

_next_temp_id = 0


def next_id() -> int:
    """Return the next temporary negative ID for resource creation."""
    global _next_temp_id
    _next_temp_id -= 1
    return _next_temp_id


def reset_temp_ids() -> None:
    """Reset the temporary ID counter (call before each batch of operations)."""
    global _next_temp_id
    _next_temp_id = 0


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------


def _get_client() -> GoogleAdsClient:
    """Initialize Google Ads client from environment variables."""
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def _is_retryable_gads(ex: GoogleAdsException) -> bool:
    """
    True for transient Google Ads failures that are safe to retry — chiefly
    CONCURRENT_MODIFICATION ("Multiple requests were attempting to modify the
    same resource at once"), plus transient internal/quota errors. Detection is
    tolerant of proto-plus vs protobuf enum representations and falls back to the
    rendered message text.
    """
    try:
        for err in ex.failure.errors:
            code = err.error_code
            for family in ("database_error", "internal_error", "quota_error"):
                val = getattr(code, family, 0)
                name = getattr(val, "name", str(val))
                if name in ("CONCURRENT_MODIFICATION", "INTERNAL_ERROR",
                            "TRANSIENT_ERROR", "RESOURCE_EXHAUSTED",
                            "RESOURCE_TEMPORARILY_EXHAUSTED"):
                    return True
    except Exception:
        pass
    msg = str(ex)
    return "CONCURRENT_MODIFICATION" in msg or "modify the same resource" in msg


def _mutate_with_retry(what: str, fn, retries: int = 5, base_delay: float = 0.5):
    """
    Call a Google Ads mutate (a zero-arg callable) and retry transient failures
    with exponential backoff (0.5s, 1s, 2s, 4s, 8s). Non-retryable errors and a
    final exhausted attempt re-raise, so existing per-call error handling still
    sees the real GoogleAdsException.
    """
    for attempt in range(retries):
        try:
            return fn()
        except GoogleAdsException as ex:
            if attempt < retries - 1 and _is_retryable_gads(ex):
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Transient Ads error on %s (attempt %d/%d); retrying in %.1fs",
                    what, attempt + 1, retries, delay,
                )
                time.sleep(delay)
                continue
            raise


# A freshly-created Merchant Center -> Google Ads link is eventually consistent:
# the MC-side link exists but Google Ads can briefly still report
# RESOURCE_NOT_FOUND on shopping_setting.merchant_id when the campaign is created.
# Give the campaign create its own patient retry so the link can propagate within
# the same run (~2 min total) instead of failing the shop.
_MERCHANT_LINK_RETRY_DELAYS = (5, 10, 20, 30, 60)  # seconds

# After creating the MC-side AdsLink, the ProductLinkInvitation is not visible on
# the Ads side immediately. Poll for it so the link is actually accepted before
# campaign creation starts. Without this, each campaign's own RESOURCE_NOT_FOUND
# retry has to absorb the full propagation delay — and on 2026-08-31 that wasn't
# enough: 5 labels × 5 retries (~11 min) still failed for three shops whose MC
# accounts were brand-new.  Accepting the invitation here fixes the root cause
# (no active link) instead of retrying the symptom (campaign create fails).
_INVITATION_ACCEPT_DELAYS = (5, 10, 15, 15, 20, 20, 20, 20)  # ~125s total


def _is_merchant_link_not_ready(ex: GoogleAdsException) -> bool:
    """
    True when a campaign create fails with RESOURCE_NOT_FOUND on the shopping
    merchant_id — i.e. the MC->Ads link hasn't propagated yet. Deliberately
    narrow (merchant_id / shopping_setting only) so we don't swallow other
    genuinely-missing resources.
    """
    msg = str(ex)
    if "RESOURCE_NOT_FOUND" not in msg:
        return False
    return "merchant_id" in msg or "shopping_setting" in msg


def _create_campaign_with_retry(fn):
    """
    Run the campaign-create mutate (a zero-arg callable) with retries for BOTH
    transient CONCURRENT_MODIFICATION (short exponential backoff) and
    merchant-link eventual-consistency RESOURCE_NOT_FOUND (patient backoff, the
    _MERCHANT_LINK_RETRY_DELAYS schedule). Scoped to campaign creation only, so
    RESOURCE_NOT_FOUND is never treated as retryable elsewhere.

    Retrying just this mutate is safe: mutate_campaigns is atomic (nothing is
    created on failure) and the budget created earlier in the flow is reused, so
    there are no duplicates.
    """
    transient_attempt = 0
    link_attempt = 0
    while True:
        try:
            return fn()
        except GoogleAdsException as ex:
            if _is_merchant_link_not_ready(ex) and link_attempt < len(_MERCHANT_LINK_RETRY_DELAYS):
                delay = _MERCHANT_LINK_RETRY_DELAYS[link_attempt]
                link_attempt += 1
                logger.warning(
                    "Campaign create hit RESOURCE_NOT_FOUND on merchant_id "
                    "(MC link still propagating); retry %d/%d in %ds",
                    link_attempt, len(_MERCHANT_LINK_RETRY_DELAYS), delay,
                )
                time.sleep(delay)
                continue
            if _is_retryable_gads(ex) and transient_attempt < 4:
                delay = 0.5 * (2 ** transient_attempt)
                transient_attempt += 1
                logger.warning(
                    "Transient Ads error on create campaign; retrying in %.1fs", delay,
                )
                time.sleep(delay)
                continue
            raise


def _is_parent_not_ready(ex: GoogleAdsException) -> bool:
    """
    True when a mutate that references a just-created ad group fails with
    RESOURCE_NOT_FOUND — i.e. the ad group / campaign hasn't propagated yet.
    A product-ad or listing-group mutate references only the ad group we created
    moments earlier, so RESOURCE_NOT_FOUND here is eventual consistency, not a
    genuinely missing resource; it is safe (and idempotent — the mutate is atomic
    and creates nothing on failure) to retry patiently.
    """
    return "RESOURCE_NOT_FOUND" in str(ex)


def _create_child_with_retry(what: str, fn):
    """
    Run a mutate that depends on a freshly-created ad group (product ad, listing
    tree) with retries for BOTH transient errors (short exponential backoff) and
    ad-group eventual-consistency RESOURCE_NOT_FOUND (patient backoff, the
    _MERCHANT_LINK_RETRY_DELAYS schedule up to ~2 min).

    This is the root-cause fix for the observed failure where a product ad was
    rejected seconds after its ad group was created — _mutate_with_retry's ~15s
    budget wasn't enough for the ad group to become visible, leaving an empty
    ad group that needed a second run to repair.
    """
    transient_attempt = 0
    not_ready_attempt = 0
    while True:
        try:
            return fn()
        except GoogleAdsException as ex:
            if _is_parent_not_ready(ex) and not_ready_attempt < len(_MERCHANT_LINK_RETRY_DELAYS):
                delay = _MERCHANT_LINK_RETRY_DELAYS[not_ready_attempt]
                not_ready_attempt += 1
                logger.warning(
                    "%s hit RESOURCE_NOT_FOUND (ad group still propagating); "
                    "retry %d/%d in %ds",
                    what, not_ready_attempt, len(_MERCHANT_LINK_RETRY_DELAYS), delay,
                )
                time.sleep(delay)
                continue
            if _is_retryable_gads(ex) and transient_attempt < 4:
                delay = 0.5 * (2 ** transient_attempt)
                transient_attempt += 1
                logger.warning("Transient Ads error on %s; retrying in %.1fs", what, delay)
                time.sleep(delay)
                continue
            raise


def _get_redshift_connection():
    """Create a Redshift connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("REDSHIFT_HOST", ""),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        dbname=os.environ.get("REDSHIFT_DB", ""),
        user=os.environ.get("REDSHIFT_USER", ""),
        password=os.environ.get("REDSHIFT_PASSWORD", ""),
    )


def mc_upsert_plan(rows: List[tuple], state: Optional[Dict[tuple, tuple]] = None) -> Dict[str, list]:
    """Split incoming rows into {insert, update, repair, unchanged} against the table's state.

    ``pa.mc_ids_efficy`` is a STATE table (Joep, 2026-08-05): one row per
    (shop_id, domain) holding that shop's current Merchant Center id.
      * key absent                        -> INSERT
      * key present, new mc               -> UPDATE the mc id AND the date
      * key present, same mc, ONE row     -> NOTHING. Not re-inserted, and the date is left
                                             alone, so the stored date stays the original
                                             creation date.
      * key present, same mc, >1 row      -> REPAIR: rewrite the key to a single row, keeping
                                             the stored (earliest) date. The value is already
                                             right, the row COUNT is not.

    That third rule is why the table had 63 surplus rows: the old code was a bare INSERT,
    so every run where get-or-create reported "created" appended another row —
    Cameranu.nl was logged 7 times, 4 of them on one day. Measured against the
    authoritative creation-date log (pa.jvs_gsd_campaign_created), the EARLIEST logged
    date matched in 39 of 49 duplicated groups and the latest in **zero**, which is
    exactly what "insert once, never touch again" produces.

    The fourth (``repair``) rule exists because "do nothing" is the wrong answer for a key
    that ALREADY holds duplicates: it made the table's own write path unable to heal damage
    it could see (2026-09-01, three keys duplicated by two overlapping runs — Farmaline.be
    and Casebump.nl twice with the same mc id, so no UPDATE would ever be planned and the
    surplus row was permanent until someone deleted it by hand).

    Pass ``state`` to plan without touching Redshift (used by the reconcile dry run).
    """
    plan: Dict[str, list] = {"insert": [], "update": [], "repair": [], "unchanged": []}
    if not rows:
        return plan
    if state is None:
        state = current_mc_state([r[1] for r in rows])
    # Later rows for the same key win, so one call cannot both insert and update a key.
    staged: Dict[tuple, tuple] = {}
    for r in rows:
        shop_name, shop_id, mc, domain, date = r
        if shop_id is None or mc is None or not domain:
            continue
        staged[(int(shop_id), str(domain).upper())] = (shop_name, int(shop_id), int(mc),
                                                       str(domain).upper(), date)
    for key, row in staged.items():
        cur_mc, cur_date, _cur_name, n_rows = state.get(key) or (None, None, None, 0)
        if cur_mc is None:
            plan["insert"].append(row)
        elif int(cur_mc) == row[2]:
            if (n_rows or 1) > 1:
                # Same value, wrong row count. Keep the stored date so collapsing the
                # duplicates does not restamp the shop's creation date with today's.
                plan["repair"].append((row[0], row[1], row[2], row[3], cur_date or row[4]))
            else:
                plan["unchanged"].append(row)
        else:
            plan["update"].append(row)
    return plan


def current_mc_state(shop_ids: List[int]) -> Dict[tuple, tuple]:
    """{(shop_id, domain): (mc_created, date, shop_name, n_rows)} for these shops.

    ``n_rows`` is how many rows that key currently has — >1 means legacy duplicates the
    upsert will collapse the next time it writes that key.
    """
    out: Dict[tuple, tuple] = {}
    ids = sorted({int(s) for s in shop_ids if s is not None})
    if not ids:
        return out
    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT shop_id, UPPER(domain), MIN(date), COUNT(*)
                FROM pa.mc_ids_efficy
                WHERE shop_id IN (%s)
                GROUP BY shop_id, UPPER(domain)
                """ % ",".join(str(i) for i in ids)
            )
            groups = cur.fetchall()
            for shop_id, domain, _dmin, n in groups:
                out[(int(shop_id), domain)] = (None, None, None, int(n))
            # The mc id of the earliest row is the state, matching the dedup rule.
            cur.execute(
                """
                SELECT shop_id, UPPER(domain), mc_created, date, shop_name
                FROM pa.mc_ids_efficy
                WHERE shop_id IN (%s)
                ORDER BY shop_id, UPPER(domain), date ASC, mc_created ASC
                """ % ",".join(str(i) for i in ids)
            )
            seen = set()
            for shop_id, domain, mc, date, shop_name in cur.fetchall():
                key = (int(shop_id), domain)
                if key in seen:
                    continue
                seen.add(key)
                n = out.get(key, (None, None, None, 1))[3]
                out[key] = (int(mc) if mc is not None else None, date, shop_name, n)
    finally:
        conn.close()
    return out


# Cross-process mutex for the pa.mc_ids_efficy read-then-write. Redshift has no unique
# constraint and no row id, so the table's one-row-per-key invariant can only be held by
# the writer — and a read-then-write is only safe if no second writer slips in between.
# Arbitrary constant ("mcid" in hex), only has to be unique among this DB's advisory locks.
MC_IDS_LOCK_KEY = 0x6D636964
MC_IDS_LOCK_TIMEOUT_MS = 900_000     # 15 min: longer than a slow push, short of hanging a run


@contextmanager
def _mc_ids_write_lock():
    """Serialise pa.mc_ids_efficy writers across processes. Yields True when held.

    Two GSD runs overlapped on 2026-09-01 and duplicated three keys. Each read the state on
    its own connection, each saw the key as absent, each inserted. The window was wide open
    because the write itself was slow — one transaction that day spanned 22 minutes
    (09:27:04 -> 09:49:18) doing a DELETE + INSERT per key, and a concurrent reader can only
    ever see COMMITTED rows, so for those 22 minutes the table looked empty for those keys.

    The lock lives in the shared PostgreSQL DB (Redshift has no advisory locks). It is
    transaction-scoped, so it is released by the commit/rollback below AND by the connection
    dying — a crashed run cannot leave it held. Best-effort by design: if PostgreSQL is
    unreachable the push still proceeds unlocked, because the delete-before-insert below is
    what actually guarantees the invariant. The lock only stops two writers from both
    doing that work and one of them wasting a Redshift transaction.
    """
    conn = None
    locked = False
    # Only the ACQUISITION is guarded. Wrapping the yield too would feed any exception
    # raised by the caller's body into this except clause, and a second yield from a
    # @contextmanager generator turns it into "generator didn't stop after throw()" —
    # i.e. the real Redshift error would be replaced by a meaningless one.
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = %s", (MC_IDS_LOCK_TIMEOUT_MS,))
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (MC_IDS_LOCK_KEY,))
        locked = True
    except Exception as ex:
        logger.warning("pa.mc_ids_efficy write lock unavailable (%s) — writing unlocked; "
                       "the delete-before-insert still holds the invariant", ex)
        if conn is not None:
            _release_pg(conn)
            conn = None
    try:
        yield locked
    finally:
        if conn is not None:
            _release_pg(conn)


def _release_pg(conn) -> None:
    """Roll back (releasing any xact-scoped advisory lock) and return to the pool."""
    try:
        conn.rollback()
    except Exception:
        pass
    try:
        return_db_connection(conn)
    except Exception:
        pass


def push_mc_ids_to_redshift(rows: List[tuple]) -> Dict[str, Any]:
    """
    Keep pa.mc_ids_efficy in step with the Merchant Center ids GSD created.

    ``rows`` is a list of (shop_name, shop_id, mc_created, domain, date) tuples, where
    ``domain`` holds the country (NL/BE/DE) and ``date`` is YYYYMMDD.

    Upsert on (shop_id, domain) — see mc_upsert_plan for the rule and why. A same-mc-id
    push against a single-row key is a no-op, so re-running a day cannot grow the table.

    THE WRITE CANNOT CREATE A DUPLICATE. Every key it touches is DELETEd first and then
    inserted exactly once, in one transaction — inserts included, not just updates. That
    matters because "insert" is a belief about the table taken from an earlier read, and
    that belief is exactly what was wrong on 2026-09-01: two overlapping runs each read
    "key absent", each ran a bare INSERT, and three keys ended up with two rows
    (Farmaline.be 643423 BE, Casebump.nl 652006 NL, Vergewallet.nl 666787 NL). Deleting the
    key regardless of what the plan believed makes the write idempotent and self-healing
    instead of trusting a read that may be stale, racing, or plain wrong.

    It is also now TWO statements instead of two per key. The old per-key DELETE+INSERT loop
    was what made the race window minutes wide (see _mc_ids_write_lock).

    Best-effort: exceptions are caught and returned in the result dict so a Redshift
    hiccup never fails the GSD run.
    """
    result: Dict[str, Any] = {"inserted": 0, "updated": 0, "repaired": 0, "unchanged": 0,
                              "duplicates_remaining": 0, "locked": None, "error": None}
    if not rows:
        return result
    write_rows: List[tuple] = []
    try:
        with _mc_ids_write_lock() as locked:
            result["locked"] = locked
            conn = _get_redshift_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS pa.mc_ids_efficy (
                            shop_name TEXT,
                            shop_id BIGINT,
                            mc_created BIGINT,
                            domain TEXT,
                            date VARCHAR(255)
                        );
                        """
                    )
                    conn.commit()
                plan = mc_upsert_plan(rows)
                result["unchanged"] = len(plan["unchanged"])
                write_rows = plan["insert"] + plan["update"] + plan["repair"]
                if write_rows:
                    # One key per row (mc_upsert_plan staged by key), so the delete list and
                    # the insert list are the same set of keys — one row out, one row in.
                    keys = tuple(f"{r[1]}|{r[3]}" for r in write_rows)
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM pa.mc_ids_efficy "
                            "WHERE (shop_id || '|' || UPPER(domain)) IN %s",
                            (keys,),
                        )
                        execute_values(
                            cur,
                            """
                            INSERT INTO pa.mc_ids_efficy
                                (shop_name, shop_id, mc_created, domain, date)
                            VALUES %s
                            """,
                            write_rows,
                        )
                        conn.commit()
                    result["inserted"] = len(plan["insert"])
                    result["updated"] = len(plan["update"])
                    result["repaired"] = len(plan["repair"])
            finally:
                conn.close()
            # Tripwire, inside the lock: re-read the keys we just wrote and assert the
            # invariant. A silent duplicate is what cost two dedup rounds (2026-08-05,
            # 2026-09-01); one SELECT makes the next regression loud instead of invisible.
            if write_rows:
                after = current_mc_state([r[1] for r in write_rows])
                written = {(r[1], r[3]) for r in write_rows}
                dupes = {k: v[3] for k, v in after.items() if k in written and (v[3] or 1) > 1}
                result["duplicates_remaining"] = len(dupes)
                if dupes:
                    logger.error("pa.mc_ids_efficy INVARIANT BROKEN after write — keys still "
                                 "holding >1 row: %s", dupes)
        logger.info(
            "pa.mc_ids_efficy: %d inserted, %d updated, %d repaired, %d unchanged "
            "(of %d pushed, lock=%s)",
            result["inserted"], result["updated"], result["repaired"], result["unchanged"],
            len(rows), result["locked"],
        )
    except Exception as ex:
        logger.error("Failed to push MC ids to pa.mc_ids_efficy: %s", ex)
        result["error"] = str(ex)
    return result


# ---------------------------------------------------------------------------
# Per-shop creation dates (n8n-vector-db PostgreSQL)
# ---------------------------------------------------------------------------
# A shop's GSD label campaigns (a/b/c/...) in a country are all created in the
# same run, so creation date is naturally per (shop_id, country) — the same
# grain the source spreadsheet uses. The Campaigns-created "Date" column joins
# each campaign to this table by (shop_id, country).


def _created_key(shop_id: Any, country: Any) -> Optional[str]:
    """Normalise (shop_id, country) into the join key used everywhere, or None."""
    if shop_id in (None, "") or not country:
        return None
    try:
        return f"{int(shop_id)}|{str(country).upper()}"
    except (TypeError, ValueError):
        return None


def ensure_campaign_created_table() -> None:
    """Create the per-(shop, country) creation-date table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {CAMPAIGN_CREATED_TABLE} (
                    shop_id      BIGINT      NOT NULL,
                    country      VARCHAR(4)  NOT NULL,
                    created_date DATE        NOT NULL,
                    shop_name    TEXT,
                    recorded_at  TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (shop_id, country)
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def upsert_created_dates(rows: List[tuple]) -> Dict[str, Any]:
    """
    Insert (shop_id, country, created_date, shop_name) rows into
    CAMPAIGN_CREATED_TABLE. ON CONFLICT DO NOTHING so the FIRST recorded date
    for a (shop, country) wins — a later run never overwrites the original
    creation date. Returns {inserted, error}; best-effort (never raises).
    """
    result: Dict[str, Any] = {"inserted": 0, "error": None}
    if not rows:
        return result
    try:
        ensure_campaign_created_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # RETURNING + fetch=True gives an accurate insert count: with
                # execute_values' internal paging, cur.rowcount would only reflect
                # the last page. DO NOTHING means only real inserts are returned.
                returned = execute_values(
                    cur,
                    f"""
                    INSERT INTO {CAMPAIGN_CREATED_TABLE}
                        (shop_id, country, created_date, shop_name)
                    VALUES %s
                    ON CONFLICT (shop_id, country) DO NOTHING
                    RETURNING 1
                    """,
                    rows,
                    fetch=True,
                )
                result["inserted"] = len(returned)
            conn.commit()
        finally:
            return_db_connection(conn)
    except Exception as ex:
        logger.error("Failed to upsert created dates: %s", ex)
        result["error"] = str(ex)
    return result


def record_created_campaigns(
    entries: List[Dict[str, Any]], created_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Persist the creation date of freshly created campaigns, deduped to one row
    per (shop_id, country). ``entries`` are the run's "created" result dicts
    (need shop_id + country). ``created_date`` is 'YYYY-MM-DD' (default: today).
    Best-effort — a logging miss must never fail a real run.
    """
    if created_date is None:
        created_date = datetime.now().strftime("%Y-%m-%d")
    seen: Dict[tuple, tuple] = {}
    dropped = 0
    for e in entries or []:
        # Second source for shop_id/country: the campaign name itself. The run
        # loop is supposed to attach both, and once did not — a missing key made
        # this function insert nothing at all, silently. Re-deriving from the name
        # means a caller can only lose a date if the name is unparseable too.
        parsed = _parse_campaign_name(e.get("campaign_name") or "")
        raw_id = e.get("shop_id") or parsed.get("shop_id")
        try:
            shop_id_int = int(raw_id) if raw_id else None
        except (TypeError, ValueError):
            shop_id_int = None
        country = (e.get("country") or parsed.get("country") or "").upper()
        if shop_id_int is None or not country:
            dropped += 1
            continue
        key = (shop_id_int, country)
        if key not in seen:
            seen[key] = (shop_id_int, country, created_date,
                         e.get("shop_name") or parsed.get("shop_name"))
    if dropped:
        # Loud, because the failure mode is invisible otherwise: the run reports
        # success, the table just never grows.
        logger.warning(
            "record_created_campaigns: %d/%d created entries had no usable "
            "(shop_id, country) and got NO creation date", dropped, len(entries or []),
        )
    result = upsert_created_dates(list(seen.values()))
    result["dropped"] = dropped
    return result


def get_created_dates() -> Dict[str, str]:
    """
    Return {"<shop_id>|<COUNTRY>": 'YYYY-MM-DD'} for every recorded (shop, country).
    Best-effort: returns {} on any error so the table still renders without dates.
    """
    try:
        ensure_campaign_created_table()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT shop_id, country, created_date FROM {CAMPAIGN_CREATED_TABLE}")
                out: Dict[str, str] = {}
                for r in cur.fetchall():
                    key = _created_key(r["shop_id"], r["country"])
                    if key and r.get("created_date"):
                        out[key] = r["created_date"].strftime("%Y-%m-%d")
                return out
        finally:
            return_db_connection(conn)
    except Exception as ex:
        logger.error("Failed to read created dates: %s", ex)
        return {}


def backfill_campaign_created_dates(days: int = 30, dry_run: bool = False) -> Dict[str, Any]:
    """
    Seed CAMPAIGN_CREATED_TABLE from the Google Ads change_event log — the CREATE
    event's change_date_time is the real creation date, deduped to the EARLIEST
    date per (shop_id, country). change_event retains only ~30 days, so this only
    recovers recent creations (the spreadsheet backfill covers the deep history);
    it's handy to fill the gap between the spreadsheet snapshot and go-live.
    Existing rows are never overwritten. dry_run=True reports without writing.
    """
    ensure_campaign_created_table()
    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    # change_event rejects a start date >30 days old (strict), so clamp to 29.
    days = min(days, 29)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    by_shop: Dict[tuple, tuple] = {}  # (shop_id, country) -> row, earliest date kept
    for cid in customer_ids:
        created_on: Dict[str, str] = {}
        try:
            for r in ga.search(customer_id=cid, query=f"""
                    SELECT change_event.campaign, change_event.change_date_time
                    FROM change_event
                    WHERE change_event.change_date_time >= '{cutoff} 00:00:00'
                      AND change_event.change_date_time <= '{now_str}'
                      AND change_event.change_resource_type = 'CAMPAIGN'
                      AND change_event.resource_change_operation = 'CREATE'
                    ORDER BY change_event.change_date_time DESC
                    LIMIT 10000"""):
                camp_id = r.change_event.campaign.rstrip("/").split("/")[-1]
                d = r.change_event.change_date_time[:10]
                if camp_id not in created_on or d < created_on[camp_id]:
                    created_on[camp_id] = d
        except GoogleAdsException as ex:
            logger.warning("Backfill change_event query failed for %s: %s", cid, ex)
            continue
        if not created_on:
            continue

        gsd_rn = ensure_campaign_label_exists(client, cid, SCRIPT_LABEL)
        try:
            for r in ga.search(customer_id=cid, query=f"""
                    SELECT campaign.id, campaign.name
                    FROM campaign
                    WHERE campaign.labels CONTAINS ANY ('{gsd_rn}')
                      AND campaign.status != 'REMOVED'"""):
                camp_id = str(r.campaign.id)
                if camp_id not in created_on:
                    continue
                parsed = _parse_campaign_name(r.campaign.name)
                try:
                    shop_id_int = int(parsed["shop_id"]) if parsed.get("shop_id") else None
                except (TypeError, ValueError):
                    shop_id_int = None
                country = (parsed.get("country") or "").upper()
                if shop_id_int is None or not country:
                    continue
                key = (shop_id_int, country)
                d = created_on[camp_id]
                if key not in by_shop or d < by_shop[key][2]:
                    by_shop[key] = (shop_id_int, country, d, parsed.get("shop_name"))
        except GoogleAdsException as ex:
            logger.warning("Backfill campaign query failed for %s: %s", cid, ex)
            continue

    rows = list(by_shop.values())
    result = {"found": len(rows), "inserted": 0, "dry_run": dry_run}
    if rows and not dry_run:
        result["inserted"] = upsert_created_dates(rows)["inserted"]
    logger.info("change_event backfill: %d found, %d inserted", result["found"], result["inserted"])
    return result


def _get_mc_service():
    """Build a Merchant Center Content API service using a service account.

    ONLY ONE of the keys in backend/service_accounts/ has Merchant Center access
    (measured 2026-07-31: acoustic-racer's beslist-index-checker@… reaches NL 5592708765,
    BE 5588879919 and DE 5342886105; the other three get 401 "The caller does not have
    access to the accounts" on all of them). The auto-detect used to take
    `os.listdir()[0]` — arbitrary directory order — so a machine without
    GSD_SERVICE_ACCOUNT_FILE set could silently pick a key with no access and fail
    mid-run with exactly that message. Sorted now, and the chosen file is logged.
    """
    sa_file = os.environ.get("GSD_SERVICE_ACCOUNT_FILE", "")
    if not sa_file:
        sa_dir = os.path.join(os.path.dirname(__file__), "service_accounts")
        if os.path.isdir(sa_dir):
            json_files = sorted(f for f in os.listdir(sa_dir) if f.endswith(".json"))
            if json_files:
                sa_file = os.path.join(sa_dir, json_files[0])
                logger.warning(
                    "GSD_SERVICE_ACCOUNT_FILE is not set; falling back to %s out of %d "
                    "key(s) in %s. Only one of them has Merchant Center access — set the "
                    "env var explicitly.", json_files[0], len(json_files), sa_dir,
                )
    if not sa_file or not os.path.exists(sa_file):
        raise RuntimeError(
            "Service account file not found. Set GSD_SERVICE_ACCOUNT_FILE env var "
            "or place a .json key file in backend/service_accounts/"
        )
    credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=MC_SCOPES)
    _mc_service_account["file"] = sa_file
    _mc_service_account["email"] = getattr(credentials, "service_account_email", "") or ""
    return build("content", "v2.1", credentials=credentials, cache_discovery=False)


# ---------------------------------------------------------------------------
# Campaign name helpers
# ---------------------------------------------------------------------------


def _parse_campaign_name(name: str) -> Dict[str, Optional[str]]:
    """Extract shop_name, shop_id, label, country and model from a campaign name.

    ``model`` is derived from the label token via _MODEL_BY_LABEL — no extra API call,
    and it describes the campaign's own structure rather than the shop's current
    Redshift flag. None for a campaign whose label is missing or not one of ours.
    """
    result: Dict[str, Optional[str]] = {
        "shop_name": None,
        "shop_id": None,
        "label": None,
        "country": "NL",
        "model": None,
    }
    # Field by field: a missing/mangled shop tag must not cost us the shop_id.
    for key, rx in (("shop_name", SHOP_NAME_REGEX), ("shop_id", SHOP_ID_REGEX),
                    ("label", LABEL_REGEX)):
        m = rx.search(name)
        if m:
            result[key] = m.group(1)
    cm = COUNTRY_REGEX.search(name)
    if cm:
        result["country"] = cm.group(1).upper()
    result["model"] = _MODEL_BY_LABEL.get((result["label"] or "").strip().lower())
    return result


def _build_campaign_name(
    country: str, shop_name: str, shop_id: int, label: str
) -> str:
    """Build a campaign name following the GSD naming convention."""
    base = f"[shop:{shop_name}] [shop_id:{shop_id}] [channel:directshopping] [label:{label}]"
    if country.upper() != "NL":
        base = f"[domein:{country.upper()}] {base}"
    return base


def _shop_name_variants(shop_name: Optional[str]) -> List[str]:
    """The shop name as Redshift gives it, plus the same name without a trailing
    '|<addition>' marker.

    Redshift hands us names like 'Hbm-machines.com|NL' or 'Woodselections.com|DE' — the
    '|XX' disambiguates one brand selling on several locales. It can appear (or change)
    AFTER campaigns were created under the bare name, and every lookup in this module
    matches the '[shop:NAME]' token EXACTLY. So the bare campaign reads as absent and
    the run creates a near-identical second set:
        [shop:Hbm-machines.com]    [shop_id:207860] … [label:a]   <- already there
        [shop:Hbm-machines.com|NL] [shop_id:207860] … [label:a]   <- created anyway
    and, on the way out, a shop switched off keeps its bare-name campaigns ENABLED
    because the pause query never sees them. Both directions are fixed by looking for
    every variant instead of only the current spelling (Joep, 2026-07-31).

    NOT covered on purpose: the reverse spelling drift (Redshift drops a suffix the
    campaign still carries) and the pre-2025 naming generation
    ('[label_test] … [branche:H&L] [label:a]'), which needs a decision about whether an
    old paused test campaign should count as "this shop already has campaigns".
    """
    name = (shop_name or "").strip()
    variants = [name] if name else []
    if "|" in name:
        bare = name.split("|", 1)[0].strip()
        if bare and bare not in variants:
            variants.append(bare)
    return variants




# --- Identity of a GSD campaign, independent of naming generation ----------
#
# Exact-name matching only ever recognised campaigns built by the CURRENT naming
# convention. Everything older carries extra tokens — `[label_test]`, `[branche:H&L]`,
# `[macro]`, `[limit]`, `[OUD]`, `#4` — so the run read them as absent and created a
# second set next to them (Toolstation.nl on 2026-07-31: 5 canonical campaigns created
# beside 15 pre-existing `[label_test] … [branche:H&L]` ones).
#
# Joep's rule (2026-07-31): a campaign is THE SAME campaign when shop name, shop_id and
# custom label match, ignoring every other token — except macro/micro variants, which
# are a separate test structure and must never be adopted.
#
# Two guards of my own, both reversible by deleting one line:
#   * `[OUD]` (Dutch for "old") marks a deliberately retired campaign. Adopting one
#     would make the run rebuild something a human archived on purpose.
#   * a `[domein:CC]` mismatch is rejected, so an NL run can never adopt a BE campaign
#     that happens to sit in the same customer id (NL_CPR and NL_CPC share 7938980174).
# AUDIT LOW — `[^\]:]` not `[^\]]`: the marker is only ever a BARE tag, so a KEYED tag
# can no longer trip it. The old pattern scanned tag CONTENTS, so a shop literally named
# "Macro.nl" would arrive as `[shop:Macro.nl]`, read as a macro variant, and be
# permanently unadoptable — the run would create a second campaign beside it every day.
# Verified against all 2.856 live campaigns (2026-08-05): 120 carry a macro/micro tag and
# every one of them is bare — `[macro]` (60) or `[macro+micro]` (60). Zero keyed forms, so
# narrowing to bare tags loses no true positive; both patterns flag the same 120 names.
_MACRO_MICRO_RE = re.compile(r"\[[^\]:]*\b(?:macro|micro)\b[^\]:]*\]", re.I)
_RETIRED_RE = re.compile(r"\[\s*oud\s*\]", re.I)
_DOMEIN_RE = re.compile(r"\[domein:([^\]]+)\]", re.I)


def _fetch_shop_campaign_candidates(
    client: GoogleAdsClient, customer_id: str, shop_id: Any
) -> List[Dict[str, Any]]:
    """Non-REMOVED SHOPPING campaigns in this account whose name carries [shop_id:N].

    One query per (shop, account) — cheaper than the previous one-exact-name-query per
    label — and the same `[shop_id:{id}]` key the low-linkage service already trusts
    (gsd_ll_service._find_enabled_campaigns). SHOPPING-only so no Search/PMax campaign
    can ever be adopted.
    """
    try:
        shop_id_int = int(shop_id)
    except (TypeError, ValueError):
        return []
    ga = client.get_service("GoogleAdsService")
    pattern = _name_contains_regexp(f"[shop_id:{shop_id_int}]")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.resource_name,
               campaign.labels, campaign.bidding_strategy_type
        FROM campaign
        WHERE campaign.name REGEXP_MATCH '{pattern}'
          AND campaign.status != 'REMOVED'
          AND campaign.advertising_channel_type = 'SHOPPING'
    """
    out: List[Dict[str, Any]] = []
    try:
        for row in ga.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "status": row.campaign.status.name,
                "resource_name": row.campaign.resource_name,
                "labels": list(row.campaign.labels),
                "bidding_strategy_type": row.campaign.bidding_strategy_type.name,
            })
    except GoogleAdsException as ex:
        logger.error("Campaign candidate lookup failed (%s, shop_id=%s): %s",
                     customer_id, shop_id, ex)
        raise
    return out


def _match_existing_campaign(
    candidates: List[Dict[str, Any]], country: str, shop_name: str, shop_id: Any, label: str
) -> Optional[Dict[str, Any]]:
    """The campaign among `candidates` that IS this (shop, shop_id, label), or None.

    Ranking when several match: the canonical name first, then ENABLED over PAUSED, then
    the newest id. Without it a run could adopt a 2024 shell while a current campaign
    sits right next to it.
    """
    want_country = (country or "NL").upper()
    name_tokens = [f"[shop:{v}]".lower() for v in _shop_name_variants(shop_name)]
    label_token = f"[label:{label}]".lower()
    canonical = _build_campaign_name(country, shop_name, shop_id, label)
    matches = []
    for c in candidates:
        name = c["campaign_name"] or ""
        low = name.lower()
        if label_token not in low:
            continue
        if not any(tok in low for tok in name_tokens):
            continue
        if _MACRO_MICRO_RE.search(name) or _RETIRED_RE.search(name):
            continue
        dm = _DOMEIN_RE.search(name)
        found_country = (dm.group(1).strip().upper() if dm else "NL")
        if found_country != want_country:
            continue
        matches.append(c)
    if not matches:
        return None
    matches.sort(key=lambda c: (
        0 if c["campaign_name"] == canonical else 1,
        0 if c["status"] == "ENABLED" else 1,
        -int(c["campaign_id"]),
    ))
    return matches[0]


def _name_contains_regexp(substring: str) -> str:
    """
    Build a GAQL REGEXP_MATCH pattern that matches names CONTAINING `substring`
    literally. Use this instead of ``LIKE '%substring%'`` whenever the substring
    can contain '[' or ']': GAQL's LIKE treats brackets as a character class, so
    ``LIKE '%[shop:X]%'`` collapses to "contains any one of these characters" and
    matches nearly every campaign in the account (it does NOT filter by shop).
    It also treats '_' as a single-char wildcard, which this avoids too.

    Regex metacharacters are escaped, then backslashes are doubled and single
    quotes escaped so the result is safe to embed in a single-quoted GAQL
    string literal.
    """
    pattern = re.escape(substring)          # escape regex specials incl. [ ] . _
    pattern = pattern.replace("\\", "\\\\")  # double backslashes for the GAQL literal
    pattern = pattern.replace("'", "\\'")    # escape any single quote for the GAQL literal
    return f".*{pattern}.*"


# ---------------------------------------------------------------------------
# Negative keywords helper
# ---------------------------------------------------------------------------


# Two-level public suffixes we encounter in shop names (.co.uk etc.)
_SECOND_LEVEL = {"co.uk", "com.au", "co.nz", "com.br", "co.za"}


def _clean_host(raw: str) -> str:
    """Normalise a shop/domain name to a bare host (no |country, scheme, www, path or note)."""
    s = raw.split("|")[0].strip().lower()   # drop |NL country marker
    s = re.sub(r"^https?://", "", s)        # drop scheme
    s = re.sub(r"^www\.", "", s)            # drop leading www.
    s = s.split("/")[0]                     # drop /path
    s = s.split()[0] if s.split() else s    # drop trailing " (note)" / " OUD"
    return s.strip(".")


def get_negatives(shop_name: str) -> List[str]:
    """
    Build negative keywords from a shop name as [full-domain, brand].

    e.g. "Gymbeam.nl" -> ["gymbeam.nl", "gymbeam"];
         "Calcuso.com|NL" -> ["calcuso.com", "calcuso"];
         "Hoopo.eu" -> ["hoopo.eu", "hoopo"].
    Handles any TLD (incl. two-level like .co.uk) and NEVER emits a bare
    TLD/country token (the old split-on-every-non-alphanumeric produced
    harmful "nl"/"com"/"eu" negatives).
    """
    if not shop_name:
        return []
    host = _clean_host(shop_name)
    if not host:
        return []
    if "." not in host:
        return [host]
    # strip the public suffix (two-level suffixes first)
    for suf in _SECOND_LEVEL:
        if host.endswith("." + suf):
            name = host[: -(len(suf) + 1)]
            break
    else:
        name = host.rsplit(".", 1)[0]
    name = name.rsplit(".", 1)[-1]          # core brand label (drop shop./nl. subdomains)
    negatives = [host]
    if name and name != host:
        negatives.append(name)
    return negatives


# ---------------------------------------------------------------------------
# Google Sheets run-logging helper
# ---------------------------------------------------------------------------


def _log_run_to_sheet(rows: List[List[Any]]) -> Dict[str, Any]:
    """
    Append one row per processed shop to the "campaigns_created" tab of the
    "Data: Direct Shopping" sheet, mirroring the original create GSD-campaigns.py.

    Each row (columns A-I): [datum (dd-mm-yyyy), shop_id, shop_name, CPC/CPR,
    Merchant Center ID, domein, op brand?, campagnes aangemaakt?, actie].

    Best-effort: any failure is logged and swallowed so it never breaks a run.
    """
    if not rows:
        return {"logged": 0}
    try:
        creds = service_account.Credentials.from_service_account_file(
            SHEETS_SA_FILE, scopes=SHEETS_SCOPES
        )
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        sheet = svc.spreadsheets()
        # Write right after the last populated LOGGING row (column A), exactly like
        # the original create GSD-campaigns.py. We deliberately key off column A —
        # NOT append() — because this tab has helper columns (J/K/L) with values
        # far below the last log row, which would make append() leave a large gap.
        col_a = sheet.values().get(
            spreadsheetId=LOG_SPREADSHEET_ID, range=f"{LOG_WORKSHEET}!A:A"
        ).execute().get("values", [])
        first_empty = len(col_a) + 1
        end_row = first_empty + len(rows) - 1
        target = f"{LOG_WORKSHEET}!A{first_empty}:I{end_row}"
        sheet.values().update(
            spreadsheetId=LOG_SPREADSHEET_ID,
            range=target,
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        logger.info(
            "Logged %d GSD run row(s) to sheet %s!%s from row %d",
            len(rows), LOG_SPREADSHEET_ID, LOG_WORKSHEET, first_empty,
        )
        return {"logged": len(rows), "first_row": first_empty}
    except Exception as ex:
        logger.warning("Failed to log GSD run to sheet: %s", ex)
        return {"logged": 0, "error": str(ex)[:300]}


# ---------------------------------------------------------------------------
# Google Ads query helpers
# ---------------------------------------------------------------------------


def get_gsd_campaigns(customer_id: str, client: Optional[GoogleAdsClient] = None) -> List[Dict[str, Any]]:
    """
    Query all non-REMOVED campaigns with the GSD_SCRIPT label for a given
    customer account. Returns last-30-day metrics.

    Pass a shared ``client`` to avoid rebuilding one per account (get_all_gsd_stats
    queries several accounts in a row).
    """
    client = client or _get_client()
    ga_service = client.get_service("GoogleAdsService")

    # Step 1: Get campaign IDs with the GSD_SCRIPT label
    label_query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign_label
        WHERE label.name = '{SCRIPT_LABEL}'
          AND campaign.status != 'REMOVED'
    """

    campaigns: Dict[str, Dict[str, Any]] = {}

    try:
        response = ga_service.search(customer_id=customer_id, query=label_query)
        for row in response:
            cid = str(row.campaign.id)
            if cid not in campaigns:
                parsed = _parse_campaign_name(row.campaign.name)
                campaigns[cid] = {
                    "campaign_id": cid,
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "shop_id": parsed["shop_id"],
                    "shop_name": parsed["shop_name"],
                    "label": parsed["label"],
                    "model": parsed["model"],
                    "country": parsed["country"],
                    "customer_id": customer_id,
                    "impressions": 0,
                    "clicks": 0,
                    "cost": 0.0,
                }
    except GoogleAdsException as ex:
        logger.error("Google Ads API error (label query) for customer %s: %s", customer_id, ex)
        raise

    if not campaigns:
        return []

    # Step 2: Get metrics for those campaigns (last 30 days)
    today = datetime.now().strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    campaign_ids = ",".join(campaigns.keys())

    metrics_query = f"""
        SELECT
            campaign.id,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign
        WHERE campaign.id IN ({campaign_ids})
          AND segments.date BETWEEN '{thirty_days_ago}' AND '{today}'
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=metrics_query)
        for row in response:
            cid = str(row.campaign.id)
            if cid in campaigns:
                campaigns[cid]["impressions"] += row.metrics.impressions
                campaigns[cid]["clicks"] += row.metrics.clicks
                campaigns[cid]["cost"] += row.metrics.cost_micros / 1_000_000
    except GoogleAdsException as ex:
        logger.warning("Could not fetch metrics for customer %s: %s", customer_id, ex)

    return list(campaigns.values())


def get_all_gsd_stats() -> Dict[str, Any]:
    """
    Fetch GSD campaigns across all accounts and return aggregated stats.
    """
    all_campaigns: List[Dict[str, Any]] = []
    errors: List[str] = []

    client = _get_client()  # build once, reuse across every account (#14)

    # NL_CPR and NL_CPC share one customer_id, and get_gsd_campaigns returns
    # ALL GSD_SCRIPT campaigns in an account regardless of type — so querying
    # per ACCOUNTS entry would fetch (and count) the NL account twice. Query
    # each DISTINCT customer_id once (#4).
    seen_customer_ids: set = set()
    for account_key, info in ACCOUNTS.items():
        customer_id = info["customer_id"]
        if customer_id in seen_customer_ids:
            continue
        seen_customer_ids.add(customer_id)
        try:
            camps = get_gsd_campaigns(customer_id, client=client)
            # Enrich with account info (metadata only; not used in the totals)
            for c in camps:
                c["account_key"] = account_key
                c["account_type"] = info["type"]
            all_campaigns.extend(camps)
        except Exception as ex:
            errors.append(f"{account_key}: {ex}")
            logger.error("Error fetching GSD campaigns for %s: %s", account_key, ex)

    # Attach persisted creation dates by (shop_id, country) — one fast Postgres
    # read; None (rendered '-') when the shop has no recorded date yet.
    created_dates = get_created_dates()
    for c in all_campaigns:
        c["created_date"] = created_dates.get(_created_key(c.get("shop_id"), c.get("country")))

    total_impressions = sum(c["impressions"] for c in all_campaigns)
    total_clicks = sum(c["clicks"] for c in all_campaigns)
    total_cost = sum(c["cost"] for c in all_campaigns)
    enabled_count = sum(1 for c in all_campaigns if c["status"] == "ENABLED")
    paused_count = sum(1 for c in all_campaigns if c["status"] == "PAUSED")

    # Per-country stats
    accounts = {}
    for country in ["NL", "BE", "DE"]:
        country_camps = [c for c in all_campaigns if (c.get("country") or "").upper() == country]
        accounts[country] = {
            "total": len(country_camps),
            "active": sum(1 for c in country_camps if c["status"] == "ENABLED"),
            "paused": sum(1 for c in country_camps if c["status"] == "PAUSED"),
        }

    return {
        "campaigns": all_campaigns,
        "accounts": accounts,
        "total_campaigns": len(all_campaigns),
        "enabled": enabled_count,
        "paused": paused_count,
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "total_cost": round(total_cost, 2),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Campaign status mutations
# ---------------------------------------------------------------------------


def _mutate_campaign_status(customer_id: str, campaign_id: str, status: str) -> Dict[str, Any]:
    """Set campaign status (ENABLED, PAUSED) or remove it (REMOVED)."""
    client = _get_client()
    campaign_service = client.get_service("CampaignService")
    resource_name = campaign_service.campaign_path(customer_id, campaign_id)

    campaign_op = client.get_type("CampaignOperation")
    if status == "REMOVED":
        # Removal uses the dedicated REMOVE operation. Setting status=REMOVED via
        # an update is rejected by the API (INVALID_ENUM_VALUE: "Enum value
        # 'REMOVED' cannot be used.").
        campaign_op.remove = resource_name
    else:
        campaign = campaign_op.update
        campaign.resource_name = resource_name
        campaign.status = getattr(client.enums.CampaignStatusEnum, status)
        campaign_op.update_mask = field_mask_pb2.FieldMask(paths=["status"])

    try:
        response = _mutate_with_retry(
            f"set campaign {campaign_id} -> {status}",
            lambda: campaign_service.mutate_campaigns(
                customer_id=customer_id, operations=[campaign_op]
            ),
        )
        result = response.results[0]
        return {"success": True, "resource_name": result.resource_name}
    except GoogleAdsException as ex:
        logger.error("Failed to set campaign %s to %s: %s", campaign_id, status, ex)
        return {"success": False, "error": str(ex)}


def pause_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Set campaign status to PAUSED."""
    return _mutate_campaign_status(customer_id, campaign_id, "PAUSED")


def enable_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Set campaign status to ENABLED."""
    return _mutate_campaign_status(customer_id, campaign_id, "ENABLED")


def remove_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Remove a campaign."""
    return _mutate_campaign_status(customer_id, campaign_id, "REMOVED")


def undo_run(
    created: Optional[List[Dict[str, Any]]] = None,
    paused: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Reverse a GSD run: PAUSE the campaigns it created and re-ENABLE the campaigns
    it paused. Both `created` and `paused` are lists of dicts with at least
    ``customer_id`` and ``campaign_id``. Operations are grouped per account and
    applied with partial failure, so one bad id doesn't sink the batch.

    Pausing (not removing) created campaigns keeps the undo reversible.
    Returns counts and any per-account errors.
    """
    result: Dict[str, Any] = {"paused_created": 0, "enabled_paused": 0, "errors": []}

    # Group campaign ids by (customer_id, target_status). "created" -> PAUSED,
    # "paused" -> ENABLED.
    groups: Dict[str, List[str]] = {}

    def _add(items, status):
        for it in (items or []):
            cid = str(it.get("customer_id") or "").strip()
            camp = str(it.get("campaign_id") or "").strip()
            if cid and camp:
                groups.setdefault(f"{cid}|{status}", []).append(camp)

    _add(created, "PAUSED")
    _add(paused, "ENABLED")

    if not groups:
        return result

    client = _get_client()
    cs = client.get_service("CampaignService")

    for key, camp_ids in groups.items():
        cid, status = key.split("|", 1)
        ops = []
        for camp in camp_ids:
            op = client.get_type("CampaignOperation")
            op.update.resource_name = cs.campaign_path(cid, camp)
            op.update.status = getattr(client.enums.CampaignStatusEnum, status)
            op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
            ops.append(op)
        try:
            req = client.get_type("MutateCampaignsRequest")
            req.customer_id = cid
            req.operations.extend(ops)
            req.partial_failure = True
            resp = _mutate_with_retry(
                f"bulk {status} ({cid})",
                lambda: cs.mutate_campaigns(request=req),
            )
            ok = sum(1 for r in resp.results if r.resource_name)
            if status == "PAUSED":
                result["paused_created"] += ok
            else:
                result["enabled_paused"] += ok
            if resp.partial_failure_error and resp.partial_failure_error.message:
                result["errors"].append({
                    "customer_id": cid, "status": status,
                    "error": resp.partial_failure_error.message[:500],
                })
        except GoogleAdsException as ex:
            logger.error("Undo failed (%s -> %s): %s", cid, status, ex)
            result["errors"].append({"customer_id": cid, "status": status, "error": str(ex)[:500]})

    logger.info("Undo run: paused %d created, enabled %d paused, %d errors",
                result["paused_created"], result["enabled_paused"], len(result["errors"]))
    return result


def reconstruct_run(
    at_iso: str,
    before_minutes: int = 60,
    after_minutes: int = 10,
) -> Dict[str, Any]:
    """
    Reconstruct what a past GSD run changed, from Google Ads change history, in a
    window around a log entry's timestamp. `at_iso` is an ISO-8601 timestamp
    (the browser sends UTC, e.g. "2026-07-14T09:20:14.000Z"). Read-only.

    Returns campaigns to undo:
      - created: campaigns CREATEd in the window                -> undo pauses them
      - paused:  campaigns whose latest status change in the window is PAUSED and
                 that were NOT created in it                    -> undo re-enables

    Only GSD ("[channel:directshopping]") campaigns across the GSD accounts are
    considered. change_event retains ~30 days, so older runs return nothing.
    """
    result: Dict[str, Any] = {"created": [], "paused": [], "errors": [], "window": {}}

    # Build the window in the accounts' timezone (Europe/Amsterdam) — that's how
    # change_event.change_date_time is expressed. The run's changes precede the
    # log timestamp (it's written after the run completes), hence the asymmetric
    # default window (mostly looking backwards).
    try:
        at = datetime.fromisoformat(at_iso.replace("Z", "+00:00"))
    except ValueError as ex:
        result["errors"].append({"step": "parse_time", "error": f"{at_iso}: {ex}"})
        return result
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    tz = ZoneInfo("Europe/Amsterdam") if ZoneInfo else timezone.utc
    start = (at - timedelta(minutes=before_minutes)).astimezone(tz)
    end = (at + timedelta(minutes=after_minutes)).astimezone(tz)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = end.strftime("%Y-%m-%d %H:%M:%S")
    result["window"] = {"start": start_s, "end": end_s, "tz": "Europe/Amsterdam"}

    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    created_ids: Dict[tuple, str] = {}        # (cid, camp_id) -> name
    status_latest: Dict[tuple, tuple] = {}    # (cid, camp_id) -> (new_status, name), newest first

    for cid in customer_ids:
        query = f"""
            SELECT change_event.change_date_time, change_event.resource_change_operation,
                   change_event.changed_fields, change_event.old_resource,
                   change_event.new_resource, campaign.id, campaign.name
            FROM change_event
            WHERE change_event.change_date_time BETWEEN '{start_s}' AND '{end_s}'
              AND change_event.change_resource_type = 'CAMPAIGN'
            ORDER BY change_event.change_date_time DESC
            LIMIT 10000
        """
        try:
            rows = list(ga.search(customer_id=cid, query=query))
        except GoogleAdsException as ex:
            logger.error("Reconstruct: change_event query failed for %s: %s", cid, ex)
            result["errors"].append({"customer_id": cid, "error": str(ex)[:400]})
            continue

        for row in rows:
            ce = row.change_event
            name = row.campaign.name or ""
            if "[channel:directshopping]" not in name:
                continue  # keep to GSD campaigns only
            key = (cid, str(row.campaign.id))
            if ce.resource_change_operation.name == "CREATE":
                created_ids.setdefault(key, name)
            if "status" in list(ce.changed_fields.paths) and key not in status_latest:
                status_latest[key] = (ce.new_resource.campaign.status.name, name)

    for (cid, camp_id), name in created_ids.items():
        result["created"].append({"customer_id": cid, "campaign_id": camp_id, "campaign_name": name})
    for key, (st, name) in status_latest.items():
        if st == "PAUSED" and key not in created_ids:
            cid, camp_id = key
            result["paused"].append({"customer_id": cid, "campaign_id": camp_id, "campaign_name": name})

    logger.info("Reconstruct [%s..%s]: %d created, %d paused, %d errors",
                start_s, end_s, len(result["created"]), len(result["paused"]), len(result["errors"]))
    return result


# ---------------------------------------------------------------------------
# Redshift queries
# ---------------------------------------------------------------------------


def get_redshift_shop_changes(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> List[Dict[str, Any]]:
    """
    Compute GSD shop changes live by diffing bt.shop_list for the chosen date
    vs. the day before.  Emits actie='aan' (flag 0->1) or 'uit' (1->0).
    Joins hda.efficy_shop_catman for branded, and derives model (CPR/CPC)
    from the is_wecantrack_shop / is_pixel_shop flags.

    Parameters
    ----------
    date_str : optional date string (YYYY-MM-DD), defaults to today.
    shop_names : optional list of shop names to filter on. Ignored when empty.
    included : how shop_names is applied — True = allow-list (only these shops),
        False = deny-list (everything except these). Matches the UI's
        "Include these shops" / "Exclude these shops" radios and the router's
        documented contract. It used to mean something else entirely; see AUDIT H1
        at the filter below.

    Returns list of dicts with: shop_id, shop_name, kolom, actie, branded, model.
    Only shops whose GSD flag actually changed on `date_str` are returned.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    # Each UNION ALL leg needs the date parameter once
    _LEG = """
                  SELECT today.shop_id,
                         today.shop_name,
                         '{flag}' AS kolom,
                         CASE WHEN COALESCE(y.{flag},0)=0 AND COALESCE(today.{flag},0)=1 THEN 'aan'
                              WHEN COALESCE(y.{flag},0)=1 AND COALESCE(today.{flag},0)=0 THEN 'uit' END AS actie,
                         c.f_branded AS branded,
                         CASE WHEN COALESCE(today.is_wecantrack_shop,0)=1
                                OR COALESCE(today.is_pixel_shop,0)=1
                              THEN 'CPR' ELSE 'CPC' END AS model
                  FROM bt.shop_list today
                  JOIN bt.shop_list y
                    ON today.shop_id = y.shop_id
                   AND y.date = today.date - 1
                   AND y.deleted_ind = 0
                  LEFT JOIN hda.efficy_shops s
                    ON s.f_shop_id = today.shop_id
                   AND s.actual_ind = 1 AND s.deleted_ind = 0
                  LEFT JOIN hda.efficy_shop_catman c
                    ON c.k_shop = s.k_shop
                   AND c.actual_ind = 1 AND c.deleted_ind = 0
                  WHERE today.deleted_ind = 0
                    AND today.date = %s::date
                    AND COALESCE(today.{flag},0) <> COALESCE(y.{flag},0)"""

    flags = ["is_gsd_nl_shop", "is_gsd_be_shop", "is_gsd_de_shop"]
    legs = "\n\n                  UNION ALL\n".join(_LEG.format(flag=f) for f in flags)

    query = f"""
                WITH changes AS (
{legs}
                )
                SELECT * FROM changes"""

    # date_str once per leg
    params: list = [date_str] * len(flags)

    conditions: list = []

    # AUDIT H1 — `included` is the allow/deny switch for shop_names, and nothing else.
    # It used to mean two unrelated things at once: it decided whether the actie filter
    # was applied, while shop_names was ALWAYS turned into `shop_name IN (…)`. So the UI's
    # default "Exclude these shops" ran on exactly those shops — the opposite of what it
    # says, and the opposite of what the router documents. Joep, 2026-08-05: the UI is the
    # truth, so Include = allow-list, Exclude = deny-list.
    #
    # actie is NULL when a flag did not change (the CASE has no ELSE), so this predicate
    # is what makes this a CHANGES query. It now always applies. That removes the old side
    # effect of `included=True` — "also return shops with no flag change today", per the
    # original docstring — deliberately rather than silently: it was never what the UI or
    # the router offered, and it is not reachable from the UI, whose radios only enable
    # once the textarea has content. If that capability is still wanted it belongs behind
    # its own explicitly-named parameter, not as a side effect of the shop-list mode.
    conditions.append("actie IN ('aan', 'uit')")

    if shop_names:
        placeholders = ",".join(["%s"] * len(shop_names))
        conditions.append(
            f"shop_name IN ({placeholders})" if included
            else f"shop_name NOT IN ({placeholders})"
        )
        params.extend(shop_names)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY shop_name, kolom"

    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Label management
# ---------------------------------------------------------------------------


# Label colour + description used when a label has to be created.
_LABEL_META = {
    SCRIPT_LABEL: ("#0000FF", "GSD Script managed campaigns"),
    "BRANDED_0": ("#00B894", "GSD shop with branded = 0 (non-branded)"),
    "BRANDED_1": ("#E17055", "GSD shop with branded = 1 (branded)"),
}

# (customer_id, label_name) -> label resource name. Labels are immutable once
# created, so caching avoids a lookup per campaign/shop across a run.
_label_resource_cache: Dict[tuple, str] = {}


def _lookup_label_resource(
    client: GoogleAdsClient, customer_id: str, label_name: str = SCRIPT_LABEL
) -> Optional[str]:
    """Resource name of an existing label, or None. READ-ONLY, unlike
    ensure_campaign_label_exists, which creates the label when it is missing — the
    preview promises to mutate nothing, so it must use this one."""
    cache_key = (customer_id, label_name)
    if cache_key in _label_resource_cache:
        return _label_resource_cache[cache_key]
    ga_service = client.get_service("GoogleAdsService")
    query = f"SELECT label.resource_name FROM label WHERE label.name = '{label_name}'"
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            _label_resource_cache[cache_key] = row.label.resource_name
            return row.label.resource_name
        # AUDIT MED — cache the MISS too. This is called inside the per-label loop, so an
        # account that simply has no GSD_LL_PAUSED label paid one pointless round trip per
        # label per shop; only hits were remembered. A genuine error is deliberately NOT
        # cached below, so a transient failure does not stick for the process lifetime.
        _label_resource_cache[cache_key] = None
    except GoogleAdsException as ex:
        logger.warning("Label lookup failed (%s, %s): %s", customer_id, label_name, ex)
    return None


def ensure_campaign_label_exists(
    client: GoogleAdsClient, customer_id: str, label_name: str = SCRIPT_LABEL
) -> str:
    """
    Ensure a label exists in the account (defaults to GSD_SCRIPT).
    Returns the label resource name; cached per (customer_id, label_name).
    """
    cache_key = (customer_id, label_name)
    if cache_key in _label_resource_cache:
        return _label_resource_cache[cache_key]

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT label.id, label.name, label.resource_name
        FROM label
        WHERE label.name = '{label_name}'
    """
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            _label_resource_cache[cache_key] = row.label.resource_name
            return row.label.resource_name
    except GoogleAdsException:
        pass

    # Create the label
    label_service = client.get_service("LabelService")
    label_op = client.get_type("LabelOperation")
    label = label_op.create
    label.name = label_name
    color, desc = _LABEL_META.get(label_name, ("#0000FF", f"{label_name} (GSD script)"))
    label.text_label.background_color = color
    label.text_label.description = desc

    response = _mutate_with_retry(
        "create label",
        lambda: label_service.mutate_labels(customer_id=customer_id, operations=[label_op]),
    )
    rn = response.results[0].resource_name
    _label_resource_cache[cache_key] = rn
    return rn


def _branded_label_name(branded) -> Optional[str]:
    """Return "BRANDED_0"/"BRANDED_1" for branded 0/1, else None (NULL/unknown)."""
    try:
        v = int(branded)
    except (TypeError, ValueError):
        return None
    if v == 0:
        return "BRANDED_0"
    if v == 1:
        return "BRANDED_1"
    return None


def _apply_branded_label(client, customer_id, campaign_resource, branded) -> None:
    """Apply the BRANDED_0/BRANDED_1 label matching the shop's branded flag."""
    name = _branded_label_name(branded)
    if not name or not campaign_resource:
        return
    _apply_label_to_campaign(
        client, customer_id, campaign_resource,
        ensure_campaign_label_exists(client, customer_id, name),
    )


def _apply_label_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource_name: str, label_resource_name: str
) -> bool:
    """Apply a label to a campaign. True when the label is on it afterwards.

    Returns a value on purpose: for GSD_SCRIPT this call decides whether the campaign is
    visible to the tool at all (get_gsd_campaigns, _pause_campaigns_for_shop and the
    creation-date log all filter on that label). A silently swallowed failure leaves a
    perfectly normal-looking campaign that the tool can neither show nor pause — 2.954
    canonical campaigns across 416 shops are in exactly that state (measured
    2026-07-31). An already-applied label counts as success.
    """
    campaign_label_service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.create.campaign = campaign_resource_name
    op.create.label = label_resource_name

    try:
        _mutate_with_retry(
            "apply label",
            lambda: campaign_label_service.mutate_campaign_labels(
                customer_id=customer_id, operations=[op]
            ),
        )
        return True
    except GoogleAdsException as ex:
        msg = _gads_err(ex) or str(ex)
        if "DUPLICATE" in msg.upper() or "ALREADY" in msg.upper():
            return True          # the label is on the campaign, which is what we wanted
        logger.warning("Could not apply label to campaign %s: %s", campaign_resource_name, msg)
        return False


# ---------------------------------------------------------------------------
# Check if campaign already exists
# ---------------------------------------------------------------------------


def check_campaign(client: GoogleAdsClient, customer_id: str, campaign_name: str) -> Optional[str]:
    """
    Check if a campaign with the given EXACT name already exists (non-REMOVED).
    Returns campaign resource name if found, else None.

    No longer the run's existence check: exact-name matching missed every older naming
    generation and produced duplicate sets. The run uses
    _fetch_shop_campaign_candidates + _match_existing_campaign (shop name, shop_id,
    custom label). This stays as a primitive for one-off name probes.
    """
    ga_service = client.get_service("GoogleAdsService")
    escaped_name = campaign_name.replace("'", "\\'")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name
        FROM campaign
        WHERE campaign.name = '{escaped_name}'
          AND campaign.status != 'REMOVED'
    """
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return row.campaign.resource_name
    except GoogleAdsException as ex:
        logger.error("Error checking campaign existence: %s", ex)
    return None


# ---------------------------------------------------------------------------
# Merchant Center helpers
# ---------------------------------------------------------------------------


def get_mc_id(mc_parent_id: str, shop_name: str) -> Optional[str]:
    """
    Look up a Merchant Center sub-account by name.
    Paginates through all sub-accounts (Content API returns them in
    the ``resources`` key, max 250 per page).
    Returns the account ID if found, else None.

    Raises on API error rather than returning None: the caller must be able to
    tell a genuine "shop not found" (safe to create) apart from a transient
    lookup failure (creating would spawn a DUPLICATE sub-account for a shop that
    may already have one).
    """
    service = _get_mc_service()
    target = shop_name.lower()
    page_token = None
    while True:
        kwargs: Dict[str, Any] = {"merchantId": mc_parent_id, "maxResults": 250}
        if page_token:
            kwargs["pageToken"] = page_token
        response = service.accounts().list(**kwargs).execute()
        for account in response.get("resources", []):
            if account.get("name", "").lower() == target:
                return str(account["id"])
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return None


def _shop_website_url(shop_name: str, country: Optional[str] = None) -> str:
    """Derive the online-store URL for an MC sub-account from the shop name.

    Beslist shop names are domains (e.g. "Hoopo.eu", "GSMpunt.nl", "Ko-co.beauty"),
    so the URL is just ``https://www.<shop_name>``. If a name has no TLD (rare),
    fall back to the country's TLD. Mirrors the original create GSD-campaigns.py
    but keeps any existing TLD (incl. .eu/.beauty) instead of blindly appending a
    country suffix (which would produce e.g. "hoopo.eu.nl").
    """
    name = shop_name.split("|", 1)[0].split(" l ", 1)[0].strip().lower()
    if not re.search(r"\.[a-z]{2,}$", name):  # no TLD → append the country's
        tld = {"NL": ".nl", "BE": ".be", "DE": ".de"}.get((country or "").upper(), ".nl")
        name += tld
    return f"https://www.{name}"


def create_merchant_id(
    mc_parent_id: str, shop_name: str, website_url: Optional[str] = None
) -> Optional[str]:
    """
    Create a new Merchant Center sub-account.
    Returns the new account ID.

    ``website_url`` populates the account's online-store URL ("Uw online winkel"
    in Merchant Center). When omitted it is derived from the shop name.
    """
    service = _get_mc_service()
    body = {
        "name": shop_name,
        "kind": "content#account",
    }
    body["websiteUrl"] = website_url or _shop_website_url(shop_name)
    try:
        response = service.accounts().insert(merchantId=mc_parent_id, body=body).execute()
        return str(response["id"])
    except Exception as ex:
        logger.error("Error creating MC sub-account for '%s': %s", shop_name, ex)
        _last_mc_error["msg"] = _mc_err(ex)
        return None


def link_to_google_ads(mc_parent_id: str, mc_account_id: str, ads_customer_id: str) -> bool:
    """
    Link a Merchant Center account to a Google Ads account.

    Two-step process:
    1. MC side: add an adsLink on the sub-account (creates a pending invitation).
    2. Ads side: accept the pending ProductLinkInvitation so campaigns can
       reference the merchant_id.

    When the MC-side link is newly created, step 2 polls with retries
    (_INVITATION_ACCEPT_DELAYS) until the invitation becomes visible on the
    Ads side. Without this, campaign creation fails with RESOURCE_NOT_FOUND
    because the link was never actually accepted.
    """
    service = _get_mc_service()
    newly_linked = False
    try:
        # Get current account info
        account = service.accounts().get(merchantId=mc_parent_id, accountId=mc_account_id).execute()

        # Add Google Ads link if not already present
        ads_links = account.get("adsLinks", [])
        ads_id_str = str(ads_customer_id)
        already_linked = any(
            str(link.get("adsId", "")) == ads_id_str and link.get("status") == "active"
            for link in ads_links
        )

        if not already_linked:
            # Remove any stale pending link for this Ads account first
            ads_links = [
                link for link in ads_links
                if str(link.get("adsId", "")) != ads_id_str
            ]
            ads_links.append({
                "adsId": ads_id_str,
                "status": "active",
            })
            account["adsLinks"] = ads_links
            service.accounts().update(
                merchantId=mc_parent_id, accountId=mc_account_id, body=account
            ).execute()
            logger.info("MC side: linked MC %s to Google Ads %s", mc_account_id, ads_customer_id)
            newly_linked = True
    except Exception as ex:
        logger.error("Error linking MC %s to Ads %s (MC side): %s", mc_account_id, ads_customer_id, ex)
        return False

    # Step 2: accept the pending invitation from the Google Ads side.
    # When we just created the MC-side link, the invitation is not visible
    # immediately (eventual consistency). Poll with retries so the link is
    # actually accepted before campaign creation starts.
    try:
        mc_id_int = int(mc_account_id)
        accepted = _accept_mc_invitation(ads_customer_id, mc_id_int)
        if not accepted and newly_linked:
            for attempt, delay in enumerate(_INVITATION_ACCEPT_DELAYS, 1):
                logger.info(
                    "Invitation for MC %s not visible yet in Ads %s; "
                    "retry %d/%d in %ds",
                    mc_account_id, ads_customer_id,
                    attempt, len(_INVITATION_ACCEPT_DELAYS), delay,
                )
                time.sleep(delay)
                accepted = _accept_mc_invitation(ads_customer_id, mc_id_int)
                if accepted:
                    break
        if not accepted:
            logger.warning(
                "No PENDING_APPROVAL ProductLinkInvitation found%s for MC %s in "
                "Ads %s; campaign create will retry with RESOURCE_NOT_FOUND.",
                " after retries" if newly_linked else "",
                mc_account_id, ads_customer_id,
            )
    except Exception as ex:
        logger.error("Error accepting MC invitation for %s in Ads %s: %s", mc_account_id, ads_customer_id, ex)
        return False

    return True


def _accept_mc_invitation(ads_customer_id: str, mc_account_id: int) -> bool:
    """
    Find and accept a pending ProductLinkInvitation for the given MC account.
    Returns True if an invitation was accepted, False if none was found yet
    (so the caller can tell "linked" from "not visible on the Ads side yet").
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT product_link_invitation.resource_name,
               product_link_invitation.merchant_center.merchant_center_id,
               product_link_invitation.status
        FROM product_link_invitation
        WHERE product_link_invitation.status = 'PENDING_APPROVAL'
    """
    response = ga_service.search(customer_id=ads_customer_id, query=query)

    for row in response:
        inv = row.product_link_invitation
        if inv.merchant_center.merchant_center_id == mc_account_id:
            invitation_service = client.get_service("ProductLinkInvitationService")
            invitation_service.update_product_link_invitation(
                customer_id=ads_customer_id,
                product_link_invitation_status=(
                    client.enums.ProductLinkInvitationStatusEnum.ACCEPTED
                ),
                resource_name=inv.resource_name,
            )
            logger.info(
                "Ads side: accepted MC invitation %s for MC %s",
                inv.resource_name, mc_account_id,
            )
            return True

    return False

    logger.info("No pending MC invitation found for MC %s in Ads %s", mc_account_id, ads_customer_id)


# ---------------------------------------------------------------------------
# Campaign creation helpers
# ---------------------------------------------------------------------------


def create_location_op(client: GoogleAdsClient, campaign_resource_name: str, country: str):
    """Create a campaign criterion operation for geo-targeting."""
    geo_target_id = GEO_TARGETS.get(country.upper(), GEO_TARGETS["NL"])

    op = client.get_type("CampaignCriterionOperation")
    criterion = op.create
    criterion.campaign = campaign_resource_name
    criterion.location.geo_target_constant = (
        f"geoTargetConstants/{geo_target_id}"
    )
    return op


def add_standard_shopping_campaign(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_name: str,
    merchant_id: str,
    country: str,
    tracking_template: str,
    label_resource_name: str,
    budget_micros: int = DEFAULT_BUDGET_MICROS,
) -> Optional[str]:
    """
    Create a standard Shopping campaign with budget, location targeting,
    tracking template, and GSD_SCRIPT label.

    Returns the campaign resource name.
    """
    campaign_budget_service = client.get_service("CampaignBudgetService")
    campaign_service = client.get_service("CampaignService")
    campaign_criterion_service = client.get_service("CampaignCriterionService")

    # Step 1: Create campaign budget
    budget_op = client.get_type("CampaignBudgetOperation")
    budget = budget_op.create
    budget.name = f"GSD Budget - {campaign_name} - {datetime.now().isoformat()}"
    budget.amount_micros = budget_micros
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.explicitly_shared = False

    try:
        budget_response = _mutate_with_retry(
            "create budget",
            lambda: campaign_budget_service.mutate_campaign_budgets(
                customer_id=customer_id, operations=[budget_op]
            ),
        )
        budget_resource = budget_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create budget for '%s': %s", campaign_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None

    # Step 2: Create campaign
    camp_op = client.get_type("CampaignOperation")
    campaign = camp_op.create
    campaign.name = campaign_name
    campaign.campaign_budget = budget_resource
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SHOPPING
    # Create PAUSED; the caller flips it to ENABLED only after the ad group,
    # product ad and listing-group tree have all succeeded, so a failure partway
    # can never leave a live, budgeted campaign with no products / no bid tree.
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.manual_cpc.enhanced_cpc_enabled = False

    # Shopping settings
    campaign.shopping_setting.merchant_id = int(merchant_id)
    campaign.shopping_setting.feed_label = country.upper()
    campaign.shopping_setting.campaign_priority = 0
    campaign.shopping_setting.enable_local = True  # matches the original create GSD-campaigns.py

    # Required in API v24+ for EU campaigns
    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )

    # Tracking template
    campaign.tracking_url_template = tracking_template

    try:
        camp_response = _create_campaign_with_retry(
            lambda: campaign_service.mutate_campaigns(
                customer_id=customer_id, operations=[camp_op]
            ),
        )
        campaign_resource = camp_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create campaign '%s': %s", campaign_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None

    # Step 3: Add location targeting
    location_op = create_location_op(client, campaign_resource, country)
    try:
        _mutate_with_retry(
            "location targeting",
            lambda: campaign_criterion_service.mutate_campaign_criteria(
                customer_id=customer_id, operations=[location_op]
            ),
        )
    except GoogleAdsException as ex:
        logger.warning("Failed to add location targeting: %s", ex)

    # Step 4: Apply GSD_SCRIPT label. NOT cosmetic: without it the campaign is invisible
    # to get_gsd_campaigns / _pause_campaigns_for_shop / the creation-date log, so it can
    # never be switched off by this tool again. Loud on failure — a silent warning here is
    # how 2.954 canonical campaigns ended up unmanageable.
    if not _apply_label_to_campaign(client, customer_id, campaign_resource, label_resource_name):
        logger.error(
            "UNMANAGED CAMPAIGN: '%s' (%s) was created but the %s label could not be "
            "applied — it will not show in Campaigns created and cannot be paused by "
            "this tool until the label is attached",
            campaign_name, campaign_resource, SCRIPT_LABEL,
        )

    logger.info("Created campaign '%s' -> %s", campaign_name, campaign_resource)
    return campaign_resource


# ---------------------------------------------------------------------------
# Ad group and ads
# ---------------------------------------------------------------------------


def add_shopping_ad_group(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource_name: str,
    ad_group_name: str,
    cpc_bid_micros: int = 100_000,  # €0.10, matching the original create GSD-campaigns.py
    shop_name: str = "",
) -> Optional[str]:
    """Create a shopping ad group. Returns ad group resource name."""
    ad_group_service = client.get_service("AdGroupService")

    op = client.get_type("AdGroupOperation")
    ad_group = op.create
    ad_group.name = ad_group_name
    ad_group.campaign = campaign_resource_name
    ad_group.type_ = client.enums.AdGroupTypeEnum.SHOPPING_PRODUCT_ADS
    ad_group.cpc_bid_micros = cpc_bid_micros
    ad_group.status = client.enums.AdGroupStatusEnum.ENABLED

    try:
        response = _mutate_with_retry(
            "create ad group",
            lambda: ad_group_service.mutate_ad_groups(
                customer_id=customer_id, operations=[op]
            ),
        )
        resource = response.results[0].resource_name
        logger.info("Created ad group '%s' -> %s", ad_group_name, resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create ad group '%s' (shop: %s): %s", ad_group_name, shop_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None


def add_shopping_product_ad_group_ad(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    shop_name: str = "",
) -> Optional[str]:
    """Create a product shopping ad in the ad group. Returns ad resource name."""
    ad_group_ad_service = client.get_service("AdGroupAdService")

    op = client.get_type("AdGroupAdOperation")
    ad_group_ad = op.create
    ad_group_ad.ad_group = ad_group_resource_name
    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
    ad_group_ad.ad.shopping_product_ad = client.get_type("ShoppingProductAdInfo")

    try:
        # Patient retry: a product ad created seconds after its ad group can be
        # rejected with RESOURCE_NOT_FOUND until the ad group propagates. Wait it
        # out (~2 min) rather than fail and leave an empty ad group for a rerun.
        response = _create_child_with_retry(
            "create product ad",
            lambda: ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id, operations=[op]
            ),
        )
        resource = response.results[0].resource_name
        logger.info("Created shopping product ad -> %s", resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create shopping product ad (shop: %s): %s", shop_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None


# ---------------------------------------------------------------------------
# Negative keywords
# ---------------------------------------------------------------------------


def add_negative_keywords(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource_name: str,
    keywords: List[str],
) -> int:
    """
    Add negative keywords to a campaign as both EXACT and PHRASE match
    (matching the original create GSD-campaigns.py behaviour).
    Returns count of successfully added criteria.
    """
    if not keywords:
        return 0

    campaign_criterion_service = client.get_service("CampaignCriterionService")
    ops = []

    for kw in keywords:
        for match_type in (
            client.enums.KeywordMatchTypeEnum.EXACT,
            client.enums.KeywordMatchTypeEnum.PHRASE,
        ):
            op = client.get_type("CampaignCriterionOperation")
            criterion = op.create
            criterion.campaign = campaign_resource_name
            criterion.negative = True
            criterion.keyword.text = kw
            criterion.keyword.match_type = match_type
            ops.append(op)

    try:
        response = _mutate_with_retry(
            "negative keywords",
            lambda: campaign_criterion_service.mutate_campaign_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        count = len(response.results)
        logger.info("Added %d negative keywords to %s", count, campaign_resource_name)
        return count
    except GoogleAdsException as ex:
        logger.error("Failed to add negative keywords: %s", ex)
        return 0


# ---------------------------------------------------------------------------
# Listing group tree builders (product partitions)
# ---------------------------------------------------------------------------


def create_listing_group_subdivision(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    parent_resource_name: Optional[str],
    dimension: Optional[Any] = None,
    temp_id: Optional[int] = None,
) -> Any:
    """
    Create a listing group SUBDIVISION operation (non-leaf node).
    Returns the operation and the resource name.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")

    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = (
        client.enums.ListingGroupTypeEnum.SUBDIVISION
    )

    if temp_id is not None:
        # ad_group_criterion_path needs THREE components
        # (customer_id, ad_group_id, criterion_id); the ad group already exists,
        # so pull its id out of the resource name. temp_id is negative (a temp
        # criterion id) so children can reference this root within the same
        # atomic mutate.
        ad_group_id = ad_group_resource_name.split("/")[-1]
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id, ad_group_id, str(temp_id)
        )

    if parent_resource_name is not None:
        criterion.listing_group.parent_ad_group_criterion = parent_resource_name

    if dimension is not None:
        criterion.listing_group.case_value = dimension

    return op, criterion.resource_name


def create_listing_group_unit_biddable(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    parent_resource_name: str,
    dimension: Optional[Any] = None,
    cpc_bid_micros: int = 1_000_000,
    negative: bool = False,
) -> Any:
    """
    Create a listing group UNIT operation (leaf node). Biddable by default;
    with negative=True it's an excluded leaf (no bid), used for the "other"
    catch-all so only the targeted products serve.
    Returns the operation.
    """
    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = client.enums.ListingGroupTypeEnum.UNIT
    criterion.listing_group.parent_ad_group_criterion = parent_resource_name
    if negative:
        criterion.negative = True
    else:
        criterion.cpc_bid_micros = cpc_bid_micros

    if dimension is not None:
        criterion.listing_group.case_value = dimension

    return op


# The product custom-label (INDEX0) VALUE on the products uses spaces, not the
# underscored campaign-label form (matches the original create GSD-campaigns.py
# `labels = ["a","b","c","no data","no ean"]`).
_CPR_LABEL_VALUE = {"no_data": "no data", "no_ean": "no ean"}


def add_sub_cpr(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    label: str,
    cpc_bid_micros: int = 50_000,
    shop_name: str = "",
) -> bool:
    """
    Create the CPR listing group tree, matching create GSD-campaigns.py `addSub`:
    a SUBDIVISION root, a biddable UNIT for product_custom_attribute[INDEX0] equal
    to this label's value (plus the invld_ean / nd_c / nd_cr nodes for no_data),
    and an excluded ("other") catch-all so only this label's products serve.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")
    value = _CPR_LABEL_VALUE.get(label, label)

    def _dim(v):
        d = client.get_type("ListingDimensionInfo")
        d.product_custom_attribute.index = client.enums.ProductCustomAttributeIndexEnum.INDEX0
        if v is not None:
            d.product_custom_attribute.value = v
        return d

    reset_temp_ids()
    ops = []
    root_op, root_resource = create_listing_group_subdivision(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=None, dimension=None, temp_id=next_id(),
    )
    ops.append(root_op)

    # Biddable unit for this label's products.
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(value),
        cpc_bid_micros=cpc_bid_micros,
    ))
    # no_data also carries the invld_ean / nd_c / nd_cr custom-label values.
    if label == "no_data":
        for extra in ("invld_ean", "nd_c", "nd_cr"):
            ops.append(create_listing_group_unit_biddable(
                client, customer_id, ad_group_resource_name,
                parent_resource_name=root_resource, dimension=_dim(extra),
                cpc_bid_micros=cpc_bid_micros,
            ))
    # "other" catch-all — excluded so the campaign only serves its own label.
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(None), negative=True,
    ))

    try:
        _create_child_with_retry(
            "CPR listing group tree",
            lambda: ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        logger.info("Created CPR listing group tree (label=%s) for %s", label, ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        # "Listing group cannot be added to the ad group because it already exists" plus
        # "Listing group referenced in the operation was not found in the ad group" is what
        # a concurrent build in the SAME ad group looks like: the other writer got the root
        # in, so our root collides and our children point at a temp id that never existed.
        # The tree may well be correct now, so check before calling it a failure
        # (2026-09-01: reported as listing_group_creation_failed on campaigns that had a
        # perfectly good tree).
        msg = _gads_err(ex)
        ag_id = ad_group_resource_name.rstrip("/").split("/")[-1]
        try:
            if _tree_targets_label(client.get_service("GoogleAdsService"),
                                   customer_id, ag_id, "CPR", label):
                logger.warning("CPR listing group create failed (%s) but the tree is "
                               "correct for label=%s on %s — treating as done",
                               msg, label, ad_group_resource_name)
                return True
        except Exception as probe_ex:            # a failed probe must not mask the real error
            logger.warning("Could not verify the CPR tree after a failed create: %s", probe_ex)
        logger.error("Failed to create CPR listing group (shop: %s): %s", shop_name, ex)
        _last_gads_error["msg"] = msg
        return False


def add_sub_cpc(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    label: str,
    shop_name: str = "",
) -> bool:
    """
    Create the listing group tree with price buckets for CPC campaigns.
    Uses BIDS_AB for a,b labels and BIDS_C for c labels.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")

    # LABELS_CPC = ["a,b", "c,no_data,no_ean"]. Test the FIRST token, not a bare
    # `"a" in label` substring — the latter is always true for "c,no_data,no_ean"
    # ("no_data" contains an 'a'), so BIDS_C was never selected and the c bucket
    # got the higher AB bids.
    bids = BIDS_AB if label.split(",")[0].strip().lower() == "a" else BIDS_C

    reset_temp_ids()
    ops = []

    # Root subdivision
    root_temp_id = next_id()
    root_op, root_resource = create_listing_group_subdivision(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=None,
        dimension=None,
        temp_id=root_temp_id,
    )
    ops.append(root_op)

    # Price bucket units
    for i, bucket in enumerate(PRICE_BUCKETS):
        bid_micros = int(bids[i] * 1_000_000)

        dimension = client.get_type("ListingDimensionInfo")
        dimension.product_custom_attribute.index = (
            client.enums.ProductCustomAttributeIndexEnum.INDEX0
        )
        dimension.product_custom_attribute.value = bucket

        unit_op = create_listing_group_unit_biddable(
            client, customer_id, ad_group_resource_name,
            parent_resource_name=root_resource,
            dimension=dimension,
            cpc_bid_micros=bid_micros,
        )
        ops.append(unit_op)

    # "Everything else" (OTHERS) unit. The subdivision partitions on
    # product_custom_attribute INDEX0, so the catch-all leaf must carry a
    # ListingDimensionInfo of the SAME dimension type with the index set and no
    # value — passing dimension=None leaves case_value unset and the API rejects
    # the whole atomic mutate.
    other_dimension = client.get_type("ListingDimensionInfo")
    other_dimension.product_custom_attribute.index = (
        client.enums.ProductCustomAttributeIndexEnum.INDEX0
    )
    other_op = create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource,
        dimension=other_dimension,
        cpc_bid_micros=int(bids[0] * 1_000_000),
    )
    ops.append(other_op)

    try:
        _create_child_with_retry(
            "CPC listing group tree",
            lambda: ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        logger.info("Created CPC listing group tree for %s", ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        # Same re-check as add_sub_cpr: a colliding concurrent build in the same ad group
        # leaves a tree that may already be right.
        msg = _gads_err(ex)
        ag_id = ad_group_resource_name.rstrip("/").split("/")[-1]
        try:
            if _tree_targets_label(client.get_service("GoogleAdsService"),
                                   customer_id, ag_id, "CPC", label):
                logger.warning("CPC listing group create failed (%s) but the tree is "
                               "present on %s — treating as done", msg, ad_group_resource_name)
                return True
        except Exception as probe_ex:
            logger.warning("Could not verify the CPC tree after a failed create: %s", probe_ex)
        logger.error("Failed to create CPC listing group (shop: %s): %s", shop_name, ex)
        _last_gads_error["msg"] = msg
        return False


def add_sub_cpc_bucket(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    bucket_label: str,
    cpc_bid_micros: int,
    shop_name: str = "",
) -> bool:
    """Listing tree for ONE price-bucket ad group: a SUBDIVISION root partitioning on
    custom_label_4, a biddable UNIT for this ad group's own bucket, and an EXCLUDED
    catch-all so the ad group serves nothing else.

    The exclusion is what makes the 14 ad groups a partition instead of 14 copies of the
    whole catalogue — note add_sub_cpc (the legacy CPC tree) leaves its catch-all
    biddable, which is why every product there serves at one flat bid.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")
    idx = getattr(
        client.enums.ProductCustomAttributeIndexEnum, CPC_BUCKET_ATTRIBUTE_INDEX
    )

    def _dim(value: Optional[str]):
        d = client.get_type("ListingDimensionInfo")
        d.product_custom_attribute.index = idx
        if value is not None:
            d.product_custom_attribute.value = value
        return d

    reset_temp_ids()
    ops = []
    root_op, root_resource = create_listing_group_subdivision(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=None, dimension=None, temp_id=next_id(),
    )
    ops.append(root_op)
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(bucket_label),
        cpc_bid_micros=cpc_bid_micros,
    ))
    # "Everything else" — same dimension type with the index set and no value, or the
    # API rejects the whole atomic mutate (see the note in add_sub_cpc).
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(None), negative=True,
    ))

    try:
        _create_child_with_retry(
            "CPC bucket listing group tree",
            lambda: ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        logger.info("Created CPC bucket tree (%s) for %s", bucket_label, ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to create CPC bucket tree '%s' (shop: %s): %s",
                     bucket_label, shop_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return False


def _bucket_tree_ok(crits: List[Any], bucket_label: str) -> bool:
    """True when this ad group's criteria are a bucket tree targeting `bucket_label`:
    a SUBDIVISION root plus a biddable UNIT keyed on custom_label_4 == the bucket."""
    if not crits:
        return False
    if not any(
        c.listing_group.type_.name == "SUBDIVISION"
        and not c.listing_group.parent_ad_group_criterion
        for c in crits
    ):
        return False
    return any(
        c.listing_group.type_.name == "UNIT"
        and not c.negative
        and c.listing_group.case_value.product_custom_attribute.index.name
        == CPC_BUCKET_ATTRIBUTE_INDEX
        and c.listing_group.case_value.product_custom_attribute.value == bucket_label
        for c in crits
    )


def _build_bucket_structure(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource: str,
    shop_name: str = "",
) -> Dict[str, Any]:
    """Create every missing piece of the 14 price-bucket ad groups under one campaign.

    Idempotent PER PIECE, not per ad group. The source script guards the listing tree
    behind `if is_created:`, so an ad group created by a run that died before its tree
    would never get one — a silent, permanent hole. Here each of ad group / product ad /
    listing tree is checked and completed independently, which also makes this function
    the repair path (_repair_campaign delegates to it).

    Returns {"ok": bool, "created": int, "reason": str|None, "error": str|None}, where
    ``created`` counts the pieces actually mutated (0 == nothing needed doing).
    """
    ga = client.get_service("GoogleAdsService")
    campaign_id = campaign_resource.rstrip("/").split("/")[-1]

    # Three campaign-wide reads instead of 3 per bucket. Children are keyed by ad group
    # id and only LIVE ad groups are consulted, so a REMOVED sibling's still-active
    # children cannot mask a hole (the trap _repair_campaign documents).
    ad_groups: Dict[str, str] = {}
    for row in ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group.id, ad_group.name, ad_group.resource_name FROM ad_group "
            f"WHERE campaign.id = {campaign_id} AND ad_group.status != 'REMOVED'")):
        ad_groups[row.ad_group.name] = row.ad_group.resource_name

    # Keyed by ad group id, so an ad belonging to a REMOVED ad group is never credited
    # to the live one that replaced it (same name, different id).
    ads_by_ag: set = set()
    for row in ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group.id FROM ad_group_ad WHERE campaign.id = {campaign_id} "
            f"AND ad_group_ad.status != 'REMOVED'")):
        ads_by_ag.add(str(row.ad_group.id))

    crits_by_ag: Dict[str, list] = {}
    for row in ga.search(customer_id=customer_id, query=(
            "SELECT ad_group.id, ad_group_criterion.listing_group.type, "
            "ad_group_criterion.listing_group.parent_ad_group_criterion, "
            "ad_group_criterion.negative, "
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.value "
            f"FROM ad_group_criterion WHERE campaign.id = {campaign_id} "
            "AND ad_group_criterion.type = 'LISTING_GROUP' "
            "AND ad_group_criterion.status != 'REMOVED'")):
        crits_by_ag.setdefault(str(row.ad_group.id), []).append(row.ad_group_criterion)

    created = 0
    for bucket_label, cpc_euro in CPC_BUCKET_BIDS:
        if _run_cancel["cancel"]:
            return {"ok": False, "created": created, "reason": "cancelled", "error": None}
        cpc_micros = int(round(cpc_euro * 1_000_000))
        _last_gads_error["msg"] = None

        ad_group_resource = ad_groups.get(bucket_label)
        if ad_group_resource is None:
            ad_group_resource = add_shopping_ad_group(
                client, customer_id, campaign_resource, bucket_label,
                cpc_bid_micros=cpc_micros, shop_name=shop_name,
            )
            if ad_group_resource is None:
                return {"ok": False, "created": created, "reason": "ad_group_creation_failed",
                        "error": _last_gads_error["msg"] or "ad group creation failed"}
            created += 1
            ad_group_id = ad_group_resource.rstrip("/").split("/")[-1]
        else:
            ad_group_id = ad_group_resource.rstrip("/").split("/")[-1]

        if ad_group_id not in ads_by_ag:
            if add_shopping_product_ad_group_ad(
                    client, customer_id, ad_group_resource, shop_name=shop_name) is None:
                return {"ok": False, "created": created, "reason": "product_ad_creation_failed",
                        "error": _last_gads_error["msg"] or "product ad creation failed"}
            created += 1

        crits = crits_by_ag.get(ad_group_id, [])
        if crits and not _bucket_tree_ok(crits, bucket_label):
            # A tree that exists but targets the wrong thing (e.g. built on INDEX0, or
            # left over from the legacy structure) is removed so the correct one can be
            # built — the same retree the CPR/CPC repair path performs.
            try:
                _remove_listing_tree(client, customer_id, ad_group_id)
            except GoogleAdsException as ex:
                logger.error("Failed to remove wrong bucket tree '%s' (shop: %s): %s",
                             bucket_label, shop_name, ex)
                return {"ok": False, "created": created, "reason": "repair_retree_failed",
                        "error": _gads_err(ex)}
            crits = []
        if not crits:
            if not add_sub_cpc_bucket(client, customer_id, ad_group_resource,
                                      bucket_label, cpc_micros, shop_name=shop_name):
                return {"ok": False, "created": created, "reason": "listing_group_creation_failed",
                        "error": _last_gads_error["msg"] or "listing group creation failed"}
            created += 1

    return {"ok": True, "created": created, "reason": None, "error": None}


def _labels_for_shop(
    campaign_type: str,
    candidates: List[Dict[str, Any]],
    country: str,
    shop_name: str,
    shop_id: Any,
) -> List[str]:
    """The label vocabulary this shop gets in this account — the ONE place that decides
    which CPC structure applies, called by both the run and the preview so they cannot
    disagree about what a run will build.

    Joep, 2026-08-17: the price-bucket structure is for NEWLY connected CPC shops. A CPC
    shop that already carries the legacy two-campaign pair keeps it, so an existing shop
    can never end up with both structures side by side (which would double its coverage
    and its spend).
    """
    if campaign_type != "CPC":
        return LABELS_CPR
    if any(_match_existing_campaign(candidates, country, shop_name, shop_id, lbl)
           for lbl in LABELS_CPC):
        return LABELS_CPC
    return LABELS_CPC_BUCKETS


# ---------------------------------------------------------------------------
# Main GSD script flow
# ---------------------------------------------------------------------------


def _find_account_info(country: str, campaign_type: str) -> Optional[Dict[str, str]]:
    """Find account info by country and type."""
    key = f"{country.upper()}_{campaign_type.upper()}"
    return ACCOUNTS.get(key)


def _pause_customer_ids(country: str, primary_customer_id: str) -> List[str]:
    """Every account a pause must sweep for this country: the live account first, then
    any legacy account from PAUSE_EXTRA_CUSTOMER_IDS. De-duplicated, order preserved, so
    the live account is always the one reported when a campaign exists in both."""
    ids = [primary_customer_id]
    for cid in PAUSE_EXTRA_CUSTOMER_IDS.get((country or "").upper(), []):
        if cid not in ids:
            ids.append(cid)
    return ids


def _is_transient_mc_error(ex: Exception) -> bool:
    """Return True if the Merchant Center API error is transient (worth retrying):
    read timeouts and HTTP 500/503. Permanent errors (403 quota, 404, etc.) are not."""
    ex_str = str(ex)
    # Read timeouts (requests.exceptions.ReadTimeout / socket.timeout)
    if "timed out" in ex_str.lower():
        return True
    # Google API HttpError 500/503
    if hasattr(ex, "resp") and hasattr(ex.resp, "status"):
        return ex.resp.status in (500, 503)
    if "HttpError 500" in ex_str or "HttpError 503" in ex_str:
        return True
    return False


# Serialises the get-or-create of ONE (parent, shop) Merchant Center sub-account across
# processes. Two-int advisory key so it can never collide with GSD_RUN_LOCK_KEY.
MC_ACCOUNT_LOCK_CLASS = 0x6D636163       # "mcac"
MC_ACCOUNT_LOCK_TIMEOUT_MS = 120_000     # 2 min: an MC create is seconds, a full list is not


def _mc_lock_key2(mc_parent_id: str, shop_name: str) -> int:
    """Stable signed-int32 key for one (parent, shop). zlib.crc32 rather than hash(),
    which is salted per process and would give each backend its own lock."""
    import zlib
    raw = f"{mc_parent_id}|{(shop_name or '').strip().lower()}".encode("utf-8")
    return zlib.crc32(raw) - 0x80000000   # crc32 is unsigned; shift into int4 range


@contextmanager
def _mc_account_lock(mc_parent_id: str, shop_name: str):
    """Hold the per-shop MC get-or-create lock. Yields True when held.

    Best-effort: if PostgreSQL is unreachable the caller proceeds unlocked (an MC account
    still beats no campaigns), and the re-check inside the lock is skipped rather than
    the whole create.
    """
    key2 = _mc_lock_key2(mc_parent_id, shop_name)
    with _session_lock_connection() as conn:
        holding = False
        try:
            try:
                with conn.cursor() as cur:
                    # Blocking, unlike the run lock: the other writer is creating THIS
                    # shop's account and will be done in seconds, and what we want is its
                    # result, not an error. lock_timeout keeps that bounded.
                    cur.execute("SET lock_timeout = %s", (MC_ACCOUNT_LOCK_TIMEOUT_MS,))
                    cur.execute("SELECT pg_advisory_lock(%s, %s)", (MC_ACCOUNT_LOCK_CLASS, key2))
                holding = True
            except Exception as ex:
                logger.warning("MC account lock unavailable for '%s' (%s) — proceeding unlocked",
                               shop_name, ex)
            yield holding
        finally:
            if holding:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s, %s)",
                                    (MC_ACCOUNT_LOCK_CLASS, key2))
                except Exception as ex:
                    logger.warning("Could not release the MC account lock for '%s': %s",
                                   shop_name, ex)


def _lookup_mc_id_with_retry(mc_parent_id: str, shop_name: str) -> tuple[Optional[str], bool]:
    """``(mc_id, lookup_ok)``. Retries transient MC API errors (read timeout, HTTP
    500/503). A lookup that returns None (shop not found) is NOT an error and does not
    retry; a permanent error (403 quota, 404, ...) returns ``(None, False)`` so the caller
    aborts instead of creating a duplicate for a shop that may already have one."""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            return get_mc_id(mc_parent_id, shop_name), True
        except Exception as ex:
            if _is_transient_mc_error(ex) and attempt < max_retries:
                logger.warning("MC lookup for '%s' failed (attempt %d/%d), retrying in 5s: %s",
                               shop_name, attempt + 1, max_retries + 1, ex)
                time.sleep(5)
                continue
            logger.error("MC lookup failed for '%s'; skipping create to avoid a duplicate: %s",
                         shop_name, ex)
            _last_mc_error["msg"] = _mc_err(ex)
            return None, False
    return None, False


def _get_or_create_mc_account(
    mc_parent_id: str, shop_name: str, ads_customer_id: str, country: Optional[str] = None
) -> tuple[Optional[str], bool]:
    """Find or create a Merchant Center sub-account and link to Google Ads.

    Returns ``(mc_id, created)`` where ``created`` is True only when a NEW
    sub-account was created (vs. an existing one being reused). On failure
    returns ``(None, False)``.

    LOOKUP AND CREATE ARE ONE CRITICAL SECTION. This used to be a bare
    check-then-create, and on 2026-09-01 two concurrent runs both read "absent" for the
    same shop and both created: Bouwlampkoning.nl got 5847763067 + 5847763163 and
    Vergewallet.nl got 5847225352 + 5847763988, one of each pair left empty with the
    campaigns on the other. `accounts.list` is also eventually consistent — a freshly
    created sub-account is not in the listing for a while (verified the same day: two
    DELETEd accounts stayed in the listing minutes after the API had already stopped
    serving them) — so even a single run calling this twice for one shop could duplicate.
    Hence the lock AND the re-check inside it, not one or the other.
    """
    _last_mc_error["msg"] = None  # cleared per attempt; set on failure below
    mc_id, lookup_ok = _lookup_mc_id_with_retry(mc_parent_id, shop_name)
    if not lookup_ok:
        return None, False

    created = False
    if mc_id is None:
        with _mc_account_lock(mc_parent_id, shop_name) as locked:
            if locked:
                # Whoever held the lock before us may have created it. Ask again — this is
                # the check that the old code was missing.
                mc_id, lookup_ok = _lookup_mc_id_with_retry(mc_parent_id, shop_name)
                if not lookup_ok:
                    return None, False
                if mc_id is not None:
                    logger.info("MC sub-account for '%s' appeared while waiting for the "
                                "lock (%s) — reusing it instead of creating a second one",
                                shop_name, mc_id)
            if mc_id is None:
                website_url = _shop_website_url(shop_name, country)
                mc_id = create_merchant_id(mc_parent_id, shop_name, website_url)
                if mc_id is None:
                    return None, False
                created = True

    link_to_google_ads(mc_parent_id, mc_id, ads_customer_id)
    return mc_id, created


def _set_campaign_status_by_resource(
    client: GoogleAdsClient, customer_id: str, campaign_resource: str, status: str
) -> bool:
    """Set an existing campaign's status via its resource name, reusing the
    shared client (unlike _mutate_campaign_status, which builds a fresh one).
    Returns True on success."""
    campaign_service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    op.update.resource_name = campaign_resource
    op.update.status = getattr(client.enums.CampaignStatusEnum, status)
    op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
    try:
        _mutate_with_retry(
            f"set status -> {status}",
            lambda: campaign_service.mutate_campaigns(customer_id=customer_id, operations=[op]),
        )
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to set %s to %s: %s", campaign_resource, status, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return False


def _tree_targets_label(ga, customer_id, ad_group_id, campaign_type, label) -> bool:
    """
    True if the ad group's listing tree looks correct for its custom label: a
    SUBDIVISION root, and (for CPR) a biddable UNIT keyed on
    product_custom_attribute[INDEX0] == the label's value. Catches the legacy
    single-root-UNIT tree and trees targeting the wrong label value.

    Scoped to a specific ad group (NOT the campaign): a campaign can retain a
    REMOVED ad group whose child criteria keep their own non-REMOVED status, so a
    campaign-wide query would read a dead ad group's tree as the live one's.
    """
    crits = list(ga.search(customer_id=customer_id, query=(
        "SELECT ad_group_criterion.listing_group.type, "
        "ad_group_criterion.listing_group.parent_ad_group_criterion, "
        "ad_group_criterion.negative, "
        "ad_group_criterion.listing_group.case_value.product_custom_attribute.value "
        f"FROM ad_group_criterion WHERE ad_group.id = {ad_group_id} "
        "AND ad_group_criterion.type = 'LISTING_GROUP' "
        "AND ad_group_criterion.status != 'REMOVED'")))
    if not crits:
        return False
    # A single biddable root UNIT (the old wrong tree) has no subdivision root.
    has_subdiv_root = any(
        c.ad_group_criterion.listing_group.type_.name == "SUBDIVISION"
        and not c.ad_group_criterion.listing_group.parent_ad_group_criterion
        for c in crits)
    if not has_subdiv_root:
        return False
    if campaign_type != "CPR":
        return True  # CPC uses a price-bucket subdivision tree; root check suffices
    expected = _CPR_LABEL_VALUE.get(label, label)
    return any(
        c.ad_group_criterion.listing_group.type_.name == "UNIT"
        and not c.ad_group_criterion.negative
        and c.ad_group_criterion.listing_group.case_value.product_custom_attribute.value == expected
        for c in crits)


def _remove_listing_tree(client, customer_id, ad_group_id) -> None:
    """Remove all (non-removed) LISTING_GROUP criteria for a specific ad group.

    Scoped by ad_group.id (not campaign.id) so a REMOVED sibling ad group's
    orphaned criteria are left alone.
    """
    ga = client.get_service("GoogleAdsService")
    svc = client.get_service("AdGroupCriterionService")
    crits = list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group_criterion.resource_name FROM ad_group_criterion "
        f"WHERE ad_group.id = {ad_group_id} AND ad_group_criterion.type = 'LISTING_GROUP' "
        f"AND ad_group_criterion.status != 'REMOVED'")))
    if not crits:
        return
    ops = []
    for c in crits:
        op = client.get_type("AdGroupCriterionOperation")
        op.remove = c.ad_group_criterion.resource_name
        ops.append(op)
    _mutate_with_retry(
        "remove listing tree",
        lambda: svc.mutate_ad_group_criteria(customer_id=customer_id, operations=ops),
    )


def _repair_campaign(client, customer_id, campaign_resource, campaign_name,
                     campaign_type, label, shop_name: str = "") -> Dict[str, Any]:
    """
    An existing campaign was found. Complete/repair it and leave it PAUSED:
    - missing ad group / product ad / listing tree -> create the missing pieces
    - a present but WRONG listing tree (single root UNIT, or not targeting this
      campaign's custom label) -> remove it and rebuild the correct one
    - fully complete AND correctly targeted -> skip unchanged
    """
    ga = client.get_service("GoogleAdsService")
    campaign_id = campaign_resource.rstrip("/").split("/")[-1]

    # The bucket campaign holds 14 ad groups, so the single-ad-group logic below —
    # which inspects ags[0] and would call a campaign complete on the strength of one
    # bucket — does not apply. _build_bucket_structure is itself the repair: it
    # completes every missing piece of every bucket and no-ops when all 14 are sound.
    if label == CPC_BUCKETS_LABEL:
        res = _build_bucket_structure(client, customer_id, campaign_resource, shop_name=shop_name)
        if not res["ok"]:
            _last_gads_error["msg"] = res["error"]
            # "cancelled" is the user stopping the run, not a broken campaign — see the
            # matching note in _create_campaigns_for_shop.
            return {"campaign_name": campaign_name,
                    "action": "skipped" if res["reason"] == "cancelled" else "error",
                    "reason": res["reason"],
                    "error": res["error"], "campaign_resource": campaign_resource}
        if res["created"] == 0:
            return {"campaign_name": campaign_name, "action": "skipped", "reason": "already_exists",
                    "campaign_resource": campaign_resource}
        logger.info("Repaired bucket campaign '%s' — completed %d missing piece(s)",
                    campaign_name, res["created"])
        return {"campaign_name": campaign_name, "action": "created", "reason": "repaired",
                "campaign_resource": campaign_resource}

    ags = list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group.resource_name FROM ad_group "
        # ORDER BY so repair and verify always pick the SAME ad group: an unordered
        # result let the two inspect different siblings and flap (AUDIT MED).
        f"WHERE campaign.id = {campaign_id} AND ad_group.status != 'REMOVED' "
        f"ORDER BY ad_group.id")))
    ad_group_resource = ags[0].ad_group.resource_name if ags else None
    # Scope the product-ad / listing-tree checks to the LIVE ad group, not the
    # campaign: a REMOVED sibling ad group keeps its child ads/criteria at their
    # own non-REMOVED status, so a campaign-wide query would see a dead ad group's
    # ad + tree and wrongly conclude the live ad group is complete (skipping the
    # repair of an ad group whose tree was deleted).
    ad_group_id = ad_group_resource.rstrip("/").split("/")[-1] if ad_group_resource else None
    has_ad = has_lg = False
    if ad_group_resource:
        has_ad = bool(list(ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group_ad.ad.id FROM ad_group_ad "
            f"WHERE ad_group.id = {ad_group_id} AND ad_group_ad.status != 'REMOVED'"))))
        has_lg = bool(list(ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group_criterion.criterion_id FROM ad_group_criterion "
            f"WHERE ad_group.id = {ad_group_id} AND ad_group_criterion.type = 'LISTING_GROUP' "
            f"AND ad_group_criterion.status != 'REMOVED'"))))

    # Validate the existing tree targets this campaign's label; drop a wrong one
    # so it gets rebuilt below.
    retree = False
    if has_lg and not _tree_targets_label(ga, customer_id, ad_group_id, campaign_type, label):
        try:
            _remove_listing_tree(client, customer_id, ad_group_id)
        except GoogleAdsException as ex:
            logger.error("Failed to remove wrong listing tree for '%s': %s", campaign_name, ex)
            _last_gads_error["msg"] = _gads_err(ex)
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_retree_failed",
                    "error": _last_gads_error["msg"], "campaign_resource": campaign_resource}
        has_lg = False
        retree = True

    if ad_group_resource and has_ad and has_lg:
        # campaign_resource is carried even though nothing was repaired: this dict gets
        # flipped to action="activated" by _create_campaigns_for_shop, and without a
        # resource run_gsd_script never derives a campaign_id — so the frontend's undo
        # builder (filter(c => c.customer_id && c.campaign_id)) silently dropped the row
        # and Reset left activated campaigns live while promising to pause them.
        # Every other return here already carries it (AUDIT H3).
        return {"campaign_name": campaign_name, "action": "skipped", "reason": "already_exists",
                "campaign_resource": campaign_resource}

    # Incomplete/mis-targeted — complete the missing pieces (stays PAUSED).
    logger.info("Repairing campaign '%s' (ad_group=%s ad=%s listing=%s retree=%s)",
                campaign_name, bool(ad_group_resource), has_ad, has_lg, retree)
    _last_gads_error["msg"] = None
    if not ad_group_resource:
        ad_group_resource = add_shopping_ad_group(
            client, customer_id, campaign_resource, label, shop_name=shop_name)
        if ad_group_resource is None:
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_ad_group_failed",
                    "error": _last_gads_error["msg"] or "ad group creation failed", "campaign_resource": campaign_resource}
    if not has_ad and add_shopping_product_ad_group_ad(client, customer_id, ad_group_resource, shop_name=shop_name) is None:
        return {"campaign_name": campaign_name, "action": "error", "reason": "repair_product_ad_failed",
                "error": _last_gads_error["msg"] or "product ad creation failed", "campaign_resource": campaign_resource}
    if not has_lg:
        tree_ok = (add_sub_cpr(client, customer_id, ad_group_resource, label, shop_name=shop_name) if campaign_type == "CPR"
                   else add_sub_cpc(client, customer_id, ad_group_resource, label, shop_name=shop_name))
        if not tree_ok:
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_listing_group_failed",
                    "error": _last_gads_error["msg"] or "listing group creation failed", "campaign_resource": campaign_resource}
    # Leave PAUSED (GSD campaigns are created paused); repair only completes the
    # missing structure so the shell is valid, it does not go live.
    return {"campaign_name": campaign_name, "action": "created",
            "reason": "retreed" if retree else "repaired", "campaign_resource": campaign_resource}


def _create_campaigns_for_shop(
    client: GoogleAdsClient,
    customer_id: str,
    mc_id: str,
    shop_name: str,
    shop_id: int,
    country: str,
    campaign_type: str,
    label_resource_name: str,
    branded: Any = None,
) -> List[Dict[str, Any]]:
    """
    Create all GSD campaigns for a shop (one per label).
    Returns a list of result dicts.

    ``branded`` is the shop's f_branded flag from Redshift; negatives are only
    added for non-branded shops (branded == 0), matching the original
    create GSD-campaigns.py.
    """
    tracking_template = TRACKING_TEMPLATES.get(country.upper(), TRACKING_TEMPLATES["NL"])
    results = []

    # One lookup of everything this shop_id already has in this account, matched per
    # label below on (shop name, shop_id, custom label) instead of the exact name.
    # A lookup failure must NOT read as "nothing exists" — that is how duplicate sets
    # get created — so the exception propagates to run_gsd_script's per-shop handler.
    candidates = _fetch_shop_campaign_candidates(client, customer_id, shop_id)

    # Which CPC structure this shop gets is decided from those same candidates, so a
    # shop already on the legacy pair is never given a bucket campaign as well.
    labels = _labels_for_shop(campaign_type, candidates, country, shop_name, shop_id)

    for label in labels:
        if _run_cancel["cancel"]:
            break  # stop before the next campaign; run_gsd_script marks the run cancelled
        campaign_name = _build_campaign_name(country, shop_name, shop_id, label)
        _last_gads_error["msg"] = None  # cleared per label; helpers set it on failure

        # Existing campaign? Complete an incomplete shell, else skip. Matched on shop
        # name (any variant) + shop_id + [label:X], ignoring naming-generation tokens
        # like [label_test] / [branche:H&L], excluding macro/micro variants.
        match = _match_existing_campaign(candidates, country, shop_name, shop_id, label)
        if match:
            existing = match["resource_name"]
            existing_name = match["campaign_name"]
            if existing_name != campaign_name:
                logger.info(
                    "Shop '%s' already has '%s' (id=%s, %s) — adopting that campaign "
                    "instead of creating '%s'",
                    shop_name, existing_name, match["campaign_id"], match["status"], campaign_name,
                )
            # Adopt it into the GSD-managed set. Without GSD_SCRIPT the campaign is
            # invisible to get_gsd_campaigns (the Campaigns-created table), to
            # _pause_campaigns_for_shop (which filters on that label) and to the
            # creation-date log — i.e. skipping the create would drop the shop out
            # of the tool entirely. Every legacy Toolstation campaign carries
            # 'Floodlight test jan 2026' or no label at all, never GSD_SCRIPT.
            #
            # AUDIT H4 — this used to sit inside the name-mismatch branch above, which
            # excluded exactly the cohort that needs it: the ~2,954 unlabelled campaigns
            # have a CORRECT name (their label application failed after create), so
            # existing_name == campaign_name and adoption never fired. Labelling is keyed
            # on the label being absent, not on the name differing — the same test
            # _pause_campaigns_for_shop already uses (`not info["labelled"]`). Only the
            # logger.info above stays inside the mismatch branch, where it is accurate.
            if label_resource_name and label_resource_name not in (match.get("labels") or []):
                _apply_label_to_campaign(client, customer_id, existing, label_resource_name)
            _apply_branded_label(client, customer_id, existing, branded)
            # Report the name that actually EXISTS, not the one we would have built:
            # _repair_campaign parses the label out of it and the run result feeds the
            # activity log / undo payload, which must point at the real campaign.
            res = _repair_campaign(
                client, customer_id, existing, existing_name, campaign_type, label, shop_name=shop_name)
            # A shop Redshift just switched ON whose campaigns already exist gets them
            # ENABLED (Joep, 2026-07-31). Brand-new campaigns still go live PAUSED.
            # Four conditions, all of them load-bearing:
            #   * the repair must not have errored — enabling a campaign with no ad group
            #     or listing tree would serve nothing and hide the breakage;
            #   * it must actually be PAUSED, so a no-op mutate is never sent;
            #   * the BID STRATEGY must already be paired (see BID_STRATEGY_PENDING). A
            #     campaign this script created is MANUAL_CPC until a colleague pairs the
            #     target-ROAS strategy in SA360, and only then may it go live. Without
            #     this check a second run of the same day would enable its OWN fresh
            #     creations without a bid strategy (Joep, 2026-07-31);
            #   * it must not carry GSD_LL_PAUSED — the low-linkage flow paused it
            #     deliberately and re-enables it itself once linkage recovers.
            if res.get("action") != "error" and match["status"] == "PAUSED":
                ll_rn = _lookup_label_resource(client, customer_id, LL_PAUSED_LABEL)
                if not _bid_strategy_ready(match.get("bidding_strategy_type")):
                    logger.info(
                        "Not enabling '%s': bid strategy is %s, so the SA360 pairing has "
                        "not happened yet", existing_name,
                        match.get("bidding_strategy_type") or "unset",
                    )
                    res["enable_skipped"] = "awaiting_bid_strategy"
                    if res.get("action") == "skipped":
                        res["reason"] = "awaiting_bid_strategy"
                elif ll_rn and ll_rn in (match.get("labels") or []):
                    logger.info(
                        "Not enabling '%s': it carries %s, so low-linkage owns its status",
                        existing_name, LL_PAUSED_LABEL,
                    )
                    res["enable_skipped"] = "ll_paused"
                    if res.get("action") == "skipped":
                        res["reason"] = "ll_paused_left_alone"
                elif _set_campaign_status_by_resource(client, customer_id, existing, "ENABLED"):
                    logger.info("Enabled existing campaign '%s' for shop '%s'", existing_name, shop_name)
                    # 'activated' rather than 'created': the run turned something ON, which
                    # the undo must be able to pause again — run_gsd_script therefore files
                    # it with the created ones, and the UI shows it as its own action.
                    res["action"] = "activated"
                    res["enabled_from"] = "PAUSED"
                else:
                    res["enable_error"] = _last_gads_error["msg"] or "enable failed"
                    logger.error("Failed to enable existing campaign '%s': %s",
                                 existing_name, res["enable_error"])
                    # AUDIT H7 — re-bucket, or the run reports a failure as "skipped",
                    # which reads as "nothing needed doing" when in fact it tried and
                    # failed and the campaign is still PAUSED. Only the skipped case is
                    # touched: if the campaign was actually repaired, action is "created"
                    # and that stays true — enable_error already carries the rest.
                    if res.get("action") == "skipped":
                        res["action"] = "error"
                        res["reason"] = "enable_failed"
                        res["error"] = res["enable_error"]
            results.append(res)
            continue

        # Create campaign
        campaign_resource = add_standard_shopping_campaign(
            client=client,
            customer_id=customer_id,
            campaign_name=campaign_name,
            merchant_id=mc_id,
            country=country,
            tracking_template=tracking_template,
            label_resource_name=label_resource_name,
        )
        if campaign_resource is None:
            # The create failed. Before filing an error, ask whether the campaign EXISTS
            # anyway — which is what a lost race looks like from this side. On 2026-09-01
            # eleven rows in the run result were exactly this: DUPLICATE_CAMPAIGN_NAME
            # ("the name is already assigned to another active or paused campaign") and
            # CONCURRENT_MODIFICATION, both raised because a second run had created the
            # very campaign this one was building, moments after this run snapshotted its
            # candidates. Reported as an error, that reads as "the campaign is missing"
            # when in truth it is there and merely unfinished — so the next run had to
            # rebuild it and the operator had a red row to chase for nothing.
            #
            # Deliberately NOT keyed on the error code: reality is the better test, and it
            # covers every wording Google may use. If the name genuinely does not exist,
            # nothing is adopted and the error is filed exactly as before. The run lock
            # (see _gsd_run_lock) should make this unreachable; it is the belt to that
            # brace, and it also covers the single-run case where `accounts.list`-style
            # lag hides a campaign this same run just made.
            create_error = _last_gads_error["msg"] or "campaign creation failed"
            adopted = check_campaign(client, customer_id, campaign_name)
            if adopted is None:
                results.append({
                    "campaign_name": campaign_name,
                    "action": "error",
                    "reason": "campaign_creation_failed",
                    "error": create_error,
                })
                continue
            logger.warning(
                "Create of '%s' failed (%s) but the campaign exists (%s) — adopting and "
                "completing it instead of reporting a failure",
                campaign_name, create_error, adopted,
            )
            if label_resource_name:
                _apply_label_to_campaign(client, customer_id, adopted, label_resource_name)
            _apply_branded_label(client, customer_id, adopted, branded)
            res = _repair_campaign(
                client, customer_id, adopted, campaign_name, campaign_type, label,
                shop_name=shop_name)
            # Keep the create error visible: the row is no longer red, and without this the
            # run would look like nothing had gone wrong at all.
            res["create_conflict"] = create_error
            if res.get("action") == "skipped":
                res["reason"] = "existed_after_create_conflict"
            results.append(res)
            continue

        # Apply the BRANDED_0 / BRANDED_1 label matching the shop's branded flag.
        _apply_branded_label(client, customer_id, campaign_resource, branded)

        if label == CPC_BUCKETS_LABEL:
            # Price-bucket structure: 14 ad groups, each with its own product ad and a
            # listing tree serving only its own custom_label_4 bucket. Built by the same
            # function the repair path uses, so a run that dies partway is completed by
            # the next one rather than leaving buckets permanently half-built.
            struct = _build_bucket_structure(
                client, customer_id, campaign_resource, shop_name=shop_name)
            if not struct["ok"]:
                # A cancel is not a failure: the loop above already stops a run at the
                # next campaign boundary without filing an error, and reporting one here
                # would make "I pressed cancel" look like "the run broke". The campaign
                # is PAUSED and _build_bucket_structure is idempotent, so a rerun
                # completes the remaining buckets.
                results.append({
                    "campaign_name": campaign_name,
                    "action": "skipped" if struct["reason"] == "cancelled" else "error",
                    "reason": struct["reason"],
                    "error": struct["error"],
                    "campaign_resource": campaign_resource,
                })
                continue
        else:
            # Create ad group
            ad_group_name = label  # ad group is named after the label (a/b/c/no_data/no_ean), matching the original script
            ad_group_resource = add_shopping_ad_group(
                client, customer_id, campaign_resource, ad_group_name,
                shop_name=shop_name,
            )
            if ad_group_resource is None:
                results.append({
                    "campaign_name": campaign_name,
                    "action": "error",
                    "reason": "ad_group_creation_failed",
                    "error": _last_gads_error["msg"] or "ad group creation failed",
                })
                continue

            # Create product ad. The campaign was created PAUSED, so a failure here
            # leaves a paused (non-spending) shell we report as an error rather than
            # a live campaign with no product ad.
            product_ad = add_shopping_product_ad_group_ad(client, customer_id, ad_group_resource, shop_name=shop_name)
            if product_ad is None:
                results.append({
                    "campaign_name": campaign_name,
                    "action": "error",
                    "reason": "product_ad_creation_failed",
                    "error": _last_gads_error["msg"] or "product ad creation failed",
                    "campaign_resource": campaign_resource,
                })
                continue

            # Create listing group tree
            if campaign_type == "CPR":
                tree_ok = add_sub_cpr(client, customer_id, ad_group_resource, label, shop_name=shop_name)
            else:
                tree_ok = add_sub_cpc(client, customer_id, ad_group_resource, label, shop_name=shop_name)
            if not tree_ok:
                results.append({
                    "campaign_name": campaign_name,
                    "action": "error",
                    "reason": "listing_group_creation_failed",
                    "error": _last_gads_error["msg"] or "listing group creation failed",
                    "campaign_resource": campaign_resource,
                })
                continue

        # Add negative keywords (best-effort) — ONLY for non-branded shops
        # (branded == 0), matching the original create GSD-campaigns.py. NULL/1
        # (branded or unknown) get no negatives.
        try:
            _non_branded = int(branded) == 0
        except (TypeError, ValueError):
            _non_branded = False
        if _non_branded:
            negatives = get_negatives(shop_name)
            if negatives:
                add_negative_keywords(client, customer_id, campaign_resource, negatives)

        # Leave the campaign PAUSED — the original script creates GSD campaigns
        # paused and never enables them; enabling is done separately/manually.
        results.append({
            "campaign_name": campaign_name,
            "action": "created",
            "campaign_resource": campaign_resource,
        })

    return results


def _pause_identity_matcher(shop_name: str):
    """Build the "is this campaign ours?" predicate used on the pause side.

    PAUSE_LABELS deliberately — both model vocabularies plus promo/tag_toppers — so a shop
    switched off for a country goes fully dark even when its derived model flipped on the
    way out (see the BE_CPC/BE_CPR note on ACCOUNTS).

    AUDIT H6, Phase 2: this used to be a closure inside _pause_campaigns_for_shop and a
    hand-copied set of the same three tests inside preview_gsd_script. Two copies of one
    rule is how preview and run drifted apart in the first place.
    """
    label_tokens = {f"[label:{l}]".lower() for l in PAUSE_LABELS}
    name_tokens = [f"[shop:{v}]".lower() for v in _shop_name_variants(shop_name)]

    def _is_ours(name: str) -> bool:
        low = (name or "").lower()
        if not any(t in low for t in name_tokens):
            return False
        if not any(t in low for t in label_tokens):
            return False
        return not (_MACRO_MICRO_RE.search(name) or _RETIRED_RE.search(name))

    return _is_ours


def find_pausable_campaigns(
    client: GoogleAdsClient, customer_id: str, shop_name: str, shop_id: Any,
    label_resource_name: Optional[str] = None,
    candidates: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Every ENABLED campaign in this account that a pause would touch for this shop.

    THE single source of truth for the pause side — both _pause_campaigns_for_shop and
    preview_gsd_script call it, so the preview can no longer under-report what the run
    will do (AUDIT H6).

    Two sources, because either alone misses real campaigns:
      (a) label-based: name carries [shop:<variant>] AND the campaign has GSD_SCRIPT.
          Note it needs NEITHER shop_id NOR advertising_channel_type — which is exactly
          why the preview used to disagree: it only ran source (b), so a shop whose
          Redshift shop_id is NULL previewed as "0 to pause" while the run happily paused
          everything this query found.
      (b) identity-based over the shop_id candidates: an unlabelled campaign that is
          nonetheless ours by name + label token (the ~2,954 unlabelled cohort).

    `candidates` lets a caller pass a list it already fetched for the create side, so the
    preview keeps its one-lookup-per-(shop, account) property. `label_resource_name` is
    account-scoped and only decides the `labelled` flag; source (a) filters on the label
    NAME in GAQL, so nothing here has to create a label — a preview stays read-only.

    Returns {campaign_id: {campaign_name, resource_name, labelled}}. Raises
    GoogleAdsException on a failed lookup: the callers must decide what to do, and neither
    may treat a failure as "nothing to pause".
    """
    is_ours = _pause_identity_matcher(shop_name)
    ga_service = client.get_service("GoogleAdsService")
    by_id: Dict[str, Dict[str, Any]] = {}

    # (a) label-based, per shop-name variant — a shop whose Redshift name gained a
    # '|NL' would otherwise keep its bare-name campaigns ENABLED forever.
    for variant in _shop_name_variants(shop_name):
        name_pattern = _name_contains_regexp(f"[shop:{variant}]")
        for row in ga_service.search(customer_id=customer_id, query=f"""
                SELECT campaign.id, campaign.name, campaign.status, campaign.resource_name
                FROM campaign_label
                WHERE campaign.name REGEXP_MATCH '{name_pattern}'
                  AND campaign.status = 'ENABLED'
                  AND label.name = '{SCRIPT_LABEL}'"""):
            by_id.setdefault(str(row.campaign.id), {
                "campaign_name": row.campaign.name,
                "resource_name": row.campaign.resource_name,
                "labelled": True,
            })

    # (b) identity-based over everything this shop_id has in the account.
    if shop_id is not None:
        if candidates is None:
            candidates = _fetch_shop_campaign_candidates(client, customer_id, shop_id)
        for c in candidates:
            if c["status"] != "ENABLED" or not is_ours(c["campaign_name"]):
                continue
            by_id.setdefault(c["campaign_id"], {
                "campaign_name": c["campaign_name"],
                "resource_name": c["resource_name"],
                "labelled": bool(label_resource_name
                                 and label_resource_name in (c["labels"] or [])),
            })
    return by_id


def _pause_campaigns_for_shop(
    client: GoogleAdsClient,
    customer_id: str,
    shop_name: str,
    shop_id: Any = None,
    country: str = "NL",
    campaign_type: str = "CPR",
    label_resource_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Pause this shop's live GSD campaigns in one account. Returns a list of result dicts —
    including one when there was NOTHING to pause, so the shop can never vanish from the
    run output.

    TWO WAYS IN, because the GSD_SCRIPT label alone was not enough (Joep, 2026-07-31:
    "the preview says it will pause Elektroshop.nl, the run pauses nothing, and the shop
    is missing from the output"):
      * the campaign carries GSD_SCRIPT — the original rule; or
      * the campaign IS one of ours by identity (shop-name variant + [shop_id:N] +
        one of this run's [label:X] tokens, macro/micro/OUD excluded) — the same test the
        create path uses. Elektroshop.nl's five ENABLED campaigns are canonical GSD names
        carrying NO label at all (2.954 campaigns are in that state, because the label is
        applied in a separate best-effort call after the create), so the label filter left
        a switched-off shop spending.
    A campaign matched only by identity gets GSD_SCRIPT attached, so the estate converges
    instead of drifting further.

    The identity test uses PAUSE_LABELS — both model vocabularies plus promo/tag_toppers —
    so `campaign_type` deliberately does NOT narrow it: a shop off for a country goes fully
    dark there even when its derived model flipped on the way out.
    """
    results = []
    try:
        # The two-source lookup lives in find_pausable_campaigns, which the preview calls
        # too — that shared call is the whole point of AUDIT H6/Phase 2.
        by_id = find_pausable_campaigns(
            client, customer_id, shop_name, shop_id, label_resource_name)
    except GoogleAdsException as ex:
        # A failed lookup must NOT fall through to "nothing to pause" — that would
        # silently leave a switched-off shop's campaigns running.
        logger.error("Error finding campaigns to pause for '%s': %s", shop_name, ex)
        return [{"shop_name": shop_name, "action": "error", "reason": str(ex)}]

    if not by_id:
        # Visible, not silent: the shop appears in the run output as skipped with a reason.
        logger.info("Nothing to pause for '%s' in %s — no live GSD campaigns matched",
                    shop_name, customer_id)
        return [{"shop_name": shop_name, "campaign_name": f"[shop:{shop_name}]",
                 "action": "skipped", "reason": "no_live_campaigns_to_pause"}]

    for campaign_id, info in by_id.items():
        campaign_name = info["campaign_name"]
        result = pause_campaign(customer_id, campaign_id)
        action = "paused" if result["success"] else "error"
        if action == "paused":
            logger.info(
                "Paused campaign '%s' (id=%s) in account %s for shop '%s'%s",
                campaign_name, campaign_id, customer_id, shop_name,
                "" if info["labelled"] else " (matched by name, not by label)",
            )
            # Adopt it, so next time the label alone finds it.
            if not info["labelled"] and label_resource_name:
                _apply_label_to_campaign(client, customer_id, info["resource_name"],
                                         label_resource_name)
        else:
            logger.error(
                "Failed to pause campaign '%s' (id=%s) in account %s: %s",
                campaign_name, campaign_id, customer_id, result,
            )
        results.append({
            "campaign_name": campaign_name,
            "campaign_id": campaign_id,
            "action": action,
            "detail": result,
            "reason": None if info["labelled"] else "matched_by_name",
        })

    return results


# Progress for the GSD preview, polled by the frontend to drive its progress bar.
# Single-flight is fine here (one preview at a time in practice).
_preview_progress: Dict[str, Any] = {"current": 0, "total": 0, "running": False}


def get_preview_progress() -> Dict[str, Any]:
    return dict(_preview_progress)


def preview_gsd_script(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> Dict[str, Any]:
    """
    Dry-run of run_gsd_script: report how many GSD campaigns WOULD be created
    and how many WOULD be paused for the current shop changes, without changing
    anything.

    Mirrors run_gsd_script's shop -> country -> label expansion but issues only
    read-only queries. No Merchant Center accounts, budgets, campaigns, ad
    groups, or status changes are created or modified.

    Returns a summary dict with totals (to_create, to_pause, already_exists) and
    a per-shop breakdown.
    """
    client = _get_client()
    summary: Dict[str, Any] = {
        "date": date_str or datetime.now().strftime("%Y-%m-%d"),
        "preview": True,
        "to_create": 0,
        "already_exists": 0,
        # Subset of already_exists: matches this run would switch ON (existing campaign,
        # currently PAUSED, bid strategy paired, not held by the low-linkage flow).
        "to_activate": 0,
        # Matches left PAUSED because the SA360 bid-strategy pairing has not happened yet.
        "awaiting_bid_strategy": 0,
        "to_pause": 0,
        "shops_aan": 0,
        "shops_uit": 0,
        "by_shop": [],
        # Flat list of the affected campaigns for a table view.
        # Each: {campaign_name, action: create|activate|skip|pause, shop_name, country, type}
        "campaigns": [],
        "errors": [],
    }

    _preview_progress.update({"current": 0, "total": 0, "running": True})

    try:
        changes = get_redshift_shop_changes(date_str, shop_names, included)
    except Exception as ex:
        logger.error("Preview: failed to get shop changes from Redshift: %s", ex)
        summary["errors"].append({"step": "redshift_query", "error": str(ex)})
        _preview_progress["running"] = False
        return summary

    if not changes:
        logger.info("Preview: no shop changes found for %s", summary["date"])
        _preview_progress["running"] = False
        return summary

    _preview_progress["total"] = len(changes)
    ga_service = client.get_service("GoogleAdsService")

    for change in changes:
        shop_id = change.get("shop_id")
        shop_name = change.get("shop_name", "")
        actie = change.get("actie", "")
        model = change.get("model", "CPR")

        campaign_type = model.upper() if model else "CPR"
        if campaign_type not in ("CPR", "CPC"):
            campaign_type = "CPR"

        # Same country/label expansion as run_gsd_script: only the country whose
        # GSD flag flipped (from the feed's `kolom`), NOT every model country.
        country = KOLOM_COUNTRY.get(change.get("kolom"))
        countries = [country] if country else []
        # `labels` is NOT decided here any more: which CPC structure a shop gets depends
        # on what it already has in the account, so it is resolved per country from the
        # candidate list below — by the same _labels_for_shop() the run calls.

        shop_row: Dict[str, Any] = {
            "shop_name": shop_name,
            "shop_id": shop_id,
            "actie": actie,
            "type": campaign_type,
            "to_create": 0,
            "already_exists": 0,
            "to_activate": 0,
            "awaiting_bid_strategy": 0,
            "to_pause": 0,
        }
        if actie == "aan":
            summary["shops_aan"] += 1
        elif actie == "uit":
            summary["shops_uit"] += 1

        for country in countries:
            account_info = _find_account_info(country, campaign_type)
            if account_info is None:
                summary["errors"].append({
                    "shop_name": shop_name,
                    "country": country,
                    "type": campaign_type,
                    "error": "no_account_config",
                })
                continue
            customer_id = account_info["customer_id"]

            # One read-only lookup of everything this shop_id has in this account,
            # reused for both the create and the pause count. Same helper the run uses,
            # so preview and run can never disagree about what "already exists" means.
            try:
                candidates = _fetch_shop_campaign_candidates(client, customer_id, shop_id)
            except GoogleAdsException as ex:
                logger.error("Preview: campaign lookup failed for '%s' in %s: %s",
                             shop_name, country, ex)
                summary["errors"].append({
                    "shop_name": shop_name,
                    "country": country,
                    "error": str(ex),
                })
                continue

            if actie == "aan":
                # Created only when no campaign matches (shop name variant, shop_id,
                # custom label) — matches _create_campaigns_for_shop. Reports the name
                # that really exists, so the table shows what will be adopted.
                ll_rn = _lookup_label_resource(client, customer_id, LL_PAUSED_LABEL)
                labels = _labels_for_shop(
                    campaign_type, candidates, country, shop_name, shop_id)
                for label in labels:
                    campaign_name = _build_campaign_name(country, shop_name, shop_id, label)
                    match = _match_existing_campaign(candidates, country, shop_name, shop_id, label)
                    if match:
                        shop_row["already_exists"] += 1
                    else:
                        shop_row["to_create"] += 1
                    # Same conditions the run applies before enabling a match, so the
                    # preview says "activate" exactly when the run would.
                    ready = bool(match) and _bid_strategy_ready(match.get("bidding_strategy_type"))
                    held = bool(match) and bool(ll_rn and ll_rn in (match.get("labels") or []))
                    will_activate = bool(match) and match["status"] == "PAUSED" and ready and not held
                    if will_activate:
                        shop_row["to_activate"] = shop_row.get("to_activate", 0) + 1
                    elif match and match["status"] == "PAUSED" and not ready:
                        # Waiting on the manual SA360 bid-strategy pairing — worth showing,
                        # it is the queue that gates going live.
                        shop_row["awaiting_bid_strategy"] = shop_row.get("awaiting_bid_strategy", 0) + 1
                    summary["campaigns"].append({
                        "campaign_name": match["campaign_name"] if match else campaign_name,
                        # AUDIT decision (Joep, 2026-08-05): "skip" -> "skip_or_repair".
                        # A match means the run will not CREATE the campaign, but it still
                        # calls _repair_campaign on it, which can add a missing ad group,
                        # listing group or budget link and then reports action "created"
                        # with reason "repaired". So "skip" promised read-only and could not
                        # keep it. Naming it honestly costs nothing; actually PREDICTING it
                        # would cost three extra GAQL reads per match (ad group, listing
                        # group, budget) on every previewed campaign, and the preview
                        # already runs one query per (shop, account).
                        "action": "activate" if will_activate else ("skip_or_repair" if match else "create"),
                        "shop_name": shop_name,
                        "country": country,
                        "type": campaign_type,
                        # Read off the label this row will be built with. On the create
                        # side that agrees with `type` by construction; it is carried
                        # anyway so every previewed row — create and pause alike — has
                        # the field, and the pause rows below are where it earns its
                        # keep (there it can legitimately differ from `type`).
                        "model": _MODEL_BY_LABEL.get(label),
                    })
            elif actie == "uit":
                # AUDIT H6, Phase 2 — the preview now asks the SAME function the run uses,
                # find_pausable_campaigns, instead of re-implementing its rules. Before this,
                # preview ran only the identity source over the shop_id candidates, so a shop
                # whose Redshift shop_id is NULL previewed as "0 to pause" while the run's
                # label-based source (which needs neither shop_id nor SHOPPING) paused
                # everything it found — divergence in the dangerous direction.
                #
                # Same accounts the run sweeps: the live one plus any legacy account, or the
                # preview would under-report again for a different reason.
                for sweep_cid in _pause_customer_ids(country, customer_id):
                    # A label resource name is account-scoped, and it is looked up READ-ONLY:
                    # ensure_campaign_label_exists() would create the label, and a preview
                    # must mutate nothing. It only decides the `labelled` flag — source (a)
                    # filters on the label name in GAQL, so nothing needs creating.
                    gsd_label_rn = _lookup_label_resource(client, sweep_cid, SCRIPT_LABEL)
                    # Reuse the candidates already fetched above for the live account so the
                    # preview keeps one lookup per (shop, account).
                    try:
                        pausable = find_pausable_campaigns(
                            client, sweep_cid, shop_name, shop_id,
                            label_resource_name=gsd_label_rn,
                            candidates=candidates if sweep_cid == customer_id else None,
                        )
                    except GoogleAdsException as ex:
                        logger.error("Preview: pause lookup failed for '%s' in %s: %s",
                                     shop_name, sweep_cid, ex)
                        summary["errors"].append({
                            "shop_name": shop_name,
                            "country": country,
                            "customer_id": sweep_cid,
                            "error": str(ex),
                        })
                        continue
                    for info in pausable.values():
                        shop_row["to_pause"] += 1
                        summary["campaigns"].append({
                            "campaign_name": info["campaign_name"],
                            "action": "pause",
                            "shop_name": shop_name,
                            "country": country,
                            "type": campaign_type,
                            # Read off the campaign that EXISTS, not off the shop's
                            # current model — a shop's model can flip on the very day it
                            # goes off, and what gets paused is whatever was built.
                            "model": _parse_campaign_name(info["campaign_name"]).get("model"),
                            "customer_id": sweep_cid,
                            "detail": "" if info["labelled"] else "matched by name (no GSD_SCRIPT label)",
                        })

        summary["to_create"] += shop_row["to_create"]
        summary["already_exists"] += shop_row["already_exists"]
        summary["to_activate"] += shop_row.get("to_activate", 0)
        summary["awaiting_bid_strategy"] += shop_row.get("awaiting_bid_strategy", 0)
        summary["to_pause"] += shop_row["to_pause"]
        summary["by_shop"].append(shop_row)
        _preview_progress["current"] += 1

    _preview_progress["running"] = False
    logger.info(
        "GSD preview: %d to create, %d to activate, %d awaiting bid strategy, "
        "%d to pause, %d already exist across %d shops",
        summary["to_create"], summary["to_activate"], summary["awaiting_bid_strategy"],
        summary["to_pause"], summary["already_exists"], len(changes),
    )
    return summary


# ---------------------------------------------------------------------------
# Post-run structural verification
# ---------------------------------------------------------------------------


def _check_bucket_campaign_structure(ga, customer_id: str, campaign_id: str) -> List[str]:
    """Structural issues for a price-bucket campaign, one entry per broken bucket
    (e.g. 'no_ad_group:2594+'). Empty list == all 14 buckets complete."""
    ad_groups: Dict[str, str] = {}
    for row in ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group.id, ad_group.name FROM ad_group "
            f"WHERE campaign.id = {campaign_id} AND ad_group.status != 'REMOVED'")):
        ad_groups[row.ad_group.name] = str(row.ad_group.id)

    ads: set = set()
    for row in ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group.id FROM ad_group_ad WHERE campaign.id = {campaign_id} "
            f"AND ad_group_ad.status != 'REMOVED'")):
        ads.add(str(row.ad_group.id))

    crits_by_ag: Dict[str, list] = {}
    for row in ga.search(customer_id=customer_id, query=(
            "SELECT ad_group.id, ad_group_criterion.listing_group.type, "
            "ad_group_criterion.listing_group.parent_ad_group_criterion, "
            "ad_group_criterion.negative, "
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.value "
            f"FROM ad_group_criterion WHERE campaign.id = {campaign_id} "
            "AND ad_group_criterion.type = 'LISTING_GROUP' "
            "AND ad_group_criterion.status != 'REMOVED'")):
        crits_by_ag.setdefault(str(row.ad_group.id), []).append(row.ad_group_criterion)

    issues: List[str] = []
    for bucket_label, _bid in CPC_BUCKET_BIDS:
        ad_group_id = ad_groups.get(bucket_label)
        if ad_group_id is None:
            issues.append(f"no_ad_group:{bucket_label}")
            continue
        if ad_group_id not in ads:
            issues.append(f"no_product_ad:{bucket_label}")
        crits = crits_by_ag.get(ad_group_id, [])
        if not crits:
            issues.append(f"no_listing_group:{bucket_label}")
        elif not _bucket_tree_ok(crits, bucket_label):
            issues.append(f"wrong_listing_tree:{bucket_label}")
    return issues


def _check_campaign_structure(
    ga, customer_id: str, campaign_id: str, campaign_name: str, campaign_type: str
) -> List[str]:
    """
    Return the list of structural issues for one campaign (empty list == OK):
    no_ad_group / no_product_ad / no_listing_group / wrong_listing_tree. Mirrors
    the completeness checks _repair_campaign uses, so "verify says bad" lines up
    with "a rerun would repair it".
    """
    issues: List[str] = []
    # The bucket campaign is 14 ad groups; checking only ags[0] would call it complete
    # on the strength of one bucket. Verify every bucket has an ad group, a product ad
    # and a tree targeting its OWN custom_label_4 value, and name the buckets that fail
    # so the warning says which ones a rerun will repair.
    if _parse_campaign_name(campaign_name).get("label") == CPC_BUCKETS_LABEL:
        return _check_bucket_campaign_structure(ga, customer_id, campaign_id)

    # Scope the ad-group-child checks to the LIVE ad group, mirroring
    # _repair_campaign — a REMOVED sibling ad group's ads/criteria stay
    # non-REMOVED and would otherwise mask a broken live ad group.
    ags = list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group.id FROM ad_group "
        # Same ORDER BY as _repair_campaign, for the same reason.
        f"WHERE campaign.id = {campaign_id} AND ad_group.status != 'REMOVED' "
        f"ORDER BY ad_group.id")))
    if not ags:
        issues.append("no_ad_group")
        return issues  # nothing else to check without a live ad group
    ad_group_id = ags[0].ad_group.id
    if not list(ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group_ad.ad.id FROM ad_group_ad "
            f"WHERE ad_group.id = {ad_group_id} AND ad_group_ad.status != 'REMOVED'"))):
        issues.append("no_product_ad")
    has_lg = bool(list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group_criterion.criterion_id FROM ad_group_criterion "
        f"WHERE ad_group.id = {ad_group_id} AND ad_group_criterion.type = 'LISTING_GROUP' "
        f"AND ad_group_criterion.status != 'REMOVED'"))))
    if not has_lg:
        issues.append("no_listing_group")
    else:
        label = _parse_campaign_name(campaign_name).get("label")
        if label and not _tree_targets_label(ga, customer_id, ad_group_id, campaign_type, label):
            issues.append("wrong_listing_tree")
    return issues


def verify_run_campaigns(
    client: Optional[GoogleAdsClient],
    campaigns: List[Dict[str, Any]],
    recheck_delay: int = 15,
) -> Dict[str, Any]:
    """
    Final safety net: confirm each created/repaired campaign has an ad group, a
    product ad, and a correctly-targeted listing-group tree. Campaigns flagged on
    the first pass are re-checked once after ``recheck_delay`` seconds so a fresh
    structure that hasn't propagated to the read API yet is not a false alarm.

    ``campaigns`` items need customer_id, campaign_id, campaign_name and (for tree
    targeting) type. Returns {checked, ok, problems:[{..., issues:[...]}]}.
    """
    client = client or _get_client()
    ga = client.get_service("GoogleAdsService")
    _fields = ("campaign_name", "campaign_id", "customer_id", "shop_name", "country", "type")

    problems: List[Dict[str, Any]] = []
    checked = 0
    for c in campaigns:
        camp_id = c.get("campaign_id")
        cust = c.get("customer_id")
        if not camp_id or not cust:
            continue
        checked += 1
        issues = _check_campaign_structure(
            ga, cust, camp_id, c.get("campaign_name", ""), c.get("type", "CPR"))
        if issues:
            entry = {k: c.get(k) for k in _fields}
            entry["issues"] = issues
            problems.append(entry)

    # Re-check only the flagged ones once, after a delay, to filter propagation lag.
    if problems and recheck_delay:
        time.sleep(recheck_delay)
        still: List[Dict[str, Any]] = []
        for p in problems:
            issues = _check_campaign_structure(
                ga, p["customer_id"], p["campaign_id"], p.get("campaign_name", ""), p.get("type", "CPR"))
            if issues:
                p["issues"] = issues
                still.append(p)
        problems = still

    result = {"checked": checked, "ok": checked - len(problems), "problems": problems}
    if problems:
        for p in problems:
            logger.warning("VERIFY: campaign '%s' (id=%s) incomplete: %s — a rerun will repair it",
                           p.get("campaign_name"), p.get("campaign_id"), ", ".join(p["issues"]))
    else:
        logger.info("VERIFY: all %d created/repaired campaigns are structurally complete", checked)
    return result


def run_gsd_script(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
    verify: bool = True,
) -> Dict[str, Any]:
    """Run the GSD flow, with at most one run live at a time.

    A thin wrapper so the lock covers the WHOLE run including its side-logs, and so every
    caller gets it — the endpoint, a future scheduler, a REPL. Raises GsdRunInProgress when
    another run already holds it; see _gsd_run_lock for what two concurrent runs did on
    2026-09-01.
    """
    with _gsd_run_lock():
        return _run_gsd_script_unlocked(date_str, shop_names, included, verify)


def _run_gsd_script_unlocked(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
    verify: bool = True,
) -> Dict[str, Any]:
    """
    Main GSD campaign creation/pausing flow. Call run_gsd_script, not this: this one
    assumes the caller already holds the run lock.

    For each shop change from Redshift:
    - If action='aan': find/create MC account, link to Google Ads,
      create campaigns with labels, ad groups, product groups, negative keywords.
    - If action='uit': pause campaigns.

    Parameters
    ----------
    date_str : date string (YYYY-MM-DD), defaults to today.
    shop_names : optional list of shop names to process (filter).
    included : if True, also process shops already included.

    Returns a results dict summarizing what was done.
    """
    client = _get_client()
    # Labels are per-account and stable across a run — resolve each customer_id
    # once instead of per shop × country (#14).
    label_cache: Dict[str, str] = {}
    overall_results: Dict[str, Any] = {
        "date": date_str or datetime.now().strftime("%Y-%m-%d"),
        "created": [],
        "paused": [],
        "errors": [],
        "skipped": [],
        "cancelled": False,
    }
    sheet_rows: List[List[Any]] = []                        # one row per processed shop, for the log sheet
    mc_created_rows: List[tuple] = []                        # (shop_name, shop_id, mc_created, country, date) for pa.mc_ids_efficy
    run_date = datetime.strptime(overall_results["date"], "%Y-%m-%d").strftime("%d-%m-%Y")  # dd-mm-yyyy from the change date, not "now"
    date_ymd = overall_results["date"].replace("-", "")     # YYYYMMDD from the change date, not "now"
    _run_cancel["cancel"] = False  # fresh run
    _run_progress.update({"current": 0, "total": 0, "running": True})

    # Get shop changes from Redshift
    # AUDIT MED — neither of these two exits may return early any more. reconcile_run_logs
    # is what heals a half-finished EARLIER run, and it lives at the end of this function:
    # bailing out here meant "just run it again" healed nothing on a day with no shop
    # changes, or when the Redshift query itself failed — i.e. exactly the quiet day you
    # would use to catch up. Both now fall through to the side-logs with an empty change
    # list, which is a no-op for the three write steps and a real run for reconcile.
    changes: List[Dict[str, Any]] = []
    try:
        changes = get_redshift_shop_changes(date_str, shop_names, included)
    except Exception as ex:
        logger.error("Failed to get shop changes from Redshift: %s", ex)
        overall_results["errors"].append({"step": "redshift_query", "error": str(ex)})

    if not changes:
        logger.info("No shop changes for %s — falling through to reconcile", overall_results["date"])

    logger.info("Processing %d shop changes", len(changes))
    _run_progress["total"] = len(changes)

    # AUDIT structural risk, outer half. The per-shop boundary inside the country loop
    # covers the work itself; this covers anything that escapes it — the per-change
    # preamble, or a bug in the boundary. Falling through instead of propagating is the
    # point: the four side-logs and the progress reset below MUST run, because the last
    # of them (reconcile_run_logs) is what repairs a half-finished run.
    try:
        for idx, change in enumerate(changes):
            _run_progress["current"] = idx
            if _run_cancel["cancel"]:
                overall_results["cancelled"] = True
                logger.info("GSD run cancelled after %d/%d shop changes", idx, len(changes))
                break
            shop_id = change.get("shop_id")
            shop_name = change.get("shop_name", "")
            actie = change.get("actie", "")
            model = change.get("model", "CPR")
            branded_yes = str(change.get("branded", "")).strip().lower() in ("1", "true", "t", "ja", "yes")

            # Determine campaign type
            campaign_type = model.upper() if model else "CPR"
            if campaign_type not in ("CPR", "CPC"):
                campaign_type = "CPR"

            # Act only on the country whose GSD flag flipped (the feed's `kolom`),
            # NOT every model country — a shop flagged for one country must not
            # create/pause campaigns in the others.
            country = KOLOM_COUNTRY.get(change.get("kolom"))
            countries = [country] if country else []

            for country in countries:
                # AUDIT structural risk — a per-shop/country exception boundary. Without
                # it one shop's lookup failure aborted the WHOLE run, which skipped
                # _log_run_to_sheet, push_mc_ids_to_redshift, record_created_campaigns and
                # reconcile_run_logs — i.e. the recovery mechanism was skipped by the exact
                # failure it exists to repair — and left _run_progress['running'] True
                # forever. Now the run carries on with the next shop and the failure is
                # reported in overall_results['errors'] like any other.
                try:
                    if _run_cancel["cancel"]:
                        overall_results["cancelled"] = True
                        break
                    account_info = _find_account_info(country, campaign_type)
                    if account_info is None:
                        overall_results["errors"].append({
                            "shop_name": shop_name,
                            "country": country,
                            "type": campaign_type,
                            "error": "no_account_config",
                        })
                        continue

                    customer_id = account_info["customer_id"]
                    mc_parent_id = account_info["mc_id"]

                    if actie == "aan":
                        # Ensure label exists (cached per account)
                        try:
                            if customer_id not in label_cache:
                                label_cache[customer_id] = ensure_campaign_label_exists(client, customer_id)
                            label_resource = label_cache[customer_id]
                        except Exception as ex:
                            overall_results["errors"].append({
                                "shop_name": shop_name,
                                "step": "ensure_label",
                                "error": str(ex),
                            })
                            continue

                        # Get or create MC sub-account and link
                        mc_id, mc_was_created = _get_or_create_mc_account(mc_parent_id, shop_name, customer_id, country)
                        if mc_id is None:
                            overall_results["errors"].append({
                                "shop_name": shop_name,
                                "country": country,
                                "step": "mc_account",
                                "error": _last_mc_error["msg"] or "failed_to_get_or_create_mc_account",
                            })
                            continue

                        # Log ONLY newly created MC sub-accounts to pa.mc_ids_efficy at the
                        # end of the run, mirroring the original create GSD-campaigns.py
                        # (which pushed its `mc_created` list; existing accounts are skipped).
                        if mc_was_created:
                            try:
                                _mc_id_int = int(mc_id)
                            except (TypeError, ValueError):
                                _mc_id_int = None
                            mc_created_rows.append(
                                (shop_name, shop_id, _mc_id_int, country, date_ymd)
                            )

                        # Create campaigns
                        campaign_results = _create_campaigns_for_shop(
                            client=client,
                            customer_id=customer_id,
                            mc_id=mc_id,
                            shop_name=shop_name,
                            shop_id=shop_id,
                            country=country,
                            campaign_type=campaign_type,
                            label_resource_name=label_resource,
                            branded=change.get("branded"),
                        )

                        for cr in campaign_results:
                            cr["shop_name"] = shop_name
                            # shop_id is NOT decoration: record_created_campaigns() keys the
                            # creation-date table on (shop_id, country) and skips any entry
                            # without it. Omitting it here made every post-run date log a
                            # silent no-op, which is why the Campaigns-created Date column
                            # went blank for everything created after the 2026-07-16 seed.
                            cr["shop_id"] = shop_id
                            cr["country"] = country
                            cr["type"] = campaign_type
                            cr["customer_id"] = customer_id
                            # Expose the numeric id (parsed from the resource name) so a
                            # later "undo" can pause exactly what was created.
                            res = cr.get("campaign_resource") or ""
                            if res:
                                cr["campaign_id"] = res.rstrip("/").split("/")[-1]
                            # 'activated' (an existing campaign this run switched ON) files with
                            # the created ones: the undo/reset payload is built from this list and
                            # pausing it again is the correct inverse. The action string survives,
                            # so the UI can still show it as its own thing.
                            if cr["action"] in ("created", "activated"):
                                overall_results["created"].append(cr)
                            elif cr["action"] == "skipped":
                                overall_results["skipped"].append(cr)
                            else:
                                overall_results["errors"].append(cr)

                        # Log one row for this shop (mirrors the original sheet).
                        # AUDIT MED — 'activated' counts as well. It only counted "created", so a
                        # run that turned five existing campaigns ON logged
                        # `campagnes aangemaakt? nee` in the sheet. run_gsd_script deliberately
                        # files activated alongside created for exactly this reason.
                        created_count = sum(1 for cr in campaign_results
                                            if cr.get("action") in ("created", "activated"))
                        sheet_rows.append([
                            run_date, str(shop_id or ""), shop_name or "", campaign_type,
                            str(mc_id or ""), country or "",
                            ("ja" if branded_yes else "nee"),
                            ("ja" if created_count > 0 else "nee"),
                            "aan",
                        ])

                    elif actie == "uit":
                        # Pause campaigns in every account that can still hold this shop's
                        # campaigns for the country — the live one plus any legacy account.
                        pause_results: List[Dict[str, Any]] = []
                        nothing_found: List[Dict[str, Any]] = []
                        for pause_cid in _pause_customer_ids(country, customer_id):
                            rows = _pause_campaigns_for_shop(
                                client=client,
                                customer_id=pause_cid,
                                shop_name=shop_name,
                                shop_id=shop_id,
                                country=country,
                                campaign_type=campaign_type,
                                label_resource_name=ensure_campaign_label_exists(
                                    client, pause_cid, SCRIPT_LABEL),
                            )
                            for r in rows:
                                r["customer_id"] = pause_cid
                                if r.get("reason") == "no_live_campaigns_to_pause":
                                    nothing_found.append(r)
                                else:
                                    pause_results.append(r)
                        # The sentinel exists so a shop can never vanish from the output — but it
                        # is per shop, not per swept account. An empty legacy account must not add
                        # a "nothing to pause" line next to campaigns the live account did pause.
                        if not pause_results:
                            pause_results = nothing_found[:1]

                        for pr in pause_results:
                            pr["shop_name"] = shop_name
                            pr["country"] = country
                            pr["type"] = campaign_type
                            pr.setdefault("customer_id", customer_id)
                            if pr["action"] == "paused":
                                overall_results["paused"].append(pr)
                            elif pr["action"] == "skipped":
                                # "nothing to pause" is information, not an error — but it MUST be
                                # in the output, or the shop silently disappears from the run.
                                overall_results["skipped"].append(pr)
                            else:
                                overall_results["errors"].append(pr)

                        # Log one row for this shop (MC ID not looked up on pause; matches
                        # the original: op brand? = n.v.t., campagnes aangemaakt? = nee).
                        sheet_rows.append([
                            run_date, str(shop_id or ""), shop_name or "", campaign_type,
                            "", country or "", "n.v.t.", "nee", "uit",
                        ])
                except Exception as ex:
                    logger.exception("Unhandled error for shop '%s' (%s) — continuing",
                                     shop_name, country)
                    overall_results["errors"].append({
                        "shop_name": shop_name,
                        "country": country,
                        "step": "shop_country_unhandled",
                        "error": str(ex)[:300],
                    })
                    continue
    except Exception as ex:
        logger.exception("GSD run loop aborted — falling through to the side-logs")
        overall_results["errors"].append({"step": "run_loop", "error": str(ex)[:300]})

    # Safety net: cancel may fire on the last shop/label, so the loops above
    # end naturally without hitting a cancel check — flag it here regardless.
    if _run_cancel["cancel"]:
        overall_results["cancelled"] = True

    logger.info(
        "GSD script complete: %d created, %d paused, %d skipped, %d errors",
        len(overall_results["created"]),
        len(overall_results["paused"]),
        len(overall_results["skipped"]),
        len(overall_results["errors"]),
    )
    if overall_results["paused"]:
        for p in overall_results["paused"]:
            logger.info(
                "  PAUSED: '%s' (id=%s) shop=%s country=%s type=%s",
                p.get("campaign_name"), p.get("campaign_id"),
                p.get("shop_name"), p.get("country"), p.get("type"),
            )
    if overall_results["created"]:
        for c in overall_results["created"]:
            logger.info(
                "  CREATED: '%s' (id=%s) shop=%s country=%s type=%s",
                c.get("campaign_name"), c.get("campaign_id"),
                c.get("shop_name"), c.get("country"), c.get("type"),
            )

    # Append this run to the log sheet (best-effort; never fails the run).
    overall_results["sheet_log"] = _log_run_to_sheet(sheet_rows)

    # Persist newly created MC ids to pa.mc_ids_efficy (best-effort; never fails
    # the run), mirroring push_to_redshift() in the original create GSD-campaigns.py.
    overall_results["mc_ids_pushed"] = push_mc_ids_to_redshift(mc_created_rows)

    # Persist creation dates of the campaigns created this run (best-effort;
    # never fails the run) so the Campaigns-created Date column stays populated
    # going forward, independent of change_event's ~30-day retention.
    # AUDIT MED — only genuinely CREATED campaigns get a creation date. overall_results
    # ["created"] deliberately also holds 'activated' entries (so undo can pause them),
    # but an activated campaign already existed: stamping the run's date as its creation
    # date is simply wrong, and ON CONFLICT DO NOTHING makes that wrong date permanent.
    _really_created = [c for c in overall_results["created"] if c.get("action") == "created"]
    overall_results["created_dates_logged"] = record_created_campaigns(
        _really_created, created_date=overall_results["date"])

    # Heal the side-logs of EARLIER runs that never got this far. All three logging steps
    # above sit AFTER the whole create loop, so a cancelled run, a crash or a uvicorn
    # restart leaves real campaigns with no sheet row, no MC id in Redshift and no creation
    # date (2026-07-31: three unfinished runs left 14 shop/country combos unlogged).
    # Comparing against change_event and writing only what is missing turns that into a
    # "just run it again" problem. Idempotent, best-effort, never fails the run.
    try:
        overall_results["reconciled"] = reconcile_run_logs(days=RECONCILE_WINDOW_DAYS)
    except Exception as ex:
        logger.error("Post-run reconcile failed: %s", ex)
        overall_results["reconciled"] = {"error": str(ex)[:300]}

    # Final structural check of everything created/repaired this run (best-effort;
    # never fails the run). Flags any campaign left without an ad group / product
    # ad / correct listing tree — the exact gap a rerun's _repair_campaign fixes.
    if verify and overall_results["created"]:
        try:
            overall_results["verification"] = verify_run_campaigns(client, overall_results["created"])
        except Exception as ex:
            logger.error("Post-run verification failed: %s", ex)
            overall_results["verification"] = {"error": str(ex)}

    _run_progress["running"] = False
    return overall_results


# ---------------------------------------------------------------------------
# Branded-label backfill
# ---------------------------------------------------------------------------


def _fetch_branded_by_shop() -> Dict[int, Any]:
    """
    Map shop_id -> f_branded for all current shops (same efficy join used by
    get_redshift_shop_changes). Value is 0, 1, or None (no catman row).
    """
    conn = _get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.f_shop_id, c.f_branded
            FROM hda.efficy_shops s
            LEFT JOIN hda.efficy_shop_catman c
              ON c.k_shop = s.k_shop AND c.actual_ind = 1 AND c.deleted_ind = 0
            WHERE s.actual_ind = 1 AND s.deleted_ind = 0
            """
        )
        out: Dict[int, Any] = {}
        for shop_id, branded in cur.fetchall():
            if shop_id is None:
                continue
            out[int(shop_id)] = branded
        return out
    finally:
        conn.close()


def backfill_branded_labels(
    dry_run: bool = True, customer_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Label every GSD_SCRIPT campaign that lacks a BRANDED_0/BRANDED_1 label with
    the one matching its shop's f_branded flag (from Redshift, by [shop_id:...]).

    dry_run=True only reports what it would do. Campaigns whose shop has an
    unknown/NULL branded flag, or whose name has no shop_id, are skipped.
    Returns a per-account + totals summary.
    """
    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    cl_service = client.get_service("CampaignLabelService")

    branded_map = _fetch_branded_by_shop()

    if customer_ids is None:
        customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    keys = ("gsd_total", "already", "BRANDED_0", "BRANDED_1", "skipped_unknown", "no_shop_id")
    summary: Dict[str, Any] = {"dry_run": dry_run, "accounts": {}, "totals": {k: 0 for k in keys}}

    for cid in customer_ids:
        gsd_rn = ensure_campaign_label_exists(client, cid, SCRIPT_LABEL)
        b0_rn = ensure_campaign_label_exists(client, cid, "BRANDED_0")
        b1_rn = ensure_campaign_label_exists(client, cid, "BRANDED_1")
        acct = {k: 0 for k in keys}
        ops = []

        for r in ga.search(customer_id=cid, query=f"""
                SELECT campaign.id, campaign.resource_name, campaign.name, campaign.labels
                FROM campaign
                WHERE campaign.labels CONTAINS ANY ('{gsd_rn}')
                  AND campaign.status != 'REMOVED'"""):
            acct["gsd_total"] += 1
            labels = set(r.campaign.labels)
            if b0_rn in labels or b1_rn in labels:
                acct["already"] += 1
                continue
            sid = _parse_campaign_name(r.campaign.name).get("shop_id")
            if sid is None:
                acct["no_shop_id"] += 1
                continue
            name = _branded_label_name(branded_map.get(int(sid)))
            if not name:
                acct["skipped_unknown"] += 1
                continue
            acct[name] += 1
            if not dry_run:
                op = client.get_type("CampaignLabelOperation")
                op.create.campaign = r.campaign.resource_name
                op.create.label = b0_rn if name == "BRANDED_0" else b1_rn
                ops.append(op)

        if not dry_run and ops:
            for i in range(0, len(ops), 1000):
                chunk = ops[i:i + 1000]
                req = client.get_type("MutateCampaignLabelsRequest")
                req.customer_id = cid
                req.operations.extend(chunk)
                req.partial_failure = True  # a label already present must not fail the chunk
                _mutate_with_retry(
                    f"backfill branded labels ({cid})",
                    lambda req=req: cl_service.mutate_campaign_labels(request=req),
                )

        summary["accounts"][cid] = acct
        for k in keys:
            summary["totals"][k] += acct[k]
        logger.info("Branded-label backfill %s account %s: %s",
                    "(dry-run)" if dry_run else "(applied)", cid, acct)

    return summary


# ---------------------------------------------------------------------------
# Merchant-ID backfill to pa.mc_ids_efficy
# ---------------------------------------------------------------------------


def backfill_recent_mc_ids_to_redshift(
    days: int = 2, dry_run: bool = True, customer_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Push the Merchant Center ids of recently created GSD_SCRIPT campaigns to
    pa.mc_ids_efficy — a one-off backfill for campaigns created before the run
    started logging MC ids itself.

    "Created recently" is taken from the ``change_event`` resource (the actual
    CREATE event's change_date_time) — ``campaign.start_date`` was removed from
    the API. For each account it finds campaigns created in the window, then reads
    shop_name/shop_id/country from the GSD_SCRIPT campaign name and merchant_id
    from the shopping setting.

    Rows are deduped per (shop_id, country, merchant_id), so a shop's 2-5 label
    campaigns collapse to one row — matching the original create GSD-campaigns.py,
    which logged one mc_created per shop. The ``date`` column gets each campaign's
    creation date (YYYYMMDD).

    dry_run=True only reports what it would push; dry_run=False also inserts.
    """
    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if customer_ids is None:
        customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    seen: Dict[tuple, tuple] = {}          # (shop_id, country, mc_id) -> row tuple
    per_account: Dict[str, int] = {}

    for cid in customer_ids:
        # 1. Campaign ids CREATEd in the window -> creation date (YYYY-MM-DD).
        #    change_event needs a finite change_date_time range and a LIMIT, and
        #    only covers the last ~30 days (fine for a 2-day backfill).
        created_on: Dict[str, str] = {}
        for r in ga.search(customer_id=cid, query=f"""
                SELECT change_event.campaign, change_event.change_date_time
                FROM change_event
                WHERE change_event.change_date_time >= '{cutoff} 00:00:00'
                  AND change_event.change_date_time <= '{now_str}'
                  AND change_event.change_resource_type = 'CAMPAIGN'
                  AND change_event.resource_change_operation = 'CREATE'
                ORDER BY change_event.change_date_time DESC
                LIMIT 10000"""):
            camp_id = r.change_event.campaign.rstrip("/").split("/")[-1]
            # keep the earliest CREATE date seen for a campaign id
            d = r.change_event.change_date_time[:10]
            if camp_id not in created_on or d < created_on[camp_id]:
                created_on[camp_id] = d

        # 2. GSD_SCRIPT campaigns with their merchant id; keep only recent ones.
        gsd_rn = ensure_campaign_label_exists(client, cid, SCRIPT_LABEL)
        found = 0
        for r in ga.search(customer_id=cid, query=f"""
                SELECT campaign.id, campaign.name,
                       campaign.shopping_setting.merchant_id
                FROM campaign
                WHERE campaign.labels CONTAINS ANY ('{gsd_rn}')
                  AND campaign.status != 'REMOVED'"""):
            camp_id = str(r.campaign.id)
            if camp_id not in created_on:
                continue  # not created in the window
            merchant_id = r.campaign.shopping_setting.merchant_id
            if not merchant_id:  # 0/unset — no MC linked, nothing to log
                continue
            parsed = _parse_campaign_name(r.campaign.name)
            shop_name = parsed.get("shop_name")
            country = parsed.get("country") or ""
            try:
                shop_id_int = int(parsed["shop_id"]) if parsed.get("shop_id") else None
            except (TypeError, ValueError):
                shop_id_int = None
            key = (shop_id_int, country, int(merchant_id))
            if key in seen:
                continue
            date_ymd = created_on[camp_id].replace("-", "")  # YYYYMMDD
            seen[key] = (shop_name, shop_id_int, int(merchant_id), country, date_ymd)
            found += 1
        per_account[cid] = found
        logger.info("MC-id backfill account %s: %d new MC id(s) since %s", cid, found, cutoff)

    rows = list(seen.values())
    summary: Dict[str, Any] = {
        "dry_run": dry_run,
        "cutoff_date": cutoff,
        "accounts": per_account,
        "unique_mc_ids": len(rows),
        "rows": rows,
    }
    if not dry_run:
        summary["push_result"] = push_mc_ids_to_redshift(rows)
    return summary


# ---------------------------------------------------------------------------
# Side-log reconciliation ("just run it again")
# ---------------------------------------------------------------------------
# A GSD run writes three side-logs, and all three happen AFTER the create loop:
#   1. pa.jvs_gsd_campaign_created   (PostgreSQL) — the Date column in Campaigns created
#   2. pa.mc_ids_efficy              (Redshift)   — Merchant Center ids for Efficy
#   3. the "campaigns_created" sheet (Sheets)     — the human run log
# A run that is cancelled, crashes, or dies on a backend restart therefore creates real
# campaigns and writes NONE of the three (Joep, 2026-07-31: "ran a couple times today
# but didn't finish"). Rather than a one-off repair script, the run now reconciles the
# three logs against Google Ads on every run, so a missed log is a "just run it again"
# problem. Ground truth is change_event, which keeps ~30 days.


def _sheet_type_from_label(label: Optional[str]) -> str:
    """The sheet's CPC/CPR column — the same two-value model _MODEL_BY_LABEL yields.

    Derived from that map rather than the old "does the label contain a comma" test:
    that test read '[label:cpc]' (no comma) as CPR and would have logged every
    price-bucket campaign under the wrong model. Unknown labels stay CPR, as before.
    """
    return _MODEL_BY_LABEL.get((label or "").strip().lower()) or "CPR"


def _norm_sheet_date(value: Any) -> Optional[str]:
    """'31-07-2026' / '1-7-2026' / '2026-07-31' -> 'YYYY-MM-DD', else None.

    The sheet's own history is inconsistent (rows from 2025 are '12-6-2025', 2026 rows are
    zero-padded), so string comparison would miss existing rows and duplicate them.
    """
    s = str(value or "").strip()
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _inventory_recent_creations(client: GoogleAdsClient, days: int) -> Dict[tuple, Dict[str, Any]]:
    """{(shop_id, country): {...}} for GSD campaigns CREATEd in the last `days`.

    One entry per shop+country (the sheet and both tables are per shop+country, not per
    label), carrying the earliest creation date seen and the data the logs need.
    """
    ga = client.get_service("GoogleAdsService")
    days = min(max(int(days), 1), 29)          # change_event retains ~30 days, strictly
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out: Dict[tuple, Dict[str, Any]] = {}
    # NL_CPR and NL_CPC share one customer_id — query each DISTINCT id once, or every
    # NL campaign is inventoried twice.
    for cid in sorted({info["customer_id"] for info in ACCOUNTS.values()}):
        created_on: Dict[str, str] = {}
        try:
            for r in ga.search(customer_id=cid, query=f"""
                    SELECT change_event.campaign, change_event.change_date_time
                    FROM change_event
                    WHERE change_event.change_date_time >= '{cutoff} 00:00:00'
                      AND change_event.change_date_time <= '{now_str}'
                      AND change_event.change_resource_type = 'CAMPAIGN'
                      AND change_event.resource_change_operation = 'CREATE'
                    ORDER BY change_event.change_date_time DESC
                    LIMIT 10000"""):
                camp = r.change_event.campaign.rstrip("/").split("/")[-1]
                d = r.change_event.change_date_time[:10]
                if camp not in created_on or d < created_on[camp]:
                    created_on[camp] = d
        except GoogleAdsException as ex:
            logger.warning("Reconcile: change_event query failed for %s: %s", cid, ex)
            continue
        if not created_on:
            continue
        # Read-only lookups: this function also backs a dry run, and
        # ensure_campaign_label_exists() would CREATE a missing label.
        gsd_rn = _lookup_label_resource(client, cid, SCRIPT_LABEL)
        if not gsd_rn:
            logger.warning("Reconcile: no %s label in %s — skipping account", SCRIPT_LABEL, cid)
            continue
        b0_rn = _lookup_label_resource(client, cid, "BRANDED_0")
        b1_rn = _lookup_label_resource(client, cid, "BRANDED_1")
        try:
            for r in ga.search(customer_id=cid, query=f"""
                    SELECT campaign.id, campaign.name, campaign.labels,
                           campaign.shopping_setting.merchant_id
                    FROM campaign
                    WHERE campaign.labels CONTAINS ANY ('{gsd_rn}')
                      AND campaign.status != 'REMOVED'"""):
                camp_id = str(r.campaign.id)
                if camp_id not in created_on:
                    continue
                parsed = _parse_campaign_name(r.campaign.name)
                try:
                    shop_id_int = int(parsed["shop_id"]) if parsed.get("shop_id") else None
                except (TypeError, ValueError):
                    shop_id_int = None
                country = (parsed.get("country") or "").upper()
                if shop_id_int is None or not country:
                    continue
                labels = set(r.campaign.labels)
                key = (shop_id_int, country)
                date = created_on[camp_id]
                rec = out.get(key)
                if rec is None or date < rec["date"]:
                    out[key] = {
                        "shop_id": shop_id_int,
                        "shop_name": parsed.get("shop_name") or "",
                        "country": country,
                        "date": date,
                        "customer_id": cid,
                        "merchant_id": int(r.campaign.shopping_setting.merchant_id or 0) or None,
                        "type": _sheet_type_from_label(parsed.get("label")),
                        "branded": ("ja" if (b1_rn and b1_rn in labels)
                                    else "nee" if (b0_rn and b0_rn in labels) else ""),
                    }
        except GoogleAdsException as ex:
            logger.warning("Reconcile: campaign query failed for %s: %s", cid, ex)
            continue
    return out


# _existing_mc_id_keys lived here. It returned the (shop_id, country, merchant_id) triples
# already logged, and reconcile used it to skip "already known" MC ids. Replaced by
# current_mc_state + mc_upsert_plan: the triple test could only answer "have I seen this
# exact combination", which silently hid the case the state rule cares about — a shop whose
# MC id CHANGED. Its only caller is gone.


# A creation is considered already logged when a sheet row for the same shop+country sits
# within this many days of it. Matches RECONCILE_WINDOW_DAYS so a re-run that logs today's
# date won't cause reconciliation to add a second row with the original creation date.
SHEET_DATE_TOLERANCE_DAYS = RECONCILE_WINDOW_DAYS


def _existing_sheet_keys() -> Dict[tuple, List[tuple]]:
    """{(shop_id, country): [(YYYY-MM-DD, actie), ...]} already logged in the sheet.

    AUDIT MED — the actie (column I, 'aan'/'uit') is part of the value now. It was read
    into the range but thrown away, so an 'uit' row inside SHEET_DATE_TOLERANCE_DAYS
    suppressed a later 'aan' row for the same shop+country: a shop switched off and back
    on within two days lost its 'aan' log line. The ±2-day tolerance itself is correct and
    deliberate — it is the key that was too coarse.
    """
    creds = service_account.Credentials.from_service_account_file(
        SHEETS_SA_FILE, scopes=SHEETS_SCOPES
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    rows = svc.spreadsheets().values().get(
        spreadsheetId=LOG_SPREADSHEET_ID, range=f"{LOG_WORKSHEET}!A:I"
    ).execute().get("values", [])
    keys: Dict[tuple, List[tuple]] = {}
    for r in rows[1:]:
        if len(r) < 6:
            continue
        d = _norm_sheet_date(r[0])
        if not d:
            continue
        try:
            shop_id = int(str(r[1]).strip())
        except (TypeError, ValueError):
            continue
        # Column I is absent on older rows; "" means "unknown", which _sheet_has_row
        # treats as matching any actie so those rows keep suppressing duplicates exactly
        # as they did before.
        actie = (r[8] or "").strip().lower() if len(r) > 8 else ""
        keys.setdefault((shop_id, (r[5] or "").strip().upper()), []).append((d, actie))
    return keys


def _sheet_has_row(have: Dict[tuple, List[tuple]], shop_id: int, country: str, date: str,
                   actie: str = "aan") -> bool:
    """True when the sheet already logs this shop+country+actie near this date.

    A row whose actie is unknown (legacy row without column I) matches any actie, so this
    stays backwards compatible: it can only ever suppress more than the old key did for
    rows that predate the column, never fewer.
    """
    entries = have.get((shop_id, country)) or []
    if not entries:
        return False
    target = datetime.strptime(date, "%Y-%m-%d")
    want = (actie or "").strip().lower()
    for d, row_actie in entries:
        if row_actie and want and row_actie != want:
            continue
        try:
            if abs((datetime.strptime(d, "%Y-%m-%d") - target).days) <= SHEET_DATE_TOLERANCE_DAYS:
                return True
        except ValueError:
            continue
    return False


def reconcile_run_logs(days: int = 7, dry_run: bool = False) -> Dict[str, Any]:
    """Fill in side-logs missing for recently created campaigns. Idempotent.

    Compares Google Ads (change_event, ground truth) against all three logs and writes
    only what is absent, so running it twice changes nothing the second time. Returns a
    per-sink summary; never raises — a reconcile failure must not fail a run.

    NOTE on pa.mc_ids_efficy: the "did WE create this account" question is now moot, and
    that resolves an ambiguity this docstring used to carry. A run gates on its own
    mc_was_created boolean; reconcile cannot, because the Content API exposes no account
    creation date, and it only ever sees which MC account a campaign POINTS AT — never who
    created it. So reconcile used to risk logging a pre-existing sub-account as new.

    Joep's call (2026-08-05): the table is STATE, one row per (shop_id, domain) holding that
    shop's current MC id. Under that reading provenance stops mattering — the row is either
    absent (insert), different (update the mc id and the date) or identical (do nothing). See
    mc_upsert_plan; reconcile delegates the decision to it rather than re-implementing it.
    """
    summary: Dict[str, Any] = {
        "days": days, "dry_run": dry_run, "shops_seen": 0,
        "created_dates": {"missing": 0, "inserted": 0},
        "mc_ids": {"missing": 0, "duplicated": 0, "inserted": 0, "updated": 0,
                   "repaired": 0, "unchanged": 0},
        "sheet": {"missing": 0, "logged": 0},
        "errors": [],
    }
    try:
        client = _get_client()
        inv = _inventory_recent_creations(client, days)
    except Exception as ex:
        logger.error("Reconcile: inventory failed: %s", ex)
        summary["errors"].append({"step": "inventory", "error": str(ex)[:300]})
        return summary
    summary["shops_seen"] = len(inv)
    if not inv:
        return summary

    # --- 1. creation dates (PostgreSQL) — ON CONFLICT DO NOTHING keeps the first date
    try:
        have_dates = get_created_dates()
        rows = [(r["shop_id"], r["country"], r["date"], r["shop_name"])
                for k, r in inv.items() if _created_key(k[0], k[1]) not in have_dates]
        summary["created_dates"]["missing"] = len(rows)
        if rows and not dry_run:
            res = upsert_created_dates(rows)
            summary["created_dates"]["inserted"] = res.get("inserted", 0)
            # A sink that failed reports inserted=0 WITH an error; without surfacing it a
            # dead DB looked exactly like "nothing to do" (AUDIT MED).
            if res.get("error"):
                summary["errors"].append({"step": "created_dates", "error": str(res["error"])[:300]})
    except Exception as ex:
        logger.error("Reconcile: creation dates failed: %s", ex)
        summary["errors"].append({"step": "created_dates", "error": str(ex)[:300]})

    # --- 2. Merchant Center ids (Redshift)
    try:
        # Reconcile no longer decides for itself what is "missing". It hands everything it
        # found to mc_upsert_plan, the same function push_mc_ids_to_redshift uses, so the
        # two cannot drift apart on the rule — H6's lesson. The old triple pre-filter
        # skipped exact matches, which happens to be the no-op case, but it also hid the
        # UPDATE case: a shop whose MC id CHANGED looked like "nothing to do" here.
        rows = [(r["shop_name"], r["shop_id"], r["merchant_id"], r["country"],
                 r["date"].replace("-", ""))
                for r in inv.values() if r["merchant_id"]]
        state = current_mc_state([r[1] for r in rows])
        plan = mc_upsert_plan(rows, state)
        summary["mc_ids"]["missing"] = len(plan["insert"]) + len(plan["update"])
        # Keys whose VALUE is already right but that hold more than one row. Counted apart
        # from "missing" because nothing is absent — the write has to collapse them, and if
        # reconcile did not trigger on this the table could never heal itself.
        summary["mc_ids"]["duplicated"] = len(plan["repair"])
        summary["mc_ids"]["to_repair"] = plan["repair"]
        summary["mc_ids"]["to_insert"] = plan["insert"]
        summary["mc_ids"]["to_update"] = [
            # what it would change FROM, so a dry run is reviewable rather than trusted
            {"shop_name": n, "shop_id": s, "country": d, "new_mc": mc, "new_date": dt,
             "old_mc": state.get((s, d), (None,))[0], "old_date": state.get((s, d), (None, None))[1]}
            for (n, s, mc, d, dt) in plan["update"]
        ]
        summary["mc_ids"]["unchanged"] = len(plan["unchanged"])
        summary["mc_ids"]["rows"] = plan["insert"] + plan["update"] + plan["repair"]
        if (summary["mc_ids"]["missing"] or summary["mc_ids"]["duplicated"]) and not dry_run:
            res = push_mc_ids_to_redshift(rows)
            summary["mc_ids"]["updated"] = res.get("updated", 0)
            summary["mc_ids"]["inserted"] = res.get("inserted", 0)
            summary["mc_ids"]["repaired"] = res.get("repaired", 0)
            if res.get("error"):
                summary["errors"].append({"step": "mc_ids", "error": str(res["error"])[:300]})
    except Exception as ex:
        logger.error("Reconcile: mc ids failed: %s", ex)
        summary["errors"].append({"step": "mc_ids", "error": str(ex)[:300]})

    # --- 3. the run-log sheet
    try:
        have_sheet = _existing_sheet_keys()
        sheet_rows = []
        for r in sorted(inv.values(), key=lambda x: (x["date"], x["shop_id"], x["country"])):
            # Reconcile only ever writes 'aan' rows (hardcoded below), so that is what it
            # must look for — an existing 'uit' row is not evidence this one was logged.
            if _sheet_has_row(have_sheet, r["shop_id"], r["country"], r["date"], "aan"):
                continue
            sheet_rows.append([
                datetime.strptime(r["date"], "%Y-%m-%d").strftime("%d-%m-%Y"),
                str(r["shop_id"]), r["shop_name"], r["type"],
                str(r["merchant_id"] or ""), r["country"],
                r["branded"], "ja", "aan",
            ])
        summary["sheet"]["missing"] = len(sheet_rows)
        summary["sheet"]["rows"] = sheet_rows
        if sheet_rows and not dry_run:
            res = _log_run_to_sheet(sheet_rows)
            summary["sheet"]["logged"] = res.get("logged", 0)
            summary["sheet"]["first_row"] = res.get("first_row")
            if res.get("error"):
                summary["errors"].append({"step": "sheet", "error": res["error"]})
    except Exception as ex:
        logger.error("Reconcile: sheet failed: %s", ex)
        summary["errors"].append({"step": "sheet", "error": str(ex)[:300]})

    logger.info(
        "Reconcile (%d days, dry_run=%s): %d shop/country combos — dates %d/%d, "
        "mc ids %d/%d (+%d/%d duplicated keys collapsed), sheet %d/%d",
        days, dry_run, summary["shops_seen"],
        summary["created_dates"]["inserted"], summary["created_dates"]["missing"],
        summary["mc_ids"]["inserted"], summary["mc_ids"]["missing"],
        summary["mc_ids"]["repaired"], summary["mc_ids"]["duplicated"],
        summary["sheet"]["logged"], summary["sheet"]["missing"],
    )
    return summary
