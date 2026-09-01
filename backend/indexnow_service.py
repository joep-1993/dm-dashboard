"""
IndexNow Service

Submits URLs to the IndexNow API for rapid search engine indexing.
Deduplicates against previously submitted URLs stored in Redshift
(pa.index_now_joep) — the same table the daily n8n indexnow_submitter flow
writes to, so the manual path and the automated flow share one source of truth.
"""
import logging
import math
import json
import os
import requests
from datetime import datetime
from typing import List, Dict, Tuple
from backend.database import (
    get_redshift_connection,
    return_redshift_connection,
)

log = logging.getLogger("indexnow")

# IndexNow settings
#
# The 10.000/day cap is allocated by Bing PER DOMAIN, so beslist.be has its own
# budget next to beslist.nl — it does not eat into the .nl quota. Our own counter
# therefore has to be per domain too (see get_today_count), otherwise the tool
# would refuse .be submissions once .nl had spent the day's 10k.
#
# The key is per host as well. The .nl key is registered with Bing at host level
# (a submit returns 200 even with a wrong or absent keyLocation), but the SAME key
# on a .be URL returns 202 — "key validation pending" — because Bing then goes
# looking for the key file on www.beslist.be, where it 404s. So .be needs either
# its own key from Bing Webmaster Tools or that key file hosted on the .be root.
# Drop it in via INDEXNOW_KEY_BE; until then .be submissions are refused up front
# instead of being fired into the void.
DOMAINS = {
    "www.beslist.nl": os.getenv("INDEXNOW_KEY_NL", "2e11f87f415a492294eaf378a8a52004"),
    "www.beslist.be": os.getenv("INDEXNOW_KEY_BE", ""),
}
DEFAULT_DOMAIN = "www.beslist.nl"
BATCH_SIZE = 10000
DAILY_LIMIT = 10000
TABLE = "pa.index_now_joep"


def list_domains() -> List[Dict]:
    """Domains the tool can submit to, and whether a key is configured for each."""
    return [
        {"domain": d, "configured": bool(key), "default": d == DEFAULT_DOMAIN}
        for d, key in DOMAINS.items()
    ]


def _resolve_domain(domain: str) -> str:
    """Normalise and validate a domain against DOMAINS. Raises ValueError."""
    d = (domain or DEFAULT_DOMAIN).strip().lower()
    d = d.replace("https://", "").replace("http://", "").rstrip("/")
    if d in ("beslist.nl", "beslist.be"):
        d = "www." + d
    if d not in DOMAINS:
        raise ValueError(
            f"Unknown domain '{domain}'. Choose one of: {', '.join(DOMAINS)}"
        )
    return d


def ensure_table_exists():
    """Ensure the tracking table exists on Redshift.

    The whole IndexNow path (manual submit + the daily n8n flow) shares one
    table: pa.index_now_joep on Redshift. It already exists and is maintained
    by n8n, so this is normally a no-op; the CREATE guards a fresh environment.
    Redshift has no CREATE INDEX, so none are declared here. `id` is an IDENTITY
    column, so inserts omit it.
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id BIGINT IDENTITY(1,1),
                url VARCHAR(2000) NOT NULL,
                submitted_date DATE NOT NULL,
                response_code INTEGER
            )
        """)
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def get_existing_urls(domain: str = None) -> set:
    """Previously submitted URLs (from Redshift), optionally scoped to one domain.

    Scoping keeps the set small now that the table holds more than one host; the
    URLs are absolute, so a cross-domain dedup would work too — it would just
    drag the whole table over the wire for nothing.
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        if domain:
            cur.execute(
                f"SELECT DISTINCT url FROM {TABLE} WHERE url LIKE %s",
                (f"https://{domain}/%",),
            )
        else:
            cur.execute(f"SELECT DISTINCT url FROM {TABLE}")
        urls = set(row["url"] for row in cur.fetchall())
        cur.close()
        return urls
    finally:
        return_redshift_connection(conn)


def _slack_alarm(text: str) -> None:
    """DM the SEO owner on Slack. Same bot-token path as backend/daily_automation.py.

    Never raises: an alarm that breaks the submit it is warning about is worse
    than no alarm.
    """
    token = os.getenv("SLACK_BOT_TOKEN", "")
    user_id = os.getenv("SLACK_USER_ID", "")
    if not token or not user_id:
        log.warning("SLACK_BOT_TOKEN/SLACK_USER_ID not set — skipping IndexNow alarm")
        return
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": user_id, "text": text},
            timeout=15,
        )
        if not resp.json().get("ok"):
            log.warning("Slack API error on IndexNow alarm: %s", resp.json().get("error"))
    except Exception as e:  # noqa: BLE001 — best effort by design
        log.warning("Failed to send IndexNow alarm: %s", e)


def _send_batch(urls: List[str], domain: str) -> Tuple[int, str]:
    """Send a batch of URLs to the IndexNow API for one host.

    Returns `(status_code, detail)`. A transport failure (timeout, DNS, TLS)
    returns code 0 so the caller can treat it the same as an HTTP error instead
    of blowing up mid-run with earlier batches already logged.

    IndexNow's contract: 200 = submitted, 202 = received but key validation
    pending, 403 = key invalid, 422 = URLs don't belong to the host, 429 = too
    many requests. Only a **200** means Bing took the batch — see submit_urls for
    why 202 deliberately does not count as success.
    """
    key = DOMAINS[domain]
    payload = {
        "host": domain,
        "key": key,
        "keyLocation": f"https://{domain}/{key}.txt",
        "urlList": urls,
    }
    try:
        response = requests.post(
            "https://api.indexnow.org/IndexNow",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(payload),
            timeout=30,
        )
    except Exception as e:  # noqa: BLE001 — surfaced as code 0 to the caller
        log.warning("IndexNow POST failed: %s", e)
        return 0, f"{type(e).__name__}: {e}"
    detail = (response.text or "").strip()[:300]
    return response.status_code, detail


def _save_submissions(urls: List[str], response_code: int):
    """Write submitted URLs to the tracking table on Redshift.

    Redshift is columnar — single-row INSERTs are pathologically slow, so we
    write multi-row VALUES statements in chunks (matching how the n8n flow
    bulk-inserts).
    """
    if not urls:
        return
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        today = datetime.today().date()
        CHUNK = 1000
        for start in range(0, len(urls), CHUNK):
            chunk = urls[start : start + CHUNK]
            placeholders = ",".join(["(%s, %s, %s)"] * len(chunk))
            params = []
            for url in chunk:
                params.extend([url, today, response_code])
            cur.execute(
                f"INSERT INTO {TABLE} (url, submitted_date, response_code) VALUES {placeholders}",
                params,
            )
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def get_today_count(domain: str = None) -> int:
    """URLs submitted today (from Redshift), per domain when one is given.

    Bing's 10k/day is a per-domain quota, so a shared counter would wrongly lock
    .be out on a busy .nl day. `response_code = 200` stays the filter: since the
    1 Sep 2026 fix only a real 200 is ever written, so this counts what Bing
    actually accepted.
    """
    ensure_table_exists()
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        today = datetime.today().date()
        if domain:
            cur.execute(
                f"SELECT COUNT(*) as cnt FROM {TABLE} "
                f"WHERE submitted_date = %s AND response_code = 200 AND url LIKE %s",
                (today, f"https://{domain}/%"),
            )
        else:
            cur.execute(
                f"SELECT COUNT(*) as cnt FROM {TABLE} WHERE submitted_date = %s AND response_code = 200",
                (today,)
            )
        row = cur.fetchone()
        cur.close()
        return row["cnt"] if row else 0
    finally:
        return_redshift_connection(conn)


def submit_urls(urls: List[str], domain: str = DEFAULT_DOMAIN) -> Dict:
    """
    Submit URLs to IndexNow API for one domain, deduplicating against previously
    submitted URLs.

    Returns dict with submission results and stats.
    """
    domain = _resolve_domain(domain)
    base = {"domain": domain, "daily_limit": DAILY_LIMIT}

    if not DOMAINS[domain]:
        # No point firing: without a key Bing answers 202 and quietly drops the
        # batch once it fails to find the key file on this host.
        return {
            **base,
            "status": "error",
            "total_input": len(urls),
            "new_urls": 0,
            "skipped_duplicates": 0,
            "batches": [],
            "today_count": get_today_count(domain),
            "message": (
                f"Geen IndexNow-key ingesteld voor {domain}. Maak er een aan in Bing "
                f"Webmaster Tools (of host het key-bestand op https://{domain}/) en zet "
                f"hem in .env als INDEXNOW_KEY_{'BE' if domain.endswith('.be') else 'NL'}."
            ),
        }

    ensure_table_exists()

    # Reject URLs from another host before they cost a request: IndexNow answers
    # 422 for those, and one stray .nl URL would take the whole .be batch with it.
    prefix = f"https://{domain}/"
    own = [u for u in urls if u.startswith(prefix)]
    foreign = len(urls) - len(own)

    if not own:
        return {
            **base,
            "status": "error",
            "total_input": len(urls),
            "new_urls": 0,
            "skipped_duplicates": 0,
            "batches": [],
            "today_count": get_today_count(domain),
            "message": (
                f"Geen enkele URL hoort bij {domain} — IndexNow weigert URLs van een "
                f"andere host (422). Kies het juiste domein of pas de URLs aan."
            ),
        }

    existing = get_existing_urls(domain)
    new_urls = [u for u in own if u not in existing]
    skipped = len(own) - len(new_urls)

    if not new_urls:
        return {
            **base,
            "status": "success",
            "total_input": len(urls),
            "new_urls": 0,
            "skipped_duplicates": skipped,
            "wrong_domain": foreign,
            "batches": [],
            "today_count": get_today_count(domain),
            "message": "No new URLs to submit — all already submitted previously.",
        }

    # Enforce daily limit — per domain, because Bing's quota is per domain.
    today_count = get_today_count(domain)
    remaining = max(0, DAILY_LIMIT - today_count)
    truncated = 0
    if remaining == 0:
        return {
            **base,
            "status": "error",
            "total_input": len(urls),
            "new_urls": len(new_urls),
            "skipped_duplicates": skipped,
            "wrong_domain": foreign,
            "today_count": today_count,
            "batches": [],
            "message": f"Daily limit reached for {domain} ({DAILY_LIMIT:,} URLs). Try again tomorrow.",
        }
    if len(new_urls) > remaining:
        truncated = len(new_urls) - remaining
        new_urls = new_urls[:remaining]

    num_batches = math.ceil(len(new_urls) / BATCH_SIZE)
    batches = []

    for i in range(num_batches):
        start = i * BATCH_SIZE
        batch = new_urls[start : start + BATCH_SIZE]
        response_code, detail = _send_batch(batch, domain)

        # Only a 200 counts. 202 means "URL received, key validation pending" —
        # Bing then goes looking for the key file and drops the batch if it isn't
        # there, which is exactly what the .nl key does on a .be URL. Logging a
        # 202 would be the old bug in a new coat: get_existing_urls() dedupes on
        # url alone, so those URLs would be retired without ever being indexed.
        # Not writing means the next run simply retries them.
        success = response_code == 200
        pending = response_code == 202

        if success:
            _save_submissions(batch, response_code)
        else:
            log.warning(
                "IndexNow batch %d of %d for %s not accepted: %s %s",
                i + 1, num_batches, domain, response_code, detail,
            )

        entry = {
            "batch_number": i + 1,
            "urls_count": len(batch),
            "response_code": response_code,
            "success": success,
        }
        if not success:
            entry["detail"] = detail
            entry["logged"] = False
            entry["pending"] = pending
        batches.append(entry)

    total_submitted = sum(b["urls_count"] for b in batches if b["success"])
    total_failed = sum(b["urls_count"] for b in batches if not b["success"])
    failed_batches = [b for b in batches if not b["success"]]

    if failed_batches:
        codes = ", ".join(
            f"{b['response_code'] or 'geen antwoord'} ({b['urls_count']:,} URLs)"
            for b in failed_batches
        )
        if all(b.get("pending") for b in failed_batches):
            reason = (
                "Bing heeft ze aangenomen maar de key nog niet gevalideerd (202) — dat "
                f"betekent vrijwel zeker dat het key-bestand op https://{domain}/ ontbreekt, "
                "en dan gooit Bing de batch weg."
            )
        else:
            reason = f"Eerste antwoord van de API: {failed_batches[0].get('detail') or '(leeg)'}"
        _slack_alarm(
            f":rotating_light: *IndexNow submit mislukt* ({domain}) — "
            f"{len(failed_batches)} van {num_batches} batches niet geaccepteerd: {codes}. "
            f"{total_failed:,} URLs zijn NIET gelogd en gaan de volgende run opnieuw mee. "
            f"{reason}"
        )

    result = {
        **base,
        "status": "error" if total_submitted == 0 else ("partial" if failed_batches else "success"),
        "total_input": len(urls),
        "new_urls": len(new_urls),
        "skipped_duplicates": skipped,
        "wrong_domain": foreign,
        "total_submitted": total_submitted,
        "total_failed": total_failed,
        "today_count": today_count + total_submitted,
        "batches": batches,
    }
    messages = []
    if foreign:
        messages.append(f"{foreign:,} URLs overgeslagen: die horen niet bij {domain}.")
    if failed_batches:
        messages.append(
            f"{total_failed:,} URLs niet geaccepteerd door IndexNow "
            f"({', '.join(str(b['response_code']) for b in failed_batches)}) — "
            "niet gelogd, gaan de volgende run opnieuw mee. Alarm naar Slack verstuurd."
        )
    if truncated > 0:
        result["truncated"] = truncated
        messages.append(f"{truncated:,} URLs skipped due to daily limit ({DAILY_LIMIT:,}).")
    if messages:
        result["message"] = " ".join(messages)
    return result


def get_submission_history(limit: int = 100) -> List[Dict]:
    """Get recent submission history.

    Reads from the Redshift copy of pa.index_now_joep — that is where the
    daily n8n `indexnow_submitter` flow logs its runs (it fetches candidate
    URLs from Redshift's datamart.* and reuses that same connection to log).
    The PostgreSQL copy that the manual submit path writes to stopped being
    fed on 2026-03-27, so history must come from Redshift to reflect the live
    (n8n-driven) submissions.
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT submitted_date, response_code, COUNT(*) as url_count
            FROM {TABLE}
            GROUP BY submitted_date, response_code
            ORDER BY submitted_date DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "date": str(row["submitted_date"]),
                "response_code": row["response_code"],
                "url_count": row["url_count"],
            }
            for row in rows
        ]
    finally:
        return_redshift_connection(conn)
