"""Daily check: which shops changed their shop_deelt_data flag?

Queries Redshift for shops whose shop_deelt_data value changed on the most
recent date vs. their previous record.  If any rows come back, formats them
as an HTML table and e-mails the result via Microsoft Graph (same OAuth flow
as the R scripts on this machine).  If no changes: exits silently.

Runs standalone (Task Scheduler) — loads .env from the project root.
"""

import base64
import io
import os
import sys
import gzip
import json
import logging
import tempfile
from datetime import datetime

import msal
import openpyxl
import requests as req

# ── bootstrap ────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:
    pass

from backend.database import get_redshift_connection, return_redshift_connection

# ── config ───────────────────────────────────────────────────────────────────
# Azure AD app used by Microsoft365R on this machine (device-code flow).
# The cached refresh_token lives in the AzureR directory and is reused here.
AZURE_CLIENT_ID = "d44a05d5-c6a5-4bbb-82d2-443123722380"
AZURE_AUTHORITY = "https://login.microsoftonline.com/common"
GRAPH_SCOPES = ["https://graph.microsoft.com/Mail.Send",
                "https://graph.microsoft.com/User.Read"]

AZURE_TOKEN_CACHE = os.path.join(
    os.environ.get("LOCALAPPDATA", ""),
    "AzureR", "ba03b8075ac5143606ec64988159a2c5",
)

MAIL_TO = ["j.schagen@beslist.nl", "t.woestenburg@beslist.nl"]

log = logging.getLogger("shop_deelt_data")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")

# ── query ────────────────────────────────────────────────────────────────────
QUERY = """\
WITH laatste AS (
    SELECT MAX(date) AS d FROM beslistbi.bt.shop_main_attributes_by_day
),
met_vorige AS (
    SELECT
        date,
        shop_id,
        shop_name,
        shop_deelt_data,
        LAG(shop_deelt_data) OVER (PARTITION BY shop_id ORDER BY date) AS vorige_waarde,
        LAG(date)            OVER (PARTITION BY shop_id ORDER BY date) AS vorige_datum
    FROM beslistbi.bt.shop_main_attributes_by_day
    WHERE date >= (SELECT d FROM laatste) - 14
)
SELECT
    shop_name,
    shop_id,
    vorige_waarde   AS oude_waarde,
    shop_deelt_data AS nieuwe_waarde,
    vorige_datum,
    date            AS wijzigingsdatum
FROM met_vorige
WHERE date = (SELECT d FROM laatste)
  AND (vorige_waarde IS NULL OR vorige_waarde <> shop_deelt_data)
  AND (is_gsd_nl_shop = 1 OR is_gsd_be_shop = 1 OR is_gsd_de_shop = 1)
ORDER BY shop_name;
"""

# ── Microsoft Graph mail ─────────────────────────────────────────────────────

def _load_refresh_token():
    """Read the refresh_token from the R AzureR cache (gzipped RDS)."""
    import re
    raw = gzip.open(AZURE_TOKEN_CACHE, "rb").read()
    # The RDS binary contains the refresh_token as a plain ASCII string.
    # It's the longest opaque token (starts with "1." or "0.") in the blob.
    candidates = re.findall(rb'[A-Za-z0-9_.~/-]{200,2000}', raw)
    for c in candidates:
        decoded = c.decode("ascii", errors="ignore")
        # The RDS binary may prefix the token with a few stray bytes.
        # Look for the "0." or "1." start of a Microsoft refresh token.
        for prefix in ("0.", "1."):
            idx = decoded.find(prefix)
            if idx != -1:
                return decoded[idx:]
    raise RuntimeError("Could not extract refresh_token from AzureR cache")


def _get_access_token():
    """Use MSAL to exchange the cached refresh_token for a fresh access_token."""
    refresh_token = _load_refresh_token()
    app = msal.PublicClientApplication(AZURE_CLIENT_ID, authority=AZURE_AUTHORITY)
    result = app.acquire_token_by_refresh_token(refresh_token, scopes=GRAPH_SCOPES)
    if "access_token" not in result:
        raise RuntimeError(f"Token refresh failed: {result.get('error_description', result)}")
    return result["access_token"]


def _send_mail_graph(html, subject, recipients, attachments=None):
    """Send an HTML mail via Microsoft Graph API.

    attachments: list of dicts with keys "name", "contentType", "contentBytes" (base64).
    """
    token = _get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html},
            "toRecipients": [
                {"emailAddress": {"address": r}} for r in recipients
            ],
        }
    }
    if attachments:
        message["message"]["attachments"] = [
            {"@odata.type": "#microsoft.graph.fileAttachment",
             "name": a["name"],
             "contentType": a["contentType"],
             "contentBytes": a["contentBytes"]}
            for a in attachments
        ]

    resp = req.post("https://graph.microsoft.com/v1.0/me/sendMail",
                    headers=headers, json=message, timeout=30)
    if resp.status_code == 202:
        log.info("Mail verstuurd naar %s", ", ".join(recipients))
    else:
        raise RuntimeError(f"Graph sendMail failed ({resp.status_code}): {resp.text[:300]}")


# ── helpers ──────────────────────────────────────────────────────────────────

def _fetch_changes():
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(QUERY)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        return_redshift_connection(conn)


COLS = ["Shop", "Shop ID", "Oude waarde", "Nieuwe waarde",
        "Vorige datum", "Wijzigingsdatum"]
KEYS = ["shop_name", "shop_id", "oude_waarde", "nieuwe_waarde",
        "vorige_datum", "wijzigingsdatum"]


def _build_html(rows):
    style = (
        "border-collapse:collapse; font-family:Calibri,Arial,sans-serif; font-size:14px;"
    )
    th_style = (
        "background:#2c3e50; color:#fff; padding:8px 12px; text-align:left;"
    )
    td_style = "padding:8px 12px; border-bottom:1px solid #ddd;"
    p_style = "font-family:Calibri,Arial,sans-serif; font-size:14px;"

    header = "".join(f"<th style='{th_style}'>{c}</th>" for c in COLS)
    body = ""
    for i, row in enumerate(rows):
        bg = "#f9f9f9" if i % 2 else "#fff"
        cells = "".join(
            f"<td style='{td_style}'>{row.get(k, '') if row.get(k) is not None else ''}</td>"
            for k in KEYS
        )
        body += f"<tr style='background:{bg}'>{cells}</tr>"

    return (
        f"<p style='{p_style}'>Hoi!</p>"
        f"<p style='{p_style}'>Voor onderstaande shops is shop_deelt_data gewijzigd:</p>"
        f"<table style='{style}'><tr>{header}</tr>{body}</table>"
        f"<p style='{p_style}'>Groeten!</p>"
    )


def _build_xlsx(rows):
    """Return the xlsx file as bytes."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "shop_deelt_data"
    ws.append(COLS)
    for row in rows:
        ws.append([row.get(k, "") if row.get(k) is not None else "" for k in KEYS])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── main ─────────────────────────────────────────────────────────────────────

def _make_attachment(rows):
    xlsx_bytes = _build_xlsx(rows)
    return {
        "name": f"shop_deelt_data_{datetime.now():%Y%m%d}.xlsx",
        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "contentBytes": base64.b64encode(xlsx_bytes).decode("ascii"),
    }


def main(*, test=False, test_recipients=None):
    if test:
        log.info("Test mode — sending a test mail")
        recipients = test_recipients or ["j.schagen@beslist.nl"]
        test_rows = [{
            "shop_name": "Test Shop BV",
            "shop_id": 12345,
            "oude_waarde": 0,
            "nieuwe_waarde": 1,
            "vorige_datum": "2026-08-06",
            "wijzigingsdatum": "2026-08-07",
        }]
        _send_mail_graph(
            _build_html(test_rows),
            f"[TEST] Shop deelt data — wijzigingen ({datetime.now():%Y-%m-%d})",
            recipients,
            attachments=[_make_attachment(test_rows)],
        )
        return

    rows = _fetch_changes()
    if not rows:
        log.info("Geen wijzigingen gevonden — geen mail verstuurd")
        return

    log.info("%d wijziging(en) gevonden", len(rows))
    _send_mail_graph(
        _build_html(rows),
        f"Shop deelt data — wijzigingen ({datetime.now():%Y-%m-%d})",
        MAIL_TO,
        attachments=[_make_attachment(rows)],
    )


if __name__ == "__main__":
    if "--test" in sys.argv:
        main(test=True, test_recipients=["j.schagen@beslist.nl"])
    else:
        main()
