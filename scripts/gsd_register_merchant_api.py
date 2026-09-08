#!/usr/bin/env python3
"""
One-off: register GCP project acoustic-racer-258913 as a Merchant API *developer* on the
three GSD Merchant Center parent accounts (NL/BE/DE).

Why this is a separate script and not part of the tool
------------------------------------------------------
Merchant API needs two things that the Content API did not:

  1. merchantapi.googleapis.com enabled on the GCP project  — done 2026-09-08;
  2. the GCP project registered as a developer WITH EACH Merchant Center account.

Step 2 cannot be done by the service account the tool runs as. Google answers
`registerGcp` from a service account with:

    403 PERMISSION_DENIED_TO_REGISTER_GCP_WITH_SERVICE_ACCOUNT
    "GCP registration is not allowed for service accounts. Please use a human user account."

So it needs a human Google account with admin on the Merchant Center accounts, once,
interactively. After that the service account can call Merchant API normally and this
script never has to run again.

Usage
-----
    ./venv/bin/python scripts/gsd_register_merchant_api.py            # dry run
    ./venv/bin/python scripts/gsd_register_merchant_api.py --commit

The script signs you in itself: it prints a Google URL, waits on a loopback server, and
throws the token away when it exits — nothing is persisted.

NOT via `gcloud auth application-default login`. gcloud's own OAuth client is barred from
the `content` scope (the browser answers "Deze app is geblokkeerd"; gcloud itself warns
"The following scopes will be blocked soon for the default client ID"), so that route
dead-ends. This script uses Beslist's own OAuth client instead — GOOGLE_CLIENT_ID /
GOOGLE_CLIENT_SECRET from .env, the one the Google Ads integration already uses.

You must sign in as a Google user with Admin on the three Merchant Center parents;
j.schagen@beslist.nl is.

`--developer-email` is the human developer contact and defaults to j.schagen@beslist.nl.
It may NOT be a service account address; Google answers such a call with
PERMISSION_DENIED_TO_REGISTER_GCP_WITH_SERVICE_ACCOUNT. The identity that AUTHORISES the
call is a separate thing and may be a service account, as long as it has ADMIN on the
Merchant Center account. Registration is reversible with `unregisterGcp`.

Read the block comment on PARENTS before using this: one GCP project registers with
exactly one Merchant Center account, so this cannot cover NL, BE and DE at once.
"""
import argparse
import json
import os
import sys

import google.auth
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Merchant Center parents — mirrors ACCOUNTS in backend/gsd_campaigns_service.py.
# ONE GCP PROJECT SERVES ONE MERCHANT CENTER ACCOUNT. Google: "Each Google Cloud project
# can only be registered with a single Merchant Center account at any given time"; a
# second one returns ALREADY_REGISTERED. Beslist has three parents, so Merchant API needs
# three projects — which is exactly why gsd-nl-merchant-api@beslist-skippy,
# gsd-be-merchant-api@beslist-pegel-factor and gsd-de-merchant-api@beslist-pattas already
# exist as users on the matching accounts. Pass --only to register one market at a time
# with the project the current credentials belong to.
PARENTS = {"NL": "5592708765", "BE": "5588879919", "DE": "5342886105"}

# Must NOT be a service account email — Google rejects that with
# PERMISSION_DENIED_TO_REGISTER_GCP_WITH_SERVICE_ACCOUNT. The CALLER may be a service
# account (it needs ADMIN on the account); the developer contact may not.
DEFAULT_DEVELOPER_EMAIL = "j.schagen@beslist.nl"

# The project that gets registered is the one the CREDENTIALS belong to, not this value —
# this only sets the quota project on the request. Signing in through an OAuth client from
# another project registers THAT project instead.
QUOTA_PROJECT = "acoustic-racer-258913"
SCOPES = ["https://www.googleapis.com/auth/content"]
LOOPBACK_PORT = 8899
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _adc_file_present() -> bool:
    """Is there an application-default credentials file to read at all?"""
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return True
    return os.path.exists(os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"))


def _interactive_user_credentials():
    """Sign in as a human via Beslist's own OAuth client and return the credentials.

    Google refuses `registerGcp` from a service account and refuses the `content` scope
    for gcloud's built-in client, so this is the remaining route. The token lives only in
    this process; nothing is written to disk.
    """
    load_dotenv(os.path.join(REPO_ROOT, ".env"))
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET are not set in .env")

    flow = InstalledAppFlow.from_client_config(
        {"installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [f"http://localhost:{LOOPBACK_PORT}/"],
        }},
        scopes=SCOPES,
    )
    # open_browser=False: WSL has no browser of its own, so the URL is printed and
    # opened on the Windows side instead.
    return flow.run_local_server(
        port=LOOPBACK_PORT, open_browser=False,
        authorization_prompt_message="Open this URL and sign in:\n\n{url}\n",
        success_message="Signed in. You can close this tab.",
    )


def _reason(ex) -> str:
    try:
        err = json.loads((ex.content or b"{}").decode())["error"]
        detail = (err.get("details") or [{}])[0]
        meta = detail.get("metadata", {})
        return f"{detail.get('reason', '')} {meta.get('REASON', '')}: {err.get('message', '')}".strip()
    except Exception:
        return str(ex)[:300]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true",
                    help="actually register; without it the script only reports the state")
    ap.add_argument("--developer-email", default=DEFAULT_DEVELOPER_EMAIL,
                    help="human Google account that becomes the developer contact; a "
                         "service account address is rejected by Google")
    ap.add_argument("--only", choices=sorted(PARENTS), action="append",
                    help="register only this market (repeatable). One GCP project can "
                         "serve exactly one Merchant Center account, so registering all "
                         "three from one project cannot work.")
    args = ap.parse_args()

    if args.developer_email.endswith(".iam.gserviceaccount.com"):
        print("--developer-email must be a human Google account, not a service account.",
              file=sys.stderr)
        return 2

    targets = {c: p for c, p in PARENTS.items() if not args.only or c in args.only}
    if args.commit and len(targets) > 1:
        print(f"Refusing to register {len(targets)} accounts from one GCP project: Google "
              f"allows one Merchant Center account per project (ALREADY_REGISTERED "
              f"otherwise). Pick one with --only.", file=sys.stderr)
        return 2

    creds = None
    # Only consult ADC when a credentials file actually exists. google.auth.default()
    # otherwise falls through to probing the GCE metadata server, which on a laptop just
    # hangs for tens of seconds before failing.
    if _adc_file_present():
        try:
            adc, _ = google.auth.default(scopes=SCOPES, quota_project_id=QUOTA_PROJECT)
            # Google refuses a service account outright, so do not even try with one.
            if not getattr(adc, "service_account_email", None):
                creds = adc
                print("Using existing application-default credentials.")
        except Exception:
            pass

    if creds is None:
        try:
            creds = _interactive_user_credentials()
        except Exception as ex:
            print(f"Sign-in failed: {ex}", file=sys.stderr)
            return 2

    svc = build("merchantapi", "accounts_v1", credentials=creds, cache_discovery=False)
    reg = svc.accounts().developerRegistration()
    failures = 0

    for country, parent in targets.items():
        name = f"accounts/{parent}/developerRegistration"
        try:
            state = reg.getDeveloperRegistration(name=name).execute()
            gcp_ids = state.get("gcpIds") or []
            if QUOTA_PROJECT in gcp_ids:
                print(f"{country} {parent}: already registered ({', '.join(gcp_ids)})")
                continue
            print(f"{country} {parent}: registered projects = {gcp_ids or 'none'}")
        except Exception as ex:
            # Expected before the first registration: reading it is itself gated.
            print(f"{country} {parent}: state unreadable ({_reason(ex)[:90]})")

        if not args.commit:
            print(f"{country} {parent}: WOULD register {QUOTA_PROJECT} "
                  f"(developerEmail={args.developer_email})")
            continue

        try:
            res = reg.registerGcp(name=name, body={"developerEmail": args.developer_email}).execute()
            print(f"{country} {parent}: REGISTERED -> {json.dumps(res)}")
        except Exception as ex:
            failures += 1
            print(f"{country} {parent}: FAILED -> {_reason(ex)}", file=sys.stderr)

    if args.commit and not failures:
        print("\nDone. Merchant API should answer within ~5 minutes. Verify with:\n"
              "  ./venv/bin/python -c \"import sys; sys.path.insert(0,'.');"
              "from dotenv import load_dotenv; load_dotenv('.env');"
              "import backend.gsd_campaigns_service as g;"
              "g._list_subaccounts('5592708765'); print(g._mc_backend_status())\"")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
