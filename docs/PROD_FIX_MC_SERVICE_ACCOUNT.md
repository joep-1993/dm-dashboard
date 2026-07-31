# Prod runbook — GSD campaign creation fails with `account_access_denied`

**For: the Claude Code agent (or engineer) working on `win-htz-006.colo.beslist.net`.**
Written 2026-07-31 from the laptop side. Everything below is read-only until the step
marked **FIX**.

## Symptom

A GSD Campaigns run reports an error row like:

```
auth/account_access_denied: The caller does not have access to the accounts: [5342886105]
```

`5342886105` is **not** a Google Ads account. It is the **DE Merchant Center** advanced
account ("beslist BV"), configured as `ACCOUNTS["DE_CPR"]["mc_id"]` in
`backend/gsd_campaigns_service.py`. The failing caller is the **service account** that
`_get_mc_service()` uses for the Content API. So: nothing needs changing in Google Ads,
and no new access needs granting in Merchant Center either — one of the keys already has
it, and the run picked a different one.

## Cause

`backend/service_accounts/` holds four key files. Measured against all three Merchant
Center parents (NL 5592708765, BE 5588879919, DE 5342886105) on 2026-07-31:

| key file | MC access |
|---|---|
| `acoustic-racer-258913-e55feb91bacc.json` (`beslist-index-checker@acoustic-racer-258913.iam.gserviceaccount.com`) | **NL + BE + DE, all OK** |
| `cla-campaign-creation-a366aea607a8.json` | none — 401 on all three |
| `cla-test-415112-c44316af6ae0.json` | none — 401 on all three |
| `seismic-machine-258913-ee09491e0f11.json` | none — 401 on all three |

When `GSD_SERVICE_ACCOUNT_FILE` is unset, `_get_mc_service()` falls back to a file from
that directory. Before today it took `os.listdir()[0]` — **arbitrary** order, not
alphabetical — so it could silently pick a key with no Merchant Center access. (The
fallback is sorted and logged now, but an explicit env var is still the correct fix.)

## 1. Check (read-only)

Run these from the prod checkout (adjust the path if it differs; the dashboard serves
from the same directory the service points at).

```powershell
cd C:\path\to\dm-dashboard
Select-String -Path .env -Pattern 'GSD_SERVICE_ACCOUNT_FILE'
Get-ChildItem backend\service_accounts\*.json | Select-Object Name
```

Three possible outcomes:

* **no match in `.env`** → this is the bug. Go to FIX.
* **match, but pointing at any file other than `acoustic-racer-258913-e55feb91bacc.json`**
  → also the bug. Go to FIX.
* **match, pointing at the acoustic-racer file** → the env var is fine; the running
  service may still have an older environment (NSSM caches `AppEnvironmentExtra` from
  when the service was installed). Check what the *process* sees:

```powershell
nssm get <ServiceName> AppEnvironmentExtra
```

Also confirm which key the app actually loaded — the backend logs the fallback:

```powershell
Select-String -Path logs\*.log -Pattern 'GSD_SERVICE_ACCOUNT_FILE is not set|service account' | Select-Object -Last 20
```

## 2. Verify the key from the prod machine (read-only, no writes)

This proves access before and after the fix. Use the prod checkout's interpreter.

```powershell
.\venv\Scripts\python.exe -c @"
import os
from dotenv import load_dotenv; load_dotenv()
from google.oauth2 import service_account
from googleapiclient.discovery import build
f = os.environ.get('GSD_SERVICE_ACCOUNT_FILE')
print('key file:', f)
c = service_account.Credentials.from_service_account_file(f, scopes=['https://www.googleapis.com/auth/content'])
print('caller  :', c.service_account_email)
svc = build('content', 'v2.1', credentials=c, cache_discovery=False)
ids = [a.get('aggregatorId') or a.get('id') for a in svc.accounts().authinfo().execute().get('accountIdentifiers', [])]
print('reachable:', ids)
for mc in ('5592708765', '5588879919', '5342886105'):
    try:
        svc.accounts().get(merchantId=mc, accountId=mc).execute(); print(' ', mc, 'OK')
    except Exception as e:
        print(' ', mc, 'DENIED', str(e)[:120])
"@
```

Expected when correct: caller is
`beslist-index-checker@acoustic-racer-258913.iam.gserviceaccount.com`, `reachable`
contains `5342886105`, and all three parents print OK.

## 3. FIX

Set the variable in the prod `.env` (create the line if absent, replace it if wrong):

```
GSD_SERVICE_ACCOUNT_FILE=C:\path\to\dm-dashboard\backend\service_accounts\acoustic-racer-258913-e55feb91bacc.json
```

Use the **absolute path on that machine**. Then restart the service so the process
re-reads `.env`:

```powershell
nssm restart <ServiceName>
```

**Before restarting, check no GSD run is in progress** — the run is in-process, so a
restart kills it mid-flight and leaves half-created campaigns:

```powershell
curl.exe -sk https://localhost:3003/api/gsd-campaigns/run/progress
```

Wait for `{"running":false}`.

## 4. Confirm

1. Re-run the probe from step 2 — DE must print OK.
2. Dry-run the script and check there are no `account_access_denied` errors:
   `POST /api/gsd-campaigns/preview` (read-only) in the GSD Campaigns UI, or
   `curl.exe -sk -X POST https://localhost:3003/api/gsd-campaigns/preview`.
3. After the next real run, any side-logs missed by earlier broken runs are healed
   automatically (`reconcile_run_logs`, part of every run since 2026-07-31). To heal
   without a full run: `POST /api/gsd-campaigns/reconcile-logs?days=7&dry_run=true`
   first to see what is missing, then the same call with `dry_run=false`.

## If a different key must be used instead

Then access genuinely has to be granted — in **Merchant Center**, not Google Ads. For
each parent account (5592708765 NL, 5588879919 BE, 5342886105 DE): Settings → People and
access → add the service account's email with **Admin**, because the flow creates
sub-accounts and adds Google Ads links. Re-run step 2 afterwards; `authinfo` must list
all three ids.

## Do not

* Do not add anything to a Google Ads account for this error — it is a Merchant Center
  permission, and the Ads side is unrelated.
* Do not delete or reorder files in `backend/service_accounts/` to "make the fallback
  pick the right one". Set the env var; the fallback is a safety net, not the mechanism.
