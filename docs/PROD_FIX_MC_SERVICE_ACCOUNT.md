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

## 0. Is the key even on this machine? (read-only)

`backend/service_accounts/` is **gitignored** (`.gitignore:63`), so the key files are NOT in
the repo and a `git pull` will never deliver them. Check first:

```powershell
Get-ChildItem backend\service_accounts\*.json | Select-Object Name, Length
```

* `acoustic-racer-258913-e55feb91bacc.json` present → continue to step 1.
* absent → copy it across from the laptop
  (`\\wsl.localhost\Ubuntu\home\joepvanschagen\projects\dm-dashboard\backend\service_accounts\`).
  It is a **private key**: copy it directly to `backend\service_accounts\` on this machine,
  never through the repo, a branch, a paste bin or a ticket.

To confirm you have the right file without opening the key material:

```powershell
.\venv\Scripts\python.exe -c "import json;d=json.load(open(r'backend\service_accounts\acoustic-racer-258913-e55feb91bacc.json'));print(d['client_email'],d['project_id'],d['private_key_id'][:12])"
```

Expect `beslist-index-checker@acoustic-racer-258913.iam.gserviceaccount.com`,
`acoustic-racer-258913`, `e55feb91bacc`.

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

---

# 2026-09-08 — the Content API sunset (separate problem, same key)

## Symptom

Run rows like `content_api_sunset: Content API for Shopping was sunset on August 18, 2026
for GCP project with id acoustic-racer-258913 …`, **and** rows of `Resource was not
found.` for every label of a shop. Both come from the same cause, so do not chase them
separately. The run panel now shows an orange banner saying which Merchant Center API
carried the run.

## Cause

Google sunset the Content API for Shopping on **2026-08-18** for GCP project
`acoustic-racer-258913` (909970840068) — the project of the only key with Merchant Center
access. Enforcement is **ramped, not a hard cut-off**: measured on 2026-09-08, ~8% of
otherwise identical `accounts.list` calls returned **HTTP 410 `content_api_sunset`** and
the rest succeeded. It is a lottery per HTTP call, and it goes to 100%.

That is why a run creates campaigns for some shops and not others:

* a shop that loses the draw during its sub-account lookup fails at step `mc_account`;
* a shop that loses it during the MC→Ads link ends up with a Merchant Center account that
  has **no** `adsLinks`, and every campaign then fails with the Google Ads error
  `Resource was not found.` (RESOURCE_NOT_FOUND on `shopping_setting.merchant_id`) — which
  points at Google Ads while the actual gap is the link.

## What the code does about it now

`gsd_campaigns_service.py` prefers **Merchant API v1** and falls back to the Content API
per call, retries the sunset 410 (`_mc_call`), lists each Merchant Center parent **once
per run** instead of once per shop, checks the MC→Ads link from the **Google Ads** side
(`product_link`) instead of the Content API, and refuses to create campaigns for an
unlinked account (error step `mc_ads_link`). Tests: `backend/test_gsd_mc_backend.py`.

## One GCP project per Merchant Center account

Merchant API does not work the way the Content API did. Two rules together decide
everything:

* *"Each Google Cloud project can only be registered with a single Merchant Center
  account at any given time"* — a second one returns `ALREADY_REGISTERED`;
* the project that gets registered is the project the **credentials** belong to. You
  cannot pass it in the call.

Three parents therefore need three GCP projects, and `acoustic-racer-258913` could never
have covered more than one of them. Set up on 2026-09-08:

| market | GCP project | service account | key |
|---|---|---|---|
| NL 5592708765 | `beslist-skippy` | `gsd-account-creation@beslist-skippy…` | `GSD_SERVICE_ACCOUNT_FILE_NL` |
| BE 5588879919 | `beslist-pegel-factor` | `gsd-account-creation@beslist-pegel-factor…` | `GSD_SERVICE_ACCOUNT_FILE_BE` |
| DE 5342886105 | `beslist-pattas` | `gsd-account-creation@beslist-pattas…` | `GSD_SERVICE_ACCOUNT_FILE_DE` |

Verified the same day against both APIs: identical account sets, identical names, identical
order, and Merchant API is ~2.5x faster. Names matter more than they look — the account
name is the only key the sub-account lookup has.

**The per-market keys are Merchant-API-only.** Their projects do not have the Content API
switched on (403 `accessNotConfigured`), so the Content API fallback deliberately keeps
using the shared `GSD_SERVICE_ACCOUNT_FILE` (acoustic-racer). Do not "tidy that up" by
routing the fallback through the per-market keys; that turns the safety net into a hard
failure.

## Adding a market, or replacing a key

1. A GCP project of its own, with `merchantapi.googleapis.com` enabled.
2. A service account in that project, **Admin** on the Merchant Center account.
3. The project registered with that account:
   `./venv/bin/python scripts/gsd_register_merchant_api.py --only NL --commit`
   The developer email must be a **human** — Google rejects a service-account address with
   `PERMISSION_DENIED_TO_REGISTER_GCP_WITH_SERVICE_ACCOUNT`. The identity that *authorises*
   the call may be a service account, as long as it has Admin.
4. The key in `backend/service_accounts/` (gitignored — copy it across, never through the
   repo) and `GSD_SERVICE_ACCOUNT_FILE_<CC>` in `.env`.

A market with no key of its own silently uses the shared key, which reaches Merchant API
for no account at all — so it lands on the Content API fallback. The run result says which:

```bash
./venv/bin/python -c "
import sys; sys.path.insert(0,'.')
from dotenv import load_dotenv; load_dotenv('.env')
import backend.gsd_campaigns_service as g
[g._list_subaccounts(p) for p in ('5592708765','5588879919','5342886105')]
print(g._mc_backend_status())"
```

Expect `{"backend": "merchant_api_v1", "per_market": {"NL": …, "BE": …, "DE": …}}`. Any
`content_api_v2.1` in there is a market still riding the sunsetting API, and the run panel
shows an orange banner naming it.

## Do not

* Do not treat `Resource was not found.` as a Google Ads problem. Check
  `product_link` for the shop's Merchant Center id first.
* Do not "fix" the 410 by lowering the retry count because runs feel slow — the retries
  are the only reason runs still complete at all.
