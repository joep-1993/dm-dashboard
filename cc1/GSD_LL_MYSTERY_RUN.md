# GSD Low-Linkage — the mysterious daily 09:50 run

**Status:** under investigation (2026-07-21). Kill switch added as a safety net; root-cause trigger not yet identified.

## Symptom

In **GSD Campaigns → Enabled/Paused history**, a low-linkage run appears **every day at exactly 09:50 CET** that pauses/enables GSD campaigns. Nobody triggers it manually and nothing in the dm-tools app is scheduled to run it. It looked like it fired right after the daily Excel retrieval at 09:50.

## What actually happens at 09:50

Two *separate* things coincide at 09:50, which is why it looks chained:

1. **Excel retrieval (read-only).** A Windows Scheduled Task `GSD-LL-Excel-Load` (DB row `pa.scheduled_tasks` id=2, working dir `C:\Users\l.davidowski\dm-dashboard`) runs `curl -X POST http://localhost:8003/api/gsd-campaigns/ll/excel-load` daily at 09:50. That endpoint only refreshes the in-memory cache — its docstring literally says *"Does NOT pause/enable any campaigns."* dm-tools *also* has an internal 09:50 timer (`_excel_scheduled_run`) that only reads the Excel. **Neither pauses/enables anything.**

2. **The real pause/enable run (mystery).** Something *else* executes an actual `run_low_linkage` at 09:50 and writes real rows to the audit table.

## Evidence gathered

- **Audit table `pa.jvs_gsd_ll_campaigns`** had 75 genuine rows on 2026-07-21 at 07:50–07:51 UTC (= 09:50 CEST): Paused for shops below the ~30 linkage threshold, Enabled above it. These rows carry the **pixel-linkage %** — a value only the feed/Excel pipeline has, never present in Google Ads change events. The table's *only* writer is `_record_action`, called only from a real `run_low_linkage`/`apply_selected`. So a genuine run executed.
- **Every version of the codebase is read-only on schedule.** `_excel_scheduled_run` never calls `run_low_linkage` in any git revision back to the feature's origin (commit `620b19d`, "low-linkage shop pauser driven by the pixel-monitor feed"). The only Timer is the read-only Excel cache. `run_low_linkage` is reachable only via `POST /ll/run` (or `/ll/apply`) — i.e. an HTTP call.
- **Activity Log entries are all `backfill-*`** — reconstructed by `backfill_activity_from_gsd` from the audit table + Google Ads change history. A run through the dm-tools UI would create a live (non-backfill) entry; there is none. The daily Paused-batch@07:50:0X + Enabled-batch ~1 min later pattern goes back to at least 2026-07-17 with machine-second precision → automated, not manual.
- **Production box 3003 is cleared.** `service.log` on `win-htz-006.colo.beslist.net:3003` is a **uvicorn access log** (every HTTP request logged regardless of Python log level). It shows **0× `/ll/run` and 0× `/ll/apply` in the 09:50 window** today, so the mutations did **not** go through 3003.
- **The caller IP: `94.142.210.226`.** Of 36 historical `POST /ll/…` calls on 3003, **31× `/ll/run` all came from this one public IP** (vs `/ll/excel-load` from `127.0.0.1`, the local Windows task). It is **not** the dev laptop's egress IP (`143.178.166.201`) — it's a distinct networked host, i.e. an automated server-side caller. The last real `dry_run=false` run on 3003 was access-log line **18574268**; after that 3003 only saw excel-load + dry_run=true previews → the real runs migrated *off* 3003 at that point.
- **Zombie instances.** l.davidowski's commit `3bf8995` notes killing zombie uvicorn instances on ports **8003/8098/8099**, each running its own Excel scheduler (duplicate Slack notifications). Those were alive at 09:50 today and are viable executors; all killed ~10:36.

## Current best theory

An automated server-side caller (`94.142.210.226`) POSTs `/ll/run` (dry_run=false) daily at ~09:50 to a **laptop/dev instance** (8003/8098/8099) — not to production 3003. The instance executes the run and writes the audit rows; its logs went to stdout/terminal and were never captured. Identity of `94.142.210.226` and the exact trigger (n8n workflow? a hand-made `schtasks` task? a cron on that host?) is **not yet confirmed**.

## Open questions / next steps

1. **Identify `94.142.210.226`** (reverse DNS from a box with a resolver, or ask infra) — this is the single most actionable lead.
2. From `service.log`: were the 31 `/ll/run` calls at ~09:50 and daily? What date is line 18574268 (when it left 3003)? dry_run true/false split?
3. Check l.davidowski's **Windows Task Scheduler** for any `DM-Dashboard-*` or hand-made task POSTing `/ll/run` (invisible from WSL).
4. Watch tomorrow (09:50) now the zombies are dead — does it stop, or land on another live port?

## Instrumentation & mitigation in place

- **Request/run logging** (l.davidowski, `3bf8995`): `/ll/run` and `/ll/apply` log port + IP + params + call stack at WARNING; `run_low_linkage`/`apply_selected` log `STARTED` with port + PID + call stack. Only emits on instances running that code (needs restart).
- **Kill switch** (this work): env `GSD_LL_KILL_SWITCH` / `POST /api/gsd-campaigns/ll/kill-switch?enabled=true`. When active, `run_low_linkage` is forced to dry-run and `apply_selected` returns a blocked result — **no campaigns mutated regardless of caller** — and the blocked attempt is logged with port/pid. `GET /ll/kill-switch` reports state.
  - **Deployed as a safety net on the dev 8003 instance** (started with `GSD_LL_KILL_SWITCH=true`). If the mystery caller hits *this* laptop's 8003 tomorrow, the run is blocked + logged instead of executed.
  - Toggle **off** (`?enabled=false`) before doing a deliberate manual run from this instance.

## Caveats

- A source change only affects a process **after restart** (bare uvicorn, no `--reload`).
- The kill switch protects only the instance it's deployed on. The real runs appear to target l.davidowski's dm-dashboard side, so the definitive fix is to identify and disable the external trigger there.
- 3003's `/ll/run` still mutates; if the caller re-targets 3003, it will fire (and be logged). Consider deploying the kill switch / a decoy endpoint there too if the run recurs.
