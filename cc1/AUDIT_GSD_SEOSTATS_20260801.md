# Audit — GSD Campaigns & SEO Stats (2026-08-01)

Scope: 12 597 lines over 7 files, reviewed in six independent slices
(`gsd_campaigns_service.py` in three parts, `gsd_campaigns_router.py`,
`frontend/gsd-campaigns.html`, `seo_stats_service.py` + router,
`frontend/seo-stats.html`). Every HIGH below was re-verified by hand against the code —
line numbers are as of commit `a56afea`.

**Two reported HIGHs did NOT survive verification** and are recorded here so nobody
"fixes" them:

* *"visits joins are missing `deleted_ind = 0`, so visit counts are inflated."*
  Measured: `chan_deriv.ref_channel_derivation_stats` holds **219 rows, all
  `deleted_ind = 0`, zero duplicate `(aff_id, channel_id)` pairs**. Nothing is inflated
  today. Adding the predicate is consistency hygiene (LOW), not a bug fix.
* *"Dagoverzicht CTR/Bounce should show a percentage-POINT badge."* That is Joep's
  explicit decision of 2026-07-31 (relative %, not pp). The only defect is the stale
  comment at `frontend/seo-stats.html:1908` claiming "rates pass a pp badge".

---

## HIGH — verified

| # | Finding | Where | Behaviour-preserving fix? | Effort |
|---|---------|-------|---------------------------|--------|
| H1 | "Exclude these shops" runs on **exactly those shops** | `gsd_campaigns_service.py:1449` | No — inverting it is the fix | S |
| H2 | `/api/seo-stats/dashboard` returns 500 for a day with no SEO visits | `seo_stats_service.py:94-98` | Yes | XS |
| H3 | `activated` campaigns cannot be undone | `gsd_campaigns_service.py:2497` | Yes | XS |
| H4 | Adoption never labels same-name campaigns (the 2 954 cohort) | `gsd_campaigns_service.py:2566-2579` | No (more labels — intended) | XS |
| H5 | Preview's activate tile always reads 0; SA360 note never shows | `frontend/gsd-campaigns.html:1852` | Yes | XS |
| H6 | Preview under-reports what the run will pause | `…:2996` vs `…:2766` | Yes (preview only) | M |
| H7 | A failed ENABLE is filed as `skipped` | `gsd_campaigns_service.py:2626` | No (bucket change) | XS |

**H1 — the scoping control is inverted.** `get_redshift_shop_changes` always appends
`shop_name IN (…)`; `included` only decides whether `actie IN ('aan','uit')` is added.
The router documents the opposite (`"If true, only include listed shops; if false,
exclude them"`), the UI defaults to *Exclude these shops*, and `toggleShopMode()`
(`frontend/gsd-campaigns.html:531`) enables the radios as soon as the textarea has
content — so the control is reachable, not parked. `gsd_ll_service.py:2047` implements
the intended semantic correctly (`(name in wanted) == bool(included)`), so the two halves
of the same page disagree. Decide first whether the UI wording or the backend behaviour
is the truth; do not ship both silently.

**H2 — live every morning until the ETL lands.** `_pct_delta` guards only the baseline
(`if not p1: return None`) but `_ratio()` / `_opb()` return `None` for a zero-visit day
and are passed as **p2**: `_pct_delta(12.3, None)` → `TypeError`. Confirmed against the
running server on 2026-08-01: `?date=2026-07-31` → **500**, `?date=2026-07-30` → 200.
Dagoverzicht defaults to yesterday. Fix: `if p1 is None or p2 is None or not p1: return None`.

**H3 — the undo gap.** `_repair_campaign`'s complete-and-correct path is the only return
without `campaign_resource`; `_create_campaigns_for_shop` then flips that same dict to
`action="activated"`, so `run_gsd_script` never derives a `campaign_id` and the frontend's
undo builder (`gsd-campaigns.html:2128`, `filter(c => c.customer_id && c.campaign_id)`)
drops it. Reset's confirm promises to pause N created campaigns and leaves the activated
ones live. Fix: add `"campaign_resource": campaign_resource` to that return.

**H4 — adoption misses its own cohort.** The `_apply_label_to_campaign` call is nested
inside `if existing_name != campaign_name:`, but the 2 954 unlabelled campaigns have a
*correct* name (their label application failed after create). `_pause_campaigns_for_shop`
adopts on `not info["labelled"]` regardless of name — already drifted. Fix: dedent one
level; keep the `logger.info` inside the mismatch branch.

**H5 — my wiring miss from 2026-07-31.** The backend returns `to_activate` and
`awaiting_bid_strategy` (`…:2867`, `…:2869`), the tile and the note read them
(`gsd-campaigns.html:1951`, `:1958`), but the `previewMeta` literal never copies them.
Clicking the teal tile does filter correctly, so only the counters are wrong.

**H6 — preview ⊂ run on the pause side.** The run pauses via two sources: a
`campaign_label` query keyed on `[shop:variant]` (no shop_id, no channel filter) **or**
identity over the candidate list. Preview iterates only the candidate list, which requires
`[shop_id:N]` and `SHOPPING`. A shop whose Redshift `shop_id` is NULL previews as "0 to
pause" while the run pauses everything it finds. Divergence is in the dangerous direction.

---

## MED — the honest-reporting cluster

* `gsd_campaigns_service.py:3183` — `if not changes: return` sits **before**
  `reconcile_run_logs(...)` (`:3397`), so "just run it again" heals nothing on a day with
  no shop changes, or when the Redshift query fails.
* `…:3849`, `…:3868` — reconcile reads only `["inserted"]` from `upsert_created_dates`
  and `push_mc_ids_to_redshift`; both return `{"inserted": 0, "error": …}` on failure, so a
  dead DB is indistinguishable from "nothing to do". The sheet branch (`:3892`) does it
  right — three sinks, two conventions.
* `…:3389` — `record_created_campaigns` stamps **today** as the creation date of
  campaigns that were merely activated (`overall_results["created"]` deliberately holds
  `activated`), and `ON CONFLICT DO NOTHING` makes it permanent.
* `…:3309` — `created_count` counts only `action == "created"`, so a run that activated
  five campaigns logs `campagnes aangemaakt? nee` in the sheet.
* `…:3793` — the sheet dedupe key is `(shop_id, country)` and ignores column I
  (`aan`/`uit`), so an `uit` row within `SHEET_DATE_TOLERANCE_DAYS` suppresses a later
  `aan` row. The ±2-day tolerance itself is correct and deliberate.
* `…:2465` / `…:3060` — `ags[0]` from an unordered GAQL result: verify and repair can
  inspect different ad groups of the same campaign and flap. Add `ORDER BY ad_group.id`.
* `…:1487-1504` — `_lookup_label_resource` caches hits but not misses, and is called
  inside the per-label loop (`:2600`); an account without `GSD_LL_PAUSED` pays one extra
  round trip per label per shop. The preview hoists the same call correctly (`:2961`).
* `frontend/gsd-campaigns.html:678` — `loadCampaigns()` reads only `data.campaigns`; a
  failed account is reported in `data.errors` and its rows are simply absent, so a broken
  DE query looks like "DE has no campaigns".
* `…:843` — bulk selection lives only in the DOM; any keystroke, sort or page change
  rebuilds `tbody` and silently discards it. **FIXED 92d96e8** (see below).
* `…:1999`, `…:2260` — neither sort-rank map knows `activate` / `activated`, so today's
  new rows sort below `skipped` in the default view.
* `frontend/seo-stats.html:1727-1774` — no request sequencing: a slow older `/daily` can
  overwrite a newer one, and `loadTileDeltas()` re-reads `lastData` after its own await, so
  run A's baseline can be divided into run B's totals across all eight tiles.
* `…:1738-1769` — a failed reload leaves the tiles, chart and Total row showing the
  previous range under a "Failed to load" line.
* `…:1347` — the heatmap scale is computed over all rows while the Total row follows the
  weekday filter; filter to "ma" and every cell is uniformly red.
* `…:1451-1454` — Top-categories columns are hardcoded "Yesterday"/"Week before" while
  the backend uses `ref-1`/`ref-8` for revenue and any `#catDate` for the day.
* `seo_stats_service.py:747-770` — Dagoverzicht revenue/OPB compare `d` vs `d-7`, while
  the module's own convention (and `get_deltas`) is `d-1` vs `d-8` because revenue settles
  late. The two cards on the same page disagree.
* `seo_stats_service.py:505` — the `level="sub"` aggregation is queried on every
  `/deltas` and read by nobody (`frontend` maps the sub table to `deepestcats`); `/deltas`
  is fetched twice per page load, so that is two wasted Redshift aggregations.

## LOW

Dead code with zero call sites: `_campaign_name_variants` (`…:796`, added and superseded
the same day), `loadStats()` (a permanent no-op behind `if (!document.getElementById(
'statTotal')) return;`, still called from 10 places), eight GSD-frontend functions
(`formatCost`, `exportXlsx`, `getCampaignRows`, `pauseCampaign`, `enableCampaign`,
`removeCampaign`, `togglePreviewRow`, `exportPreviewXlsx`), `skeletonInto`
(`seo-stats.html:1719`), the unreachable `logger.info` after `return False`
(`…:1808-1810`), `_as_distribution`'s fallback loop, and assorted dead CSS/ids.
Also: `asyncio.get_event_loop()` in 20 router coroutines (deprecated since 3.10), `_CACHE`
never evicts, a malformed `?date=` returns 500 instead of 400, and `_MACRO_MICRO_RE`
scans the `[shop:…]` tag so a shop literally named "Macro.nl" would be unadoptable
(latent — no such shop verified).

**✅ SHIPPED 2026-08-05 (`d682c39`)** — the dead functions went in Phase 3 (`751399a`); this
commit closes the rest. Every item was re-checked against the code first, and the list was
wrong or incomplete on three of them:

* **`_MACRO_MICRO_RE`** — fixed by narrowing the tag body to `[^\]:]`, so only a BARE tag can
  mark a variant. Grounded before changing it: of 2.856 live campaigns, 120 carry a
  macro/micro tag and **all 120 are bare** (`[macro]` 60, `[macro+micro]` 60). Zero keyed
  forms, both patterns flag the same 120 names, and `[shop:Macro.nl]` now passes. Not as
  latent as the audit implied: the run would have created a duplicate campaign beside such a
  shop **every day**.
* **Malformed `?date=`** — a `_check_date` helper in both routers. **Call it before the
  `try`:** HTTPException is an Exception, so validating inside the block lets the handler's
  `except Exception` relabel the 400 as a 500 — the same bug one indent deeper. Both helpers
  carry that warning. GSD's `/preview`, `/run` and `/ll/run` now reject a typo before
  spending a Redshift round trip or starting a real run; verified `running=false` afterwards.
* **`_CACHE`** — sweeps expired entries on write plus an oldest-first cap of 200 that never
  evicts the key being written. 1.000 distinct keys → 200 entries.
* **`_as_distribution`'s fallback loop — the list was WRONG to call this deletable.** It is
  unreachable today, but `total` sums all of `raw`, so a bucket missing from the order list
  stays in the denominator: delete the loop and the slices silently stop summing to 100%. It
  now `logger.warning`s when it fires, which also catches the subtler harm — an unmentioned
  bucket is appended at the end and shifts every slice colour, exactly what the order list
  exists to prevent.
* **Two items no longer existed**: no unreachable statement follows any `return` in
  `gsd_campaigns_service` (AST-checked, zero hits — the H4 dedent took it), and
  `.dev-legend` was already gone.
* **Dead ids/CSS, measured not assumed.** Removed: seo-stats' `#loadBtn` disable/re-enable
  dance (button gone, `if (btn)` guarded the one certainty, so three lines + the `finally`
  were no-ops — `loadStats`' shape again); `.stat-card`, `.btn-action`, `.btn-pause`,
  `.btn-activate`, `.btn-remove` in gsd-campaigns (orphaned by `751399a`);
  `.activity-log .log-entry/.log-time/.log-action` (the log is a table now);
  `.delta-card` + its six `.dc-*` descendants; `.neg`.
  **KEPT, because a class grep is not proof of death** — and all four look dead to a naive
  scan: `.log-success`/`.log-error` come from a template literal
  (`entry.success ? 'log-success' : 'log-error'`), `.metric-tile` is assigned via
  `card.className`, `#sparkTip` is created at runtime by `sparkTip()` so its absence from the
  HTML is correct, and every `.flatpickr-*` rule is a theme override the library applies.
* **`get_event_loop()` → `get_running_loop()`**, all 67 sites in 15 files (the audit said 20;
  it was 67). AST-verified that every one sits inside an `async def`, where the two are
  equivalent — mechanical, not behavioural.

## Decisions, not defects

* Reconcile logs an MC id whenever the `(shop_id, country, merchant_id)` triple is absent,
  while a run logs only accounts it **created**. The Content API exposes no account
  creation date, so the substitution is deliberate and documented — but it does mean Efficy
  can receive a row for a pre-existing sub-account. Joep's call which semantic is wanted.
* Preview cannot predict `repaired` without three extra GAQL reads per match. Either pay
  them or rename the preview action to `skip_or_repair`.

---

## Phased plan

**Phase 0 — ✅ SHIPPED 2026-08-05 (`9b04eaa`)** (behaviour-preserving, ~40 lines)
H2, H3, H5, surface the reconcile's sink errors, `ORDER BY ad_group.id` on both queries.
Smoke test: `/api/seo-stats/dashboard` returns 200 for today *and* yesterday; the preview's
activate tile shows a non-zero count on a day with matches.
*Gate met:* `?date=2026-07-31` was a confirmed 500 on 2026-08-01 and returns 200.

**Phase 1 — ✅ SHIPPED 2026-08-05 (`3ff1455` = H1, `8d0a1b7` = the rest)**
H1, H4, H7, the created-date filter, the sheet `aangemaakt?` flag, the sheet dedupe `uit`
filter, **and the per-shop boundary in `run_gsd_script`** (see structural risk).

* **H1 decided: the UI is the truth** (Joep, 2026-08-05). Include = allow-list,
  Exclude = deny-list. `included` no longer doubles as the `actie` filter, so the old
  side effect "`included=True` also returns shops with no change today" is gone —
  removed deliberately, and if wanted it belongs behind its own named parameter.
* *Gate met:* against 2026-08-04 (6 change rows / 5 shops, 2 picked) Include returns
  exactly the picked set, Exclude contains none of them, the two are complements over the
  day, and their row counts add up to the unfiltered run.
* The Phase 1 diff is ~443 lines but almost all re-indentation from the two exception
  wraps; `git diff -w` showed 49 code lines added / 12 removed. Review it that way.

**Phase 2 — ✅ SHIPPED 2026-08-05 (`939cf4d`)** converge preview and run (H6)
Extracted into `_pause_identity_matcher()` + `find_pausable_campaigns()`, which both
`preview_gsd_script` and `_pause_campaigns_for_shop` now call.
*Measured against NL_CPR:* Voordeelvanger.nl and Bosmenshop.nl each return 6 pausable
campaigns, identical to the old identity-only path while `shop_id` is present (no
regression). Force `shop_id` to NULL and the shared lookup still finds 6 and 5 where the
old preview reported **0**. Bosmenshop's 6-vs-5 is the useful bit: one campaign is found
ONLY by identity because it carries no GSD_SCRIPT label, so neither source can be dropped.

**The five MEDs that were in no phase — ✅ SHIPPED 2026-08-05 (`28318db`)**
Reconcile reachability (`if not changes: return` sat before it), the heatmap scale vs the
weekday filter, Top-categories column labels, the failed-reload stale render, and the
stale pp-badge comment. The reload fix needed `computeTotals()` to become null-safe, which
also closed a pre-existing latent throw.

**Phase 3 — ✅ SHIPPED 2026-08-05 (`751399a`)** cleanup, net −95 lines
Dead code, the dead `sub` cat level, label negative-caching, sort ranks, request sequencing
in seo-stats, and `loadCampaigns` surfacing per-account failures.

* **The dead-code list in this doc was WRONG about one entry.** `exportXlsx` exists in both
  pages and the **seo-stats one is live** (wired to the Export button) — it was listed here
  among the "eight GSD-frontend functions". Deleting both would have removed the SEO Stats
  export. Check each name per file before trusting a list like this again.
* `loadStats` was as described: a permanent no-op (#statTotal exists nowhere but inside it)
  called from 11 places, three of them inside functions that were themselves dead.
* **Bulk-selection state — DONE in its own commit, `92d96e8`.** Held back from Phase 3 on
  purpose (it mutates live campaigns), then shipped separately. Now a `selectedCampaigns`
  Set keyed `customer_id|campaign_id`; the checkbox renders from it and writes back to it,
  and `renderCampaigns()` re-runs `updateBulkButtons()` so the header checkbox follows the
  newly drawn page.
  The old behaviour failed SAFE (empty), the new one could fail the other way — acting on a
  set you can no longer see — so it ships with three guards, which are the actual content of
  the change:
  1. `bulkAction` resolves keys against `allCampaigns` and counts how many fall outside
     `filteredCampaigns`, then names that count in the confirm ("NOTE: n of them are not
     visible under the current filter") **before** acting.
  2. `loadCampaigns` prunes keys that no longer exist upstream — **on success only**, inside
     the `try` after the assignment, so a failed fetch cannot silently discard a selection.
     Without this, a campaign removed here or elsewhere keeps the bulk buttons enabled and
     `Copy (n)` above what an action would touch.
  3. A completed bulk action clears every key it submitted, no-ops included — their statuses
     just changed underneath, so leaving them checked invites a second live mutation on
     stale state.
  Header checkbox stays page-scoped ("select all on this page"); buttons and the Copy label
  report the whole selection, because that is the number the confirm quotes.
  `getSelectedCampaignRows` reads the same Set, so Copy and Pause can no longer disagree.
* Watch out when verifying a `/deltas` change: the response is cached AND the local :8003
  has no `--reload`, so a removed field can look unfixed twice over. Restart, then
  `?force=true`.

**LOW cluster — ✅ SHIPPED 2026-08-05 (`d682c39`)**, details under `## LOW` above. Three of
the seven items did not match the list: two no longer existed, and `_as_distribution`'s
"dead" loop needed keeping (plus a tripwire) rather than deleting. Second time this doc's own
inventory has been wrong — re-check before acting on it.

**Still open — decisions, not code. No code items remain.**
* The two "Decisions, not defects" items above (Efficy MC-id row for a pre-existing
  sub-account; pay 3 extra GAQL reads for `repaired` in preview, or rename it
  `skip_or_repair`).
* ~~`seo_stats_service.py:~726` Dagoverzicht `d` vs `d-7`.~~ **RESOLVED 2d86d6b** — Joep's
  call was to keep the window and surface the bias. Both dates are real days, so the
  arithmetic was never wrong; what differs is MATURITY (d is still filling in, d-7 settled a
  week ago), which makes `revenue_wow`/`opb_wow` read low every morning. Aligning to
  `get_deltas` would have shown 4 August's revenue beside 5 August's visits under a
  "5 August" heading. New `revenue_settling` (day is today or yesterday) drives an amber
  marker + tooltip on SEO-omzet and OPB only — visits/CTR/bounce do not settle late.
  The statistically correct fix (compare d-7 at the same maturity) needs maturity snapshots
  we do not keep.

## Biggest structural risk

`run_gsd_script` has **no per-shop exception boundary**. One shop's lookup failure aborts
the whole run and skips `_log_run_to_sheet`, `push_mc_ids_to_redshift`,
`record_created_campaigns` **and** `reconcile_run_logs` — the recovery mechanism is skipped
by the exact failure it exists to repair — while `_run_progress["running"]` stays `True`
forever. Fix in Phase 1: wrap the per-country body in `try/except Exception` that appends
to `overall_results["errors"]` and continues, and move the four logging steps plus the
progress reset into a `finally`.
