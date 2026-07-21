# Healthscore 2.0 — Plan

## 0. Goal

Today only ~35% of monthly organic SEO visits land on a URL in the HTML-sitemap
set. HS2.0 redesigns URL **selection** to maximize that coverage, using a
score-first model with guaranteed buckets, a fixed per-category cap, and
backtest-derived weights over four business variables: **search volume,
revenue, CTR, bounce** (+ a momentum signal).

**North-star KPI:** % of monthly organic SEO visits landing on a set URL.
Measured on `datamart.fct_visits` organic (channel via `chan_deriv`,
`is_real_visit=1`) — NOT `search_console.visits` (that column counts all
visits, not organic-only).

---

## 1. Architecture decision — standalone, layered, non-invasive

HS2.0 is a **standalone Python module** (dm-tools service style) that:

1. **Reads** `bt.hs_sitemap_input_details_deepestcat` as a read-only feature
   source (search volume, OPB revenue, visits, impressions, product/offer
   counts, benchmarks). The existing build SQL — including `bakje`, `sort`,
   `*_ok` — is **left untouched**. It becomes the baseline we shadow against,
   not something we edit.
2. **Adds** the two missing variables per-URL from the SEO Stats source
   (`datamart.fct_visits`): CTR and Bounce.
3. **Adds** fresh keyword search volume from a monthly-cached Keyword Planner
   table (fills today's 3.4% search-volume coverage gap).
4. **Computes** its own composite Healthscore + normalization + buckets + cap.
5. **Writes** its own output table (`bt.hs2_data`) — parallel to the live
   `new_hs_data`, so both can be compared.
6. Exposes functions a FastAPI router + frontend page call later.

Benefits: live sitemap pipeline never at risk; trivial shadow comparison;
reversible; frontend-integration-ready.

---

## 2. Variable → source map (final)

| Variable       | Source                                                              | Grain         | Status   |
|----------------|---------------------------------------------------------------------|---------------|----------|
| Search volume  | Keyword Planner cache `bt.hs_keyword_search_volume` (monthly refresh) + existing `avg_search_volume` fallback | keyword/category | ➕ expand |
| Revenue        | `hs_sitemap_input_details_deepestcat.opb_final` / `sea_seo_revenue_final` | per URL       | ✅ reuse |
| **CTR**        | `fct_visits` (bvb+outclicks)/visits, per URL via `dim_visit.url`    | per URL       | ➕ add    |
| **Bounce**     | `fct_visits` no-product-click share, per URL                        | per URL       | ➕ add    |
| Momentum       | `fct_visits` last-14d vs prior-14d visits                           | per URL       | ➕ add    |
| Coverage KPI   | `fct_visits` organic (chan_deriv)                                   | measurement   | —        |

**SEO Stats definitions (must match the existing tool):**
- CTR = `Σ(number_of_bvb_clicks + number_of_outclicks) / Σ visits`
- Bounce = `Σ(visits with number_of_cpc_productclicks=0 AND number_of_ww_productclicks=0) / Σ visits`
- Revenue (own omzet) = SEO-Stats uses `bt.cpa_outclicks_transactional.click_revenue`; HS input's `opb_*` is the pre-computed equivalent — reuse it.

**Grain caveats:** join on canonicalized `url`; use `dim_visit.type_url` for
URL-type splits, never `clean_url` (rolls facets up to base category). Country
= `'nld'` in fct_visits world, `'nl'` in the HS input tables — reconcile.

---

## 3. The Healthscore formula

Per candidate URL, six features, each **percentile-normalized within its
category** (`deepest_category_id`); skewed features (`log1p` first): search
volume, revenue, CTR, (1−bounce), momentum, and impressions/position upside.

`HS = Σ wᵢ · percentile(featureᵢ)`, linear + explainable. Weights `wᵢ` from
backtest (§5).

Roles by grain:
- Per-URL discriminators (which URLs win a slot): CTR, bounce, revenue, momentum.
- Category-demand signals (how many slots a category gets): search volume,
  aggregate impressions.

---

## 3b. R-urls are the coverage lever (Phase 1 finding + decision)

Phase 1 proved the ~35-45% overall coverage is **entirely an R-url problem**:
non-R-url coverage ≈86% (C-url ~90%, PLP ~78-80%, Browse ~100%), but R-urls are
~51% of SEO visits and only ~3-5% covered (set holds ~16k R-urls of ~5.5M).

**Decision (user): option (a)** — R-urls are scored and capped in like any other
URL, to maximize coverage.

Implications:
- `bt.hs_r_urls` = candidate universe only (~5.5M urls + `deepest_subcat_id`,
  NO signals). HS2.0 builds R-url features itself.
- R-url features come from the SAME `fct_visits` join as C-urls
  (`dim_visit.type_url='R-url'`): visits, CTR, bounce, revenue, momentum.
- R-url search term = `dim_visit.r_terms` → optional Keyword Planner volume,
  scoped to candidate R-urls only (not all 5.5M) to keep the cache lean.
- Cold-start R-urls (in hs_r_urls, no visit history): eligible via the
  guaranteed/new bucket + keyword-demand signal.

## 4. Selection (score-first + guarantees + fixed cap)

Per category, per run:
1. **Guaranteed slots:** new URLs (created ≤2wk), seasonal (strong SEO visits
   in the upcoming fortnight last year), rising (top momentum).
2. Fill to fixed cap **N** by descending HS.
3. **Fixed cap N** set from the Phase-3 coverage-vs-N knee per category
   (floor/ceiling guardrails); frozen per run.
4. Convert today's hard `*_ok` gates into **score inputs**, not filters —
   a URL earning real organic visits should not be excluded for failing an
   impressions/OPB benchmark.

---

## 5. Backtest (derive weights + validate + set N)

1. Train month M, holdout M+1 (features as-of end-of-M — no leakage).
2. Target = organic SEO visits in M+1.
3. Grid/coordinate search weights to maximize holdout **visit coverage** at cap.
4. Produce **coverage-vs-N curve per category** → pick N at the knee.
5. Validate on a 2nd holdout month (seasonality guard).
6. Deliverable: `current 35% → projected coverage`, weight vector, N table →
   user sign-off before cutover.

---

## 6. Keyword Planner integration

- Universe: 1,083,275 distinct `ga_keyword` (NL); only 37,279 (3.4%) have
  volume today.
- Cost: 10k keywords/request → ~109 requests for full refresh; 35 rotating
  customer IDs. Refresh **monthly** (search volume is a 12-mo avg), cache in
  `bt.hs_keyword_search_volume`. Twice-weekly runs read cache → **0** added
  API calls.
- Confirm dev-token access level (Basic vs Standard) before first backfill.
- Long-tail returns 0/null — treat 0 as signal.

---

## 7. Phases

- **Phase 0 — DONE (discovery):** engine = `hs_sitemap_input_details_deepestcat`;
  bakje/sort/*_ok in its build SQL; standalone-layered architecture chosen.
- **Phase 1 — Coverage KPI harness:** true coverage query on fct_visits organic;
  historical baseline + split by type_url/category.
- **Phase 2 — Feature build:** per-URL CTR/bounce/momentum from fct_visits;
  keyword-volume cache; join to HS input on canonical url.
- **Phase 3 — Score + backtest:** normalize, grid-search weights, coverage-vs-N
  curve, N table. → sign-off gate.
- **Phase 4 — Selection + writer:** score-first + guarantees + cap → `bt.hs2_data`.
- **Phase 5 — Shadow + cutover:** 2–3 cycles projected vs actual; repoint
  renderer; standing coverage tile; monthly weight re-fit.
- **Phase 6 — Frontend:** FastAPI router + dm-tools page (run trigger, coverage
  dashboard, per-category N + weight controls).

---

## 8. Key tables (reference)

- `bt.hs_sitemap_input_details_deepestcat` — HS engine input (3.3M rows, per URL);
  also `_bidcat`, `_maincat`, `hs_imc_input_details_deepestcat`, `hs_r_urls`.
- `bt.new_hs_data` — current denormalized output (datasets: visits / new_urls /
  search_console).
- `datamart.fct_visits` + `datamart.dim_visit` — CTR/bounce/momentum/coverage
  (via `chan_deriv.ref_channel_derivation_stats`).
- `bt.cpa_outclicks_transactional` — own-omzet revenue (SEO Stats basis).
- dm-tools `backend/keyword_planner_service.py` — `get_search_volumes()`.
- dm-tools `backend/seo_stats_service.py` — CTR/bounce/revenue definitions.
