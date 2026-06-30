# Auto-Redirects scoring redesign — plan (2026-06-30)

Source: user questions in `~/redirects.txt` (3 lists). Goal: make the reliability
score reflect **(1) query coverage** and **(2) facet/category product-count
dominance** — the two confidence signals the user named.

## Root cause (confirmed against live baseline of the 30 flagged URLs)

The flagged redirects are scored by **flat constants** in the search-derived
branches of `main_parallel_v2.py`, NOT by `calculate_reliability_score`:

| match_type | line | flat score |
|---|---|---|
| search_derived_samecat | 2904 | 65 |
| search_derived_samecat_faceted | 2897 | 70 |
| search_derived_subcat (rescue) | 2555 | 75 |
| search_derived_subcat_multi_facet | 2454 | 70 |
| cross_type_rejected_kept_origin | 2607 | 70 |
| origin_subcat_name (L13) | 2839 | 70 |
| cross_maincat_fallback | 1159 (helper) | 65 / 45 |

`dom_cat_share` (dominance) and `total` (count) are computed in
`search_derived.py` and even interpolated into the reason strings, but never
enter the score. That is why list #2 (poor fit) and list #3 (good fit) both
land on 65 — the score is constant.

## STATUS (2026-06-30)
- **Phase A: DONE + regression-validated.** 300-URL OLD-vs-NEW diff: 0 production
  A/B redirects fell to D, tier B grew +6, 0 redirect URLs changed (score-only),
  mean delta -0.6. 26 existing + 8 new V45 tests pass. List #2 all demoted; List
  #3 miele 65→72. NOT yet committed.
- **vazen coverage-timing fix: DONE.** subcategory_name* rows now re-score
  coverage AFTER the facet append (double-vowel collapse grote≈Groot + filler
  exclusion "mooie"), lift-only. `mooie_grote_vazen` 65→95(A). Corpus regression:
  0 over-lift, 0 subcategory rows wrongly lifted (narrowly scoped to
  qualifier-faceted rows). 34 tests pass.
- **Phase B (list #1 matcher): NOT STARTED.**

## Phase A — scoring (lists #2 & #3). Conservative: penalty-only on weak fits, no auto-suppression.

- **A1** `score_search_derived(base, coverage, dom_share, dom_count, match_type)`
  in `reliability_scorer.py`. Tunable bands (module constants):
  - coverage: two-sided (reward >=90/75, penalise <50/25)
  - dominance (dom_share): two-sided (reward >=0.85/0.65, penalise <0.45/0.30)
  - absolute count guard: PENALTY ONLY (dom_count <1000 / <500 / <200) — a tiny
    result set makes share unreliable (motorhelm share=1.0 on 242 products).
- **A2** Plumb `dom_cat_count` (probe payload `search_derived.py:292`) through to
  the output row + into the scorer. Use it (not maincat `total`, which is bogus
  in fallback mode) for the count guard.
- **A3** Replace the flat constants above with `score_search_derived(...)`.
- **A4** Coverage-calc gap fix: treat "mooie" etc. as non-counting (generic
  adjective / stopword) and credit a query token represented in the APPENDED
  facet value, so `mooie_grote_vazen` reports true coverage.
- **A5** Regression harness over the 30 cases + corpus sample; iterate bands.

## Phase B — matcher / facet-selection (list #1), after A
- (a) facet-value probe INSIDE the resolved subcat (biggest bucket)
- (b) wrong-value-within-right-facet disambiguation (tuinaarde liters, dubbele fietstas)
- (c) cross-maincat preference (bedhekje→baby_peuter, tochtstopper→klussen)

## Test assets
- `/tmp/redirects_baseline.csv` — 30 flagged URLs (col `list` = 1/2/3)
- `/tmp/baseline_out.csv` — current-engine output (the "OLD" side of the diff)
- harness: re-run engine + diff OLD vs NEW per row, assert #2 down / #3 up.
