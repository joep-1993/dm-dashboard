# LEARNINGS
_Capture mistakes, solutions, and patterns. Update when: errors occur, bugs are fixed, patterns emerge._

## dm-tools AI titles — storage-capacity facet (GB/MB/TB) stranded at title front (2026-06-18, commit `da4d4d7`)
User-reported H1 "**128 GB** Google Pixel 9 Pro XL" for `/products/elektronica/elektronica_19875536_19934132/c/modelnaam_mob~23811518~~opslagcap_mob~99950`. The capacity should trail ("Google Pixel 9 Pro XL 128 GB").
- **Root cause:** `_SPEC_UNITS_RE` (`backend/ai_titles_service.py` ~1026) listed physical/power units (cm, kg, watt, …) but **no storage units**, so `is_spec_value("128 GB", "opslagcap_mob")` returned False. The value then fell through `_build_v3_h1`'s special-case checks into the `other_adj` bucket (~2213), which is emitted **before** the category noun — stranding capacity at the front. This contradicted the polish prompt rule that says maten like "128 GB" go "helemaal achteraan".
- **Fix:** added `tb|gb|mb|kb` to `_SPEC_UNITS_RE`. Verified `is_spec_value` True for `128 GB / 1 TB / 512 MB / 256GB` and still False for brand/model/kleur/materiaal; `_build_v3_h1(...)` → "Google Pixel 9 Pro XL … 128 GB". Reset the ~3,510 `opslagcap_mob` URLs to pending (jobs→pending + content deleted) so they regenerate.
- **Order-rules aside:** `pa.facet_position_rules` already had `opslagcap_mob` order_index 2160 > `modelnaam_mob` 1854, i.e. the *ordering* was correct — but `other_adj` only governs facets that aren't classified as specs/sizes, and the noun is emitted between `other_adj` and `sizes`. The real lever for "spec to the very end" is `is_spec_value()`, not order_index.

## dm-tools unique-titles — frontend "pending" view hides jobs that already have content (2026-06-18)
Resetting `unique_titles_jobs.status='pending'` alone is **not enough** to re-surface/regenerate a URL: the frontend pending list only shows URLs whose `pa.unique_titles_content` row is absent. After flipping 480 `t_droogrek` jobs to pending the UI still showed only 5 — the 5 without content; the other 475 kept their old titles. To force a full regen, also **delete the `pa.unique_titles_content` rows** for the set (the `unique_titles_content_bak_typefacet_reset_20260603` table is the precedent for this). Match facet URLs with `strpos(url,'<facet>~')>0` (literal — avoid `LIKE '%t_droogrek%'`, the `_` is a wildcard and would also match `o_droogrek`).

## dm-tools deploy — backend has NO --reload; restart to apply code changes before the worker runs (2026-06-18)
Live backend is bare `uvicorn backend.main:app --host 0.0.0.0 --port 8003` (no `--reload`), serving from `/home/joepvanschagen/projects/dm-tools`, with the unique-titles worker **in-process**. After editing `ai_titles_service.py`, the worker keeps using the old code until a manual restart, so a "reset to pending" right after a code fix will regenerate with the OLD logic unless you `kill <pid>` and relaunch (`nohup venv/bin/uvicorn … &`). Health check: `curl -s -o /dev/null -w '%{http_code}' http://localhost:8003/` → 307 (redirect to /static/) is healthy.

## dm-tools AI titles — doelgroep people-noun "voor X" routing in the v3 builder (2026-06-17, commits `cff9c44`, `b8e6a6f`, `76b82f8`)
User-reported H1 "Mennen Alcoholvrije **mannenstick**" for a deodorant URL (`doelgroep_drogisterij~560636 + t_deodorant~12237439`). Root cause: the **v3** deterministic builder (`backend/ai_titles_service.py`, `_build_v3_h1` loop ~2120) routes every `doelgroep*` facet into the **pre-noun** slot, placing the audience word directly before the product noun ("Mannen Stick"); the AI polish then agglutinates the pair into a Dutch compound. None of the polish guards caught it — `_v3_preserves_content` passes (both substrings survive), `_v3_preserves_brands` only checks brands, and `_v3_polish_mangled_audience` (a) only covers the closed set `{dames,heren,kinder,kinderen,meisjes,jongens,unisex}` and (b) *intentionally* allows audience+noun compounds like "dameskleding".
- **Key divergence:** the **v1** path (`generate_title_from_api` ~1812) already special-cases `doelgroep_drogisterij` as a trailing `f"voor {val}"` suffix; **v3 dropped it**. Fix 1 (`cff9c44`): restored it in v3 — `doelgroep_drogisterij` → `voor_values` ("voor mannen"), fashion doelgroep still pre-noun.
- **Value-based generalisation** (`b8e6a6f`): the generic `doelgroep` facet (and any future one) is *mixed* — mostly Heren/Dames but occasionally people-nouns (Mannen/Vrouwen → "Mannenhelm"/"Vrouwenhelmen"). Added module constant `_V3_PEOPLE_NOUN_AUDIENCE` (near `_FACET_ORDER_FALLBACK` ~128): a doelgroep VALUE in this set routes to "voor X" on ANY facet; Heren/Dames/Meisjes/Jongens/Kinder stay pre-noun (legit compounds). Started `{mannen,vrouwen}` (225 titles, the real gluing offenders incl. the helmets).
- **Scope reality check** (why value-based, not facet-based): glued people-noun compounds in the whole corpus = only **3** (1 drogisterij + 2 generic-doelgroep helmets); but `doelgroep_drogisterij` is the *systemic* case (3,491 titles using Mannen/Vrouwen/Volwassenen by convention). `volwassenen` was first **excluded** — it never agglutinates and reads fine trailing ("Fietsen Volwassenen") — then **added** (`76b82f8`) by explicit user preference ("voor volwassenen" reads cleaner), affecting ~2,126 non-drogisterij titles. Detection regex for "glued people-noun": `(^|[^a-z])(mannen|vrouwen)[a-z]` (word-start + immediately followed by letters); standalone is `…([^a-z]|$)`.
- **Deploy**: bare uvicorn, **no `--reload`** → each code change needs a manual kill+relaunch (`kill -9` the old PID — it ignored SIGTERM; relaunch `setsid nohup bash -c 'exec ./venv/bin/python3 ./venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8003'`; verify sole listener via `ss -ltnp | grep 8003`). DB-only changes (facet rules) ride the 60s cache without restart, but these were code.
- **Requeue to regenerate**: flip `pa.unique_titles_jobs.status='pending'` (+`attempts=0, last_error=NULL`) for the affected url_ids — drogisterij 7,195; mannen/vrouwen 225; volwassenen 2,126. Title-only fix → did NOT touch `faq_jobs`/`kopteksten_jobs`. Affected-set detection = stored `unique_titles_content.h1_title` matching the people-noun regex on non-drogisterij doelgroep URLs.

## Redshift type-facet URL gap → bulk-load into `pa.urls` + queue 3 pipelines (2026-06-17)
Found trafficked faceted URLs containing a **type-facet** but absent from `pa.urls`, and loaded the curated set. Recipe (now also in auto-memory `pa_urls_loading_procedure.md`):
- **Discovery**: 743 type-facet slugs from `pa.facet_position_rules WHERE is_type_facet=true` → anchored regex `(/c/|~~)<slug>~[0-9]` (trailing `~` prevents prefix collisions like fiets/fietstas) against `datamart.dim_visit.url` joined `fct_visits` (real visits, `/c/`, `dim_date_key` last 365d). Redshift creds in `dm-tools/.env` (host …redshift…, port 5439, db beslistbi). **psycopg2 `%` gotcha**: `LIKE '%/c/%'` literals must be doubled to `%%` when the query also passes a `%s` param.
- **Anti-join**: load Redshift rows into a PG temp table, `LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(raw) WHERE u.url_id IS NULL`. `pa.canonicalize_url` returns **NULL for non-NL** (beslist.be, shop.*, computers.beslist.be) — those silently inflate "missing" if not filtered by domain. 160,638 distinct → 54,857 already present → **45,925 NL missing** (45,012 no-shop / 913 winkel facet); .be etc. out of scope.
- **Load** (`/tmp/load_urls.py`, openpyxl+psycopg2 under `~/.mysql-venv`): single transaction, dry-run (rollback) → `--commit`. `pa.urls.url_id` auto-sequences, `url` is UNIQUE → `INSERT … ON CONFLICT (url) DO NOTHING`; tag `notes='redshift type-facet gap 2026-06-17'` for traceability/rollback. The 3 job tables are PK'd on `url_id`, status NOT NULL → `INSERT … SELECT 'pending' … ON CONFLICT (url_id) DO UPDATE SET status='pending'`. User-curated xlsx of **42,612** rows loaded: all new (0 collisions), queued pending in `unique_titles_jobs`, `faq_jobs`, `kopteksten_jobs`. Rollback = DELETE job rows then urls by the notes tag.

## R-URL optimizer — surfaced keyword-branch coverage/generic-facet guard (2026-06-17, commit `e03ba72`)
User-reported: `aluminium-overgangsprofiel_tapijt` → klussen `/c/materiaal~486519` 'Aluminium' at reliability **100**, note "coverage 286%". Root cause in `facet_probe.py` `_check_surfaced` **keyword-match branch** (~466): it deliberately waives the coverage FLOOR (so niche literal matches like "Ketoconazol" win), but also waived the two guards the coverage-winner branch + `_probe_one` already apply — `cov <= 1.0` (a surfaced count > base_total is OR-fallback/maincat-wide inflation: 106/37=286%) and `_is_generic_attribute_facet` (materiaal/kleur/maat). The 286% became `match_score=286` → reliability `286/100*60+20` clamped to 100; the Issue-#3 bridgeless floor didn't fire because "aluminium" literally bridges "Aluminium". Fix: keyword branch now also requires `cov <= 1.0` AND not a generic-attribute facet; Stage-1.5 `_subcat_keyword_facet` coverage capped at 1.0; `_covpct` in `main_parallel_v2.py` `min(100, …)` as a backstop. Verified the bogus materiaal~Aluminium case is rejected while a 2%-coverage niche keyword match survives; suite 26/26. Optimizer is a subprocess → no uvicorn restart.

## R-URL optimizer — V41 source-facet preservation + facet order + spurious brand/probe (2026-06-16, commit `80f7256`)
Four user-reported bad suggestions, four distinct root causes. Reproduced each by running `main_parallel_v2.py` on a 5-row CSV (venv python: `dm-tools/venv/bin/python3`, flags `--multi-facet --enable-facet-probe`), patched, re-ran to confirm.
1. **Spurious brand in the search-derived append (issue #1).** `wc_papier_aanbieding` shipped `merk~23881557` 'Paper Dreams' at **tier B** — "papier" only fuzz-matches "Paper" and IS the product noun. The V39 `brand_match_is_spurious` guard runs only on the lexical cascade `r`; the **search-derived append path** (local-matcher branch ~2185 + coverage-probe branch ~2225 in `process_url_v2`) bypassed it and set `final_score=75` directly. New helper `_spurious_brand_facet(pf_name, pf_value_name, keyword, dom_cat_name, matcher)` mirrors V39 in BOTH branches; a spurious merk/winkel is dropped and the bare (correct) dom_cat redirect kept → now lands on **Toiletpapier**, tier B.
2. **Non-canonical facet order (issue #2).** Several append points (`url_builder` lines ~225/~700, the search-derived `~~` joins) **prepend** the source URL's `existing_facet` instead of merging it into the alphabetical sort → `t_reismand~..~~dier_dierenbenodigdheden~..`. New `_canonicalize_facet_order(url)` re-sorts the `~~` pieces by facet name on the FINAL url (just before the return) and recomputes the `facet_names` column → `dier_..~~t_reismand~..`. Beslist's own canonical order is alphabetical-by-facet-name (already used in `build_faceted_redirect`); this just normalises the paths that skipped it.
3. **Bridgeless `facet_probe_fallback` score (issue #3).** `facet_probe_fallback` sets `match_score = result-set coverage%`, so `vogelgeluiden` → `ruimte_woonaccessoires~505268` 'Keuken' scored **41** — the destination subcat IS vogel-related (so coverage hit 100% via V35 Fix A) but the **FACET** has nothing to do with the query. New `_keyword_bridges_value(keyword, value_names)` in `reliability_scorer.py` hard-floors a `facet_probe_fallback` to **0** when no keyword content token (≥3 chars, exact or ≥4-char stem either direction) bridges the promoted facet value — same outcome as a generic-only lexical match (borax→'Poeder'). Synonym/leftover-probe appends ride a different match_type (`*_with_probe_facet`) so they're untouched.
4. **Existing-facet preservation = the two rules the user asked for (issue #4).** V40 only refused cross-**maincat** jumps and only on the cascade `result` (line ~1866); but late overrides (`facet_probe_fallback`, search-derived rescue, Fix D/E) rewrite `final_redirect_url` AFTER that guard and can drop the pinned facet even within the same maincat — `max_30_kg` + `t_reismand` jumped to `type_dierenriemen` 'Halsbanden' and lost `t_reismand`. New **V41 final guard** (right before the return, sees every override): when the source R-URL pins a `/c/` facet, any final URL that **jumps maincat OR drops that facet** (`_existing_facet_in_url()` checks each `~~` axis survives) is reverted to `build_category_only(parsed)` = origin subcat WITH the facet intact → `dieren_accessoires_480779/c/t_reismand~23795956`. This is the documented intended V40 outcome that V40 alone didn't reach (the probe-fallback produced a different bad result downstream of it).
- Helpers live next to `_facet_url_parts` in `main_parallel_v2.py`. 13 new tests in `tests/test_facet_preservation.py`; full suite **26/26**. Optimizer is a subprocess → no uvicorn restart.

## R-URL optimizer — V40 weight-qualifier-as-brand + maincat-bound facet (2026-06-16, commit `4a7028a`)
Two bad cross-category suggestions traced to the same morning batch that later produced V41 (`80f7256`). V40 was the first pass; V41 finished the job (the probe-fallback override sat *downstream* of V40's guard, so V40 alone didn't reach its stated `max 30 kg` outcome — see the V41 entry).
1. **Weight/range qualifier matched as a brand.** `max_30_kg` (a pet-carrier weight class) routed to a single-brand leash page: the lone token "max" coverage-matched the brand **Max & Molly** (and Lex & Max) because "max" exactly equals a brand token, so V39's `brand_match_is_spurious` didn't fire (it only rejects *fuzzy*/category-word brand hits) and the cascade shipped a cross-subcat `merk` redirect. Fix in `src/matcher.py`: new `WEIGHT_RANGE_QUALIFIERS = {max, min, maximaal, minimaal, vanaf, tot}` treated as non-distinctive for STRICT facets — `match_by_token_coverage` skips a merk/winkel value matched ONLY on a qualifier token, the per-word merk/winkel passes skip these words, and `brand_match_is_spurious` no longer counts a qualifier as a genuine brand mention. Legit multi-token brand queries unaffected ("max factor mascara" still keeps Max Factor via "factor"). Also extended `DIMENSION_PATTERN` (in `main_parallel_v2.py`) to recognise `kg/g/gram/kilo` and the `max/min/vanaf/tot N` range form, so `has_dimensions` is no longer False for weight-class keywords.
2. **Maincat-bound facet preservation (first attempt).** When the source R-URL already carries an appended `/c/` facet, that facet VALUE id is bound to its main category — the same facet/value won't exist under a different maincat, so a cross-maincat redirect silently drops it onto a page the facet can't filter. In `process_url_v2` (~line 1866): refuse any result whose main category differs from the source's when `parsed.existing_facet` is set, fall back to `build_category_only` (rebuilds the original subcat WITH the facet intact). Same-maincat cross-subcat redirects untouched. **Limitation that motivated V41**: this guard ran only on the cascade `result` and only checked maincat — late overrides (`facet_probe_fallback`, search-derived rescue, Fix D/E) rewrite `final_redirect_url` afterward and could still drop the facet within the same maincat. V41's final guard moved the check to just-before-return and added the "facet must survive" rule.
13/13 tests passed at the time. Optimizer is a subprocess → no uvicorn restart.

## R-URL optimizer — V36 cross-maincat last-resort fallback + verified collection (2026-06-12/13, commit `8bf6f03`)
User example: `/products/sport_outdoor_vrije-tijd/.../r/opvouwbare_wandelstok_anwb/` got NO redirect; expected cross-maincat jump to gezond_mooi **Wandelstokken** + **Opvouwbaar**. Two root causes found:
- **Fix E only nominates on the HEAD token** (first meaningful), so modifier-first keywords ("opvouwbare …") never reach the cross-maincat path.
- **`_is_semantic_match` didn't know Dutch consonant-doubling plurals** (stok→stok**k**en): "wandelstok" vs "Wandelstokken" stripped `-en` → stem "wandelstokk", remainder "k" not a suffix → 97-score match vetoed. Fixed in `src/matcher.py` (both directions, consonant-only).
- **V36** (`main_parallel_v2.py`): when the WHOLE cascade ends with `redirect_url=None` (both the final return AND the `rejected_long_unmatched` early return), `_cross_maincat_any_token_match` scans full keyword + every meaningful token (≥4 chars, non-stop/shop/digit) for a ≥95 subcat-name match in a DIFFERENT maincat (same-maincat is excluded — cascade step 3 already tried it; re-emitting would resurrect rejected matches). Builds subcat redirect (existing `/c/` facet DROPPED — value ids are category-scoped) + `_append_facet_to_subcat_redirect` for leftovers. Cache-only search probe (Fix E rule: AND-mode, share ≥0.6, slug-compatible) → tier C `cross_maincat_fallback_verified`; unverified → tier D and must pass `_rescue_long_unmatched_token` (first version hijacked "endoscoop riool inspectie camera kabel 30m" → accessoires **Kabels** via the attribute token 'kabel'; the guard kills exactly that). Guard needed a **vowel-collapse re-check** (`_covered_after_vowel_collapse`): its stem test ('opvouwbare'→'opvouwbar') can't see 'Opvouwbaar' (double-a) as a prefix. Prefetch loop in `main()` also nominates any-token pairs so verification has cache in real runs.
- **A/B method that caught both bugs**: re-run the last production batch through OLD code (`git stash`) and NEW code with the SAME search cache, diff redirects. First naive compare against the stored xlsx mixed in cache-drift noise — stash-A/B is the clean way. Final: 100 URLs → 98 identical, 1 rescued (wandelstok), 1 improved (opklapbed: was jumping to **Loungesets** via `facet_probe_fallback`; consonant-doubling fix keeps it in Logeerbedden with `met_matras_bed`), 0 lost.
- **Verified-rate reality check** (collection run for user's 20-row sample): top 50K R-URLs by visits (365d Redshift, via `_fetch_redshift_rurls`) → 612 fallback candidates (no redirect + cross candidate) → only **32 verified** after live prefetch (~5% of candidates; AND-dominance is a high bar). Patterns in the verified set: Philips airfryers stuck under Huis & Tuin → `huishoudelijke_apparatuur` Airfryers + `merk~Philips`; "zonder alcohol" parfums under Gezond & Mooi → `parfum_aftershave` Parfums. Deliverable `Downloads\claude\cross_maincat_fallback_verified_20.xlsx`; full set `/tmp/xmc_verified.json` (volatile). Screening order matters for speed: cheap candidate check FIRST, cascade only on hits (~5× faster than cascade-first).
- The optimizer runs as a **subprocess** of the dashboard (`rurl_optimizer_v2_service.py` spawns `main_parallel_v2.py`) — code changes need NO uvicorn restart.

## SEO week 23 vs 22 drop — internal factors cleared; seo_prio runs are PROPOSALS only (2026-06-12)
Channel SEO wk23 (31 May–6 Jun) vs wk22: revenue −17%, visits −5%. Conclusion: **purely external** (summer-category seasonality + WK-voetbalshirt spike deflating + one-off orders). Key findings while verifying:
- **`pa.seo_prio_runs`/`seo_prio_results` propose, never apply.** `seo_prio_service.py` is analysis-only (no PUT to taxv2 anywhere in dm-tools/dm-dashboard). The 2026-05-19 run proposed 10,709 `turn_off`s; live check of ALL 2,652 affected categories via `GET /api/CategoryFacetSettings?categoryId=` showed only **62 of them actually off** — and `pa.publish_log` is the daily ~250K-URL content publish (counts fluctuate daily), NOT a noindex push; don't read seo-prio application into it.
- **A real manual batch happened 2026-05-28** (in wk22): 446 CategoryFacetSettings flipped `seoPriority=false` tree-wide, concentrated in Meubels/Woonaccessoires (`t_meubelset` 120×, `kleur` 47×, `gelegenheid_woonacc` 40×, `t_stoel` 19×). Only 62 overlap the run's turn_off list; **8 were facets the run said KEEP**; 380 had ~zero visits. Status-check of the 70 with traffic (UA `Beslist script voor SEO`): 26×301 (taxonomy consolidation upward — flips justified), **44 still 200**. Biggest live-page loss: **Bureaustoelen `t_stoel`** (1,081 visits/yr, run said keep, page 200) — revert candidate. Net traffic impact of the whole batch ≈200 visits/wk → NOT a driver of the −17%.
- **Verification recipe**: taxonomy IDs ≠ legacy URL ids — BFS `GET /api/Categories/{id}` per node (tree returns ONE level of subCategories per call; 3,575 fetches), map legacy id from the nl-NL urlSlug tail (`…_484303`), then sweep `CategoryFacetSettings` per category and filter `updatedAt`. ~3.5K+2.6K requests with 20-thread pool ≈ minutes.
- **Shop-grain dissolves "anomalies"**: Bedden (+18% visits/−74% rev) = ONE Emma-sleep.nl €1,103 SEO transaction in wk22 (shop fully active wk23, all channels); Sportshirts = Voetbalshop.nl WK spike (558→109 outclicks, €1,873→€218, partly offset by Voetbalshirtskoning €70→285 / bol.com Plaza €99→575); Shirts = ~€2 transactional base, pure noise. Note `revenue_excl` (shop omzet, transaction grain) ≠ visit-grain attributed revenue — patterns match, absolutes don't.
- WSL gotcha: **no `psql` binary anymore** — query 10.1.32.9 via `psycopg2` (system python3 and `~/.mysql-venv` both have it).
- Deliverables: `Downloads\claude\seo_week23_vs_week22_conclusion.txt`, `…_conclusions.pdf` (5-slide 16:9 reportlab deck; also made `indexed_plp_utm_tagging_story.pdf` from the 06-11 txt — render-check pages via `~/.mysql-venv` pypdfium2).

## dm-tools repo cleanup + kopteksten/faq audit phases 5–6 (2026-06-12/13, commits `5ad1e6d`, `2dac5b2`, `31f5e04`, `282448a`)
- **Cleanup** (`5ad1e6d` + ref-fix `2dac5b2`): root one-off scripts → `scripts/analysis/`, task-spec notes → `notes/` (query.txt path refs updated in seoPrio.txt/monthly_share.txt/query_gap_run.py), prototypes+CSVs → `scripts/prototypes/`, Google-Ads script.js+notes → `scripts/google_ads/`. **Load-bearing root files that must NOT move**: `themes.py` (lazy `from themes import …` inside `thema_ads_service.py` — resolves via repo-root cwd of the uvicorn process), `thema_ads_optimized` symlink, `scripts/swagger_taxv2.json` (referenced by the beslist-apis skill). Deleted: stale `logs/uvicorn.log` (May 13), root/scripts `__pycache__` (root one instantly regenerates — live process), `NOTE_FOR_CLAUDE.md` (its own instruction: delete after pull; rurl-history un-track confirmed). Gotcha: `git mv` stages the RENAME but later `sed` edits to moved files are unstaged — the first commit shipped without the path fixes; needed the follow-up commit. `git pull --rebase` refuses with unstaged changes → `git stash push` / `pop` around it.
- **Audit phase 5** (`31f5e04`, found uncommitted in worktree): faq_service URL helpers (clean_url/parse_beslist_url/build_api_params) single-sourced from scraper_service; `link_validator.replace_url_in_content` now matches hrefs on a NORMALIZED form (abs/rel host + trailing slash) — exact-equality silently replaced nothing while reporting `has_changes=True`. **Phase 6** (`282448a`, same pattern next session-start): `extract_selected_facets`+`build_product_subject` single-sourced; scraper_service absorbed the richer FAQ behavior (url_name in extracted facets, `type_productlijn` policy: brand-line slug keeps the category appended). Tests `backend/test_kopteksten_faq_audit.py` lock all of it (23 pass). Deploy note: phase-5 needed a manual uvicorn kill+relaunch (in-process import); did it after confirming **only `pending` rows** in `pa.{faq,kopteksten,unique_titles}_jobs` (no running jobs lost). New process now logs to root `uvicorn.log` (old one was attached to a terminal pts).

## dm-tools — Kopteksten upload-urls overhaul + category-suppression for model URLs (2026-06-12, commits `d1e1ffc`, `02253fd`, `45d5f64`; + DB-only changes)
User hit `relation "pa.jvs_seo_werkvoorraad_shopping_season" does not exist` adding URLs to Kopteksten. **Root cause: `/api/upload-urls` (`backend/main.py` ~L782, used by BOTH the file picker AND the manual paste box) was never migrated in the Big-Bang refactor** — it still inserted into the **legacy Redshift** werkvoorraad table via `get_output_connection()`, which only points at Redshift when `USE_REDSHIFT_OUTPUT=true`. The `.env` has it `false`, so the insert hit Postgres (no such table). The worker reads pending jobs from **Postgres** `pa.kopteksten_jobs ⋈ pa.urls` anyway.
- **Fix (`d1e1ffc`):** rewrote the endpoint to the post-Big-Bang schema — canonicalize + `bulk_upsert_urls` into shared `pa.urls`, then queue pending rows in `pa.kopteksten_jobs`, `pa.faq_jobs`, `pa.unique_titles_jobs` (`ON CONFLICT (url_id) DO NOTHING`, all 3 tables have a unique `url_id`). So adding URLs feeds kopteksten + FAQ + unique-titles at once; no more Redshift dependency. Counts via `INSERT ... RETURNING url_id` + `len(fetchall())` (psycopg2 `executemany` rowcount is unreliable for true insert counts).
- **UTF-16 decode-order bug (`02253fd`):** the endpoint's decode loop tried `utf-16` FIRST. An **even-length** ASCII/UTF-8 payload (a single pasted URL = 62 bytes) decodes as UTF-16 into silent CJK garbage — **no exception, no `�`** — so the loop accepted it; canonicalization then dropped it → `0 new, 0 duplicates`, nothing queued. (Files with a trailing newline are odd-length → utf-16 raises → falls through, which is why file uploads looked fine.) Fix: only lead with utf-16 when a BOM (`\xff\xfe`/`\xfe\xff`) proves it (Excel "Unicode text" exports carry one); else prefer utf-8-sig/utf-8. The unique-titles import endpoint (`main.py` ~L2976) already had the safe order.
- **Result breakdown (`45d5f64`):** `"X new, Y duplicates"` hid two buckets, so 39 URLs with 7 repeated lines reported `0 new, 32 duplicates (39 total)` with 7 unaccounted. Backend now returns `added`/`already_queued`/`repeated_in_input`/`invalid` (sum to `total_urls`); frontend renders all of it via a shared `formatUploadResult` helper; bumped `app.js?v=3` in `index.html` (cache-bust). `repeated_in_input` = valid lines that collapsed onto an earlier canonical (same URL pasted twice — `bulk_upsert_urls` dedups via a `seen` set).
- **Category suppression for `modelnaam_mob` URLs (DB-only, NO code change):** the data-driven `type_facet_override_recipe` — set `pa.facet_position_rules.is_type_facet=TRUE` for a slug so its value substitutes for the category noun and the category isn't appended. Flipped `modelnaam_mob` false→true (was imported `replace_category=0` from facet_order.xlsx 2026-05-19). Live builders consume it via `_type_facet_override_by_slug(slug)` in BOTH `_is_type_facet_for` defs (`ai_titles_service.py` L1589 v1 + L2451 v3) → `has_category_override` → `effective_category=''`. Cache TTL is 60s (`_FACET_POSITION_RULES_TTL_SEC`), so DB edits go live without restart. Result: `Samsung Galaxy S23` not `... Mobiele telefoons`.
- **DON'T flip every mobile facet — verify the actual value first.** `smart-of-classic` was ALREADY `is_type_facet=true` (since the 2026-05-19 import, `replace_category=1`). `mobiel_k~20092054` is **NOT** "Mobiele telefoons" — it's `facet_name="Kenmerken", value="Refurbished"`; "Mobiele telefoons" is the appended CATEGORY. So `"Refurbished Mobiele telefoons"` is a *correct* title and `mobiel_k` must stay a normal facet (flipping it would collapse the title to just "Refurbished"). **Resolve a facet value/category for a real URL with `fetch_products_api(url, include_related=False)`** → `category_name` + `selected_facets` (each has `url_name`, `facet_name`, `detail_value`). Don't reason from synthetic example facets — a made-up `mobiel_k="Smartphones"` invented a redundancy that doesn't exist in real data and sent us chasing the wrong facet.
- **Regenerate after a rule flip:** rules only affect NEW generations. Reset `pa.unique_titles_jobs.status='pending'` (clear `last_error`) for the affected URLs (`WHERE url LIKE '%modelnaam_mob%'`, 5,656 rows); `update_title_record` upserts `pa.unique_titles_content` (`ON CONFLICT DO UPDATE`) so no stale content and no need to delete. Title pipeline default is v3 (`AI_TITLES_PIPELINE`, `process_single_url`→`generate_title_v3`).
- **Auto-start = VBS in the Windows Startup folder** (`.../Start Menu/Programs/Startup/start-dm-tools.vbs`): `wscript`→`wsl -d Ubuntu -- bash -c "cd dm-tools && source venv/bin/activate && uvicorn ... :8003"` (no `--reload`, serves from **dm-tools**). To (re)start the persistent instance from WSL without a reboot: `cscript //Nologo "<vbs path>"` — it spawns a process parented by WSL `/init`, fully detached from the Claude session. **Detached `setsid/nohup ... &` from the Bash tool gets killed on tool-exit**, and harness `run_in_background` dies with the session — the VBS is the only thing that survives.

## R-URL optimizer — full code audit + Phase 0–3 cleanup (2026-06-12, commits `2f617dd`, `0da66ff`, `f81aed9`)
Audited the whole optimizer (~10K lines) and worked a prioritized fix plan. **The dominant meta-lesson: speculative "correctness" refactors of the matching/scoring CORE net-negative on real data.** The audit's theoretical bugs (cross_cat short-circuit ordering, etc.) **don't fire** on real R-URLs, while any change to the bucketing/scoring silently perturbs the ~30 accreted guards (V12–V35, Fix A–E) — each of which was itself tuned to a real failing URL. **Drive matching-core fixes from a real wrong redirect, NOT from an audit list** (that's how Fix A–E came about). Safe, no-downside work = cleanup, output-correctness, isolated stemming, zero-diff dedup.

- **New `/audit` slash command** (`~/.claude/commands/audit.md`): survey target → fan out parallel review agents (general-purpose) per slice with a strict rubric (file:line, severity, no functionality loss, verify-before-report) → spot-verify the top HIGH findings myself (agents misread — e.g. one flagged a `_norm_dim` regex that was actually fine) → synthesize a phased plan separating behavior-preserving cleanup from regression-gated changes → stop at the plan. Reusable for any function/module.
- **Phase 0 (`2f617dd`, behavior-preserving):** deleted dead code (`REVERSE_SYNONYMS`/`get_search_terms_for_facet`/`expand_keyword` — zero call-sites; grep the tree before deleting), fixed unreachable `chunksize` branch (`>100000` was `elif` after `>10000`), dropped dead `mask` in `get_type_facets_only`, no-op `if/else`, redundant 2nd `HIGH_SUBCAT_THRESHOLD`, orphaned docstring; precompiled `BAD_CROSS_CATEGORY_PATTERNS`; `facet_filter` `sorted()[0]`→`min()`, `import re` hoist, bare `except:`→`except Exception`.
- **Phase 1 (`2f617dd`, output-correctness):** the final return dict emitted `r.facet_*` (the CASCADE result) while `redirect_url=final_redirect_url` (post-override) — so Fix D / V28-rescue / guard rows reported facets of the DISCARDED match. Now: when `final_redirect_url != r.redirect_url`, derive `facet_*` from the URL the row actually points at (bare-category overrides → 0 facets); non-overridden rows byte-identical. Display value-names can't be reconstructed from a URL so they blank on overridden rows.
- **Phase 2a (`0da66ff`, regression-gated):** `s.rstrip('s').rstrip('en')` strips the CHAR SET {e,n} → over-strips ('tuinen'→'tui'); replaced with suffix-aware `_strip_plural_suffix` (`re.sub r'(?:en|s)$'`). Grouped the toilet/fontein bad-pattern alternation; apply only the single WORST bad-pattern penalty (was summing). Regression: 2 changes (1 improvement, 1 neutral tier-D), 0 production losses.
- **Phase 2b — REVERTED:** cross_cat short-circuit reorder (`max(score)` + V35-style ≥2-axis gate). Over 6,808 URLs: **0 gains, 1 regression** (dropped a valid `m_verlichting~Onderbouw` facet, tier B→D, via a subtle V35-bucketing interaction on a child-subcat+existing-facet row). The theoretical bug never manifested → reverted.
- **Phase 2c — DEFERRED:** changing `search_derived` AND-vs-fallback classification would threaten the validated **Fix D/E** `mode=='and'` signal; changing `subcategory_id` on relocation alters the **load-bearing V31 guard** (`r.subcategory_id == parsed.subcategory_id`, which build_multi_facet sets to ORIGIN for all results — that's how V31 detects "own-subcat match"; `subcategory_id` is NOT an output column). High regression risk, no demonstrated benefit.
- **Phase 3 low-risk (`f81aed9`, zero-diff):** folded cascade steps 2b+3 (near-identical >=95 subcat-name pipelines, differing only in match context + reason tag) into `_high_subcat_name_match`. **BYTE-IDENTICAL** over 6,808 URLs incl. the `reason` column. Step 5 left inline (different threshold-gate shape). Skipped the output-row factory (touches the fragile 7-exit main return for cosmetic gain) and the `process_global_rurls.py` tail-share (the tails have DRIFTED — sharing them = behavior-changing A2, not zero-diff).

**Verified bugs catalogued for reactive fixing (NOT yet fixed — fix when a real URL hits them):** `build_multi_facet` cross_cat short-circuit picks first-in-order not max-score and pre-empts richer same-subcat builds (url_builder ~L434); `subcategory_id`=origin on relocation paths (only matters if V31 is reworked); `filter_by_subcategory` uses bare `str.contains(id)` → over-matches sibling ids (504063 in 5040631); `process_global_rurls.py` is a drifted fork missing Fix A–E/V28/V35 (maintainability time-bomb — needs A2 parity with its own regression on global inputs).

**Process:** zero-diff/regression harness = every-2nd `/r/` URL from Postgres `public.rurl_processed` (~6,808), 8-worker `Pool(init_worker_v2)` over `/tmp/rc.pkl`, NEW=working tree vs OLD=`git stash push -- <files>` then pop; for Fix E prefetch the cross-maincat `(target_maincat, keyword)` pairs first. Harness is strong (caught the 2b regression that syntax+smoke missed) but NOT exhaustive (samples every-2nd URL) — for a pure refactor demand a **byte-zero** diff, not "small and explainable".

## R-URL optimizer — Fixes A–E: subcat-name / synonym credit + search-verified cross-cat (2026-06-11, commits `3139e91`, `5593816`, `78a5705`)
User reported 5 more weird redirects. **Unifying root cause: the engine matches keyword tokens against facet VALUES only, and never credits a token that is represented by the SUBCATEGORY NAME** — so a product-noun token (`rolgordijn`, `tafel`, `driewielers`) that *names the category* is treated as "unmatched," which both (a) zeroes the V27 reliability score and (b) lets a stray secondary-token match win. Fixes all in `backend/rurl_optimizer_v2/`.

- **Fix A — V27 credits the destination subcategory name** (`main_parallel_v2.py`, the matched/unmatched loop ~L1624): add `redirect_cat_name` (whole + its ≥3-char words) to `facet_values_lower` so a token naming the landing subcat counts as matched. `bamboe_rolgordijn_buiten` → `materiaal~Bamboe~~ruimte~Buiten` in "Rolgordijnen" was **score 0 / tier D** (V27 "long unmatched token: rolgordijn") → now **tier A**. The redirect URL was always correct; only the score was wrong.
- **Fix B — probe-facet fallback** (before `build_category_only`): when the lexical cascade finds NO facet match, promote a high-coverage (`coverage≥0.6`, `value_count≥15`) Search-API facet probe (`derive_facet`), resolved to its shallowest same-maincat subcat via `_resolve_probe_facet_url`. **Skips `merk`/`winkel`** (else `airfryer`→merk~<brand>). Gate is `not result.success` (a `cross_maincat_blocked` result is a truthy RedirectResult with `success=False` — must test `.success`, not truthiness). `inklapbare_tafel` (Puzzels) → Speeltafels `o_speeltafels~Inklapbaar`. New `match_type='facet_probe_fallback'` added to `TRUSTED_MATCH_TYPES`.
- **synonym credit in scoring + new synonyms** (`src/synonyms.py`, scoring loop): the matched/unmatched loop now also adds `get_synonyms(word)` to the forms tested, so a synonym-bridged token scores as matched (e.g. `combimagnetron`→`Magnetronfunctie`, which is not a substring). New synonyms: levensfase audience words (`volwassen(en)`→`volwassenenspeelgoed`, `dreumes`→`dreumesspeelgoed`, `kleuter(s)`→`kleuterspeelgoed`) and `combimagnetron`→`magnetron`/`magnetronfunctie`. The levensfase synonym lets `_append_facet_to_subcat_redirect` attach `levensfase~Volwassenenspeelgoed` for "volwassen".
- **Fix D — same-maincat search-derived override** (late, before maincat validator): when the Search API shows a strong dominant category in the R-URL's OWN maincat (`dom_cat_share≥0.6`, mode and/fallback) but the matcher only made a weak stray cross-subcat match (`≤1` matched token, redirect in neither the R-URL's subcat nor the dom cat), redirect to the dom-cat page. `bestuurbare_auto_100_km_h` (Puzzels stray `th_puzzels~"Auto"`) → **RC auto's** (69% dominant). Same-maincat only.
- **Fix E — cross-MAINCAT redirect, VERIFIED by AND-mode product evidence** (the breakthrough): `driewielers_volwassen` (fietsen) → speelgoed **Driewielers** + `levensfase~Volwassenenspeelgoed`. When the keyword's HEAD noun exactly names a subcat in another maincat AND the cascade didn't reach that domain, fire ONE cache-only verification probe `derive_redirect(target_maincat, keyword)` and **only jump if `mode=='and'` and `dom_cat_share≥0.6` and the dom slug is within the named subcat's tree**. Prefetch the `(target_maincat, keyword)` pairs in `main()` (new pass) so workers stay cache-only.

**THE key signal (Fix E): Search API `mode=AND` vs `fallback`.** A genuine full-keyword match returns `mode=and` with a small real `total` (driewielers in speelgoed → 62 products, Driewielers, share 1.0). A coincidental name match returns `mode=fallback` over millions of OR hits with a noise dom_cat (`zink soldeerbout` in gezond_mooi → fallback, "Zink" supplements 0.88; `olie opvangbak` → fallback, "Visolie"). The name match `Zink`/`Visolie` looks identical to `Driewielers` — the AND/fallback flag is what tells them apart. **A bare cross-maincat subcategory-NAME match is NOT trustworthy evidence on its own.**

**Gotchas / process:**
- **Cross-maincat name-jumping is NOT safely generalisable from a name match — reverted TWICE.** First as a broad override (hijacked ~88 good results: `keuken_onderbouw_verlichting`→tuin Verlichting, `kast_hal_jassen`→mode Jassen, `nagel_stickers`→kantoor Stickers). Re-gated to head-token-only + fallback-only (per user idea "only when other options are crappy") — STILL ~25 wrong (`zink soldeerbout`→Zink supplements, `olie opvangbak`→Olie, `pindakaas_houder_voor_vogels`→food), because a head noun routinely names subcats in several maincats while the R-URL's own maincat is the right domain. Only the **product-evidence (AND-mode) verification** (Fix E) made it safe → 4 jumps, all correct.
- **Tier thresholds** (`get_reliability_tier`): A≥90, B≥75, C≥50, D<50. A cross-subcategory narrowing (parent→child, e.g. `combimagnetron`: Koken→Ovens & fornuizen) carries a penalty and lands ~48 → tier D even with 100% coverage; that's the honest score, didn't tune it.
- **`_resolve_probe_facet_url` non-determinism** (Fix B): broke shallowest-subcat depth ties by `frozenset` iteration order (varies by `PYTHONHASHSEED`), so the same probe resolved to different same-depth subcats across runs (surfaced as 11 phantom diffs in the Fix-E regression). Fixed: break ties on the URL tuple `(depth, u)`.
- **Regression method**: sample = every-2nd `/r/` URL from Postgres `public.rurl_processed` (13,615 → 6,808). Build `/tmp/rc.pkl` via `save_data_cache`, run an 8-worker `Pool(init_worker_v2)` harness writing CSV (url, redirect, match_type, tier, success); NEW = working tree, OLD = `git stash push -- main_parallel_v2.py` then pop. For Fix E the harness must **prefetch the cross-maincat `(target_maincat, keyword)` pairs first** (workers are cache-only; uncached → Fix E silently no-ops). Final batches: Fix A+B+syn → 322 tier corrections all upward, 0 lost, 0 cross-maincat; Fix D → 8 changes all same-maincat; Fix E → 4 cross-maincat all AND-verified, 0 wrong, 0 lost.
- **Diagnosing a single URL**: build `_worker_data` in-process (mirror `init_worker_v2`), `prefetch_pairs([(maincat,kw)])` + `prefetch_facet_probes(...)`, then `process_url_v2((url, True))` with stdout captured. `derive_redirect`/`derive_facet` are **cache-only** (never hit the API in-worker); the prefetch (sequential, throttled, `SEARCH_QPS`) is the only live-call path. The categoryless Search API query returns **400** — `category` is required, so global cross-maincat dominance isn't directly queryable (must probe a candidate maincat).

## DMA+ shop exclusion — "No parent for CL3 found" on item-id BE PLA trees (2026-06-11, commit `f6a3b5c`)
User got `PLA/Videokaarten_a: No parent for CL3 found` excluding Joybuy.nl. **Root cause: the BE Videokaarten ad groups have NO custom-label-3 (shop) level** — they subdivide `INDEX1 → INDEX0 → product_item_id`, and the only biddable leaf is an **unlabeled item-id "Everything else" catch-all** (`case_value = product_item_id{}`, empty value, ByteSize 2). `add_shop_exclusions_batch`'s rebuild only recognised a *labeled* positive UNIT to convert into a CL3 subdivision, so it bailed. NOT systemic — BE has 10,084 CL3 nodes elsewhere; Videokaarten is the exception. (NL Videokaarten worked because those trees DO have a CL3 level.)
- **Fix** (`backend/campaign_processor.py`): find EVERY positive leaf UNIT (labeled or not), **copy each leaf's `case_value` verbatim via `client.copy_from`** onto the new SUBDIVISION (preserves any dimension — item_id "Other" marker, brand, product_type, custom label — so the rebuilt node stays a valid sibling), `ByteSize()==0` → pass `None`. Also **SELECT `product_type`/`product_brand`/`product_item_id`** in the read query (not just custom_attribute) so the verbatim copy doesn't drop the leaf's real dimension. Preserve the leaf bid on the new CL3 catch-all; whole rebuild stays one atomic mutate.
- **Validation**: `mutate_ad_group_criteria(request=MutateAdGroupCriteriaRequest(..., validate_only=True))` lets Google check the transform WITHOUT writing (the kwarg form `validate_only=True` is rejected — must use a request object). Validated all 3 BE ad groups, then applied for real. joybuy.nl now an ENABLED negative CL3 node in a/b/c.
- **Gotchas**: laiza creds are **read-only** (service-account `...read-only.json`) — can't mutate; the write path is DMA+/`dma_plus_service._get_client()` (OAuth from dm-tools `.env`: client_id/secret/refresh_token). proto-plus `WhichOneof("dimension")`/`ByteSize()` return empty unless the field is SELECTed in the GAQL query. DMA+ exclude UI → `process_exclusion_sheet_v2` → `add_shop_exclusions_batch`; error string is `f"{ag_name}: {error}"`.

## R-URL optimizer — category-match misfire batch (2026-06-11, commits `bb483f8` + `aba5067`)
User reported a series of redirects landing in the wrong category. Root cause in every case: a single **stray/weak keyword token cross-matched a facet (usually a `type_*` value) in an unrelated category** and pre-empted the correct in-category match. Fixes in `backend/rurl_optimizer_v2/` (`main_parallel_v2.py`, `src/synonyms.py`, `src/validation_rules.py`):

- **`dekbed_zonder_hoes`** → was `type_opberger~"Opberghoes"` (Opbergzakken); "hoes" substring-hit "Opberg**hoes**". Fix: synonym `zonder hoes`/`hoesloos`/`hoesloze` → `"zonder overtrek"` (eigenschap_beddengoed~23812125). Now `synonym` match in own subcat.
- **`hoesloze_dekbedden`** (maincat-only) → same synonym; resolves via search-derived rescue to Dekbedden + the facet.
- **`afdekplaat_inductiekookplaat`** → was rejected (no redirect). Synonym `afdekplaat inductiekookplaat` → `"inductie beschermer"` (type_kh~23814360, Keukenhulpjes). "afdekplaat" shares no letters with "beschermer", so only a synonym bridges it.
- **`antislipmat_bad-douche`** → was sibling `t_zeepd~"Douche"` (Zeepdispensers); "douche" is the OWN category noun (Douchematten). Fix: COMPOUND_DECOMPOSITIONS `antislipmat`→`antislip` (glued compound the matcher scored 0 on; `antislip` alone scores 100 vs o_matten~"Antislip") **+** a new **step 1c own-subcat compound retry** that runs BEFORE the parent/sibling fallback (step 2), so the own-subcat decomposed match wins.
- **`rolgordijn_zonder_boren`** → was `type_kh~"Appelboren"` (Keukenhulpjes); "boren" substring-hit "Appel**boren**". The child subcat **Rolgordijnen** (557622_557624) scored 99 on "rolgordijn" but step 1's cross-category type match set a result first. Fix: **defer a purely-cross-category step-1 result** (all hits have `cross_category_path`) so steps 2b/3 (subcategory-NAME) win — **but only when `_has_strong_subcat_name_match()` confirms a ≥95 own-subtree/maincat subcat-name match exists**; otherwise keep the cross-cat match. Deferred result restored as last-resort fallback before `build_category_only`.
- **Guards/extras**: (a) `cross_type_rejected_kept_origin` guard — single low-coverage token + search-derived dom_cat == URL's own subcat → keep origin (preserve existing `/c/`); (b) **synonym-aware long-unmatched guard** — the V28 search-derived rescue's `_rescue_long_unmatched_token` was rejecting correct synonym matches because the descriptive token ("afdekplaat"/"hoesloze") isn't literally in the facet name; now feed `local_match.keyword` (the synonym source phrase) into the represented-text when `local_match.match_type=='synonym'`; (c) **V27 stopwords-only short-circuit now preserves an existing `/c/` facet** (was dropping it — surfaced by adding `getest`); (d) **`api.scrape.do` URLs dropped at input** (`main()`), not just the in-worker guard; (e) stopword `getest` added (covers "als beste getest"; deliberately NOT bare `10` — would strip "iphone 10"/"maat 10"/"10 kg").

**Gotchas / process:**
- **Deferring ALL purely-cross-category step-1 matches is too broad** — first attempt regressed `toilet_fontein_met_kastje` (cross-cat `t_wastafel`, correct for a fontein) → nonsense `ruimte_verwarmingen`, and `onkruid…elektrisch` → `type_grasmaaier`. Gating the defer on a strong subcat-NAME match removed all collateral.
- **Regression method**: aggregated 856 unique `/r/` URLs from `data/rurl-optimizer-v2-input/*.csv`, ran OLD (via `git stash`) vs NEW writing JSON dicts, diffed. Final: **0 redirects lost**; intended fixes + bonus (3 rows that previously dropped a valid existing facet on stopwords-only keywords now keep it; `douchestang_zonder_boren`→Douchestangen, `ziki_boxershorts`→Boxershorts gained). The optimizer venv is `dm-tools/venv` (has `fuzzywuzzy`); `python` isn't on PATH — use `/usr/bin/python3` or the venv. No linter → `py_compile` gate.
- **Note**: another session landed V35 work (`892565e`, plus uncommitted `_resolve_probe_facet_url`/Fix B/C, levensfase+combimagnetron synonyms) on the same files — left untouched.

## Redshift channel derivation: `utm_source=dma` ≠ paid; indexed PLA URLs (2026-06-11)
User asked why ~15K rows with `utm_source=dma` in the URL show `marketing_channel='SEO'`. **`marketing_channel` is a pure function of `(aff_id, channel_id)` via `chan_deriv.ref_channel_derivation_stats` — it never reads the URL/utm.** The two are independent and not reconciled.
- **Current mappings** (`deleted_ind=0`): **DMA paid** = aff_id 906 (NL)/907 (BE)/909 (DE), channel_id 1, traffic_type Paid; **SEO** = aff_id **0** (no paid affiliate), channel_id 4, traffic_type Free.
- Of all `utm_source=dma` real visits (2024-01→2026-05): 33.5M DMA-paid vs **972K SEO** (the user's "~15K" is the *grouped* row count; true distinct-URL count ≈ **650K** — their BI client capped the result set). The SEO bucket is **99.9% on `/p/` product URLs**, ~€0.03/visit.
- **What they are**: paid Google Ads PLA landing URLs (`/p/…/nl-nl-gold-<ean>/?utm_source=dma&gbraid=…`) that Google **indexed and now serves organically** — no live paid click, so `aff_id=0`. The derivation is *correct*; the utm tag is just stale text in the URL string.
- **To identify true paid DMA, filter `marketing_channel='DMA paid'`** (or aff 906/907/909, channel_id 1), NOT `url LIKE '%utm_source=dma%'`. Always add `chan.deleted_ind=0` to the derivation join (`deleted_ind=0` = current row; `=1` = superseded — these tables are soft-delete/versioned; without it stale/dup mappings can fan out the join).
- **SEO canonicalization angle**: live check (one read before AWS-WAF throttled me to 202/405) showed the clean gold-EAN URL **301s → slug URL** with a **clean param-free `rel=canonical` (server-rendered react-helmet) + `index,follow`**. So canonical IS correct — but param URLs are indexed anyway because **`rel=canonical` is a hint Google can override**, helped by the content-bearing-looking `productId=` param and these being real ad-destination URLs at scale. Misconceptions corrected: Google does NOT auto-strip `utm`; `&` is parsed fine; `#fragment` is ignored. Recommended fix: **301 the tagged URLs to the clean slug with tracking params stripped**, keep tagged URLs out of sitemaps/internal links, confirm verified Googlebot is WAF-allowlisted; verify Google-chosen canonical via **GSC URL Inspection**. (Findings also in memory `redshift_channel_derivation.md`.)

## Bothits AI-bot log extraction — full run completed + merge step (2026-06-10)
Ran the full CloudFront AI-bot pipeline (runbook: `cc1/BOTHITS_PROCESS.md`) end-to-end over
7 date-folders. Filtered **64.55M** bot rows from **229,288 .gz** → 11,062 unique IPs;
verified **2,978** IPs (8,021 failed, 63 unverifiable) against official ranges + rDNS;
final **64,401,787** kept. Output in `Downloads\claude\bothits_new\`.
- **Resume gotcha**: `bothits_filter.py` writes `_ip_inventory.json` only after ALL folders
  (accumulated in-process). A crash leaves per-folder `.csv`/`.gz` but no inventory, so a
  partial re-run (`[folders...]` arg) would yield an inventory missing the done folders and
  break verification. **Resume = re-run all 7 from scratch** — it's idempotent.
- **Durability pattern that worked**: chained the 3 steps in `bothits_chain.sh` (waits on
  `pgrep -f bothits_filter.py`, guards on the `TOTAL` line, then verify → finalize), launched
  via `nohup`. Survives reboot; progress in `bothits_stage/_chain.log`. Polled with
  ScheduleWakeup between turns rather than blocking.
- **New step 4**: `bothits_merge.py` concatenates all `<out>/*.gz` into one `all-dates.gz`
  (~1.71 GB, 64.4M rows), keeping the `#Version`/`#Fields` header from the first file only.
  Single-threaded gzip ≈ 5–8 min — background it.
- Never stage long runs to `/tmp` (WSL wiped it mid-run once); staging lives on `/`.

## Top-N facet combination blueprints per category (2026-06-09)
Extension of the blueprint work below: `scripts/pagetitles_topn_combinations.py` (param N, default 5) ranks each category's facets by **summed SEO visits** (from the Redshift traffic cache `/tmp/seo_traffic_rows.pkl` — a facet's score = sum of visits of all URLs in that category that use it), takes the **top N**, and emits the blueprint for **every non-empty subset** of those N facets (power set = 2^N−1 per category, reusing `bp.build_row`/`facet_phrase`). Writes the complete set to a `top{N}_combinations` sheet and appends only net-new `(cat_id, canon_key)` combos to `all_combined` (source=`top{N}_combinations`).
- **Excel's hard per-sheet limit is 1,048,576 rows.** Across 3,486 SEO-trafficked categories: top-5 = 80,390; top-7 = 240,710; **top-8 = 405,318 (fits)**; top-10 = **1,114,950 (OVER the limit — won't fit one sheet)**. Always size the power set before generating: top-N is 33× bigger going 5→10.
- Top-K facets ⊆ top-(K+M) facets, so a smaller run's combos are a subset of a larger run's — appending top-8 after top-5 only adds the delta (top-5 added 46,817 to all_combined; top-8 then added 337,676 net-new). all_combined ended at 539,215 (154,722 base + 46,817 top5 + 337,676 top8), under the limit.
- **openpyxl round-trips silently flatten PivotTables/charts to static values** (and the user's `dt_all_combined` pivot over all_combined didn't survive interactive after my saves). Saving a ~540k+405k+80k-row workbook with openpyxl takes ~1–2 min — run it backgrounded.

## tblPageTitles blueprints straight from faceted URLs (2026-06-08)
New deliverable: clean, deterministic `tblPageTitles` title/h1/description blueprints built directly from the faceted `/c/` URL structure, instead of reverse-templatizing rendered copy (the older `scripts/pagetitles_from_unique.py` approach). Two new scripts, both pushed to dm-dashboard.

- **`scripts/pagetitles_blueprint_from_urls.py`** — for each faceted URL: leaf slug → cat_id (`/tmp/slug2id.json`), facet types → ordered placeholders. **Facet order comes from `pa.facet_position_rules.order_index`** (asc; merk=3, kleur=22, maat=2300). The NOUN is a type-facet (`is_type_facet=true`) when the set has one; otherwise **`!!sub_category!!` is inserted at the canonical type-facet slot — effective `order_index` 1700** (after brand/colour, before size). Every blueprint therefore contains a category or type-facet.
- **Fixed templates**: title = `!!current_query!! <phrase> kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl`; h1_title = `<phrase>`; description = `Zoek je <phrase>? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop <phrase> met !!DISCOUNT!! korting online! &#10062; beslist.nl`. `<phrase>` is the same ordered placeholder string in all three (no `!!current_query!!` in h1/desc).
- **Normalization**: each URL is **lowercased before parsing** (case-insensitive `slug2id_l`, keys canonicalized lowercase-sorted via `canon_key`); facet types `unquote()`-decoded so `%28`→`(`/`%20`→space match their `facet_slug`; `pricemin`/`pricemax` dropped (price-slider params, not facets) via `IGNORE_FACETS`.
- **Dedup/skip**: one row per distinct `(cat_id, canon_key)`, key = `'~'.join(sorted(types))`; skip-set = prior `tblPageTitles_new_from_unique.xlsx` combos (101,300) **∪ live MySQL `beslist.tblPageTitles` NL combos (142,076)**. Of 195,538 distinct combos in `pa.urls`, only **1,628** are genuinely new. **Why so few**: the old xlsx was itself built as "gaps not already in tblPageTitles", so the ~95k it didn't cover are mostly already live in the table — deduping against the table removes them. Excluding the stale 96k base output ≡ excluding `tblPageTitles ∪ _v2` (its extra combos are all already in the table), so no genuinely-new combo is wrongly dropped.
- **`scripts/pagetitles_blueprint_from_seo_traffic.py`** (companion, imports the first as a module) — reads the Redshift SEO-traffic faceted URLs from `query.txt` (datamart.fct_visits+dim_visit+chan_deriv, SEO channel, `/c/` urls, Jan-2025→Jun-2026; 671,318 rows, cached `/tmp/seo_traffic_rows.pkl`). Aggregates visits+revenue per `(cat_id, key)`, keeps only combos absent from tblPageTitles ∪ xlsx ∪ the generated blueprints → **10,932 trafficked combos with no blueprint** (€6,823 total revenue). **33% of rows / 84% of that revenue are `winkel` (shop) facet combos** — user chose to KEEP them.
- **Gotchas**: `pymysql` lives only in `~/.mysql-venv` — run both scripts under `~/.mysql-venv/bin/python` (it also has psycopg2+openpyxl). openpyxl can't overwrite an xlsx open in Excel → `PermissionError` fallback writes `_v2`/`_v3`; once the file is closed, `mv` the latest version onto the base name to consolidate. Redshift = plain `psycopg2` on **port 5439**, creds in `dm-tools/.env` (`REDSHIFT_HOST/PORT/DB/USER/PASSWORD`), helper `get_redshift_connection()` in `backend/database.py`. The big Redshift query took ~6.5 min.
- **Final deliverable** `Downloads\claude\tblPageTitles_blueprint_from_urls.xlsx`: sheet `new_pagetitles` (1,628), `seo_traffic_new` (10,932 + visits/revenue, revenue-sorted), and `all_combined` (**154,722** = the two new sets + all 142,162 existing tblPageTitles NL rows, with a `source` column = tblPageTitles / new_from_urls / seo_traffic). Verified 0 overlap between the created sheets and tblPageTitles.

## bt.search_console data quality + core-update analysis (2026-06-10)
Built a Search Console period comparison (May vs June 2026, `country='nld'`) and then
stress-tested whether the table's daily counts can be trusted. Deliverable +
working script:
- **Excel:** `/mnt/c/Users/JoepvanSchagen/Downloads/claude/search_console_may_vs_june_2026_nld_v2.xlsx`
  (6 sheets: Info & methodology, By URL-type, By keyword length, By maincat,
  Seasonal (excluded), Included deepest cats). `_v2` because the non-v2 file was
  locked open in Excel at save time — `wb.save` raises `PermissionError`; the
  script now falls back to a `_v2.xlsx` name.
- **Script:** `/home/joepvanschagen/sc_compare.py` (standalone; loads the
  beslist-query skill `.env`, psycopg2 + openpyxl). Re-runnable.

**Comparison setup (decisions baked in):** P1=May {11,13,14,15,16}, P2=June
{1,3,4,5,6} — 5 clean days each. Δ = June−May. CTR = SUM(clicks)/SUM(impr).
Avg ranking = impression-weighted `avg_position` (valid here because all 10 days
are clean). Weather-seasonal deepest categories excluded BOTH directions
(May-peakers like Tuinstoelen −52% AND June-risers like Plafondventilators +111%,
Koelboxen +87%, Parasols +65%) — 117 deepest cats removed, identified data-driven
by name regex on `deepest_category_name` (false positives scrubbed: Oogschaduws,
Zijwindschermen, Kattenhangmatten, Tuinbroeken; kept generic Sport & outdoor bucket).

**KEY DATA-QUALITY FINDINGS (the important part):**
- **ALWAYS filter `deleted_ind=0`.** The table holds duplicate/superseded snapshots
  with `deleted_ind=1` that carry ~0 clicks but real impressions. Not filtering
  inflates impressions and tanks CTR. (My first exploratory query omitted the
  filter and falsely flagged June 7 as corrupted — it is NOT.)
- **June 7 is clean** under `deleted_ind=0` (3.30M impr / 34k clicks / pos 7.11).
  So the original "6 days each" plan (June 1,3,4,5,6,7) is actually viable; the
  shipped file conservatively used 5. Regen-to-6 offered, not yet done.
- **June 2 is a genuine impressions glitch** even in clean data: ~1.29M impr
  (~40% of normal ~3.0M) while clicks (31,077) are perfectly normal. Independent
  failure — impressions pipeline broke without touching clicks.
- **Fresh-data backfill lag:** the most recent ~2–3 days are incomplete and get
  revised upward later (June 7's impressions doubled between two queries minutes
  apart in-session as it backfilled; June 8/9 still low). Drop the last 2–3 days
  of any pull.

**TRUST VERDICT:**
- **Clicks = trustworthy.** Proven by June 2: impressions broke, clicks stayed
  normal (31,077, between June 1's 31,981 and June 3's 31,281). Only caveat: last
  2–3 days under-counted until backfill.
- **avg_position = trustworthy per-row, but aggregation method matters.** On bad-
  impression days (June 2, 8) the **impression-weighted** AND **simple-average**
  position collapse (~7.5 → ~4.5) because the missing rows are the high-impression
  long-tail. The **click-weighted** position stays rock-stable (3.96–4.29 all
  window). → For any analysis touching suspect days, use **click-weighted**
  `avg_position` (SUM(avg_position*clicks)/SUM(clicks)), never impression-weighted
  or simple AVG. (On clean comparison days impression-weighting is fine.)

**CORE-UPDATE ANALYSIS PLAN (agreed approach, not yet built):**
1. Pin windows to the OFFICIAL Google core-update rollout dates (need from user);
   compare pre-rollout baseline vs post-completion, EXCLUDE the rollout days.
2. Daily time series of clicks + click-weighted position to locate the step-change
   and confirm it aligns with rollout (aggregate click-wt pos was ~flat 4.1–4.3 in
   May–June → impact is in redistribution, not the mean).
3. Paired url/keyword analysis (same url present in both windows → position delta
   distribution; how many moved up >2 vs down >2) — NOT aggregate means.
4. Segment winners/losers by `type_url`, `keyword_length`, and intent flags
   (`is_transactional_*`, `is_commercial_*`, `is_informational`).
5. Trust rules: `deleted_ind=0`; drop June 2 + trailing 2–3 days; clicks &
   click-weighted position primary; impressions directional-only; equal weekday
   mixes (whole weeks) to avoid weekday composition masquerading as ranking shift.

OPEN ITEMS: (a) ~~get official core-update rollout dates~~ DONE — May 2026 core
update ran **May 21 → June 2, 2026** (Search Engine Land / SE Roundtable);
(b) regen current may-vs-june deliverable to 6 days — still open, superseded in
practice by the new core-update workbook below; (c) ~~build the time-series +
paired winners/losers workbook~~ DONE — see next section.

## May 2026 core-update analysis RESULTS (2026-06-10)
Built `/home/joepvanschagen/core_update_analysis.py` → deliverable
`Downloads/claude/core_update_may2026_analysis_nld.xlsx` (6 sheets: Info,
Daily time series, Pre vs Post segments, Paired ranking shift, Top winners,
Top losers). Re-runnable; standalone (loads beslist-query `.env`).

**Window design (improved on the may-vs-june file):** pinned to the official
rollout (May 21–June 2). PRE = **May 13–17**, POST = **June 3–7** — both Wed–Sun
(identical weekday mix, removes weekday-composition bias), both clean, rollout
days excluded. Beats the old `{11,13,14,15,16}` vs `{1,3,4,5,6}` which mixed
weekdays and included June 1 (mid-rollout).
- **The late-May traffic surge is seasonal, not algorithmic.** Daily series shows
  May 22–26 impr 3.7M→5.1M / clicks→46k, then back to baseline. May 25 = Tweede
  Pinksterdag (NL holiday). It sits *inside* the excluded rollout window, so it
  doesn't contaminate pre/post — but don't mistake it for core-update lift.
- **June 8 has the SAME impressions glitch as June 2** (1.30M impr vs ~3M, rows
  ~460k vs ~580k, clicks normal, impr-wt pos collapses to 4.8). June 9 incomplete
  (17.9k clicks). Both excluded. June 7 confirmed clean (3.30M/34k). So a fresh
  pull today (June 10) has clean days only through June 7.

**HEADLINE — impact is REDISTRIBUTION, not an aggregate ranking move.** Across
409,355 paired url+keyword units (present in BOTH windows):
- click-weighted position 4.430 → 4.450 (**+0.020, flat**) — the mean barely moved.
- BUT 157,599 pairs improved ≥0.5 vs 137,970 declined ≥0.5, and 72,317 moved
  **up >2** while 62,830 moved **down >2**. Lots of churn under a flat mean.
- Paired clicks fell −12.8% (82,465→71,939) even though more pairs improved than
  declined — established url+keyword pairs lost click share (likely to newly-
  ranking June pages not in the paired set). **Only the up-by->2 bucket GAINED
  clicks (+2,990); every other bucket lost** → redistribution toward the big winners.

**By URL-type (paired):** R-url (241k pairs) took the hit — clicks −15%, position
slightly worse (4.23→4.28). C-url (category, 152k) ranking *improved* 4.60→4.56,
clicks −8%. PLP improved notably (9.05→8.51). Browse-url small but +32% clicks.
→ category/PLP pages held or gained rank; product/R-url pages bore the loss.

**Reusable method notes:**
- `top` is a **reserved keyword in Redshift** (`SELECT TOP n`) — can't name a CTE
  `top` (syntax error "at or near top"). Renamed to `ranked`.
- Per-pair ranking uses **impression-weighted** avg_position (valid because both
  windows are all-clean days); the daily-series **click-weighted** pos is what
  stays stable through the glitch days and confirms June 2/8 are impressions-only.
- Heavy part is the paired inner-join over ~400k url+kw groups (full run ~5–6 min);
  Python stdout to a redirected file is block-buffered — use `python3 -u` to watch.

## May 2026 core-update — IMPRESSION-FREE re-analysis + independent cross-check (2026-06-10)
User didn't fully trust the impressions column, so rebuilt the analysis using it
NOWHERE. Script `/home/joepvanschagen/core_update_analysis_v2.py` → deliverable
`Downloads/claude/core_update_may2026_impression_free_nld.xlsx` (6 sheets: Info,
Daily time series, Paired ranking shift incl. rank-bucket transition matrix,
Top winners, Top losers, SEO visits cross-check). Same windows (May 13–17 vs
June 3–7).

**Trust split that makes impressions unnecessary:** clicks = trusted (proven by
June 2/8); avg_position = trusted *per-row* (Google-supplied) — the only leak in
v1 was *weighting position by impressions*. v2 fixes it:
- Per-pair position = **UNWEIGHTED mean of daily avg_position** (`AVG(avg_position)`),
  defined even at zero clicks, never touches impressions.
- Volume = **CLICK SHARE** (pair clicks / total NLD clicks in window) — self-
  normalizes the seasonal traffic level, so a site-wide up/down cancels and what
  remains is genuine redistribution. Δshare in pp.
- Daily series drops impressions entirely; **row count** is the glitch tell instead.

**SAME conclusion as v1, now impression-free + corroborated:**
- Universe (non-seasonal) NLD clicks PRE 149,188 → POST 137,330 (−7.9%, weekday-matched).
- Paired 409,355 units: **click-share 55.3% → 52.4% (−2.89pp)** — established pairs
  ceded ~3pp of the click pie to newly-ranking/non-paired URLs (the redistribution).
- Unweighted-mean position 7.64 → 7.38 (long-tail *improved*); 158,695 pairs up ≥0.5
  vs 136,543 down. Only the up->2 bucket gained clicks; all others lost.
- By URL-type: **R-url took the hit** (−2.99pp share, pos ~flat 4.49→4.45); **C-url
  share flat (−0.05pp) but rank improved 4.93→4.75**; PLP rank improved 8.95→8.58;
  Browse +0.14pp share. → product/term pages lost, category/facet pages held/gained.

**INDEPENDENT cross-validation (the trust clincher):** Redshift SEO real-visits
(`datamart.fct_visits`+`dim_visit`+`chan_deriv.ref_channel_derivation_stats`,
`is_real_visit=1`, `dv.domain='1'`, `marketing_channel='SEO'`; url-type via
`dv.url LIKE '%/r/%'` vs `'%/c/%'`) — a source that never saw a GSC impression —
**agrees on direction and magnitude**: R/term −12.2%, C/facet +0.1%, other +0.3%,
total −6.1%. So the decline is real and concentrated in R/term/product pages,
regardless of how much you trust GSC impressions.

**Method gotchas:** short SQL aliases bite — Redshift rejected `ts` as a column
alias ("syntax error at or near ts"); use explicit names (`fl_tsale` etc.). v2
re-runs the full paired CTE once per segment (8 heavy joins for the intent loop) →
~10–12 min total; could be cut by materializing `pair` to a temp table first.

## May-vs-June SC: maincat-bucket-filtered variant + device + summary (2026-06-10)
User wanted the original `search_console_may_vs_june_2026_nld_v2.xlsx` analysis
re-cut to drop the "maincat bucket" rows. Script `/home/joepvanschagen/sc_compare_filtered.py`
→ `Downloads/claude/search_console_may_vs_june_2026_nld_no_maincat.xlsx`. Same
windows as the v2 file (May 11,13–16 vs June 1,3–6, impression-weighted ranking).
- **"Maincat buckets" = deepest_category_name == main_category_name** (e.g.
  `Klussen — Klussen`, `Woonaccessoires — Woonaccessoires`, and the `Beslist.nl`
  homepage bucket at 37.5k clicks). 32 buckets, ~96k clicks. Detect via a CTE:
  `GROUP BY deepest_category_id HAVING MAX(deepest_category_name)=MAX(main_category_name)`
  then `NOT IN`. Removing them leaves ~3,129 real deep-leaf cats (~184k clicks) and
  is applied to EVERY sheet (shared `UNIV` filter = excl ∪ mainbucket NOT IN).
- Sheets: Info, **Summary** (consolidated Avg-ranking Δ abs / Clicks Δ abs / CTR Δ
  rel across URL-type+keyword-length+device+maincat, each sorted by clicks Δ),
  By URL-type, By keyword length, **By device**, By maincat, Seasonal (excluded),
  Maincat buckets (excluded), Included deepest cats.
- **Finding (same movement, three angles):** total clicks −7,858 / impr −7.5% /
  pos +0.03. It's a **mobile** story (MOBILE −5,739 ≈ 73% of the drop, impr −12.4%;
  DESKTOP impr flat −0.9% and rank *improved* −0.20), a **mid-tail** story
  (Mid-tail −7,272 of −7,858), and a **furniture/home** story (Meubels −2,903,
  Woonaccessoires −2,352, Klussen −914; gainers Drogisterij +294, Fietsen +161).

## R-URL optimizer: V34 size facet on by default (2026-06-06)
User asked why Auto-Redirects proposed `/products/mode/mode_432360/c/fanshop~1335065~~ut_voetbalshirt~9134156`
for `/products/mode/r/nederlands_elftal_shirt_thuis_junior_maat_122-128_(xs)/`
and dropped the size. The size machinery (`src/size_tokens.py`) DOES recognise
`122-128`/`(xs)` and collect the match — it was just never emitted, gated behind
the `RESCUE_INCLUDE_SIZE` flag (off by default because per-size pages churn in/out
of stock). User asked to flip it on by default. Commit `04b0653`.

- **There are TWO facet-assembly paths and the flag only governed one.** Know
  which path a URL hits before debugging a missing facet (check the `reason`
  prefix):
  1. **V28 search-derived rescue** (`main_parallel_v2.py` ~line 1789) — fires
     only when the Search API finds a dominant deepest cat (`dom_cat_share >=
     0.75`, `DOMINANCE_THRESHOLD`). Consumes `multi_facets` + `size_facet` from
     the facet-probe cache. This was the ONLY path that read `RESCUE_INCLUDE_SIZE`.
  2. **`[child_subcat]` / `[V14 subcategory_match]`** (`_append_facet_to_subcat_redirect`,
     ~line 337) — fires when a subcategory NAME matches; appends facets by fuzzy-
     matching leftover tokens against the target subcat's facet pool. Has its own
     assembler and never touched the flag.
  The example URL resolves via path #2, so flipping the flag alone changed
  nothing for it. Worse, even path #1 wouldn't have fired here: the cached search
  signal had `dom_cat_name=Voetbaltenues dom_cat_share=0.36` (below 0.75) and the
  probe payload had no `size_facet` — and that dom_cat (`mode_4850293_7296077`) is
  a DIFFERENT category from where the winning redirect lands (Shirts `mode_432360`),
  so its maat values wouldn't even apply.

- **Flag lives in three spots — flip all three or the subprocess overrides you.**
  `RESCUE_INCLUDE_SIZE` module global (for direct imports), `init_worker_v2(...,
  rescue_include_size=...)` default (multiprocessing workers re-set the global per
  worker), and the CLI arg (the subprocess path argparse-parses, then passes into
  `init_worker_v2` initargs). The CLI was `action='store_true'` (defaults False),
  so it would force the worker back to False even with the global flipped. Changed
  to `argparse.BooleanOptionalAction default=True` (Python 3.9+, env is 3.12) →
  on by default with `--no-rescue-include-size` as the off-switch. The FastAPI
  service (`rurl_optimizer_v2_service.py`) builds argv WITHOUT the flag, so it now
  inherits the size-on worker default.

- **Fix for path #2: deterministic size append.** The fuzzy leftover collector
  (`_collect_longest_per_axis_from_leftover`) can't match numeric/short sizes —
  `122-128` is numeric, `XL` is <3 chars, both fail its len/fuzzy gates (this is
  exactly why `size_tokens.py` exists). Added a flag-gated step after the merk
  pass in `_append_facet_to_subcat_redirect`: `extract_sizes(parsed.keyword)` →
  `match_size_value(...)` against `[fv for fv in facet_values if _is_size_facet(fv.facet_name)]`,
  then append the matching FacetValue (wrapped in a `MatchResult`, `match_type='size_token'`).
  Skipped if a size axis was already collected, so no double-append.

- **`match_size_value` prefers letter over numeric** when both are present. Title
  "maat 122-128 (xs)" → picks `maat_mode_bovenkleding~471667` (XS), not `~23811956`
  (122/128). Both are valid maat values in the Shirts subcat; if you ever want the
  numeric form, that preference is in `size_tokens.match_size_value`
  (`return letter_hit or numeric_hit`).

- **Verified live; quantifies the thin-page tradeoff.** Search API for category
  `mode_432360` with the assembled filters: fanshop+voetbalshirt = 32 products;
  +maat XS (471667) = 2; +maat 122/128 (23811956) = 1. The page resolves, but
  size-narrowing is now ON for every run — these thin pages can empty out when
  that size sells out. That's the documented reason the flag was off; user
  accepted the tradeoff. No linter configured → validate with
  `dm-tools/venv/bin/python -m py_compile` (and run via the venv python, not
  system python — workers crash silently without `fuzzywuzzy`).

## R-URL optimizer: main-pass multi-facet convergence via subtree rescue (2026-06-03)
Follow-up to the 2026-06-02 hyphen/facet work. A category-pinned R-URL
`/products/mode/mode_432360/r/nike-nederlands-elftal-trainingsshirt/` collapsed
to the bare Shirts page; user wanted `mode_432360_469350` (Sportshirts) with
fanshop~Nederlands Elftal ~~ merk~Nike ~~ type_sportshirts. The main pass
(`process_url_v2`) preserves hyphens and matches facets only inside the pinned
subcat, so it under-served these; the global pass (`process_global_url`) splits
hyphens and does type-facet→child-subcat discovery, so the maincat-less variant
already worked. Converged them. Commit `55f1048`.

- **V32 `_is_cat_noun` used substring containment, collapsing rich queries.** For
  subcat "Shirts" it stemmed to `shirt` and tested `'shirt' in token`; the glued
  token `nike-nederlands-elftal-trainingsshirt` contains `shirt`, so the WHOLE
  query was judged "just the category noun" → bare-category redirect. Fix:
  whole-token + hyphen-split residual (`_split_strip_keyword` + `len>=2`
  equality), so V32 only fires when nothing meaningful remains (`/r/shirt/` still
  collapses; multi-token queries proceed).
- **Delegate facet-finding to the global-pass pattern, but BOUNDED to the anchor
  subtree.** `_derive_facets_in_subtree`: hyphen-split + drop bare category noun
  (whole-token — keep `trainingsshirt`, drop `shirt`), discover the best type
  facet whose subcat slug is under the anchor (`mode_432360` + children),
  descend, full multi-facet match. Bounding to the subtree is strictly SAFER
  than the unanchored global pass — it can't jump to an unrelated maincat.
- **Wire it as a RESCUE, never a pre-empt — verified by full-corpus diff.** A
  step-0 pre-empt (run delegation first for every subcat'd URL) regressed ~a
  dozen real URLs in the 754-URL diff: it overrode already-correct anchored
  matches (`alcatel_senioren_mobiel` Mobiele telefoons → wrongly Huistelefoons)
  and dropped facets (`illy_koffiebonen_1kg` lost '1 kg'). Pattern was sharp:
  wins where the baseline FAILED, regressions where it already SUCCEEDED.
- **Adoption rule is the safety mechanism (monotonic-safe).** Run the rescue when
  the cascade produced `<=2` facets; ADOPT only when (a) baseline had 0 facets,
  or (b) the rescue lands in the SAME destination subcat with strictly MORE
  facets (pure enrichment). (b) is why `samsung_55-inch_4k_uhd_tv` gets enriched
  (merk+4K → +55 inch) while `alcatel` (rescue's Huistelefoons is a DIFFERENT
  subcat) is left untouched. Trigger width only affects how often the rescue
  runs, never correctness.
- **Abbreviation gap: query acronym vs spelled-out facet value.** `televisie_b`
  value is "4K Ultra HD"; query says "4k uhd". `4k` is 2 chars (<
  MIN_KEYWORD_LENGTH_FOR_FUZZY=3 → dropped); `uhd` shares no letters with "Ultra
  HD" and `_is_semantic_match` rejects acronym↔expansion; token-coverage reduces
  the value to just `['ultra']`. So only the exact phrase "4k ultra hd" matched —
  ANY pass missed it. Fix = synonym entries in `synonyms.py` mapping the
  abbreviation to the EXACT normalized facet value name (`uhd`/`4k` →
  `"4k ultra hd"`, `fhd` → `"full hd"`, `8k` → `"8k ultra hd"`,
  `hd ready` → `"hd-ready"`); the matcher's synonym branch then gets an exact hit
  (score 95). Same class as hoesloze↔"Zonder overtrek".
- **Harness gotcha: don't `git stash` to swap code versions in a shared repo.**
  Multiple Claude agents run concurrently in this same working copy (saw 3+
  `claude` procs + a `pagetitles_from_unique.py build` at 74% CPU + concurrent
  pushes + a `git clean` deleting my untracked temp files mid-run). A botched
  `git stash pop` popped a pre-existing autostash and conflicted. Diff two code
  versions by `git show HEAD:<path> > /tmp/base.py` and importlib-loading it with
  the package dir on `sys.path` — keep temp files in /tmp (immune to git clean),
  run FOREGROUND. The full 754-URL side-by-side diff is too slow (~timed out at
  595s); only the URLs with a subcategory id are affected (both V32 + rescue gate
  on `parsed.subcategory_id`), so diff just those.

## R-URL optimizer: facet drop-out from 1-char subcat fragments + main-vs-global hyphen split (2026-06-02)
Session started from a user report: `/products/mode/r/nike_replica_-_..._nederlands_elftal_thuis_..._junior/` redirected with only `merk~84748` (Nike), dropping `fanshop~1335065` (Nederlands Elftal) and `ut_voetbalshirt~9134156` (Thuis), even though both facets exist in the chosen subcat `mode_432360_432464` (T-shirts). Two commits shipped: `0133a77` (main pass) + `c898cb2` (global pass).

- **A 1-char token from a hyphenated subcat name absorbs everything.** `_append_facet_to_subcat_redirect` (main pass) and the global pass both decide which keyword tokens are "already covered by the category" via `re.findall(r'\w+', matched_category_name)` + substring containment (`tok in cw or cw in tok`). For subcat "T-shirts" this yields `{'t','shirts'}`, and the 1-char `'t'` substring-matches **any** token containing a 't' — so `elftal` (el**ft**al) and `thuis` (**t**huis) were marked absorbed and stripped from the leftover BEFORE facet matching, leaving only `nike`. **Fix**: filter matched-name tokens to a min length (main pass `>=3`, global pass `>=2` so real 2-char nouns like `tv`/`3d` still absorb their own token). Verified: the same URL now yields `fanshop~1335065~~ut_voetbalshirt~9134156~~merk~84748`.
- **The main pass PRESERVES hyphens in the keyword; the global pass SPLITS them.** `parser._normalize_keyword` deliberately keeps `-` (so `tv-meubel`, `e-bike`, `TP-Link` stay one token and `match_subcategory_name` scores `tv-meubel` vs "TV-meubels" at 99). But `process_global_rurls.extract_keyword_from_global_url` does `re.sub(r'[-_+/]', ' ', kw)`, flattening hyphens to spaces. Consequence: the SAME slug behaves differently depending on whether a maincat is in the URL. `/products/mode/r/nike-nederlands-elftal-trainingsshirt/` (main pass, glued) under-matches; `/products/r/nike-nederlands-elftal-trainingsshirt/` (global pass, split) correctly gets all 3 facets.
- **Do NOT "fix" the main pass by splitting hyphens inside `match_multi_word` only.** Tried it — matching improved (3 facets) but `r.keyword` stays the glued token, so the downstream coverage/reliability scorer (`r.keyword.split()` → one 37-char token) computed 0% coverage → reliability 0 → triggered the V28 search-derived rescue → which then HARD-REJECTED on the long unmatched token. Net result: a working 2-facet redirect became `rejected_long_unmatched` / no redirect — strictly worse. Lesson: keyword tokenization must be consistent across matcher + coverage scorer + rescue, so a hyphen split belongs at parse/normalize time (with its own regression pass), not bolted onto one matcher function. Reverted; `matcher.py` left at baseline.
- **Hyphen role is genuinely ambiguous in the R-URL corpus** (of 74 hyphenated keywords seen): separators (`senioren_flip_telefoon_-_3g`, `nike-nederlands-elftal-trainingsshirt`) vs compounds/brands (`t-shirt`, `led-theelichtjes`, `1-persoonsbed`, `zwart-wit`, `g-star`). A blunt global split regresses compounds. The single distinguishing signal in the data: separators have 2+ hyphens, compounds have exactly 1 — but even a `>=2`-hyphen heuristic doesn't save `tv-meubel-hout` (2 hyphens → split → `meubel` mismatches).
- **Global-pass compound-noun recovery via bigrams.** Because the global extractor splits hyphens, a compound subcategory like "TV-meubels" can no longer be matched: `tv meubel` scores 84 (below the 95 threshold) while `tv-meubel` scores 99. Fix in `process_global_url` section 1: after full-keyword + per-word subcat-name attempts, retry **adjacent word bigrams rejoined with a hyphen** (`tv-meubel`, `meubel-hout`) at the HIGH threshold. This wins the compound-noun subcat BEFORE the greedy type-facet discovery (section 1.5b) drags `meubel` onto `Kapstokmeubels`. `/r/tv-meubel-hout/` went Kapstokken → TV-meubels.
- **Once a subcat is derived, strip the category-name tokens before in-subcat facet matching.** Section 1 used to run `match_multi_word(FULL keyword, subcat_facets)`, so `tv meubel hout` → TV-meubels still matched `meubel` onto brand `Profijt Meubel` / type `Wandmeubels`, and `curacao shirt` matched `shirt` onto the bogus `Fietsshirts` facet. Now it computes a leftover (keyword minus tokens the category name accounts for, same `_absorbed_by_cat` containment) and matches only that; empty leftover → bare subcat redirect. Effect across the corpus: removed a systematic `shirt`→`Fietsshirts` over-match on ~17 "curacao …" queries (they now land on a clean `Shirts` page), and `tv-meubel-hout` → TV-meubels `/c/materiaal~Hout`.
- **Open: token-coverage `return [tc]` single-result short-circuit caps multi-attribute queries at ONE facet.** `match_multi_word` runs `match_by_token_coverage` on the full keyword first and, if it matches, `return [tc]` — short-circuiting the per-word/pair passes. So `nederlands elftal trainingsshirt wk 2010` returns only `fanshop~Nederlands Elftal` (no type/merk), and after the leftover-strip change `nederlands_elftal_t-shirt_-_ek_88_-_xl_-_oranje` lost its valid `kleur~Oranje`. Separate, still-open issue behind several multi-facet gaps.
- **Repro harness note**: drive single URLs through `main_parallel_v2.process_url_v2((url, True))` (main pass) or `process_global_rurls.process_global_url((url, kw))` (global pass) after `preload_data(use_cache=True)` + `save_data_cache` + the module's `init_worker*`. Diff two code versions side-by-side by `git show HEAD:<path> > _tmp.py` and importing both (same package dir so `from src...` resolves) — do NOT use `git stash` to swap versions (it can pop an unrelated autostash and conflict).

## DM Review tool — Excel + pptx slide-2 refresh, pivot-driven charts (2026-05-28)
New tool `/api/dm-review` (button at `/static/dm-review.html`) refreshes the source workbook `review_dm_seo.xlsx` (OneDrive) for slide 2 of `DM review_NEW.pptx`. Pulls monthly + daily visits/omzet for SEO + DMA organic from `fct_visits`, monthly + daily SERP ranking by URL type / device from `bt.search_console` (country=`nld`, impression-weighted `avg_position`). Files: `backend/dm_review_service.py`, `backend/dm_review_router.py`, `backend/dm_review_pptx_tables.py`, `frontend/dm-review.html`. Dashboard tile + SEO-tools dropdown entry added on all 27 frontend pages (entries sorted alphabetically).

- **pptx charts here are OLE-linked to ranges inside the source Excel, NOT embedded.** Inspecting `chart.part.rels` shows `is_external=True` with `reltype=oleObject` pointing at the SharePoint URL. `python-pptx`'s `chart.replace_data()` fails on these with `"target_part property on _Relationship is undefined when target-mode is external"`. So updating the Excel is enough — PowerPoint reads the link on "Update Links". No need to touch chart XML for the data-bound charts.
- **The cell ranges the charts read from are pivot-table OUTPUT cells, not the raw data.** Raw data lives in cols A-D of `visits_omzet` / `visits_omzet_dag` / `serp_device`. The pivots sit further right (cols N-T, J-K) and the OLE link references those. Just appending rows to A-D doesn't update the chart until the pivot itself refreshes.
- **Each pivot table has a FIXED `cacheSource.worksheetSource.ref` like `A1:D1155` that doesn't auto-grow.** Appended rows are invisible to the pivot. Fix: `wsr.ref = "A1:D{new_last_row}"` after writing. Done in `_extend_pivot_sources()` in `dm_review_service.py`. Also set `pt.cache.refreshOnLoad = True` so the pivot recomputes when Excel next opens the file.
- **Pivots with rolling-window filters (e.g. "last 12 months only") add new cache items as HIDDEN.** Inspecting `pt.pivotFields[0].items`: newly-cached month came in with `h=True` while the older selected months stay `h=None`. The chart then doesn't see the new month even though the pivot has fresh data. Fix: count current visible items (= window size N), find the N most-recent dates in the cacheField, and set `h` accordingly on each pivotField item. Implementation in `_roll_pivot_filter_window()`. For pivots with all items visible (no filter), no-op.
- **PowerPoint's "Refresh Data" alone doesn't trigger Excel's pivot recompute.** User reported needing to manually open the Excel between running the tool and opening the pptx for the chart values to update. The `refreshOnLoad=True` flag triggers it when Excel itself opens the file (manually or via OLE). Acceptable workflow: open xlsx → save → open pptx → Update Links.
- **`openpyxl.utils.dataframe.execute_values(..., fetch=True)` with `page_size < total_rows` returns ONLY the last page's rows.** Bit me when I tried `INSERT ... RETURNING url_id` for 2,347 rows at page_size=2000 — got 347 rows back instead of 2,347. Workaround: don't rely on the RETURNING result; look up url_ids in a separate `SELECT` after the bulk insert. (Different file/context — auto-redirects URL ingestion — but the same gotcha applies wherever paginated `execute_values` meets `RETURNING`.)
- **openpyxl pivot APIs**: `ws._pivots` lists pivots on a sheet; `pt.cache.cacheFields[i].sharedItems` holds the actual cached values (date `<d v="ISO"/>`, num `<n>`, str `<s>`); `pt.pivotFields[i].items` holds the selection state — each item has `x` (cache index), `h` (hidden), `t` (`'data'` / `'default'`). To read raw item values, dump the sharedItems via `tostring(si.to_tree())` and regex-extract `<d v="...">`.
- **Encoding fallback for the Redirect Tool CSV upload (`backend/redirect_tool_router.py`).** Bulk redirect CSVs exported from Excel on Windows are cp1252-encoded; the parser used to default to utf-8 and 400 on the first `ë`. New `_read_csv_any_encoding` tries `utf-8-sig` → `cp1252` → `latin-1`. Frontend now shows a yellow warning (`#parseWarningBox`) when the file wasn't utf-8 so the user can eyeball non-ASCII rows for mojibake before submitting. cp1252 will eagerly decode anything (so a corrupted utf-8 silently lands as `Ã«`), but the warning + per-row preview catches it.
- **pptx info-icon pattern**: inline SVG with `<title>` child for the native browser tooltip (no JS deps, no Bootstrap tooltip init). 16×16, purple circle `#5e4a90` with white "i" (a small `<circle>` for the dot + `<rect>` for the stem). Used in Auto-Redirects and now DM Review. On a purple card-header background the circle blends, but the white "i" still reads clearly.
- **Slide-2 pptx tables**: there are three on slide 2 — Tabel 13 (SERP rankings, 4×4: `Type URL | prev_month | last_month | Delta`), Tabel 25 (Visits target/behaald, 3×2: `Kanaal | Target | Behaald`), Tabel 27 (Revenue, identical structure with `€` in target). All three update via `python-pptx` Cell text manipulation (`_set_cell_text` preserves the first run's font). Differentiate visits vs revenue card by presence of `€` in the existing target cell.
- **Target Excel layout** (`seo_targets.xlsx` sheet `2026`): row 6 = omzet target, row 8 = visits target, col C = Jan, col D = Feb, … col N = Dec, col O = totaal. Column index from month: `2 + month` (Jan=1 → col 3). May 2026 visit target = 3,318,426; revenue target = €461,268.

## R-URL optimizer — global R-URL routing + facet-selection quality (2026-05-27, session 2)
Follow-up session on the global (`/products/r/<kw>/`) pipeline, fixing three flagged football-shirt redirects. Commits `e61a692` (routing) and `6bc0f9b` (facet selection). Git tags `rurl-pre-multifacet-primary` marks the pre-change state for revert.

- **Relative vs absolute global R-URL parsed DIFFERENTLY — the bug only reproduced with the relative form.** The dashboard fed `/products/r/nederlands_elftal_shirt/` (no domain); my repro used the absolute `https://www.beslist.nl/products/r/...`. The absolute form parsed as invalid (correct → routes to global pass); the relative form was mis-parsed VALID with `main_category="products"`, building the malformed `https://www.beslist.nl/products/products/` and "No facet match". Cause: the relative `MAIN_CAT_ONLY_PATTERN` has an optional `(?:products/)?` prefix that **backtracks** and captures the literal `products` segment as the maincat; the absolute pattern requires a literal `/products/` so it can't. **Lesson**: when a repro "works" but the user's run doesn't, suspect input-shape divergence (domain present/absent, trailing slash) before chasing caches or stale processes — I burned a lot of time re-running the absolute form and clearing caches before testing the relative form in the parser directly. Fix: `_invalid_global()` returns an invalid parse when a captured maincat == "products", mirroring the absolute behavior.

- **A maincat-less URL has to escape the main pipeline AND be re-extractable by the global one.** Two compounding bugs: (1) parser mis-parse above; (2) the global pass's own keyword regex `GLOBAL_RURL_PATTERN` **required** `beslist.nl` in the URL, so even once routed correctly a relative URL extracted an EMPTY keyword → produced nothing. Both had to be fixed (made the domain optional) for the relative form to work end-to-end. **Rule**: every URL-shape regex in the pipeline (parser + global keyword extractor) must accept the same set of shapes; a fix to one is silently undone by the other.

- **`build_multi_facet` picked its primary facet by LIST ORDER, not score.** `primary_match = facets_from_different_category[0]`. When matched facets live in different subcats, the first one wins and only facets sharing its subcat-path are combined; the rest are dropped. For `...nederlands_elftal_uitshirt...` a spurious `"shirts"→"Skishirts"` (type_sportshirts, 80) was first and beat fanshop `"Nederlands Elftal"` (100). Fix: `max(..., key=score)` (stable on ties). **Shared code** with `main_parallel_v2` — validated a 6-URL categorised corpus byte-identical before/after.

- **The rich in-subcat match was gated only on a subcategory-NAME word match.** Global section 1 (full multi-facet match inside a discovered subcat) only fired when a keyword WORD matched a subcat name ≥95. `nike-nederlands-elftal-trainingsshirt-dames` had no such word, so it fell to the cross-type fallback, which preferred a cross-maincat `type_landen` (cadeaus) and V26-blocked → no redirect. Added **Stage 1.5b**: when no subcat name matches, take the best-scoring **type-facet** value (`type_sportshirts "Trainingsshirts"` → lives in `mode_432360_469350`), use ITS subcat as the discovered category, and run the full in-subcat match — picking up fanshop/merk/doelgroep that only exist inside that subcat. Result is the full combined URL `.../mode_432360_469350/c/doelgroep_mode~432482~~fanshop~1335065~~merk~84748~~type_sportshirts~9253235`. **Insight**: a type-facet value pinpoints a subcategory just as reliably as a subcat-name token; don't gate subcat discovery on names alone.

- **The same facet value can live in multiple subcats; which URL you get depends on which subcat you query.** fanshop "Nederlands Elftal" (1335065) exists in both `mode_432360_432464` (T-shirts) and `mode_432360_469350` (Sportshirts). Discovering the subcat via "shirts"→Shirts(parent) vs via type_sportshirts→Sportshirts changes the resulting `/c/` path. The user's desired combined URL was only achievable by discovering the 469350 subcat (where all four facets co-exist).

- **The maincat-predictor API (`productsearch.api.beslist.nl/categories/predict?query=&country=nl`) is NOT reachable from the WSL/dev sandbox** — no DNS for that host (only `productsearch-v2.api.beslist.nl` @ 10.130.144.221 resolves, and it 404s on the predict path). The `! curl` trick runs in the same sandbox, so it returns empty too. An idea to use it as a Stage 1.5c maincat-discovery fallback was shelved because the response schema couldn't be verified here. If revisited: needs a sample response from a host on the corp VPN, plus SQLite caching + QPS budget for ~10k+ URL Redshift runs.

## R-URL optimizer — facet-probe reachability, dom_cat semantics, two-pipeline routing (2026-05-27)
Big session reworking redirect-match quality from 11 flagged cases. The engine is a pipeline: matcher → search-derived rescue (V28) → facet-probe (V29/V31) → reliability scorer → url_builder. Fixes touched all of them. Commits 6ec604f, 87ad733, e6c8373, a38354c, b445ac3, 99e05a6. Git tag `rurl-pre-rework` (@e6c8373) marks the pre-Q8/Q10 state for revert.

- **A facet value can hide in THREE places, and the probe must check all of them.** The keyword-match fix (prefer a facet value whose NAME is a query token over the coverage winner) only works if the value is reachable. For "ketoconazol shampoo" it kept losing to type_shampoos 'Anti-roos' because: (1) Stage 1 `_check_surfaced` reads the **maincat-level** V28 payload, which OR-fallbacks (total 1.4M) and surfaces only merk/winkel; (2) Stage 2 candidates come from the `facets.csv` snapshot, which is **stale** — the ingr_shamp 'Ketoconazol' value was created 2026-04-20, after the snapshot, so it isn't listed. Nizoral worked only because it's a `merk` value and merk IS surfaced at maincat level. **Fix**: Stage 1.5 — a live **subcat-level** query (`category={dom_slug}&query={kw}`) that surfaces niche values, gated on a leftover token + only when Stage 1 misses. **Rule**: maincat-level search facets ≠ subcat-level facets; for niche/new facet values you must probe at the subcat (dom_cat) level, and never trust facets.csv to be current.

- **Any derived-data cache needs a schema-version stamp.** `search_derived` had `SCHEMA_VERSION` (re-derives on bump) but `facet_probe_cache` did NOT — so stale picks from older selection logic lingered forever, and the user kept getting the pre-fix 'Anti-roos' result even after the code fix. Added `PROBE_SCHEMA_VERSION` stamped by `_probe_put` / checked by `_probe_get`. **Rule**: when you change selection/derivation logic behind a cache, bump a version the reader checks — clearing manually is a one-off that the next deploy forgets.

- **The dominance-gate gotcha (Q8).** `_classify` picks the search-derived dom_cat by raw product **volume**, so "anti snurk kussen" → 'Massagekussens' (436) not 'Anti-snurk' (56). A semantic override (prefer the leaf cat whose NAME matches more distinctive query tokens, gated to >1 token + ≥10 products) picks 'Anti-snurk' — but the redirect STILL didn't change, because `_build_redirect_url` returns None when `dom_cat_share < DOMINANCE_THRESHOLD` (0.75); Anti-snurk is 10% share, so no rescue URL was built and the engine fell back to a weak Tier-D matcher result. **Fix**: a strong name match (≥2 tokens) bypasses the dominance gate (that gate exists to suppress noisy low-share VOLUME picks, not name matches). **Lesson**: a "correct in isolation" fix to one stage (dom_cat selection) can be silently nullified by a gate two stages downstream — trace the whole pipeline, and validate END-TO-END via the CLI, not just the unit.

- **Category head-noun false positives (Q10).** The primary `match_multi_word` call wasn't passed `category_name`, so the `words_in_category` skip never fired and the category's own head noun ("partytent", when already in the Partytenten subcat) matched a sub-type value `t_partytent "Zijwanden partytent"` — over-narrowing the page. Wiring `category_name=<subcat display name>` fixes it (mechanism already existed, just not connected at that call site). Separately, dimension tokens ("3x3") need an explicit match against unit-bearing facet values ("3x3 meter") that the lexical matcher skips over the unit word.

- **Long-unmatched-token hard-reject must be scoped to the rescue path.** Rejecting any ≥8-char unmatched query token (Q4 'bewegingssensor', Q7 'waterfilter', Q9 'inductiekookplaat') would also kill legitimate semantic-coverage matches with no lexical bridge (e.g. "hoesloze dekbedden" → eigenschap_beddengoed 'Zonder overtrek'). The discriminator: good matches go through `subcategory_name_with_probe_facet` (matched a real subcat, unmatched list empty); wrong ones through the search-derived rescue (`search_derived_subcat*`, populated unmatched). Scoping the reject to the rescue path only catches the wrong ones. Use stem-EQUALITY not substring: 'filter' ⊂ 'waterfilter' but a 'Filter' attribute ≠ a water filter.

- **Generic-attribute facets are noise as pure coverage winners.** A kleur/materiaal/maat/gewicht/formaat value that wins the probe on coverage alone (not a keyword match) is usually noise ("fontein wc" → materiaal~Keramiek). Suppress those for the coverage winner but keep them when they ARE a keyword match (so "rode jurk" → kleur~Rood still works). Type/eigenschap facets carry intent even via coverage, so they're NOT suppressed.

- **Two pipelines for two URL shapes.** `main_parallel_v2.py` only handles category-scoped R-URLs (`/products/{maincat}/r/...` or `/products/{maincat}/{subcat}/r/...`); its parser REJECTS maincat-less `/products/r/<kw>/` "mainpage" URLs (~10.5k of them). Those go through `process_global_rurls.py`, which matches the keyword against all categories. The global pass patches its results back into the FULL dataframe (preserving the main run's category rows) so running it on a mixed input never loses category results. Frontend now: Redshift → "Mainpage R-urls" checkbox controls whether to also pull/process them; manual + file upload → always run the global pass (process whatever is fed). Backend forces `also_global=True` for non-redshift sources.

- **Methodology that paid off.** (1) Build a small regression corpus (the 30-row `data/output/e2e.csv` + `data/input/sample.csv` + the flagged URLs + known-good controls). (2) Capture a CLEAN-CACHE baseline, make the change, clear caches again, re-run — comparing with clean caches both times isolates code changes from cache/data drift (a stale-cache baseline muddied the first Q8 diff and made it look like a regression). (3) git-tag the pre-change state. The corpus caught the Q8 Tier-D regression on the first (conservative) attempt before it shipped; the deeper rework then showed exactly 2 intended changes with identical tier distribution.

- **Don't conflate Search-API `crawlable` with taxonomy `noIndexNoFollow`.** Early in the session I wrongly called a facet value "no-index" based on the Search API's per-value `crawlable:false` flag. The authoritative SEO flag is the **Taxonomy API** `seoPriority` / `noIndexNoFollow`; the engine never reads the Search-API `crawlable` field. Verify SEO claims against taxonomy, not search.

## SEO Rulings — fixed-table sizing + double-encoded entities (2026-05-27)
- **`table-layout: fixed` + `width: auto` silently scales your pixel column widths.** Symptom that wasted a round-trip: bumped a column from 140px→180px and another 640px→800px in the Details tables, but on screen they looked identical. Cause: with `table-layout: fixed; width: auto` inside a `.table-responsive` (width:100%) block, the table resolves its width from the *container*, then clamps up to `min-width`. When the sum of declared column widths exceeds that resolved width, fixed-layout distributes the available width **proportionally** — so the columns are really percentages of a clamped total, and absolute px bumps barely move. **Fix**: `width: max-content` makes the table exactly the sum of its column widths (no scaling), and it overflows into the horizontal-scroll wrapper instead. `min-width` stays as a floor. **Rule**: any fixed-layout table whose columns should honour exact px widths needs `width: max-content` (or an explicit `width` ≥ the column sum), never `width: auto`.
- **beslist double-encodes glyphs in the meta description.** The search-result icons (e.g. ❎ = `&#10062;`, ✔️) arrive in the `<meta name="description">` content as `&amp;#10062;` — i.e. the `&` is itself entity-encoded. A single HTML-entity decode only reaches `&#10062;`; you need to **loop the decode until the string stabilises** (bounded to ~4 passes) to land on the actual glyph. Implemented as a textarea-based `decodeEntities()` that re-runs until `value === input`, then the result is fed through `escapeHtml` before innerHTML insertion (textarea parses entities but never executes markup, so it's XSS-safe). The unique-titles tool sidesteps this by injecting the title/description straight into innerHTML unescaped — works but is XSS-risky; the looped-decode-then-escape approach is the safer equivalent. Pairs with the `r.encoding="utf-8"` fetch fix (raw bytes must decode as UTF-8 first, *then* entity-decode).
- **Persisted-run UI masks fixes (again).** Same lesson as the encoding note below: the page rehydrates the last run from `pa.seo_rulings_runs`, so a server-side fix to title/description handling won't show until a fresh run overwrites the stored row (or you `DELETE FROM pa.seo_rulings_runs`). When the user says "still wrong after your fix", check whether they're looking at a stored run vs. a fresh one. The new Recent-runs Remove button + `DELETE /runs/{id}` make this easy to clear from the UI now.

## SEO Rulings tool — encoding, taxv2 quirks, persistence (2026-05-26)
- **beslist.nl serves HTML as UTF-8 but doesn't set `charset` in the `Content-Type` header.** Per RFC 7231 the requests library then falls back to ISO-8859-1, which mangles any non-ASCII char in the response. Symptom: a `<title>` containing ✔️ (UTF-8 bytes `E2 9C 94 EF B8 8F`) comes back as `âœ"ï¸` (or `âï¸` after copy/paste loses some chars) and the SEO Rulings "Rendered on page" column shows the mangled form. **Fix in one line**: `r.encoding = "utf-8"` immediately after `_SESSION.get(...)` and BEFORE accessing `r.text`. `r.text` is computed lazily on first access using `r.encoding`, so the assignment-before-read order is load-bearing. Affects every requests-based scraper that hits beslist.nl pages without forcing UTF-8 — worth grepping `r\.text` across the codebase if you see mojibake elsewhere.

- **The taxv2 active-flag is `isEnabled`, NOT `is_active`.** `GET /api/Categories/{id}` returns `{id, parentId, isBiddingCategory, isOverviewCategory, isStacked, isEnabled, createdAt, updatedAt}`. The cat_urls.csv snapshot doesn't carry the flag so the SEO Rulings sampler hits the live API per candidate (results memoized in a per-run cache to amortize). Worth knowing: every taxv2-driven sampler should filter on `isEnabled=true` because disabled categories often still appear in the CSV snapshot but return 404 on the live site.

- **`pa.unique_titles_content.url` (via `pa.urls`) stores RELATIVE paths** like `/products/mode/.../c/...`, not absolute URLs. Calling `requests.get` on a relative path fails or hits localhost. Any caller that fetches one of these URLs MUST prepend the site base (`https://www.beslist.nl`). The SEO Rulings `_check_variable` function does this defensively (`raw_url if raw_url.startswith("http") else f"{SITE_BASE}{raw_url}"`) and returns the absolute form in the response so the frontend's link cell can href to a real URL.

- **Stale cat_urls.csv slugs at max depth — depth-fallback is required.** The current snapshot has 6 depth-3 leaves all under `huishoudelijke_apparatuur` (Tefal Easy Fry variants), every single one of which 404s on the live site. A `_pick_one_live` with `SAMPLE_MAX_TRIES=50` exhausts the whole pool and gives up, leaving the "Deepest category" slot empty. **Fix**: walk `range(max_depth, 1, -1)` so when the entire max-depth pool is 404, the sampler drops to depth=2 (or whatever is alive) instead of dropping the slot. Pattern is reusable for any code that wants "the deepest available reachable category" rather than "the absolute deepest available in the snapshot".

- **Slack `chat.postMessage` `channel` field accepts both user IDs and channel IDs.** Same endpoint posts a DM (channel=`U…`) or to a channel (channel=`C…`). So the env-var name `SLACK_USER_ID` is a slight misnomer — a channel ID drops in with no code change. Bot needs `chat:write` for both; DM also requires `im:write` if the user hasn't messaged the bot first.

- **`python-dotenv` loads `.env` ONCE at process start. uvicorn `--reload` does NOT re-read it on file change.** uvicorn's reload watcher only re-imports Python modules when source files change; it does not re-execute `load_dotenv()` and doesn't refresh `os.environ`. After adding new env vars (e.g. `SLACK_BOT_TOKEN`), the process must be killed + restarted, not reloaded. Symptom: code clearly reads `os.getenv("SLACK_BOT_TOKEN")` and that var is freshly set in `.env`, but the call still sees the empty string. Worth a one-line `pkill -f uvicorn ... && nohup uvicorn ... &` rather than fighting reload behaviour.

- **Persisted UI state can mask a server-side fix.** The SEO Rulings page rehydrates from `pa.seo_rulings_runs` via `GET /api/seo-rulings/last` on `DOMContentLoaded`, so if a run was stored before a fix to `_fetch` (encoding) or `_check_variable` (URL absolutization), the page keeps showing the buggy result even though new runs would render correctly. **Pattern**: when the user reports "still seeing X" after a fix, check whether the page reads from a persisted store and whether old rows in that store carry the pre-fix data. Easiest cleanup is `DELETE FROM pa.seo_rulings_runs` to force a fresh run on next visit.

- **Static-file frontends don't need uvicorn restarts; backend module changes do (in theory).** SEO Rulings' frontend is at `/static/seo-rulings.html` with inline JS — served directly from disk each request, so a hard browser refresh (Ctrl+Shift+R) picks up the new HTML/JS immediately. Backend `.py` changes are picked up by uvicorn `--reload` for request handlers, but NOT for module-level state that's already in memory (caches, sessions, long-running threads — see the 2026-05-20 "uvicorn --reload does NOT propagate to long-running background threads" learning). For env vars, see the python-dotenv note above.

- **psycopg2 `RealDictCursor` returns dict-shaped rows; `row["col"]` is the safe access pattern.** Already covered in the 2026-05-20 facet-position-rules learning, but worth re-flagging because the SEO Rulings persistence code (`_persist_run`, `get_last_run`) relies on it. New helpers should always use `row["col"]` rather than tuple-unpacking — works for both regular and `RealDictCursor` only via the same `if hasattr(row, 'get'):` shape-check; we standardised on dict-style.

- **Files**: `backend/seo_rulings_service.py` (new, ~590 lines: pipeline + caches + persistence), `backend/seo_rulings_router.py` (new, 3 endpoints), `backend/main.py` (router include + startup init), `frontend/seo-rulings.html` (new, single-page UI with run button + summary + Slack panel + per-check tables). Migration: `init_seo_rulings_tables()` creates `pa.seo_rulings_runs` (run_id SERIAL PK, started_at, finished_at, passed_count, failed_count, result JSONB) on startup. Commits `0cceac4` (initial), `1b9d60f` (dashboard tile + persistence + clickable details + mt-5 alignment across tool pages).

## R-URL optimizer: V32 cross-depth brand/shop facet rescue in build_multi_facet (2026-06-03)
- **Symptom**: Auto-Redirects suggested the bare `https://www.beslist.nl/products/mode/mode_432360_432464/c/fanshop~1335065` for `/products/mode/r/nike_nederlands_elftal_uitshirt_2020-2022/` — the Nederlands Elftal fanshop facet was appended but **the Nike merk facet was not**, even though the kit is a Nike product.
- **Reproduction harness** (no full pipeline run needed): set `main_parallel_v2._worker_data` by hand (`parser`/`facet_filter`/`matcher`/`builder`/`category_lookup`/`all_type_facets`/`categories_df` from `preload_data(use_cache=True)`) then call `process_url_v2((url, True))`. Confirmed `matched_keywords='nederlands, elftal'`, `unmatched_keywords='nike, ...'`. Dropping to `matcher.match_multi_word(kw, mc_facets, require_type_for_merk=True/False)` directly showed the matcher **does** return BOTH facets at score 100 regardless of the flag — so `require_type_for_merk` was a red herring; the drop was downstream in the URL builder. **Always invoke via `dm-tools/venv/bin/python`** — system python lacks `fuzzywuzzy` and the matcher import dies.
- **Root cause**: `FacetFilter.get_facet_values` keeps **one representative row per facet value** across a maincat, at whatever depth `CHILD_DOMINANCE_THRESHOLD = 0.7` (facet_filter.py) settles on. A concentrated value (fanshop "Nederlands Elftal") resolves to the leaf `mode_432360_432464`; a value spread thin across dozens of subcats (merk "Nike": 2285 here, 2452 there, …) never clears 70% of any parent so it parks at the shallow parent `mode_432360`. In `url_builder.py:build_multi_facet`, the maincat-level R-URL (no subcat) sends both matches into `facets_from_different_category`; the primary is picked by `max(score)` (fanshop wins the 100/100 tie because merk is a *strict* facet `match_multi_word` sorts last), then the `same_target_matches` loop only appends `other` matches whose `_extract_category_path_from_facet_url(other.url) == category_path`. Nike's cached path is `…/mode_432360` ≠ the primary's `…/mode_432360_432464`, so it's dropped — **even though Nike genuinely exists under the leaf** (Search API: 3416 products; the combined `fanshop~1335065~~merk~84748` page returns real products). This is the case the 2026-05-13 "attach all same-target matches" fix did NOT cover: that one combined facets already sharing the same depth (`nike_schoenen_dames`, both in `mode_432362`); this one is a depth *mismatch* from the dedup artefact.
- **Fix** (commit `a361498`, 5 files +227 −4): a leaf-existence-verified rescue.
  - `src/url_builder.py`: `UrlBuilder.__init__` gains optional `self.facet_url_exists` (callable `(url:str)->bool`, default `None`). In the `same_target_matches` loop, an `elif` rescues `other` when its axis is in new module const `_CROSS_DEPTH_RESCUE_AXES = {'merk','winkel'}` **and** `facet_url_exists(f"{category_path}/c/{other.facet_value.url_fragment}")` is true — i.e. the brand/shop value really exists under the primary's leaf. `url_fragment` (`merk~84748`) is depth-independent, so only the leaf URL needs verifying. When the checker is `None` it falls through to the old drop → standalone callers and unit tests are unaffected.
  - **Why gated to merk/winkel only**: a brand/shop is one entity smeared across many subcats (the dedup artefact). Type/colour/etc. facets are genuinely subcategory-specific, so a depth mismatch there usually signals real "different subcat" intent, not an artefact — rescuing them would fabricate wrong combos.
  - `src/facet_filter.py`: new `FacetFilter.facet_url_set()` → cached `frozenset` of all facet URLs (O(1) membership; `_url_set_cache`).
  - `main_parallel_v2.py` (`init_worker_v2`) **and** `process_global_rurls.py` (`init_worker`): both now build the `FacetFilter` + `UrlBuilder` explicitly and wire `builder.facet_url_exists = facet_filter.facet_url_set().__contains__`.
- **The cross-maincat blocks still fire first**: the rescue lives inside the `if category_path:` arm, *after* the V16 merk-missing and V26 cross-maincat early-returns, so it can never bridge into a different maincat — and it only appends a URL the facet set proves exists.
- **Test**: `backend/rurl_optimizer_v2/tests/test_cross_depth_rescue.py` (new — first test in this package). 4 pure tests (no DB/network): rescue fires; no rescue when brand absent from leaf (never fabricate a dead end); legacy drop when checker unset; rescue limited to merk/winkel. Has a `__main__` runner so it works without pytest. **`pytest` was not in the venv** — installed it (`venv/bin/pip install pytest`). No linter is configured for this project (no ruff/flake8/pre-commit/pyproject), so `/lcp` lint == `python -m py_compile` on the changed files.
- **Verified end-to-end**: target now → `…/mode_432360_432464/c/fanshop~1335065~~merk~84748` (combo returns 4 live products); `nike schoenen dames` and `samsung televisie` also correctly carry the brand now; `zwarte jurk` (single facet) and cross-maincat paths unchanged.

## Conventions — file output location (2026-05-18)
- **All deliverables (xlsx, csv, pdf, png, etc.) go to `C:\Users\JoepvanSchagen\Downloads\claude\`** (WSL path: `/mnt/c/Users/JoepvanSchagen/Downloads/claude/`).
- Create the folder with `mkdir -p` if it doesn't already exist. Intermediate working files can stay in the Linux home dir; only the final artifact the user will open from Windows needs to land in `Downloads\claude`.
- Mirrored in user memory at `feedback_downloads_folder.md` so the convention applies across all Claude sessions, not just dm-dashboard.

## Unique titles: per-facet position rules + type-facet table + inline LLM classifier (2026-05-20)
- **The catalyst**: the user's grammar audit of `pa.unique_titles_content` (960k h1 titles, ~8% flagged) surfaced two distinct classes of bug — facet ordering wrong (`Kussenhoezen leren` vs `Leren kussenhoezen`) and category missing because a "type-facet" replaced it (`vidaXL Bruine kunststof zelfklevende woonkamer` for a Vloeren URL with `t_laminaat` facet). Both want the same control surface: a per-URL-slug configuration table. Shipped as one config table (`pa.facet_position_rules`), seeded from the user's curated `facet_order.xlsx` (2,284 slugs with global order_index 1..2284 + boolean replace-category flag, 690 marked as type-facets), with an inline LLM classifier that auto-populates missing slugs on first sighting. Commit `78cd3ca` (rebased), 3 files +538 -23 lines.

- **The "type-facet" concept already existed in a different shape** (`pa.facet_type_classifications`, keyed by `(facet_name lowercased, sample_category lowercased)`, populated by `backend/facet_classifier.py:batch_classify_facets`). 34k rows, LLM-classified per (facet_name, category) pair. The new table is keyed by **url_slug** alone (global). They coexist: `_type_facet_override_by_slug()` consults the new table first; if it returns None (no opinion), the call chain falls through to the legacy classifier, then the hardcoded `_ALWAYS_TYPE_URL_SLUGS = {'t_stoel'}` and `_ALWAYS_TYPE_URL_SLUGS_MAINCAT_ONLY = {'t_meubelset': 'meubilair'}` sets. The new slug-keyed lookup wins because the URL slug is the more reliable signal (one slug can map to multiple Dutch labels like "Type" / "Thema" across categories, but the slug itself is unambiguous). **Rule for future code**: when a classifier is keyed on something derived (display label), the underlying primary key (URL slug) is usually a stronger signal — prefer it as the override layer.

- **Excel-driven import requires a CSV-quoting + locale-fallback trick.** The user's Excel exports as `;`-delimited CSV (Dutch locale), columns become `slug;sample_values;total_values;is_type_facet;is_type_facet_new;...`. Read via `csv.DictReader(f, delimiter=';')`. The `to_bool` helper accepts `True/true/T/1/yes/y` — Excel writes `True`/`False` (capitalized) and Python's `bool()` of those strings would mistakenly return True for both, so explicit string-comparison is mandatory.

- **The Taxonomy API's `/api/Facets/{id}/values` response is wrapped + multi-locale.** Shape is `{total: int, items: [...]}` (not a bare list). Each value's `labels[]` array carries `nameInColumn` / `nameOnDetail` (not `name`) keyed by locale. **Two pitfalls**: (1) strict `locale='nl-NL'` filter silently drops ~30% of facets where labels use `locale='global'` (publishers like Activision, brand-line series like Bosch Serie 4). Fallback chain: `nl-NL → nl-BE → global → any-locale`. (2) `?take=500` caps the response; default page size is 50 — for high-cardinality facets like `type_productlijn` (1331 values), pagination via `?skip=N&take=500` is needed if you want the full list, but for value-label-by-id lookups a single page is enough. **Rule**: for any taxv2 endpoint that returns labeled lists, always probe the response shape (`{items: [...]}` or bare list) and the locales present before writing the parser.

- **psycopg2 misinterprets literal `%` in SQL as parameter placeholders.** Symptom: `IndexError: tuple index out of range` from `cur.fetchone()` after a query that has `WHERE url LIKE '/products/schoenen/%' AND ... %s` with a single bind value. Python's `%`-formatting (which psycopg2 uses internally with the regular `cursor.execute`) tries to consume the bind value into the literal `%'` first, leaving the actual `%s` unbound. **Two workarounds**: (a) route the LIKE pattern through a bind variable too — `WHERE url LIKE %s AND ... %s` with `(SCHOENEN_PATTERN, OTHER)` — both `%` chars are then placeholders, no conflict; (b) double the literal `%` to `%%` in the SQL string. Option (a) is cleaner because it also lets you reuse the pattern. **Rule**: never inline a literal `%` in a psycopg2 query that also has `%s` parameters — always bind both.

- **uvicorn `--reload` does NOT propagate to long-running background threads.** The dm-tools backend runs a worker thread (started by `start_processing()`) that holds **closure references** to imported functions like `_facet_position_clause`. When uvicorn detects a file change and reloads the module, the worker thread keeps using the OLD function objects — so a code fix lands in the module namespace but the in-flight worker never sees it. Symptom: file shows the fix, an `importlib.reload + generate_title_from_api(url)` smoke test produces the correct title, but URLs the worker actually processes still show the buggy output. **Fix**: kill the uvicorn process and start it fresh (`ps aux | grep uvicorn` → `kill <pid>` → restart). The worker thread's imports re-bind to the current module on process startup. Auto-reload is fine for request-handler routes but NOT for background-thread workers. **Rule for future debugging**: when a fix tests clean in isolation but production output is still buggy, the running worker has stale closures. Restart, don't iterate the patch.

- **The position-clause feeding bug** that triggered the user's "34 Maat 34" reports. `_facet_position_clause(selected_facets)` was being called with the full facet list, which still contained the Maat facet (with `detail_value='Maat 34'`). The AI's prompt then listed `"Maat 34"` in the VOLGORDE section AND the post-AI `size_values` append also added `"Maat 34"`. The AI sometimes truncated the prompt-listed `"Maat 34"` to just `"34"` (rule 5 says "sizes belong at the end") and inserted it mid-title, while the append step added the canonical `"Maat 34"` at the end → `…schoenen 34 Maat 34`. **Fix**: pass `non_size_facets` (which excludes spec/suffix/voor values — those are handled by code, not the AI) instead of `selected_facets`. **General rule**: when a clause in a prompt advertises values that ANOTHER code path is also going to add post-AI, the AI WILL find a way to duplicate them. Either remove the value from the clause OR from the post-AI append, never both.

- **Hard kill switches for layered safety**: the system has three independent revert paths. (a) **Soft data revert**: `TRUNCATE pa.facet_position_rules;` — empty table → helper returns `{}` → all callers fall through to the legacy classifier and the pre-2026-05-19 behaviour. (b) **Env kill switch**: `DISABLE_FACET_POSITION_RULES=1` short-circuits `_load_facet_position_rules()` to return `{}` without even hitting the DB. `DISABLE_FACET_INLINE_CLASSIFY=1` similarly disables the inline LLM classifier. (c) **Hard code revert**: restore `backend/ai_titles_service.py.bak.2026-05-19_pre_facet_position_rules` (kept on disk, intentionally not in git). **Pattern worth reusing**: any new feature gated on a config table should have an env-var kill switch in the helper that loads from the table — empty-table-equals-no-op is good but a single env var is the fastest "stop everything" lever when production is misbehaving.

- **Inline LLM classifier — batch by category for cheap fill-in.** When 249 unrulled slugs exist and the user wants them all classified at once, group by sample category (one LLM call per category-batch) instead of one-call-per-slug. In practice ~80% of slugs grouped under the empty/null category (most pa.urls rows have `deepest_subcat_name=NULL`), so the 249 went through as ONE big prompt with all slugs listed — 187s for the round-trip, $cheap. Per-URL inline classification (the production code path) only ever sees 0–3 missing slugs per URL, so the prompt stays tight. **Anchors in the prompt are load-bearing**: `merk=3, kleur=22, doelgroep_mode=400, materiaal=600, stijl=900, vorm=1200, eigenschappen=1500, thema=1900, formaat=2145, maat=2300, conditie=2400` — without these the LLM picks arbitrary integers in the 1..2400 range and the ordering becomes meaningless.

- **Inline LLM bias toward "front of title" for type-facets.** The LLM systematically placed `is_type_facet=True` slugs at low orders (3, 22, 400) — interpreting "this facet is important enough to replace the category" as "this facet goes at the very front of the title". But type-facets should go where the CATEGORY noun would have been — mid-to-late, around the productnoun slot. For the deterministic v3 composer, low-order type-facets end up in the `other_adj` bucket (sorted before category) and produce nonsense. **Cheap mass fix**: `UPDATE pa.facet_position_rules SET order_index = 1700 WHERE source='llm_suggested' AND is_type_facet AND order_index < 1000` (114 rows). 1700 is between the `eigenschappen=1500` and the post-category clauses. The classifier's prompt should ideally tell the LLM "type-facets get order ≥1500" explicitly — that's a future prompt tweak.

- **Schema gotcha — Postgres rejects expressions in PRIMARY KEY but accepts them in UNIQUE INDEX.** Original migration had `PRIMARY KEY (facet_slug, COALESCE(scope_category, ''))` and failed at apply time with `syntax error at or near "("`. Replaced with `CREATE UNIQUE INDEX ... ON pa.facet_position_rules (facet_slug, COALESCE(scope_category, ''))`. Functionally equivalent for `ON CONFLICT (facet_slug, scope_category)` upserts (Postgres infers the index). **Rule**: when an upsert key needs to treat NULL as a value, use a partial UNIQUE INDEX with COALESCE, not PRIMARY KEY.

- **`RealDictCursor` silent data loss**. The dm-tools connection pool is configured with `cursor_factory=RealDictCursor` — fetchall returns `RealDictRow` (a dict subclass). My initial implementation of `_load_facet_position_rules()` did `for slug, order_index, is_type, position in cur.fetchall()` — that iterates the dict's KEYS as 4-tuples, silently producing one "row" with keys-as-values per actual DB row, but only the first iteration's row got kept (a dict has only 4 keys). The smoke test reported `rules loaded: 1` instead of the expected 2,284. **Fix**: defensively handle both shapes: `if hasattr(row, 'get'): slug = row.get('facet_slug') else: slug, ... = row`. **General rule**: any helper that may run in either a script (regular cursor) or the server (RealDictCursor) needs the defensive shape check. Don't assume tuple-unpacking.

- **The orphan-slug discovery**: 16 of 2,096 distinct URL-slugs in `pa.urls` have no matching entry in `/api/Facets` at all (`artikelgroep`, `bouw_stoomoven`, `combimagnetron`, `dier_dienenbenodigdheden`, `fisher_price_series`, `flatscreen_tv`, `o_bloempot`, `opties_babybox`, `opties_bureaus`, `prodl_keukenm`, `rugged_mobiele_telefoon`, `serie_airfryer`, `soort_meubel`, `type_fiets`, `type_harddisk`, `type_keukmach`). These appear in URLs (and therefore drive title generation) but the taxonomy doesn't know them — probably deprecated/renamed/never registered. **Implication**: any taxv2-driven classifier needs a "no facet for this slug" fallback. The inline LLM call still produces a verdict from slug name + URL's sample VALUE_ID + category context — that's the failsafe.

- **Worker-pickup gate is a `LEFT JOIN unique_titles_content` on h1_title nullness, not on `jobs.status`.** Discovered when 60,907 reset jobs showed up as only 111 in the UI's pending count. `get_unprocessed_urls()` is:
  ```sql
  WHERE j.status = 'pending'
    AND (c.title IS NULL OR c.title = '' OR c.h1_title IS NULL OR c.h1_title = '')
  ```
  Resetting `unique_titles_jobs.status='pending'` alone is a no-op — URLs that have existing content are skipped to avoid burning LLM calls on already-processed URLs. To FORCE regeneration: null out `unique_titles_content.h1_title` for the affected url_ids too. Did this with a backup column (`h1_title_prev_20260520 text`, populated from current h1_title before nulling) so the old titles are recoverable. **Rule**: "reset to pending" semantics depend on what gates the worker — read the SELECT in `get_unprocessed_urls` before deciding whether to also clear the content table.

- **Non-product URLs in `pa.urls`** (109 rows total): landing pages, themed inspiration pages (`/cadeaus_voor_haar/`, `/badkamer_ideeen/`, `/kerstkleding/`), info/account pages (`/x/`, `/accounts/aanmelden/`), all under `pa.urls` but NOT under `/products/...`. The productsearch API can't fetch data for these — they always fail `generate_title_from_api` with `api_failed`. Marked all 98 (the 11 already had different statuses) as `status='failed'` with `last_error='skipped: non-product URL (not a /products/ path)'` in `unique_titles_jobs`, `kopteksten_jobs`, `faq_jobs`. **`is_active` column on `pa.urls` is NOT consulted anywhere in the code** (grep confirmed) — flipping it to `FALSE` is purely an audit marker, doesn't change worker behaviour. The real "skip these forever" mechanism is the `status='failed'` in the job tables. **Rule**: when adding a "skip these URLs" flag, audit grep for the column name first; if the existing column isn't consulted, marking the JOB rows is the actual control surface.

- **Files**: `backend/ai_titles_service.py` (+419 -23 lines: helpers `_load_facet_position_rules`, `_type_facet_override_by_slug`, `_ordered_facet_values`, `_facet_position_clause`, `_classify_unrulled_facets_inline`, `_persist_classified_facets`, `_classify_and_persist_unrulled`; refactored `_is_type_facet_for` helper inside both `generate_title_from_api` and `generate_title_v3`; `_build_v3_h1` post_category bucket + sorted other_adj; `_build_polish_prompt` accepts `position_rules_clause` kwarg; both v1 and v2 templates append it), `migrations/2026-05-19-facet-position-rules.sql` (+33 lines), `scripts/list_unrulled_facets.py` (+86 lines, new, read-only audit). Commit `78cd3ca`.

## R-URL optimizer: V31 facet-probe path appends covering facet on niche queries (2026-05-19)
- **The trigger case from the user**: ran Auto-Redirects on `https://www.beslist.nl/products/huis_tuin/r/hoesloze_dekbedden/`. Got back the bare Dekbedden subcat (`huis_tuin_505062_505149/`). The "right" answer was the same subcat with a facet appended: `.../c/eigenschap_beddengoed~23812125` — the "Zonder overtrek" value (literally "without cover", which is synonymous with "hoesloos"). Keyword and facet value share **zero tokens** ("hoesloze" vs "Zonder overtrek"). No lexical or synonym path the matcher can use can ever bridge that gap. But running the search API for "hoesloze dekbedden" in dom_cat yields 17 products, 12 of which carry `eigenschap_beddengoed=23812125` — coverage 70%. The V29 facet-probe layer was built exactly for this signal; it was wired up to neither fire nor consume for the common case. Fixed in commit `e21bf0b` with four coordinated pieces (one per file). **Full commit body** has the bullet-by-bullet detail; this section captures the load-bearing learnings worth keeping for future debug sessions.

- **The Beslist search API has an OR-fallback heuristic that breaks the obvious "total = AND match count" assumption.** When AND-matching produces fewer products than `limit`, the API silently switches to OR-mode and returns `total = whole-cat OR count` (millions). For "hoesloze dekbedden" with `limit=50`: total=6,978,564 (whole-maincat OR), but only 17 products are genuinely AND-matched (visible in `categories[]` array counts). limit=1 forces AND mode for the unfiltered query (AND-count >= 1 is always true). For filtered probes (`query × facet_filter`), the same kicks in whenever the filter+AND count is tiny. **Signature**: `total >= 10000` but the products returned look entirely on-topic, OR `coverage = filtered_total / base_total > 1.0` (impossible for a real AND-restricted subset). **Two pragmatic detections**: (1) `_classify` no longer short-circuits on `total >= AND_MODE_TOTAL_THRESHOLD` — it always extracts `dom_cat_*` from `categories[]` (which still reports true AND counts even in fallback mode). Mode is reported as `"fallback"` vs `"and"` so callers can tell apart; downstream consumers (`_build_redirect_url`, `_do_probe`, `prefetch_facet_probes`) accept either. (2) `_probe_one` rejects `cov > 1.0` — that's the unambiguous OR-fallback fingerprint in a filter probe and prevents non-covering facets like `materiaal=Katoen` (cov=1345.3) from beating the real winner (`eigenschap_beddengoed=23812125`, cov=0.706).

- **`matched_keywords` / `unmatched_keywords` are unreliable for "did this keyword token actually land in the target?" checks.** The matched_keywords logic in `main_parallel_v2.py:_evaluate_url_result_v2` short-circuits to "everything matched" whenever `r.match_type in TRUSTED_MATCH_TYPES = {synonym, token_coverage, subcategory_name}` — including the common subcategory_name case. For "hoesloze dekbedden" matching subcat "Dekbedden", both tokens are marked matched even though "hoesloze" has zero lexical overlap with "Dekbedden". The V31 leftover-token consumer therefore **computes leftovers locally**: token-in-keyword loop over `(redirect_cat_name + facet_value_names + redirect_url)` lowercased, with a `rstrip('e').rstrip('s')` stem strip for Dutch plurals. **Rule**: any post-match analyzer that needs the actual leftover keyword tokens should re-derive them from the URL/target text, not read the stored field — that field is a score input, not a truth signal.

- **The V29 probe iterates candidates sorted by raw subcat-wide `count` desc; this is biased against niche-but-relevant facet values.** Catch-all values like kleur=Wit (count=5248 in `huis_tuin_505062_505149`) crowd the head; the actually-covering value (`eigenschap_beddengoed=23812125` "Zonder overtrek", count=683) sits at rank ~32 of 190. The old `MAX_CANDIDATES_PER_PAIR=15` cap silently dropped the real winner from consideration. Fix: raised cap to 50 + added `EARLY_STOP_COVERAGE=0.9` so easy cases short-circuit and don't pay the 3x probe cost. A smarter ordering (by query-specificity signal — e.g. which facets the base response surfaces vs. all candidates) would be ideal but the API doesn't give us that signal cleanly; the base response's `facets[]` is artificially capped (~2 facets for this query, doesn't include eigenschap_beddengoed at all). **If a future query keeps missing the right facet, the 50-cap is likely too low — bump it before redesigning the ordering.**

- **The dashboard service was passing `--multi-facet` but not `--enable-facet-probe`.** Without it the V29 prefetch never runs and `derive_search_facet` reads an empty cache, so any consumer in the engine returns empty-handed. **One-line fix** in `rurl_optimizer_v2_service.py`: `argv.append("--enable-facet-probe")` unconditionally after the multi-facet flag. The flag adds ~one extra sequential probe pass per unique (maincat, keyword) pair, cached for 7 days, so the typical run cost is amortised to near-zero. The cache lives at `backend/rurl_optimizer_v2/data/cache/search_derived.sqlite`.

- **Schema-versioned cache invalidation pattern** — added `SCHEMA_VERSION = 2` stamped into every cached payload by `_classify`; `_cache_get` returns None for entries lacking the right schema, forcing re-fetch. Bump the constant whenever the classifier's output shape changes. Old-format entries (e.g. `{mode: fallback, total: N}` with no dom_cat) get re-classified on next run without manual cache surgery. **Reusable**: any cache that stores derived-from-API data and might evolve should follow this — write SCHEMA_VERSION, read-side filters on mismatch.

- **The V31 leftover-token consumer is gated on `matcher_subcat == dom_cat_url_slug`.** If they differ, we don't append the probe's facet — the facet may not even exist in the matcher's chosen subcat (would emit a 404 URL). When they match (most cases where V28 also liked the matcher's pick), the leftover signal is safe to use. Failure mode if this gate is removed: the engine would attach a facet from a different subcat and the redirect would 404.

- **Found a latent hang in the engine when running outside its venv**: `main_parallel_v2.py` uses `multiprocessing.Pool` with `spawn` workers; if the worker's `init_worker_v2` import chain fails (e.g. `fuzzywuzzy` missing in system Python), the workers crash but the parent doesn't propagate — pool stays open, parent process sleeps forever waiting for futures. Symptom: 0.2% CPU, 250MB RAM, no progress, no stdout. **Always invoke with the dm-tools venv**: `PYTHONPATH=. /home/joepvanschagen/projects/dm-tools/venv/bin/python main_parallel_v2.py ...`. The dashboard service already does this via the service runner; only matters for direct CLI invocation.

- **Files**: `backend/rurl_optimizer_v2_service.py` (1 line: flag), `backend/rurl_optimizer_v2/src/search_derived.py` (schema version, classifier reads categories[] in fallback mode, _build_redirect_url accepts fallback), `backend/rurl_optimizer_v2/src/facet_probe.py` (`mode in {and, fallback}`, `dom_cat_count` as base_total, `cov > 1.0` rejection, MAX 15→50, early-stop), `backend/rurl_optimizer_v2/main_parallel_v2.py` (V31 leftover-token consumer block + accept fallback in legacy-disagree elif). 4 files, +148 -14 lines, commit `e21bf0b`.

## R-URL optimizer: V31 chain — count-aware dedup + Dutch suffix decomposer + matcher precedence (2026-05-20)
- **The session's diagnostic pattern**: the user surfaced six wrong redirects, each a different failure mode. I diagnosed each by reading `rurl_processed` (Postgres in n8n-vector-db) and looking at `match_type` + `reason` — that combination tells you *which* code path fired (`subcategory_name` / `multi` / `cross_category_type` / `search_derived_subcat` / `search_derived_subcat_with_facet` / `category_fallback` / `cross_maincat_blocked`) and *what* it found. **Rule**: when a redirect looks wrong, always pull the cached row first. The `reason` field carries human-readable trace from every pass (`[maincat]`, `[child_subcat]`, `[V28 compound:'X']`, `[V31] appended ...`). Fixed in commit `b68e3d5`/`f589d8b` (rebased), 9 files, +426 -41 lines.

- **`_deduplicate_to_highest_level` in facet_filter.py assumed a parent row exists in the data — it usually doesn't.** Old algorithm: pick the shallowest-depth instance per `facet_value_id`, ties broken by Python's `min` iteration order. For brand "Ferrero Rocher" across Eten & drinken subcats, the Search API's facets.csv returns rows for the leaf subcats (Bonbons count=14, Chocolade count=10) plus a couple of unrelated depth-1 siblings (Brood count=4, Medische voeding count=2) but **no Snoep row** (the natural parent of Bonbons+Chocolade). The shallowest-pick then locked onto Brood and the user's `/r/ferrero_rocher/` redirected there. **New algorithm**: pick the count-leader globally, tie-broken by shallower. Then check the leader's descendants — if any descendant has count ≥ `CHILD_DOMINANCE_THRESHOLD` (0.7) × leader.count, promote to that descendant (covers the case where the parent IS in the data and a single child concentrates most of its products). Also check the leader's ancestors — if the leader is <70% of its ancestor, fall back to the ancestor (preserves "prefer broader when no clear winner"). New `_is_strict_descendant(child, parent)` helper uses `parent_stem + '_'` to avoid the numeric-id-prefix false positive (`x_5745190` is NOT a descendant of `x_574519`). **Rule**: any dedup that picks "the parent" needs to handle the case where the parent isn't in the data — fall back to global count-leader.

- **V27 zeroes the reliability score whenever any long unmatched token (≥8 chars) exists.** Concrete case: `/r/zweefparasol_met_verrijdbare_voet/` matched 2 facets (Met voet + Zweefparasols) cleanly in the URL's own subcat, score 95/96. But `verrijdbare` (11 chars, an adjective modifier) didn't match anything, so V27 zeroed the score → search-derived rescue fired → picked Parasolvoeten + kleur=Zwart in a *different* subcat. **The matcher's anchored multi-facet result was more trustworthy than search-derived's different-subcat guess**, but the rescue path didn't know that. Added a V31 guard before the rescue: when `r.success && r.facet_count >= 1 && r.subcategory_id == parsed.subcategory_id` and search-derived's `dom_cat_url_slug` resolves to a different subcat, restore the score to tier-C (60) and keep the matcher's URL. Same fix simultaneously unblocked Q2 (Kussenboxen + Waterdicht). **Future work**: soften V27's long-unmatched rule for matches with high coverage; the in-subcat guard handles the dominant cases today.

- **`DOMINANCE_THRESHOLD = 0.60` in `search_derived.py` was too low.** At 60% the "dominant category" claim is noise — `/r/elektrische_sigaretten/` hit Kapperstassen (hairdresser bags) at 60% via incidental product-description text overlap. Bumped to 0.75. Below threshold the engine returns the rescue=None state and falls back to maincat page (tier D) — the safe fallback the user explicitly wanted ("no URL is better than a wrong guess"). **Rule**: any "X% of N dominantly in category Y" heuristic needs a threshold ≥0.70 to be trustworthy; 0.50–0.65 is the noise band.

- **Hyphen-as-separator in matcher normalization was the root cause of cross-category nonsense matches.** Both `parser._normalize_keyword` and `matcher._normalize` did `text.replace('-', ' ')`, so `tv-meubel` tokenized to `['tv', 'meubel']`. The `meubel` substring then fuzzy-matched (partial_ratio) `Kapstokmeubels` at score ~95 cross-maincat, and the redirect went to Coat racks instead of TV cabinets. **Fix**: keep hyphens in both `_normalize_keyword` and `_normalize` — they're publisher-intended compound boundaries (`tv-meubel`, `e-bike`, `TP-Link`, `A-DATA`, `Bébé-jou`). Both sides of the comparison normalize identically. Tested against 237 hyphenated URL slugs + 1181 hyphenated facet value names in the cache — no regressions, and `tv-meubel` now matches the cross-maincat `TV-meubels` category at score 99 via the per-token cross-cat fallback.

- **GENERIC_NOUNS — sibling of GENERIC_ADJECTIVES for cross-category jumps.** Tokens like `meubel`, `set`, `kast`, `huis`, `tafel` appear as substrings of dozens of category names. A keyword token that matches *only* via a generic noun isn't strong enough to justify a cross-category jump. Added `GENERIC_NOUNS` to `validation_rules.py` (12 entries), wired into `_v27_reject_reason` with a new `match_type` param: `cross_category_type` matches where every matched token is in `GENERIC_ADJECTIVES ∪ GENERIC_NOUNS` get hard-rejected. In-subcat matches on these tokens are still fine — the rule fires only when the match drags the user across categories. All call sites (`reliability_scorer.calculate_reliability_score`, `main_parallel_v2._evaluate_url_result_v2`, `process_global_rurls.process_global_url`) pass `match_type` through.

- **The Dutch compound suffix decomposer** in `synonyms.py`. Beslist keywords routinely glue compounds (`wasdroger` = `was` + `droger`, `badkamerkast`, `eetkamerstoel`, `tuinparasol`) but the facet values store the base noun (`droger`, `kast`, `stoel`, `parasol`). `expand_compounds` previously only knew the explicit `COMPOUND_DECOMPOSITIONS` map (~15 entries). New `_suffix_split(token)` helper: if the token isn't already in the map AND doesn't contain a hyphen (which is a publisher-intended boundary already), check if it ends with a known Dutch noun suffix (≥4 chars) and has a prefix ≥3 chars. Emit TWO variants per match — `'prefix suffix'` (full split) AND suffix-only (drops prefix). The suffix-only one matters: the token-coverage scorer drops sharply when extra prefix fragments appear; for `'combi wasmachine wasdroger'` the split form `'combi wasmachine was droger'` has 2/4 = 50% coverage against `'Wasmachine en droger kasten'` (below threshold), but the suffix-only form `'combi wasmachine droger'` has 2/3 = 67% and matches at ~85. Curated suffix list (24 entries) covers appliances, furniture, garden, bedding, lighting, kitchen. **The hyphen-skip is load-bearing**: without it, `tv-meubel` would split into `'tv-' + 'meubel'` (weird hyphen-trailing prefix) and the cross-cat per-word fallback would never get a chance to find TV-meubels.

- **V28 retry was breaking on the first match — even when that match was cross-maincat-blocked.** Compound decomposition produces multiple variants; V28 used to break out of the loop on the first variant whose match_with_partial against subcat (or match_multi_word against maincat) returned anything. For `/r/combi_wasmachine/wasdroger/` the first variant matched `type_droger=Condensdrogers` in a different maincat (`huishoudelijke_apparatuur`) via `all_type_facets`, triggering the cross-maincat block → tier D / redirect=None. The variant that would have matched the in-subcat `t_badkast=Wasmachine en droger kasten` (in Badkamerkasten under meubilair) was tried later but never reached. **Fix**: V28 now (1) tries `match_multi_word` against subcat facets FIRST per variant (the old code only used `match_with_partial`, which treats the variant as one phrase and misses cases where token-coverage 2/2 after stopword filter would match), and (2) drops cross-maincat hits in both subcat and maincat loops via `[mr for mr in (results or []) if not getattr(mr, 'cross_category_path', None)]`, letting the loop continue to a same-maincat variant. **Rule**: when retrying variants, never accept the first hit unconditionally — filter for same-maincat first, then break.

- **Product-type modifiers as stopwords (combi, combo, multi).** Token-coverage scorer treats `combi` in `'combi wasmachine wasdroger'` as a non-matched token, dragging coverage from 2/2 to 2/3 below threshold. `combi` is a Dutch product-type modifier (`combi-magnetron`, `combi-ketel`, `combi-koelkast`), not a primary product noun. Adding it to STOPWORDS lets the matcher ignore it for coverage purposes. **Safety check before adding**: queried facets.csv — only 7 facet values contain `combi` verbatim (`Combi-asbakken` in 1 cat, `Combi stoomoven` in 2 cats, `Combi` as `o_schaats` value in 4 sport cats). The `combi stoomoven` case still matches via the `stoomoven` token alone (verified). Same logic for `combo` (English variant) + `multi`. **Generalisable rule**: before adding a token to STOPWORDS, grep facets.csv for it — if it appears as a meaningful facet value in many categories, it's not a stopword.

- **`combi` in STOPWORDS also helped the search-derived path.** For `/r/beste_koop_consumentenbond/` the failure was symmetric: `consumentenbond` wasn't recognised as a marketing label, so the matcher fell through to search-derived which found 100% Polydaun-branded products (because Polydaun puts the badge in its product copy) and emitted `merk~Polydaun` spuriously. Added `consumentenbond` + `koop` to STOPWORDS — combined with `beste` (already there) all three tokens become stopwords → V27 short-circuit fires → redirects to clean Hoofdkussens category page. **Rule**: marketing/review badges (`consumentenbond`, `getest`, `winnaar`, `bestseller-like`) belong in STOPWORDS; they get inflated coverage from product-copy text matches.

- **Q4 V31 leftover-token merk-append was over-eager on generic adjectives.** `/r/mini_airco_voor_caravan/` correctly matched `type_airco=Caravan airco`, but then V31's facet-probe coverage check saw `mini` as an unmatched leftover token, queried "products with type=Caravan airco AND 'mini' in text", got 80% Evolar coverage (Evolar uses 'mini' in product titles), and appended `merk~Evolar`. `mini` is in `GENERIC_ADJECTIVES` — it shouldn't drive brand selection. **One-line fix**: the `keyword_words` loop in `main_parallel_v2.py` (around line 1202) now also filters `GENERIC_ADJECTIVES`. The Beslist catalog uses these adjectives as facet values (size, color, shape) — never as evidence of a brand. **Rule**: any "leftover token correlates with brand" coverage check needs to filter GENERIC_ADJECTIVES + GENERIC_NOUNS first.

- **V14 cross-category per-word fallback for non-generic tokens.** V30 had disabled the per-word fallback in cross-category subcat-name matching due to false positives — but those false positives were exactly the "generic noun matched a substring" cases that GENERIC_NOUNS now filters. Re-enabled with strict guards: only tokens ≥6 chars, not in STOPWORDS/SHOP_NAMES/GENERIC_ADJECTIVES/GENERIC_NOUNS, at score ≥95. Longest-first iteration. For `/r/tv-meubel_set/`, `tv-meubel` (9 chars, not generic) scores 99 against `TV-meubels` cross-maincat → tier B. Without GENERIC_NOUNS as the safety net, this couldn't be re-enabled.

- **Methodology gotcha — when the matcher returns NO match, that's still useful evidence.** `match_with_partial("wasmachine droger", fvs)` returns the target facet at score 90, but `match_with_partial("combi wasmachine droger", fvs)` returns nothing. The 'combi' noise token drags the partial_ratio below threshold. When debugging "why didn't the matcher find X?", always also test the matcher with stopwords/modifiers stripped from the keyword — if it then finds X, the issue is upstream noise filtering, not the facet pool.

- **Files**: `backend/rurl_optimizer_v2/main_parallel_v2.py` (+98 V31 guards/checks, V28 retry rewrite), `backend/rurl_optimizer_v2/src/facet_filter.py` (+CHILD_DOMINANCE_THRESHOLD class const, rewritten `_deduplicate_to_highest_level`, new `_is_strict_descendant`), `backend/rurl_optimizer_v2/src/matcher.py` (hyphen preservation, count-aware dedup), `backend/rurl_optimizer_v2/src/parser.py` (hyphen preservation), `backend/rurl_optimizer_v2/src/reliability_scorer.py` (`_v27_reject_reason` takes match_type, GENERIC_NOUNS rejection), `backend/rurl_optimizer_v2/src/search_derived.py` (DOMINANCE_THRESHOLD 0.60→0.75), `backend/rurl_optimizer_v2/src/synonyms.py` (DUTCH_COMPOUND_SUFFIXES, `_suffix_split`, extended `expand_compounds`), `backend/rurl_optimizer_v2/src/validation_rules.py` (`consumentenbond`/`koop`/`combi`/`combo`/`multi` stopwords + new GENERIC_NOUNS set), `backend/rurl_optimizer_v2/process_global_rurls.py` (one-line `match_type` passthrough). 9 files, +426 -41 lines, commit `b68e3d5` rebased to `f589d8b`.

## Redirect API behavior — redirect.api.beslist.nl (2026-05-20)
- **API surface**: 4 endpoints — `GET /api/redirect?searchterm=<url>&country=<nl|be>` (resolver, single-URL lookup), `POST /api/redirect` (body is a JSON **array** of `{fromUrl, toUrl, country, statusCode?}`, accepts batch but not transactional), `DELETE /api/redirect?fromUrl=<url>` (single-row delete, returns 404 if not found — safe to call defensively), `GET /api/redirects?limit=N&offset=M[&urlContains=...]` (paging is required — no default). Spec at `https://redirect.api.beslist.nl/swagger.json`. Contact: teamsearch@beslist.nl. Backend is PHP 8.4 + MySQL, fronted by Varnish.

- **The `url_redirect.url_UNIQUE` constraint is stricter than "no chains".** Empirically tested 2026-05-19. Two distinct failure modes:
  - **Chain rejection**: POST `b→c` while `a→b` exists returns `500 "Duplicate entry '/b/' for key 'url_redirect.url_UNIQUE'"`. So a URL can't be both `fromUrl` and `toUrl` at the same time. In 91 random sampled rows, **zero** chains existed — the schema enforces this strictly.
  - **Existing-target rejection**: POSTing a brand-new `fromUrl` toward a `toUrl` that's already in the table (as a target of other rows) can ALSO fail with `Duplicate entry '/computers/'` even though `/computers/` is a target of dozens of existing rows. The exact storage rule isn't clear from outside, but the pragmatic upshot is: **chain-flattening on insert (rewriting `new=X` to its terminal target when `X` is a fromUrl) catches one class of failure, NOT all.** Treat per-row failures as expected; surface the API error message to the user.

- **There is no PUT endpoint** — mutations are delete-then-recreate. To redirect d→e when `a→d`, `b→d`, `c→d` already exist, the only path is: DELETE each existing row (`a`, `b`, `c`), then POST them again with `toUrl=e`, then POST `d→e` if you also want direct-visit forwarding.

- **`country` is stored as a literal CSV string in one column.** **500/500 sampled rows** across 10 different offsets have `country='nl, be'` — every existing redirect applies to both countries. So `?country=nl` (exact filter) matches almost nothing useful; use `urlContains=` for filtering instead. The resolver endpoint accepts `country=nl` or `country=be` and matches against the CSV substring — they're effectively equivalent.

- **Resolver responses are Varnish-cached for 1h (`cache-control: max-age=3600`).** Newly POSTed redirects don't show up in `GET /api/redirect?searchterm=...` immediately — but they DO show up immediately in `GET /api/redirects` (list endpoint is not cached). The Redirect Tool's Recent Results relies on the list endpoint being uncached for verification.

- **The writer node sometimes returns 500 `MySQL --read-only`.** Whatever DNS the public hostname `redirect.api.beslist.nl` points at can be a read replica during failover or maintenance. If POST returns `Database error: SQLSTATE[HY000]: General error: 1290 The MySQL server is running with the --read-only option`, the route is on the replica — retry later or ask teamsearch for the writer endpoint. Surfaced once during the 2026-05-19 Redirect Tool build (the tool now handles this gracefully by reporting per-row failures).

- **`toUrl` is NOT unique** — many rows can share the same target (e.g. dozens of rows have `toUrl=/computers/`). So the constraint is "a URL can't be both sides at once" combined with whatever the existing-target rejection rule actually is. Don't assume `toUrl` uniqueness when designing batch logic.

## R-URL optimizer: hyphenated maincats, leftover-token facet matching, build_multi_facet sibling-dropping (2026-05-13)
- **Five connected fixes shipped together** in `backend/rurl_optimizer_v2/`. Commits `495a280` (rurl-optimizer) + `56ff81f` (url-checker copy fix), both pushed to dm-dashboard. Backend has `--reload` so all changes were live without manual restart.

- **(1) Hyphenated maincats lost their `/products/{maincat}/` segment.** `matcher.py:1178` and `process_global_rurls.py:435` extracted the maincat via `re.match(r'^([a-z_]+?)_\d+', url_name)`. The character class `[a-z_]` excludes `-`, so for `sport_outdoor_vrije-tijd_484428` the regex failed entirely → fell back to `category_path = f"/products/{url_name}"`. Result: `/products/sport_outdoor_vrije-tijd_484428/` (no maincat segment), which 404s on the live site. Affected ~240 subcats: 224 `sport_outdoor_vrije-tijd` + 13 `films-series` + 3 `boeken-*`. **Fix**: replaced both regexes with the split-on-`_`-until-digit logic that `parser.py` already uses (`_extract_main_category_from_subcategory_name`). Reads more like prose, doesn't trip on hyphens. **Rule for future code**: when extracting a structural prefix from a Beslist URL slug, split on `_` and iterate until you hit a numeric segment — don't use a regex character class.

- **(2) Defensive maincat-path validator at the end of `process_url_v2`.** Even with the root fix, any OTHER code path that constructs a `/products/...` URL could still emit a malformed one. Added a final validator: parse the redirect URL; if the second path segment (after `/products/`) contains a numeric id token (the malformed pattern), try to infer the maincat from the segment's underscore-prefix and **repair in place**, appending `; repaired missing maincat segment 'X/'` to the reason. If no maincat can be inferred, suppress the redirect (`final_redirect_url=None`, `match_type='malformed_redirect'`) and flag the row for review. Lives in `main_parallel_v2.py` right before the final return dict.

- **(3) Strict-exact merk match from leftover tokens** (`main_parallel_v2.py:_append_facet_to_subcat_redirect`). The previous code had an explicit `if fmatch.facet_value.facet_name.lower() == 'merk': return result` guard with the comment "merk facets from leftover tokens are too risky — a stray brand word next to a category name shouldn't deep-link into a brand page." This was over-conservative: for genuine brand-then-category searches like `bic_aanstekers_50_st`, after the subcat-name match consumed "aanstekers" the leftover "bic" was an exact match for `merk~BIC` but got suppressed. **Fix**: added a per-token merk pass after the existing non-strict match, requiring `score >= STRICT_FACET_EXACT_THRESHOLD` (=100). Mirrors the strict-exact rule that `match_multi_word`'s fourth pass uses elsewhere. Single-token exact match keeps false-positive risk in check.

- **(4) Specificity rescue for V14.1 winners** (new `_maybe_promote_to_specific_subcat` helper in `main_parallel_v2.py`, called from steps 2b and 3). The V14.1 per-word subcat-name matching breaks on the first ≥95 hit — for `gereedschap_trolley` it picks "Gereedschap" (`klussen_486173`, exact 100) and never considers deeper siblings. But `Gereedschap` has 1114 facet values, none trolley-related; the user's "trolley" leftover dies. **Fix**: after the V14.1 winner is picked, check whether its facets can absorb any leftover token. If not, scan deeper same-maincat siblings whose first display word shares a 4+ char prefix with the matched word. If a deeper sibling's facets DO absorb a leftover token, swap to it. For `gereedschap trolley`, this promotes to `Gereedschapskoffers` (`klussen_486172_1348201`) which carries the `soort_gereedschapskoffers~Gereedschapstrolley` facet value. **Subtle bit**: the matcher's standard `MIN_LENGTH_RATIO=0.4` guard blocks `"trolley"` (7 chars) from matching `"Gereedschapstrolley"` (19 chars) — ratio 0.37 trips the guard. The rescue's facet-hit check uses a Dutch-compound-suffix fallback (token endswith leftover with ≥3 char prefix) to bypass the length-ratio guard *only* in this context where the subcat-stem prefix already establishes semantic similarity. The same compound-suffix fallback lives inside `_append_facet_to_subcat_redirect` so the actual facet attachment succeeds too.

- **(5) Multi-axis longest-per-axis leftover collector.** Replaced the legacy `joined → compound-suffix → per-token-first-hit` chain in `_append_facet_to_subcat_redirect` with a unified scan: for each non-strict facet axis (excluding winkel + merk), pick the facet value whose tokens are all covered by the leftover (modulo Dutch morphology OR compound-suffix), preferring the **longest facet value name** on collision (`"Nike Air"` beats `"Nike"`). Then merge the joined match in as a typo/phrase safety net, longer value winning. Result: multi-attribute leftovers like `rood_jurken_dames` attach one facet per axis (`/c/doelgroep_mode~Dames~~kleur~Rood`), and short leftover tokens like `"dames"` in `pescara_jeans_dames` now match `doelgroep_mode~Dames` even when the joined matcher rejected them at `MIN_LENGTH_RATIO` (5/13 = 0.38 < 0.4 — token-coverage scored 75, below the 80 fuzzy threshold). Output sorted by facet value length descending (most specific first), merk pass appended last. New helpers: `_leftover_token_matches_facet_token`, `_collect_longest_per_axis_from_leftover`.

- **(6) `build_multi_facet` "for simplicity, just use the primary facet" silently dropped sibling matches** (`src/url_builder.py:486-557`). When `match_multi_word` returned multiple facet matches whose URLs all pointed to the same target subcat (e.g. `nike_schoenen_dames` → both `doelgroep_mode~Dames` AND `merk~Nike` live in `mode_432362`), the "facets-from-different-category" branch kept ONLY `facets_from_different_category[0]` and dropped the rest. Hardcoded `facet_count=1` in the return. The comment literally said "For simplicity, just use the primary facet to ensure validity." **Fix**: collect every match whose `facet_value.url` resolves (via `_extract_category_path_from_facet_url`) to the same `category_path` as the primary, dedupe by facet axis (Beslist allows one value per axis — higher score wins), sort alphabetically by facet name for stable URLs (mirrors the same-category branch at `:573`), `~~`-join into the final fragment. Cross-subcat matches still skipped — gluing facets from different subcats produces an invalid Beslist filter URL. The existing V16 (merk_missing) and V26 (cross-maincat blocked) guards above this branch still fire first. `nike_schoenen_dames` now correctly emits `/c/doelgroep_mode~Dames~~merk~Nike`.

- **Two-step matching architecture clarified during this session** (helpful for future debugging):
  - **Step 2b/3 path** (`_append_facet_to_subcat_redirect`): subcat picked by name match → leftover tokens get matched against the picked subcat's own facet pool → fragments appended. This is where the merk-leftover, compound-suffix, specificity-rescue, and multi-axis-longest-per-axis logic lives.
  - **Step 4 path** (`build_multi_facet`): keyword matched directly against the whole maincat's facet pool via `match_multi_word` → returns one match per facet axis → builder picks the target subcat from the primary match's `facet_value.url`. This is where the "sibling matches dropped" bug lived.
  - **Either path can produce the same kind of multi-facet URL**, but they get to it differently and have different bug surfaces. When debugging a missing facet, first check which path fired by looking at the `reason` prefix: `[child_subcat]` / `[subcat_name_high]` → step 2b/3, `[maincat]` → step 4, `[V28]` → search-derived rescue.

- **Token-coverage scorer subtlety**: `match_by_token_coverage` (`src/matcher.py:361+`) returns `score = 50*coverage + 30*specificity + 20*adjacency`. For `"pescara dames"` leftover with facet `"Dames"`: coverage=0.5 (1 of 2 kw tokens), specificity=1.0, adjacency=1.0 → score=75. `match_with_partial` checks `tc.is_match` which requires `score >= config.FUZZY_THRESHOLD=80`. 75 < 80 → falls through to legacy partial_ratio, which then trips `_is_valid_fuzzy_match`'s length ratio. The new `_collect_longest_per_axis_from_leftover` bypasses the threshold by checking the all-tokens-covered constraint directly — context (subcat-name match has already won) compensates for the missing fuzzy threshold.

- **URL Checker "Copy for Excel" silently produced unusable output** (`frontend/url-checker.html:475`). Two bugs: (a) embedded `\t`/`\r`/`\n` in scraped `meta_title`/`meta_description`/`h1` broke the TSV mid-row when Excel parsed it (row 2 started inside row 1's last cell); (b) `navigator.clipboard.writeText` had no `.catch()` so any rejection (focus loss, permissions, oversize string) disappeared silently — user saw "nothing copied" with no error in console. **Fix**: per-cell sanitize replacing whitespace runs with single space + add `.catch()` that logs the DOMException and surfaces the message in the alert. Matches the pattern in `redirect-checker.html`/`canonical.html` which already had `.catch()`. **Rule**: any frontend tool that copies user-visible scraped text via `clipboard.writeText` MUST sanitize control chars in TSV cells AND have a `.catch()`. The working tools' pattern is the template.

- **Files touched**: `backend/rurl_optimizer_v2/main_parallel_v2.py` (~340 net lines: rescue helper, multi-axis collector, leftover refactor, append fallback chain, maincat validator), `backend/rurl_optimizer_v2/process_global_rurls.py` (regex → split-until-digit), `backend/rurl_optimizer_v2/src/matcher.py` (regex → split-until-digit), `backend/rurl_optimizer_v2/src/url_builder.py` (multi-facet attaches all same-target matches), `frontend/url-checker.html` (Copy for Excel sanitize + .catch()).

## FAQ/Kopteksten dashboard counts: buckets must partition the JOBS table (2026-05-11)
- **The bug**: `/api/status` and `/api/faq/status` reported numbers that didn't sum to Total. For FAQ: Total 390,052, Processed 249,004, Skipped 152,311, Failed 22,292, Pending 1,681 — sum 425,288 (off by +35k). Root cause: each bucket queried a DIFFERENT table. Total = `pa.faq_jobs` row count; Processed = `pa.faq_content_v2` row count; **Skipped = `pa.url_validation WHERE is_valid=FALSE`** (URL-level, not faq_jobs-scoped); Failed/Pending = `pa.faq_jobs` filters. The URL-validation skipped set is NOT a subset of faq_jobs (some invalidated URLs never had jobs created). ~117k pending faq_jobs whose URL is invalid were silently absent from any bucket.
- **The fix**: redefine each bucket to be a subset of `pa.faq_jobs` (and same for `pa.kopteksten_jobs`):
  - `Processed = COUNT(*) WHERE status='success'` (was: content table row count, which can drift from jobs.success by 76 / 1,334 rows due to historical imports)
  - `Skipped = COUNT(*) WHERE status='pending' AND url IS invalid in pa.url_validation` (was: the global URL-level count)
  - `Failed`, `Pending` unchanged
  - Now Processed + Skipped + Failed + Pending = Total exactly. Files: `backend/main.py:589-604` (Kopteksten), `backend/main.py:1692-1707` (FAQ).
- **Effect on the dashboards** (one-time visible change): per-tool Skipped drops from the shared 152,311 to its job-scoped subset (117,351 FAQ / 133,781 Kopteksten); per-tool Processed drops from content-table count to jobs-success count (-76 FAQ / -1,334 Kopteksten). Total stays the same.
- **The two Skipped concepts are different but live in different tables**:
  - URL-level skipped: `pa.url_validation WHERE is_valid=FALSE` — shared across all tools, source of truth for "this URL has no products / not reachable"
  - Per-tool skipped: pending jobs whose URL is in the URL-level skipped set. **This is what each tool's dashboard should show.** A URL can be in url_validation invalid but have no job row at all (~12k in this state pre-cleanup) — those don't count as Skipped for any specific tool.
- **One-time DB cleanups** during the same session (all `pa.{faq,kopteksten}_jobs` total grew from 390,052/390,062 → 402,339/402,349):
  - **Inserted 12,287 pending rows** in both `pa.faq_jobs` and `pa.kopteksten_jobs` for URLs that had `is_valid=FALSE` in `pa.url_validation` but no job row. These URLs were invisible to the dashboard. Now they show as Skipped.
  - **Deleted 22,602 rows from `pa.faq_content_v2`** and **6,160 rows from `pa.kopteksten_content`** whose URL is currently invalid (we had live FAQ/kopteksten content on URLs with no products). **Reset the corresponding 22,544 FAQ + 6,123 Kopteksten jobs from `success` → `pending`** so they're now counted as Skipped instead of Processed. When the URL becomes valid again, the existing `recheck-skipped` flow at `main.py:1497-1640` already does the right thing: DELETE from url_validation + UPDATE both jobs to pending + INSERT job rows if missing. Content gets regenerated by the normal worker.
  - **Reset all failed jobs to pending** (22,294 FAQ + 14,261 Kopteksten) for retry. The bulk of Kopteksten failures (~59%) were OpenAI 429 rate limits — retryable.
- **Defense in depth: filter at the publish layer too.** Added `LEFT JOIN pa.url_validation v ON v.url_id=u.url_id WHERE (k.content IS NOT NULL OR f.faq_json IS NOT NULL) AND (v.is_valid IS NULL OR v.is_valid = TRUE)` to **all four** publish queries in `backend/content_publisher.py` (`get_all_content` @147, `get_content_batch` @197, `get_total_content_count` @252, `get_all_content_items` @310). Going forward even if content gets regenerated for a URL that's currently invalid (race condition, manual import), it can't leak to production. The publish-side filter is the source-of-truth gate; the delete-side cleanup is a one-time consistency pass.
- **Bigbang migration didn't fix this on its own** — the 2026-05-07 schema cutover restructured the tables but didn't touch the read-side aggregation queries in `/api/status` and `/api/faq/status`. The bug pre-existed and persisted into the new schema because the bucket definitions were unchanged. Lesson: when refactoring storage, also audit any read-side queries that compose multi-table counts — the partition invariant doesn't survive a schema move automatically.
- **FAQ pipeline now writes `pa.faq_jobs.last_error`** for forensic visibility:
  - `fetch_products_api` (faq_service.py:399+): every error return path now carries `error_detail` (HTTP code, exception type+message, invalid-facet context/value). Previously the `return None` paths were opaque — `process_single_url_faq` only saw "API failed" with no underlying detail.
  - `generate_faqs_for_page` (faq_service.py:555+): return signature changed from `Optional[FAQPage]` to `(Optional[FAQPage], Optional[str])`. The second element is the captured `JSONDecodeError` / general exception message. Only one in-repo caller (faq_service.py:761); the `batch_api_service.py:25` import is unused.
  - `process_single_url_faq` (faq_service.py:729+): sets `result["error_detail"]` for every failure path including the wrapping `except Exception`.
  - Persistence: `main.py:1840-1901` (realtime FAQ batch) and `batch_api_service.py` two FAQ-jobs INSERT sites (lines ~376 and ~518): INSERT now writes `last_error`, falling back to `reason` when no separate detail was captured so the column is never NULL for failed rows. Going forward every new `failed` faq_job will have populated `last_error` instead of NULL. Pre-existing 22,294 failed rows had `skip_reason` only and no detail — those got reset to pending in the cleanup, so any new failure will populate both fields.
- **Kopteksten failures already track `last_error`** (no code change needed). Breakdown was: 59% OpenAI 429 rate limits, 20% generic `api_failed`, 11% `no_valid_links` (structural, not retryable), 8% duplicate-key violations (race condition where content was inserted by a concurrent process but the job stayed marked failed — those URLs probably had content already, could be promoted to success by checking for matching `kopteksten_content` rows; not done this session, just reset all to pending).
- **Frontend polish**: Recent Results URLs in Kopteksten (`frontend/js/app.js:553`) and FAQ (`frontend/js/faq.js:220`) are now clickable — wrapped in `<a href="https://www.beslist.nl${item.url}" target="_blank" rel="noopener" class="text-decoration-none">`. Matches the existing Unique Titles pattern at `frontend/unique-titles.html:653`. Two-character grep target if you need to find similar URL-display sites in other tools.
- **Sticky-thead inside `overflow:auto` container can break when the container doesn't actually scroll** (separate gotcha from this session): `frontend/url-checker.html` had `.results-table { max-height: 600px; overflow: auto }` with `<thead class="table-light sticky-top">`. With only a few result rows the container never establishes a scroll context, so `position: sticky` falls back to the page scroll context — the header rendered in the middle of the table rows instead of at the top. Three attempted CSS fixes (background-color, !important box-shadow override, CSS variables) all failed because the issue wasn't transparency — it was the sticky positioning context. Final fix: just dropped `sticky-top` from the thead. Can be re-added properly behind a "if rows > N" toggle later if needed. Commit `a39483f`.
- **Files**: `backend/main.py` (both status endpoints), `backend/content_publisher.py` (4 publish queries), `backend/faq_service.py` (error capture), `backend/batch_api_service.py` (FAQ-jobs INSERTs ×2), `frontend/js/app.js`, `frontend/js/faq.js`, `frontend/url-checker.html`. Commits: `a39483f` (url-checker), `583cef7` (dashboard count fix + last_error + clickable URLs).

## Per-main-category SEO-priority analysis — reusable script (2026-05-13)
- **Goal**: data-driven keep_on / turn_off / turn_on / review recommendations per (category, facet) inside one main category, with judgment-based thresholds (no fixed %). Output is xlsx; **never** touches the API. First run was for Horloges (id 30000) at the user's request.
- **Script**: `cc1/seo_prio_main_cat.py`. Run as `python3 cc1/seo_prio_main_cat.py <main_cat_taxv2_id> "<main_cat_name>"`. Self-contained, loads creds from `dm-tools/.env`. Examples: `30000 "Horloges"`, `32000 "Schoenen"`, `700 "Films & Series"`. Get the top-level taxv2 IDs via `GET {TAXV2}/api/Categories?locale=nl-NL` — there are 32 of them.
- **Pipeline mirrors the dashboard's SEO Priority tool** (`dm-tools/backend/seo_prio_service.py`) but adds:
  1. Single main-category scoping via `dv.main_cat_name = <name>` in the Redshift query (cuts work massively).
  2. **Legacy_id → taxv2_id mapping** (this is the load-bearing bit — see gotcha below).
  3. Inheritance-aware "currently true" detection via `GET /api/Categories/{id}?includeFacets=true` (the `seoPriority` field on each linked facet is already resolved across the inheritance chain — Direct/Inherited/Dependent).
  4. Judgment-based `judge()` instead of fixed 10%/2% thresholds.
- **CRITICAL gotcha — two ID spaces**: taxv2 uses small new IDs (Horloges=30000, Smartwatches=9004665) but live URLs still embed legacy IDs (`/products/horloge/horloge_649387/c/...` — 649387 is the legacy id for Smartwatches). My first run silently joined zero rows and flagged 100% of currently-true combos as `turn_off` because every URL got dropped at the URL→cat lookup. The fix: each category's nl-NL `urlSlug` carries its legacy id as a trailing `_<digits>` suffix, so a one-time `legacy_to_v2 = {legacy: v2_id}` lookup built from the sub-tree walk is enough to join. Root-level URLs like `/products/horloge/c/...` have no subcat segment — fallback is `slug_to_v2[root_slug]` (e.g. `horloge` → 30000). Symptom that exposed the bug: distribution `turn_off=127, keep_on=0` — if you see that on a rerun, the ID mapping is broken again.
- **judge() anchors** (encoded in the script's reason strings; tweak in-place if a category needs different cutoffs — Beslist trade-off is crawl-budget vs ranking surface, so leave headroom for tail facets):
  - `near-zero` = <50 visits AND <€1 over 2y → `turn_off` high
  - `tiny`      = <0.3% visits AND <0.3% revenue AND <500 visits AND <€20 → `turn_off` medium
  - `material`  = ≥1% visits OR ≥1% revenue OR ≥1000 visits OR ≥€100 → `keep_on` high
  - Currently-off + ≥500 visits + material → `turn_on` high; ≥200 visits + ≥0.5% share → `turn_on` medium
  - Everything in between currently-on → `review` (manual call)
- **Horloges results 2026-05-12** (130 rows: 34 turn_off / 10 review / 83 keep_on / 3 turn_on). xlsx at `~/Downloads/claude/horloges_seo_prio.xlsx`. Reviewed manually, not pushed to taxv2.
- **Related write helpers** documented elsewhere in this session: bulk `seoPriority=false` flips via `PUT /api/CategoryFacetSettings` (upsert, not partial-PUT — GET-merge-PUT to preserve `displayOrder`/others) with `X-User-Name: SEO_JOEP`. The Horloges run was read-only.
- **Files**: `cc1/seo_prio_main_cat.py` (new, ~280 lines, parameterized). One-off Horloges variant lived at `/tmp/horloges_seo_prio.py` during development.

## Unique-titles v3 thaw-and-update pass — still in fridge (2026-05-08)
- **Status**: opt-in via `AI_TITLES_PIPELINE=v3`. Default remains `v1`. Pulled out of the fridge for an iteration, pushed back with several regressions addressed but three new ones discovered. Commit: `84e410c`. See the previous shelving section ("Unique-titles v3 pipeline experiment — shelved at ~76% acceptable") for the original A/B journey.
- **What changed in this pass** (all in `dm-tools/backend/ai_titles_service.py`):
  - **Category-override** lifted from v1 (`batch_classify_facets` + `_NEVER_URL_SLUGS` / `_ALWAYS_TYPE_URL_SLUGS`). Computed inside `generate_title_v3` BEFORE calling `_build_v3_h1`; passes `effective_category=''` when override fires. Required relaxing the `_build_v3_h1` early-return: `if not category_name and not selected_facets` instead of `if not category_name`. **Fixes the `Wanten Handschoenen` / `Ventilatieventielen Ventilatiematerialen` / `Kandelaars Kaarsenhouders` redundancy class** flagged at original shelving — verified on 15 type-facet URLs.
  - **`generate_title_v3(polish=False)` codepath** added. Skips OpenAI entirely; deterministic `composed_h1` is the final output. A/B (500 random URLs) showed polish only changed output in 12-17% of cases — most polish responses are identical to composed after `_v3_restore_casing` strips polish-applied case changes. **User signal 2026-05-08**: "looks fine without polishing" → no-polish is the favored path now. The polish-on regression class (non-brand agglutinations like `damedeodorant`, `herenspolshorloges`) doesn't apply on the no-polish path — it resurfaces if polish is re-enabled.
  - **Conditie facet → end of H1**: new `conditions: List[str]` bucket detected via `fname=='conditie' or 'conditie' in url_slug or 'condition' in url_slug`. Slot order: ... → sizes → conditions (last). Verified on 8 URLs: `Apple iPad 2019 Tablets 10 inch Nieuw`.
  - **Standalone `Met`/`Zonder` lowercased mid-title**: final-pass regex `(?<=\S)\s+(Met|Zonder)\b` runs after dedup. Brings v3 in line with v1's polish rule 3 ("non-eigennamen NÁ het eerste woord in kleine letters") even when polish is off.
  - **Color precedence** (kleurtint / kleurcombi over generic kleur): new `kleurtint: List[str]` bucket. Generalized kleurcombi match from `url_slug.startswith('kleurcombi')` to `'kleur' in slug AND 'combi' in slug` (plus `fname` check) so `kleur_combinatie`, `kleurcombinaties_schoenen`, `kleurcombinaties_woonacc` all hit. After loop: `if kleurtint or color_combos: colors = []`. Front color slot uses `kleurtint or colors` (specific overrides generic); kleurcombi keeps post-category position. Effect: `Wit en groen Textiel Adidas Court Sneakers Maat 40` → `Adidas Court Textiel Sneakers Wit/groen Maat 40`.
- **Final slot order** (Option A from user 2026-05-08): `front_colors` (kleurtint OR generic colors) → brand → populaire_serie → type_productlijn → productlijn → materials → other_adj → doelgroep → CATEGORY → met_clauses → voor_values → color_combos → sizes → conditions.
- **A/B numbers** at end of session (500 URLs, polish=False): v1 differs from v3 in 340/496 (~69%, down from 73% pre-color-precedence). xlsx at `~/v1_vs_v3_500_2026-05-08.xlsx`.
- **New regressions surfaced during scoring** (also in EXPERIMENTAL header in source — keep header + LEARNINGS in sync):
  1. **Brand acronym lowercasing in builder** — `HEMA Uitnodigingen` → `Hema Uitnodigingen`. Beslist's facet detail_value capitalizes brands by default; something in the dedup/casing path is title-casing the all-caps form. Reproduces on every HEMA URL. Investigate `_dedupe_facet_values` first; the final `if h1[0].islower()` block shouldn't touch interior tokens, so the culprit is one of the dedup passes normalising casing.
  2. **Brand mangling on `&`** — `Heckett & Lane` brand produces `Bruine & Lane Stoffen Moderne Dekbedovertrekken …`. The brand string with embedded `&` interacts badly with one of the dedup passes, dropping the first token. Reproducible on Heckett & Lane URLs. Possibly `_dedupe_prefix_overlap` or `_strip_pre_clause_duplicates` treating `&` as a clause boundary.
  3. **Attributive vs predicate inflection on measurements** — `20 cm diep 73 cm hoog` → `20 cm diepe 73 cm hoge`. Persists with polish off, so it's the builder, not the AI. Would need a measurement-noun proximity rule (don't inflect adjective when a measurement immediately precedes).
- **What to do when picking this up again**:
  - **A/B harness** is committed at `dm-tools/scripts/v3_ab_100.py`. Run as `python3 scripts/v3_ab_100.py 500`. Outputs `~/v1_vs_v3_<N>_<date>.xlsx` with columns `url, v1_h1, v3_h1, error, verdict`. Currently calls `polish=False`; if revisiting polish, swap the `run_one` call to also call `polish=True` and add columns.
  - **Targeted spot-check** for type-facet override at `scripts/v3_verify_override.py`. Run after any change to category-override logic.
  - The 3 new regressions above are independent and triagable in any order. (1) and (2) feel like one investigation: trace what mutates `brand` after it's appended to parts. (3) is its own thing — the lowercase regex that runs late doesn't seem to be the cause.
  - The original 2026-05-06 pickup notes mention "consider dropping the AI polish step entirely" — that has effectively happened on the recommended path (polish=False is now favored). The polish=True codepath is still there for A/B but is no longer the target.
- **Files**: `dm-tools/backend/ai_titles_service.py` (header rewritten + ~80 net new lines under EXPERIMENTAL header), `dm-tools/scripts/v3_ab_100.py` (new), `dm-tools/scripts/v3_verify_override.py` (new). Commit `84e410c` pushed to dm-dashboard.

## Big Bang DB refactor: collapsed per-tool tables into one URL catalog (2026-05-07)
**TL;DR for future-you debugging anything table-related**: the SEO content tools (Kopteksten, FAQ, Unique Titles) used to each have their own URL-keyed tables. As of 2026-05-07 they share `pa.urls` (single canonicalized URL catalog, ~980k rows) plus per-tool `*_jobs` / `*_content` tables keyed on `url_id` (BIGSERIAL FK). If a query mentions an old table name, it's stale code. Old tables still exist as `*_old_2026_05_07` snapshots until ~2026-05-14, then get dropped.

**Old → new table mapping** (this is the lookup table when something breaks):
- `pa.jvs_seo_werkvoorraad` → gone. The "URL universe" concept is now `pa.urls`. Per-tool eligibility = "row exists in pa.kopteksten_jobs / pa.faq_jobs / pa.unique_titles_jobs" (see eligibility-backfill note below)
- `pa.jvs_seo_werkvoorraad_kopteksten_check` → `pa.kopteksten_jobs(url_id, status, last_error, attempts, created_at, updated_at)`
- `pa.content_urls_joep` → `pa.kopteksten_content(url_id, content, created_at, updated_at)`
- `pa.faq_tracking` → `pa.faq_jobs(url_id, status, skip_reason, last_error, attempts, created_at, updated_at)`
- `pa.faq_content` → `pa.faq_content_v2(url_id, page_title, faq_json TEXT, schema_org TEXT, created_at, updated_at)` — the `_v2` suffix is temporary; rename to `pa.faq_content` after step 5 drops the old one. faq_json stored as TEXT (not JSONB) because some legacy rows have literal newlines that break strict JSONB parsing
- `pa.unique_titles` (the wide table) → split across:
  - `pa.unique_titles_jobs(url_id, status, last_error, http_status, final_url, last_checked_at, attempts, created_at, updated_at)` — the URL-probe columns (status_code/final_url/checked_at) live here, not on a separate table
  - `pa.unique_titles_content(url_id, h1_title, title, description, original_h1, title_score, title_score_issue, created_at, updated_at)`
- `pa.url_validation_tracking` → `pa.url_validation(url_id, last_checked_at, http_status, is_valid, reason)` — `is_valid=FALSE` means "skipped" (URL has no products / not reachable)
- `pa.link_validation_results` (kopteksten broken-link results) → `pa.kopteksten_link_validation(url_id, total_links, valid_links, broken_links, broken_link_details JSONB, validated_at)`
- `pa.faq_validation_results` (FAQ broken-link results) → `pa.faq_link_validation(url_id, total_links, valid_links, gone_links, validated_at)`
- `pa.content_history` — UNCHANGED, still keyed on URL string (append-only audit log; not joined cross-tool)
- `pa.publish_log` — UNCHANGED (no URL column)
- `pa.jvs_seo_werkvoorraad_shopping_season` — UNCHANGED (lives in Redshift, separate concern)

**Where the new code lives** (when you're debugging "why is X broken"):
- `dm-tools/backend/url_catalog.py` — the URL→url_id helper. THREE functions: `canonicalize_url(s)` (Python implementation of the same rules as the SQL `pa.canonicalize_url()` function), `get_url_id(cur, url, *, create=True)` (single-URL upsert+lookup), `bulk_upsert_urls(cur, urls)` (returns `{canonical_url: url_id}` for batch ops). Use these everywhere — never write raw URL strings to the new tables, the FK only takes url_id
- `pa.canonicalize_url(text)` — PL/pgSQL function that lives in the DB. Same rules as the Python helper. Used in WHERE clauses (e.g. lookup endpoints accept "user-supplied URL" and call canonicalize_url to find the catalog row)
- Migration files: `dm-tools/migrations/2026-05-07-bigbang-step{1,2,3a,3a-fix,3b,3c,4}-*.{sql,md}` — read these in order to understand the migration's full story. step 1 = create new tables; step 2 = backfill; step 3a/3b = code refactor docs; step 3c = perf indexes + ANALYZE; step 4 = rename old tables
- Touched code (every file that now uses the new schema):
  - `backend/main.py` — all FAQ + Kopteksten endpoints (~100 references migrated)
  - `backend/unique_titles.py` — full rewrite of the DAO
  - `backend/ai_titles_service.py` — DB-touching functions (init_ai_titles_columns is now a no-op; get_unprocessed_urls / update_title_record / get_ai_titles_stats / get_recent_results / analyze_and_flag_failures all switched)
  - `backend/content_publisher.py` — the `FULL OUTER JOIN content_urls_joep + faq_content` queries became `LEFT JOIN`-from-`pa.urls` over the new content tables
  - `backend/link_validator.py` — `update_content_in_redshift`, `add_urls_to_werkvoorraad`, `reset_faq_to_pending`
  - `backend/batch_api_service.py` — FAQ + Kopteksten batch worker writes
  - `backend/import_content.py`, `import_missing_content.py`, `find_bad_urls.py`, `check_unique_titles_urls.py`, `compare_prompts.py`, `sync_werkvoorraad.py`, `sync_redshift_flags.py`, `scripts/score_titles.py`, `scripts/export_scored_titles.py` — admin scripts migrated
  - `backend/database.py::init_db()` — old per-tool CREATE TABLE blocks removed; only `pa.content_history` and the thema_ads tables stay
  - Stubs for one-shot scripts that have already run and now refer to gone tables: `backend/migrate_shared_validation.py`, `backend/deduplicate_content.py`, `backend/fix_faq_*.py`, `scripts/csv_utils/import_content.py`, `scripts/fix_faq_item_names.py` — these print an "OBSOLETE" message instead of erroring

**Eligibility-backfill subtlety** — if you're debugging "why isn't my URL being picked up by the FAQ/Kopteksten worker":
- Old model: `pa.jvs_seo_werkvoorraad` (390k rows) was the universe of URLs eligible for BOTH tools; per-tool tracking was sparser (only URLs that had been processed)
- New model: per-tool eligibility = "row exists in `pa.kopteksten_jobs` / `pa.faq_jobs`". So adding a URL to one tool's queue doesn't add it to the other's
- During step 2, I ran a one-shot eligibility backfill: for every werkvoorraad URL not already in faq_jobs/kopteksten_jobs, insert a `status='pending'` row. Result: both job tables have exactly 390,022 rows — the canonical werkvoorraad universe preserved
- Going forward, `link_validator.add_urls_to_werkvoorraad(urls)` writes ONLY to `pa.kopteksten_jobs` (was previously the implicit "shared" eligibility marker via werkvoorraad). If FAQ should also pick up those URLs, the caller has to explicitly INSERT into `pa.faq_jobs` too

**FAQ pending=0 is correct, not a bug** — the dashboard shows pending=0 for FAQ even though there are 117k status='pending' jobs. Reason: those 117k URLs all have an `is_valid=FALSE` row in `pa.url_validation` (they were marked skipped in past `no_products_found` runs). The new query correctly excludes validation-skipped URLs from the work queue, and the OLD query had the same semantics (excluded URLs in `url_validation_tracking`). Don't "fix" this thinking it's a bug.

**Rollback path** (if everything goes sideways):
- Step 4's SQL has a commented-out reverse: `ALTER TABLE pa.X_old_2026_05_07 RENAME TO X` (×9). This restores the old tables in place
- The new tables stay populated independently — NO data is lost on rollback. Both schemas are current up to the cutover point
- Step 5 (drop old tables for real) hasn't run yet at time of writing. Do NOT run it until the app has been verified end-to-end for at least a week

**Performance gotchas caught during the migration**:
- `ORDER BY ... DESC LIMIT N` queries: writing them as `JOIN pa.urls THEN ORDER+LIMIT` is the slow plan (parallel hash join + top-N heapsort over 980k urls). Always rewrite as subquery-LIMIT-then-JOIN: do the order+limit on the smaller table first, then join via PK lookup. ~25× speedup on the recent-results panels in `/api/status`, `/api/faq/status`, `get_recent_results`, `/api/validation-history`. See migrations/step3c doc + commit `d5c8739`
- `COUNT(*) FROM pa.urls LEFT JOIN content tables WHERE content IS NOT NULL` was 5.9s. UNION-ALL of the two content tables → 0.5s. ~12× speedup on `/api/content-publish/stats`. Commit `4ac8808`
- After backfill, ALWAYS run ANALYZE on the new tables — the planner had stale row-count estimates and was picking bad plans. Done as part of `migrations/step3c`. If the dashboard suddenly slows down post-cutover, ANALYZE first, EXPLAIN second
- The dashboards used to take 2+ minutes to load before the perf fix; they're now sub-second. If they're slow again, check `ORDER BY ... LIMIT` queries first

**Subtle data-quality fix during cutover** — `bigbang fix: backfill content for CSV-imported (ai_processed=FALSE) rows` (commit `6bbdc0e`): the step 2 backfill of `pa.unique_titles_content` only copied rows with `ai_processed=TRUE`. ~400k OLD rows had `ai_processed=FALSE` AND title/h1 populated (CSV imports via `bulk_upsert_titles` writes content but never flips ai_processed). The OLD eligibility query treated them as done because the content was present; the NEW `get_unprocessed_count` thought they were pending. Fix: backfilled the 399,906 missing content rows + flipped status='pending' → 'success'. Now `bulk_upsert_titles` always sets status='success' on every upsert, so this can't recur

**The cutover process in practice** — this took one focused day, in this order:
1. Create new tables (step 1) — additive, zero-risk
2. Backfill data (step 2) — additive, zero-risk; ANALYZE after
3. Refactor code one tool at a time (step 3a unique titles → 3b/c FAQ + Kopteksten bundled because content_publisher joins both)
4. Restart uvicorn — that was the actual cutover moment; everything kept writing to OLD tables until then
5. Watch for stale-data symptoms (e.g. dashboard pending count off) and run targeted backfills (step 3a-fix)
6. Add perf indexes + rewrite hot queries (step 3c)
7. Rename old tables (step 4) — forcing function: anything I missed now fails LOUDLY with "relation does not exist"
8. After 1 week of green: drop the old tables (step 5, not yet run)

If you need to repeat this on another data domain, that ordering is what worked.

**Files I should look at first if something breaks**:
- "URL X isn't being processed" → `backend/url_catalog.py::canonicalize_url` (does it canonicalize cleanly?), then check membership in `pa.kopteksten_jobs` / `pa.faq_jobs` (does it have an eligibility row?), then `pa.url_validation` (is it `is_valid=FALSE`?)
- "Dashboard shows wrong count" → look at the SQL in `backend/main.py` for the relevant endpoint; common bug = forgetting the `LEFT JOIN pa.url_validation v ... WHERE v.is_valid IS NULL OR v.is_valid = TRUE` filter on pending counts
- "Recent results panel is slow" → make sure the query is subquery-LIMIT-then-JOIN, not the other way
- "Foreign-key violation on insert" → caller is writing a url_id that doesn't exist in `pa.urls`. Use `get_url_id(cur, url)` instead of writing raw url_ids
- "Why does my old script error with 'relation does not exist'" → the table got renamed in step 4. Either migrate the script (look at the OLD→NEW mapping at the top) or run it against the `*_old_2026_05_07` snapshot

## Unique-titles v3 pipeline experiment — shelved at ~76% acceptable (2026-05-06)
- **What we built**: an alternative `generate_title_v3()` in `ai_titles_service.py` that replaces the current `generate_title_from_api()` flow. v1 fetches Beslist's api_h1, runs ~5 dedup passes, strips brand/color/size to set them aside, sends the cleaned remainder + a 11-rule prompt to gpt-4o-mini for a full rewrite, then reassembles. v3 skips api_h1 entirely: composes the H1 deterministically from the facets in fixed slots (`<colour> <merk> <populaire_serie> <type_productlijn> <productlijn> <materials> <other adjectives> <doelgroep> <category> <met-clauses> <voor-clauses> <color-combos> <size>`), hands it to gpt-4o-mini with a much shorter polish-only prompt (5 rules: inflect, agglutinate, lowercase, no add/remove, no reorder), then runs three guards plus the same dedup passes. ~600 lines of strip-and-prepend gone, ~120 lines of compose+polish added.
- **Why we explored it**: the user observed that `generate_title_from_api`'s pre-AI cleanup + post-AI dedup + post-AI hallucination guard collectively form an unwieldy "the AI did something weird, scrub the output" loop. The composed builder skips that — the AI never sees an unsanitised input.
- **Results on the 100-URL A/B sample** (sample baseline = stored v1 outputs):
  - Round 1 (no guards): ~30% acceptable. AI dropped content, lowercased brands, agglutinated wildly.
  - Round 2 (added `_v3_preserves_content` token-set guard): ~70% acceptable. Content drops caught and fall back to composed.
  - Round 3 (removed `_apply_hallucination_guard` because its prefix-match length-diff ≤3 was rejecting legitimate Dutch agglutination — `koraaltops` failed against `koraal`+`tops` since 10−6=4>3): same ~70%.
  - Round 4 (added `_v3_restore_casing` to copy original casing token-by-token from composed): ~76%. Brand-lowercasing class fixed.
  - Round 5 (added `_v3_preserves_brands` to detect brand-swallowing): ~76% (same — brand swallowing now triggers fallback rather than shipping `arapumps`).
- **Why we shelved at 76% vs the 85% threshold**: two regression classes remain that need real work, not another guard:
  1. **Composed-builder semantic redundancy** — `Ventilatieventielen Ventilatiematerialen`, `Kandelaars Drijfhout Hoge Kaarsenhouders`, `Wanten Handschoenen`. The facet value and the canonical category are near-synonyms, but `_dedupe_facet_values` only catches identical/inflected matches. Needs a synonym table or a "category implied by this facet value" classifier.
  2. **AI agglutination errors that aren't brand-swallowing** — `damestmultivitaminen` (extra `t`), `schuifdekselkoelkasten` (compound that doesn't exist in Dutch). Brand guard doesn't catch these because no brand was lost; content guard doesn't catch them because all source tokens are still substrings.
- **What stays from this experiment** (kept in code as opt-in):
  - `AI_TITLES_PIPELINE` env var: `v1` (default) or `v3`. Lets you flip per-deployment to A/B test. Set in `start_processing()` worker.
  - `_build_v3_h1`, `generate_title_v3`, `_v3_preserves_content`, `_v3_preserves_brands`, `_v3_restore_casing`, `_POLISH_PROMPT_V3_TEMPLATE` — all in `ai_titles_service.py` under a clear "EXPERIMENTAL — IN FRIDGE" header so future-you knows the status.
- **What to revisit when picking this up again**:
  - The 100-URL A/B xlsx files at `/tmp/v1_vs_v3_100_*.xlsx` are gone (tmpfs). Re-generate with the snippets from the conversation history.
  - The semantic redundancy class is the bigger blocker — without a synonym map we'd need a lookup against `pa.facet_type_classifications` plus a category-stem comparison.
  - Consider whether to drop the AI polish step entirely: round 4 was already 68/100 fellback (= composed h1 unchanged after casing-restore). If the deterministic builder + casing-restore is accepted often enough on its own, the AI polish step adds cost for marginal value.
  - Cost: v3's polish prompt is ~5x shorter than v1's rewrite prompt, so per-call cost is lower; but if the polish output is rejected by guards in 30%+ of cases, the AI spend becomes pure waste.
- **Files**: `dm-tools/backend/ai_titles_service.py` (~250 lines added under the EXPERIMENTAL header), no schema changes, no DB writes by the experiment. Can be ripped out cleanly by deleting the EXPERIMENTAL section + reverting the `_pipeline = os.getenv` switch in `start_processing`.

## Redshift type traps: `date` is DATE, `dim_date_key` is BIGINT (2026-05-05)
- **The trap**: `bt.shop_main_attributes_by_day.date` is a real `DATE` column. The queries.txt sample compared it to a `'YYYYMMDD'` string literal which works by Redshift's implicit cast on bare literals — but `TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD')` evaluates to a string at runtime and the implicit cast silently fails to match (no error, just zero rows). Use `date = CURRENT_DATE - 1` directly, or compare against a properly cast `DATE` value
- **The companion trap**: `bt.shop_list.dim_date_key` is `BIGINT` (YYYYMMDD packed as integer). For an upper-bound filter against today/yesterday you have to write `dim_date_key <= CAST(TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD') AS BIGINT)` — TO_CHAR returns text and the Redshift planner won't auto-coerce text-to-bigint comparisons silently
- **The diagnostic that found it**: `SELECT MAX(date), MIN(date) FROM table` returns `datetime.date(...)` for DATE columns vs. integer for BIGINT date keys; psycopg2's RealDictRow shows the type plainly. Always probe the column type when a "should-just-work" date filter returns 0 rows. Better than guessing at `to_date()`/`to_char()` permutations
- **General rule for Redshift date filtering**: when copying a query from a working sample, look at the column's actual type (`information_schema.columns` or just `pg_typeof`) rather than mimicking the literal format. The sample may have worked through implicit-cast quirks that don't survive a TO_CHAR rewrite
- **File**: `dm-dashboard/backend/gsd_check_service.py`

## `ai_processed=TRUE` blocks re-generation even when content is NULL'd (2026-05-05)
- **The trap**: bulk-resetting bad H1s by `UPDATE pa.unique_titles SET title=NULL, description=NULL, h1_title=NULL WHERE …` is a no-op for the worker. Eligibility for the AI batch is `ai_processed IS NULL/FALSE` (per `unique_titles.queue_urls_for_generation` docstring), and the reset above leaves `ai_processed=TRUE` from the prior run. The rows sit forever as "NULL h1, ai_processed=TRUE", invisible to the worker, looking re-queued from a SQL perspective but never actually picked up. Found it by checking why a re-queued URL still showed yesterday's bad H1 — `ai_processed_at` was hours before the most recent restart
- **The fix**: every reset query needs to also set `ai_processed=FALSE, ai_processed_at=NULL, ai_error=NULL`. One catch-all sweep that fixes prior buggy resets: `UPDATE pa.unique_titles SET ai_processed=FALSE, ai_processed_at=NULL, ai_error=NULL WHERE h1_title IS NULL AND ai_processed=TRUE`. Found 11,833 stuck rows on 2026-05-05 — 5k+ above the sum of explicit reset batches that day, meaning the same trap had been compounding across earlier sessions
- **General rule**: if a worker uses a separate "processed" flag to gate eligibility (vs. just NULL-ness of the content column), every reset path needs to flip that flag, not just the content. Easy to forget when the model has both a `<col>` and a `<col>_processed`/`status` field. Audit reset utilities for this pattern when adding new ones
- **File**: `dm-tools/backend/unique_titles.py:queue_urls_for_generation` (eligibility convention)

## H1 dedup pipeline class of bugs: hyphenated tokens, plural derivations, AI-inserted fillers (2026-05-05)
- **Three orthogonal failure modes encountered in one session**, each fixed in `ai_titles_service.py` (`_dedupe_prefix_overlap` + `_apply_hallucination_guard`):
  - **Hyphenated targets**: `"Fisher Price Fisher-Price …"` — the prefix-overlap rule saw `"Fisher"` (6) as a prefix of `"Fisher-Price"` (12) and dropped it, leaving the orphan token `"Price"`. The real duplication was the multi-token `"Fisher Price"` form, which `_dedupe_internal_compounds` handles correctly via `_norm_for_dedupe` (strips spaces AND hyphens). Fix: skip hyphenated `b` in `_dedupe_prefix_overlap` so the next pass can resolve it cleanly
  - **Plural derivations the 6-char floor blocked**: `"Sweat sweaters"` / `"Plant planten"` / `"Color Colors"` / `"tuinstoel … Tuinstoelen"`. The 6-char floor is intentional (avoids dropping `"Aqua"` before `"Aquariums"`) but blocks legitimate plural-of-singular cases. Fix: targeted plural-suffix list `('s', 'en', 'ers')` with a 4-char floor — fires only when `b == a + suf`, so `"Aqua"+"riums"` (riums not in list) still passes through. Lookahead window also extended 2→4 positions to catch cases where the AI inserts intervening tokens like `"instap … Heren schoenen Instappers"` (3 tokens apart)
  - **AI-inserted glued / fragmented tokens**: the v1 hallucination guard checked only 8 hardcoded common-offender words. v2 (already implemented as a prefix-match guard against the input vocabulary + simple Dutch inflections) was the obvious upgrade — drops `"wandelzomer"` (no allowed word matches by 5+ char prefix with len-diff ≤3) and orphan `"Sluiting"` (extracted by AI from `"ritssluiting"`). Default flipped from v1 → v2. v2 also keeps `"Katoenen"` because `katoen + 'en'` is in the inflected whitelist
- **The general shape**: dedup safety nets are a defense-in-depth stack (each pass handles a different shape), and the order matters. Earlier passes that drop tokens prematurely starve later passes of context. When a new bug surfaces, ask "which pass is firing too aggressively, vs. which pass is missing the case entirely" — sometimes the fix is to *narrow* an existing rule so a later pass can take over
- **The audit pattern**: when a stored bad H1 reproduces under a re-run, the new code is innocent — `ai_processed_at` is the receipt for which code generated it. Always check the timestamp before debugging the pipeline
- **Files**: `dm-tools/backend/ai_titles_service.py:_dedupe_prefix_overlap`, `_apply_hallucination_guard`, `generate_title_from_api`

## DMA+ Monthly polish: auto-adapt, dry-run, UI cleanup, Windows codec fix (2026-04-23)
- **Auto-adapt the new delta layout for ALL per-shop ops** (commit `407ce9c`). `_run_operation` in `dma_plus_service.py` now detects if the uploaded xlsx has any of the `{NL,BE} - Nieuw (aanmaken)` / `{NL,BE} - Afvallers` sheet names; if so it picks the right sheet (nieuw for inclusion/exclusion, afvallers for reverse-*) for the selected country and fans each row to cl1 a/b/c before handing off to the v2 processor. No changes to `campaign_processor.py`. If the file looks like a delta workbook but is missing the sheet the selected op/country needs, fails fast with a clear `ValueError`.

- **Dry-run support for Process Monthly Excel** (commit `3252b26`). All four v2 processors already accepted a `dry_run=False` kwarg; the monthly orchestrator was throwing that away. Threaded it through: `POST /api/dma-plus/monthly` now takes a `dry_run` form field → `start_monthly(dry_run=…)` → `run_monthly_delta(…, dry_run=dry_run)` → each of the four lambdas passes `dry_run=dry_run` into the processor. Frontend dry-run toggle is now visible for the Monthly op too; task dict + log lines carry a `[DRY RUN]` tag. Full audit of every `mutate_*` call inside the four processors confirmed they're all gated by `if dry_run: … else: <mutation>`, with inclusion generating fake `DRY_RUN_<uuid>` resource names so downstream `.split('/')[-1]` logic doesn't choke.

- **Verifying dry-run is actually dry** (today). The log message `"✅ PLA/Klompen_a: 1 removed, 0 not found"` reads like a real removal but in dry-run mode is synthesised from a fake `{'success': [...]}` result dict — the actual `reverse_exclusion_batch` call is skipped. Definitive check: query `change_event` in Google Ads for the run window: `SELECT change_event.change_date_time, change_event.resource_change_operation, change_event.change_resource_type, change_event.user_email FROM change_event WHERE change_event.change_date_time >= 'YYYY-MM-DD HH:MM:SS' AND ... LIMIT N`. Real mutations show up here within seconds; dry-run produces 0 rows. **GAQL gotcha**: the field is `change_resource_type`, NOT `changed_resource_type` (the obvious guess returns `UNRECOGNIZED_FIELD`).

- **Windows cp1252 crash on ✅/🌳/Dutch characters** (commit `4b013c7`). Running the backend on native Windows Python crashed mid-run with `'charmap' codec can't encode characters in position 2-71`. Root cause: `sys.stderr` defaults to the console's cp1252 codec on Windows; the Google Ads library + `campaign_processor.py` emit log lines with emoji and Dutch category names that can't encode. Our stdout-capture blocks redirected only `sys.stdout` — seven sites in `_run_operation` + one in my `_run_one_operation`. Fix (belt-and-suspenders): (1) at module import of both `dma_plus_monthly.py` and `dma_plus_service.py`, reconfigure `sys.stdout` and `sys.stderr` to `encoding='utf-8', errors='replace'` so a rogue byte becomes `?` instead of killing the worker; (2) during processor calls, redirect BOTH `sys.stdout` and `sys.stderr` to the same in-memory `StringIO` and restore in `finally`. **General rule**: any worker-thread wrapper that captures subprocess/library output has to capture stderr too, and on any codepath that might run on Windows, `errors='replace'` on stdio is cheap insurance.

- **Download xlsx UI placement** (commit `c862512`). Button was sitting as a 4th tile in the stats row (Countries / Rows / Errors / Download). Moved into the Results card header next to "Copy Results" — stats row now has three equal `col-4` tiles. Also reset the header button at the start of every `showResults` call so it only ever appears for Monthly Delta runs and doesn't linger across op switches.

## DMA+ Expanded: Monthly Delta + Category Coverage in the Dashboard (2026-04-22)
- **Built**: `backend/dma_plus_monthly.py` adds two new capabilities to the existing DMA+ page — (1) Monthly Delta that reads a multi-sheet xlsx (`NL/BE - Nieuw (aanmaken)`, `NL/BE - Afvallers`, 3 cols each) and fans each row to cl1 ∈ {a,b,c} at €50, running Include→Exclude on Nieuw then Reverse-exclude→Reverse-include on Afvallers per country; (2) Category Coverage that writes TRUE/FALSE per taxv2 category for `PLA/{name}_{cl1}` existence (to spot naming mismatches).
- **Isolation from existing code**: instead of modifying `campaign_processor.py` (8805 lines) to accept a `sheet_name=` kwarg, the orchestrator builds a *fresh openpyxl.Workbook per step* with the conventional sheet name (`toevoegen`, `uitsluiten`, `verwijderen`) that the v2 processors already expect. Reuses `_populate_cat_ids_sheet`, `_extract_results`, `_patch_campaign_processor`, `_get_client` from `dma_plus_service.py` — no duplication.
- **Taxonomy crawl trap**: `/api/Categories?rootCategoriesOnly=false` on the taxv2 API returns only the ~30 root categories despite the flag. Full tree must be BFS-crawled via `/api/Categories/{id}?includeSubCategories=true` per node — 3575 categories, ~19s with `ThreadPoolExecutor(max_workers=12)`. The existing `_fetch_all_cat_ids_from_taxonomy_api` serves a different purpose (maincat-rooted cat_ids mapping), so I added a separate `_fetch_taxv2_tree` for the full-tree coverage check.
- **Task integration**: reuses the existing `_set_task`/`_get_task`/`_check_cancelled`/`TaskCancelled` + `/status/{id}` + `/cancel/{id}` endpoints, so the new flows show up in the same history and support the same cancel button. Output xlsx goes to `/tmp/dma-plus-output/` and is served via a new `GET /api/dma-plus/download/{task_id}`.
- **SystemExit bites threads**: when a job runner uses `except Exception`, `initialize_google_ads_client`'s `sys.exit(1)` on missing OAuth creds raises `SystemExit` (a `BaseException`), the thread dies silently, and the task is stuck at `running` forever. Fix: `except BaseException` in thread wrappers with `type(exc).__name__` in the error message.
- **Validated**: live NL coverage run via the new `/api/dma-plus/coverage?country=NL` → completed in ~40s, 3575 categories, 9843 PLA campaigns, 3227 TRUE per cl1, xlsx downloaded from `/api/dma-plus/download/{id}` verified (127 KB, 3576 rows, correct schema).
- **Not validated**: monthly delta end-to-end (would create real ad groups). Building blocks are the same ones the existing per-shop flow uses daily, so low risk — but first live run should be watched.

## Google Ads API v17 is dead — library must target current API (2026-04-22)
- **Symptom**: every GAQL call returns `501 GRPC target method can't be resolved. /google.ads.googleads.v17.services.GoogleAdsService/Search`.
- **Cause**: `google-ads==24.1.0` targets API v17 which Google has retired. The endpoint no longer exists server-side.
- **Fix**: bump to `google-ads==30.0.0` (current). Validated read-only in a fresh venv against NL: `campaign` search (9843 rows), `ad_group_criterion` listing_group reads with proto-plus `case_value._pb.WhichOneof("dimension")`, enum + proto construction (`AdGroupCriterionOperation`, `ListingGroupTypeEnum`, `ProductCustomAttributeIndexEnum`, `copy_from`), `prefetch_pla_campaigns_and_ad_groups` (9843/12051). Mutation paths not exercised; first live workflow should be watched.
- **Lesson**: pin only minor version (`google-ads>=30,<31`) and schedule yearly refresh. Google retires API versions on a known cadence.

## JS-truncated table text can't be hover-expanded; CSS-clipped text can (2026-04-21)
- **The trap**: the URL Checker's URL columns were rendered with `truncate(url, 50)` — a JS helper that slices the string to 50 chars + "..." before returning it as cell content. No amount of `white-space: normal; overflow-wrap: anywhere; :hover` styling can bring back characters that never entered the DOM. User noticed: Title / Description / H1 expanded cleanly on hover, URL cells stayed truncated
- **The rule**: if you want "hover to see full value", the full value MUST be in the DOM. Clipping should live in CSS (`white-space: nowrap; overflow: hidden; text-overflow: ellipsis`) + an optional `:hover` override. JS string-slicing is destructive and silently breaks any "reveal more" interaction. Keep `truncate()` for log output, banners, status chips — places where the full text genuinely shouldn't render — not for table cells that also need a reveal affordance
- **The fix**: drop the `truncate()` call on URL cells, escape the full URL via `escapeHtml()` and let the existing `.url-cell` CSS handle visual clipping. Rows with 100-char URLs now hover-expand just like the meta cells do. File: `dm-dashboard/frontend/url-checker.html`

## Hover-expand in fixed-layout tables: `overflow-wrap: anywhere`, NOT `overflow: visible` (2026-04-21)
- **The earlier version of the fix**: initial attempt used `.url-cell:hover { white-space: normal; overflow: visible; }` — on hover the cell content would spill past the column's 280px fixed width and trigger a horizontal layout shift because `overflow: visible` tells the renderer "content is allowed to extend beyond the box", which in a fixed-layout table ripples into the sibling columns' rendering and triggers a scrollbar/reflow
- **The right pairing**: `.url-cell:hover { white-space: normal; overflow-wrap: anywhere; word-break: break-word; }` — no overflow change. The cell stays the fixed column's width; `overflow-wrap: anywhere` makes long unbroken tokens (URLs, no-space strings) wrap inside the column; row height grows vertically to fit the wrapped lines. No horizontal shift, just a vertical reflow of the row on mouse enter/leave. User explicitly validated this was the trade-off they wanted
- **Applies to any `table-layout: fixed` table**: the fixed layout is what makes column widths rigid, so any hover effect that relies on `overflow: visible` is fighting the layout engine. Prefer wrapping inside the cell. If you really want the cell to grow SIDEWAYS on hover (rare), you'd need to switch to `table-layout: auto` and accept that column widths will jitter constantly
- **File**: `dm-dashboard/frontend/url-checker.html`

## HTML entity decoding: textarea roundtrip + loop for double-encoded content (2026-04-21)
- **The textarea trick**: `document.createElement('textarea').innerHTML = '&#x27;'` then `.value` returns `'` — browser handles every named entity (`&amp;`, `&eacute;`) and every numeric entity (`&#x27;`, `&#10062;`) natively, no hand-rolled lookup table. Reuse a single module-level textarea instance to avoid per-call allocation
- **The double-encoding case**: the scraper surfaced `&#10062;` (a ❎ cross mark) literally in product descriptions. Root cause: source pages sometimes have `&amp;#10062;` in their HTML — BeautifulSoup decodes one layer to `&#10062;`, which arrives at the client as an 8-char string that *looks* like a decimal entity but has already survived one decode pass. A single textarea roundtrip turns `&amp;#10062;` into `&#10062;` (good) but a single pass on an already-one-decoded input leaves `&#10062;` untouched (wait — no, it would decode that too. Let me re-check). Actually `innerHTML = '&#10062;'; value` does return ❎. So the single-pass should have worked. The real failure mode was: the source page had `&amp;#10062;`, BS4 normalised to `&#10062;` on the way out, client decoded once → ❎. But in some cases the string had additional escaping (e.g. JSON-source with `\u0026` for `&`), so the decoded result still contained `&#...;`. Safe fix: loop up to 3 times, stopping as soon as a pass makes no change
- **Why 3 iterations not forever**: a pathological input like `&amp;amp;amp;...` with 50 nested encodings is vanishingly rare and more likely indicates an upstream bug than a legitimate case to decode. 3 covers single, double, triple encoding; anything more should be surfaced as raw so someone notices
- **Rule of thumb**: when you decode anything (entities, URL encoding, base64), and the decoded output *still looks encoded*, don't just add another decode call — loop until stable with a small cap. Saves you from playing whack-a-mole when upstream re-encodes
- **File**: `dm-dashboard/frontend/url-checker.html:decodeEntities`

## `btn-outline-danger` is overridden to burnt-orange in `frontend/css/style.css` — inline-style your red buttons (2026-04-21)
- **The trap**: I added a Remove button with `class="btn btn-outline-danger"` expecting Bootstrap's red outline. Live-rendered as orange. Traced to `frontend/css/style.css:373-380` — there's an explicit `.btn-outline-danger { border-color: var(--color-button); color: var(--color-button); }` override that aliases the danger class to `#CC5500` (the dm-dashboard brand orange used for primary CTAs like "Run"). The comment `/* Outline danger buttons - orange like other outline buttons */` flags it as intentional project-wide styling
- **The workaround**: use inline `style="border: 1px solid #d63031; color: #d63031; background: white;"` with `onmouseover/out` handlers — matches the pattern already used for per-card action buttons (Export, Revert) across DMA+, DMA Bidding, GSD Budgets. Pick `#d63031` because that's the red already in use for decrease badges in the same color palette (`.badge-decrease`, search-status alerts) — stays consistent with the rest of the project's palette rather than introducing a 4th red
- **Why this class of override exists**: the dashboard's design language is "all outline buttons share the same accent color so the UI doesn't look like a bootstrap demo." That's a reasonable call, but it means `.btn-outline-danger` doesn't mean "danger" anymore — it means "outlined secondary CTA". If you actually want a red destructive button, you have to opt out of the project class system. Worth a grep of `style.css` for any `.btn-outline-*` override before reaching for Bootstrap classes in this codebase

## API escape hatches: keep backend query params when removing the UI control that drives them (2026-04-21)
- **The pattern, used three times this week**: GSD Budgets started with three UI toggles (`Upload missed-shops`, `Limit shops`, `Skip missed-shops upload`). User asked to remove two of them as the default behavior settled. Took the UI controls out but left the FastAPI query params in place with their existing defaults. Cost: one line per param still passing through — essentially free. Benefit: you can curl `/api/gsd-budgets/run?limit_shops=5` for a smoke test without reverting the UI, and CLI-style scripts can still target the non-default behavior. No frontend rewrite needed if the toggle comes back
- **When NOT to do this**: if the removed behavior changed semantics (e.g. used to "skip step 3" but now step 3 isn't skippable because later code depends on it), remove the param too — a param that silently gets ignored is worse than a 404. Default-True / default-False params whose "off" value is still a valid execution path are the right candidates for this pattern
- **The question to ask**: "if I remove this UI control, would I ever want to hit the endpoint with the non-default value from a script or curl?" If yes, keep the param. If no, remove it. For ops-ish dashboards where a script might eventually replay a run, the answer is usually yes
- **Side benefit on reversibility**: keeps the backend API surface stable for the two-step "ship the UI simplification first, remove the param later if nothing reaches for it" roll-out, vs. a risky single-step rip

## Auditing a "dry-run" feature: grep every mutation, don't trust the obvious ones are the only ones (2026-04-21)
- **The trap**: ported GSD Budgets with what felt like thorough dry-run coverage — `adjust_campaign_budget` returned early on `dry_run`, `upload_missed_shops` was gated at the call site with `and not dry_run`. The big Google Ads mutation and the big Redshift write were both safe. Felt complete. User asked "is dry run truly dry run?" and a 10-second grep answered "not quite": `sync_shop_exclusions()` runs FIRST in the flow and does `DELETE FROM pa.gsd_shop_exclusions_joep; INSERT ...` on every run regardless of dry_run. I'd framed it as "read-side refresh, not a user-facing mutation" — which is technically true — but it IS still a write to Redshift, and a user ticking the Dry Run box reasonably expects zero writes
- **The audit recipe**: one grep, run it on every ported service module before declaring dry-run done — `grep -nE 'INSERT|UPDATE|DELETE|TRUNCATE|mutate_|update_|\.commit\(\)|write_text|\.write\(' service.py`. For each hit, confirm one of three things: (1) gated by `if dry_run: return` before the call; (2) gated at the call site (`and not dry_run`); (3) intentional local-only write (history JSON, credential yaml cache, temp files under `backend/data/`) that you've consciously decided is fine. Anything that's a write to an external system (Redshift, Google Ads, Sheets, email, S3) MUST be in category 1 or 2
- **Frame for users: what "dry-run" means operationally**: the fix was to also gate `sync_shop_exclusions` — so dry-run is now strictly read-only for external systems. Trade-off: dry-run uses whatever's currently in `pa.gsd_shop_exclusions_joep` rather than the live sheet, which may be stale if the sheet was edited since the last live run. Captured this in the run result as `exclusions_sync_status: "synced" | "skipped_dry_run" | "failed: <msg>"` so the UI can tell the user exactly what happened. Surfacing the status field matters — a silent "skipped" is worse than a noisy one
- **General principle**: when a pure-read operation and a pure-write operation share a function and someone adds a dry-run mode later, the write path must be gated but the read must continue. The gate belongs where the function is CALLED, not inside the function — a function named `sync_shop_exclusions()` has "write" in its name and shouldn't pretend to no-op. Gating at the call site also makes the dry-run vs live divergence obvious in the calling code

## Setuptools 81 drops `pkg_resources` by default — vendored SDKs that import it will explode silently (2026-04-21)
- **The trap**: the Google Search Ads 360 SDK (`searchads360-py.tar.gz`, required by the GSD Budgets tool port) vendors a thin helper (`util_searchads360`) whose `metadata_interceptor.py` does `import pkg_resources`. Pip resolved `setuptools==82.x` into the dm-tools venv and `pkg_resources` — deprecated since setuptools 67 — is simply not installed by setuptools ≥ 81. Import blew up at process start with `ModuleNotFoundError: No module named 'pkg_resources'`, NOT at pip-install time. No pip warning; only surfaces the first time the vendored SDK is imported
- **The fix**: pin `setuptools<81` in `requirements.txt` alongside the tarball line. Added a comment above the pin so future-me (or other Claude) doesn't "clean up" the pin during a dep refresh. Long-term, a Google-issued SDK build that drops `pkg_resources` (it's deprecated API) removes the need — so the pin is expected to age out, not live forever
- **Why this class of bug is sneaky**: `pkg_resources` was the default-installed distribution-resources API for 15+ years. Packages still depend on it implicitly via vendored helpers (NOT via `install_requires`), so a transitive `setuptools>=81` upgrade at `pip install` time silently pulls the floor out from under them. Any dep chain that pulls `setuptools>=81` AND has an older vendored helper doing `import pkg_resources` breaks on next run. Grep your `backend/vendor/` for `pkg_resources` whenever a random-feeling `ModuleNotFoundError` appears
- **File**: `dm-dashboard/requirements.txt` (+ `backend/vendor/util_searchads360/interceptors/metadata_interceptor.py` is where the import lives)

## Port a Python CLI script to the dashboard: what you actually need to refactor (2026-04-21)
- **The 4 source-script patterns that must change for dashboard integration**: (1) top-level imperative code (`for ... in main_loop(): mutate()`) gets refactored into a pure `run_X()` function with explicit parameters — the imports and constants and helper defs stay module-level, but the `__main__` body moves inside a function so a FastAPI request handler can call it; (2) `psycopg2.connect(...)` calls with hardcoded credentials become `backend.database.get_redshift_connection()` / `return_redshift_connection()` — use the existing pool, don't open fresh sockets per call; (3) `print()` statements become `logger.info()` — the dashboard captures uvicorn stdout but structured log calls filter cleaner; (4) functions that mutate (e.g. `adjust_campaign_budget`) grow a `dry_run: bool` parameter, guarded by `if dry_run: return {"status": "dry_run", ...}` BEFORE the API call — not after. Verify the mutation call is actually gated, don't just set a flag
- **Per-country configuration table is the clean shape**: the NL + BE versions of `GSD_verhogingen_verlagingen*.py` diverged in 7 values (customer_id, domain, sa360_account, sheet_id, country_code, email subject, email greeting). Instead of two `if country == "NL":` branches scattered through the service, I made one `COUNTRY_CONFIG: Dict[str, Dict[str, Any]]` dict at module top and a `_resolve_country(country)` helper. Every country-dependent site reads `cfg = _resolve_country(country); cfg["customer_id"]` — makes it obvious at a glance which code paths are country-parameterised vs shared. Adding DE later is a single dict entry, not a hunt-and-replace. Rule: if you see N if-else branches on a single categorical variable, lift the variable into a config dict
- **What NOT to port from old scripts**: the GSD source had `sendMail` + `sendMail1` + `sendMail2` (Microsoft Graph device-code flow, per-user MSAL token cache) that can't run unattended from a request handler. Dropped entirely. The dashboard UI + XLSX export replace the email. If someone later needs email, that's a separate scheduled task with a pre-seeded token cache — not a user-triggered endpoint. Similarly dropped `get_total_marge1`, `adjust_campaign_budget1`, `upload_missed_shops1` — legacy retry/dead variants that were never called from `__main__`
- **Concurrent-run safety**: two users clicking "Run" simultaneously would race on `CampaignBudgetService.mutate_campaign_budgets` calls. Added a module-level `_run_lock = threading.Lock()` wrapping the entire `run_gsd_budgets` body. Costs nothing (serializes multi-minute API calls) and prevents double-mutation. DMA Bidding's runs are fast + idempotent enough that it doesn't have one; GSD Budgets' potentially-hundreds-of-mutations run does
- **Run-history persistence = two files**: write a `_load_run_history_from_disk()` + `_save_run_history_to_disk()` pair, store JSON under `backend/data/<tool>_history.json`, gitignore it. Load ONCE at module import, save INSIDE `_history_prepend` (same lock that protects the list). uvicorn `--reload` re-imports the module = re-reads history; clean restart preserves history across crashes. Without this, every uvicorn tweak wipes the in-memory deque and the UI's history table comes up empty — which users mistake for "the tool lost my runs". DMA Bidding learned this the hard way; cloned the pattern for GSD Budgets

## Inserting a nav-dropdown link into N copies of N HTML files: one regex, scoped by enclosing `<a href="...">` (2026-04-21)
- **The problem**: the dashboard has 20+ frontend pages, each with the same Google-Ads nav-dropdown markup inlined (no templating). Adding a new tool means inserting a `<a href="/static/<new>.html" class="nav-dropdown-item">...</a>` entry between two existing ones in EVERY page. Manual edits across 19+ files invite inconsistency drift (one page gets the new link, others don't — most common root cause of "the tool exists but I can't navigate to it")
- **The reliable approach**: write a single-pass Python regex whose `old_string` is just the *transition* between the anchor-that-comes-before and the anchor-that-comes-after (e.g. `GSD Campaigns</a>\s+<a href="/static/dma-bidding.html"`), and whose replacement re-inserts the same whitespace prefix + the new anchor between them. This works across pages where the `nav-dropdown-item` class may or may not have `active` appended (exactly one page, the current tool's own page, does) because the pattern deliberately doesn't match the class suffix. Run with `subn` to get a hit count per file, and print "no match" rows so you notice when a page uses a different transition pair (happens when the tool you're anchoring against was itself recently added)
- **Scoping updates to a specific tool's icon**: same pattern, but anchor the regex to the enclosing `<a href="/static/<tool>.html"...>` — otherwise a regex matching just the SVG inner paths can collide with a lookalike icon elsewhere on the page (e.g. DMA+'s plus-in-circle was identical to the initial placeholder for GSD Budgets, so a bare SVG replace would have hit both anchors). `href="..."[^>]*>\s*<svg[^>]*>OLD_PATHS</svg>` with the href matched first guarantees per-tool scoping
- **Always exclude the new tool's OWN page from the loop**: the new `gsd-budgets.html` gets its nav-dropdown markup written from scratch (in the template I author directly), so re-matching against it during the bulk insert would either no-op (if the new anchor is already there) or double-insert (if the nav-dropdown structure has the "before anchor" pattern but not the "after anchor"). Either skip the file by name, or assert `n == 1` instead of `n >= 1` and investigate the count
- **File**: `dm-dashboard/frontend/*.html` — 20 files touched in one Python run, all 20 report `+1`

## Parsing that strips URL segments for validation loses information the suggestion builder needs (2026-04-20)
- **The bug pattern**: `parse_beslist_url` was doing one `re.sub(r'/r/[^/]+/', '/', path)` to "ignore the /r/{bucket}/ query segment for validation purposes". That's correct for validation (the bucket isn't a category or a facet, it shouldn't be matched against the taxonomy). But it's destructive: by the time `build_suggested_url` ran, `ParsedUrl` had no record that `/r/` ever existed. Result: a valid URL `.../r/dark_grey/c/merk~83292` came back as a suggestion `.../c/merk~83292` — a different URL that may not return the same product set
- **The general rule**: parsing has two audiences — validators (want normalised/simplified structure) and reconstructors (want lossless input). A single destructive pass serves the first but breaks the second. When a parser has both roles, capture stripped segments into named fields on the parsed object rather than discarding them. `ParsedUrl` now has `r_query: str = ""` alongside `maincat_slug`, `subcat_slug`, `facets` — the strip still happens, but the segment is preserved for reconstruction
- **Regex-edge-case hygiene while you're touching it**: the original `re.sub(r'/r/[^/]+/', '/', path)` required a trailing slash on the `/r/` segment. A URL like `/products/x/y/r/dark_grey` (no trailing slash, which does happen in raw input) slipped past entirely — `/r/` stayed in the path and downstream checks emitted confusing issues. Changed to `re.search(r'/r/([^/]+)(?:/|$)', path)` for capture plus `re.sub(..., count=1)` for removal so end-of-string and mid-path variants both work
- **Related pattern — the dead HAS_BUCKET check**: `check_structural_errors` at `:391` has `if "/r/" in parsed.path:` — but parsed.path is the *stripped* path, so this branch literally never fires. It's been dead since the /r/ strip was introduced. Would have surfaced as "issue emission drift" in an audit, but compiled+passed tests never catch dead negative-assertions. Rule of thumb: when reviewing a parser's strip step, grep for checks against the post-strip field that are looking for the stripped pattern — those are dead-code candidates
- **File**: `backend/url_validator_service.py:parse_beslist_url`, `build_suggested_url`

## Lowercase-the-whole-output beats lowercase-per-segment (2026-04-20)
- **Where per-segment leaks**: `build_suggested_url` was calling `.lower()` individually on `maincat_slug` and `subcat_slug`, and on the facet slug *key* used for de-duplication — but not on facet *values*, not on the newly-added `r_query`, not on the scheme or netloc. So `https://WWW.Beslist.NL/products/Huis_Tuin/.../r/Dark_Grey/c/Merk~83292` lowercased maincat+subcat+facet-slug and came out as `https://WWW.Beslist.NL/.../r/Dark_Grey/c/merk~83292` — still mixed case. Per-segment lowercase is a magnet for "I forgot this field" bugs every time you add a field
- **The fix**: one `path = path.lower()` after full path assembly + `scheme.lower() / netloc.lower()` on the URL prefix. Covers everything the builder emits now and everything it might emit later. Per-segment `.lower()` calls upstream become redundant but harmless — I left them for local clarity at their call sites
- **When per-segment IS right**: when different segments have different case rules (e.g. a path that preserves user-supplied identifiers mixed with system-controlled slugs). Beslist URLs aren't that — they're uniformly lowercase by convention — so the universal rule applies. Check the convention before applying the universal rule
- **File**: `backend/url_validator_service.py:build_suggested_url`

## LLM prompts enforcing "forbidden anchor texts" need a block-list + post-processing guard, not just instructions (2026-04-20)
- **The trap**: the FAQ prompt said *"Gebruik GEEN generieke verwijzingen zoals 'deze gids', 'deze pagina', 'hier' of vergelijkbare vage linkteksten"* — plain negative instruction. The model ignored it: scan of `pa.faq_content` turned up 1,280 rows with vague anchors, including the user's example (`"Dark Grey variant kun je hier klikken"`). Plain negation in a prompt is weak when the model has a strong prior to make helpful-sounding links
- **What actually works**: three layers stacked — (1) an explicit **VERBODEN LINKTEKSTEN** block-list spelling out each forbidden phrase, (2) a **FOUT/GOED example pair** showing a wrong version and the corrected rewrite ("voor de Dark Grey variant kun je <a>hier klikken</a>" → "bekijk de <a>Philips Airfryer XXL</a>"), (3) a positive rule ("linktekst MOET de productnaam of een logische zoekterm zijn") plus an escape hatch ("als dat niet natuurlijk past, maak dan GEEN hyperlink — herschrijf liever de zin zonder link"). The escape hatch matters: without it the model will still force a link and fall back to "hier" as the least-bad option
- **Always add a programmatic post-processing guard even after prompt hardening**: `faq_service.py` now has a `VAGUE_ANCHOR_TEXTS` set + a normalise (lowercase, strip punctuation) step inside `clean_urls_in_answer`. Any `<a>` with an anchor that matches gets unwrapped (tag removed, text kept). Belt-and-suspenders: the prompt is the 95 % fix; the guard is the 5 % safety net for the model's off-days. Prompts drift, guards don't
- **Applied in 4 prompt sites** for dm-tools: `faq_service.py` (single-URL FAQ), `batch_api_service.py` (FAQ batch + Kopteksten batch system message), `gpt_service.py` (Kopteksten subcategory + main-category prompts and system messages). Guard currently only in `faq_service.py`'s single-URL path — worth adding the same guard to the batch FAQ path + kopteksten paths if the problem recurs there

## Two tracking conventions for "pending" in the same codebase — absence-of-row vs status='pending' (2026-04-20)
- **The two conventions, as implemented**: Kopteksten's pending query is `WHERE NOT EXISTS (SELECT 1 FROM pa.jvs_seo_werkvoorraad_kopteksten_check t WHERE t.url = w.url)` (`batch_api_service.py:_fetch_pending_kopteksten_urls`, `main.py:445,519`) — i.e. **a URL is pending iff it has NO tracking row**. FAQ's pending query is `WHERE NOT EXISTS (SELECT 1 FROM pa.faq_tracking t WHERE t.url = w.url AND t.status != 'pending')` (`batch_api_service.py:_fetch_pending_faq_urls`) — i.e. **a URL is pending iff it has no tracking row OR its tracking row is explicitly `status='pending'`**
- **Why this matters**: to reset URLs to pending you have to use the right operation for each. For FAQ, `UPDATE pa.faq_tracking SET status='pending', skip_reason=NULL` works (there's already precedent at `link_validator.py:721`). For Kopteksten, `UPDATE SET status='pending'` does NOTHING useful — the tracking row still exists, `NOT EXISTS` is still false, the URL stays hidden from the batch. You have to `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check` instead
- **How I got bitten**: during the reconcile, step 3 deleted tracking rows (kopteksten-style reset = good), step 4 inserted `status='success'` for all content without tracking (intending to only hit genuine orphans). But because step 3 ran first, the just-deleted 4,893 rows became fresh orphans and step 4 re-inserted them as success. Single-transaction ordering mattered and I didn't think it through. Fix sequencing: if you're doing *both* delete-to-reset AND orphan-backfill, either do backfill FIRST or exclude the step-3 URL set from step 4's INSERT (`WHERE c.url NOT IN (<step3 url list>)`)
- **Audit-before-mutate discipline**: for multi-step tracking reconciles, snapshot the working URL lists into a temp table at the start of the transaction. Then each step's DELETE/INSERT references the snapshot, not live-queries against tables that earlier steps just mutated. A `CREATE TEMP TABLE reset_targets AS SELECT url FROM pa.... WHERE status IN ('failed','completed')` at the top would have made this unbreakable
- **Rule of thumb**: before running any cross-table reconcile, diagram the state transitions on paper. Four tables interacting in one transaction is the point at which mental simulation stops being reliable

## Multi-table content pipelines rot in ~6 distinct ways — audit all of them at once (2026-04-20)
- **The audit rubric, in order I learned to check**: for any {werkvoorraad, tracking_table, content_table} trio, run these six counts:
  1. **Content with no tracking row** (orphan content). 6,484 in kopteksten, 11 in FAQ. Causes: one-off imports that populated content but forgot to register tracking (`import_content.py` did this in Jan 2026). Impact: URL is effectively done but batch can still pick it up again if absence-of-row = pending
  2. **Tracking `status='success'` with no content row** (missing output). 0 in this codebase — the upsert path is reliable. Would flag a broken insert branch
  3. **Tracking `status='failed'` with content exists** (bad state). 1,741 in kopteksten. Root cause pattern here was *the second UPSERT attempt failed with a duplicate-key error AFTER the first attempt's content had already landed in `content_urls_joep`* — leaving tracking='failed' lying about content that's actually fine
  4. **Unknown status values** not in the schema's comment (`pending|success|failed|skipped`). 3,152 rows with `status='completed'` — a non-standard term some historical import used. Doesn't block anything but a sign that status columns drift without enum enforcement
  5. **Duplicate content rows per URL**. 0 — the unique constraint on `url` holds
  6. **Tracking orphans** (tracking row references URL not in werkvoorraad). 0 in both
- **Plus the cross-system contradiction**: URLs appearing in `pa.url_validation_tracking` as `skipped|no_products_found` *while also* having generated content. 7,816 kopteksten + 23,853 FAQ. Not an error by itself — "content was generated earlier, URL was later skip-listed because products disappeared" is a valid timeline — but it means the content is stale and its source products are gone. Candidate for a sweep-and-retire (delete the content, since skipped URLs won't regenerate anyway)
- **Surprising count: 74,812 "untracked" werkvoorraad URLs for kopteksten**. These aren't a bug — they're just the pending backlog. But "74k" jumps out as "something wrong" until you remember `pending = absent tracking row` (see previous learning). Worth logging the distinction explicitly in any audit output so it doesn't look like a gap
- **Value of running all six in one script**: each query is 2-3 lines of SQL and runs in seconds even at 200k+ rows with the right `EXISTS`/`LEFT JOIN` shape. Running them as one block surfaces the full picture; running them one-at-a-time leads to tunnel vision on whichever bucket looks biggest first

## link_validator classifies these as "gone" — it's broader than "product was removed" (2026-04-20)
- **The `gone` definition, as implemented in `backend/link_validator.py`**: any of the following triggers gone:
  1. URL's pimId is not found in ES (the true "product was removed" case)
  2. URL is found but `shopCount < min_offers` (default 2) — product exists but has fewer than 2 offers, so validator treats it as retire-worthy
  3. URL's format can't be parsed into (maincat_id, pimId) via `extract_from_url` — i.e. `/p/slug/` without the maincat/pimId segments gets bucketed as gone even though the product likely exists at its canonical URL. This is the branch at `:334-335` in `lookup_plp_urls_for_content` and `:653-656` in `validate_faq_links`
  4. V4 UUID URLs where phase-1 `id`-based lookup misses — reliable because V4 UUIDs live in `id`, not `pimId`
- **Not classified as gone** (good): ES query exceptions (timeout, 500). The code explicitly prints `- skipping batch (not marking as gone)` and drops the link from the result dict. Prevents mass-false-positives during ES blips
- **Sample result on 200 random content rows (687 product links)**: 13 gone verdicts, 0 truly not-in-ES. 11 × `UNRECOGNIZED_FORMAT`, 2 × `shopCount=1`. So in practice the validator's "gone" output is dominated by categories 2 and 3 above, NOT category 1. The user confirmed (2026-04-20) that (3) — unparseable URLs — being flagged gone is intentional: reprocessing will emit a fresh valid link, which is what you want. (2) remains a design call: `min_offers=2` means a product with 1 offer triggers a rewrite, which is either "correct" or "too aggressive" depending on the brief
- **Practical implication**: before drawing conclusions from "validator flagged N URLs as gone," always decompose N by classification. Pull the sampled gone URLs back through ES directly (size=1 terms query on pimId/id, read `shopCount` + `plpUrl`) and bucket them: `TRULY_GONE_not_in_ES`, `LOW_OFFERS_sc=X`, `UNRECOGNIZED_FORMAT`, `NO_PLPURL`. Operators care about which bucket dominates, not the total — a 99 %-LOW_OFFERS result is a threshold conversation, a 99 %-UNRECOGNIZED_FORMAT result is a URL-shape conversation, and only a high share of TRULY_GONE_not_in_ES is actually "products disappeared"

## Taxonomy API: /api/CategoryFacets silently omits Dependent-inheritance facets; use /api/Categories/{id}.facets (2026-04-20)
- **The bug**: URL Validator's `get_category_facets` queried `/api/CategoryFacets?categoryId={id}` and got 11 facets for cat 9001287 (Lampen). But the category actually has 20 applicable facets — the other 9 (`pl_lamp`, `pl_camera`, `pl_hg`, `pl_klussen`, `pl_leifheit`, `pl_afwasmiddel`, `pl_wasmiddel`, `p_ladder`, `t_uvlamp`) all have `inheritanceStatus="Dependent"` and are silently filtered out. Symptom: real, working beslist.nl URLs using those facets got flagged `FACET_NOT_LINKED`. Ground truth is beslist.nl itself returning results for `/products/klussen/klussen_486171_486136/c/merk~486378~~pl_lamp~16018130`
- **Where the full list lives**: inline inside `/api/Categories/{id}.facets`. Each entry has a top-level `facetId` (not nested under `facet.id` like CategoryFacets), top-level `labels`, `isEnabled`, `noIndexNoFollow`, and an `inheritanceStatus` field that's one of `Direct`, `Inherited`, or `Dependent`. The inline list returns all three; CategoryFacets returns only the first two
- **Bonus**: the detail endpoint `/api/Categories/{id}` also has `isOverviewCategory` / `isStacked` / `parentId` fields and the inline facets — so calling it once covers both `get_category_detail` AND `get_category_facets`. Halves API round-trips for any URL with facets. `get_category_facets` now just pulls from `get_category_detail(cat_id)` which was already cached
- **How to diagnose this class of bug**: when a validator says "Facet X is not on Category Y" but beslist.nl serves the URL fine, the issue is almost certainly the taxonomy endpoint choice, not the validator logic. Check the `/api/Categories/{id}` inline facets array first — CategoryFacets is a subset
- **Response-shape gotchas worth noting**: `/api/Categories/{id}` returns a dict with `.facets`; `/api/CategoryFacets` returns a list (not wrapped in `{items:...}`). Passing an invalid cat_id to CategoryFacets returns 404 with a bare JSON string body (`"Category with ID X not found"`) — `r.json()` returns a str, not a dict, and any `.get()` on it blows up. Inspect `r.status_code` before `.json()` if validating unknown IDs
- **File**: `backend/url_validator_service.py:get_category_facets` — now calls `get_category_detail` and extracts `.facets` from the cached response

## Validators that return-early on a high-severity issue: downstream fixers must do their own lookups (2026-04-20)
- **Where it bit**: the URL validator's `validate_against_taxonomy` returns early on `MAINCAT_NOT_FOUND` — category lookup never runs, so the result has no `cat_name` populated and no `HIERARCHY_MISMATCH` is emitted. When I built `build_suggested_url`, I initially relied on `HIERARCHY_MISMATCH` being present to trigger the maincat-slug rewrite. That meant URLs like `/products/meubilair/mode_468972/c/...` (wrong maincat slug but right category) got no suggestion, because the validator bailed before it could detect the hierarchy problem — user's report: "suggested url is empty for all input urls"
- **Fix**: in `build_suggested_url`, do an independent `_cache.get_category(subcat_slug)` lookup whenever there's a subcat to work with, regardless of which issue codes fired. Derive the correct maincat from the category's own `maincat` field. This works whether the validator emitted `HIERARCHY_MISMATCH` (known bad pair) or `MAINCAT_NOT_FOUND` (unknown maincat slug but recognisable category)
- **General pattern**: when a validator chains checks and short-circuits on the first failure, any downstream consumer that wants to *fix* issues has to be self-sufficient — don't assume the validator has populated all the metadata it *could* have. Re-do the lookups. The extra CSV-dict `.get()` calls are cheap; the bug of relying on missing fields is expensive
- **Related pattern**: caches indexed by slug need a reverse-index for name→slug lookups. Added `TaxonomyCache.get_maincat_slug_by_name(name)` — linear scan over the ~30 maincats is fine; no point pre-computing a second index for a dict that size
- **Files**: `backend/url_validator_service.py:build_suggested_url`, `TaxonomyCache.get_maincat_slug_by_name`

## "Prefer whichever tab has content" leaks stale input from hidden tabs (2026-04-20)
- **The bug**: URL Validator had tabs Paste | Upload. After pasting 4 URLs + validating, user switched to Upload and loaded a 234k-URL file. Hit Validate — it processed the 4 manual URLs instead of the uploaded file. Root cause was a "prefer manual text if non-empty, else uploaded" fallback block that always fired regardless of which tab was active. The textarea retained its 4 URLs when the user switched tabs; that stale content won
- **The right model**: decide the input source *strictly* by which tab is active. `#manualTab.classList.contains('active')` → read textarea; else → read uploadedUrls. No fallback between them. Each path gets its own empty-state alert ("No URLs provided — paste URLs or switch to the Upload tab" / "No file uploaded — upload a file or switch to the Paste tab") so the user knows exactly what the current tab expects
- **Pattern**: any UI that offers "two ways to provide input X" (manual + upload, URL + form, etc.) needs a single source-of-truth for which way is active. Fallback logic ("if A is empty use B") feels friendly but creates ghost-state bugs the moment the user switches modes mid-flow. Tabs = hard selection
- **File**: `frontend/url-validator.html:startValidation`

## Uvicorn restart wipes in-memory caches — budget for a slow first run after every deploy (2026-04-20)
- **The pattern in practice**: URL Validator's `TaxonomyCache` is a module-level singleton holding `_category_detail`, `_category_facets`, `_facet_values`. CSV-backed maps (maincats, categories) survive reload because they're rebuilt from disk on first call. But the API-fetched dicts are purely in-memory — every Taxonomy API round-trip done during a run is lost at kill. First validate run after a restart re-fetches everything for any category/facet the URL set touches; for 234k URLs that's minutes
- **Symptom**: user reported "validator is suddenly much slower" after I pushed the Suggested URL feature. First guess: my new code. Reality: the restart I did to deploy it. CPU stayed around 4% because the backend was I/O-bound waiting on Taxonomy API responses, not CPU-bound on validation logic
- **Mitigation for the user**: subsequent runs with similar data are fast (cache warm). For the operator, the lesson is to **batch backend changes before restarting** — deploying 5 small backend fixes one-at-a-time means 5 cold-cache first-runs for whoever's using the tool. Bundle the restart to the end of a series of related changes
- **Longer-term fix options**: (a) persist `_category_detail`/`_category_facets`/`_facet_values` to disk, reload on boot — same pattern DMA+ uses for `dma_plus_history.json`; (b) pre-warm the cache on import (fetch for every category in `cat_urls.csv`) — probably too slow at boot; (c) add a "warm the cache" admin endpoint to kick off after a deploy. Option (a) is the cleanest; the API responses are effectively static reference data
- **Related existing note**: the 2026-04-19 entry about uvicorn running without `--reload` — same root cause, different symptom. The full deployment story: kill uvicorn → cache wiped → first user eats the API round-trip cost

## Rendering 40k+ table rows freezes the browser — cap + batch the DOM writes (2026-04-20)
- **The symptom**: URL Validator with 44k results popped a "page unresponsive" dialog and stayed laggy even after recovery. Root cause is the same one that bites every "just loop through results" renderer: per-row `document.createElement` + `appendChild` in a `forEach`, each row also getting an `addEventListener`, repeated 44k times. Browser commits a layout/style recalc after each append; memory for 44k listeners alone is non-trivial
- **Three-part fix, in order of impact**:
  1. **Cap rendered rows** — slice to `MAX_RENDERED_ROWS = 1000` with a "showing X of Y" notice. The full data stays in `allResults` for filtering/export; the DOM only ever sees 1000
  2. **One `innerHTML` assignment** instead of N appendChilds. Build a `parts = []` array of template strings, `tbody.innerHTML = parts.join('')` at the end. Orders of magnitude faster because the browser does one parse+commit instead of 44k
  3. **Event delegation** on `<tbody>` — one `click` listener that walks up to the nearest `tr.url-result-row` via `closest()`. Zero per-row listeners. Gate with `tbody.dataset.delegated = '1'` so re-renders don't stack listeners
- **Pagination is the natural second step**: once you've proven the page freezes at N rows, adding pagination (25/50/100 per page) makes the cap unnecessary — each page is at most 100 rows and feels instant. The mc-id-finder pattern (`frontend/mc-id-finder.html:142-153`) is the reference: `<select id="perPage">` + SVG prev/next `.btn-page` buttons + `<span class="page-info" id="pageInfo">` showing "X-Y of Total". Ported verbatim to url-validator
- **Watch for**: `filterResults` that re-runs `renderResults(subset)` on every filter click also triggers the freeze if you haven't capped/paginated. Reset `currentPage = 1` on filter change so users don't land on an out-of-bounds page
- **Files**: `frontend/url-validator.html:renderPage/renderRows/filterResults`

## The dm-tools orange is #CC5500, not Bootstrap's #fd7e14 (2026-04-20)
- **Where it lives**: `frontend/css/style.css:7` — `--color-button: #CC5500` (burnt orange), `--color-button-hover: #E97451` (coral). Every "primary" button across the dashboard uses these; Bootstrap's default orange (`#fd7e14`) is noticeably brighter and looks off next to the nav/body chrome
- **Pattern**: before hardcoding any brand color, `grep -n "color-button\|--color-" frontend/css/style.css`. The CSS vars are the source of truth; hand-picked hex values drift
- **Applied**: active-state filter buttons (All/Valid/Warnings/Errors) in url-validator — used `#fd7e14` first pass, user flagged the mismatch, swapped to `#CC5500`

## One horizontal scrollbar at the table level, not per-cell (2026-04-20)
- **The trap**: giving a single cell (e.g. the Issues column with long wrapped text) its own `overflow-x: auto` wrapper creates a tiny per-row scrollbar. Visually noisy (one per row) and awkward UX
- **The right layer**: Bootstrap's `.table-responsive` parent already provides ONE horizontal scrollbar at the bottom of the whole table when content overflows. So the fix for "long content in one column" is: set `white-space: nowrap` on the cell (or all cells) and let the whole table get wider; the single bottom scrollbar handles it
- **Applied**: url-validator Issues cell — removed the `.issues-scroll` wrapper div entirely, kept `white-space: nowrap` on `.url-result-row td`

## Uniform table row heights require nowrap on EVERY cell (2026-04-20)
- **Why rows came out unequal**: `height: 38px` on `.url-result-row` sets a *minimum* — any cell that wraps its text (long maincat name, two issues stacked, etc.) blows the row taller. One tall cell breaks the row; rows with short cells stay at 38px; result = visibly uneven table
- **Fix**: `white-space: nowrap` on `.url-result-row td` AND on `thead th`. Now nothing in any row can wrap; the row is always exactly the height of a single line of its tallest-font-sized cell. Combined with the fixed `height: 38px`, every row is identical
- **Pattern**: any "fixed row height" requirement on an HTML table needs nowrap on every cell to work. `height` alone is necessary-but-not-sufficient

## Dashboard layout consistency: narrow centered column + DMA+-style action bar (2026-04-19)
- **Pattern**: every tool page in this dashboard should open with `<div class="container mt-4"><div class="row"><div class="col-md-10 mx-auto">…</div></div></div>`. Pages that skip the inner `row/col-md-10` (full `container` width) stick out visually even when nothing else is wrong — the user noticed dma-bidding was wider than dma-plus, faq, canonical, 301-generator, rfinder, etc. Grep `col-md-10 mx-auto` to enumerate; anything without it in the content area needs wrapping
- **Action bar convention**: primary CTA + dry-run / options live in a single right-aligned flex row: `<div class="d-flex justify-content-end align-items-center gap-3"><form-check…><button…>`. DMA+ (`dma-plus.html:191-198`) is the reference. Dry-run-by-default (`checked`) is the safer default for anything with Google Ads blast radius — the native `confirm()` in the submit handler is the live-mode guardrail
- **Disable-until-relevant gating**: form controls that only make sense with some input should be `disabled` + `opacity: 0.5` by default, re-enabled via the parent input's `oninput`/`onchange`. Use inline style toggling rather than a separate CSS class — simpler, no selector plumbing, and the dim effect is visually enough to signal "not in scope yet." See `updateFilterModeState()` in dma-bidding for the pattern
- **Responsive tables in narrow columns**: once the content column is narrowed to `col-md-10`, tables with many columns can overflow. Wrap in `table-responsive` (Bootstrap) AND set `min-width` on the `<table>` to force horizontal scrolling past the column edge rather than squishing unreadably. dma-bidding history table uses `min-width: 720px`
- **Silent bug to watch for when refactoring tables**: the "Loading…" placeholder row's `colspan="N"` must match the real column count — easy to forget when you add/remove a `<th>`. Dashboard had dma-bidding showing `colspan="6"` for an 8-column table for some time before it got noticed

## Server restart required: the dashboard uvicorn runs without --reload (2026-04-19)
- **Root cause of "my fix didn't work"**: the Windows Task Scheduler job that auto-starts the dashboard on boot launches `venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8003` *without* `--reload`. So code changes pushed to main don't reach the live server until the uvicorn process is killed and respawned — the dev-time "FastAPI auto-reloads on edit" assumption doesn't hold here
- **Symptom**: persisted history showed a validate_trees run at 16:33 (after my 15:40 code push) with the OLD affected-entity shape. `ps aux | grep uvicorn` showed start time `04:07` — well before the push — and no `--reload` in the args
- **How to verify live code matches the repo**: `ps aux | grep uvicorn` for the start time; any persisted artefact (history JSON, logs, DB row) dated after your push that still reflects old behaviour means the process didn't reload. Then `kill <PID> && nohup venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8003 > /tmp/uvicorn.log 2>&1 &` to respawn
- **Longer-term fix options** (not done yet): add `--reload` to the Task Scheduler command (watches file mtimes, adds a small memory footprint and a dev-flavour restart on every save — probably fine for this deployment), OR wire a post-push git hook / webhook that restarts the service
- **File**: `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` (auto-start script mentioned in PROJECT_INDEX.md)

## Swapped executemany tuple order silently breaks UPDATE (2026-04-18)
- **The bug**: `cur.executemany("UPDATE ... SET content = %s WHERE url = %s", [(url, content), ...])` — the tuple was `(url, content)` but the positional SQL expected `(content, url)`. psycopg2 binds positionally, so the UPDATE ran as `SET content=<url-string> WHERE url=<html-string>` every time — zero row matches, zero rows updated, conn.commit() succeeds with `rowcount=0`, no exception, no log. Every link-correction run since the code was written was a no-op. Two sites in `main.py` (`/api/validate-links` and `/api/validate-all-links`) had the same swap because the second endpoint was copy-pasted from the first
- **Why it hid**: `executemany` doesn't raise on 0-row updates. The surrounding code counted "URLs corrected" from the in-memory `urls_corrected` counter, which was incremented *regardless of DB outcome*. So the status dashboard reported "corrected 47 URLs" while the DB had 0 updates. Only a content audit would catch it
- **Pattern for any `executemany` UPDATE/DELETE**:
  1. The positional order of the SQL (`SET a=%s, b=%s WHERE c=%s`) must match the tuple order pushed into the list. Write one before the other and match them deliberately
  2. Prefer named parameters (`SET content = %(content)s WHERE url = %(url)s` with list-of-dicts) — psycopg2 supports them and they survive reordering
  3. If you *must* use positional binding in executemany, put a `# tuple order: (content, url)` comment on the list-append line and never edit one without the other
- **Generalisation**: whenever a symptom is "the UI says it worked but the data didn't change," suspect silently-zero-row mutations. Log `cur.rowcount` after every `executemany` that's supposed to mutate, or assert it matches `len(updates)` when you know no concurrent deletes happened
- **File**: `backend/main.py:/api/validate-links`, `backend/main.py:/api/validate-all-links`

## CREATE TABLE IF NOT EXISTS never adds columns to an existing table (2026-04-18)
- **The bug**: Thema Ads service `INSERT INTO thema_ads_jobs (..., batch_size, is_repair_job, theme_name)` against a schema whose `CREATE TABLE IF NOT EXISTS` block never listed those columns. On any fresh DB, job creation fails with `column "batch_size" does not exist`. Live DBs had the columns because someone added them by hand during the original rollout — that manual ALTER never made it into the init script, so the codebase's source-of-truth schema drifted from reality
- **Fix shape**: every column added after initial rollout needs an explicit `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` beside the `CREATE TABLE IF NOT EXISTS`. Columns in the CREATE block are only applied on first creation; they're skipped forever after. Postgres 9.6+ supports `ALTER TABLE ... ADD COLUMN IF NOT EXISTS col TYPE [DEFAULT ...]` in a single statement with multiple columns, so you can stack them
- **Project caveat**: there are *two* db-init paths in this repo — `backend/thema_ads_db.py:init_db` and `backend/database.py:init_db` — and they both define the same tables. Any schema migration has to land in both or the one that runs first "wins" and the other silently diverges. Long term this should be consolidated to one; for now, keep the ADD COLUMN IF NOT EXISTS blocks aligned in both files
- **Pattern**: when you add a column to an already-shipped table, grep the whole repo for `CREATE TABLE IF NOT EXISTS <tablename>` and add the `ALTER ... ADD COLUMN IF NOT EXISTS` next to *each* one. A single source-of-truth schema file would be cleaner, but that's a bigger refactor — until then, discipline
- **Files**: `backend/thema_ads_db.py`, `backend/database.py`

## Pool-returning connection vs raw connection: know which you have (2026-04-18)
- **The bug**: `backend/database.py:init_db` did `conn = get_db_connection()` (pool) then `conn.close()` at the end (close raw). `conn.close()` on a pooled connection destroys it — the pool's `putconn` is what returns the slot. Under frequent dev restarts this slowly drained the 2–20 connection pool until startup-time init couldn't get a connection
- **How to tell them apart in this repo**:
  - `backend/database.py:get_db_connection` → pool-backed, must `return_db_connection(conn)`
  - `backend/thema_ads_db.py:get_db_connection` → direct `psycopg2.connect`, `conn.close()` is correct (pooling was disabled there with a comment explaining why)
- **Pattern**: when a module exports `get_X_connection` and `return_X_connection`, the pair signals pooling. Call `return_X_connection`. If a module exports only `get_X_connection`, read the body — pooling may or may not be in play
- **Bigger lesson**: having two `get_db_connection` functions in sibling modules *with different semantics* is the real trap. Agents/humans pick the wrong one by importing the closest. Consolidating is on the backlog
- **Files**: `backend/database.py:init_db`, `backend/thema_ads_db.py`

## Frontend XSS via `innerHTML = "...${error.message}..."` (2026-04-18)
- **The pattern**: `resultDiv.innerHTML = \`<div class="alert alert-danger">Error: ${error.message}</div>\`` is ~40 copies across app.js / faq.js / thema-ads.js and several HTML files. `error.message` can be anything the backend puts into an HTTPException `detail` — and FastAPI echoes user input into errors (e.g. "Invalid URL: <script>…"). Click-through of a malformed URL + rendered error = stored-XSS-adjacent risk
- **Fix shape**: a shared `escapeHtml` helper at the top of each JS/HTML script tag (no build step in this project, so no single import point), then `${escapeHtml(error.message)}` everywhere the error lands in innerHTML. `data.error`, `data.detail`, `errors.join(', ')` — same treatment because they're all server-echoed strings. The grep query `innerHTML\s*=.*\$\{[^}]*error` surfaces every instance
- **Longer-term**: a real fix is to swap the `<div><alert>` error pattern to `textContent` assignment on a pre-built alert element. But that's a bigger UI refactor; inline `escapeHtml` is good enough until there's a reason to
- **Pattern beyond errors**: any `${foo}` interpolated into `innerHTML` where `foo` comes from the server (filenames, URLs, shop names, custom_id) is a risk. The innocuous-looking `${row.url}` in a results table can be the same bug if the backend echoed a user-uploaded URL

## Prefer structured results over log-parsing when the processor already has them (2026-04-18)
- **Symptom**: `validate_trees` dry-run export only contained campaign names — ad group and tree columns were empty. Root cause was the usual shared-parser mismatch: `validate_trees` logs `📁 Campaign: PLA/X` (no `(N ad group(s))` suffix) and 3-space-indented `   🔧 PLA/ag_a: …` status lines, but `_parse_affected_entities` requires the exclusion-style `(N ad group(s))` header and `\s{4,}` ad-group indentation. Campaigns limped through via the broad `r'campaign.*?(PLA/[^\s,()]+)'` fallback; ad groups and tree descriptions didn't
- **Anti-fix**: changing the log format to appease the parser (add more spaces, add the `(N ad group(s))` suffix) would've worked but is the wrong direction — it bakes parser assumptions into every processor's `print()` statements. Each new processor has to remember the format or regress the export
- **Fix**: `validate_trees` already populates `stats['details']` with `{campaign, ad_group, status, message}` per ad group — a structured table the processor builds naturally. In `_run_operation`, after the default `_parse_affected_entities(full_log)` runs, detect `operation == "validate_trees"` and *override* `affected` by iterating `result_data["details"]`. Split into `campaign_ad_group_pairs` (created/error → main Campaigns sheet) and `skipped_pairs` (skipped → dedicated Skipped sheet), then drop both into `affected` alongside the existing `campaigns/ad_groups/trees` sets. Frontend `exportRow` adds a new Skipped sheet when `aff.skipped_pairs` is present. Files: `backend/dma_plus_service.py:_run_operation` (~30-line override block after the parser call), `frontend/dma-plus.html:exportRow`
- **Pattern**: a log parser exists because some processors don't expose structured output. When a processor *does* have structured details (stats rows, validation results, etc.), bypass the parser for that operation instead of retrofitting the log format. The log stays human-friendly and the export gets richer data. Every `validate_*` operation in this codebase has a `details` list — they're all candidates for this override pattern if their exports look sparse
- **Skip categorisation in validate_trees**: the three skip branches all funnel into `status='skipped'`, differentiated by the `message` field:
  - `"Campaign name doesn't start with PLA/"`
  - `"No _a/_b/_c suffix in campaign name"` (no `_` after `PLA/`)
  - `"Suffix 'X' is not a/b/c"` (last-`_` suffix like `_store`, `_1`, `_d`)
- Giving Skipped its own sheet rather than lumping into Campaigns keeps the "faulty trees you should act on" (errors, dry-run proposals) visually separate from "not our convention, no action needed" rows — the user was looking at 1395 skipped of 12051 total, which would swamp the 990 real errors if intermixed

## Honest dry-run requires stubbing resource names, not just skipping writes (2026-04-17)
- **Shape of the problem**: `process_inclusion_sheet_v2` makes a chain of 5 Google Ads mutation calls — create campaign → attach negative list → create ad group → build listing tree → create product ad. Steps 3, 4, 5 consume a resource name returned by step 1 or 3; downstream code extracts IDs via `.split('/')[-1]`. A naive dry-run that just `if dry_run: return` on each helper would break the chain with `None` substrings
- **Fix**: synthesize plausible fake resource names (`customers/{cid}/campaigns/DRY_RUN_<uuid.hex[:10]>`, same shape for ad groups) and keep them flowing through the code. The `.split('/')[-1]` still yields `DRY_RUN_<hex>` which is a valid string for log lines. Calls that don't return anything load-bearing (negative list, tree build, ad creation) get a simple skip with a `[DRY RUN] Would ...` print so the log still shows what *would* have happened
- **Also skip the `time.sleep()`s between mutation steps in dry-run** — they exist for API rate-limiting, which obviously doesn't matter when no writes happen. Skipping turns a ~30s live run into a sub-second dry-run
- **Pattern**: when retrofitting dry-run onto code that wasn't designed for it, wrap the call sites in the caller, not the helpers. Helpers stay pure and reusable; all the dry-run conditionals live in one place. Only push `dry_run` into a helper when the helper has side-effects that need different code paths (e.g. a batch mutator that needs to construct a plausible success response)
- **File**: `backend/campaign_processor.py:process_inclusion_sheet_v2` (~40 lines of wrapping), same pattern in exclusion / reverse_inclusion / reverse_exclusion

## Google Ads Label description lives on `text_label`, not Label (2026-04-18)
- **Symptom**: `LabelService.mutate_labels` threw `Unknown field for Label: description` when setting `op.create.description = "..."`. Silently caught, so the DM_DASHBOARD label was never created — and therefore never attached to any campaign/ad-group, which the user only noticed via a stray log line
- **Fix**: Description (and background color) live on the `text_label` sub-message. Use `op.create.text_label.description = "..."` instead
- **Pattern**: when the Google Ads proto has a sub-message named after the parent type (e.g. Label has `text_label`, Campaign has `app_campaign_setting`, etc.), most "interesting" fields live inside that sub-message. The bare field on the parent is usually just `name` / `id` / `status` / `resource_name`. Before assuming a field sits directly on the top level, check the swagger / proto definition

## "Successful row" counters can hide zero-action runs (2026-04-18)
- **Bug class**: `process_reverse_exclusion_sheet` marks a workbook row TRUE if the underlying batch call returned `success > 0 OR not_found > 0` AND `errors == 0`. The intent — "the shop is no longer excluded for any reason" — is sensible, but it conflates "we removed N exclusions" with "there were 0 exclusions to remove". User saw "3 ok, 0 failed" and assumed the 282×3 wibra.nl exclusions were gone; they weren't
- **Fix**: keep the per-row counter but add **run-wide action counters** alongside it: `run_total_batch_calls / run_total_removed / run_total_already_not_excluded / run_total_mutate_errors`. Render them as separate summary lines, and emit a clear warning when `removed == 0 && batch_calls > 0`. Also walk `GoogleAdsException.failure.errors` in the batch helper so error messages aren't truncated to 100 chars
- **Pattern**: a counter is honest only if it counts the thing the user thinks it counts. When the underlying API has multiple "success-shaped" outcomes (removed, was-already-removed, no-op, etc.), surface them separately. The single "Rows OK" number is fine for at-a-glance UI but should never be the only signal in the log
- **Verification approach that worked**: when the user reported "the script claimed success but didn't actually mutate," the diagnostic path was (1) replicate the batch helper's read+filter step in isolation against the live ad group → confirms it identifies the right resource_name; (2) call `mutate_ad_group_criteria` directly with that exact resource_name → confirms the API works; (3) call the full batch helper in isolation on a different ad group → confirms the helper end-to-end; (4) call `process_reverse_exclusion_sheet` itself on a small slice → confirms the orchestration. Each step narrows the possible failure surface
- **File**: `backend/campaign_processor.py:reverse_exclusion_batch`, `process_reverse_exclusion_sheet`

## A shared log parser needs coverage for every processor's log format (2026-04-17)
- **Pattern observed three times this session**: new mutating operation added → export rows look empty for that op → root cause is the processor prints its own flavor of `📁 Campaign: …` / `Creating campaign: …` / `CAMPAIGN N/M: …` / `📁 Campaign: PLA/X (N ad group(s))` and my parser only knows a subset. Each one cost a round-trip with the user before I checked the log format
- **Fix cadence that works**: before adding a processor to DMA+, grep its `print(f"...")` statements for campaign-name and ad-group-name lines and cross-check the patterns in `_parse_affected_entities`. Header variants I've now catalogued:
  - `    📁 Campaign: PLA/X (N ad group(s))` — exclusion / reverse_exclusion
  - `CAMPAIGN 1/3: PLA/Klussen store_a` — reverse_inclusion
  - `   Creating campaign: PLA/Klussen store_a` — inclusion
  - `   ──── Ad Group: PLA/wibra.nl_a ────` — reverse_inclusion
  - `   ──── Ad Group 1/3: PLA/wibra.nl_a ────` — inclusion
  - `      ⏭️/✅/❌ PLA/X: ...` — exclusion / reverse_exclusion status lines
- **Pattern**: when a shared downstream consumer (parser, exporter, summarizer) touches output from multiple producers, changes to any producer's output format are a silent regression risk. Worth a single "log-format contract" comment block above `_parse_affected_entities` listing every expected format, so whoever adds the next processor has a checklist
- **File**: `backend/dma_plus_service.py:_parse_affected_entities`

## Log-parsing regexes trip on entity names with spaces (2026-04-17)
- **Bug**: exported DMA+ Affected Campaigns only showed `PLA/Klussen` when the run had processed `PLA/Klussen store_a`, `PLA/Klussen store_b`, `PLA/Klussen store_c`. Three distinct campaigns collapsed to one export row. Parser regex was `r'campaign.*?(PLA/[^\s,()]+)'` — the `[^\s]` stops at the first space, truncating `"PLA/Klussen store_a"` → `"PLA/Klussen"`, and the `campaigns = set()` dedupes to one
- **Fix**: two specific patterns per log format, broad fallback only if they miss. For reverse-inclusion's `CAMPAIGN N/M: PLA/X store_a` header: `r'^\s*CAMPAIGN\s+\d+/\d+:\s+(PLA/.+?)\s*$'` (greedy to EOL). For exclusion's `📁 Campaign: PLA/X (N ad group(s))`: `r'Campaign:\s+(PLA/.+?)\s+\(\d+\s+ad\s+group'`. Both tolerate spaces inside the name because they anchor on a distinctive terminator (end-of-line, or `" ("`)
- **Pattern**: parsers should anchor on *stable boundaries* in the log (distinctive surrounding tokens), not assume the thing in the middle matches a character class. When adding a new log format, write the regex against real log samples — don't guess at field shapes
- **Also**: walk the log linearly when you need to pair entities (campaign → its ad groups). Regex-over-every-line gives you flat sets; a running "current campaign" cursor gives you pairs. This is what made the paired Campaigns export work across two different log formats with the same code
- **File**: `backend/dma_plus_service.py:_parse_affected_entities`

## Persist small dev state (history, cache) even when the "real" store is a DB (2026-04-17)
- **Context**: DMA+ Change History lived in a module-level `deque(maxlen=200)`. Every uvicorn restart wiped it — painful during active development when I'm restarting 3-5 times per session
- **Why not Postgres**: the project *has* a remote PG (`10.1.32.9`) via `backend.database`. But a schema migration + connection pooling + audit of how other modules read history would have been overkill for ~200 small JSON-serializable records
- **What worked**: `backend/data/dma_plus_history.json` — load-on-import via `_load_history_from_disk()`, save-on-mutate via a `_history_append(entry)` helper that holds a `threading.Lock`. `clear_history()` also persists the empty state. Total ~25 lines, works instantly across restarts
- **Pattern**: for small, append-mostly, process-local state, a JSON file in `backend/data/` beats a DB table for dev ergonomics. Promote to PG later if it grows or needs cross-process sharing. The key is not fighting the in-memory deque — keep it as the source of truth during a process's life, and treat disk as the rehydration mechanism
- **Gotcha**: when you forward mutations to helpers, make sure *every* mutation site funnels through the persistent wrapper. I had three `_history.appendleft(...)` call sites (completion / cancellation / error) and had to update all of them to `_history_append(...)` — it's easy to miss one and wonder why some history survives restart but some doesn't
- **File**: `backend/dma_plus_service.py:_load_history_from_disk`, `_history_append`

## Taxonomy API: `isBiddingCategory` exists but DMA campaigns aren't on bidcat level (2026-04-17)
- **Initial hypothesis (WRONG)**: DMA campaigns in Google Ads are named `PLA/{bidcat}_{cl1}`. I built a bidcat walker and swapped the processor to use it — user tested and the deepest_cat lookup was actually correct all along. Reverted the same session
- **What the walker change was doing**: `GET /api/Categories/{id}` returns a CategoryDto with `isBiddingCategory` populated on the TOP-LEVEL object. The embedded `subCategories` array entries carry only `id, parentId, isEnabled, labels` — **no `isBiddingCategory`**. The flat `/api/Categories?rootCategoriesOnly=false` endpoint ignores the param and returns only root cats (tried three boolean encodings, all returned 32). So to enumerate bidcats you'd have to walk the tree and fetch each category's detail individually — Klussen subtree alone = ~284 GETs (~19s with Session reuse; full taxonomy ≈ 10 min cold)
- **Pattern — don't act on architectural hypotheses without verification**: the user mentioned "DMA campaigns are on bidcat level" and I immediately restructured the fetch around that claim. I should have first: (a) sampled a few Google Ads campaign names, (b) checked whether those names match deepest_cat *or* bidcat, and (c) only then made the change. Instead I spent a commit + service restart on an assumption that turned out wrong
- **If reviving this**: the bidcat walker idea works mechanically but isn't the right semantic for this processor. Keep `_fetch_subcategories_recursive` (deepest-cat) as the authoritative walker
- **File**: `backend/dma_plus_service.py:215-265` (the original deepest-cat walker, restored)

## Log truncation for display ≠ log truncation for parsing (2026-04-17)
- **Bug**: DMA+ exclusion export showed only ~22 campaigns when the run processed hundreds. Affected-entity regex was being applied to `result_data["log"]` which had already been sliced to the last 5000 chars for UI display
- **Fix**: Keep a separate `full_log = captured.getvalue()` local variable per branch; let the display `"log"` field stay truncated, but parse downstream artifacts (affected campaigns, counts, anything for export) from `full_log`
- **Pattern**: whenever you truncate stdout capture for a UI field, the parser must run on the pre-truncation value. Name the two explicitly (`full_log` vs `display_log`) so it's obvious which is which and future branches can't accidentally reuse the truncated one
- **File**: `backend/dma_plus_service.py:380-523`

## FastAPI async handlers block the event loop when they call sync I/O (2026-04-17)
- **Symptom**: DMA+ Start button felt dead — POST `/api/dma-plus/start` took minutes to return because `start_operation` synchronously called `_build_exclusion_workbook → _populate_cat_ids_sheet → _fetch_all_cat_ids_from_taxonomy_api` (a recursive Taxonomy API walk). Worse: while the event loop was blocked the Cancel button was also useless — frontend only sets `currentTaskId` *after* the start response, so cancel had nothing to target
- **Fix**: `start_operation` now only seeds the task record (`status:"queued"`) and spawns the background thread. ALL heavy work (maincat resolve, workbook build, Google Ads client init) moved into `_run_operation`. POST returns in <10ms with a task_id that Cancel can immediately target
- **Pattern**: `async def` handlers that do blocking I/O are worse than sync handlers — sync handlers get a threadpool slot, async handlers hog the loop. If a handler must do slow work, it should hand it off to a thread/task and return an ID. "The response doesn't need the result" is the tell that background execution is right
- **Cancel plumbing**: flip a flag on the task record and have the worker `_check_cancelled(task_id)` at every phase boundary. Raising a custom `TaskCancelled` exception keeps the control flow clean. Widen the allowed-to-cancel statuses to include `queued`/`initializing`, not just `running`, or users can't cancel during the slow init phase
- **Still missing**: once control is inside a monolithic sync function (e.g. `cp.process_exclusion_sheet_v2`), you can't cancel without a callback threaded through that function. Flagging this as a known limitation is better than pretending cancel works everywhere
- **File**: `backend/dma_plus_service.py:570-616, 307-395`

## xlsx over CSV kills Excel's Windows-1252 mojibake (2026-04-17)
- **Symptom**: DMA+ CSV export displayed `→` as `â†’`, `✅` as `âœ…`, etc. Backend data was correct UTF-8; Excel was opening the CSV as Windows-1252 and mis-decoding every multi-byte character
- **Fix**: Switch output to `.xlsx` via SheetJS (`XLSX.utils.aoa_to_sheet → book_append_sheet → writeFile`). Xlsx stores strings as UTF-8 inside XML parts of the zip; Excel always decodes them correctly. No BOM needed, no locale-dependence
- **Bonus**: multi-sheet support came for free — split the one noisy CSV into `Status` + `Campaigns` tabs
- **Alt fix** (kept in back pocket): prepend `\ufeff` BOM to the CSV Blob. Works but fragile — some tools strip BOMs, and it doesn't fix column-formatting issues
- **Pattern**: for any user-facing tabular export containing non-ASCII characters (arrows, emoji, accented names), default to xlsx. CSV is fine for ASCII or data pipelines that explicitly declare encoding

## Blocking `<script src>` stalls the whole page when the CDN is unreachable (2026-04-17)
- **Symptom**: Adding `<script src="https://cdn.sheetjs.com/...">` before the inline `<script>` made the Start button do nothing after page load. The inline script couldn't parse until SheetJS finished downloading, so `startOperation` was undefined when the onclick fired — silently no-op, no console error
- **Fix**: add `async` to the CDN script. It loads in parallel and doesn't hold up the inline script. Protect any callsite that needs the library with a guard: `if (typeof XLSX === 'undefined') { alert('Excel library still loading — try again in a second.'); return; }`
- **Pattern**: third-party CDN scripts should always be `async` or `defer` unless the inline script directly needs a symbol from them. The worst-case failure mode (corp network blocks the CDN) is "nothing on the page works, no error" which is painful to debug

## Two-stage cap: validate before you truncate (2026-04-17)
- **Bug**: n8n IndexNow submitter fetched top-10K URLs from Redshift, then validated supplier count. If N of those 10K failed validation, the run submitted only `10K − N` URLs — URLs 10001+ that might have passed were never considered
- **Fix**: fetch 15K (headroom), run validation on ALL of them, THEN cap to 10K. Report `rejected (<3 suppliers)` and `truncated (daily cap, post-validation)` as separate numbers in the Slack summary
- **Pattern**: when a pipeline has (1) a natural input limit and (2) a filter that discards some inputs, always filter-then-truncate, not truncate-then-filter. The other ordering silently wastes quota and is invisible unless you instrument both counts

## One repo serving two environments — env-gated features beat two forks (2026-04-15)
- **Context**: `dm-tools` (localhost, 8003, no auth) and `dm-dashboard` (networked, 3003, password-protected, Windows Task Scheduler UI) had drifted into two parallel repos with ~11 differing files. Same project, different deployment constraints. The old workflow was "commit to both repos after every change" which is error-prone and was already producing divergent features
- **Consolidation approach**: dm-tools absorbed every dashboard feature, but behind env vars:
  - `DASHBOARD_PASSWORD` empty → middleware is a pass-through; set → login required
  - `ENABLE_TASK_SCHEDULER=false` (default) → router not mounted, `/api/config` returns `task_scheduler_enabled:false`, and a small frontend script hides the Automation card in `dashboard.html`
  - `CORS_ORIGINS` unset or `*` → permissive; comma-separated hosts → restricted
  - `BASE_URL` and `DISABLE_SSL_VERIFY` in `daily_automation.py` → same script works for http://localhost:8003 and https://win-htz-006.colo.beslist.net:3003
- **Why this is better than two forks**: one code path to test, one set of commits, no "which repo has the latest fix" ambiguity. Every new feature only has to be written once
- **Gotcha**: creating tables (e.g. `scheduled_tasks`) unconditionally is fine — they're dormant when the feature is off. Mounting routers conditionally is cleaner than nesting `if env_var:` inside every handler
- **Frontend feature flags via `/api/config`**: any new env-gated UI now just `fetch('/api/config').then(cfg => { if (cfg.feature) show(el) })`. No build step needed for a vanilla-JS frontend
- **Pattern**: when two deploys of the same app diverge, unify via env flags before the drift becomes unmanageable. Each additional month of "two forks" makes the merge harder

## Git remote swap for a canonical-repo switch (2026-04-15)
- **Goal**: make `joep-1993/dm-dashboard` the canonical push target without moving files or re-cloning
- **Steps used**:
  1. Force-pushed consolidated history to dm-dashboard with `--force-with-lease=main:<old-hash>` (safety catch — fails if the remote moved since last fetch)
  2. `git remote rename origin dm-tools-old`
  3. `git remote rename dm-dashboard origin`
  4. `git branch --set-upstream-to=origin/main main`
- **Now plain `git push` / `git pull` target dm-dashboard**. The old remote is kept under a new name for reference (will be removed when dm-tools GitHub repo is archived)
- **Pitfall hit**: `--force-with-lease=main:21eb8dc` (short hash) failed with "stale info". Had to use the full 40-char hash from `git rev-parse dm-dashboard/main`. `--force-with-lease` needs the exact SHA string
- **Pattern**: use `--force-with-lease` (never plain `--force`) when rewriting shared remote history — it refuses to overwrite if someone else pushed in between

## FAQ prompt — missing facet context for filtered pages (2026-04-14)
- **Issue**: FAQs on faceted URLs (e.g. `/c/merk~819441`) had generic category questions instead of brand/facet-specific ones. Example: "AEG boormachines" page got "Wat is het voordeel van een accuboormachine?" with no mention of AEG
- **Root cause**: `selected_facets` was returned by `fetch_products_api` but never included in the FAQ prompt. The AI only saw the h1_title (which often contained the brand) but had no explicit instruction to write facet-specific questions
- **Fix**: Added `facet_context` (list of active filters) and a conditional instruction to both `faq_service.py` and `batch_api_service.py` — only injected when `selected_facets` is non-empty, so non-faceted pages are unaffected
- **Scale**: ~18K of 222K faceted FAQs (8%) had clearly generic questions (title words absent from all questions). True number likely higher since heuristic is conservative
- **Pattern**: When data is available in page_data but unused in the prompt, the AI can't be expected to know the page's filtering context. Always pass relevant metadata explicitly

## Kopteksten prompt — repetitive opening phrases (2026-04-14)
- **Issue**: Nearly all generated kopteksten started with "Bij het kiezen van een..." — monotonous output
- **Root cause**: The system prompt already banned "Als je op zoek bent naar" etc. but the model defaulted to another formulaic opener
- **Fix**: Added a soft discouragement (not a hard ban) to both subcategory and main category system prompts: "Vermijd ook om te vaak te openen met 'Bij het kiezen van' — gebruik dit hooguit af en toe, niet standaard"
- **Pattern**: When banning specific openings, the model often shifts to a new default. Soft variation rules ("use sparingly") work better than hard bans for preventing monotony without over-constraining

## Link validator — V4 UUID lookup used the wrong ES field (2026-04-13)
- **Bug**: `backend/link_validator.py:query_elasticsearch_by_plpurl` phase-1 lookup queried ES on `pimId`, but V4 UUIDs live in the `id`/`groupId` fields. `pimId` stores `nl-nl-gold-...` values, never `V4_...`. So the terms query *always* returned 0 hits
- **Impact**: *Every* V4 product link on content and FAQ pages was silently skipped by the validator — never replaced when slugs changed, never flagged `gone`, never triggering regeneration. This has been true for as long as the V4 branch existed in this file. Phase-2 wildcard fallback was disabled (it timed out), so the bug had no safety net
- **Fix**: Changed `"pimId"` → `"id"` in the terms query and source list. Also repurposed the phase-1 miss path: V4 UUIDs not returned by the `id` lookup are now marked gone (previously left out of the result dict entirely). ES request failures still skip the batch in the `except` branch, so transient errors can't cause spurious regeneration
- **Verification**: tested on `/products/mode/mode_432356/` (4 V4 links) — before: 0/0/0, after: 2 slug replacements + 2 gone URLs matching ground truth
- **Files**: `backend/link_validator.py` (one function, ~50 line diff). Same fix benefits `validate_faq_links` because it delegates to the same helper
- **Pattern**: when a schema sample disagrees with query assumptions, always verify field names against a live document before debugging around the symptoms

## thema_ads_* tables missing PKs → GROUP BY errors + broken inserts (2026-04-13)
- **Symptom**: startup log showed `Error cleaning up stale jobs: column "j.status" must appear in the GROUP BY clause`. `cleanup_stale_jobs` in `thema_ads_router.py` calls `thema_ads_service.list_jobs` which did `SELECT j.*, SUM(...) ... GROUP BY j.id`
- **Root cause**: `thema_ads_jobs` in the live DB had zero constraints — no PK, no unique, nothing. Postgres functional-dependency detection only accepts `SELECT j.*` with `GROUP BY j.id` when `id` is declared PK. The schema files (`thema_ads_schema.sql`, `thema_ads_db.py`, `database.py`) all declare `id SERIAL PRIMARY KEY`, but the live table was created without those constraints
- **Fix 1 — query**: rewrote `list_jobs` to pre-aggregate counts in a subquery on `thema_ads_job_items` and LEFT JOIN, eliminating GROUP BY on the outer query. Semantically identical
- **Fix 2 — schema**: added sequences, `SET DEFAULT nextval(...)`, PRIMARY KEYs on all three `thema_ads_*` tables (`thema_ads_jobs`, `thema_ads_job_items`, `thema_ads_input_data`), and FKs from the two child tables to `thema_ads_jobs(id)` with ON DELETE CASCADE. All three were empty so zero risk
- **Side effect fixed**: inserts without explicit id would have failed (column NOT NULL, no default). Now they auto-increment as intended
- **Pattern**: periodically verify that `CREATE TABLE IF NOT EXISTS` declarations in the code match actual live schema — the "IF NOT EXISTS" silently skips structural mismatches

## AI Titles — facets that should act as the category name (2026-04-13)
- **Context**: for some URLs the facet value is already the product noun (e.g. `t_wanddeco` → "wandplaten"), so appending the generic `category_name` ("Wanddecoratie") produces redundant titles like *"Acryl Metalen wandplaten Wanddecoratie kopen?..."*
- **Implementation**: added `CATEGORY_OVERRIDE_FACETS` set (currently `{'t_wanddeco'}`) in `improve_h1_title`. When any such facet is in `selected_facets`, the code strips `category_name` from the H1 (both prefix and suffix, case-insensitive) and clears the local `category_name` variable so downstream logic can't re-append it
- **Why a set, not a generic `t_*` rule**: some `t_*` facets are genuinely descriptors, not category-equivalents. Explicit opt-in keeps behavior predictable
- **Reset**: 61 URLs containing `t_wanddeco` reset to pending
- **Related pattern**: similar to the existing "Soort facet with product-type suffix" logic at lines 535-544, but generalized and explicit

## AI Titles — stijl adjectives must precede the noun (2026-04-13)
- **Issue**: with `stijl_test~8064049` (value "Industriële"), the AI placed the style adjective correctly ~97% of the time but sometimes at the end: *"Gouden Stoffen Verstelbare Barkrukken Industriële"*, *"Zwarte Grote Hoekbureaus 4 laden Industriële"*
- **Root cause**: prompt rule 4 only explicitly covered colors and materials. Style facets (`stijl_test`, `stijl_woonaccessoires`, `stijl`, `stijl_schoenen`, `stijl_tas`, `stijl_tegels`, `stijl_tuinart` — ~44k URLs across 7 families) had no explicit rule, so the model's placement was inconsistent
- **Fix**: extended rule 4 to name stijl adjectives with examples ("Industriële", "Moderne", "Scandinavische") and an explicit "NOOIT aan het einde van de titel" clause
- **Reset**: 1,994 URLs containing `stijl_test~8064049` (572 processed + 1,422 unprocessed) reset to pending
- **Pattern**: the AI follows explicit, example-backed rules far more reliably than implicit category-like behavior. When one facet family has a recurring placement issue, check whether the prompt names that family

## Uvicorn was running without --reload (2026-04-13)
- **Symptom**: code edits to `backend/*.py` didn't take effect until process restart
- **Cause**: scheduled-task startup script `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` ran `uvicorn backend.main:app --host 0.0.0.0 --port 8003` with no `--reload`
- **Fix**: added `--reload` to the ps1 script and restarted the process. Future edits hot-swap via WatchFiles
- **Also**: scheduled task `DM Tools Dashboard` logon trigger got a 10-minute delay (`<Delay>PT10M</Delay>`) so WSL/Ubuntu has time to be ready before uvicorn tries to bind :8003

## OpenAI Batch API — File Size Limit & Chunking (2026-04-10)
- **200MB limit** per batch file for gpt-4o-mini. 29K FAQ prompts with product data exceeded this
- **Fix**: Split into chunks of 5,000 requests each. For 29K URLs = 6 chunks, processed sequentially
- **Queue time**: OpenAI batch queue can be very slow — first run took ~8 hours (mostly queue waiting, not processing)
- **Chunk size**: 5,000 is conservative. Could potentially go higher but 200MB is hard limit

## Faulty URLs in unique_titles (2026-04-10)
- **158,742 URLs removed** from all 6 DB tables, exported to `~/faulty_unique_title_urls.xlsx`
- `/r/` URLs (143,626): product redirect URLs like `/products/fietsen/r/accu-slot/` — Product Search API can't parse these (no `/c/` facet path)
- `populaire_themas_accessoires` (8,134): invalid facet — API returns 400 "facet is not valid"
- `type_parfum` (6,901): invalid facet — same 400 error
- `pl_pennen` (90): invalid facet — same 400 error
- **Pattern**: When batch processing shows many `api_failed` or `facet_not_available` errors, check for systematic bad URL patterns to clean from DB

## Unique Titles Batch UI Race Condition (2026-04-10)
- **Problem**: Process All button turned yellow briefly, progress bar didn't show, `undefined` text appeared
- **Root cause**: `loadAiStatus()` polls `/api/ai-titles/status` every 2 seconds and resets the UI when `is_running: false`. The batch uses a separate state (`/api/ai-titles/batch-status`), so the normal status always shows idle → UI gets reset
- **Fix**: Set `aiBatchPolling = true` immediately on click (before fetch), then `loadAiStatus` returns early when flag is set. Also hide batch/workers inputs during batch run and restore in `resetAiBatchUI`

## Frontend Consistency Standards (2026-04-10)
- **Page widths**: All tools use `col-md-10 mx-auto`. Was inconsistent: unique-titles had `col-lg-8`, redirects/keyword-planner/url-checker/redirect-checker had `col-md-11`
- **Input fields**: Use `input-group` with inline label prefix (e.g., `<span class="input-group-text">Batch</span>`) — consistent across Kopteksten, FAQ, Unique Titles
- **Button alignment**: Inputs left, buttons right via `d-flex justify-content-between align-items-center`
- **Publishing section**: Dropdowns + last push timestamp left, Refresh Stats + Publish buttons right
- **Badge color**: `.badge.bg-success` was overridden to grey in `css/style.css` via `--color-section`. Fixed to `#198754` (Bootstrap green)
- **FAQ recent results X-button overflow**: Fixed with CSS grid (`grid-template-columns: 1fr auto`) + `overflow: hidden` on content div. Flexbox approach failed because long URLs could push the button out

## OpenAI Batch API Integration (2026-04-10)
- **Service**: `backend/batch_api_service.py` — bulk processing for FAQ and kopteksten via OpenAI Batch API
- **Endpoints**: `POST/GET /api/batch-start`, `POST/GET /api/faq/batch-start`, `/api/batch-status`, `/api/faq/batch-status`
- **Frontend**: "Bulk API" checkbox on FAQ (`faq.html`) and Kopteksten (`index.html`). When checked, greys out batch size, workers, single-batch button. Process All triggers batch pipeline
- **Flow**: Fetch pending URLs → Product Search API (50 threads) → build JSONL → upload to OpenAI → poll every 15s → download results → save to DB
- **Cost**: 50% cheaper than real-time API
- **Speed**: Prepare phase ~5-15 min (API calls), OpenAI processing ~15-60 min
- **State tracking**: Global `_batch_state` dict with thread lock, phases: preparing → uploading → processing → saving → complete/error

## Query Performance — LEFT JOIN vs NOT EXISTS (2026-04-10)
- **Issue**: FAQ URL selection query took 4.2s per batch due to LEFT JOIN across 3 large tables (werkvoorraad 280K, faq_tracking 255K, url_validation_tracking 86K)
- **Fix**: Converted to NOT EXISTS subqueries — 190ms (16.5x faster). Also fixed kopteksten query (7.7s → 2.9s)
- **Affected queries**: 4 total in main.py — FAQ URL selection, FAQ pending count, kopteksten URL selection, kopteksten pending count
- **Pattern**: Always prefer `NOT EXISTS` over `LEFT JOIN ... WHERE x IS NULL` on PostgreSQL for anti-joins

## Worker Limits & Connection Pool (2026-04-10)
- **OpenAI rate limits**: 30,000 RPM, 150,000,000 TPM for gpt-4o-mini — extremely generous
- **Bottleneck**: Each OpenAI call takes ~30s. With 20 workers = ~40 URLs/min. With 50 workers = ~100 URLs/min
- **Changes**: DB pool maxconn 20→60, worker limit 20→100 (backend + frontend), frontend defaults 20→50
- **Files changed**: `database.py`, `main.py` (6 validation checks), `index.html`, `faq.html`, `app.js` (4 checks), `faq.js` (3 checks)

## Winkel Facet URLs — No API Data (2026-04-10)
- **Issue**: 29,632 URLs with `winkel~` facet had titles that were just bare category names (e.g., "Zoogcompressen" instead of "Bol.com Zoogcompressen")
- **Root cause**: Product Search API returns empty facets array for winkel-filtered requests. The shop filter is applied but no facet metadata comes back
- **Decision**: Removed all winkel URLs from all 6 tables. Shop-specific pages don't need SEO titles
- **Tables cleaned**: unique_titles, content_urls_joep, kopteksten_check, faq_content, faq_tracking, werkvoorraad

## Title Scoring — Full Run Completed (2026-04-09)
- **Script**: `scripts/score_titles.py` — GPT-4o-mini, 25 titles/batch, 20 concurrent workers
- **Result**: 1,023,808 titles scored, avg 8.00. Distribution: 7.8% score 10, 29% score 9, 33.4% score 8, 17.5% score 7, 12.1% score ≤6
- **Runtime**: ~4.4 hours for 684K titles (two parallel processes), ~40-43 titles/sec, 0 errors
- **Export**: `scripts/export_scored_titles.py` → `~/unique_titles_scored.xlsx` (41MB, color-coded scores, distribution sheet)
- **Decision**: All titles scoring <7 (125,436) were reset to pending for regeneration
- **Pool exhaustion**: `maxconn=20` in `database.py` matches worker count — under peak load the pool can exhaust. Not currently blocking but could be bumped to 25

## FAQ/Kopteksten Tracking Ghost Records
- **Issue**: 45,004 URLs in `pa.faq_tracking` had status='success' but no content in `pa.faq_content`. Also 373 ghost success records in kopteksten tracking
- **Impact**: Ghost records prevented URLs from being picked up for generation (pipeline thinks they're done)
- **Fix**: Reset ghost success records to 'pending'. This explains why FAQ content count (200K) was lower than kopteksten (218K) despite same URL pool
- **Check query**: `SELECT count(*) FROM pa.faq_tracking t WHERE t.status = 'success' AND NOT EXISTS (SELECT 1 FROM pa.faq_content c WHERE c.url = t.url AND c.faq_json IS NOT NULL AND c.faq_json != '')`

## AI Title Kinder+Meisjes/Jongens Dedup Fix
- **Issue**: Titles like "Kinder Meisjes Panty's" — "Kinder" not stripped when "Meisjes" present
- **Root cause**: Dedup logic at `ai_titles_service.py:509` was facet-name-based (only matched `doelgroep`, `doelgroep mode`, `doelgroep schoenen`) but actual facets use names like `doelgroep_feestkleding`, `doelgroep_fietsen`, `doelgroep_horloge`, `dg_kind_horloge`, etc.
- **Fix**: Changed to value-based approach — any facet with value "Kinder"/"Kinderen"/"Baby" is dropped when any facet has value "Meisjes"/"Jongens". Also strips "Kinder" prefix from category names in H1 (e.g., "Kinderfietsen" → "fietsen")
- **Affected**: 403 URLs reset

## AI Title English Hallucination — "vases"
- **Issue**: GPT translated Dutch "vazen" to English "vases" in 9 titles
- **Pattern**: Original H1 had "vazen", AI output had "vases". Only found for this one word so far but worth watching
- **Fix**: Reset 9 URLs to pending. Consider a post-processing check for common Dutch→English mistranslations if pattern recurs

## Bad URL Patterns in unique_titles
- **Removed**: 1,944 URLs containing "pricemax" (2) or "+" (1,943) — these are invalid/malformed facet URLs

## Docker-Free Dashboard (dm-dashboard)
- **Repo**: `https://github.com/joep-1993/dm-dashboard` — standalone version of dm-tools without Docker
- **Key changes vs dm-tools**: Added `load_dotenv()` to `main.py`, changed default DB DSN from `db:5432` to `localhost:5432`, moved hardcoded API keys to env vars (`UNIQUE_TITLES_API_KEY`, `CONTENT_API_KEY_*` in `content_publisher.py`)
- **Password protection**: Cookie-based auth via `DASHBOARD_PASSWORD` env var. HMAC session token, 30-day cookie, middleware blocks all routes except `/login` and `/api/health`. Leave empty to disable
- **Missing files from original gitignore**: `themes.py` + `thema_ads_optimized/` (was mounted as Docker volume from `/projects/theme_ads/`), `themes/` dir (headline/description templates), `categories.xlsx` (was excluded by `*.xlsx` gitignore rule — added exception)
- **Credentials file**: `~/dm-dashboard.env` — NOT in repo, copy as `.env` on target machine
- **Both repos must be kept in sync**: When editing dashboard code, commit to both `dm-tools` and `dm-dashboard`
- **Date**: 2026-04-03

## n8n IndexNow Submitter — Slack Message Bug
- **Issue**: Slack message showed "New URLs submitted: 0" and "API response: 0" despite 10K URLs being successfully submitted to IndexNow
- **Root cause**: `build_summary1` node used `$input.all()` which receives output from `log_to_tracking_table` (Postgres node) — returns query execution results, NOT the `url_count`/`response_code` fields from `build_tracking_insert1`
- **Fix**: Changed to `$('build_tracking_insert1').all()` for url_count and `$('submit_to_indexnow1').first().json.statusCode` for response code — reference upstream nodes directly instead of relying on passthrough data
- **File**: `C:\Users\JoepvanSchagen\Downloads\indexnow_submitter (1).json`
- **Date**: 2026-04-03

## MC ID Finder Tool
- **Backend**: `backend/mc_id_finder_service.py` + `backend/mc_id_finder_router.py` — 1 endpoint under `/api/mc-id-finder/`
- **Frontend**: `frontend/mc-id-finder.html` — search by shop names (textarea, one per line) + country checkboxes (NL/BE/DE), dynamic table columns based on checked countries, CSV export
- **Redshift query**: Joins `beslistbi.hda.efficy_shop_dm` (MC IDs) with `bt.shop_main_attributes_by_day` (shop names) on `k_shop = efficy_k_shop`
- **Gotcha: `shop_name` is on `shop_main_attributes_by_day` (alias `r`), NOT on `efficy_shop_dm` (alias `m`)**. Initial query used `m.shop_name` which doesn't exist — caused "column does not exist" error
- **Gotcha: `f_mc_id_nl/be/de` are strings, not integers**. Using `> 1` worked by accident but proper filtering is `NOT IN ('','0','1')`
- **Gotcha: Many shops have `efficy_k_shop = NULL`** in `shop_main_attributes_by_day`, so the inner JOIN drops them. ~1,888 shops have working joins with MC IDs. Shops like bol.com won't appear because they lack the Efficy link
- **Date**: 2026-04-02

## GSD Campaigns Tool
- **Backend service**: `backend/gsd_campaigns_service.py` (1,247 lines) — ported from `C:\Users\JoepvanSchagen\Downloads\Python\scripts_def\create GSD-campaigns WB.py` (2,757 lines)
- **Router**: `backend/gsd_campaigns_router.py` — 7 endpoints under `/api/gsd-campaigns/`
- **Frontend**: `frontend/gsd-campaigns.html` — stats, sortable/paginated campaign table, run script, activity log
- **GAQL gotcha**: `FROM campaign_label` resource cannot be combined with metrics (clicks, cost, impressions). Must use two queries: first get campaign IDs from `campaign_label`, then fetch metrics from `campaign` with `WHERE campaign.id IN (...)`.
- **Google Ads accounts**: NL CPR=7938980174, BE CPR=2454295509, DE CPR=4192567576, NL CPC=7938980174, BE CPC=7565255758. MCC=3011145605
- **Creation label**: `GSD_SCRIPT` — used to identify campaigns created by the script
- **Credentials**: All from env vars (GOOGLE_DEVELOPER_TOKEN, GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_LOGIN_CUSTOMER_ID, REDSHIFT_*). Service account for Merchant Center via `GSD_SERVICE_ACCOUNT_FILE` env var
- **Date**: 2026-03-31

## Frontend Redesign — Dropdown Menu + Dashboard
- **Menu system**: 4 dropdown categories (Generators, Indexation, Google Ads, SEO tools) + Dashboard button. CSS in `frontend/css/style.css` with `.nav-dropdown*` classes
- **Dashboard**: `frontend/dashboard.html` — categorized tool cards with color-coded icon backgrounds (purple=Generators, blue=Indexation, coral=Google Ads, green=SEO tools), stroke-style SVG icons, hover effects (icon becomes outlined, shadow intensifies)
- **Responsive**: Top-level menu items and dropdown items scale down at breakpoints (1600/1200/992px)
- **Tool renames**: Kopteksten Generator→Kopteksten, FAQ Generator→FAQ's, 301 Generator→Redirects, Canonical Generator→Canonicals, SEO Index Checker→Index Checker
- **Date**: 2026-03-31

## FAQ Structured Data — Item Name Fix
- **Issue**: Google Rich Results Tool / Search Console showed "item name: N/A" for FAQ structured data
- **Root cause**: `FAQPage` JSON-LD had no `"name"` property at the top level — only the nested `Question` objects had `"name"`
- **Fix (new FAQs)**: Added `"name": self.page_title` to `FAQPage.to_schema_org()` in `backend/faq_service.py:95`
- **Fix (existing FAQs)**: Pure SQL migration using `replace()` to inject `"name": "<page_title>"` after `"@type": "FAQPage"` in the `schema_org` column. Script: `backend/fix_faq_sql.py`. Updated 204,216 rows
- **Gotcha**: Python `-c` inline scripts have double-quote escaping issues with SQL — use a `.py` file instead
- **Gotcha**: Row-by-row UPDATE of 204K rows over remote DB is extremely slow — use single SQL `replace()` statement for bulk string manipulation in JSON columns
- **Date**: 2026-03-31

## DMA Bid Strategy — ROAS Condition
- **Change**: Added ROAS >= 130% as additional condition for bid strategy increases (L1→L2, L2→L3)
- **ROAS formula**: DMA/CLA omzet (from "Omzet DMA en CLA" conversion action) / cost
- **Verified**: Campaign Auto's_a (21806762283) on 2026-03-19 returned ROAS 71.49% — matched Google Ads UI
- **Files**: `DMA_verhogingen_verlagingen.py` — added `roas` to data collection, increase conditions, email headers, CSV
- **Date**: 2026-03-31

## Basements Homepage — Simplified n8n Workflow
- **File**: `C:\Users\JoepvanSchagen\Downloads\basements_homepage_simple.json`
- **Purpose**: Simpler version of `basements_homepage.json` that skips DB table writes and posts directly to keywords API
- **Key difference**: Original writes to `pa.basements_hp_joep` table, checks redirects via DB updates, then reads back and posts. Simplified version processes everything in-memory
- **Homepage basement**: ALL URLs must be posted as `deepestCategoryId: 0` (homepage), not per cat_id from Redshift
- **n8n SplitInBatches gotcha**: "done" output only passes through items from the last batch, not all accumulated items. For ~100 items, skip SplitInBatches entirely — HTTP Request node iterates over all items automatically
- **n8n HTTP body gotcha**: `contentType: "raw"` with `body: "={{$json}}"` may not serialize objects correctly. Pre-stringify with `JSON.stringify()` in a Code node and pass the string as body
- **Redshift query returns strings**: `cat_id` and `order` come back as strings — cast with `Number()` before posting to keywords API
- **Date**: 2026-03-31

## DMA Bid Strategy Automation Script
- **Location**: `C:\Users\JoepvanSchagen\Downloads\Python\scripts_def\DMA_verhogingen_verlagingen.py`
- **Purpose**: Automatically adjust DMA campaign bid strategies (Level 1/2/3) based on profit, OPB, and clicks
- **Account**: 3800751597 (DMA NL), MCC: 3011145605
- **Bid strategies**: `DMA: Level 1 - 0,07`, `DMA: Level 2 - 0,11`, `DMA: Level 3 - 0,15` — defined in MCC account but referenced by sub-account campaigns with different customer prefix in resource name (must match on strategy ID, not full resource name)
- **DMA/CLA Profit formula**: Conversion value of "Omzet DMA en CLA" conversion action - cost. Queried via Google Ads API `metrics.all_conversions_value` filtered by `segments.conversion_action_name = 'Omzet DMA en CLA'`
- **OPB (Conv.-waarde/klik)**: Standard metric calculated as `conversions_value / clicks` — despite appearing as custom column in Google Ads UI, it's derivable from standard metrics
- **Google Ads API custom columns**: NOT queryable via GAQL (`custom_column` resource doesn't exist in Google Ads API v22). `CustomColumnService` also removed in v22. Custom columns are UI-only; replicate their formulas using standard metrics instead
- **SA360 API**: Can query custom columns via `custom_columns.id[{id}]` syntax, but only for columns defined in SA360 (not Google Ads custom columns)
- **Cross-account bid strategies**: MCC bid strategies have resource names like `customers/3011145605/biddingStrategies/123`, but campaigns reference them as `customers/3800751597/biddingStrategies/123`. Match on the numeric strategy ID only, not the full resource name
- **Credentials**: Uses env vars `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` + hardcoded refresh token (different from GSD script credentials)
- **DRY_RUN mode**: Set `DRY_RUN = True` to skip actual bid strategy changes but still fetch data, evaluate rules, and send email report
- **Test script**: `test_dma_profit.py` — verifies DMA/CLA Profit calculation matches Google Ads UI for a specific campaign/date
- **Date**: 2026-03-30

## IndexNow n8n Workflow Fix
- **File**: `docs/indexnow_n8n.json`
- **Issue**: `submit_to_indexnow` HTTP Request node showed no output — IndexNow API returns HTTP 200 with empty body on success
- **Fix**: Enabled `fullResponse` option on HTTP Request node so status code is visible in output. Fixed `build_tracking_insert` to read URLs from upstream `has_urls?` node (not from HTTP response body, which is empty)
- **Date**: 2026-03-30

## CloudFront Log Downloader (cloudfront-logs project)
- **Location**: `/home/joepvanschagen/projects/cloudfront-logs/`
- **Purpose**: Python script to download CloudFront access logs from S3 bucket
- **S3 Bucket**: `production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt`
- **AWS Credentials**: stored in `dm-tools/scripts/amazon_cred` (access key + secret)
- **Script**: `download_cloudfront_logs.py` — self-contained, no config file needed (credentials + bucket as constants)
- **Features**: date filter, from_date (all logs from date onwards), days (last N days), list_only mode, skip already-downloaded files (resume-safe), configurable download_dir
- **Default download dir**: `C:\Users\JoepvanSchagen\Downloads\Cloudfront`
- **Run from**: PyCharm (WSL-based), call `main()` with keyword args
- **Scale**: ~67K log files in bucket (as of 2026-03-26)
- **Date**: 2026-03-26

## Shared URL Validation Tracking (pa.url_validation_tracking)
- **Purpose**: Unified table for tracking skipped URLs (`no_products_found`) across both kopteksten and FAQ features
- **Problem**: Previously, kopteksten used `pa.jvs_seo_werkvoorraad_kopteksten_check` and FAQ used `pa.faq_tracking` separately for tracking skipped URLs. This caused different "Skipped" counts in the frontend dashboard (e.g., kopteksten showing 58K skipped, FAQ showing 62K skipped)
- **Solution**: Created `pa.url_validation_tracking` with columns: `url` (PK), `status` (skipped/rechecked), `skip_reason`, `feature_source` (kopteksten/faq/both), `created_at`, `updated_at`
- **What goes in shared table**: Only `no_products_found` skips — these are URL-level issues (URL has no products), not feature-specific
- **What stays in feature tables**: Feature-specific failures (`no_valid_links`, `ai_generation_error`, `faq_generation_failed`) remain in `pa.jvs_seo_werkvoorraad_kopteksten_check` and `pa.faq_tracking`
- **Status endpoints**: Both `/api/status` and `/api/faq/status` now read skipped count from the shared table, ensuring identical numbers
- **Total count formula**: `total = processed + skipped + failed + pending` — always adds up
- **Recheck**: Both recheck endpoints operate on the shared table. FAQ recheck delegates to kopteksten recheck
- **Migration**: `backend/migrate_shared_validation.py` merges existing skipped URLs from both feature tables into the shared table
- **Files changed**: `backend/schema.sql`, `backend/database.py`, `backend/main.py`, `backend/link_validator.py`, `backend/migrate_shared_validation.py` (new)
- **Date**: 2026-03-20

## Taxonomy API v2 — Facet & Category Management
- **Base URL**: `http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl`
- **Auth**: None needed from internal network (JWT Bearer in spec but not enforced)
- **Spec**: `scripts/swagger_taxv2.json`
- **Key fields**: `noIndexNoFollow` (on facet), `seoPriority` (on category-facet setting or facet value)
- **seoPriority status** (2026-03-17): Not set anywhere in production — all null/inherit across 3,575 categories
- **Upsert seoPriority**: `PUT /api/CategoryFacetSettings` with `{"categoryId": int, "facetId": int, "seoPriority": bool}`
- **Categories have nl-NL labels** with `name` + `urlSlug` — fetch via `GET /api/Categories/{id}`
- **Facets have nl-NL labels** — fetch via `GET /api/Facets/{id}` or search `GET /api/Facets?searchTerm=...`
- **Full docs**: See `docs/ARCHITECTURE.md` → "Beslist Taxonomy API v2" section
- **PUT /api/Facets/values/{id} clears omitted fields**: When updating a facet value, always include ALL fields in the body (nameInColumn, nameOnDetail, seoPriority). Omitted fields are reset to empty strings. Always GET the current value first, then merge your changes before PUTting.
- **Date**: 2026-03-17, updated 2026-03-25

## categories.xlsx is gitignored — regenerate from DB if missing
- **File**: `backend/categories.xlsx` — loaded at startup by `category_keyword_service.py`
- **Source**: `SELECT main_category_name, category_id, category_name FROM category_descriptions` (remote DB)
- **Columns**: maincat, maincat_id (MIN category_id per main_category), deepest_cat, cat_id
- **Impact**: Backend crash-loops with `FileNotFoundError` if missing
- **Date**: 2026-02-20

## pa.unique_titles had no UNIQUE constraint — duplicates accumulated
- **Root cause**: No PK or unique index on `url` column → same URLs inserted multiple times
- **Fix**: Deduped via `CREATE TABLE ... AS SELECT DISTINCT ON (url)`, swapped tables, added `CREATE UNIQUE INDEX idx_unique_titles_url ON pa.unique_titles (url)`
- **Scale**: 361,861 duplicate rows removed (1,016,763 → 654,902), then restored 267,031 missing from local DB → final 1,035,455
- **Date**: 2026-02-20

## Feb 19 migration missed unique_titles data
- Local DB (`seo_tools_db` container, `pa.unique_titles`) had 843,812 URLs
- Remote DB only received 654,902 — **267,031 URLs were never synced**
- Also 113,522 URLs in werkvoorraad were never in unique_titles at all
- **Always check local DB** (`docker exec seo_tools_db psql -U postgres -d seo_tools`) for missing data after migration issues
- **Date**: 2026-02-21

## Content publishing: timeout and serialization
- Full payload is ~1.36 GB (252K items) — takes ~10 min to upload
- DB fetch takes ~7 min (FULL OUTER JOIN content_urls_joep + faq_content)
- **Must use `data=payload_json`** not `json=payload` (avoids requests double-serialization)
- **Timeout must be ≥1800s** (old 600s was killing uploads mid-way)
- Staging environment may be down (504) — test against production directly
- The unique_titles publish (`/api/unique-titles/publish`) is a SEPARATE endpoint from content publish — uploads CSV to `custom-title-description/import-per-url`
- **Date**: 2026-02-21

## Database Connection Quick Reference

### Primary Database (used by BOTH dm-tools app AND n8n workflows)
- **Host**: `10.1.32.9` (internal: `n8n-vector-db-rw.n8n.svc.cluster.local`)
- **Database**: `n8n-vector-db`
- **User**: `dbadmin` / **Password**: `Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6`
- **Schema**: `pa`
- **Access**: `docker exec -e PGPASSWORD='Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6' seo_tools_db psql -h 10.1.32.9 -U dbadmin -d n8n-vector-db -c "SELECT ..."`
- **Changed**: 2026-02-19 — migrated from local seo_tools_db to remote DB so n8n can run without laptop

### Local Docker DB (still running but no longer primary)
- **Container**: `seo_tools_db` (exposed on port 5433)
- **Database**: `seo_tools`
- **User**: `postgres` / **Password**: `postgres`

### Redshift (LEGACY - not actively used, USE_REDSHIFT_OUTPUT=false)
- **Credentials**: See `.env` file in dm-tools project

**IMPORTANT**: Frontend, backend, AND n8n all use the remote DB at 10.1.32.9. The local seo_tools_db container is still running but is no longer the primary database.

## Category Lookup from CSV (cat_urls.csv)
- **Problem**: Category name was derived from first product's `categories` array in API response — could be wrong (e.g., robot vacuum cleaners instead of regular vacuum cleaners)
- **Solution**: `backend/data/cat_urls.csv` (3,557 rows, `;`-delimited) maps URL parts to category names. Loaded lazily by `backend/category_lookup.py`
- **CSV columns**: `maincat;deepest_cat;url_name;cat_id` — `url_name` like `/meubilair_389369/` matched against parsed URL `category` variable
- **Usage**: Both `scraper_service.py` (kopteksten) and `faq_service.py` (FAQ) call `lookup_category(main_category, category)` before falling back to API-derived category
- **Fallback**: Top-level pages (`category=None`) and unknown URL parts fall back to old behavior (first product's categories)
- **Files**: `backend/category_lookup.py`, `backend/data/cat_urls.csv`, `backend/scraper_service.py`, `backend/faq_service.py`
- **Date**: 2026-03-19

## Main Category URLs: Separate Content Generation Path
- **Problem**: 31 main category URLs (`/products/{maincat}/`) have no subcategory — products span many different subcategories, so the API-derived h1_title/product_subject is wrong (picks deepest category from first product)
- **Solution**: Added `MAIN_CATEGORY_H1` mapping in `scraper_service.py` with fixed H1 titles from `maincaturls.xlsx`. `is_main_category_url()` detects these URLs (no subcategory, no filters). `generate_main_category_content()` in `gpt_service.py` uses a broader introductory prompt
- **Prompt rules**: No "Welkom op de ... pagina", no "ons/onze/wij/we", broader overview mentioning subcategories, 2-4 product links from diverse subcategories
- **Database**: URLs in `pa.jvs_seo_werkvoorraad` (for kopteksten processing), excluded from FAQ via `pa.faq_tracking` with `status='skipped', skip_reason='main_category_url'`
- **Files**: `scraper_service.py`, `gpt_service.py`, `main.py`
- **Date**: 2026-03-17

## Beslist.nl Product Count Extraction
- **Pattern**: `"productCount":(\d+),"selected":true` — finds the product count of selected facets in the embedded JSON
- **Context**: Beslist pages embed facet data as JSON in the HTML. Every facet value has a `productCount`, but only the `selected:true` ones reflect the current page's result count
- **Multiple matches**: When multiple facets are selected, they all share the same productCount — take the first match
- **File**: `backend/main.py` — function `extract_product_count()`
- **Date**: 2026-02-13

## N8N Workflow Conversion from Python Scripts
- **Pattern**: When converting Python backend logic to n8n workflows, use Code nodes for complex logic (URL parsing, API response processing, content post-processing) and native n8n nodes for simple operations (DB queries, HTTP requests, OpenAI calls)
- **Node type versions**: Match existing workflows for compatibility (postgres v2.6, openAi v1.8, code v2, if v2.2, splitInBatches v3, httpRequest v4.2, scheduleTrigger v1.2)
- **Error handling**: Use `onError: "continueRegularOutput"` on all nodes — WARNING: this silently swallows errors! Check node outputs carefully when debugging
- **ES in n8n**: Use `fetch()` API in Code nodes for Elasticsearch HTTP queries (n8n Code nodes support fetch natively)
- **n8n Code node limitations**: `URLSearchParams` is NOT available — use manual `encodeURIComponent()` + string concatenation. `new URL()` IS available for URL parsing.
- **Bulk DB writes**: Don't use per-item Postgres nodes inside SplitInBatches loops — use pure SQL with `INSERT INTO ... SELECT` or run bulk queries in parallel from the source node. Data references like `$('nodeName').all()` don't work from the loop's "done" branch.
- **n8n vector DB was missing PKs/sequences**: Tables synced from Redshift had no primary keys, unique constraints, or auto-increment sequences. Had to add these manually for INSERT operations to work.
- **Files**: `docs/kopteksten_generator_n8n.json`, `docs/link_validator_n8n.json`
- **Date**: 2026-02-19

## N8N Code Node Capabilities and Limitations
- **Available in Code nodes**: `$input`, `$json`, `$env`, `fetch()`, `DateTime`, `console`, `new URL()`, `$('nodeName').all()`
- **NOT available in Code nodes**: `$helpers`, `$helpers.httpRequestWithAuthentication`, credential access, `URLSearchParams`
- **For HTTP requests**: Must use `fetch()` with manual auth headers (no credential helper access)
- **For OpenAI in Code nodes**: Call the API directly via `fetch('https://api.openai.com/v1/chat/completions', {...})` with manual `Authorization: Bearer ${apiKey}` header
- **Environment variables**: Access via `$env.VARIABLE_NAME` — OPENAI_API_KEY must be set as n8n environment variable on the server
- **Parallel HTTP pattern**: Use `async function` + `Promise.all()` with a concurrency limiter:
  ```javascript
  async function withConcurrency(items, limit, fn) {
    const results = [];
    let index = 0;
    async function worker() {
      while (index < items.length) {
        const i = index++;
        results[i] = await fn(items[i], i);
      }
    }
    await Promise.all(Array.from({length: Math.min(limit, items.length)}, () => worker()));
    return results;
  }
  ```
- **Date**: 2026-02-19

## N8N Flow Optimization: Bulk Operations Pattern
- **Problem**: SplitInBatches loops with per-item DB queries and API calls are extremely slow in n8n
- **Solution**: Replace loops with single Code nodes that do ALL work internally using bulk operations
- **Link Validation optimization**:
  - ONE Elasticsearch query per maincat instead of per URL (was ~100+ queries, now ~31 max)
  - All DB operations use bulk SQL (7 queries instead of ~100 per-item queries)
  - Removed 14 nodes, replaced with 7 bulk nodes
- **Kopteksten Generation optimization**:
  - `fetch_all_products`: Parallel Product Search API calls (5 concurrent) in single Code node
  - `generate_all_content`: Parallel OpenAI API calls (3 concurrent) via fetch() in single Code node
  - Bulk DB writes for write_result and remove_from_check
  - Removed SplitInBatches loop entirely
  - Reduced maxTokens from 2000 to 1000
  - Total nodes reduced from 35 to 20
- **Key lesson**: n8n is much faster when heavy logic is inside Code nodes with parallel processing, rather than using SplitInBatches with many sequential nodes
- **Date**: 2026-02-19

## N8N Production Push to Beslist API
- **Purpose**: Push generated SEO content (content_top, content_bottom, content_faq) to production website
- **API endpoint**: `POST https://website-configuration.api.beslist.nl/automated-content`
- **Auth header**: `X-Api-Key: Sectional~Publisher~Dumpling1`
- **Data transformation** (handled by `content_publisher.py`):
  - `content_top`: From `content_urls_joep.content`, sanitized ('' → ' → &#39;)
  - `content_bottom`: From FAQ Q&As with internal beslist.nl links only
  - `content_faq`: From FAQ schema.org JSON-LD
- **CRITICAL: Send ALL items in a SINGLE request** — the API replaces ALL content per call
- **n8n approach**: `push_to_production` Code node calls FastAPI `POST /api/content-publish?environment=production`, polls for completion (backend handles DB fetch + payload build + API call)
- **Date**: 2026-02-19, updated 2026-02-23

## N8N Postgres Node: NEVER use queryBatching "independently" with dynamic content
- **Problem**: `queryBatching: "independently"` naively splits SQL on semicolons — including semicolons inside string literals (HTML, CSS, JSON content)
- **Impact**: INSERT/UPDATE statements with HTML content (e.g., `style="color: red; font-size: 12px"`) get split into broken fragments
- **Fix**: Remove `queryBatching: "independently"` from ALL exec nodes that run dynamically built SQL. These nodes each execute a single SQL statement, so batching is unnecessary
- **Date**: 2026-02-21

## N8N Postgres Node: Chained exec nodes lose $json context
- **Problem**: When Postgres exec nodes are chained (A → B → C), node B's output replaces `$json` for node C. So `$json.field` in C references B's query result, not the original Code node output
- **Fix**: Use `$node["sourceCodeNode"].json.field` instead of `$json.field` for all chained Postgres nodes after the first one
- **Exception**: The FIRST exec node after a Code node CAN use `$json.field` since it receives data directly
- **Date**: 2026-02-21

## Unique Titles Publish: Case-sensitive duplicates cause API failure
- **Problem**: `pa.unique_titles` uses PostgreSQL (case-sensitive PK), but the API's MySQL has case-insensitive unique constraint on `url`
- **Impact**: URLs like `/c/dGVsZXZpc2` and `/c/dgvszxzpc2` coexist in PG but MySQL rejects the CSV with "Duplicate entry" error
- **Fix**: Deleted 422 mixed-case duplicates, lowercased remaining 72 URLs with caps. The `upsert_title()` function already lowercases on insert
- **All URLs in unique_titles should be lowercase**
- **Date**: 2026-02-21

## N8N Workflows: 5 individual flows + 1 combined pipeline
- **Individual workflows** in `Downloads/flows/`:
  1. `1_content_generator.json` — 50K URL content generation (10:00)
  2. `2_seo_link_validator.json` — 50K SEO link validation (14:00)
  3. `3_faq_link_validator.json` — 50K FAQ link validation (15:00)
  4. `4_publisher.json` — Publish SEO+FAQ to production (18:00)
  5. `5_faq_generator.json` — 50K FAQ generation (12:00)
- **Combined workflow**: `seo_content_pipeline.json` — 30 nodes, all 5 phases sequential in one flow (Schedule 10:00 → SEO validate → SEO generate → FAQ validate → FAQ generate → publish → Slack)
- **Date**: 2026-02-21, combined 2026-02-23

## N8N Publishing: Delegate to FastAPI backend to avoid OOM
- **Problem**: Publishing 244K rows creates ~1GB JSON payload. n8n Code node OOMs building contentItems array + JSON.stringify + fetch body (~3-4GB peak). PostgreSQL `json_agg` also OOMs at 1GB text buffer limit
- **Solution**: n8n Code node calls `POST http://app:8003/api/content-publish?environment=production`, then polls `GET /api/content-publish/status/{taskId}` every 15s (up to 40 min). Backend (`content_publisher.py`) handles DB fetch, payload build, and API call in Python where memory management is better
- **Key**: The Beslist API replaces ALL content per call — no append/upsert. Must send everything in one request
- **Output fields**: `success`, `total_urls`, `total_published` — matches Slack message template
- **Date**: 2026-02-23

## N8N OpenAI API Key: Hardcode in Code nodes
- **Problem**: `process.env.OPENAI_API_KEY` in n8n Code nodes may throw ReferenceError (process not available) or return undefined
- **Fix**: Hardcode the key directly: `const OPENAI_API_KEY = 'sk-proj-...'`
- **Updated in**: `generate_all_content`, `generate_all_faqs` (both combined and individual workflows)
- **Date**: 2026-02-23

## ON CONFLICT Requires UNIQUE Constraint in PostgreSQL
- **Problem**: `INSERT ... ON CONFLICT (url) DO UPDATE` silently does a plain INSERT when there is no UNIQUE constraint or index on the `url` column
- **Root cause**: PostgreSQL's ON CONFLICT clause needs a unique index/constraint to detect conflicts. Without it, no conflict is ever detected, so every INSERT succeeds — creating duplicates
- **Context**: Tables migrated from Redshift to PostgreSQL lose all constraints (primary keys, unique indexes, foreign keys). Redshift supports these syntactically but doesn't enforce them, so they're often missing in the source DDL
- **Impact**: `faq_content` had 79,523 duplicate rows (241,033 total, 161,510 unique). `faq_tracking` had 94,387 duplicate rows (243,671 total, 149,284 unique)
- **Fix**:
  1. Deduplicate existing data: `DELETE FROM table WHERE ctid NOT IN (SELECT MIN(ctid) FROM table GROUP BY url)`
  2. Add UNIQUE constraint: `ALTER TABLE table ADD CONSTRAINT table_url_unique UNIQUE (url)`
  3. Add ON CONFLICT to INSERT: `INSERT INTO table (url, ...) VALUES (...) ON CONFLICT (url) DO UPDATE SET ...`
- **Key lesson**: After migrating tables from Redshift (or any system), ALWAYS verify and re-add: primary keys, unique constraints, indexes, and foreign keys
- **Date**: 2026-02-19

## AI Title Generation: Met-Feature Duplication Fix
- **Problem**: Met-feature values (e.g., "Korte mouwen") appeared twice in titles — once in the base H1 from the API (e.g., "Korte mouwen nachthemden") and again as a "met" clause ("met Korte mouwen")
- **Root Cause**: Size and suffix values were stripped from `ai_h1` before sending to OpenAI, but met-feature values were NOT stripped. The AI saw "Korte mouwen nachthemden" AND received instructions to add "met korte mouwen"
- **Fix**: After classifying met-features, strip them from `ai_h1` using case-insensitive regex replace (handles "met "/"zonder " prefixed values too). Applied to ALL met-features: mouwen, capuchon, rits, knopen, veters, strepen, print, etc.
- **Scale**: 106 affected URLs found and reset (17 for korte mouwen + 89 for other met-features)
- **File**: `backend/ai_titles_service.py` — lines 593-610
- **Date**: 2026-02-12

## AI Title Generation: Met-Feature by Facet Name (Materiaal Band)
- **Problem**: Facet `m_band` (URL parameter) has API facet name "Materiaal band" — not "m_band". Code was checking `fname == 'm_band'` which never matched
- **Lesson**: Always check the actual API facet name (via `fetch_products_api()`) rather than assuming it matches the URL parameter name
- **Fix**: Changed check to `fname == 'materiaal band'`
- **Scale**: 356 URLs reset
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-12

## AI Title Generation: Vermogen/Power Facets as Spec Values
- **Problem**: Facet `watt_frituurpannen` (API name "Vermogen (Watt)") with values like "2001 tot 3000" wasn't detected as a spec value. The range format without unit didn't match the number+unit regex
- **Fix**: Added `fname.startswith('vermogen')` to `is_spec_value()` — catches all power/output facets regardless of value format
- **Scale**: 121 URLs reset
- **File**: `backend/ai_titles_service.py` — function `is_spec_value()`
- **Date**: 2026-02-12

## AI Title Generation: Soort Facet Category Replacement
- **Problem**: "Soort" facets with product-type values (e.g., "Parka jassen", "Bomberjacks") created redundant titles: "G-Star Parka jassen jacks" where "jacks" is the generic category already superseded by the specific Soort value
- **Detection**: The API returns `category_name` (e.g., "Jacks", "Winterjassen") separately. When a Soort facet's value ends with a product type suffix, the trailing category name in the H1 is redundant
- **Fix**: Detect Soort facets whose last word ends with a product type suffix (jassen, jacks, broeken, shirts, schoenen, jurken, truien, etc.). Strip trailing `category_name` from `api_h1` using case-insensitive regex
- **Key**: Uses `endswith()` on the last word of the Soort value against a tuple of ~30 common Dutch product type suffixes
- **File**: `backend/ai_titles_service.py` — between facet dedup and facet classification sections
- **Date**: 2026-02-12

## IndexNow: Migrated from Redshift to Local PostgreSQL
- **Problem**: IndexNow service used Redshift for URL deduplication tracking. `SELECT DISTINCT url FROM pa.index_now_joep` on 800K+ rows was slow and competed with other Redshift queries, causing the frontend to become unresponsive
- **Fix**: Switched all IndexNow DB operations from `get_redshift_connection()` to `get_db_connection()` (local PostgreSQL). Added proper indexes (`idx_indexnow_url`, `idx_indexnow_date`). Migrated 813,978 rows from Redshift
- **Daily limit**: Added 10K daily URL limit with counter, progress bar in UI, and enforcement in `submit_urls()`
- **UI improvements**: Auto-loading history on page load, submission details (new/submitted/skipped/truncated), auto-refresh history after submission
- **File**: `backend/indexnow_service.py`
- **Date**: 2026-02-12

## Winkel (Shop) Facet URLs Are Useless for AI Titles
- **Problem**: URLs with `winkel~` facets return no selected facets from the Product Search API — only the bare category name. Results in empty titles like "bedden"
- **Action**: Deleted 48,578 winkel URLs from `pa.unique_titles`. These should not be processed for AI titles
- **Date**: 2026-02-12

## AI Title Generation: Code-Level Facet Classification
- **Problem**: OpenAI (gpt-4o-mini) persistently adds "met" before sizes ("met Maat L", "met Grote maten") despite extensive prompt rules forbidding it. Prompt-only fixes failed after 5+ iterations.
- **Solution**: Moved facet handling from prompt rules to Python code preprocessing in `generate_title_from_api()`:
  1. **Spec/size auto-detection** (`is_spec_value()`): Automatically detects values that belong at the end of the title using regex pattern matching — number+unit (liter, watt, cm, kg, persoons, etc.), bare numbers, size abbreviations (S/M/L/XL), "Maat X"/"Wijdte X", "Grote/Kleine maten", and maat/wijdte facet name fallback. No hardcoded facet name list needed.
  2. **Bare number "Maat" prefix**: Numbers from maat facets get "Maat" prepended (e.g., "57" → "Maat 57")
  3. **Met-feature pre-combination**: Feature values are pre-combined into a ready-made clause (e.g., "met korte mouwen, print en borstzak") and passed as an exact string for the AI to use.
  4. **Conditional met rule**: When no features exist, prompt says "Voeg NOOIT 'met' toe". When features exist, provides exact clause to copy.
  5. **Value-based met-classification** (not facet-name-based):
     - API `detail_value` starting with "met "/"zonder " → automatic met_values
     - Small hardcoded set of feature values needing "met" added: mouwen, capuchon, hals, rits, knopen, veters, draaiplateau, grill, strepen
     - Values ending with "print" (e.g., "panterprint", "dierenprint") get "met" automatically; without "print" suffix they're treated as adjectives (e.g., "panter t-shirt" not "t-shirt met panter")
     - Opties/functies/features facets are NOT blanket-classified (some are adjectives like "Ademende", "Hittebestendige")
     - Everything else → regular (adjective before product name)
  6. **Brand deduplication**: If Merk value appears inside another facet (e.g., Merk="Epson" + Productlijn="Epson EcoTank"), standalone brand facet is dropped
  7. **Color deduplication**: If both Kleur and Kleurtint*/Kleurcombinati* are present, base color dropped in favor of specific shade/combination
  8. **Audience deduplication**: If general audience (Kinder/Baby) + specific (Meisjes/Jongens) both present, general is dropped
  9. **Hallucination removal**: Post-processing strips Heren/Dames/Kinderen/Nieuwe etc. from output if not present in input. Recognizes inflected forms (Nieuw→Nieuwe) to avoid stripping valid adjective inflections.
  10. **Trailing "met" safety net**: Strips dangling " met" from AI output before appending
  11. **Suffix values**: Color combinations (Kleurcombinaties), "Volwassenen" (levensfase), and "Vanaf X jaar" (geschikte_leeftijd) appended after title but before size values
  12. **First letter capitalization**: Ensures title starts with capital, checks against `lead_values` (brand/productlijn) for intentional lowercase (e.g., "iPhone")
  13. **Category name fallback**: Appends deepest category name when missing from H1 (e.g., "Vrijstaande 23 liter" → "Vrijstaande 23 liter magnetrons")
  14. **Adjective inflection prompt rule**: Rule 10 tells AI to inflect adjectives correctly ("Nieuw" → "Nieuwe")
  15. **Brand/productlijn strip-and-prepend**: Brand and productlijn are stripped from AI input and prepended in code after, preventing AI from misplacing multi-word brands like "The Indian Maharadja"
  16. **Color before audience in prompt**: Prompt rules 4+5 specify colors/materials come before audience ("blauwe Heren hoodies" not "Heren blauwe hoodies")
- **Key lesson**: When LLM prompt rules fail repeatedly for a specific pattern, move that logic to deterministic code. Code-level preprocessing is 100% reliable vs prompt rules being probabilistic.
- **Key lesson 2**: Don't classify entire facet groups (opties/functies) as met-features — they contain both adjectives ("Ademende", "Hittebestendige") and nouns ("Draaiplateau"). Use value-based classification.
- **Key lesson 3**: Auto-detect spec/size values with regex (number+unit) instead of hardcoding facet names — more robust across categories.
- **File**: `backend/ai_titles_service.py` — function `generate_title_from_api()`
- **Date**: 2026-02-11

## Category Depth-Based Extraction for AI Titles
- **Problem**: AI titles used wrong category name for parent-level URLs. E.g., `/products/huis_tuin/huis_tuin_505061/c/...` (Badkameraccessoires) got title saying "douchegordijnen" (a child subcategory)
- **Root Cause**: `faq_service.py` extracted `categories[-1]` (deepest product category) instead of the URL's own category level. Products belong to deep subcategories, but the URL targets a higher level.
- **Fix**: Count numeric sub-IDs in URL's category segment to determine depth, then use `categories[url_depth]` instead of `categories[-1]`
  - `huis_tuin_505061` → 1 sub-ID → `categories[1]` = "Badkameraccessoires"
  - `huis_tuin_505061_505308` → 2 sub-IDs → `categories[2]` = "Douchegordijnen"
- **Exception**: URLs with type facets (t_, type_) are correct because type facets replace the category name (e.g., `t_transportwagens` overrides the URL's category)
- **Reset**: 8,109 parent-level URLs without type facets reset to pending for reprocessing
- **File**: `backend/faq_service.py` — function `fetch_products_api()`
- **Date**: 2026-02-11

## Bad URL Detection: facet_not_available (400) from Product Search API
- **Problem**: ~3-5% of pending URLs in `unique_titles` return HTTP 400 (`facet_not_available`) from the Product Search API — facets or categories that no longer exist
- **Detection**: `backend/find_bad_urls.py` — checks each URL against `productsearch-v2.api.beslist.nl` with 20 parallel workers, flags 400 responses
- **Scale**: Partial scan of 155K/916K URLs found ~4,589 bad URLs. Bad rate varies by alphabetical range (0.1-18% per batch)
- **Pattern**: These are redirect source URLs (old facet IDs, renamed categories) that should have been caught by the Redirects Admin sheet
- **Tables to clean**: `pa.unique_titles`, `pa.jvs_seo_werkvoorraad`, `pa.jvs_seo_werkvoorraad_kopteksten_check`, `pa.faq_tracking`, `pa.faq_content`
- **Date**: 2026-02-11

## AI Title Prompt Engineering: Earlier Iterative Fixes
- **Issues fixed via prompt rules** (before code-level approach):
  1. **"met" for features**: FOUT/GOED examples + exception to allow "met"/"zonder"
  2. **"voor" with audiences**: Audiences before product name, "voor" forbidden
  3. **Hallucinated sizes**: Temperature 0.7→0.3, anti-hallucination wording
  4. **Conflicting rules**: Scoped "NOOIT" to specific prepositions
- **Lesson**: LLM prompts need FOUT/GOED examples for every edge case. Rules saying "NOOIT X" get over-generalized unless precisely scoped.
- **File**: `backend/ai_titles_service.py` — both `generate_ai_title()` (prompt 1) and `generate_title_from_api()` (prompt 2)
- **Date**: 2026-02-11

## Database Cleanup: German URLs and Garbage Data
- **Problem**: Databases contained ~210 German URLs (from beslist.de), ~112 garbage URLs (empty facet values, truncated names), and ~66 landing/theme pages
- **Detection methods**:
  1. German category paths: `möbel`, `schuhe`, `essen_getränke`, `haus_garten`, etc. (mojibake encoding: `Ã¶`=ö, `Ã¤`=ä)
  2. German facet names: `farbe`, `marke`, `zielgruppe`, `materialien`, `sportbekleidung`
  3. Cross-reference: Loaded 2,719 Dutch facet url_names from `facets_20260204.csv`, compared against facet names in pending URLs — remaining 84 mismatches were all Dutch variants/typos, no more German
  4. Garbage: URLs with empty facet values (`url ~ '/c/.*~($|~~|/)'`), `no-text` strings, leading spaces, `pricemax`/`pricemin`
- **Cleanup**: Deleted from all 5 tables (unique_titles, werkvoorraad, werkvoorraad_kopteksten_check, faq_tracking, faq_content)
- **Date**: 2026-02-11

## AI Titles: Stop Button and Scraping Fallback Removal
- **Stop button fix**: Changed `_run_processing()` from submitting all URLs to ThreadPoolExecutor at once to chunked processing (chunk_size = num_workers * 2). Stop flag checked between chunks for responsive stopping.
- **Scraping fallback removed**: `process_single_url()` now only uses productsearch API method. Returns `api_failed` error when API returns None, instead of falling back to scraping.
- **Error message improvement**: Last error now includes the failing URL: `f"{reason} ({url})"`
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-11

## Canonical Generator: Facet Sorting Bug with ~ Separator
- **Problem**: When two facets share a prefix (e.g., `kleur` and `kleurtint`), the URL sorted the longer facet first: `/c/kleurtint~17171868~~kleur~393175`
- **Root Cause**: `facets.sort()` sorted the full `facet~value` string. Since `~` (ASCII 126) > `t` (ASCII 116), `kleurtint~...` sorted before `kleur~...`
- **Fix**: Sort by facet name only (part before `~`): `facets.sort(key=lambda f: f.split("~")[0] if "~" in f else f)`
- **Result**: `kleur~393175~~kleurtint~17171868` (correct alphabetical order)
- **File**: `backend/canonical_service.py` — function `_sort_facets()`
- **Date**: 2026-02-10

## Canonical Generator: Filter No-Index URLs with "+"
- **Problem**: URLs containing `+` are no-index and should not appear in canonical generator results
- **Fix**: Added `AND dv.url NOT LIKE '%%+%%'` to the Redshift query in `fetch_urls_from_redshift()`
- **File**: `backend/canonical_service.py` — function `fetch_urls_from_redshift()`
- **Date**: 2026-02-10

## Facet Volume Processing: Batch Search Volumes for All Facet Values
- **What**: Process 140K+ facet values across 31 maincats, combining each facet with all deepest category names within its maincat, looking up search volumes via Google Ads API
- **SIC/SOD handling**: Facet values containing `<!-- SIC: X --><!-- SOD: Y -->` use SOD before category ("zwarte schoenen") and SIC after category ("schoenen zwart"). Plain facet values use the same text in both positions
- **Facet cleaning**: `clean_facet_value()` strips HTML comments (`<!-- ... -->`), normalizes whitespace. `parse_sic_sod()` extracts SIC/SOD/plain text
- **Keyword-to-row tracking**: Uses `keyword_to_rows` dict mapping each keyword combo to a set of facet row indices, allowing volume distribution back to source rows
- **Output**: Same columns as input CSV + `search_volume` column (grand total of all keyword combos for that facet across all deepest cats)
- **Resume capability**: `facets_progress.txt` tracks completed maincats; script skips them on restart
- **Scale**: ~81M keyword combinations, ~8,128 API batches, 35 customer IDs for quota rotation
- **Files**: `backend/category_keyword_service.py` (functions: `clean_facet_value`, `parse_sic_sod`, `process_facet_volumes`, `_normalize_keyword`), `backend/run_facet_volumes.py` (batch runner script)
- **Date**: 2026-02-10

## Google Ads API: gRPC ResourceExhausted vs GoogleAdsException
- **Problem**: Quota rotation wasn't working — rate limit errors (429) crashed entire maincats instead of rotating to next customer_id
- **Root Cause**: The code only caught `GoogleAdsException`, but 429 rate limits throw `google.api_core.exceptions.ResourceExhausted` (a gRPC-level exception), which is a different exception class
- **Fix**: Import `ResourceExhausted` from `google.api_core.exceptions` and catch it separately before `GoogleAdsException`. Also added catch-all `Exception` handler that checks for "resource exhausted" in message string
- **Key lesson**: Google Ads API errors come in two flavors: (1) API-level `GoogleAdsException` with `.failure.errors` list, and (2) gRPC-level exceptions like `ResourceExhausted` which bypass the `GoogleAdsException` handler entirely
- **File**: `backend/keyword_planner_service.py` — function `_query_search_volumes()`
- **Date**: 2026-02-10

## Redshift Connection Pool: SSL SYSCALL Error Fix
- **Problem**: "SSL SYSCALL error: EOF detected" in canonical generator when querying Redshift
- **Root Cause**: Stale connections in psycopg2 ThreadedConnectionPool — connections go idle, Redshift drops them, but the pool doesn't know
- **Fix**: (1) Added TCP keepalive settings to pool (keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5), (2) Added health check in `get_redshift_connection()` — runs `SELECT 1` before returning pooled connection, gets fresh one if stale
- **File**: `backend/database.py` — functions `_get_redshift_pool()`, `get_redshift_connection()`
- **Date**: 2026-02-10

## Category Keyword Volumes: Keyword + Category Combination Tool
- **What**: Combines a keyword (e.g., "nike") with all 3,535 deepest category names in both singular/plural forms and both word orders (4 combos per category: "nike schoenen", "schoenen nike", "nike schoen", "schoen nike")
- **Singular/plural forms**: Pre-computed and stored in `backend/category_forms.json` (3,564 entries including maincat names). Generated with Dutch heuristics: remove -en (with doubled consonant fix: "brillen"→"bril"), remove -s, handle -'s. Falls back to appending -en for assumed-singular words
- **Maincat entries**: Each unique maincat name is also combined with the keyword, stored as a deepest_cat row where `deepest_cat = maincat` and `cat_id = maincat_id`
- **Categories preloaded**: `backend/categories.xlsx` loaded at startup into `PRELOADED_CATEGORIES` (no file upload needed). 4 columns: maincat (A), maincat_id (B), deepest_cat (C), cat_id (D)
- **Output Excel**: Same as input + column E (search_volume_deepest_cat) + column F (search_volume_maincat)
- **API batch behavior**: ~14,200 keyword combinations → 2 batches of 10,000. Google Ads `GenerateKeywordHistoricalMetrics` may return slightly different rounded volumes when batch payload changes (adding/removing keywords from same request). This is NOT API variance (consecutive identical requests return identical results)
- **Files**: `backend/category_keyword_service.py` (service), `backend/category_forms.json` (pre-computed forms), `backend/categories.xlsx` (preloaded data), `frontend/keyword-planner.html` (UI section)
- **Endpoints**: `POST /api/keyword-planner/category-volumes` (JSON: `{"keyword": "nike"}`), `POST /api/keyword-planner/category-volumes/download` (JSON: `{"deepest_cat_results": [...]}`)
- **Date**: 2026-02-10

## Link Validator V4 UUID Lookup: Wildcard Queries Kill ES Performance
- **Problem**: V4 UUID plpUrl lookups used `wildcard` queries (`*V4_xxx*`) which caused constant 60s timeouts on Elasticsearch, making the "Validate All" feature extremely slow (~180K URLs taking hours)
- **Root Cause**: Leading wildcards (`*V4_xxx*`) force a full index scan in ES. With 20 parallel workers, each content item having 2-4 V4 links, hundreds of 60s timeouts cascade across batches
- **Fix (two-phase lookup)**:
  1. **Phase 1**: Fast `terms` query on `pimId` field with V4 UUIDs (instant, uses ES index)
  2. **Phase 2**: Wildcard disabled entirely — V4 URLs not found via pimId are skipped (not marked as gone). Wildcard queries on these ES indices always timeout and never return results anyway
- **Key lesson**: `result.get(key)` returns `None` for missing keys, which was then stored as `lookup_to_plp_url[key] = None` and interpreted as "product is GONE". Fix: only store results for keys actually present in the result dict (`if key in result: lookup_to_plp_url[key] = result[key]`), so unfound V4 URLs are skipped instead of falsely marked as gone
- **Impact**: Validator went from timing out/appearing stuck to completing 139K URLs in reasonable time
- **File**: `backend/link_validator.py` — function `query_elasticsearch_by_plpurl()` + callers in `lookup_plp_urls_for_content()` and `validate_faq_links()`
- **Date**: 2026-02-09 (supersedes 2026-02-08 wildcard fix)

## Detecting Cut-Off Content in Database
- **Problem**: Some generated content was cut off mid-sentence/mid-word (e.g., "om je h"), likely due to OpenAI token limits truncating the response
- **Detection query**: Strip HTML tags and check if text ends without sentence-ending punctuation:
  ```sql
  SELECT url FROM pa.content_urls_joep
  WHERE TRIM(regexp_replace(content, '<[^>]+>', '', 'g')) !~ '[.!?")'']$'
  ```
- **Scale**: Found 349 out of 179,949 content items (~0.2%) with cut-off content
- **Fix**: Back up to `content_history` (reason: `cut_off_content`), delete from `content_urls_joep`, add to `werkvoorraad` with `kopteksten=0` for regeneration
- **Date**: 2026-02-09

## Restoring Falsely Reset URLs: Don't Forget kopteksten_check
- **Problem**: After restoring content from `content_history` back to `content_urls_joep`, frontend status numbers didn't add up (processed + pending + skipped + failed > total)
- **Root Cause**: The validator deletes `kopteksten_check` entries when moving URLs to pending. Restoring content without re-adding `kopteksten_check` entries causes double-counting: URLs appear as both "processed" (have content) and "pending" (in werkvoorraad, no kopteksten_check entry)
- **Full restore checklist**:
  1. Restore content from `content_history` → `content_urls_joep`
  2. Re-add `kopteksten_check` entries with status `success`
  3. Clear `link_validation_results` for re-validation
  4. Optionally clean up `content_history` backup entries
- **Date**: 2026-02-09

## Keyword Planner Integration with Google Ads API
- **Purpose**: Look up Google Ads search volumes for keywords, with normalization that preserves traceability back to original keyword
- **Key pattern**: Build `cleaned_to_originals` mapping (e.g., `{"e bike": ["e-bike", "E-Bike"]}`) → query API with deduplicated cleaned keywords → map results back to originals
- **Normalization**: Replace `-` and `_` with spaces, remove special chars, lowercase, collapse whitespace (via `clean_keyword()`)
- **Quota management**: 35 hardcoded customer_ids with rotation on `RESOURCE_EXHAUSTED` + exponential backoff
- **API**: `GenerateKeywordHistoricalMetricsRequest` with geo=2528 (NL), language=1010 (Dutch), batch_size=10000
- **Google Ads credentials**: Already in `.env` / Docker container env vars (`GOOGLE_DEVELOPER_TOKEN`, `GOOGLE_REFRESH_TOKEN`, etc.)
- **Package**: `google-ads` v29.0.0 already installed in Docker container
- **Files**: `backend/keyword_planner_service.py` (service), `backend/main.py` (4 endpoints), `frontend/keyword-planner.html` (UI)
- **Date**: 2026-02-09

## Content Lookup URL Format Mismatch
- **Problem**: URL lookup function in dm-tools frontend returned "URL not found in content database" for URLs that existed
- **Root Cause**: `lookup_content()` in `main.py` normalized input to relative path (`/products/...`) but DB could store full URLs (`https://www.beslist.nl/products/...`) or vice versa
- **Fix**: Build both relative path and full URL variants, query with `WHERE url = %s OR url = %s`
- **File**: `backend/main.py` — endpoint `/api/content/lookup`
- **Date**: 2026-02-08

## Link Validator False Positives from ES Failures
- **Problem**: Validation run flagged 28,999 URLs as "gone products", but many products were actually valid (shopCount >= 2 in ES)
- **Root Cause**: Exception handlers in `lookup_plp_urls_for_content()` (line ~260) and FAQ validator (line ~570) set ALL products to `None` (gone) when an ES query fails:
  ```python
  except Exception as e:
      for pim_id in pim_ids:
          lookup_to_plp_url[pim_id] = None  # BUG: marks all as gone!
  ```
- **Fix**: On ES failure, skip the batch entirely instead of marking as gone. In the result builder, only include links whose `lookup_value` exists in `lookup_to_plp_url` — missing entries are simply omitted (not treated as gone)
- **Impact**: Re-validation after fix: 13,133 URLs (45%) were false positives and kept their content; 15,866 had genuinely gone products
- **Restore process**: Content backed up in `pa.content_history` → re-insert into `content_urls_joep`, set `kopteksten_check` to 'completed', set `werkvoorraad.kopteksten = 1`, clear `link_validation_results` entries, re-run validation
- **File**: `backend/link_validator.py` — both content validator and FAQ validator had the same bug
- **Date**: 2026-02-06

## Canonical Generator FACET+FACET Logic
- **Purpose**: Canonicalize URLs with multiple facets to URLs with fewer facets (remove redundant facet)
- **How it works**: Given old_facet (e.g. `merk~nike`) and new_facet (e.g. `productlijn~air-max`):
  1. Fetch URLs containing BOTH old_facet AND new_facet from Redshift (`contains_all` parameter)
  2. Remove the old_facet from the URL, keeping the new_facet
  3. Example: `beslist.nl/c/merk~nike~~productlijn~air-max` → `beslist.nl/c/productlijn~air-max`
- **Key**: Uses `contains_all` (multiple AND LIKE conditions) in SQL, not just a single `contains`
- **File**: `backend/canonical_service.py` — functions: `_apply_facet_facet()`, `fetch_urls_from_redshift()`, `fetch_urls_for_rules()`
- **Date**: 2026-02-06

## Stuck Pending URLs - Tracking Table Covers All Werkvoorraad URLs
- **Problem**: Frontend shows 0 pending URLs despite ~32K URLs not having content
- **Cause**: ALL URLs in `pa.jvs_seo_werkvoorraad` also exist in `pa.jvs_seo_werkvoorraad_kopteksten_check` (tracking table), even those with status='pending' that were never actually processed
- **Root Cause**: The pending calculation uses:
  ```sql
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad w
  LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check t ON w.url = t.url
  WHERE t.url IS NULL
  ```
  This returns 0 when every werkvoorraad URL has a corresponding tracking entry, regardless of status.
- **Symptoms**:
  - Frontend kopteksten status shows "Pending: 0"
  - Tracking table has entries with status='pending' that block the LEFT JOIN
  - Total URLs and content counts don't add up
- **Diagnosis**:
  ```sql
  -- Check if tracking table covers all werkvoorraad
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad;          -- e.g., 243,702
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad_kopteksten_check;  -- same number = problem

  -- Check tracking status breakdown
  SELECT status, COUNT(*) FROM pa.jvs_seo_werkvoorraad_kopteksten_check GROUP BY status;
  -- Look for 'pending' entries - these are blocking the pending count
  ```
- **Solution**: Delete the 'pending' (and optionally 'failed') entries from tracking table:
  ```sql
  DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE status = 'pending';
  -- Optionally also: DELETE ... WHERE status = 'failed';
  ```
- **Result**: URLs removed from tracking become truly pending and show up in frontend
- **Prevention**: This happens when URLs are bulk-loaded into both werkvoorraad AND tracking simultaneously. Only load URLs into werkvoorraad; the tracking table should only be populated by the processing workflow.
- **Date**: 2026-02-06

## DMA Script Tree Structure Reference

### Listing Group Tree Variants (campaign_processor.py)

**V2 tree (build_listing_tree_for_inclusion_v2)** — no CL1:
```
ROOT → CL3=shop_name(subdiv) → CL4=maincat_id(unit, positive) + CL4 OTHERS(negative)
     → CL3 OTHERS(negative)
```

**V1+CL1 tree (build_listing_tree_with_cl1)** — with CL1:
```
ROOT → CL3=shop_name(subdiv) → CL4=maincat_id(subdiv) → CL1=cl1(unit, positive) + CL1 OTHERS(negative)
                               → CL4 OTHERS(negative)
     → CL3 OTHERS(negative)
```

### Key Constraints
- **No UPDATE on listing groups** — only CREATE and REMOVE. To change a value (e.g. CL3 shop name), remove entire tree and rebuild.
- **SUBDIVISION requires OTHERS** — when creating a subdivision node, its OTHERS case MUST be in the same mutate operation.
- **Temporary resource names** — use `next_id()` to link nodes within the same mutate, then extract actual names from response for subsequent mutates.
- **Response index formula** — for `build_listing_tree_with_cl1` MUTATE 1: CL4 subdivision for maincat at index `i` is at `resp1.results[4 + i*2]`.
- **UNIT→SUBDIVISION conversion pattern** — when a node (e.g. CL4) is a UNIT but needs children added (e.g. CL1), you must: (1) REMOVE the old UNIT, (2) CREATE a SUBDIVISION with same dimension/parent, (3) CREATE children under the new SUBDIVISION — all in one atomic mutate. Used in both `_add_cl0_exclusion_to_ad_group` (CL0 under leaf) and `validate_cl1_targeting_for_ad_group` (CL1 under CL4). **Date**: 2026-03-19

### Sheet Processing Functions
| Function | Sheet | Input | Purpose |
|----------|-------|-------|---------|
| `process_check_sheet` | "check" | shop_name, maincat_id, cl1 | Replace pipe-version CL3 exclusions via cat_ids lookup |
| `process_check_cl1_sheet` | "toevoegen" | shop_name, maincat, maincat_id, cl1 | Check and rebuild trees missing CL1 targeting |
| `process_check_new_sheet` | "check_new" | shop_name, ad_group_name, campaign_name | Replace pipe-version CL3 subdivision targeting directly |

- **Date**: 2026-02-06

## DMA Script Batch Processing Optimization
- **Purpose**: Optimize Google Ads campaign processing functions to reduce API calls by 90%+
- **Pattern**: Group shops by (maincat_id, custom_label_1) and process together instead of individually
- **Key Functions Optimized**:
  1. `process_reverse_exclusion_sheet()` - Removes shop exclusions (CL3) in batches
  2. `process_exclusion_sheet_v2()` - Adds shop exclusions (CL3) in batches
  3. `process_uitbreiding_sheet()` - Creates campaigns/ad groups, now finds campaign once per group
- **New Batch Functions**:
  - `reverse_exclusion_batch(client, customer_id, ad_group_id, ad_group_name, shop_names)` - Removes multiple shop exclusions in one API call
  - `add_shop_exclusions_batch(client, customer_id, ad_group_id, ad_group_name, shop_names)` - Adds multiple shop exclusions in one API call
- **How It Works**:
  1. Read sheet and group rows by (maincat_id, cl1) key
  2. For each group, look up deepest_cats from cat_ids sheet ONCE
  3. For each campaign/ad group, read listing tree ONCE for all shops
  4. Add/remove all exclusions in single batch mutate operation
- **Efficiency Gain**: ~318,468 API calls → ~28,152 API calls (91% reduction) for typical workload
- **File**: `/home/joepvanschagen/projects/dma_script/campaign_processor.py`
- **Test Files**: `test_reverse_exclusion_optimized.py`, `test_reverse_exclusion_integration.py`
- **Date**: 2026-02-04

## Excel UTF-8 Encoding Fix
- **Problem**: Excel shows garbled text like "KÃ¼ppersbusch" instead of "Küppersbusch"
- **Cause**: UTF-8 text was incorrectly decoded as Latin-1/Windows-1252
- **Solution**: Re-encode as Latin-1 then decode as UTF-8:
  ```python
  def fix_encoding(text):
      return str(text).encode('latin-1').decode('utf-8')
  ```
- **Use Case**: Fixed 635 entries in symbols.xlsx
- **Date**: 2026-02-04

## Efficient Bucket Performance Query Pattern
- **Problem**: Need to aggregate visits/revenue for 628K+ buckets without 628K queries
- **Solution**: Single query approach:
  1. Query ALL URLs with visits/revenue from Redshift (no bucket filtering)
  2. Extract bucket patterns from URLs using regex: `r'([a-zA-Z0-9_]+~\d+)'`
  3. Match to bucket set in Python (O(1) lookup per bucket)
  4. Aggregate results per bucket
- **Performance**: ~2-3 minutes for 628K buckets vs days with individual queries
- **Example Query**:
  ```sql
  SELECT SPLIT_PART(dv.url, '?', 1) as url, count(*) as visits,
         sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) as revenue
  FROM datamart.fct_visits fcv
  JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
  WHERE dv.url LIKE '%beslist.nl/products/%/c/%'
  GROUP BY 1
  ```
- **Date**: 2026-02-04

## User Preferences
- **Default Project**: When user says "the frontend" or "start the frontend" without specifying a project, always assume **dm-tools**
- **Frontend URL**: Always use http://localhost:8003/static/index.html (served by the backend via docker-compose, not a separate server)
- **Skip Permissions**: User can say "skip-permissions" mid-conversation to skip permission prompts (configured in `~/.claude/settings.json`)
- **Date**: 2026-01-27

## Recheck Skipped URLs Feature
- **Purpose**: Re-check URLs that were skipped during content/FAQ generation to see if products are now available
- **Reason for Skip**: URLs get status='skipped' with reason 'no_products_found' when scraper finds no products
- **How It Works**:
  1. Fetches URLs with `status='skipped'` that haven't been rechecked yet
  2. Re-scrapes each URL via Product Search API to check if products are now available
  3. If products found: removes URL from tracking table → gets picked up for content generation
  4. If still no products: marks as "rechecked" to avoid infinite loops
- **Tracking Table**: `pa.url_validation_tracking` (shared across kopteksten and FAQ since 2026-03-20)
  - Previously: SEO used `pa.jvs_seo_werkvoorraad_kopteksten_check`, FAQ used `pa.faq_tracking` separately
  - Now: Both features read/write skipped URLs from the shared table
  - FAQ recheck delegates to kopteksten recheck endpoint
- **API Endpoints**:
  - `POST /api/recheck-skipped-urls` - Recheck skipped URLs (used by both SEO and FAQ)
  - `POST /api/faq/recheck-skipped-urls` - Recheck FAQ skipped URLs (delegates to kopteksten recheck)
  - `DELETE /api/recheck-skipped-urls/reset` - Reset recheck markers to allow rechecking again
  - `DELETE /api/faq/recheck-skipped-urls/reset` - Reset FAQ recheck markers
- **Parameters**: `parallel_workers` (1-20), `batch_size` (configurable via UI)
- **UI**: "Recheck Skipped" button next to "Validate All" on both SEO and FAQ pages
- **Date**: 2026-02-01, updated 2026-03-20 (shared tracking table)

## N8N Integration Setup
- **N8N Skills**: Installed 7 skills at `~/.claude/skills/` from [n8n-skills](https://github.com/czlonkowski/n8n-skills)
  - n8n-expression-syntax, n8n-mcp-tools-expert, n8n-workflow-patterns
  - n8n-validation-expert, n8n-node-configuration, n8n-code-javascript, n8n-code-python
- **N8N MCP Server**: Configured at `~/.claude/mcp.json`
  - URL: `https://n8n.aks.mgmt.beslist.nl`
  - Requires Node.js 22+ (upgraded via nvm)
  - Full path to npx: `/home/joepvanschagen/.nvm/versions/node/v22.22.0/bin/npx`
- **Date**: 2026-01-27

## Database Sync to N8N Vector DB
- **Purpose**: Copied all dm-tools tables to N8N's PostgreSQL for use in n8n workflows
- **Target Database**:
  - Host: `10.1.32.9` (internal: `n8n-vector-db-rw.n8n.svc.cluster.local`)
  - Database: `n8n-vector-db`
  - User: `dbadmin`
- **Tables Copied** (15 tables, ~2.3M rows):
  - `pa.content_urls_joep` (220K rows) - SEO content
  - `pa.faq_content` (241K rows) - FAQ content
  - `pa.unique_titles` (1M rows) - AI-generated titles
  - `pa.jvs_seo_werkvoorraad` - Work queue
  - `pa.link_validation_results`, `pa.faq_validation_results` - Validation results
  - `pa.content_history` - Content backup
  - Plus tracking tables and thema_ads tables
- **Script Pattern**: Used `psycopg2.extras.execute_values()` with JSONB handling via `Json()` wrapper
- **Date**: 2026-01-27

## CSV Encoding Fix for Excel
- **Problem**: Excel shows garbled characters like "CafetiÃ¨res" instead of "Cafetières"
- **Cause**: Excel defaults to Latin-1 encoding when opening CSV files without BOM
- **Solution**: Add UTF-8 BOM (Byte Order Mark) at start of file:
  ```bash
  printf '\xEF\xBB\xBF' > fixed.csv && cat original.csv >> fixed.csv
  ```
- **Verification**: `file` command should show "UTF-8 (with BOM) text"
- **Date**: 2026-01-27

## Canonical URL Generator
- **Purpose**: Replaces Google Apps Script + Google Sheets workflow for generating canonical URLs
- **Data Source**: Redshift (`datamart.fct_visits` + `datamart.dim_visit`) instead of GA4
- **Transformation Types**:
  - CAT-CAT: Replace category slug (e.g., `schoenen_430884` → `schoenen_430885`)
  - FACET-FACET: Replace facet value (e.g., `merk` → `populaire_serie`)
  - CAT+FACET: Change category for faceted URLs, keep facet
  - CAT+FACET1: Change category for faceted URLs, remove facet
  - BUCKET+BUCKET: Replace bucket value (e.g., `merk~23597985` → `populaire_serie~2590809`)
  - REMOVEBUCKET: Remove bucket from URL
- **Files**:
  - Backend: `backend/canonical_service.py`
  - Frontend: `frontend/canonical.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/canonical/generate` - Generate canonicals
  - `POST /api/canonical/preview` - Preview affected URLs
  - `GET /api/canonical/fetch-urls` - Search Redshift URLs
  - `POST /api/canonical/transform` - Test single URL
- **Bug Fix**: Regex `re.sub(r'/+', '/', url)` also replaced `://` in URLs; fixed with `re.sub(r'(?<!:)//+', '/', url)`
- **Date**: 2026-01-27

## Redirect Checker Tool
- **Purpose**: Check HTTP status codes, redirect URLs, and canonical URLs for input URLs
- **User Agent**: `"Beslist script voor SEO"` (same as other scraper tools)
- **Base URL**: `https://www.beslist.nl` (for relative URL normalization)
- **Features**:
  - Check status codes (200, 301, 302, 404, etc.)
  - Detect redirect URLs (Location header from first request without following redirects)
  - Extract canonical URLs from HTML using regex
  - Parallel workers with rate limiting (token bucket algorithm)
  - Configurable timeout, workers (default: 20), and rate limit (default: 2 req/sec)
  - Click-to-copy for URL cells in results table
  - CSV and Excel export
  - Streaming progress updates via NDJSON
- **Files**:
  - CLI Script: `redirect_checker.py` (standalone, can process Excel/CSV files)
  - Frontend: `frontend/redirect-checker.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/redirect-checker/check` - Check URLs (streaming NDJSON response)
  - `POST /api/redirect-checker/download` - Download results as Excel
- **Status Code Logic**: Shows initial status code (301/302) not final status (200) - captures the redirect before following it
- **URL Normalization**: Relative URLs starting with `/` are prefixed with `https://www.beslist.nl`
- **Date**: 2026-01-30

## R-Finder Tool
- **Purpose**: Find /r/ URLs from Redshift visits data (replaces Google Apps Script that queried GA4)
- **Data Source**: Redshift (`datamart.fct_visits` + `datamart.dim_visit`) - same as Canonical Generator
- **Features**:
  - Filter by multiple keywords (AND logic - URL must contain ALL filters)
  - Date range filtering (default: 2015-01-01 to today)
  - Minimum visits threshold
  - Copy-to-clipboard with tab-separated output for Excel
  - Relative URLs in output for easier copying
- **Exclusions** (same as original GA4 script):
  - `device=`, `/sitemap/`, `sortby=`, `/filters/`, `/page_`, `shop_id=`, `+`
  - Mismatched category combinations (e.g., `/cadeaus_gadgets_culinair/meubilair_`)
- **Files**:
  - Backend: `backend/rfinder_service.py`
  - Frontend: `frontend/rfinder.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/rfinder/search` - Search for /r/ URLs with filters
  - `GET /api/rfinder/stats` - Get total URL/visits statistics
- **Troubleshooting**: URLs with all filter terms may have very few visits - check all-time data if recent date range returns 0 results
- **Date**: 2026-01-29

## Link Validator V4 UUID Support
- **Problem**: Product URLs with V4 UUID format were incorrectly marked as "gone" during link validation
- **URL Formats Supported**:
  1. Old: `/p/gezond_mooi/nl-nl-gold-6150802976981/`
  2. New numeric: `/p/product-name/286/6150802976981/`
  3. V4 UUID: `/p/product-name/137/V4_2f09146b-402b-48d0-b966-655e1416a43d/`
- **Cause**: `extract_from_url()` only checked `potential_pim_id.isdigit()`, which returned False for V4 UUIDs
- **Solution**: Added explicit check for `potential_pim_id.startswith('V4_')` before the numeric check
- **Impact**: Both SEO content validation and FAQ validation use the same `extract_from_url()` function
- **Location**: `backend/link_validator.py` - `extract_from_url()`
- **Date**: 2026-01-24

## Performance Optimizations
- **Connection Pools**:
  - PostgreSQL: `maxconn` increased from 10 → 20
  - Redshift: `maxconn` increased from 5 → 10
  - Scraper HTTP pool: `pool_connections` and `pool_maxsize` increased from 1 → 10
- **Verbose Logging Removed**: Connection pool logging was causing I/O overhead on every connection
- **Combined Status Queries**: Status endpoint reduced from 5 separate COUNT queries to 1 combined query
- **Batched DB Updates**: Link validation now uses `executemany()` instead of individual UPDATE loops
- **Database Indexes Added**:
  - `idx_content_urls_url` (UNIQUE) on `pa.content_urls_joep(url)`
  - `idx_werkvoorraad_check_url` on `pa.jvs_seo_werkvoorraad_kopteksten_check(url)`
  - `idx_werkvoorraad_check_status` on `pa.jvs_seo_werkvoorraad_kopteksten_check(status)`
  - `idx_link_validation_content_url` on `pa.link_validation_results(content_url)`
- **Location**: `backend/database.py`, `backend/main.py`, `backend/scraper_service.py`
- **Date**: 2026-01-24

## GPT URL Truncation Fix
- **Problem**: GPT sometimes truncates product URLs in generated content
  - Full URL: `https://www.beslist.nl/p/product-name/452/8718969401258/`
  - Truncated: `https://www.beslist.nl/p/product-name/` (missing maincat_id and pimId)
- **Impact**: Found 6,486 content items with 22,317 truncated/broken links
- **Solution**: Added `fix_truncated_urls()` function in `backend/gpt_service.py`
  - Builds mapping of product-name slugs to full URLs from original product list
  - Finds truncated URLs in GPT output using regex
  - Replaces them with correct full URLs
  - Logs how many URLs were fixed
- **When Applied**: Automatically runs after GPT generates content in `generate_product_content()`
- **Location**: `backend/gpt_service.py` - `fix_truncated_urls()`
- **Date**: 2026-01-25

## Content History Backup Table
- **Purpose**: Backup content before deletion during link validation or resets
- **Table**: `pa.content_history`
- **Columns**:
  - `url` (TEXT) - The category URL
  - `content` (TEXT) - The backed up content
  - `reset_reason` (TEXT) - Why it was reset (e.g., 'gone_products', 'truncated_urls')
  - `reset_details` (JSONB) - Additional details (e.g., list of gone URLs)
  - `original_created_at` (TIMESTAMP) - When the content was originally created
  - `reset_at` (TIMESTAMP) - When the backup was made
- **Used By**: Link validator, manual resets
- **Location**: `backend/database.py`, `backend/main.py`
- **Date**: 2026-01-25

## Minimum Offer Count for Content Generation
- **Change**: Increased minimum shopCount from 2 to 3 for including product links
- **Applies To**:
  - SEO Content: `backend/scraper_service.py:548` - `shop_count >= 3`
  - FAQ Content: `backend/faq_service.py:485` - `shop_count >= 3`
- **Does NOT Apply To**:
  - Link Validator: Still uses `min_offers=2` (existing content not affected)
- **Reason**: Ensures better quality PLPs are linked in generated content
- **Date**: 2026-01-25

## AI Title Generation Service
- **Purpose**: Generates SEO-optimized titles using productsearch API + OpenAI
- **Location**: `backend/ai_titles_service.py`
- **Frontend**: Unique Titles Manager (`/static/unique-titles.html`)
- **API Endpoints**:
  - `GET /api/ai-titles/status` - Get processing status and stats
  - `POST /api/ai-titles/start?batch_size=100&num_workers=15` - Start AI title generation
  - `POST /api/ai-titles/stop` - Stop processing
  - `GET /api/ai-titles/recent` - Get recently processed titles
- **Processing Method**:
  - **Faceted URLs** (containing `~~` or `/c/`): Uses productsearch API to get H1 and facet data, then OpenAI improves it
  - **Non-faceted URLs**: Falls back to scraping + OpenAI
  - **Workers**: Configurable parallel workers (default 15), each with 0.5s delay (max 2 URLs/worker/sec)
- **Database columns added to `pa.unique_titles`**:
  - `ai_processed` (BOOLEAN) - Whether URL has been processed
  - `ai_processed_at` (TIMESTAMP) - When it was processed
  - `ai_error` (TEXT) - Error message if failed
  - `original_h1` (TEXT) - Original H1 before AI rewrite
- **Generated Content**:
  - **H1**: AI-improved title from API (e.g., "FRESK groene RVS BPA vrij waterflessen")
  - **Title**: `{H1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl`
  - **Description**: `Zoek je {H1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {H1} met !!DISCOUNT!! korting online! &#10062; beslist.nl`
- **OpenAI Prompt Rules**:
  - Facet values must stay intact (e.g., "Rode Duivels" is one theme, not split)
  - Brand always first (e.g., "Apple iPhones" not "iPhones van Apple")
  - ALWAYS use adjectives for colors/materials (e.g., "Houten bank" not "bank van hout")
  - NEVER use "in" or "van" for colors/materials
- **Post-processing**:
  - `format_dimensions()`: "31 cm 115 cm" → "31 cm x 115 cm"
  - `normalize_preposition_case()`: Lowercase prepositions unless at start of sentence
  - **Lowercase words**: met, in, zonder, van, voor, tot, op, aan, uit, bij, naar, over, onder, tegen, tussen, door, om, en, of
- **Date**: 2026-01-23

## Docker Commands
```bash
# Development
docker-compose up              # Run with logs
docker-compose up -d           # Run in background
docker-compose logs -f app     # View app logs
docker-compose down            # Stop everything
docker-compose down -v         # Stop and remove volumes

# Debugging
docker-compose ps              # Check status
docker exec -it <container> bash  # Enter container

# Import data from local file into container
docker cp /path/to/file container_name:/tmp/file
docker-compose exec -T db psql -U postgres -d dbname -c "COPY table (column) FROM '/tmp/file';"

# CSV import from Windows to container
docker cp /path/to/file.csv content_top_app:/app/file.csv
docker-compose exec app python -m backend.import_content

# Access Frontend
# Navigate to http://localhost:8003/static/index.html
```

## Common Issues & Solutions

### Beslist Product Search API Facet Validation Errors
- **Problem**: FAQ processor API calls fail with HTTP 400 for certain URLs
- **Cause**: URLs contain facet names or value IDs that are no longer valid for that category
- **Error Response Types**:
  - Invalid facet name: `{"context": "facet", "errorInfo": "The given facet is not valid.", "value": "personage"}`
  - Invalid facet value: `{"context": "merk", "errorInfo": "The given facet value is not valid.", "value": 19957206}`
- **Solution**: Detect 400 errors with "not valid" in errorInfo, return `facet_not_available` error type instead of generic `api_failed`
- **FAQ Processor Error Reasons Reference**:
  - `facet_not_available` - URL contains invalid facet name or value ID for category
  - `api_failed` - Generic API failure (non-400 or unparseable error)
  - `no_products_found` - API returned 0 products (skipped)
  - `faq_generation_failed` - OpenAI generation failed
- **Location**: backend/faq_service.py - `fetch_products_api()` and `process_single_url_faq()`
- **Date**: 2025-12-26

### External API SQL Escaping Errors
- **Problem**: Website-configuration API returns MySQL INSERT exception when content contains apostrophes
- **Error**: `An exception occurred while executing 'INSERT INTO ... VALUES ('...DVD's...')`
- **Cause**: External API's MySQL INSERT not properly escaping single quotes in content
- **Additional Issue**: Legacy content contains double single quotes (`''`) which need normalization
- **Solution**: Sanitize content before sending to external API:
```python
def sanitize_for_api(text: str) -> str:
    if not text:
        return ""
    # First normalize double single quotes to single (legacy data issue)
    text = text.replace("''", "'")
    # Then replace single quotes with HTML entity
    return text.replace("'", "&#39;")
```
- **Location**: backend/content_publisher.py - `sanitize_for_api()`
- **Date**: 2026-01-15

### Background Task Pattern for Long-Running Operations
- **Problem**: Browser times out when API operations take >30 seconds (e.g., publishing 162K+ URLs)
- **Solution**: Use threading with task_id polling pattern:
  1. Start background thread with unique task_id
  2. Return task_id immediately to client
  3. Client polls status endpoint every 2 seconds
  4. Background thread updates shared dict with progress/result
```python
_tasks = {}
_task_lock = threading.Lock()

def start_task(params) -> str:
    task_id = str(uuid.uuid4())[:8]
    with _task_lock:
        _tasks[task_id] = {"status": "pending", ...}
    thread = threading.Thread(target=_run_task, args=(task_id, params))
    thread.daemon = True
    thread.start()
    return task_id

def get_task_status(task_id: str) -> Dict:
    with _task_lock:
        return _tasks.get(task_id, {"error": "Task not found"})
```
- **Location**: backend/content_publisher.py - `start_publish_task()`, `get_publish_task_status()`
- **Date**: 2026-01-15

### Orphaned URLs After Content Deletion
- **Problem**: URLs become "lost" when content is deleted (validation/regeneration) but URL not in werkvoorraad
- **Symptoms**: Total URL count drops, URLs cannot be reprocessed
- **Cause**: Content deleted from `content_urls_joep` but URL never existed in `jvs_seo_werkvoorraad`
- **Solution**: When deleting content, always ensure URLs are added to werkvoorraad:
```python
# After deleting content, add URLs to werkvoorraad for reprocessing
for url in deleted_urls:
    cur.execute("""
        INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
        VALUES (%s, 0)
        ON CONFLICT (url) DO UPDATE SET kopteksten = 0
    """, (url,))
```
- **Recovery**: Check for orphaned URLs in validation_results or tracking tables and add to werkvoorraad
- **Location**: backend/main.py - validate-links and validate-all-links endpoints
- **Date**: 2025-12-15

### GPT Generating Multiple Paragraphs
- **Problem**: Generated content contains `\n\n` (double newlines) creating multiple paragraphs
- **Solution**: Add explicit instruction to BOTH system message and user prompt:
  - System: `"Schrijf ALTIJD als één doorlopende alinea zonder witregels of meerdere paragrafen."`
  - User: `"Schrijf de tekst als EEN doorlopende alinea, GEEN meerdere paragrafen of witregels."`
- **Location**: backend/gpt_service.py
- **Date**: 2025-12-15

### Connection Pool Mismatch in Export Functions
- **Error**: `trying to put unkeyed connection`
- **Cause**: Using `get_output_connection()` but returning with `return_db_connection()` - wrong pool
- **Solution**: Always use matching return function for connection type:
  - `get_db_connection()` → `return_db_connection()`
  - `get_output_connection()` → `return_output_connection()`
  - `get_redshift_connection()` → `return_redshift_connection()`
- **Location**: backend/main.py - export endpoints
- **Date**: 2025-12-15

### Illegal Characters in Excel Export (openpyxl)
- **Error**: `\u0011 cannot be used in worksheets`
- **Cause**: Content contains control characters that Excel doesn't allow
- **Solution**: Sanitize content before writing to Excel worksheet:
```python
import re
# Remove control characters except tab, newline, carriage return
illegal_chars = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
content = illegal_chars.sub('', content)
```
- **Location**: backend/main.py - `/api/export/xlsx` endpoint
- **Date**: 2025-12-15

### GPT Content Truncation Mid-Entity
- **Problem**: Generated content cut off mid-HTML entity (e.g., `&amp` without `;`)
- **Cause**: OpenAI `max_tokens` limit reached before response completed
- **Symptoms**: Content ends with incomplete entities like `&amp`, `&quot`, `&#123`
- **Solution**:
  1. Increase `max_tokens` (500 → 1000 for ~100 word content with HTML)
  2. Check `finish_reason` to detect truncation:
```python
response = client.chat.completions.create(...)
if response.choices[0].finish_reason == "length":
    print(f"Warning: Response was truncated")
```
- **Location**: backend/gpt_service.py (line 89)
- **Date**: 2025-12-15

### Redshift Serializable Isolation Violation (Error 1023)
- **Error**: `Error: 1023 DETAIL: Serializable isolation violation on table - 37521601, transactions forming the cycle are: 573354047, 573354048, 573354046 (pid:1073775083)`
- **Cause**: Multiple concurrent batch jobs updating the same Redshift table (`pa.jvs_seo_werkvoorraad_shopping_season`) with individual UPDATE statements in loops
- **Symptoms**:
  - Frontend shows "Error: 1023 DETAIL: Serializable isolation violation"
  - Occurs when running multiple concurrent batches
  - Transaction cycles formed by N individual UPDATEs competing for same rows
- **Root Cause**: Individual UPDATE per URL in loop creates many small transactions:
  ```python
  # ❌ Wrong: Individual UPDATEs cause serialization conflicts
  for (url,) in update_werkvoorraad_success_urls:
      output_cur.execute("""
          UPDATE pa.jvs_seo_werkvoorraad_shopping_season
          SET kopteksten = 1
          WHERE url = %s
      """, (url,))
  ```
- **Solution**: Replace with batch UPDATE operations using IN clauses:
  ```python
  # ✅ Correct: Single batch UPDATE prevents conflicts
  if update_werkvoorraad_success_urls:
      url_list = [url for (url,) in update_werkvoorraad_success_urls]
      placeholders = ','.join(['%s'] * len(url_list))
      output_cur.execute(f"""
          UPDATE pa.jvs_seo_werkvoorraad_shopping_season
          SET kopteksten = 1
          WHERE url IN ({placeholders})
      """, url_list)
  ```
- **Impact**:
  - Eliminates serialization conflicts in concurrent batch processing
  - Reduces transaction count from N individual UPDATEs to 1 batch UPDATE
  - Shorter transaction time reduces collision window
  - 15-20% throughput improvement (fewer round-trips to Redshift)
- **Locations Fixed**:
  - backend/main.py:295-317 (batch processing endpoint - success and processed updates)
  - backend/main.py:770-791 (link validation endpoint - broken link resets)
- **Date**: 2025-10-28

### Docker/WSL Integration
- **Error**: `docker-compose: command not found` in WSL 2
- **Cause**: Docker Desktop WSL integration not enabled
- **Solution**: Enable WSL integration in Docker Desktop settings
  - Open Docker Desktop → Settings → Resources → WSL Integration
  - Enable integration for your WSL distro
  - Restart WSL terminal
- **Documentation**: https://docs.docker.com/go/wsl2/

### FastAPI Async Endpoints with psycopg2 ThreadedConnectionPool
- **Problem**: API endpoint hangs indefinitely at `get_output_connection()` call. The async event loop was blocked by synchronous `getconn()` from ThreadedConnectionPool
- **Symptoms**:
  - First batch processes successfully
  - Second batch hangs forever at database connection
  - Logs show "[ENDPOINT] Getting output connection..." but never reach "[POOL] get_output_connection() called"
  - No errors, no timeouts - just infinite hang
- **Root Cause**: FastAPI async endpoint calling synchronous blocking psycopg2 pool operations
  - `async def` endpoint uses asyncio event loop
  - `pool.getconn()` is synchronous and blocks
  - Blocking the event loop prevents any async operations from completing
  - Even `await loop.run_in_executor()` doesn't fully solve it due to connection pool thread safety
- **Solution**: Convert endpoint from `async def` to `def` (synchronous)
```python
# ❌ Wrong: Async endpoint with sync database pool
@app.post("/api/process-urls")
async def process_urls():
    conn = get_output_connection()  # Blocks event loop!

# ✅ Correct: Synchronous endpoint
@app.post("/api/process-urls")
def process_urls():
    conn = get_output_connection()  # No event loop blocking
```
- **Alternative**: Use async-compatible driver (asyncpg) if async is required, but adds complexity
- **Impact**: Immediate fix - endpoint processes multiple batches successfully
- **Location**: backend/main.py (line 181), backend/database.py (connection pool functions)
- **Date**: 2025-10-23

### Redshift executemany() Blocking Indefinitely
- **Problem**: Second API request hangs at "[POOL] Getting Redshift connection..." - connection pool exhaustion
- **Symptoms**:
  - First request succeeds and completes
  - Second request waits forever for a Redshift connection
  - Logs show "Got Redshift connection" but never "Returned Redshift connection"
  - Connection pool exhausted (maxconn=5, all connections stuck)
- **Root Cause**: Redshift doesn't handle `executemany()` well with INSERT/UPDATE statements
  - Batch operations block indefinitely
  - Connection never completes transaction
  - Connection never returned to pool
  - Subsequent requests wait forever for available connection
- **Solution**: Replace all `executemany()` calls with individual `execute()` loops
```python
# ❌ Wrong: Blocks indefinitely on Redshift
if insert_content_data:
    output_cur.executemany("""
        INSERT INTO pa.content_urls_joep (url, content)
        VALUES (%s, %s)
    """, insert_content_data)

# ✅ Correct: Individual executes
if insert_content_data:
    print(f"[ENDPOINT] Inserting {len(insert_content_data)} content records...")
    for url, content in insert_content_data:
        output_cur.execute("""
            INSERT INTO pa.content_urls_joep (url, content)
            VALUES (%s, %s)
        """, (url, content))
    print(f"[ENDPOINT] Content inserts complete")
```
- **Performance**: Slightly slower than executemany() but actually completes (vs hanging forever)
- **Note**: executemany() works fine on PostgreSQL, only Redshift has this issue
- **Testing**: Verified 3 sequential requests complete successfully after fix
- **Location**: backend/main.py (lines 286-315)
- **Date**: 2025-10-23

### Redshift SQL Differences - ON CONFLICT Not Supported
- **Problem**: URL upload fails with syntax error: "syntax error at or near 'ON'"
- **Cause**: PostgreSQL's `ON CONFLICT DO NOTHING` syntax not supported by Redshift
- **Impact**: Cannot use INSERT ... ON CONFLICT for duplicate handling in Redshift
- **Solution**: Use batch checking strategy instead:
  1. Query existing URLs with WHERE IN (batches of 500)
  2. Filter duplicates in Python using set difference
  3. Batch insert only new URLs with executemany()
- **Example**:
```python
# Get existing URLs in batches
existing_urls = set()
batch_size = 500
for i in range(0, len(urls), batch_size):
    batch = urls[i:i + batch_size]
    placeholders = ','.join(['%s'] * len(batch))
    cur.execute(f"SELECT url FROM table WHERE url IN ({placeholders})", batch)
    existing_urls.update(row['url'] for row in cur.fetchall())

# Filter and insert new URLs
new_urls = [(url,) for url in urls if url not in existing_urls]
cur.executemany("INSERT INTO table (url) VALUES (%s)", new_urls)
```
- **Performance**: Batching queries (500 URLs per query) keeps Redshift queries fast
- **Location**: backend/main.py - `/api/upload-urls` endpoint (lines 463-542)
- **Date**: 2025-10-21

### Data Consistency Issue: Local Content Not Synced to Redshift
- **Problem**: 69,391 URLs had content locally, but only 60,000 had kopteksten=1 in Redshift (9,567 URLs out of sync)
- **Cause**: Batch processing completed locally but Redshift updates were lost or incomplete due to:
  - Network interruptions during batch commits
  - Interrupted processing sessions before Redshift sync
  - Failed Redshift UPDATE operations (silent failures)
- **Symptoms**:
  - System shows 50k+ pending URLs but only processes 24 per batch
  - Filtering logic excludes URLs that have local content but kopteksten=0 in Redshift
  - Progress stalls despite thousands of "pending" URLs
  - Status counts don't match actual content count
- **Impact**: URLs with completed content stuck in pending state, wasting processing cycles
- **Solution**: Created `backend/sync_redshift_flags.py` script to sync local content with Redshift
  - Queries `pa.content_urls_joep` (local content table - source of truth)
  - Updates Redshift `kopteksten=1` for all URLs with content
  - Batch updates (1000 URLs per query) for performance
  - Safe to run anytime (idempotent, only updates kopteksten=0 → kopteksten=1)
- **Script Usage**:
```bash
docker-compose exec -T app python -m backend.sync_redshift_flags
```
- **Result**: Synced 9,567 URLs, pending count dropped from 50,345 to 40,754 (accurate)
- **Prevention**: Run sync script after interrupted sessions or if progress stalls
- **Location**: backend/sync_redshift_flags.py, backend/main.py (filtering logic updated)
- **Date**: 2025-10-22

### Frontend Showing NaN/undefined in Batch Progress
- **Problem**: Frontend displays "Batch 1 Complete: undefined successful, NaN failed/skipped" during batch processing
- **Cause**: JavaScript directly using `data.processed` and `data.total_attempted` without null/undefined checks
- **Symptoms**:
  - Progress text shows "undefined" and "NaN" instead of numbers
  - Happens when API response has missing or undefined fields
  - Calculations like `total_attempted - processed` produce NaN
- **Solution**: Add default values using || operator:
```javascript
const batchProcessed = data.processed || 0;
const batchTotal = data.total_attempted || 0;
const batchFailed = batchTotal - batchProcessed;
```
- **Benefits**: Safe handling of undefined/null values, always displays valid numbers
- **Location**: frontend/js/app.js (lines 219-242)
- **Date**: 2025-10-22

### Beslist.nl Hidden 503 Errors in HTML Body
- **Problem**: Scraper marks URLs as "no_products_found" when actually rate limited
- **Cause**: Beslist.nl returns HTTP 200 status with "503 Service Unavailable" in HTML body when rate limited
- **Impact**: 33,946 URLs incorrectly marked as failed/skipped due to undetected rate limiting
- **Detection**:
```python
if response.status_code == 200:
    # Check for hidden 503 in HTML body
    if '503' in response.text or 'Service Unavailable' in response.text:
        print(f"Scraping failed: Hidden 503 (rate limited) for {url}")
        return None  # Keep URL in pending for retry
```
- **Behavior**: Returning None from scraper keeps URL in pending state (not marked as processed)
- **Location**: backend/scraper_service.py (lines 119-123)
- **Date**: 2025-10-21

### Docker Network Connectivity Loss After Restart
- **Problem**: After restarting Docker, all network connections from container timeout (ping, DNS, HTTP requests)
- **Symptoms**:
  - `docker-compose exec -T app python3 -c "requests.get('https://beslist.nl')"` hangs/times out
  - Even basic commands fail: `ping 8.8.8.8` times out
  - DNS lookups fail: `nslookup beslist.nl` times out
  - Scraper returns "scraping_failed" for all URLs
- **Root Cause Options**:
  1. **Proxy environment variables**: `HTTP_PROXY`/`HTTPS_PROXY` in docker-compose.yml pointing to invalid/inaccessible proxy
  2. **VPN routing issues**: VPN split tunneling configuration broken after Docker restart
  3. **WSL2 network bridge**: WSL2 network adapter needs refresh after Docker restart
- **Diagnostic Commands**:
```bash
# Test from host (should work)
curl -A "Beslist script voor SEO" https://www.beslist.nl/

# Test from container (fails if network broken)
docker-compose exec -T app sh -c "ping -c 2 8.8.8.8"
docker-compose exec -T app sh -c "nslookup beslist.nl"
```
- **Solutions** (try in order):
  1. **WSL restart** (fixes most issues): `wsl --shutdown` from Windows PowerShell, then restart WSL terminal
  2. **Check/unset proxy variables**:
```bash
echo $HTTP_PROXY $HTTPS_PROXY  # Check if set
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
docker-compose down && docker-compose up -d
```
  3. **Remove proxy from docker-compose.yml**: Change `- HTTP_PROXY=${HTTP_PROXY:-}` to `- HTTP_PROXY=`
  4. **Check VPN configuration**: Verify VPN split tunneling still routes beslist.nl traffic correctly
- **Prevention**: After restarting Docker, always test basic connectivity before processing URLs
- **Location**: docker-compose.yml (lines 23-24), network configuration
- **Date**: 2025-10-21

### Port Conflicts
- FastAPI on 8003 (external) → 8000 (internal container port)
- PostgreSQL on 5433 (not 5432) for same reason
- Frontend accessible at http://localhost:8003/static/index.html

### CORS Errors
- Check `allow_origins` in main.py
- For dev: use `["*"]`
- For production: specify exact frontend URL

### Database Connection
- Wait for PostgreSQL to fully start
- Check DATABASE_URL in .env
- Run `docker-compose logs db` to debug

### Database Schema Column Missing
- **Error**: `column "status" does not exist`
- **Cause**: Schema changes applied to wrong database (postgres vs content_top)
- **Solution**: Check DATABASE_URL in docker-compose.yml, apply schema to correct database
- **Command**: `docker-compose exec -T db psql -U postgres -d content_top < backend/schema.sql`

### Pending Count Not Decreasing After Processing (UPDATED 2025-10-22)
- **Problem**: Pending count stays static at 11,756 even after processing 100 URLs, system shows "No URLs to process"
- **Root Cause**: Skipped and failed URLs were:
  1. ✅ Written to local PostgreSQL tracking table (pa.jvs_seo_werkvoorraad_kopteksten_check)
  2. ❌ **NOT updating the Redshift kopteksten flag** (pa.jvs_seo_werkvoorraad_shopping_season)
- **Symptoms**:
  - Redshift kept showing URLs as unprocessed (kopteksten=0)
  - System fetched same URLs repeatedly
  - Immediately filtered them out (already in local tracking)
  - Result: "No URLs to process" despite 11,756 pending
  - Pending count calculation: total_urls - tracked = constant (never decreases)
- **Impact**: URLs stuck in infinite loop, no progress possible
- **Solution (2025-10-20)**: Add `redshift_ops.append(('update_werkvoorraad', url))` for permanent failures:
  - no_products_found (line 86) - Page loads but has no products
  - no_valid_links (line 111) - AI generates content without valid links
  - ai_generation_error (line 127) - AI service error
- **Solution (2025-10-21)**: **REMOVED** Redshift update for scraping failures:
  - scraping_failed (line 79) - Network errors, 503, timeouts, access denied
  - **Reason**: Temporary network/access issues should be retried, not marked as permanently processed
  - **Behavior**: URLs with scraping failures stay in pending, can be retried on next run
  - **Status**: Local tracking still records 'failed' status for monitoring
- **Solution (2025-10-22)**: **Three-state tracking system** + **503-specific handling**:
  - kopteksten=0: Pending (not yet processed)
  - kopteksten=1: Has content (successfully processed)
  - kopteksten=2: Processed without content (skipped/failed non-503 errors)
  - **503 errors (rate_limited_503)**: NOT marked in Redshift, kept pending for retry, batch stops immediately
  - **Local tracking query changed**: Now filters ALL processed URLs (not just successful), preventing infinite retry loop
- **Result**:
  - Permanent failures (no products, bad content) → kopteksten=2 in Redshift
  - Successful content → kopteksten=1 in Redshift
  - 503 rate limiting → kopteksten=0 (stays pending), batch stops immediately
  - Non-503 scraping failures → kopteksten=2 (won't retry)
- **Location**: backend/main.py (lines 73-135, 247-260), backend/scraper_service.py (returns {'error': '503'})

### Frontend Showing N/A for Timestamps from Redshift
- **Problem**: Recent Results section showed "N/A" timestamps because Redshift output table (pa.content_urls_joep) lacks created_at column
- **Cause**: Redshift table schema doesn't include timestamp columns, but frontend expected created_at field
- **Symptoms**:
  - API returns `"created_at": null` for all recent results
  - Frontend displays "N/A" next to every URL
  - Local PostgreSQL has timestamps but Redshift doesn't
- **Solution Options**:
  1. **Query local PostgreSQL for timestamps** (implemented): Use separate connection to local database for recent results with timestamps
  2. **Hide timestamps in UI** (implemented): Conditionally render timestamp element only when data available
  3. **Add created_at to Redshift** (not implemented): Requires Redshift schema change and backfill
- **Implementation**:
```python
# Backend: Query local PostgreSQL for timestamps
try:
    local_conn = get_db_connection()  # Local PostgreSQL
    local_cur = local_conn.cursor()
    local_cur.execute("SELECT url, content, created_at FROM pa.content_urls_joep ORDER BY created_at DESC LIMIT 5")
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': r['created_at'].isoformat() if r['created_at'] else None} for r in local_cur.fetchall()]
except:
    # Fallback: Query Redshift without timestamps
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': None} for r in output_cur.fetchall()]
```
```javascript
// Frontend: Hide timestamp when null
const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';
itemDiv.innerHTML = `
    <h6>${item.url}</h6>
    ${dateText ? `<small>${dateText}</small>` : ''}  // Only render if available
`;
```
- **Location**: backend/main.py (lines 333-361), frontend/js/app.js (lines 312-322)

### OpenAI httpx Compatibility
- **Error**: `TypeError: Client.__init__() got an unexpected keyword argument 'proxies'`
- **Cause**: OpenAI 1.35.0 incompatible with httpx >= 0.26.0
- **Solution**: Pin httpx==0.25.2 in requirements.txt

### Beslist.nl AWS WAF Challenge and User Agent Whitelisting
- **Problem**: Scraper returns "scraping_failed" for all URLs, but pages load fine in browser
- **Symptoms**:
  - `curl https://www.beslist.nl/...` returns AWS WAF "Human Verification" challenge page
  - Same URL with user agent `"Beslist script voor SEO"` returns actual HTML content
  - Without correct user agent: `<title>Human Verification</title>` and AWS WAF JavaScript challenge
  - With correct user agent: `<title>Ellen boren goedkoop kopen? | Beste aanbiedingen | beslist.nl</title>`
- **Root Cause**: Beslist.nl uses AWS WAF (Web Application Firewall) with:
  1. **User agent whitelisting**: Only allows specific user agents to bypass bot protection
  2. **JavaScript challenge**: Presents CAPTCHA/challenge for unrecognized bots
- **Whitelist Details**:
  - **User Agent**: `"Beslist script voor SEO"` (whitelisted)
  - **IP Address**: 87.212.193.148 (whitelisted, but user agent is primary authentication)
- **Testing**:
```bash
# Without user agent (gets WAF challenge)
curl https://www.beslist.nl/products/... | head -c 200
# Returns: <!DOCTYPE html><html lang="en"><head><title>Human Verification</title>

# With whitelisted user agent (gets actual page)
curl -A "Beslist script voor SEO" https://www.beslist.nl/products/... | head -c 200
# Returns: <!DOCTYPE html><html lang=nl-NL><head><title>Ellen boren goedkoop kopen?
```
- **Solution**: Ensure scraper uses correct user agent `"Beslist script voor SEO"`
- **Verification**: Check `USER_AGENT` constant in backend/scraper_service.py (line 11)
- **Important**: User agent authentication works regardless of IP address (confirmed working from 94.142.210.226, not just 87.212.193.148)
- **Location**: backend/scraper_service.py (line 11, 87)
- **Date**: 2025-10-21

### AI Generating Long Hyperlink Text
- **Problem**: AI generates very long anchor text (e.g., full product names with specifications like "Beeztees kattentuigje Hearts zwart 120 x 1 cm")
- **Cause**: Prompt instructions were vague about link text length
- **Solution**: Update GPT prompt with explicit constraints: "KORTE, heldere omschrijving (max 3-5 woorden)" with concrete example
- **Example**: "Beeztees kattentuigje Hearts zwart 120 x 1 cm" → "Beeztees kattentuigje Hearts"
- **Location**: backend/gpt_service.py - both system message and user prompt

### Browser Cache Not Showing Updated JavaScript
- **Issue**: JavaScript changes not visible in browser after editing
- **Cause**: Browser caches static files (CSS/JS) aggressively
- **Solution**: Hard refresh to bypass cache
  - Windows/Linux: Ctrl + Shift + R or Ctrl + F5
  - Mac: Cmd + Shift + R

### Browser Auto-Linking HTML Tags in Template Literals
- **Problem**: When inserting HTML content via template literals, browser auto-links HTML tags (e.g., `</div>` becomes a clickable link)
- **Cause**: Inserting raw HTML with `<a href>` tags directly into template literals like `${content}` causes browser to parse ALL text including the subsequent HTML tags as potential URLs
- **Solution**: Create DOM structure first with empty placeholders, then insert HTML content separately via `innerHTML`
- **Example**:
```javascript
// ❌ Wrong: Browser auto-links HTML tags
html += `<div>${content}</div>`;

// ✅ Correct: Insert HTML separately
const div = document.createElement('div');
div.innerHTML = content;
```
- **Location**: frontend/js/app.js - refreshStatus() function

### Windows File Paths Not Accessible from Docker Container
- **Problem**: Docker container cannot access Windows file paths like `C:/Users/...` or `/mnt/c/Users/...`
- **Cause**: Container has isolated filesystem, Windows paths are not mounted by default
- **Solution**: Copy file into container using `docker cp`, then run script
- **Example**:
```bash
# Copy CSV from Windows to container
docker cp /mnt/c/Users/JoepvanSchagen/Downloads/file.csv content_top_app:/app/file.csv

# Run Python script in container
docker-compose exec app python -m backend.import_content
```
- **Location**: CSV import workflow for bulk content upload

### Cloudflare Rate Limit Testing with Whitelisted IP
- **Problem**: Need to determine optimal scraping rate for whitelisted IP (87.212.193.148)
- **Testing Methodology**: Progressive speed testing from conservative (1.0-1.3s) to aggressive (0s burst mode)
- **Test Results**:
  - 1.0-1.3s delay: 100% success (10 URLs)
  - 0.5-0.7s delay: 100% success (10 URLs)
  - 0.3-0.5s delay: 100% success (10 URLs)
  - 0.1-0.3s delay: 100% success (10 URLs)
  - 0.05s delay: 100% success (15 URLs)
  - 0.02s delay: 100% success (15 URLs)
  - 0.01s delay: 100% success (15 URLs)
  - 0s delay (burst mode): 100% success (15 URLs)
- **Key Finding**: Whitelisted IP has NO rate limiting from Cloudflare, even at burst mode
- **Recommended Delays**:
  - **Optimized Mode** (default): 0.2-0.3s delay (~3-5 URLs/sec) - balanced speed with minimal risk
  - **Conservative Mode**: 0.5-0.7s delay (~2 URLs/sec) with 1 worker only - maximum safety for cautious operation
- **Implementation**: Two modes available via `conservative_mode` parameter and frontend checkbox
- **Location**: backend/scraper_service.py (lines 70-82), backend/main.py (conservative_mode enforcement)

### Custom User Agent for Scraper Identification
- **Problem**: Need to identify scraper traffic in server logs for debugging and traffic analysis
- **Solution**: Set custom user agent string that describes the scraper purpose
- **Implementation**: Define `USER_AGENT` constant at top of scraper service with descriptive string
- **Example**: `USER_AGENT = "Beslist script voor SEO"` instead of generic browser user agent
- **Benefits**:
  - Easier to filter and analyze scraper traffic in server logs
  - Clear identification for IT/operations teams
  - Distinguishes scraper from regular browser traffic
  - Helps with debugging rate limiting or blocking issues
- **Location**: backend/scraper_service.py (line 11)

## Git Commands
```bash
# SSH Setup
ssh-keygen -t ed25519 -C "your@email.com"  # Generate SSH key
cat ~/.ssh/id_ed25519.pub                   # Display public key (add to GitHub)
ssh -T git@github.com                       # Test GitHub connection

# Repository Setup
git init                                    # Initialize repository
git remote add origin git@github.com:user/repo.git
git branch -M main                          # Rename branch to main
git push -u origin main                     # Push to GitHub

# Configuration
git config user.name "username"
git config user.email "email@example.com"
```

## Project Patterns

### No Build Tools Benefits
- Edit HTML/CSS/JS → Save → Refresh browser
- No npm install delays
- No webpack configuration
- No node_modules folder (saves 500MB+)
- Works identically on any machine with Docker

### Real-time Progress Tracking with Polling
- **Pattern**: JavaScript polls API endpoint every 2 seconds for status updates
- **Benefit**: Live progress updates without WebSockets complexity
- **Example**: Poll `/api/status`, update progress bar, auto-stop when complete
```javascript
pollInterval = setInterval(updateJobStatus, 2000);
if (status === 'completed') clearInterval(pollInterval);
```

### Multi-Stage Docker Builds
- **Pattern**: Separate builder stage (with gcc, build tools) from runtime stage
- **Benefit**: Smaller final image (build dependencies not included)
- **Example**: Builder installs Python packages, final stage only copies venv
```dockerfile
FROM python:3.11-slim as builder
RUN apt-get install gcc && pip install -r requirements.txt

FROM python:3.11-slim
COPY --from=builder /opt/venv /opt/venv
```

### Environment Variable Management
- **Pattern**: Use python-dotenv for configuration
- **Benefit**: Secure secrets, reusable configuration, safe for version control
- **Example**: Load from .env file at startup
```python
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Use environment variables
api_key = os.getenv("OPENAI_API_KEY")
```

### Choosing Synchronous vs Async Endpoints in FastAPI
- **Pattern**: Use synchronous endpoints when working with synchronous database drivers
- **Use Case**: Endpoints that perform database operations with psycopg2 (synchronous driver)
- **Rule of Thumb**:
  - **Use `def` (sync)**: When using synchronous libraries (psycopg2, most database drivers)
  - **Use `async def`**: When using async-compatible libraries (httpx, aiofiles, asyncpg)
- **Why It Matters**: Async endpoints with sync operations block the event loop, causing hangs and deadlocks
- **Implementation**:
```python
# ✅ Correct: Sync endpoint with sync database driver
@app.post("/api/process-urls")
def process_urls(batch_size: int = 10):
    conn = get_db_connection()  # psycopg2 - synchronous
    # ... database operations ...
    return_db_connection(conn)
    return {"status": "success"}

# ✅ Also correct: Async endpoint with async operations
@app.get("/api/external-data")
async def fetch_external():
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.example.com")
    return response.json()

# ❌ Wrong: Async endpoint with sync database
@app.post("/api/process-urls")
async def process_urls():
    conn = get_db_connection()  # Blocks event loop!
```
- **Migration Path**: If you need async with databases:
  1. Switch to async driver (asyncpg for PostgreSQL)
  2. Update all database calls to use `await`
  3. Update connection pool to async pool
- **Performance Note**: Sync endpoints are perfectly fine for most use cases and often simpler to reason about
- **Location**: backend/main.py - all endpoints (converted to sync on 2025-10-23)
- **Date**: 2025-10-23

### Debugging Connection Pool Issues with Detailed Logging
- **Pattern**: Add detailed logging at each connection lifecycle step to identify pool exhaustion or blocking
- **Use Case**: Debugging why database connections hang, aren't returned, or pool is exhausted
- **Implementation**:
```python
def get_db_connection():
    """Get connection from pool"""
    pool = _get_pg_pool()
    print(f"[POOL] Getting PG connection...")
    conn = pool.getconn()
    print(f"[POOL] Got PG connection")
    return conn

def return_db_connection(conn):
    """Return connection to pool"""
    if conn:
        pool = _get_pg_pool()
        print(f"[POOL] Returning PG connection...")
        pool.putconn(conn)
        print(f"[POOL] Returned PG connection")
```
- **Benefits**:
  - Quickly identify where connections get stuck (e.g., "Getting..." but never "Got...")
  - See if connections are being returned (look for "Returned" logs)
  - Track connection lifecycle across requests
  - Diagnose pool exhaustion (multiple "Getting..." with no "Got...")
- **Debugging Workflow**:
  1. Add detailed logs to all connection get/return functions
  2. Run failing request
  3. Check logs for incomplete lifecycles
  4. Identify where connection is stuck or not returned
- **Example Debug Output**:
```
[POOL] Getting PG connection...
[POOL] Got PG connection
[ENDPOINT] Processing 2 URLs...
[POOL] Getting Redshift connection...
[POOL] Got Redshift connection
[ENDPOINT] Inserting 2 content records...
[ENDPOINT] Content inserts complete
[POOL] Returned Redshift connection
[POOL] Returned PG connection
```
- **Location**: backend/database.py (lines 44-98)
- **Date**: 2025-10-23

### Custom Slash Commands for Permission Management
- **Pattern**: Create markdown files in .claude/commands/ for frequently used operations
- **Use Case**: Quick toggles for Claude Code settings without manual file editing
- **Benefit**: Simple one-command access to complex configuration changes
- **Implementation**:
  1. Create `.claude/commands/` directory
  2. Add markdown files with plain text instructions (e.g., `skip-permissions.md`)
  3. Claude Code executes the instructions when command is invoked
- **Example Commands**:
  - `/skip-permissions`: Set `defaultMode` to `bypassPermissions` in `.claude/settings.local.json`
  - `/restore-permissions`: Set `defaultMode` back to `default`
- **Benefits**:
  - No need to remember file paths or JSON syntax
  - Consistent execution across team members
  - Self-documenting through command names
- **Location**: `.claude/commands/*.md`

### Project Separation Strategy
- **Pattern**: Separate distinct projects into independent repositories
- **Benefit**: Clean git history, independent versioning, easier management
- **Example**: content_top (SEO) and theme_ads (Google Ads) as separate repos
- **Implementation**:
  1. Identify files by project domain
  2. Clean backend to remove cross-project dependencies
  3. Update docker-compose and .gitignore
  4. Create new repository for separated project
  5. Copy files and create independent git history

### Parallel URL Processing with ThreadPoolExecutor
- **Pattern**: Process multiple URLs concurrently using Python's ThreadPoolExecutor
- **Benefit**: Significant speed improvement for I/O-bound tasks (scraping + AI)
- **Implementation**: Each worker gets own DB connection, configurable 1-10 workers
```python
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    results = list(executor.map(process_single_url, urls))
```

### Handling Hybrid Database Schema Differences (Timestamps)
- **Problem**: Hybrid architecture (PostgreSQL + Redshift) where output table exists in both databases, but Redshift table lacks created_at column
- **Use Case**: Displaying recent results with timestamps when Redshift is primary output destination
- **Solution**: Query local PostgreSQL for recent results with timestamps as fallback
- **Implementation**:
  1. Check if output connection is Redshift or PostgreSQL
  2. For Redshift: Query local PostgreSQL connection separately for timestamp data
  3. Handle gracefully when timestamps unavailable (set to None)
  4. Frontend conditionally displays timestamps only when available
- **Benefits**:
  - Works with schema differences between databases
  - Graceful degradation when timestamps unavailable
  - No need to modify Redshift schema
  - Users see timestamps when possible, clean UI when not
- **Example**:
```python
# Always query local PostgreSQL for timestamps
try:
    local_conn = get_db_connection()
    local_cur = local_conn.cursor()
    local_cur.execute("SELECT url, content, created_at FROM pa.content_urls_joep ORDER BY created_at DESC LIMIT 5")
    recent_rows = local_cur.fetchall()
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': r['created_at'].isoformat() if r.get('created_at') else None} for r in recent_rows]
except Exception as e:
    # Fallback to output connection without timestamps
    output_cur.execute("SELECT url, content FROM pa.content_urls_joep LIMIT 5")
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': None} for r in output_cur.fetchall()]
```
- **Location**: backend/main.py (lines 333-361)

### Conditional UI Element Display Based on Data Availability
- **Pattern**: Hide UI elements when data is unavailable instead of showing placeholder text like "N/A"
- **Use Case**: Timestamps, optional metadata, or any field that may not always be present
- **Benefits**:
  - Cleaner user interface
  - Avoids confusing users with "N/A" or "null" text
  - Dynamic layout adjusts to available data
- **Implementation**:
```javascript
// Check for data availability
const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';

// Conditionally render element
itemDiv.innerHTML = `
    <h6 style="${dateText ? 'max-width: 85%;' : ''}">${item.url}</h6>
    ${dateText ? `<small>${dateText}</small>` : ''}
`;
```
- **Alternative Approaches**:
  - CSS display: none (requires extra DOM elements)
  - React conditional rendering (not applicable for vanilla JS)
- **Location**: frontend/js/app.js (lines 312-322)

### Database Cleanup and State Reset Workflow
- **Pattern**: When removing bad AI-generated results, follow 4-step process to ensure clean state
- **Use Case**: Removing results with quality issues (e.g., long hyperlinks) and reprocessing
- **Steps**:
  1. Re-add URLs to pending queue: `INSERT INTO pa.jvs_seo_werkvoorraad ... ON CONFLICT (url) DO NOTHING`
  2. Remove from tracking table: `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE url IN (...)`
  3. Delete bad results: `DELETE FROM pa.content_urls_joep WHERE url IN (...)`
  4. Reset kopteksten flag: `UPDATE pa.jvs_seo_werkvoorraad SET kopteksten = 0 WHERE url IN (...)`
- **Benefit**: Ensures URLs can be reprocessed without duplicates or state conflicts
- **Important**: Use transactions (BEGIN/COMMIT) to ensure atomicity

### Database Query Performance - Avoiding NOT IN with Large Datasets
- **Problem**: Query timeout on status endpoint with 75,858 URLs (30+ seconds → timeout)
- **Cause**: `NOT IN (SELECT url FROM table)` performs poorly on large datasets (75k+ rows)
- **Solution**: Replace with `LEFT JOIN ... WHERE IS NULL` pattern
- **Performance**: Query time reduced from 30+ seconds to <100ms
- **Example**:
```sql
-- ❌ Slow: NOT IN subquery (75k rows = timeout)
SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad
WHERE url NOT IN (SELECT url FROM pa.jvs_seo_werkvoorraad_kopteksten_check);

-- ✅ Fast: LEFT JOIN pattern (<100ms)
SELECT COUNT(*)
FROM pa.jvs_seo_werkvoorraad w
LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check c ON w.url = c.url
WHERE c.url IS NULL;
```
- **Additional Optimization**: Add index on frequently filtered columns (e.g., `CREATE INDEX idx_kopteksten_check_status ON pa.jvs_seo_werkvoorraad_kopteksten_check(status)`)
- **Location**: backend/main.py - `/api/status` endpoint

### CSV Export with Proper Encoding and Formatting
- **Pattern**: Export database content to CSV with UTF-8 encoding and proper newline handling
- **Use Case**: Exporting AI-generated content that contains HTML, special characters, and multiline text
- **Implementation**:
  1. Add UTF-8 BOM (`\ufeff`) for Excel compatibility
  2. Strip newlines from content fields to prevent row breaks: `content.replace('\n', ' ').replace('\r', ' ')`
  3. Use `csv.QUOTE_ALL` to properly escape special characters
  4. Use `BytesIO` for binary output with UTF-8 encoding
  5. Set proper content type: `text/csv; charset=utf-8`
- **Benefits**:
  - No empty rows in exported CSV
  - Proper UTF-8 character display (fixes "geÃ¯" → "geï")
  - Excel opens file correctly without import wizard
- **Example**:
```python
from io import StringIO, BytesIO
import csv

output = BytesIO()
output.write('\ufeff'.encode('utf-8'))  # UTF-8 BOM

text_output = StringIO()
writer = csv.writer(text_output, quoting=csv.QUOTE_ALL, lineterminator='\n')
writer.writerow(['url', 'content'])

for row in rows:
    content = row['content'].replace('\n', ' ').replace('\r', ' ') if row['content'] else ''
    writer.writerow([row['url'], content])

output.write(text_output.getvalue().encode('utf-8'))
```
- **Location**: backend/main.py - `/api/export/csv` endpoint

### CSV Import for Bulk Content Upload
- **Pattern**: Import pre-generated content from CSV with semicolon delimiters and UTF-8 BOM
- **Use Case**: Bulk upload of AI-generated content (e.g., 19,791 items) from external sources
- **Implementation**:
  1. Read CSV with UTF-8-sig encoding (auto-strips BOM)
  2. Use semicolon (`;`) as delimiter for compatibility
  3. Extract `url` and `content_top` columns
  4. Insert into three tables atomically:
     - `pa.jvs_seo_werkvoorraad` - mark as processed (`kopteksten = 1`)
     - `pa.content_urls_joep` - store generated content
     - `pa.jvs_seo_werkvoorraad_kopteksten_check` - track as success
  5. Use `ON CONFLICT DO NOTHING` to skip duplicates
  6. Commit every 100 rows for progress tracking
- **Benefits**:
  - Handles large files (19k+ rows) efficiently
  - Transactional safety with periodic commits
  - Progress reporting during import
  - Skips duplicates automatically
- **Example**:
```python
import csv

with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        url = row['url'].strip()
        content = row['content_top'].strip()

        # Insert into work queue (mark as processed)
        cur.execute("INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten) VALUES (%s, 1) ON CONFLICT (url) DO UPDATE SET kopteksten = 1", (url,))

        # Insert content
        cur.execute("INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s) ON CONFLICT DO NOTHING", (url, content))

        # Track as success
        cur.execute("INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status) VALUES (%s, 'success') ON CONFLICT DO NOTHING", (url,))
```
- **Location**: backend/import_content.py

### Hybrid Database Architecture (Local PostgreSQL + Cloud Redshift)
- **Pattern**: Split database responsibilities between local PostgreSQL and cloud Redshift
- **Use Case**: Large-scale data processing where some tables benefit from cloud storage
- **Architecture**:
  - **Local PostgreSQL**: Fast tracking tables (processing status, temporary data)
  - **Redshift**: Persistent data tables (work queue, generated content)
- **Implementation**:
  1. Create separate connection functions: `get_db_connection()` (local), `get_redshift_connection()` (cloud), `get_output_connection()` (smart router)
  2. Route operations based on table purpose: tracking → local, data → Redshift
  3. Handle schema differences (e.g., Redshift table has no `created_at` column)
  4. Sync operations across both databases (delete from both, update in Redshift + track locally)
- **Benefits**:
  - Local tracking is fast (no network latency)
  - Centralized data in Redshift (accessible to other systems)
  - Can scale independently (add Redshift replicas without affecting local operations)
  - Redshift optimized for large datasets (166K+ URLs)
- **Environment Variables**:
  ```bash
  # Redshift configuration
  USE_REDSHIFT_OUTPUT=true
  REDSHIFT_HOST=production-redshift.amazonaws.com
  REDSHIFT_PORT=5439
  REDSHIFT_DB=database_name
  REDSHIFT_USER=username
  REDSHIFT_PASSWORD=password
  ```
- **Important**: Redshift credentials should be in `.gitignore` (use separate config file)
- **Location**: backend/database.py (lines 12-29), backend/main.py (throughout)

### Hyperlink Validation with Status Code Checking
- **Pattern**: Validate hyperlinks in generated content by checking HTTP status codes (301/404)
- **Use Case**: Quality control for AI-generated content - detect broken product links
- **Implementation**:
  1. Extract all `<a href>` tags from HTML content using BeautifulSoup
  2. Prepend base domain (`https://www.beslist.nl`) to relative URLs
  3. Check HTTP status with `requests.head()` (faster than GET)
  4. Parallel processing with ThreadPoolExecutor for speed
  5. If broken links found (301/404), auto-reset content to pending for regeneration
  6. Store validation results in JSONB column for audit trail
  7. Skip URLs already validated (LEFT JOIN check)
  8. Reset validation history when needed via DELETE endpoint
- **Benefits**:
  - Automated quality control for product links
  - Parallel validation speeds up large batches
  - Historical tracking of broken links
  - Auto-recovery workflow (reset to pending)
  - Incremental validation - only checks unvalidated URLs
  - Can reset and re-validate all URLs when needed
- **Example**:
```python
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor

def validate_content_links(content):
    soup = BeautifulSoup(content, 'html.parser')
    links = [link['href'] for link in soup.find_all('a', href=True) if link['href'].startswith('/')]

    broken_links = []
    for link in links:
        full_url = 'https://www.beslist.nl' + link
        response = requests.head(full_url, allow_redirects=False, timeout=10)
        if response.status_code in [301, 404]:
            broken_links.append({'url': link, 'status_code': response.status_code})

    return {'broken_links': broken_links, 'has_broken_links': len(broken_links) > 0}

# Parallel validation
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(validate_single_content, content_items))
```
- **Location**: backend/link_validator.py, backend/main.py - `/api/validate-links` endpoint

### CloudFront WAF Blocking Bot Traffic
- **Problem**: Website returns HTTP 403/405 errors for certain URLs when scraped
- **Cause**: CloudFront (AWS CDN) Web Application Firewall detecting automated traffic
- **Symptoms**:
  - Some category pages (e.g., `/products/accessoires/`) blocked regardless of User-Agent
  - Residential IP addresses more likely to be blocked than datacenter IPs
- **Troubleshooting**:
  1. Check public IP: `curl -s https://api.ipify.org`
  2. Test URL directly: `curl -I -A "User-Agent" "https://example.com"`
  3. Verify IP details: `curl -s https://ipinfo.io/YOUR_IP`
- **Solutions**:
  - Whitelist scraper IP in CloudFront WAF rules
  - Use slower request rates to avoid rate limiting
  - Contact IT department to adjust WAF settings
  - Consider using datacenter IPs instead of residential
- **Example**: IP `87.212.193.148` (Odido Netherlands residential FTTH) blocked by beslist.nl CloudFront

### VPN Routing Bypass for Whitelisted IP (Windows + WSL2/Docker)
- **Problem**: Company VPN routes all traffic through different IP, but scraper needs to use whitelisted IP (87.212.193.148) for beslist.nl
- **Scenario**:
  - Without VPN: Machine uses 87.212.193.148 (whitelisted)
  - With VPN: All traffic routes through 94.142.210.226 (not whitelisted)
  - Need: VPN connected for work (Redshift access), but scraper uses whitelisted IP
- **Failed Approaches**:
  1. OpenVPN client-side routing (`route X.X.X.X net_gateway`) - Error: "option 'route' cannot be used in this context [PUSH-OPTIONS]"
  2. OpenVPN `route-nopull` - `net_gateway` keyword doesn't work client-side
  3. OpenVPN `pull-filter ignore "redirect-gateway"` - VPN still captured CloudFront traffic
  4. Privoxy on Windows - Proxy itself routes through VPN
  5. Docker `network_mode: "host"` - Still uses VPN routing
- **Working Solution**: Windows Static Route with Lower Metric
  ```cmd
  # Step 1: Find your default gateway (before/during VPN)
  route print 0.0.0.0
  # Look for physical adapter (Ethernet/Wi-Fi), note Gateway IP (e.g., 192.168.1.1)

  # Step 2: Add persistent route with interface specification (as Administrator)
  route delete 65.9.0.0
  route add -p 65.9.0.0 mask 255.255.0.0 192.168.1.1 metric 1 if 10
  # Replace 192.168.1.1 with your gateway
  # Replace 10 with your Wi-Fi/Ethernet interface number from 'route print'

  # Step 3: Verify route is active
  route print 65.9.0.0
  # Should show metric 1 in Active Routes

  # Step 4: Restart WSL2 to pick up new routing
  # In PowerShell: wsl --shutdown
  # Then restart Docker Desktop
  ```
- **Why It Works**:
  - Windows routing is hierarchical: lower metric = higher priority
  - VPN routes typically have metric 25-50
  - Our route with metric 1 takes precedence for CloudFront IPs (65.9.0.0/16)
  - WSL2 and Docker inherit Windows routing table
  - Route is persistent (`-p` flag) - survives reboots
  - Interface specification (`if 10`) ensures it binds to physical adapter
- **Verification**:
  ```bash
  # From WSL2/Docker
  curl https://api.ipify.org          # Should show whitelisted IP
  curl https://www.beslist.nl/health  # Should show same IP
  ```
- **Result**: VPN stays connected, Redshift accessible, beslist.nl sees whitelisted IP (87.212.193.148)

### OpenVPN Split Tunneling Limitations on Windows Client
- **Problem**: Cannot configure OpenVPN split tunneling from client-side config file
- **Root Cause**: OpenVPN server pushes routes that override client-side directives
- **Why Client-Side Routes Fail**:
  1. `route X.X.X.X net_gateway` requires server push context - error: "cannot be used in this context [PUSH-OPTIONS]"
  2. `net_gateway` keyword only works in server-pushed routes, not client config
  3. `route-nopull` removes ALL routes including necessary internal network routes
  4. `pull-filter` can filter server options but Windows VPN adapter still captures traffic at OS level
- **Key Learning**: For corporate VPNs, split tunneling must be configured at:
  - **Server level** (requires admin/IT): Server pushes specific routes instead of redirect-gateway
  - **OS routing level** (can do yourself): Add Windows static routes with lower metric (see VPN Routing Bypass pattern above)
- **Alternative if Server-Side Split Tunneling Available**:
  - Server config: `push "route 10.0.0.0 255.0.0.0"` instead of `push "redirect-gateway def1"`
  - Client automatically gets split tunnel without config changes

### Privoxy Proxy Configuration for Docker/WSL2 Access
- **Problem**: Docker containers in WSL2 cannot connect to Privoxy running on Windows localhost
- **Cause**: Privoxy listens on 127.0.0.1:8118 by default, which only accepts local connections
- **Solution**: Configure Privoxy to accept connections from WSL2 network
  1. Edit Privoxy config file (usually `C:\Program Files\Privoxy\config.txt`)
  2. Change: `listen-address  127.0.0.1:8118`
  3. To: `listen-address  0.0.0.0:8118` (all interfaces) OR `listen-address  172.21.160.1:8118` (WSL2 gateway only)
  4. Restart Privoxy service
- **Finding WSL2 Gateway IP**: `ip route | grep default | awk '{print $3}'` (from WSL2, returns Windows host IP like 172.21.160.1)
- **Docker Proxy Config**:
  ```python
  session.proxies = {
      'http': 'http://172.21.160.1:8118',
      'https': 'http://172.21.160.1:8118'
  }
  ```
- **Note**: In this project, we ultimately used Windows static routing instead of Privoxy (more reliable)

### WSL2 IP Gateway Discovery
- **Problem**: Need to access Windows services (like Privoxy) from Docker containers running in WSL2
- **Solution**: Windows host is accessible via WSL2's default gateway IP
- **Command**: `ip route | grep default | awk '{print $3}'`
- **Example Output**: `172.21.160.1` (this is the Windows host IP from WSL2 perspective)
- **Common Use Cases**:
  - Connecting to Windows-hosted proxy servers
  - Accessing Windows file shares from containers
  - Connecting to Windows-hosted databases or services
- **Important**: This IP changes if network configuration changes, so don't hardcode in committed code

### Batch Database Operations for Performance
- **Problem**: Each URL processing makes 2 Redshift calls (INSERT content + UPDATE werkvoorraad), causing connection overhead
- **Impact**: With parallel workers, this creates many simultaneous Redshift connections (e.g., 10 workers × 2 calls = 20 connections)
- **Solution**: Batch Redshift operations after parallel processing completes
- **Implementation**:
  1. Modify worker function to return tuple: `(result_dict, redshift_operations)`
  2. Workers collect operations in list instead of executing: `redshift_ops.append(('insert_content', url, content))`
  3. After all workers complete, execute all operations in single transaction
  4. Use single Redshift connection for entire batch
- **Benefits**:
  - Reduces Redshift connections from N×2 (per URL) to 1 (per batch)
  - Improves throughput by 15-20% with parallel workers
  - Reduces connection overhead and network latency
  - Single transaction ensures atomicity for entire batch
- **Example**:
```python
# Worker function returns operations instead of executing
def process_single_url(url):
    redshift_ops = []
    # ... processing logic ...
    redshift_ops.append(('insert_content', url, content))
    redshift_ops.append(('update_werkvoorraad', url))
    return (result, redshift_ops)

# Batch execution after parallel processing
with ThreadPoolExecutor(max_workers=3) as executor:
    result_tuples = list(executor.map(process_single_url, urls))

# Collect all operations
all_redshift_ops = []
for result, ops in result_tuples:
    all_redshift_ops.extend(ops)

# Execute in single transaction
output_conn = get_output_connection()
output_cur = output_conn.cursor()
for op in all_redshift_ops:
    if op[0] == 'insert_content':
        output_cur.execute("INSERT INTO pa.content_urls_joep ...")
    elif op[0] == 'update_werkvoorraad':
        output_cur.execute("UPDATE pa.jvs_seo_werkvoorraad ...")
output_conn.commit()
```
- **Important Note (2025-10-23)**: Use individual `execute()` loops for Redshift, NOT `executemany()`
  - Redshift `executemany()` blocks indefinitely and never releases connections
  - PostgreSQL `executemany()` works fine
  - See "Redshift executemany() Blocking Indefinitely" error section for details
- **Location**: backend/main.py - `process_single_url()`, `process_urls()` endpoint

### Conservative Mode Pattern with ThreadPoolExecutor
- **Problem**: Need to pass additional parameters to worker functions when using ThreadPoolExecutor.map()
- **Use Case**: Conservative mode flag needs to be passed to each worker alongside the URL
- **Solution**: Use `functools.partial` to bind parameters before passing to executor
- **Implementation**:
```python
from functools import partial
from concurrent.futures import ThreadPoolExecutor

# Bind conservative_mode parameter to function
process_func = partial(process_single_url, conservative_mode=True)

# Pass partially-applied function to executor
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    results = list(executor.map(process_func, urls))
```
- **Benefits**:
  - Clean parameter passing without changing function signature
  - Worker function receives both url (from map) and conservative_mode (from partial)
  - No need for lambda or wrapper functions
- **Alternative Approaches Rejected**:
  - Lambda functions: More verbose and harder to read
  - Wrapper functions: Unnecessary code duplication
  - Tuple unpacking: Requires restructuring URL list
- **Location**: backend/main.py - `process_urls()` endpoint (lines 216-218)

### CSS Theme Override Pattern with Custom Properties
- **Problem**: Need to override Bootstrap default colors consistently across entire UI
- **Use Case**: Apply custom brand colors (#059CDF blue, #9C3095 purple, #A0D168 green) to match company branding
- **Solution**: CSS custom properties (CSS variables) with !important overrides
- **Implementation**:
```css
/* Define custom color palette */
:root {
    --color-primary: #059CDF;   /* Blue */
    --color-info: #9C3095;      /* Purple/Magenta */
    --color-success: #A0D168;   /* Green */
}

/* Override Bootstrap classes */
.bg-primary { background-color: var(--color-primary) !important; }
.btn-primary { background-color: var(--color-primary); border-color: var(--color-primary); }
.text-primary { color: var(--color-primary) !important; }

/* Include hover states (20% darker) */
.btn-primary:hover { background-color: #0480b3; border-color: #0480b3; }
```
- **Benefits**:
  - Single source of truth for color values (CSS variables)
  - No need to modify Bootstrap source files
  - Easy to maintain and update colors
  - Supports hover states and all Bootstrap color classes
  - !important ensures overrides work everywhere
- **Coverage**: Primary, Info, Success colors for buttons, badges, alerts, backgrounds, text, progress bars
- **Location**: frontend/css/style.css (lines 4-148)
- **Documentation**: ARCHITECTURE.md includes full color codes, usage map, and rationale

### Database Deduplication Strategy
- **Problem**: Content table had 48,846 duplicate records (108,722 total → 59,876 unique URLs)
- **Use Case**: After bulk imports or if multiple generation runs created duplicate content
- **Solution**: Use temporary table with ROW_NUMBER() window function to deduplicate
- **Implementation**:
```sql
-- Create temp table with deduplicated data
CREATE TEMP TABLE content_deduped AS
SELECT url, content
FROM (
    SELECT url, content,
           ROW_NUMBER() OVER (PARTITION BY url ORDER BY content) as rn
    FROM pa.content_urls_joep
)
WHERE rn = 1;

-- Replace original table
DELETE FROM pa.content_urls_joep;
INSERT INTO pa.content_urls_joep (url, content)
SELECT url, content FROM content_deduped;
```
- **Benefits**:
  - Handles large datasets efficiently (100K+ records)
  - Single transaction ensures data integrity
  - Window function picks one record per URL (randomly if no timestamp)
  - Works on Redshift without created_at column
- **Script**: `backend/deduplicate_content.py`
- **Result**: Removed 48,846 duplicates, 100% clean (0 duplicates remaining)

### Werkvoorraad Synchronization Pattern
- **Problem**: Content exists but werkvoorraad table not updated (URLs marked pending but have content)
- **Use Case**: After bulk imports, manual content additions, or interrupted processing
- **Solution**: Use SQL JOIN to update werkvoorraad table based on content table
- **Implementation**:
```sql
-- Update werkvoorraad table
UPDATE pa.jvs_seo_werkvoorraad_shopping_season w
SET kopteksten = 1
FROM pa.content_urls_joep c
WHERE w.url = c.url AND w.kopteksten = 0;

-- Add tracking records
INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status)
SELECT c.url, 'success'
FROM pa.content_urls_joep c
LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check k ON c.url = k.url
WHERE k.url IS NULL
ON CONFLICT (url) DO UPDATE SET status = 'success';
```
- **Benefits**:
  - Efficient single-query update for thousands of URLs
  - Synchronizes both werkvoorraad and tracking tables
  - Prevents duplicate content generation
- **Script**: `backend/sync_werkvoorraad.py`
- **Result**: Synchronized 17,672 URLs, 0 overlaps remaining

### Link Validation Performance Analysis
- **Conservative Mode**: 0.5-0.7s delay per link check
  - 100 items with ~350 links = ~3m52s (~2.3s per item)
  - Rate: ~1,552 items/hour
  - Use case: Maximum caution, avoiding any rate limit concerns
- **Optimized Mode**: No delay between checks
  - Estimated 5-10x faster than conservative
  - With 5 workers: ~60K items in ~1 hour
  - Rate: ~60,000 items/hour (38x faster)
- **Recommendation**: Use optimized mode for link validation
  - Link validation just checks HTTP status (HEAD requests)
  - Much lighter than content scraping
  - Whitelisted IP has no rate limits
  - Conservative mode unnecessary for validation workloads

### Connection Pooling with psycopg2.pool.ThreadedConnectionPool
- **Problem**: Each worker creates/closes database connections for every URL processed
- **Impact**: Connection overhead of 50-200ms per URL adds up significantly
- **Solution**: Implement connection pooling to reuse connections across requests
- **Implementation**:
```python
from psycopg2 import pool

# Create connection pools (global, initialized on first use)
_pg_pool = None
_redshift_pool = None

def _get_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=os.getenv("DATABASE_URL"),
            cursor_factory=RealDictCursor
        )
    return _pg_pool

def get_db_connection():
    """Get connection from pool"""
    return _get_pg_pool().getconn()

def return_db_connection(conn):
    """Return connection to pool"""
    if conn:
        _get_pg_pool().putconn(conn)
```
- **Benefits**:
  - 30-50% faster per URL (eliminates connection overhead)
  - Reduces network latency and handshake time
  - Better resource utilization (reuse existing connections)
  - Automatic connection management (pool handles lifecycle)
- **Configuration**: Pool size 2-10 connections per database (PostgreSQL + Redshift)
- **Important**: Always return connections to pool with `return_db_connection(conn)` in finally blocks
- **Location**: backend/database.py (lines 6-67), backend/main.py (all connection usage updated)

### Redshift COPY Command for Bulk Inserts
- **Problem**: Using executemany() for Redshift bulk inserts causes multiple network round-trips
- **Impact**: Batch operations take longer than necessary, especially for large batches
- **Solution**: Use COPY command which is 5-10x faster for bulk data loads
- **Implementation**:
```python
from io import StringIO

# Prepare data buffer
buffer = StringIO()
for url, content in insert_content_data:
    # Escape tabs and newlines
    content_escaped = content.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    buffer.write(f"{url}\t{content_escaped}\n")
buffer.seek(0)

# Use COPY command (Redshift only)
if use_redshift:
    output_cur.copy_from(buffer, 'pa.content_urls_joep', columns=['url', 'content'], sep='\t')
else:
    # Fallback to executemany for PostgreSQL
    output_cur.executemany("INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s)", insert_content_data)
```
- **Benefits**:
  - 20-30% faster for Redshift batch operations
  - Reduces network round-trips from N to 1 per batch
  - More efficient for large datasets (100+ rows)
  - Automatically falls back to executemany() for PostgreSQL
- **Performance**: COPY is 5-10x faster than INSERT for bulk operations in Redshift
- **Location**: backend/main.py (lines 260-275)

### Three-State URL Tracking in Redshift
- **Pattern**: Use tri-state flag instead of boolean for better tracking granularity
- **Use Case**: Need to distinguish between "successfully processed with content" vs "processed but no usable content" vs "not yet processed"
- **Implementation**:
  - `kopteksten = 0`: Pending (not yet processed)
  - `kopteksten = 1`: Successfully processed with content (has entry in content_urls_joep table)
  - `kopteksten = 2`: Processed without content (skipped, failed, no products, AI errors, etc.)
- **Benefits**:
  - Query for problematic URLs: `WHERE kopteksten = 2` shows all non-productive URLs
  - Better analytics: Can calculate success rate, skip rate, etc.
  - Clear distinction between "has content" and "tried but failed"
  - Prevents re-processing of legitimately empty pages
- **Redshift Operations**:
  - Success: `('update_werkvoorraad_success', url)` → sets kopteksten=1
  - Processed without content: `('update_werkvoorraad_processed', url)` → sets kopteksten=2
  - 503 errors: No Redshift update → stays kopteksten=0 for retry
- **Location**: backend/main.py (lines 73-135 for logic, 267-308 for batch execution)
- **Date**: 2025-10-22

### Distinguishing Scraping Failure Types for Retry Logic
- **Pattern**: Return different indicators from scraper for retriable vs non-retriable failures
- **Use Case**: Need to stop batch immediately on rate limiting (503) but mark other failures as processed
- **Implementation**:
  - **503 errors** (rate limiting): Return `{'error': '503'}` - triggers immediate batch stop
  - **Other failures** (timeout, network error): Return `None` - marked as processed (kopteksten=2)
  - **Success**: Return dict with scraped data
- **Benefits**:
  - Batch stops immediately on first 503 (not after 3 consecutive failures)
  - Non-retriable failures (timeout, connection error) don't stay in pending forever
  - Clear signal to calling code about failure type
  - Prevents wasting API calls when rate limited
- **Processing Logic**:
```python
scraped_data = scrape_product_page(url)
if scraped_data and scraped_data.get('error') == '503':
    # Rate limited - keep pending, stop batch
    result["reason"] = "rate_limited_503"
    rate_limited = True
    break
elif not scraped_data:
    # Other failure - mark as processed (kopteksten=2)
    result["reason"] = "scraping_failed"
    redshift_ops.append(('update_werkvoorraad_processed', url))
```
- **Location**: backend/scraper_service.py (returns {'error': '503'}), backend/main.py (lines 73-87, 256-260)
- **Date**: 2025-10-22

### Batch UPDATE Operations to Prevent Serialization Conflicts
- **Pattern**: When updating multiple rows in concurrent transactions, use batch operations with IN clauses instead of loops
- **Use Case**: Concurrent batch jobs updating the same database table (common in parallel processing systems)
- **Problem**: Individual UPDATEs in loops create transaction cycles when multiple workers access same rows
  - Worker A updates URL 1, waits for URL 2
  - Worker B updates URL 2, waits for URL 1
  - Serialization conflict detected, one transaction aborted
- **Solution**: Collect all URLs first, then execute single batch UPDATE
  ```python
  # ❌ Wrong: Loop with individual UPDATEs (causes conflicts)
  for (url,) in update_urls:
      cur.execute("UPDATE table SET status = 1 WHERE url = %s", (url,))

  # ✅ Correct: Single batch UPDATE with IN clause
  if update_urls:
      url_list = [url for (url,) in update_urls]
      placeholders = ','.join(['%s'] * len(url_list))
      cur.execute(f"""
          UPDATE table SET status = 1
          WHERE url IN ({placeholders})
      """, url_list)
  ```
- **Benefits**:
  - Prevents serialization conflicts in concurrent transactions
  - Reduces database round-trips from N to 1
  - Shorter transaction duration = lower conflict probability
  - 15-20% performance improvement for batch operations
  - Works with DELETE and UPDATE operations
- **Important Notes**:
  - Applies to both PostgreSQL and Redshift
  - Critical for any concurrent batch processing system
  - Also improves performance even without concurrency
  - Use same pattern for DELETE operations with broken links
- **Example - Link Validation**:
  ```python
  # Collect URLs with broken links first
  urls_with_broken_links = []
  for validation_result in validation_results:
      if validation_result['has_broken_links']:
          urls_with_broken_links.append(validation_result['content_url'])

  # Execute batch operations
  if urls_with_broken_links:
      placeholders = ','.join(['%s'] * len(urls_with_broken_links))
      # Delete content
      cur.execute(f"DELETE FROM pa.content_urls_joep WHERE url IN ({placeholders})", urls_with_broken_links)
      # Reset flags
      cur.execute(f"UPDATE pa.jvs_seo_werkvoorraad_shopping_season SET kopteksten = 0 WHERE url IN ({placeholders})", urls_with_broken_links)
  ```
- **Location**: backend/main.py (lines 295-317 for batch processing, lines 770-791 for link validation)
- **Date**: 2025-10-28

### Database Synchronization Pattern for Hybrid Architecture
- **Pattern**: Periodic sync script to ensure consistency between local and cloud databases
- **Use Case**: Hybrid architecture (local PostgreSQL + Redshift) where local writes may not complete in cloud due to network issues or interruptions
- **Problem**: Local content exists but cloud flags (kopteksten) not updated, causing mismatch between source of truth
- **Implementation**:
  1. Identify "source of truth" table (e.g., local content table with actual data)
  2. Identify "tracking" table (e.g., cloud flags indicating processing state)
  3. Create sync script that queries source table and updates tracking table
  4. Use batch updates (1000 rows) for performance on large datasets
  5. Make idempotent (safe to run multiple times, only updates stale records)
- **Benefits**:
  - Recovers from interrupted batch operations
  - Maintains data consistency across hybrid architecture
  - Can run safely anytime without duplicating work
  - Prevents progress stalls due to filtering mismatches
  - Provides one-time fix for accumulated inconsistencies
- **Example**:
```python
# Sync script structure
def sync_flags():
    # 1. Get source of truth
    local_cur.execute("SELECT url FROM pa.content_urls_joep")
    urls_with_content = [row['url'] for row in local_cur.fetchall()]

    # 2. Check cloud for stale records
    output_cur.execute("""
        SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad_shopping_season
        WHERE url IN (%s) AND kopteksten = 0
    """, urls_with_content)

    # 3. Batch update cloud flags
    for batch in chunks(urls_with_content, 1000):
        output_cur.execute("""
            UPDATE pa.jvs_seo_werkvoorraad_shopping_season
            SET kopteksten = 1
            WHERE url IN (%s) AND kopteksten = 0
        """, batch)
        output_conn.commit()
```
- **When to Run**: After interrupted sessions, network issues, or when progress stalls unexpectedly
- **Location**: backend/sync_redshift_flags.py (created 2025-10-22)
- **Date**: 2025-10-22

### Auto-Stop on Consecutive Scraping Failures
- **Pattern**: Track consecutive failures and stop batch processing after threshold
- **Use Case**: Detecting rate limiting (503 errors) and preventing wasted processing time
- **Implementation**:
```python
consecutive_failures = 0
for result in results:
    if result['status'] == 'failed' and result.get('reason') == 'scraping_failed':
        consecutive_failures += 1
        if consecutive_failures >= 3:
            print(f"[RATE LIMIT DETECTED] Stopping batch - {consecutive_failures} consecutive scraping failures")
            break
    else:
        consecutive_failures = 0  # Reset on success
```
- **Benefits**:
  - Prevents marking thousands of URLs incorrectly during rate limiting
  - Saves API costs (OpenAI) by stopping early
  - Clear signal to user that system is rate limited
  - Automatic recovery when resumed later
- **Threshold**: 3 consecutive failures (configurable)
- **Location**: backend/main.py - `process_urls()` endpoint (lines 242-256)
- **Date**: 2025-10-21

### CSV Upload with Relative URL Conversion
- **Pattern**: Handle CSV files with relative URLs by converting to absolute URLs
- **Use Case**: Importing URL lists where some URLs are relative (/products/...) instead of absolute (https://...)
- **Implementation**:
```python
import csv
from io import StringIO

# Parse CSV with auto-detected delimiter
csvfile = StringIO(file_content.decode('utf-8-sig'))
dialect = csv.Sniffer().sniff(csvfile.read(1024), delimiters=',;\t')
csvfile.seek(0)
reader = csv.reader(csvfile, dialect)

# Convert relative URLs to absolute
base_url = 'https://www.beslist.nl'
urls = []
for row in reader:
    if row and row[0].strip():
        url = row[0].strip()
        # Convert relative to absolute
        if url.startswith('/'):
            url = base_url + url
        urls.append(url)
```
- **Benefits**:
  - Handles both relative and absolute URLs seamlessly
  - Auto-detects CSV delimiter (comma, semicolon, tab)
  - Handles UTF-8 BOM encoding
  - Skips empty rows automatically
- **Batch Checking**: For Redshift compatibility (no ON CONFLICT), use batch checking:
  - Query existing URLs in batches of 500
  - Filter duplicates in Python
  - Insert only new URLs
- **Location**: backend/main.py - `/api/upload-urls` endpoint (lines 463-542)
- **Date**: 2025-10-21

### Elasticsearch plpUrl Lookup with Maincat Routing
- **Pattern**: Query Elasticsearch using pimId with maincat-specific indices
- **Use Case**: Map old product URLs to new plpUrl format
- **URL Formats Supported**:
  - Old: `/p/gezond_mooi/nl-nl-gold-6150802976981/` (maincat_url + pimId with prefix)
  - New: `/p/product-name/286/6150802976981/` (maincat_id in URL, pimId without prefix)
- **Implementation**:
  1. Load maincat mapping from CSV (maincat_url → maincat_id)
  2. Extract maincat_id from URL (check URL patterns)
  3. Build index name: `product_search_v4_nl-nl_{maincat_id}`
  4. Query with terms filter on pimId field
  5. Batch queries (10K pimIds per request)
- **ES API Endpoint**: `https://elasticsearch-job-cluster-eck.beslist.nl/{index}/_search`
- **Query Example**:
```json
{
  "_source": ["plpUrl", "pimId"],
  "size": 10000,
  "query": {
    "terms": {
      "pimId": ["nl-nl-gold-6150802976981", "nl-nl-gold-..."]
    }
  }
}
```
- **Maincat Mapping**: `/mnt/c/Users/JoepvanSchagen/Downloads/Python/maincat_mapping.csv` (semicolon-delimited, columns: maincat;maincat_url;maincat_id)
- **Location**: cc1/lookup_plp_urls.py
- **Date**: 2025-12-09

### Elasticsearch-based Link Validation (replaces HTTP checking)
- **Pattern**: Use Elasticsearch plpUrl lookup instead of HTTP HEAD requests for link validation
- **Use Case**: Validate product links in generated content - check if products still exist and URLs are correct
- **Why Replace HTTP Checking**:
  - HTTP HEAD requests are slow (network latency per link)
  - Can trigger rate limiting on large batches
  - Elasticsearch lookup is faster and more reliable
  - Can also detect URL changes (old URL → new plpUrl)
- **Implementation**:
  1. Extract all `<a href="/p/...">` links from HTML content using BeautifulSoup
  2. Parse pimId and maincat_id from each URL
  3. Query Elasticsearch for current plpUrl by pimId
  4. Compare: if plpUrl differs → replace in content; if product GONE → reset to pending
- **Three Outcomes**:
  - **Valid**: plpUrl matches original URL → no action
  - **Outdated**: plpUrl differs → auto-replace URL in content (kopteksten stays 1)
  - **Gone**: Product not found in ES → reset URL to pending (kopteksten=0) for content regeneration
- **Benefits**:
  - No rate limiting concerns (internal ES cluster)
  - Batch processing of multiple links per content item
  - Auto-correction of outdated URLs without regenerating content
  - Clear distinction between "needs URL update" vs "needs new content"
- **Location**: backend/link_validator.py (completely rewritten)
- **Date**: 2025-12-10

### Product Search API Content Generation
- **Pattern**: Generate SEO content by querying Product Search API and using product descriptions
- **Use Case**: Create content for SEO URLs with specific filters (brand, color, category)
- **URL Format**: `/products/{maincat}/{category}/c/{filter1~value1~~filter2~value2}`
  - Example: `/products/accessoires/accessoires_2596345/c/merk~2685977`
  - Also supports: `/products/{maincat}/c/{filters}` (without subcategory)
- **API Endpoint**: `https://productsearch-v2.api.beslist.nl/search/products`
- **Required API Parameters**:
  ```
  query=                           # Can be empty for category browsing
  mainCategory={maincat_name}      # e.g., "kantoorartikelen" (name, not ID)
  category={category_urlname}      # e.g., "kantoorartikelen_558052_558970"
  filters[{facet}][0]={value_id}   # e.g., filters[merk][0]=2829915
  limit=76                         # Max products to return
  offset=0                         # Pagination offset
  isBot=false                      # REQUIRED - API returns 400 without this
  countryLanguage=nl-nl            # REQUIRED - API returns 500 without this
  experiment=topProducts           # Optional, for ranking experiment
  trackTotalHits=false             # Optional
  ```
- **API Error Messages**:
  - Missing `isBot`: `{"errors":"isBot is a required parameter."}` (HTTP 400)
  - Missing `countryLanguage`: `findCategoryIdByCategoryUrlAndCountryLanguage(): Argument #2 ($countryLanguage) must be of type string, null given` (HTTP 500)
- **Request Headers**:
  ```
  Accept: application/json
  User-Agent: Beslist script voor SEO
  ```
- **Product Filtering** (as of 2026-01-28):
  - **Type filter**: Only include `type="result"` products, skip `type="orResult"`
    - `result` = exact match for all filters (correct brand, category, etc.)
    - `orResult` = partial/related match (may be wrong brand or loosely related)
  - **shopCount filter**: Only include products with `shopCount >= 2`
  - If URL returns only `orResult` products, it is skipped (no content generated)
  - Code location: `backend/scraper_service.py`, `backend/faq_service.py`
- **Implementation**:
  1. Parse URL to extract maincat name, category, and filters
  2. Build API params with required `isBot=false` and `countryLanguage=nl-nl`
  3. Build API URL with filters encoded as `filters%5B{name}%5D%5B0%5D={value}`
  4. Fetch products and filter by shopCount
  5. Extract `plpUrl` and `title` from each product
  6. Generate GPT content using product titles/descriptions
  7. Output includes proper `<a href="{plpUrl}">` links with product titles as anchor text
- **Output**: Excel file with columns: url, maincat_id, category, products_found, success, content
- **GPT Settings**: max_tokens=500 (increased from 200 to accommodate HTML links)
- **Location**: backend/scraper_service.py, backend/seo_content_generator.py
- **Date**: 2025-12-10, updated 2026-01-28

### Switching from Redshift to Local PostgreSQL Only
- **Context**: User requested to stop using Redshift and use only local PostgreSQL
- **Changes Made**:
  1. Updated `/api/status` endpoint to query local tables only
  2. Updated `/api/process-urls` endpoint to get pending URLs from local werkvoorraad
  3. Content is now saved directly in `process_single_url()` instead of batching to Redshift
  4. Removed all Redshift batch operations from process_urls endpoint
- **Tables Used (Local PostgreSQL)**:
  - `pa.jvs_seo_werkvoorraad` - Source URLs to process
  - `pa.jvs_seo_werkvoorraad_kopteksten_check` - Tracking table (status: success/failed/skipped)
  - `pa.content_urls_joep` - Generated content storage
- **Pending Calculation**: Uses LEFT JOIN to find URLs in werkvoorraad not yet in tracking table
- **Location**: backend/main.py
- **Date**: 2025-12-11

### URL Format Normalization
- **Problem**: Mixed URL formats causing mismatches between tables (relative vs absolute)
- **Formats Found**:
  - Absolute: `https://www.beslist.nl/products/...`
  - Relative: `/products/...`
  - Invalid: `/l/...` (old format)
- **Solution**: Normalize all URLs to absolute format
  ```sql
  -- Update relative to absolute
  UPDATE table SET url = 'https://www.beslist.nl' || url WHERE url LIKE '/products/%';
  -- Delete invalid /l/ URLs
  DELETE FROM table WHERE url LIKE '/l/%';
  ```
- **Date**: 2025-12-11

### MAIN_CATEGORY_IDS Authoritative Source
- **Source File**: `maincat_ids_new.xlsx` - DO NOT EDIT mapping without updating this file
- **Location**: backend/scraper_service.py MAIN_CATEGORY_IDS dict
- **How to Find Correct ID**: Check product URLs on beslist.nl category page - ID is in URL path `/p/product-name/{maincat_id}/ean/`
- **Key Mappings** (31 total):
  - autos: 37000, baby_peuter: 8, boeken: 701, computers: 6
  - elektronica: 655, fietsen: 38000, huis_tuin: 165, klussen: 35000
  - meubilair: 10, mode: 137, schoenen: 32000, speelgoed_spelletjes: 332
- **Date**: 2025-12-12

### Content Generation Performance Optimization
- **API Delay**: Reduced from 0.1-0.2s to 0.02-0.05s per call (5x faster)
- **Default Workers**: Increased from 3 to 6 (2x parallelization)
- **Default Batch Size**: Increased from 10 to 50 (5x per request)
- **Why Safe**: Product Search API is internal, less restrictive than scraping
- **Location**:
  - API delay: backend/scraper_service.py line 488
  - Workers/batch: frontend/index.html
- **Date**: 2025-12-12

### Product Search API-based Content Generation with Facet Extraction
- **Pattern**: Use Product Search API output to extract selected facet values and build product subjects
- **Use Case**: Generate SEO content for filtered category pages (e.g., `/products/elektronica/.../c/kleur~19958432~~modelnaam_mob~23748469`)
- **Implementation**:
  1. Parse URL to extract maincat, category, and filter parameters
  2. Call Product Search API with filters
  3. Extract facets where `"selected": true` from response
  4. Use `detailValue` field (Dutch adjective form) for colors/materials
  5. Build product subject with smart ordering: colors → product names → brands → category
- **Smart Category Name Inclusion**:
  - Include category name when only generic facets present (brand, color, target group)
  - Skip category when specific product/model/type facets exist (would be redundant)
  - Example: "Nike Heren" needs category → "Nike Heren voetbalschoenen"
  - Example: "iPhone 15" has specific product → no category needed
- **Key Functions**:
  - `parse_beslist_url()` - Extracts maincat, category, filters from URL
  - `build_api_params()` - Constructs API query parameters
  - `extract_selected_facets()` - Finds facets with `"selected": true`
  - `build_product_subject()` - Builds product name from facets
  - `scrape_product_page_api()` - Main function replacing HTML scraping
- **Integration**: Used in frontend SEO Content Generation (process_single_url in main.py)
- **Location**: backend/scraper_service.py
- **Date**: 2025-12-11

### SQL Filtering for Unvalidated URLs (LEFT JOIN vs In-Memory)
- **Problem**: "No content to validate" error when URLs exist but query returns wrong results
- **Cause**: SQL query with LIMIT but filtering done in-memory returns same already-validated rows
- **Wrong Approach**:
  ```python
  # Fetches all content, filters in Python - doesn't scale with LIMIT
  cur.execute("SELECT url, content FROM content LIMIT 1000")
  rows = cur.fetchall()
  unvalidated = [r for r in rows if r['url'] not in validated_set]  # May be empty!
  ```
- **Correct Approach**:
  ```sql
  -- Filter at database level using LEFT JOIN
  SELECT c.url, c.content
  FROM pa.content_urls_joep c
  LEFT JOIN pa.link_validation_results v ON c.url = v.content_url
  WHERE v.content_url IS NULL
  LIMIT 1000
  ```
- **Benefits**:
  - Efficiently finds unvalidated URLs at database level
  - LIMIT applies after filtering, not before
  - Works regardless of validation history size
- **Location**: backend/main.py `/api/validate-all-links` endpoint
- **Date**: 2025-12-11

### Content Generation Performance Optimizations
- **Problem**: Processing 131K URLs at ~4-10 seconds per URL would take 18-46 days
- **Goal**: Reduce processing time to 3-9 days (2.8-6x faster)
- **Optimizations Implemented**:
  1. **Reduced scraping delay** (0.5-1s → 0.05-0.1s): Whitelisted IP doesn't need aggressive rate limiting
  2. **Reduced AI max_tokens** (500 → 300): Content is max 100 words (~130 tokens), so 300 is sufficient
  3. **Batch local PostgreSQL commits**: Changed from 3-5 commits per URL to 1 commit per URL (all operations in single transaction at end)
  4. **Switch to lxml parser**: BeautifulSoup now uses lxml instead of html.parser (2-3x faster HTML parsing)
  5. **Use executemany() for Redshift**: Batch all INSERTs and UPDATEs using cursor.executemany() instead of loop
- **Performance Impact**:
  - Scraping delay: Save ~0.5-0.9s per URL (10-20% speedup)
  - AI tokens: Save ~0.5-1s per URL (10-15% speedup)
  - Local DB batching: Save ~0.2-0.4s per URL (5-10% speedup)
  - lxml parser: Save ~0.3-0.5s per URL (5-8% speedup)
  - executemany(): Save ~0.1-0.2s per batch (marginal but helpful)
  - **Total: 30-50% faster per URL** (4-10s → 2.5-7s per URL)
- **Combined with parallel workers**: Original default of 3 workers can be increased to 5-7 for linear speedup
- **Expected Results**:
  - Before: ~120-300 URLs/hour with 3 workers
  - After: ~350-840 URLs/hour with 3 workers, or ~580-1,400 URLs/hour with 5 workers
  - 131K URLs: 18-46 days → 5-15 days (with 3 workers) or 4-9 days (with 5 workers)
- **Files Modified**:
  - `backend/scraper_service.py`: Adjusted delay to 0.2-0.3s (balanced for Cloudflare), switched to lxml parser
  - `backend/gpt_service.py`: Reduced max_tokens from 500 to 300
  - `backend/main.py`: Refactored process_single_url() to batch local commits, added executemany() for Redshift
- **Location**: backend/scraper_service.py (lines 70-72, 102), backend/gpt_service.py (line 89), backend/main.py (lines 52-145, 208-243)
- **Note on Scraping Delay**: Initial attempt at 0.05-0.1s was too aggressive, causing Cloudflare HTTP 202 (queuing) responses even with whitelisted IP. Adjusted to 0.2-0.3s as sweet spot between speed and avoiding rate limits.

### Status API Consistency Fix - Distinct URL Counts
- **Problem**: Content status showed processed=167,841 but total_urls=149,954 (inconsistent)
- **Cause**: `processed` used `COUNT(*)` (total rows) while `total_urls` used `COUNT(DISTINCT url)`
- **Impact**: Duplicate URLs in content table caused misleading dashboard numbers
- **Solution**: Changed both endpoints to use `COUNT(DISTINCT url)` for all counts
  ```sql
  -- Before: COUNT(*) = 167,841 (total rows including duplicates)
  SELECT COUNT(*) as processed FROM pa.content_urls_joep;

  -- After: COUNT(DISTINCT url) = 149,954 (unique URLs only)
  SELECT COUNT(DISTINCT url) as processed FROM pa.content_urls_joep;
  ```
- **Also Fixed**: FAQ status endpoint now uses `pa.content_urls_joep` as source for total_urls (same as content status)
- **Result**: Both status endpoints show consistent counts based on distinct URLs
- **Location**: backend/main.py - `/api/status` (line 315) and `/api/faq/status` (lines 1066-1079)
- **Date**: 2025-12-17

### Color Theme - Purple Navbar, Grey Headers, Orange Buttons
- **Change**: Complete UI color overhaul
  - Top navbar: Purple (`#5e4a90`)
  - Section headers: Light grey (`#E8E9EB`)
  - All buttons: Burnt orange (`#CC5500`) with coral hover (`#E97451`)
- **Button Types Updated**: btn-primary, btn-info, btn-success, btn-secondary, btn-warning, btn-outline-* (all variants)
- **Header Types Updated**: All card-header elements use light grey
- **Exceptions**: btn-danger kept red for destructive actions
- **CSS Variables**:
  ```css
  --color-navbar: #5e4a90;       /* Purple - top navbar only */
  --color-section: #E8E9EB;      /* Light grey - section headers */
  --color-button: #CC5500;       /* Burnt orange - for buttons */
  --color-button-hover: #E97451; /* Coral orange - for hover */
  ```
- **Location**: frontend/css/style.css
- **Date**: 2025-12-17

### Project Merge - Unified SEO Tools Platform
- **Change**: Merged seo_faq, content_top, and theme_ads into single unified platform
- **Port**: All services now run on port 8003 (previously 8003 + 8002)
- **Approach**: Used FastAPI APIRouter to keep thema_ads code modular
- **Files Copied**:
  - `theme_ads/backend/thema_ads_service.py` → `backend/thema_ads_service.py`
  - `theme_ads/backend/database.py` → `backend/thema_ads_db.py`
  - `theme_ads/backend/main.py` → `backend/thema_ads_router.py` (converted to APIRouter)
  - `theme_ads/frontend/thema-ads.html` → `frontend/thema-ads.html`
  - `theme_ads/frontend/js/thema-ads.js` → `frontend/js/thema-ads.js`
  - `theme_ads/thema_ads_optimized/` → `thema_ads_optimized/`
  - `theme_ads/themes/` → `themes/`
- **Router Pattern**:
  ```python
  # thema_ads_router.py
  router = APIRouter(prefix="/api/thema-ads", tags=["thema-ads"])

  # main.py
  from backend.thema_ads_router import router as thema_ads_router
  app.include_router(thema_ads_router)
  ```
- **Benefits**: Single deployment, shared CSS/styling, unified dashboard
- **Date**: 2025-12-17

### UI Cleanup - Compact Tabs and Collapsible Info
- **Change**: Cleaned up thema-ads UI for better usability
- **Tab Improvements**:
  - Shortened tab names: Plan, Ad Groups, Discover, Check-up, Cleanup, All Themes, Activate, Duplicates
  - Compact padding and font-size for single-row display
  - Consistent anthracite (#2d3436) text with orange hover (#E97451)
- **Content Cleanup**:
  - Removed verbose explanatory text from all tabs
  - Added collapsible "More info" buttons for detailed documentation
  - "More info" buttons use inverted orange style (border/text → filled on hover)
- **CSS Pattern for collapsible info buttons**:
  ```css
  .more-info-btn {
      border: 1px solid #CC5500;
      color: #CC5500;
      background: transparent;
      transition: all 0.2s ease;
  }
  .more-info-btn:hover {
      background: #CC5500;
      color: white;
  }
  ```
- **Location**: frontend/thema-ads.html
- **Date**: 2025-12-17

### Canonical URL Generator - UI & Feature Updates
- **Button Styling**:
  - "Download CSV" button: Changed from `btn-outline-light` (grey) to `btn-outline-warning` (orange)
  - "Add Rule" buttons: Changed from `btn-outline-primary` (blue) to `btn-warning` (solid orange)
- **Tab Styling**:
  - Tab text color changed to anthracite (#3a3a3a) with bold font
  - Hover state: darker anthracite (#1a1a1a)
  ```css
  .nav-tabs .nav-link { color: #3a3a3a; font-weight: bold; }
  .nav-tabs .nav-link:hover { color: #1a1a1a; }
  .nav-tabs .nav-link.active { color: #3a3a3a; font-weight: bold; }
  ```
- **FACET-FACET Category Filter**: Added optional category filter to FACET-FACET rules (same as CAT+FACET)
  - Frontend: Added "Category (opt)" column and input field
  - Backend: Added `cat: Optional[str] = None` to `FacetFacetRule` dataclass
  - URL fetching and rule application now respect the category filter
- **Location**: `frontend/canonical.html`, `backend/canonical_service.py`
- **Date**: 2026-01-27

### Brand Mismatch Detection and Content Reset for Merk URLs
- **Problem**: SEO content generated before 2026-01-27 for URLs with `merk~` facet often linked to products from wrong brands
- **Root Cause**: AI prompt wasn't strict enough about linking only to products matching the URL's brand filter
- **Detection Method**: SQL-based validation using brand lookup table
  1. Load brand ID → name mapping from Excel into `pa.merk_lookup` table
  2. Extract merk ID from URL using regex: `regexp_match(url, '~~merk~([0-9]+)')` or `/merk~([0-9]+)`
  3. Check if brand name appears in content (missing = problematic)
  4. Extract product links from content and check if slug starts with brand name (wrong brand links)
- **Detection Query Pattern**:
  ```sql
  -- Find URLs where brand name is missing from content
  SELECT url FROM pa.content_urls_joep c
  WHERE c.url LIKE '%/merk~%'
    AND EXISTS (
        SELECT 1 FROM pa.merk_lookup m
        WHERE m.merk_id = (regexp_match(c.url, '/merk~([0-9]+)'))[1]::integer
        AND c.content NOT ILIKE '%' || m.brand_name || '%'
    );

  -- Find URLs where links point to wrong brands
  SELECT url FROM (
      SELECT url, LOWER(brand_name) as brand_lower,
          (regexp_matches(content, 'href="https://www.beslist.nl/p/([^/"]+)', 'g'))[1] as product_slug
      FROM content_with_brands
  ) WHERE LOWER(product_slug) NOT LIKE brand_lower || '%';
  ```
- **Reset Process** (to make URLs pending for regeneration):
  1. Archive old content: `INSERT INTO pa.content_history ... WHERE reset_reason = 'brand_mismatch_reset'`
  2. Delete content: `DELETE FROM pa.content_urls_joep WHERE url IN (...)`
  3. Reset werkvoorraad flag: `UPDATE pa.jvs_seo_werkvoorraad SET kopteksten = 0 WHERE url IN (...)`
  4. **Critical**: Delete from tracking table: `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE url IN (...)`
- **Pending URL Logic**: URLs are pending when they exist in `jvs_seo_werkvoorraad` but NOT in `jvs_seo_werkvoorraad_kopteksten_check`
- **Results** (2026-01-28):
  - Total old merk content: 93,459
  - Missing brand name: 19,888 (21%)
  - Wrong brand links: 35,442 (38%)
  - Total reset to pending: 55,330 (59%)
- **Location**: Database tables `pa.merk_lookup`, `pa.content_history`, `pa.content_urls_joep`, `pa.jvs_seo_werkvoorraad_kopteksten_check`
- **Date**: 2026-01-28

### Beslist Product Search API Facet Validation Errors
- **Problem**: API calls fail with HTTP 400 for certain URLs
- **Cause**: URLs contain facet names or value IDs that are no longer valid for that category
- **Error Response Types**:
  - Invalid facet name: `{"context": "facet", "errorInfo": "The given facet is not valid.", "value": "personage"}`
  - Invalid facet value: `{"context": "merk", "errorInfo": "The given facet value is not valid.", "value": 19957206}`
- **Solution**: Detect 400 errors with "not valid" in errorInfo, return `facet_not_available` error type
- **Error Reasons Reference**:
  - `facet_not_available` - URL contains invalid facet name or value ID for category
  - `api_failed` - Generic API failure (non-400 or unparseable error)
  - `no_products_found` - API returned 0 products (skipped)
- **Location**: backend/scraper_service.py, backend/faq_service.py
- **Date**: 2025-12-26

### External API SQL Escaping Errors
- **Problem**: Website-configuration API returns MySQL INSERT exception when content contains apostrophes
- **Error**: `An exception occurred while executing 'INSERT INTO ... VALUES ('...DVD's...')`
- **Cause**: External API's MySQL INSERT not properly escaping single quotes
- **Solution**: Sanitize content before sending to external API:
```python
def sanitize_for_api(text: str) -> str:
    if not text:
        return ""
    # Normalize double single quotes to single (legacy data issue)
    text = text.replace("''", "'")
    # Replace single quotes with HTML entity
    return text.replace("'", "&#39;")
```
- **Location**: backend/content_publisher.py
- **Date**: 2026-01-15

### Redshift Serializable Isolation Violation (Error 1023)
- **Error**: `Error: 1023 DETAIL: Serializable isolation violation on table`
- **Cause**: Multiple concurrent batch jobs updating the same Redshift table with individual UPDATE statements
- **Solution**: Replace individual UPDATEs with batch UPDATE using IN clauses:
```python
# ❌ Wrong: Individual UPDATEs cause serialization conflicts
for url in urls:
    cur.execute("UPDATE ... SET kopteksten = 1 WHERE url = %s", (url,))

# ✅ Correct: Single batch UPDATE
if urls:
    placeholders = ','.join(['%s'] * len(urls))
    cur.execute(f"UPDATE ... SET kopteksten = 1 WHERE url IN ({placeholders})", urls)
```
- **Impact**: Eliminates serialization conflicts, 15-20% throughput improvement
- **Location**: backend/main.py
- **Date**: 2025-10-28

### FastAPI Async vs Sync Endpoints with psycopg2
- **Problem**: API endpoint hangs indefinitely at database connection
- **Cause**: Async endpoint (`async def`) calling synchronous psycopg2 pool operations blocks event loop
- **Solution**: Use synchronous endpoints (`def`) when using synchronous database drivers
```python
# ❌ Wrong: Async endpoint with sync database pool
@app.post("/api/process-urls")
async def process_urls():
    conn = get_db_connection()  # Blocks event loop!

# ✅ Correct: Synchronous endpoint
@app.post("/api/process-urls")
def process_urls():
    conn = get_db_connection()  # No event loop blocking
```
- **Rule**: Use `def` with psycopg2, use `async def` only with async-compatible drivers (asyncpg)
- **Location**: backend/main.py, backend/database.py
- **Date**: 2025-10-23

### Database Query Performance - LEFT JOIN vs NOT IN
- **Problem**: Query timeout with 75k+ URLs when using NOT IN subquery
- **Solution**: Replace with LEFT JOIN ... WHERE IS NULL pattern
```sql
-- ❌ Slow: NOT IN subquery (timeout)
SELECT COUNT(*) FROM table1 WHERE url NOT IN (SELECT url FROM table2);

-- ✅ Fast: LEFT JOIN pattern (<100ms)
SELECT COUNT(*) FROM table1 t1
LEFT JOIN table2 t2 ON t1.url = t2.url
WHERE t2.url IS NULL;
```
- **Performance**: 30+ seconds → <100ms
- **Location**: backend/main.py - `/api/status` endpoint
- **Date**: 2025-10-22

### R-Finder Tool - Finding /r/ Redirect URLs from Redshift
- **Purpose**: Find visited /r/ redirect URLs from Redshift for analysis
- **Endpoint**: `POST /api/rfinder/search` with filters, min_visits, date range, limit
- **Query Logic**: Uses AND logic for multiple filters (all must match)
- **Tables**: Same as canonical generator (`datamart.fct_visits`, `datamart.dim_visit`)
- **Key Features**:
  - Multiple URL path filters (combined with AND)
  - Minimum visits threshold
  - Date range filtering (default: 2015-01-01 to present)
  - Copy-to-clipboard with tab-separated relative URLs
- **Frontend**: `frontend/rfinder.html` with `frontend/js/rfinder.js`
- **Backend**: `backend/rfinder_service.py`
- **Location**: http://localhost:8003/static/rfinder.html
- **Date**: 2026-01-29

### Canonical REMOVEBUCKET Transformation
- **Purpose**: Remove facet buckets from URLs based on facet/category rules
- **Input**: Excel file with columns: `facet`, `caturl`
- **Script**: `run_canonical_removebucket.py` (standalone, bypasses API timeout)
- **Algorithm**:
  1. Parse facet→category mapping from Excel
  2. Query Redshift for URLs containing facet patterns (`facet~number`)
  3. For each URL matching a category, remove specified facet buckets
  4. Clean up separators (`~~` → `~`, trailing `/c/`, etc.)
- **Key Function**:
  ```python
  def remove_bucket_from_url(url, facet_name):
      pattern = rf'{re.escape(facet_name)}~\d+'
      # Handles: facet~~other, other~~facet, /c/facet
  ```
- **Results** (2026-01-30):
  - 780 rules (30 facets, 13 categories)
  - 10,000 URLs queried
  - 7,778 URLs transformed
  - 22 facets had no matching URLs in visited data
- **Output**: `/tmp/canonicals_output_removebucket.csv`
- **Date**: 2026-01-30

---
_Last updated: 2026-02-03 (301 Generator, UI/UX improvements, navigation updates)_

## 301 Generator Tool
- **Purpose**: Generate 301 redirects for URLs with unsorted facets or facet/category transformations
- **Features**:
  - Sort facets alphabetically (facet names before `~` are sorted)
  - Category transformations (CAT-CAT): Replace category slugs
  - Facet transformations (FACET-FACET): Replace facets with/without IDs
  - Works with facets with IDs (e.g., `merk~4412606` → `materiaal~484491`)
  - Works with facets without IDs (e.g., `merk` → `materiaal`, keeps the ID)
  - Category filter for facet rules (apply only to specific categories)
- **Files**:
  - Backend: `backend/redirect_301_service.py`
  - Frontend: `frontend/301-generator.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/301-generator/generate` - Generate 301 redirects
  - `POST /api/301-generator/check` - Check single URL sorting
- **URL Sources**: Redshift visits data or manual URL input
- **Output**: CSV export with original URL and redirect target
- **Date**: 2026-02-03

## UI/UX Improvements (2026-02-03)
- **Project Rename**: "SEO Tools Dashboard" → "DM Tools Dashboard"
- **Tool Rename**: "SEO Content Generator" → "Kopteksten Generator"
- **Navigation Updates**:
  - All tools now linked in all page headers
  - Sticky navigation header (stays visible when scrolling)
  - Selected tool highlighted with lighter purple (#8b7bb5)
  - Removed `target="_blank"` from all navigation links
- **Dashboard Redesign**:
  - Inverted icon colors (purple background, white icons)
  - Removed bullet point feature lists for compact tiles
  - Removed subtitle text
  - Updated footer: "Digital Marketing tools by Joep van Schagen - 2026"
- **Footer**: Added consistent footer to all pages
- **Default Values Updated**:
  - FAQ Generator: Batch size 100, Parallel workers 20, Validation batch 500, Validation workers 20
  - Kopteksten Generator: Same defaults as FAQ Generator
  - Content Publishing: Default environment changed from "staging" to "production"
- **301 Generator Cleanup**:
  - Removed "Sort only" option
  - Removed info box
  - Changed rule headers from "Category Rules (CAT-CAT)" to "Category rules"
  - Changed X-button character from `X` to `×` (matching canonical generator)
  - Removed "New Maincat" option from category rules
- **Date**: 2026-02-03

## Combined Reset Validation Functionality
- **Change**: "Reset Validation" button now also resets skipped URLs recheck status
- **Behavior**: Calls both `/api/validation-history/reset` and `/api/recheck-skipped-urls/reset`
- **Purpose**: Single button to reset all validation/recheck state for fresh start
- **Location**: `frontend/js/app.js` - `resetValidationHistory()` function
- **Date**: 2026-02-03

## Redirect Checker Comma-Separated Input
- **Change**: URL input now accepts both newline and comma-separated URLs
- **Regex**: `urlInput.split(/[\n,]+/)` instead of just newlines
- **Location**: `frontend/redirect-checker.html`
- **Date**: 2026-02-03

## 301 Generator Bulk Paste & Remove All
- **Bulk Paste Feature**: Added ability to paste tab-separated data from Excel into both category and facet rules
  - Button "Bulk Paste" opens modal with textarea for pasting
  - Parses tab-separated lines (Excel format): `old_value\tnew_value` or `old_value\tnew_value\tcategory_filter`
  - Empty existing rules are automatically removed before adding new ones
  - Placeholder text explains expected format with outlined styling for visibility
- **Remove All Buttons**: Added "Remove All" button for both category and facet rules
  - Confirms before removing: "Are you sure you want to remove all category/facet rules?"
  - Clears all rules and adds one empty rule as placeholder
- **Key Functions**:
  - `parseCategoryBulk()` - Parse bulk category rules
  - `parseFacetBulk()` - Parse bulk facet rules (supports optional 3rd column for category filter)
  - `removeAllCategoryRules()` - Remove all category rules
  - `removeAllFacetRules()` - Remove all facet rules
- **Location**: `frontend/301-generator.html`
- **Date**: 2026-02-03

## 301 Generator Auto-Filter from Rules
- **Problem**: Previously fetched ALL faceted URLs from Redshift (e.g., 100,000), then applied rules to find matches (e.g., 93)
- **Solution**: Now automatically extracts patterns from rules and uses BATCHED queries
- **Smart Pattern Extraction**:
  - Extracts first facet from compound facets: `merk~83723~~model_lamp~123` → `merk~83723`
  - Deduplicates to get unique prefixes only
  - Handles many rules efficiently (100+ rules → ~10 unique patterns)
- **Batched Queries** (better than OR logic):
  - Runs separate query for each unique pattern
  - Each query is fast and simple: `WHERE url LIKE '%pattern%'`
  - Combines and deduplicates results
  - No limit on number of patterns
- **New Function**: `fetch_urls_with_facets_batched(patterns, ...)` in `redirect_301_service.py`
- **API Response**: Includes `search_patterns` showing which patterns were used
- **Frontend Changes**:
  - Removed "URL contains" filter (redundant with auto-filter)
  - Auto-filter always enabled when using Redshift
- **Example**: 36 rules with `model_lamp~XXXXX` → 36 batched queries (one per unique pattern)
- **Date**: 2026-02-03

## Google Ads WSA Error 10048 (Port Exhaustion)
- **Error**: "failed to connect to all addresses; WSA Error 10048"
- **Cause**: Too many API connections opened too quickly, exhausting available local ports
- **Windows Socket Error 10048** = "Address already in use" (WSAEADDRINUSE)
- **Solutions**:
  - Add delays between API calls (0.3s recommended)
  - Implement retry logic with exponential backoff
  - Wait 2-4 minutes after errors for ports to release (TIME_WAIT state)
- **Implementation**: Added to `process_reverse_exclusion_sheet` in campaign_processor.py
- **Date**: 2026-02-04

## Google Ads "unauthorized_client" Error
- **Error**: "unauthorized_client: Unauthorized" when initializing GoogleAdsClient
- **Root Cause**: refresh_token was generated with different client_id/client_secret than those in google-ads.yaml
- **Working Script Pattern** (`create GSD-campaigns WB.py`):
  - Hardcodes refresh_token and developer_token
  - Loads client_id/client_secret from environment variables (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)
  - Uses `GoogleAdsClient.load_from_dict()` instead of `load_from_storage()`
- **Fix**: Updated campaign_processor.py to use same approach as working script
- **Date**: 2026-02-04

## cat_ids Mapping for Campaign Matching
- **Purpose**: Map maincat_id to list of deepest_cats for finding related campaigns
- **Sheet**: `cat_ids` sheet in workbook with columns: maincat, maincat_id, deepest_cat, cat_id
- **Function**: `load_cat_ids_mapping(workbook)` returns `{maincat_id: [deepest_cat1, deepest_cat2, ...]}`
- **Usage Pattern**:
  1. Get maincat_id from input row
  2. Look up all deepest_cats: `cat_ids_mapping.get(maincat_id_str, [])`
  3. For each deepest_cat, construct campaign name: `PLA/{deepest_cat}_{cl1}`
- **Used By**: `process_exclusion_sheet_v2`, `process_reverse_exclusion_sheet`
- **Date**: 2026-02-04

## Canonical Generator Category Filter Bug
- **Problem**: CAT+FACET rules with category filter weren't respecting the filter
- **Root Cause**: `fetch_urls_for_rules()` fetched all URLs with facet but didn't filter by category
- **Affected Functions**:
  - `fetch_urls_for_rules()` - wasn't filtering fetched URLs
  - `_determine_tasks()` - applied rule to URLs not matching category
  - `_apply_cat_facet()` - didn't skip rules where category didn't match
- **Fix**: Added category filter checks in all three functions
- **Location**: `backend/canonical_service.py`
- **Date**: 2026-02-04

## Kopteksten Skip Reasons
- **Table**: `pa.jvs_seo_werkvoorraad_kopteksten_check` (PostgreSQL in seo_tools_db)
- **Columns**: url, status (success/skipped/failed), skip_reason
- **Skip Reasons**:
  - `no_products_found` (54,053) - page has no products
  - `api_failed` (3,670) - scraper API call failed
  - `no_valid_links` (402) - AI content has no valid /p/ links
  - `ai_generation_error: {msg}` - OpenAI generation failed
  - `rate_limited_503` - 503 error (rate limiting)
- **Reset Query**: `DELETE FROM ... WHERE status IN ('skipped', 'failed') AND skip_reason <> 'no_products_found'`
- **Date**: 2026-02-04

## IndexNow Integration
- **Service**: `backend/indexnow_service.py` - adapted from standalone `index_now.py`
- **Uses**: `database.py` Redshift connection pool (not hardcoded credentials)
- **API**: POST to `https://api.indexnow.org/IndexNow` with host, key, keyLocation, urlList
- **Redshift table**: `pa.index_now_joep` for submission tracking
- **Endpoints**: POST `/api/indexnow/submit`, POST `/api/indexnow/upload-excel`, GET `/api/indexnow/history`
- **Date**: 2026-02-10

## SEO Index Checker Integration
- **Service**: `backend/index_checker_service.py` - Google Search Console URL Inspection API
- **Uses**: Service account JSON files in `backend/service_accounts/` (gitignored)
- **Quota**: 2,000 requests/day per service account, rotates on quota exhaustion
- **Service account credentials**: Different from Keyword Planner (service account JSON vs OAuth2) - NOT interchangeable
- **Endpoints**: POST `/api/index-checker/check`, POST `/api/index-checker/upload-excel`, GET `/api/index-checker/quota`
- **Date**: 2026-02-10

## AI Title Generation - Size Placement Rule
- **Issue**: Sizes (Maat L, XL, 42, etc.) were placed before the product name ("Nike Heren Maat L tanktops")
- **Fix**: Added rule to both prompts in `ai_titles_service.py` to place sizes AFTER the product name
- **Correct**: "Nike Heren tanktops Maat L"
- **Both prompts updated**: `generate_ai_title()` (rule 4) and `generate_title_from_api()` (rule 6)
- **Reset**: 2,231 URLs with `maat` facets reset to pending for reprocessing
- **Date**: 2026-02-10

## Silent Docker Exec Output
- **Issue**: `docker exec` with inline Python (`python3 -c "..."`) sometimes produces no output
- **Workaround**: Write a .py script file and run it with `python3 -m backend.script_name` from `/app`
- **Date**: 2026-02-10

## Dutch Singular/Plural Form Rules (category_forms.json)
- **Dutch words never end in 'z'**: Always wrong — should be s, es, uis, ars, oos, aus, etc.
  - Examples: hoez→hoes, doz→doos, kluiz→kluis, laarz→laars, sauz→saus, muiz→muis
- **Dutch words never end in 'v'**: Always wrong — should be f
  - Examples: schroev→schroef, schijv→schijf, bruv→bruf
- **-el endings need case-by-case analysis**: Many -el words are correct (artikel, stoel, meubel, wiel, doel)
  - Only fix genuine errors: onderdel→onderdeel, panel→paneel (doubled vowel before -el)
- **File**: `backend/category_forms.json` — 148 fixes applied (94 z-endings, 14 v-endings, 40 -el forms)
- **Date**: 2026-02-17

## AI Title Generation: doelgroep_drogisterij as Voor-Facet
- **Problem**: Target group facet values (mannen, vrouwen, kinderen) were placed directly before product name
- **Fix**: Added `voor_values`/`voor_originals` lists in `ai_titles_service.py`. When `fname == 'doelgroep_drogisterij'`, values are wrapped as "voor {value}" and appended after the title
- **Flow**: Detected in facet classification → stripped from H1 → appended as "voor mannen" etc. after title
- **Reset**: 765 URLs containing this facet reset to pending
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-17

## AI Title Generation: aantal_puzzelstukjes as Spec Value
- **Problem**: Puzzle piece count facet (e.g., "500 Stukjes") was placed before product name instead of after
- **Fix**: Added `fname == 'aantal_puzzelstukjes'` check to `is_spec_value()` function
- **Reset**: 432 URLs reset to pending
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-17

## Kopteksten Generator Uses jvs_seo_werkvoorraad Table
- **Problem**: New URLs appeared in unique titles generator but not in kopteksten generator
- **Root Cause**: Kopteksten generator pulls from `pa.jvs_seo_werkvoorraad`, NOT `pa.content_urls_joep`
- **Lesson**: `content_urls_joep` is for unique titles tracking only; `jvs_seo_werkvoorraad` is for kopteksten
- **Date**: 2026-02-17

## Facet Volume Batch Processing (New Excel Input)
- **Script**: `backend/run_facet_volumes_new.py` — processes facet/category search volumes from Excel input
- **Input**: Excel with 'facets' sheet (maincat IDs in col B, facet values in col G) and 'cats' sheet (maincat IDs in col B, deepest cats in col C)
- **SIC/SOD handling**: HTML comment format `<!--SIC-->value<!--/SIC-->` for after-category form, `<!--SOD-->value<!--/SOD-->` for before-category form
- **Resume-capable**: Uses progress file to track completed maincats, saves Excel after each maincat
- **Output**: Search volume written to column K of the facets sheet
- **Scale**: 236,232 facets across 31 maincats, 2.1B total search volume
- **Date**: 2026-02-17

## UTF-8 Mojibake Fix for Excel Files
- **Problem**: "KÃ¤rcher" displayed instead of "Kärcher" — UTF-8 bytes read as latin-1
- **Fix**: `val.encode('latin-1').decode('utf-8')` — re-encode as latin-1 then decode as UTF-8
- **Scale**: Fixed 2,928 values in faet_values_new.xlsx
- **Date**: 2026-02-17

## Visits/Revenue Aggregation per Facet from Redshift
- **Script**: `backend/add_visits_revenue.py` — aggregates URL visits/revenue per facet bucket
- **Data source**: Redshift query extracting URLs with `/c/` path, visits count, and CPC+WW revenue since 2024
- **Matching logic**: URLs split on `/c/` then on `~~` to extract individual facet buckets
- **Scale**: 1.56M URLs → 74,145 facets matched, 33.6M total visits, 3.15M total revenue
- **Output**: Visits in column I, revenue in column J of output Excel
- **Date**: 2026-02-17

## Docker-Owned File Permission Workaround
- **Problem**: Output Excel file created by Docker (root) couldn't be overwritten by user process
- **Workaround**: Save to a different filename (e.g., `_final.xlsx`) owned by the user, then copy
- **Date**: 2026-02-17

## Syncing dm-dashboard → dm-tools
- **dm-dashboard repo**: https://github.com/joep-1993/dm-dashboard — Docker-free version of the dashboard
- **Dual commit policy**: Dashboard changes must be committed to both dm-tools and dm-dashboard repos
- **Sync process**: `git pull` in dm-dashboard, compare diffs, apply to dm-tools manually
- **Date**: 2026-04-03

## PostgreSQL Stale Connection Recovery
- **Problem**: Long-lived PG pool connections go stale, causing queries to fail silently
- **Fix**: Added TCP keepalives (`keepalives=1, keepalives_idle=30`) to PG pool, plus `SELECT 1` health check in `get_db_connection()` before returning connections — matches the existing Redshift pool pattern
- **Files**: `backend/database.py`
- **Date**: 2026-04-03

## Last Push Timestamp Feature
- **Purpose**: Shows when content was last published to production, next to the Publish button
- **Components**: `pa.publish_log` table, `GET /api/content-publish/last-push` endpoint, `fetchLastPushTimestamp()` JS function (in both app.js and faq.js)
- **Format**: DDMMYYYY HH:MM, right-aligned next to Publish button
- **Auto-refresh**: Fetched on page load and after each successful publish
- **Date**: 2026-04-03

## Running dm-tools Without Docker
- **Script**: `./run_local.sh setup` (first time) then `./run_local.sh` to start
- **Requirements**: `python3.12-venv` apt package, Python venv at `dm-tools/venv/`
- **Key difference**: `load_dotenv()` added to top of `main.py` — loads `.env` file for env vars (no-ops in Docker since vars already set)
- **thema_ads_optimized**: Docker mounts from `../theme_ads/thema_ads_optimized`; locally, `run_local.sh` creates a symlink automatically
- **DATABASE_URL**: `.env` must point to remote DB (`10.1.32.9`) instead of Docker's `db:5432`
- **Auto-start**: Windows Task Scheduler task "DM Tools Dashboard" runs `wsl.exe -d Ubuntu -e bash -c "cd ... && source venv/bin/activate && uvicorn ..."` at logon
- **Date**: 2026-04-03
