# BACKLOG
_Future features and deferred work. Update when: deferring tasks, planning phases, capturing ideas._

## Product Vision
_What are we building and why?_

[Define your product vision here]

## Future Enhancements

### DMA Exclusions
- [ ] **OOS residual: stale crawl-OOS matches with no contradicting stock signal** (logged 2026-06-29). After the `is_cheapest_offer` + stale-crawl guards (LEARNINGS 2026-06-29), a flagged headline offer can still be a false positive when the Google AIU crawl-OOS verdict is days stale but NOTHING contradicts it — `beslist_served=True`, `feed_stock=null`, and beslist's index has no stock for that shop (e.g. Douglas.nl `0038097025002`). These read as `match`/excludable and rely on operator spot-check. Crawl-age (`google_last_update`) was rejected as an auto-discriminator (genuine matches 2-3d, ~half the worklist 4-6d → any threshold guts coverage). Options if it becomes painful: (a) an **independent live stock check** (fetch PLP/offer per match before allowing exclude — bigger build, slower scans, live-source reliability uncertain); (b) a **caution column** surfacing `google_last_update` age + feed/served/ES-stock signals on match rows to prioritise manual review (low-risk, non-suppressing — user leaned this way before picking the same-shop ES guard). Either is additive on top of `_oos_verdict`. See LEARNINGS "OOS headline verdict moved from ES `bestOffer`". **UPDATE 2026-06-29:** option (b) shipped (bcc14bb/d8cae1b) — `stale_crawl` flag (crawl ≥ `CRAWL_STALE_DAYS`=3), amber "⚠ crawl Nd" badge, de-selected from Select-all but still individually excludable, "hide stale crawl" filter. The auto-detect residual (option a, live check) is still open but lower priority now that these are visibly flagged + not bulk-excludable.
- [x] **Ask the OOS monitor owner for a bulk `is_cheapest_offer` endpoint** (logged 2026-06-29, **SHIPPED 2026-06-30**). The scan enriched each live-in-DMA EAN with one `GET /api/v1/overrides?q=<ean>` round-trip; that enrichment was the dominant cost of a COLD scan (~9-10 min for a full scan) and is **server-bound** — raising client concurrency 16→32 gave no speedup (0.255 vs 0.241 s/EAN), so the only real fix was a bulk endpoint. Requested shape: `POST /api/v1/overrides/by-eans {country, state, eans:[...]}` → per-EAN rows (`is_cheapest_offer, ean_offer_count, beslist_served, feed_stock, google_last_update, shop_name`), uncapped and regardless of served-state. **DONE:** the monitor owner (Bram) built it near-exactly — `POST /api/v1/overrides/by-eans` (≤1000 EANs/call → 422 over, one headline-collapsed row per EAN, uncapped, keeps `beslist_served=False` rows). Integrated in `d772355`: the per-EAN `q=` fan-out → chunked bulk fetch (~2350 round-trips → 3 calls; 3 EANs cold 0.09s). He also raised the `/oos-products` cap 2000→20000. See LEARNINGS "OOS bulk /by-eans migration (2026-06-30)".
- [ ] **General serving-leaf walker for allow-list / store-format category trees** (logged 2026-06-25). The exclusion tool's category targeting only handles **block-list** trees (biddable CL3-OTHERS). **Allow-list** trees — the `store_`-format, multi-ad-group campaigns like `PLA/Sport & outdoor store_b` where CL3-OTHERS is NEGATIVE and specific shops are the included positive leaves — are currently **skipped** (safe, but the category portion of those exclusions is not applied; bestsellers+APlus still are). To cover them, replace the per-family `_leaf_for_category`/`_leaf_for_aplus` shortcuts with a general walker: descend the tree following the product's matched custom attributes (CL0/CL1/CL3 from `shopping_performance_view`, captured per ad group), stop at the biddable UNIT the product actually serves under, then subdivide THAT on item_id (reusing the existing convert/append + prune-on-enable logic + the `_ad_group_cpc` bid fallback). Needs careful multi-structure round-trip verification before live use. Matters for OOS bulk-exclusion coverage (many OOS products live in `store_`-format campaigns). See LEARNINGS "OOS feed integration + allow-list tree fix (2026-06-25)".


### Phase 1: Core Features
- [ ] User authentication
- [ ] Data persistence patterns
- [ ] Basic CRUD operations

### Phase 2: Improvements
- [ ] Better error handling
- [ ] Request logging
- [ ] Admin interface
- [x] Export functionality ✅ #completed:2025-10-03

### Phase 3: Scale (if needed)
- [ ] Redis caching
- [ ] Background jobs
- [ ] Multiple workers
- [ ] Monitoring

### Google Ads Automation - Scalability
- [ ] Process 1M ads in 1-3 days with chunking strategy
- [x] Implement progress tracking and resume capability ✅ #completed:2025-10-02
- [ ] Add distributed caching (Redis) for multi-worker processing
- [ ] Create horizontal scaling with worker queue (Celery/RQ)
- [x] Build monitoring dashboard for batch processing status ✅ #completed:2025-10-02
- [ ] Add pause/resume controls to frontend ✅ #completed:2025-10-02 (implemented ahead of schedule)

## Technical Debt
- [ ] Add input validation
- [ ] Implement logging
- [ ] Add tests
- [ ] API documentation
- [ ] Create utility to split large Excel files into processable chunks (10k-50k rows)
- [ ] Add comprehensive error handling for Google Ads API failures
- [x] Add error handling to Thema Ads frontend ✅ #completed:2025-10-02

## Ideas Parking Lot
_Capture ideas for future consideration_

- **DM Review tool — slide 3 (Werkvoorraad) refresh**: deferred from the 2026-05-28 session. Slide 3 of `DM review_NEW.pptx` shows content coverage (FAQ%, Kopteksten%, AI-titles%, etc.) + URL counts (e.g. "389,994 URLs (+33%)"). User said "for sheet 3 I need to provide some extra context", and we shipped slide 2 only. Excel feed tabs for slide 3 are likely `new_visits`, `ut`, `canon`, `red`, `t&d`, `open_facets`, `werkvoorraad`, possibly `top_3_10_*` — see preview output in 2026-05-28 LEARNINGS.
- **Bulk CSV validation endpoint**: Pre-validate large CSVs before job creation (check customer IDs exist, ad groups are valid) - could save time by catching errors before job execution
- **Automated secret scanning in pre-commit hooks**: Prevent accidental commits of secrets with local validation before push (e.g., detect-secrets, git-secrets, or custom regex patterns)
- **Improve 202 retry logic for Cloudflare queuing**: Consider exponential backoff for HTTP 202 responses (2s, 5s, 10s) instead of single 2s retry - may reduce failure rate during high-load periods
- **Adaptive delay based on 202 response rate**: Monitor HTTP 202 response rate in real-time and dynamically adjust scraping delay to stay below Cloudflare's threshold. Start at 0.2s, increase to 0.5s if 202 rate exceeds 10%, decrease back to 0.2s if 202 rate drops below 2%. Would provide automatic optimization between speed and rate limit avoidance.

- **Investigate Kasten (Meubels) SEO ranking decline** (flagged 2026-06-22). During the WoW SEO visit analysis (week 2026-06-14→06-20 vs 06-07→06-13, NL bot-filtered `fct_visits`), the **Meubels → Kasten** subcategory lost **−2,468 visits (9,463 → 6,995, −26%)** — ~13% of the ~19,026 total weekly SEO visit drop, one of the larger single contributors (after Woonaccessoires −7,329 and Klussen −4,986). GSC (`bt.search_console`, `country='nld'`) shows the **whole Kasten URL cluster slipped ~0.5 position uniformly** (impr-weighted avg 3.60 → 4.10) across **all** page types together — R-urls 117,588→76,736 (−35%), C-urls 107,309→75,334 (−30%), Browse/category 58,036→42,597 (−27%) — while rank dropped in lockstep. That uniformity points to a **category-/site-level cause** (Google core update reshuffle, competitor climbing, or a sitewide signal change on the Kasten pages), NOT a template-specific bug. **Caveats learned:** (1) huge impression losses on category + IKEA-branded head-term pages are near-zero-CTR (~0.25% browse pages, ~1.6% overall) so they barely move visits — IKEA queries lost −24,641 impr but only −365 clicks; rank by **click/visit loss, not impressions**. (2) visit loss is broad long-tail erosion — worst single URL only −79 visits (the IKEA Malm ladekast `/r/` page), top 12 URLs ≈ only 10% of the total. **To dive in later:** check whether the rank drop is a Google update (timing vs known updates) vs a specific competitor overtaking on high-volume cabinet terms ("kledingkast", "ladekast", "tv meubel", "dressoir"); pull keyword-level daily position trend for the Kasten hub pages; verify nothing changed on the category-page templates/feed/availability. Query path + the search_console `clean_url` gotcha (use `type_url`/raw `url`, never `clean_url` — it collapses /c/ and /r/ to the base category URL) are in the assistant's memory (`redshift_real_visits_query`, `search_console_clean_url_gotcha`).

## Fridged / Parked Work
_Work that exists in the codebase but is intentionally NOT wired into production. Pick up later._

- **Kopteksten v3 — wire per-maincat informational prompts to production (DECISION PENDING)** (staged 2026-07-01, user reviewing output first). Built + benchmarked; see LEARNINGS "Kopteksten v3". `dm-tools/backend/gpt_service_v3.py` = `generate_product_content_v3(h1, products, maincat)` using per-maincat prompts in `backend/data/kopteksten_maincat_prompts_v3.json` (+ normalized length footer + its own v3 user prompt that lifts v1's single-alinea/150-word caps). NOT wired (`main.py` still uses v1), NOT committed. Benchmark `scripts/koptekst_v3_comparison.py` → `Downloads\claude\koptekst_v1_vs_v3_2026-07-01.xlsx` (v3 209 vs v1 112 words, 100% vs 0% multi-paragraph). Deliverable docs: `kopteksten_informational_prompts_2026-07-01.md` + `..._per_maincat_2026-07-01.json`. **To activate**: resolve `main_cat_name` for the URL (category_lookup / deepest_category→maincat) and route through `generate_product_content_v3` behind an env/query toggle; confirm content_top renders multiple paragraphs (user says yes). **Open cleanups if pursued**: refactor 31 full prompts → 1 shared base + 31 content-modules (~9% overlap now = 31 boilerplate copies); optional deterministic filler-word scrub ("ideaal"/"perfect", ~63% in both v1 & v3, model ignores prompt ban). **This effectively supersedes the Koptekst prompt v2 below** (v2's comparison-authority angle is one narrow idea; v3 is the full informational-koopgids rework grounded in ranking-content analysis).

- **Koptekst prompt v2** (parked 2026-05-22; largely SUPERSEDED by Kopteksten v3 above). New prompt lives in `dm-tools/backend/gpt_service_v2.py` (`SYSTEM_MESSAGE_V2`, `generate_product_content_v2`). NOT wired into `backend/main.py` — production still uses v1 in `backend/gpt_service.py`. Only consumer today is the benchmark script `dm-tools/scripts/koptekst_v2_comparison.py` (pulls N random URLs from `pa.kopteksten_content`, regenerates with v2, writes side-by-side Excel + aggregate metrics + both prompts to Downloads). Latest n=20 run: 20 v1 valid, 19 v2 valid (1 zero-product URL), v2 hits comparison-authority claim ~98% vs v1 ~0%, relative links ~91% vs v1 ~6%, opening-cliché rate 0% vs v1 ~94%. **Variation fix shipped**: `COMPARISON_AUTHORITY_PHRASINGS` (12 templates, varied syntax positions + quantifiers — "alle/diverse/uiteenlopende/talloze/breed aanbod/meerdere") picked at random per call in `build_user_prompt_v2`, system prompt now defers to the per-call hint and explicitly bans the "Op Beslist vind je veel aanbieders van ..." cliché which had become the default. n=20 sample showed 9 distinct templates used across 19 kopteksten with 0 cliché hits. **To activate**: swap the v1 import in `backend/main.py` for `generate_product_content_v2`, or add an env/query-param toggle for gradual cutover. Latest benchmark: `/mnt/c/Users/JoepvanSchagen/Downloads/claude/koptekst_v1_vs_v2_n20_variation.xlsx`.

- **R-URL optimizer L4 — optional "palm"→Palmbomen synonym** (parked 2026-06-19). `/r/tuinplanten_winterharde_palm/` lands on Tuinplanten + `s_bomen~Waaierpalm` (valid). Preferred Bomen + `type_boom~Palmbomen` is blocked by `_is_semantic_match` (keyword-at-start rule, by design). Safe route if ever wanted: add `palmbomen`/generic tree-types as explicit `COMPOUND_DECOMPOSITIONS` or synonym entries so "palm" resolves to `type_boom~Palmbomen` in the Bomen context — data only, no rule change. NOT a bug; low priority. See LEARNINGS "R-URL optimizer — 7-fix batch".

---
_Last updated: 2026-06-22_
