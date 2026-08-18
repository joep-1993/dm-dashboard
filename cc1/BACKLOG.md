# BACKLOG
_Future features and deferred work. Update when: deferring tasks, planning phases, capturing ideas._

## Product Vision
_What are we building and why?_

[Define your product vision here]

## Future Enhancements

### Twee systemen noemen dezelfde pagina anders: h1_title vs product_subject (logged 2026-08-18)
- [ ] `unique_titles_content.h1_title` en het facet-afgeleide `product_subject` zijn het op
  een groot deel van de facetpagina's ONEENS. Steekproef van 150 uit de 881 gemarkeerde
  kopteksten: **73% verschilt**. Voorbeelden:
  `'Speedo Gezondheidsslippers'` vs `'Speedo Teenslippers'` · `'Roze korfbalschoenen'` vs
  `'Roze Hockeyschoenen'` · `'MSV Prullenbakken'` vs `'MSV Afvalbakken'` ·
  `'Hedgren Schoudertassen'` vs `'Hedgren Reistassen'`.

  Dit is een inconsistentie op de pagina zelf: de H1/title zegt het ene, de koptekst het
  andere. Het vertekende ook de driftmeting van 2026-08-18 — driekwart van de 881
  "onderwerpdrift" was in werkelijkheid dit naamverschil, niet een foute koptekst.
  Verdient een eigen ronde: welke van de twee is leidend, waar loopt het uiteen, en moet
  de generator op `h1_title` draaien in plaats van op `product_subject`.

### Facetwaarden die elkaar uitsluiten staan samen op één product (logged 2026-08-18)
- [ ] 296 producten in categorie 9001062 `Rackets` dragen zowel `Sporten: Tennis` (484337)
  als `Sporten: Padel` (13339994), waardoor padelrackets terecht op een Tennis-gefilterde
  pagina terechtkomen. Nu nog laag risico voor kopteksten (slechts 3 halen `shopCount >= 2`),
  maar het is brondata die niemand bewaakt. Melden bij wie de facetmapping beheert; een
  facetconflict-filter in `scraper_service` is bewust NIET gebouwd omdat de ONDERWERP-regel
  in de prompt hetzelfde goedkoper afdekt.

### GSD: twee bugs in het LEGACY CPC-pad, bewust uitgesteld (logged 2026-08-17)
- [ ] Bij het integreren van de prijsbucket-structuur voor nieuwe CPC-shops zijn twee
  live defects in het bestaande CPC-pad gevonden en geverifieerd tegen de feed
  (`dra.gmc_products_issues`, ~40M rijen). **Volledige beschrijving en cijfers staan in
  TASKS.md bij de sprint-entry van 2026-08-17** — hier alleen de kern, zodat het niet
  onder een afgevinkte taak verdwijnt:
  - `PRICE_BUCKETS` eindigt op `1597-2584` / `2584-Onbeperkt`; de feed heeft `1597-2594` /
    `2594+`. Die twee buckets matchen nul producten. Geldt óók voor de campagnes die door
    `scripts_def/GSD-CPC.py` zijn gebouwd, want die heeft dezelfde spelling.
  - `add_sub_cpc` partitioneert op INDEX0 (de score) in plaats van INDEX4 (de prijs), dus
    alles valt in de biddable "overig"-node: geen prijsdifferentiatie en beide campagnes
    serveren de hele catalogus. Raakt alleen de campagnes die het dashboard zelf heeft
    gebouwd — een handvol, want in 60 dagen ging er 1 nieuwe CPC-shop aan.

  Uitgesteld omdat Joep op 2026-08-17 koos voor "alleen nieuwe CPC-shops": repareren raakt
  514 live shop-landcombinaties en is een eigen besluit met een eigen dry-run. Er staat een
  waarschuwing bij de constanten in `gsd_campaigns_service.py` zodat niemand de twee
  bucketlijsten per ongeluk "gelijktrekt".

### SEO Priority: "revert deze push" op basis van de apply-log (logged 2026-08-13)
- [ ] `pa.seo_prio_apply_log` legt per write de oude én nieuwe waarde vast
  (`old_value` → `new_value`, met `status='applied'`). Daarmee is een terugdraai-knop een
  kleine stap: selecteer een reeks logregels en schrijf `old_value` terug via hetzelfde
  read-merge-write-pad. Waarom het nog niet gebouwd is: `inherit` terugzetten vraagt een
  PUT met `seoPriority: null` (kan, veld is `nullable`) en dat is een ander code-pad dan de
  bool-write die er nu staat; en er moet nagedacht worden over wat er gebeurt als iemand
  ná jouw push handmatig iets anders heeft gezet — blind terugzetten overschrijft dat dan.
  Minimaal: bij het terugdraaien de huidige waarde teruglezen en alleen herstellen als die
  nog gelijk is aan wat jij erin hebt gezet.

### SEO/GEO brainstorm: 50 onderwerpen wachten op prioritering (logged 2026-08-12)
- [ ] **Bepaal per onderwerp of het nieuw is of een uitbreiding van iets dat er al staat.**
  Het brainstormbord is op 2026-08-12 uitgelezen naar **50 post-its in 7 thema's** —
  volledige inventaris in `cc1/SEO_GEO_BRAINSTORM.md`, Excel in
  `Downloads\claude\SEO_GEO_brainstorm_onderwerpen.xlsx`, generator in
  `cc1/seo_geo_brainstorm_to_xlsx.py`. De fase-indeling (Awareness / Consideration /
  Decision / Trust Validation) is op verzoek losgelaten; gegroepeerd op onderwerp.

  Verdeling: Contentformats & paginatypes **16**, Keuzehulp & onderbouwing op de site **10**,
  Autoriteit/auteurs/reviews **6**, Off-site/links/social **6**, Technisch &
  LLM-vindbaarheid **5**, Eigen data als contentmotor **4**, Loyaliteit & accounts **3**.

  Waarom dit nog niet oppakbaar is: **er staat geen enkele prioriteit op het bord** op één
  TOP!-sticker na (datagedreven trendrapporten). Geen effort, geen eigenaar, geen
  onderbouwing per idee. Wie hier verder mee wil, moet eerst die laag toevoegen — en dat is
  een gesprek, geen invuloefening. De Excel heeft die kolommen daarom bewust niet.

  Wat wél alvast te doen is zonder besluit: **het overlap-onderzoek**. Meerdere onderwerpen
  raken bestaande onderdelen van het dashboard — `Schema markup toevoegen` en `Javascript
  rendering afbeeldingen` liggen dicht bij de SEO Rulings-checks, `FAQ` heeft al een
  generator (`faq_service.py` + `faq_v2_publisher.py`), en de contentkalender-ideeën leunen
  op dezelfde Redshift-data als SEO stats. Dat scheidt "nieuw bouwen" van "uitbreiden"
  vóórdat er over prioriteit gepraat wordt.

  Let op bij verifiëren: de bron-PDF is een **kapotte export** (zie LEARNINGS 2026-08-12) —
  vraag om de afbeelding van het bord, niet om de PDF.

### Basements deepest-cats: welke vorm krijgt de results-check? (logged 2026-08-06)
- [ ] **Kies een aanpak voordat we `basements_deepest_cats_{nl,be,de}.json` verbouwen.** De
  `check_and_results`-node (uit `basements_homepage_nl`, op 2026-08-06 ook in de drie
  maincat-flows gezet) drukt per `/c/`-URL het echte AND-aantal producten uit de Search API en
  gooit lege/dunne pagina's uit de basement. Voor de maincats is dat een kwartiertje extra per
  run; voor de deepest cats **niet**, en de vraag is welke van de vier routes we nemen. Joep
  wilde de keuze eerst uitgediept hebben — daarom hier geparkeerd, nog geen besluit.

  Waarom het bij deepest anders ligt (gemeten, zie LEARNINGS 2026-08-06):
  **110.627 URLs** in de top-100-set over 3.321 cats, gemiddeld 1,66 facetten per URL →
  **~190.000 Search-API-calls** en **~14 uur** sequentieel per run. De flow start om 18:15 en
  zou tot de volgende ochtend doorlopen, over de volgende run heen, met een n8n-worker bezet.
  Twee dingen maken dat extra vervelend: er is **geen hervat-mechanisme** (de `chk`-join in
  `get_cat_ids` staat uitgecommentarieerd, en in de maincat-variant wijzen de drie
  check-tabellen naar drie verschillende namen), en er is **geen bijvulruimte** — een deepest
  cat heeft gemiddeld maar 33 kandidaat-URLs in 30 dagen, dus de cap in `build_query` verhogen
  levert niets op. Bij ~30% drop gaat een basement van gemiddeld 33 naar ~23 links.

  De vier routes zoals ze op tafel lagen:
  1. **Cache-tabel + JOIN (mijn voorkeur).** Aparte nachtelijke job checkt URLs en schrijft
     `url -> aantal resultaten` weg met een TTL (7–30 dagen); de basement-flow filtert er met
     een JOIN op en blijft net zo snel als nu. Eerste vulling ~190k calls, daarna nog maar een
     paar duizend per dag. Meeste bouwwerk: nieuwe tabel + nieuwe workflow + aanpassing in
     `build_query`. **Open sub-vraag: waar landt die tabel — Redshift `pa.` of de gedeelde
     Postgres op 10.1.32.9?** Bewust nog niet ingevuld.
  2. **Inline met parallelle batches.** Zelfde patch als maincat, maar `Promise.all` in blokken
     binnen de Code node: ~14 uur → ~1,5 uur. Geen nieuwe infra, maar wél ~190k calls elke dag
     en 20–40 req/s op productie-ES tijdens de run.
  3. **Inline sequentieel.** Exact dezelfde patch als de maincat-flows, kleinste diff en
     makkelijk te reviewen — maar die ~14 uur per run.
  4. **Alleen de gratis SQL-filters.** Nul API-calls: `winkel~`-URLs eruit (1.694 stuks) en
     eventueel een limiet op facetdiepte, rechtstreeks in `build_query`. Vangt de echt lege
     facetcombinaties niet af.

  Let op bij route 1 en 2: de node zoals hij nu in de maincat-flows staat, is geschreven voor
  het per-cat item `{cat_id, date_to, urls:[…]}` uit `group_by_cat_id`. Een losse cache-job
  werkt op platte URL-rijen en heeft dus een andere wrapper (de evaluatiekern kan hetzelfde
  blijven). En de `countryLanguage` + percent-decode-gotchas uit LEARNINGS gelden daar net zo.

### Shop-campaigns mist de WoW-laag die SEO stats wél heeft (logged 2026-08-06)
- [ ] **Baseline-fetch toevoegen zodat de tegels en de tooltip een vergelijking kunnen tonen.**
  Shop-campaigns is op 2026-08-06 visueel gelijkgetrokken met SEO stats (layout, chart-chrome,
  kleuren, loader — zie TASKS), maar twee dingen bleven bewust liggen omdat ze géén
  opmaakkwestie zijn: de **WoW-pillen in de tooltip** en de **sparklines + delta-badges in de
  tegels**. Beide hangen aan data die deze pagina niet ophaalt — SEO stats doet daarvoor een
  extra `/daily`-call over de voorgaande 7 dagen (`loadWowBase()`) plus één over de
  even lange periode ervóór (`loadTileDeltas()`). Zonder die twee calls is er niets om tegen
  af te zetten en zou een pil een verzonnen getal tonen.
  Aandachtspunt bij het bouwen: de tegels zijn hier al de legenda én de toggles, dus een
  sparkline moet — net als in SEO stats — een niet-klikbaar gebied worden, anders klapt de
  serie uit zodra je een dag wilt aflezen. En de deltafetch moet dezelfde `loadToken`-guard
  respecteren als `load()`, anders deelt een oude baseline zich door een nieuwe periode.

### Verwijderde maincat-level URLs kunnen zichzelf terugschrijven (logged 2026-08-06)
- [ ] **Beslis of de 4.699 opgeschoonde URLs permanent contentloos moeten blijven.** Op
  2026-08-06 zijn koptekst + FAQ verwijderd voor 4.699 maincat-level `/c/`-URLs (zie TASKS).
  De job-rijen zijn mee weggegooid, maar `pa.urls` is ongemoeid — dus een backfill die
  job-rijen aanmaakt voor URLs zónder job pikt deze set weer op en genereert alles opnieuw.
  Als "weg" ook "weg blijven" betekent, is er een guard nodig: een uitsluiting in de
  queue-logica, of een `notes`-tag op die URLs waar de queue op filtert. De set is exact
  reproduceerbaar via `pa.del_targets_maincat_c_20260806`. Doen we niets, dan is dit een
  eenmalige opschoning die stilletjes terugdraait — en dat merk je pas als de content er weer
  staat.
- [ ] **Publish draaien om de live site in lijn te brengen.** Bewust uitgesteld; tot die tijd
  staan ~4.300 kopteksten en ~4.600 FAQ's nog op beslist.nl. Let op dat de eerstvolgende
  volledige publish dit hoe dan ook doorvoert (replace-all), ook als iemand anders hem draait
  voor iets heel anders.

### SEO titles — converge the legacy tblPageTitles corpus, or accept the split (logged 2026-07-31)
- [ ] **Decide whether the legacy blueprints should follow the current builder at all.**
  Audited 2026-07-31: of 154.721 legacy NL rows in `pa.page_titles_existing`, **92.329
  (59,7%) would be phrased differently** by today's builder. Today's position-pin work
  explained only 1.933 of those (all now regenerated); the rest is years-old divergence —
  `merk` position (2.147), category placement (4.144), other reordering (4.293), added or
  suppressed placeholders (3.360), and a long tail dominated by the
  `!!sub_category_lower!!` vocabulary. Two hard blockers before any bulk convergence:
  **3.208 rows contain hand-written words** ("voor kinderen", "maat", "Ontwormen") that a
  rebuild deletes, and **116.132 rows use `!!sub_category_lower!!`** — switching those to
  `!!sub_category!!` capitalises the category word in live H1s. Also unknown from our side:
  whether `pa.page_titles_existing` still reflects MySQL, and which store the site prefers
  when both hold a `(cat_id, key)`. Data-quality note: **384.493 of its 539.214 rows have a
  non-country value in `country_code`** (the export is column-misaligned outside NL), so
  scope every query to `country_code='NL'`. Full diff:
  `Downloads/claude/seo_titles_legacy_vs_current_20260731.csv`.

### BLOCKER — the OpenAI key has no credits (found 2026-07-31)
- [ ] **Top up / repoint the OpenAI key: every AI-titles call returns
  `429 insufficient_quota / credit_balance_exhausted`.** v3 falls back to its
  deterministic composed H1, so output is still correct but **unpolished** — no adjective
  inflection ("Hardhout Potdekselplanken" where polish writes "Hardhouten"). This silently
  degrades every unique-titles generation, not just the 5 t_tuinhout URLs regenerated that
  day; those 5 are worth re-running once credits are back (see TASKS "t_tuinhout flipped
  to a type facet"). Nothing in the UI signals the fallback — worth a status line if it
  cannot be fixed quickly.

### GSD — 2.954 canonical campaigns carry no GSD_SCRIPT label (found 2026-07-31)
- [ ] **Decide on a label backfill.** 416 shops, 2.456 of them ENABLED, plus 8.565
  legacy-named unlabelled ones. Unlabelled = invisible in Campaigns created, **not
  pausable by the tool**, no creation date logged (Elektroshop.nl went `uit` on 2026-07-31
  and kept running). Cause: the label is applied in a separate best-effort call after the
  create, and failures were swallowed — now returns a bool and logs `UNMANAGED CAMPAIGN`,
  so the set should stop growing. Attaching GSD_SCRIPT to the 2.954 canonical ones makes
  them manageable in one go, which is a deliberate decision, not a bugfix. The legacy-named
  8.565 are a separate call. Scan pattern: `scratchpad/gsd_unlabeled_split.py`
  (re-create under `scripts/analysis/` when approved). See LEARNINGS.
  **PARTLY SELF-HEALING since 2026-07-31:** the create path adopts a matched campaign and
  the pause path labels anything it pauses by name, so a campaign gets its label the first
  time its shop flips on or off. That only reaches shops that appear in the changes feed —
  a full backfill is still the only way to fix the whole estate at once.

### GSD — the SA360 bid-strategy pairing queue (logged 2026-07-31)
- [ ] **Surface the "awaiting bid strategy" list as a worklist, not just a preview count.**
  105 GSD campaigns sat PAUSED on MANUAL_CPC waiting for the manual target-ROAS pairing in
  SA360 (70 of them created 2026-07-31; by the end of that afternoon 55 of those 70 were
  paired and ENABLED, 15 were not — the queue moves, but only a person can see where it is). The run refuses to enable them (correct — see
  LEARNINGS) and the preview shows the count, but the people doing the pairing have no
  list. A small read-only endpoint/table (shop, country, label, campaign id, created date)
  would make the handover explicit; `Downloads/claude/gsd_would_activate_20260731.csv` is
  the shape.

### Is the post-cliff /c/ rate drift a second decline? (logged 2026-07-30)
- [ ] **Separate the post-10-March drift from seasonal category mix.** After the
  bol.com cliff, /c/ outclicks-per-visit recovered to ~0,85 (April) and then drifted
  down again to 0,800 by July (1-facet: 0,836 apr → 0,811 may → 0,798 jun → 0,800 jul).
  Unknown whether that is a second, still-running erosion or just summer mix — summer
  categories demonstrably monetise lower (Sport & outdoor OPB fell €188 → €114 per
  1.000 visits while its visits rose 13%). Method: repeat the month-over-month
  within-depth decomposition (which came out 98% within-depth / 1,6% depth-mix for the
  full year) but with a *category*-mix counterfactual per month. Worth doing because it
  decides whether anything is still actively degrading, or whether the whole level shift
  is already explained by 10 March and needs no further action. See LEARNINGS "Een
  dag-op-dag CTR-dip najagen vóór je het jaar bekijkt".

### SEO Titles — example URL per built combo (logged 2026-07-30)
- [ ] **Give Built-titles rows an example URL so the Facets column can link.** The
  frontend already renders the Facets value as a link when `source_url` is absolute
  (works in Pushed titles: 43.874/43.889 rows). Built rows have **0 of 33.730**,
  deliberately: they come from `scripts/pagetitles_top5_allchannel_combos.py --write`,
  which synthesises combos as cartesian products of each category's top-5 facets and
  passes `source_url=None` — they were never a single URL, which is why they had no
  blueprint. And it cannot be constructed from stored data: `key` holds facet *names*,
  not the facet *value ids* a `/c/` URL needs. Two ways to look one up:
  **(a) Redshift traffic match (recommended)** — scan faceted `/c/` URLs, compute
  `(cat_id, canon_key)` per URL with the existing `parse_url` + `canon_key`, store the
  highest-traffic match as `example_url`. Reuses the generation path's own code; a
  combo with no traffic simply gets no link, which is honest. **(b) Search API
  construction** — resolve one facet value per facet name and build the URL; covers
  zero-traffic combos but can produce a link to an empty PLP. Either way it needs a
  new nullable column plus a backfill over the 33.730 built rows. See LEARNINGS
  "Een gesynthetiseerde combo heeft géén source-URL".

### Content/FAQ export endpoints block the whole app (logged 2026-07-30)
- [ ] **Make the five export endpoints non-blocking.** `/api/export/xlsx`,
  `/api/export/json`, `/api/faq/export/xlsx`, `/api/faq/export/json` and
  `/api/export/combined/xlsx` are `async def` and do blocking DB + workbook work over
  the entire corpus (460.860 URL's) — so they run **on the event loop** and every
  other request queues behind them. Surfaced when a smoke test of all five made the
  dashboard unresponsive for minutes (`ss -lnt` showed Recv-Q 11 on the listen
  socket) and left the process holding 3,8 GB. One user clicking the new Export
  dropdown's "Combined" reproduces it. Fix is small: drop `async` (FastAPI then uses
  the threadpool) or wrap the work in `run_in_executor`, as
  `seo_stats_router.dashboard` already does. Consider also streaming the workbook or
  capping the row count. See LEARNINGS "`async def` op een full-corpus export".

### Auto-Redirects (rurl_optimizer_v2)
- [ ] **Live subcat probe before accepting a `[maincat]` cross-subcat rescue** (logged 2026-07-21). The Refresh-facets button + auto-refresh (LEARNINGS 2026-07-21) fix *staleness* of `facets.csv`, but not the underlying **Search-API drop**: a rebuild only captures what the bare per-subcat Search-API call returns, and that call non-deterministically omits facets/whole subcats (verified: subcat `389409` had **0 rows** in the snapshot even though its `/c/merk~4874240` is live). So pass-1 `filter_by_subcategory` finds nothing in the source subcat and the engine falls through to the maincat-wide rescue, emitting a `[maincat]` redirect to a *different* subcat (often thin — the Riviera Maison target had count=1). Durable fix: before accepting a `[maincat]` result, **live-probe the source subcat** (`facet_probe.py` already has `_subcat_keyword_facet`/`_fetch_subcat_facets`) for the matched facet value; if it exists there, prefer the same-subcat `/c/...` target over the cross-subcat one. Additive, one throttled call per `[maincat]` candidate. Could also log/flag `[maincat]` cross-subcat rescues for review. See LEARNINGS "Auto-Redirects — a 'cross-subcat' redirect traced to a stale `facets.csv`…".

### DMA Exclusions
- [ ] **OOS residual: stale crawl-OOS matches with no contradicting stock signal** (logged 2026-06-29). After the `is_cheapest_offer` + stale-crawl guards (LEARNINGS 2026-06-29), a flagged headline offer can still be a false positive when the Google AIU crawl-OOS verdict is days stale but NOTHING contradicts it — `beslist_served=True`, `feed_stock=null`, and beslist's index has no stock for that shop (e.g. Douglas.nl `0038097025002`). These read as `match`/excludable and rely on operator spot-check. Crawl-age (`google_last_update`) was rejected as an auto-discriminator (genuine matches 2-3d, ~half the worklist 4-6d → any threshold guts coverage). Options if it becomes painful: (a) an **independent live stock check** (fetch PLP/offer per match before allowing exclude — bigger build, slower scans, live-source reliability uncertain); (b) a **caution column** surfacing `google_last_update` age + feed/served/ES-stock signals on match rows to prioritise manual review (low-risk, non-suppressing — user leaned this way before picking the same-shop ES guard). Either is additive on top of `_oos_verdict`. See LEARNINGS "OOS headline verdict moved from ES `bestOffer`". **UPDATE 2026-06-29:** option (b) shipped (bcc14bb/d8cae1b) — `stale_crawl` flag (crawl ≥ `CRAWL_STALE_DAYS`=3), amber "⚠ crawl Nd" badge, de-selected from Select-all but still individually excludable, "hide stale crawl" filter. The auto-detect residual (option a, live check) is still open but lower priority now that these are visibly flagged + not bulk-excludable. **UPDATE 2026-07-01 — OBSOLETE:** the OOS monitor's new `GET /overrides/exclude-eans` list guarantees freshness (only EANs confirmed OOS within ~2 days appear), so stale-crawl matches no longer reach the tool at all. The entire client-side staleness/verdict layer (`stale_crawl`, `_crawl_age_days`, `CRAWL_STALE_DAYS`, `_oos_verdict`) was removed (commit `c8f5a9e`). Closing. See LEARNINGS "OOS flow simplified to trust the monitor's `exclude-eans` list (2026-07-01)".
- [x] **Ask the OOS monitor owner for a bulk `is_cheapest_offer` endpoint** (logged 2026-06-29, **SHIPPED 2026-06-30**). The scan enriched each live-in-DMA EAN with one `GET /api/v1/overrides?q=<ean>` round-trip; that enrichment was the dominant cost of a COLD scan (~9-10 min for a full scan) and is **server-bound** — raising client concurrency 16→32 gave no speedup (0.255 vs 0.241 s/EAN), so the only real fix was a bulk endpoint. Requested shape: `POST /api/v1/overrides/by-eans {country, state, eans:[...]}` → per-EAN rows (`is_cheapest_offer, ean_offer_count, beslist_served, feed_stock, google_last_update, shop_name`), uncapped and regardless of served-state. **DONE:** the monitor owner (Bram) built it near-exactly — `POST /api/v1/overrides/by-eans` (≤1000 EANs/call → 422 over, one headline-collapsed row per EAN, uncapped, keeps `beslist_served=False` rows). Integrated in `d772355`: the per-EAN `q=` fan-out → chunked bulk fetch (~2350 round-trips → 3 calls; 3 EANs cold 0.09s). He also raised the `/oos-products` cap 2000→20000. See LEARNINGS "OOS bulk /by-eans migration (2026-06-30)".
- [ ] **General serving-leaf walker for allow-list / store-format category trees** (logged 2026-06-25). The exclusion tool's category targeting only handles **block-list** trees (biddable CL3-OTHERS). **Allow-list** trees — the `store_`-format, multi-ad-group campaigns like `PLA/Sport & outdoor store_b` where CL3-OTHERS is NEGATIVE and specific shops are the included positive leaves — are currently **skipped** (safe, but the category portion of those exclusions is not applied; bestsellers+APlus still are). To cover them, replace the per-family `_leaf_for_category`/`_leaf_for_aplus` shortcuts with a general walker: descend the tree following the product's matched custom attributes (CL0/CL1/CL3 from `shopping_performance_view`, captured per ad group), stop at the biddable UNIT the product actually serves under, then subdivide THAT on item_id (reusing the existing convert/append + prune-on-enable logic + the `_ad_group_cpc` bid fallback). Needs careful multi-structure round-trip verification before live use. Matters for OOS bulk-exclusion coverage (many OOS products live in `store_`-format campaigns). See LEARNINGS "OOS feed integration + allow-list tree fix (2026-06-25)".

### GSD Campaigns (Low-Linkage)
- [x] **The Activity Log is not a complete record — entries are written client-side after the run** (logged 2026-07-28, **DONE same day**). `log_run_activity()` now writes the entry inside `start_ll_run` / `start_ll_apply`, before progress flips to `done`, so a run whose tab closes is still recorded and the frontend only renders. The six client-side `logActivity('LL …')` calls are gone. Still client-side: the `Reset` entries, which follow a synchronous `POST /undo` (much smaller window) and share their code path with the GSD reset — worth moving too if the log should be fully backend-owned. See LEARNINGS "Activity-Log-write naar de backend".
- [x] **Attach the undo payload when logging an LL run live, so recent runs get a Reset button** (logged 2026-07-28, **DONE same day** — `llUndoFrom()` at both LL call sites, `undo_ll_run()` reversing through `apply_selected` so the label is maintained, routing in `POST /undo` so an older frontend is covered too, and `backfill_ll_undo()` for the 5 existing rows, pending the backend deploy. See LEARNINGS "LL-undo gebouwd" and TASKS). Every LL run since the last `backfill_activity_from_ll()` (i.e. from 22 Jul on) shows an empty Reset cell in the Activity Log, because `renderLog()` only draws the button when `undo.created.length + undo.paused.length > 0` and the six LL `logActivity()` call sites never pass the `extra` argument that carries it — only the GSD `Run Script` site (frontend ~line 2110) does. So no LL run has ever logged its own undo; every Reset button visible in the log was manufactured by the backfill from `pa.jvs_gsd_ll_campaigns`. Fix: pass `{ undo }` from the run result, which already has `customer_id` + `campaign_id` + `campaign_name` per campaign in `data.paused` / `data.enabled`, applying the same inversion the backfill uses (undo of *paused* = re-enable → `{created: [], paused: camps}`; undo of *enabled* = pause → `{created: camps, paused: []}`). **Re-running the backfill is NOT the fix** for the 5 existing rows: entry_id schemes differ (`backfill-{action}-{run_time}` vs a browser `crypto.randomUUID()`), so `ON CONFLICT (entry_id)` never matches and you get duplicate entries beside them instead of a filled undo — those 5 need a targeted UPDATE joining `pa.jvs_gsd_ll_campaigns` on the run's time window. Consider also giving LL entries the reconstruct-from-change-history fallback that `Run Script` has. See LEARNINGS "Alleen BACKFILLED activity-entries hebben een Reset-knop".
- [x] **Confirm `EXCEL_DIR` keeps its earlier files, else the snapshot backfill is a no-op** (logged 2026-07-28, **ANSWERED same day**). It does keep dated files — prod's `gsd_shops_nl_be_2026-07-28_.xlsx` naming means one file per day survives, and `GET /ll/excel-dates` on win-htz-006 returned the full 7-day window (22-28 Jul) straight after deploy, so `backfill_excel_snapshots()` did its job and the Date picker needed no week-long warm-up. No change needed to the scheduled script.
- [ ] **No auth on the real-mutation endpoints `/ll/run` + `/ll/apply`** (logged 2026-07-22). Both endpoints (`backend/gsd_campaigns_router.py`) start real Pause/Enable mutations on GSD campaigns with **no authentication** — `/ll/run` even defaults `dry_run=False`. Anyone who can reach the dashboard URL (`https://win-htz-006.colo.beslist.net:3003/static/gsd-campaigns.html`) can trigger real campaign mutations. Surfaced by the 2026-07-22 mystery-run investigation: the automatic 09:50 leak was closed (zombie APScheduler killed + scheduler→load-only), but the noon run turned out to be a **human** on the shared office/VPN egress IP `94.142.210.226` clicking Apply — which nothing prevents. Options: (a) add auth (basic/SSO/shared token) in front of the GSD dashboard + endpoints; (b) require an explicit confirm/2-step on `/ll/apply`; (c) keep the **kill switch** ON on prod by default (it was OFF on 2026-07-22) and toggle off only for a deliberate run. Interim: kill switch already exists (`GSD_LL_KILL_SWITCH` / `POST /ll/kill-switch`). See `GSD_LL_MYSTERY_RUN.md` → RESOLUTION and LEARNINGS "GSD LL mystery run resolved (2026-07-22)".


### GSD Tag Toppers (dashboard-tool)
- [ ] **Twee runs staan open** (logged 2026-08-07). (a) `add_only_kandidaten_2026-08-07.xlsx`
  opnieuw draaien voor de 61 campagnes die faalden op het Merchant Center id — dat is nu gefixt.
  (b) `tag_toppers_fix_kandidaten.xlsx` draaien om de 48k historische uitsluitingsgaten te
  dichten. Beide add-only, dus wat er staat blijft staan; volgorde maakt niet uit. Draai eerst
  Preview voor het volume.
- [ ] **De audit periodiek maken** (logged 2026-08-07). `audit_tt.py` beantwoordt "staat alles
  wat een tag_toppers-campagne target ook uitgesloten bij de zusters?" in 9 minuten, read-only.
  Nu een scratchpad-script; als knop of maandelijkse job in de tool zou het de 180 campagnes
  waar de uitsluiting nóóit gedraaid heeft eerder hebben gevonden. Neem `build_fix_excel.py`
  mee — dat zet de uitkomst direct om in een uploadbare kandidatenlijst.
- [ ] **De telcel-controle kijkt alleen naar kolom E** (logged 2026-08-07). `parse_workbook`
  vergelijkt `number_of_productids` met het aantal gevonden ids, maar leest die telling uit
  `raw[4]`. Bij brede rijen — waar de ids kolom D t/m N vullen en de telling erachteraan
  schuift — is dat een id-string en wordt de check stilzwijgend overgeslagen. Juist bij die
  rijen ving hij eerder een ontbrekende cel af (967 waar 2096 stond). Beter: de laatste
  numerieke cel van de rij als telling nemen, ongeacht de kolom.
- [ ] **Weesbudgetten voorkomen in plaats van opruimen** (logged 2026-08-07). Het budget wordt
  vóór de campagne aangemaakt (dat moet, een campagne vereist een budget), dus elke mislukte
  create laat er één achter — 101 opgeruimd op 7 aug. Overweeg het budget op te ruimen in het
  faalpad van `_create_tag_toppers_campaign`.

### GSD tag_toppers
- [ ] **Clean the 7 malformed negative keywords at the SOURCE, then re-sync** (logged 2026-07-28). The tag_toppers negatives sync copies its source campaign verbatim, and Google **accepts** these rather than rejecting them, so they now exist in two places: `"Babywinkel'`, `"Lampenconcurrent.nl'` (2×), `"passasports.nl`, `Weidswonenenslapen.be""` (all stray quotes/apostrophes, BROAD), plus `KUUS.` (trailing dot) and `Beautyplaza.com/nl-be` (full suffixed domain as a keyword). `partial_failure=True` was set specifically so bad keywords would bounce individually — they didn't. Only 7 of 1,122, so low priority, but note the fix has to be **at the source campaign first**: cleaning only the tag_toppers copy leaves the source to re-propagate it on the next sync. Also worth checking whether the generator that writes these still emits them (`gsd_campaigns_service.get_negatives` was hardened on 2026-07-15 — these may be pre-hardening leftovers). See LEARNINGS "tag_toppers-negatives sync".
- [x] **DEELS OPGELOST 2026-08-07.** De matcher-logica is opnieuw gebouwd en staat nu op twee
  plekken in versiebeheer: `GSD_tagtoppers.py` (`find_sibling_campaign` / `sync_negatives_from_sibling`,
  draait bij het aanmaken van een campagne) en `backend/gsd_tag_toppers_service.py` (de bulk-tool).
  Geverifieerd: 881/881 tag_toppers-campagnes vinden hun zuster (NL 507 / BE 351 / DE 23, 0 missers).
  **Wat nog mist t.o.v. het origineel:** de losse sweep over álle bestaande campagnes en het
  audit-script dat bewijst dat een run geland is — de nieuwe code synct alleen bij aanmaken of
  vanuit de Excel-tool, niet als periodieke job over de hele set.
- [ ] **Persist the tag_toppers negatives sync out of the session scratchpad** (logged 2026-07-28). `gads_client.py` + `tag_toppers_sync.py` + `resync_unmatched.py` + `audit_workbook.py` (~300 lines of real logic) only exist in `/tmp/claude-1001/.../fd95554b-.../scratchpad/` and won't survive a reboot — same failure mode as the Q3 tag_toppers rebuild and the SEO-Titles extraction scripts. The sync is **idempotent** (a re-run over already-synced campaigns reports 0 added), so it works as a periodic job: `tag_toppers_sync.py --apply --include-paused` then `resync_unmatched.py --apply --allow-paused-source --strip-suffix`. `scripts_def/` per the `/newscript` convention would fit. **When moving it, take the audit script too** — it's what caught that campaign names aren't unique, and it's the only thing that proves a run landed. Consider whether this belongs as a button in the GSD Campaigns tool rather than a standalone script, since `gsd_campaigns_service` already owns negative-keyword generation.

### UI consistency
- [ ] **Roll the "Done" banner out to the other tools that still end a run in a grey alert** (logged 2026-07-28). `UI_BLUEPRINT.md` now has a "Done banner" section and `.alert-done-yellow` is shared in `css/style.css`, but only DMA Exclusions and SEO titles use it. Candidates found while grepping: `unique-titles.html` (four `alert alert-success` end states, plus a progress bar whose text becomes `Done: N processed, N failed` while the bar stays put), `canonical.html` (`Done — N succeeded, N failed` in an `alert-success`), `keyword-planner.html`, and `gsd-campaigns.html` (its own hand-rolled `<div class="fw-bold mb-2">Done</div>` variants — the run/LL bars are the origin of the progress pattern, so worth aligning). All of these render **grey**, because `style.css` flattens `.alert-success`/`.alert-info` to `--color-section`. Mechanical per page: copy `showDoneBanner`/`hideDoneBanner` from `seo-titles.html`, swap the alert, hide the bar on finish, and add the dismissal guard if a status poll keeps running. Low risk, frontend-only, no restart.

### SEO Titles
- [ ] **`q_newurls.sql` + de kandidaat-extractie uit `/tmp` naar `scripts/` halen** (logged 2026-07-27). De 2-maands `/c/`-visit-export en de loader die daaruit SEO-Titles-kandidaten bouwt staan alleen in sessie-scratchpads (`/tmp/claude-1001/.../6d3ab396/` en `.../0a8399d7/`) en overleven geen reboot. Waard om te bewaren: `q_newurls.sql` (canonicalisatie in SQL die `pa.canonicalize_url` spiegelt, `/c/`-predicaten op het gestripte pad), `extract_candidates.py` (hergebruikt `parse_url`/`canon_key`/`_resolve_cat`/`load_existing_combos`, dus per definitie in sync met de tool) en `build_blueprints.py` (dry-run/`--commit`, asserts op key-pariteit en `MAX_TITLE_LEN`). **Bij het overzetten de source_url-fix meenemen**: prefix `https://www.beslist.nl` vóór het wegschrijven, want de query levert host-loze paden en het frontend gate't de facet-link op `/^https?:/i`. Overweeg meteen of dit een knop in de Generators-tool moet worden i.p.v. een los script — `fetch_top_urls()` doet al bijna hetzelfde maar met een top-N i.p.v. een visit-drempel over het volledige 2-maands venster.
- [ ] **Beslissen wat er met de 4.745 single-visit combo's gebeurt** (logged 2026-07-27). Van de 7.450 nieuwe kandidaten heeft **64% precies 1 visit in twee maanden**; samen zijn alle nieuwe combo's maar 16.115 visits (~0,1% van het SEO-volume). De visits≥2-set (2.806) is gebouwd; de staart niet. Vraag: is een blueprint voor een combo met 1 visit/2mnd de AI-generatie- en push-kosten waard, of moet er een permanente drempel in de Generators-tool komen? Nu is de default een top-N op visits, wat de vraag omzeilt maar niet beantwoordt.

### SEO-omzet / marketplaces
- [ ] **Uitzoeken wat er op 10 maart 2026 met bol.com Plaza gebeurde** (logged 2026-07-27, **hoogste waarde-item uit de SEO-analyse**). Outclicks vielen in één dag van ~20.000 naar ~7.000 (9 mrt 20.380 → 12 mrt 6.006) en zijn daar sindsdien gebleven. Kost ~**€10.300 per week** aan onze omzet op alleen SEO (bol NL+BE, −58,6%) en ~€237k/week aan shop-GMV. Het is een **stap, geen trend**, dus vermoedelijk terug te draaien. **Niet SEO-specifiek** — raakt elk kanaal (18d voor/na: SEO −72%, SEA −78%, Overig −70%, GSAAS −74%, AI −80%), dus de vraag hoort bij wie de bol.com-feed/het account beheert, niet bij SEO. Twee concrete ingangen: (a) waarom houdt **DMA paid met −31%** zoveel beter stand dan SEO met −72%? Dat suggereert dat niet simpelweg alle producten wegvielen maar dat de resterende set anders over kanalen verdeeld is; (b) check feed-volume / aantal aangeboden offers / ranking-positie van bol.com-offers voor en na 10 maart. Let op: Amazon-clicks **verdubbelden** exact op dezelfde dag — mogelijk één bewuste switch in plaats van twee losse gebeurtenissen. Zie LEARNINGS "SEO-omzet loopt vanaf juli 2026 achter op vorig jaar".
- [ ] **Valideren of de CPR-repricing van 8 juli doet wat bedoeld was** (logged 2026-07-27). Sinds de switch van vaste `cpa_cpc` €0,12 naar per-shop ROAS-pricing: SEO-visits/dag **−0,4%** maar OPB **−14,7%** en omzet/dag −15,1%. Op deepest-cat niveau is de spreiding extreem bij vrijwel stabiele visits — omhoog: Rolgordijnen OPB 120 → **1.412**, Jaloezieën 92 → 1.177, Gereedschapswagens 55 → 1.048, Jacuzzi's 133 → 855; omlaag: Vaatwassers 466 → **14**, Dressoirs 737 → 89, Laptops 383 → 77, Broeken 215 → 34, Autoreinigers 1.268 → 123 (bij visits **+7%**). Test: zet de OPB-instorters naast hun nieuwe per-shop ROAS-target om te bepalen of dit bewuste repricing is of een fout in de nieuwe pricing. Netto kost het ~€7k/week aan OPB op SEO. Platform-breed, dus niet alleen SEO.

### GSAAS / CSS Centre
- [ ] **`bt.search_console` mangles URLs whose first query param is `aff_id`** (logged 2026-07-27). The ETL strips `?aff_id=<digits>` including the `?` and leaves the following `&` orphaned, so `beslist.nl/?aff_id=734&utm_source=gsaas&…` is stored as `beslist.nl/&utm_source=gsaas&…` (a second variant keeps un-decoded `&amp;` entities). Positional, not global: the one June-2026 URL that kept its aff_id has it after an `&` (`…/p/klussen/…?productId=…&aff_id=893`). `datamart.dim_visit` performs the same strip but **re-forms the `?` correctly**, so this is a reporting defect only — live clicks are unaffected. Impact: any GSC-keyed join/report on those URLs hangs off a URL that doesn't exist (the correct `?utm` form shows **7 impressions** for all of 2026), and the CSS provider link is invisible under its real URL. The populating job was **not found in this repo** — locate it (n8n? a Redshift ETL?) and fix the substitution to emit `?` when it removes the leading param. Low urgency (nothing user-facing breaks) but it will keep confusing anyone auditing GSAAS URLs. See LEARNINGS "`bt.search_console` has synthetic placeholder keywords + mangles `?aff_id=` URLs (2026-07-27)".
- [ ] **CSS Centre hardcodes `aff_id=734`, so non-NL domains attribute to the NL affiliate** (logged 2026-07-27). CSS account **140784594** ("Links to your business") pins `aff_id=734` in both the Homepage URL and the Product search URL. In Redshift, aff 734 consequently carries **beslist.nl (domain 1) AND shopcaddy.de (domain 12)** — German CSS badge traffic lands inside the affiliate every report treats as NL (aff 750 = beslist.be is clean). Long-standing (≥ Jan 2025), small volume (~503 visits Jul 1–26 vs 42,148 NL) but it makes aff_id→domain non-1:1, which is a trap for any per-market split keyed on aff_id. Action: check the other CSS accounts' "Links to your business" fields (DE especially) and give each market its own aff_id. **CSS Centre change, not a repo change** — and per the note below, the dashboards intentionally don't split by domain anyway, so this is about correct attribution rather than fixing a reported number. See LEARNINGS "GSAAS channel — no Beslist-owned feed…".
- **NOT a bug — do not "fix":** `seo_stats_service.py` and `performance_standup_service.py` report SEO / DMA organic / GSAAS **all-domain** (NL+BE+DE), with no `dv.domain` filter. Joep decided on 2026-07-27 to keep it: the standup Excel's ~2,800 rows of history and its `YOY` sheet were written by this code and are all-domain (verified day-for-day), so filtering would step-change a live series. NL-only impact if it is ever wanted: SEO −18.1%, DMA organic −15.7%, GSAAS −10.2%.


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

- **Investigate Kasten (Meubels) SEO ranking decline** (flagged 2026-06-22). During the WoW SEO visit analysis (week 2026-06-14→06-20 vs 06-07→06-13, NL bot-filtered `fct_visits`), the **Meubels → Kasten** subcategory lost **−2,468 visits (9,463 → 6,995, −26%)** — ~13% of the ~19,026 total weekly SEO visit drop, one of the larger single contributors (after Woonaccessoires −7,329 and Klussen −4,986). GSC (`bt.search_console`, `country='nld'`) shows the **whole Kasten URL cluster slipped ~0.5 position uniformly** (impr-weighted avg 3.60 → 4.10) across **all** page types together — R-urls 117,588→76,736 (−35%), C-urls 107,309→75,334 (−30%), Browse/category 58,036→42,597 (−27%) — while rank dropped in lockstep. That uniformity points to a **category-/site-level cause** (Google core update reshuffle, competitor climbing, or a sitewide signal change on the Kasten pages), NOT a template-specific bug. **Caveats learned:** (1) huge impression losses on category + IKEA-branded head-term pages are near-zero-CTR (~0.25% browse pages, ~1.6% overall) so they barely move visits — IKEA queries lost −24,641 impr but only −365 clicks; rank by **click/visit loss, not impressions**. (2) visit loss is broad long-tail erosion — worst single URL only −79 visits (the IKEA Malm ladekast `/r/` page), top 12 URLs ≈ only 10% of the total. **To dive in later:** check whether the rank drop is a Google update (timing vs known updates) vs a specific competitor overtaking on high-volume cabinet terms ("kledingkast", "ladekast", "tv meubel", "dressoir"); pull keyword-level daily position trend for the Kasten hub pages; verify nothing changed on the category-page templates/feed/availability. Query path + the search_console `clean_url` gotcha (use `type_url`/raw `url`, never `clean_url` — it collapses /c/ and /r/ to the base category URL) are in the assistant's memory (`redshift_real_visits_query`, `search_console_clean_url_gotcha`). **UPDATE 2026-07-08 (month-scale follow-up, see LEARNINGS "June-vs-May 2026 non-PLP ranking decline"):** confirmed a **real regression, MOBILE-specific** (desktop improved), cluster-wide across **Tuinartikelen/Meubels/Woonaccessoires/Klussen** — NOT Kasten-specific (Kasten weighted pos ~flat 3.97→4.00 at month scale). Non-PLP mobile ranking slipped worst on Browse ("cat-url") + R-url; C-url held on clicks (impression cushion); PLP up on desktop only. Retailer-brand nav queries (action/ikea/jysk/gamma/lidl) dropped hardest (−21.5%). cat-url losses are impression/coverage-driven (flat/better rank), R-url losses are rank-driven. Remaining diagnosis (keyword-level mobile trends, mobile CWV/rendering on /c/+browse templates, Google-update timing) still open.

## Fridged / Parked Work
_Work that exists in the codebase but is intentionally NOT wired into production. Pick up later._

- **Kopteksten v3 — wire per-maincat informational prompts to production (DECISION PENDING)** (staged 2026-07-01, user reviewing output first). Built + benchmarked; see LEARNINGS "Kopteksten v3". `dm-tools/backend/gpt_service_v3.py` = `generate_product_content_v3(h1, products, maincat)` using per-maincat prompts in `backend/data/kopteksten_maincat_prompts_v3.json` (+ normalized length footer + its own v3 user prompt that lifts v1's single-alinea/150-word caps). NOT wired (`main.py` still uses v1), NOT committed. Benchmark `scripts/koptekst_v3_comparison.py` → `Downloads\claude\koptekst_v1_vs_v3_2026-07-01.xlsx` (v3 209 vs v1 112 words, 100% vs 0% multi-paragraph). Deliverable docs: `kopteksten_informational_prompts_2026-07-01.md` + `..._per_maincat_2026-07-01.json`. **To activate**: resolve `main_cat_name` for the URL (category_lookup / deepest_category→maincat) and route through `generate_product_content_v3` behind an env/query toggle; confirm content_top renders multiple paragraphs (user says yes). **Open cleanups if pursued**: refactor 31 full prompts → 1 shared base + 31 content-modules (~9% overlap now = 31 boilerplate copies); optional deterministic filler-word scrub ("ideaal"/"perfect", ~63% in both v1 & v3, model ignores prompt ban). **This effectively supersedes the Koptekst prompt v2 below** (v2's comparison-authority angle is one narrow idea; v3 is the full informational-koopgids rework grounded in ranking-content analysis).

- **Koptekst prompt v2** (parked 2026-05-22; largely SUPERSEDED by Kopteksten v3 above). New prompt lives in `dm-tools/backend/gpt_service_v2.py` (`SYSTEM_MESSAGE_V2`, `generate_product_content_v2`). NOT wired into `backend/main.py` — production still uses v1 in `backend/gpt_service.py`. Only consumer today is the benchmark script `dm-tools/scripts/koptekst_v2_comparison.py` (pulls N random URLs from `pa.kopteksten_content`, regenerates with v2, writes side-by-side Excel + aggregate metrics + both prompts to Downloads). Latest n=20 run: 20 v1 valid, 19 v2 valid (1 zero-product URL), v2 hits comparison-authority claim ~98% vs v1 ~0%, relative links ~91% vs v1 ~6%, opening-cliché rate 0% vs v1 ~94%. **Variation fix shipped**: `COMPARISON_AUTHORITY_PHRASINGS` (12 templates, varied syntax positions + quantifiers — "alle/diverse/uiteenlopende/talloze/breed aanbod/meerdere") picked at random per call in `build_user_prompt_v2`, system prompt now defers to the per-call hint and explicitly bans the "Op Beslist vind je veel aanbieders van ..." cliché which had become the default. n=20 sample showed 9 distinct templates used across 19 kopteksten with 0 cliché hits. **To activate**: swap the v1 import in `backend/main.py` for `generate_product_content_v2`, or add an env/query-param toggle for gradual cutover. Latest benchmark: `/mnt/c/Users/JoepvanSchagen/Downloads/claude/koptekst_v1_vs_v2_n20_variation.xlsx`.

- **R-URL optimizer L4 — optional "palm"→Palmbomen synonym** (parked 2026-06-19). `/r/tuinplanten_winterharde_palm/` lands on Tuinplanten + `s_bomen~Waaierpalm` (valid). Preferred Bomen + `type_boom~Palmbomen` is blocked by `_is_semantic_match` (keyword-at-start rule, by design). Safe route if ever wanted: add `palmbomen`/generic tree-types as explicit `COMPOUND_DECOMPOSITIONS` or synonym entries so "palm" resolves to `type_boom~Palmbomen` in the Bomen context — data only, no rule change. NOT a bug; low priority. See LEARNINGS "R-URL optimizer — 7-fix batch".

---
_Last updated: 2026-06-22_
