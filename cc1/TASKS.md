# TASKS
_Active task tracking. Update when: starting work, completing tasks, finding blockers._

## Current Sprint
_Active tasks for immediate work_

### 2026-08-25 — SEO-diagnose 24 augustus: dagbeeld, kanaal Overig, R-url-daling

Analyse, geen code. Rapport als artifact:
`https://claude.ai/code/artifact/85b61137-d687-4778-bab2-9f0f53429f6d`. Methode en cijfers in
LEARNINGS (zelfde datum), twee entries.

Gedaan:

- [x] SEO dag ma 24-08 vs ma 17-08 (−10,3% bezoeken) én week 18-24 vs 11-17 (+2,6% bezoeken,
      +10,1% omzet, OPB +7,3%). De dagvergelijking is misleidend: 17-08 was de hoogste dag van de
      reeks.
- [x] Kanaal **Overig Kanaal** uitgeplozen: +604% op de dag is scraperverkeer, geen groei. Echt
      NL+BE-verkeer eronder is vlak.
- [x] R-url-daling per categorie (−22,0% over vier maandagen) toegeschreven aan zomer-uitdoving:
      Ventilatoren −93,7%, Parasols −79,8%, Opblaaszwembaden −91,5%. Meubels bleek tuinmeubels.
- [x] GSC-toets om vraaguitval van rankingverlies te scheiden, op volledige weken en in
      NL-scope. Van de zes verdachte categorieën blijven er twee over.
- [x] Longtail-vraag beantwoord: er ís geen top-10 (0,3% van de bruto daling over 218.109 urls).
      Wel één cluster: winkel-/folderzoektermen −13,8%.

Open, in volgorde van urgentie:

- [ ] **Scraper afvangen.** Signatuur staat in LEARNINGS. Zolang dit meetelt in `is_real_visit=1`
      ligt er een vervuilingslaag over elke totaalrapportage — en over kanaal Overig helemaal.
      Vraag bij data/infra of dit een WAF-regel wordt of een correctie op de vlag.
- [ ] **Uitzoeken of `dim_visit.domain` wordt bijgeladen voor 18 t/m 23 augustus.** Zo niet, dan
      moeten de NL/BE-splitsingen in de dashboards over naar `country_code`. Controleer meteen
      welke tools op `domain` filteren — SEO stats doet het niet, maar dat is niet gecheckt voor
      de rest.
- [ ] **Auto's op queryniveau uitpluizen.** Enige categorie met aantoonbaar positieverlies
      (gewogen 6,20 → 6,77, clicks −15,4% bij impressies −6,9%). Welke termen, en aan wie.
- [ ] **Fietsen-SERP's met de hand bekijken.** Impressies +1,2%, positie stil op 5,96 → 5,97, en
      tóch −9,4% bezoeken en CTR 1,11 → 1,04. Vraag noch ranking verklaart het; verdenking ligt bij
      een AI-overview of een uitgebreider shopping-blok. Alleen te zien door te kijken.
- [ ] **Drie R-urls die naar exact nul vielen nakijken**: `beslist.be/.../r/toolstation/`,
      `/r/verduisterend/` (Sport & outdoor) en `/r/lasbril/c/t_lasbescherming~7204157`. Van 26-41
      bezoeken naar 0 is zelden vraaguitval — eerder uit de index, een redirect, of geen producten.
      Klein, maar het enige uit de longtail met een concrete oorzaak.

Bewust **geen** taak: jagen op individuele R-urls. De longtail is 218.109 urls diep en de bruto
daling wordt week-op-week vrijwel volledig gecompenseerd door een even grote bruto stijging.
Alleen clusters dragen genoeg volume.

### 2026-08-25 — Bot Hits: periodepresets in het Filters-blok + een echte laadstaat

Gedaan (commits `98e94ad`, `5b41e51`):

- [x] Presetgroep 7d/14d/30d/90d (`btn-group btn-group-sm`), eerst onder het datumvak, daarna op
      verzoek als eigen kolom "Snelkeuze" tussen Periode en Domein — Periode van `col-md-3` naar
      `col-md-auto` om de rij te laten passen, Toepassen van `mt-3` naar `mt-4` omdat URL-type nu
      verder naar rechts eindigt.
- [x] Anker op `coverage.last_day` i.p.v. gisteren, start geknipt op `first_day`; `boot()` gebruikt
      nu dezelfde helper en niet meer zijn eigen (tijdzone-mengende) rekenwerk.
- [x] Klik filtert direct (deed het al) + generation-token, zodat vier klikken achter elkaar geen
      race meer zijn.
- [x] Actieve preset blijft opgelicht, gestuurd door de datumvelden; handmatig bereik = niets actief.
- [x] Laadstaat volgens UI_BLUEPRINT: shimmer over de drie Overzicht-grafieken, skeletonrijen in de
      bot-familietabel, de "laden…"-tekstrij van de URL's-tab ook een skeleton, plus de canonieke
      inline "Bezig met laden…" in de filterkaart.
- [x] `.btn-preset` de `min-width: 3rem` die het blueprint voorschrijft.

Die drie open punten zijn diezelfde dag afgehandeld (`51afaef`, `13beeb8`, `3ebb8ea`) — twee
ervan anders dan hier eerst stond:

- [x] **`/top-urls`**: eerst gemeten in plaats van geoptimaliseerd. Niet de limiet is het probleem
      (250 en 500 zijn even snel) maar het bereik: 30 dagen 4,4s, 90 dagen 10,0s, 192 dagen 89,8s.
      `work_mem` verhogen doet vrijwel niets, de sprong naar 90s is een planner-flip, en de
      `count(DISTINCT)` weghalen levert voor de gebruikte bereiken niets op — cijfers en `EXPLAIN`
      in LEARNINGS (zelfde datum). Daarom een grens: de tab kijkt max 90 dagen terug en meldt dat
      boven de tabel. **Een rollup-tabel blijft de enige weg naar sub-seconde en is bewust geen
      taak**: die maakt het bekende been niet-uitputtend, en dat is de eigenschap waarop deze tab
      betrouwbaar is. Pas oppakken als je die lijst structureel over lange reeksen wil lezen.
- [x] **De wrappende filterrij**: opgelost met `order-md-last order-xl-0` op de Snelkeuze-kolom,
      dus de kleinste kolom zakt in plaats van de checkboxlijst. **Het voorstel dat hier eerst
      stond (`col-md-12 col-xl-10` op de wrapper) was fout en moet niemand meer oppakken**: dat is
      exact de eigen-paginabreedte die op 2026-08-14 is opgeheven, waarbij Bot Hits één van de vijf
      omgezette pagina's was. Zie UI_BLUEPRINT, "Fixed width wrapper — ÉÉN breedte".
- [x] **`seo-stats.html`** heeft nu ook `min-width: 3rem` op `.btn-preset`; daarmee is er geen
      pagina meer zonder.

### 2026-08-24 — Audit Auto-Redirects: fase 0-3 (V61)

Vijf parallelle reviews over `rurl_optimizer_v2` + de service, daarna zelf geverifieerd. Volledige
bevindingenlijst en de gefaseerde aanpak staan in de sessie; mechaniek en de twee foute
verdachten in LEARNINGS (zelfde datum). Poorten: 150 echte R-URL's met productie-vlaggen, een
geschudde `facets.csv`, en per fix de populatie die de bug raakt.

**Fase 0 — gemeten op `rurl_processed` (41.472 URL's)**
- [x] 352 rijen met `match_type='cross_maincat_blocked'` en lege `redirect_url`.
- [x] 1.343 van 24.155 facet-fragmenten (5,6%) staan niet in de catalogus; 114 daarvan op het
      subcat-append-pad. Live steekproef van 12: 6x 0 producten, 4x HTTP 400 — echt dood.

**Fase 1 — gedrag-behoudend (47,1s -> 14,4s op 150 URL's, 3,3x)**
- [x] Memo per worker op `filter_by_subcategory` / `filter_by_main_category` / `get_facet_values`
      (82 -> 53 ms koud, ~0 warm), `regex=False`, `to_dict("records")` -> `zip`.
- [x] `match_subcategory_name`: `iterrows()` eruit + memo (71,8 -> 3,7 ms koud, ~0 warm).
- [x] Identiteit bewezen door oud en nieuw naast elkaar te laden: 0 mismatches op 60 subcats x2 +
      8 maincats + het exclude-pad, en 0 op 1.445 `match_subcategory_name`-aanroepen.
- [x] Checkpoint schrijft append met vaste kolomvolgorde; caches atomair (`.tmp` + `os.replace`);
      `facet_probe` serveert geen gecachete `mode:"error"` meer en hergebruikt `_SESSION`;
      regex-patronen naar module-scope; `_facet_value_name_lookup` via `col_mapping` + warning.
- [x] Dode code weg (elk met een tree-brede grep bevestigd): `_measurements_match`,
      `_normalize_measurement`, `filter_by_subcategory_name`, `get_type_facets_only`,
      `get_unique_facet_names`, `load_r_urls`, `save_to_cache`, `_derive_multi_facets`.
      `require_type_for_merk` en de twee lege `pass`-takken kregen een eerlijke comment.

**Fase 2 — determinisme: identiek op alle 35 kolommen met een geschudde cache**
- [x] Tien tie-breaks gesloten (dedup naar categoriediepte 3x, dimensiepas, token-coverage,
      axis-dedup, `sorted_results`, drie naam-dedups, `max(type_matches)`, qualifier-`next`,
      de leftover-collector, `_maybe_promote_to_specific_subcat`).
- [x] Q3 cross-axis-dedup: bij gelijke waardetekst wint de as die de query noemt
      (`vaatwasser diepte 50 cm` bleef anders op `breedte_vaatw` hangen).

**Fase 3 — gedrag-veranderend, elk apart gemeten**
- [x] **#1** laatste redmiddel aan het EIND van de functie (247/352 gered, 0 verloren, tier C).
- [x] **#2** geen facet aanplakken dat niet op de doelpagina bestaat (dode fragmenten 66 -> 16 op
      de 114-rij-populatie; tiers zakken A 14->9 / B 29->25, en dat is de bedoeling).
- [x] **#10** Fix E slaat over bij een facet-gepinde bron-URL. **#11** idem voor de
      cross-maincat-fallback in de V28-afwijzing.
- [x] **#13** merk dat ook winkelnaam is (vidaXL/IKEA/Hema, 396 waarden) niet meer weggegooid als
      de query het letterlijk noemt; `wc papier` -> `Paper Dreams` blijft geblokkeerd.
- [x] **#14** minimumlengte op `_strip_plural_suffix` + echte suffix-strip in
      `_is_bare_category_noun`. Bleek onschuldig aan de airfryer-regressie; blijft erin.
- [ ] **Cascade-poorten op `_ok`** — 8 extra reddingen en 36 tier-A/B, maar brak 4 van 150. Kan
      pas als de V26-blokkade meeschuift met de cascade. Eigen meting nodig.
- [x] **16 resterende dode fragmenten**: opgelost met één centrale prune vlak vóór de output
      i.p.v. per pad — 16 -> **0** op de getroffen populatie, tiers onveranderd, 1 wijziging op de
      150-poort (`boxspring met tv lift` verliest een facet met live 0 producten). De uitzondering
      geldt alleen voor het bronfacet én alleen op de origin-pagina; de twee lossere varianten
      lieten respectievelijk 1 en 5 dode fragmenten staan.
- [x] **2 rijen die hun redirect verloren**: bestaan niet meer — nagemeten tegen `df_old`, nul
      rijen gaan van "wel een redirect" naar "geen".
- [x] Dubbele Search-call weg: `_subcat_keyword_facet` en `_fetch_subcat_facets` bouwden een
      identiek verzoek en vuurden allebei. 5 unit-tests in `tests/test_v61_subcat_probe_reuse.py`.
- [x] Taxonomie-BFS: een mislukte node sloeg de hele subboom over. Nu verzamelen + één retry +
      afbreken. Rooktest: 3.575 categorieën (= de cache) in 183s met de 20-QPS-rem.

**Fase 4 — ops (raakt de live service op :8003; 150-poort identiek op alle 35 kolommen)**
- [x] RC4 uit de workers: `prefetch_insubcat_facets` doet het vooraf met één bucket, workers
      draaien `cache_only=True`. Gemeten: 142 pairs vooraf, 0 gewijzigde redirects.
- [x] Run-lifecycle: historie-rij bij runstart (met pid), Tier-A-rijen per chunk naar schijf,
      `start_new_session=True` op alle drie de Popens, historie atomair + kapot bestand opzij.
- [x] `_sweep_stale_tasks` markeert `interrupted` i.p.v. `completed` — en `rurl-optimizer.html`
      kent die status nu, anders pollt de frontend eeuwig door (die lijst is
      `completed|failed|cancelled`).
- [x] Facets-refresh weigert zolang er een run draait (`_a_run_is_active`).
- [x] Rate limiter op `db_loader` (`RURL_FACETS_QPS`, default 20) — **rebuild gaat van ~6 naar
      ~14 min**. En `search_derived._cache_get` had zijn OperationalError-guard om `_connect()`
      heen terwijl sqlite pas lockt bij `execute()`; plus `journal_mode=WAL`.
- [ ] **Nog niet actief op :8003** — uvicorn draait zonder `--reload`, dus dit gaat pas mee bij
      de volgende deploy (kill + relaunch).

### 2026-08-24 — V60: de facetpool was afgekapt op de top-N per facet

Fix 2 uit de V59-sectie hierboven. Mechaniek, de meetopzet en de dunne-staart-cijfers staan in
LEARNINGS (zelfde datum). Nog niet in een echte Tier-A-run gedraaid.

- [x] **Tweede pass in `load_facets`** (`backend/rurl_optimizer_v2/src/db_loader.py`):
      `_search_category_facets(slug, filter_facet, filter_value)` bouwt de call (pass 1 gebruikt
      'm ook), `_reprobe_truncated_facet_values` haalt de paren op die afgekapt kunnen zijn.
      Welke dat zijn wordt uit de data afgeleid: de cap kan alleen het hoogste aantal waarden
      zijn dat een facet ergens haalt. Schakelaar `FACET_VALUE_REPROBE`.
- [x] **`merk` + `winkel` blijven afgekapt** (`REPROBE_SKIP_FACETS`). `winkel` doet niet mee in
      matching, en `merk` van 100 naar duizenden tail-merken per categorie verandert het
      merkmatchen én de omvang van de cache genoeg om een eigen evaluatie te verdienen.
- [x] **Rebuild gedraaid**: 3.543 categorieën in 57s, 11.772 paren in 247s, +91.166 waarden,
      25 fouten (0,2%). 624.884 rijen (was 518.141), totaal 349s. Backup:
      `data/cache/facets.csv.bak-20260824`. `data/` staat in `.gitignore`, dus **de
      productiemachine moet zelf een rebuild draaien** om hier iets aan te hebben.
- [x] **Blast radius schoon gemeten** met een controlebuild (tweede pass uit, zelfde dag):
      998 rijen, 81 via het subcat-append-pad, 3 veranderd — rij 94 en 109
      (`ingr_shamp~Ketoconazol`) en rij 223 (`ruimte~Balkon`). Alle drie winst, geen regressies.
- [x] **Tests**: `tests/test_v60_facet_value_reprobe.py` (7, met een stub voor de Search API);
      hele suite 107 passed.
- [ ] **Beleidsvraag voor Joep: ondergrens op dunne facetwaarden?** 28% van de 91.166 nieuwe
      waarden heeft precies 1 product, de mediaan is 4. `ingr_shamp~Ketoconazol` is live 1
      product tegen 12.748 op de kale Shampoo-categorie. Volgens V56 en
      `feedback_thin_destination_not_a_score_signal` is dat correct gedrag, maar het is wel een
      nieuwe klasse bestemmingen die deze fix ontsluit. Guard bouwen alleen op verzoek.
- [ ] **Rebuild op productie inplannen** en daarna een echte run vergelijken met de vorige.

### 2026-08-24 — SEO titles: dedup op de live store, en een placeholder-bug die IT moet beoordelen

Aanleiding was één cat/facet-vraag over `9001451 / merk~type_plantenbakken`. Mechaniek, de
metingen en het GET-endpoint staan in LEARNINGS (zelfde datum). Code gepusht als `1e01075`,
backend herstart.

- [x] **`!!type_plantenbak!!` gecorrigeerd** naar `!!type_plantenbakken!!` (9001451,
      `merk~type_plantenbakken`). Enige rij van de 539.214 met die typo; staging + productie
      geüpsert, live geverifieerd als `CLP Plantenrekken`.
- [x] **Dedup vraagt nu de store** i.p.v. de juli-snapshot: `load_existing_combos()` →
      `load_local_combos()` (alleen `pa.seo_titles_blueprints`) + `store_has_combos()` op
      `GET /page-titles/{catId}/record?key=`, gecachet in de nieuwe tabel
      `pa.page_titles_api_cache`. Plus `get_store_record()` als losse read.
- [x] **De drie scripts die op `beslist.tblPageTitles` leunden** (die tabel bestaat niet meer):
      `pagetitles_blueprint_from_urls.py`, `pagetitles_blueprint_from_seo_traffic.py` en
      `pagetitles_from_unique.py` vragen nu de store ná de scan (stdlib urllib). De eerste twee
      hebben daardoor geen MySQL meer nodig — ze draaien onder de repo-venv i.p.v.
      `~/.mysql-venv`. `pagetitles_top5_allchannel_combos.py` idem.
- [ ] **Wacht op IT: is "een placeholder vult maar één keer" by design of een bug?** Joep kaart
      het aan. Reproductie:
      `/products/horloge/c/horloge_stijl~23590956~~kleur~5798159~~serie_horloge~10474515` →
      "Shop met 71% korting online!". Repareren zij het, dan hoeven wij niets.
- [ ] **Zo niet: description-template fixen en 84.881 rijen herpushen.** Het tweede `<phrase>`
      in `Zoek je <phrase>? … Shop <phrase> met !!DISCOUNT!! korting online!` vervangen door
      alleen `!!sub_category!!` (of de zin herschrijven zonder herhaling), in
      `backend/seo_titles_service.py` én `scripts/pagetitles_blueprint_from_urls.py`. Is één
      batch-upsert, geen hergeneratie. Eerst een handvol op staging/prod testen en met de GET
      terugverifiëren.
- [ ] **Aan IT vragen wat de canonieke store is.** `beslist.tblPageTitleImport` (41.394 rijen) is
      een dode kopie die een push niet bijwerkt. Zolang dat onduidelijk is, is de GET onze enige
      leesweg — en die kan alleen per combo, geen list-endpoint.
- [ ] **Overweeg `pa.page_titles_existing` uit te faseren.** Nu alleen nog de bron van het
      "existing"-tabblad en van de teller `existing_blueprints` in `get_stats()` (een
      snapshot-rijtelling, geen live totaal). De helft van de rijen is bovendien de
      shifted layout.


### 2026-08-24 — V59: facetwaarden die het categorienoun herhalen zijn nu wél matchbaar

Uit twee rijen van `Downloads\redirects_global_828a73ad_20260820_094234.xlsx`. Mechaniek,
de diagnose-truc voor de afgekapte facetlijst en de blast-radius-methode staan in LEARNINGS
(zelfde datum). Nog niet in een echte Tier-A-run gedraaid.

- [x] **V59 in `_collect_longest_per_axis_from_leftover`** (`backend/rurl_optimizer_v2/
      main_parallel_v2.py`): de tokens van de gematchte subcategorienaam dekken ook facet-tokens,
      mits minstens één token door een echt leftover-token verdiend wordt en met
      `_tokens_equal_strict` op de subcat-kant. `elektrische verwarming badkamer` levert nu
      `…/main_sanitair_559440/c/ruimte_verwarmingen~19257689~~t_verwarming~19254910`
      (2.435 producten live) i.p.v. alleen de Badkamer-facet.
- [x] **`_value_has_no_unclaimed_fragment`-guard**: op het vergevingspad moet élke resterende
      woord-/nummer-run in de facetwaarde door de query genoemd zijn. Houdt `philips airfryer`
      weg bij `productlijn_koken~'Philips airfryer XL'` — `_coverage_tokens` gooit `xl` weg,
      dus zonder guard leek die waarde volledig gedekt.
- [x] **Blast radius op de export**: 998 rijen, 81 via het subcat-append-pad, 1 veranderd
      (rij 222). Zonder de guard 2.
- [x] **Tests**: `backend/rurl_optimizer_v2/tests/test_v59_subcat_noun_coverage.py` (10 tests);
      hele suite 100 passed.
- [x] **Fix 2 — de facetpool is incompleet.** Gedaan als V60, zie de sectie hieronder.
- [x] **Rij 223 opnieuw draaien zodra fix 2 er is**: levert nu
      `…/meubilair_389371_6383260/c/ruimte~4945789` (68 producten live).

### 2026-08-20 — `/r/`-URL's met een slash in de zoekterm kunnen eindelijk redirects krijgen

De canonicalisatielus van 2026-06-30 is door teamsearch gefikst. Mechaniek, de
double-encode-gotcha en de cache-lagen staan in LEARNINGS (zelfde datum) en in auto-memory
`redirect_api_behavior.md`.

- [x] **Testcase live gezet**: `/products/r/wasmachine%2fdroger_kast/` → `…/c/t_badkast~23813977`
      (row **8665289**, country `nl, be`). Origin geverifieerd via de hoofdletter-`%2F`-truc.
      Row 8665288 (letterlijke-slash-vorm) is een dood bijproduct van de decode-gotcha — mag weg.
- [x] **Populatie in kaart**: 3.916 URL's met `%2f` in de term, 23.270 real visits/365d, waarvan
      3.913 nog zonder redirect. Lijst: `Downloads\claude\r_urls_met_slash_365d_20260820.xlsx`.
- [ ] **`post_redirect` escapet `%` niet** (`backend/redirect_tool_service.py`) — `%2f`-URL's uit
      de Redirect-tool worden stil dode rows. `%` → `%25` bij het POSTen, plus een
      preflight-waarschuwing op fromUrls met `%2f`. **Doen vóór de bulk-run.**
- [ ] **Bulk-run**: redirects instellen voor de 3.913 open URL's. Doelen per geval bepalen — de
      slash betekent drie dingen: een of-scheiding (`slaapstoel%2fbed`), een maat of breuk
      (`1%2f2`, `45%2f45`, `28x1_3%2f8`, `13w%2f827`), en soms een dubbel geplakt pad.
      **Rows aanmaken vóórdat iemand de URL opvraagt** — anders zet je zelf een 200 in CloudFront
      voor >1h.
- [ ] **Bij teamsearch**: (a) CloudFront-invalidation op `/products/r/wasmachine%2fdroger_kast/`
      als je de testcase live wil zien zonder de TTL af te wachten; (b) de frontend-bug die
      `/r/`-links met het pad in de zoekterm genereert
      (`…%2fmode_accessoires%2fr%2faction_koffer_handbagage`).

### 2026-08-20 — DMA bidding + exclusions kijken ook naar account 4089798584 (DMA NL 2)

Commit `7f9422f` op `main` (2 services + 2 pagina's). Leerpunten in LEARNINGS (zelfde datum);
het account en de `_label`-asymmetrie staan ook in auto-memory.

- [x] **NL loopt in beide tools op twee accounts** — `3800751597` + `4089798584`. Bidding:
      `COUNTRY_ACCOUNT_IDS`, één pass per account (strategieën/campagnes/metrics/omzet) omdat
      alles name-keyed is. Exclusions: `ACCOUNTS` als lijst, `lookup`/`resolve_targets`/
      `oos_scan` lopen over alle accounts. BE ongewijzigd.
- [x] **`_label` overslaan in bidding, alleen in 4089798584** (`ACCOUNT_CAMPAIGN_SKIP`). Per
      account en niet globaal: het oude account heeft een ENABLED `PLA/Onbekend_label` op
      Level 2 die de tool al beheert. Gemeten over de paused set: 1056 `_label` geskipt,
      1056 `_limit` gemapt op L1 670 / L2 152 / L3 234.
- [x] **Exclusions pakt `_limit` én `_label`** (keuze van Joep) via een account-eigen
      categorie-regex. Preview op `nl-nl-gold-8716096010459`: 11 targets, 5 in DMA NL
      (trio + bestsellers + APlus) en 6 in DMA NL 2, geen warnings.
- [x] **Mutaties gaan naar het account van het object.** Bidding leidt het af uit
      `campaign.resource_name` (ook de revert-knop), exclusions bewaart `customer_id` per
      target in de record; oude records zonder dat veld vallen terug op het primaire account.
      Ad-group-lock en de `oos_exclude`-groepering zijn nu per (account, ad group).
- [x] Live schrijftest op de paused `PLA/Dartborden_c_label`: negative in
      `customers/4089798584/...`, `enable` klapte de boom terug naar exact
      `UNIT INDEX3 '' bid 200000`. Testrecord daarna uit `dma_exclusions` verwijderd.
- [x] Frontend: per-account regel onder de statkaarten (DMA Bidding) en een Account-kolom in
      resultaat/export/preview/saved-detail — alleen zichtbaar bij >1 account, dus BE ziet
      er hetzelfde uit als eerst.
- [ ] **Nakijken zodra DMA NL 2 aangaat.** Alle 2112 campagnes staan nu PAUSED en bidding
      filtert op ENABLED, dus die kant is nog niet met echte data gelopen: eerste run in dry
      run bekijken (verwacht ~1056 `_limit`-campagnes op de ladder, nul `_label`).
- [ ] **Bulk-OOS in NL schrijft nu ~2x zoveel criteria** (11 i.p.v. 5 ad groups op het
      testitem). Als een run daardoor te lang duurt: de accounts binnen `_ga_batch_agg` /
      de apply-fase parallel trekken i.p.v. serieel per account.
- [ ] Geen `PLA/APlus` en geen `PLA/Amazon bestsellers` in DMA NL 2 — die branches zijn daar
      een stille no-op. Komen ze er later, dan pakt de tool ze automatisch; geen actie, alleen
      weten.

### 2026-08-19 — Healthscore-console herzien op ~20 punten van Joep

Commit `6dee44b` op `main` (frontend + backend). Leerpunten in LEARNINGS (zelfde datum),
vastgelegde regels in UI_BLUEPRINT §Tables, §Buttons, §Labels, §Stat tiles.

- [x] **Categorieselectie is een cascade van twee zoekbare dropdowns** (maincat → categorie).
      Verving een `<select>` met 3.569 opties naast een los filterveld. Eén component, twee
      keer gebruikt; de knop draagt `form-select` dus hij is niet van een echte select te
      onderscheiden. Typen filtert (AND op woorden, naam + maincat + id), ↑/↓, Enter, Esc.
- [x] **Voortgangsbalk met echte noemer** — één categorie per eenheid, `phase/done/total/what`
      uit `preview()`/`push_run()` via het job-dict. Prep en finish zijn indeterminate zonder
      percentage; prep draagt een `step` omdat daar bij één categorie ~3 van 3,5 minuut zit.
- [x] **De run staat in een eigen kaart** (`#runCard`), zowel na een preview als bij een klik
      in Recent runs. Voorheen onderin de kaart waar je hem startte én onder de tabel.
- [x] **Export- en verwijderselectie in Recent runs.** Beide knoppen zetten dezelfde
      selectiemodus aan en wisselen houdt de vinkjes. Export via `?ids=` naar dezelfde
      CSV-schrijver (geen tweede kolomdefinitie in de frontend), `POST /runs/delete` laat de
      snapshots op schijf staan.
- [x] Push is één oranje knop "Push" met OK/Annuleren i.p.v. uitgetypte REPLACE (de backend
      blijft zijn token eisen); "Alleen de gekozen categorie" eruit.
- [x] Dekkingstabel canoniek gemaakt en verdubbeld: hele run onder de totaaltegels
      (in-set/totalen opgeteld en dán het percentage, zoals `_weighted_cov`) plus per categorie.
- [x] Tegelrijen op één regel, labels in drie palettes, badge-centrering gemeten, alles/niets
      als selectievakje met halve staat, testcats alfabetisch in drie kolommen, dode CSS eruit
      (`.hs-head`, `.chip`, `.hs-table`, `.spinner-inline`).
- [x] `.btn-outline-red.active` toegevoegd aan de gedeelde active-groep in `style.css` — zonder
      die selector was een actieve destructieve knop zwart (Bootstraps eigen `.btn.active`).
- [x] **Outline-knoppen in een kaartkop app-breed transparant** (Joep gaf groen licht dezelfde
      dag). Eén regel in `style.css`: `.card-header :is(…):not(:hover):not(:disabled):not(.active)`.
      De pagina-regel in Healthscore is weggehaald, want een page-`<style>` laadt later en zou de
      gedeelde regel stil overschrijven. Nagemeten in alle vier de staten (rust transparant,
      disabled `#f4f5f9`, active `#f3f0fa`, `.btn-run` blijft gevuld) en visueel gecheckt op
      Bot Hits, SEO stats, Index Checker en Canonicals. Zie UI_BLUEPRINT §Buttons.
- [ ] **`.tool-table td` padding gelijk aan de kop, app-breed.** Nu pagina-regel in Healthscore.
      Waarden staan overal 10px links van hun kop (`th` 6px/14px, `td` 4px van `.table-sm`).
      Bredere cellen verschuiven kolombreedtes op 35 pagina's, dus meten vóór uitrollen.
- [ ] Optioneel, als Joep het tóch wil: previews uit Recent runs filteren. Nu blijven ze staan,
      want een push start altijd vanuit een bestaande preview-run — zonder die rij kun je later
      niet meer pushen. Eén filterregel in `/runs`.
- [ ] Nog steeds open uit de vorige ronde: de tweede tegelrij van een run met één categorie
      herhaalt de totaaltegels. Onschuldig, maar het leest als twee metingen.

### 2026-08-19 — Parfumerie-facetvalues zonder SEO-visits + bug in SEO-facetlinking

Analyse-sessie, geen code in de repo. Volledige uitwerking:
**`cc1/SEO_FACETLINKS_DEPENDENT_FACETS.md`**; kortere versie in LEARNINGS (zelfde datum).

- [x] **Uitdraai opgeleverd**: `Downloads\claude\Parfumerie_facetvalues_zonder_SEO_visits_20260819.xlsx`.
      Populatie = facet value met `seoPriority=true` én cat/facet `seoPriority=true` in maincat
      Parfumerie (29000) = **3.802 values**. Venster 19-02-2025 t/m 18-08-2026, `fct_visits` +
      `dim_visit`, `is_real_visit=1`, alle domeinen. **1.956 (51%) zonder SEO-visit**: tab 1 =
      1.775 zonder visit op álle kanalen, tab 2 = 181 met alleen niet-SEO-traffic (420 visits).
      Derde tab documenteert methode + caveats.
- [x] **Collectie-URL's aangevuld met hun parent-merk** (kolom L resp. U): `type_parfum` werkt
      alleen als `merk~<merkId>~~type_parfum~<id>`. 485 van de 641 Collectie-rijen hebben een
      merk (bronnen: producten, `pa.urls`, bezochte URL's, historische `dim_visit`-URL's,
      GSC-URL's — bron staat per rij in de sheet); 156 niet, want 0 producten én nooit een URL.
- [x] **Bug gevonden en gemeten**: `seoPriority` zet facet values in een `<noscript>`-blok voor
      Googlebot, maar de site leest `isSeoFacet` uit ProductSearch v2 en die staat voor
      **dependent facetten** altijd `false`. Downstream is alles correct
      (`tbl_CS_Cat_Column_Order.seo_prio=1`, slot aanwezig), dus de breuk zit in de projectie /
      indexering van ProductSearch v2. Platformbreed: 3432 (Parfumerie), 3821 (Schoenen),
      5514 (Laptops).
- [ ] **Ticket uitzetten bij eigenaar ProductSearch v2 / de indexer.** Bewijsregels staan in
      `SEO_FACETLINKS_DEPENDENT_FACETS.md` §2 + reproduce-commando's. Niet zelf te fixen: de API
      is read-only (17 endpoints, allemaal GET). #priority:high
- [ ] **Apart, lager**: dependency-registratie (`/api/Facets/{id}/value-dependencies`) staat stil
      sinds de migratie van 2026-01-27 — Armani (234 producten met collectie-waarden) en ARMAF
      BEAUTÉ ontbreken, 41 van de 197 registraties zijn leeg. Geen write-endpoint; enige route is
      een volledige maincat-import (`POST /api/Import/sessions` → review → `/commit`).
- [ ] **Opruimen op basis van de uitdraai**: 1.011 values met 0 producten + 16 die niet meer in de
      zoekindex bestaan kunnen op `seoPriority=false` (GET-merge-PUT met flat body).
- [ ] Optioneel: kolom "laatste echte visit ooit" toevoegen — steekproef gaf één value (V Canto)
      met z'n laatste visit op 2025-02-02, 17 dagen vóór de venstergrens.

### 2026-08-19 — Healthscore als categorie-console + de knoppen door de hele app

Twee commits op `main`: `6961d7f` (style/ui) en `42577e7` (feat/healthscore).

- [x] **De destructieve knop doet nu wat UI_BLUEPRINT al voorschreef.** `.btn-outline-red`
      c.s. stond in rust op een roze rand `#e3b3b3` en vulde bij hover met een bijna-wit
      `#fdf0f0`; nu rand `#d64545` in rust en een volle `#d64545` met witte tekst bij hover.
      Eén regelgroep, dus élke destructieve knop erft het. Geverifieerd door de echte
      `:hover`-regel uit `document.styleSheets` te lezen en op een tweede knop te plakken —
      `--screenshot` kan niet hoveren.
- [x] **39 knoppen op 18 bestanden van inline hexen + `onmouseover` naar de canonieke
      klassen** (mapping-tabel in UI_BLUEPRINT §Buttons). Dit was de oorzaak van Joeps
      melding "de Cancel ziet uit als een oude Remove": zulke knoppen luisteren niet naar
      `style.css`, dus ze bleven achter bij de fix hierboven. `_tool-template.html` was de
      bron van de verspreiding en is als eerste om. `grep -r 'onmouseover="this.style'
      frontend/` is nu leeg; houd dat zo.
- [x] **GSD Budgets' actielabels outlined** + de badge-tekst 1px omhoog (Bootstrap zet
      `line-height:1`, gemeten op 8x-shots; meetrecept in LEARNINGS).
- [x] **Healthscore herbouwd** (heet weer gewoon Healthscore): run per main/deepest
      categorie, de twaalf testcats met dry run, en Recent runs met klikbare rijen die de
      uiteenzetting per run openen. Nieuw `backend/healthscore_runs.py` + `pa.hs2_runs`;
      preview en push zijn twee stappen en de push replayt de opgeslagen payload. Nieuwe
      per-categorie coverage via `seo_visits_by_type()`. Gevalideerd op een wegwerp-uvicorn:
      Douchewanden 90,8% visit cov, Films & Series (maincat) 93,2%.
- [ ] **:8003 herstarten om de nieuwe endpoints te laden.** `/api/healthscore/health` is 200
      (oude router), maar `/test-bucket`, `/categories` en `/runs` geven **404** op live — de
      pagina toont dus "Not Found". Statische bestanden komen van disk, alleen de router
      niet. Bare uvicorn zonder `--reload` → `fuser -k 8003/tcp` + relaunch. Wachtte op een
      Auto-Redirects run die inmiddels geklapt is (zie hieronder). #priority:high
- [x] **`main_parallel_v2.py` klapte na drie uur op `UnboundLocalError: derived`**
      (`8a94e8b`). `derived` wordt alleen gezet binnen `if has_matchable and
      parsed.main_category and parsed.keyword:`, maar het V53-blok staat op
      functieniveau en leest hem in zijn eigen guard. `has_matchable` is False zodra de
      query alleen uit stopwoorden/shopnamen bestaat — **171 van de 20.000 inputrijen
      (0,9%, 40.122 visits)**: 'beste koop consumentenbond', 'de goedkoopste', 'als
      beste getest'. Eén zo'n rij die ook een multi-facet maincat-redirect krijgt is
      genoeg; uit een pool-worker gooit `imap_unordered` hem door en de hele run valt
      om. `derived` krijgt nu een `{}`-fallback. Buurblok 3398 had de guard wél, dus
      vergeten. AST-regressietest erbij, 91 tests groen. Zie LEARNINGS.
- [x] **Checkpoint schreef elke keer alles opnieuw** (zelfde commit): `pd.DataFrame(
      results)` in plaats van `results[last_save_count:]`, dus checkpoint k schreef
      k*5000 rijen — 30.000 rijen voor 14.995 unieke urls, kwadratisch groeiend.
- [ ] **De run van 19 aug is voor 75% te redden, maar het progress-bestand bevat de
      duplicaten van vóór die fix.** Herstart met **hetzelfde `-o`-pad** (de resume-tak
      leest `<output>_progress.csv`), en dedupliceer dat bestand eerst op
      `original_url` — de eindsave concateneert het met de nieuwe batch, dus anders
      lopen de dubbele rijen door naar het resultaat. #priority:medium
- [ ] **Elf knoppen dragen nog `btn-outline-danger`** in plaats van `btn-outline-red` (Bot
      Hits, DMA Exclusions 2x, DMA+, GSD Budgets, Index Checker, Keyword Planner 2x, SEO
      titles 2x, Unique Titles). Ze **renderen goed** — beide namen zitten in dezelfde
      regelgroep — het is puur de naam die niet zegt wat je krijgt. #priority:low
- [ ] **Twee ambers in de app**: GSD Budgets' `.lbl-amber` is `#b45309` (de tint die de DRY
      RUN-badge daar al droeg), GSD Tag Toppers' is `#b26a00`. 15 ΔE, dus zichtbaar
      verschillend. Bewust zo gelaten om DRY RUN niet te verkleuren; wie dit gelijktrekt,
      doet het op beide pagina's. #priority:low

### 2026-08-18 — Auto-Redirects V55-V58: H1-vergelijking, producttelling eruit, merkfacet-guard

Joeps review van `Downloads\redirects_global_f4383643_20260814_112614.xlsx`. Vier commits op
`main` (`9f7463f` + `0a23aa5`), 89 tests groen.

- [x] **V55 — H1 van de C-url tegen die van de R-url** (Joeps idee bij regel 110). Het bestaande
      V26-signaal kon het niet uitdrukken: `token_set_ratio` geeft 100 op een deelverzameling,
      dus "Shampoo" en "Shampoo Ketoconazol" scoorden beide 100 tegen "ketoconazol shampoo". Zie
      LEARNINGS. Vervangen door `compute_h1_overlap` (symmetrisch, F1 over beide dekkingen), met
      een lift van +10 bij overlap >= 90 én querydekking >= 90 — alleen omhoog, gemaximeerd op 89
      zodat H1 alleen nooit een tier A maakt, en nooit op een afgekeurde rij.
- [x] **V55 — twee staleness-bugs die daaruit rolden.** `out_facet_value_names` werd alleen
      bijgewerkt door de tak die hem zette, dus RC4/RC4-source/V53 leverden `/c/...` uit terwijl
      het veld "geen facet" rapporteerde (32 van 497 rijen — de kale "Shampoo" in de h1-kolom).
      En `h1_similarity` werd vóór de cascade berekend. Beide nu ná de cascade opnieuw.
- [x] **V56 — de producttelling op de bestemming raakt de score niet meer** (Joeps besluit).
      Een dunne facetpagina is een taxonomieprobleem voor een ander team en zegt niets over de
      vraag of dit het juiste antwoord op de query is. `FACETED_COUNT_PENALTY_BANDS` ingetrokken
      (staat als lege tuple, niet verwijderd). **`COUNT_PENALTY_BANDS` op een KALE categorie
      blijft** — daar meet de telling niet hoe vol de pagina is maar hoe ver een `dom_share` te
      vertrouwen is; een test zet de twee naast elkaar zodat een opruiming ze niet samenvouwt.
      73 rijen omhoog, 0 omlaag, 0 doelen verlegd; A+B 93 -> 105.
- [x] **De V28-redenregel overdreef het bewijs met ordes van grootte.** Hij printte `total`,
      dat in OR-fallback de héle maincategorie is: "865778 products dominantly in 'Shampoo'
      (100%)" ging over een aandeel gemeten over TWEE producten. Noemt nu de AND-treffers en zegt
      het erbij als de modus fallback was. Alleen tekst, geen score verandert.
- [x] **V57 — merkfacet dat de query nooit noemde** (Joeps regel 102: Oordoppen -> Make-up
      accessoires `/c/merk~'Generic'`). De guard stelde de omgekeerde vraag; zie LEARNINGS.
      4 doelen veranderd, nul echte merkredirects geraakt, A+B blijft 105.
- [x] **V58 — de search-derived url vertrouwde een ongecontroleerde categorie-slug.** Drie rijen
      wezen naar een niet-bestaand segment (`/products/gezond_mooi/gezond_mooi/` voor "anwb",
      idem voor "a.h" en "optidee bestellen"). De fallback-tak van `_classify` leest per product
      `categories[-1]`, en dat IS de maincat als het product niet dieper is ingeschaald; die tak
      kan naam en slug uit verschillende niveaus geven ('Woonaccessoires' met slug `huis_tuin`).
      Nu een vormcontrole: `<maincat>_<cijfers>` of geen url. Dubbele-slug-urls 3 -> 0.
- [x] **Nagemeten met een echte A/B** (oude code in een worktree, `data/cache` als symlink,
      `--reuse-data-cache`). Let op: `facets.csv` ververste vandaag om 17:56 en verplaatste
      ~50 doelen — een meting van vóór en ná die refresh is geen A/B. Zie LEARNINGS.
- [x] Voor/na per rij: `Downloads\claude\redirects_f4383643_herrun_18aug.xlsx`, met
      `url_14aug`, `doel_gewijzigd`, `query_dekking`, `h1_match` en `target_products`.
- [x] **xlsx-export uitgebreid** met `h1_match` en `target_products` naast de bestaande `h1`.

**Openstaand, wacht op Joeps keuze** (zie BACKLOG): de 4 kale sprongen met 0% querydekking
zichtbaar maken via `flag_for_review`, of ze echt onderdrukken.

### 2026-08-18 — HS2.0 opnieuw gedraaid en live op alle 12, tool terug in de nav

- [x] **Live-check na 13 dagen: 11 van de 12 buckets stonden nog exact op onze selectie**
      (url-sets byte-identiek aan de bewaarde payloads). Alleen **Fietsen 38000** was
      overschreven, en die set is 100% een subset van `bt.new_hs_data` 'Augustus 2026' → de
      **HS1.0-pipeline is de overschrijver**. Zie LEARNINGS voor het bewijs en de vormtest.
- [x] **Alles herbouwd as-of 2026-08-18 en alle 12 gepusht, 12/12 teruggelezen en identiek.**
      `pa.hs2_features` 1.098.271 urls (was 1.081.728); `pa.hs2_sitemap` 1.141.415 rijen /
      3.539 cats (1.040.258 scored + 101.157 new); `pa.hs2_sitemap_maincat` 654.839 rijen /
      32 maincats; maincat-map ververst (3.569 cats). Nieuwe standen: Stoelen 4.844,
      Eetkamerstoelen 1.289, Sneakers 4.120, Voer 1.898, Douchewanden 238, Mobiele telefoons
      1.524, Airconditionings 450, Dekbedovertrekken 2.568, Grasmaaiers 742, Shirts 1.978,
      Kantoor 16.909, Fietsen 13.304. Drop-kosten **1.495 SEO-visits/90d** (eerste rollout
      1.441), waarvan Kantoor 827 en Fietsen 427 — die laatste is HS1.0-inhoud die we
      terugpakken. Snapshots: `Downloads/claude/hs2_payloads_20260818/` (incl.
      `live_snapshot_361_nl_prepush.json`).
- [x] **De new-URL-bucket zakte van 287k naar 101k** — dat bevestigt het vermoeden van 3
      augustus dat die 287k opgeblazen was door een facet-migratie in het 20-daagse venster.
- [x] **Caps bewust NIET herbouwd.** De per-cat seizoenscaps (21 juli) staan per
      `calendar_month`, dus augustus zat er al in. En `pa.hs2_maincat_cap` is 1,5x de **live**
      set: herbouwen zou Fietsens cap op HS1.0's kleinere set ankeren (6.376 i.p.v. 10.091
      urls) en hem stil verlagen.
- [x] **De selectie is stabiel** — de 10 testcats en Kantoor kwamen na 15 dagen binnen ~1% op
      hetzelfde formaat terug. Fietsen is de enige grote beweging en dat is het herstel.
- [x] **255-tekens-guard op de payload-builders** (`53e9767`). Zie LEARNINGS; Kantoor daarna
      16.918 -> 16.909.
- [x] **Healthscore-tool weer aangezet** (`8f9bd5e`): nav-entry alfabetisch tussen DM Review en
      Index Checker op 34 pagina's + de tegel "Healthscore 2.0" terug in de SEO Tools-sectie
      van dashboard.html. `/static/healthscore.html` 200 en `/api/healthscore/health` healthy,
      dus de router draaide al. Statische bestanden komen van disk → alleen Ctrl+Shift+R nodig.
- [ ] **Fietsen 38000 is nu de kanarie.** Komt HS1.0 hem opnieuw overschrijven, dan is de
      publisher nog steeds actief. Wekelijks terugkijken volstaat; de vormtest uit LEARNINGS
      kost twee GET's. #priority:high
- [ ] **Meet de 12 tegen hun controls** (40000 Multimedia-accessoires voor Kantoor, 37000
      Auto's voor Fietsen). Let op: de baseline is nu **18 augustus**, niet 3/4 augustus — de
      set is opnieuw samengesteld. #priority:medium
- [ ] **Airconditionings 9005317 krimpt in september.** Cap gaat van 567 (aug) naar 225 (sep,
      season_index 0,24), dus de volgende run zal die categorie ongeveer halveren. Verwacht
      gedrag, maar het ziet uit als een regressie als je het niet weet. #priority:low

### 2026-08-18 — Titels die een zustercategorie noemen: 4.503 opgeruimd

Joep's Teenslippers/Gezondheidsslippers-melding. Oorzaak in de titelgenerator, niet in
kopteksten; volledige analyse in LEARNINGS.md.

- [x] **Oorzaak gevonden en afgebakend** — `fetch_products_api` valt bij een gemiste
      CSV-lookup terug op de categorie van het eerste product. Code is sinds `bc68056`
      (2026-07-21) correct; de fout zat in data van jan–mei 2026.
- [x] **`scripts/analysis/scan_titel_zustercategorie.py`** — detector met stamtest, zodat
      "Koekenpannenset" op een Pannensets-pagina niet meetelt. 6.504 van 1.022.042 titels.
- [x] **Proef op 46 Slippers-URLs** — 45 opgelost, 1 `api_failed`
      (`schoenen_430879_430974/c/merk~480833`, verouderde facetwaarde op een dode URL).
- [x] **Volledige hergeneratie 6.504** — 6.391 success / 113 failed in 551s; scan daarna
      6.504 → 2.001, nul nieuwe treffers. Backups in `Downloads\claude\`
      (`titel_backup_alle_2026-08-18.json`, `titel_backup_slippers_2026-08-18.json`).
- [x] **Resterende 2.001 verklaard** — `is_type_facet`-override, correct gedrag. Niet
      hergenereren.
- [ ] **OPEN — de 6.391 nieuwe titels moeten nog live.** `unique_titles_content` is de
      werkvoorraad, niet de productie. Vraag van Joep 2026-08-18: kan dat vanuit het
      dashboard.

### 2026-08-18 — Kopteksten strakker op het onderwerp + DMA Exclusions-knoppen

Aanleiding: padelrackets genoemd én gelinkt in de koptekst van de Tennisrackets-pagina.
Volledige analyse in LEARNINGS.md ("ean2pim is de verkeerde meetlat").

- [x] **Onderzocht of ES `source=ean2pim` hierbij helpt** — nee. Het is een waarde van
      `categorization.categorizationSource`, gaat over de categorie-as (tennis en padel
      delen categorie 9001062 `Rackets`, beide prob 1.0), en voegt binnen `shopCount >= 2`
      niets toe aan onze bestaande filtering. Search API geeft `categorization` niet terug.
- [x] **Harde ONDERWERP-regel in de v3-userprompt** (`gpt_service_v3.py`). Bakent af op
      `h1_title` zelf, dus een brede pagina blijft breed: subject `'Rackets'` → padel blijft
      (terecht), subject `'Tennis Rackets'` → geen padel, 4/4 runs, ook met padelproducten
      op positie 2 en 3 van de 30.
- [x] **`facets_not_resolved`-guard** (`scraper_service.py` + `main.py` +
      `batch_api_service.py`). Staat er een facet in de URL en herkent de API er geen, dan
      niet genereren maar `failed` + pending. Was een stille terugval op de kale
      categorienaam. Blast radius vandaag: 66 `winkel~`-URLs, 0 met koptekst, 0 in de wachtrij.
- [x] **Percent-decode in `parse_beslist_url`** — `%7E`-URLs gaven nul filters en dus altijd
      het brede onderwerp. 15 kopteksten, 33 URLs. FAQ deelt deze parser, dus die profiteert mee.
- [x] **Driftscan gebouwd** — `scripts/analysis/scan_koptekst_onderwerpdrift.py`. Zoekt een
      tweede productgroep op hetzelfde kopwoord (tennis**rackets** → padel**rackets**).
      Drie keer aangescherpt tegen valse positieven (20.302 → 6.396): kopwoord-vocabulaire
      uit alle h1_titles, term los-vs-aaneen, en herschikkingen uit de titel zelf.
      229.096 gescand → 6.396 drift, waarvan 881 in de openingszin.
- [x] **881 kopteksten hergenereerd** (572 vervangen, 241 `no_valid_links`, 60 zonder
      producten, 8 API-fout; 598s). Backup in `Downloads\claude\koptekst_backup_voor_regen_2026-08-18.json`.
      Tennisrackets-URL zelf geverifieerd schoon.
- [x] **DMA Exclusions**: "Clean enabled" van `btn-outline-danger` naar `btn-outline-orange`
      (ruimt historie op, muteert niets in Google Ads); page-local `.btn-fill-primary`
      opgeruimd naar de gedeelde `.btn-run`.
- [ ] **OPEN — de 881 was te hoog.** 73% van de gemarkeerde pagina's heeft een
      `product_subject` dat afwijkt van `h1_title`; die teksten waren nooit fout. Zie BACKLOG.

### 2026-08-17 — Vier UI-punten: klikbare datum, zoekvelden, Model-chips, filterrij

Vier losse wensen van Joep, drie in SEO stats en een in GSD Campaigns. Commit `f665863`.

- [x] **Datum in Per-day overview is klikbaar** → zet Top categories én Dagoverzicht op die
      dag en herlaadt ze. Beide kaarten houden hun eigen datumkiezer; de klik schríjft die,
      er komt geen derde waarheid bij. Aangeklikte rij blijft paars (`#e6dff5`, dezelfde tint
      als het actieve weekdagfilter ernaast), en die markering wordt in de rij-renderer gezet
      zodat sorteren en doorbladeren hem niet wegpoetsen. Alleen de sectie die echt van dag
      verandert haalt opnieuw op; flatpickr krijgt `setDate(iso, false)`; beide kaarten
      knipperen één keer, achter een `prefers-reduced-motion`-guard.
- [x] **Top categories wordt geladen maar NIET opengeklapt** (Joep, 2026-08-18 — eerst
      klapte hij open). Dichtgeklapt blijft dicht achter de "Show categories"-banner, met
      de nieuwe dag er al achter; stond hij open, dan blijft hij open en ververst hij in
      beeld. De klik zegt dus "laad deze dag", niet "vouw deze sectie uit".
- [x] **Performance standup laadt mee op de datumklik** (Joep, 2026-08-18). Was de enige
      van de drie dagsecties met een losstaande `#refDate`. Zelfde patroon als de andere
      twee: kiezer schrijven, alleen ophalen als de dag echt verschuift, kaart flitst mee.
- [x] **Een nog lege nieuwste dag valt uit de grafiek en de Per-day overview** (Joep,
      2026-08-18). `trimEmptyTail()` knipt hem weg direct na de fetch, dus grafiek, tabel,
      tegels, totalen en export lezen dezelfde reeks. Alleen aan de staart, in meervoud
      (lag is soms twee dagen), leeg = elke telbare metriek op nul, en niets knippen als
      álles leeg is. De weggelaten dagen staan in de metaregel. **Hing eraan vast:**
      `loadTileDeltas` mat het venster aan de datumkiezer en de totalen aan de data — met
      een weggeknipte dag werd dat zes dagen tegen een baseline van zeven (~14% scheef).
      Het venster komt nu uit de laatste datum die echt in de data zit. Commit `cc2d654`.
- [x] **Zoekveld per subtabel in Top categories** (Top maincats en Top deepest cats apart —
      één gedeeld veld filtert ook de tabel waar je niet naar kijkt). Zoekregel van Bot Hits:
      `type=search`, meerdere woorden = AND op substrings, over alle tekstkolommen, `oninput`.
      Filtert vóór de Top-N-slice. Nul treffers zegt `Geen categorie gevonden voor "…"` i.p.v.
      "No data.". Typen links hertekent alleen de linkertabel.
- [x] **Model-chips in Campaigns created** naar de vorm van de Status-chips ernaast
      (transparant, 1px rand, label in dezelfde tint), CPR lichtblauw `#1f99c4`, CPC roze
      `#be4693`. Raakt ook de preview-tabel via de gedeelde `modelBadge()` — met opzet.
- [x] **`↻ Refresh` viel over twee regels** in de filterrij. Knoppen en selects houden hun
      breedte (`nowrap` + `flex-shrink: 0`), het zoekveld is het krimpelement (250 → 190px).
- [x] **Alle vier in een echte render gecontroleerd** (Windows Chrome headless tegen :8003,
      met een tijdelijke zelfklikkende kopie van de pagina — recept in LEARNINGS).
- [x] **UI_BLUEPRINT bijgewerkt** met drie regels die hieruit volgen: *Zoekveld boven een
      tabel* (nieuw kopje onder Form controls), *een cel die een andere sectie stuurt* (onder
      Tables) en *een knoplabel is nooit het krimpelement* (onder Buttons). Plus een noot bij
      Labels/badges dat twee labelkolommen in één rij in KLEUR verschillen, niet in vorm.

**Opgemerkt en niet opgelost (hoort bij de sprint hieronder):** de Model-kolom staat op álle
2.905 rijen op `-`. Niet de chips — `/api/gsd-campaigns/campaigns` levert het veld `model`
niet, want de draaiende uvicorn (gestart 08:58) is ouder dan commit `81f20d9`. Wordt vanzelf
opgelost door de herstart die hieronder al als openstaand punt staat.

### 2026-08-17 — GSD: prijsbucket-structuur voor nieuwe CPC-shops + Model-kolom

Joep leverde `Downloads\claude\create GSD-campaigns CPR CPC split.py` aan: "kijk of en hoe we
dit veilig kunnen integreren, en of het script nog andere afwijkingen heeft."

- [x] **Gediffed tegen het origineel** (`scripts_def/create GSD-campaigns.py`), niet tegen onze
      port. 174 regels verschil, **exact één feature**: de CPC-bucketstructuur. De
      modelafleiding en de CPC/CPR-mailkolom zaten al in het origineel én al bij ons. Het
      script zelf kan niet draaien — `for campagne_data in redshiftdata1:` is een NameError die
      ook in het origineel staat, dus de CPC-tak heeft nooit gedraaid.
- [x] **Overgenomen:** 1 campagne per shop met 14 adgroups (één per `custom_label_4`-bucket),
      elk eigen max CPC, tree serveert alleen de eigen bucket, "overig" uitgesloten. Geen
      a/b/c-split — een CPC-shop deelt geen conversiedata. Bods 1-op-1 uit het script (0,09–0,35).
- [x] **Alleen voor NIEUWE CPC-shops** (Joeps keuze). `_labels_for_shop()` beslist het, uit de
      kandidatenlijst die de create-kant toch al ophaalt; een shop die het legacy paar
      (`[label:a,b]` / `[label:c,no_data,no_ean]`) al draagt houdt dat, dus nooit twee
      structuren naast elkaar.
- [x] **Drie dingen bewust anders dan het script:** `[label:cpc]` in de naam i.p.v.
      `[new cpc structure]` (zonder `[label:]`-token is de campagne onzichtbaar voor de
      pause-fallback, `_match_existing_campaign` én de labelkolom — het Emob.nl-patroon); onze
      helpers i.p.v. kale `mutate_*` (retries, `feed_label`, `contains_eu_political_advertising`
      hard, `_name_contains_regexp` i.p.v. GAQL `LIKE '%…]%'`); en merk-negatives + BRANDED-label
      krijgt de bucketcampagne wél.
- [x] **Idempotent per onderdeel.** Het script hangt de tree achter `if is_created:` — een run
      die halverwege sterft laat een adgroup zonder tree achter die nooit meer wordt aangevuld.
      `_build_bucket_structure()` checkt adgroup/product-ad/tree apart en is meteen het
      reparatiepad. `_repair_campaign` en `_check_campaign_structure` hebben een bucket-tak
      gekregen: die keken naar `ags[0]` en zouden 14 adgroups beoordelen op de eerste.
- [x] **`_sheet_type_from_label` gefikst:** testte op een komma in het label, dus `[label:cpc]`
      zou als CPR in de run-log zijn beland.
- [x] **Model-kolom** tussen Shop en Country in Campaigns created (filter, sortering,
      Copy-export) en in de preview-tabel. Geen extra API-call: afgeleid uit het
      `[label:X]`-token dat we toch al parsen. **Twee waarden, CPC en CPR** — de kolom gaat over
      het model, niet over de structuur; het verschil tussen de twee CPC-structuren blijft in de
      campagnenaam leesbaar.
- [x] Getest: 30 assertions (labeltokens, modelafleiding, de `c`/`cpc`-botsing, pause-dekking
      voor alle drie de campagnesoorten, structuurkeuze, sheet-kolom, naamopbouw), JS-syntax,
      kolomtellingen. Commit `81f20d9`.

**Openstaand:**

- [ ] **NIET getest tegen de Google Ads API** — er is geen campagne aangemaakt. Voor livegang:
      backend herstarten (draait zonder `--reload`, dus `fuser -k 8003/tcp` + opnieuw starten),
      dan de **preview** op een datum waarop een CPC-shop aanging en controleren dat de
      Model-kolom klopt, dan pas één echte run via de include-filter. Let op: in 60 dagen ging
      er **1** nieuwe CPC-shop aan tegen 65 CPR, dus die datum moet je opzoeken.
- [ ] **Twee live bugs in het LEGACY CPC-pad, bewust niet meegefixt** (Joep: alleen nieuwe
      shops). Geverifieerd tegen `dra.gmc_products_issues` (~40M rijen): (a) `PRICE_BUCKETS`
      eindigt op `1597-2584` / `2584-Onbeperkt`, maar de feed heeft `1597-2594` / `2594+` — die
      twee buckets matchen nul producten; (b) `add_sub_cpc` partitioneert op **INDEX0** (waar de
      score A/B/C/No data/No EAN in zit) i.p.v. INDEX4, dus geen enkele bucket-node matcht en
      alles valt in de "overig"-node — die bij ons biddable is. Netto draaien de legacy
      CPC-campagnes op één vlak bod zonder prijsdifferentiatie, en serveren de a/b- en de
      c-campagne allebei de hele catalogus. Staat als waarschuwing bij de constanten zodat
      niemand de twee bucketlijsten "gelijktrekt". Omvang: 514 live CPC shop-landcombinaties,
      maar vrijwel alles is door `GSD-CPC.py` gebouwd (dat de juiste INDEX4 gebruikt maar
      dezelfde twee foute bucketnamen heeft), dus de INDEX0-variant raakt een handvol campagnes.

### 2026-08-14 — Bot Hits: R-urls werden als Cat-url geteld

Joep zag R-url op 0,0% in de URL-type-donut en vroeg of dat kon kloppen.

- [x] **Oorzaak.** `url_type()` in `backend/bothits_ingest.py` checkte `/r/` met een
      `startswith` ná `/products/`, terwijl beslist's R-urls `/products/<cat>/r/<term>` heten.
      Alleen de kale `/r/<term>`-vorm werd goed geclassificeerd (604 URLs op 177.000).
      `seo_stats_service._urltype_case()` deed het al goed met `LIKE '%/r/%'` als eerste arm —
      de twee tools spraken elkaar tegen, terwijl het commentaar bij `URLTYPE_BUCKETS` beweerde
      dat ze het eens waren.
- [x] **Gefikst:** `/r/` bovenaan als containment-check. 13 gevallen in een unit-test.
      Nagekeken dat geen ander type ooit `/r/` in de URL heeft, dus de nieuwe eerste regel
      kaapt niets weg.
- [x] **Her-ingest van het hele S3-venster** (`scripts/bothits_reingest_urltype.py`, per datum):
      40 bruikbare datums 2026-07-05 t/m 08-13, 94 minuten, 37,7 GB, 0 mislukt. 2026-07-03 zit
      niet in S3 en 07-04 heeft één uur — die weigert de ingest terecht.
- [x] **Resultaat in de donut** (venster 07-15..08-13): R-url van 180 hits (0,00%) naar
      7.113.183 (7,90%); Cat-url van 7.637.929 (8,49%) naar 1.314.723 (1,46%); C-url van
      45,74% naar 44,86%. Het totaal bleef exact 90.009.315 hits en de winst van R-url komt
      precies uit die drie — dus niets bijgekomen, niets verdwenen.

**Openstaand / bewust zo gelaten:**

- [ ] **Alles vóór 2026-07-05 houdt de oude indeling.** `pa.bothits_daily` bewaart het ruwe type
      zonder URL-tekst en de logs zijn na ~42 dagen uit S3 verdwenen, dus dit is niet meer te
      repareren. De tool gaat terug tot 2026-02-14, dus een periode die over 07-05 heen loopt
      mengt twee definities. **De UI-noot is op 2026-08-17 weggehaald** op verzoek van Joep
      (commit `fee6f6c`); het standaardvenster is de laatste 30 dagen vanaf de laatst gedekte
      dag, dus 07-05 valt daar inmiddels buiten — de eerdere schatting "~oktober 2026" ging uit
      van het volledige bereik en niet van het standaardvenster. Het commentaar bij
      `URLTYPE_BUCKETS` in `bothits_service.py` blijft staan: de breuk in de historie is nog
      steeds waar, ook nu hij niet meer op het scherm staat. Deze regel blijft open zolang de
      tool tot 2026-02-14 terugkijkt.
- [ ] **Mijn eerste impactschatting was een factor 60 te laag** (0,13% i.p.v. 7,9%), omdat ik hem
      op `bothits_unknown_daily` had gemeten — die tabel houdt alleen de staart van onbekende
      URLs. Zie LEARNINGS; geen actie, wel een valkuil om te onthouden bij de volgende sizing.

### 2026-08-14 — Projectroot opgeruimd (niets weggegooid)

Joep: "alles netjes in mapjes, ongebruikte bestanden weggooien (bij twijfel niet weggooien)",
daarna aangescherpt naar **niet weggooien maar opbergen**. Aanpak staat nu als `/cleanup` in
`~/.claude/commands/cleanup.md`.

- [x] **Opgeborgen in `attic/`** (met `git mv`, dus als rename in de historie): `Dockerfile` +
      `docker-compose.yml` → `attic/docker/`, en `start.sh` + `run_local.sh` +
      `start-dm-tools.bat` → `attic/start-scripts/`. `attic/README.md` legt per map uit waarom
      het daar staat, wat er niet meer werkt vanaf die plek (docker-compose mount relatieve
      paden) en hoe je het terugzet.
- [x] **Verplaatst zonder archief:** de losse `query.txt` uit de root →
      `notes/query-visits-per-deepest-subcat.txt` (blijft untracked, zoals hij was).
- [x] **Verwijderd (herbouwbaar):** `__pycache__` op root en in `scripts/`, plus
      `.pytest_cache`. Drie verouderde logs uit de root naar `logs/archive/`.
- [x] **Docs bijgewerkt** waar ze naar de verhuisde bestanden wezen: een verwijzing bovenaan de
      Docker-secties van `README.md` en `docs/START_HERE.md`, de boomweergave in START_HERE, en
      de "two modes"-regel in `docs/ARCHITECTURE.md`.
- [x] **Gecontroleerd:** API, een pagina en `/api/thema-ads/themes` alle drie 200, `from themes
      import` werkt nog, geen nieuwe fouten in het log, en nul verwijzingen naar de oude paden.

**Bijna misgegaan, en het staat als les in LEARNINGS:** `themes.py` leek dood (grep op de
bestandsnaam gaf nul treffers) maar wordt op tien plekken geïmporteerd als
`from themes import …`. Grep op de modulenaam, niet op de bestandsnaam.

**Openstaand — jouw beslissing:**

- [ ] **`backend/__pycache__` (960 KB, `root:root`, feb–apr).** Er heeft ooit iets de backend
      als root gedraaid. Verplaatsen lukt niet zonder sudo, want de kernel eist schrijfrecht op
      de map die verhuist: `sudo mv backend/__pycache__ attic/pycache-root-owned` — of
      `sudo rm -rf backend/__pycache__`, want dit is het enige in deze lijst dat volledig
      herbouwbaar is. Zolang het er staat compileert Python die modules bij elke start opnieuw
      in het geheugen.
- [ ] **De twee optimizer-caches, samen 756 MB.** `rurl_optimizer_v2/data/cache` (438 MB,
      nieuwste 8 juli) is van de versie die je gebruikt; `rurl_optimizer/data/cache` (318 MB,
      nieuwste 24 april) is van v1. Opbergen heeft geen zin — dat kost hetzelfde als weggooien
      (de volgende run fetcht opnieuw uit Redshift) maar houdt de ruimte bezet.
- [ ] **`README.md` beschrijft Docker nog als deploypad**, in tegenspraak met `CLAUDE.md`. Dat
      was al zo vóór deze opruiming. Herschrijven is een eigen klusje.

### 2026-08-14 — Drie kleinere punten: tegelrij, uitklappaneel, grafiekloader

- [x] **SEO Stats: de acht tegels bovenin op één rij.** Ze pasten vier pixels per tegel te
      krap: de rij is 1076px en `gap-2` kost 7 × 8px, dus (1076 − 56) / 8 = 127,5px, terwijl
      de gedeelde `.stat-card` `min-width: 130px` aanhoudt. Met "vs prev 7d" ónder het
      percentage in plaats van ernaast kan de tegel smaller (127px, één rij bij 1920 én
      1500px). Dat loste ook iets op wat er al stond: bij GSAAS revenue wikkelde die caption
      al binnen de tegel ("VS PREV" / "7D"). Gescoped op `#statsRow`, dus Dagoverzicht (5
      tegels, 202px) en de standup (6, 167px) houden hun bredere vorm. Onder ~1400px wikkelt
      de rij alsnog naar 4+4, en dat hoort ook — daar is 107px per tegel te smal voor
      "434.456".
- [x] **Bot Hits: het uitklappaneel per bot-familie.** Verticale padding van 0,9 naar 1,6rem
      (met 0,9 plakte de eerste grafiek tegen de rij waar je net op klikte), en de vier
      grafiektitels op `font-weight: 600` via een nieuwe `.chart-title`. Bewust niet
      `.muted-note` verzwaren: die zit op deze pagina ook op veldlabels ("Dagen ophalen",
      "Splitsen op", "Top") en op twee statusregels. De twee titels in het URL-uitklappaneel
      zijn meegegaan.
- [x] **SEO Stats: de grafiek toont nu dat hij laadt.** De tabel shimmerde al, maar de grafiek
      bleef de vorige range tonen. Nu een `.chart-skel` met dezelfde `skelShimmer`-keyframes
      als de tabelrijen, `aria-busy` erbij, en één `chartLoading()` die op **elke** uitgang van
      `load()` wordt aangeroepen — ook in de catch, want een shimmer die na een mislukte fetch
      blijft draaien leest als "nog bezig".

      Twee correcties onderweg, allebei uit de meting: over 420px is het grijs van `.skel-bar`
      een muur (lichtere stops), en de canvas-opacity eronder deed niets omdat de shimmer er
      dekkend bovenop lag — terwijl mijn commentaar beweerde dat de oude lijn erdoor schemerde.
      Nu heeft de shimmer alpha en staat de canvas op 0,32.

**Openstaand:**

- [x] **De tegels boven de grafiek laden nu ook** (2026-08-14). Opgelost zonder ze te
      vervangen: één klasse op de rij, en met CSS wordt de waarde een shimmerbalk (tekst
      transparant + shimmer als achtergrond), gaat de WoW-badge op `visibility: hidden` (niet
      `display: none`, anders zakt de tegel in) en dimt het lijntje naar 0,25. Label, rand en
      klikvlak blijven staan, dus de metric-toggle werkt nog tijdens het laden — nagemeten:
      `metric-on` → `metric-off` op een klik. Op de eerste load is de rij nog leeg, dus daar
      dragen de grafiek en de tabellen het signaal.

### 2026-08-14 — Negen lay-outpunten, ronde 2

- [x] **GSD Tag Toppers' preview-knop** was al `btn-outline-purple`, dus die werd paars zodra
      het vocabulaire terug was. Geen wijziging nodig.
- [x] **`.btn-tool` weer neutraal grijs.** Dat zijn de "Open tool"-knoppen op de startpagina:
      vijftien naast elkaar in een tegelgrid, en in paars is de knop dan het opvallendste
      element op de pagina in plaats van de tool waar hij bij hoort.
- [x] **MC ID Finder:** kop naar "Search Merchant Center id's".
- [x] **Thema Ads:** `.more-info-btn` naar de vlakke stijl (dezelfde rand, radius en
      tekstkleur als een neutrale outline-knop, i.p.v. `#6c757d` met radius 4px), en
      "Reset Labels" van `btn-warning` naar `btn-outline-purple` — `btn-warning` is in het
      vlakke thema de gevulde oranje CTA, en dit is een secundaire actie.
- [x] **SEO Stats:** de "Show categories"-banner van paars naar grijs. Hij is 460px breed en
      klapt alleen een sectie open; in paars was hij het opvallendste op de pagina.
- [x] **Canonicals-tabs:** de vorm was al goed (onderlijn, geen kaders) — de actieve lijn
      stond op het **blauwe** accent terwijl UI_BLUEPRINT §Tabs brand-paars voorschrijft, met
      de reden erbij ("blauw zou als een tweede merk lezen"). Het thema had die regel
      overschreven; nu weer paars, dashboardbreed.
- [x] **Canonicals' Preview/Generate/"+ Add Rule"** zat al in de vorige commit.
- [x] **Paginabreedte gelijkgetrokken, en dat heft een besluit van 2026-07-30 op.** De
      blueprint had twee toegestane breedtes: standaard `col-md-10` (1076px) en een
      *data-dense uitzondering* `col-lg-11` (1186px) voor precies vijf pagina's. Alle vijf
      omgezet: **Bot Hits, SEO Stats, SEO titles, DMA Exclusions, Healthscore**. Een meting
      over alle 35 pagina's bevestigde dat er geen zesde was. Twee bijvangsten: `url-checker`
      en `seo-titles` hadden hun kolom **zonder** `.row`, wat 1080px geeft in plaats van
      1076 — dat mist de negatieve marges van het grid. Eén pagina wijkt met opzet af:
      `dashboard.html`, want de tegelgrid van de startpagina hoort niet in een 1076px-kolom.
- [x] **SEO Stats: drie tegels erbij in Performance standup** — SEO CTR, OPB en Bounce, met
      hetzelfde lijntje en dezelfde Δ%-kop als hun drie buren. Bounce is de enige waar omhoog
      slecht is, dus die kleurt omgekeerd (`pctText(..., invert)`).

      **Backend:** `/deltas` gaf alleen bezoeken en omzet, dus er is een `seo_rates`-blok bij
      gekomen met waarden én deltas. De dagkeuze is daarin het echte werk: CTR en Bounce
      hangen aan de **bezoekdagen** (ref vs ref-7), OPB aan de **omzetdagen** (ref-1 vs
      ref-8), zodat teller en noemer van OPB dezelfde dag zijn. Zou OPB de omzet van ref-1
      delen door de bezoeken van ref, dan meet de tegel vooral het verschil tussen twee
      dagen. Voor CTR en Bounce staat de puntverandering er ook bij, want een Δ% op een
      percentage is een percentage van een percentage.

      **En een bug in een gedeelde helper, die deze tegels aan het licht brachten:**
      `_fetch_daily` haalt bezoeken met een `IN`-lijst maar omzet met
      `WHERE tac.date BETWEEN dates[0] AND dates[-1]`. Dat maakte de **sortering** van
      `dates` een stille voorwaarde: mijn ongesorteerde `[vis_p1, vis_p2, rev_p1, rev_p2]`
      gaf bezoeken voor alle vier de dagen maar omzet 0,0 voor de twee buiten de range
      (gemeten: 05-08 en 13-08 op nul, 06-08 en 12-08 niet). Opgelost met `min()`/`max()` in
      de helper zelf in plaats van bij de beller, zodat de volgende beller er niet in trapt.

### 2026-08-14 — theme-flat samengevoegd met style.css, en negen lay-outpunten

Joep keurde de flat-proef goed. Eén stylesheet dus, en daarna een reeks punten die op het
thema voortbouwen.

**De merge (commit `8a455bb`):**

- [x] **`theme-flat.css` bestaat niet meer**, de `<link>` is uit alle 35 pagina's, en de
      oude waarden zijn UIT `style.css` weg in plaats van overschreven: de grijze kopbalk,
      de knopschaduw, de oranje `.btn-secondary`, vijf per-Bootstrap-kleurnaam gesorteerde
      knopregels die allemaal dezelfde oranje zetten, de paarse `.btn-outline-purple`, de
      oranje `.btn-outline-danger`, vier losse disabled-regels (incl. `#processBtn`) en de
      dubbele rand op `.date-box`. Terugdraaien = tag **`ui-voor-flat`**, geen `sed` meer.
- [x] **Eén cascadebug, en die kwam alleen uit de meting.** `.bg-primary:not(.navbar)` heeft
      door de `:not()` specificiteit 0,2,0 — gelijk aan `.card-header.bg-primary` — en beide
      hebben `!important`. Met twee bestanden won de kopbalk op bestandsvolgorde; in één
      bestand won `.bg-primary`, en dan kleurde een `card-header bg-primary` weer ouderwets
      grijs (gemeten op SEO titles). Opgelost met `:not(.card-header)`.
- [x] **Verificatie:** computed styles van 74 selectors × 20 properties, base én forced
      `:hover` (CDP `CSS.forcePseudoState`), op alle 35 pagina's, vóór en na. Twee identieke
      runs geven een ruisvloer van 52 verschillen (shop-campaigns laadt niet elke keer
      dezelfde DOM; `dashboard`'s `.btn-tool` wordt midden in een transitie gemeten). Eén
      verdachte tabelbreedte op seo-rulings uitgesloten met een A/B op dezelfde live data:
      oude CSS uit git geïnjecteerd geeft exact dezelfde 1200px.

**De negen punten van Joep:**

- [x] **Paars en oranje outline terug in het vocabulaire.** De eerste themaversie maakte alle
      outline-knoppen neutraal grijs; dat is teruggedraaid. `.btn-outline-purple` is weer
      paars (7,35:1 op wit) = "andere actie", en dat is precies wat de blueprint al
      voorschreef voor **Refresh**. `.btn-outline-orange` is weer oranje = "+ Add rule" /
      Export. Neutraal grijs blijft voor klassen die niets zeggen (de Bootstrap-kleurnamen,
      `.btn-preset`, `.btn-bulk-*`).
- [x] **Alle Refresh-knoppen outlined paars.** De meeste gebruikten al `btn-outline-purple`;
      drie afwijkers rechtgetrokken (GSD Campaigns had er twee met inline hexes plus
      `onmouseover`-JS, Redirect-tool gebruikte de pagina-eigen `.btn-purple-outline`). De
      Copy- en Export-knoppen in diezelfde toolbars gingen mee, want hand-gestyled paars
      naast een canonieke paarse knop is precies wat de blueprintregel moet voorkomen.
- [x] **Canonicals — Transformation Rules:** Preview URLs is paars (volgde uit het
      vocabulaire), Generate is de oranje CTA (`btn-run` i.p.v. `btn-secondary`), en de zes
      "+ Add Rule"-knoppen zijn oranje-outline. De lokale gevuld-paarse knopklasse is weg.
- [x] **Canonicals — Recent results en DMA+ — Run History naar de blueprint-tabel.**
      `.tool-table-wrap` + `table-sm/-hover/tool-table` + `thead.table-light`; weg: zebra,
      `table-bordered`, inline padding per `<th>`, zes keer 16,66% kolombreedte en
      `text-center` over de hele tabel. De Delete-knop in Canonicals is de canonieke
      `btn-outline-red` i.p.v. een inline rode rand met twee `onmouseover`-handlers.
      Tijdstempels in het blueprintformaat `YYYY-MM-DD HH:MM` (de en-US-notatie wikkelde over
      twee regels). **Let op het verschil tussen die twee pagina's:** Canonicals krijgt
      `…+00:00` uit de API, DMA+ schrijft `datetime.now().isoformat()` (naïef lokaal). Bij
      DMA+ is de "Z"-truc uit `fmtTs` dus verkeerd — die zou de historie twee uur later
      tonen. Alleen het formaat overgenomen, niet de conversie.
- [x] **Bestandsupload 32rem.** Kopteksten stond op 939px en Unique titles op 1042px; de
      referentie (Auto-Redirects, in een `col-md-6`) was 513px. Als gedeelde regel
      `input[type="file"].form-control { max-width: 32rem }` i.p.v. per pagina, want er zijn
      elf van die velden over negen pagina's.
- [x] **Datumpickerbreedte hangt nu aan het VELDTYPE.** flatpickr → 5,3rem (er staat altijd
      exact `YYYY-MM-DD`, gemeten 79,0px tekst), native → 6,6rem (Chrome zet zijn eigen
      icoon ín het veld). Eerst was dat één 6,6rem met 26px lucht. De picker in SEO titles
      ging daarmee van 263px naar 221px, en DMA Bidding volgt automatisch.
- [x] **R-Finder en DMA Bidding krijgen de paarse kalender.** Beide hadden de kale
      OS-kalender. Het `.flatpickr-*`-themablok stond byte-identiek op zes pagina's (md5
      gecontroleerd) en staat nu één keer in `style.css`. **Daarbij één les:** in een
      page-`<style>` kon dat blok niet verliezen, maar vanuit `style.css` wél — flatpickr's
      eigen `border-radius: 5px` won op alle acht pagina's totdat de linkvolgorde
      rechtgezet was naar bootstrap → flatpickr → style.css.
- [x] **Redirect Generator** had zijn "+ Add … Rule"-knoppen al op `btn-outline-purple`
      (dus geen wijziging nodig), **DMA Exclusions'** Preview ging van `btn-outline-primary`
      naar `btn-outline-purple`.
- [x] **UI_BLUEPRINT herschreven:** §theme-flat is nu §"Het vlakke thema" en beschrijft geen
      proeflaag meer; knoptabel bijgewerkt (paars/oranje/grijs, nieuwe disabled-look);
      datumpicker- en uploadbreedtes vastgelegd; de gedeelde tabelbasis en de linkvolgorde
      gedocumenteerd; de kaartkop-quote bijgewerkt naar de vlakke waarden.

**Bonusvondst uit de meting, en het is een echte bug die het thema verborg:** URL Validator
had `.btn-outline-purple-on-dark` / `-orange-on-dark` — wit label voor een donkerpaarse
kaartkop. Die kop bestaat niet meer: sinds het vlakke thema rendert élke `.card-header` op
`#f4f5f9`. Zolang het thema die knoppen naar wit-vlak dwong viel het niet op; toen ik ze aan
hun eigen pagina teruggaf stonden er witte labels op een lichte kop. Vier pagina-regels weg,
knoppen op de canonieke klassen.

**Openstaand:**

- [x] **De knopregels in de page-`<style>`-blokken zijn opgeruimd en het `!important` is van
      de knopgroepen af** (2026-08-14). 42 regels over twaalf pagina's plus zes id-selectors:
      kleur eruit, maatvoering behouden (padding, font-size, de 1,5px rand van `.btn-tool`, de
      scheidingslijn van een segmented control). 38 keer `!important` weg uit style.css; wat
      blijft staan (51×) hoort bij de kaartkop, de chips en het formulierblok, waar Bootstrap's
      utilities hun kleur zelf met die vlag zetten.

      **Drie dingen die de meting eruit haalde, en die het waard zijn om te weten:**
      1. **Een `border-left` is geen `border-color`.** De scheidingslijn tussen de presets
         (`.btn-group .btn-preset:not(:first-child) { border-left: 1px solid #d9d4e8 }`) viel
         buiten mijn kleurfilter en won daarna op specificiteit — de lijn sprong van `#c3c7cc`
         naar `#d9d4e8`. Nu `border-left-width/style` zonder kleur.
      2. **Index Checker's actieve filter was niet te zien.** Die knoppen zijn
         `btn btn-outline-secondary active`, en Bootstrap maakt van `.active` een gevulde
         `#6c757d`-knop; ons `!important` overschreef dat, dus actief zag eruit als inactief.
         Het vocabulaire heeft nu een eigen aan-staat (paarse tint `#f3f0fa`, paarse rand en
         label, weight 600 — dezelfde taal als `.metric-toggle.on`). Bij url-validator wordt
         het label van de actieve filter daardoor 600 in plaats van 500; de oranje vulling
         daar komt uit zijn eigen `!important`-regel en blijft.
      3. **Healthscore's `.btn-hs` had `border: none`** en is nu 2px hoger en breder, want het
         thema geeft elke CTA een 1px rand — in dezelfde kleur als de vulling, dus onzichtbaar.
         Daarmee is die knop even groot als elke andere hoofdactie.

      Verificatie: computed styles vóór/na over 35 pagina's, base én forced `:hover`, met de
      ruisvloer van twee identieke runs eraf getrokken (66 ruis, 96 totaal). Wat overbleef zijn
      exact de drie punten hierboven plus één mid-transitie-meting op een hover.
- [ ] **De acht varianten van het `.tool-table`-blok over elf pagina's samenvoegen.** De
      canonieke basis staat nu in `style.css`, de pagina-blokken staan er nog naast.
- [ ] **DMA+ slaat tijdstempels naïef-lokaal op** (`datetime.now().isoformat()` in
      `dma_plus_service.py`, zes plekken). Dat werkt zolang de server op Europe/Amsterdam
      staat en is een valstrik zodra iemand die waarden met UTC-kolommen vergelijkt. Migreren
      betekent bestaande opgeslagen waarden meenemen, dus apart oppakken.

### 2026-08-14 — Datumkiezer: dubbel kader weg, en `/static` revalideert weer

Twee meldingen van Joep (screenshots `2026-08-14 09 26 35.png` en `09 49 55.png`) met
dezelfde oorzaak eronder: hij keek naar een oudere `style.css` dan de server had.

- [x] **Dubbel kader om de datumkiezer weg.** Vijf pagina's hadden nog een eigen
      `#startDate, #endDate { border: 1px solid #d9d4e8; border-radius: 10px; padding: … }`
      uit de tijd van losse datumvelden; sinds `.date-box` zit het kader om het HELE bereik,
      dus dat tekende er een tweede binnenin. Nagemeten in de screenshot: buitenlijn
      `#d4d5d5` (de box), 1px daarnaast `#d9d4e8` (de pagina-regel). Weg in **seo-stats,
      bothits, shop-campaigns en gsd-campaigns**; `accent-color`/`color-scheme` blijven voor
      de native picker, en de `width`-override in gsd-campaigns blijft voor de placeholder.
      Óók weg: de `#id::-webkit-calendar-picker-indicator`-regels — een id-selector wint van
      `style.css` en zette Chrome's glyph weer aan naast onze `::before`.
- [x] **Focus verhuisd naar de box.** De oude `#startDate:focus`-regels waren dode code (de
      velden hebben `border: 0`, `box-shadow: none`). Nu `.date-box:focus-within` in
      `style.css` én `theme-flat.css`, plus `outline: none` op de velden — zonder dat laatste
      tekent Chrome zijn eigen ring om de border-box en valt die als een zwart kadertje
      midden in de box.
- [x] **`/static` stuurt `Cache-Control: no-cache`.** `main.py` mount nu via een
      `NoCacheStatic(StaticFiles)` die de header in `file_response` zet. Bewaren mag,
      navragen moet; `StaticFiles` zet zelf al een etag, dus het is een 304 zonder body
      (gemeten: 0 bytes). **Dit was de eigenlijke oorzaak van beide meldingen** — SEO titles
      had zelf niets: die twee kale native date-inputs vielen onder elkaar in hun `col-md-3`
      omdat het `.date-box`-blok in de gecachte CSS ontbrak. Backend zonder `--reload`, dus
      herstart uitgevoerd (alle runs stonden idle: `seo-titles: idle`,
      `ai-titles: is_running false`, geen subprocessen).
- [x] **SEO titles: Stop en Generate naar rechts.** `justify-content-end` lijnt alleen af
      binnen de kolom, en drie keer `col-md-3` vult 9 van de 12 kolommen — `ms-auto` duwt de
      kolom zelf naar de rechterkant van de `.row`.
- [x] Gecontroleerd door de vier pagina's headless te renderen (Playwright in WSL, libs uit
      een lokale map — recept in `LEARNINGS.md`), inclusief `getComputedStyle`: `#startDate`
      `border: 0px none`, `.date-box` één `1px solid rgb(214,216,215)`. Plus een scan over
      alle twaalf `.date-box`-pagina's: nul resterende rand-regels op datumvelden.

**Openstaand:**

- [ ] **Eenmalig `Ctrl+Shift+R` bij Joep.** Wat de browser vóór deze fix heeft opgeslagen,
      is opgeslagen zonder die header; daarna revalideert alles vanzelf.
- [ ] **Het verdict op de flat-proef staat nog open** — zie de sectie van 2026-08-13.

### 2026-08-13 — Audit Bothits, ronde 2: alle vijf fases uitgevoerd

Tweede audit over 5.020 regels (vijf parallelle reviewers, daarna nagerekend tegen de live
DB en het echte archief). De elf bevindingen van de eerste ronde staan verderop; dit zijn
nieuwe. Twee structurele risico's staan bovenaan omdat ze de rest ordenen:

**A. De URL's-tab beantwoordt zijn eigen vraag niet.** `get_top_urls()` leest
`pa.bothits_unknown_daily` op grond van een docstring die zegt dat de `pa.urls`-match kapot
is. Die match is op 13-08 gerepareerd (`a2ee990`) en de querylaag ging niet mee. Gemeten op
2026-08-12: de tab ziet **41.427 van 3,39 mln bot-hits (1,2%)** en sluit álle 191.108
known-URL-hits én alle productpagina's per constructie uit. Zichtbaar bewijs: de top-3
"meest gecrawlde URL's" is `/data/graphql`, een OG-fallback-plaatje en `/favicon.ico`.
`pa.bothits_url_daily` staat op 20.300.271 rijen t/m 08-12, dus de blokkade is weg.

**B. "Staat in de ledger" wordt gelezen als "is compleet".** `already_ingested()` test alleen
aanwezigheid; `run_drop` archiveert daarop bronbestanden en `_prune_archive` wist ze na de
retentie — na het S3-venster van ~42 dagen definitief. `is_complete` komt alleen uit 24
uurstempels: niet uit het aantal distributies, niet uit mislukte downloads, niet uit
onleesbare `.gz`. Vijf partiële datums staan al in de ledger (03-26, 04-13, 04-21, 05-01, 06-09).

**Fase 0 — gedaan (geen gedragswijziging):**

- [x] **Staging-retentie vuurde niet, en dat was de hele bug.** `_prune_archive` hing alleen
      aan `run_drop`, terwijl het werk via `backfill` liep. 18 datummappen voorbij de grens,
      **18,88 GB opgeruimd** (38 → 20 GB); nu ook aangeroepen vanuit `run_backfill`. Vóór het
      wissen per datum getoetst op `is_complete` + `known_rows > 0`.
- [x] **`/top-urls` tie-break** `ORDER BY hits DESC, w.url` — rang 250 zat op 208 hits met
      drie rijen gelijk, dus de lijst flapte tussen identieke verzoeken.
- [x] **XSS-sink dicht**: `chkbox()` escapet nu `META.hosts` (host-header → `norm_host`
      lowercaset alleen, `skip_host` laat alles door dat op `.beslist.nl` eindigt). Eén van
      de twee sinks in het bestand die het niet deed; de URL-tabel escapete al correct.
- [x] **Filters herladen de URL's-tab** als die openstaat — de filterkaart staat boven de
      tabstrip, dus Toepassen liet oude rijen staan onder een kop met de oude telling.
- [x] **`badge bg-info` → `badge-purple`** (was wit-op-lichtgrijs, de val die UI_BLUEPRINT
      zelf beschrijft) · `colspan="7"` → 6 · `cancelling` reset in de `finally` ·
      `ORDER BY url_id` in `load_url_ids` (2 botsende sleutels, last-wins was ongedefinieerd) ·
      `out = (None, None)` onvoorwaardelijk in `classify_ua` · dode `else`-tak in `_filters` weg.
- [x] **Doc-drift**: bevinding 8 stond hier onterecht open (gefixt in `53fcea2`) · het
      herstelrecept in BOTHITS_PROCESS eiste onnodig een ledger-DELETE (`--date` omzeilt de
      done-filter al) · router-docstring zei dat `/top-urls` en `/url` weg waren terwijl ze
      leven · `verify.py` claimde een all-or-nothing guard die er niet is · `get_top_urls`'
      docstring beschreef een defect dat niet meer bestaat.

**Fase 1 — hard falen i.p.v. stil falen — GEDAAN (geen cijfer verandert):**

Regressiepoort voor de hot-loop-wijziging: `process_file()` over 24 echte bestanden uit
6 distributies (13,4 MB, 103.840 regels, 45.174 bot-hits) met md5 over de gesorteerde
cube-, known- én unknown-items. **Byte-voor-byte identiek** vóór en ná.

Het harnas staat in de repo: **`scripts/bothits_parse_fingerprint.py`** (gebruik in de
docstring). Gebruik het ook voor fase 2 — elke wijziging in `process_file()` hoort hier
langs. Twee dingen die het harnas zelf moest leren: sorteer de steekproef op **grootte
per distributie** (alfabetisch pak je 20 kruimelbestanden van 480 B met nul bot-hits, en
één distributie mist de hosts die `skip_host` juist moet wegfilteren), en let op de
staging-retentie van 21 dagen — `BOTHITS_FP_SRC` moet naar een datum wijzen die er nog
staat.

- [x] **`hours_present` en `is_complete` staan nu in de DDL** én in `SCHEMA_MIGRATE`
      (catalogus-guard, niet `ADD COLUMN IF NOT EXISTS` — die pakt de AccessExclusiveLock
      óók als er niets te doen valt, en de migratie loopt bij élke `ingest_date()`).
      Getest: migratie is een no-op tegen live en idempotent bij twee runs, en een VERSE
      opbouw uit `bothits_schema.sql` slikt de echte ledger-INSERT — beide in een
      transactie met rollback, dus zonder de shared DB aan te raken.
- [x] **Veldnamen worden één keer geresolveerd bij `#Fields:`**, via `REQUIRED_FIELDS`, en
      een ontbrekend veld geeft een `RuntimeError` met de kolomlijst uit het bestand erbij.
      Bewezen op vier synthetische gevallen: hernoemd veld → fout, weggehaald veld → fout,
      géén `#Fields:`-header → waarschuwing (was volledig stil), te korte regels → geteld
      als `bad_lines` met een waarschuwing (waren stil genegeerd). De per-regel `KeyError`
      kan niet meer bestaan, dus de tak die een kapotte dag als een goede dag liet
      doorgaan is weg.
- [x] **Frontend: één `getJSON()`** die op `!r.ok` gooit met `detail` als melding, plus een
      `#loadError`-banner boven de tabs. Bij een fout worden de drie charts en de bot-tabel
      LEEGGEMAAKT — cijfers van de vorige selectie laten staan is erger dan niets tonen.
      Toegepast op `refresh()`, `loadUrls()` (de `forEach` staat nu binnen de `try`),
      `loadIngest()` (had helemaal geen `try`) en `boot()` (liet bij een kapotte `/meta`
      alle drie de filterlijsten op "laden…" staan). Plus `maxlength="200"` op `#urlQ`,
      want de route zet `max_length=200` en een lange gekopieerde URL gaf een 422 die de
      tabel eeuwig op "laden…" zette.
- [x] **Ingest-lock lekt niet meer**: `Thread.start()` in een `try`, en bij een fout gaat
      de state terug op niet-lopend en wordt de lock vrijgegeven. Hiervoor hield één
      mislukte thread-start de lock voor de rest van het procesleven vast — knop én timer
      dood tot een herstart van uvicorn.
- [x] **Mislukte runs zeggen dat ze mislukt zijn.** `run_drop` en `run_backfill` geven nu
      dezelfde vorm terug met een eigen `failed`-lijst en `status` ∈
      `ok` / `partial` / `failed` / `cancelled` / `no_drop_dir`; fouten staan niet meer
      tussen de "overgeslagen" datums. `run_backfill` gaf hiervoor een plátte lijst waarin
      een mislukte datum simpelweg ontbrak. De CLI print een samenvatting en geeft
      **exitcode 1** bij `failed`/`partial`. `scripts/bothits_nightly.py` telt `failed` mee
      in zijn exitcode — een nacht waarin élke datum omviel eindigde met 0, dus de
      Windows-taak stond op "gelukt". Alle drie de statustakken bewezen met een
      gesimuleerde parse-fout.
- [x] Kleiner: beide POST-endpoints hebben nu foutafhandeling en de onbereikbare
      `except S3NotConfigured` is weg (met uitleg waaróm hij niet kon vuren) · de
      Verwerk-knop blijft niet meer onherroepelijk disabled als de POST faalt ·
      onbekende `group_by` echoot niet meer de typefout maar de kolom die echt gebruikt
      is (geverifieerd: `?group_by=bot_familly` → `bot_class`) · `_prune_archive` telt met
      `os.walk` zodat `archive_freed_mb` klopt met wat `rmtree` wist · `--date` sorteert
      (bewezen met drie datums omgekeerd opgegeven) en noemt in een waarschuwing WELKE
      gevraagde datum niet in de boom zit · de ingest-poll heeft een handle, een
      re-entry-guard en een `clearTimeout` op `hidden.bs.tab` (elk tabbezoek tijdens een
      run startte een extra keten).

**Fase 2 — het volledigheidscontract — GEDAAN (gedragswijzigend):**

- [x] **`is_complete` komt uit drie voorwaarden** i.p.v. alleen `n_hours >= 24`: alle 24
      uurbuckets, **geen onleesbaar bestand** (`failed_files`), en **minstens zoveel
      bestanden als S3 zei te hebben** (`expected_files`). Beide nieuw in de ledger, met
      catalogus-guard in `SCHEMA_MIGRATE`; bestaande rijen krijgen `0` en `NULL` — "onbekend"
      is eerlijker dan een nul die als bewijs van volledigheid leest.
- [x] **`expected_files` komt uit een manifest** dat `bothits_s3.fetch()` per datum
      achterlaat in `<staging>/_manifest/<datum>.json`. Een sidecar en geen returnwaarde
      omdat download en ingest twee fases zijn met alleen de staging-map als koppeling — zo
      overleeft het aantal ook een crash ertussen. `None` bij een backfill uit het archief:
      daar is geen autoriteit, dan blijft het bij uren + leesbare bestanden.
- [x] **Ledger-aanwezigheid ≠ compleet.** `already_ingested(with_completeness=True)` geeft
      `{datum: is_complete}`; `run_drop` verwerkt een incomplete datum opnieuw i.p.v. hem
      over te slaan, en `bothits_s3` meldt hem als `herstel (incompleet geladen)` i.p.v.
      `al_geingest`. Er stonden er vijf: 03-26, 04-13, 04-21, 05-01, 06-09.
- [x] **Archiveren en prunen zijn gegated op volledigheid.** `_archive()` draait alleen na
      een compleet geladen datum, en `_prune_archive` weigert elke datummap die niet als
      compleet in de ledger staat (en ruimt niets op als de DB onbereikbaar is). Dit is het
      laatste punt waarop een bronbestand nog te redden is — buiten het S3-venster van ~42
      dagen is er geen tweede kopie. Getest: compleet+oud wordt gewist, incompleet blijft.
- [x] **Known-URL-tripwire staat vóór de commit en gooit** in plaats van te loggen. Hij
      stond eronder, dus juist op de dag dat hij nodig is stond de kapotte datum al in de
      ledger als volledig en was `_archive()` de eerstvolgende stap.

**Regressiepoort — uitgevoerd, en de oude baseline was ZELF verkeerd.** Herverwerking van
2026-08-12 uit `~/bothits_s3/_processed/2026-08-12/` gaf `files=2905`,
`raw_lines=6.946.608`, `bot_lines=3.550.758`, 1.450 cube-rijen en een **byte-identieke**
`bothits_unknown_daily` — maar `known_rows` ging van 142.054 naar **142.057**.

Uitgezocht in plaats van weggewuifd, en het is geen regressie: de %-alias-fix uit
`53fcea2` (auditbevinding 8) gold nog niet toen 08-12 oorspronkelijk werd geladen — die
run was de meting van de parser-versnelling, vóór dat commit. Bewezen door de dag twee
keer te parsen met en zonder aliassen: **zonder = 142.054, met = 142.057**, exact het
verschil. Dat is precies wat die fix belooft ("geldt alleen voor datums die je daarna
verwerkt"). **De juiste baseline is dus 142.057**; de andere datums (08-09 t/m 08-11,
geladen ná 12:33 UTC) hadden de aliassen al.

**Wat NIET is gedaan, en waarom — dit is een correctie op de audit zelf.** De audit stelde
voor om te eisen dat élke distributie 24 uur heeft. Gemeten op de 21 staging-datums: drie
datums hebben een distributie met minder (07-31: 22, 08-10: 23, 08-11: 23) en het is elke
keer `E14VW8EO449KG7` — de kleinste distributie, 139 bestanden/dag ≈ 5,8 per uur — met de
missende uren 00, 02 en 19. Dat is geen verloren data maar een uur zonder één request;
CloudFront schrijft dan geen bestand. Als poort zou 14% van de datums omvallen als
"incompleet" en zou `run_drop` ze daarna nooit meer oppakken. Het zit er nu in als
**waarschuwing** (`_warn_thin_distributions`), niet als eis. Het bestandsAANTAL is om
dezelfde reden geen maat: complete dagen lopen legitiem van 1.591 tot 4.969 bestanden.

**Fase 3 — metriekcorrecties — GEDAAN (cijfers op het scherm zijn veranderd):**

- [x] **Facet-diepte telt alleen nog category-URL's.** `facet_depth()` geeft 0 voor álles
      zonder `/c/`, dus de nul-balk zat vol productpagina's — pagina's die per definitie
      géén facetten hebben. Depth 0 gaat van **49.225.165 naar 7.682.976**. De kaartkop
      noemt de reikwijdte nu ook.
      **Let op, mijn eerste formulering van deze poort was fout.** Ik schreef "depth ≥1 is
      100% category, dus die blijven gelijk". Niet waar: depth 1/2/3 verliezen samen 1.355
      hits (1.060 `list`, 293 `other`, 2 `product` — URL's mét `/c/` die geen
      categoriepagina zijn). De juiste verwachting is de *category-shaped* kolom uit de
      meting: depth 1 = 15.252.392, depth 2 = 5.402.707, depth 3 = 6.491.495, depth 4–6
      onveranderd. Zo gemeten, zo uitgekomen.
- [x] **URL's-tab leest nu BEIDE tabellen**, samengevoegd en opnieuw gerankt:
      `pa.bothits_url_daily` (in `pa.urls`, uitputtend geteld) + `pa.bothits_unknown_daily`
      (de staart, dagelijkse top-500 per familie). Nieuwe kolom **"In pa.urls"** zegt per rij
      uit welke bak hij komt, want de twee zijn niet symmetrisch. In de standaard-top-250
      staan nu 238 rijen uit `pa.urls` die er hiervoor per constructie niet in konden:
      `/products/gezond_mooi/` (9.048 hits), `/products/mode/` (8.321), `/products/huis_tuin/`
      (8.078). Snelheid **5,8s koud / 0,08s warm**; de eerste opzet van het `url_type`-filter
      kostte 37s (een `ANY()` met ~1 mln url_id's) en is nu een `EXISTS` in de aggregatie —
      10,9s voor C-url, 2,0s voor Cat-url.
- [x] **`get_url_detail` volgt mee.** Zonder dat gaf elke rij uit de bekende kant een leeg
      uitklappaneel, want die URL's staan niet in de onbekende tabel. Beide benen getest:
      paneeltotaal exact gelijk aan het rijtotaal.
- [x] **`url_type`/`facet_depth` voor de bekende kant komen uit `pa.urls.url`** — een VIERDE
      kopie van dat vocabulaire, dus geverifieerd i.p.v. gehoopt: op 20.000 echte
      `pa.urls`-rijen geeft de SQL exact hetzelfde als `url_type()`/`facet_depth()` uit de
      ingest, 0 verschillen op beide. Herhaal die test zodra een van die functies verandert.
- [x] **Partiële IP-range-fetch zet echte crawlers niet meer op `failed`.** `_fetch_all` is
      all-or-nothing per operator en geeft `(data, all_ok)`; `verdict()` geeft `unchecked`
      voor een operator die niet in `_TABLE` staat; en een onvolledige ophaal wordt **niet**
      gecached (anders doet die halve lijst zich 24 uur als waarheid voor) maar valt terug op
      de verouderde cache. Getest met bing eruit: `unchecked` i.p.v. `failed`, Googlebot
      ongemoeid. Poort gehaald: 0 (datum,familie)-paren boven 50% failed, split onveranderd
      89,38 / 9,63 / 0,99.
- [x] **Alleen "PLP" aanvinken zegt nu waaróm de lijst leeg is.** Productpagina's staan niet
      in `pa.urls` én gaan nooit naar `unknown_daily`, dus die selectie kán geen rijen
      opleveren terwijl het Overzicht er honderden miljoenen hits voor toont.
- [x] **"Per dag" gebruikt de eigen `days` van de rij.** De bot-tabel deelde élke familie
      door de grootste `days` in de set: Google-AI (826 hits over 14 dagen) stond op 28/dag
      i.p.v. 59. De URL-tabel deed het al goed, dus dezelfde kolomnaam betekende op één
      pagina twee dingen.
- [x] **`get_meta` haalt `first_day`/`last_day` uit de cube**, de rest uit de ledger. De
      cube-fix zat alleen in `_range()`, maar de frontend seedt de datumvelden uit `/meta` en
      stuurt daarna altijd beide datums mee — dus `_range()` sloot kort en paste zijn eigen
      bescherming nooit toe.
- [x] **`waste_pct` is `None` zodra er op `known` gefilterd wordt** i.p.v. een zelfverzekerde
      0,0 of 100,0. De noemer is dan al gefilterd op precies de eigenschap die de teller meet.

**Fase 4 — perf en dedup — GEDAAN (geen enkel cijfer verandert):**

Hot loop opnieuw byte-voor-byte identiek getoetst met `scripts/bothits_parse_fingerprint.py`.

- [x] **`unquote()` staat nu ná `skip_host` en `classify_ua`.** 56,5% van de regels is
      non-bot (103.840 → 45.174 in de steekproef), dus meer dan de helft van het
      decodeerwerk ging naar een pad dat daarna werd weggegooid. Kan omdat `stem` tot dat
      punt nergens gebruikt wordt.
- [x] **`skip_host` op een voorgekookte frozenset + tuple** i.p.v. per regel `"." + d`
      bouwen in een genexpr: **0,177 → 0,054 µs** per aanroep (3,3×).
- [x] **`heapq.nlargest`** i.p.v. een volledige sort per bot-familie (~1,1 mln entries
      sorteren om er 500 te houden). Uitkomst identiek: de `(hits, key)`-tuples zijn totaal
      geordend, dus ties breken hetzelfde.
- [x] **Eén filter-builder.** `_known_filters` en `_unknown_filters` zijn dunne wrappers om
      `_filters`, met de echte verschillen als vlaggen (`alias`, `has_url_type`,
      `has_known`). Dit was met fase 3 op drie gekomen.
- [x] **`FILE_DATE_RX` komt uit de ingest**, niet nog eens gedefinieerd in `bothits_s3`.
      Geen circulaire import (getest in beide volgordes + via `backend.main`).
- [x] **`distributions()` pagineert**, en `preview()` haalt hem met `force=True` op zodat
      de belofte "een zevende distributie wordt automatisch opgepikt" ook zonder herstart
      geldt. `CommonPrefixes` en `Contents` delen dezelfde 1000-limiet.
- [x] **`preview()` lijst parallel.** Serieel waren dat 270 round-trips bij `days=45`, op de
      4-thread pool die álle leesroutes delen.
- [x] **`n_2xx..n_5xx` hebben eindelijk een lezer**: de statusverdeling in het
      URL-detailpaneel, mét `overig` expliciet erbij (de 0xx-rest). Aansluiten i.p.v.
      schrappen, want een per-URL 4xx-aandeel is precies wat je van een crawlbudget-vreter
      wil weten. Getest: 2xx+3xx+4xx+5xx+overig telt exact op tot `hits`.

### 2026-08-13 — Flat-restyle: theme-flat.css over het hele dashboard (PROEF)

Joep: alle knoppen en stijlelementen (card-header, knopkleuren, dropdowns) naar de stijl
van `Downloads\claude\2026-08-13 18 11 15.png`, mét een terugweg.

- [x] **`frontend/css/theme-flat.css`**, gelinkt op alle **35** pagina's ná `style.css`.
      Een aparte override-laag en géén bewerking van `style.css`, precies zodat
      terugdraaien één commando is: `sed -i '/theme-flat.css/d' frontend/*.html`. Het tag
      **`ui-voor-flat`** wijst naar de laatste commit ervóór.
- [x] **Palet uit de screenshot gemeten met PIL**, niet geschat: pagina `#f4f5f5`, paneel
      `#f4f5f9`, vlak `#ffffff`, rand `#d6d8d7`, accent `#8796ef`, chip `#eaeef9`.
- [x] **Twee accenten, en dat is een meting geen smaak**: wit op het screenshot-accent
      `#8796ef` haalt **2,75:1** en zakt door WCAG AA. Dat accent is nu de kleur van
      lijnen (tab-onderlijn, chiprand, focusring); gevulde knoppen krijgen `#5566e0` →
      **4,81:1**.
- [x] **`!important` op kleur, niet op maatvoering.** Veertien pagina's herdefiniëren
      knoppen in hun eigen `<style>`, en dat blok wint altijd van een stylesheet. Zonder
      dit sloeg het thema daar half aan. Padding, breedte en `nowrap` blijven van de
      pagina, dus lay-outs blijven heel.
- [x] **Knopvocabulaire teruggebracht tot drie groepen**: gevuld accent (primair),
      neutrale outline (de rest), rood (destructief). Het onderscheid oranje-actie versus
      paarse-actie verdween — dat bestond alleen in kleur, niet in betekenis. De eigen
      klassen van pagina's (`.btn-purple`, `.btn-hs`, `.btn-preset`, `.btn-tool`,
      `.btn-*-action`, `.btn-bulk-*`) zijn in diezelfde drie ondergebracht.
- [x] Visueel gecontroleerd op bothits, seo-prio, seo-stats, canonical en healthscore.

**Bijgesteld na de eerste ronde (Joep, 2026-08-13):**

- [x] **Oranje CTA terug.** De gevulde knoppen stonden op het blauwe accent en zijn terug
      op `#CC5500` / hover `#E97451` — exact de waarden uit `style.css`, dus geen nieuwe
      oranje. **Blauw en oranje hebben nu elk een eigen betekenis**: blauw = selectie en
      interactie (tab-onderlijn, focusring, chips), oranje = actie. De hover van de
      outline-knoppen ging daarom van blauw naar neutraal grijs; met een oranje CTA
      ernaast waren dat drie kleuren op één knoppenrij.
      Twee kanttekeningen: `.btn-purple` en `.btn-hs` wáren paars, niet oranje — ze zitten
      in de primaire groep omdat het op hun pagina's de hoofdactie is, dus voor die twee is
      het geen exacte terugzetting. En wit op `#CC5500` is 4,31:1: genoeg voor knoptekst,
      net onder de 4,5:1 voor gewone tekst. Bestaande huiswaarde, ongewijzigd overgenomen;
      `#B84D00` geeft 5,1:1 als het ooit moet.
- [x] **Streepje in de datumkiezer gecentreerd.** Bij een bereik kleefde het aan de tweede
      datum. Gemeten in de gerenderde pagina: beide inputs zijn 6,6rem maar de datum is
      ~84px en links uitgelijnd, dus alle speling van veld 1 viel vóór het streepje —
      **60px links tegen ~15px rechts**. Eerste datum nu rechts uitgelijnd via
      `:has(.sep)`, zodat de vijf losse datumvelden links blijven. Bewust niet de velden
      smaller gemaakt: die breedte moet zowel het flatpickr-formaat (`2026-08-13`) als het
      native (`13-08-2026`) als Chrome's eigen kalendericoon aankunnen.
- [x] **Bothits:** ondertitel "alleen categorie-URL's …" uit de Facet-diepte-kop. De
      reikwijdte van de grafiek verandert niet.

**Openstaand / bewust niet gedaan:**

- [ ] **De paarse navbar is niet aangeraakt.** Die staat niet op de screenshot en het is
      het enige element dat het dashboard herkenbaar maakt. Eén variabele in
      `theme-flat.css` als hij toch mee moet.
- [ ] **Rood blijft rood** voor destructieve acties, alleen platter. Een verwijderknop
      hoort niet in de accentkleur te verdwijnen.
- [ ] **Bestaande bug, niet van deze restyle**: op `healthscore` loopt het label
      "Dashboard" over de icoonknop rechtsboven heen. `.nav-dashboard-btn` is
      `width: 2.75rem` zonder `overflow: hidden`, en de lange paginatitel duwt de nav.
      Geverifieerd door dezelfde pagina zónder het thema te renderen — daar staat het er
      ook. Eén regel CSS, maar buiten de gevraagde scope gelaten.
- [x] **Verdict op de proef: BEVALT** (Joep, 2026-08-14) → `theme-flat.css` is samengevoegd
      met `style.css` en bestaat niet meer. Zie de sectie van 2026-08-14 hieronder. De
      `!important`-laag vervalt daarmee **niet helemaal**: hij compenseerde niet alleen de
      laag maar ook Bootstrap's eigen utilities én de 77 knopregels in zeventien
      page-`<style>`-blokken. Dat laatste is nu de openstaande opruiming.

### 2026-08-13 — Vier losse punten van Joep (na de audit)

- [x] **Type-kolom in Top X URL's spreekt nu dezelfde taal als Filters > URL-type.** De
      tabel toonde het ruwe type (`category_facet`, `product`), het filter bood buckets aan
      (`C-url`, `PLP`). Eén `RAW_TO_BUCKET` naast de bestaande `BUCKET_TO_RAW`. Het ruwe
      type komt als `url_type_raw` mee, want de trailing-slash-regel hangt daaraan.
- [x] **Look & feel van Bothits naar het Semrush-achtige voorbeeld** (screenshot in
      `Downloads\claude\2026-08-13 18 11 15.png`): tabs zonder kader met alleen een
      onderlijn (grijs bij hover, gekleurd bij selectie), lichtere typografie,
      filtercontrols als afgeronde pillen op een zachte achtergrond, periode als één kader
      met kalendericoon. **Dit is een PROEF en staat bewust in één apart blok in
      `bothits.html`** — het wijkt af van UI_BLUEPRINT en van de rest van het dashboard.
      Zie de notitie in UI_BLUEPRINT.md voor adopteren of terugdraaien.
- [x] **Apply to Taxonomy is oranje**: `btn-run` i.p.v. `btn-success`. Dat is meteen de
      canonieke huisklasse voor een primaire actie (`--color-button` = #CC5500), dus geen
      losse kleur erbij, en disabled valt vanzelf terug op grijs outline.
- [x] **SEO Priority onderscheidt drie soorten leeg.** "No rows match the current filter"
      las als "zet je filter anders", terwijl de gewone reden is dat de run niets te doen
      vond. Nu: 0 acties · alles staat al goed (alleen `keep`, met de tip om *Show kept
      rows* aan te zetten) · of er zijn wél rijen maar het filter snijdt ze weg, mét aantal.

**Verdict: aangenomen — dashboardbreed doorgevoerd (2026-08-13)**

- [x] **Tabvorm naar `css/style.css`** en het 3-regelblok van **elf** pagina's weggehaald.
      Dat blok stond in de page-`<style>` en laadt dus ná de stylesheet: was het blijven
      staan, dan had de gedeelde regel niets gedaan. Alleen `thema-ads` houdt een lokale
      override (acht tabs op één regel: `nowrap` + 0,9rem); kleur, gewicht en onderlijn
      komen daar wél uit de gedeelde regel.
- [x] **`.date-box` naar `css/style.css`** en doorgevoerd op **alle tien** pagina's met een
      datumveld: 9 bereiken + 5 losse datums = 14 boxes over 23 inputs. `form-control` is
      eraf; ids, `value` en `onchange` zijn ongemoeid, dus flatpickr-inits (die op `#id`
      targeten) en alle `.value`-lezers werken onveranderd.
- [x] **Drie dingen die onderweg misgingen en zijn opgelost** — staan als waarschuwing in
      UI_BLUEPRINT zodat niemand ze opnieuw bouwt:
      1. Chrome's **eigen** kalendericoon zit ín het veld, dus op pagina's zónder flatpickr
         stonden er ineens twee (bij een bereik drie). Niet verborgen — dan is de picker
         alleen nog met typen te bereiken — maar uitgerekt over het veld en transparant:
         glyph weg, klikvlak juist groter.
      2. `inline-flex` liet het label ernáást staan zodra er ruimte was en eronder als die
         er niet was — dezelfde pagina, twee uitkomsten, afhankelijk van de kolombreedte.
         Nu `flex` + `width: fit-content`, dus altijd onder het label zoals een gewone
         `form-control`.
      3. Het icoon is een `::before` met een data-URI en geen inline `<svg>`, anders staat
         datzelfde blok vijftien keer over tien pagina's.
- [x] **Eén pagina-eigen uitzondering**: `gsd-campaigns` heeft twee losse datums met een
      uitleg-placeholder ("Leave empty for most recent") die niet in 6,6rem past — daar
      een lokale `width: 13.5rem` i.p.v. de gedeelde maat oprekken voor alle tien.
- [x] Visueel gecontroleerd op negen pagina's, zowel flatpickr- als native-varianten
      (headless Chrome; screenshots in `Downloads\claude\chk*.png`).

**Openstaand:**

- [ ] **De actieve tab is brand-paars, niet Semrush-blauw.** Bewuste afwijking: blauw is in
      dit dashboard nergens een accentkleur en zou als een tweede merk lezen. Eén
      hex-waarde in `style.css` als Joep het tóch blauw wil.
- [x] **Typografie-proef teruggedraaid** (Joep, 2026-08-13). Bothits volgt weer gewoon
      Bootstrap. Wat die eigen font-stack betreft was het een schijnwijziging: Bootstrap 5.3
      gebruikt zelf al `system-ui, -apple-system, "Segoe UI", …`, dus op Windows kwam er
      **precies hetzelfde lettertype** uit — het zichtbare verschil zat in de grootte
      (16 → 14px) en in kleinere kaartkoppen (h5 1,25rem → 1rem). Wie het voorbeeld écht
      wil benaderen heeft een webfont nodig (Inter ligt het dichtst bij), en dat is een
      externe afhankelijkheid plus een font-flits bij het laden — aparte keuze, niet gedaan.
- [ ] **Het `url_type`-filter op de URL-tab kost 10,9s** (C-url) tegen 2,0s voor Cat-url.
      Verder terug te brengen door url_type/facet_depth in `pa.bothits_url_daily` te zetten
      i.p.v. ze uit `pa.urls` af te leiden — maar dat is een schemawijziging plus een
      backfill van 20,3 mln rijen, dus alleen doen als het gaat knellen.


**Twee dingen om te weten bij het oppakken:** :8003 draait met `--reload`, dus elke `.py`-edit
herstart de server en breekt een lopende ingest af — draai een regressie-ingest als los
`setsid`-proces. En de scheduler-crash op een kapotte `BOTHITS_AUTO_INGEST_AT` (`replace()`
staat buiten de `try`) is nu onbereikbaar omdat `AUTO_INGEST` uit staat; wie hem aanzet moet
die eerst fixen.

### 2026-08-13 — SEO Priority: false positives op globaal uitgeschakelde facetten

Vervolg op de sessie hieronder. Joep meldde dat de tool zei dat het **Winkel**-facet aanstond
op categorie Insectenhotel terwijl hij dat live niet zag. De taxonomy API gaf de website
gelijk; onderweg bleek de settings-rij-fallback van diezelfde ochtend te ruim. Volledige
uitleg in LEARNINGS (bovenaan).

- [x] **Gecontroleerd tegen taxv2: Winkel staat níet aan op Insectenhotel (9003879).** Facet
      3252 heeft `isEnabled=false`, hangt aan **0** categorieën API-breed, en de
      `CategoryFacetSettings`-rij (40901) heeft `seoPriority=null` — een restant van de
      bulk-seed van 16-03, geen bewijs van leven.
- [x] **BUG: `resolve()`-fallback promoveerde dode facetten tot schrijfbare kandidaat.** Een
      settings-rij bewijst *identiteit*, niet *levensvatbaarheid*. De master-`isEnabled` werd
      nergens in de pipeline gelezen terwijl hij gratis in beide payloads meekomt.
- [x] **Guard in `resolve()`** — vierde returnwaarde `blocked_reason`; zulke rijen worden
      `keep` / `disabled` mét reden i.p.v. een voorstel. `_parse_target("disabled")` → `None`.
- [x] **Onafhankelijke guard in `_apply_one()`** — weigert te schrijven naar een
      `isEnabled=false` facet, ongeacht wat de run voorstelde. Nodig omdat oude runs
      `proposed_seo_prio='1'` al opgeslagen hebben en een heropende run die anders alsnog
      doorduwt. Bij een API-storing bewust *niet* blokkeren (falen naar schrijven).
- [x] **Impact gemeten op de run van 19 mei**: 2.538 rijen op globaal uitgeschakelde
      facetten, waarvan **1.711 een flip voorstelden**; 98% is `winkel`. Nooit toegepast —
      `pa.seo_prio_apply_log` bevat geen winkel-write, dus niets op te ruimen.
- [x] **Getest**: offline stub-test (alle guards + de `s_dierenhuis`-case die moet blijven
      werken) én live tegen taxv2 op 9003879 — winkel BLOCKED, de zes echte facetten ok.
- [x] **Skill-docs gecorrigeerd** (`~/.claude/skills/beslist-apis/`, buiten deze repo):
      `Winkel (ID: 1)` was fout (404); het zijn **31 facetten**, alle `isEnabled=false`.
      Plus de bredere val: **slugs zijn niet uniek** (Merk 1289 *én* 3253, Kleur 5657 *én*
      3255 — één kopie per hoofdcategorie-boom). Nieuwe sectie *"Verifying a facet is
      actually live"* met het drielagenmodel.

**Openstaand:**

- [ ] **De 43 slugs die tijdens de impactmeting timeouten nog natellen.** De 2.538 is een
      ondergrens: de telling keek alleen naar slugs waarvan élke kopie uitstaat, terwijl de
      guard ook per-categorie dode kopieën vangt. #priority:low
- [ ] **Overweeg `isHidden` net zo te behandelen als `isEnabled`.** Een facet met
      `isHidden=true` op déze categorie rendert hier ook niet, maar `seoPriority` erop zetten
      is niet per se zinloos (het is een andere as). Nu stelt de tool er nog flips op voor —
      bewust gelaten, maar het verdient een expliciet besluit. #priority:medium
- [ ] **Run-brede assert uitbreiden**: naast "0% geresolved" ook alarmeren als een
      significant deel van de voorgestelde flips op `blocked` facetten landt. #priority:low

### 2026-08-13 — SEO Priority: resultatentabel met vinkjes + write-back naar taxv2

Vraag van Joep: na een run kreeg je alleen "Download excel"; hij wilde de resultaten in een
tabel zien en geselecteerde rijen doorschieten naar de Taxonomy API. Onderweg bleken drie
leesfouten in de bestaande analyse — zonder die fixes was er niets te schrijven geweest.
Volledige uitleg in LEARNINGS (bovenaan).

- [x] **Resultatentabel is niet meer verstopt.** Klapt vanzelf open als een run klaar is, en
      er staat nu een **Results**-knop per run in de historie (de tabel zat achter een klik
      op het run-id — niemand vond dat). Nieuwe kolommen: `seoPriority` (`inherit → ON`) en
      `Applied`.
- [x] **Vinkjes + apply-balk.** Per rij een checkbox (uit bij `keep`, bij een ontbrekend
      `facet_id` en bij al toegepaste rijen, met een `title` die zegt wáárom), select-all in
      de header, *Select all actionable* over het hele filter heen, *Clear*. Selectie
      overleeft sorteren/filteren/pagineren omdat hij op `(deepest_cat_id, facet_slug)` zit
      — dezelfde sleutel waarop de backend de rij opzoekt.
- [x] **`POST /api/seo-prio/apply/{run_id}`** — read-merge-write + read-back per facet, max
      300 rijen per call, categorieën 4-parallel en facetten binnen een categorie serieel.
      Dry-run schakelaar; de knoptekst verandert mee ("Preview (dry run)" vs "Apply N to
      Taxonomy") zodat een preview nooit als een echte push leest. Live pushen vraagt een
      confirm met de ON/OFF-aantallen.
- [x] **Audit-trail** `pa.seo_prio_apply_log` (incl. dry runs, `applied_by`), plus
      `applied_status/_value/_at/_error` op de resultaatrijen. Log overleeft het verwijderen
      van een run.
- [x] **BUG: legacy PDM-id gebruikt als taxv2 category-id.** Hele slug wordt nu gemapt via
      `backend/data/cat_urls.csv`. 25.805/25.808 combo's resolven.
- [x] **BUG: facet `urlSlug` uit `labels[]` lezen** i.p.v. van het facet-object — hierdoor
      hadden ALLE rijen in alle bestaande runs `facet_id = NULL`.
- [x] **BUG: `bool("inherit") is True`** in `_decide()` — `turn_on` kon nooit voorkomen en
      `turn_off` werd voorgesteld voor facetten die nooit aan stonden.
- [x] **Fallback voor verborgen facets** (staan niet in `GET /api/CategoryFacets`, houden wel
      hun settings-rij) via `GET /api/Facets?searchTerm=`, alleen geaccepteerd als de
      categorie al een settings-rij voor dat facet heeft.
- [x] **Live geverifieerd met een omkeerbare write** (kleur/3255 @ Insectenhotel 9003879:
      `inherit → ON`, read-back ok, `displayOrder 6` intact, daarna terug naar `null`; geen
      ander facet geraakt). Resultaatrij daarna teruggezet.

**Openstaand:**

- [ ] **Bestaande runs opnieuw draaien.** Alles van vóór vandaag heeft verkeerde
      voorstellen (en `facet_id = NULL`, dus de vinkjes staan uit — je kunt er niets mee
      doorschieten). Niet automatisch te repareren: de `current_seo_prio` van die runs is
      nooit echt gelezen. #priority:medium
- [ ] **`current_seo_prio` is de EXPLICIETE waarde, niet de effectieve.** Staat een facet op
      `inherit` terwijl de ouder `seoPriority=true` heeft, dan is hij feitelijk aan maar
      leest de tool "inherit" — en `turn_off` vuurt alleen op expliciet-ON. Bewuste keuze
      (conservatief), maar het betekent dat de tool erfelijk-aan-staande facetten niet kan
      uitzetten. taxv2 geeft de effectieve waarde nergens kant-en-klaar terug: de
      `seoPriority` op `LinkedFacetDto` is iets anders dan die in CategoryFacetSettings
      (Grasmaaiers 3784: `null` vs `true`). Uitzoeken vóór iemand hierop vertrouwt.
      #priority:medium
- [ ] **Overweeg een run-brede assert**: als 0% van de facetten resolvet, is de run stuk —
      nu wordt dat alleen naar de console geprint. #priority:low

### 2026-08-13 — Bot-analyse: ByteDance, legacy product-URL's en de WAF-challenge

Analysesessie, geen codewijziging. Alles uit de ruwe CloudFront-logs (13–28 juli +
12 augustus) en `pa.bothits_*`; lessen staan in LEARNINGS.

- [x] **"Moet ik de ByteDance-bot blokkeren?" — advies: nee.** Piek was mei (155.992
      hits/dag, 5,25% van alle bot-hits); laatste 7 beschikbare dagen 14.193/dag = 0,48%,
      0,2% van de category-crawl, 19,5 min origin-tijd op 12-08. Wat hij op de piekdag
      trok was voor 61% geen content (SVG-sprite 90.428×, `/jserrors`, `/data/graphql`) en
      voor 31% dode legacy product-URL's. Bovendien wordt **40,8% van Bytespider al door
      de WAF gechallenged**, dus er ligt al een rem op. `verify_state` is
      `unverifiable` (ByteDance publiceert geen IP-lijst), dus echt-vs-nep is daar niet te
      bepalen en een UA-blokkade raakt alleen het nette deel.
- [x] **Legacy product-URL's uitgezocht.** 224.242 unieke URL's / 256.506 hits over 17
      dagen; 3,0 mln hits over 147 dagen in de cube. **Nooit een echte 200** — 201.395
      URL's gaven alleen 404, 7.884 hebben een 3xx. Bots: Apple 131.926, Googlebot 63.302,
      ByteDance 34.903, OpenAI 11.074. Herkomst: 89% zonder referer (crawler-inventaris),
      en GSC geeft **nul impressies** op `/d\d{6,}/` — ze staan volledig uit de index.
- [x] **Nep-Googlebot gequantificeerd.** 3,35% van alle Googlebot-UA-requests (77.068 van
      2,3 mln) komt van IP's die falen op Google's gepubliceerde ranges. Daarvan 40,1% een
      202, 55,8% een 405, en **106 een echte 200 (0,14%)**. 67 /24-blokken van ~250 IP's
      elk, top-20 dekt 98,5%. **Echte Googlebot wordt niet geraakt** (2,22 mln verified
      requests, 0 challenges) — dus geen SEO-risico.
- [x] Opgeleverd in `Downloads\claude`: `waf_bot_bevindingen_20260813.txt` (actielijst),
      `nep_googlebot_ip_blokken.csv` (67 blokken met status-split),
      `legacy_product_urls_bothits_volledig.csv` (224.242 rijen).

**Openstaand — dit ligt bij wie de WAF beheert, niet bij ons:**

- [ ] **`/robots.txt` uitzonderen van élke WAF-regel** (path-exclusion `^/robots\.txt$`
      bovenaan de rule group). PRIORITEIT: nu komt 50% van de bots niet bij het bestand en
      concluderen die "geen crawlregels" — zie LEARNINGS. Dit blokkeert ook de
      Disallow-maatregel hieronder. Nameting: verwachting >99% status 200, 0× 202/403.
- [ ] **Uitvragen welke twee WAF-regels de 202/405-mix maken** en er één consistente
      response van maken (voorkeur 403 — een 202 is een succescode en een 405 op een GET is
      semantisch onjuist).
- [ ] **De 686 https-301's op `/robots.txt`** — waar 301't dat naartoe? Robots.txt hoort
      zonder hop geserveerd te worden.
- [ ] **403's heroverwegen**: YandexBot 486, ClaudeBot 91, facebookexternalhit 72. Dat
      laatste is de WhatsApp/FB-linkpreview; gedeelde beslist-links krijgen zo geen preview.
      Vermoedelijk onbedoeld.
- [ ] **`/sitemap*` meenemen in de bot-regel** — daar gaan die 106 doorgelaten 200's naartoe.
- [ ] **Pas ná de robots.txt-fix**: `Disallow` op `/jserrors`, `/data/graphql`,
      `/shoppingcart/header`, `/js/routing`, `/assets/beslist-frontend/svg/` + de sprite
      hard cachen aan de edge (90.428 origin-hits op één SVG op de piekdag).
- [ ] **Eigen URL-hygiëne**: de legacy `/…/dNNNNNN/`-URL's zijn 100% dood maar worden nog
      3,0 mln keer per 147 dagen gecrawld. Niets aan te doen aan de crawler-inventaris,
      maar wél uitzoeken waarom `boeken.beslist.nl` en `fok.beslist.nl` nog als referer
      opduiken.
- [ ] **Tripwire op `verify_state = 'failed'`** per bot-familie in het dashboard (nu 3,35%
      voor Googlebot). Signaal om op te acteren: 'failed' stijgt terwijl 202+405 daalt.
      Let op: rijen van vóór 2026-08-11 staan op `unchecked` en horen niet in de noemer.

### 2026-08-13 — Nachtelijke S3-ingest verhuizen naar een externe machine

Aanleiding: de bucket bewaart ~42 dagen, dus elke dag zonder ophaal kost permanent één
logdatum. Het gat 06-10 t/m 07-03 (24 dagen) is precies dat, en het groeide tussen 11 en
13 augustus nog met drie dagen.

- [x] **Uitgezocht waar de bestaande nachtelijke job draait**: een `threading.Timer` in
      het uvicorn-proces, en `_fire()` gaat **zonder `before=`** dus hij haalt niets uit
      S3 — alleen de dropfolder. `BOTHITS_AUTO_INGEST` aanzetten beschermt het venster
      dus niet. (Ik had eerder het tegendeel gezegd; rechtgezet.)
- [x] **Overdrachtsprompt geschreven** voor de Claude op de externe machine:
      `Downloads\claude\bothits_nachtelijke_ingest_PROMPT.txt` (301 regels, geen
      credentials — alleen de sleutelnamen). Bevat de drie valkuilen bovenaan, een
      kant-en-klaar `scripts/bothits_nightly.py`, de `schtasks`-regel plus de drie
      instellingen die schtasks niet kan zetten, en wat er teruggekoppeld moet worden.
- [ ] **Wacht op uitvoering op die machine.** Harde stop als hij `10.1.32.9:5432` niet
      kan bereiken — dat is een intern IP.
- [ ] **`scripts/bothits_nightly.py` bestaat nog niet in de repo.** Die Claude commit hem
      naar `main`; tot die tijd staat de inhoud alleen in de prompt.
- [ ] **Native-Windows pad, als hij die route neemt**: `ingest_date()` moet dan een
      spawn-context met `initializer` krijgen (per worker `load_url_ids()` +
      `load_ip_ranges()`). Nu vraagt hij hard `get_context("fork")` en dat bestaat daar
      niet.
- [ ] **Achterstand inlopen** zodra de taak loopt: kijken welke datums nog in S3 staan
      maar niet in de ledger. Eenmalig een ruimere run (~36 GB) — eerst overleggen.
- [ ] **Zet `BOTHITS_AUTO_INGEST` op Joeps machine niet aan** zolang de externe machine
      de ophaler is; twee planners op dezelfde datums is dubbel werk.

### 2026-08-13 — Audit Bothits: fase A t/m C uitgevoerd

Elf bevindingen over 4.005 regels (`a5ecefa`). Wat er af is:

- [x] **Het standaard datumbereik hing aan `pa.bothits_ingest`** — een procestabel. Het
      herstelrecept (ledger-rijen weggooien) verschoof daarmee stil het venster waarop de
      tool opent; tijdens de herlaad stond `max(log_date)` even op 2026-06-09. Nu uit de
      cube, net als `days_in_range` in `get_url_detail` (dezelfde fout, mijn eigen code).
- [x] **`run_backfill` negeerde Cancel** — de langste job van het systeem.
- [x] **Chart-lek** na een sorteerklik met een open paneel; opruimen zit nu in één
      `closeDetails()` die ook de render-functies aanroepen. Plus `escapeHtml` op
      `bot_family` en een dode conditie weg.
- [x] **`scan_tree` sloeg `_processed/` niet echt over** — en dit is een bevinding die ik
      eerst OMGEKEERD had gerapporteerd, zie LEARNINGS. De check vergeleek de basename, dus
      alleen die map zelf viel af en niet de datum-submappen: elke run schuimde het hele
      archief af (46.097 bestanden, +2.900/dag). Nu echt overgeslagen, met
      `backfill --date` als expliciet herstelpad dat er juist wél in leest.
- [x] **Staging-retentie**: `BOTHITS_STAGING_RETENTION_DAYS` (default 21, 0 = nooit).
      Verwerkte bronbestanden bleven eeuwig staan, ~900 MB per datum, 30 GB op de meetdag.
- [x] **Parser 4× sneller**: memo op de ruwe UA-string plus een unie-regex als snelle
      afwijzing. 0 verschillen op 117.492 echte logregels, end-to-end byte-voor-byte
      identiek (incl. md5 over alle cube-rijen), en 125s → **29s** per logdatum.
- [x] **Bevinding 8 is gefixt** in `53fcea2` — stond hier tot 2026-08-13 onterecht als open.
      160 `pa.urls`-rijen met `%`-encoding konden nooit matchen omdat de parser de logpaden
      `unquote()`t; `load_url_ids()` zet nu de gedecodeerde vorm als extra sleutel erbij
      (letterlijke sleutel wint). Geldt alleen voor datums die je daarna verwerkt.
- [ ] **`get_top_urls()` terug naar `pa.bothits_url_daily`** is géén omzetting maar een
      ontwerpkeuze. Gemeten op 30 dagen: **21,5s koud / 9,4s warm**, tegen ~1s op de
      huidige bron. Met de query omgebouwd (eerst aggregeren op `url_id`, dan pas `pa.urls`
      erbij voor de top 250) wordt het 4,5s. Onder een seconde alleen met een kleiner
      standaardvenster of een dagrollup.

### 2026-08-13 — Bot Hits opschonen: 12 families, twee tabs, en de URL's-tab herbouwd

Een lange ronde losse verzoeken van Joep (`e6d45cc`), plus twee dingen die onderweg
boven kwamen drijven.

- [x] **Families van 31 naar 12** via `pa.bothits_bot.is_tracked` (12 aan, 19 uit,
      gecommit in de DB). Keuze uit drie voorgelegde varianten: Google + grote AI +
      Applebot, later Bing erbij op verzoek. Totaal gaat van 95,1 naar 91,2 mln
      bot-hits (95,9%). Die vlag betekent nu twee dingen — zie BOTHITS_PROCESS.
- [x] **Drie tabs eruit** (URL's, Crawl-verspilling, Categorieën) met `/top-urls`,
      `/top-waste`, `/categories` en het al ongebruikte `/url`, plus hun
      service-functies (~220 regels). Ingest ongemoeid: de URL-tabellen worden nog
      gevuld, dus terugzetten is de endpoints uit git halen.
- [x] **URL's-tab teruggebracht op `pa.bothits_unknown_daily`.** Top 250 (gemeten,
      zie LEARNINGS), kop volgt de keuze, rij klapt open met een donut per
      bot-familie en de dagreeks van die URL. Nieuw endpoint `/url` levert beide.
- [x] **Palet opnieuw gemeten met de Base-kolom erbij**: acht benoemde families is het
      plafond (7,7 / 15,8), negen faalt op normaal zicht. Base-only haalt zelfs zes
      niet. Getallen en de les in UI_BLUEPRINT.
- [x] **Facet-diepte één reeks in één kleur**, in de grote grafiek en in het paneel; de
      staart-vouwing zit nu in `foldDepthTail()` en geldt voor beide.
- [x] **Hover-blokken noemen overal een percentage.** Bij één reeks is de noemer de hele
      reeks (voetregel heet dan "Hele reeks"), bij een stapel het dagtotaal.
- [x] **Tegels helemaal weg** — inclusief de cqw-clamp die er net in zat om de waarde
      te laten passen. Zie LEARNINGS voor waarom dat een maatprobleem was en geen
      uitlijnprobleem.
- [x] Kleiner: kruisje op de onvolledige-dagen-banner met Toepassen eronder ·
      dekkingstekst weg · Bot-familie als standaardsplitsing · kleurblokjes uit de
      familietabel · donuts in het paneel gekaderd · daglijn altijd lichtblauw ·
      Logs ophalen + Verwerk dropfolder outline-paars met ↻ · dagen-picker 50px.
- [ ] **`/summary` levert vier velden die niemand meer leest** (`total_hits`,
      `product_hits`, `catalog_hits`, `waste_pct`) nu de tegels weg zijn. Laten staan
      tot duidelijk is of er iets terugkomt dat ze gebruikt; anders opruimen.
- [ ] **De URL's-tab is een dagelijkse top-500 per familie en dus geen volledige
      ranglijst.** Zodra `pa.bothits_url_daily` weer vult, kan `get_top_urls()` terug
      naar die tabel en verdwijnt de beperking. De bron staat in de docstring.
- [ ] **DuckAssist (0,12%) heeft een eigen kleur en Amazon (0,11%) niet** — dat is de
      achtste plek op volume, en die grens is arbitrair tussen twee vrijwel gelijke
      families. Alternatief is zeven benoemen met de schone regel "≥ 0,4%".

### 2026-08-12 — SEO/GEO brainstormbord uitgelezen naar Excel + CPR/CPC-overzichtsquery

Aanleiding: Joep vroeg (1) waar `create GSD-campaigns.py` het CPC/CPR-onderscheid maakt en
een Redshift-query voor shopnaam + type, en (2) de onderwerpen van het SEO/GEO brainstormbord
in een Excel met vergelijkbare post-its onder elkaar, fases losgelaten.

- [x] **CPR/CPC gevonden in Python, niet in SQL.** `getRedShiftData` (regel ~1660) labelt op
      twee vlaggen uit `bt.shop_list`: `is_wecantrack_shop` of `is_pixel_shop` → CPR. Query
      geschreven en getest (`Downloads\claude\shop_model_cpr_cpc.sql`), teruggebracht tot
      `shop_name` / `shop_id` / `model` op verzoek. 1.461 GSD-shops.
- [x] **Live-check erbij op vraag van Joep**: maar **1.029 van de 1.461 is live**, en bij CPC
      is het 76 van 332 — de `is_gsd_*_shop`-vlaggen blijven staan nadat een shop offline
      gaat. Variant met live-filter in hetzelfde .sql-bestand. Cijfers in LEARNINGS.
- [x] **Brainstormbord: 50 post-its in 7 thema's** naar
      `Downloads\claude\SEO_GEO_brainstorm_onderwerpen.xlsx` (tabs `Onderwerpen` +
      `Samenvatting`, kleurcel gevuld met de echte post-itkleur, filter + freeze panes).
      Inventaris vastgelegd in `cc1/SEO_GEO_BRAINSTORM.md`, generator in
      `cc1/seo_geo_brainstorm_to_xlsx.py` zodat de Excel herbouwbaar is.
- [x] **De bron-PDF bleek kapot** (4 slices, ~200 px weg per naad, rechterkant afgekapt →
      11 onleesbare post-its). Compleet gemaakt met een afbeelding van het bord die Joep
      stuurde. Detectiemethode + waarom correlatie op witruimte misgaat: zie LEARNINGS.
- [ ] **Geen prioritering opgenomen** — op het bord staat alleen een TOP!-sticker op
      "Datagedreven trendrapporten". Kolommen voor prioriteit/effort/eigenaar zouden
      verzonnen zijn. Joep is gevraagd of hij een variant met scorekolommen wil; geparkeerd
      in BACKLOG samen met het overlap-onderzoek (welke onderwerpen bestaan al in het
      dashboard).
- [ ] **Twee blokjes boven de panelen blijven onleesbaar** (iets met "…samen groeien"). Ze
      vallen in zowel de PDF als de afbeelding buiten beeld; alleen op te lossen met een
      nieuwe export.

### 2026-08-12 — BUG: `is_known_url` staat op false voor alles ná de backfill

**Prioriteit: dit maakt vier onderdelen van Bot Hits onbruikbaar.** Gevonden doordat Joep
vroeg waarom de grafiek "Facet-diepte — bekend vs onbekend" geen onbekend toonde; het
bleek geen labelkwestie maar een datafout.

Wat er staat, gemeten op 2026-08-12:

| geïngest op | datums | hits met `is_known_url` |
|---|---|---|
| 2026-08-11 (backfill uit het archief) | 116 | normaal |
| 2026-08-12 (via de S3-ophaal) | 30 | **0** |

De 30 kapotte datums zijn 2026-07-13 t/m 2026-08-11 — precies het bereik waar de tool
standaard op opent. Bewijs dat het fout is en niet gewoon zo: URL's die aantoonbaar wél
in `pa.urls` staan (`/products/mode`, `/products/gezond_mooi`, `/products/huis_tuin`)
zijn in die dagen als onbekend geteld.

**Symptomen die hier allemaal op terug te voeren zijn:**
- Tegel "Categorie-URL's" zegt `0 daarvan in pa.urls`, tegel "Facet-verspilling" zegt
  100% (was 86,7%, zie [[bothits_dashboard]]).
- De facet-diepte-grafiek toont maar één kleur.
- **De tabbladen URL's, Crawl-verspilling en Categorieën geven "geen resultaten"** — die
  lezen `pa.bothits_url_daily`, en die tabel stopt op 2026-06-09.
  _(2026-08-13: die drie tabs zijn eruit. URL's is teruggekomen op
  `pa.bothits_unknown_daily`, die wél doorloopt — dat werkt alleen zolang de match kapot
  is, want dan valt praktisch elke gecrawlde URL in de "onbekende" tabel. Zodra dit
  gerepareerd is, kan `get_top_urls()` terug naar `pa.bothits_url_daily`.)_

**Wat het NIET is** (nagetrokken, zodat niemand dit opnieuw hoeft te doen):
- `pa.urls` is niet leeg of veranderd: 1.031.796 rijen, laatste instroom 2026-07-29, dus
  compleet toen die ingests draaiden.
- De lookup werkt: `load_url_ids()` levert 1.031.794 sleutels en `/products/mode` zit
  erin.
- Fork-overerving werkt: een `ProcessPoolExecutor`-child ziet de global van de parent.
- De huidige code werkt: `process_file()` op een echt logbestand geeft 708 bekende hits
  tegen 3.294 onbekende.

> **OPGELOST 2026-08-13 — het was de start-methode van de worker-pool** (`a2ee990`).
> `uvicorn --reload` start de app zelf via een **spawn**-context en een child erft die
> default, dus `ProcessPoolExecutor` in de server spawnde in plaats van te forken. Elke
> worker importeerde de module opnieuw en begon met een **lege `URL_IDS` én `IP_RANGES`**.
> Eén oorzaak, beide symptomen: `known_rows = 0` (elke URL leek onbekend) en
> `verify_state = 'unchecked'` voor álles. Dat tweede was het bewijs — twee
> onafhankelijke globals die op dezelfde runs falen, terwijl de backfill-datums netjes
> `verified`/`failed` dragen.
>
> Daarom was alles wat hier onder "wat het NIET is" staat óók echt niet fout: die tests
> liepen in een SCRIPT, en daar is de default fork. De code was nooit stuk in de
> omgeving waarin hij getest werd. Fix: expliciete fork-context. Gemeten op 2026-08-12,
> dezelfde 2.905 bestanden: server vóór 0 · script 142.054 · server ná 142.054.

- [x] **Herlaad de 30 datums 2026-07-13 t/m 2026-08-11.** Loopt sinds 13-08 12:50, buiten
      de server (los proces, want `uvicorn --reload` breekt bij elke .py-edit af).
      87.228 bestanden / 29,8 GB, ~2½ uur. Eerst één gratis probe gedaan met 12 augustus
      — die stond nog niet in de ledger — en dáár bleek de bug nog live: 30 datums
      ophalen zou 30 GB hebben gekost voor 30× hetzelfde kapotte resultaat.
- [x] **Reproduceert het? Ja**, en dat was precies de observatie die de oorzaak gaf. Deze
      keer bleven de bronbestanden staan (`~/bothits_s3/_processed/<datum>/`), dus het was
      naast de server na te spelen — en daar werkte het, wat het verschil aanwees.
- [x] **Tripwire ingebouwd**: >100k bot-hits met nul bekende URL's is nu een ERROR in het
      log met de vermoedelijke oorzaak erbij.
- [ ] **De tripwire is alleen een logregel.** Dat helpt alleen wie kijkt. Een vlag in
      `pa.bothits_ingest` zou de Ingest-tab de verdachte datum kunnen laten tonen.

### 2026-08-12 — Bot Hits: merkkleuren, breedte, annuleren, eigen statussectie

Vier punten van Joep in één ronde (`2f120ec`).

- [x] **Paginabreedte gelijk aan SEO Stats** — in twee stappen, want de eerste was niet
      goed. `container-fluid` + eigen max-width 1500px werd eerst een blote
      `.container` (`2f120ec`), en dat rapporteerde ik als "gelijk per constructie" op
      grond van dezelfde klasse en dezelfde BS-versie. Joep vroeg het na; gemeten was
      het 1194px tegen 1304px. SEO Stats zit in het blueprint-skelet
      `container > row > col-lg-11 mx-auto`, en een blote container is precies zo'n vorm
      die de blueprint verbiedt. Rechtgezet in `16ff551`, nagemeten op de gerenderde
      randen bij 1280/1400/1600px: overal identiek. Zie LEARNINGS voor de les én voor
      de meetfout die ik onderweg maakte.
- [x] **Eerste drie legendakleuren = lichtblauw/roze/lichtgroen** in bot_family,
      bot_class, host en url_type. Overige slots opnieuw gezocht met de validator.
      url_type wordt er béter van (CVD 2,7 → 5,9); bot_class en host zakken van 10,0
      naar 7,7, wat de bodem van de merktrio zelf is. Details in UI_BLUEPRINT.
- [x] **Nieuwe hue `#c7706b`** voor bot_family, omdat acht reeksen mét de trio niet
      onder het bestaande palet passen (beste was 4,9 ΔE). Joeps keuze tussen "familie
      naar Overig" en "tint toevoegen". Enige kleur in deze codebase die in Bot Hits
      bestaat en niet in SEO Stats.
- [x] **Cancel-knop** op de ophaalactie, coöperatief: tussen bestanden bij downloaden,
      tussen logdatums bij verwerken. Getest in beide fases plus de vlag-hygiëne.
- [x] **Status + balk + Cancel in een eigen `card-body`-sectie onder Filters**, verborgen
      tot er iets te melden is. Ophaalknop blijft in de Filters-kop.
- [ ] **Grijs ↔ lichtblauw staat op 12,0 normaal (vloer is 15) en dat is pre-existing.**
      Niet op te lossen met een seriekleur — alleen door de Overig-band anders te doen
      (andere neutrale tint, of arcering i.p.v. kleur). Geldt in Bot Hits én in
      `dashDonutUrlType` van SEO Stats, waar hetzelfde paar al genoteerd staat.
- [ ] **Nog niet met een echte ophaalactie gezien**, net als de balk zelf: annuleren is
      getest met een nagebootste S3-client, niet op 924 MB.

### 2026-08-12 — Bot Hits: voortgangsbalk bij "Nieuwe logs ophalen"

Aanleiding: Joep zag alleen `Bezig sinds … — fetch: download 2026-08-09: 2904 bestanden,
924 MB…` staan en vroeg om een balk (`9358121`).

- [x] **De oorzaak zat in de backend**, niet in de opmaak: `progress()` vuurde één keer
      per logdatum, vóór de download. `fetch()` plant nu eerst en downloadt daarna, zodat
      het totaal vóór de eerste byte bekend is. Geen extra S3-calls — dezelfde
      `list_date()`, een fase eerder.
- [x] **Tellers elke 25 bestanden** naar `_ingest_state["fetch_progress"]`, dat
      `/ingest/status` al doorgaf. Balk loopt op **bestanden**, niet op bytes (zie
      LEARNINGS voor waarom de hervattingstak dat afdwingt).
- [x] **Twee standen** volgens UI_BLUEPRINT: bepaald tijdens downloaden (poll 1,5s),
      onbepaald tijdens verwerken doordat de tellers bij de faseovergang gewist worden.
      Inline variant zonder Cancel — een lopende download is niet af te breken.
- [x] **Getest met een nagebootste S3-client** (geen 924 MB getrokken): noemer laat al
      geïngeste dagen buiten beschouwing, tellers monotoon tot exact het totaal, mislukte
      downloads blokkeren de balk niet, faseovergang wist op het juiste moment.
- [ ] **Nog niet met een echte ophaalactie gezien.** De logica is end-to-end getest maar
      de eerstvolgende echte run is het bewijs; als de balk raar doet, kijk eerst of
      `/ingest/status` een `fetch_progress` teruggeeft.

### 2026-08-12 — SEO Stats: WoW-delta per slice in de donut-hovers

Aanleiding: Joep vroeg de WoW-delta (procentueel) in de hover van "Type urls - Visits",
en als het kon ook in "Apparaten - Visits" en "Apparaten - Omzet". Alle drie gedaan
(`237bff6`).

- [x] **Backend**: `_fetch_device_split` en `_fetch_urltype_split` draaien er ook voor
      d-7 bij; `_as_distribution` krijgt `prev_raw` en zet een `wow` per bucket. Drie
      extra Redshift-queries op een cache-miss, niets op een hit. Niet af te leiden uit
      `daily` — dat is een dagtotaal per metriek zonder device- of url-type-dimensie.
- [x] **Definitie**: procentuele verandering van de **waarde** van de slice, niet van
      zijn aandeel, zodat het dezelfde bewerking is als de tegels erboven. Staat in
      UI_BLUEPRINT, inclusief waar de twee lezingen uit elkaar lopen.
- [x] **Frontend**: delta van de chart-instance af (`$dist`) in plaats van uit de
      label-callback, eigen kleurenpaar voor het donkere blok, en een horizontale clamp
      omdat de tip breder werd. Patronen in UI_BLUEPRINT, valkuil in LEARNINGS.
- [x] **`Apparaten - Omzet` krijgt het `*`-markertje** van de omzet/OPB-tegels: de
      per-device delta's daar vergelijken een dag die nog vult met een dag die een week
      geleden klaar was.
- [x] **Nagerekend op 11-08 vs 04-08**: elke donut telt op tot de kop-tegel en de
      gewogen slice-delta's komen uit op de headline (visits −6,1%, omzet −35,5%).
      Randgevallen apart getest (ontbrekende bucket → `n/a`, nul → `0%`, vaste
      slice-volgorde, clamp).
- [ ] **Openstaande vraag voor Joep**: wil hij er ook de verschuiving van het *aandeel*
      in procentpunten bij? Nu staat alleen de volumedelta erin. Bij een dag waarop
      alles met dezelfde factor beweegt, zegt de volumedelta niets over de mix.
- [ ] **`keys.txt` is nu genegeerd** (`21c7fef`) maar staat nog wel op schijf met echte
      AWS-keys erin. Prima als lokale kladlijst; wel iets om te weten bij het delen van
      de map of een backup.

### 2026-08-11 — Bot Hits: 19 visuele punten uit suggestions_new.txt

Aanleiding: Joep's lijst met visuele aanpassingen (regels 1-20). Alles gedaan; de
patronen staan in UI_BLUEPRINT, de twee bugs in LEARNINGS.

- [x] **Overgenomen uit andere tools**: hover-blok + daggrafiek-chrome + datumvelden
      (SEO Stats), tabs (Canonicals), tegels (GSD Budgets), outlined uppercase labels
      (DMA Exclusions), donut-layout (SEO Stats), footer.
- [x] **Eigen keuzes bijgeschreven in UI_BLUEPRINT**: de eerste-drie-kleuren-regel
      (lichtblauw `#1f99c4` / roze `#be4693` / lichtgroen `#91c34e`, met ΔE), stat-tiles,
      hover-blok, klikbare legenda boven het plot, en dat de alpha-ladder niet voor
      gestapelde reeksen geldt.
- [x] **Donut (punt 22-23, na Joeps feedback)**: de directe labels zijn er weer uit —
      te veel chrome rond een kleine ring. In plaats daarvan `donutMinAngle`, die elk
      niet-leeg segment op ~1,8° trekt zodat Homepage (0,2%) en R-url (0,0003%) een
      zichtbare, hoverbare lijn zijn; `spacing: 0`, want 2px vreet zo'n boog op.
      Bewuste vertekening, de tooltip noemt de echte aantallen. Facet-diepte: gelijke
      hoogte als de donut-kaart (die 60px witruimte was `h-100` + ongelijke plots) en
      de staart 7..11 (967 hits) datagestuurd samengevouwen tot "7+".
- [x] **URL-type naar zes buckets** (R-url / C-url / PLP / Cat-url / Homepage / Overige)
      in de QUERYLAAG, met de prioriteit van `seo_stats_service._urltype_case()`. Geen
      re-ingest nodig; ruwe types blijven in de cube.
- [x] **Layout**: tegels boven Filters, "In pa.urls"-filter weg, Reset/Toepassen
      rechtsonder, ophaal-knop naast de dekkingstekst, infotekst compacter.
- [x] **Per bot-familie**: alle headers weer sticky (de `position: relative`-bug),
      Soort-kolom gecentreerd, rijen uitklapbaar met drie grafieken per familie
      (donut URL-type + donut Domein + staaf Facet-diepte) uit `/summary?bot_family=`.
- [x] **Hover-info**: aandeel per regel, met het dagtotaal als noemer en uitgezette
      banden eruit.
- [ ] **`position: relative` op `th.sortable` staat nog in zes andere tools**:
      gsd-campaigns, gsd-tag-toppers, gsd-check, shop-campaigns, mc-id-finder,
      seo-titles. Daar scrollen de sorteerbare headers dus ook weg. `_tool-template.html`
      is al gefixt, dus nieuwe tools erven het niet meer.
- [x] **Punten 23-27 (tweede feedbackronde)**: "Invalid Date" in het uitklappaneel — het
      hover-blok van de daggrafiek las de x-as-titel als datum, en die is daar "2 facets";
      `weekdayLongOf` eist nu een ISO-datum en de sub-caption "hits per dag" verschijnt
      alleen bij een datum-as. Verder: de inforegel boven het paneel weg, kadertje om de
      facet-diepte-grafieken, de rij-hover van Bootstrap uitgezet op de detailrij (die
      pakt élke tbody-rij, ook een paneel zonder klikbare inhoud), en het pijltje voor
      "Nieuwe logs ophalen" eruit.
- [x] **Punten 28-30 (derde ronde)**: de facet-diepte-grafiek was de laatste met de
      standaard Chart.js-tooltip → nu hetzelfde donkere blok en dezelfde chrome.
      Reset-knop in Filters weg (Toepassen blijft: de checkbox-lijsten laden niet
      automatisch). Shop Campaigns' presets eindigen op vandaag − 3, en de begin-range
      bij het openen is meegetrokken zodat de pagina opent op wat "30d" geeft.
- [ ] **"All"-preset in Shop Campaigns eindigt nog op vandaag.** Zelfde staart van
      onvolledige dagen als de andere presets hadden, maar "alles" dat op −3 stopt is
      ook vreemd. Joeps keuze.
- [ ] **`ymd()` in seo-stats.html gebruikt `toISOString()`.** Dat schuift in CEST een
      dag terug bij middernacht. Latent, want flatpickr krijgt daar Date-objecten, maar
      het is dezelfde off-by-one die mij in een testformatter beet. Shop-campaigns doet
      het goed met lokale datumdelen.
- [ ] **Visueel nog niet beoordeeld.** `node --check` is groen en de endpoints geven de
      juiste data, maar er is hier geen browser: of de donut-labels binnen de kaart
      blijven en of het uitklappaneel op een smal venster niet knelt, moet iemand met
      een browser bekijken.

### 2026-08-11 — Bot Hits: IP-verificatie, en alleen nog beslist.nl

- [x] **`backend/bothits_verify.py`**: IP's toetsen aan de officieel gepubliceerde
      ranges. Alleen de `RANGE_SOURCES`-tabel uit `~/bothits_verify.py` overgenomen,
      aangevuld met bingbot/Applebot/Google user-triggered. Geen rDNS (onnodig: de vier
      grootste families matchen 100% op range), geen keep-set die data weggooit.
      `verify_state` is een dimensie op de cube met verified/failed/unverifiable/unchecked.
- [x] **Gemeten na re-ingest van 2026-03-10**: verified 87,15%, unverifiable 12,51%,
      failed **0,35%** (OpenAI 12.570, Googlebot 85, Anthropic 67, Bing 4). Ingest-tijd
      onveranderd (66 s), cube nog 10 MB. Zichtbaar als splitsing "IP-verificatie".
- [x] **Domeinfilter naar een keep-list**: `BOTHITS_KEEP_DOMAINS`, default `beslist.nl`.
      beslist.be, shopcaddy.de en de shop.*-varianten eruit (43.525.762 hits = 45,08%,
      790,8 GB verkeer, ~169 MB). Bestaande rijen ook verwijderd, want een reeks die
      halverwege van samenstelling verandert leest als een verkeersval. **Terugzetten
      staat in BOTHITS_PROCESS.md** — env-var plus re-ingest, en dat kan alleen zolang
      `BOTHITS_BACKUP_DIR` bestaat: dat archief is de enige kopie van BE/DE feb–juni.
- [ ] **`VACUUM FULL` overwegen.** De 169 MB is vrije ruimte binnen de tabellen, nog
      niet teruggegeven aan het OS. Bij ~27 MB/dag groei is het binnen een week
      hergebruikt, dus alleen doen als de DB-omvang nu knelt (exclusive lock).
- [x] **Alle 116 datums hebben een verdict** (2026-08-11): verified 88,01%,
      unverifiable 11,11%, failed 0,88%.

### 2026-08-11 — Bot Hits haalt zijn eigen CloudFront-logs uit S3

Aanleiding: Joep wees op `~/projects/cloudfront-logs/download_cloudfront_logs.py` en wilde
die stap in de tool, bij de Refresh-knop van 'Hits per dag'. Zie BOTHITS_PROCESS.md.

- [x] **`backend/bothits_s3.py`**: `preview(days)` (list-only, per datum files/MB/uren +
      reden) en `fetch(days)` (parallelle download naar `BOTHITS_S3_DIR`, overslaan wat er
      met dezelfde grootte al ligt). Listen per (distributie, datum)-prefix; distributies
      uit `Delimiter="."` in plaats van hardcoded.
- [x] **`start_ingest_async(src=, before=)`**: download en ingest onder hetzelfde lock, met
      `phase` + `fetch` op de status zodat de UI de downloadfase kan tonen. Bestaand gedrag
      van de Verwerk-knop en de nachtelijke timer ongewijzigd.
- [x] **Endpoints** `GET /api/bothits/s3/preview` en `POST /api/bothits/s3/fetch`
      (`days` 1-45), 400 als de credentials missen. Status via de bestaande `/ingest/status`.
- [x] **UI**: dagen-input + "⤓ Nieuwe logs ophalen" náást Refresh in de kaartkop van Hits
      per dag, met confirm die files/MB/datums quoot, een poller die ook een elders gestarte
      run oppikt, en `refresh(true)` als hij klaar is.
- [x] **Getest**: preview tegen de echte bucket (3 dagen = 8.683 files / 2.705 MB), 10 echte
      keys gedownload (0,8 s, geldige gzip, `#Version: 1.0`), `scan_tree` leest de datum uit
      de bestandsnaam, en de hele keten met een stub voor de 900 MB — de 24-uur-poort weigerde
      de halve dag correct (`incomplete (3/24 hours)`) en `on_done` leegde de cache. Endpoints
      over HTTP OK na herstart (PID nieuw, 11-08 10:54), `days=0`/`days=99` → 422.
- [x] **`boto3` in `requirements.txt`** en in de venv geïnstalleerd (1.43.68); credentials in
      `.env` als `BOTHITS_S3_*`, niet in de repo.
- [x] **Backfill gedaan (2026-08-11)**: 92 datums (niet 85 — binnen het Feb-Mrt-venster
      ontbraken 03-08, 03-09 en 03-11 ook), 187.536 bestanden, ~1u50, **0 mislukte datums**.
      Daarna de redo van de 23 datums van vóór die dag, zodat alle 116 een `verify_state`
      hebben. Eindstand: 116 datums 2026-02-14 t/m 06-09, 15.004.598 rijen, 2,41 GB
      (10,0% van de database). Cijfers en groeiprognose in BOTHITS_PROCESS.md.
- [x] **Ledger en cube kloppen weer op elkaar** (307.544.182 = 307.544.182, 0 datums met
      verschil). Dat lukte niet in één keer: 2026-03-10 bleef 1.518.585 hits schelen, omdat
      die om 11:46 opnieuw was geladen om `verify_state` te testen — vóórdat de keep-list
      bestond. Zijn ledger-rij telde dus nog alle domeinen terwijl de cube-rijen later met
      de hand waren opgeruimd, en hij viel buiten de redo-lijst omdat zijn `verify_state`
      al gezet was. **Les voor een volgende handmatige DELETE: `bot_lines` in de ledger
      wordt alleen door een ingest herschreven, dus die loopt uit de pas.**
- [ ] **`failed` is (nog) geen historische spoofing-maat.** Over 116 dagen: 2.700.043
      failed hits (0,88%), waarvan OpenAI 1.995.111 en Googlebot 689.241. Per week bekeken
      is OpenAI structureel 4-12% en stijgend, maar Googlebot puntvormig: 4,59% in de week
      van 16 feb en 0,7-1,9% in mei/juni, elders 0,00-0,03%. Dat past beter op **verlopen
      IP-ranges** dan op spoofing — we toetsen logs van feb-juni aan de lijst van vandaag,
      en Google rouleert. Als tripwire op VERSE logs is het bruikbaar; historisch niet.
      Wil je dat wel: de opgehaalde `ipranges.json` per dag wegschrijven i.p.v.
      overschrijven, dan kun je achteraf tegen de ranges van toen toetsen.
- [ ] **Dagelijks ophalen aanzetten** zodra de backfill staat. De bucket bewaart ~42 dagen,
      dus zonder dagelijkse run verdwijnt er permanent historie. Nu staat
      `BOTHITS_AUTO_INGEST=false` en die timer draait alleen `run_drop()` op de dropfolder —
      voor S3 moet die kant ook de fetch aanroepen (`before=`), of de knop wordt handwerk.
- [ ] **20 dagen zijn definitief weg**: 2026-06-10 t/m 06-29 zit niet in S3 (retentie) en
      niet in het archief. Als die gaten storen in de grafiek: annoteren, niet zoeken.

### 2026-08-11 — Eerste BE-content live: FAQ op /products/elektronica/

Aanleiding: Joep vroeg of we een FAQ op een beslist.be-pagina kunnen maken, en of de upload
een country-veld heeft. Dat heeft hij (`country_code`, enum BE/NL/DE), en de .be-frontend
vraagt de FAQ al op. Zie LEARNINGS 2026-08-11.

- [x] **6 FAQ's gegenereerd tegen het BE-assortiment** (`countryLanguage=be-nl` op
      productsearch, 30 producten, 15 met ≥2 shops als linkbron). Zelfde productie-prompt uit
      `faq_service.build_faq_prompt`, met `www.beslist.nl` → `www.beslist.be` geswapt, óók in
      de voorbeeld-URLs. Alle 6 links geverifieerd: HTTP 200 op .be, geen redirect, elk
      product met live BE-aanbod (2–10 shops). Los script in de scratchpad, JSON-kopie in
      `Downloads\claude` (`be_faq_elektronica_20260811.json` + `..._records_20260811.json`).
- [x] **Naar productie gepusht**: `POST /faq` → 200, `{"records":6}`, ids 2290437-2290442.
      `GET ?country_code=BE` geeft 6 rijen, `country_code=NL` nog steeds zijn eigen 6
      (626155-626160) — de country-scope van de upsert-key houdt dus stand. Terugdraaien kan
      met `DELETE /faq?url=/products/elektronica/&country_code=BE`.
- [x] **Bewust NIET in de DB gezet.** `/products/elektronica/` bestaat al als NL-rij in
      `pa.urls`; BE-content in `pa.faq_content_v2` zou meeliften op de gewone NL-publish.
- [ ] **Rendering op de .be-pagina bevestigen.** Stond bij het afsluiten nog op
      `faq(...): []` en geen `FAQPage`-schema, met een CloudFront-object dat alleen ouder
      werd (`age` 203 → 503s). Querystrings worden genegeerd, dus busten kan niet van buiten:
      wachten tot het object verloopt, of een invalidatie voor `/products/elektronica/` laten
      draaien door iemand met toegang tot die distributie.
- [ ] **`faq_v2_publisher` multi-market maken** als dit breder uitgerold wordt. Drie dingen:
      `country_code` niet meer hardcoden (regel ~203), `pa.faq_v2_push_state` van
      `(url_id, env)` naar `(url_id, env, country)` — anders overschrijven een NL- en een
      BE-push elkaars md5 en slaat de volgende `mode="new"`-run URL's onterecht over — en een
      BE-URL-set, want `pa.urls` kent geen land.
- [ ] **Docstring van `faq_v2_publisher` bijwerken.** Die zegt dat `GET /faq` alleen een
      exacte url accepteert en dat er geen list-all is; inmiddels bestaan wildcards (`*`/`%`),
      `limit` (max 1000) en een `country_code`-filter.

### 2026-08-11 — SEO Stats: Bounce-delta kleurde groen bij een stijging

Aanleiding: Joep zag in Dagoverzicht een groene delta op de Bounce-tegel, terwijl een
stijgende bounce slechter is. Zie LEARNINGS 2026-08-11.

- [x] **`pctBadge()` heeft een `invert`-parameter** (`frontend/seo-stats.html`). Positief →
      rood, negatief → groen, teken en waarde ongewijzigd. Nul en n/a blijven grijs.
- [x] **Beide Bounce-tegels gedraaid**: Dagoverzicht via de `badgeHtml`-ingang van `dashTile`
      (met eigen null-check) en de samenvattingsrij via `LOWER_IS_BETTER.has(key)`, want
      `ORDER` bevat `seo_bounce`.
- [x] **`HEAT_INVERT_KEYS` opgeruimd** ten gunste van één `LOWER_IS_BETTER`-set, die zowel de
      badges als de heatmap-fade in de dagtabel voedt.
- [x] **Geverifieerd**: `node --check` op het inline script OK, `pctBadge` doorgemeten op 8
      gevallen (niet-geïnverteerd gedrag ongewijzigd), en :8003 serveert het nieuwe bestand al
      — statisch bestand, dus alleen Ctrl+Shift+R, geen herstart.

### 2026-08-10 — Content Publishing: de "Last push"-tegel stond stil op 05-08

Aanleiding: Joep meldde dat Kopteksten én FAQs `Last push: 05-08-2026 10:58` toonden terwijl
er net gepusht was. Oorzaak: het endpoint las alleen `pa.publish_log`, die alleen de
volledige batch-publish vult. Zie LEARNINGS 2026-08-10.

- [x] **`/api/content-publish/last-push` herschreven** (`backend/main.py`). Neemt nu
      `content_type` (`koptekst`|`faq`), pakt het maximum van de batch-log én de bijbehorende
      `*_push_state.pushed_at`, guard met `to_regclass` zodat een nog niet aangemaakte
      state-tabel de tegel niet 500't, en geeft de tijd tz-aware terug (`timezone.utc`) zodat
      de browser hem niet 2 uur te vroeg toont.
- [x] **Frontend gesplitst**: `app.js` vraagt `?content_type=koptekst`, `faq.js`
      `?content_type=faq`. Tot nu quootte de FAQ-kaart de koptekst-batch.
- [x] **Restart-taak van vanmiddag afgevinkt** (de `canon_key`-fix hieronder): uvicorn draait
      als PID 590, gestart 18:35, ná de commit van 18:10 — die fix is dus al live.

- [x] **Backend herstart om 23:22** (PID 6600), nadat de FAQ-publish om 23:20:56 leegliep
      (`urls_pending=0`). Endpoint geverifieerd: `koptekst` → `16:32:33+00:00` (18:32 lokaal,
      `source=incremental`), `faq` → `21:20:56+00:00` (23:20 lokaal), zonder parameter nog
      steeds koptekst, `content_type=bogus` → 400. Let op: de startup hangt ~30s op de GSD
      LL Excel-retry (Windows-pad onbereikbaar), dus :8003 antwoordt niet direct na relaunch.
- [ ] **Overwegen: incrementele publishes ook in `pa.publish_log` loggen.** Nu is die tabel
      alleen batch-historie, terwijl de naam suggereert dat het de volledige push-historie is.
      Eén rij per incrementele run zou de tegel én een echte historie geven (de state-tabel
      kent alleen de laatste push per url, geen runs).
- [ ] **Env-scoping van de tegel.** Hij staat naast de Environment-selector maar rapporteert
      altijd `production`; wie naar staging pusht ziet dus een productiedatum. FAQ-staging
      staat bijvoorbeeld nog op 03-08 13:05. Ofwel de selector doorgeven, ofwel het label
      "(production)" laten zeggen.

### 2026-08-10 — SEO/Unique Titles: type-facet staleness en de blueprint-keys

Aanleiding: Joep zag `Vlinderkasten vogelhuisjes` in Unique Titles en vroeg of
`s_dierenhuis` een type-facet is. Dat is het (`is_type_facet=true`, order 484) — de titel
was stale, van vóór de facet_order-import van 19-05-2026. Zie LEARNINGS 2026-08-10.

- [x] **`s_dierenhuis` als type-facet bevestigd** voor beide tools. `seo_titles_service.
      load_rules()` en `ai_titles_service._type_facet_override_by_slug()` lezen dezelfde
      `pa.facet_position_rules`, en de slug zit niet in `_NEVER_URL_SLUGS`.
- [x] **71/72 unique titles geregenereerd** via `process_single_url(url, True)` (niet via
      `status='pending'` — de worker slaat gevulde rijen over). 44 H1's gewijzigd, dubbele
      categorie weg. Snapshot: `Downloads\claude\unique_titles_s_dierenhuis_before_20260810.csv`.
      1 URL faalt op `facet_not_available` (dode `merk~23814784`).
- [x] **Blueprint-audit: alle 85.608 rijen hercompileerd** met de huidige regels. 84.906
      byte-identiek; de 702 afwijkers zijn hand-edits, niet stale output. De type-facet-fout
      zit dus níet in de blueprints (allemaal gebouwd 6–31 juli, ná de 19-05 import).
- [x] **454 duplicaatrijen verwijderd** — 450 combo's stonden 2–3× onder verschillende
      key-spellingen, met byte-identieke content. Canonieke rij behouden waar aanwezig (441),
      anders de alfabetisch eerste (9). 85.154 over, 0 dubbele combo's. Rollback:
      `Downloads\claude\seo_titles_blueprints_dedup_deleted_20260810.csv`.
- [x] **`canon_key()` in `upsert_blueprint_built()`** zodat de UI-route geen ongesorteerde
      keys meer kan wegschrijven. `update_blueprint()` bewust ongemoeid: schrijft de key
      niet, en zijn `WHERE key=%s` moet de nog-ongesorteerde rijen editbaar houden.
- [x] **13 nieuwe combo's aangemaakt** uit Joeps 33-URL-lijst, `status='built'`, niet
      gepubliceerd. 12× cat 9003879 (Insectenhotel), 1× cat 9001466 (Vogelhuisjes,
      `kleur~materiaal~merk~s_dierenhuis` — correct zónder `!!sub_category!!`).

- [x] **Backend herstart, `canon_key`-fix is live.** Uvicorn draait als PID 590, gestart
      18:35 — ná de commit van 18:10, dus `/api/seo-titles/create-built` canonicaliseert.
- [ ] **De 13 built blueprints publiceren** met `publish_built(env="production")` — zij
      zijn nu de enige `built` rijen, dus dat pusht precies deze set.
- [ ] **551 ongesorteerde keys** blijven staan (canonicalisatie bewust overgeslagen door
      Joep, 2026-08-10). Functioneel ok: de dedup canonicaliseert bij vergelijking.
- [ ] **15 misvormde blueprints opruimen**, allemaal live gepusht: 3 met een placeholder
      die niet in de key zit (`!!soort_hals_trui_shirt!!` in cat 9000727, `!!inhoud!!` in
      9005311/9005316) en dus nooit gesubstitueerd wordt; 5 met geplakte placeholders
      (`!!doelgroep_schoenen!!!!sub_category!!`); 8 met spatie voor/achter of dubbele
      spatie. Ook cat 9004736 heeft `dekens` hardcoded i.p.v. `!!sub_category!!`.
      Fixen vergt `status='built'` + gerichte herpush.
- [ ] **Steekproef op de 126.358 pre-19-05 titels** die op een type-facet-URL staan. Niet
      allemaal fout — alleen waar de categorienaam ook echt werd aangeplakt. Voorstel: 500
      random URLs regenereren en tellen hoeveel H1's veranderen, dán beslissen over de
      volle sweep.
- [ ] **Weeskeys in `/page-titles`**: 1.005 ongesorteerde keys zijn al gepusht en deze tool
      heeft geen DELETE tegen die API. Uitzoeken bij de eigenaar of het live systeem bij
      lookup canonicaliseert; zo niet, dan staan er dode page-titles.
- [ ] **`merk~23814784`** is een dode taxonomy-facetwaarde die minstens 2 URLs raakt.
      Taxonomy opschonen of die URLs uit de queue halen.

### 2026-08-10 — GSD Tag Toppers: de drie fouten uit run #14, en de tegels

Aanleiding: Joep vroeg waarom er vrijdag geen campagnes waren aangemaakt. Antwoord: de
runs met `add_only_kandidaten` (620 rijen, `campaigns_to_create=61`) stonden **beide op
dry-run**; de enige echte run die dag was #8 met een ander bestand. Daarna run #14
(10 aug, echt): 47 campagnes aangemaakt, 13 mislukt, 34 foutrijen.

- [x] **Tegel "Campagnes aan te maken" → "Campagnes aangemaakt"** bij een echte run.
      Nieuw veld `campaign_created` (0/1) per rij, pas 1 als `_create_tag_toppers_campaign`
      echt iets teruggeeft; `campaigns_to_create` telde rijen met `campaign_action ==
      "aanmaken"` en dus ook de mislukte. In run #14 zou de tegel 61 hebben getoond bij
      47 gelande campagnes. Tegelfilter volgt mee; oude runs zonder het veld vallen terug
      op de oude teller.
- [x] **Tegels lopen live mee tijdens een run.** `_state["summary"]` werd alleen in het
      `finally` gezet terwijl `_state["results"]` al per rij bijwerkte — vandaar een
      vullende tabel met tegels op 0. Berekening zit nu in `_summarize()` en draait ook in
      de lus; `/progress` stuurt de samenvatting mee, zodat de tegels elke tick (1,5s)
      bijwerken zonder de tabel te hertekenen (die blijft op elke 4e tick).
- [x] **Boomwortel via `_mutate_with_retry`** — ging er rechtstreeks omheen, dus geen retry
      op CONCURRENT_MODIFICATION. 14 foutrijen. Nu mét retry, en met de eis dat beide ops
      landen.
- [x] **Convert leest partial failures uit** in plaats van blind `resp.results[1]`. Dat
      leverde negatieve units zonder parent op → misleidende `The required field was not
      present`. 14 foutrijen.
- [x] **Merk- en producttype-uitsluitingen werken** — `_read_campaign_tree` leest nu alle
      zeven dimensies, `_level_spec()` leidt het niveau af uit de siblings, en één
      `_set_case_value()` schrijft alle vormen. Bevestigd met `validate_only` op de echte
      Koffiestore-boom. Voor/na: 169 van 179 ad groups identiek, 10 omgeslagen van append
      naar convert (allemaal merk).
- [x] **Herstart + live** — backend draait weer op :8003 (zonder `--reload`, dus deploy is
      kill + relaunch), dashboard bereikbaar.

- [x] **Campagne zonder boom herstelt zichzelf** — `_ensure_tag_toppers_tree()` bouwt ad
      group (indien weg), boomwortel + item-id OTHERS en de ontbrekende shopping ad, in
      plaats van elke run `geen listing-tree gevonden` te melden. Run #14 liet er 12 achter
      (6 NL / 5 BE / 1 DE, allemaal PAUSED met 0 criteria en 0 ads). Ops serverside
      geaccepteerd met `validate_only` op de echte lege ad group van Kalenderwinkel
      (199154634477).
- [x] **Convert retryt op partial-failure-regels** (`CONVERT_RETRIES`) — CONCURRENT_MODIFICATION
      komt daar als respons-regel binnen en belandt in `retryable`, waar mijn eerste fix niet
      naar keek; vandaar de kale melding `subdivision niet aangemaakt`. Trof steeds de eerste
      convert in een ad group (bij VidaPlayer én Kalenderwinkel bleef `nd_c` als enige staan).

- [x] **Item-id-niveau wordt opgezocht in plaats van aangenomen** — `_plan_tag_toppers_adds`
      hing ids blind onder de root. Notino's `[label_test]`-campagne heeft daar eerst een
      custom-attribute niveau, dus alle 1105 ids werden afgekeurd op `Dimension type`. Nu
      `parent` in plaats van `root`, met een melding bij nul of meerdere kandidaten.
      `validate_only`: Notino OK via het diepere niveau, Cameranu (normale boom) onveranderd.
- [x] **Rij-retry op transportfouten** (`ROW_RETRIES` + `_is_transient`) — een 503 gooide de
      hele rij eruit vóór er iets gepland was, wat als "Fout" met lege uitklap in beeld kwam
      (Vente-unique.nl). Mag herhaald worden omdat de tool add-only is.
- [x] **Herstel neemt ook de negatives over** — de afgebroken aanmaak kwam nooit tot die
      stap, dus alle 12 achtergebleven campagnes hadden er 0. Zonder deze aanvulling zou
      een hersteld campagne-object structureel compleet zijn (boom + ad) maar zonder de
      merk-negatives gaan vertonen zodra je hem aanzet. `_copy_negatives` vergelijkt met
      wat er al staat en is dus idempotent. Alle 12 vinden een zustercampagne als bron.
- [x] **Export** `Downloads\claude\tag_toppers_aangemaakt_2026-08-10_1022.xlsx` — de 61
      campagnes uit run #14 met id, naam en werkelijke uitkomst: 49 compleet, 12 zonder boom.

- [x] **Item-ids worden genormaliseerd naar lowercase** — Google slaat ze zo op en matcht
      case-ongevoelig; de tool vergeleek case-gevoelig, zag élk id als ontbrekend en stuurde
      duplicaten. Bij Makro.nl (420 ids) sloopten 28 duplicaten alle 392 geldige ops, want
      product-group-ops zijn atomair per ad group. Na de fix: 28 aanwezig / 392 te sturen.
      Ook `_read_partial_failure` merkt die atomaire bijvangst nu als opnieuw-te-proberen,
      zodat één ongeldige op niet langer een hele batch kost.

- [x] **Eindcontrole: de set staat compleet** — preview over alle 620 rijen (14:06–14:13,
      niets geschreven): 620× status ok, 620× "bestaat", **0 aan te maken campagnes,
      0 toe te voegen ids** (20.741 van 20.741 al aanwezig), **0 te maken uitsluitingen**
      over **3.577** gecontroleerde zuster-ad-groups, 0 fouten en geen enkele melding over
      niet-ondersteunde niveaus. Daarmee zijn ook de 12 lege campagnes, Makro en de
      brand-niveaus (Koffiestore, Intratuin) afgerond. Kanttekening: dit is de tool die
      zijn eigen plan herberekent, dus een blinde vlek in díe logica zou hier niet
      opvallen — daarom zijn het item-id-niveau en de merk-vormen apart met
      `validate_only` tegen de echte bomen getoetst.

**Open:**
- Categorie-, staat- en kanaalniveaus zijn nog niet schrijfbaar. Komen in de gescande
  bomen niet voor; ze worden nu gemeld in plaats van fout te gaan.
- De kolom "Campagnes" in de run-historie toont nog `campaigns_to_create` uit de
  DB-kolom. `campaigns_created` zit wel in de opgeslagen summary-JSON, dus het
  resultatenscherm van een teruggezette run klopt; alleen de historietabel niet. Fix zou
  een extra kolom in `gsd_tag_toppers_runs` zijn.
- ~~13.209 uitsluitingen uit run #8 zijn nooit geland~~ — afgehandeld: de eindcontrole
  hierboven vindt geen openstaande uitsluitingen meer.

### 2026-08-07 — GSD Tag Toppers: bulk add-only tool (nieuw)

Nieuwe tool onder Google Ads: `/static/gsd-tag-toppers.html` + `/api/gsd-tag-toppers`.
Backend `backend/gsd_tag_toppers_service.py` (~1030 regels) + `gsd_tag_toppers_router.py`.
Verwerkt een kandidaten-Excel (shop_id / shop_name / country / productids) en doet per rij:
tag_toppers-campagne zoeken of aanmaken (PAUSED) mét negatives van een zuster, product ids
**add-only** in de boom hangen, en dezelfde ids uitsluiten in alle niet-REMOVED zusters —
ook add-only.

- [x] **Excel-parser** — leest alle cellen vanaf kolom D en houdt tokens van 15+ alfanumerieke
      tekens over. Daarmee is een rij die over 969 cellen is uitgesmeerd (Excel's celgrens van
      32.767 tekens) dezelfde code als een normale rij, en valt de weggeschoven
      `number_of_productids`-telcel er vanzelf buiten. Geverifieerd op
      `add_only_kandidaten_2026-08-07.xlsx`: 622 rijen, 24.933 ids, **0 mismatches** met de telkolom.
- [x] **Add-only bomen** — bewust NIET via `rebuild_tree_with_specific_item_ids` uit
      GSD_tagtoppers.py; die sloopt de boom en bouwt hem opnieuw, wat precies de bestaande ids
      zou wissen. Drie mutatievormen, alle drie serverside gevalideerd met `validate_only`:
      positieve item-ids onder de tag_toppers-root, negatieve item-ids in een bestaande
      item-id-container, en unit→subdivision conversie.
- [x] **Zuster-negatives** — dezelfde matcher als de sync van 2026-07-28 (naam **én** shop_id,
      genormaliseerd op case + suffix, ENABLED vóór PAUSED, dedupe op lowercase+matchtype).
- [x] **Parallel** — 6 workers met een lock per (account, shop): volle preview van 70 → ~10 min.
- [x] **Frontend** — upload → Preview → tabel → Run. `Run` blijft dicht tot er een preview is
      geweest en vraagt daarna nog een bevestiging; `POST /run` weigert zonder `confirm=true`.
      Uitleg onder "i"-knoppen per UI_BLUEPRINT. Nav toegevoegd aan alle 34 pagina's
      (A-Z tussen GSD Check en MC ID Finder).
- [x] **Grote echte run 7 aug 14:46 — 620 rijen: 18.956 ids toegevoegd, 181.511 uitsluitingen
      gemaakt, 113 rijen met fouten.** Daarvan waren er 61 het mislukte campagne-aanmaken
      (verkeerd Merchant Center id, zie LEARNINGS); de overige ~52 zijn niet meer na te kijken
      omdat die run net vóór het opslaan van de rijen draaide. De run zelf is uit de historie
      verwijderd op Joeps verzoek — deze regel is wat ervan over is.
- [x] **Eerste echte run gedraaid** (Joep, 2 rijen): 4/4 ids toegevoegd, 32/35 uitsluitingen.
      Het add-only schrijfpad doet wat het moet — dat was het enige dat `validate_only` niet
      kon aantonen. De drie missers waren één `CONCURRENT_MODIFICATION` op een convert.
- [x] **Drie bugs die die run blootlegde**, alle drie gefixt (zie LEARNINGS):
      geen retry op CONCURRENT_MODIFICATION; de planner telde alleen NEGATIEVE item-ids mee
      waardoor al bestaande nodes opnieuw werden gepland; en de mutates draaiden zonder
      `partial_failure`, zodat één afgekeurde operatie een blok van maximaal 1000 sloopte.
- [x] **`Recent runs`** — tabel `gsd_tag_toppers_runs` in Postgres, gevuld aan het eind van elke
      run (datum, type, bestand, rijen, gepland vs geland, fouten, duur). 13 sorteerbare kolommen.
      Geverifieerd dat het een herstart overleeft; dat is hier geen luxe, want elke
      backend-wijziging vereist een handmatige herstart en wiste tot nu toe de hele runstand.
- [x] **Uitklapbare rijen** met de mutaties per campagne, in de opmaak van DMA Exclusions.
      Status per mutatie: gelukt / overgeslagen / deels / gepland / mislukt.
- [x] **UI-ronde na Joep's testsessie** (7 aug, ~15 iteraties): Preview/Run in plaats van
      Dry run/Voor het echt draaien, uitleg onder "i"-knoppen, één pad van Excel upload naar
      Preview met Run in de resultatenkop, sorteerbare kolommen, tabel die zich tijdens een run
      vult (Refresh weg), en alle labels outlined in één `.lbl`-vocabulaire. Actienamen kort
      gehouden: aanmaken / toevoegen / negatives / uitsluiten.
- [x] **Audit: staan alle getargete ids ook uitgesloten bij de zusters?** Read-only sweep over
      alle 881 tag_toppers-campagnes (9 min, 10 workers). **257 met een gat (NL 190/507,
      BE 64/351, DE 3/23), samen 48.172 unieke ids.** Opvallend: bij **180** ervan missen ÁLLE
      getargete ids — daar is de uitsluiting nooit gedraaid, dat is iets anders dan de
      gedeeltelijke gaten die de `partial_failure`-bug gaf. Deliverable
      `Downloads\claude\tag_toppers_exclusion_audit.xlsx` (3 tabbladen: gaten / per ad group /
      alles). Script `audit_tt.py` in de session-scratchpad — ⚠ ephemeral.
- [x] **Fix-Excel gegenereerd** uit die audit: `tag_toppers_fix_kandidaten.xlsx`, 254 rijen /
      50.108 ids, in het formaat dat de tool leest (ids over meerdere cellen waar nodig).
      Alle 254 shops hebben een bestaande tag_toppers-campagne, dus dit pad maakt niets aan —
      het dicht alleen de uitsluitingsgaten. Script `build_fix_excel.py`, ook scratchpad.
- [x] **101 weesbudgetten opgeruimd** (NL 40 / BE 57 / DE 4), lijst in
      `Downloads\claude\tag_toppers_weesbudgetten.csv`.
- [x] **Run-historie compleet**: runs bewaren nu de volledige rijen inclusief `targets`
      (~2 KB/rij), dus een run is aan te klikken en zet het resultatenscherm terug zoals het
      was — tegels, tabel én uitklap. Plus CSV-export per run en paginering (25/pagina).
- [x] **Toolmax NL+BE gedraaid** (de twee brede rijen uit de kandidatenlijst, die in de eerdere
      runs ontbraken omdat daar de *kopie* van 620 rijen was geüpload): **4.192 ids toegevoegd,
      33.536 uitsluitingen, 0 fouten**. Dat is meteen de zwaarste bevestiging dat de fixes van
      7 aug houden — vier keer zwaarder dan de run waarop ze gevonden zijn.
- [x] **Twee bugs uit run 8 gefixt** (zie LEARNINGS): item-id OTHERS zonder case_value in
      multi-label bomen (gaf REQUIRED_FIELD_MISSING op elke ad group van een shop), en de
      CONCURRENT_MODIFICATION-retry die stilzwijgend was uitgeschakeld door het aanzetten van
      `partial_failure`.
- [x] **Voortgang en Cancel op mutatie-korrel** — teller van geschreven criteria naast de
      rijteller, `indeterminate` balk zolang geen rij klaar is, en cancel die tussen
      mutatie-blokken grijpt in plaats van pas tussen rijen.
- [x] **Beheerde staat in een tabel** — `gsd_tag_toppers_items` in de gedeelde Postgres
      (10.1.32.9 / n8n-vector-db, schema `public`, naast `gsd_tag_toppers_runs`): één regel
      per item id per shop/land, soft delete via `active` + `removed_at`, `source` per regel.
      Import is idempotent en gaat in blokken van 5.000 via `execute_values` — 24.933 ids in
      ~16s. Endpoints: `/items/summary`, `/items/import-excel`, `/items/import-live` (vult
      vanuit Google Ads, achtergrond), `/items/to-upload` (tabel als bron voor Preview/Run).
      Sleutel = `(country, shop_id, shop_key, item_id)`; zie LEARNINGS waarom shop_key alleen
      niet volstaat.
- [x] **Tool op het startscherm** — tegel op `dashboard.html` in de Google Ads-kleur.
      Daarbij hersteld dat het nav-script van 7 aug de link óók in dashboard.html had gezet,
      waar geen navbar is: die landde als los linkje midden in de GSD Check-tegel.
- [x] **Menu- en tegelvolgorde opgeschoond** — Bothits hernoemd (was "Bot Hits") en van de
      paarse Generators-kleur naar het groene `#00b894` van de SEO-tools, en `DMA+` stond in
      elke navbar tussen DMA Bidding en DMA Exclusions. Alle vier de navbar-dropdowns en alle
      vier de tegelgroepen zijn nu A-Z.
- [ ] **Volgende stap: frontend voor de beheerde staat** — kaart met de shops, import-knoppen
      en Preview/Run vanaf de tabel. Daarna pas het verwijder-pad (diff-run die ids weghaalt
      die op inactief staan), achter een aparte bevestiging met een limiet op het aantal
      verwijderingen per run.
- [ ] **Open: het volume van een volledige run.** Toolmax NL alleen is 2096 ids ×
      8 containerplekken = 16.768 uitsluitingen; over alle 622 rijen loopt dat hard op.
      Preview geeft het totaal vóór er iets geschreven wordt — kijk daar eerst naar.
- [ ] **Open: rijen met een mislukte mutate opnieuw draaien.** Door de ontbrekende
      `partial_failure` is er in de runs van 7 aug echt werk niet geland. Add-only, dus
      Run opnieuw draaien vult alleen het gat.

### 2026-08-06 — Bot Hits: crawler-log dashboard (nieuw)

Nieuwe tool onder SEO tools: `/static/bothits.html` + `/api/bothits`. Runbook en de
gemeten onderbouwing van de korrel staan in **cc1/BOTHITS_PROCESS.md** — lees die eerst
voordat je de tabellen aanpast.

- [x] **Schema** `scripts/bothits_schema.sql` — `pa.bothits_daily` (cube),
      `pa.bothits_url_daily` (alleen `pa.urls`), `pa.bothits_unknown_daily` (top-500/dag/familie),
      `pa.bothits_ingest` (ledger), `pa.bothits_host` / `pa.bothits_bot` (dimensies).
- [x] **Ingest** `backend/bothits_ingest.py` — 33 bot-families in 6 klassen, idempotent per
      logdatum (`DELETE WHERE log_date`), ~55 s/dag op 16 cores.
- [x] **Backfill** 116 dagen (2026-02-14 t/m 2026-06-09) uit het 102 GB-archief.
- [x] **Dashboard** — tegels, stacked-area per dag (splitsbaar op 8 dimensies), URL-type-donut,
      facet-diepte bekend/onbekend, tabellen voor bots / top-URL's / crawl-verspilling /
      hoofdcategorieën, CSV-export, ingest-tab.
- [x] **Dropfolder + nachtelijke run** — `BOTHITS_DROP_DIR`, `threading.Timer` (géén APScheduler),
      default **uit** via `BOTHITS_AUTO_INGEST`; knop en timer delen één lock. Een datum wordt pas
      geladen bij 24/24 uur, verwerkte files gaan naar `_processed/`.
- [x] **Nav** toegevoegd aan alle 33 toolpagina's + tegel op `dashboard.html`.

#### IN DE KOELKAST — oppakken bij de volgende sessie

**1. De backfill draait nog en moet afgemaakt.** Gestart 2026-08-06 15:36 als losstaand
`setsid`-proces (`python3 -m backend.bothits_ingest backfill --redo`), log in
`/tmp/claude-…/scratchpad/backfill2.log`. Ongeveer 100-120 s per dag, 116 dagen ≈ 3 uur.
Overleeft een uvicorn-restart, **niet** een WSL-herstart.

```bash
cd ~/projects/dm-dashboard
python3 -m backend.bothits_ingest status          # hoeveel dagen staan erin
python3 -m backend.bothits_ingest backfill        # hervat, slaat geladen datums over
```

Hervatten is veilig en idempotent — hij slaat over wat in `pa.bothits_ingest` staat.
Verwacht eindresultaat: 116 dagen, ~2.500 cube-rijen/dag, ~150k URL-rijen/dag (≈ 20M totaal).

**2. Dubbele bot-dimensierijen opruimen (ná de backfill).** De eerste run schreef namen met
de casing uit de user-agent, dus er staan nog rijen als `DiffBot` naast `Diffbot`. Na de
volledige `--redo` wijzen er geen feiten meer naar; ze zijn wees en vervuilen alleen de
filterlijst. Opruimen met een **gescopeerde** DELETE (een `TRUNCATE`/`DELETE` zonder `WHERE`
op deze DB wordt door een hook geblokkeerd, en terecht):

```sql
DELETE FROM pa.bothits_bot b
WHERE NOT EXISTS (SELECT 1 FROM pa.bothits_daily d WHERE d.bot_id = b.bot_id);
```

**3. Zet `BOTHITS_AUTO_INGEST=true` in `.env`** zodra de dropfolder daadwerkelijk gevuld
wordt. Staat nu bewust uit zodat er niets ongemerkt draait.

**Bewust niet gedaan (geen bug, ontwerpkeuze)**
- [ ] **Geen IP-verificatie.** Classificatie is puur user-agent; een vervalste Googlebot telt
      mee. De oude rDNS+range-logica staat nog in `~/bothits_verify.py` als dit gaat knellen.
- [ ] **Geen URL-detail voor productpagina's.** `/p/` is ~58% van de hits maar bijna volledig
      uniek per hit; die zitten alleen in de cube. Per-product analyse zou de feitentabel
      vervijfvoudigen.
- [ ] **`bothits_unknown_daily` is een dagelijkse top-500 per familie**, geen uitputtende
      ranglijst van de onbekende staart. Het dashboard zegt dat er expliciet bij.
- [ ] **Data stopt op 2026-06-09** tot er nieuwe logs in de dropfolder gaan.
- [ ] **Crawl-verspilling wordt gedomineerd door niet-pagina's** (`/data/graphql` 260k,
      `/shoppingcart/header` 206k). Die staan als `url_type = other`; overweeg een aparte
      `api`-klasse als dat de tabel te veel vertroebelt.

### 2026-08-06 — results-check in de drie maincat-basementflows

Bestanden in `Downloads\claude\N8N`, als **nieuwe** `*_with_check.json` naast de originelen
(die zijn ongemoeid, zodat je kunt vergelijken/terugvallen):
`basements_maincat_nl_with_check.json` (`nl-nl`), `_be_` (`be-nl`), `_de_` (`de-de`).

- [x] **`check_and_results` ingebouwd** tussen `Loop Over Items` (loop-output) en
      `create_post_json`; de takken naar `write_check_table` / `write_basement_info` blijven
      zoals ze waren. Node aangepast op de geneste vorm `{cat_id, date_to, urls:[…]}` uit
      `group_by_cat_id`, zodat `create_post_json` níet hoefde te veranderen. `order` wordt per
      cat opnieuw genummerd 1..N. `retryOnFail: false` — de code is al fail-open per URL, een
      retry zou ~5k calls verdubbelen. Extra outputvelden per cat: `urls_candidates`,
      `urls_checked`, `urls_dropped` (afkap op TARGET telt niet als drop).
- [x] **`build_query` cap 100 → 150.** De check kan alleen krimpen; met 150 kandidaten blijft
      de basement op 100 staan. Kan hier omdat er 215.797 kandidaten op alle rangen zijn.
- [x] **`safeDecode()` op category-slug en facetnamen** — nodig voor DE, zie LEARNINGS. Zit ook
      in de NL-versie (onschadelijk, robuuster). **Let op: het NL-bestand is hierna opnieuw
      gegenereerd; gebruik die versie, niet de eerste.**
- [x] **Getest tegen de live Search API**, node-body uitgevoerd zoals n8n hem draait
      (async function body, `$input`/`$helpers` geïnjecteerd): NL 40 URLs → 7 gedropt;
      BE 30 → 10; DE 30 → 14; overal 0 HTTP-fouten en `order` aaneengesloten. Edge-cases:
      geen `$helpers` / http gooit altijd → 100 behouden en netjes afgekapt; item zonder
      `urls`; kapotte `$input` → `[]` i.p.v. crash.
- [ ] **Nog niet gedaan: importeren + een run bekijken.** Verwachting NL ~15–20 min extra
      runtime (~3.650–5.000 calls). Volumes voor BE/DE niet gemeten (Redshift was traag), dus
      die schatting geldt voor NL. DE staat om 18:30, gelijk met NL-maincat.
- [ ] **Pre-existing bug, bewust niet aangeraakt:** in de maincat-flows schrijft
      `write_check_table` naar `pa.deepest_cat_ids_check_joep` en insert `create_post_json` in
      `pa.jvs_basements_deepest_1`, terwijl `create_table`/`empty_basements_table`
      `pa.main_cat_ids_check_joep` en `pa.basements_main_joep` beheren — en `get_cat_ids` leest
      wéér een andere naam (`pa.maincat_ids_check_joep`). De 'done'-resume werkt daardoor niet.
- [ ] **Deepest-cats flows: keuze open**, zie BACKLOG. `basements_maincats.json` valt buiten
      scope (inactief, 62 nodes, andere opzet — geen `build_query`/`group_by_cat_id`).

### 2026-08-06 — Shop-campaigns gelijkgetrokken met SEO stats

- [x] **Layout + gedrag** naar het SEO stats-model: tegels eerst en klikbaar (ze zijn nu de
      legenda én de toggles, dus de losse pillenrij + `buildToggles()` zijn weg en Chart.js'
      eigen legenda staat uit), kaart heet **Performance per day** i.p.v. "Trend", datumkiezer
      + presets in de body van die kaart, Refresh op de titelregel. **Load-knop weg**: laden
      gebeurt on change (debounce 400ms, `onChange` óók op de flatpickr-instanties) met een
      `loadToken`-guard, want er kunnen nu twee loads tegelijk lopen. Een mislukte load wist
      voortaan ook tegels/grafiek/Total-rij i.p.v. de vorige range te laten staan.
- [x] **"All"-preset** (24-06-2026 t/m vandaag). Geverifieerd dat 24-06 echt de eerste dag met
      data is: 44 dagen, allemaal gevuld, en 0 dagen met data tussen 01-05 en 23-06.
      Bugfix die daarbij hoorde: `ymd()` gebruikte `toISOString()` → dag te vroeg (LEARNINGS).
- [x] **Loader** voor de grafiek: skeleton in de vorm van een grafiek, opaak over de canvas,
      plus shimmerende tegelwaardes op dezelfde fetch. Patroon staat in UI_BLUEPRINT § Charts.
- [x] **Grafiek visueel gelijkgetrokken**: gevulde vlakken met aflopende alpha, dunne ronde
      lijnen, punten alleen bij hover, faint grid, eenheid op het tick-label, korte
      datumlabels, donkere tooltip met weekdag. As-captions alleen als één zijde twee assen
      draagt (deze pagina kan dat, SEO stats niet).
- [x] **Kleuren** = SEO stats' palet, 9 van de 10. Vier slots op Joeps verzoek — Clicks
      lichtgroen, Cost lichtblauw, Revenue roze, Impressions bruin — de vijf overige
      doorgerekend met `validate_palette.js`. Elke check gelijk of beter dan de bron;
      meetwaarden staan als commentaar boven `METRICS` en in LEARNINGS. Let op: Revenue en Cost
      staan naast elkaar in `ORDER` en zijn samen het zwakste paar (CVD 7,7, WARN-band), dus de
      adjacent-score is lager dan wat de zoektocht zonder die pins haalde (21,7).
- [x] **Kleine dingen**: "Export Excel" → "Export" (en van inline styles naar de canonieke
      `btn-outline-purple`), `.btn-preset` krijgt `min-width: 3rem` zodat de vijf presetcellen
      even breed zijn.
- [ ] **Open: WoW-pillen in de tooltip en sparklines + delta-badges in de tegels.** SEO stats
      heeft die, Shop-campaigns niet — ze hangen aan een extra baseline-fetch (equal-length
      vorige periode) die deze pagina nog niet doet. Bewust buiten scope gelaten: dat is
      databasis-werk, geen visuele afstemming.

### 2026-08-06 — HS2.0: Grasmaaiers-deck + all-channel drift in de cap-scripts gefixt

- [x] **Deck `Healthscore_2.0_Grasmaaiers.pptx`** (Downloads\claude, 8 slides) in de stijl van
      de hoofddeck; generator `scripts/analysis/healthscore_hs2_grasmaaiers_presentation.py`
      herberekent alles uit `hs2_catdiff_seasonal_v2.csv` bij het bouwen. Grasmaaiers gekozen
      omdat het de enige testcat is waarvan de selectie écht live staat (push 03-08-2026).
      Kern: 797 → 1.001 URL's, dekking bezoeken **65,3% → 83,6%** (+18,3pp), omzet 84,0% → 90,0%.
      Volledig gedragen door R-urls (7,2% → 70,2%, +63pp); PLP zakt 90,9% → 76,4% als bewuste
      ruil; C-urls (78% van de omzet) blijven op 95,4%. Verlies: 395 URL's / 88 bezoeken / €4,13
      tegenover 468 bezoeken erbij. PDF-export moet handmatig uit PowerPoint (geen libreoffice
      in WSL).
- [x] **All-channel drift gefixt in `healthscore_caps.py` + `healthscore_cat_seasonality.py`.**
      Beide importeerden `_SEO_JOIN`/`_SEO_WHERE` terwijl `pa.hs2_cat_knee` en
      `pa.hs2_cat_month` all-channel gevuld waren. Een rebuild had elke knie ~3× verkleind en
      daarmee elke cap en elke sitemap — stil, exit 0. Nu `_ALL_JOIN`/`_ALL_WHERE` + een
      `_guard_knee_shrink()` die aborteert als de mediane `knee90` meer dan halveert
      (`HS2_ALLOW_KNEE_SHRINK=1` om te overrulen). Getest tegen de live tabel: gelijke rebuild
      1,00× gaat door, 1/3-rebuild wordt geblokkeerd. Zie LEARNINGS voor de meetwaarden.
- [ ] **Nog niet gecommit** — beide scriptwijzigingen + cc1-updates staan lokaal.
- [ ] **Openstaand: de 03-08-selectie is nog niet nagemeten.** De echte test is of de
      gerealiseerde SEO-dekking van Grasmaaiers in aug/sep richting de voorspelde 83,6% beweegt.
      Nu meten kan niet — augustus is nog niet compleet.

### 2026-08-06 — Kopteksten + FAQ's van 4.699 maincat-level /c/-URLs verwijderd

- [x] **Geteld: 17.235 van de 79.777 maincat-level URLs (21,6%) hadden een koptekst en/of FAQ**,
      over 33 maincats. Maincat-level = geen subcategorie-segment in het pad; facetten tellen
      niet mee als diepte. Afgeleid uit het URL-pad omdat `pa.urls.main_cat_name` /
      `deepest_subcat_name` voor 94% NULL zijn — zie LEARNINGS voor de query en de `_[0-9]`-guard
      tegen 20 kapotte paden. `schoenen` is in z'n eentje 41.661 van de 79.777 (13,8% dekking),
      `mode` heeft met 50,9% de hoogste.
- [x] **Verwijderd, na uitsluiting van 7 maincats** (`meubilair`, `mode`, `fietsen`, `horloge`,
      `schoenen`, `sieraden_horloges`, `speelgoed_spelletjes`) en filter op `/c/`: 4.699 URLs.
      Eén transactie, backups vooraf, verificatie vóór commit (backup-count == delete-count,
      resterend 0 per tabel).

      | tabel | verwijderd |
      |---|---:|
      | `pa.kopteksten_content` | 4.306 |
      | `pa.faq_content_v2` | 4.431 |
      | `pa.kopteksten_push_state` | 4.343 |
      | `pa.faq_v2_push_state` | 8.792 (4.581 prod + 4.211 staging) |
      | `pa.kopteksten_link_validation` | 4.354 |
      | `pa.faq_link_validation` | 4.577 |
      | `pa.kopteksten_jobs` | 4.699 |
      | `pa.faq_jobs` | 4.699 |

      Stand na afloop: `kopteksten_content` 240.517 → 236.211, `faq_content_v2` 250.071 →
      245.640, maincat-level met content 17.235 → 12.536. Binnen de uitgesloten-set blijven 23
      over: de kale roots `/products/<maincat>/`, die vielen buiten het `/c/`-filter.

      **Terugdraaien:** `INSERT INTO pa.<tabel> SELECT * FROM pa.<tabel>_bak_maincat_c_20260806`
      per tabel. De scope staat in `pa.del_targets_maincat_c_20260806` (url_id + url), dus de set
      is exact reproduceerbaar. Backups nog niet opgeruimd — Joep laat weten wanneer dat mag.
- [ ] **Openstaand: de live site loopt achter.** Bewuste keuze van Joep — niet direct
      unpublishen, de eerstvolgende volledige publish ruimt het op (replace-all). Tot dan staan
      ~4.300 kopteksten en ~4.600 FAQ's nog online op beslist.nl terwijl ze uit de DB weg zijn.
- [ ] **Openstaand: niets belet regeneratie.** `pa.urls` is ongemoeid gelaten, dus een backfill
      die job-rijen aanmaakt voor URLs zonder job zet deze 4.699 gewoon terug in de wachtrij.
      Zie BACKLOG.

### 2026-08-05 — GSD/SEO-Stats audit uit de koelkast: Phase 0 + 1 live

- [x] **Phase 0** (`9b04eaa`) — H2, H3, H5, reconcile-sinkfouten zichtbaar, `ORDER BY
      ad_group.id` op beide ad-group-queries. Allemaal gedragsbehoudend. H2 was een **dagelijkse
      500**: `_pct_delta` bewaakte alleen de baseline terwijl `_ratio()`/`_opb()` None teruggeven
      voor een dag zonder visits en als p2 doorgegeven worden; Dagoverzicht staat default op
      gisteren. Gate uit de audit gehaald: `?date=2026-07-31` was een bevestigde 500 op 1 aug en
      geeft nu 200.
- [x] **Phase 1** (`3ff1455` = H1, `8d0a1b7` = de rest).
      **H1-beslissing van Joep: de UI is het juiste gedrag** — Include = allow-list,
      Exclude = deny-list. `included` deed twee dingen tegelijk: het bepaalde of het
      `actie IN ('aan','uit')`-predicaat werd toegepast, en `shop_names` werd *altijd*
      `shop_name IN (…)`. Dus "Exclude these shops" liep op precies díe shops. Nu gescheiden;
      de oude bijwerking "`included=True` geeft ook shops zonder wijziging vandaag" is bewust
      wég (hoort achter een eigen parameter als je hem terug wil).
      Gate gehaald tegen 2026-08-04: Include is exact de gekozen set, Exclude bevat er geen
      één van, samen zijn ze het complement van de dag, en de rijaantallen tellen op.
      Verder: H4 (adoptie miste juist zijn eigen cohort van ~2.954 naamgelijke campagnes —
      dedent), H7 (mislukte ENABLE stond als `skipped`), creatiedatum alleen nog voor écht
      *created* campagnes, sheet-vlag `aangemaakt?` telt `activated` mee, en de
      sheet-dedupekey kent nu de actie (een `uit`-rij binnen ±2 dagen onderdrukte een latere
      `aan`-rij).
      **Plus het structurele risico, beide helften:** `run_gsd_script` had géén
      exception-grens. Eén shop die faalde brak de hele run af en sloeg `_log_run_to_sheet`,
      `push_mc_ids_to_redshift`, `record_created_campaigns` én `reconcile_run_logs` over — het
      herstelmechanisme werd overgeslagen door precies de fout waarvoor het bestaat — terwijl
      `_run_progress["running"]` voor altijd True bleef.
      **Let op bij reviewen:** die diff is ~443 regels maar bijna volledig herindentatie van de
      twee try-wraps. `git diff -w` geeft 49 toegevoegde en 12 verwijderde coderegels; lees hem zo.
- [x] **Phase 2 — preview en run convergeren (H6)** (`939cf4d`). Uitgetrokken naar
      `_pause_identity_matcher()` + `find_pausable_campaigns()`, die preview én run nu beide
      aanroepen. Gemeten tegen NL_CPR: met shop_id aanwezig identiek aan de oude weg (geen
      regressie), met shop_id op NULL vindt de gedeelde lookup 6 resp. 5 campagnes waar de
      oude preview **0** rapporteerde. Bosmenshop 6-vs-5 laat zien dat beide bronnen nodig
      blijven: één campagne wordt alléén op identiteit gevonden omdat het GSD_SCRIPT-label
      ontbreekt. De run pauzeert via twee bronnen (een
      `campaign_label`-query op `[shop:variant]` zónder shop_id/kanaalfilter, óf identiteit over de
      kandidatenlijst); preview loopt alleen de kandidatenlijst af en eist `[shop_id:N]` +
      `SHOPPING`. Een shop met NULL shop_id previewt dus als "0 te pauzeren" terwijl de run alles
      pauzeert wat hij vindt — **divergentie in de gevaarlijke richting**. De regels staan nu op
      twee plekken met de hand bij, en dát is hoe H6 ontstond. #priority:high
- [x] **De vijf MEDs die in géén fase zaten** (`28318db`): reconcile weer bereikbaar
      (`if not changes: return` stond ervóor, dus "run hem nog eens" repareerde niets op precies
      de rustige dag waarop je dat doet), heatmap-schaal volgt nu het weekdagfilter, Top-categorieën
      koplabels komen uit de `comparison`-datums van de backend i.p.v. hardcoded "Yesterday", een
      mislukte reload laat niet langer de vórige periode staan, en het stale pp-badge-commentaar.
- [x] **Phase 3 — opruimen** (`751399a`, netto −95 regels). Dode code, het dode `sub`-catniveau,
      label negative-caching, sorteerrangen, request-sequencing in seo-stats, en `loadCampaigns()`
      die per-account-fouten nu wél toont.
      **Let op — de dode-codelijst in de audit had het bij één naam MIS:** `exportXlsx` bestaat in
      béide pagina's en die in seo-stats is **live** (Export-knop). Alleen de gsd-campaigns-variant
      is weg. Check bij zo'n lijst elke naam per bestand.
      Bulk-selectiestate zat bewust **niet** in deze batch (mutaties op échte campagnes) en is
      apart geland — zie hieronder.
- [x] **Bulk-selectiestate in GSD Campaigns** (`92d96e8`, eigen commit na Phase 3). Selectie
      leefde in de DOM (`.camp-check:checked`), en elke re-render bouwt `tbody` opnieuw op — dus
      typen in het zoekveld, sorteren of pagineren gooide hem weg, en dat zijn precies de drie
      dingen die `renderCampaigns()` aanroepen. 40 vinkjes op pagina 1, door naar pagina 2, Pause:
      er gebeurde niets. Nu een `selectedCampaigns`-Set op `customer_id|campaign_id` (precies wat
      de bulk-endpoints aannemen); de checkbox rendert eruit én schrijft erin.
      Oud gedrag faalde *veilig* (leeg), nieuw gedrag kan de ándere kant op falen — handelen op een
      set die je niet meer ziet — dus drie waarborgen, en dát is de eigenlijke inhoud:
      1. `bulkAction` lost keys op tegen `allCampaigns` en telt hoeveel er buiten
         `filteredCampaigns` vallen; die telling staat in de confirm ("NOTE: n of them are not
         visible under the current filter") **vóór** het handelen, niet erna.
      2. `loadCampaigns` schoont keys op die upstream niet meer bestaan — **alleen bij succes**,
         binnen de `try` ná de assignment, zodat een mislukte fetch nooit stil een selectie wist.
         Zonder dit houdt een elders verwijderde campagne de bulkknoppen enabled en `Copy (n)`
         hoger dan wat een actie raakt.
      3. Na een bulkactie worden álle ingezonden keys gewist, óók de no-ops: hun status is net
         veranderd, dus aangevinkt laten staan nodigt uit tot een tweede live mutatie op stale
         state.
      Kopcheckbox blijft pagina-scoped ("alles op deze pagina"); knoppen en het Copy-label melden
      de héle selectie, want dat is het getal dat de confirm citeert. `getSelectedCampaignRows`
      leest dezelfde Set, dus Copy en Pause kunnen niet meer van mening verschillen.
- [x] **Dagoverzicht `d` vs `d-7`** (`2d86d6b`). Joeps keuze: venster houden, de vertekening
      tónen. Beide datums zijn echte dagen, dus de rekensom was nooit fout — wat verschilt is
      **rijpheid**: `d` vult nog aan, `d-7` is een week geleden uitgekristalliseerd, dus
      `revenue_wow`/`opb_wow` lezen elke ochtend laag en lopen dicht als de dag rijpt. Uitlijnen op
      `get_deltas` was de verleidelijke fix en is hier slechter: dan staat de omzet van 4 augustus
      naast de visits van 5 augustus onder een kop "5 augustus". Nieuwe `revenue_settling` (dag is
      vandaag of gisteren) zet een amberen marker + tooltip op SEO-omzet en OPB — amber omdat het
      cijfer *incompleet* is, niet fout. Visits/CTR/bounce rijpen niet na en krijgen geen noot.
      De statistisch juiste fix (d-7 op dezelfde rijpheid vergelijken) vraagt maturity-snapshots
      die we niet bewaren.
- [x] **De LOW-cluster** (`d682c39`) — de zeven items die in géén fase zaten. Elk item eerst
      tegen de code gecheckt, en de lijst had het bij **drie** mis:
      * `_MACRO_MICRO_RE` scande de tag-inhoud, dus een shop die écht "Macro.nl" heet komt binnen
        als `[shop:Macro.nl]`, wordt als macro-variant gelezen en is daarmee permanent
        niet-adopteerbaar — de run zou er **elke dag** een tweede campagne naast zetten. Niet zo
        latent als de audit suggereerde. Fix: `[^\]:]`, dus alleen een *bare* tag markeert nog.
        Eerst gemeten: van 2.856 live campagnes hebben 120 een macro/micro-tag en **alle 120 zijn
        bare** (`[macro]` 60, `[macro+micro]` 60). Nul keyed vormen, beide patronen vlaggen
        dezelfde 120 namen.
      * Foute `?date=` gaf 500 i.p.v. 400. `_check_date` in beide routers. **Roep hem vóór de
        `try` aan:** HTTPException *is* een Exception, dus binnen het blok valideren laat de
        handler's `except Exception` de 400 weer als 500 teruggeven — precies dezelfde bug, één
        indent dieper. GSD's `/preview`, `/run` en `/ll/run` weigeren een typo nu aan de deur in
        plaats van na een Redshift-round-trip of een gestarte run (`running=false` nagemeten).
      * `_CACHE` groeide oneindig (keys bevatten de datums). Nu: verlopen entries worden bij een
        write geveegd + oudste-eerst cap op 200 die nooit de net geschreven key wegvaagt.
        1.000 losse keys → 200 entries.
      * **`_as_distribution`'s fallback-loop mocht juist NIET weg** — hij is vandaag onbereikbaar,
        maar `total` telt héél `raw`, dus een bucket die niet in de order-lijst staat blijft in de
        noemer: loop weg = slices tellen stil niet meer op tot 100%. Hij logt nu een warning als
        hij vuurt, wat ook de subtielere schade pakt (een onbekende bucket wordt achteraan
        geplakt en verschuift alle slice-kleuren — precies wat de order-lijst moet voorkomen).
      * Twee items bestónden niet meer: géén onbereikbare statement na een `return` in
        `gsd_campaigns_service` (AST-check, nul hits — de H4-dedent nam hem mee) en `.dev-legend`
        was al verwijderd.
      * Dode ids/CSS **gemeten, niet aangenomen.** Weg: seo-stats' `#loadBtn` disable/enable-dans
        (knop bestaat niet, `if (btn)` dekte juist het enige zekere, dus 3 regels + de `finally`
        waren no-ops — de vorm van `loadStats`), `.stat-card`/`.btn-action`/`.btn-pause`/
        `.btn-activate`/`.btn-remove` (verweesd door `751399a`), `.activity-log
        .log-entry/-time/-action` (log is nu een `<table>`), `.delta-card` + zes `.dc-*`, `.neg`.
        **Bewust GEHOUDEN, want een class-grep is geen doodsbewijs** — en alle vier lijken dood
        voor een naïeve scan: `.log-success`/`.log-error` komen uit een template literal,
        `.metric-tile` via `card.className`, `#sparkTip` wordt at-runtime aangemaakt (afwezig in
        de HTML is dus correct) en elke `.flatpickr-*` regel is een theme-override die de library
        zelf toepast.
      * `get_event_loop()` → `get_running_loop()`, alle **67** plekken in 15 bestanden (de audit
        zei 20). Per AST geverifieerd dat elke call in een `async def` staat, waar de twee
        equivalent zijn — mechanisch, niet gedragsveranderend.
- [x] **Beslissing 1: `pa.mc_ids_efficy` is een STATE-tabel** (`eb17d60` code, `ccf22da` opruiming).
      Joeps regel: één rij per (shop_id, domain) met de *huidige* MC-id. Nieuwe MC-id voor een
      bestaande key → rij **én datum** updaten; zelfde MC-id → niet toevoegen, niet updaten.
      Daarmee is de auditvraag zélf weg: herkomst ("hebben wíj dit account aangemaakt?") doet niet
      meer mee, want de rij is óf afwezig (insert), óf anders (update), óf identiek (niets).
      **Het grotere defect zat aan de andere kant en dat had de audit gemist:** het schrijfpad was
      een kale `INSERT` zonder dedupe, dus élke run waarin get-or-create "created" zei plakte een
      rij erbij — **63 surplusrijen op 520 keys (10,8%)**, Cameranu.nl NL **7×** gelogd, 4× op
      dezelfde dag. Leest Efficy rijen als events, dan kregen ze maanden dubbele
      "nieuw account"-signalen.
      Wélke rij de waarheid is, is **gemeten**: tegen `pa.jvs_gsd_campaign_created` matcht de
      **vroegste** datum in **39 van 49** groepen en de laatste in **nul** — precies wat "één keer
      inserten, daarna afblijven" oplevert. Latere rijen zijn re-logs en backfill-artefacten (de
      MC-backfill dateert uit `change_event`, ~30 dagen retentie, dus een recente datum op een oud
      account).
      Reconcile beslist niet meer zelf wat "missing" is maar geeft alles aan dezelfde
      `mc_upsert_plan` als de writer (weer de H6-les), en dát repareerde een blinde vlek: de oude
      triple-prefilter sloeg exacte matches over (de no-op) maar verborg daarmee de UPDATE-case —
      een shop wiens MC-id was **veranderd** las als "niets te doen".
      Opruiming: 583 → 520 rijen, 0 keys verloren, 0 rijen verzonnen. Backup in
      `pa.mc_ids_efficy_bak_20260805` + CSV's in `Downloads\claude`. Script:
      `scripts/dedup_mc_ids_efficy.py`.
      **Redshift-valkuil (kostte een verwarrende false failure):** een write terugleggen over
      dezelfde langlopende connectie leest de snapshot van vóór de write (serializable), en de
      volgende write in die verschaalde transactie sterft met "Serializable isolation violation".
      Lees op een verse connectie.
- [x] **Beslissing 2: preview-actie `skip` → `skip_or_repair`** (`4b025ae`). Een match betekent
      dat de campagne níet wordt aangemaakt, maar de run doet er wél `_repair_campaign` op en kan
      dan `created`/`repaired` melden — "skip" beloofde dus read-only en hield dat niet. Vóórspellen
      kost 3 extra GAQL-reads per match op élke previewde campagne; hernoemen kost niets. Tile-key
      blijft `'skip'` (die telt `already_exists`); rendert als "skip / repair" met de reden in een
      tooltip.
- [x] **Superfoodsonline.nl state-rijen bijgeschreven** (2026-08-05). Reconcile écht gedraaid
      (`days=29`, `dry_run=false`) ná een verse dry run die bevestigde dat álle openstaande writes
      bij shop 649612 hoorden: `mc_ids missing 2 | inserted 2 | updated 0 | unchanged 27`,
      `sheet missing 1 | logged 1` (rij 1274), geen errors. Tabel 520 → **522 rijen, 522 keys,
      0 surplus**. **Let op: reconcile heeft géén shop-filter** — dit was alleen veilig omdat alle
      drie de writes bij die ene shop hoorden. Altijd eerst dry-run + controleren wie er in zit.
- [ ] **Kamera-express.nl (182, NL): de opgeslagen MC-id bestaat NIET (meer).** Alle 2.668
      subaccounts onder de drie GSD-parents gescand + `get()` per id:
      * NL parent `5592708765` → **670182955** "Kamera-Express.nl" (kamera-express.nl, Ads
        7938980174). Enige Kamera-account onder de NL-parent, en wat alle 14 live GSD-campagnes
        gebruiken (7 NL_CPC + 7 NL_CPR, allemaal ENABLED).
      * BE parent `5588879919` → **702356923** én **5609659447**, béide "Kamera-express.be", zelfde
        website, zelfde Ads-account 2454295509. **Twee subaccounts voor één shop.**
      * DE parent → niets.
      * `5619578895` (in de tabel) en `5619583143` (pre-dedup) staan onder **géén enkele** parent en
        geven **401** op `get()` (= niet ons subaccount), tegenover de **400** die een
        verkeerde-parent-gok oplevert. Het `5619…`-prefix matcht andere GSD-gemaakte subaccounts,
        dus waarschijnlijk: run van 2025-07-10 maakte twee NL-subaccounts aan, later is
        geconsolideerd op het al bestaande `670182955` en zijn de GSD-accounts verwijderd. Precies
        de "pre-existing sub-account"-zorg uit de audit — maar de échte schade is een **dode id**,
        niet een spookrij.
      **Openstaand:** NL-rij op `670182955` zetten. De **datum** is het onbeslisbare deel:
      `20250710` (wanneer GSD hem voor het eerst aanraakte, staat er nu) vs `20250613` (de
      GSD-created-date van de shop uit `pa.jvs_gsd_campaign_created`). Eén-rij-update via het
      nieuwe upsert-pad zodra Joep kiest. De dubbele BE-accounts zijn geen tabelprobleem
      (`shop_list`: 182 is `listed_on_nl=1, listed_on_be=0`, dus geen BE-rij), maar dubbele
      subaccounts splitsen feeddata — iemand mag daar in Merchant Center naar kijken.
      **De audit zelf is volledig gesloten: geen code-items meer open.**

### 2026-08-05 — Kopteksten incrementeel publiceren, en de HS2.0-push werd overschreven

- [ ] **BLOKKER: zoek uit wie nog naar de Keywords API publiceert.** **Update 2026-08-18: de
      overschrijver is de HS1.0-pipeline** — de live Fietsen-set was 100% een subset van
      `bt.new_hs_data` 'Augustus 2026' (`max(load_date)` 17 aug). De job zelf is nog niet
      gevonden, en het is nog onverklaard waarom Kantoor 13 dagen ongemoeid bleef terwijl
      Fietsen terugkwam — beide zijn `main_category_id` in HS1.0. Oorspronkelijke notitie: alle
      12 HS2.0-categorieën
      stonden binnen een dag terug op hun oude inhoud (zie LEARNINGS voor het bewijs, incl.
      Grasmaaiers op 407 waar zowel pre-push als onze payload 752 was). Niets in deze repo doet
      dat behalve `healthscore_keywords.py`, en `workflow_entity` bestaat niet in de gedeelde
      Postgres — dus n8n-definities staan elders. **Tot dit gevonden is, is elke HS2.0-meting
      onbetrouwbaar.** Kandidaten: een n8n-workflow op een andere host/DB, een scheduled script
      op de Windows-server, of de HS1.0-pipeline die dit project moet vervangen.
      #priority:high
- [x] **12 categorieën opnieuw gepusht** (5 aug ~13:30) vanuit de bewaarde payload-bytes, niet
      opnieuw gebouwd — live is dus exact de gevalideerde set. 12/12 teruggelezen en kloppend.
      Snapshots van de teruggedraaide staat: `Downloads/claude/hs2_payloads_reverted_20260805/`.
- [x] **Kopteksten publiceert nu incrementeel** (`70a163f`, `e05acf1`). `Publish` = upsert over
      `/automated-content/records`, alleen nieuw/gewijzigd, bijgehouden via md5 in
      `pa.kopteksten_push_state`. `Publish All` = de oude full-set batch, en nog steeds het enige
      dat écht prunet. Retireren gebeurt als **push van `content_top = ""`** in dezelfde chunks
      (Joeps idee) i.p.v. één DELETE per url — ~11 extra chunks tegen ~21.810 requests. Veilig
      want geen nieuwe staat: 7,2% van de live records had al een leeg content_top.
- [x] **Publish All uitgevoerd + push state geseed** (5 aug, door Joep resp. hier). De batch liep
      op de nieuwe code: `content_type` gelogd als `content_top`, payload 323 MB i.p.v. 1.730 MB
      (−81%, dat is de `content_faq`-verwijdering), 232s i.p.v. 978s. Productie-steekproef daarna:
      `content_bottom` populated **0/1000** (was 917) en leeg `content_top` **0/1000** (was 72) —
      de 13.902 FAQ-only urls zijn dus gepruned. Seeden was hard verantwoord: **0** kopteksten
      gewijzigd sinds de publish-start, dus de publishable set was exact wat live stond. 249.809
      rijen geseed, `pending 0 / stale 0`.
- [x] **Per-url Push + Delete & Push op URL Lookup** (`e357d8c`, `a6165e9`, `4e59fc3`). Push = één
      record van ~1 KB. Delete & Push haalt hem ook direct uit de live store; sinds content_bottom
      dood is, is dat een gewone record-DELETE.
- [x] **`Delete` reset de job nu écht op pending** (`a38fc92`) — deed dat niet, zie LEARNINGS.
      Zelfde fix in de FAQ-delete.
- [x] **UI uit suggestions_new.txt** (`73a84f4`): done-banners in `alert-done-yellow` i.p.v. het
      platgetrokken grijze `alert-success`; het ruwe API-response-blok alleen nog bij een mislukte
      run en niet meer in `alert-secondary`; Shop-campaigns' 7d/14d/30d/90d-groep gelijk aan
      SEO Stats (6 regels verbatim overgezet, rulesets nu identiek).
- [ ] **`#processResult` heeft hetzelfde grijze-success-probleem** ("Bulk API complete!") op zowel
      Kopteksten als FAQ's. Buiten de scope van het verzoek gelaten (dat ging over
      `#publishResult`); zelfde fix als het opgepakt wordt.
- [ ] **FAQ's `/faq` staat op 94,4%** — 240.958 van 255.274 urls, laatste push 3 aug 16:14. De
      resterende ~14.316 moeten gepusht zijn **vóór** een volgende Publish All, want die wist
      `content_bottom` overal en dan hebben die pagina's op geen van beide plekken FAQ-content.

### 2026-08-04 — GSD Check: is_pixel_shop erbij, en de laatste twee tabellen naar de blueprint

- [x] **`is_pixel_shop` in GSD Check** (`1ae749e`), uit **dezelfde** as-of-gisteren snapshot als de
      GSD-vlaggen (`bt.shop_main_attributes_by_day`, niet `shop_list`). Dat is het hele punt: GSD
      Campaigns leidt `model` af als CPR bij `is_wecantrack_shop OR is_pixel_shop`, anders CPC — en de
      tracking-vlag valt weg in dezelfde feed-update als de GSD-vlag. Uit twee verschillende as-of
      datums lezen zou precies de casus verbergen waarvoor je de tool opent. Direct zichtbaar:
      gisteren staan alle drie Emob-shops op `pixel=1` mét GSD-vlag 1; vandaag vielen beide weg.
- [x] **GSD Check + MC ID Finder result-tables volgen nu UI_BLUEPRINT § Tables.** Dit waren de laatste
      twee met de oude paarse uppercase header op een plain `.table`. Nu grijs `#f8f9fa`, sticky,
      1rem headers op `table table-sm table-hover tool-table` + `thead.table-light`, body 0.9rem, in
      een omrande `.tool-table-wrap`. Kolomstrategie = **content widths + horizontale scroll**
      (`width:max-content; min-width:100%` + nowrap): GSD Check heeft nu tien kolommen en MC ID Finder
      bouwt zijn landkolommen at runtime, dus elke waarde moet volledig leesbaar blijven. Copy/Export
      uit de card-header naar een `.filter-row` bóven de tabel, en herbenoemd naar **"Copy" / "Export"**
      (zoals de Campaigns-created toolbar). Sorteerglyphs ↑/↓ → ▲/▼. [[feedback_ui_blueprint]]
      **Valkuil bij zo'n class-rename:** `sortBy()` doet
      `querySelectorAll('.result-table th.sortable')` — vergeet je die, dan verdwijnt de actieve
      sorteerpijl geruisloos terwijl het sorteren zelf blijft werken.
- [x] **Emob.be `no_live_campaigns_to_pause` was géén nieuwe bug** — de fix (`802cc7f`, 14:26) zat op
      disk en in GitHub, maar de draaiende uvicorn was om 14:21 gestart (4,5 min ervóór) en heeft geen
      `--reload`. De runs van 14:54/14:56 liepen dus nog op oude code. Herstart om 15:41 loste het op.
      **Les: bij "de fix werkt niet" eerst `ps -o lstart` van de uvicorn tegen de commit-tijd zetten
      voordat je de code gaat debuggen.** [[dm_tools_backend_no_reload]]
- [ ] **Emob.nl en Emob-moebel.de nog controleren** — die stonden in dezelfde stale runs, dus hun
      campagnes zijn vermoedelijk ook nog niet gepauzeerd. Na een nieuwe run verifiëren in de
      accounts zelf, niet op de run-samenvatting.

### 2026-08-04 — Kopteksten-publish is content_top-only, FAQ splitst af

- [x] **`content_faq` en `content_bottom` uit de kopteksten-publish** (`152661d`). Zie LEARNINGS: de
      batch-endpoint is een REPLACE, `content_faq` werd altijd stil weggegooid (792 MB per upload) en
      `content_bottom` is nu van Publish 2.0. Payload valt van ~1,5 GB naar ~280 MB en van 265.151 naar
      251.248 urls; de 13.902 FAQ-only urls verdwijnen daarmee uit die store (correct — zonder
      `content_bottom` zouden dat lege rijen zijn). `content_bottom` gaat als `""` mee omdat het veld
      verplicht is, en dát ruimt de oude waarden op.
- [x] **`content_type`-selector weg** uit de API en beide frontends (seo_only/faq_only waren
      destructief tegen een replace-endpoint). `publish_log.content_type` logt nu `content_top`.
- [x] **FAQ URL Lookup: Publish-knop toegevoegd** naast Delete, via nieuwe `POST
      /api/faq/publish-v2/url` + `publish_faq_v2_url()`. Synchroon (één url ≈ 6 records, één POST) en
      `replace=True` als default — omgekeerd aan de bulkrun, want `/faq` is additief en een gerichte
      re-push moet eerst DELETE'en om te matchen met wat op het scherm staat. Record-bouw naar
      `_build_records()` zodat bulk en single niet kunnen divergeren. "Delete & Reset to Pending" →
      "Delete" (de confirm zegt nog steeds dat hij op pending zet, want dat doet hij).
- [x] **Performance Standup terug als losse tool onder SEO tools** (`cdf1e1e`) — nav-entry op 32
      pagina's + dashboard-tile terug; de tool zelf was nooit stuk, alleen onbereikbaar.
- [ ] **Excel-exports emitten nog `content_faq`/`content_bottom`-kolommen** (`main.py:2174`, `2738`).
      Ongewijzigd gelaten (niet gevraagd), maar als iets daarvan terugvoert naar deze API is de
      `content_faq`-kolom daar even inert als hier.
- [ ] **Overweeg de bulk-publish naar `/automated-content/records` te verhuizen.** Die endpoint is een
      echte upsert, dus dan verdwijnt de "elke ontbrekende url wordt verwijderd"-eigenschap — en
      daarmee de noodzaak om 251k rijen te sturen om er één te wijzigen.

### 2026-08-04 — HS2.0 LIVE op 10 testcategorieën + 2 pilot-maincats

- [x] **Alle 10 testcategorieën live gezet** (op Joep's go). Onafhankelijk teruggelezen uit de API:
      12/12 buckets matchen de gepushte payload exact. Elke categorie eerst `snapshot_live()` →
      `validate_payload` (0 problemen overal) → drop-list geprijsd → push met `confirm_token`.

      | cat | naam | before | after |
      |---|---|---|---|
      | 9000047 | Stoelen | 7.348 | 4.870 |
      | 9000066 | Eetkamerstoelen | 854 | 1.272 |
      | 9000608 | Sneakers | 3.019 | 4.125 |
      | 9000953 | Voer | 1.379 | 1.880 |
      | 9002072 | Douchewanden | 88 | 241 |
      | 9005282 | Mobiele telefoons | 1.160 | 1.452 |
      | 9005317 | Airconditionings | 1.308 | 448 |
      | 9001646 | Dekbedovertrekken | 1.604 | 2.573 |
      | 9003581 | Grasmaaiers | 752 | 752 |
      | 9000668 | Shirts | 4.538 | 1.924 |

      **Totale prijs van alle drops: 1.441 SEO-visits over 90 dagen**, waarvan 968 Airconditionings.
      Grasmaaiers kwam identiek terug (752 → 752, 0 added, 0 dropped) — de schoonste bevestiging dat
      de pipeline deterministisch is. `preserve_cross_category=True` deed exact wat de meting
      voorspelde: Stoelen preserveerde 2.415 → 4.870 records, Shirts 907 → 1.924, en hun drop-kosten
      klapten in naar 8 en 2 visits (was 35.866 zonder de mitigatie). **De twee non-leaf
      categorieën waren daarmee veilig te pushen.** #priority:high

- [x] **Pilot-maincats live: Kantoor (361) en Fietsen (38000)** — om te bewijzen dat de
      maincat-procedure werkt. Kantoor 16.625 → 16.954, Fietsen 12.190 → 13.326, 0 validatieproblemen,
      drop-kosten 100 resp. 349 visits (90d). Bewust gekozen: **geen van beide bevat een testcategorie**
      (die zitten onder 10, 137, 165, 655, 12000, 27000, 32000, 34000, 36000), dus de twee pilots zijn
      niet verstrengeld. Seizoensneutraal in augustus (0,99 en 1,09), anders dan Grasmaaiers (1,42, ná
      de piek). Contrast in breedte: Kantoor trekt uit 220 subtree-categorieën, Fietsen uit 73.
      **Let op de churn**: Kantoor houdt maar 2.216 van 15.478 live urls (14%), Fietsen 2.848 van
      10.091 (28%) — bijna volledige vervanging van welke urls gelinkt worden. #priority:high

**Open — vervolg:**
- [ ] **32-maincat backtest van de gewichten.** 0.889/0.111 zijn gefit op within-deepest-cat
      percentiles; de maincat-pass hergebruikt ze ongewijzigd. De pilot valideert de *mechaniek*, niet
      de *selectiekwaliteit* — lees traffic-beweging op Kantoor/Fietsen dus als voorlopig, en doe deze
      backtest vóór uitbreiding naar meer maincats. #priority:high
- [ ] **Meet Airconditionings (9005317) eerst.** De enige categorie die kromp (603 → 448 urls) en 67%
      van alle weggevallen traffic. Seizoenscap werkt zoals bedoeld (augustus is ná de piek), maar dit
      is de enige plek waar dit rollout-moment een echt verlies kan laten zien.
- [ ] **Controls vastgelegd vóór de push, niet aanraken**: 40000 Multimedia-accessoires als control
      voor Kantoor, 37000 Auto's voor Fietsen (zelfde voertuigen-domein, vergelijkbare seizoenspatroon).
      Let bij de baseline op dat Grasmaaiers al sinds 3 aug live staat en de andere elf sinds 4 aug.
- [ ] **Rollback = `Downloads/claude/hs2_payloads_preserved/`** — 12 snapshotbestanden, opnieuw posten
      herstelt. Niet perfect lossless: de GET geeft `order` niet terug, dus een restore hernummert de
      rijen; urls en anchor-tekst komen exact terug.

### 2026-08-04 — Content Publishing: Refresh eruit, FAQ-publishknoppen omgedraaid

- [x] **Refresh-knop weg uit Content Publishing** op FAQs (`frontend/faq.html`) én Kopteksten
      (`frontend/index.html`). `fetchLastPushTimestamp()` blijft bestaan: faq.js/app.js roepen hem
      al aan bij page load én na elke publish, dus de knop haalde alleen een waarde opnieuw op die
      niets anders verandert. Geen dode code. De `↻ Refresh Status`-knop in de **Processing
      Status**-card is een andere knop en staat er nog.
- [x] **FAQ: Publish en Publish All omgewisseld, Publish vol-oranje.** Nieuwe volgorde `Publish All`
      (`btn-outline-orange`) → `Publish` (`btn btn-run`) uiterst rechts. Volgt UI_BLUEPRINT:
      vol-oranje `btn-run` = primaire CTA uiterst rechts, `btn-outline-orange` = oranje non-run
      actie. `.btn-run` heeft zijn eigen `:disabled`-grijs, dus de bestaande disable-logica in
      `publishFaqV2`/`pollFaqV2` (alleen het `disabled`-attribuut, geen class-writes) kon blijven.
      Ids houden hun V2-namen. [[feedback_ui_blueprint]]

### 2026-08-04 — GSD pause: drie shops gingen uit en geen enkele werd gepauzeerd

Gemeld door Joep: "Emob-moebel.de to pause, maar error `no_account_config`". Bleek één oorzaak
met drie symptomen over NL/BE/DE — zie LEARNINGS (1 entry). 21 ENABLED campagnes bleven staan.

- [x] **`ACCOUNTS`: elk land één account voor beide modellen.** `DE_CPC` toegevoegd (bestond
      niet → `no_account_config`) en `BE_CPC` omgezet van 7565255758 naar 2454295509 op Joep's
      aanwijzing ("everything is in 2454295509"). Distinct customer_ids 4 → 3; LL-map wordt
      `NL:[7938980174] BE:[2454295509] DE:[4192567576]`; stats-view labelt elk account nog
      `*_CPR` (insertion order). #priority:high
- [x] **`PAUSE_LABELS`** — pause matcht beide model-vocabulaires + `promo` + `tag_toppers`, dus
      een uitgezette shop gaat volledig donker en het pauzeren hangt niet meer aan een juist
      afgeleid model. Zowel in `_pause_campaigns_for_shop` als in het `uit`-pad van de preview,
      zodat die twee niet uit elkaar lopen. #priority:high
- [x] **`PAUSE_EXTRA_CUSTOMER_IDS = {"BE": ["7565255758"]}`** + `_pause_customer_ids()` — het
      oude BE_CPC-account wordt bij pauzeren meegesweept (Joep: "there are only paused campaigns
      in that account so it wouldn't hurt checking it"). Pause-only: `_find_account_info()` kijkt
      hier niet naar, dus er wordt nooit meer in gecreëerd. Kosten = 1 read per shop, en niet-
      ENABLED wordt overgeslagen → 0 mutaties. Geverifieerd op de twee shops die daar wél iets
      hebben (Beddenbriljant.nl 599127, Duifhuizen.nl 27143): 2 kandidaten, 2 identity-match,
      0 ENABLED. #priority:medium
- [x] **Backend herstart** (geen `--reload`, PID 16040 → 35982) en over HTTP geverifieerd:
      `POST /api/gsd-campaigns/preview` geeft 21 to pause / 0 errors, 7 per land.

**Open:**
- [ ] **De 21 campagnes zijn nog ENABLED — de echte pause is NIET gedraaid.** Preview zegt
      7× NL (7938980174), 7× BE (2454295509), 7× DE (4192567576) voor Emob.nl / Emob.be /
      Emob-moebel.de op 2026-08-04. Wacht op Joep's go.
- [ ] **Model-afleiding bij de bron fixen** (`get_redshift_shop_changes`, `_LEG`): lees `model`
      bij `actie='uit'` uit de rij van GISTEREN. Nu `DE_CPC` bestaat is de luide `no_account_config`
      ingeruild voor een stille mis-create: een DE-shop die als CPC leest maakt 2 CPC-campagnes
      aan i.p.v. 5 CPR. Smal risico (bij `aan` staat de tracking-vlag normaal mee aan) maar het is
      de enige plek waar dit écht opgelost wordt.
- [ ] **Vraag open:** de 4 campagnes in 7565255758 dragen `GSD_SCRIPT` maar het account staat niet
      in `ACCOUNTS`, dus ze blijven onzichtbaar in de Campaigns-lijst van het dashboard (die over
      distinct `ACCOUNTS`-customer_ids loopt). Pause bereikt ze, de stats-view niet. Wel of niet
      ook laten meelezen?

### 2026-08-03 — HS2.0 out of the fridge: first seasonal-cap build + Keywords API payload generator

- [x] **Rebuilt features + sitemap as_of 2026-08-03** — `pa.hs2_features` 1,081,728 urls;
      `pa.hs2_sitemap` 1,313,740 rows (1,026,478 scored + 287,262 new) over 3,539 cats. This is
      the **first sitemap ever built with the seasonal caps** — the stored 30-jun set was flat
      N=1000, so the 21-jul cap model had never actually produced a selection. Joep's answer on
      the open cap question: **keep the all-channel knee**. #priority:high
- [x] **Payload generator `backend/healthscore_keywords.py`** — builds/validates/diffs
      `POST /sitemap` bodies from `pa.hs2_sitemap` + `page_heading`, dry-run only. All 10 test
      cats validate clean: 16,181 records (19,537 with `preserve_cross_category=True`), 2.5 MB,
      payloads dumped to `Downloads/claude/hs2_payloads{,_preserved}/`. `push()` refuses unless
      `confirm_token == "REPLACE <catId>"`; `snapshot_live()` is the only undo (no DELETE on the
      API). **Nothing has been posted.** See LEARNINGS (4 entries). #priority:high

- [x] **FIRST LIVE HS2.0 CATEGORY — Grasmaaiers 9003581** (2026-08-03, on Joep's go). No-op
      self-replace first (`before=409 after=409`, content verified identical), then the real
      payload (`before=409 after=752`, live content == payload, 752 urls: +586 new, 166 kept,
      80 dropped and all 80 had 0 SEO visits in the 90d window). **POST needs no auth** — 200
      with no key from the internal network, so `confirm_token` is the only guard. Rollback
      snapshot: `Downloads/claude/hs2_payloads_preserved/live_snapshot_9003581_nl.json`
      (re-push it to restore). #priority:high

**Open — next steps:**
- [ ] **Measure Grasmaaiers.** SEO visits / coverage for 9003581 over the coming weeks. August
      is *past* its seasonal peak (season_index 1.42), so compare against a control category
      rather than against last month, or the seasonal decline reads as a regression.
- [ ] **NEXT UP — bring a LARGE test set live.** Joep, 2026-08-03: "almost ready". The
      machinery is done (`backend/healthscore_keywords.py`); what a bigger rollout needs first:
      1. **Leaf-only selection.** Pick candidates by `GET /api/Categories/{id}` →
         `subCategories == []`, NOT by name. Stoelen (19 children) and Shirts (8) must stay out
         until the parent/child partition question is answered, or push parent+children together.
      2. **`preserve_cross_category=True` for a partial rollout** — mandatory, or URLs HS2.0
         still wants get deleted because their own category wasn't in the batch (measured: 2,935
         urls / 35,866 visits on the 10-cat set). It does push those cats over their cap.
      3. **`snapshot_live()` every category before its push** — the only rollback that exists.
         Keep the snapshots; a batch of 100 categories is 100 restore files.
      4. **Pre-flight the drop list per category**: how many URLs it removes and how many of
         those had SEO visits in the 90d window. Grasmaaiers went out at 0 traffic dropped;
         hold that bar, or accept the loss knowingly.
      5. **Hold back a control group** of comparable untouched categories, chosen before the
         push, so seasonality can be separated from the HS2.0 effect.
      6. Still-open decisions that get bigger with scale: **PLP** (27% of the selection, channel
         carries none), the **new-URL bucket** (unpushable: no category, ~59% phantom), and the
         **near-non-binding caps** (window mismatch, 206 of 3,539 cats trimmed).
- [ ] **PLP decision.** 6,106 of 22,287 selected test-cat records (27%) are `/p/` product pages
      and the channel carries none. Drop them, or decide product pages belong in HTML sitemaps.
- [ ] **New-URL bucket.** 287,262 rows → ~120k real pages after the phantom-facet filter, and
      they need generated anchors (`generate_title_v3(url, polish=False)` + taxonomy label
      cache). They also carry `deepest_category_id = NULL`, so they are unpushable until
      attributed to a category. Surviving examples are thin brand pages
      (`'YCOVSFP Tuinhuisonderdelen'`) — needs a junk guardrail like the R-urls got.
- [ ] **The caps barely bind.** August caps sum to 2.65M against a 1.06M candidate pool; the cap
      trims only ~206 of 3,539 cats, the other 3,327 take their whole supply. Root cause is a
      window mismatch — base cap = knee over **12 months all-channel**, candidates = **90 days**
      of activity. Fix the windows, not the seasonality shape.
- [ ] **Re-measure the HS1.0 baseline against the Keywords API**, not `bt.new_hs_data` — the
      +13.7pp headline was measured on the wrong surface (see LEARNINGS).

### Done 2026-08-03 — FAQ Publish 2.0 can be cancelled mid-push

- [x] **Cancel button for Publish 2.0** (commit `9dac89a`, on main, pushed).
      `POST /api/faq/publish-v2/cancel/{task_id}` zet een coöperatieve vlag die de
      push-loop **één keer per URL** leest → stoppen gebeurt tussen batches, nooit
      halverwege een POST. De halfvolle batch wordt **weggegooid** (Cancel = stop met
      schrijven naar de live API) en `urls_done` rolt terug naar wat écht gepubliceerd is;
      die URL's blijven ongestempeld in `pa.faq_v2_push_state` en gaan mee met de volgende
      `mode="new"`-run. Task landt als `status="cancelled"`, niet `completed`+vlag, zodat de
      banner niet als "Done" kan renderen — frontend: rood-omlijnde Cancel onder de bar,
      `Cancelling — ` als prefix op de live tellers, en een `alert-info` "Stopped — partial
      run". Getest met stubs (geen DB, geen live `/faq`): cancel na 5 van 10 URL's → 2 POSTs,
      gestempeld 0-3, `urls_processed=4`, `status='cancelled'`. #claude-session:2026-08-03 #priority:medium
- **Deploy-noot**: live :8003 draait **zonder `--reload`** (uvicorn PID 185082, up sinds
  1 aug), dus de Cancel-knop geeft 404 tot een kill+relaunch. Nog niet gedaan — een
  herstart sloopt lopende Tier-A rurl-runs, dus met Joep afstemmen.

### OPEN — audit of GSD Campaigns + SEO Stats (2026-08-01) — full report in `cc1/AUDIT_GSD_SEOSTATS_20260801.md`

Six-slice review of 12 597 lines; every HIGH re-verified by hand. **Two reported HIGHs did
NOT survive verification** — the `deleted_ind` "inflated visits" claim (measured: 219 rows,
all `deleted_ind=0`, zero duplicate pairs) and "CTR/Bounce should use a pp badge" (that is
Joep's 2026-07-31 decision). Do not "fix" either.

**Phase 0 — behaviour-preserving, ~40 lines, no gate needed:**
- [ ] `seo_stats_service.py:94` — `_pct_delta` guards only the baseline, so a day with no
      SEO visits 500s `/api/seo-stats/dashboard`. **Live now**: `?date=2026-07-31` → 500,
      `2026-07-30` → 200, and Dagoverzicht defaults to yesterday — broken every morning
      until the ETL lands. Fix: `if p1 is None or p2 is None or not p1: return None`.
- [ ] `gsd_campaigns_service.py:2497` — add `"campaign_resource": campaign_resource` to
      `_repair_campaign`'s already-exists return; without it every `activated` campaign is
      missing from the undo payload and Reset silently leaves it live.
- [ ] `frontend/gsd-campaigns.html:1852` — `previewMeta` never copies `to_activate` /
      `awaiting_bid_strategy`, so the teal tile always reads 0 and the SA360 note never
      appears (the backend does return both).
- [ ] `gsd_campaigns_service.py:3849,3868` — reconcile reads only `["inserted"]` from two
      of its three sinks; a dead DB looks like "nothing to do".
- [ ] `gsd_campaigns_service.py:2465,3060` — `ORDER BY ad_group.id` so verify and repair
      cannot inspect different ad groups of the same campaign.

**Phase 1 — behaviour-changing, needs a regression gate:**
- [ ] **H1, needs Joep's decision first:** "Exclude these shops" runs on exactly those
      shops (`:1449` always appends `shop_name IN (…)`; `included` only toggles the `actie`
      filter). UI wording and `gsd_ll_service.py:2047` say the opposite. Decide whether the
      wording or the behaviour is the truth.
- [ ] `:2566-2579` — dedent the GSD_SCRIPT attach out of the name-mismatch branch, or the
      2 954 canonically-named unlabelled campaigns are never adopted.
- [ ] `:2626` — a failed ENABLE stays `action="skipped"`; bucket it as an error.
- [ ] `:3389` / `:3309` / `:3793` — don't stamp creation dates for activations, count
      activations in the sheet's `campagnes aangemaakt?`, and exclude `uit` rows from the
      sheet dedupe key.
- [ ] **Structural:** `run_gsd_script` has no per-shop exception boundary — one failure
      aborts the run and skips all four side-logs *including the reconcile that exists to
      heal exactly that*, leaving `_run_progress["running"]` True forever.
      Gate: `POST /preview` identical before/after except where intended; for H1, Exclude
      must return the complement of Include over the same shop list.

**Phase 2 — converge preview and run:** preview under-reports pauses (`:2996` vs `:2766`).
Extract the two-source pause lookup and `_is_ours` into helpers both call.

**Phase 3 — cleanup:** ~10 dead functions in the GSD frontend, `_campaign_name_variants`,
`loadStats()` (permanent no-op called from 10 places), the dead `sub` cat level (two wasted
Redshift aggregations per SEO-Stats page load), label negative-caching, request sequencing
in seo-stats, bulk-selection state, sort ranks for the new actions.

> **Status 2026-08-05: alle drie fases geland** — `9b04eaa` (fase 0), `3ff1455` + `8d0a1b7`
> (fase 1, H1 apart), `939cf4d` (fase 2 / H6), `28318db` (de vijf losse MEDs), `751399a`
> (fase 3), `92d96e8` (bulk-selectiestate, apart), `2d86d6b` (Dagoverzicht).
> Alleen de twee "Decisions, not defects" staan nog open, en dat zijn keuzes, geen defecten.
> Zie de bovenste sectie van dit bestand voor de details per fase.

### Done 2026-07-31 (late) — legacy pin rows adopted + GSD pause matched by identity

**1.846 legacy blueprints regenerated and pushed** (`scripts/analysis/seo_titles_adopt_pin_rows.py`).
These are tblPageTitles rows containing a position-pinned facet whose phrase the current
builder orders differently. Pushing them **adopts** the combo into /page-titles, so this
tool now owns them instead of the MySQL export.
- **87 rows deliberately skipped**: their live phrase contains hand-written words
  ("Ontwormen !!dier_dierenbenodigdheden!!", "… supplementen"). A rebuild deletes editorial
  text, which is a content call. CSV: `seo_titles_pin_skipped_editorial_*.csv`.
- **TRAP: never re-sort a legacy key.** `build_blueprint()` sorts the facet tokens and
  /page-titles upserts on `(cat_id, key)` as a STRING, so a re-sorted key writes a SECOND
  record and leaves the live one untouched. **616 legacy NL rows have `key <> canon_key`**;
  one was in this batch (`9000953 dier_dierenbenodigdheden~d_voer~merk~s_voer`) and silently
  failed to publish (1.845 of 1.846). Script now pins `bp["key"] = r["key"]`; that row was
  cleaned up and pushed under its legacy key. Final: 1.846 pushed.

**GSD pause now matches by identity, and a shop can no longer vanish.** Joep: "the preview
says it will pause Elektroshop.nl, the run pauses nothing, and Elektroshop is missing from
the output entirely."
- Cause: `_pause_campaigns_for_shop` only touched campaigns carrying GSD_SCRIPT, and
  Elektroshop's five ENABLED campaigns carry **no label at all** (one of the 2.954). The
  empty result list meant no rows were appended, so the shop disappeared from the output.
- Fix: pause a campaign when it carries GSD_SCRIPT **or** is ours by identity (name variant
  + shop_id + one of this run's label tokens, macro/micro/OUD excluded) — the same test the
  create path uses. A campaign matched only by name gets GSD_SCRIPT attached, so the
  unlabelled estate converges instead of growing. Preview mirrors it and marks those rows
  "matched by name (no GSD_SCRIPT label)".
- **"Nothing to pause" is now a visible `skipped` row** with reason
  `no_live_campaigns_to_pause`. Silence was the actual reporting bug.
- Verified: preview now reports `to_pause 5` for Elektroshop with all five flagged.

**Live-data note:** 55 of the 70 campaigns from this morning's activation list are now
ENABLED + TARGET_ROAS (paired in SA360 while we worked); 15 remain PAUSED + MANUAL_CPC and
are exactly what the bid-strategy guard still refuses to enable.

### Done 2026-07-31 (late) — OpenAI credit guard: signalled in the UI, generation stopped

The key ran out of credits and **nothing said so**: v3 catches a failing polish call and
falls back to its deterministic H1, so batches kept reporting success while producing
unpolished titles. Now:

- **`backend/openai_guard.py`** — one hook, installed at startup, wrapping
  `chat.completions.create` **and** `batches.create` at the CLASS level, so all ~14 call
  sites in 8 modules are covered (and future ones automatically). A quota error sets a
  flag in `pa.system_flags` (Postgres → shared across uvicorn, workers and the UI, and it
  survives a restart); the first successful call clears it.
- Detection matches the error CODE (`insufficient_quota` / `credit_balance_exhausted`),
  **not** the prose, and deliberately does NOT trip on a plain 429 rate limit.
- **Refused with 409** (message shown verbatim in the UI): `/api/process-urls`,
  `/api/faq/process-urls`, `/api/ai-titles/start`, `/api/batch-start`,
  `/api/faq/batch-start`, `/api/ai-titles/batch-start`. Single-URL calls stay open on
  purpose — that is the natural "try one to see if credits are back" path, and a success
  clears the flag.
- **Running work stops between units**: `_run_processing` checks between chunks (leaves
  the rest pending, not failed), `batch_api_service` checks between chunks and sets a
  clear error state.
- **UI**: `frontend/js/openai-banner.js` (sticky red bar + "Ik heb bijgevuld" button that
  clears the flag) on unique-titles.html, index.html and faq.html. Status endpoints:
  `GET /api/system/openai-status`, `POST /api/system/openai-status/clear`.
- **Tested end to end** with the live dead key: a real 429 sets the flag, the endpoint
  reports it cross-process, all six start endpoints answer 409, the banner renders
  (screenshot), clear works, and a fresh failure re-blocks. The flag is left SET, because
  the key really is empty — see BACKLOG for the top-up.

### Done 2026-07-31 (late) — position pins live in blueprints + GSD `|NL` dedup

- **`pa.facet_position_rules.position` is now honoured by the blueprint builder**
  (`seo_titles_service.facet_phrase`), the same column `ai_titles_service` has honoured
  for months: `pre_noun` → directly in front of the noun (also a type-facet noun),
  `end` → after everything, `end_before_size` → degraded to `end` (a blueprint holds
  placeholders, so "before the sizes" is not decidable at build time). `load_rules()`
  now returns a 3-tuple and filters `scope_category IS NULL`; `_rule()` stays tolerant
  of the old 2-tuple. **1.187 pushed blueprints re-pushed + 49 queued rewritten**,
  0 stale after. Backups are now per-run files
  (`seo_titles_repush_backup_<stamp>.csv`) — a fixed name had already eaten one.
- **GSD create/pause/preview look under every shop-name variant** — `Hbm-machines.com|NL`
  also matches `Hbm-machines.com` (`_shop_name_variants`). Fixes duplicate sets on the
  way in and campaigns left ENABLED on the way out. Preview output verified byte-identical
  on today's 16 changes; the positive path proven read-only against the live API.
- **Then widened, per Joep:** campaign identity is now shop name (any variant) +
  shop_id + `[label:{cl}]`, ignoring `[label_test]` / `[branche:H&L]` / `[STANDARD]` /
  `[AFFILIATE]` decorations and excluding macro/micro (and `[OUD]`, my call).
  `_fetch_shop_campaign_candidates` + `_match_existing_campaign`, used by BOTH the run
  and the preview. A match is **adopted**: GSD_SCRIPT is attached if missing, else the
  shop would vanish from the tool. Replay of this morning's state confirms
  `[label_test] … [label:b]` (19884113478) would have been adopted, not re-created.
- **Adoption now ENABLES** (Joep, 2026-07-31): a shop Redshift switches ON whose
  campaigns already exist gets them set to ENABLED; brand-new campaigns still start
  PAUSED. Three guards: the repair must not have errored, the campaign must be PAUSED,
  and it must NOT carry `GSD_LL_PAUSED` (low-linkage owns that status and re-enables it
  itself — `LL_PAUSED_LABEL` mirrors `gsd_ll_service.LL_LABEL`; importing would be
  circular, keep them in sync). New result action **`activated`**, filed with `created`
  so undo/reset can pause it back; new preview action `activate` + `to_activate` count,
  new tiles/pill colours (`#00838f`) in both panels.
- **CORRECTED the same day, and this one is critical.** The first version would have
  enabled the run's OWN fresh creations. Going live requires a manual SA360 step (a
  colleague pairs the target-ROAS bid strategy) and only then may a campaign be enabled —
  so the run now refuses to enable anything whose `bidding_strategy_type` is still
  `MANUAL_CPC`/unset (`BID_STRATEGY_PENDING`, `_bid_strategy_ready()`), reported as
  `awaiting_bid_strategy`. Measured signature: **ENABLED TARGET_ROAS 2.354 · PAUSED
  TARGET_ROAS 354 · PAUSED MANUAL_CPC 105**. Do NOT test `campaign.bidding_strategy`
  (the portfolio field) — it is empty on every GSD campaign because SA360 attaches a
  STANDARD strategy, so that test would reject everything.
  Today's preview after the fix: **to_activate 0, awaiting_bid_strategy 70**, 5 to create.
  Positive path verified too: Aktiewonen.nl's PAUSED+TARGET_ROAS campaigns would enable.

### Done 2026-07-31 — GSD side-logs reconcile themselves ("just run it again")

Three unfinished runs today created campaigns but wrote **none** of the three side-logs
(all three steps sit after the create loop): `pa.jvs_gsd_campaign_created`,
`pa.mc_ids_efficy` and the `campaigns_created` sheet.

- **Backfilled today**: 13 MC-id rows to Redshift (Toolstation NL skipped — already
  logged with the same mc id 687755389) and **14 sheet rows** from row 1248. Creation
  dates were already complete (backfilled earlier today).
- **`reconcile_run_logs(days, dry_run)`** compares change_event (ground truth, ~30-day
  retention) against all three logs and writes only what is missing. Runs at the end of
  every `run_gsd_script` (`RECONCILE_WINDOW_DAYS = 7`, best-effort) and is exposed as
  `POST /api/gsd-campaigns/reconcile-logs?days=7&dry_run=true`. Verified idempotent: a
  second pass reports 0/0/0.
- **`SHEET_DATE_TOLERANCE_DAYS = 2`** is load-bearing: the sheet's datum is the RUN date
  while change_event reports the campaign's own creation time in the account timezone.
  Hoopo.eu/Zurbrueggen/Scentulp/Geurfris BE are logged 14-07 with campaigns dated 15-07 —
  exact date matching duplicated all four in the first dry run.
- MC-id rule (documented divergence): a run logs only MC accounts it CREATED, but the
  Content API exposes no account creation date, so the reconcile logs any
  `(shop_id, country, merchant_id)` triple missing from the table.
- Sheet type is derived from the label token (`,` → CPC, else CPR) and `op brand?` from
  the campaign's BRANDED_0/1 label. The shop that went `uit` today (Elektroshop.nl) is
  deliberately NOT logged — its campaigns were never paused (no GSD_SCRIPT label).

### Done 2026-07-31 — the DE `account_access_denied` error is a Merchant Center key, not Google Ads

`auth/account_access_denied: The caller does not have access to the accounts:
[5342886105]` — 5342886105 is the **DE Merchant Center** advanced account ("beslist BV",
`ACCOUNTS["DE_CPR"]["mc_id"]`), and the caller is the Content API service account, so no
Google Ads change would have helped. Measured all four keys in
`backend/service_accounts/` against all three MC parents (read-only):
**only `acoustic-racer-258913-e55feb91bacc.json`
(`beslist-index-checker@acoustic-racer-258913.iam.gserviceaccount.com`) has access** —
NL/BE/DE all OK; the other three return 401 on every parent (`authinfo` empty).
`_get_mc_service()` fell back to `os.listdir()[0]` — arbitrary order — when
`GSD_SERVICE_ACCOUNT_FILE` was unset, so a machine could silently pick a dead key. Now
sorted + a warning naming the file, and `_mc_err` appends `[caller: <sa email>]` to
access errors. **Check `GSD_SERVICE_ACCOUNT_FILE` in prod's .env on win-htz-006** —
runbook for the agent on that machine: `docs/PROD_FIX_MC_SERVICE_ACCOUNT.md` (check →
verify read-only → fix → confirm, plus the NSSM `AppEnvironmentExtra` trap and the
"wait for run/progress to be false before restarting" warning).
- **`backend/service_accounts/` is gitignored** (`.gitignore:63`), so the four key files
  live on each machine by hand and **a `git pull` never delivers them**. The runbook
  therefore opens with step 0: check the key is present on that box before pointing the env
  var at it, and if it is missing copy it directly (private key — not via the repo, a
  branch or a ticket). Verify the right file by its identity, not by opening the key:
  `client_email beslist-index-checker@acoustic-racer-258913.iam.gserviceaccount.com`,
  `project acoustic-racer-258913`, `private_key_id e55feb91bacc…` (matches the filename).
  Laptop copy: `backend/service_accounts/acoustic-racer-258913-e55feb91bacc.json`,
  referenced by `.env` line 57.

### OPEN — 2.954 canonical GSD campaigns have no GSD_SCRIPT label (found 2026-07-31)

416 shops, 2.456 of them ENABLED (plus 8.565 legacy-named unlabelled ones). They are
invisible in Campaigns created and **cannot be paused by the tool** — `Elektroshop.nl`
went `uit` today and will keep running. Cause: the label is applied in a separate
best-effort call after the create, and failures were swallowed (now returns a bool +
logs `UNMANAGED CAMPAIGN`). **Decision needed:** run a label backfill (attach GSD_SCRIPT
to canonical unlabelled campaigns → instantly manageable/pausable), and separately
decide about the legacy-named estate. Scan script:
`scratchpad/gsd_unlabeled_split.py` pattern — re-create under `scripts/analysis/` when
the backfill is approved.

### Done 2026-07-31 (late) — t_tuinhout flipped to a type facet

- `pa.facet_position_rules`: `t_tuinhout` → `is_type_facet=true`, `order_index=1700`
  (was 1544/non-type). Its values ARE the noun (Schuttingplanken, Vlonderplanken,
  Tuinpalen), so the "Tuinhout" category no longer stacks behind them.
- 9 blueprints in cat 9004934 rebuilt: **7 pushed rows re-pushed to production**
  (`seo_titles_repush_stale.py --apply`), **2 queued 'built' rows rewritten in place**
  via the new `--refresh-built` flag (queued rows are invisible to the re-push path but
  would otherwise be published with the pre-flip phrase). Global check after: 0 stale.
- 5 of 62 unique titles still ended in "… Tuinhout" (the per-category classifier had
  the other 57 right already); regenerated, 0 left. Backup:
  `Downloads/claude/unique_titles_tuinhout_backup_20260731.csv`.
- **BLOCKER surfaced: the OpenAI key has no credits** (`429 insufficient_quota /
  credit_balance_exhausted`). v3 fell back to the deterministic composed h1, so the
  output is correct but unpolished — "Hardhout Potdekselplanken" where polish would
  write "Hardhouten". Re-run those 5 once credits are topped up.
- **8 legacy `pa.page_titles_existing` rows for the same category are NOT managed
  here** and 5 of them still render `!!sub_category_lower!! !!t_tuinhout!!`. They come
  from the tblPageTitles export and are excluded by dedup; overwriting them via
  /page-titles is a separate decision.

### Done 2026-07-31 (evening) — suggestions_new round 3 (SEO Stats layout + GSD dates)

New bullets in `suggestions_new.txt` (5 SEO Stats / 2 GSD Campaigns), all shipped:

- **Refresh in Performance per day is transparant** — de witte pil eraf, override
  verwijderd. Zie LEARNINGS: die card-headers renderen grijs, niet paars
  (`!important` in style.css verslaat de inline stijl). UI_BLUEPRINT herschreven.
- **Sectie-orde**: Performance per day → **Per-day overview → Top categories** →
  Dagoverzicht → Performance standup.
- **"Update Excel" module weg uit de frontend** (kaart + `ps-` CSS + IIFE).
  `performance-standup.html` en `/api/performance-standup/run` bestaan nog en werken
  nog; alleen de ingangen zijn weg (comment in dashboard.html bijgewerkt).
- **DMA Organic visits Δ sparkline = `#d63384`**, via `STANDUP_SPARK_COLORS` — alleen
  in Performance standup, `METRICS.dma_visits` (chart + topregel) blijft violet.
- **GSD Country-header niet meer afgekapt**: kolom 3 van `.ll-history-table` van 90px
  naar 120px (14px padding + 1,2rem sorteer-gutter vraagt ~92px). Meteen ook de
  "All Countrie" selects van 120 → 145px, in beide tabellen.
- **GSD Date-kolom is 100% gevuld** (was 47 van 2.793 leeg): root cause +
  code-fix + change_event-backfill, zie LEARNINGS. **Prod (win-htz-006) heeft de
  code-fix nog niet** — tot een deploy blijven nieuwe creaties leeg en is
  `POST /api/gsd-campaigns/campaigns/backfill-created-dates?days=29` de tijdelijke
  vulling (change_event bewaart ~30 dagen, dus niet langer laten liggen).

### Done 2026-07-31 (later) — SEO Stats interaction pass + FAQ publish bar

- **"Visits & revenue per day" → "Performance per day".**
- **Chart matches the tile sparklines**: area fill + 1,75px stroke. Fill alpha steps down
  with the series count (`<=2 -> 2b`, `<=4 -> 22` = identical to the tiles, `else 14`) so
  ten translucent washes cannot turn the plot to mud. NOTE the fill makes the shared-axis
  problem more visible: SEO visits (~69k) buries DMA/GSAAS (~2k) under a wash. Inherent
  to one count axis (no dual axis allowed), not caused by the fill — open if it annoys.
- **Summary tiles are now the chart's metric toggles**: same `selected` set as the pills,
  same on/off vocabulary. The number/label toggles; the sparkline area stays hover-only.
- **Top categories has its OWN "Compare day"**, independent of Performance standup.
  `loadDeltas()` is split into `loadCatDeltas()` + `loadStandupDeltas()`, each with its own
  ref_date and skeleton; the server caches /deltas per ref_date so equal dates cost one
  query. `lastDeltas` (standup) and `lastCatDeltas` (categories) are separate state.
- **Dagoverzicht's day picker moved to the title row**; Refresh stays in the body because
  `btn-outline-purple` on the purple header is unreadable.
- **CTR/Bounce WoW badges are percentages** (`-1%`, `+0,5%`) instead of percentage points,
  per Joep. Both are in the payload (`ctr_wow` + `ctr_wow_pp`); `ppBadge` was deleted
  rather than left dead. Reminder: a % of a % means "the rate moved 1% relative".
- **FAQ Publish 2.0 gets a real progress bar** (backend already had the counters),
  indeterminate during `phase='counting'`, plus `.badge-purple` replacing the invisible
  grey mode chip.
- **Measured, for the record:** a full FAQ v2 push is **265.632 URLs / 1.593.741 records**
  (avg exactly 6,00 per URL) — not the ~1,7M I had been saying.

### Open — bol/CTR follow-ups (2026-07-31)
- **Re-check the "dying tail" by `shop_id`.** fonQ, Beckhuis, Naduvi, Aliexpress NL CSS,
  PetsHome, Hema.be all read as going to ~0 across 10 March, but that was measured on
  `shop_name` — the exact trap that made the bol reading wrong twice. May be renames.
- **The residual −0,057 CTR after substitution**: is it listing density (fewer tiles per
  page) or mix? Amazon + rest recovered ~70% of bol's loss; the rest reached the metric.
- **What the two new bol ids (665180/665181) actually are** — retail moving to its own
  account (re-composition) or incremental volume? They carry ~185k/month combined now.
- Both memories were corrected on 2026-07-31: the event is bol measured BY ID, not a
  "Plaza cliff", and any number from `cpa_outclicks_transactional` needs
  `COUNT(DISTINCT sent_outclick_id_stat)`.

### Done 2026-07-30 (analysis) — SEO 29 vs 22 Jul read, and the /c/ CTR erosion root cause

Redshift-only session, no code changes. Asked for a yesterday-vs-last-week SEO read;
ended up finding a 12-month structural decline behind it.

- **29 vs 22 Jul**: SEO visits +1,1% (55.978 vs 55.375), but that flat headline hides a
  rotation. A heat cluster gained +2.974 visits (Tuinartikelen +32%, Huishoudelijk +34%,
  Sport & outdoor +29%; Klimaatbeheersing +180%, Zwembaden +172%, Parasols +207%) while
  the rest of the site lost −2.371 (−5,2%). Same move in SEA (+35% in those cats) →
  weather/demand, not SEO. 22 Jul was the trough of the heat cycle, so the WoW
  comparison flatters. Growth was mobile-only (+3,0% vs desktop −3,8%).
- **Root cause found for the "facet CTR drop"**: /c/ outclicks-per-visit is down ~15%
  over 12 months on every facet depth, with a hard step on **10 March 2026** (0,897 →
  0,815 overnight, all depths at once, visits flat) = the bol.com cliff seen at rate
  level rather than volume. The 29-July dip itself was noise (−2,5σ in the noisiest
  bucket). Ruled out along the way: settlement lag, category mix, hour mix, device,
  low-intent traffic dilution, and any single broken facet combination.
- **Not confirmed**: the 3+-facet long tail is *not* drying up — it is the only growing
  part of /c/ (visits/day +14% YoY) while 1-facet is −18% and 2-facet −19%.
- Settled-revenue read (28 vs 21 Jul, since D-1 revenue is unsettled): −6,5% on +1,9%
  visits. Largest mover Sanitair −€413 is lumpy CPR attribution on ~1.300 visits, not a
  trend. Heat traffic monetises below average (Sport & outdoor visits +13%, revenue −32%).
- Both LEARNINGS entries logged; memory `c_facet_outclick_rate_erosion` written. One
  open follow-up in BACKLOG (is the post-cliff drift a second decline?).

### Done 2026-07-31 — gap combos measured on TRAFFIC, then 1.789 built

**The gap is almost all dead traffic — URL counts were a bad proxy.** The earlier
"66.416 buildable gaps, Laptops 9.870 URLs" framing counted URLs, which says nothing
about demand. `scripts/analysis/seo_titles_gap_traffic.py` attaches real SEO visits via
`fetch_top_urls()` (the generator's OWN query, so a combo's number is what the
generator would have seen).

Measured 2025-07-30..2026-07-30 (365d): **40.156** buildable uncovered combos drew
**73.086 visits/yr** and **EUR 5.419** — ~1,8 visits and 13 eurocent per combo per
year. Not one uncovered combo reaches a visit per WEEK (best: 35/yr, Bouwstenen). Depth
does NOT stratify: 1,5-3,6 visits/combo at every level, so there is no rich seam — the
"just build depth 3-4" idea was wrong for the same reason.

Threshold curve: >=3 visits/yr = 6.616 combos / 45% of gap traffic · **>=6 = 1.789
combos / 21%** · >=12 = 284 / 5,7% · >=52 = **0**.

**Built the >=6 cut: 1.789 blueprints** via
`scripts/analysis/seo_titles_build_gap_combos.py --apply` — 4,5% of the combos for a
fifth of the traffic, growing the set ~2,4% instead of ~53%. status='built' (Publish
stays a deliberate click) and each row carries its sample_url as `source_url`, so these
are the FIRST built rows whose Facets column links to a live example. Blueprints now
**33.749 built / 43.889 pushed**. Undo is printed by the script.

**WATCH THE DATE WINDOW.** `fetch_top_urls()` defaults `date_to` to a hardcoded
**20260608**, so a run without `--date-to` silently stops there. A first pass with only
`--date-from 20260501` therefore measured 39 days, not 3 months, and hid all
seasonality (Kerstversiering is 3 of the top 12 over a full year). Always pass both.

**Answered:** the 31.960 built titles did NOT need regenerating for the facet-order
work — the reorder check reports 0 stale rows across all 75.849 blueprints. Note there
is no before/after COLUMN; what changed was order values plus UNKNOWN_ORDER 1500->1750.

**Open:** whether to also build the 3-5 visits/yr band (4.827 more combos, another 24%
of gap traffic). Counter-argument worth weighing: this measures existing demand, not
headroom — if thin pages underperform *because* they lack a title, current visits
understate the upside, and the 3+-facet tail grows +14% YoY.

### Done 2026-07-30 (Dagoverzicht v2 + SEO-titles gap analysis)

**SEO stats — Dagoverzicht.** Legend and the info line removed; headings are now
"Apparaten - Visits" / "Apparaten - Omzet"; Desktop `#7BAB3A` and Tablet `#AA3A95`
(trio re-validated: all six checks pass, worst adjacent ΔE 20.6 protan). Day picker +
Refresh moved out of the purple header into the card body as `btn-outline-purple` with
`↻ Refresh` — purple-on-purple was unreadable.

**Tiles are sparkline tiles now**, in Dagoverzicht AND the top summary row. The
Dagoverzicht ones come from a new `series` block on `/api/seo-stats/dashboard` (14
days, same `_fetch_daily` rows as the tiles, so each series' last point IS the tile
value); the summary row needs no request — `lastData.daily` is already in memory.
CTR/Bounce/OPB gained WoW badges: **pp** for the two rates, % for OPB.

**Third donut: "Type urls - Visits"** (R-url 50,9% / C-url 32,4% / PLP 14,7% /
Browse / Overig), classified from the URL path, summing exactly to the headline
visits. **No revenue counterpart** — see LEARNINGS: the revenue table's `vis_url` is
the outbound shop URL.

**Gap analysis: Unique Titles vs SEO titles.**
`scripts/analysis/unique_titles_vs_seo_titles_gap.py` — of 1.022.296 URLs with a
unique title, 901.520 fall in a covered (cat, combo); **67.498 combos are not
covered**: 66.416 buildable (113.175 URLs) + 1.082 impossible. Laptops dominates
(9.870 URLs), then Smartwatches, Schoenen, Tablets. CSV:
`Downloads/claude/unique_titles_without_seo_title.csv`.
**Open decision:** whether to run a generation pass for those 66.416 buildable combos
— they are mostly deep 4-facet combos, so it would grow the blueprint set by ~2x.

**Rebuild status confirmed:** re-running the reorder check over all 75.849 rows now
reports **0 changes** — every built AND pushed blueprint matches the current rules.

### Done 2026-07-30 (SEO titles blueprints) — facet order, impossible + junk combos

**Facet order (DB-only, `pa.facet_position_rules`; no code change).** The category
placeholder sits at `SUBCATEGORY_ORDER = 1700`, so `order_index < 1700` renders
before the noun and `> 1700` after. Moved after the category on Joep's request:
`verpakking` 613→2295, `verpakking_bier` 1609→2296, then the contents family
`inhoud_parfum_ml` 962→2301, `inhoud_aquarium` 957→2302, `inhoud_glijmiddel`
1500→2303, `inhoud_jerrycan` 1504→2304, `motorinhoud` 1508→2305. Packaging sits
BEFORE contents deliberately — Dutch titles end with the size ("navulling 100 ml").
Every row's `reasoning` records the old value. **This table is also read by
`ai_titles_service`, so the AI unique titles reorder too.**

**Impossible combos (facet dependencies).** The Taxonomy API models them:
`type_parfum` (3432) depends on `merk` (3027) via `dependentMetadata` /
`/api/Facets/{id}/value-dependencies`. Audit found **1.553 of 77.619 rows (2,00%)**
impossible — whole families, not one facet (`pl_*`/`productlijnen-*` need `merk`,
`kleurtint_*` need `kleur`). **1.377 built rows deleted** (backup in
Downloads/claude). Script: `scripts/analysis/seo_titles_dependency_audit.py`.

**Junk combos.** `parse_url` accepted a facet with an empty value, so junk URLs
(`/c/merkm~`) became blueprints. Parser fixed; **356 built rows deleted** of 360
found. Script: `scripts/analysis/seo_titles_purge_junk_facets.py`.

**Default order fixed:** `UNKNOWN_ORDER` 1500 → 1750, so a facet with no rule now
trails the noun instead of preceding it. The `side`-column refactor was recommended
and then WITHDRAWN — see LEARNINGS for why (type facets are the noun and 699 of 747
sit below 1700).

Blueprints now: **31.997 built / 43.889 pushed** (was 33.730 / 43.889).

**Done since:**
- **Stale pushed rows re-pushed.** A facet-order edit only changes future builds;
  94 pushed rows still carried the old phrase. `scripts/analysis/
  seo_titles_repush_stale.py` flips exactly those to `built`, rewrites them, and
  publishes that combo set (`/page-titles` is an upsert). Ran to production:
  `HTTP 200 {"records":94}`, all back to `pushed`. 66 of the 94 were the
  `geschikte_leeftijd` pin, so live Speelgoed titles changed too — intended.
- **Built rows rebuilt for the new order** — 72 rows, via
  `scripts/analysis/seo_titles_reorder_preview.py --apply`. The "rebuild all 31.997"
  framing was wrong: only 166 rows differed at all.
- **RETIRED built rows purged** (37). The other 37 are `pushed` — see below.
- **Dependency check wired into the generation path.** `pa.facet_dependencies`
  (711 rows: 504 children need `merk`, 43 need `kleur`) is refreshed with
  `seo_titles_dependency_audit.py --refresh-cache`, which probes EVERY taxonomy
  facet so a dependent facet is covered the first time it appears in a URL. The
  generation loop skips such combos and counts them in a new **Impossible** counter
  shown in the run stats. An empty cache degrades to the old behaviour (build
  everything) rather than dropping every combo. Full replace on refresh, so a
  dependency removed upstream stops blocking.

**Open:**
- **`/page-titles` CANNOT unpublish** — verified: it answers `Allow: POST`, no DELETE,
  no GET. So the **176 pushed impossible + 4 pushed junk + 37 pushed RETIRED** rows
  can't be removed from the live site by us. Two routes: ask the API owner for a
  delete verb, or upsert a neutral title over those combos. Deleting our own row is
  the wrong move — it leaves the blueprint live AND drops it from the dedup log, so
  the combo gets rebuilt and re-pushed.
- **AI check on facet order — agreed plan, not started.** Sequence, in this order:
  1. **Calibrate before trusting.** Run the classifier over facets that ALREADY have
     a human rule and measure agreement on (side before/after, is-type-facet). If it
     can't reproduce human judgement where the answer is known, it doesn't get to
     fill gaps. Inputs need no extra work: `GET /api/Facets` returns slug + Dutch
     label for all 3.668 facets.
  2. **Backfill only genuine gaps**, stamped `source='ai'` with a `reasoning` line so
     AI rows stay distinguishable from manual ones.
  3. **Report disagreements** on existing rules ranked by how many blueprint rows
     they touch — a shortlist to review, never a mass update.
  Keep expectations calibrated: of 5.469 taxonomy slugs with no rule only **4** are
  used in blueprints, so the payoff is mostly future-proofing, not a backlog fix.
  Remember `ai_titles_service` reads the same table, so any change moves the AI
  unique titles too.
- **74 non-type facets sitting exactly at 1700** — behaviour is right (they land
  after the category) but by an alphabetical tie-break, not by declaration.
- **`t_zandspeelg` needed no change** — it is already `is_type_facet=true` (order
  1031) and the live H1 "Hape Speelgoedemmers Strandspeelgoed" proves it: the
  category *Zandspeelgoed* is suppressed. If anything reads wrong there it is
  `thema_speelgoed` (order 1929 + `position=end`) trailing the noun — open question.

### Done 2026-07-30 (later) — suggestions_new.txt items 1-7 complete, page widths, Healthscore deck

`suggestions_new.txt` is **fully worked through**. Commits: `daf5eea` `73a77ae`
`2bcdfc5` `cb3acc4` `9cfa834` `6f2d5af` `75c25f8` `7a1a0e4` `b83e3d1` `db7092d`.

- **[1] weekday filter** in Per-day overview — click a Day value to filter, click
  again to clear. Totals follow the filter; tiles + export stay whole-range.
- **[7] no view-jump** on that filter: the clicked row keeps its screen position
  (capture its viewport offset, shift the window by however far it moved). It jumps
  to the page **holding the clicked date**, not page 1 — clearing a filter grows
  every index, so the anchor row can land on a later page.
- **[3] chart chrome + dark hover card**, **[6]** x-ticks read "Jul 29" while the
  labels stay ISO (the tooltip parses them as dates). Vertical gridlines were
  removed and then **re-added on request**, in the faint grid colour.
- **[4] Dagoverzicht** — new single-day section + `GET /api/seo-stats/dashboard`.
  Device split from `useragent` on BOTH tables (Joep's choice); tablet tested before
  mobile (an iPad UA has neither "mobi" nor "iphone", Android tablets have
  "android"). Donuts with hover, validated palette. Tie-out verified against the
  headline figures.
- **[5] Top categories collapsed** behind a wide centred banner-button under the
  title bar (moved out of the header on request).
- **[2] Healthscore deck** → `Downloads/claude/Healthscore_2.0_vs_1.0.pptx` + `.pdf`,
  builder in `scripts/analysis/healthscore_hs2_presentation.py`. Style sampled from
  the GEO deck; every figure from `pa.hs2_shadow` / `pa.healthscore_coverage` /
  `pa.hs2_sitemap`, seasonal caps recomputed from the CSV (70,2→84,7 visits).
- **Page widths** — now two sanctioned widths, `col-md-10` default and `col-lg-11`
  for SEO stats / Healthscore / SEO titles / DMA Exclusions. See UI_BLUEPRINT and
  LEARNINGS; net effect is only that the two bare-container pages came inside.
- **Export dropdown** on Recent (FAQ) Results replacing three buttons, both pages.
- **IndexNow** Submission History table capped at 800px and centred.

**Open / next:**
- **Example-URL links on Built titles' Facets column — needs a decision.** The link
  already works where a URL exists; built rows have none *by design*. Two options in
  BACKLOG; recommendation is the Redshift traffic match with no-link fallback.
- **Export endpoints are `async def` doing full-corpus work** — one Combined export
  from the UI freezes the dashboard for everyone. See BACKLOG + LEARNINGS.
- **healthscore.html's Apps button** renders oversized: its grid SVG has no
  `width`/`height` (only a viewBox) and it lacks `title="Apps"`/`aria-label="Apps"`.
- Dagoverzicht costs ~25s on a date not queried before (5-min cache after). A nightly
  pre-warm of yesterday would hide it; not built.

### Done 2026-07-30 — SEO stats blueprint pass, Windows launcher, FAQ Publish 2.0 staging

**SEO stats (4 chat items + 1 follow-up)** — `frontend/seo-stats.html`:
- Refresh in "Visits & revenue per day": `btn-outline-secondary` (which style.css
  themes **orange**, so it read as a second CTA next to Load) → `btn-outline-purple`
  with the `↻` glyph.
- Performance standup now draws skeletons instead of a grey spinner overlay
  (tiles + both lists; also drawn in `load()` because `loadDeltas()` starts only
  after `/daily` resolves; failure path clears them). Overlay CSS deleted.
- `HEAT_KEYS` gained `dma_omzet`/`gsaas_omzet` — the two columns behind "Show DMA &
  GSAAS revenue" rendered white between shaded neighbours.
- "Export Excel" → "Export"; Export and Load both moved from inline hexes to
  `btn-outline-purple` / `btn-run`, so `disabled` renders grey (blueprint: unavailable
  wins over colour). Blueprint actually prescribes **orange** for Export-type
  actions — left purple deliberately, flagged to Joep.

**Windows task "DM Tools Dashboard" fixed** — failed every morning with result 1.
Root cause reproduced: `wsl.exe -e bash -c "… &"` never leaves a live process.
Rewrote `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` (backup
`.bak-20260730`): `nohup setsid`, WSL-readiness retry, 120s deadline poll,
idempotent (skips if :8003 answers → no duplicate GSD-LL APScheduler), logs to
`%LOCALAPPDATA%\dm-dashboard-launcher.log` + `logs/uvicorn-8003.log`, no `ReadKey`.
Task definition untouched. See LEARNINGS for the two self-inflicted bugs
(`&&`-chain + `&` precedence; PowerShell 5.1 quote mangling).
- **Open:** `Microsoft-Windows-TaskScheduler/Operational` is disabled on this
  machine, so there is no per-run history. Enable it if this recurs.

**FAQ Publish 2.0 → staging** — `backend/faq_v2_publisher.py`, `backend/main.py`,
`frontend/js/faq.js`. Push state is now per `(url_id, env)` (idempotent in-place
migration, existing rows → `production`); stats endpoint takes `?environment=` and
reports `has_api_key`; frontend quotes the selected env's counts. The staging key
was **already** in `.env` and identical to the one Joep sent — nothing added.
Verified with a `limit=3` staging push (18 records) and no cross-env leakage.
- **Open:** those 3 URLs now have live FAQ records in staging (harmless; the
  endpoint upserts on `(url, question)`). Cleanup = 3 × `DELETE /faq?url=` plus
  deleting their `env='staging'` state rows.
- **Open:** the staging key is in the chat transcript — worth rotating.

## suggestions_new.txt backlog — second UI/feature list from Joep (opened 2026-07-29)

Source of truth is `/home/joepvanschagen/projects/dm-dashboard/suggestions_new.txt`
(untracked). A **new** file, separate from `suggestions.txt` — don't confuse them.

**BEWARE: this file gets RENUMBERED.** It opened with 10 bullets + two blank
lines, then grew to 22 bullets with the blanks removed — so what were items 13-22
during the work became **lines 11-20**. Item numbers in the first commit message
(`f39e9df`, "items 1-22") are the numbering *at the time of that commit*, which no
longer matches the file. Always re-read and re-count; never trust a remembered
line number here. Joep also appends mid-session (11-20 arrived after 1-10 were
reported done, then 21-22 after that) — same warning as the first list.

Done in `f39e9df` — current line numbers in brackets:
1 [1] SEO stats Total visits / Total revenue, 2 [2] Top categories Yesterday
swap, 3 [3] chart palette, 4 [4] Update Excel "i" hover, 5 [5] URL Validator
layout + rename, 6 [6] table-header weight, 7 [7] Reset outlined orange, 8 [8]
Recent Results × direction, 9 [9] Process/Validate Batch renames, 10 [10] SEO
Priority filter warm-up, [11] Redshift upload buttons, [12] Keyword Planner
button spacing, [13] Keyword Redirects progress bar, [14] IndexNow Export
centring, [15] Index Checker button + info text, [16] Thema Ads three fixes,
[17] Shop-campaigns headers + Refresh, [18] Redirect Tool input-group, [19]
Redirect Generator button height, [20] R-Finder × SVG. Plus the chart-tooltip
height jump, which came in over chat and is not in the file at all.

**Item 1 was rebuilt**: first read as two Per-day-overview *columns*, actually
meant two *chart series* in "Visits & revenue per day". The column version was
removed. See LEARNINGS — "Per-day overview" in this file has twice meant the
graph above it.

[21] and [22] done in `7170e43`:

- **[21] Unique titles** — pencil centred (`top:50%` + `translateY(-50%)`, was
  `top:0.4rem`); `this run: -` gone from the Failed tile (element **and** the two
  JS writes to `#aiFailed`); Start Batch → Process Batch (markup + the restore in
  the finally-block); edit pencil added to Recent Results.
  **This one needed a backend change**: `get_recent_results` did not select
  `c.description`, and the save form posts all four fields as CSV — so the new
  pencil would have written `"undefined"` over a good description. Added the
  column, and hardened `editTitle` with `?? ''` because a failed row's content
  columns are NULL (LEFT JOIN on `unique_titles_content`).
- **[22] Canonicals** — "Generate Canonicals" → "Generate", plus the placeholder
  sentence that quotes the button name.

### Follow-up polish, asked over chat rather than added to the file (2026-07-29)

Not numbered anywhere — if you are reconciling the file against the commits, these
have no matching bullet:

- SEO stats: chart legend forced onto one row (11px / padding 8 / 6px dots) and
  `DMA Organic` shortened to `DMA` in `METRICS`, which also renames the table
  header and the Excel column. Per-day WoW headers reduced to plain `WoW %` (the
  metric name stays in the `title` **and** in the Excel export, where the strings
  are object keys and a bare "WoW %" would collide and drop columns). Notes column
  20% → 12% while Show deltas is on. Whitespace around the two Top-categories
  sub-headings.
- Thema Ads: orange CTAs right-aligned in every tab (Activate Ads, Download CSV,
  Find & Remove Duplicates were left-aligned), and the Check-up pair taken out of
  a `.btn-group`. Note `.btn-warning` / `.btn-success` are BOTH themed
  `--color-button` orange here, so "the orange buttons" is nearly all of them.
- All remove-× consolidated onto shared `btn btn-outline-red btn-remove-row` +
  SVG: Redirect Generator (4), Canonicals (6), Kopteksten, FAQ's, R-Finder.
  `.btn-danger-invert`, canonical's `.btn-remove` and rfinder's page-local
  `.btn-remove-row` all deleted. See LEARNINGS — this is the item-8 follow-up that
  came back because the inverted class was left in place the first time.
- Thema Ads' Start / Pause / Resume right-aligned too — also a `.btn-group`, also
  three independent actions (only one visible at a time). No `btn-group` left on
  that page.
- **Chart palette resolved** (was the open decision). Joep chose DMA revenue →
  bordeaux `#722F37` and GSAAS revenue → navy `#001F3F`; that forced Total revenue
  off dark plum to `yellow-800 #936305`, and Total visits off dark blue to deep
  turquoise `#107063` after Joep found it too close to navy. Failing pairs 10/45 →
  **5/45**, all five between the untouched brand base-500 hues. Measurements are in
  the `METRICS` comment in seo-stats.html — read it before touching any of it.
- Frontpage (`dashboard.html`) tools alphabetised within each category. Only
  Google Ads was wrong (GSD Check after MC ID Finder; Thema Ads before
  Shop-campaigns). Uses the **navbar's** order, not a strict string sort: a literal
  sort puts `DMA+` after `DMA Exclusions` (space sorts before `+`), which would
  make the frontpage disagree with the nav on all 33 pages.

### Category title coverage + FAQ Publish 2.0 (2026-07-29, all over chat)

- **Bare category page titles come from Unique Titles, NOT tblPageTitles** — see
  LEARNINGS. Coverage 3,435/3,543 (97.0%); the 108 gaps were all just missing from
  `pa.urls`. Worst maincat was **Muziekinstrumenten: 45 of 96 lacked a TITLE**
  (i.e. no `unique_titles_content` row) — say it that way, not "missing from
  pa.urls", which is what an earlier version of this line said and it cost a
  round trip on 29 Jul: someone read it as still-to-load work when the load had
  already happened in the batch below. **Muziekinstrumenten is DONE** — 95/96 in
  `pa.urls`, 44 queued pending, 51 already titled; the 96th
  (Contrabaskammen `9000570`) is a 404 and deliberately excluded.
- Loaded the **95 live** of those 108 into `pa.urls` + queued `pending` in
  `pa.unique_titles_jobs` ONLY (faq/kopteksten untouched). 13 were 404 and skipped.
  Rollback tag: `notes = 'category bare-url unique-titles gap 2026-07-29'`
  (delete from unique_titles_jobs first, then pa.urls). Generation runs behind the
  existing ~22.5k pending backlog.
- Pushed Wandpanelen's 15 built blueprint combos to prod `/page-titles`
  (`{"status":"OK","records":15}`). **Two dangerous defaults in `publish_built()`**:
  `combos=None` pushes ALL 33,745 built rows, and `push_unique_titles=True` fires a
  full CSV upsert of the entire unique-titles corpus. Always pass both explicitly.
- **FAQ "Publish 2.0"** shipped (`ed1a4ed`): `backend/faq_v2_publisher.py` +
  `POST /api/faq/publish-v2` (+ `/status/{id}`, `/stats`) + a button between Refresh
  and Publish in FAQ's Content Publishing. Full contract for the undocumented
  `/faq` section is in the module docstring. It is **additive** (upsert on
  (url, question)), so stale questions survive a regeneration — `replace=True`
  exists but costs one DELETE per URL (~280k) so it is not the default.
  Note `schema_org` has no home in `/faq` — the endpoint rejects the field.
- **Publish 2.0 is incremental by default** (`fc095bd`): `pa.faq_v2_push_state`
  (url_id PK, content_md5, records, pushed_at) tracks what was pushed;
  `mode="new"` (default) sends only URLs with no state row or a changed
  md5(faq_json), `mode="all"` re-pushes everything. The UI's "Publish 2.0" button
  is incremental; the small **"all"** button beside it is the full ~1.7M re-push.
  See LEARNINGS for why md5 beats an `updated_at` watermark, and for the testing
  trap (with a `limit`, `mode=new` advances to the NEXT n URLs — judge it by
  `pa.faq_v2_push_state`, not the record count).
  **Still unexercised: the unbounded run** (largest test is 3 of 280,636 URLs)
  **and the button in a real browser** — DOM wiring only verified statically.

### Open
- Nothing in `suggestions_new.txt` as of 2026-07-29 15:05 — but RE-READ the file
  rather than trusting this line; Joep has appended to it three times this session,
  and several later requests arrived over chat without ever entering the file.
- Optional, never asked for: the last 5 colourblind-unsafe pairs in SEO stats'
  chart all sit between brand base-500 hues (GSAAS visits ↔ SEO CTR is the worst
  at CVD 4.9). Fixing them means moving GSAAS visits and SEO Bounce off their 500
  stops — i.e. editing brand colours, so don't do it unprompted.

### Deliberately not done (reported to Joep, not refused)
- Thema Ads' seven other `.alert-info` blocks are collapse disclosure content,
  not banners, so they stay grey. Only the Auto-Queue banner was asked about.
- ~~`thema-ads.html` Activation Plan Refresh~~ — **DONE** in `ed1a4ed`: was the
  last `btn-outline-primary` Refresh in the app (that class is themed ORANGE
  here), now `btn-outline-purple`, and its card header moved from `float-end` to
  flex. Four other `btn-outline-primary` buttons remain app-wide (redshift-upload
  Parse, dma-plus Copy Results, dma-exclusions Preview + Re-enable) — all orange,
  none a Refresh, none asked about.
- The chart palette's colourblind-unsafe pairs: **resolved, no longer awaiting a
  call.** Joep chose bordeaux + navy (see the section above), taking it from 10 of
  45 failing pairs to 5 — and all 5 that remain sit between untouched brand
  base-500 hues, so closing them means editing brand colours. Left alone
  deliberately; measurements are in seo-stats.html's METRICS comment.

## suggestions.txt backlog — UI/feature list from Joep (opened 2026-07-28)

Source of truth is `/home/joepvanschagen/projects/dm-dashboard/suggestions.txt`
(untracked, now 50 lines / 43 bullets). **Joep appends to this file mid-session** —
item 49 arrived after the first batch and item 50 after the second, so RE-READ it
before reporting "nothing left" rather than trusting these notes. Item numbers
below are that file's LINE numbers, which is the vocabulary used in the commits.
**ALL 43 done**, in `569288a` (23 items), `10f4152` (8 + 14), `8be2ca1`
(11, 15b, 18b, 19, 20b, 25, 49), `45a1339` (33, 39, 40), `17916fe` (9),
`57ccbb7` (7), `b593ca2` (6), `2493689` (1), `d6b8c91` (2), `7ddb91d`
(48 dry run), `b69bd20` (48 --write), `5c2556e` (47) and `2694478` (50).

### Done
1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
23, 24, 25, 26, 29, 30, 31, 32, 33, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
50.

Three "done" items keep a deliberate leftover reference, so don't treat a grep
hit as unfinished work: item 46's `tsvOutput` and item 39's "View Full Content"
survive only as **code comments** explaining what was removed, and item 13's
Healthscore is still linked from `healthscore.html`'s OWN navbar (it is unlinked
from the other 31 pages and the frontpage, which is what was asked).

Notes worth keeping from the first batch: every button touched moved to the
canonical classes in `style.css` (which also brings the grey-outline disabled
state); the theme repaints Bootstrap's danger variants orange, so
`#resetValidationBtn` and `.btn-remove` needed explicit red overrides;
Healthscore is unlinked from 31 navbars but the page is intact and still
reachable by URL. Item 43's hover pencil was never seen rendered — it only
appears once a Search Titles query returns rows.

Notes from the 2026-07-28 tables/features batch:
- The four history tables (IndexNow, Auto-Redirects, GSD Budgets, DMA Bidding)
  now share one shape: `.tool-table-wrap` + grey sticky 1rem headers + sortable
  glyphs + skeleton rows. IndexNow's was a 4-column CSS grid, not a table.
- **Sorting must key the raw value.** Timestamps sort as epoch; the Input and
  Changes columns needed `_filename` / `_changes` flattened out of
  `params`/`summary` at load time to be sortable at all.
- **Item 18b's premise was wrong in the blueprint, not in the code.** GSD
  Budgets already used the native `<input type="date">` UI_BLUEPRINT described;
  what "the blueprint picker" means in practice is the purple **flatpickr** that
  SEO stats / SEO titles / GSD Campaigns / Shop-campaigns all render. Added it
  there and corrected UI_BLUEPRINT. Still native (deliberately, nobody asked):
  DMA Bidding, SEO Priority, Performance Standup, R-Finder.
- Item 9's Old URL is **not** the same string as the backend's redirect `old`.
  The column is the category url with `/c/winkel~{shop}` swapped for
  `/r/{keyword}/`; `keyword_redirect_service._kw_to_old_path` builds a site-wide
  `/products/r/{keyword}/` instead. A row can produce several (final_keyword is a
  `; `-joined combination set) — the cell lists them all.
- Items 39/40: the row-click toggle bails on `closest('a, button')`, which is
  what keeps the url link navigating, the delete × working, and the FAQ
  accordion's own question buttons from collapsing the row around them. It also
  bails while text is selected so drag-to-copy doesn't fold the row shut.
- **Bug found by actually rendering the page:** IndexNow's Submission History
  never drew a single row. `loadHistory()` was called during script evaluation
  but reads a `let` declared further down the same block → temporal dead zone,
  and the ReferenceError died silently inside the async function. Bootstrapping
  from `DOMContentLoaded` fixes it. Worth checking for elsewhere: any page whose
  init calls run at the top of a script block that later declares `let`/`const`.

### Features done 2026-07-28 (second half of the session)
- **7** — skeleton CSS moved into `style.css` (it had been pasted into seven
  pages); added to every table that is on screen and filled by a single fetch.
  **Left on a progress bar on purpose:** long multi-item runs (URL
  Checker/Validator, Index Checker, Redirect Checker/Generator, Canonicals,
  Thema Ads, Keyword Planner's two Google-Ads tables) — a skeleton claims data is
  arriving *now*, and those tables are hidden until the run ends anyway.
  Three traps it surfaced: several `catch` blocks never replaced the skeleton (so
  a failed load shimmered forever); a table filled by a function that runs after
  *another* table's fetch needs its own up-front skeleton (SEO stats
  `loadDeltas`); and runtime-built headers mean reading the column count back off
  the rendered `<th>`.
- **6** — the module from `performance-standup.html` now sits in SEO stats between
  Per-day overview and the (different, pre-existing) "Performance standup" card,
  renamed "Update Excel". **Every id is `ps`-prefixed** — the page already owns
  `#startDate`/`#endDate` for the chart range, so an unprefixed paste would have
  had the two fighting over the same inputs. Own IIFE, `ps-`namespaced CSS.
  Standalone page unlinked from 32 navbars + its dashboard tile, still works by URL.
- **1** — maincat + deepest-cat type-ahead (`<datalist>`, no JS library) on the SEO
  Priority run form, deepest filtered by maincat. New
  `GET /api/seo-prio/categories`. **The list must come from Redshift's
  `dv.main_cat_name` / `dv.deepest_subcat_name`, not the taxonomy API** — the
  labels don't always agree and a mismatch silently returns zero rows minutes into
  a run. That DISTINCT costs ~23s for 3,845 pairs / 31 maincats, so it runs on a
  daemon thread (~40ms response, `loading:true`, frontend polls) with a 24h
  in-process cache + `backend/data/seo_prio_categories.json` so restarts are warm.
  A scope that matches nothing now ends as an **error naming the scope**, not a
  silent 0-row run.
- **2** — URL Checker became the "Check live URL's" module at the bottom of URL
  Validator, and the Validator's output is restyled to match the Checker's (card,
  green header with the actions, badge summary, scrollable fixed-column table).
  **The two collided on nearly every id** (`urlInput`, `fileInput`,
  `progressCard`, `progressBar`, `resultsBody`, `resultsCard`) and on `results` /
  `showResults` / `escapeHtml`, hence `chk`-prefixed ids + its own IIFE + CSS
  scoped under `#checkModule`. The copyable url cells moved from
  `onclick="copyToClipboard('…')"` (hand-rolled JS-string escaping over scraped
  text) to tbody event delegation reading `data-copy`. Fixed in passing:
  `loadCache()` wrote to `#cacheContent`, which is not in the markup — it threw on
  every page load and the `catch` threw the same way, so it was unhandled.

- **47** — "+ Add filter row" in R-Finder; boxes within a row still AND, each ROW
  is its own search with its own result set, its own limit, and its own lines in
  Copy/Download (the row+filters columns appear only when there is more than one
  row, so single-row output is byte-identical to before).
  **Rows are separate QUERIES, not one query with the rows OR'd.** A shared query
  lets a broad row eat the whole LIMIT and return nothing for a narrow one —
  checked with `['/r/']` vs `['vloerkleed']` at limit 20, both still return 20.
  One row = a full ~17s /r/ scan, so rows run concurrently (3 rows in 4.8s),
  **capped at 4** — the Redshift pool is maxconn=10 and `get_redshift_connection`
  *raises* rather than blocks when it is dry, so leaving 6 free is deliberate.
  New `fetch_r_urls_by_row()`; `filters` and the `urls`/`total` response fields
  still work for older callers.
  **Found a latent bug in shared code doing this:** `database.py`'s pool getters
  had an unguarded lazy init, so N threads racing their *first* DB call each built
  a separate pool, last assignment won, and a connection from an orphaned pool
  died on return with psycopg2 "trying to put unkeyed connection". Reproduced with
  a 12-thread barrier on a cold pool; fixed with a double-checked lock. This could
  have hit **any** concurrent cold start, not just R-Finder.
  Also fixed: the CSV writer wrapped urls in bare quotes with no escaping, so a
  url containing `"` produced a broken row.
- **50** — Redirect Tool: preview rows get a checkbox (+ select-all) and Submit
  posts only the ticked ones, where before it always posted the whole preflight.
  "Copy for Excel" → "Copy" (this tool only; URL Validator and R-Finder keep
  theirs, which item 50 didn't mention). Label column gone from Recent results —
  header, `<td>`, all three colspans and the skeleton width, now 8 columns.
  **Selection is keyed on each row's ORIGINAL index into `preflight.processed`,
  not its display position**, and lives outside the DOM: the table is sortable +
  filterable and `renderPreviewTable()` rebuilds every `<tr>`, so a positional key
  would submit the wrong rows the moment you sort.
  Safety decisions, because this writes live redirects:
  - Default = every submittable row selected, i.e. exactly what Submit did before.
  - **Empty selection submits NOTHING**, no fallback to "all" — an accidentally
    cleared selection must not become a full push.
  - Submitting a subset confirms, naming N of the total.
  - Skipped / already-correct rows get a dash, not a checkbox (no API call to
    make, so a checkbox would imply otherwise).
  - The "Replace existing" toggle moves rows in AND out of the selection —
    without that, turning it off could leave a now-skipped row ticked.
  - Select-all acts only on rows VISIBLE under the current filter.
  - **Selection clears after a successful submit.** The preflight data is not
    refreshed, so those rows still look submittable; leaving them ticked would let
    a second click silently re-post what just went live.
  `submit_rows()` iterates rows independently with no cross-row logic, so posting
  a subset is safe — verified before wiring it up.
- **48** Top 5 facets per category by visits (all channels) → all combos as SEO
  title blueprints. **Joep chose DRY RUN (2026-07-28): generate to Excel, push
  nothing.** Script: `scripts/pagetitles_top5_allchannel_combos.py`.
  `canon_key` sorts facet types, so order does NOT create distinct keys: "all
  combos of 5" = **31 per category**, not 325 — verified empirically (the 31
  combinations and all 325 permutations collapse to the same 31 canon_keys).
  Not the same as the existing `scripts/pagetitles_topn_combinations.py`, which
  ranks by **SEO** visits from a stale `/tmp/seo_traffic_rows.pkl`; item 48 wants
  **all channels**, so the new script queries Redshift with no
  `marketing_channel` filter and reuses `seo_titles_service.build_blueprint` (the
  live push logic) rather than the archival script copy.
  **Dry run of 2026-07-28** (`--from 2025-01-01 --url-limit 400000`, all channels,
  20250101..20260728) →
  `Downloads/claude/top5_facet_combos_allchannel_2026-07-28.xlsx`:
  - 400,000 faceted /c/ urls / 11,198,978 visits → **3,441 categories**
    (399,847 urls used; 83 unresolvable slug, 70 no facets)
  - **73,105 blueprint rows — 33,612 NEW, 39,493 already exist**
  - Not 3,441 × 31 = 106,671, because most categories have fewer than 5 distinct
    facets in the traffic. So the old "~110k" estimate was the ceiling, and the
    real ask is **~34k new rows**, not 110k.
  - Sanity-checked: no duplicate (cat_id, key), titles capped at exactly 200
    (the MAX_TITLE_LEN trim), longest h1 148.
  - Sheet 2 `per_category` lists each category's ranked top facets with their
    visit counts, so the ranking can be checked without reading 73k rows.

  **CREATED for real on 2026-07-28** (`--write`, second run): **33,745 rows in
  `pa.seo_titles_blueprints` with status='built'**. Table is now
  `pushed=43,874 | built=33,745`. Counts differ by ~130 from the dry run above
  because the Redshift window moved on by a few hours between the two runs —
  the ranking is recomputed each time, it is not a fixed snapshot.
  **Nothing was pushed to /page-titles.**
  - `source_url` is NULL by design: these combos are synthesised from the
    ranking, not scraped from one URL. `publish_built()` only reads
    cat_id/key/title/h1_title/description/country_code so that is safe, but it
    does mean the optional per-URL AI-title push has nothing to do for them.
  - ⚠️ **This changes what the SEO Titles "Publish" button does.** With no rows
    selected it pushes ALL status='built' rows, and before this there were none.
    Select rows to push a subset.
  - Undo: `DELETE FROM pa.seo_titles_blueprints WHERE status='built' AND
    created_at >= '2026-07-28 16:43:41';` — verified to match exactly 33,745 rows
    and zero 'pushed' ones.
  - **`created_at` is UTC.** The first `--write` run stamped `datetime.now()`
    (CEST) and compared against a naive TIMESTAMP filled by `now()` on an
    `Etc/UTC` server, so the verification count printed 0 and the printed undo
    statement would have deleted nothing. The script now takes its stamp from
    `SELECT now() AT TIME ZONE 'UTC'` and shouts if the count disagrees. Same
    trap as [postgres_utc_timestamps_display].
  **Still pending before any push:** `/page-titles` validates a POST atomically in
  5,000-row batches, so one bad row fails 5,000 (see LEARNINGS 2026-07-27) —
  push a selected subset rather than all 33,745 in one go.

### Where the wandpanelen thread ended (2026-07-28)
Root cause fixed and pushed (`60dfe78`): category resolution is API-first with
`cat_urls.csv` as a rewritten cache. Background walk **confirmed** in a
long-lived process (0 → 3,543 categories at ~t=131s). The 5 wandpanelen urls
regenerated **5/5 correct** ("Groene Woonaccessoires" → "Groene Wandpanelen").

**Correction to the earlier measurement:** the "352 urls across 37 categories"
figure was a substring heuristic and is mostly FALSE POSITIVES. Regenerating all
347 of the rest scored only **~4 that now name the deepest cat** — the others are
deliberate **type-facet overrides**, where a facet is meant to suppress the
category name (e.g. `type_knikkerbaan` → "Knikkers" instead of "Knikkerbanen").
So the real blast radius of the CSV bug was ~9 urls, not 352. Do NOT re-run that
regeneration expecting a big win, and do not treat the heuristic as a bug list.

**Open, needs Joep:** whether to tell the website team about the live page's own
H1 (beslist.nl renders "Groene Woonaccessoires" for `/wandpanelen/` too — a
separate gap in the site's title builder that this tool does not control).

### Other loose ends
- **Prod (`win-htz-006:3003`) is a long way behind.** Everything from 28 July is
  local-only: the whole suggestions.txt sweep plus three BACKEND changes that need
  a deploy to take effect there — `seo_prio_service` (category filter + the
  `/api/seo-prio/categories` endpoint), `rfinder_service.fetch_r_urls_by_row`, and
  the `database.py` pool-init lock. Without that last one, prod keeps the
  concurrent-cold-start race described in LEARNINGS.
- `cc1/GSD_LL_MYSTERY_RUN.md` was finally committed on its own (`b226780`) after
  being stash-cycled through six pushes. The parallel session's tag_toppers cc1
  notes got the same treatment (`a60fefa`) rather than being left to repeat it —
  **if you find cc1 files dirty from another session, commit them separately
  instead of sweeping them into your own commit.**
- **Local `:8003` HAD `--reload` on 28 July**, so backend edits went live with no
  restart (verified by hitting a brand-new endpoint right after saving). It did
  **not** have it on 3 June. Run `ps -eo pid,args | grep 'uvicorn backend.main'`
  before assuming either way — the start scripts are not proof.
  Startup blocks ~35s on `load_excel_data` retrying against a prod-only path.
  The Task Scheduler launcher exited 1 on 28 Jul 09:08 — unexplained.
- `backend/data/seo_prio_categories.json` is now gitignored (`10ceb4f`) — it is a
  regenerable 24h cache, unlike `cat_urls.csv` which is also the offline fallback.
- **Never committed, still only in this session's scratchpad:** nothing. All the
  session's scripts landed in `scripts/`. (Contrast with the tag_toppers sync,
  which is still scratchpad-only — see BACKLOG.)

- [x] **category_lookup is API-first, cat_urls.csv is now a cache of the API** (2026-07-28, backend). Root cause of Joep's "/wandpanelen/ titles say Woonaccessoires": the CSV only holds the old `slug_catid` url form, newer categories use a bare slug (`wandpanelen`, id 9005645), so `lookup_category()` missed and `faq_service` fell back to indexing the API product's `categories[]` by url depth — landing on an ancestor. 352 urls / 37 categories affected. Nothing in the repo had ever generated the CSV. Now: in-memory slug map from Taxonomy API v2 (1h TTL, `urlSlug` read from `labels[]` per locale), every successful walk rewrites the CSV atomically, CSV read only when the API is unreachable. **The walk must never run inline** — the first version took 167.8s on a cold call; it now runs on a daemon thread with an in-flight guard while callers answer from the CSV (0.01s). 3,543 categories; callers unchanged. Corrected in passing: Unique Titles scrapes nothing, it is API-based throughout. **Not yet verified: a background walk completing in a long-lived process** (test process exited first). **Not yet done: regenerating the 352 affected urls.** See LEARNINGS "cat_urls.csv kon nieuwe categorie-slugs NOOIT vinden". #claude-session:2026-07-28 #priority:high

- [x] **UI batch from suggestions.txt — 25 of 41 items** (2026-07-28, commits `569288a` + `10f4152`). Canonical button classes throughout (which also brings the grey-outline disabled state), page-width audit with three tools fixed, Healthscore unlinked from 31 navbars, and the Keyword Redirects run state + Thema Ads button placement. Two colours needed forcing because the theme repaints Bootstrap's danger variants orange. Remaining: 15b, 18b, 20b, 11, 19, 25 (mechanical/tables) and 1, 2, 6, 7, 9, 33, 39, 40, 47, 48 (features). #claude-session:2026-07-28 #priority:medium

- [x] **GSD Campaigns — the Activity Log entry is written by the backend, and the 5 undo-less rows are repaired** (2026-07-28). `log_run_activity()` runs inside `start_ll_run` / `start_ll_apply` **before** progress flips to `done` (the frontend reloads the log on `done=True`, so the row must already exist), which makes the log independent of the browser — the gap that lost the 21 Jul zombie batch and the 28 Jul 08:28 run. All six client-side `logActivity('LL …')` calls removed in favour of `loadActivityLog()`; `llUndoFrom()` deleted as dead code since the backend now builds the payload. Previews aren't logged; a kill-switched attempt is, and says so. Two test findings: `datetime.now()` must be **aware** (`AMSTERDAM_TZ`) or a naive local value lands 2h ahead in a TIMESTAMPTZ under an `Etc/UTC` session, and the `entry_id` must key on the **run** (`started_at`), not the logging second — five writes in one second collapsed to two rows before that fix. Verified with 24 checks against the real DB (test rows deleted). **Backfill applied on prod**: dry run 5 matched / 0 skipped, then `dry_run=false` → 5 rows updated; **0 of 43 entries now lack a Reset button**, and the repaired payloads carry `ll: true`. See LEARNINGS "Activity-Log-write naar de backend". #claude-session:2026-07-28 #priority:medium

- [x] **GSD Campaigns — centre the per-shop detail table in the expanded LL preview row** (2026-07-28, frontend-only). `.ll-detail-table` gets `margin-left/right: auto`. Auto margins on the *table* rather than flex-centring the `overflow-x:auto` wrapper: when the table is wider than the row the auto margins collapse to 0 so it still starts flush left and scrolls, whereas a centred flex line clips the overflowing start. Verified headless in both states. #claude-session:2026-07-28 #priority:low

- [x] **GSD Campaigns — LL runs store an undo payload, and the reset is label-aware** (2026-07-28, backend + frontend). Both LL success call sites now pass `llUndoFrom(data)`, so new runs get a working Reset. The reversal does NOT go through the status-only `/undo` path: LL also manages the `GSD_LL_PAUSED` label, and a status flip would leave a re-paused campaign untagged — invisible to every future enable run, since that lookup finds candidates by the label. New `undo_ll_run()` replays the run through `apply_selected()` with each action flipped (labels both directions, kill switch honoured, audited, guarded against racing a live run) and returns the existing `{paused_created, enabled_paused, errors}` shape. **Routing lives in `POST /undo`, not the browser** — one code path in the UI, and an older deployed frontend gets the correct behaviour for free. New `backfill_ll_undo()` + `POST /ll/undo-backfill` (dry-run by default) repairs the 5 live-logged rows: groups audit rows into 60s islands, matches the run ending ≤30 min before each entry, and **only writes when the rebuilt counts equal the entry's own details text** — the first attribution rule (bounded by the previous entry) rebuilt 22 Jul as 80/175 vs a logged 50/71 and the count check caught it. Dry run matches all 5 exactly. **NOT written yet, on purpose:** prod's backend lacks `undo_ll_run`, so filling the payloads now would show 5 Reset buttons that still do the status-only flip. Run `POST /ll/undo-backfill?dry_run=false` right after the backend deploy. Verified with 21 checks on `undo_ll_run` + 15 on the frontend builder (incl. a double-flip round trip), zero Google Ads calls (apply_selected monkeypatched, then re-run for real with the kill switch on). See LEARNINGS "LL-undo gebouwd". #claude-session:2026-07-28 #priority:medium

- [x] **GSD Campaigns — Activity Log paginated (10/page default)** (2026-07-28, frontend-only). The log rendered all 43 rows at once. Now paginated with the same `.pagination-controls` idiom the other tables on the page use (Per page 10/25/50/100/Show all, orange chevrons, `X-Y of Z`), default 10, newest first. `renderLog()` slices `log` and drives the controls; `logActivity()` resets to page 1 so a run that just finished is visible where it lands; the page clamps on over-run and on a per-page shrink, so you can never end up staring at a blank table; preview/dry-run entries stay excluded from the count. Verified with 21 stubbed-DOM checks (empty state, 43 rows across pages 1/2/last, clamping, Show all, per-page shrink, preview exclusion) plus a headless render of the paginated card. Also answered Joep's question in the same session: the missing Reset buttons on the 22–28 Jul LL runs are unrelated to pagination — see LEARNINGS "Alleen BACKFILLED activity-entries hebben een Reset-knop" and the BACKLOG item for the fix. #claude-session:2026-07-28 #priority:low

- [x] **GSD Campaigns — LL Date picker works again: daily Excel stored per day for a week** (2026-07-28, backend + frontend). Joep noticed the Date picker in "Pause / Enable low linkage shops" stopped doing anything once the 09:50 Excel load became the only data source. It was dead input: `run_low_linkage`'s docstring said `date_str` is "Ignored when source='excel'", and `gsd-campaigns.html:962` hardcodes `source='excel'` since the data-source toggle was removed — so the frontend posted `date` and the backend discarded it. `pa.jvs_gsd_ll_excel_load` only ever held metadata; the rows lived in the volatile in-memory `_EXCEL_DATA` cache, one day at a time. New `pa.jvs_gsd_ll_excel_snapshots` (PK `(data_date, shop_id)`, 7-day rolling retention, auto-created) written best-effort from `load_excel_data()`; `run_low_linkage(source='excel', date_str=...)` replays that day's feed **and** flags. Design calls: `data_date` = the file's mtime date (not `now()`, so a restart re-reading the same file doesn't mint a second snapshot); prune relative to the date being written (so replaying an old file prunes less, never more); a missing date **fails loudly** listing what exists rather than silently using the newest file; a date with no load resolves to the most recent on-or-before it (matching `get_shop_flags()`) but surfaces a `snapshot_note` warning so the substitution is never silent. Frontend: picker constrained to the stored days (flatpickr `enable` + min/max), disabled when none, hint + counts tooltip, confirm dialog names the chosen date, result header shows the day it ran against. New `GET /ll/excel-dates` + `POST /ll/excel-dates/backfill`; `backfill_excel_snapshots()` also runs at startup to replay files still on disk. Verified with 39 checks against the real shared Postgres (round-trip, retention, upsert, on-or-before fallback, not-found error, all three date paths) — all return before `_get_client()`, so no Google Ads call and no mutation — plus 13 stubbed-DOM checks on the picker logic and a headless render. **Backend change → needs a manual uvicorn restart, and NOT during an LL run (in-process orchestrator). Prod is a separate box (win-htz-006:3003) — its own deploy.** Open: unknown whether `EXCEL_DIR` keeps old files, so the backfill may be a no-op (see BACKLOG). See LEARNINGS "GSD LL Date-picker deed niets". **Follow-up same day:** Joep asked for the "N days stored (…)" hint under the picker to go — the greyed-out days in the calendar already convey which dates exist, so the healthy state now renders nothing (`#llDateHint:empty { display:none }` keeps the gap from lingering). The empty-window and endpoint-down messages stay, since those states need explaining. #claude-session:2026-07-28 #priority:medium

- [x] **IndexNow — Submit moved right + renamed, Refresh matches Redirect Tool** (2026-07-28, frontend-only). Submit button right-aligned in a `d-flex justify-content-end` and relabelled `Submit`; class `btn-primary` → `btn-run` (visually identical — this theme already maps `.btn-primary` to burnt orange — but canonical per UI_BLUEPRINT and it brings the required grey-outline disabled state, which matters because `setLoading()` disables the button on every submit). Refresh dropped its inline styles + `onmouseover`/`onmouseout` handlers for the shared `btn btn-sm btn-outline-purple`, whose values equal Redirect Tool's local `.btn-purple-outline`. Also removed dead code found in passing: that card header's inline `background: #5e4a90` + `text-white` never rendered — `style.css` sets `.card-header { background-color: var(--color-section) !important; color:#333 !important }` and an `!important` stylesheet rule beats a non-important inline style (verified computed: `rgb(232,233,235)` / `rgb(51,51,51)`). Mattered for the decision: had the header really been purple, a transparent purple-outline button would have been invisible on it. Judgement call flagged to Joep: the Excel tab's `Upload & Submit` was right-aligned too (label untouched) so the CTA doesn't jump between tabs. Verified in headless Chrome side-by-side against Redirect Tool. #claude-session:2026-07-28 #priority:low

- [x] **SEO titles — finished runs end in a "Done" banner instead of a bar parked at 100%** (2026-07-28, frontend-only). Joep: after a publish run the progress bar stays in view at 100%. Nothing about a done-state existed in `UI_BLUEPRINT.md`, so the pattern was lifted from DMA Exclusions (`showOosDone()`), generalised into `showDoneBanner(id, html, tone)` / `hideDoneBanner(id)`, and applied to **both** places a run ends in `seo-titles.html`: Publish (bar down, banner with pushed/failed/skipped counts, raw response folded into `<details>` instead of a bare `<pre>` dump) and Retrieve URL data (label/percent row **and** bar hidden — the bar sat animated at 100% through the whole `generating_titles` phase since `pct` is `null` there; counters row untouched). Tones: yellow `done` / `alert-warning` / `alert-danger` / `alert-info` for a stopped run. Three things fixed while wiring it: the failure paths produced no end state at all (a failed publish just made the bar vanish), `/publish` rolls the pre-flight length skips into `d.failed` so the summary double-counted them, and a stopped generate run lands as `status="done"` with `should_stop` still set so the banner claimed success. `.alert-done-yellow` promoted from `dma-exclusions.html` into shared `css/style.css` (theme flattens `.alert-success`/`.alert-info` to grey, which is why it can't be a Bootstrap success alert); duplicate local rule removed. `UI_BLUEPRINT.md` gained a "Done banner" section (shared CSS, markup, tone table, content shape, the dismissal-vs-poll guard, and show-it-on-the-failure-path-too). Verified by lifting the real banner code out of the file and running it against the actual response shapes under a stubbed DOM in `node`; **not** eyeballed in the running app — `localhost:8003` is unreachable from WSL. Static files → browser refresh, no restart. See LEARNINGS "Een afgeronde run mag geen balk op 100% laten staan". #claude-session:2026-07-28 #priority:low

- [x] **DMA Exclusions — Timestamp column rendered in Amsterdam time instead of raw UTC** (2026-07-27, commit `ef5c53e` on main, pushed; frontend-only). Joep queried `07:43` on item `nl-nl-gold-8720526007155` for a ~09:45 run — the value was a correct **UTC** instant (09:43:21 CEST), just never converted. Root cause spans the whole chain: the shared Postgres at `10.1.32.9` runs `TimeZone=Etc/UTC`, `created_at`/`applied_at` are `TIMESTAMP` (no tz) `DEFAULT now()`, the backend's `.isoformat()` emits no offset (`dma_exclusions_service.py:1264`), and the cell did pure string work on that text (`replace("T"," ").slice(0,16)`). New `fmtTs()` helper appends an explicit `"Z"` **before** `new Date()` — the critical bit, since JS parses an offset-less date-time as LOCAL, making the naive fix a silent no-op in CEST — then formats via `toLocaleString("sv-SE", {timeZone:"Europe/Amsterdam"})` to keep the existing `YYYY-MM-DD HH:MM` shape with DST handled. Unparseable values fall back to the old slice. Cell keeps the raw UTC on a `title` tooltip; column header now names the timezone. Display-only: stored values unchanged, `savedSortVal` still sorts the raw ISO field. Verified 07:43Z→09:43 (July) / 08:43 (January), already-offset strings not double-shifted, 22:30Z rolls to next day, null/garbage hit fallbacks; inline JS passes `node --check` (no linter configured in this repo). Static file → browser refresh, no restart. See LEARNINGS "DMA Exclusions Timestamp showed UTC as if local". #claude-session:2026-07-27 #priority:low

- [x] **SEO Titles — publish progress bar + `h1_title` 200-cap fix + pre-flight lengtecheck** (2026-07-27, `backend/seo_titles_service.py`, `backend/main.py`, `frontend/seo-titles.html`). (1) **Progress bar** vervangt de banner "Publishing… this can take a while for large batches.". Zonder contractwijziging: `/publish` draait al via `run_in_executor`, dus de service schrijft voortgang in `_pub_state` (`_pub_reset`/`_pub_set`/`get_publish_status()`) en het frontend pollt de nieuwe `GET /api/seo-titles/publish-status` **terwijl zijn eigen POST nog loopt**. Leest `Pushing blueprints 5000/39206 (batch 1/8)`; opake fasen (AI-titles, dedup-refresh) krijgen alleen een label op hetzelfde percentage. Teller schuift pas op ná de commit van de status-flip; `mark_publish_error()` in de `except` van het endpoint voorkomt een eeuwig draaiende bar. (2) **Bug uit een echte push**: `400 Invalid record values — h1_title too long`; `build_blueprint` capte alleen `title`. `MAX_H1_LEN=200` toegevoegd met dezelfde trim-aanpak; `description` bewust ongemoeid (37.883 gepushte rijen zijn >200 en worden geaccepteerd). (3) **Pre-flight lengtecheck** in `publish_built` omdat de API atomisch valideert — één te lange rij weigerde een batch van 5.000 en zette alle 5.000 op `failed`; te lange records worden nu vóór het posten gequarantineerd met een expliciete `last_error` en gerapporteerd in `result['skipped_too_long']`. Herstel: de ene rij (205→196) herbouwd, 5.000 `failed`→`built`. Getest: 5 state-machine-cases, h1-cap op de eerder falende combo (166), AST-parse, `node --check`. Backend draaide niet op deze box (niets op 8003) — geen herstart gedaan, geen echte publish gesmoketest. Zie LEARNINGS "`/page-titles` capt ook `h1_title`…" + UI_BLUEPRINT. #claude-session:2026-07-27 #priority:high

- [x] **Multi-value (`+`) facet-urls uit de pa.urls-load verwijderd** (2026-07-27, alleen DB). 415 van de 21.004 geladen urls bevatten een `+` (multi-value facet, bv. `horloge_diameter~...+...+...`); pa.urls had er historisch 31 op 1M rijen, dus dit week sterk af — vergelijkbaar met de `winkel~` facets die wél bewust werden gefilterd. Ze falen ook echt: alle 415 stonden op `failed`/`api_failed` in `unique_titles_jobs` met 0 rijen in `unique_titles_content`. Verwijderd uit `pa.urls` + de 3 job-queues, backup in `pa.urls_bak_plus_20260727`. Batches nu 8.429 + 12.160. Overweging voor de loader: filter toevoegen naast `winkel~`/`deals=`. Zie LEARNINGS "Multi-value (`+`) facet-urls…". #claude-session:2026-07-27 #priority:medium

- [x] **SEO Titles — 2.806 blueprints gebouwd uit de visits>0 /c/-export + `source_url` backfill** (2026-07-27, geen repo-code gewijzigd; DB-writes op `pa.seo_titles_blueprints`). `q_newurls.sql` van sessie `6d3ab396` ongewijzigd herdraaid (visits>0 i.p.v. de ≥2/≥3 loadfilter) → 305.788 canonieke `/c/`-urls → via de eigen functies van de tool (`parse_url`/`canon_key`/`_resolve_cat`/`load_existing_combos`) 87.329 combo's, 79.730 al gedekt, **7.599 nieuw** (149 `winkel`-shopfacet uitgesloten → 7.450, 16.115 visits). Zeer longtail: 4.745 combo's met 1 visit; ≥2=2.806, ≥5=710, ≥10=156. Voor **visits≥2** blueprints gebouwd met `build_blueprint`+`_upsert_blueprint`: 2.806 rijen `status='built'`, 0 titels afgekapt (max 198 van 200), niets gepusht naar `/page-titles`. **Bug die ik zelf introduceerde en fixte:** `source_url` gevuld met de host-loze url uit de Redshift-query, waardoor het frontend (`seo-titles.html:456`, gate `/^https?:/i`) de facet-key als dode `<code>` rendert i.p.v. een link — gebackfild met een `https://www.beslist.nl`-prefix (2.806 rijen; 43.874 nu absoluut, 0 relatief). Deliverable `Downloads\claude\SEO_titles_nieuwe_kandidaten_2026-07-27.xlsx`. Zie LEARNINGS "SEO Titles — `source_url` MOET absoluut zijn…" + BACKLOG. #claude-session:2026-07-27 #priority:medium

- [x] **SEO-omzet YoY-achterstand ontleed (analyse, geen code)** (2026-07-27). Joep: omzet +7% WoW maar derde week op rij achter op vorig jaar, én lager dan mei/juni. Uitgesplitst naar week/categorie/deepest-cat/type-url/device/shop met een **364-dagen YoY-shift** (weekdag-uitlijning, zo–za). Uitkomst: **drie oorzaken, geen ervan SEO-specifiek of met SEO-werk te fixen** — (1) bol.com Plaza cliff 10 mrt 2026 (~20k→7k clicks/dag in één dag; raakt ELK kanaal: SEO −72%, SEA −78%, maar DMA paid −31%), (2) CPR-billing switch 8 jul (visits/dag −0,4%, OPB −14,7%), (3) wegvallen junihittegolf. "Pas" 3 weken achter is een **basiseffect**: de bol-drag bestond al sinds april (−5.482→−10.331/wk) maar werd gemaskeerd door portfoliogroei + twee hittepieken; in juli sprong de LJ-basis van bol+amz +64% terwijl bol+amz dit jaar juist +20% deden. Top-4 marketplaces −17.267 op een gat van −14.904 → **rest portfolio +6,1%**. Dimensies: 70% visits / 30% OPB; hub-pagina's harder dan longtail (main_cat −27,1% vs sub_sub_cat −15,7% visits); PLP −44,2%; desktop −37,8% vs mobiel −20,7%. Zomercats: irrelevant vs vorig jaar (+485) maar **97% van de daling vs week 21 juni** (Airconditionings alleen −10.980) — die twee vragen niet verwarren, daar ging ik eerst de fout in. Excel-deliverable in `Downloads\claude\SEO_deepestcat_wk19-25jul_vs_vorigjaar.xlsx` (3.514 cats, 6 tabs). Zie LEARNINGS "SEO-omzet loopt vanaf juli 2026 achter…" + BACKLOG "bol.com Plaza cliff". #claude-session:2026-07-27 #priority:high

- [x] **GSAAS audit — setup / traffic / "feed" URLs (investigation, no code change)** (2026-07-27). Answered three questions: the setup is clean (`aff_id 734+750`, `channel_id 1`), the traffic decline is **Google-side not ours** (GSAAS −11.8% 14d/14d tracking Google Shopping −19.1% while SEO held −3.8%; YoY Jul NL −15% / BE −29%), and the "feed URLs" question resolved to **GSAAS having no Beslist-owned feed at all** — ~92% of the channel is the `utm_content=css-provider-link` attribution badge configured per CSS account (**140784594**) in CSS Centre, with `$0` = Google's raw query generating the `/products/r/<query>/` landings. Chased a `beslist.nl/&utm_source=gsaas` URL (missing `?`) that looked like a live bug and **is not one** — it's a `bt.search_console` ETL artifact that strips `?aff_id=<digits>` and orphans the following `&`; `dim_visit` does the same strip but re-forms the `?`, so live clicks are fine. Two claims of mine were retracted mid-investigation (see LEARNINGS for both, they are method lessons worth keeping). Also confirmed **all-domain reporting in `seo_stats_service.py` + `performance_standup_service.py` is deliberate** — Joep chose to leave it since the standup Excel's ~2,800 rows of history + `YOY` sheet are all-domain (verified day-for-day). See LEARNINGS "GSAAS channel — no Beslist-owned feed…" + "`bt.search_console` has synthetic placeholder keywords…" + BACKLOG entries. #claude-session:2026-07-27 #priority:medium

- [x] **SEO Titles — Built-titles panel overhaul + inline edit + Existing-combo enrichment** (2026-07-21, commits `236a0d5`, `a9f2a8d`, `6fdd6d1` on main, deployed :8003). Reworked the "Built titles" panel to match GSD "Campaigns created": filter+actions row above the table, sortable header arrows, skeleton loading rows, content-width columns + horizontal scroll (values one line, no truncation), dropped cat_id, renamed cols (Deepest cat / H1 title / Facets), facet combo as a hyperlink to its example URL (`?`-params stripped). Added **Copy** (selected rows → TSV incl. hidden cols + meta desc), clickable **stat tiles** (Existing/Built/Pushed/Failed) driving the view, an **edit modal** (hover pencil, dimmed backdrop, Save/Cancel only) backed by `POST /api/seo-titles/update` + `/create-built` (editing an Existing combo creates a `status='built'` blueprint), Publish disabled in the Existing view, filter "N results" count, "Retrieve URL data" moved to the page bottom. **Existing view** normalises the two `pa.page_titles_existing` layouts and fills Deepest cat (`cat_name` backfilled from taxonomy by cat_id, 98.5%) + meta description (`browse_description` backfilled from `/html-title-descriptions`, 98.3%) — new scripts `backfill_page_titles_existing_{catname,description}.py`; loader DDL declares both cols + reminds to re-backfill after a reload. **Generator**: `geschikte_leeftijd` pinned after the category/type-facet noun (`facet_phrase` noun_order+0.5), regenerated 1,041 built rows. Also fixed the **FAQ JSON export 500** (raw control char → `json.loads(strict=False)`). See LEARNINGS "SEO Titles — `pa.page_titles_existing` …" + "FAQ export JSON 500'd …". #claude-session:2026-07-21 #priority:medium

- [x] **Auto-Redirects — UI: outlined status badges, drop 2.0 toggle, facet-cache age in tooltip** (2026-07-21, commit `545578c` on main). Recent-runs Status labels → outlined badges (`border border-<color> text-<color>` instead of solid `bg-<color>`); removed the "2.0" (useV2) checkbox and made `apiBase()` always use the v2 engine (`/api/rurl-v2`), dropped the dead toggle listener; removed the standalone "Facet cache: Nd old (stale)" line and moved the cache age + last-rebuilt time into the Refresh-facets button's hover tooltip. Static-file refresh, no restart. #claude-session:2026-07-21 #priority:low

- [x] **Auto-Redirects — Refresh-facets button + auto-refresh stale `facets.csv` before a run** (2026-07-21, commits `c535587` feature + `0706237` colour fix on main, deployed :8003). Traced a user-reported "cross-subcat" redirect (`meubilair_389370_389409/r/riviera_maison/` → `..._389369_10538533/c/merk~4874240`, reason `[maincat]`) to a **stale/incomplete `facets.csv`**: pass-1 `filter_by_subcategory` reads only that cached Search-API snapshot, which had **0 rows for subcat 389409** (whole subcat absent) and Riviera Maison only under `10538533` → maincat rescue routed elsewhere; the correct same-subcat `/c/merk~4874240` was live but invisible. Cache was never rebuilt on a schedule (build-once, `use_cache=True`); live copy was 84 days old. Added: **Refresh facets** button (left of Run, canonical `btn-outline-purple`, arrow icon) + `POST /api/rurl-v2/refresh-facets` & `GET /refresh-facets/status`, lock-guarded background rebuild (`DataLoader(use_cache=False)` subprocess), drops the shared preload pickle on success; **`start_optimize` auto-refreshes when `facets.csv` > 7d old** (`FACETS_MAX_AGE_DAYS`), logging into the run, failing soft to the stale cache. Rebuild ≈ 5–8 min (~3,540 subcat Search-API calls @ 12 workers). Did NOT trigger an actual rebuild. See LEARNINGS "Auto-Redirects — a 'cross-subcat' redirect traced to a stale `facets.csv`…" + BACKLOG "live subcat probe before `[maincat]` rescue". #claude-session:2026-07-21 #priority:medium

- [x] **GSD LL — "last successful data load" persisted (stop resetting on restart)** (2026-07-21, commit `248877d` on main, deployed). Tooltip reset every restart because `load_excel_data` stamped `datetime.now()` into the in-memory cache only (startup pre-load → restart time). Now: `loaded_at` = Excel file mtime (stable data date) + persisted to singleton table `pa.jvs_gsd_ll_excel_load` (upsert), with `get_excel_data_status()` DB-fallback and a frontend gate change (show `loaded_at` regardless of `has_data`). Best-effort persistence. Null on this dev box (Excel path unreachable); populates on the box that reads the file. A prior 2026-07-17 mtime fix (`90796f9`) had silently regressed to `now()`. See LEARNINGS "GSD LL 'last successful data load' reset …". #claude-session:2026-07-21 #priority:medium

- [x] **Consolidate the two local checkouts → single `dm-dashboard` folder** (2026-07-21, DONE; commits `cc82397`/`caebbed`/`2b55df7` on main). Renamed `projects/dm-tools`→`projects/dm-dashboard` (deleted the stale duplicate after rescuing its uncommitted cc1 work as `cc82397`), relocated the venv in place via sed (NOT recreate — requirements.txt pins only 22/79 pkgs), reconciled `.env` (live folder = source of truth; fixed abs SA path, added `GSD_SHEETS_SERVICE_ACCOUNT_FILE`, `DASHBOARD_PASSWORD` empty). **Boot launcher = Task Scheduler `\DM Tools Dashboard` (at logon) → `~/scripts/start-dm-dashboard.ps1`** — fixed its dm-tools path + dropped `--reload`; deleted the redundant Startup `.vbs` (was double-launching); repo `start-dm-tools.bat` is dead (no task). Fixed ~30 hardcoded `/projects/dm-tools` paths in scripts/docs (`caebbed`) + a stale ARCHITECTURE remote-alias note (`2b55df7`). Live :8003 verified from the new path (301 routes). Drift risk persists via the win-htz-006 box's own checkout. See LEARNINGS "Local folder consolidation …". #claude-session:2026-07-21 #priority:medium

- [x] **GSD Campaigns — re-fix Enabled/Paused history column widths + remove Taakplanner frontend** (2026-07-21, commit `d477112` on main; dm-tools checkout). (1) The 07-17 (`008cc5e`) "fixed column widths" on the Enabled/Paused history table had **regressed** — committed CSS was back to `width:max-content` + nowrap (natural columns that reflow on sort/paginate). Re-applied properly: `.ll-history-table { table-layout:fixed; width:100% }` + `overflow:hidden;text-overflow:ellipsis` + `nth-child` px widths (Date 150 / Shop 220 / Country 90 / Action 110; Campaign = remainder). (2) Removed the **Taakplanner** (Task Scheduler) tool from the frontend — it 500s in WSL dev (`schtasks.exe` is Windows-only). Deleted `frontend/task-scheduler.html` + `js/task-scheduler.js`, removed the dashboard "Automation" card + `task_scheduler_enabled` reveal script. **Backend router/service + `pa.scheduled_tasks`/`pa.scheduled_task_runs` left intact; the 2 live Windows tasks (daily-automation @07:00, GSD-LL-Excel-Load @09:50) keep running on l.davidowski's box** — UI removal does not stop them. Deploy = static-file browser refresh (dashboard.html/gsd-campaigns.html; no uvicorn restart). See LEARNINGS "GSD Enabled/Paused history fixed-width REGRESSED …". #claude-session:2026-07-21 #priority:low

- [x] **Healthscore 2.0 — all-channel seasonal caps + 1-month look-ahead + COMMIT (Phases 1–6 + 3.5)** (2026-07-21, commit `e423557` on main, pushed to dm-dashboard; live :8003 already served the code). Two cap-model changes: (1) seasonal caps sized on **all-channel** visits (was SEO-only) via `_ALL_JOIN`/`_ALL_WHERE` in `_refresh_cat_month` + `_refresh_cat_knee` — coverage KPI + URL score stay SEO-only; universe grew 3,574→3,608 cats. (2) **forward-max one-month look-ahead** `mult(m)=max(idx[m],idx[m+1])` (Dec→Jan wrap) so caps ramp before a peak without dropping during it. Rebuilt `pa.hs2_cat_{month,knee,cap}` (43,296 rows). Built the **HS1.0-vs-HS2.0 comparison Excel** (`Downloads/claude/HS2.0_vs_HS1.0_vergelijking.xlsx`, 5 sheets; builder `scripts/analysis/healthscore_catdiff_excel.py`) + per-category URL-type breakdown (R-url 2.6%→22.6%, PLP 53.9%→28.9%). **June holdout can't exercise the look-ahead** (cats already at/past peak → +0.0; measure on a run-up month like May). See LEARNINGS "Healthscore 2.0 — seasonal caps go all-channel …". #claude-session:2026-07-21 #priority:high

- [x] **Healthscore 2.0 — implement for the 10 test categories** (DONE 2026-08-04 — all 10 live, see the
      Current Sprint entry for the before/after table and drop costs; the leaf question for Stoelen and
      Shirts was answered with `preserve_cross_category=True` rather than by excluding them). Roll HS2.0 selection live for the validation set: cats 9000047 Stoelen, 9000066 Eetkamerstoelen, 9000608 Sneakers, 9000953 Voer, 9002072 Douchewanden, 9005282 Mobiele telefoons, 9005317 Airconditionings, 9001646 Dekbedovertrekken, 9003581 Grasmaaiers, 9000668 Shirts. Decide the cutover mechanism (write HS2.0 selection into the live HTML-sitemap path for just these cats), then measure. Open Q from this session: whether to keep all-channel knee (+13% footprint) or switch base-cap to SEO-only + all-channel only for the *seasonality* signal; validate the look-ahead on a run-up month first. #priority:high

- [x] **Unique-title mojibake — fix + root-cause + deploy** (2026-07-21, commit `bc68056` on main, deployed). User saw garbled H1s in `pa.unique_titles_content` (`ImprimÃ©tops`, `plissã©gordijnen`). (a) **DB repaired**: 1,100 mojibake rows (`Ã©`/`ã©` map, both cases) + 5,312 doubled-word rows; backups `pa.unique_titles_content_bak_mojibake_20260721` / `_bak_dupword_20260721`. Dedup used a **brand+numeric exclusion list** (Joseph Joseph, Miu Miu, Samsøe Samsøe, `Watch 5 5 ATM`… must NOT collapse). (b) **Root cause**: `backend/data/cat_urls.csv` **content itself was mojibaked** (27/3558 rows: `PlissÃ©gordijnen`, `FÃ¶hns`…); the read path (`utf-8-sig`) was correct so it faithfully propagated into every generated title via `fetch_products_api`. Bug class = `requests` ISO-8859-1 fallback on `text/html` with no `charset` header (Beslist omits it for our scraper UA). (c) **Fixes**: new `backend/text_encoding.py::fix_mojibake` (no-op on clean text) wired into `category_lookup._load` + `fetch_products_api`; repaired the 27 CSV rows (backup `cat_urls.csv.bak_mojibake_20260721`); `response.encoding="utf-8"` added to `scraper_service.py` (live kopteksten path) + dead `scrape_page_h1`. (d) **Deployed**: bare uvicorn kill+relaunch (pid 57818), `/api/version` = `bc68056`. `scrape_page_h1` is DEAD (0 callers). No DB regen needed (batch worker skips non-empty). See LEARNINGS "Unique-title mojibake …". #claude-session:2026-07-21 #priority:high

- [x] **GSD Campaigns — deploy + commit MC websiteUrl fix; pin the service-account key** (2026-07-20, DONE). (a) **Deploy DONE**: killed the live uvicorn (PID 305, old code, no `/api/version`) and relaunched from the dm-tools checkout (`venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8003`, detached → `uvicorn.log`); `/api/version` now returns `e3f42c3`. App imports clean incl. the other session's Healthscore WIP (299 routes). (b) **Commit DONE**: fix is `e3f42c3` on main; backfill was ops-only (no code). (c) **SA key pinned DONE**: appended `GSD_SERVICE_ACCOUNT_FILE=<abs path to acoustic-racer-258913-e55feb91bacc.json>` to `dm-tools/.env` (line 57) — only that key has parent-MCA access; auto-detect of first `*.json` was non-deterministic. Verified: load_dotenv reads it + a live read-only MC list call succeeds. **Backfill of the 11 pre-existing empty accounts also DONE** (scratchpad `mc_url_backfill.py`). NOTE: live backend serves from **dm-tools**, not dm-dashboard (contra the 2026-07-17 note); dm-dashboard checkout has NO `service_accounts/` dir, so if serving ever moves there the pin must point at an absolute path that exists. See LEARNINGS "MC sub-accounts created without a store URL …". #claude-session:2026-07-20 #priority:medium

- [ ] **GSD LL — confirm the 09:50 /ll/run trigger source** (created 2026-07-20; check on/after **2026-07-21 ~09:55 CEST**, i.e. after one 09:50 cycle). Background: real LL pause/enable runs land daily at **07:50:01–07:53 UTC (09:50 CEST)** since 2026-07-17, but the dm-tools code path can't cause them (Windows task = `curl .../ll/excel-load` only; `load_excel_data` only caches). Proven it's still the dm-tools backend doing the work (`pa.jvs_gsd_ll_shop_cycles.updated_at` bumps at run-end), so **something POSTs `/ll/run?source=excel`** ~1s after the excel-load. **Unknown = the caller's location.** Commit `6331838` now logs `ip/dry_run/source/user-agent` on `/ll/run`. **Steps:** (1) **prereq** — verify the **deployed** backend runs code with the `/ll/run` IP/UA logging (commit ≥ `6331838`, ideally `01ad22b`): hit `GET /api/version` on the deployed host (endpoint added in `a4175b4`; returns the commit that was HEAD at server start) — if it predates `6331838`, arrange a restart first or there'll be no log line; (2) `grep "GSD LL /ll/run called" <uvicorn-access/app log>` around 07:50 UTC → read `ip=` + `user-agent=`. **Interpret:** `ip=127.0.0.1` → local task/script on the backend host (l.davidowski's machine); other IP → **remote** trigger (n8n/other host, backend is LAN-exposed). (3) Cross-check new rows in `pa.jvs_gsd_ll_campaigns` + `shop_cycles` bump at 07:50–07:53 UTC. If it turns out to be n8n, also search the n8n instance for a 09:50 HTTP-request workflow hitting `/ll/run`. See LEARNINGS "LL pause/enable actually RUNS daily ~09:50 …". #claude-session:2026-07-20 #priority:high

- [x] **GSD Campaigns — UI polish batch + deploy (pull+restart)** (2026-07-17, commits `008cc5e`, `f9e4bb4`, `de47503`; dm-dashboard checkout). Nav "Dashboard" button → Material apps grid icon on all 32 pages; LL foldout table `width:auto`; Enabled/Paused history `table-layout:fixed` + fixed column widths (no reflow on sort) + ellipsis/title; "Paused" badges → outlined orange `#CC5500` (history + LL run/detail); centered Shop ID + Action in LL run table; dry-run badge shortened to "dry run"; foldout detail header renamed Action→Status. **Deployed**: killed the stale dm-tools-served backend and restarted from the dm-dashboard checkout on :8003 (dm-tools venv) — now serves current code (requires dashboard login). See LEARNINGS "deploy/restart from dm-dashboard checkout …". Also renamed the Enabled/Paused history "Copy for Excel" button to just "Copy" (commit `3d953bf`; user chose no count — that table copies all filtered rows and has no row selection). #claude-session:2026-07-17 #priority:low

- [x] **GSD Campaigns — LL last-load from file mtime + accordion detail rows + orange dry-run badge** (2026-07-17, commits `90796f9` backend, `00a7935` frontend; dm-dashboard checkout). (1) `fix`: "last successful data load" tooltip showed the server-restart time (persisted JSON in maybe-missing `backend/data/` → fell back to `now()`); now derived from the Excel file's mtime (~09:50), stable across restarts. (2) `feat`: LL run-table detail rows are now accordion (clicking another row auto-collapses the open one). (3) `style`: dry-run badge → canonical orange `#CC5500` + white text. See LEARNINGS "LL last-load timestamp from file mtime …". #claude-session:2026-07-17 #priority:low

- [x] **GSD Campaigns — backfill 10 missed labels + fix LL run-table Skipped count & detail headers** (2026-07-17, commits `d19aba1`, `46c0020` on main; done in dm-dashboard checkout — dm-tools blocked by another session's uncommitted Healthscore work). (1) Original labeling missed 10 campaigns because the global `change_event` query hit `LIMIT 10000` in the busy NL account and mis-dated 13-juli pauses as pre-cutoff; re-scanned reliably with **per-campaign** change_event queries, found + labeled the 10 (5 Geheugenhulp NL, 4 Houtenspeelgoed NL, 1 Valhallaboardgames NL), verified. (2) `fix`: run table showed "1 campagne" for Skipped groups (nested `campaigns` array undercounted) → `groupCampaignCount()` sums nested Skipped campaigns, used for count + sort. (3) `style`: detail sub-table headers now grey `#f8f9fa` (match Enabled/Paused history). See LEARNINGS "change_event truncation missed labels …". #claude-session:2026-07-17 #priority:medium

- [x] **GSD Campaigns — ad-hoc: bulk `GSD_LL_PAUSED` labeling of externally-paused campaigns** (2026-07-17, ops task, no code committed; scripts in session scratchpad `ll_label_*.py`). From `paused_ll_shops.xlsx` (33 shops, `Shopnaam`+`Country`, Country ∈ {NL, BE, DE, "NL + BE"}), labeled currently-PAUSED campaigns whose name contains `[shop:{shopnaam}]` in the listed countries where the pause was on/after 2026-07-10. Result: **101 campaigns labeled, 0 errors, verified** (70 standard `a/b/c/no_data/no_ean` + 31 non-standard/test `[label_test]`/`tag_toppers`/`merk`). Key facts: the LL tool's own pauses were **all already labeled** (log `pa.jvs_gsd_ll_campaigns`), so the 101 were paused **outside** the tool and their dates came from Google Ads `change_event`; matched via `_name_contains_regexp("[shop:X]")`, applied via `_ensure_label`+`CampaignLabelService`. See LEARNINGS "ad-hoc bulk GSD_LL_PAUSED labeling …". **Caveat surfaced + accepted:** `GSD_LL_PAUSED` makes the LL Enable pass re-enable these later (incl. the test campaigns). #claude-session:2026-07-17 #priority:medium

- [x] **GSD Campaigns — LL card simplified to Excel-only + last-load "i" tooltip; unlogged campaign actions** (2026-07-16, commit `14923e5` on main). Frontend-only `frontend/gsd-campaigns.html`. Removed the "Pixel Monitor Feed / Excel File" data-source toggle (Preview/Run hardcode `source='excel'`) and the "Daily data load (09:50 CET)" schedule toggle + "Load now" button + cached-data status line — **backend scheduler & `/ll/excel-*` endpoints left intact**. Added a purple "i" info button by the card header showing `Last successful data load: DD-MM-YYYY, HH:MM` on hover (from `/ll/excel-data` `loaded_at`, fallback `/ll/excel-schedule` `last_run_at`). Stopped logging individual/bulk Pause/Enable/Remove campaign actions in the Activity Log (failures now `alert()`); only Run Script / LL Run / Reset remain logged. Also answered: Reset button only renders for GSD create-runs, so manual pause/activate rows correctly had none. **Deploy:** static file, browser refresh (no uvicorn restart). **Open:** the optional Date field is now dead UI for the Excel source — remove if undesired. #claude-session:2026-07-16 #priority:low

- [x] **GSD Campaigns — LL per-shop pause/enable cycle tracking + repair REMOVED-tree fix** (2026-07-16, commit `6f9b0fa` on main). `gsd_ll_service.py`, `gsd_campaigns_router.py`, `gsd_campaigns_service.py`, `gsd-campaigns.html`. (1) New counter table **`pa.jvs_gsd_ll_shop_cycles`** (n8n-vector-db, PK `(shop_id, country)`, `pause_count/enable_count/last_*_at/currently_paused`) — bumped **once per run per shop+country per action** (not per campaign) via `cycle_events` set + `_bump_shop_cycles` (ON CONFLICT +1) in both `run_low_linkage` and `start_ll_apply` (best-effort). `backfill_shop_cycles(gap_minutes=30)` seeds history from `pa.jvs_gsd_ll_campaigns` with a **gap-and-islands** query (no run_id in the log; minute-bucketing would over-count) — ran live: 277 campaign rows → 45 (shop,country) rows / 45 pause events / 0 enables. `get_shop_cycles` reader + `GET /api/gsd-campaigns/ll/shop-cycles`. (2) Frontend: orange-outlined **"Export history"** button on Enabled/Paused history → per-shop `Times paused / Times re-enabled` Excel (aggregated across countries); removed the redundant "Export Excel" there; **Campaigns-created "Copy for Excel" now copies only checked rows**. (3) `_repair_campaign` listing-tree `has_lg` check now filters `status != 'REMOVED'` (was the lone query missing it) — verified via a live remove-ad-group test that repair correctly recreates. See LEARNINGS "GSD Campaigns — LL per-shop pause/enable cycle tracking …". **Deploy:** kill+relaunch bare uvicorn (needed for the new endpoint + counting). **Open:** no UI table for the cycle counts yet (data via endpoint only); Export-Excel on Campaigns-created still exports all filtered (not selection-aware). #claude-session:2026-07-16 #priority:medium

- [x] **GSD Campaigns — MC-id Redshift logging + eventual-consistency retry root-cause + post-run verify** (2026-07-16, commit `56e65fa` on main). `backend/gsd_campaigns_service.py`. (1) **MC-id logging** to `pa.mc_ids_efficy` `(shop_name, shop_id, mc_created, domain=COUNTRY, date=YYYYMMDD)` mirroring the standalone `create GSD-campaigns.py::push_to_redshift`: new `push_mc_ids_to_redshift()` (best-effort, `execute_values`, uses `_get_redshift_connection` env creds); `_get_or_create_mc_account` now returns `(mc_id, created)` so only **genuinely new** MC sub-accounts are logged; wired into `run_gsd_script` → `overall_results["mc_ids_pushed"]`. (2) **Backfill** `backfill_recent_mc_ids_to_redshift(days=2, dry_run=True)` — `campaign.start_date` is GONE in **API v24**, so creation date comes from `change_event` (needs FINITE date range + LIMIT + ~30d retention); deduped per `(shop_id, country, merchant_id)`; ran live → **12 MC ids pushed** (8 NL + 4 BE, 14/15 Jul). (3) **Root cause of Hoopo.eu no_data "no ad group":** ad group WAS created (19:19:34) but the product ad failed transiently (`RESOURCE_NOT_FOUND` while the fresh ad group propagated) → empty ad group, fixed only by a 2nd run's `_repair_campaign` 37 min later. Fix: `_create_child_with_retry` (patient 5/10/20/30/60s + short transient backoff, atomic→no dupes) wired into `add_shopping_product_ad_group_ad`, `add_sub_cpr`, `add_sub_cpc`. (4) **Post-run verify** `verify_run_campaigns()` — checks ad group/product ad/listing tree(+targeting) over everything created/repaired, re-checks flagged ones after a delay to filter propagation lag; runs at end of `run_gsd_script` (`verify=True`) → `overall_results["verification"]`. See LEARNINGS "GSD Campaigns — MC-id Redshift logging …". **Open:** frontend doesn't display `verification` yet (`gsd-campaigns.html` unchanged) — optionally add a post-run "check" panel; backfill is append-only (re-running double-inserts). Deploy = kill+relaunch bare uvicorn. #claude-session:2026-07-16 #priority:medium

- [x] **GSD Campaigns — original-alignment + branded labels + resilience + sheet logging + UI overhaul** (2026-07-15, commits `bd39c6a`→`44f75f8`). Backend: aligned to original create GSD-campaigns.py (€10 budget, ad-group cpc €0.10, enable_local=True, negatives EXACT+PHRASE only for branded==0); BRANDED_0/BRANDED_1 labels + `backfill_branded_labels()` (2742 existing labelled); retry helpers for CONCURRENT_MODIFICATION, campaign-create RESOURCE_NOT_FOUND (MC link propagation), transient MC lookup (timeout/500/503); Google-Sheet run logging to `campaigns_created`. Ran a live original-vs-tool output comparison (paused sample, dumped, removed). UI: col-md-10 width, run-table styling for all tables + light headers, monotonic progress bar, Activity Log GSD/LL labels (previews hidden), checkbox bulk Pause/Activate/Remove (replaced per-row buttons), columns Shop|Country|Campaign|Status, table-layout:fixed. See LEARNINGS "GSD Campaigns — original-alignment, branded labels, resilience …". Bulk activate/pause now skips no-op statuses (commit `1fbf3e4`). **Open/optional:** widen `_parse_campaign_name` to handle `[merk:…]` campaigns; find/repoint the separate BROAD-negative generator if it still runs. #claude-session:2026-07-15 #priority:medium

- [x] **Healthscore 2.0 — HTML-sitemap selection redesign, Phases 1–5** (2026-07-15). New standalone module `dm-tools/backend/healthscore_service.py` (CLI `coverage|features|keywords|sitemap`), reads Redshift, **writes to n8n Postgres `pa.*`** (per user). Run with `dm-tools/venv/bin/python`. **P1** coverage KPI (`pa.healthscore_coverage`) → diagnosed the ~35% is entirely R-urls (R-url ~5% covered & ~51% of SEO visits; non-R ≈86%; old set held only 16k of ~5.5M R-urls). **P2** per-URL features (`pa.hs2_features`, 1.06M urls × visits/CTR/bounce/revenue/momentum, 90d+14d) + Keyword Planner cache (`pa.hs_keyword_search_volume`, 327k terms; API limits confirmed non-issue). **P3** backtest (no-leakage as-of/holdout, 2 splits) → **model locked `0.889·log(visits)+0.111·log(revenue)` per-cat percentile top-N; CTR/bounce/momentum/search-volume all earned 0** (keep as guardrails only). **P4** selector+writer (`pa.hs2_sitemap`, N=1000 + guaranteed new-URL bucket from user's `bt.facet_facetvalues` 20d query; 907k urls). **P5 shadow (validated):** out-of-sample vs live set → **45.1%→71.0% visit / 50.9%→75.2% revenue at a smaller footprint (+25.9pp/+24.2pp)**; adds 569k uncovered June visits, drops 140k churn. Plan: `dm-tools/docs/HEALTHSCORE_2.0_PLAN.md`; analysis scripts `scripts/analysis/healthscore_*.py`. See LEARNINGS "Healthscore 2.0 …". **NOT committed/pushed yet.** #claude-session:2026-07-15 #priority:high
- [x] **Healthscore 2.0 — Phase 6 (frontend) + productionize** (2026-07-21, DONE in commit `e423557`). (a) FastAPI router `healthscore_router.py` + `frontend/healthscore.html` page (run triggers, coverage dashboard, projected-win compare, coverage-by-type, sitemap composition) — built + wired into nav on all 32 pages + dashboard card. (b) **Still open → folded into the "implement for the 10 test categories" task**: the twice-weekly run schedule (`features → sitemap`) and pointing the live sitemap renderer at `pa.hs2_sitemap` are NOT yet wired (shadow-only so far). (c) Open tunings unchanged (N=1000 vs trimming; new-URL bucket normalizing). #priority:high

- [x] **GSD negatives — hardened `get_negatives` + reconciled yesterday's campaigns** (2026-07-15). (1) Hardened `create GSD-campaigns.py::get_negatives` (standalone script, NOT in git): new `_clean_host` + any-TLD/two-level handling, core-brand extraction, keeps both negatives for 2-letter brands; returns `[full-domain, brand]`. Verified against all 55,502 `bt.shop_list` names. (2) Reconciled all **39 NL `GSD_SCRIPT` campaigns created 2026-07-14** to EXACT+PHRASE `[full-domain, brand]` (removed 45 BROAD incl. bare-tld `nl`/`com`/`eu`, added 84; filled 15 empty label-variant campaigns +60). End state 39/39 with exactly 4 negatives each, 0 BROAD, 0 bare-tld. Idempotent tool: `scratchpad/fix_gsd_negatives.py` (`--apply`). Creds: dma_script refresh token + Windows `GOOGLE_CLIENT_ID`/`SECRET` (inline token in script is expired). See LEARNINGS "GSD negatives — `get_negatives` fragility …". #claude-session:2026-07-15 #priority:high
- [x] **Find & fix the BROAD-negative generator for GSD campaigns** (2026-07-15, commit `d10a5bc`). Source = the live **dm-tools GSD Campaigns tool** `backend/gsd_campaigns_service.py` (FastAPI reimplementation; the real generator now — standalone `create GSD-campaigns.py` is legacy w/ expired token). Bug: `get_negatives` split shop_name on every non-alphanumeric run (`re.split(r"[^a-zA-Z0-9]+", …)`, `len(w)>1`) → emitted the bare tld (`Calcuso.com|NL`→`calcuso,com,nl`), and `add_negative_keywords` hardcoded BROAD. Fixed to `[full-domain, brand]` via `_clean_host` (any-TLD incl. two-level, no bare tld) + EXACT+PHRASE. No JS Ads Scripts / n8n involved. **Deploy note:** dm-tools backend is bare uvicorn (no --reload) → kill+relaunch to make the fix live for new runs. #claude-session:2026-07-15 #priority:high

- [x] **GSD Create-campaigns: Repaired run-result tile + real Merchant Center error surfacing** (2026-07-14 pm, on `main`; commits `3a8916a`,`5eaf5d9`). (1) **Repaired tile** between created/paused — repaired campaigns come back inside `data.created` with `reason` repaired/retreed (fresh creates have no reason), so they were hidden in the created count; now their own `repaired` action + blue `#0984e3` tile/label/filter/rank, reason shown in Detail. Preview (dry-run, never repairs) unchanged. (2) **Real MC error in the run table** — `failed_to_get_or_create_mc_account` was hiding an HTTP 403 `accessNotConfigured` ("Content API for Shopping … disabled for project 1007333749964"), which is a GCP-project config issue (NOT a sub-account limit) affecting all 3 MC parents (NL 5592708765 / BE 5588879919 / DE 5342886105). MC errors are `HttpError`s not `GoogleAdsException`s → added `_last_mc_error`+`_mc_err()` (reads `error_details` → "reason: message"), set in `create_merchant_id`/`_get_or_create_mc_account`, used as the `error` field → Detail column now shows the actual reason. **Action item for user:** enable Content API for Shopping in GCP project 1007333749964 to unblock BE MC-account creation. See LEARNINGS "Repaired is a distinct run outcome" + "MC errors are plain HttpErrors". #claude-session:2026-07-14 #priority:medium
- [x] **GSD Create-campaigns: responsive Cancel + cancelled banner + dead-code cleanup** (2026-07-14 pm, on `main`; commit `84e02f7`). Cancel was only checked between shops → felt dead mid-shop and the UI stayed on "Cancelling…". Now `_run_cancel` is checked per-country and per-label (`break` before the next campaign) plus a post-loop safety net (cancel on the last shop/label ends the loops naturally otherwise). Still stops before the *next* campaign (can't interrupt a blocking Ads API call). Frontend: fetch resolves with `cancelled:true` → `renderRunResultPanel` swaps the progress bar for the "Cancelled — stopped early." warning banner + result table (status bar disappears, like a normal finish) and logs `(cancelled)` to the Activity Log. Also deleted a DUPLICATE dead OLD run-result block (178 lines: `renderRunResult`/`renderRunDetail`/`renderRunErrorsTable`/`renderRunCampaignsTable` + dup `runActionLabel`/`selectRunTile`/`sortRunResult`) that shadowed the active renderer. See LEARNINGS "Cancel granularity fix + cancelled banner". #claude-session:2026-07-14 #priority:medium
- [x] **GSD Create-campaigns: run progress bar, run-result table, error surfacing, auto-repair, ad-group naming** (2026-07-14 pm, on `main`; commits `08dea30`,`bc46eb2`,`5d86f8e`). Run now shows a 0-100% progress bar (`_run_progress`+`GET /run/progress`) and renders its result as a **table** (tiles created/paused/skipped/errors + all, click-to-filter, sortable, filter, Export/Copy) like the preview, not raw JSON. Create helpers now surface the real GoogleAdsException (`_gads_err`→`_last_gads_error`→`error` field on failure entries) so partial-creation errors aren't "—". **Auto-repair** (`_repair_campaign`): a run completes an incomplete PAUSED shell (missing ad group / ad / listing tree) + enables it instead of skipping. **Ad-group name = label** (a/b/c/no_data/no_ean), matching original `create GSD-campaigns.py` (was `"<campaign> - Ad Group"`). Diagnosed Calcuso.com|NL "3 created / 2 errors" = 2 partial-creation shells (`c` failed at ad-group, `no_data` at listing-tree) — removed both so they recreate fresh. **Follow-ups (same session, `b7c53d4`/`ff67aaf`):** (1) campaigns now created **PAUSED** — removed the "flip to ENABLED" regression in create + repair (original never enables). (2) **Correct CPR listing tree** — `add_sub_cpr(label)` now builds SUBDIVISION root + biddable UNIT on `product_custom_attribute[INDEX0]==label VALUE` (SPACES: `"no data"/"no ean"`, not underscore) + no_data extras (invld_ean/nd_c/nd_cr) + excluded "other"; was a single root UNIT bidding on everything. (3) run-table cols reordered to Campaign/Country/Action/Detail. **One-off cleanup of today's 20 GSD_SCRIPT campaigns (acct 7938980174):** paused all, rebuilt 14 wrong single-UNIT trees (remove criteria + re-add), renamed 22 old-named ad groups→label, repaired 4 no-tree shells (Hema a/b, Geurfris c/no_ean). Auto-repair SKIPS wrong-but-present trees, so pre-existing wrong trees need explicit rebuild. **(4) `fc54e7a`/`20adadb`:** `_repair_campaign` now VALIDATES tree targeting (`_tree_targets_label`) — a wrong tree (single-UNIT or wrong label value) is removed + rebuilt (`retreed`), so a normal Run now auto-fixes wrong trees for the shops in the run (no longer skips them). Country column left-aligned in preview+done tables. See LEARNINGS "GSD Create-campaigns: run UI, partial-failure shells…". #claude-session:2026-07-14 #priority:medium

- [x] **GSD Campaigns — preview mode, per-run undo + change_event reconstruct, GAQL LIKE shop-match bug fix, + big preview/LL/Activity-Log UI pass** (2026-07-14, branch `rurl-v45-confidence-scoring`, commits `eed3f90`→`1a0eeb6`). Read-only **Preview** for "Create GSD-campaigns" (`preview_gsd_script` + `POST /preview`; per-shop `_preview_progress` + `GET /preview/progress` → real 0-100% bar). Per-run **Reset**: runs performed after this ships stash created/paused ids → direct `undo_run`/`POST /undo` (pause created + re-enable paused); older runs `reconstruct_run`/`POST /reconstruct` from Google Ads `change_event` (Amsterdam-tz window off the log timestamp, ~30d retention, review-before-apply). **Critical fix:** GAQL `LIKE '%[shop:X]%'` matches the WHOLE account (brackets = char class) → a GSD "uit" paused every enabled campaign (cause of today's mass-pause); switched 4 sites to `REGEXP_MATCH` via `_name_contains_regexp`. Also fixed a self-inflicted `exportPreviewXlsx` JS name collision (renamed GSD one → `exportGsdPreviewXlsx`). UI: preview affected-campaign table (outlined action labels, sortable/filter/Export+Copy, click-tile-to-filter, shops link → compact bordered table w/ Country col derived from the ACTUAL previewed campaigns); Activity Log → full-width table w/ outlined all-caps Action labels; LL preview → GSD-style clickable tiles (all/paused/enabled/skipped/errors, skipped+errors now rows, select-all hidden when none selectable) + `table-light` headers matching the shops table. **Open:** `run_gsd_script` ignores the per-country `kolom` flag from `get_redshift_shop_changes` (a per-country feed) and activates ALL model countries (NL/BE/DE for CPR, NL/BE for CPC) per change row — confirm whether it SHOULD scope to `kolom`; and browser-drive the reset flows + LL tile filtering (verified endpoints via curl + `node --check` only). **Reconciled onto `main`** (`6c67073`) — file-level reconcile (NOT a git merge, which conflicted in 7 files and would have dragged older rurl-only seo-stats/kopteksten commits): took rurl's version of the 6 today files, then **unioned `_pause_campaigns_for_shop`** with main's same-day fix `a7df13b` (adds `FROM campaign_label` + `label.name='GSD_SCRIPT'` + per-campaign audit logging) — so the pause query now carries BOTH the `GSD_SCRIPT` label filter AND the REGEXP shop match. seo-stats + cc1 left at main's versions. Branches still differ in history (rurl has older un-reconciled commits) but today's file contents now match on main; live app serves from the rurl working copy so no restart needed. **Later same-day follow-ups** (cherry-picked cleanly to main, preserving the a7df13b union): (1) **per-country `kolom` fix** (`f78896e`→`8b31381`) — run/preview were looping ALL model countries per shop-change instead of the single flipped country from the feed's `kolom`; `Calcuso.com|NL` created NL+BE+DE instead of NL only (3× over-create). Now `KOLOM_COUNTRY`-scoped; preview to_create 180→60, verified. (2) **red Cancel button** (`7af6d37`→`28ccbb7`) — cooperative `_run_cancel` flag checked between shops + `POST /run/cancel`; stops further creates/pauses, already-done shops stay (undoable via Reset). See LEARNINGS "GSD Campaigns — preview + undo/reconstruct + the GAQL LIKE bracket bug". #claude-session:2026-07-14 #priority:high
- [x] **Redirect Tool — outlined per-source Source labels; dm-tools content-width audit + GSD fix** (2026-07-14, commits `1df5271`/`6f4ae30`/`72d8b17`). Recent-results Source column → outlined UPPERCASE badges colored per `input_method` (file=purple, form=orange, text=blue, upload=purple, else grey), DMA-Exclusions style. **Width audit:** all 32 frontends share the identical Bootstrap `.container` (5.3.0, no overrides) — but the real content-width driver is the inner wrapper: **25/32 center content in `row > col-md-10 mx-auto` (~83%)**, 2 use `col-lg-11` (dma-exclusions, seo-titles), and **7 put cards straight in `.container` (full width)**: dashboard, gsd-campaigns, performance-standup, redshift-upload, seo-stats, shop-campaigns, task-scheduler. That full-width GSD Campaigns is why it looked broader than Redirect Tool. Briefly wrapped GSD in `col-md-10` (`72d8b17`) then **reverted per user** (`dc5bd21`) — GSD Campaigns intentionally stays full-width. So no width changes remain from this session; the 7 full-width tools + 2 col-lg-11 are left as-is (not normalized). Separate minor vertical drift: `mt-4` vs `mt-5` top margin on dma-exclusions/seo-stats/shop-campaigns. #claude-session:2026-07-14 #priority:low

- [x] **GSD Campaigns — low-linkage run progress bar + interactive Preview table** (2026-07-09). Commit `ed700e7`. `POST /ll/run` now runs in a background daemon thread (single-at-a-time, `{started}`/`{busy}`) streaming to `_LL_PROGRESS`; new `GET /ll/progress`. UI: striped animated progress bar (FAQ/Kopteksten pattern) polled 0.8s on Preview+Run; Preview results are now a sortable / filterable / paginated (10·25·50·100·Show-all) table with Export Excel + history-styled headers; added "10" page size to all three tables (default 10 on Campaigns created); removed the top counter tiles. Restarted backend (pid 77587), verified dry-run end-to-end (58 feed rows → 235 would-pause, 0 errors, live Ads reads). Gotcha: `pkill -f "uvicorn…"` killed my own shell — use `setsid`/PID. See LEARNINGS "…progress bar + interactive Preview table". #priority:medium

- [x] **GSD Campaigns — low-linkage Pause/Enable tool** (2026-07-09). New feature driven by the pixel-monitor GSD feed: `backend/gsd_ll_service.py` + `POST /ll/run` (dry_run/date/shop_names/included) & `GET /ll/history` in `gsd_campaigns_router.py` + UI in `frontend/gsd-campaigns.html`. Pauses ENABLED campaigns of GSD=0 shops still flagged GSD in `shop_list` (label `GSD_LL_PAUSED`) and re-enables labeled campaigns of GSD=1 shops; every action logged to `pa.jvs_gsd_ll_campaigns`. Base feature reconciled+pushed as `620b19d` (on both `main` and `rurl-v45-confidence-scoring`); UI refinements this session: purple "i"-tooltip like DMA Exclusions, history table matched to "Campaigns created" width/sort, renamed "Run GSD Script"→"Create GSD-campaigns" & "Campaigns"→"Campaigns created", added date-picker + shop-names + filter-mode to the run card, default pagination 25, and split the history into its own **"Enabled / Paused history"** section (country+status filters, Refresh/Export Excel/Copy for Excel — `7b1b4d1`). Backend restarted (pid 67935); `/ll/history` verified 200. **Open:** first real Run should use Preview first to eyeball the campaign list; confirm the date-picker semantics (as-of `shop_list` flags) match intent. See LEARNINGS "GSD Campaigns — low-linkage Pause/Enable tool". #priority:medium

- [x] **GSD Campaigns UI polish (7 tweaks)** (2026-07-09). `frontend/gsd-campaigns.html` only, commit `26fd39e`: (1) SEO-stats flatpickr date picker on the Run date field; (2) removed the purple left-border accent on the Run GSD Script card; (3) Shop-filter-mode radios greyed/disabled until "Shop names" has content (`toggleShopMode()` on input+load); (4) "Export .xlsx"→"Export Excel"; (5) "Copy"→"Copy for Excel"; (6) both restyled to outlined-purple; (7) Activity-Log "Clear" button made transparent (was white bg). Static HTML, no linter, browser refresh deploys. See LEARNINGS "GSD Campaigns UI polish + shared conventions". #priority:low

- [x] **SEO Stats: Top subcats can now sort to negative visit deltas** (2026-07-08). Root cause: backend `_fetch_cat_deltas` slices `by_visits`/`by_revenue` to the top-100 *most-positive* deltas and only computes the negative `worst_*` lists for `level=="deepest"`; the Top-subcats table (>100 leaves) never received the declining tail, so ascending sort showed no negatives (maincats worked only because <100 rows). Fix (frontend-only, `frontend/seo-stats.html`): `catSourceRows()` merges `by_*` + `worst_*` deduped for both the table and the XLSX export; no backend change. **Branch note:** committed on `rurl-v45-confidence-scoring` (the live seo-stats frontend), NOT main — main's `seo-stats.html` still binds the table to `src:'subcats'` (no `worst_*` list) and would need commits `736949f`+`382914e` first. Coverage caveat: merged list = ~100 top risers + ~100 top fallers/metric; raise backend `TOP_N` to sort the mid-distribution. See LEARNINGS "SEO Stats — Top subcats couldn't sort to negative deltas". #priority:medium

- [x] **Auto-Redirects Tier-A perf audit + Phase 1-3 speedups** (2026-07-07). Fixed the `--reuse-data-cache` no-op (`os.remove` deleted the shared pickle every chunk → ~90s rebuild ×N; now gated by `--keep-data-cache` + post-loop cleanup), clamped the 1M `fetch_limit`, head-limited before the shop `.apply`, cached the lowercased facet URL column (459k-row per-URL scan), + regex/memoize/Session/itertuples/batch cleanups; `SEARCH_QPS` now `RURL_SEARCH_QPS` env (default 20). 55/55 tests pass; backend restarted. **Deferred:** #12 per-worker sqlite conn, #10 insubcat prefetch. **Pending before full trust:** OLD-vs-NEW single-chunk output diff on a Redshift/API box; raising `RURL_SEARCH_QPS` needs IT sign-off. See LEARNINGS "Tier-A run performance audit + Phase 1-3 speedups". #priority:medium

- [x] **"SEO titles" generator shipped** (2026-07-06). New Generators tool: Redshift top-X SEO faceted `/c/` URLs → `(cat_id, canon_key)` → dedup vs `pa.page_titles_existing` (tblPageTitles.xlsx, 539,214 rows) ∪ `pa.seo_titles_blueprints` → build deterministic blueprint + AI unique title → push to `/page-titles` (upsert, JSON array, `X-Api-Key`). Files: `backend/seo_titles_service.py`, `frontend/seo-titles.html`, `scripts/load_pagetitles_existing.py`, routes in `main.py`. Row-select + Remove + Publish(selected). **Open:** confirm with the website-configuration API owner that `/page-titles` is the store the live site reads titles from before the first prod push. See LEARNINGS "SEO titles generator …". #priority:medium
- [x] **Redirect-Tool preflight retry hardening** (2026-07-06). `_get_with_retry` now retries timeout + connection + 502/503/504 with exponential backoff (was single 8s attempt, 503 not retried); `LOOKUP_TIMEOUT` 8→12, `LOOKUP_RETRIES` 1→3. Fixes run #30/#31 mass "preflight error" skips from a slow `redirect.api.beslist.nl`. Re-run the skipped subset with "replace existing" to carry them through. #priority:medium

- [ ] **Auto-Redirects — 4 residual `should be` rows (hard/ambiguous, likely wontfix)** (2026-07-03). `geleider`→"Schuifdeursystemen" (semantic; RC6 lands safe parent Deuraccessoires); `japanse verlichting`→Woonaccessoires+Japans-style (no huis_tuin verlichting subcat, target is a stretch); `lampen boven eettafel` (no source subcat; needs `eettafel`→ruimte~Eetkamer room map); `dubbele`→`aantal_fietsen~'2 fietsen'` (contradictory review note + 2==double semantic leap). `parkside` is a bad target (Excel's Zaagbladen subcat has 0 Parkside products = empty page; current populated result kept). Perf follow-up: extend the V28 prefetch to warm both the RC4 `rc4:` probe keys AND the V50 relaxed-query derives before a full global pass. #priority:low
- [x] **dm-tools DMA Exclusions — "Headline offer" fixed (stale ES bestOffer → live in-stock offer) + backfill + OOS-API validity probe** (2026-07-06, branch `rurl-v45-confidence-scoring`). User saw many "—" headline offers, then a WRONG one (`8721398474489` showed Drogistwereld.nl, live headline is Drogist.nl in stock). Root cause: `_headline_offer_uncached` returned the ES `bestOffer` shop, but that flag stays pinned to the cheapest offer after it sells out (`stock=0`) while the live PLP re-ranks to the cheapest **in-stock** offer. Fix `abf6283`: rank all same-EAN offers by `(in-stock, cheapest total = sale|regular + delivery, bestOffer tiebreak)`, fall back only when nothing in stock; `status` now display-only (sole consumer = cache-error guard). Blanks were transient ES misses at save-time (968/2503 NULL); the existing `backfill_headline_shops`/`POST /backfill-headline-shops` has **no UI button** — ran it manually, `only_missing=False` → 2450/2503 updated, 53 unresolved (gone from index). Investigation of 10 `oos_scan` ids: `stock=None`≠OOS (use `blockStatus==0 & productValid & display_online`); 10/10 products have ≥1 available ES offer, but ES is stale (7–25d) + different signal than Google's crawl, and live PLP blocks our scraper — so NOT provably faulty (monitor targets the *advertised* offer, not the product). Deployed via kill 75121 + `setsid` relaunch (pid 89946), verified live. See LEARNINGS "Headline offer was the STALE bestOffer". **Open:** does the gold ad deep-link the OOS shop offer or the re-ranking PLP? (if PLP → false positives) — and add a UI "Backfill headline offers" button. #claude-session:2026-07-06 #priority:high

- [x] **dm-tools DMA Exclusions — OOS "Scan failed: HTTP 410 Gone" diagnosed + shop/PLP enrichment + table layout** (2026-07-02, branch `rurl-v45-confidence-scoring`). The 410 was upstream: the OOS monitor removed `GET /overrides/oos-eans` (now 410 → use `exclude-eans`); the `exclude-eans` migration (c8f5a9e) already fixed it and the 09:18 bare-uvicorn restart deployed it, so the user's error was from the pre-restart process (verified live NL 200 / BE 200). Then enriched the OOS scan: **Shop** (CL3 = `product_custom_attribute3`, already fetched in the GA query & cached — just surfaced in `_build_oos_candidate`) and **PLP url** (separate ES `headline_offer` lookup, fetched only for the final capped set in a 16-worker parallel pass). Frontend: Shop column, EAN links to PLP (`safeUrl`), shop in filter, table `width:auto`+fixed col widths so Category stops swallowing slack, numeric cols right-aligned, ellipsized cells. Gotcha: `pkill -f "uvicorn…"` self-matched and killed the shell mid-restart — use `setsid` / kill by PID. Shipped 2a00e84. See LEARNINGS "OOS … HTTP 410". #claude-session:2026-07-02 #priority:high

- [x] **dm-tools SEO stats — "Top subcats" → "Top deepest cats"** (2026-07-02, same branch). Second Top-categories table now lists true leaf categories (matches Performance Standup). Frontend-only: backend already returned `deepestcats` (`_fetch_cat_deltas(..., "deepest")`, `is_lowest_category=1`); pointed the `sub` table's `src` at it, relabelled heading/column/Excel sheet. Identical row shape → sort + `-` row drop unchanged. Shipped 736949f. See LEARNINGS "SEO stats … deepest cats". #claude-session:2026-07-02 #priority:medium

- [x] **Kopteksten v3 — per-maincat *informationele* koopgids-prompts uit Google-ranking-analyse (built + benchmarked, STAGED, NOT wired, NOT committed)** (2026-07-01). User wanted a serious rework (not a tweak) of the Kopteksten prompt based on the content of pages that actually rank on google.nl. Input `Downloads\claude\seo_urls_content_prompt.xlsx` (162,367 URLs; col A=main_cat_name, B=deepest_subcat, D=page_heading=zoekterm, E=visits). Sampled **117 visit-weighted terms across all 31 maincats**; one research agent per maincat Googled its terms, read the top informational pages (koopgidsen/adviespagina's/merkuitleg/Wikipedia/reviews), and drafted a per-maincat prompt. **Universal finding:** ranking pages are koopgidsen, not blurbs — 6 patterns in all 31 maincats (kies-op-gebruik first; measurable specs WITH meaning; type/variant trade-offs; compatibility+rules; onderhoud/veiligheid/levensduur; koperstaal + scannable multi-section structure) + a bonus winner (honest myth-busting, price-neutral). **Prompts are genuinely distinct per maincat** — avg pairwise similarity only ~9% (the 9% = shared hard-constraint boilerplate); each has 45-101 words unique to that maincat. **Deliverables** (`/mnt/c/Users/JoepvanSchagen/Downloads/claude/`): `kopteksten_informational_prompts_2026-07-01.md` (full reference: 6 patterns, shared base prompt, normalized length policy, per-maincat evidence+prompt), `kopteksten_prompts_per_maincat_2026-07-01.json` (31 prompts keyed by maincat), `koptekst_v1_vs_v3_2026-07-01.xlsx` (benchmark, 43 side-by-sides). **Staged in dm-tools working copy (uncommitted):** `backend/gpt_service_v3.py`, `backend/data/kopteksten_maincat_prompts_v3.json`, `backend/data/koptekst_v3_benchmark_urls.json`, `scripts/koptekst_v3_comparison.py`. **NOT wired** — `main.py` still uses v1 in `gpt_service.py`; only consumer of v3 is the offline benchmark (never touches DB/jobs/production). **Benchmark result (43 URLs w/ products):** v3 vs v1 = 209 vs 112 words, 100% vs 0% multi-paragraph, richer buyer guidance across technical AND soft cats; both clean on prices/wij/exclamations. User decisions: content_top rendering CAN do multi-paragraph (so single-alinea rule dropped for v3); generic filler words left as-is (model-level, same in v1). **User is reviewing the Excel before any wiring/rollout.** Full detail in LEARNINGS "Kopteksten v3". #claude-session:2026-07-01 #priority:high

- [x] **Auto-Redirects V45 scoring redesign (redirects.txt lists #2 & #3)** (2026-06-30, branch `rurl-v45-confidence-scoring`). Search-derived branches shipped flat reliability constants blind to coverage + product-count dominance; `score_search_derived()` now folds both in (bare-cat full count penalty, faceted mild). List #2 all demoted, #3 miele 65→72 + vazen 65→95 (post-append coverage re-score). 0 production A/B→D, score-only, 39 tests. See LEARNINGS "V45/V46". #priority:high
- [x] **Auto-Redirects V46 in-subcat facet selection (list #1 bucket a)** (2026-06-30, same branch). `_value_distinctive_match` (descriptor-aware) fixes usb-ventilator→opties_ventilator~23795868, spikeball→merk~23864170, kinderbankje→opties_stoel~17094990, puree_stamper→type_stmp~6380575. See LEARNINGS. #priority:high
- [x] **Auto-Redirects V47 — correct a wrong/stale facet value vs the live probe (list #1 bucket b)** (2026-06-30, same branch). tuinaarde matched inhoud_tuinaarde~"1200 liter" (~23936743, not on live page) for a 40-liter query; post-processor overrides an in-URL axis to the probe's query-NAMED value (→~23590378). Gated: existing axes only, `_value_distinctive_match`, skip merk/winkel + multi-occurrence axes (avoids the laser_360 merk~X~~merk~X dup I hit when over-reaching). 300-corpus: 2 non-brand value corrections, 0 new dups. #priority:high
- [x] **Auto-Redirects V54 — stop caching transient probe failures (cross-maincat routing cluster: bedhekje)** (2026-07-08, branch `rurl-v45-confidence-scoring`, commit `58d04de`). Cross-maincat cluster re-diagnosed: tochtstopper done (curated), lampen correct as-is (klussen Hanglampen @0.79), hekjes is same-maincat (below V53 floor), solar not fixable via name-match (target is a facet not a subcat name — deferred, de-ranked to 45/D). **bedhekje was the fixable one:** its cross-maincat candidate (baby_peuter 'Bedhekjes', name 99) never verified because `_classify` cached `mode='error'` on a transient None API response and `_cache_get` served it as a fresh hit → permanently blocked re-fetch. Fix: `_cache_put` skips errors, `_cache_get` treats cached errors as a miss. bedhekje → **baby_peuter_563182_5257400 (80/B, verified)**. Only 36/55108 (0.1%) entries poisoned. Bare-corpus: **1 URL change (an improvement: t-shirt 30 jaar wrong cadeaus→correct mode T-shirts), 0 tier changes, 0 A/B→D**. 55 tests pass. **Lesson: never cache a transient fetch failure as an answer (cf. redirect_tool_prefetch_bug).** See LEARNINGS "V54". #claude-session:2026-07-08 #priority:high
- [x] **Auto-Redirects V53 — align maincat facet-match subcat to full-query search-derived dom_cat (redirects.txt batch2 list #1: swiffer/accu)** (2026-07-08, branch `rurl-v45-confidence-scoring`, commit `81b8b07`). The `[maincat] Matched N facet` path parks the matched facet VALUE at the subcat where that value's product COUNT is highest (FacetFilter), ignoring the unmatched head noun → swiffer_doekjes landed on parent Schoonmaakartikelen not Schoonmaakdoeken; accu_12v_72ah on wrong sibling not Auto-accu's. Post-processor rewrites the subcat to `derived['dom_cat_url_slug']` when same-maincat, not-an-ancestor, **dom_share>=0.45**, and the facet value exists there (in-memory facets.csv check, no live call). **swiffer → klussen_486260_488654, accu → autos_482566_6437006** (both exact wanted targets); lego kraan unchanged (search-derived agrees: Bouwstenen @0.55, user alt is search-unjustified). **Design lesson: dominance is the safety signal, NOT parent/child — a low-dom child (adidas outlet→Hardloopschoenen @0.23, led lamp→LED Strips @0.1) is a worse pick than the parent; the 0.45 floor cleanly separates good from bad.** Bare-corpus: 8/1200 rewrites, all dom>=0.47 plausible, 0 tier changes, 0 A/B→D. 55 tests pass. See LEARNINGS "V53". #claude-session:2026-07-08 #priority:medium
- [x] **Auto-Redirects V52 — fold maincat facet-match into dominance+count scoring (redirects.txt batch2 lists #2 & #3)** (2026-07-08, branch `rurl-v45-confidence-scoring`). The `multi` / `*_with_probe_facet` paths were scored coverage-aware but DOMINANCE-BLIND → deurbel 24 volt (3 products) and windmolentje (1249 products) both 70/C. Added those 3 types to `_V45_DOM_SCORED_TYPES` (not COVERAGE_FLAT → dom+count only, no double-dock). windmolentje 70→**76/B**, deurbel 70→**60/C**, solar_buitenlamp 70→**45/D** (side benefit: list-#1 bad suggestions self-derank). Validated on a **BARE-URL corpus** (1200 real /r/query/ URLs, facets stripped — the indexnow corpus is ~100% existing-facet OUTPUT and V52 correctly ignores those): 0 URL changes, **0 A/B→D**, 54 tier demotions (all low-dominance weak redirects, e.g. tweedehands_fitness dom 0.01) + 33 score-ups (high-dominance strong matches). 55 tests pass. **Two methodology traps recorded in LEARNINGS: (a) engine resumes from `<output>_progress.csv` — use fresh filenames; (b) test bare URLs, not indexnow output.** 1 file, `main_parallel_v2.py`. NOT committed yet at time of writing. #claude-session:2026-07-08 #priority:high
- [ ] **Auto-Redirects redirects.txt batch2 — list #1 routing fixes (DEFERRED, each its own increment)** (2026-07-08). Diagnosed 12 "weird suggestions"; none is a scoring issue and V52 already de-ranks the worst. Buckets: (1) subcat-selection lego_kraan/swiffer_doekjes/accu_12v/kinder_auto — **swiffer + accu DONE (V53, 81b8b07)** via search-derived dom_cat alignment; lego was already search-justified (Bouwstenen); kinder_auto still open (its own diagnosis needed); (2) over-faceting relax_fauteuil (materiaal 'Leer' duplicates bekleding) + smalle_kast (spurious kleurtint appended); (3) cross-maincat solar_buitenlamp→tuin_accessoires s_lamp / hekjes_voor_honden→Hondenrekken (weak same-maincat over better cross-maincat); (4) broekpak dames grote maat under-facet (missing populaire_themas_mode 'grote maat'); (5) spy_camera_wifi o_rookmelder value pick; (6) koelkast-met-vriezer = maincat-less global-pass URL (out of per-URL tool scope). Plus windmolentje "voor in de tuin" coverage filler (exclude maincat-name tokens from coverage denom) — user-suggested, deferred (touches coverage for all types; V52 already lifts windmolentje to B). #priority:medium
- [x] **Auto-Redirects V51 — synonym-aware coverage for RC4-enriched rows (redirects.txt list #1: vintage)** (2026-07-08, branch `rurl-v45-confidence-scoring`, commit `21f44f4`). Picked up the "pikachu/vintage category_fallback" open case; found RC4 already fixed pikachu (80/B) and routes vintage to the correct `bouw_koelkast~23593989 'Retro'` — but vintage scored **37/D** because the V45 coverage recompute compares the query literally to the appended value NAME, and "vintage" shares no token with "Retro" (RC4's probe matched them only through its curated `_ENRICH_SYNONYMS` map). Fix: the recompute now expands each query word with the SAME curated `_ENRICH_SYNONYMS` (+`_stem`) the probe uses, so a value matched via synonym reads as covered. Lift-only (guarded by `_recomputed > _v45_cov`). vintage 37/D→**63/C** (coverage 0→100), pikachu unchanged 80/B, dubbele still bare (semantic gap, deferred). Regression: **0 changes across 300 random /r/ URLs + 32 synonym-targeted URLs**; 55 tests pass. Optimizer is a subprocess → no uvicorn restart to deploy. 1 file, `main_parallel_v2.py`. See LEARNINGS "V51". #claude-session:2026-07-08 #priority:medium
- [ ] **Auto-Redirects list #1 — REMAINING cases (RE-DIAGNOSED 2026-07-08; task below was pre-RC4 and STALE)** (2026-06-30, updated 2026-07-08). Re-ran the three category_fallback cases against current HEAD — RC4 (2026-07-03, in-subcat facet enrichment) already resolved most of the "bare category not facet" complaint: **pikachu → personage~23600616 (80/B, DONE by RC4)**; **vintage → bouw_koelkast~23593989 'Retro' — RC4 routes it correctly, was scored 37/D purely by a coverage bug → FIXED 2026-07-08 (V51 synonym-aware coverage), now 63/C** (honest tier for a 15-product faceted page). Genuinely still open: (1) **dubbele → aantal_fietsen~23588103**: value name is literally "2 fietsen"; needs a `dubbele`→"2 fietsen" numeric/quantity synonym — fragile & niche, low value, DEFERRED. (2) **Fix-D `_keep_fd` gate** (60_cm_breed→a_woonacc~60cm): can't loosen without re-admitting waxinelicht→Groot junk; needs a category-on-topic discriminator. (3) **cross-maincat routing** — bedhekje→baby_peuter **DONE (V54, 58d04de)**; tochtstopper done (curated); lampen→klussen Hanglampen is actually correct (@0.79, user pref debatable); **only solar_buitenlamp remains** (target is a facet not a subcat name → needs a dominant-MAINCAT product-count signal; deferred, de-ranked to 45/D). (4) **lexical/semantic** (peuter≈Kind + doelgroep generic-attr-suppressed, loungeset≈Loungebank): synonym entries, broad effects. **Latent follow-up:** RC4-enriched rows are scored with `dom_share`/`dom_cat` from the maincat-wide probe, which often describes a DIFFERENT category than the RC4 target subcat (vintage's dom_cat = Broodroosters 0.23, not koelkasten) — the -15 dom penalty there is a wrong signal that happens to offset the too-lenient count guard (dom_count=67 not the facet's own 15). Fixing it "properly" = neutralize dominance + use the facet's own count for RC4 rows; deferred (changes tiers only marginally, needs its own corpus diff). Pre-existing dup bug also found earlier: schoenen_570132 emits `doelgroep_schoenen~430828~~doelgroep_schoenen~430828` (same-value dup, in OLD too — separate matcher dedup gap). #priority:medium
- [ ] **Auto-Redirects list #1 — lexical/semantic gaps** (2026-06-30, updated 2026-07-08). Synonyms: peuter≈Kind (doelgroep, also generic-attr-suppressed), loungeset≈Loungebank. **Cross-maincat routing mostly resolved (V54 + curated): bedhekje/tochtstopper done, lampen correct; only solar_buitenlamp remains** (facet-target, needs dominant-MAINCAT product signal — the genuinely-architectural piece, deferred; solar de-ranked to 45/D so not urgent). #priority:low

- [ ] **R-URL optimizer — 3 user-reported subcat/axis mis-picks (NOT Fix-D-append)** (2026-06-24). From the `redirects.txt` batch: (a) `badmeubel_120` lands on the right category (Badkamermeubelen) but the wanted `b_meubels~9272494` type facet is never surfaced by the maincat-level probe → needs a probe *inside the resolved subcat* before append (Phase-2-class). (b) `balkon_setje` & `badkamer_trolley` are decided by the lexical cascade (not Fix D): correct facet (`ruimte~Balkon`/`~Badkamer`) resolved to the WRONG subcat (arbitrary carrier / Badkamermeubelen instead of bare-maincat / Keukentrolleys), head noun dropped — same family as the token-coverage short-circuit + subtree-rescue subcat-selection tasks below. (c) `dopjes_stoelpoten` matched the wrong axis (`onderdelen_kast~Poten` not `onderdelen_stoel`), already score 17/tier D — matcher precision. Each needs the OLD-vs-NEW corpus diff. See LEARNINGS "Fix D facet-append … 2026-06-24 follow-up". #priority:medium
- [ ] **R-URL optimizer Fix D — Phase 2 junk suppression** (saved 2026-06-24, Phase 1 shipped). Suppress volume-artifact Fix D redirects (lichtgewicht→Insectenbestrijding, waxinelicht→Gedenkartikelen@60%) WITHOUT the over-broad P4 rule (it suppressed 67% incl. correct redirects). Order: (1) fix `-oos/-ozen` Dutch plural-morphology gap in `_keyword_bridges_value` (rescues false-suppress bucket + helps matching everywhere) — **DONE 2026-07-03 (V48/RC3, commit `9dbcae6`): additive `_bridge_stem` f→v/s→z/double-vowel, 0 lost of 36,715 pairs**; (2) suppress only single-token generic-attribute queries with probe `no_match` that name no category post-fix, sized against `Downloads\claude\fixd_policy_comparison.xlsx`; (3) suppress fallback = source's own category page (`builder.build_category_only`, user-chosen). Re-confirm candidate set vs current caches — post-Phase-1 lichtgewicht no longer hits Fix D. Harness: `backend/rurl_optimizer_v2/test_fixd_policies.py`. See LEARNINGS "Fix D facet-append (V44 Phase 1)". #priority:medium

- [ ] **Bureaustoelen `t_stoel` seoPriority revert candidate + provenance of the 28 May batch** (2026-06-12). The 2026-05-28 manual batch (446 CategoryFacetSettings → `seoPriority=false`) turned off Bureaustoelen `t_stoel` — 1,081 visits/yr, seo-prio run verdict KEEP, page still 200. Verify in GSC, then restore via GET-merge-PUT `CategoryFacetSettings` (include displayOrder etc., `X-User-Name: SEO_JOEP`). Also: ask who/what ran the 28 May batch (not the dashboard — no write code; rulings checks ran same morning). 7 other keep-verdict flips were 301-redirected URLs = justified. See LEARNINGS "SEO week 23 vs 22 drop". #priority:medium
- [ ] **QLED-TV's wk23 revenue collapse unexplained** (2026-06-12). Visits +5% / revenue −95% wk22→wk23, only 9 low-traffic facets touched — looks like one large shop pausing or a feed issue, NOT SEO. Shop-grain Redshift check (like the Bedden/Sportshirts one in LEARNINGS) never ran for QLED. #priority:low
- [ ] **Decide: upload the 32 verified cross-maincat redirects via Redirect Tool** (2026-06-13). `Downloads\claude\cross_maincat_fallback_verified_20.xlsx` (top 20 by visits) delivered for review; full set of 32 in `/tmp/xmc_verified.json` (volatile — regenerate via the V36 collection recipe in LEARNINGS if lost). If approved, push through the Redirect Tool (chain-flatten handles the /r/ sources). #priority:medium
- [ ] **Core-update analysis on bt.search_console (next step)** (2026-06-10). Comparison deliverable done (`/mnt/c/Users/JoepvanSchagen/Downloads/claude/search_console_may_vs_june_2026_nld_v2.xlsx`); script `/home/joepvanschagen/sc_compare.py`. Still owed: (a) get OFFICIAL Google core-update rollout dates from user → window pre-baseline vs post-completion, exclude rollout days; (b) build daily clicks + **click-weighted** avg_position time series to locate the step-change; (c) paired url/keyword winners/losers analysis (position-delta distribution, segment by type_url/keyword_length/intent flags); (d) decide whether to regen current file to 6 days each (June 7 confirmed CLEAN). Trust rules: `deleted_ind=0`, drop June 2 + trailing 2–3 days, clicks & click-weighted position are reliable / impressions are not. See LEARNINGS 2026-06-10 entry for full detail. #priority:medium
- [ ] **R-URL optimizer: token-coverage single-result short-circuit caps multi-facet redirects at ONE facet** (found 2026-06-02). `matcher.match_multi_word` runs `match_by_token_coverage` on the full keyword first and `return [tc]` if it matches, short-circuiting the per-word/pair passes. So multi-attribute queries get only the single best-covering facet: `nederlands elftal trainingsshirt wk 2010` → only `fanshop~Nederlands Elftal` (no type/merk); after the 2026-06-02 leftover-strip change, `nederlands_elftal_t-shirt_-_ek_88_-_xl_-_oranje` lost its valid `kleur~Oranje`. Fix likely = let token-coverage seed the result set but continue the per-axis passes (dedupe by axis) instead of returning early. Needs a regression pass against the cases the short-circuit was added to protect (the V29 "vaste senioren telefoons" → "Senioren telefoon" example). #priority:medium
- [ ] **R-URL optimizer (global pass): curacao-style queries now land on parent `Shirts` instead of `Sportshirts`** (found 2026-06-02). Side effect of the leftover-strip fix (`c898cb2`): the bogus `Fietsshirts` facet that used to pin `Sportshirts` is gone, but the queries fall back to the broader `Shirts` (mode_432360) rather than the more specific `Sportshirts` (mode_432360_469350). Net better (bogus facet removed) but a specificity downgrade — decide whether to prefer the deeper sibling when the leftover has no facet. #priority:low
- [ ] **R-URL optimizer (main pass): subtree-rescue bare-baseline branch jumps to a wrong child for some single-token compounds** (found 2026-06-03, from `55f1048`). When the anchored cascade yields 0 facets, the rescue adopts any faceted result — and for single-token compound nouns the type-discovery sometimes picks a wrong child: `gangkast` → Hoogslapers (hallway cabinet ≠ loft bed), `bureaukast` → Bureaus, `tv_meubel_met_lift` → Bedonderdelen. The same-subcat enrichment branch (b) is safe; only the 0-facet "adopt any" branch (a) is exposed. Likely fix: require a high-confidence type match (score threshold) before descending to a DIFFERENT child subcat in branch (a); a weak match should stay on the bare parent. Re-diff the subcat-present corpus after. #priority:medium
- [ ] **R-URL optimizer: full 754-URL main-pass corpus diff never completed** (2026-06-03). The side-by-side harness (`git show HEAD:… > /tmp/base.py` + importlib, both modules in one process) timed out at ~450/754 under the 595s cap. Key cases verified manually (target, alcatel, illy, tapijt, samsung, gangkast), but a clean full diff of `55f1048` vs its parent over all subcat-present URLs is still owed. Faster approach: run each version in a SEPARATE process writing JSON, then diff the JSONs (lower peak memory, parallelisable). #priority:low
- [ ] **Audit follow-ups deferred from the 2026-06-23 phase-0–2 fixes** (low-risk, latent). (1) Redirect-tool full no-op-on-retry idempotency — restore-on-failure already makes retries non-destructive, so this only avoids confusing re-run false-failures; adding it puts a live lookup in the mutating submit path. (2) Redirect-tool trailing-slash match-insensitivity (`equiv_key`/`url_variants`) — needs coordinated changes + a product decision on slash semantics; over-merge risk. (3) Canonicals `transform_url` fixpoint-idempotency — corpus is already idempotent, a fixpoint loop could over-apply. (4) Canonicals `_apply_cat_cat` position-anchoring — unanchored `str.replace` could corrupt a slug that's a substring of another segment, but it's single-occurrence in practice. Each needs the OLD-vs-NEW regression diff if picked up. See LEARNINGS "Auto-Redirects V42/V43 … — Deferred". #priority:low
- [ ] **Decide gated-subcategory policy for Auto-Redirects beyond horloge** (2026-06-23). `GATED_SUBCATEGORIES` currently seeds only Horlogebandjes + Horloge-onderdelen. The "accessory subcat steals a generic brand query" pattern recurs elsewhere (phone cases vs phones, cartridges vs printers). Also confirm the chosen rule (count-leader-after-exclusion, so casio→Digitale horloges not Polshorloges) is what's wanted per maincat, or switch specific maincats to a forced default. #priority:low

## In Progress
_Tasks currently being worked on_


## Completed
_Finished tasks (move here when done)_

- [x] **tag_toppers negatives sync — NL + BE + DE ALL DONE** (2026-07-28, ops task, no code committed; scripts in session scratchpad `gads_client.py` / `tag_toppers_sync.py` / `resync_unmatched.py` / `audit_workbook.py`). Per `[label:tag_toppers]` campaign, copy any negative keyword its shop's active non-tag_toppers `[channel:directshopping]` campaign has but it lacks. **881 campaigns in scope (NL 507 / BE 351 / DE 23, ENABLED+PAUSED; 112 REMOVED excluded), 1,122 negatives added across 313 campaigns, 0 errors**, verified by a duplicate-aware re-read of all 881 rows (`before + added == live` on every row). Four passes: exact-name active sibling (800 camps / 1,016 negs) → case-insensitive shop name (4 / 8) → PAUSED sibling fallback (64 / 78) → `|NL`-style suffix stripped (13 / 20). Deliverable `Downloads\claude\tag_toppers_negatives.xlsx` (requested columns + `note` + `match status`, colour-coded per pass, `no match` sheet now empty). Auth = OAuth from `dma_script/.env` + Windows env client id/secret (laiza's service account is read-only). Key gotchas — `shop_id` is NOT unique per shop (652237 = Bruna.nl AND Hubfootwear.com), campaign names are NOT unique but all duplicates are REMOVED, shop name case differs between a shop's own campaigns, and campaign-level criteria are the only negatives source (0 ad-group, 0 ENABLED shared sets). **Open follow-up: 7 malformed keywords propagated from the source campaigns** (see BACKLOG). See LEARNINGS "tag_toppers-negatives sync". #claude-session:2026-07-28 #priority:medium

- [x] **Kopteksten v3 — wire the per-maincat prompt into the Batch-API path** (2026-07-06, commit `85e0c8e`, pushed to dm-dashboard). Real-time/regeneration path already used v3 since 2026-07-02; the OpenAI Batch-API bulk path (`batch_api_service._build_kopteksten_messages`) still hard-coded v1. Routed it through v3, gated by the same `KOPTEKST_PROMPT_VERSION` env var (default `v3`) so one toggle drives both paths and `v1` falls back with no code change; `_build_kopteksten_messages(page_data, url)` resolves maincat via `resolve_maincat_from_url` (needs the real `/products/{maincat}/{sub}/` format) and builds the per-maincat system + v3 user prompt (generic v3 base for unmapped slugs). Deployed via manual uvicorn restart. Also produced a per-maincat-prompt explainer deck (`Downloads/claude/Kopteksten_v3_per_maincat_prompts.pptx`). See LEARNINGS "Kopteksten v3 into the Batch-API path". #claude-session:2026-07-06 #priority:medium

- [x] **Fix `shutdown_event` crash on a stale `_taxonomy` import** (2026-07-06, commit `90fd3b3`, pushed to dm-dashboard). `main.py::shutdown_event` did `from backend.url_validator_service import _taxonomy` — a name that no longer exists (session lives on `_cache._session`), so the ImportError crashed the whole handler ("Application shutdown failed") and no HTTP sessions got closed on shutdown. Now imports the module and resolves defensively with nested `getattr`, matching the other session lookups. Applies on next backend start. See LEARNINGS "shutdown_event crashed on a stale import". #claude-session:2026-07-06 #priority:medium

- [x] **Auto-Redirects — Tier-A responsiveness + green bars + 3 redirect-quality fixes** (2026-07-03, commits `6194e04`/`77226c4`/`be2efa5`/`2245379`/`8f47772`/`04cac6d`, pushed to dm-dashboard). Tier-A "stuck at 1%" was invisible prefetch, not a hang: `PYTHONUNBUFFERED=1` + parse `[V28/V29 prefetch] X/Y` into the status + adaptive chunk `clamp(target*20,1000,20000)` + `--reuse-data-cache` (V29 facet-probe prefetch ~270s/1k-chunk is the inherent bottleneck). All progress bars → green `#00b894` (shared `.progress-bar` rule was orange `var(--color-button)`; bg-* state classes still override). tochtstrippen: don't append a facet for the category-noun token (leftover-detection now skips a token that `_keyword_bridges_value`-bridges the resolved category name). nespresso: stale cache, not a live bug (loose bridge counts espresso⊂nespresso; current code gives merk-only; left as-is). hoek: cross-category facet jump on a generic attribute — pure H1 threshold can't separate afkortzaag(50,good) from hoek(53,bad), so suppress cross-cat jump when H1<45 OR (H1<65 & all-generic incl. shape nouns). Engine fixes are live per-run (subprocess); CSS live on refresh; the Tier-A service fixes needed the restart (done). See LEARNINGS "Tier-A responsiveness + green bars + three redirect-quality fixes". #claude-session:2026-07-03 #priority:high

- [x] **Auto-Redirects — "Tier A limit" mode (process Redshift until N tier-A redirects)** (2026-07-03, commit `6ad6a26`, pushed to dm-dashboard). Redshift-only checkbox + "Tier A target" input; chunked loop (`_run_tier_a_loop` + `_run_optimizer_chunk`) processes 20k-URL chunks (highest-visits first, cache/shopname-filtered) until N tier-A (score≥90) redirects exist / Redshift exhausted / 300k-URL cap. Output = tier-A only, capped, sorted by score; all rows still cached. Overrides plain Limit. Verified end-to-end (target 5, 300-URL chunks → 5 rows @ 95-96). **NEEDS BACKEND RESTART to activate** (bare uvicorn, no --reload) — before it, the frontend's tier_a_limit is ignored by the old router AND row_limit is omitted → unbounded run. See LEARNINGS "Tier A limit mode". #claude-session:2026-07-03 #priority:high

- [x] **Auto-Redirects V50 — over-specific query relaxation (the "cross-subcat routing" RC, resolved)** (2026-07-03, commit `4f07a47`, pushed to dm-dashboard). Investigation showed the RC isn't facet-value routing: **slush** is a business/taxonomy preference (IJsmachines has MORE slush products than Funcooking, 186 vs 91 — not derivable, stays curated); **playmobil** is query relaxation (`playmobil family fun grote camping` collapses to 1 product, but `playmobil family fun` dominates in Bouwstenen where the cascade already finds the series). V50: when a result collapses to noise (`search_derived_dom_count<=2`, ≥4 sig tokens), re-run the whole cascade on the query minus 1..3 trailing sig tokens, adopt only if score ≥ current+25. On a 200-row collapse sample only playmobil flipped (1%) — `hot wheels ultimate garage`→Speelgoed garages and other low-count-but-correct picks untouched. Removed playmobil curation (now derived); kept slush. Fixed a maincat-only URL-rebuild bug (duplicated maincat segment). 55 tests. See LEARNINGS "V50". #claude-session:2026-07-03 #priority:medium

- [x] **dm-tools DMA Exclusions — surface the live headline-offer shop (fix mislabelled "Shop" column)** (2026-07-03, commit `0112c91`, deployed + pushed to dm-dashboard). User: excluded item `nl-nl-gold-8806097002291` showed shop `azerty.nl` but the headline offer is `MediaMarkt.nl`. Root cause: the "Shop"/"Headline shop" column rendered `product_custom_attribute3` (the **DMA feed CL3 partition shop**, `min()`-collapsed across serving rows → possibly just alphabetically-first), NOT the live ES `bestOffer`. `headline_offer()` already returned the real `headline_shop` but every caller discarded it (only `plp_url` was kept). Fix: keep the whole dict in `apply()`/`oos_exclude()`/`oos_scan()` (same ES call), thread `headline_shop` through `_persist_apply` into a new `dma_exclusions.headline_shop` column (`ADD COLUMN IF NOT EXISTS`; `COALESCE`-guarded on re-apply; migration self-heals on the next restart via `_ensure_table`). `backfill_headline_shops()` + `POST /backfill-headline-shops` filled existing rows from the live index (reflects *today's* bestOffer, not exclusion-time — ES has no history): 1564 rows → 1535 filled, 29 unresolved. Per user's follow-ups: **removed the DMA-feed-shop column entirely** (saved list, OOS candidates, preview panel, Excel export) leaving only the left-aligned Headline offer column, dropped the interim `≠ feed` badge + helpers, widened the Country column, and removed the "Fill headline offers" button (kept the endpoint). Deployed via manual uvicorn restart (pid 390845). See LEARNINGS "the 'Shop' column was the DMA feed shop, not the live headline offer". #claude-session:2026-07-03 #priority:medium

- [x] **Auto-Redirects — final `should be` rows: dimensions + audience + slush/playmobil** (2026-07-03, commit `9de9244`, pushed to dm-dashboard). `_split_dims` (query `200x200` ~ value `200 x 200`) → 2_persoons_bed enriches with aantal_slaapplek + afmeting (exact). Audience synonyms peuter/kleuter→`Kind` doelgroep → peuter gets `doelgroep~Kind` (dropped speculative dreumes→baby). Curated slush→`type_funcooking~Slush` and playmobil→`playmobil_series~Family Fun` (their facet value lives in a sibling subcat of the dominance pick — the general fix is the new "cross-subcat facet-value routing RC" task). 55 tests pass. Final of the 22: **14 exact + 2 score-only + 2 partial (peuter, geleider); 4 residual** (geleider/japanse/lampen/dubbele hard-or-ambiguous, parkside empty-target). See LEARNINGS "final 'should be' rows". #claude-session:2026-07-03 #priority:high

- [x] **Auto-Redirects V49 RC4 phase-2 — prefer-source routing + enrichment synonyms** (2026-07-03, commit `5243f61`, pushed to dm-dashboard). Prefer-source: a cross-maincat jump whose source subcat has a distinctive non-brand facet the query names is rerouted back to source subcat + facet (never over verified/curated; 1/120 on the risk surface, correct). Fixes `loungeset hoes 320`→tuin_accessoires `t_tuinmeubelhoes` (was meubilair 'Loungesets' 40/D). Tiny `_ENRICH_SYNONYMS` (vintage↔retro) for values that are lexical synonyms of the query word → `vintage`→`bouw_koelkast~Retro`. `parkside` left (its Excel target subcat 'Zaagbladen' has 0 Parkside products = empty page). 53 tests pass. Final of the 22: 13 exact + 2 score-only + 2 partial; 7 remain (RC1/semantics — see Current Sprint). See LEARNINGS "V49 RC4 phase-2". #claude-session:2026-07-03 #priority:high

- [x] **Auto-Redirects V49 (RC4) — in-subcat facet enrichment for bare category redirects** (2026-07-03, commit `bfaa19c`, pushed to dm-dashboard; optimizer subprocess, no restart). Probe INSIDE the resolved subcat for a distinctive non-brand facet the query names and append it to a bare category page — enrichment-only, never changes the category. New `facet_probe.py::_extract_enrichment_facets` (SEPARATE from `_extract_multi_facets` → RC2 untouched): accent-folds (`geisoleerd`~`Geïsoleerd`), strips parentheticals (`pikachu`~`Pikachu (pokémon)`), excludes merk/winkel (kills the `peuter`→`Peuterey` trap). `derive_insubcat_facet` relaxes the query (full → each token longest-first, since the exact multi-token query has ~0 in-subcat products) and caches under a `rc4:` key. Wiring gotcha fixed: reset `final_score=70` before the V45 block or a `category_fallback` base of 0 tanks it (pikachu 10→80). Fixes pikachu→`personage` (80/B), geisoleerd_tuinhuis→`o_tuinhuis` (84/B), 2_persoons_bed→`aantal_slaapplek` (75/B). Corpus: 8% of sampled bare rows enriched, all sane. 52 tests pass (`tests/test_v49_enrichment.py`). Remaining `should be` rows → new "RC4 follow-ups" task (prefer-source routing / synonyms / RC1). See LEARNINGS "V49 (RC4)". #claude-session:2026-07-03 #priority:high

- [x] **Auto-Redirects V48 — match the reviewed "should be" targets (6 of 7 root causes)** (2026-07-03, commit `9dbcae6`, pushed to dm-dashboard; optimizer is a subprocess so no uvicorn restart needed). From `auto_redirects_v1_v2.xlsx` (the `redirects.txt` #1/#2/#3 lists re-reviewed after V45–47). Diagnosed all 22 rows into 7 root causes; shipped 6, each corpus-validated. **RC3** additive Dutch plural-voicing in `_keyword_bridges_value` (`_bridge_stem`: f→v/s→z/double-vowel) — 0 lost of 36,715 pairs, 762 new; fixes `kruimeldief`→`type_stofzuiger`. This is the `-oos/-ozen` morphology gap the Fix-D Phase-2 task named as prerequisite (1) — now DONE. **RC2** Fix D keeps a distinctive non-brand facet + drops a subsumed brand (asics→`populaire_serie`; corpus-wide lego→`lego_series`, samsung→`productlijn_mobtel`, seiko→`serie_horloge`). **RC5** verified cross-maincat name match > weak same-maincat stray (`bedhekje`→baby_peuter; fired 0/40 sampled). **RC6** threshold-gated suppress-to-source (`muur`, `60_cm_breed`; 0.8% of rows). **RC7** bare low-coverage scored lower (`aftakdoos` 63→49/D) + strong verified cross-maincat → tier B (`miele` 72→80/B). **RC8** `CURATED_OVERRIDES` dict (`wasmachine droger`→meubilair `t_badkast`, `tochtstopper`→klussen `Tochtstrips`). Added `tests/test_v48_bridge_morphology.py` + `tests/test_v48_scoring.py`; 48 pass. Blast radius measured on the sqlite probe cache (full-engine corpus run hits live Search API + times out). See LEARNINGS "V48: match the reviewed 'should be' targets". #claude-session:2026-07-03 #priority:high

- [x] **dm-tools DMA Exclusions — parallelize the exclusion process (audit + 4 phases)** (2026-07-02, commit `92070ea`, deployed + pushed to dm-dashboard). `/audit` of the exclusion flow → made it faster without behavior change. Serial bottlenecks removed: bulk `oos_exclude`/`oos_reenable` looped `apply()`/`enable()` per item; each `apply()` rebuilt the GA client + re-ran the ~6s lookup + mutated targets one by one. **Phase 1** (per-item): memoized `_get_client()`, warm `_RES_CACHE` in `lookup()` (kills the double ~6s query on preview→apply), parallelized per-target writes, overlapped the PLP lookup, bounded scan caches. **Phase 2** (bulk): resolve all items concurrently → execute grouped **by ad_group_id** (parallel across ad groups, serial within with 2nd+ re-resolved via new `_resolve_ad_group_target`), new per-ad-group `_ad_group_lock` on `_apply_one_target`+`enable`'s revert, `oos_reenable` parallel; `oos_scan` PLP try/except + no wasted GA wave past `limit`. **Phase 3**: deterministic `_pick_category` (sort), `already_excluded`/`noop` statuses + frontend badges, dedup ids, `_ga_search_rows` guard. **Phase 4**: dead `_build_target` params, heapq eviction, monotonic TTLs (skipped enable bid inherit-vs-explicit — live-bidding risk, effective value identical). Kept the live mutation byte-identical to the old path on purpose (reuse `_apply_one_target`). Verified: OLD-vs-NEW offline tree simulator (byte-identical incl. shared-ad-group subdivide/append) + two live apply→enable round-trips on `nl-nl-gold-6941057404028` (0 errors, full restoration), run via venv pre-restart. Est: single apply ~14s→~5s, bulk 100-item ~13min→~1min. See LEARNINGS "parallelized the exclusion process". #claude-session:2026-07-02 #priority:high

- [x] **dm-tools Kopteksten — wire v3 per-maincat prompts into production** (2026-07-02, commit `892027a`, pushed to dm-dashboard). Made the v3 informational koopgids prompts (31 per-maincat system prompts, `kopteksten_maincat_prompts_v3.json` + `gpt_service_v3.py`) the DEFAULT for new kopteksten via `KOPTEKST_PROMPT_VERSION` (default `v3`, env `=v1` to fall back — v1 stays fully intact in `gpt_service.py`). Wired into both branches of `main.py::process_single_url` (the one live path for manual + daily-automation generation). Added `resolve_maincat_from_url` with an explicit 31-entry URL-slug→maincat map (legacy URL slugs ≠ v3 prompt keys); unknown slug → generic v3 base. Same model/product-context/link-validation as v1; only the system message changes. Validated: syntax, main.py import (`[startup] Koptekst-promptversie: v3`), all 31 live slugs resolve, mocked E2E generation uses the right per-maincat prompt. **Deploy pending** — a DMA Exclusion run was live so the backend restart (pid 293, no `--reload`) was deferred to the user; v3 goes live on next relaunch. See LEARNINGS "Kopteksten — v3 per-maincat prompts wired into production". #claude-session:2026-07-02 #priority:high

- [x] **dm-tools DMA Exclusions — OOS flow simplified to trust the monitor's `exclude-eans` list** (2026-07-01, commit `c8f5a9e`). The monitor owner replaced the `/oos-eans` + `/by-eans` pair with one authoritative `GET /api/v1/overrides/exclude-eans?country=NL` → `{healthy, as_of, count, eans}`, guaranteeing every listed EAN is the cheapest, still-live, Google-OOS headline offer confirmed within ~2 days ("not on the list" = safe to re-enable). Removed ALL client-side re-verification (~290 net lines): the whole verdict layer (`_enrich_oos_headline`/`_oos_verdict`/`_oos_offers`/`_oos_by_eans`/`_headline_offers`/`_crawl_age_days` + `CRAWL_STALE_DAYS`/`_OOS_CACHE`/… consts) is gone; `_oos_eans` → `_exclude_eans`. **Scan** = list ∩ live-in-DMA (GA, for spend/metrics only), all excludable, `limit` caps candidates, response adds `healthy`/`as_of` and drops `headline_counts`; still read-only. **Exclude** drops the server-side safety net. **Re-enable recovered** reverts to pure set-membership (absent-from-list → re-enable) with a `healthy=false` guard that re-enables nothing on a degraded snapshot; stays a separate action. Frontend trims the scan table 8→5 cols, Select-all = all non-excluded, adds a "⚠ monitor unhealthy" badge; kept the ES `headline_offer` (apply()'s Saved-list PLP). Deployed via manual uvicorn restart; verified live (`healthy:true, count:757`; scan limit=20 → 200 ~12s) with read-only smoke tests. Obsoletes the cold-scan-time + stale-crawl-residual BACKLOG items. See LEARNINGS "OOS flow simplified to trust the monitor's `exclude-eans` list". #claude-session:2026-07-01 #priority:high

- [x] **dm-tools DMA Exclusions — per-item progress bar for OOS Re-enable recovered** (2026-06-30, commit `6ded3c3`, frontend-only). Follow-up to the `/by-eans` migration: the Re-enable button only had a spinner. `reenableRecovered()` now keeps the `Checking…` spinner while `/oos/recovered` resolves the set, then loops the recovered rows calling `POST /enable/{id}` per item (not the bulk `/oos/reenable`) driving the shared `oosProgress` bar with live `% (done/total)` + Cancel, torn down before the post-run refresh — mirrors `excludeSelectedOos`/`enableSelected`. Bulk `/oos/reenable` now UI-unused but kept programmatically. `node --check` validated; live on browser refresh (StaticFiles, no restart). See LEARNINGS "Re-enable progress bar — per-item, client-driven". #claude-session:2026-06-30 #priority:medium
- [x] **dm-tools DMA Exclusions — xlsx export: Item ID PLP hyperlinks + Category/Shop left-align + empty-Category "n/a"** (2026-06-30, commit `1da8a69`). User asks: make the export's Item ID column hyperlink to the PLP url, left-align Category + Shop, and fill empty Category with "n/a". All in `export_xlsx()` (`backend/dma_exclusions_router.py`). `plp_url` was already stored per row but absent from the export column map — surfaced it as `cell.hyperlink` + blue underlined font on each Item ID cell (plain text where no url). Alignment + column lookups by label (reorder-safe). Empty Category (`Targets=1`, bestsellers/APlus-only — no resolvable category from GA serving history) → "n/a". Confirmed the empty-Category pattern is expected, not a bug (all 58/733 empty rows have exactly 1 target). Needs manual uvicorn restart to deploy. See LEARNINGS "xlsx export: Item ID hyperlinks…". #claude-session:2026-06-30 #priority:low
- [x] **dm-tools DMA Exclusions — OOS bulk `/by-eans` migration (scan + recovery + re-enable loader)** (2026-06-30, commit `d772355`). The monitor owner (Bram) shipped the bulk endpoint requested 2026-06-29: `POST /api/v1/overrides/by-eans {country,state,eans:[...]}` — ≤1000 EANs/call (422 over), one headline-collapsed row per EAN, uncapped, keeps `beslist_served=False` rows. Integrated: per-EAN `q=` fan-out → chunked bulk fetch (`_oos_by_eans` + rewritten `_oos_offers`, state in cache key) — **~2350 round-trips → 3 calls** (3 EANs cold 0.09s, warm 0.000s); verdict chain untouched (each EAN → 0-or-1-row list). Fixed the `beslist_served=False` fall-through: a contradicted/gone row now → `stale` (kept) instead of a stale ES guess. `oos_recovered` rewritten as ONE `state=problem` bulk pass (keep excluded only if `status==open AND is_cheapest_offer True`; recovered / rival-headline / absent → re-enable — now also re-enables the old "vanished" bucket, the safe direction). Frontend: the **Re-enable recovered** button now shows a phase-aware spinner (`Checking…`→`Re-enabling N…`, locked, restored in `finally`) — user reported it had no loader. Verified all 4 endpoint claims live; round-trip-tested the bulk verdict mapping (Wibra `feed_stock:8` → stale/kept, Oakley → match). Deployed (manual uvicorn restart) after the in-flight re-enable run finished. Only `dma_exclusions_service.py` + `dma-exclusions.html` committed; pre-existing router/redirect-tool work left alone. See LEARNINGS "OOS bulk `/by-eans` migration"; BACKLOG bulk-endpoint item marked SHIPPED. #claude-session:2026-06-30 #priority:high
- [x] **dm-tools DMA Exclusions — OOS exclude done-banner + stop lingering progress bar** (2026-06-29, commit `565fbe8`). Progress-bar teardown was in a `finally` wrapping the whole flow incl. the slow post-exclude `await scanOos()`, so "Excluding X…" + the bar lingered through the re-scan. Moved teardown into a `finally` around just the exclude loop (clears the instant the run ends), and replaced the blocking `alert()` with a dismissible done-banner summarising processed/excluded/skipped/errors (green/amber/blue). See LEARNINGS "OOS exclude progress bar lingered". #claude-session:2026-06-29 #priority:medium

- [x] **dm-tools DMA Exclusions — OOS scan performance + limit-as-matches + caching** (2026-06-29, commits `638a64e`,`7331a9d`,`bcc14bb`,`d8cae1b`,`b2da009`,`79ad135`,`70fe61e`,`1bf9ef8`). User: "Scan OOS loads for a very long time." Profiled the pipeline: GA `shopping_performance_view` ~25s/200-EAN batch (serial → ~20 min full scan) was the bottleneck; OOS-monitor enrichment ~0.24s/EAN (server-bound); ES negligible. Fixes: **GA batches now run in concurrent waves of 6 → 6.4x** (151.8s→23.8s for 6; GA doesn't serialize at the client layer) with a transient-error retry; **scan `limit` now counts headline MATCHES** (was live-in-DMA), default **100** (~one wave, the sweet spot), stops early + trims overshoot, returns `scanned`/`oos_total`; **OOS pool stays 16** (32 gave no speedup — monitor is server-bound) but per-EAN lookup retries so a transient stall doesn't drop the row to stale ES; **stale-crawl caution** (`google_last_update` ≥ 3d, no counter-signal) flags + de-Select-alls + "hide stale crawl" filter; **30-min TTL cache + GA/enrich pipelining** → warm re-scan **212.7s→1.5s (142x)**, identical results. Cold full scan still ~13-15 min (enrichment server-capped) → needs a bulk endpoint (BACKLOG). See LEARNINGS "OOS scan performance". #claude-session:2026-06-29 #priority:high

- [x] **dm-tools SEO stats — WoW deltas in the "Visits & revenue per day" chart tooltip** (2026-06-29, commit `2e83b68`, frontend-only). The per-day chart's custom `externalTooltip` now appends a WoW % delta pill to each metric row (reusing `wowText()`/`wowColor()` — same red→white→green fade + "n/a" — and a "WoW vs. same day last week" sub-caption), sourced from the per-day `${k}_wow` fields already computed in `initTable()` (no backend change). Fix it required: `initTable` only computed `_wow` for `TABLE_COLS` (4 keys), but the chart can plot all 6 `ORDER` metrics → looped the `_wow` computation over `ORDER` so `dma_omzet`/`gsaas_omzet` get a delta too; table still renders only `TABLE_COLS`. `perfRows = lastData.daily.slice()` shares object refs, so the keys are visible to the tooltip even though `renderChart()` runs one line before `initTable()` (tooltip fires on hover). `frontend/seo-stats.html` only → live on refresh. See LEARNINGS "WoW deltas added to the 'Visits & revenue per day' chart tooltip". #claude-session:2026-06-29 #priority:low
- [x] **dm-tools DMA Exclusions — OOS headline verdict from monitor's `is_cheapest_offer` + stale-crawl guards** (2026-06-29, commits `1daca6c`,`437da9f`,`1ccdde5`,`6bd7526`,`cf62cca`,`e8c3e0a`). The OOS monitor now exposes the headline signal directly, so the match/differs decision moved off the stale ES `bestOffer` check onto **`is_cheapest_offer`** (per OOS owner: True == served headline, independent of stock). Per-EAN lookup switched to **`/api/v1/overrides?state=active&q=<ean>`** because the plain `/oos-products` list is capped at 2000 rows AND served-only, so capped-out / no-longer-served EANs were silently falling back to stale ES (which matched an in-stock different-shop offer, e.g. Brekz.nl, and wrongly excluded). Added a `stale` status (kept, never excluded) for cheapest rows whose crawl OOS flag is contradicted by `beslist_served=False`, `feed_stock>0`, or **beslist's product index showing the SAME shop in stock** (`headline_offer` now returns `shop_stock`; `_es_shop_instock` veto, same-shop-scoped so genuine matches like Dreamland survive). ES kept only for `plp_url` + fallback; displayed `headline_shop` now OOS-sourced. `_oos_verdict` is the single decision used by `oos_scan`+`oos_exclude`. Crawl-age rejected as a discriminator (would gut coverage). Residual: a stale flag with no stock data anywhere (e.g. Douglas.nl) still reads match → manual spot-check. UI: amber "stale OOS" badge + count, OOS table widths rebalanced. See LEARNINGS "OOS headline verdict moved from ES `bestOffer`". #claude-session:2026-06-29 #priority:high
- [x] **dm-tools DMA Exclusions — clickable Saved rows show campaigns + OOS Shop/PLP columns** (2026-06-26, commits `27569f3`,`1404576`). Saved-exclusion rows are now clickable → expand a centered table of the Campaign + Ad group each negative was added to, backed by `exclusion_targets()` + `GET /api/dma-exclusions/exclusion/{id}/targets` (reads the persisted `targets` JSONB; works for reverted rows too, no Google Ads call). Enable button `stopPropagation()`s + shows a spinner. OOS table gained Shop + PLP columns (`plp_url` from the ES doc's `plpUrl` in `headline_offer`); OOS source tag is an orange-outlined bold badge; reverted the experimental full-width OOS card. Also confirmed exclusions persist (PostgreSQL `dma_exclusions`) — the "empty list" after the laptop crash was just being off-VPN (`No route to host` to `10.1.32.9` + Redshift). See LEARNINGS "clickable Saved-exclusion rows". #claude-session:2026-06-26 #priority:medium
- [x] **dm-tools DMA Exclusions — OOS headline-offer check + scan limit + button sizing** (2026-06-26). OOS scan now cross-checks each candidate against the product search ES index (`product_search_v4_nl-nl_*`, term on `eans`, zero-padded to 13) and **only excludes when the OOS EAN IS the product's headline (`bestOffer`) offer** — apparel/footwear carry one EAN per size variant and the gold ad rides the headline, so an OOS non-headline variant whose headline is a different in-stock variant/shop was being wrongly excluded (killing a live ad). `headline_offer()` → `match`/`differs`/`no_headline`/`not_found`/`error`; `oos_scan` adds `headline_*` per row + `headline_counts`; `oos_exclude` skips only `differs` server-side; UI gets a Headline column, locked `differs` rows, Select-all = confirmed-match-only, summary breakdown. `stock` field is unreliable (null on ~half of live offers) → use EAN-identity, not stock. Live NL: 975 live → 871 match / 18 kept / 86 unconfirmed. Also added an OOS scan `limit` input next to the country picker (`/oos/scan?limit=N`) and made Preview/Apply `btn-sm`. See LEARNINGS "OOS headline-offer check". #claude-session:2026-06-26 #priority:high
- [x] **dm-tools DMA Exclusions — OOS feed integration + allow-list tree fix** (2026-06-25). Added an "Out-of-stock (OOS) waste" section fed by the GMC crawl-override monitor. Bridge = GTIN → `nl-nl-gold-<gtin>` (product_id_v3 is opaque, doesn't match); EANs from `/api/v1/overrides/oos-eans`. 964/1,633 OOS EANs live in DMA (€2,334/30d) but they still convert (€3,259, ROAS ~1.4) → built as review-and-select (per-row 30d clicks/spend/conv + warning), NOT blanket auto-exclude. New svc fns `oos_scan`/`oos_exclude`/`oos_recovered`/`oos_reenable` + `/api/dma-exclusions/oos/*` + UI section; `source` column ('manual'|'oos') on `dma_exclusions`; re-enable recovered via same API (state=recovered / dropped-off-active). Fixed a core bug it exposed: DMA category trees are block-list OR allow-list (`store_`-format campaigns have CL3-OTHERS NEGATIVE) — now skip negative leaves safely + ad-group-CPC bid fallback. Verified: 59-ad-group OOS product → 2 excluded / 57 skipped / 0 errors, all restored byte-for-byte. Allow-list full coverage deferred (see BACKLOG: serving-leaf walker). See LEARNINGS "OOS feed integration". #claude-session:2026-06-25 #priority:high
- [x] **GSD tag_toppers Q3-2026 refresh — NL + BE + DE ALL DONE** (2026-06-25). Full rebuild of the tag_toppers program across all 3 accounts (~835 shops): per shop, drive the `[label:tag_toppers]` campaign to ONLY the new high-performing item set and exclude those items from the regular campaigns; create PAUSED toppers + exclusion layers for new shops; teardown (strip exclusions + pause) for shops dropped from the new list. **~381 toppers created PAUSED** (NL 189 / BE 169 / DE 23), all labeled **`tag_toppers_bid_strat`** → **user activates in SA360** (set bid-strategy/tracking/budget; items are excluded-but-paused until then). 0 unresolved failures; NL fully verified + BE/DE spot-verified across every bucket. Scripts: `Downloads\Python\scripts_def\tag_toppers\` (rollback `tt_restore_unified.py <backup.jsonl> --live`). NL run was rocky (3 crashes → fixes); BE/DE single clean passes. <25-id threshold considered then dropped. **Latent follow-up (not blocking): shops whose regular ad groups lack a standard INDEX0-label structure are skipped by the exclusion gate (no layer added) — those toppers will double-serve vs regular when activated; revisit if it matters.** See LEARNINGS "GSD tag_toppers Q3-2026 refresh". #claude-session:2026-06-25 #priority:high
- [x] **dm-tools DMA Exclusions tool** (2026-06-25). New Google Ads tool to exclude an individual product (item id) from DMA campaigns and re-enable it per product. `backend/dma_exclusions_service.py` + `_router.py` (`/api/dma-exclusions`: lookup/preview/apply/list/enable/{id}/export/xlsx), `frontend/dma-exclusions.html`, lazy DB table `dma_exclusions` (UNIQUE(item_id,market), JSONB reversal metadata). Resolves bid category from `shopping_performance_view` (segment by `product_item_id`), then targets the category `_a/_b/_c` + `PLA/Amazon bestsellers` + `PLA/APlus`. Two tree ops: convert the biddable CL3-OTHERS / INDEX0=cl0 UNIT→item_id SUBDIVISION (preserve bid) for category+untouched-APlus, or append a negative under the existing item_id subdivision for bestsellers/already-split-APlus; enable removes the negative and collapses back if it was the sole negative. APlus ad group found via one CL0-value-filtered criterion query (not a 1387 scan). GOTCHA: tree reads MUST filter `status != 'REMOVED'` (phantom UNKNOWN/parent-None nodes broke the atomic subdivide). Dry-run Preview→Apply; verified with a byte-for-byte apply→enable round-trip on `nl-nl-gold-6941057404028`. Markets NL+BE, MCC `3011145605`. Nav added across 29 pages + dashboard tile; Export Excel + house-style UI tweaks. See LEARNINGS "DMA Exclusions". #claude-session:2026-06-25 #priority:high
- [x] **dm-tools SEO stats — heatmap + WoW hover on SEO revenue & SEO visits** (2026-06-26, frontend-only). Per-day overview follow-up: (1) SEO-visits column now shows a WoW % `title=` hover (added `seo_visits` to `WOW_BY_KEY`; the `seo_visits_wow` value was already computed in `initTable`); (2) SEO-revenue column now gets the red→white→green diverging heatmap (new `HEAT_KEYS = [...VISIT_KEYS,'seo_omzet']`, used by `visitColScales()` + the cell-render branch instead of `unit==='count'`) plus the matching WoW hover. `frontend/seo-stats.html` only → live on refresh. See LEARNINGS "WoW deltas + period-over-period tiles → Follow-up". #claude-session:2026-06-26 #priority:low
- [x] **dm-tools SEO stats — WoW deltas + period-over-period tiles** (2026-06-26, commit `e2e52d3`, frontend-only). Per-day overview: SEO-visits WoW % column + "Show deltas" toggle adding a WoW % column for every metric (red→white→green fade pill, `wowColor`); DMA/GSAAS value cells get a WoW hover tooltip. Default range now last 7 days. **Fix for "all n/a in 7-day view":** `loadWowBase()` fetches the 7 days before the range as a lookup-only baseline (`wowBase`) merged into `initTable`'s date→row map — the prior-7-day window is exactly the missing `-7` days for any range length. Top tiles show **period-over-period** deltas vs the equal-length prior window (`loadTileDeltas`/`fetchTotals`, caption "vs prev Nd", scales with range; reuses `wowBase` when N=7). Top-categories header got a purple "i" tooltip (`catInfo`) showing the compared dates per metric. "Weekday"→"Day" + width tweaks; Excel export mirrors visible delta columns. Date math uses `shiftDays` local-string arithmetic (not `toISOString`). See LEARNINGS "WoW deltas + period-over-period tiles". #claude-session:2026-06-26 #priority:medium
- [x] **dm-tools SEO stats dashboard (live web Performance Standup)** (2026-06-25). New SEO-tools page `frontend/seo-stats.html` + `backend/seo_stats_service.py`/`_router.py` (`/api/seo-stats`). `/daily` per-channel visits+revenue (SEO/DMA organic/GSAAS, this-month default), `/deltas` channel %-deltas + top maincats/subcats/deepestcats (visits ref-vs-7d, revenue ref-1-vs-8d, anchored on a "Compare day" picker), `/notes` GET/PUT persisted in `pa.seo_stats_notes` (date PK + colour). Chart w/ metric toggles, fixed-width centered per-day table with red→green visits heatmap + editable colored Notes column (6 pastel presets, focus-reveal), Performance-standup section (per-channel tiles + top-3 gaining/declining deepest-cat tables excluding `-`/Beslist.nl/maincat-landings). Added to SEO-tools dropdown across 28 pages + dashboard tile; also reordered Google Ads dropdown (Shop-campaigns above Thema Ads). GOTCHAs: Postgres pool is RealDictCursor (use `r["col"]`); dashboard.html has tiles not a nav dropdown (bulk first-match insert injected a stray link). See LEARNINGS "SEO stats dashboard". #claude-session:2026-06-25 #priority:high

- [x] **dm-tools Shop-campaigns dashboard (SA360 performance of SHOP/ campaigns)** (2026-06-24, commit `0062c3d`). New Google Ads tool tracking per-day clicks/revenue/cost/margin/conversions/CTR/conv-rate/avg-CPC of all `SHOP/*` campaigns from **Search Ads 360** (login `9816507046`, vendored `util_searchads360`). Revenue=`Totaal: Revenue` (cc `29314662`), margin=`Totaal: Profit` (cc `29126930`, same as GSD Budgets marge) — manager-level custom columns that resolve on child accounts. `backend/shop_campaigns_service.py`+`_router.py` (`/api/shop-campaigns`: `/performance` per-day zero-filled, `/inventory` status counts, `/top-performers` range-aggregated ranked-by-revenue); `frontend/shop-campaigns.html`: flatpickr range+presets, summary tiles, multi-series Chart.js trend w/ HTML tooltip, sortable+paginated per-day table, full-width Top campaigns/ad-groups tables (per-page 10/50/100/all + name filter), 3-sheet Excel export. All SHOP/ campaigns paused/no-data as of now → renders zeros until live; query shape verified live on Direct Shopping `7938980174`. Runtime SA360 yaml gitignored; deployed via uvicorn kill+relaunch. Detail in LEARNINGS "Shop-campaigns dashboard". #claude-session:2026-06-24 #priority:high
- [x] **Auto-Redirects V42/V43: model-number matching + gated subcategories + colour combinations** (2026-06-22/23, commits `4df4329`,`3e16e32`,`b96d5dc`). V42: `_coverage_tokens` keeps standalone ≥4-digit model/series numbers, **facet-aware** (number kept only if a candidate facet value has it; off in the maincat-wide pass) → `philips 7000` now adds `productlijn_scheren~…`. 0 redirect regressions on the 13,753-URL `/r/` corpus (the naive `[0-9]` variant had 90). V43 gated subcategories (`GATED_SUBCATEGORIES`): generic brand queries no longer land on accessory subcats — `casio`→Digitale horloges instead of Horlogebandjes (straps); seeded Horlogebandjes + Horloge-onderdelen, allowed on intent token / source-in-subcat. V43 colour combos (`color_combo.py`): live-probe `kleurcombinaties_*` (only surface under a `filters[kleur]` query) + persistent sqlite cache → `servies blauw-wit` → `kleur~…~~kleurcombinaties_woonacc~…`. Detail in LEARNINGS "Auto-Redirects V42/V43 …". #claude-session:2026-06-23 #priority:high
- [x] **Redirect-tool audit fixes (phases 0–2)** (2026-06-22, commit `60a6cba`). Headline = data-loss fix: replace/incoming-rewire were delete-then-post with no rollback (a POST failure after DELETE dropped the redirect) → now restores on failure (live-tested). Plus `from/to` header aliases on file uploads (was 0 rows), strip `?`/`#` in `strip_domain`, lookup-failure ≠ "no redirect" (preflight skips instead of mis-submitting), server-side renorm at submit, partial-rewire→warning, and Phase-0/1 hygiene. 0 redirect regressions on the read-only preflight diff. #claude-session:2026-06-23 #priority:high
- [x] **Canonicals audit fixes (phases 0–2)** (2026-06-23, commit `6be35e2`). XSS fix (Redshift URLs were injected into an inline onclick via a quote-only escaper → delegated handler reads `currentResults`, no URL in any attribute); Redshift LIKE-wildcard escaping with **`ESCAPE '!'`** (a backslash breaks the string literal); YYYYMMDD date validation; generic 5xx instead of leaking SQL; export-excel off the event loop; DB rollback/cursor/`Json` hygiene. `transform_url` byte-identical over 559-URL corpus (0 changed). Detail in LEARNINGS. #claude-session:2026-06-23 #priority:high
- [x] **dm-tools Canonicals → Redirect tool push + run persistence** (2026-06-22, commit `4b9c092`). Select generated canonicals (per-row checkbox + select-all + URL-substring filter) and Push to production through the Redirect tool's existing async preflight (`/preview`) → confirm modal → `/submit`, reusing chain-flatten / already-redirected / replace-existing. Defaults statusCode 200 (canonical) + country nl,be, editable; absolute→relative handled by the Redirect tool's `strip_domain` so no conversion needed. Canonical generations now persist (lazily-created `canonical_runs` table, no migration) with a "Recent results" card (Load restores the table for select+push after refresh / Delete); generate auto-saves + returns run_id. Download CSV → Download Excel (new `POST /api/canonical/export-excel`, real .xlsx via pandas+openpyxl). New endpoints `GET/DELETE /api/canonical/runs[/{id}]`. Also rolled the ↻ Refresh-button arrow out to FAQ's, Kopteksten, Redirect Tool, IndexNow, Index Checker, SEO Prio, SEO Rulings, GSD Campaigns, Thema Ads, and aligned Canonicals button styling to the theme orange/purple. Backend deployed via uvicorn kill+relaunch (no --reload). Detail in LEARNINGS "Canonicals → Redirect tool push + run persistence". #claude-session:2026-06-22 #priority:high
- [x] **dm-tools AI titles: v3 facet-ordering + polish-inflection batch** (2026-06-22, commits `a3f341d`,`e740a6b`,`be5325d`,`861b981`,`61368ea`,`debfd12`,`13eccbe`,`d5ff006`). Eight user-reported H1 fixes in `backend/ai_titles_service.py` (v3 live): leading `op`/`aan`→post-noun (3,177 regen); embedded `met/op/aan` clause peeled out of catch-all values (`_EMBEDDED_PREP_RE`, the Kruidenrekken case); `draadloos opladen` polish over-inflection + casing (`_v3_fix_adverb_before_infinitive`, 40); `btu_units`→append "BTU" (182); `mobiel_k` pinned `position='end'` so phone features trail the `smart-of-classic` noun (1,730); plus two consequences of that move — mixed-case brand cap "IPhone"→"iPhone" (`_v3_capitalize_first`, 439) and trailing predicative adjective "Kleine"→"Klein" (`_v3_fix_trailing_adjective`, 5). All regenerated by facet (the reusable pattern — embedded clauses can't be detected from stored text). Detail in LEARNINGS "v3 facet ordering + polish-inflection batch". #claude-session:2026-06-22 #priority:high
- [x] **Taxonomy: corrected facet value "IPad OS"→"iPad OS" + patched 4 titles** (2026-06-22). Facet value 575098 (`besturingssysteem_tablet`, facetId 3402) via flat-body PUT (the GET's nested `labels` shape 400s — see LEARNINGS). Search-API catalog lags the master, so the 4 affected `pa.unique_titles_content` titles were string-patched directly. beslist-apis skill GET-merge-PUT example corrected. #claude-session:2026-06-22 #priority:medium
- [x] **R-URL optimizer: Fix D facet-append (V44 Phase 1)** (2026-06-24). Fix D (`search_derived_samecat`) emitted bare dominant-category pages even when the Search-API probe had matched the exact facets. Phase 1: enrich the dominant category with probe-matched facets — brand facet when the query names the brand, non-brand facet only when the category name is lexically on-topic (`name_link`). `search_derived_samecat_faceted` score 70; never suppresses. 227/1,416 enriched, 0 removed. Verified live on all 4 user URLs (droogrek→`/c/bevestiging_rekken~19275898~~o_droogrek~23591184`, intex→`/c/merk~85303`, waxinelicht+lichtgewicht unchanged). Dry-run harness `backend/rurl_optimizer_v2/test_fixd_policies.py` + `Downloads\claude\fixd_policy_comparison.xlsx` proved broad suppression (P4, 67%) kills correct redirects → deferred to Phase 2. Subprocess, no restart. Detail in LEARNINGS "Fix D facet-append (V44 Phase 1)". #claude-session:2026-06-24 #priority:high
- [x] **R-URL optimizer: Fix D append rule relaxed to name_link OR all_repr** (2026-06-24, commit `0e82553`). Follow-up to V44 Phase 1: the `name_link`-only non-brand rule left pure-attribute queries on the bare category. Now also appends when every significant query token is represented by the category name or a matched facet (`all_repr`) → `voor mannen` → `gezond_mooi_560582_3219169/c/doelgroep_drogisterij~560636`. Union is a strict superset of the shipped rule: +56 correct facets, 0 lost over 1,416 rows; `all_repr`-only was rejected (too strict, dropped 70 incl. good type facets). Detail in LEARNINGS. #claude-session:2026-06-24 #priority:high

- [x] **R-URL optimizer: 7 structural fixes from user-reported suggestions** (2026-06-19, commits `b8b3428`,`4ff739a`,`d036439`,`b8bd574`). 17 flagged suggestions → RC1 numeric-sibling+clamp, RC2 probe lexical-bridge, RC3 head-noun guard, RC4 qualifier probe, RC5 perfect-match lift, RC6 dimension-dedup + L11 fragment-facet drop, L13 origin-vs-sideways. 16/17 cases fixed; each reproduced + OLD-vs-NEW corpus diff (3k Redshift slice, 0 regressions), 26/26 tests. Also caught wild bugs (1600 watt→"800 Watt", aluminium-overgangsprofiel). L4 deliberately NOT fixed — conflicts with the `_is_semantic_match` keyword-at-start rule (would need a synonym, not a rule change). Full detail in LEARNINGS "R-URL optimizer — 7-fix batch". Optimizer = subprocess, no restart needed. #claude-session:2026-06-19 #priority:high

- [x] **dm-tools AI titles: storage-capacity facet (GB/MB/TB) to title end + opslagcap_mob requeue** (2026-06-18, commit `da4d4d7`). H1 "128 GB Google Pixel 9 Pro XL" → `_SPEC_UNITS_RE` had no storage units, so `is_spec_value("128 GB")` was False and the value bucketed into `other_adj` (before the noun). Added `tb|gb|mb|kb`; verified `_build_v3_h1` → "Google Pixel 9 Pro XL … 128 GB". Requeued ~3,510 `opslagcap_mob` URLs: jobs→pending + `unique_titles_content` rows deleted (status flip alone doesn't re-surface URLs that already have content — frontend hides them). Deployed via uvicorn kill+relaunch (no --reload). Detail in LEARNINGS "storage-capacity facet stranded at title front" + "pending view hides jobs that already have content". #claude-session:2026-06-18 #priority:high
- [x] **dm-tools unique-titles: reset t_droogrek type-facet URLs to pending** (2026-06-18). Confirmed `t_droogrek` is `is_type_facet=true` (order 1132) vs non-type `o_droogrek`. Flipped 480 jobs→pending; UI showed only 5 until the 475 existing `unique_titles_content` rows were deleted (no backup, per user). #claude-session:2026-06-18 #priority:medium
- [x] **Google Ads: built 178 branded SHOP Search campaigns from the blueprint** (2026-06-19). `Downloads\claude\sea_branded_campaigns_def.xlsx` sheet `ads` → 178 campaigns / 4,326 ad groups across 28 client accounts. PAUSED, €10/day budget each, Maximize-conv-value @ tROAS 1.5, Google Search+partners, NL+Dutch, label SHOP_CAMPAIGN; ad groups/keywords/RSAs ENABLED; `_EXACT`→exact, `_MB_PH`→broad+phrase+exact-negatives; Dasty `wibra`→`dasty` keyword variants in 7 Wibra cleaning categories (Search-API checked). Built via google-ads 30 from `dma_script/.venv` (OAuth write creds), validate_only→pilot→full, atomic mutate/campaign, idempotent. All 178 verified live (label + PAUSED + per-account counts match). API gotchas (EU-political field required, `.` illegal in RSA paths→drop TLD, CONCURRENT_MODIFICATION transient) in LEARNINGS + memory `google_ads_campaign_create_gotchas.md`. #claude-session:2026-06-19 #priority:high
- [x] **Google Ads: fixed maincat-keyword contamination + Jaloezieën mojibake in the branded build** (2026-06-19, user-reported). 18 duplicate ad-group rows (same name, deepest-cat + maincat keyword sets) had been MERGED, pulling parent-category keywords (e.g. `aliexpress horloges` in a Smartwatches ad group) into all 18 deepest-cat ad groups. Fix: removed **272 maincat criteria** live across 11 accounts + deleted 36 dup rows from the blueprint `ads` sheet (keep deepest-cat copy only, never merge). Also repaired the `Jaloezieën`→`JaloezieÃ«n` mojibake in 12 blueprint cells AND in the live ad-group names + RSAs (rename + recreate; RSA creatives immutable, remove+create must be separate mutates). Detail in LEARNINGS "maincat-keyword merge bug" + "mojibake". #claude-session:2026-06-19 #priority:high
- [x] **dm-tools AI titles: doelgroep people-noun "voor X" routing (mannenstick fix)** (2026-06-17, commits `cff9c44`, `b8e6a6f`, `76b82f8`). H1 "Mennen Alcoholvrije mannenstick" → v3 builder put `doelgroep_drogisterij` pre-noun and polish agglutinated it. Restored v1's "voor mannen" suffix for `doelgroep_drogisterij` (all 7,195), then generalised to a value-based rule `_V3_PEOPLE_NOUN_AUDIENCE={mannen,vrouwen,volwassenen}` routing those VALUES to "voor X" on any doelgroep facet (Heren/Dames/… stay pre-noun). Requeued: drogisterij 7,195 + mannen/vrouwen 225 + volwassenen 2,126 in `pa.unique_titles_jobs` (title-only; faq/kopteksten untouched). Deployed via manual uvicorn kill+relaunch (no --reload). Detail in LEARNINGS "doelgroep people-noun voor X routing". #claude-session:2026-06-17 #priority:high
- [x] **Redshift type-facet URL gap → bulk-load 42,612 into `pa.urls` + queue 3 pipelines** (2026-06-17). 743 type-facet slugs (`is_type_facet=true`) → anchored regex over `datamart.dim_visit` (real visits, /c/, 365d) → anti-join `pa.urls` via `pa.canonicalize_url` (NL only): 45,925 NL missing. User curated to 42,612; loaded (ON CONFLICT DO NOTHING, tagged `notes='redshift type-facet gap 2026-06-17'`), all new, queued pending in unique_titles/faq/kopteksten jobs. Recipe in LEARNINGS + auto-memory `pa_urls_loading_procedure.md`. #claude-session:2026-06-17 #priority:medium
- [x] **R-URL optimizer: surfaced keyword-branch coverage/generic-facet guard (aluminium 286% fix)** (2026-06-17, commit `e03ba72`). `aluminium-overgangsprofiel_tapijt`→`materiaal~Aluminium` scored 100 ("coverage 286%"): `_check_surfaced` keyword branch waived the `cov<=1.0` + generic-attribute guards the other branches have. Added both to the keyword branch; capped Stage-1.5 coverage at 1.0; `min(100,…)` backstop on `_covpct`. Suite 26/26; subprocess → no restart. Detail in LEARNINGS "surfaced keyword-branch guard". #claude-session:2026-06-17 #priority:high
- [x] **R-URL optimizer: V41 source-facet preservation + facet order + spurious brand/probe (4 user-reported URLs)** (2026-06-16, commit `80f7256`). Four bad suggestions, four root causes, all reproduced on a 5-row CSV and re-verified. **(1)** `wc_papier_aanbieding`→`merk~Paper Dreams` (tier B): the search-derived append path bypassed V39's `brand_match_is_spurious`; new `_spurious_brand_facet()` applied in both append branches drops the brand → bare Toiletpapier, tier B. **(2)** facet order non-canonical (`t_reismand~~dier_…`): append paths prepend `existing_facet` instead of alpha-merging; new `_canonicalize_facet_order()` on the final URL → `dier_…~~t_reismand`. **(3)** `vogelgeluiden`→`ruimte_woonaccessoires` 'Keuken' scored 41: `facet_probe_fallback` uses coverage% as score; new `_keyword_bridges_value()` floors it to 0 when no keyword token bridges the facet value (like borax→'Poeder'=0). **(4)** the two rules the user asked for — `max_30_kg`+`t_reismand` jumped to `type_dierenriemen` 'Halsbanden' dropping the facet (same maincat, so V40's cross-maincat-only guard on the cascade `result` missed it; late `facet_probe_fallback` override rewrote `final_redirect_url` afterward). New **V41 final guard** before the return: source URL pins a `/c/` facet → any final URL that jumps maincat OR drops that facet (`_existing_facet_in_url()`) reverts to `build_category_only` (origin subcat + facet intact). Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `src/reliability_scorer.py`, new `tests/test_facet_preservation.py` (13 tests; suite 26/26). Detail in LEARNINGS "V41 source-facet preservation". #claude-session:2026-06-16 #priority:high

- [x] **R-URL optimizer: V36 cross-maincat last-resort fallback + 20 verified-redirect sample** (2026-06-12/13, commit `8bf6f03`). User case `/r/opvouwbare_wandelstok_anwb/` (no redirect) now → gezond_mooi Wandelstokken `/c/o_hulpmiddelen~19251043` (Opvouwbaar). Any-token ≥95 cross-maincat subcat match when the cascade ends empty; search-verified → tier C, unverified → tier D + long-unmatched-token guard (with vowel-collapse re-check); plus `_is_semantic_match` consonant-doubling plural fix (stok→stokken). Stash-A/B over last production batch: 98/100 identical, 1 rescued, 1 improved, 0 lost; 13 module tests pass. Collection: 50K Redshift R-URLs → 612 candidates → 32 verified → `Downloads\claude\cross_maincat_fallback_verified_20.xlsx`. Detail in LEARNINGS "V36 cross-maincat fallback". #claude-session:2026-06-13 #priority:high
- [x] **SEO week 23 vs 22 revenue drop (−17%) — internal-factor investigation** (2026-06-12). Verdict: purely external (seasonality + WK-shirt spike + one-off orders). Established that seo_prio runs are proposals only (10,709 proposed, 62 live); found the real 2026-05-28 manual batch (446 seoPriority=false flips, ~200 visits/wk impact, mostly justified by 301s); shop-grain cleared Bedden (one €1.1K Emma-sleep order), Sportshirts (Voetbalshop WK spike), Shirts (noise). Deliverables: `seo_week23_vs_week22_conclusion.txt` + slide-deck PDF in `Downloads\claude`. Detail in LEARNINGS "SEO week 23 vs 22 drop". #claude-session:2026-06-12 #priority:high
- [x] **dm-tools repo cleanup + audit phases 5–6 committed** (2026-06-12/13, commits `5ad1e6d`, `2dac5b2`, `31f5e04`, `282448a`). Root one-offs → `scripts/analysis/`, notes → `notes/`, prototypes/google_ads grouped under `scripts/`; load-bearing root files (`themes.py`, symlink, swagger json) left in place; backend verified live after each step. Phases 5–6: faq_service helpers single-sourced into scraper_service (URL helpers, extract_selected_facets, build_product_subject incl. type_productlijn policy), link_validator normalized-href replacement fix; 23 tests pass; uvicorn restarted for phase 5 (subprocess-based optimizer needs no restart). Detail in LEARNINGS "dm-tools repo cleanup + audit phases 5–6". #claude-session:2026-06-13 #priority:medium
- [x] **R-URL optimizer: category-match misfire batch (6 user-reported URLs)** (2026-06-11, commits `bb483f8` + `aba5067`). Every case = a stray/weak keyword token cross-matched a `type_*` value in an unrelated category and pre-empted the correct in-category match. Fixes: synonyms `zonder hoes`/`hoesloos`→`"zonder overtrek"`, `afdekplaat inductiekookplaat`→`"inductie beschermer"`, COMPOUND_DECOMP `antislipmat`→`antislip`; new **step 1c own-subcat compound retry** (before parent/sibling fallback); **defer purely-cross-category step-1 result** gated on `_has_strong_subcat_name_match()` (fixes `rolgordijn_zonder_boren`→Rolgordijnen); `cross_type_rejected_kept_origin` guard; **synonym-aware** `_rescue_long_unmatched_token` (feed `local_match.keyword` when match_type=='synonym'); **V27 stopwords-only now preserves existing `/c/` facet**; `getest` stopword; `api.scrape.do` dropped at input. Verified OLD-vs-NEW over 856 `/r/` URLs: **0 lost**, intended fixes + bonus (douchestang→Douchestangen, ziki_boxershorts→Boxershorts, 3 preserved-facet rows). Detail in LEARNINGS "R-URL optimizer — category-match misfire batch". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `src/synonyms.py`, `src/validation_rules.py`. #claude-session:2026-06-11 #priority:high
- [x] **Redshift: explain `utm_source=dma` URLs showing `marketing_channel='SEO'`** (2026-06-11). `marketing_channel` is derived purely from `(aff_id, channel_id)` (chan_deriv.ref_channel_derivation_stats), independent of the URL. SEO bucket = aff_id 0 / channel_id 4 = ~972K visits, 99.9% on indexed paid PLA `/p/` URLs reached organically (~650K distinct param URLs; user's "15K" was a BI-client cap). Correct derivation, stale utm tag. Takeaways logged in LEARNINGS "Redshift channel derivation" + memory `redshift_channel_derivation.md`. Open SEO follow-up: 301-strip tracking params on PLA landing URLs / GSC URL-Inspection canonical check. #claude-session:2026-06-11 #priority:medium

- [x] **Top-N facet combination blueprints per category** (2026-06-09). `scripts/pagetitles_topn_combinations.py` (param N, default 5): ranks each category's facets by summed SEO visits (Redshift cache), takes top N, generates the blueprint for every non-empty subset (power set, 2^N−1 per category), writes a `top{N}_combinations` sheet (full set) and appends net-new combos to `all_combined` (source col). Ran top-5 (80,390 rows) and top-8 (405,318). **Excel per-sheet limit 1,048,576**: top-10 would be 1,114,950 (over), so capped at top-8 per user. `all_combined` → 539,215 (154,722 base + 46,817 top5 + 337,676 top8 net-new). Removed the stale `dt_all_combined` pivot the user had built. Deliverable `Downloads\claude\tblPageTitles_blueprint_from_urls.xlsx`. Detail in LEARNINGS "Top-N facet combination blueprints per category". #claude-session:2026-06-09 #priority:medium
- [x] **tblPageTitles blueprints built directly from faceted URLs (+ SEO-traffic gap pass + combined sheet)** (2026-06-08, commits `2ed637a` + `0ccec4d` + `426823e`). Two new scripts, both pushed to dm-dashboard. **`scripts/pagetitles_blueprint_from_urls.py`**: clean deterministic title/h1/description blueprints from the faceted `/c/` URL structure (not reverse-templatized copy). Facet order from `pa.facet_position_rules.order_index`; type-facet (`is_type_facet`) is the noun, else `!!sub_category!!` inserted at the type slot (effective order 1700). Fixed templates — title `!!current_query!! <phrase> kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl`, h1 = `<phrase>`, desc `Zoek je <phrase>? … Shop <phrase> met !!DISCOUNT!! korting online! &#10062; beslist.nl`. URLs lowercased before parsing (case-insensitive slug2id, lowercase-sorted canon keys), facet types `unquote`-decoded, `pricemin`/`pricemax` dropped. Dedup per `(cat_id, canon_key)`; skip-set = prior `tblPageTitles_new_from_unique.xlsx` (101,300) ∪ live MySQL `tblPageTitles` NL (142,076) → **1,628 genuinely-new combos** out of 195,538 in `pa.urls` (the old xlsx was "gaps not in tblPageTitles", so the table covers ~95k of the rest). **`scripts/pagetitles_blueprint_from_seo_traffic.py`**: reads the Redshift SEO-traffic faceted URLs from `query.txt` (671,318 rows, SEO channel, Jan-2025→Jun-2026), aggregates visits+revenue per combo, excludes tblPageTitles ∪ xlsx ∪ generated → **10,932 trafficked combos with no blueprint** (€6,823); 33% rows / 84% revenue are `winkel` (shop) combos — user chose to keep them. Then added an **`all_combined`** sheet = the two new sets + all 142,162 existing tblPageTitles NL rows with a `source` column (**154,722 rows**). Deliverable: `Downloads\claude\tblPageTitles_blueprint_from_urls.xlsx` (sheets `new_pagetitles`, `seo_traffic_new`, `all_combined`). Run scripts under `~/.mysql-venv/bin/python` (only venv with pymysql). NOT yet applied to MySQL — `tblPageTitles`/`t_pdm` is read-only, needs a write account. Full detail in LEARNINGS "tblPageTitles blueprints straight from faceted URLs". #claude-session:2026-06-08 #priority:medium
- [x] **R-URL optimizer: enable V34 size facet in redirects by default + wire it into the child-subcat path** (2026-06-06, commit `04b0653`). User asked why `/products/mode/r/nederlands_elftal_shirt_thuis_junior_maat_122-128_(xs)/` redirected to `/c/fanshop~1335065~~ut_voetbalshirt~9134156` without the size facet, then asked to flip `RESCUE_INCLUDE_SIZE` on by default. **Two parts** in `backend/rurl_optimizer_v2/main_parallel_v2.py`: **(1)** Flipped `RESCUE_INCLUDE_SIZE` `False`→`True` in all three spots — module global, `init_worker_v2` default param, and the CLI (now `argparse.BooleanOptionalAction default=True`, so `--no-rescue-include-size` disables per-run). The FastAPI service subprocess (`rurl_optimizer_v2_service.py`) never passes the flag, so it inherits the size-on worker default. **(2)** Added a deterministic size step to `_append_facet_to_subcat_redirect` (the `[child_subcat]` path) — the flag previously only governed the V28 search-derived rescue (~line 1789), but this example URL resolves via the subcategory-name-match path, which has its OWN facet assembler and never read the flag. The fuzzy leftover collector there can't match numeric/short sizes (`122-128`, `XL`), so the new step uses `src/size_tokens.py` (`extract_sizes` + `match_size_value`) against the target subcat's own `maat_*` facet values; appended last, gated on `RESCUE_INCLUDE_SIZE`, skipped if a size axis was already collected. **Gotcha**: `match_size_value` PREFERS letter over numeric when both present — title "maat 122-128 (xs)" picks `maat_mode_bovenkleding~471667` (XS) not `~23811956` (122/128). **Verified** live: URL now → `/products/mode/mode_432360/c/fanshop~1335065~~ut_voetbalshirt~9134156~~maat_mode_bovenkleding~471667`; Search API shows 32 products without size vs 2 with maat=XS (1 with 122/128) — the inherent thin-page tradeoff of pinning a size, now on for every run. No linter configured → `py_compile` only. Full detail in LEARNINGS "R-URL optimizer: V34 size facet on by default". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py` (+48 -12). Pushed to dm-dashboard. #claude-session:2026-06-06 #priority:medium
- [x] **R-URL optimizer: main-pass multi-facet convergence (subtree rescue + enrichment, V32 whole-token fix, TV-resolution synonyms)** (2026-06-03, commit `55f1048`). Category-pinned R-URLs like `/products/mode/mode_432360/r/nike-nederlands-elftal-trainingsshirt/` collapsed to the bare Shirts page instead of `mode_432360_469350` (Sportshirts) with the multi-facet set, because the main pass (`process_url_v2`) preserves hyphens and only matches facets inside the pinned subcat — whereas the global pass splits hyphens and does type-facet→child-subcat discovery (so the maincat-less variant already worked). **Three parts** in `backend/rurl_optimizer_v2/main_parallel_v2.py`: **(1)** `_derive_facets_in_subtree()` + helpers `_split_strip_keyword` / `_is_bare_category_noun` / `_facet_url_parts` — global-pass-style derivation BOUNDED to the anchor's subtree: hyphen-split keyword, drop the bare category noun (whole-token equality — keeps the compound `trainingsshirt` that discovers the child, drops standalone `shirt`), discover the best type facet under the anchor slug, descend to that child subcat, full multi-facet match. **(2)** Wired as a RESCUE not a pre-empt: a step-0 pre-empt regressed ~a dozen real URLs (overrode correct anchored matches like `alcatel_senioren_mobiel`→wrongly Huistelefoons, dropped `illy_koffiebonen_1kg`'s '1 kg'). Runs only when the cascade produced `<=2` facets; ADOPTS only when (a) baseline had 0 facets, or (b) rescue lands in the SAME destination subcat with strictly more facets (pure enrichment). Adoption rule is monotonic-safe; trigger width only affects frequency. **(3)** Tightened V32 `_is_cat_noun` from substring containment (`'shirt' in 'trainingsshirt'` → whole query judged "just the category noun" → bare-category collapse) to whole-token + hyphen-split residual. Plus TV-resolution abbreviation synonyms in `src/synonyms.py` (`uhd`/`4k` → `"4k ultra hd"`, `fhd` → `"full hd"`, `8k` → `"8k ultra hd"`, `hd ready` → `"hd-ready"`) — facet values spell the resolution out while queries use the acronym, and `4k`/`uhd` can't lexically bridge "4K Ultra HD". **Verified**: target → `mode_432360_469350/c/fanshop~1335065~~merk~84748~~type_sportshirts~9253235`; `samsung_55-inch_4k_uhd_tv` → `/c/beeldschermgrootte_inch_lcdtv~4164789~~merk~101373~~televisie_b~19954383`; `/r/shirt/` still collapses (V32); `alcatel`/`illy`/`tapijt_reiniger` unchanged. Full detail in LEARNINGS "main-pass multi-facet convergence via subtree rescue". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py` (+204 -9), `backend/rurl_optimizer_v2/src/synonyms.py` (+20). Pushed to dm-dashboard. Residual follow-ups logged above (gangkast-class wrong-child jumps; full corpus diff owed). #claude-session:2026-06-03 #priority:high
- [x] **R-URL optimizer: global-pass compound-noun routing + 1-char subcat-fragment fix** (2026-06-02, commits `0133a77` + `c898cb2`; docs `008a867`). Main-pass `_append_facet_to_subcat_redirect` tokenized the matched subcat name with `\w+`, so "T-shirts" → {'t','shirts'} and the 1-char 't' substring-absorbed every leftover token containing a 't' (`elftal`, `thuis`) before facet matching — `/products/mode/r/nike_replica_…_nederlands_elftal_thuis_…/` got only merk~Nike. Fix `0133a77`: filter matched-name tokens to `len>=3`. Global pass (`process_global_rurls.py`, fix `c898cb2`): (a) bigram subcat recovery — the global keyword extractor flattens hyphens to spaces, so compound subcats like "TV-meubels" (`tv meubel` scores 84 vs `tv-meubel` 99) were missed; retry adjacent word bigrams rejoined with a hyphen at the HIGH threshold before greedy type-facet discovery drags `meubel`→Kapstokmeubels; (b) strip category-name tokens before in-subcat facet matching (`tv meubel hout`→TV-meubels no longer matches `meubel`→brand/type; removed bogus `shirt`→Fietsshirts on ~17 curacao queries). Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/process_global_rurls.py`. Pushed to dm-dashboard. #claude-session:2026-06-02 #priority:high

- [x] **R-URL optimizer: fix facet drop-out from 1-char subcat fragments + global-pass compound-noun routing** (2026-06-02, commits `0133a77` + `c898cb2`). User reported `/products/mode/r/nike_replica_-_..._nederlands_elftal_thuis_..._junior/` redirecting with only `merk~84748` (Nike), dropping `fanshop~1335065` (Nederlands Elftal) + `ut_voetbalshirt~9134156` (Thuis) despite both existing in the chosen subcat `mode_432360_432464` (T-shirts). **Root cause (main pass)**: `_append_facet_to_subcat_redirect` in `backend/rurl_optimizer_v2/main_parallel_v2.py` tokenizes the matched subcat name with `re.findall(r'\w+', "t-shirts")` → `{'t','shirts'}`; the 1-char `'t'` substring-matches every leftover token containing a 't', so `elftal`/`thuis` were absorbed (treated as covered by the category) before facet matching. **Fix `0133a77`**: filter matched-name tokens to `len >= 3`. Verified the URL now yields `fanshop~1335065~~ut_voetbalshirt~9134156~~merk~84748`. **Then investigated** the maincat-present variant `/products/mode/r/nike-nederlands-elftal-trainingsshirt/`: under-matches because `parser._normalize_keyword` PRESERVES hyphens (the keyword stays one glued token), whereas the maincat-less global pass (`process_global_rurls.extract_keyword_from_global_url`) SPLITS them via `re.sub(r'[-_+/]',' ',kw)`. A localized hyphen-split inside `match_multi_word` was tried and **reverted** — matching improved but `r.keyword` stayed glued so the coverage scorer hit 0%, reliability dropped to 0, and the V28 rescue HARD-REJECTED the row (worse than before). **Global-pass fixes `c898cb2`** (`backend/rurl_optimizer_v2/process_global_rurls.py`): (a) **bigram subcat recovery** — after full-keyword + per-word subcat-name passes, retry adjacent word bigrams rejoined with a hyphen (`tv-meubel`) at the HIGH (≥95) threshold, because the split `tv meubel` scores 84 vs "TV-meubels" while `tv-meubel` scores 99; wins the compound-noun subcat before the greedy type-facet discovery drags `meubel` onto `Kapstokmeubels`. (b) **leftover-token stripping** — once a subcat is derived, strip tokens the category name accounts for (same `_absorbed_by_cat` containment, `len >= 2`) and match only the leftover; empty leftover → bare subcat redirect. Diff vs baseline (54 real + 10 synthetic URLs): `/r/tv-meubel-hout/` Kapstokken → TV-meubels `/c/materiaal~Hout`; `/r/tv-meubel/` → bare TV-meubels; ~17 "curacao … shirt" queries dropped the bogus `Fietsshirts` over-match and now land on a clean `Shirts` page; trainingsshirt global URL keeps its correct 3 facets. Full detail in LEARNINGS section "R-URL optimizer: facet drop-out from 1-char subcat fragments…". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/process_global_rurls.py`. Both pushed to dm-dashboard. #claude-session:2026-06-02 #priority:high

- [x] **DM Review tool — slide 2 refresh end-to-end** (2026-05-28). New tool `/api/dm-review` (button at `/static/dm-review.html`) that does six things in one click: (1) UPSERTs the last 3 months of monthly visits/omzet + last 60 days of daily visits/omzet for SEO + DMA organic into `review_dm_seo.xlsx` tabs `visits_omzet` and `visits_omzet_dag` (Redshift `fct_visits` + `dim_visit` + `chan_deriv`; omzet = `cpc + ww + affiliate` to match Performance Standup). (2) UPSERTs last 60 days of impression-weighted SERP avg_position per device into `serp_device` (`bt.search_console` country=`nld`). (3) For the wide `serp` tab, inserts (or in-place updates) one new month column for the current yyyymm with avg_position per URL type (Cat-url / C-url / PLP / R-url), shifts the Delta column right, and recomputes Delta as month-over-month % change. (4) Extends each pivot table's `cacheSource.worksheetSource.ref` to cover the newly-appended rows and sets `refreshOnLoad=True`. (5) Slides each rolling-window pivot filter forward — counts current visible items, picks the N most-recent dates from the cache, flips item `h` flags so the newest is visible and the oldest drops off. (6) Updates pptx tables on slide 2: Tabel 13 (SERP, prev_month / last_month / Delta) from the `serp` tab, Tabel 25 (Visits target/behaald) + Tabel 27 (Revenue target/behaald) from `seo_targets.xlsx` sheet `2026` (row 8 visits / row 6 omzet, col C=Jan) against SEO actuals from `visits_omzet`. Files: `backend/dm_review_service.py` (~400 lines), `backend/dm_review_router.py`, `backend/dm_review_pptx_tables.py`, `frontend/dm-review.html`, plus DM Review entry added to the SEO-tools dropdown on all 27 frontend pages and a tile on `dashboard.html`. SEO-tools dropdown reordered alphabetically across all pages + dashboard. The Excel charts in `DM review_NEW.pptx` are OLE-linked to the pivot output cells, so chart values refresh via PowerPoint's "Update Links" (with one caveat: PowerPoint's Refresh Data doesn't trigger Excel pivot recompute on its own — user needs to open the xlsx once between running the tool and opening the pptx). New tool `/api/dm-review` (button at `/static/dm-review.html`) that does six things in one click: (1) UPSERTs the last 3 months of monthly visits/omzet + last 60 days of daily visits/omzet for SEO + DMA organic into `review_dm_seo.xlsx` tabs `visits_omzet` and `visits_omzet_dag` (Redshift `fct_visits` + `dim_visit` + `chan_deriv`; omzet = `cpc + ww + affiliate` to match Performance Standup). (2) UPSERTs last 60 days of impression-weighted SERP avg_position per device into `serp_device` (`bt.search_console` country=`nld`). (3) For the wide `serp` tab, inserts (or in-place updates) one new month column for the current yyyymm with avg_position per URL type (Cat-url / C-url / PLP / R-url), shifts the Delta column right, and recomputes Delta as month-over-month % change. (4) Extends each pivot table's `cacheSource.worksheetSource.ref` to cover the newly-appended rows and sets `refreshOnLoad=True`. (5) Slides each rolling-window pivot filter forward — counts current visible items, picks the N most-recent dates from the cache, flips item `h` flags so the newest is visible and the oldest drops off. (6) Updates pptx tables on slide 2: Tabel 13 (SERP, prev_month / last_month / Delta) from the `serp` tab, Tabel 25 (Visits target/behaald) + Tabel 27 (Revenue target/behaald) from `seo_targets.xlsx` sheet `2026` (row 8 visits / row 6 omzet, col C=Jan) against SEO actuals from `visits_omzet`. Files: `backend/dm_review_service.py` (~400 lines), `backend/dm_review_router.py`, `backend/dm_review_pptx_tables.py`, `frontend/dm-review.html`, plus DM Review entry added to the SEO-tools dropdown on all 27 frontend pages and a tile on `dashboard.html`. SEO-tools dropdown reordered alphabetically across all pages + dashboard. The Excel charts in `DM review_NEW.pptx` are OLE-linked to the pivot output cells, so chart values refresh via PowerPoint's "Update Links" (with one caveat: PowerPoint's Refresh Data doesn't trigger Excel pivot recompute on its own — user needs to open the xlsx once between running the tool and opening the pptx).

- [x] **Redirect Tool encoding fallback + preview warning** (2026-05-28). Windows-exported redirect CSVs are cp1252; the old parser defaulted to utf-8 and threw `Could not parse file: 'utf-8' codec can't decode byte 0xeb in position 4447` on the first `ë`. New `_read_csv_any_encoding` in `backend/redirect_tool_router.py` tries `utf-8-sig` → `cp1252` → `latin-1`. When the fallback kicks in, the response includes a `warning` field and the frontend (`frontend/redirect-tool.html`) shows a yellow alert above the preview rows so the user can eyeball any non-ASCII rows for mojibake before submitting. Tested on `red_wb.csv` (6,786 rows, 19 rows with `ë`/`ï` characters) — all decoded correctly.

- [x] **R-URL optimizer: V32 cross-depth brand/shop facet rescue in `build_multi_facet`** (2026-06-03, commit `a361498`). User asked why Auto-Redirects suggested the bare `/products/mode/mode_432360_432464/c/fanshop~1335065` for `/products/mode/r/nike_nederlands_elftal_uitshirt_2020-2022/` without appending the Nike merk facet. Root cause: `FacetFilter` keeps one representative row per facet value at whatever depth `CHILD_DOMINANCE_THRESHOLD=0.7` settles on — fanshop "Nederlands Elftal" (concentrated) resolves to the leaf `mode_432360_432464`, merk "Nike" (smeared across dozens of subcats) parks at the shallow parent `mode_432360`. `build_multi_facet`'s `same_target_matches` loop only appends matches whose facet-URL path equals the primary's `category_path`, so Nike was dropped despite genuinely existing under the leaf (3416 products; combined page returns 4 live products). The matcher itself returned BOTH facets at score 100 (`require_type_for_merk` was a red herring) — the drop was purely in the URL builder. This is the cross-*depth* case the 2026-05-13 "attach all same-target matches" fix didn't cover (that handled same-depth same-target like `nike_schoenen_dames`). Fix: `UrlBuilder` gains optional `facet_url_exists` checker; `build_multi_facet` rescues a `merk`/`winkel`-axis facet (new const `_CROSS_DEPTH_RESCUE_AXES`) after verifying `f"{category_path}/c/{url_fragment}"` exists in `FacetFilter.facet_url_set()` (cached frozenset) — so it never fabricates a dead-end page. Wired in both `init_worker_v2` (`main_parallel_v2.py`) and `init_worker` (`process_global_rurls.py`); `None` checker → legacy drop, so unit tests/standalone callers unaffected. Gated to merk/winkel only because type/colour facets are genuinely subcat-specific (a depth mismatch there is real intent, not a dedup artefact); cross-maincat V16/V26 blocks still fire first. New test `backend/rurl_optimizer_v2/tests/test_cross_depth_rescue.py` (first test in this package — 4 pure tests, also runnable without pytest via `__main__`); `pytest` installed into the venv (was missing). No linter configured for the project, so lint == `py_compile`. Verified `nike schoenen dames` + `samsung televisie` now also carry the brand; `zwarte jurk` (single facet) unchanged. Full LEARNINGS section "R-URL optimizer: V32 cross-depth brand/shop facet rescue". Files: `backend/rurl_optimizer_v2/src/url_builder.py`, `backend/rurl_optimizer_v2/src/facet_filter.py`, `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/process_global_rurls.py`, `backend/rurl_optimizer_v2/tests/test_cross_depth_rescue.py` (new). 5 files, +227 −4 lines. #claude-session:2026-06-03 #priority:high

- [x] **Unique titles: per-facet position rules + type-facet table + inline LLM classifier** (2026-05-20, commit `78cd3ca`). Catalyst was the user's grammar audit of `pa.unique_titles_content` (960k rows, ~80k flagged — `Kussenhoezen leren` style ordering bugs + `vidaXL Bruine kunststof zelfklevende woonkamer` style category-missing bugs from type-facet replacement). Shipped as one new config table `pa.facet_position_rules` (facet_slug + order_index + is_type_facet + optional position pin + scope_category + source) seeded from the user's curated `facet_order.xlsx` — 2,284 slugs with global numeric order 1..2284 + boolean replace-category flag, 690 marked is_type_facet. Coexists with legacy `pa.facet_type_classifications` as the override layer: new slug-keyed table wins, falls through to legacy per-(facet_name, category) classifier when slug isn't present. `backend/ai_titles_service.py` (+419 -23 lines) adds helpers `_load_facet_position_rules` (60s TTL cache, defensive against both regular and `RealDictCursor` shapes), `_type_facet_override_by_slug`, `_ordered_facet_values`, `_facet_position_clause`, plus the inline LLM classifier trio (`_classify_unrulled_facets_inline` + `_persist_classified_facets` + `_classify_and_persist_unrulled`). Both `generate_title_from_api` and `generate_title_v3` refactored their `has_category_override` chain into a nested `_is_type_facet_for(f)` helper that consults the table FIRST. `_build_v3_h1` adds a `post_category` bucket placed right after the category noun (for facets with `position='end'` pin) and decorate-sorts `other_adj` by `order_index`. `_build_polish_prompt` accepts a `position_rules_clause` kwarg appended after the rules section in both v1 and v2 templates. Inline classifier anchored on hardcoded order scale (`merk=3, kleur=22, doelgroep_mode=400, materiaal=600, stijl=900, vorm=1200, eigenschappen=1500, thema=1900, formaat=2145, maat=2300, conditie=2400`) so verdicts cluster on known buckets; `ON CONFLICT DO NOTHING` for parallel-safe inserts; merges into in-memory cache. Migration `migrations/2026-05-19-facet-position-rules.sql` creates the table (note: Postgres rejects expressions in PRIMARY KEY, so uniqueness via UNIQUE INDEX on `(facet_slug, COALESCE(scope_category, ''))`). Discovery script `scripts/list_unrulled_facets.py` lists slugs present in pa.urls but absent from rules — read-only audit surface. Bulk-classified 249 missing slugs in one ~3min LLM call (batched by category since most pa.urls rows have null `deepest_subcat_name`); auto-fixed 114 type-facet verdicts that the LLM placed at low orders (3/22/400) by bumping to 1700 (type-facets belong near the productnoun, not at the brand position). User reviewed 117 LLM-classified rows via Excel (`is_type_facet_new` column in `llm_classified_facets_review_v5_20260520.csv`) — applied 74 flips (1 false→true `type_r`, 73 true→false for series/audience/dimension/style facets). Reset 60,907 URLs total (74 flipped slugs across all categories + 334 `/schoenen/+maat` URLs with pre-existing range-size duplicate pattern `19 snowboots Maat 18 Maat 18/19`) by backing up h1_title to new column `unique_titles_content.h1_title_prev_20260520` and nulling current h1 — necessary because the worker's `get_unprocessed_urls` gate is `c.h1_title IS NULL OR c.title IS NULL`, not just `j.status='pending'`. **Caught and fixed mid-test bug**: production-testing surfaced duplicate-size titles like `Nike Air Max Plus Kinder schoenen 34 Maat 34` — `_facet_position_clause` was being called with full `selected_facets` (still containing Maat with `detail_value='Maat 34'`), AI inserted "34" mid-title, post-AI `size_values` appended "Maat 34" again → dup. Fix: pass `non_size_facets` instead. **uvicorn `--reload` does NOT propagate to long-running worker threads** (they hold closure refs to old function objects) — had to kill uvicorn + restart so the worker re-imports. Also marked 98 non-product URLs as permanently `failed` across 3 job tables (`unique_titles_jobs`, `kopteksten_jobs`, `faq_jobs`) — landing pages, account pages, `/x/` junk; productsearch API can't fetch them. `is_active` on pa.urls is NOT consulted in code (grep-confirmed) — flipping it is purely audit; real control is the JOB status. Kill switches: env `DISABLE_FACET_POSITION_RULES=1` short-circuits loader, `DISABLE_FACET_INLINE_CLASSIFY=1` disables auto-classification, `TRUNCATE pa.facet_position_rules` is soft data revert, `backend/ai_titles_service.py.bak.2026-05-19_pre_facet_position_rules` (on disk, intentionally not committed) is hard code revert. Full LEARNINGS section "Unique titles: per-facet position rules + type-facet table + inline LLM classifier". Files: `backend/ai_titles_service.py`, `migrations/2026-05-19-facet-position-rules.sql` (new), `scripts/list_unrulled_facets.py` (new). 3 files, +538 -23 lines. #claude-session:2026-05-20 #priority:high

- [x] **R-URL optimizer: V31 chain — diagnosed six wrong redirects, tightened matcher precedence, added count-aware dedup + hyphen preservation + Dutch suffix decomposer** (2026-05-20). Six user-surfaced wrong-redirect cases in one session, each a different failure mode. Diagnostic pattern: pull cached row from `rurl_processed` (Postgres in n8n-vector-db); `match_type` tells you the code path, `reason` carries the per-pass trace (`[maincat]`, `[child_subcat]`, `[V28 compound:'X']`, `[V31] appended ...`). Eleven fixes shipped as one chain in commit `b68e3d5` (rebased to `f589d8b`), 9 files +426 -41 lines. **(1) Count-aware dedup** in `src/matcher.py` + `src/facet_filter.py`. `match()` / `match_with_partial()` dict-comprehension dedups now keep the highest-count instance per normalized key. `_deduplicate_to_highest_level()` rewritten: pick count-leader globally (tie-broken by shallower depth), then descendant-promotion via `CHILD_DOMINANCE_THRESHOLD = 0.7`, then ancestor-fallback if leader < 70% of its closest ancestor. New `_is_strict_descendant(child, parent)` helper guards numeric-id-prefix false positives (`x_5745190` is NOT a descendant of `x_574519`). Critical because the Search API's facets.csv often DOESN'T contain a parent row when its children are listed (no Snoep row for Ferrero Rocher despite Bonbons + Chocolade rows). **(2) V31 guard against search-derived override of clean in-subcat matches** in `main_parallel_v2.py`. V27 zeros the reliability score whenever any long unmatched token (≥8 chars) exists; that then triggers the search-derived rescue path, which can overwrite a perfectly good multi-facet match with a different-subcat guess. Pre-check before rescue: if matcher's result has `r.success && r.facet_count ≥ 1 && r.subcategory_id == parsed.subcategory_id` and search-derived's `dom_cat_url_slug` resolves to a different subcat, restore tier-C (60) and keep the matcher's URL. Fixes Q2 (Kussenboxen + Waterdicht) and Q3 (Parasols multi-facet Met voet + Zweefparasols). **(3) `DOMINANCE_THRESHOLD: 0.60 → 0.75`** in `src/search_derived.py`. At 60% the "dominant category" claim is noise — `/r/elektrische_sigaretten/` landed on Kapperstassen at 60% via incidental product-description text overlap. Now falls back to maincat page (tier D, safe) instead of guessing. **(4) GENERIC_NOUNS + V27 cross-cat generic-only rejection** in `src/validation_rules.py` + `src/reliability_scorer.py`. New 12-entry `GENERIC_NOUNS` set (`meubel`, `set`, `kast`, `huis`, `tafel`, etc.) — sibling to GENERIC_ADJECTIVES for common compound-category roots. `_v27_reject_reason` now takes `match_type` and hard-rejects `cross_category_type` matches where every matched token is in GENERIC_ADJECTIVES ∪ GENERIC_NOUNS. All callers (`reliability_scorer.calculate_reliability_score`, `main_parallel_v2._evaluate_url_result_v2`, `process_global_rurls.process_global_url`) pass match_type through. **(5) Hyphen preservation** in `src/parser.py:_normalize_keyword` and `src/matcher.py:_normalize`. Both used `.replace('-', ' ')`, tokenizing `tv-meubel` to `['tv', 'meubel']` — the `meubel` substring then fuzzy-matched `Kapstokmeubels` cross-maincat. Hyphens are publisher-intended compound boundaries (`tv-meubel`, `e-bike`, `TP-Link`, `A-DATA`, `Bébé-jou`). Tested against 237 hyphenated URL slugs + 1181 hyphenated facet value names — both sides normalize identically. **(6) V14 cross-category per-word fallback** for non-generic tokens in `main_parallel_v2.py`. V30 had disabled per-word cross-cat fallback due to false positives — but those FPs were exactly what GENERIC_NOUNS now filters. Re-enabled strictly: tokens ≥6 chars, not in STOPWORDS/SHOP_NAMES/GENERIC_ADJECTIVES/GENERIC_NOUNS, score ≥95. Longest-first iteration. Lands `/r/tv-meubel_set/` on TV-meubels (score 99, tier B). **(7) Pass-4 [maincat] generic-only rejection** in `main_parallel_v2.py`. New `_maincat_match_is_generic_only(match_results)` rejects matches where every kept facet matches a generic keyword token, letting V14 cross-cat have a turn. **(8) GENERIC_ADJECTIVES skip in V31 leftover-token merk-append** in `main_parallel_v2.py`. The `keyword_words` loop around line 1202 now filters GENERIC_ADJECTIVES alongside STOPWORDS/SHOP_NAMES. Fixes Q4 (`/r/mini_airco_voor_caravan/` no longer appends spurious `merk~Evolar` because `mini` correlated 80% with Evolar by chance). **(9) New stopwords** in `src/validation_rules.py`: `consumentenbond` + `koop` (marketing/review labels — fixes the consumentenbond pathology where the matcher fell through to search-derived and got 100% Polydaun coverage from product-copy text matches); `combi` + `combo` + `multi` (product-type modifiers — token-coverage scorer drops sharply when these are unmatched). Safety check ran first: only 7 facet values contain `combi` verbatim (`Combi-asbakken`, `Combi stoomoven`, `Combi` as `o_schaats` value), all still match via non-`combi` tokens (verified `combi_stoomoven` still resolves correctly). **(10) Dutch compound suffix decomposer** in `src/synonyms.py`. New `DUTCH_COMPOUND_SUFFIXES` set (24 entries: droger, machine, kast, meubel, stoel, tafel, etc.) + `_suffix_split()` helper + extended `expand_compounds()`. When a token isn't in `COMPOUND_DECOMPOSITIONS` and doesn't contain a hyphen (already-intended boundary), check if it ends with a known noun suffix (≥4 chars) and has a prefix ≥3 chars. Emit BOTH `'prefix suffix'` (full split) AND suffix-only (drops prefix). The suffix-only variant matters because the token-coverage scorer drops sharply when extra prefix fragments appear — for `'combi wasmachine wasdroger'` the full split has 2/4 = 50% coverage against `'Wasmachine en droger kasten'` (below threshold), but the suffix-only `'combi wasmachine droger'` has 2/3 = 67% and matches at ~85. Hyphen-skip is load-bearing: without it `tv-meubel` would suffix-split to `'tv- meubel'` and the cross-cat per-word fallback wouldn't find TV-meubels. **(11) V28 retry rewrite** in `main_parallel_v2.py`. Old V28 used only `match_with_partial` against subcat facets (treats variant as one phrase, misses multi-word cases) and broke on the first match — even when that match was cross-maincat. Now (a) tries `match_multi_word` against subcat facets FIRST per variant, (b) filters cross-maincat hits in both subcat and maincat loops via `[mr for mr in (results or []) if not getattr(mr, 'cross_category_path', None)]`, letting the loop continue to a same-maincat variant. Fixes the user's target URL `/r/combi_wasmachine/wasdroger/` → `/meubilair_389371_395590/c/t_badkast~23813977` (Badkamerkasten + Wasmachine en droger kasten, tier C). **Verified end-to-end**: TARGET + Q1 (`/elektrische_sigaretten/` → maincat fallback tier D) + Q2 + Q3 + Q4 + Q5 (`tv-meubel_set` → TV-meubels tier B) + regression cases (Ferrero Rocher → Bonbons tier A; `combi_stoomoven` still works; consumentenbond V27 short-circuit still works). Full LEARNINGS section "R-URL optimizer: V31 chain". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/process_global_rurls.py`, `backend/rurl_optimizer_v2/src/facet_filter.py`, `backend/rurl_optimizer_v2/src/matcher.py`, `backend/rurl_optimizer_v2/src/parser.py`, `backend/rurl_optimizer_v2/src/reliability_scorer.py`, `backend/rurl_optimizer_v2/src/search_derived.py`, `backend/rurl_optimizer_v2/src/synonyms.py`, `backend/rurl_optimizer_v2/src/validation_rules.py`. #claude-session:2026-05-20 #priority:high

- [x] **Redirect Tool — new dm-tools page that bulk-uploads redirects to `redirect.api.beslist.nl` with chain-flatten + run history** (2026-05-19, commit `3cf3447`). New "Redirect Tool" under the Redirects menu + Dashboard tile. Takes a CSV/Excel/paste of `old, new, statuscode, country, label` rows, preflights each one against the live redirect API, rewrites any `new` value that's already a `fromUrl` in the DB (so the POST doesn't 500 on the `url_redirect.url_UNIQUE` constraint), and submits the survivors. Run history persisted in `redirect_tool_runs` table in n8n-vector-db (id, created_at, label, input_method, total_rows, flattened, skipped_home, success, failed, results JSONB) plus a created_at DESC index, exportable to Excel via openpyxl. **Backend** at `backend/redirect_tool_router.py` + `backend/redirect_tool_service.py` — 9 routes mounted at `/api/redirect-tool`: `parse-file` / `parse-text` (CSV/XLSX/TSV/TXT or pasted text, normalizes header aliases from/to/fromUrl/tourl), `preview` (per-row chain-flatten preflight), `submit` (POSTs one row at a time so per-row pass/fail is captured even though the API accepts arrays), `check-url` (single-URL lookup returning BOTH outgoing resolver hit AND incoming rows with this URL as toUrl, paged via `urlContains=`), `runs` / `runs/{id}` / `runs/{id}/export` / DELETE `runs/{id}`. **URL normalization** treats literal-space / underscore / `%20` as equivalent — `url_variants()` generates the three forms, `check_url_is_fromUrl()` tries each, `equiv_key()` drives self-redirect detection. `strip_domain()` accepts full URLs, bare hostnames, or paths; returns /-prefixed path. **Homepage safety**: `is_homepage()` hard-rejects `/`, `/index`, `/index.html`, empty, or any URL that resolves to those after `strip_domain` (so POST'ing `https://www.beslist.nl` is caught; matrix verified including bare hostname). Skipped rows are flagged with `skip_reason='old URL is the homepage (safety block)'` and never POSTed. **Frontend** at `frontend/redirect-tool.html` matches Auto-Redirects house style: card headers as h5, "i" tooltip uses the purple SVG glyph from `seo-prio.html`, input source as two `form-check-inline` radios (File upload / Manual input), file picker `col-md-6` + `form-label` + `form-control`, textarea placeholder `"old, new, statuscode, country, label"`, Upload button right-aligned. Preview table colors rows red on skip / yellow on flatten with a "flattened from `<X>`" badge. Recent results uses `table-light` thead + `history-row` rows + badge for source + purple-outlined View/Export + red-outlined Delete (lifted to reusable `.btn-purple-action` / `.btn-red-outline` classes), purple-outline Refresh matching Auto-Redirects Export-all. Country picker omitted — 500/500 sampled rows in the redirect store have `country='nl, be'`, so the field is cosmetic. **Nav integration**: Redirect Tool entry added to the Redirects dropdown across 20 static pages + Dashboard tile. Swapped Auto-Redirects' nav glyph from arrow-swap to a lightning bolt (zap) to emphasise "automatic"; the old arrow-swap moved to the new Redirect Tool. Dashboard tile icon swapped to match. **Empirical findings on the redirect API itself** (full section in cc1/LEARNINGS.md → "Redirect API behavior — redirect.api.beslist.nl"): the `url_redirect.url_UNIQUE` constraint blocks BOTH chain inserts AND inserts whose toUrl already appears anywhere in the table (a URL can't be both sides at once, AND some "already-targeted" URLs also reject); chain-flattening catches case 1 but not case 2 — the tool surfaces these API errors per-row in Recent results. Resolver responses are Varnish-cached for 1h; list endpoint is not. The writer node sometimes returns `MySQL --read-only` 500s during failover — tool handles per-row. No PUT endpoint — mutations are delete-then-recreate. **`country` is a CSV string** (`'nl, be'`), not an enum — `?country=nl` exact filter matches almost nothing, use `urlContains` instead. **`toUrl` is NOT unique** — many rows share the same target. Files: `backend/redirect_tool_router.py`, `backend/redirect_tool_service.py`, `frontend/redirect-tool.html`, `frontend/dashboard.html` (tile), 20× `frontend/*.html` (nav entry), DB migration via psycopg2 against existing DATABASE_URL. Commit `3cf3447`, pushed to dm-dashboard. **Other-session follow-up** (separate commit, not in this entry): background-task polling refactor adding `/preview-status/{task_id}` + `/submit-status/{task_id}` endpoints + ThreadPoolExecutor + `replace_existing` flag — frontend now drives a progress bar instead of waiting on the long POST. #claude-session:2026-05-19 #priority:high

- [x] **R-URL optimizer: V31 facet-probe path appends covering facet on niche queries** (2026-05-19). User ran Auto-Redirects on `/products/huis_tuin/r/hoesloze_dekbedden/` and got the bare Dekbedden subcat (`huis_tuin_505062_505149/`); the better answer is the same subcat with `/c/eigenschap_beddengoed~23812125` ("Zonder overtrek" — synonymous with "hoesloos" but shares zero tokens, so no lexical or synonym path the matcher can use bridges it). The V29 facet_probe layer was built for exactly this signal but wasn't wired up. Four-piece fix, all in commit `e21bf0b`: **(1)** `backend/rurl_optimizer_v2_service.py` — append `--enable-facet-probe` to the engine argv unconditionally so the V29 prefetch runs during dashboard runs (without it, `derive_search_facet` reads an empty cache). **(2)** `backend/rurl_optimizer_v2/main_parallel_v2.py` — new V31 leftover-token consumer after the V28 rescue elif. Fires when `reliability_score >= 50`, a redirect exists, and the matcher's chosen subcat slug equals `derived.dom_cat_url_slug` (otherwise the facet may not live in the matcher's subcat → 404). Computes leftover tokens **locally** against `(redirect_cat_name + facet_value_names + redirect_url)` with `rstrip('e').rstrip('s')` for Dutch plurals — `unmatched_keywords` is unreliable because `TRUSTED_MATCH_TYPES` (synonym, token_coverage, subcategory_name) marks every token as matched even when zero lexical overlap exists. Append preserves any existing `/c/` via `~~` join and skips when the probe's axis already appears there. Sets `match_type = <prior>_with_probe_facet` + appends a `[V31] appended ... for leftover token(s): ...` segment to reason for export traceability. **(3)** `backend/rurl_optimizer_v2/src/search_derived.py` — `_classify` no longer short-circuits on `total >= AND_MODE_TOTAL_THRESHOLD`. The search API switches to OR-fallback when AND-match is smaller than `limit` (50), inflating `total` to whole-cat OR count (6.9M for huis_tuin), but `categories[]` still carries per-category AND counts, so we extract `dom_cat_id/name/url_slug/count` from there in both modes. Mode still reported as `"fallback"` vs `"and"` so callers tell apart; `_build_redirect_url` + `prefetch_facet_probes` + `facet_probe._do_probe` updated to accept either. Introduced `SCHEMA_VERSION=2` stamped into every cached payload; `_cache_get` returns None for entries lacking it, forcing re-fetch of legacy `{mode: fallback, total: N}` rows that never carried a dom_cat. **(4)** `backend/rurl_optimizer_v2/src/facet_probe.py` — `_do_probe` uses `dom_cat_count` as `base_total` (falls back to `total` only when absent) since in fallback mode `total` is the inflated OR count → 12/6.9M ~= 0 for genuine winners. `_probe_one` rejects `cov > 1.0`: impossible for a real AND-restricted subset, so it's the OR-fallback signal in filter probes; without it, non-covering facets like `materiaal=Katoen` came back with bogus coverage 1345.3 and beat the real winner. `MAX_CANDIDATES_PER_PAIR` raised 15→50 because the actually-covering facet value often sits deep in the count-sorted tail (Zonder overtrek ranks ~#32 of 190 because catch-all values like `kleur=Wit count=5248` dominate the head). `EARLY_STOP_COVERAGE=0.9` short-circuits once a clear winner shows up so easy cases don't pay the 3x cost. **Verified end-to-end** on the user's URL: engine output csv row shows `redirect_url: .../huis_tuin_505062_505149/c/eigenschap_beddengoed~23812125`, `match_type: subcategory_name_with_probe_facet`, score 95, reason includes `[V31] appended ... ('Zonder overtrek', coverage 70%) for leftover token(s): hoesloze`. **Latent gotcha**: invoking the engine outside its venv (system python, no `fuzzywuzzy`) makes the spawn-pool workers crash silently and the parent hang at 0.2% CPU forever; always use `dm-tools/venv/bin/python`. Full debugging detail in `cc1/LEARNINGS.md` → "R-URL optimizer: V31 facet-probe path" section. Files: `backend/rurl_optimizer_v2_service.py`, `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/src/search_derived.py`, `backend/rurl_optimizer_v2/src/facet_probe.py`. Commit `e21bf0b`, pushed to dm-dashboard. #claude-session:2026-05-19 #priority:high

- [x] **R-URL optimizer: hyphenated maincats fixed + richer leftover-token facet matching + build_multi_facet now attaches all same-target matches** (2026-05-13). Six fixes in `backend/rurl_optimizer_v2/`, shipped in two commits: `495a280` (rurl-optimizer changes across `main_parallel_v2.py` + `process_global_rurls.py` + `src/matcher.py` + `src/url_builder.py`) and `56ff81f` (url-checker copy fix in `frontend/url-checker.html`). **(1) Hyphenated maincats** (`sport_outdoor_vrije-tijd`, `films-series`, `boeken-*` — ~240 affected subcats) were producing `/products/{url_name}/` URLs without the maincat segment because `re.match(r'^([a-z_]+?)_\d+', url_name)` in two places used `[a-z_]` which excludes `-`. Replaced both regexes (`matcher.py:1178`, `process_global_rurls.py:435`) with the split-on-`_`-until-digit logic mirroring `parser.py`. **(2) Maincat-path validator** added to the end of `process_url_v2` as defense in depth: detects redirect URLs whose second path segment contains a numeric id (the malformed pattern), infers the missing maincat from the slug, repairs in-place with a `; repaired missing maincat segment 'X/'` reason; suppresses + flags for review if no maincat can be inferred. **(3) Strict-exact merk match from leftover tokens** in `_append_facet_to_subcat_redirect`: removed the over-conservative "merk from leftover is too risky" guard; added a per-token merk pass that accepts score=100 exact matches only. `bic_aanstekers_50_st` now correctly emits `.../aanstekers/c/merk~BIC`. **(4) Specificity rescue for V14.1 winners**: new `_maybe_promote_to_specific_subcat` helper. When the per-word subcat-name match picks a broad parent (e.g. `klussen_486173` Gereedschap) whose facets can't absorb any leftover token, scan deeper same-maincat siblings whose first display word shares a 4+ char prefix with the matched word; swap to the sibling whose facets DO absorb the leftover. `gereedschap_trolley` now lands on `Gereedschapskoffers` (`klussen_486172_1348201`) with `soort_gereedschapskoffers~Gereedschapstrolley` appended via a Dutch-compound-suffix fallback (token endswith leftover with ≥3 char prefix) that bypasses the matcher's `MIN_LENGTH_RATIO=0.4` guard *only* in this subcat-stem context. **(5) Multi-axis longest-per-axis leftover collector**: replaced the legacy `joined → compound-suffix → per-token-first-hit` chain with a unified scan — for each non-strict facet axis, pick the facet value whose tokens are all covered by the leftover, preferring the longest facet value name on ties. Joined match still runs alongside as a typo/phrase safety net. Multi-attribute leftovers like `rood_jurken_dames` now attach one facet per axis (`/c/doelgroep_mode~Dames~~kleur~Rood`); `pescara_jeans_dames` correctly gets `doelgroep_mode~Dames` even though token-coverage scored it 75 (below `FUZZY_THRESHOLD=80`) because the all-tokens-covered constraint replaces the threshold check. New helpers: `_leftover_token_matches_facet_token`, `_collect_longest_per_axis_from_leftover`. **(6) `build_multi_facet` "for simplicity, just use the primary facet" bug** in `src/url_builder.py:486-557`: when `match_multi_word` returned multiple matches whose URLs all pointed to the same target subcat (e.g. `nike_schoenen_dames` → both `doelgroep_mode~Dames` AND `merk~Nike` live in `mode_432362`), the "facets-from-different-category" branch silently dropped everything except `facets_from_different_category[0]`. Hardcoded `facet_count=1`. Comment said it explicitly: "For simplicity, just use the primary facet to ensure validity." Fixed: collect every match whose `facet_value.url` resolves to the same `category_path` as the primary, dedupe by facet axis (higher score wins), sort alphabetically for stable URLs, `~~`-join. Cross-subcat matches still skipped (would emit invalid Beslist filter URLs). `nike_schoenen_dames` now emits `/c/doelgroep_mode~Dames~~merk~Nike`. **(7) URL Checker Copy-for-Excel** (`frontend/url-checker.html`): silently produced unusable output because embedded `\t`/`\r`/`\n` in scraped `meta_title`/`meta_description`/`h1` broke the TSV mid-row, and the `clipboard.writeText` promise had no `.catch()` so rejections disappeared. Sanitized each cell (whitespace runs → single space) + added `.catch()` that logs the DOMException and surfaces the error in the alert. Matches the pattern that `redirect-checker.html` and `canonical.html` already used. **Architecture clarified**: there are two paths that can produce a multi-facet redirect URL — `_append_facet_to_subcat_redirect` (after a subcat-name match) and `build_multi_facet` (after step 4 maincat facet matching). Either can hit different bug surfaces; debug by checking the `reason` prefix (`[child_subcat]`/`[subcat_name_high]` → step 2b/3, `[maincat]` → step 4). Full LEARNINGS section "R-URL optimizer: hyphenated maincats, leftover-token facet matching, build_multi_facet sibling-dropping". Files: `backend/rurl_optimizer_v2/main_parallel_v2.py`, `backend/rurl_optimizer_v2/process_global_rurls.py`, `backend/rurl_optimizer_v2/src/matcher.py`, `backend/rurl_optimizer_v2/src/url_builder.py`, `frontend/url-checker.html`. #claude-session:2026-05-13 #priority:high

- [x] **FAQ/Kopteksten dashboard counts now sum to total + content-on-invalid-URLs cleaned up + FAQ last_error capture + clickable URLs in Recent Results** (2026-05-11). Bug: `/api/status` and `/api/faq/status` sourced Skipped from `pa.url_validation` (URL-level) and Processed from the content tables; neither partitioned the jobs tables, leaving ~117k FAQ + ~133k Kopteksten pending-on-invalid-URL rows silently unaccounted for. Fix: redefined buckets as subsets of `pa.{faq,kopteksten}_jobs` — Processed = `status='success'`, Skipped = `status='pending' AND url is_valid=FALSE`, so Total = Processed + Skipped + Failed + Pending exactly. **Side effect**: dashboards now show per-tool Skipped (smaller, job-scoped) instead of the shared URL-level count. **One-time DB cleanups** (totals grew to 402,339 / 402,349 after these): inserted 12,287 pending rows in both job tables for invalid URLs that had no job row (previously invisible to dashboards); deleted 22,602 `pa.faq_content_v2` + 6,160 `pa.kopteksten_content` rows whose URL is currently invalid (FAQ content was being held on URLs with no products), reset their corresponding success jobs to pending so they're counted as Skipped; reset all failed jobs (22,294 FAQ + 14,261 Kopteksten) back to pending for retry — bulk of Kopteksten failures were OpenAI 429 rate limits. **Defense in depth**: added `LEFT JOIN pa.url_validation v + (v.is_valid IS NULL OR v.is_valid=TRUE)` filter to all 4 publish queries in `backend/content_publisher.py` so invalid-URL content can't leak to production going forward even if regenerated. **FAQ last_error capture**: `fetch_products_api` now returns `error_detail` in all error paths (HTTP code, exception type+message, invalid-facet context/value); `generate_faqs_for_page` signature changed to `(FAQPage|None, error_str|None)`; `process_single_url_faq` propagates `result['error_detail']`; persistence at `main.py:1840-1901` + two `batch_api_service.py` FAQ-job INSERT sites now write `last_error`. Going forward every new failed `faq_jobs` row has populated `last_error` instead of NULL. **Recent Results clickable URLs**: Kopteksten (`frontend/js/app.js:553`) and FAQ (`frontend/js/faq.js:220`) URL blocks wrapped in `<a href="https://www.beslist.nl${item.url}" target="_blank">`, matching the existing Unique Titles pattern. **Bonus fix**: removed broken `sticky-top` from `frontend/url-checker.html` thead — sticky positioning fell back to page scroll context when the inner `overflow:auto` container didn't actually scroll (few rows), rendering the header floating mid-table. Files: `backend/main.py`, `backend/content_publisher.py`, `backend/faq_service.py`, `backend/batch_api_service.py`, `frontend/js/app.js`, `frontend/js/faq.js`, `frontend/url-checker.html`. Commits `a39483f` (url-checker fix) + `583cef7` (everything else). Full debugging guide in cc1/LEARNINGS.md → "FAQ/Kopteksten dashboard counts" section #claude-session:2026-05-11 #priority:high

- [x] **Unique Titles v3 — thaw-and-update pass, still in fridge** (2026-05-08). Pulled v3 out of the fridge for an iteration; pushed it back with several regressions addressed but three new ones discovered. Default still `AI_TITLES_PIPELINE=v1`; v3 stays opt-in. Commit `84e410c` pushed to dm-dashboard. **Updates inside `generate_title_v3` / `_build_v3_h1`**: (1) **Category-override reused from v1** — `batch_classify_facets` from `pa.facet_type_classifications` + `_NEVER_URL_SLUGS` (`type_productlijn`, `personage`, `seizoen_schoenen`) + `_ALWAYS_TYPE_URL_SLUGS` (`t_stoel`). When any selected facet is a "type-facet" carrying the product noun (e.g. `t_wanddeco→"wandplaten"` in Wanddecoratie), `category_name` is suppressed before the deterministic builder runs — fixes the `Wanten Handschoenen` / `Ventilatieventielen Ventilatiematerialen` redundancy class flagged in the original shelving notes. Required relaxing `_build_v3_h1`'s early `if not category_name: return ''` to `if not category_name and not selected_facets`. Verified on 15 type-facet URLs: every one shows category dropped (no redundant suffix). (2) **`generate_title_v3(polish=False)` codepath** — skips the OpenAI call entirely, returns deterministic composed_h1 directly. Added because the 100/500-URL A/B showed polish changed output in only 12-17% of cases (most outputs identical after `_v3_restore_casing` strips polish-applied case changes). User signal 2026-05-08: "looks fine without polishing" → no-polish is the favored path. (3) **Standalone `Met`/`Zonder` lowercased mid-title** — final-pass regex `(?<=\S)\s+(Met|Zonder)\b` runs after dedup. Brings v3 in line with v1's polish-rule-3 ("non-eigennamen NÁ het eerste woord in kleine letters") even when polish is off. (4) **Conditie facet → end of H1** — new `conditions: List[str]` bucket detected via `fname=='conditie' or 'conditie' in url_slug or 'condition' in url_slug`. Appended after `sizes` (last slot). Verified on 8 conditie URLs: `Nieuw` / `Gebruikt` / `Refurbished` lands at the end (`Apple iPad 2019 Tablets 10 inch Nieuw`). (5) **Color precedence** — new `kleurtint: List[str]` bucket; generalized kleurcombi match from `url_slug.startswith('kleurcombi')` to `'kleur' in slug AND 'combi' in slug` (also checks `fname`) so `kleur_combinatie`, `kleurcombinaties_schoenen`, `kleurcombinaties_woonacc` all hit. After loop: `if kleurtint or color_combos: colors = []` (drop generic kleur). `kleurtint` takes the front color slot in place of kleur; kleurcombi keeps its post-category slot. Effect: `Wit en groen Textiel Adidas Court Sneakers Maat 40` → `Adidas Court Textiel Sneakers Wit/groen Maat 40`. **A/B at 500 URLs after all changes**: v1 differs from v3-no-polish in 340/496 (~69%, down from 73% pre-color-precedence). xlsx at `~/v1_vs_v3_500_2026-05-08.xlsx`. **New regressions surfaced during scoring** (added to EXPERIMENTAL header in source as the open-blocker list): (a) brand acronym lowercasing in builder (`HEMA Uitnodigingen` → `Hema Uitnodigingen`); (b) brand mangling on `&` (`Heckett & Lane` → `Bruine & Lane …`); (c) attributive vs predicate inflection on measurements (`20 cm diep 73 cm hoog` → `20 cm diepe 73 cm hoge`). The polish-on path's non-brand agglutination class (`damedeodorant`, `herenspolshorloges`) from the original shelving notes does NOT apply on the polish=False path — but resurfaces if polish gets re-enabled. **Files**: `dm-tools/backend/ai_titles_service.py` (~80 net new lines under EXPERIMENTAL header, header text rewritten with current status), `dm-tools/scripts/v3_ab_100.py` (new — A/B harness, sample size as argv[1]), `dm-tools/scripts/v3_verify_override.py` (new — type-facet spot-check). **Pickup notes for next thaw**: see "Unique-titles v3 thaw-and-update pass" section in cc1/LEARNINGS.md. #claude-session:2026-05-08 #priority:medium #status:shelved-opt-in

- [x] **Big Bang DB refactor — collapsed per-tool URL tables into one `pa.urls` catalog + per-tool `*_jobs` / `*_content` tables (FK on `url_id`)** — single coherent cutover for the entire SEO content surface (Kopteksten, FAQ, Unique Titles, content publisher, URL validation, link validation). Old tables (`pa.jvs_seo_werkvoorraad`, `pa.jvs_seo_werkvoorraad_kopteksten_check`, `pa.content_urls_joep`, `pa.faq_tracking`, `pa.faq_content`, `pa.faq_validation_results`, `pa.url_validation_tracking`, `pa.link_validation_results`, `pa.unique_titles`) renamed to `*_old_2026_05_07` as the forcing function — anything unmigrated now fails loudly with "relation does not exist" instead of writing to the frozen pre-cutover snapshot. **What's new**: `pa.urls` (980k unique canonicalized URLs, single source of truth, BIGSERIAL PK), per-tool `pa.kopteksten_jobs / pa.kopteksten_content / pa.kopteksten_link_validation`, `pa.faq_jobs / pa.faq_content_v2 / pa.faq_link_validation` (`_v2` suffix temporary, rename in step 5), `pa.unique_titles_jobs / pa.unique_titles_content`, plus shared `pa.url_validation`. **Helper**: new `backend/url_catalog.py` (`canonicalize_url`, `get_url_id`, `bulk_upsert_urls`) — every write path uses this; the SQL function `pa.canonicalize_url(text)` mirrors the same rules so the WHERE-clause `WHERE u.url = pa.canonicalize_url(%s)` lookup pattern works. **Migration steps** (all in `dm-tools/migrations/2026-05-07-bigbang-step*.{sql,md}`): step 1 = create new tables; step 2 = backfill from union of all sources (with canonicalization deduping ~2,594 silent duplicates); step 3a = Unique Titles code refactor; step 3a-fix = backfill 399,906 content rows for CSV-imports that had `ai_processed=FALSE` but content populated (legacy eligibility quirk); step 3b/3c = FAQ + Kopteksten bundled (content_publisher joins both, can't migrate one at a time); step 3c-perf = btree indexes on created_at/updated_at + ANALYZE + subquery-LIMIT-then-JOIN rewrites (25× speedup on recent-results panels, 12× on /api/content-publish/stats); step 4 = rename old tables. **Eligibility-backfill subtlety**: old `werkvoorraad` was the universe for both kopteksten + FAQ; new schema needs explicit per-tool job rows. One-shot insert of `status='pending'` rows for every werkvoorraad URL not already in faq_jobs/kopteksten_jobs — both job tables are now exactly 390,022 rows, the canonical werkvoorraad universe preserved. **`add_urls_to_werkvoorraad` behavior change**: now writes ONLY to `pa.kopteksten_jobs` (was implicitly shared via werkvoorraad). Callers that need FAQ eligibility must explicitly insert into `pa.faq_jobs` too. **NOT migrated** (deferred or out of scope): `pa.content_history` (audit-only, append-only, low-value to migrate); `pa.publish_log` (no URL column); `pa.jvs_seo_werkvoorraad_shopping_season` (Redshift, separate concern). One-shot scripts (`fix_faq_*.py`, `migrate_shared_validation.py`, `deduplicate_content.py`, duplicates in `scripts/`) converted to no-op stubs that print "OBSOLETE" instead of crashing. **Verification**: full FastAPI app imports cleanly; all 6 main dashboard endpoints (`/api/status`, `/api/faq/status`, `/api/content-publish/stats`, `/api/ai-titles/status`, `/api/failure-reasons`, `/api/validation-history`) return 200; data integrity audit shows 0 orphaned content rows in any of the 3 content tables (every url_id FK resolves); row-count deltas vs old tables match the canonicalization-dedup expectation. Dashboards now sub-second (down from 2+ minutes pre-perf-fix). **Old tables stay as 1-week safety net** — step 5 will DROP TABLE them after verification (not yet run). **Files** (full list in `cc1/LEARNINGS.md`): `dm-tools/backend/{url_catalog,unique_titles,ai_titles_service,content_publisher,link_validator,batch_api_service,main,database,thema_ads_db,import_content,import_missing_content,find_bad_urls,check_unique_titles_urls,compare_prompts,sync_werkvoorraad,sync_redshift_flags,deduplicate_content,migrate_shared_validation,fix_faq_*}.py`, `dm-tools/scripts/{score_titles,export_scored_titles,fix_faq_item_names,csv_utils/import_content}.py`, `dm-tools/backend/schema.sql`, `dm-tools/migrations/2026-05-07-bigbang-step*.{sql,md}`, `dm-tools/docs/ARCHITECTURE.md` (Database Architecture section rewritten). **8 commits**: `def56c2`, `6bbdc0e`, `b829487`, `be80293`, `473ad7c`, `d5c8739`, `b00e59f`, `4ac8808`. Pushed to dm-dashboard. **If anything table-related breaks in the future** — see the Big Bang section at the top of `cc1/LEARNINGS.md` for the old→new table mapping, helper-module lookup, and "files I should look at first" debugging guide #claude-session:2026-05-07 #priority:high

- [x] **Unique Titles v3 pipeline experiment — built, tuned to ~76%, shelved as opt-in** (deterministic builder + AI polish, alternative to v1's strip-and-rewrite). New `generate_title_v3()` in `dm-tools/backend/ai_titles_service.py` composes H1 deterministically from facets in fixed slots (`<colour> <merk> <populaire_serie> <type_productlijn> <productlijn> <materials> <other adj> <doelgroep> <category> <met-clauses> <voor-clauses> <color-combos> <size>`), hands to gpt-4o-mini for polish only (5-rule prompt vs v1's 11), then runs `_v3_preserves_content` (token-set guard, allows agglutination), `_v3_preserves_brands` (rejects polish that swallows brands into compounds), `_v3_restore_casing` (token-by-token original casing copy). Hallucination guard skipped — its prefix-match length-diff ≤3 wrongly rejected legitimate Dutch agglutination. **Final A/B**: 100 random AI-processed URLs, ~76% acceptable vs v1, below the 85% threshold to auto-promote. **Two open regressions blocked promotion**: (1) composed-builder semantic redundancy when facet value is a near-synonym of the canonical category (`Ventilatieventielen Ventilatiematerialen`, `Kandelaars Kaarsenhouders`, `Wanten Handschoenen`) — needs a synonym lookup, not another guard; (2) AI agglutination errors that aren't brand-swallowing (`damestmultivitaminen`, `schuifdekselkoelkasten`) — current guards don't catch because all source tokens remain as substrings. **Status**: shelved opt-in. `AI_TITLES_PIPELINE` env var (`v1` default, `v3` to enable) set in `start_processing()`. Code lives under "EXPERIMENTAL — IN FRIDGE" header in `ai_titles_service.py`. Full A/B journey + design rationale in cc1/LEARNINGS.md ("Unique-titles v3 pipeline experiment — shelved at ~76% acceptable"). Pick-up notes for future-me documented there. Files: `dm-tools/backend/ai_titles_service.py` (~250 lines added under EXPERIMENTAL header). #claude-session:2026-05-06 #priority:medium #status:shelved-opt-in

- [x] **GSD Check** — new dm-dashboard tool under Google Ads. Mirrors MC ID Finder's UX: textarea of shop names → Redshift query → 8-column sortable/paginated table with CSV export (`shop_name`, `is_gsd_nl_shop`, `is_gsd_be_shop`, `is_gsd_de_shop`, `shop_phase`, `hide_online`, `is_disabled`, `accountmanager_name`). Joins `bt.shop_main_attributes_by_day` (filtered to `date = CURRENT_DATE - 1` for the GSD flags) with the latest pre-yesterday `bt.shop_list` snapshot via ROW_NUMBER over `dim_date_key` for shop_phase / hide_online / is_disabled / accountmanager_name. **Two type-specific gotchas hit during smoke-test** (logged in cc1/LEARNINGS): (1) `date` is DATE — `TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD')` silently returns 0 rows because the implicit-cast that makes a bare `'YYYYMMDD'` literal work doesn't fire on a runtime-computed string. Fixed by comparing `CURRENT_DATE - 1` directly. (2) `dim_date_key` is BIGINT — needed `CAST(... AS BIGINT)` for the upper-bound filter. Files: `dm-tools/backend/gsd_check_{service,router}.py`, `dm-tools/frontend/gsd-check.html`, `dm-tools/backend/main.py`, `dm-tools/frontend/dashboard.html`, +22 frontend HTML files for nav-dropdown link insertion (single Python regex bulk pass) #claude-session:2026-05-05

- [x] **Unique Titles dedup pipeline hardening** — three orthogonal fixes plus operational cleanups, all logged in cc1/LEARNINGS. **(a) Hyphen-aware prefix overlap** (`_dedupe_prefix_overlap`): skip hyphenated `b` so cases like `"Fisher Price Fisher-Price …"` no longer drop the standalone `"Fisher"` and orphan `"Price"`. The multi-token form is left for `_dedupe_internal_compounds` whose `_norm_for_dedupe` collapses spaces + hyphens into the same key. **(b) Plural-suffix dedup rule** + lookahead extension: targeted suffix list `('s', 'en', 'ers')` with a 4-char floor fires only when `b == a + suf` — catches `"Sweat sweaters"`, `"Plant planten"`, `"Color Colors"`, `"tuinstoel … Tuinstoelen"` while still letting `"Aqua Aquariums"` through (riums not in list). The original 6-char/+3-diff generic rule preserved as a separate clause. Lookahead window widened 2→4 positions so cases like `"Ecco Byway Zwarte instap Heren schoenen Instappers Maat 43"` (3 tokens between prefix and full form) still trigger. **(c) v2 hallucination guard as default** (`generate_title_from_api`): flipped `halluc_mode` default from `'v1'` (8 hardcoded words) to `'v2'` (prefix-match against input vocabulary + Dutch inflections). v2 drops `"wandelzomer"` (LLM concatenated facets `"Wandel"` + `"Zomer"` into a glued token) and orphan `"Sluiting"` (AI extracted from `"ritssluiting"`) while keeping legitimate inflections like `"Katoenen"` via the `+'en'` whitelist entry. **(d) `--reload` propagation fix**: `start-dm-tools.bat` (Windows Task Scheduler boot script) was missing `--reload` even though `start.sh` and `run_local.sh` had it. Every reboot was launching uvicorn without auto-reload, silently turning every code edit into "needs manual restart" — exactly what bit me when an earlier "v2 default" claim turned out to still be running v1 because the WSL uvicorn was the boot-script instance. Added `--reload` to the .bat. **(e) `ai_processed=TRUE` stuck-row sweep**: discovered that bulk-resetting H1s via `SET h1_title=NULL` doesn't re-queue them — eligibility is `ai_processed IS NULL/FALSE`. Found 11,833 rows in this state across multiple prior reset batches and unstuck them via `UPDATE … SET ai_processed=FALSE, ai_processed_at=NULL, ai_error=NULL WHERE h1_title IS NULL AND ai_processed=TRUE`. Logged the gotcha. **(f) Category-URL audit**: verified all 3,880 category-only URLs (no `/c/` facets) against the canonical category name from `cat_urls.csv`. 3,144 exact-match (~81%), 243 stylistic variants (plural/accent), 244 unmapped, 249 real mismatches re-queued for regen. Re-queue cleared title/description/h1_title AND `ai_processed`. Files: `dm-tools/backend/ai_titles_service.py`, `dm-tools/start-dm-tools.bat` #claude-session:2026-05-05

- [x] DMA+ Monthly polish pass — auto-adapt, dry-run, Windows codec, UI cleanup. **(a) Auto-adapt the new delta layout for every per-shop op** (`407ce9c`): `_run_operation` now detects `{NL,BE} - Nieuw (aanmaken)` / `{NL,BE} - Afvallers` sheets in the uploaded xlsx and transforms on the fly — Include/Exclude read the Nieuw sheet, Reverse-* read Afvallers, each row fans out to cl1 a/b/c, processors stay untouched. Fails fast with a clear ValueError if the selected op/country needs a sheet the uploaded file doesn't have. **(b) Dry-run for Process Monthly Excel** (`3252b26`): orchestrator was throwing away the `dry_run` kwarg that all four v2 processors already supported. Threaded it through `/api/dma-plus/monthly` → `start_monthly` → `run_monthly_delta` → the four lambdas. Dry-run toggle is now visible for Monthly op on the frontend; task dict + log carry `[DRY RUN]`. Full audit confirmed every `mutate_*` call inside the four processors is gated with `if dry_run: … else: …` — inclusion even mints fake `DRY_RUN_<uuid>` resource names so downstream logic keeps running. Verified dry-run is actually dry by querying `change_event` in Google Ads for the run window — 0 events across both NL and BE. Be aware: log lines like `"✅ PLA/X: 1 removed"` under dry-run are synthesised (L218–225 builds a fake success dict); the wording is the shared message template. GAQL field is `change_resource_type`, not `changed_resource_type`. **(c) Windows cp1252 crash on Dutch/emoji output** (`4b013c7`): `'charmap' codec can't encode characters in position 2-71`. Root cause: `sys.stderr` defaults to cp1252 on Windows, Google Ads library + campaign_processor emit logs with ✅/🌳/→/Dutch names. Our stdout-capture blocks only redirected stdout — 7 sites in `_run_operation` + 1 in `_run_one_operation`. Fix (belt-and-suspenders): reconfigure `sys.stdout`/`sys.stderr` to `encoding='utf-8', errors='replace'` at module import of both `dma_plus_monthly.py` and `dma_plus_service.py`; and during processor calls redirect BOTH stdout and stderr to the same in-memory StringIO. **(d) UI polish** (`c862512`): Download xlsx button moved out of the stats row (where it replaced a number tile, looked unbalanced) into the Results card header next to Copy Results; header button reset at the start of every `showResults` call so it only shows for Monthly Delta. Stats row is now three equal `col-4` tiles (Countries / Rows processed / Errors). Files: `backend/dma_plus_monthly.py`, `backend/dma_plus_service.py`, `backend/dma_plus_router.py`, `frontend/dma-plus.html` #claude-session:2026-04-23 #priority:high

- [x] DMA+ — ported the "expanded" dma_script version into the dashboard. **(a) New backend module** `backend/dma_plus_monthly.py` (~400 lines) isolated from the existing 1014-line `dma_plus_service.py`: parallel BFS taxv2 crawler (`_fetch_taxv2_tree`, 3575 categories in ~19s via `ThreadPoolExecutor(max_workers=12)` against `/api/Categories/{id}?includeSubCategories=true` — the flat `/api/Categories` endpoint only returns root categories despite `rootCategoriesOnly=false`), `_fetch_pla_campaign_names` (queries `campaign` resource directly, not `ad_group`, so zero-adgroup campaigns still count), four `_build_*_workbook_from_source` helpers that read a 3-col source sheet (A=shop, B=maincat, C=maincat_id) and fan each row to cl1 ∈ {a,b,c} into the conventional processor sheet name (`toevoegen` / `uitsluiten` / `verwijderen`) — avoiding any changes to the 8805-line `campaign_processor.py`. **(b) Orchestrators**: `run_monthly_delta(task_id, wb_bytes)` loops NL→BE running Include → Exclude on "Nieuw" then Reverse-exclude → Reverse-include on "Afvallers"; `run_category_coverage(task_id, country)` writes TRUE/FALSE per-cl1 flags (cols A/B/C) + category_id/name/parent_id/is_leaf for every taxv2 category. **(c) Task integration**: reuses the existing `_set_task`/`_get_task`/`_check_cancelled`/`TaskCancelled` + `/api/dma-plus/status/{id}` + `/cancel/{id}` pipeline, appends lines to a `log` list on the task dict (capped at 500) for live tailing, writes summary + errors to `output_path = /tmp/dma-plus-output/{type}_{country}_{ts}.xlsx`, enters the shared history. **(d) Router**: three new endpoints on `dma_plus_router.py` — `POST /api/dma-plus/monthly` (file upload), `POST /api/dma-plus/coverage?country=NL|BE`, `GET /api/dma-plus/download/{task_id}` (FileResponse). **(e) Frontend**: new "Monthly Delta & Category Coverage" card in `dma-plus.html` with file input / country picker / dedicated progress bar / live log tail / errors table / download button; runs independently of the existing per-shop Start flow. **(f) Fix found during smoke-test**: my initial job-runner wrapper used `except Exception`, which can't catch `SystemExit` raised by `initialize_google_ads_client`'s `sys.exit(1)` on missing OAuth creds — the worker thread dies silently, task stuck at "running" forever. Broadened to `except BaseException` with `type(exc).__name__` in the logged error. **(g) Validated end-to-end**: live `/api/dma-plus/coverage?country=NL` via the dashboard — completed in ~40s, `summary: {NL: {a:3227, b:3227, c:3227, total:3575}}`, 9843 PLA campaigns, xlsx downloaded from `/download/{id}` verified (127 KB, 3576 rows, schema matches). **(h) Not validated**: monthly delta end-to-end (would create real ad groups — building blocks are the same the per-shop flow uses daily, so low risk, but first live run should be watched). Files: `backend/dma_plus_monthly.py` (new), `backend/dma_plus_router.py`, `frontend/dma-plus.html` #claude-session:2026-04-22 #priority:high

- [x] Bump `google-ads` 24.1.0 → 30.0.0 — API v17 was retired, every GAQL call returned `501 GRPC target method can't be resolved`. Validated read-only against NL: `campaign` search (9843 rows), `ad_group_criterion` listing_group reads with proto-plus `case_value._pb.WhichOneof("dimension")`, enum + proto construction (`AdGroupCriterionOperation`, `ListingGroupTypeEnum`, `ProductCustomAttributeIndexEnum`, `copy_from`), `prefetch_pla_campaigns_and_ad_groups` (9843/12051). Mutation paths not exercised. Applied to both `dma_script/requirements.txt` and implicitly to dm-dashboard's venv (already on `google-ads>=25.1.0`). #claude-session:2026-04-22 #priority:high

- [x] URL Checker — polish pass, driven entirely by in-session user feedback. **(a) Button restyling**: Download CSV / Download Excel buttons moved from Bootstrap `btn-light` to the outlined pattern the project uses elsewhere (`border-color + color` + hover swap), with the project's burnt-orange `#CC5500` instead of the Copy-for-Excel purple. Later removed the CSV button entirely ("Excel is enough") — simplified `downloadResults(format)` to a one-purpose `downloadExcel()` since it had a single remaining caller. **(b) Fixed-width results table**: added `table-layout: fixed; min-width: 1800px` inside an `overflow: auto` wrapper, explicit px widths on every `<th>` (40 / 280 / 70 / 240 / 280 / 100 / 260 / 300 / 250 / 80), default `td` styling of `nowrap + ellipsis`. Solves the earlier issue of cells reflowing weirdly as content arrived. **(c) Canonical column**: added between Redirect URL and Title, wired to the backend's existing `canonical_url` field (the field was already populated by `extract_canonical_from_html` and appeared in the XLSX column list, but had no UI surface). Copyable cell with ellipsis clipping, same pattern as Redirect URL. **(d) Status column**: centered header + cells, rendered as Bootstrap badges matching the URL Validator palette (200 `bg-success`, 3xx `bg-warning text-dark`, 4xx/5xx `bg-danger`, other `bg-secondary`). Retired the `.status-*` CSS classes (left in the file, unused — harmless). **(e) Summary badges**: switched Total + per-status-code badges from grey/blue to the same palette as the row cells so summary + detail read the same; swapped the hard-to-read grey Total for orange `#CC5500`. Then removed the Redirects badge — 301 count already communicated that. **(f) Hover-expand UX**: first attempt used `white-space: normal; overflow: visible;` on `:hover` which caused a horizontal layout shift inside the fixed-layout table (content spilled past the column and shoved siblings). Fix: drop `overflow: visible`, switch to `overflow-wrap: anywhere; word-break: break-word;` — cell stays at column width, text wraps inside, row grows vertically. Captured in cc1/LEARNINGS as a rule. **(g) URL cells hover-expand parity**: Title/Description/H1 hover-expanded correctly but URL cells didn't — root cause was JS-level `truncate(url, 50)` slicing the string before it hit the DOM. No CSS can un-slice what was never rendered. Switched to full `escapeHtml(url)` and let the existing `.url-cell` CSS handle visual clipping. Captured in cc1/LEARNINGS. **(h) Strip beslist host from table + exports**: new `stripBeslist(url)` helper (regex `^https?:\/\/(www\.)?beslist\.(nl|be)`) applied to input_url / redirect_url / canonical_url in the row renderer, title tooltip, click-to-copy payload, CSV (while it still existed), TSV copy, and the POST body to `/api/url-checker/download`. Raw `results` array kept unmodified — stripping happens at render/export time via a `stripResult(r)` helper. **(i) Decode HTML entities in Title / H1 / Description**: added `decodeEntities(str)` that roundtrips through a single reusable textarea element to handle every named + numeric entity natively. User still saw `&#10062;` after the initial implementation — upstream was double-encoding (`&amp;#10062;` → BS4 decoded to `&#10062;` → my single pass could have decoded it but in practice some inputs were triple-encoded). Made the helper loop up to 3 passes, stopping as soon as a pass is idempotent. `stripResult(r)` now runs the decode on `meta_title` / `meta_description` / `h1` so both display and exports get the decoded text. Captured in cc1/LEARNINGS. **(j) "Verdict" column** (initially proposed as "Self-Canonical", renamed per user request for the column header — backend field name stays `self_canonical` for spreadsheet clarity): new `computeSelfCanonical(r)` compares the page's canonical URL against the effective URL (`redirect_url || input_url`) after a small normalization (lowercase host, strip trailing slash, keep query+hash, use `URL()` with a fallback). Three-state: green-`true` / red-`false` / muted `-` when there's no canonical to compare. New 100px centered column between Canonical and Title; table `min-width` bumped 1800→1900. Field surfaced in TSV headers and added to `backend/main.py:3518` XLSX column list. Files: `dm-dashboard/frontend/url-checker.html`, `dm-dashboard/backend/main.py:3518` #claude-session:2026-04-21

- [x] Unique Titles — add Remove button to individual-title editor. Calls the existing `DELETE /api/unique-titles/{url:path}` endpoint (was already implemented in `main.py:2655` but had no UI path). Button hidden by default (`d-none`); `editTitle()` unhides it when populating the form from a search-result row; Clear re-hides it. Confirm dialog before the DELETE. On success: reset form, refresh stats, and re-run `doSearch()` if a search query is still active so the removed row disappears. Styled as red-outlined via inline `style="border:1px solid #d63031;..."` + hover handlers — not `btn-outline-danger`, because `frontend/css/style.css:373` aliases that class to the project's burnt-orange `#CC5500`. Logged the styling override in cc1/LEARNINGS.md so it doesn't get reintroduced. File: `dm-dashboard/frontend/unique-titles.html` #claude-session:2026-04-21

- [x] GSD Budgets — UI simplification pass. Removed the **Upload missed-shops** checkbox (upload is now the default on live runs, dry-run still skips it via the existing `not dry_run` gate). Removed the **Limit shops** number input. In both cases the backend query params (`skip_missed_upload`, `limit_shops`) were kept in the FastAPI signature with their original defaults — lets you still curl `/api/gsd-budgets/run?limit_shops=5` for smoke-tests or hit the endpoint with `skip_missed_upload=true` from a cron-style script, without rewriting the UI. Pattern logged to cc1/LEARNINGS.md as "API escape hatches when removing UI controls." Also answered the "what shops are picked when limit is used?" question: `ORDER BY shop_id ASC` → the N oldest-registered shops matching all other filters (not alphabetical, not performance-ranked). File: `dm-dashboard/frontend/gsd-budgets.html` #claude-session:2026-04-21

- [x] GSD Budgets — dry-run audit + fix. User asked "is dry run truly dry run?" — grep of all mutations caught that `sync_shop_exclusions` was running DELETE+INSERT on `pa.gsd_shop_exclusions_joep` even in dry-run (it runs at the top of `run_gsd_budgets` to keep the exclusions table in sync with the Google Sheet before the main shop query reads it). `adjust_campaign_budget` (Google Ads) and `upload_missed_shops` (Redshift) were already properly gated. Fix: gated the sync at the call site with `if dry_run:` branch that sets `exclusions_sync_status = "skipped_dry_run"` and skips. Added `exclusions_sync_status` field to the run_result (`"synced" | "skipped_dry_run" | "failed: <msg>"`) so the UI can show the user what happened. Trade-off: dry-run now uses the existing table contents (as stale as the last live run) rather than the live sheet — logged in cc1/LEARNINGS.md as an intentional consequence of strict-dry-run semantics. File: `dm-dashboard/backend/gsd_budgets_service.py:892-902` #claude-session:2026-04-21

- [x] **GSD Budgets** — brand-new dm-dashboard tool under Google Ads, ported from `GSD_verhogingen_verlagingen.py` (NL) + `GSD_verhogingen_verlagingen_BE.py` (BE). Automates daily GSD campaign-budget verhogingen/verlagingen based on 7-day shop marge + rev/click + SA360 per-campaign marge + a Google Sheets BUDGET_CONSTRAINED flag. **(a) Backend** (`dm-dashboard/backend/gsd_budgets_service.py` + `gsd_budgets_router.py`): refactored the 1600-line imperative NL script into pure functions, parameterised country via a `COUNTRY_CONFIG` dict with per-country values for `customer_id` (NL 7938980174 / BE 2454295509), `domain` (1/2), `sa360_account` (`Direct Shopping` / `Beslist.be: Direct Shopping`), `campaign_limited_sheet` id; everything else shared. Uses `backend.database.get_redshift_connection()` pool instead of fresh psycopg2 sockets. Threading lock (`_run_lock`) serialises full runs so two Run clicks can't race on budget mutations. Run history persisted to `backend/data/gsd_budgets_history.json`, cap 50, re-loaded on module import. **(b) Router** mirrors DMA Bidding exactly: `GET /api/gsd-budgets/{health,stats,history,history/{id}}` + `POST /run` with query params `country, dry_run, start_days_ago, end_days_ago, limit_shops, shop_names, shop_names_excluded, skip_missed_upload`. `ThreadPoolExecutor(max_workers=2)` runs the blocking work off the async loop. **(c) Frontend** (`frontend/gsd-budgets.html`): cloned from `dma-bidding.html`. Controls: NL/BE radio, date pickers, limit-shops number input, shop-names textarea with include/exclude mode radios, **Upload missed-shops** toggle (checked = upload to `pa.jvs_gsd_missed_shops`, default ON; always skipped in dry-run), Dry Run toggle (default ON). Results grouped by action (verhogen-20 / verlagen-20 / verlagen-25 / c-verlagen-20) with per-campaign mutation rows; separate "Shops > €25 verlies" collapsible. XLSX export produces one sheet per action type + Top-25 sheet. **(d) Nav integration**: Euro-symbol SVG icon in dashboard tile + Google Ads dropdown on all 19 pages carrying the dropdown; single-regex Python bulk-insert. Purple-outlined **Add shop exclusions** button in the Run card header linking to the maintained Google Sheet `1y7kZmo9O7KO4uaG9wwq_wOtovsDMas07TAFDb0cyGAE`. **(e) Exclusions flow**: every run, `sync_shop_exclusions()` truncates `pa.gsd_shop_exclusions_joep` and re-inserts the full sheet contents; main shop query filters `NOT IN (SELECT "shop id" FROM pa.gsd_shop_exclusions_joep)`. So the sheet is always the source of truth. **(f) Decision tree** (unchanged from source, unit-verified across 8 scenarios): `marge < -25` → verlagen-25 + top-25 list; `-25 < marge < -5` → verlagen-20; `marge > 0, linkage > 0.5, transactions >= 7, rev_click > 1.38` + delta > -15 → verhogen-20, delta < -30 → c-verlagen-20, else → missed-shops list; `rev_click < 1.38` → verlagen-20; anything else → no action. Per-campaign gates: verlagen-25/20 need `campaign_marge < 0`, c-verlagen-20 needs `[label:c]` in name, verhogen-20 needs `campaign_marge > 0 AND budget_constrained = 1`. **(g) Dependencies**: `gspread`, `pytz`, `setuptools<81` (pin so `pkg_resources` stays available for the vendored SA360 SDK), local tarball `./backend/vendor/searchads360-py.tar.gz` all added to `requirements.txt`. Dockerfile copies `backend/vendor/` BEFORE pip install so the local tarball resolves. **(h) Deliberately NOT ported**: `sendMail` / `sendMail1` / `sendMail2` (MSAL device-code flow, can't run unattended from a request handler — UI + XLSX export replace it), plus all `*_1` / `*_2` retry/dead-code variants. **(i) Config**: new `.env` var `GSD_SHEETS_SERVICE_ACCOUNT_FILE=backend/data/gsd-campaign-creation.json` (the service-account JSON itself is gitignored + copied over from the Windows source). **(j) Smoke-test**: dm-tools uvicorn on :8003 stopped, dm-dashboard uvicorn started in its place using the dm-tools venv with the 3 missing deps installed; `/login` returns 200, `/static/gsd-budgets.html` returns 307 to login as expected. User tested live. Files: `dm-dashboard/backend/gsd_budgets_{service,router}.py`, `dm-dashboard/frontend/gsd-budgets.html`, `dm-dashboard/backend/main.py`, `dm-dashboard/backend/vendor/{util_searchads360/,searchads360-py.tar.gz}`, `dm-dashboard/requirements.txt`, `dm-dashboard/Dockerfile`, `dm-dashboard/.env.example`, `dm-dashboard/.gitignore`, `dm-dashboard/frontend/dashboard.html`, 19x `dm-dashboard/frontend/*.html` for nav link #claude-session:2026-04-21

- [x] URL Validator — Suggested URL: preserve `/r/{query}/` + force all-lowercase output. **(a) `/r/` preservation**: `parse_beslist_url` was stripping the `/r/{bucket}/` segment during normalisation (single `re.sub` that dropped bucket and trailing slash together) so `build_suggested_url` had no way to know it was ever there. Result: a URL like `/products/huis_tuin/.../r/dark_grey/c/merk~83292` came back as `/products/huis_tuin/.../c/merk~83292` — a different URL shape. Fix: added `r_query: str = ""` to `ParsedUrl`, captured the bucket via `re.search(r'/r/([^/]+)(?:/|$)', path)` (matches with or without trailing slash so edge-cases like `.../r/dark_grey` without trailing slash also parse cleanly) BEFORE the strip, then `build_suggested_url` re-emits `/r/{r_query}` between the category segments and the `/c/` facets per the blueprint `/products/{maincat}/{subcat}/r/{query}/c/{f1}~~{f2}`. Trailing-slash rule unchanged: if `/c/` follows, no trailing slash; otherwise (category-only or `/r/`-only) trailing slash. Dead-code note: the `HAS_BUCKET` issue check at `:391` compares `parsed.path` after the strip — it never fires. Left as-is since preserving `/r/` means there's nothing to flag anyway. **(b) Force lowercase**: `maincat_slug` and `subcat_slug` were already lowercased individually in `build_suggested_url`, but the `/r/{query}` segment (newly added), facet *values*, scheme, and netloc were passed through verbatim. Switched to a single `path = path.lower()` + `scheme.lower() / netloc.lower()` pass over the final output instead of lowercasing per-segment. Verified: `https://WWW.Beslist.NL/products/Huis_Tuin/.../r/Dark_Grey/c/Merk~83292` → `https://www.beslist.nl/products/huis_tuin/.../r/dark_grey/c/merk~83292`. Commits `8dc0a8b` (/r/ preservation), `c9af8b1` (lowercase). File: `backend/url_validator_service.py` #claude-session:2026-04-20

- [x] FAQ + Kopteksten — ban vague-anchor hyperlinks ("klik hier", "deze link", "hier", "deze", etc.), audit + reconcile the tracking tables, and fix one validator false-positive (then revert on user clarification). **(a) Prompts**: hardened the hyperlink rules in 4 locations — `backend/faq_service.py` (FAQ single-URL), `backend/batch_api_service.py` (FAQ batch + Kopteksten batch system message), `backend/gpt_service.py` (Kopteksten subcategory prompt + system message, main-category prompt + system message). Each now has an explicit VERBODEN LINKTEKSTEN list (`klik hier`, `hier klikken`, `hier`, `deze link`, `deze pagina`, `deze gids`, `deze`, `lees meer`, `meer info`, `kijk hier`, `bekijk hier`, `via deze link`, `ga naar`, `link`), a FOUT/GOED example pair matching the real FAQ the user flagged ("Dark Grey variant kun je hier klikken"), and a rule that anchors MUST be the product name or a logical search term — if that doesn't fit naturally, omit the link instead of using a vague one. **(b) Post-processing guard**: new `VAGUE_ANCHOR_TEXTS` set in `faq_service.py` — any `<a>` tag whose text normalises (lowercase, strip punctuation) into the set is unwrapped before the FAQ is saved, even if the model slipped. Belt-and-suspenders. **(c) Existing offenders scan**: wrote one-off audit query against `n8n-vector-db` — found 1,280 FAQ rows (0.58 %) and 274 kopteksten rows (0.13 %) with vague anchors. Breakdown: FAQ dominated by `hier` (966), `deze pagina` (205), `deze link` (191); kopteksten mostly `hier` (240) + `hier klikken` (22). Reset all 1,554 to pending so next batch regenerates them — FAQ via `UPDATE pa.faq_tracking SET status='pending', skip_reason=NULL`, kopteksten via `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check` (kopteksten "pending" is encoded as *absence of tracking row*, FAQ uses `status='pending'` explicitly — different conventions for the same concept). **(d) Full DB audit**: broader sweep across `pa.content_urls_joep` / `pa.faq_content` + their tracking tables. Findings: 6,484 orphan kopteksten content rows (content but no tracking, ~80 % from Jan 2026 — likely `import_content.py` that never registered tracking); 1,741 kopteksten rows with status='failed' but content exists (mostly `api_failed` or `duplicate key` constraint violations where the second UPSERT failed but the first already saved the content); 3,152 kopteksten rows with status='completed' (not in schema — one-off Feb-2026 import used a non-standard name); 11 FAQ orphans + 3 FAQ failed-but-has-content (tiny, no action). Cross-system: 7,816 kopteksten + 23,853 FAQ URLs also appear in `pa.url_validation_tracking` as `skipped | no_products_found` — content predates the skip, not dangerous. **(e) Reconcile — ordering bug**: attempted to reset failed+completed to pending (delete their tracking rows) AND backfill orphan content as success in *one* transaction. The delete fired first, so the just-deleted 4,893 rows became fresh orphans, which the backfill then re-inserted as `status='success'`. Net effect: my "reset to pending" got silently reverted to success. Partially recovered via `DELETE WHERE created_at::date='2026-02-01'` (captures most of the 3,152 completed cohort — 816 rows deleted; rest are unrecoverable as a cohort because the created_at timestamps got rewritten during the backfill). Accepted the residual ~2,336 drift. **(f) Validator correctness probe**: ran `link_validator.py` against 200 random content rows (687 product links). 13 "gone" verdicts, 0 truly gone — 11 `UNRECOGNIZED_FORMAT` (truncated `/p/slug/` URLs missing maincat_id+pimId), 2 `shopCount=1`. Reported both as false-positive risks. User clarified: unparseable URL format SHOULD be treated as gone (reprocess and let the next batch emit a valid link). Reverted the unrecognized-format skip fix; kept the `shopCount<2` threshold note for the user's decision. Net code state on the validator: no change from pre-session. **(g) Prompt updates + post-processing guard committed**; no changes to `link_validator.py`. Files: `backend/faq_service.py`, `backend/batch_api_service.py`, `backend/gpt_service.py`, `docs/ARCHITECTURE.md`, `cc1/LEARNINGS.md` #claude-session:2026-04-20

- [x] URL Validator — fix FACET_NOT_LINKED false positives from wrong Taxonomy endpoint. User flagged that `/products/klussen/klussen_486171_486136/c/merk~486378~~pl_lamp~16018130` returns results on beslist.nl but the validator was flagging `pl_lamp` as not linked to the category. Investigation: `/api/CategoryFacets?categoryId=9001287` returns 11 facets; the inline `.facets` array inside `/api/Categories/9001287` has 20. The 9 missing all have `inheritanceStatus="Dependent"` — the product-line facets (`pl_lamp`, `pl_camera`, `pl_hg`, `pl_klussen`, `pl_leifheit`, `pl_afwasmiddel`, `pl_wasmiddel`, `p_ladder`) plus `t_uvlamp`. `CategoryFacets` silently returns only `Direct` + `Inherited`. Fix: rewrote `TaxonomyCache.get_category_facets` to pull from `get_category_detail(cat_id).facets` (response shape differs — top-level `facetId` + inline `labels` instead of nested under `facet.id`). Bonus: now shares one API round-trip with `get_category_detail` instead of two separate calls, roughly halving cache warm-up time. Commit `82ddc8e` — file `backend/url_validator_service.py:get_category_facets` #claude-session:2026-04-20

- [x] URL Validator — Suggested URL column + UX polish (follow-up session). **(a) Suggested URL column** (`backend/url_validator_service.py`, `backend/url_validator_router.py`, `frontend/url-validator.html`): new `suggested_url` field on `ValidationResult` + `build_suggested_url(parsed, issues)` helper that rebuilds a corrected URL from parsed components when issues are safely fixable. Main use case is `HIERARCHY_MISMATCH` ("Category X belongs to Kleding, not Meubels") — rewrites the `/products/<wrong-maincat>/` segment using a new `TaxonomyCache.get_maincat_slug_by_name(name)` reverse-lookup (linear scan over ~30 maincats, negligible). Also fixes `UPPERCASE_IN_PATH`, `FRAGMENT_PRESENT`, tracking/unwanted params, `DUPLICATE_FACET`, `HAS_BUCKET` by rebuilding from parsed components. Bails (returns "") on blockers: `MAINCAT_NOT_FOUND`, `NO_MAINCAT`, `CATEGORY_NOT_FOUND`, `CATEGORY_DISABLED`, `FACET_NOT_LINKED`, `INVALID_VALUE_ID`, `VALUE_NOT_FOUND`, `INVALID_CHARS`, `EMPTY_FACET_SLUG/VALUE`, `MISSING_PRODUCTS_PREFIX`, `DOUBLE_MAINCAT`. Green-highlighted in the table (between Status and Issues), included in Excel download + Copy-for-Excel. **(b) Trailing-slash rule** for Suggested URL: URLs with `/c/` (facets) get no trailing slash; URLs without `/c/` (category-only, incl. cases where `/r/` was stripped) keep a trailing slash — matches Beslist's canonical convention. **(c) Column pruning**: removed Maincat / Category / Facets columns from the on-screen table (too noisy at a glance). Excel download + Copy-for-Excel keep Maincat + Category; Facets Valid/Total dropped from both exports. **(d) Tab-source bug fix**: after pasting + validating manual URLs then switching to Upload tab, the old logic always preferred `urlInput` textarea content if non-empty, so uploaded file URLs were ignored. Rewrote `startValidation` to pick source strictly by active tab (`#manualTab.contains('active')` → textarea; else `uploadedUrls`) with no cross-source fallback. **(e) Progress bar style**: replaced the card-wrapped 8px plain bar with FAQ-generator-style 25px striped + animated bar, percentage label on the right now kept in sync per batch. **(f) Upload UX**: file uploader shrunk to 66.66% width; Validate button now disabled + "Loading file..." label from file pick until the `/api/url-validator/upload` response arrives (big 234k-URL files take several seconds to parse server-side, and users could click Validate before `uploadedUrls` was populated). **(g) Notice text** simplified to `Showing X-Y of Z results` (dropped the "Click Download Excel for the full list" tail — pagination makes it redundant). **(h) Helper text removed**: the `.xlsx/.csv/.txt` upload hint. **(i) Footer added**: standard `Digital Marketing tools by Joep van Schagen - 2026`. **(j) Orange selected state**: All/Valid/Warnings/Errors filter buttons all turn `#CC5500` (dm-tools `--color-button`) when active — first pass used Bootstrap's brighter `#fd7e14` and the user flagged the mismatch. Commits `e27b3d6`, `5c290d4`, `0114a7b`, `9af5ef2`, `46201f2`, `ece61c0`, `500dbb1`, `558e844` #claude-session:2026-04-20

- [x] URL Validator — scale + layout pass. **(a) Batching**: frontend now chunks `urls` into 50k-sized requests and merges results/totals client-side, so submissions >50k no longer hit the backend's `HTTPException(400, "Maximum 50,000 URLs per request")` cap (`url_validator_router.py:43`). Progress bar shows `batch N/M (K URLs)`. Backend limit left in place as per-request safety valve. **(b) Freeze fix**: 44k results page-unresponsive dialog traced to per-row `createElement + appendChild + addEventListener` in `renderResults`. Rewrote to build one `parts[]` template-string array → single `tbody.innerHTML` assignment, plus event-delegation on `<tbody>` for row-expansion clicks. **(c) Pagination**: ported the mc-id-finder pattern (`frontend/mc-id-finder.html:142-153`) — per-page select (25/50/100), SVG prev/next `.btn-page` buttons, `page-info` span. Replaces the earlier 1000-row cap; filter changes reset `currentPage=1`. **(d) DMA+ table layout**: results table restyled to match DMA+ run history — `table-sm table-bordered`, `#f0f0f0` header with `2px solid #dee2e6` bottom border, 8/12 header padding, 6/12 body padding, 0.85rem body font. **(e) Active-filter orange**: All/Valid/Warnings/Errors buttons all turn `#CC5500` when active (dm-tools brand orange from `--color-button` — first pass used Bootstrap's `#fd7e14` and looked off). **(f) Single-line issues with one scrollbar**: dropped the per-cell `.issues-scroll overflow-x` wrapper; now `white-space: nowrap` on all row cells and Bootstrap's `.table-responsive` provides one horizontal scrollbar at the bottom of the whole table. **(g) Uniform row heights**: `.url-result-row { height: 38px }` + nowrap on every `td` forces every row to the same single-line height (one wrapping cell used to blow the row taller). **(h) Misc**: removed the "Upload .xlsx, .csv, or .txt file. Looks for a column named 'URL'…" helper text; added the standard `Digital Marketing tools by Joep van Schagen - 2026` footer. File: `frontend/url-validator.html` #claude-session:2026-04-20

- [x] DMA Bidding parity with DMA+ — (a) history table restyled to match DMA+'s visual: plain card header, `table-sm table-bordered`, gray 8/12 header row, 6/12 body cells, outline badges (border + colored text on transparent bg) instead of solid pills. Columns folded 8→6 (Time / Country / Date Range / Changes / Status / Export); DRY RUN folded into an inline badge next to the status. (b) Renamed DMA+ section header "Change History" → "Run History" so both tools use the same terminology. (c) **Persisted run history to disk** — `backend/data/dma_bidding_history.json` with load-on-import + save-on-every-mutation, cloned from DMA+'s pattern (`_load_run_history_from_disk` / `_history_prepend` / `_history_clear`). Three mutation sites (insert / cap-pop / clear) now funnel through helpers. Especially important because dashboard uvicorn runs without `--reload` → previously every kill+respawn wiped every past DMA Bidding run. Commits `fee1382`, `5a8ac51` #claude-session:2026-04-19

- [x] DMA Bidding date-range UI — replaced the two number-based "days ago" inputs with native `<input type="date">` calendar pickers in a FAQ-style flex bar (`d-flex gap-3 align-items-end` with width-fixed controls: Country 90px, dates 160px). Backend API stays unchanged — frontend converts picked dates to `start_days_ago`/`end_days_ago` ints via `daysAgoFromInput()` before submitting, so no router surgery needed. Seeded at today-9 / today-3 so existing muscle memory still works. Added sanity check: both dates required, start must be ≥ end. Commits `920a4f7`, `1a65080` #claude-session:2026-04-19

- [x] DMA Bidding layout overhaul — brought in line with the rest of the toolkit. (a) Wrapped content in `row/col-md-10 mx-auto` so the page is the narrower-centered-column width all other tool pages use (was full `container` width, visually inconsistent with dma-plus / faq / canonical / 301-generator). (b) Restructured Run card into 4 clean rows: Country/Start/End → Campaign-names textarea (col-md-7, not full-width) → Filter-mode radios → Run bar. (c) Moved Filter-mode radios from top-right to **under** the Campaign-names textarea (reads top-down: what to match → how to match); radios stacked vertically; disabled + `opacity: 0.5` until the textarea has input (meaningless empty-form option otherwise), toggled via `oninput="updateFilterModeState()"`. (d) Dropped the ON/OFF form-switch + red "LIVE MODE — changes will be applied!" warning in favour of a DMA+-style plain Dry Run checkbox inline with the Run button (default checked — live-mode users still get the native confirm prompt). (e) History table got `min-width: 720px` inside the existing `table-responsive` wrapper so 8 columns scroll horizontally in the narrower layout. (f) Fixed pre-existing stale `colspan=6` on the Loading row (table has 8 cols). Commits `cf1601c`, `a4044d2`, `e0eb44b`, `28d012d` #claude-session:2026-04-19

- [x] Restarted uvicorn on WSL (PID 340362 → 532162) because the dashboard's Windows Task Scheduler startup script launches uvicorn without `--reload`, so none of the audit-sweep code changes were picked up until a manual restart. Lesson: any dm-tools fix pushed to main does not reach the live server until the uvicorn process is killed and respawned; FastAPI "auto-reload" doesn't apply in this deployment. Worth adding `--reload` to the Task Scheduler script, or at least a restart hook tied to the dm-dashboard repo #claude-session:2026-04-19

- [x] Dashboard-wide audit sweep — 8 parallel audit agents per tool group, then verified + fixed verified findings. **Critical fixes**: (1) `main.py` `/api/validate-links` + `/api/validate-all-links` had swapped executemany tuples — every link correction since the code was written was a silent no-op (`SET content=<url> WHERE url=<html>`, zero rows matched, `conn.commit` happy); (2) `thema_ads_db.py` + `database.py` missing 4 ALTER TABLE ADD COLUMN IF NOT EXISTS (`batch_size`, `is_repair_job`, `theme_name`, `ad_group_name`) for columns the service INSERTs into — live DBs had them from manual ALTERs, fresh DBs would've exploded; (3) `main.py:/api/export/json` queried `get_output_connection()` (Redshift) against a local-PG table — returned 0 rows; sibling xlsx endpoint already used the right connection; (4) `thema_ads_service.py` pause-job had no check in the customer processing loop — paused jobs chewed through the full batch; (5) thema_ads completed-with-errors collapsed to plain 'completed'; (6) `database.py:init_db` `conn.close()` on pool connection instead of `return_db_connection()`. **P1 sweeps**: (7) `gpt_service.fix_truncated_urls` regex only matched absolute `https://…/p/` hrefs; relative `/p/…` hrefs from GPT slipped through; (8) ~40 `innerHTML = \`<div>Error: ${error.message}</div>\`` XSS sites escaped via shared `escapeHtml` helpers in app.js/faq.js/thema-ads.js + indexnow.html + index-checker.html; (9) `url_validator_service` slug lookups now lowercase at both CSV load and `.get()` call; (10) `canonical_service` facet-removal regex got `IGNORECASE` and `(?:/|$)` anchor for trailing-slash URLs; (11) `campaign_processor.py` hardcoded `c:/Users/JoepvanSchagen/...` paths moved behind `DMA_EXCEL_PATH` env var; (12) Google Ads error truncation bumped from 100 to 300 chars; (13) `batch_api_service.py` JSON-line parser skipped empty lines, empty-dict guard on `list(values())[0]`, dead tail `os.remove`; (14) `faq_service.py` markdown fence extraction now handles missing closing fence; unused `import random` removed; (15) `unique_titles.py:get_titles` LIMIT param SQL-injection hardening; (16) `dma_plus_service.py` `_tasks` dict now guarded by `_tasks_lock`; (17) `mc_id_finder_service` unsupported-country-code no longer KeyErrors, cursor in `with`-block; (18) `dma_bidding_service` records mutation_result even when target strategy isn't configured (no more silent "moved but not really"); (19) `index_checker.py` standalone quota 1000 → 2000 to match service. **False positives (audit wrong, skipped)**: redirect_301 IGNORECASE logic was fine; rfinder_service fetchone-None guard was fine; google_ads_helpers `script_label` ordering was fine (Python late-binds); daily_automation `/cancel/all` is a documented sentinel endpoint. **Deferred**: campaign_processor cancel-plumbing (needs task_id threading through processors — design change, out of scope); thema_ads label-missing fallback (deliberate degradation path); keyword_planner partial-batch retry (complex); content_publisher empty-input success shape (cosmetic); large P2 refactors (duplicate validate-links endpoints, 4x build_listing_tree helpers, 10+ dead process_*_sheet functions) #claude-session:2026-04-18

- [x] DMA+ validate_trees export — dry-run export was campaign-names-only because `_parse_affected_entities` doesn't recognise validate_trees' log format (`📁 Campaign: PLA/X` without `(N ad group(s))` suffix, 3-space-indented `   🔧 PLA/ag_a: …` status lines vs parser's `\s{4,}` requirement). Ad groups + tree descriptions silently dropped. Fix: in `_run_operation`, when `operation == "validate_trees"`, override `affected` by walking `result_data["details"]` (which already has `{campaign, ad_group, status, message}` per ad group). Split into `campaign_ad_group_pairs` (created/error rows → main Campaigns sheet) and `skipped_pairs` (skipped rows → dedicated Skipped sheet so 1395 convention-mismatch rows don't swamp the 990 actual errors). Frontend `exportRow` emits the new "Skipped" sheet with columns Campaign / Ad Group / Reason when `aff.skipped_pairs` is present. Files: `backend/dma_plus_service.py:635-670`, `frontend/dma-plus.html:exportRow` #claude-session:2026-04-18

- [x] n8n indexnow_submitter — added `validate_suppliers` Code node that mirrors `backend/link_validator.py` (queries ES `product_search_v4_nl-nl_{maincat}/_search` on `pimId` for numeric URLs and on `id` for V4 UUIDs), keeps only URLs with `shopCount >= 3`. Pushed SQL `limit 10000 → 15000` and moved the 10K daily cap from `dedup_and_batch` to AFTER validation so rejected URLs no longer steal daily quota. Slack summary now reports rejected (<3 suppliers) and post-validation truncation separately. File: `C:\Users\JoepvanSchagen\Downloads\indexnow_submitter.json` #claude-session:2026-04-17
- [x] DMA+ export — replaced client-side CSV writer in `frontend/dma-plus.html` with SheetJS 2-sheet .xlsx: **Status** (Time/Operation/Country/Status/Summary) + **Campaigns** (3-col zip of campaigns/ad groups/product trees). Fixes the `â†’` mojibake the old CSV had (xlsx uses UTF-8 XML internally, no BOM issue). SheetJS loaded via `async` after inline-script hang incident. File: `frontend/dma-plus.html:247-248,495-528` #claude-session:2026-04-17
- [x] DMA+ backend — POST `/api/dma-plus/start` returned only after multi-minute Taxonomy API walk during workbook build. Moved ALL heavy init (resolve_maincat, `_build_exclusion_workbook`, Google Ads client) into `_run_operation` thread; `start_operation` now just seeds a task record with `status:"queued"` and spawns. POST returns in <10ms. File: `backend/dma_plus_service.py:570-616` #claude-session:2026-04-17
- [x] DMA+ cancel — cancel flag was set but never checked. Added `TaskCancelled` exception + `_check_cancelled(task_id)` helper called at every phase boundary (before maincat resolve, before workbook build, after build, before client init, after client init, before main processing). Widened `cancel_task` to accept `queued`/`initializing`/`running`. Note: mid-run cancel inside `cp.process_*` still requires a callback plumbed through `campaign_processor.py` — out of scope for this session #claude-session:2026-04-17
- [x] DMA+ export missing campaigns — `_parse_affected_entities` was reading the display-truncated `result_data["log"]` (`[-5000:]`), so any campaign whose `📁 Campaign:` line was earlier than the last 5k chars was silently dropped from the export. Added `full_log` local across all 5 branches of `_run_operation`; parsing now uses the full stdout capture, display log still truncated. File: `backend/dma_plus_service.py:380-523` #claude-session:2026-04-17
- [x] DMA+ log summary — `process_exclusion_sheet_v2` now reports per-maincat `Categories in {name}: N` and `Campaigns found in {name}: X/Y (Z%)` where Y = categories × cl1 groups. Missing-field log now names the missing column (`maincat_id` and/or `custom_label_1`) instead of generic "Missing required fields". File: `backend/campaign_processor.py:5248-5295,5462-5475` #claude-session:2026-04-17
- [x] DMA+ missing-campaign visibility — processor used to silently skip `PLA/{cat}_{cl1}` names absent from the pre-fetched Google Ads cache. Added inline `⚠️  Campaign not found in Google Ads cache: {name}` log at the point it would normally print `📁 Campaign:`, plus a consolidated per-group line `⚠️  N campaign name(s) missing from Google Ads cache: name1, ..., name10 (+X more)`. Tells you exactly which `PLA/{cat}_{cl1}` the exclusion query for that cl1 couldn't hit #claude-session:2026-04-17
- [x] DMA+ bidcat migration — **REVERTED same session**. Hypothesis was that DMA campaigns are named `PLA/{bidcat}_{cl1}`; user confirmed after testing that campaigns are actually on deepest_cat level. `_fetch_all_cat_ids_from_taxonomy_api` restored to the original deepest-cat walk. The missing-campaign visibility (⚠️ log lines) added in the same commit was kept. File: `backend/dma_plus_service.py:215-265` #claude-session:2026-04-17
- [x] DMA+ reverse operations — added `reverse_inclusion` (removes ad groups) and `reverse_exclusion` (removes shop exclusions). Backed by existing `cp.process_reverse_inclusion_sheet_v2` + `cp.process_reverse_exclusion_sheet`. New `_build_reverse_exclusion_workbook` creates the 'verwijderen' sheet layout + cat_ids; reverse_inclusion reuses `_build_inclusion_workbook` ('toevoegen' sheet, same structure). Frontend got two new op cards and a `SHOP_INPUT_OPS` constant so the 4 shop-tab-aware ops all share gating. Files: `backend/dma_plus_service.py`, `backend/dma_plus_router.py`, `frontend/dma-plus.html`, `backend/campaign_processor.py` (parser tweaks) #claude-session:2026-04-17
- [x] DMA+ dry-run, properly — before this it was cosmetic for inclusion/exclusion (checkbox sent, flag ignored). Now `dry_run: bool = False` is threaded through `process_inclusion_sheet_v2`, `process_exclusion_sheet_v2`, `process_reverse_inclusion_sheet_v2`, `process_reverse_exclusion_sheet` — mutation calls (campaign/ad group creation, shop exclusion add/remove, ad group removal) are skipped; fake `customers/{cid}/campaigns/DRY_RUN_<hex>` resource names are synthesized so downstream code that extracts IDs via `.split('/')[-1]` still works. Each processor prints a `(DRY RUN: ...)` banner. Inter-step `time.sleep()`s also skipped in dry-run so inclusion dry-runs finish in seconds. Files: `backend/campaign_processor.py`, `backend/dma_plus_service.py` #claude-session:2026-04-17
- [x] DMA+ DM_DASHBOARD label — every campaign + ad group created by `process_inclusion_sheet_v2` is now tagged with a Google Ads `DM_DASHBOARD` label (created lazily via `LabelService.mutate_labels` if missing; resource_name cached per customer_id). New helpers `ensure_dm_dashboard_label`, `apply_dm_dashboard_label_to_campaign`, `apply_dm_dashboard_label_to_ad_group` handle the lookup/create/attach. "Already attached" errors on re-runs are treated as success. Skipped in dry-run since fake resource names would fail. File: `backend/campaign_processor.py:84-170` #claude-session:2026-04-17
- [x] DMA+ history persisted to disk — `_history` deque was in-memory only, so server restarts (frequent during dev) wiped Change History. Added `backend/data/dma_plus_history.json` load-on-import / save-on-mutate via a new `_history_append(entry)` helper and a `_history_lock`. `clear_history` also persists the empty state. File: `backend/dma_plus_service.py:58-110` #claude-session:2026-04-17
- [x] DMA+ per-row remove — new red Remove button next to Export in Change History. Fires `DELETE /api/dma-plus/history/{task_id}`; backend `remove_history_entry` rebuilds the deque without the matching task and writes the updated JSON. Files: `backend/dma_plus_service.py`, `backend/dma_plus_router.py`, `frontend/dma-plus.html` #claude-session:2026-04-17
- [x] DMA+ export polish — Campaigns sheet now pairs each campaign with its ad group on the same row (new `campaign_ad_group_pairs` field from `_parse_affected_entities`, built by walking the log linearly and tracking "current campaign" across `📁 Campaign:` and `CAMPAIGN N/M:` headers). Missing campaigns moved to a dedicated "Missing campaigns" sheet. Affected Product Trees populates for exclusion via a synthesized `🌳 Tree to modify:` line per campaign. Fixed `_summarize_result` for reverse ops (was dumping the raw dict in Change History). Files: `backend/dma_plus_service.py:606-710`, `frontend/dma-plus.html:495-545` #claude-session:2026-04-17
- [x] DMA+ reverse_exclusion export empty — the processor's logs were aggregate-only (no per-campaign / per-ad-group / tree lines), so the parser had nothing to latch onto. Added `📁 Campaign:`, `🌳 Tree to modify:`, per-ad-group `⏭️/✅/❌ PLA/ag: N removed, M not found` lines, and `⚠️ Campaign not found in Google Ads cache` for misses. Also captures `maincat_name` during row grouping for the tree description. Now produces the same quality export as exclusion. File: `backend/campaign_processor.py:process_reverse_exclusion_sheet` #claude-session:2026-04-17
- [x] DMA+ inclusion pairing — inclusion's logs use `Creating campaign: PLA/X store_a` and `──── Ad Group N/M: PLA/Y ────` formats that neither pairing regex recognized, so Campaigns and Ad Groups showed flat (unpaired) in the export. Added a `Creating\s+campaign:\s+(PLA/.+?)\s*$` regex to the campaign header chain and extended the ad-group regex with `(?:\s+\d+/\d+)?` to accept the `N/M` marker. Parser-only change; no processor edits. File: `backend/dma_plus_service.py:_parse_affected_entities` #claude-session:2026-04-17
- [x] DMA+ ad groups missing for space-named categories — exclusion / reverse_exclusion ad-group status regex `(PLA/[^:\s()]+):\s` stopped at the first whitespace, so categories like `Accessoires elektrisch gereedschap`, `CO2 Meters`, `Afvoerbuizen & hulpstukken` (33 of 846 in the user's last run) were silently dropped. Their campaign header still matched (campaigns regex used `.+?`), so the pair flushed with `("PLA/X with spaces_a", "")` — empty ad-group cells in the export. Changed character class to `[^:]+` so the capture terminates on the colon (the actual log delimiter) instead of whitespace. Diagnosed entirely from the persisted history JSON, no need to re-import the user's Excel. File: `backend/dma_plus_service.py:_parse_affected_entities` #claude-session:2026-04-18
- [x] DMA+ tree column alignment — Affected Product Trees was sorted alphabetically while Campaigns/Ad Groups came from log-order pairs, so the three columns were misaligned. Made `campaign_ad_group_pairs` 3-tuples `(campaign, ad_group, tree)` — tree is captured from the most recent `🌳 Tree…` line for the current campaign and repeated across all that campaign's ad-group rows. Frontend reads `pair[2]` and falls back to the old flat `trees[i]` for legacy 2-tuple history rows. Files: `backend/dma_plus_service.py:_parse_affected_entities`, `frontend/dma-plus.html:exportRow` #claude-session:2026-04-18
- [x] DMA+ reverse_exclusion silent-failure visibility — the per-row "✅ Successful" counter marked rows TRUE whenever the shop ended up not-excluded for any reason (removed OR was already not excluded), so "3 ok, 0 failed" hid runs where 0 mutates actually happened. Added run-wide counters `run_total_removed / run_total_already_not_excluded / run_total_mutate_errors / run_total_batch_calls` rendered as separate summary lines, plus a "⚠️ No exclusions were actually removed" footer when `removed == 0 && batch_calls > 0`. Beefed up `reverse_exclusion_batch` exception handler to walk `gae.failure.errors` and print full per-error details instead of `str(gae)[:100]`. End-to-end verification on cl1=c (282 batch calls, all 282 wibra.nl exclusions removed cleanly) confirmed the function works through the same code path the dashboard uses — past silent failures were almost certainly transient. Files: `backend/campaign_processor.py:reverse_exclusion_batch`, `process_reverse_exclusion_sheet` #claude-session:2026-04-18
- [x] DMA+ stats row + DRY RUN badge — the Results stats row was showing "Processed / Success / Failed" from workbook-row booleans, which doesn't convey what the run actually did in Google Ads. Replaced for the 4 mutating ops with **Campaigns / Ad Groups / Trees** counts derived from `_parse_affected_entities`' parsed sets. `edited_campaigns/ad_groups/trees` surfaced on `result_data` and on the history entry so the Change History can render the same numbers without re-parsing the log. `reverse_inclusion` doesn't emit `🌳 Tree…` lines (ad-group removal implicitly removes the tree), so trees falls back to ad-group count. Also added a `dry_run` field to every history entry (completion / cancellation / error paths) and a brown-outlined **DRY RUN** badge next to the operation name in Change History. Caveat: for exclusion / reverse_exclusion the counts include `⏭️ no-op` ad groups because the parser captures every status line; the exact "actually mutated vs no-op" split still lives in the per-op log summary block. Files: `backend/dma_plus_service.py:_run_operation`, `frontend/dma-plus.html` (stats row + history row) #claude-session:2026-04-18
- [x] DMA+ audit — spot-checked dry-run gating across every processor call site. Findings: (a) `validate_ads_for_campaigns` has no `dry_run` parameter, so Dry Run + Fix Mode still writes — unfixed, flagged to user; (b) `validate_cl1_targeting_for_campaigns` and `validate_ads_for_campaigns` both try to save per-run xlsx to a hardcoded `C:\Users\JoepvanSchagen\...` path — throws on Linux/WSL (cl1 unhandled, ads caught); (c) `validate_trees` creates a tempfile via `NamedTemporaryFile(delete=False)` and never cleans up; (d) `process_exclusion_sheet_v2` summary still shows single "✅ Successful" counter without the actual-mutation split I added to reverse_exclusion. None of these are the active bug but worth tracking if they resurface #claude-session:2026-04-18
- [x] DMA+ audit fixes (items 1–4) — (1) `validate_ads_for_campaigns` gained a `dry_run` parameter; banner "(DRY RUN: missing ads will be reported but no ads will be created)" when `fix and dry_run`; `add_shopping_product_ad` call skipped and replaced with `[DRY RUN] Would create shopping ad` log. `_run_operation` forwards `dry_run=dry_run`. (2) Removed the hardcoded `C:\Users\JoepvanSchagen\...` xlsx writes from both `validate_cl1_targeting_for_campaigns` and `validate_ads_for_campaigns` — the same data already lives in `stats['details']` and is exported via the dashboard. (3) `validate_trees` in `_run_operation` now wraps the `NamedTemporaryFile(delete=False)` in `try/finally: os.unlink(excel_path)` to stop leaking files under `/tmp`. (4) `process_exclusion_sheet_v2` got the same run-wide action counters as `process_reverse_exclusion_sheet` (`run_total_added / run_total_already_excluded / run_total_mutate_errors / run_total_batch_calls`) + a "⚠️ No exclusions were actually added" footer when `added == 0 && batch_calls > 0`. Files: `backend/campaign_processor.py`, `backend/dma_plus_service.py` #claude-session:2026-04-18
- [x] DMA+ label creation bug — `ensure_dm_dashboard_label` failed with `Unknown field for Label: description` because the Google Ads API puts description on `text_label`, not directly on `Label`. Changed `op.create.description = …` → `op.create.text_label.description = …`. Verified live: label created (id 22186996514) with correct description persisted. File: `backend/campaign_processor.py:ensure_dm_dashboard_label` #claude-session:2026-04-18
- [x] Add URL Validator tool — validates beslist.nl category/facet URLs against Taxonomy API v2 without crawling. Checks: category exists + enabled, facets linked to category, facet values exist + seoPriority, structural errors (double /products/, double maincat, tracking params, uppercase, /r/ buckets stripped). Excel/CSV upload + paste input. Files: `backend/url_validator_service.py`, `backend/url_validator_router.py`, `frontend/url-validator.html`. Nav added to all 20 frontend pages + dashboard card #claude-session:2026-04-16
- [x] Add DMA+ tool — web UI for campaign_processor.py (5 operations: include/exclude shops, validate CL1/ads/listing trees). Copied campaign_processor.py + google_ads_helpers.py from dma_script. Features: NL/BE dropdown, Excel upload + Quick Shop Input (auto CL1 a/b/c, budget EUR 50), maincat name↔ID cross-matching, progress bar, change history with per-row CSV export (affected campaigns/ad groups/trees). Live category index from Taxonomy API v2 with 1h cache (3,500+ entries, fallback to cat_urls.csv). Fixed bid strategy names (DMA+ shops A/B/C). Files: `backend/dma_plus_service.py`, `backend/dma_plus_router.py`, `backend/campaign_processor.py`, `backend/google_ads_helpers.py`, `frontend/dma-plus.html`. Nav added to all pages + dashboard card #claude-session:2026-04-16
- [x] Canonical analysis with /mode/ control group — queried Redshift for treatment (pa.jvs_canonicals, 76k URL pairs) vs new control group (/mode/ + /c/ URLs, ~12.5k). Results: visits lift +10.2 pp, revenue lift +39.6 pp (much stronger than old all-category control +4.9 pp). Updated the original PDF with new data, charts, and conclusions. Output: `C:\Users\JoepvanSchagen\Downloads\claude\canonicals_analysis.pdf` #claude-session:2026-04-16
- [x] Investigated FAQ regeneration loop on merk~781 (Illy koffiezetapparaten) — products have borderline shopCounts (2-4) that fluctuate around the validation threshold. Validator flags them as gone, FAQ resets to pending, regeneration picks up same products from Search API. Identified in content_history: same URL reset 4 times since Feb 2026 #claude-session:2026-04-16
- [x] Investigated GSC index reporting feasibility — URL Inspection API not suitable (per-URL, 8k/day limit). GSC API does not expose Page Indexing report. Recommended Sitemaps API (indexed/submitted counts per sitemap) as best alternative #claude-session:2026-04-16

- [x] Consolidate dm-tools and dm-dashboard into one repo — two parallel repos had drifted over weeks. Back-ported all dashboard improvements into dm-tools so dm-tools became a superset: env-gated auth middleware (DASHBOARD_PASSWORD/DASHBOARD_SECRET), env-driven CORS (CORS_ORIGINS), DMA bidding NL/BE country selector + MCC-owned strategies, UNIQUE_TITLES_API_KEY from env, Task Scheduler module gated on ENABLE_TASK_SCHEDULER, /api/config endpoint for frontend feature flags, daily_automation.py parameterized (BASE_URL / DISABLE_SSL_VERIFY / optional login). Then force-pushed dm-tools/main to dm-dashboard so both remotes match. Files touched: backend/main.py, backend/database.py, backend/daily_automation.py, backend/unique_titles.py, backend/dma_bidding_service.py, backend/dma_bidding_router.py, frontend/dashboard.html, frontend/dma-bidding.html, new task_scheduler_* files #claude-session:2026-04-15
- [x] Swap git remotes — renamed `origin` (dm-tools) → `dm-tools-old`, and `dm-dashboard` → `origin`. Plain `git push` now goes to dm-dashboard. dm-tools repo will be archived on GitHub later #claude-session:2026-04-15
- [x] Enable password protection on localhost dashboard — added DASHBOARD_PASSWORD=lakers24 and random 64-char DASHBOARD_SECRET to local .env. Restarted backend (detached nohup, PID persists across terminal). Verified: `/` → 307 to /login, wrong password → 401, correct → 303 + session cookie, /api/config returns `{"task_scheduler_enabled":false}` locally #claude-session:2026-04-15

- [x] Fix generic FAQs on faceted URLs — FAQ prompt didn't include selected facets, so brand/color/material-filtered pages got generic category questions. Added facet context + conditional instruction to both `faq_service.py` and `batch_api_service.py`. Reset 18,047 generic FAQs (8% of 222K faceted URLs) to pending for regeneration. Files: `backend/faq_service.py:572-593`, `backend/batch_api_service.py:159-178` #claude-session:2026-04-14
- [x] Add "Bij het kiezen van" to discouraged opening phrases in kopteksten prompts — too many generated texts started with this phrase. Added soft variation rule (not a ban) to both subcategory and main category system prompts in `gpt_service.py`. Files: `backend/gpt_service.py:80,159` #claude-session:2026-04-14
- [x] Fix stijl adjective placement in AI titles — AI was sometimes putting "Industriële" at the end of titles ("Barkrukken Industriële"). Extended prompt rule 4 in `ai_titles_service.py` to explicitly name stijl adjectives alongside colors/materials, with "NOOIT aan het einde" clause. Benefits all 7 `stijl*` facet families (~44k URLs). Reset 1,994 URLs matching `stijl_test~8064049` to pending #claude-session:2026-04-13
- [x] Add t_wanddeco to CATEGORY_OVERRIDE_FACETS in AI title generation — when URL has `t_wanddeco` facet, suppress the generic `category_name` ("Wanddecoratie") so the facet value ("wandplaten") carries the product noun. Strips category_name from H1 prefix/suffix and prevents re-append. Reset 61 URLs to pending. File: `backend/ai_titles_service.py:453-475` #claude-session:2026-04-13
- [x] Fix V4 product URL validator — ES query was on `pimId` but V4 UUIDs live in the `id`/`groupId` fields, so phase-1 lookup always returned 0 hits. Every V4 link on content and FAQ pages was silently skipped by the validator (never replaced when slugs changed, never flagged gone). Changed `"pimId"` → `"id"` in `query_elasticsearch_by_plpurl`, and now treat phase-1 misses as GONE (reliable signal, wildcard fallback no longer needed). Same fix benefits `validate_faq_links` via shared helper. File: `backend/link_validator.py:147-230` #claude-session:2026-04-13
- [x] Fix thema_ads startup error — `list_jobs` query failed with "column must appear in GROUP BY" because `thema_ads_jobs.id` had no PRIMARY KEY (live DB schema didn't match the code's CREATE TABLE IF NOT EXISTS). Rewrote to pre-aggregate counts in a subquery (no outer GROUP BY). File: `backend/thema_ads_service.py:571-589` #claude-session:2026-04-13
- [x] Add missing PKs/sequences/FKs to `thema_ads_jobs`, `thema_ads_job_items`, `thema_ads_input_data` — all three tables had zero constraints in the live DB despite schema files declaring SERIAL PRIMARY KEY. Added sequences with defaults, PKs on id, and FKs from child tables to `thema_ads_jobs(id)` with ON DELETE CASCADE. All tables empty, zero risk #claude-session:2026-04-13
- [x] Enable uvicorn `--reload` — added to `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` and restarted the running process. Future backend edits hot-swap via WatchFiles without manual restart #claude-session:2026-04-13
- [x] Delay DM Tools Dashboard scheduled task by 10 minutes after login — added `<Delay>PT10M</Delay>` to the LogonTrigger so WSL has time to be ready before uvicorn binds :8003 #claude-session:2026-04-13

- [x] First FAQ Bulk API run completed — 29,076 FAQs generated via OpenAI Batch API (6 chunks of 5K). FAQ content now at 230,241 total. 4 failures, 0 errors. Took ~8 hours (mostly OpenAI queue time) #claude-session:2026-04-11
- [x] Fix batch API 200MB limit — split into 5K-request chunks. OpenAI has 200MB file size limit for gpt-4o-mini batches, 29K prompts exceeded this. Both FAQ and kopteksten batch pipelines updated #claude-session:2026-04-10
- [x] Fix unique titles batch UI — progress bar instead of button text, aiBatchPolling flag set immediately to prevent loadAiStatus from resetting UI. Removed 158,742 faulty URLs (/r/ URLs, populaire_themas_accessoires, type_parfum, pl_pennen) from all 6 DB tables, exported to ~/faulty_unique_title_urls.xlsx #claude-session:2026-04-10
- [x] Frontend polish — standardized page widths to col-md-10 across all tools (unique-titles was col-lg-8, redirects/keyword-planner/url-checker/redirect-checker were col-md-11). Unified input-group layout for batch/workers fields. Buttons right-aligned across all sections. Cleaned up MC ID Finder, URL Checker, Redirect Checker, R-Finder, Redirects. Added Bulk API to unique titles. Fixed FAQ recent results X-button overflow (CSS grid). Pre-filled title suffix in unique titles Add/Edit form #claude-session:2026-04-10
- [x] Add OpenAI Batch API integration — new `batch_api_service.py` for bulk FAQ and kopteksten generation (50% cheaper). "Bulk API" checkbox in frontend, 4 new endpoints, background thread with phase-based progress. Prepares prompts with 50 concurrent Product Search API threads, uploads JSONL, polls OpenAI, saves results in bulk #claude-session:2026-04-10
- [x] Optimize FAQ/kopteksten query performance — converted 4 LEFT JOIN queries to NOT EXISTS in main.py (FAQ URL selection 4.2s→190ms = 16.5x faster). Increased DB pool maxconn 20→60, worker limits 20→100, frontend defaults to 50 workers #claude-session:2026-04-10
- [x] Remove 29,632 winkel facet URLs from all 6 DB tables — Product Search API returns no facet data for winkel-filtered URLs #claude-session:2026-04-10
- [x] Remove 102 merk~0 URLs from all 6 DB tables #claude-session:2026-04-10
- [x] Score all 1M+ unique titles — ran `score_titles.py` (GPT-4o-mini, 25/batch, 20 workers) across 684K unscored titles, two parallel processes, 0 errors, ~4.4 hours. Final: 1,023,808 titles scored, avg 8.00, 70% score 8+. Exported to `~/unique_titles_scored.xlsx` (41MB) #claude-session:2026-04-09
- [x] Reset 125,436 unique titles with score < 7 to pending for regeneration #claude-session:2026-04-09
- [x] Remove 1,944 bad URLs from unique_titles containing "pricemax" or "+" #claude-session:2026-04-09
- [x] Fix "vases" hallucination — AI translated Dutch "vazen" to English "vases" in 9 titles, reset to pending #claude-session:2026-04-09
- [x] Fix FAQ tracking ghost success records — 45,004 URLs marked 'success' in faq_tracking but no content in faq_content, reset to pending. Also fixed 9 failed→success (had content), inserted 7 missing tracking records #claude-session:2026-04-09
- [x] Fix kopteksten tracking ghost success records — 373 URLs marked 'success' in kopteksten_check but no content, reset to pending #claude-session:2026-04-09
- [x] Fix Kinder+Meisjes/Jongens redundancy in AI title generation — replaced narrow facet-name-based dedup with value-based approach in `ai_titles_service.py:509-523`. Any facet with "Kinder"/"Kinderen"/"Baby" value dropped when "Meisjes"/"Jongens" present. Also strips "Kinder" prefix from category names in H1. Reset 403 affected URLs #claude-session:2026-04-09
- [x] Create dm-dashboard repo (Docker-free version) — standalone version at github.com/joep-1993/dm-dashboard with setup.sh, .env.example, password protection, load_dotenv, all API keys moved to env vars. Added missing themes.py, thema_ads_optimized/, themes/, categories.xlsx #claude-session:2026-04-03
- [x] Fix n8n IndexNow Submitter Slack message — build_summary1 was reading $input (Postgres output) instead of $('build_tracking_insert1') for url_count and response_code #claude-session:2026-04-03
- [x] Add MC ID Finder tool under Google Ads — Redshift lookup for Merchant Center IDs (NL/BE/DE) by shop name. Multi-shop textarea input, country checkboxes for dynamic columns, CSV export. Backend: mc_id_finder_service.py + mc_id_finder_router.py. Fixed: shop_name on wrong table alias, MC ID fields are strings not ints, many shops lack efficy_k_shop join #claude-session:2026-04-02
- [x] Add URL Lookup to FAQ tool — lookup endpoint, FAQ preview with Q&A display, delete & reset to pending. Purple hover on all toggle buttons (View All FAQs, View Full Content, Contract) #claude-session:2026-04-01
- [x] Add DMA Bidding tool — backend service (438 lines), router (5 endpoints), frontend with stats/dry run/include-exclude/results tables/CSV export/history. Ported from standalone DMA_verhogingen_verlagingen.py script #claude-session:2026-04-01
- [x] Background task pattern for recheck-skipped-urls — progress bar + cancel button, matching validate-all pattern, both Kopteksten and FAQ #claude-session:2026-04-01
- [x] Unify URL counts — werkvoorraad as single source of truth for total, added 33 missing URLs to werkvoorraad, fast subqueries (no heavy JOINs), both tools show same total (292,975). Also synced validate-all confirm dialog + button colors between Kopteksten and FAQ #claude-session:2026-03-31
- [x] Quality audit + fixes — removed orphaned dashboard loadStats() JS, added date validation to IndexNow export endpoint, made GSD service account auto-detect, added missing footer to Keyword Planner, styled Test API Connection button #claude-session:2026-03-31
- [x] Build GSD Campaigns tool — full Google Shopping campaign management in DM Tools. Backend: `gsd_campaigns_service.py` (1,247 lines ported from standalone script), `gsd_campaigns_router.py` (7 API endpoints). Frontend: `gsd-campaigns.html` with stats cards, campaign table (sortable, paginated, filterable), run script with date/shop filters, pause/enable/remove per campaign, activity log, xlsx export. Queries Google Ads API for campaigns with `GSD_SCRIPT` label across 5 accounts (NL/BE/DE CPR + NL/BE CPC) #claude-session:2026-03-31
- [x] Frontend redesign — new dropdown menu system (4 categories: Generators, Indexation, Google Ads, SEO tools), modern stroke-style SVG icons, Dashboard frontpage with categorized tool cards (color-coded icon backgrounds, hover effects), responsive topbar with scaling items #claude-session:2026-03-31
- [x] Tool renames: Kopteksten Generator→Kopteksten, FAQ Generator→FAQ's, 301 Generator→Redirects, Canonical Generator→Canonicals, SEO Index Checker→Index Checker #claude-session:2026-03-31
- [x] Frontend polish — FAQ validate-all behavior synced with Kopteksten, Thema Ads tabs matched Canonicals layout, Keyword Planner input section cleaned up, Index Checker quota status matched Kopteksten style, IndexNow badges outlined with color coding + per-date XLSX export, button shadows site-wide #claude-session:2026-03-31
- [x] Fix FAQ structured data "item name: N/A" in Google Rich Results — added `"name": page_title` to FAQPage JSON-LD in `faq_service.py:to_schema_org()`, migrated all 204,216 existing `pa.faq_content` rows via pure SQL `regexp_replace` on `schema_org` column. Script: `backend/fix_faq_sql.py` #claude-session:2026-03-31
- [x] Add ROAS >= 130% condition to DMA bid strategy increases (`DMA_verhogingen_verlagingen.py`) — ROAS calculated as DMA/CLA omzet / cost, added to increase rules, console log, email tables, and CSV attachment #claude-session:2026-03-31
- [x] Create simplified basements homepage n8n workflow (`basements_homepage_simple.json`) — processes in-memory instead of writing to DB tables, still checks redirects/duplicates, posts directly to keywords API. Removed SplitInBatches (caused data loss), all items flow inline through HTTP Request node #claude-session:2026-03-31

- [x] Create DMA bid strategy automation script (`DMA_verhogingen_verlagingen.py`) — adjusts campaigns between Level 1/2/3 bid strategies based on DMA/CLA Profit (conversion action "Omzet DMA en CLA" - cost), OPB (conv_value/clicks), and click thresholds. Dry run mode, email report with CSV attachment, test script for profit verification. Account 3800751597, MCC 3011145605 #claude-session:2026-03-30
- [x] Fix IndexNow n8n workflow `submit_to_indexnow` node — enabled fullResponse on HTTP Request, fixed tracking insert to read URLs from upstream node instead of empty response body #claude-session:2026-03-30

- [x] Build CloudFront log downloader script — Python/boto3, downloads .gz logs from S3 bucket to local dir. Self-contained (no config file), supports date/from_date/days/list_only params, resume-safe (skips already-downloaded). Location: `/home/joepvanschagen/projects/cloudfront-logs/` #claude-session:2026-03-26

- [x] Create shared URL validation tracking table `pa.url_validation_tracking` — unifies `no_products_found` skip tracking across kopteksten and FAQ features so both dashboards show identical skipped counts. Migration script merges existing data. Total counts now always add up (processed + skipped + failed + pending). Files: `schema.sql`, `database.py`, `main.py`, `link_validator.py`, `migrate_shared_validation.py` (new) #claude-session:2026-03-20
- [x] Add CSV-based category lookup for kopteksten and FAQ generation — uses `backend/data/cat_urls.csv` (3,557 mappings of URL parts to category names) instead of deriving category from first API product. Falls back to old behavior when URL not in CSV. New module: `backend/category_lookup.py`, modified: `scraper_service.py`, `faq_service.py` #claude-session:2026-03-19
- [x] Fix validate_cl1_targeting_for_ad_group to handle CL4 UNIT nodes — when CL4 (maincat) is a UNIT instead of SUBDIVISION, convert it to SUBDIVISION first (remove UNIT, create SUBDIVISION with same dimension/parent), then add CL1 children underneath. Same pattern as _add_cl0_exclusion_to_ad_group fix. File: campaign_processor.py #claude-session:2026-03-19
- [x] Add main category URL support — 31 main category URLs (`/products/{maincat}/`) now generate broader introductory content using fixed H1 titles (from maincaturls.xlsx), special GPT prompt for general category overviews with product links. URLs added to werkvoorraad, excluded from FAQ generator via faq_tracking skip. Files changed: scraper_service.py (MAIN_CATEGORY_H1 mapping, is_main_category_url()), gpt_service.py (generate_main_category_content()), main.py (routing in process_single_url) #claude-session:2026-03-17
- [x] Create combined n8n workflow `seo_content_pipeline.json` (30 nodes, 5 phases): SEO validation → SEO generation → FAQ validation → FAQ generation → publish → Slack. Single Schedule Trigger (10:00), all phases sequential, 50K URL limits per phase #claude-session:2026-02-23
- [x] Create 5th n8n workflow `5_faq_generator.json` (7 nodes): FAQ generation in bulk (50K URLs) — fetch products from Product Search API, generate FAQs via OpenAI, validate URLs in answers, write to faq_content + faq_tracking #claude-session:2026-02-23
- [x] Fix OpenAI API key in n8n Code nodes: hardcoded key directly in `generate_all_content` and `generate_all_faqs` (n8n `process.env` not reliable for env vars) #claude-session:2026-02-23
- [x] Fix publishing OOM: replaced n8n Code node payload building (244K rows × ~4KB = ~1GB) with call to FastAPI backend's `POST /api/content-publish?environment=production`. Backend handles DB fetch, payload build, and API call in Python. Also tried PostgreSQL `json_agg` approach — also OOMs at 1GB text buffer limit #claude-session:2026-02-23
- [x] Split n8n workflow into 4 independent flows: content_generator (50K URLs), seo_link_validator (50K), faq_link_validator (50K), publisher. All bulk, no loops, no queryBatching. Output: `Downloads/flows/` #claude-session:2026-02-21
- [x] Fix unique titles publish: root cause was 422 case-sensitive duplicate URLs (PG case-sensitive PK vs MySQL case-insensitive unique). Deleted dupes, lowercased 72 remaining URLs with caps. Full 1M+ publish works #claude-session:2026-02-21
- [x] Fix exec_write_results error: removed `queryBatching: "independently"` from all 12 exec Postgres nodes (semicolons in HTML content broke query splitting) #claude-session:2026-02-21
- [x] Fix faq_validation_results missing UNIQUE constraint: added `faq_validation_results_url_key UNIQUE (url)` — required by ON CONFLICT in backend code #claude-session:2026-02-21
- [x] Fix content publishing: increased timeout 600s→1800s, use `data=` instead of `json=` to avoid double-serialization, add progress tracking + timing breakdown. Tested all 252K items (1.36 GB) successfully in ~10 min #claude-session:2026-02-21
- [x] Restore 267,031 missing unique_titles from local DB (lost during Feb 19 migration). Remote DB now 1,035,455 URLs (was 654,902). Filter publish/count to only rows with titles #claude-session:2026-02-21
- [x] Import 113,522 URLs from werkvoorraad into unique_titles (never synced) #claude-session:2026-02-21
- [x] Fix missing categories.xlsx (regenerated from category_descriptions DB table, 3233 categories), fix duplicate URLs in AI title recent results (deduped pa.unique_titles: 1,016,763→654,902 rows, added UNIQUE index on url) #claude-session:2026-02-20

- [x] Add production push to n8n flow: `get_all_publish_content` (FULL OUTER JOIN content_urls_joep + faq_content), `push_to_production` Code node (transforms content_top/content_bottom/content_faq, batches 5000 items, POSTs to `https://website-configuration.api.beslist.nl/automated-content`), updated Slack message with push results #claude-session:2026-02-19
- [x] Optimize n8n link validation: replaced per-item SplitInBatches loop with single `validate_all_links` Code node — ONE ES query per maincat instead of per URL (~31 queries instead of ~100+), all DB operations use bulk SQL (7 queries instead of ~100 per-item), removed 14 nodes replaced with 7 bulk nodes #claude-session:2026-02-19
- [x] Optimize n8n kopteksten generation: added `fetch_all_products` (parallel Product Search API, 5 concurrent) and `generate_all_content` (parallel OpenAI via fetch(), 3 concurrent) Code nodes, bulk DB writes, removed SplitInBatches loop entirely, reduced maxTokens 2000→1000, total nodes 35→20. Requires OPENAI_API_KEY env var on n8n server #claude-session:2026-02-19
- [x] Fix FAQ duplicate issue: deduped faq_content (79,523 dupes, 241K→161K unique) and faq_tracking (94,387 dupes, 243K→149K unique), added UNIQUE constraints on url column, added ON CONFLICT DO UPDATE to main.py INSERT. Root cause: no ON CONFLICT + no UNIQUE constraint on remote DB after Redshift migration. Fixed 58,857 URLs with content but no tracking (showed as "pending") #claude-session:2026-02-19
- [x] Migrate frontend DATABASE_URL from local seo_tools_db to remote n8n vector DB (10.1.32.9) — unified DB for frontend + n8n, works without laptop #claude-session:2026-02-19
- [x] Sync data from local DB to remote: werkvoorraad (271K), kopteksten_check (271K), content (225K merged). Deduped remote tables, added PKs and auto-increment sequences #claude-session:2026-02-19
- [x] Fix n8n kopteksten generator: bulk write_check/write_werkvoorraad (pure SQL, no per-item queries), URL path parsing (new URL() instead of broken split), manual query string (no URLSearchParams in n8n) #claude-session:2026-02-19
- [x] Expose seo_tools_db on port 5433 in docker-compose.yml for external access #claude-session:2026-02-19
- [x] Convert kopteksten generator Python script to n8n workflow JSON (16 nodes: Schedule Trigger → PostgreSQL → Loop → Code → HTTP Request → OpenAI → write results) using Product Search API instead of web scraping #claude-session:2026-02-19
- [x] Convert link validator Python script to n8n workflow JSON (16 nodes: Schedule Trigger → PostgreSQL → Loop → extract links → Elasticsearch → compare/decide → update/delete/backup) #claude-session:2026-02-19
- [x] Export Category Keyword Volumes script to single .txt file with credentials, all code, data files, and run instructions #claude-session:2026-02-17
- [x] Fix 148 singular/plural forms in category_forms.json — words ending in z (94, e.g., hoez→hoes, doz→doos, kluiz→kluis), v (14, e.g., schroev→schroef, schijv→schijf), and wrong -el (40, onderdel→onderdeel, panel→paneel) #claude-session:2026-02-17
- [x] Export 824,450 IndexNow submitted URLs to CSV (url, submitted_date, response_code) #claude-session:2026-02-17
- [x] AI title: doelgroep_drogisterij facet as "voor-facet" — values appended as "voor mannen", "voor vrouwen" etc. instead of before product name, reset 765 URLs #claude-session:2026-02-17
- [x] AI title: aantal_puzzelstukjes facet at end of title (e.g., "Ravensburger Circus Puzzel 500 Stukjes"), reset 432 URLs #claude-session:2026-02-17
- [x] Import 18,329 new URLs into unique_titles (12,529 new), faq_tracking (12,560 new), content_urls_joep (14,889 new), and jvs_seo_werkvoorraad (12,560 new) #claude-session:2026-02-17
- [x] Run facet volume batch for 236,232 facets across 31 maincats (new input with 'cats' sheet for categories) — grand total 2.1B search volume, fix UTF-8 mojibake (2,928 values), 324 min runtime #claude-session:2026-02-17
- [x] Add visits and revenue per facet from Redshift (1.56M URLs since 2024) — matched 74,145 facets, 33.6M total visits, 3.15M total revenue, written to columns I/J of output Excel #claude-session:2026-02-17
- [x] Add URL Checker tool - check status codes, meta title, meta description, H1, product count (from selected facet productCount), canonical URL. Streaming API with max 10 workers / 2 req/sec. Supports paste URLs + file upload (.xlsx/.csv/.txt). Dashboard card + nav links in all 13 pages #claude-session:2026-02-13
- [x] Add structured data itemCondition RefurbishedCondition to structured_data_iphone file #claude-session:2026-02-13
- [x] Check status codes for 67 meubilair facet URLs — 55x 200, 12x 301 (facet values no longer active) #claude-session:2026-02-13
- [x] Bad URL scan: created find_bad_urls.py to detect facet_not_available (400) URLs via Product Search API — partial scan of 155K/916K found ~4,589 bad URLs (~3%) before stopping #claude-session:2026-02-11
- [x] AI title: suffix placement for "Volwassenen" (levensfase) and "Vanaf X jaar" (geschikte_leeftijd) values, reset 200 URLs #claude-session:2026-02-11
- [x] DB cleanup: cross-reference Redirects Admin Excel (459K redirects) — found 41,886 old URLs in DB, added 206,580 missing redirect targets from column B, removed 295,946 /l/ /p/ /r/ URLs #claude-session:2026-02-11
- [x] AI title: brand strip-and-prepend (deterministic brand positioning), print refinement (only "met" when value ends with "print"), color-before-audience prompt rules, category depth-based extraction fix, reset 8,109 parent-level URLs #claude-session:2026-02-11
- [x] AI title post-processing: auto-detect spec/size values (regex number+unit), category name fallback, first-letter capitalization, adjective inflection, color combo suffix placement, kleurcombinatie dedup, "Maat" prefix for bare numbers, print patterns (strepen/bloemen) as met-values #claude-session:2026-02-11
- [x] AI title code-level facet classification: sizes stripped/appended in code (not AI), met-features pre-combined, brand/color/audience dedup, hallucination post-filter #claude-session:2026-02-11
- [x] AI title prompt: sizes after product name, no duplicate brands, condition/format before product, met/zonder features after product in single clause, audiences before product without "voor", anti-hallucination (temp 0.3) #claude-session:2026-02-11
- [x] Fix AI title stop button - chunked ThreadPoolExecutor submission for responsive stopping #claude-session:2026-02-11
- [x] Remove scraping fallback from AI title generation - API-only with error on failure #claude-session:2026-02-11
- [x] Add failing URL to AI title error messages #claude-session:2026-02-11
- [x] Fix Search Titles to accept full URLs and show exact match first #claude-session:2026-02-11
- [x] Clean up databases: remove /l/ URLs (45 each from 4 tables), German URLs (206+108), landing/theme pages (66), garbage URLs (112+48) #claude-session:2026-02-11
- [x] Cross-reference facet names against Dutch facets CSV to verify no remaining German URLs #claude-session:2026-02-11
- [x] Integrate IndexNow tool into dm-tools - service layer, 3 API endpoints, frontend with manual/Excel input, dashboard card, nav links in all tools #claude-session:2026-02-10
- [x] Integrate SEO Index Checker into dm-tools - Google Search Console URL Inspection API, service account quota rotation, frontend with results filtering and CSV download, dashboard card, nav links in all tools #claude-session:2026-02-10
- [x] Fix AI title size placement - sizes (Maat L, XL, 42) now placed after product name, reset 2,231 URLs with maat facets to pending #claude-session:2026-02-10
- [x] Import 15,666 new URLs into kopteksten/FAQ databases and 15,085 into unique titles from ut_new_urls.xlsx #claude-session:2026-02-10
- [x] Filter URLs containing "+" from canonical generator results (no-index URLs) - added SQL exclusion in fetch_urls_from_redshift #claude-session:2026-02-10
- [x] Fix canonical generator facet sorting bug - `kleur` now correctly sorts before `kleurtint` by sorting on facet name only (before `~` separator) #claude-session:2026-02-10
- [x] Add facet volume batch processing - process 140K+ facet values × deepest cats per maincat, SIC/SOD-aware keyword generation, resume-capable runner script, output CSV with search_volume column #claude-session:2026-02-10
- [x] Fix Google Ads API quota rotation - catch gRPC `ResourceExhausted` exception (separate from `GoogleAdsException`) for proper customer_id rotation on 429 rate limits #claude-session:2026-02-10
- [x] Fix Redshift SSL SYSCALL errors - add TCP keepalives and connection health checks to psycopg2 pool #claude-session:2026-02-10
- [x] Add Category Keyword Volumes sub-function to Keyword Planner - combines keyword with 3,535 preloaded category names (singular+plural, both orders), aggregates search volumes per deepest_cat and maincat, Excel download with search_volume_deepest_cat and search_volume_maincat columns, includes maincat name as its own deepest_cat row #claude-session:2026-02-10
- [x] Add Keyword Planner tool - Google Ads search volume lookup with keyword normalization, Excel upload/download, customer_id quota rotation, consistent purple UI styling across all tools #claude-session:2026-02-09
- [x] Fix V4 UUID lookup performance - replaced slow wildcard queries with two-phase pimId lookup + disabled wildcard fallback; fixed `result.get()` bug that falsely marked unfound V4 URLs as "gone" #claude-session:2026-02-09
- [x] Detect and reset 349 cut-off content items - found content truncated mid-sentence using regex, backed up and re-queued for regeneration #claude-session:2026-02-09
- [x] Restore 49,591 falsely reset URLs - validator bug marked V4 URLs as gone, restored from content_history + re-added kopteksten_check entries #claude-session:2026-02-09
- [x] Fix V4 UUID slug change false positives - `query_elasticsearch_by_plpurl()` now uses wildcard on V4 UUID instead of exact plpUrl match; prevents reset loop when product slugs change #claude-session:2026-02-08
- [x] Fix content lookup URL format mismatch - `/api/content/lookup` now queries both relative path and full URL variants #claude-session:2026-02-08
- [x] Restore content from backup for `/products/schoenen/schoenen_430884/c/populaire_serie~12895260` with corrected URL slug #claude-session:2026-02-08
- [x] Increase recheck-skipped-urls max batch size from 200 to 500 #claude-session:2026-02-08
- [x] Fix link validator false positives - ES query failures were marking all products as "gone", now skips batch instead; restored 13,133 falsely reset URLs #claude-session:2026-02-06
- [x] Fix FACET+FACET canonical generator - now fetches URLs containing BOTH facets and removes old facet instead of replacing it #claude-session:2026-02-06
- [x] Add process_check_sheet() to campaign_processor.py - replaces pipe-version shop exclusions (e.g. "Artandcraft.com|NL" → "artandcraft.com") via cat_ids/deepest_cats lookup #claude-session:2026-02-06
- [x] Add replace_shop_exclusions_batch() helper - REMOVE old + CREATE new CL3 exclusions in single atomic mutate call #claude-session:2026-02-06
- [x] Add process_check_cl1_sheet() to campaign_processor.py - checks listing trees for CL1 targeting, rebuilds with CL1 + CL4 if missing, preserves CL3 exclusions #claude-session:2026-02-06
- [x] Add build_listing_tree_with_cl1() tree builder - creates CL3→CL4(subdivision)→CL1 structure supporting multiple maincat_ids #claude-session:2026-02-06
- [x] Add process_check_new_sheet() to campaign_processor.py - replaces CL3 subdivision pipe-version targeting via direct campaign/ad group reference from "check_new" sheet #claude-session:2026-02-06
- [x] Fix stuck pending kopteksten URLs - deleted 32,477 'pending' entries from tracking table that blocked LEFT JOIN pending calculation #claude-session:2026-02-06
- [x] Update ARCHITECTURE.md database section - replaced outdated Redshift-primary docs with current seo_tools_db-primary architecture, 3-database reference table #claude-session:2026-02-06
- [x] Add database connection quick reference and "Stuck Pending URLs" issue to LEARNINGS.md #claude-session:2026-02-06
- [x] Clean up project directory - removed 21 unused files (~85MB total: scripts, data files, logs), moved redirect_checker.py to scripts/, 301-generator_script.js to docs/ #claude-session:2026-02-06
- [x] Remove content_top Docker containers (content_top_db, content_top_app) - dm-tools is the active project #claude-session:2026-02-06
- [x] Optimize campaign_processor.py functions with batch processing and grouping by (maincat_id, cl1) - 90%+ reduction in API calls #claude-session:2026-02-04
- [x] Create add_shop_exclusions_batch() function for adding multiple shop exclusions in one API call #claude-session:2026-02-04
- [x] Optimize process_exclusion_sheet_v2() to group shops and batch exclusion operations #claude-session:2026-02-04
- [x] Optimize process_uitbreiding_sheet() to group shops by campaign and find/create campaign once per group #claude-session:2026-02-04
- [x] Fix Excel encoding issue - convert UTF-8 text incorrectly decoded as Latin-1 (KÃ¼ppersbusch → Küppersbusch) #claude-session:2026-02-04
- [x] Query and aggregate bucket performance data from Redshift for 628K facets in single efficient query #claude-session:2026-02-04
- [x] Export skipped kopteksten URLs with skip_reason to Excel (58,271 URLs from PostgreSQL) #claude-session:2026-02-04
- [x] Add skip_reason column to Redshift pa.jvs_seo_werkvoorraad_kopteksten_check table #claude-session:2026-02-04
- [x] Reset 4,230 skipped URLs (non-no_products_found) to pending for retry #claude-session:2026-02-04
- [x] Fix canonical generator CAT+FACET category filter bug - wasn't filtering by category in fetch_urls_for_rules #claude-session:2026-02-04
- [x] Update campaign_processor.py to use separate file for reverse exclusions (REVERSE_EXCLUSION_FILE_PATH) #claude-session:2026-02-04
- [x] Fix Google Ads authentication in campaign_processor.py - use GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET env vars like working script #claude-session:2026-02-04
- [x] Update process_reverse_exclusion_sheet to use cat_ids mapping (maincat_id → deepest_cats lookup) #claude-session:2026-02-04
- [x] Fix campaign name pattern in reverse exclusion from "PLA/{deepest_cat} store_{cl1}" to "PLA/{deepest_cat}_{cl1}" #claude-session:2026-02-04
- [x] Add rate limiting (0.3s delay) and retry logic (3 attempts with exponential backoff) to reverse exclusion API calls #claude-session:2026-02-04
- [x] Fix google_ads_helpers.py import path - add script directory to sys.path #claude-session:2026-02-04
- [x] Add enable_negative_list_for_campaign function to google_ads_helpers.py (looks up shared set by name) #claude-session:2026-02-04
- [x] Add "Recheck Skipped" feature - re-checks skipped URLs (no_products_found) to see if products are now available, separate button next to "Validate All" on both SEO and FAQ pages, respects batch size and parallel workers inputs #claude-session:2026-02-01
- [x] Create Redirect Checker tool - checks HTTP status codes, redirect URLs, and canonical URLs with parallel workers and rate limiting, click-to-copy results, CSV/Excel export #claude-session:2026-01-30
- [x] Update dashboard styling with purple (#5e4a90) icons, orange (#CC5500) bullet points, modern card layout #claude-session:2026-01-30
- [x] Run canonical REMOVEBUCKET transformation for 780 rules (30 facets, 13 categories) - transformed 7,778 URLs #claude-session:2026-01-30
- [x] Create R-finder tool - new frontend page + API endpoints to find /r/ redirect URLs from Redshift visits data #claude-session:2026-01-29
- [x] Standardize navigation headers across all frontend tools (consistent order, Dashboard button inverted at end) #claude-session:2026-01-29
- [x] Reset all failed/skipped URLs to pending (6,788 URLs) #claude-session:2026-01-28
- [x] Reset all merk URLs to pending for orResult filtering (45,600 archived, 51,401 removed from tracking) #claude-session:2026-01-28
- [x] Add orResult product filtering - skip type="orResult" products, only include type="result" exact matches #claude-session:2026-01-28
- [x] Change shopCount minimum from 3 to 2 for all products in scraper_service.py and faq_service.py #claude-session:2026-01-28
- [x] Add Product Search API documentation to docs/ARCHITECTURE.md (required params, type field, filtering) #claude-session:2026-01-28
- [x] Detect and reset 55,330 merk URLs with brand mismatches (19,888 missing brand name + 35,442 wrong brand links) #claude-session:2026-01-28
- [x] Create pa.merk_lookup table with 97,363 brand ID→name mappings from Excel #claude-session:2026-01-28
- [x] Change alert-info to alert-warning in app.js for consistent yellow styling across SEO Content Generator #claude-session:2026-01-20
- [x] Update publishing section in SEO Content Generator to match FAQ Generator (remove dry run, add content type selection, remove dev environment) #claude-session:2026-01-20
- [x] Remove conservative mode from Link Validation section in SEO Content Generator (HTML + JS) #claude-session:2026-01-20
- [x] Update publish function: remove dry run option, add content type selection (all/seo_only/faq_only), remove dev environment option #claude-session:2026-01-20
- [x] Add minimum 2 offers validation to link validator - PLPs with shopCount < 2 now treated as "gone" #claude-session:2026-01-20
- [x] Change content_faq format from HTML divs to JSON-LD schema with script tag wrapper (schema_org_to_script_tag function) #claude-session:2026-01-20
- [x] Fix production publish failures caused by case-insensitive duplicate URLs - added deduplication and removed 11 duplicate entries #claude-session:2026-01-20
- [x] Successfully publish 164,286 URLs to production in single 1GB payload #claude-session:2026-01-20
- [x] Configure OpenAI API key and Google Ads credentials in .env file #claude-session:2026-01-19
- [x] Reset 10,545 failed/skipped content URLs and 2,451 failed/skipped FAQ URLs to pending for reprocessing #claude-session:2026-01-19
- [x] Fix docker-compose mount path for thema_ads_optimized (../theme_ads/thema_ads_optimized) #claude-session:2026-01-19
- [x] Add content_bottom field to publishing - extracts FAQ Q&As with internal beslist.nl links, format: `<br /><strong>Question</strong><br>Answer<br>` with `<br />` between Q&A pairs for blank lines #claude-session:2026-01-19
- [x] Add batched publishing support to content_publisher.py - tested API limits on staging (max ~14,000 items / ~57MB per request), discovered Beslist API replaces table on each request (batching won't work without API changes) #claude-session:2026-01-19
- [x] Remove 1 URL containing /l/ from content_urls_joep table #claude-session:2026-01-19
- [x] Add content publishing feature with background task pattern - supports dev/staging/production environments, single-payload publishing (no batching), SQL sanitization for apostrophes ('' → ' → &#39;), 10-minute timeout for large payloads (~512MB) #claude-session:2026-01-15
- [x] Deduplicate content_urls_joep table (33,759 duplicates removed), add unique constraint on url column, copy 6,039 URLs from Redshift, reset 2,577 truncated content URLs to pending #claude-session:2026-01-15
- [x] Add facet_not_available error type to FAQ processor - distinguishes invalid facet/value API errors (400) from generic failures, includes invalid_facet details in response #claude-session:2025-12-26
- [x] Add PostgreSQL database service to docker-compose.yml with healthcheck - app now auto-starts db container with depends_on condition #claude-session:2025-12-24
- [x] Reset 11 FAQs with improper /p/ URLs (missing pim_id) to pending for regeneration #claude-session:2025-12-24
- [x] Fix FAQ processor to include URLs reset to pending (was only fetching URLs with no tracking entry, now also includes status='pending') #claude-session:2025-12-23
- [x] Fix FAQ status pending count to include URLs reset after validation (was only counting URLs with no tracking entry, now also includes status='pending') #claude-session:2025-12-23
- [x] Add FAQ link validator with Elasticsearch lookup, validation tracking table (pa.faq_validation_results), and frontend UI (Validate Links, Validate All, Reset Validation buttons) #claude-session:2025-12-23
- [x] Remove Redshift sync calls from main.py - system now uses PostgreSQL only for all operations #claude-session:2025-12-23
- [x] Remove 1,329 URLs containing /r/ from all database tables (faq_tracking, content_urls_joep, werkvoorraad, kopteksten_check) #claude-session:2025-12-23
- [x] Fix FAQ URL validation - remove fabricated URLs, only keep valid /p/ URLs from provided list, updated prompt examples #claude-session:2025-12-21
- [x] Filter product links to only include products with ≥2 offers (shopCount) in both FAQ and SEO content generators #claude-session:2025-12-21
- [x] Change FAQ hyperlinks to use product URLs (/p/) instead of category URLs (/c/) - deleted all 100K FAQs, reset to pending for regeneration #claude-session:2025-12-21
- [x] Change combined export to include ALL URLs (FULL OUTER JOIN) - URLs without content_top or content_faq now included with empty cells #claude-session:2025-12-21
- [x] Optimize FAQ generator performance - reuse OpenAI client, increase HTTP pool (1→10/20), remove sleep delay, increase max workers (10→20), batch DB inserts #claude-session:2025-12-18
- [x] Add content_bottom column to FAQ exports (XLSX and combined) - HTML formatted FAQs with bold questions and regular answers with hyperlinks #claude-session:2025-12-18
- [x] Fix FAQ hyperlinks to use full beslist.nl URLs instead of relative/localhost URLs - added post-processing and fixed 379 existing records #claude-session:2025-12-18
- [x] Standardize alert colors to yellow (alert-warning) across both tools #claude-session:2025-12-17
- [x] Update FAQ prompt to use informal Dutch tone ("jij"/"je" instead of "u"/"uw") #claude-session:2025-12-17
- [x] Fix content preview HTML truncation bug - strip HTML tags before truncating to prevent broken links in results list #claude-session:2025-12-17
- [x] Fix Product Search API to support URLs without /c/ filters - updated parse_beslist_url in both scraper_service.py and faq_service.py #claude-session:2025-12-17
- [x] Fix FAQ prompt to prevent fake URLs and generic link texts - added strict instructions to only use provided URLs, removed 32 problematic FAQ records #claude-session:2025-12-17
- [x] Standardize UI colors across tools - inline styles for badges (success=#198754, warning=#ffc107, danger=#dc3545), consistent alert-warning backgrounds #claude-session:2025-12-17
- [x] Switch link validator to PostgreSQL only - removed Redshift dependency from link_validator.py, all validation now uses local PostgreSQL #claude-session:2025-12-15
- [x] Add single-paragraph constraint to GPT prompt - updated gpt_service.py to require single continuous paragraph, reset 12,779 URLs with multiple paragraphs for regeneration #claude-session:2025-12-15
- [x] Fix validation 'moved to pending' not tracking URLs - URLs with gone products now properly added to werkvoorraad table for reprocessing #claude-session:2025-12-15
- [x] Recover orphaned URLs and fix data consistency - recovered 8,972 URLs from validation results + 56,666 content URLs not in werkvoorraad, total now 163,250 unique URLs #claude-session:2025-12-15
- [x] Fix export endpoint errors and switch to XLSX format - fixed created_at column missing, connection pool mismatch, changed CSV to XLSX with illegal character sanitization #claude-session:2025-12-15
- [x] Fix GPT content truncation at &amp entities - increased max_tokens from 500 to 1000, added truncation warning logging #claude-session:2025-12-15
- [x] Fix export data source mismatch - changed export to read from local PostgreSQL instead of Redshift (94K→177K rows) #claude-session:2025-12-15
- [x] Update MAIN_CATEGORY_IDS from maincat_ids_new.xlsx - replaced all mappings with correct values from authoritative source file #claude-session:2025-12-12
- [x] Optimize content generation speed - reduced API delay (0.1-0.2s → 0.02-0.05s), increased default workers (3 → 6), batch size (10 → 50) #claude-session:2025-12-12
- [x] Fix Total URLs count to show all unique URLs across werkvoorraad + content tables (not just werkvoorraad) #claude-session:2025-12-12
- [x] Switch to local PostgreSQL only - remove all Redshift dependencies from process-urls and status endpoints, content saved directly to local DB #claude-session:2025-12-11
- [x] Add "meubilair" (ID: 10) to MAIN_CATEGORY_IDS mapping in scraper_service.py - fixes API 400 errors for furniture URLs #claude-session:2025-12-11
- [x] Create import_missing_content.py script - imports CSV content to local PostgreSQL, converts relative URLs to absolute, updates tracking table #claude-session:2025-12-11
- [x] Fix double single quotes in content ('') → single quote (') - updated 3,594 records #claude-session:2025-12-11
- [x] Normalize URL formats across all tables - convert relative /products/ URLs to absolute https://www.beslist.nl/products/, remove /l/ format URLs #claude-session:2025-12-11
- [x] Sync tracking table with content table - add tracking entries for 25K+ URLs that had content but weren't tracked #claude-session:2025-12-11
- [x] Integrate Product Search API-based content generation into frontend SEO Content Generation - extracts selected facets (detailValue) to build product subjects (e.g., "Gele iPhone 15", "Nike Heren voetbalschoenen"), smart category name inclusion based on facet types #claude-session:2025-12-11
- [x] Add "Validate All" button to frontend link validation - validates ALL unvalidated URLs in single batch, uses LEFT JOIN with WHERE IS NULL for efficient filtering #claude-session:2025-12-11
- [x] Add urls_corrected count to link validation results display - shows how many URLs were auto-corrected vs moved to pending #claude-session:2025-12-11
- [x] Create seo_content_generator.py to generate SEO content from Product Search API using URL filters (parses /products/{maincat}/{category}/c/{filters}, fetches 30 products, generates GPT content with plpUrl links), outputs to Excel #claude-session:2025-12-10
- [x] Rewrite link_validator.py to use Elasticsearch plpUrl lookup instead of HTTP status checks, auto-correct outdated URLs in content, reset URLs with GONE products to pending (kopteksten=0), validate via local PostgreSQL #claude-session:2025-12-10
- [x] Create lookup_plp_urls.py script to query Elasticsearch API for plpUrl using pimId, supports both old URL format (/p/maincat_url/pimId/) and new format (/p/product-name/maincat_id/pimId/), batches of 10K, maincat mapping from CSV #claude-session:2025-12-09
- [x] Fix Redshift serialization conflict error (Error 1023) by replacing individual UPDATE loops with batch UPDATE operations using IN clauses #claude-session:2025-10-28
- [x] Fix async/threading deadlock causing batch processing to hang after first batch (converted endpoint to synchronous, replaced executemany with individual executes) #claude-session:2025-10-23
- [x] Fix URL filtering logic to use content table instead of tracking table (changed from pa.jvs_seo_werkvoorraad_kopteksten_check to pa.content_urls_joep for accurate filtering) #claude-session:2025-10-22
- [x] Fix data consistency issue between local content and Redshift flags (created sync_redshift_flags.py, synced 9,567 URLs with kopteksten=1) #claude-session:2025-10-22
- [x] Implement 503 detection with immediate batch stop (changed from 3 consecutive failures to immediate stop on first 503) #claude-session:2025-10-22
- [x] Fix batch size issue causing single-URL processing (changed local tracking query to filter ALL processed URLs, not just successful ones) #claude-session:2025-10-22
- [x] Implement three-state URL tracking system: kopteksten=0 (pending), =1 (has content), =2 (processed without content) for better analytics #claude-session:2025-10-22
- [x] Fix frontend batch processing showing NaN/undefined values (added default value handling with || operator in JavaScript) #claude-session:2025-10-22
- [x] Implement hidden 503 detection and auto-stop after 3 consecutive scraping failures (rate limit protection) #claude-session:2025-10-21
- [x] Reset 33,970 failed/skipped URLs back to pending state in batches (fixing false "no_products_found" from rate limiting) #claude-session:2025-10-21
- [x] Fix URL upload handling CSV format with relative URLs (convert /products/... to https://www.beslist.nl/products/..., Redshift-compatible batch checking) #claude-session:2025-10-21
- [x] Fix scraping failure handling: network errors (503, timeout, access denied) now keep URLs in pending for retry instead of marking as processed #claude-session:2025-10-21
- [x] Improve scraping error messages with specific HTTP status codes (403 Forbidden, 503 Service Unavailable, etc.) #claude-session:2025-10-21
- [x] Diagnose Docker network connectivity issue after restart (all external connections timing out, including ping/DNS) #claude-session:2025-10-21
- [x] Run one-time Redshift sync to fix already-processed URLs (synced 1,051 URLs, remaining: 52,779 truly unprocessed) #claude-session:2025-10-20
- [x] Fix critical bug: pending count not decreasing because skipped/failed URLs not updating Redshift kopteksten flag (causing infinite fetch loop) #claude-session:2025-10-20
- [x] Implement performance optimizations: connection pooling (30-50% faster), Redshift COPY command (20-30% faster), reduced OpenAI max_tokens (300→200), optimized URL fetching (3x→2x batch multiplier) #claude-session:2025-10-20
- [x] Fix Recent Results timestamps showing N/A by querying local PostgreSQL and conditionally hiding timestamps in frontend when unavailable #claude-session:2025-10-20
- [x] Add conservative mode to link validator (0.5-0.7s delay per link check, forced 1 worker, checkbox UI) #claude-session:2025-10-17
- [x] Create deduplication utility script removing 48,846 duplicate records (108,722→59,876 unique URLs) #claude-session:2025-10-17
- [x] Create werkvoorraad synchronization utility script updating 17,672 URLs from pending to processed #claude-session:2025-10-17
- [x] Fix date display showing "1-1-1970, 01:00:00" to show "N/A" when created_at is null #claude-session:2025-10-17
- [x] Update ARCHITECTURE.md with UI theme documentation (color codes, usage map, conservative mode) #claude-session:2025-10-17
- [x] Customize UI theme with brand colors (#059CDF blue, #9C3095 purple, #A0D168 green) using CSS custom properties #claude-session:2025-10-17
- [x] Add conservative mode option for cautious scraping (0.5-0.7s delay, forced 1 worker, checkbox UI) #claude-session:2025-10-17
- [x] Optimize scraper delay from 0.5-0.7s to 0.2-0.3s based on rate limit testing (2-3x speed improvement) #claude-session:2025-10-17
- [x] Conduct comprehensive rate limit testing showing NO rate limiting even at 0s delay with whitelisted IP (87.212.193.148) #claude-session:2025-10-17
- [x] Create comprehensive ARCHITECTURE.md documenting system design, technology choices, and architectural decisions for future reference #claude-session:2025-10-16
- [x] Update scraper user agent from generic Chrome UA to 'Beslist script voor SEO' for better traffic identification in server logs #claude-session:2025-10-16
- [x] Create /skip-permissions and /restore-permissions slash commands for quick permission mode toggling #claude-session:2025-10-16
- [x] Switch input table to pa.jvs_seo_werkvoorraad_shopping_season (updated all 6 references in backend/main.py, reset tracking table with 72,992 URLs ready for processing) #claude-session:2025-10-15
- [x] Optimize content generation performance (30-50% faster: 0.2-0.3s delay, lxml parser, 300 max_tokens, batched commits, executemany) #claude-session:2025-10-10
- [x] Fix URL filtering to allow failed/skipped URL retries (filter only successful, add ON CONFLICT handling) #claude-session:2025-10-10
- [x] Fix Recent Results font size issue (replace Bootstrap .small with explicit font-size) #claude-session:2025-10-10
- [x] Add manual URL input field to Upload URLs (textarea with uploadManualUrls function) #claude-session:2025-10-10
- [x] Configure VPN split tunneling to bypass scraper traffic to whitelisted IP (87.212.193.148) #claude-session:2025-10-10
- [x] Integrate Redshift for output tables (pa.jvs_seo_werkvoorraad, pa.content_urls_joep) with hybrid architecture #claude-session:2025-10-08
- [x] Clean up 1,903 URLs with numeric-only link text from Redshift, reset to pending #claude-session:2025-10-08
- [x] Remove batch size upper limit for link validation (batch_size: min 1, no max) #claude-session:2025-10-07
- [x] Remove batch size upper limit for SEO content generation (now unlimited) #claude-session:2025-10-07
- [x] Implement hyperlink validation feature with parallel processing (301/404 detection, auto-reset to pending) #claude-session:2025-10-07
- [x] Create CSV import script for pre-generated content (19,791 items imported) #claude-session:2025-10-07
- [x] Change frontend port from 8001 to 8003 (avoid port conflicts) #claude-session:2025-10-07
- [x] Reorganize frontend UI (Link Validation moved between SEO Generation and Status) #claude-session:2025-10-07
- [x] Optimize slow database queries in status endpoint (NOT IN → LEFT JOIN, add status index) #claude-session:2025-10-04
- [x] Fix CSV export formatting (UTF-8 encoding, newline removal, proper quoting) #claude-session:2025-10-04
- [x] Fix HTML rendering bug causing browser to auto-link HTML tags #claude-session:2025-10-04
- [x] Fix AI prompt to generate shorter hyperlink text #claude-session:2025-10-04
- [x] Display full URLs in frontend Recent Results #claude-session:2025-10-04
- [x] Add contract/collapse button for expanded content #claude-session:2025-10-04
- [x] Add parallel processing with configurable workers (1-10) #claude-session:2025-10-03
- [x] Add upload URLs functionality with duplicate detection #claude-session:2025-10-03
- [x] Add export functionality (CSV/JSON) #claude-session:2025-10-03
- [x] Add delete result and reset to pending functionality #claude-session:2025-10-03
- [x] Track skipped/failed URLs separately from pending #claude-session:2025-10-03
- [x] Add expandable full content view in Recent Results #claude-session:2025-10-03
- [x] Separate content_top and theme_ads into independent repositories #claude-session:2025-10-03
- [x] Create frontend interface on http://localhost:8001/static/index.html with batch processing #claude-session:2025-10-03
- [x] Add "Process All URLs" button with progress tracking and stop functionality #claude-session:2025-10-03
- [x] Clean backend/main.py to only include SEO content generation endpoints #claude-session:2025-10-03
- [x] Update docker-compose.yml to remove theme_ads dependencies #claude-session:2025-10-03
- [x] Update CLAUDE.md to reflect content_top as SEO-only project #claude-session:2025-10-03
- [x] Initialize project from template #claude-session:2025-09-30

- [x] Sync dm-dashboard stale connection fix to dm-tools #claude-session:2026-04-03
- [x] Sync last-push timestamp feature to dm-tools (backend + frontend + faq) #claude-session:2026-04-03
- [x] Add Docker-free local run support (dotenv, run_local.sh, venv, symlink) #claude-session:2026-04-03
- [x] Create Windows Task Scheduler auto-start for dashboard #claude-session:2026-04-03
- [x] Add PowerShell startup script with auto-close — `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` wraps the WSL uvicorn command, health-checks port 8003, closes window on success, stays open with error message on failure. Updated scheduled task to use script instead of inline wsl.exe command #claude-session:2026-04-05

## Blocked
_Tasks waiting on dependencies_

---

## Task Tags Guide
- `#priority:` high | medium | low
- `#estimate:` estimated time (5m, 1h, 2d)
- `#blocked-by:` what's blocking this task
- `#claude-session:` date when Claude worked on this
