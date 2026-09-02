# Audit — de acht nooit-geauditeerde tools (2026-09-02)

Scope: **12.163 regels over 16 bestanden**, verdeeld over tien onafhankelijke slices en
parallel gereviewd. De acht tools die op 2026-09-02 nog nooit een `/audit` hadden gehad:
GSD Tag Toppers, Healthscore 2.0, SEO Priority, SEO Titles, SEO Rulings, Facet Watch,
GSD Budgets en GSD Check. Op verzoek buiten scope: Performance Standup, Shop Campaigns
en DM Review.

**Regelnummers zijn van vóór de fixes** (HEAD `fba59ad`). De fase 0-5-uitvoering staat in
elf commits, `768a1b7` t/m `a0a55c7`, dus in de huidige code zijn ze verschoven. Het
gefaseerde plan zelf staat in TASKS onder `2026-09-02 (3)` en in het artifact:
https://claude.ai/code/artifact/7e69740b-5ad7-41ec-bac9-341128dc3ce5

**Waarom dit bestand bestaat:** de zes fases zijn gebouwd uit de 31 HIGH's plus een handvol
structureel meewegende MED's. De overige ruim zestig MED's en de LOW-categorie zaten in geen
enkele fase en stonden nergens vast. Dit is het volledige register. Status per bevinding:
`GEDAAN` (doorgevoerd in deze sessie), `OPEN`, of `VERVALLEN`.

---

## Wat de verificatie NIET overleefde

Drie bevindingen zijn na eigen nameten ingetrokken of gecorrigeerd. Hier vastgelegd zodat
niemand ze "alsnog fixt".

* **"GSD Check duurt 156 seconden."** Gemeten door de review-agent, reproduceert niet. De
  échte originele query uit git doet er **9,4 s** over voor 'coolblue' en 4,5 s voor 'bol'.
  Het structurele probleem klopt wel — de window-functie draait onbegrensd over 87,8 M rijen
  en degradeert mee met de tabelgroei en de clusterbelasting — maar niet met die orde van
  grootte. De tweetraps-fix is gemeten op 1,3 s, dus ~7x, niet ~120x.
* **"`_subdiv_op` in dma_exclusions kan een lege parent krijgen."** Niet bereikbaar. Alle drie
  de leaf-selectors (`_leaf_for_category`, `_leaf_for_aplus`, `_bestsellers_subdiv`) eisen
  `dim == "custom_attr"`, en `_read_tree()` zet `dim` alleen als de node een case value heeft.
  In Google Ads heeft uitsluitend de root géén case value. Het verschil met gsd_tag_toppers is
  geen drift maar een verschil in taak: die module bouwt zelf bomen inclusief root, deze
  verbouwt alleen bestaande takken. De invariant hing wél af van drie selectors elders en is
  nu expliciet afgedwongen (`a0a55c7`).
* **"De dubbele `{phrase}` in de SEO-Titles-description mangelt 85.870 rijen."** De premisse —
  een `!!facet!!` resolvet alleen op zijn eerste voorkomen — geldt niet meer. Live getoetst op
  drie `/c/`-pagina's met de SEO-UA: beide helften renderen gevuld. Daarmee vervalt ook het
  BACKLOG-besluit om 84.881 records opnieuw te pushen. Wat er wél uit kwam is een ándere bug,
  zie ST-M2.

**Twee eigen meetfouten, ook de moeite van het onthouden waard.** `/api/audit-logs` negeert
`From`/`To` stil en geeft dan de héle log terug (1,5 M rijen vanaf 2025-12-08); de werkende
parameternamen zijn `FromDate`/`ToDate`. En bij het vergelijken van `/api/CategoryFacets` met
`/api/Categories/{id}.facets` las ik `id` waar het veld `facetId` heet, wat "1 tegen 25
facetten" opleverde in plaats van het echte **243 tegen 25**. Verifieer de probe vóór de
bevinding.

---

## GSD Tag Toppers — `gsd_tag_toppers_service.py` (2.153) + router (163)

Muteert live Google Ads. Twee productie-incidenten op zijn naam: de zombie-APScheduler-run en
twee gelijktijdige runs met dubbele campagnenamen.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| TT-H1 | HIGH | `:1041` | `_merchant_id_for_shop` matcht op shop_id alleen — de docstring op regel 21 verbiedt precies dat | GEDAAN |
| TT-H2 | HIGH | `:967` | Een leaf die zijn bod erft leest als 0 en krijgt bij conversie €0,20 opgelegd | GEDAAN |
| TT-H3 | HIGH | `:494` | Gemengde boom: `if containers … else` slaat de converteerbare tak over, ad group meldt "niets te doen" | GEDAAN |
| TT-H4 | HIGH | `:597` | `_copy_negatives` leest `partial_failure_error` nooit; aanroeper geeft planned == applied door | GEDAAN |
| TT-H5 | HIGH | `:1606` | `_get_client()` buiten de try die de run-lock vrijgeeft — credentialfout wedgt de tool tot herstart | GEDAAN |
| TT-M1 | MED | `:500` | Een id dat positief getarget is telt als "already excluded"; de zuster blijft erop bieden en er wordt niets gemeld | OPEN |
| TT-M2 | MED | `:467` | `_convertible_leaves` dropt een leaf met onbepaald niveau zonder spoor — anders dan de `WRITABLE_DIMS`-tak, die wel in `unsupported` landt | OPEN |
| TT-M3 | MED | `:247` | `shop_id` rauw in GAQL LIKE. `_` is een wildcard, `'` breekt de query. De naamcheck in Python vangt het meestal — maar `_merchant_id_for_shop` had die niet | DEELS (die lookup is weg) |
| TT-M4 | MED | `:360/:414/:443` | Kwadratische boomscans: `_item_id_containers` roept `_level_dim` aan per subdivision, elk een volledige `nodes.values()`-scan | GEDAAN |
| TT-M5 | MED | router `:127-163` | Vijf dode routes (`/items/summary`, `/items/import-excel`, `/items/import-live`(+progress), `/items/to-upload`) met ~250 regels service erachter. `/items/to-upload` wisselt de geüploade rijen onder de Run-knop vandaan | OPEN |
| TT-M6 | MED | `:405` | `existing` scant de hele boom zonder parent-scoping en zonder negatief-filter, waarna "alle ids stonden er al" wordt gemeld voor ids die juist uitgesloten zijn | OPEN |
| TT-M7 | MED | `:1444` | `max(trees.items(), key=len)` laat bij gelijke boomgrootte de API-volgorde beslissen welke ad group de ids krijgt | OPEN |
| TT-M8 | MED | `:826` | `_read_partial_failure` slikt een niet-deserialiseerbaar detail zonder log — resultaat is "deels" met een lege uitklap | OPEN |
| TT-M9 | MED | `:1490` | Een campagne die in een retry wordt aangetroffen loopt door de "bestaat"-tak en krijgt daar nooit zijn negatives | OPEN |
| TT-M10 | MED | `:988` | De convert-retry herstuurt onvoorwaardelijk een `remove`; landde die de eerste keer wél, dan faalt de hele atomaire set en blijft de leaf verwijderd zonder vervanging | OPEN |
| TT-M11 | MED | `:1684` | De run-historie kent `campaigns_to_create` maar niet `campaigns_created`; `get_runs` selecteert `summary` niet, dus de UI toont gepland i.p.v. aangemaakt | OPEN |
| TT-M12 | MED | `:1653` | De bestandsnaam van de run wordt in de `finally` gelezen, minuten later — `/upload` kan hem intussen vervangen hebben | OPEN |
| TT-M13 | MED | `:1747` | Een run die live gemuteerd heeft kan zonder historie-rij eindigen als Postgres onbereikbaar is; alleen een `logger.error` | OPEN |
| TT-M14 | MED | `:613` | `_Temp.path` bouwt per operatie een service-client — 33k constructies op een grote run, puur om een resource-naam te formatteren | OPEN |
| TT-M15 | MED | `:605-705` | Verbatim duplicatie met `dma_exclusions_service.py` (`_Temp`, `_unit_op`, `_subdiv_op`, `_remove_op`, `_children`), plus drift in het retry-predicaat | DEELS (predicaat gelijkgetrokken; merge open, zie BACKLOG) |
| TT-L1 | LOW | `:217` | `wb.close()` niet in een `finally` — een raise in de lus lekt de ZipFile | OPEN |
| TT-L2 | LOW | `:202` | `stated_n = stated if isinstance(stated, int)` — een telcel die als `1105.0` binnenkomt schakelt de controle uit, en `isinstance(True, int)` is True | OPEN |
| TT-L3 | LOW | `:584` | Hardgecodeerde chunk van 200 naast de module-eigen `MUTATE_CHUNK = 1000`, zonder uitleg | OPEN |
| TT-L4 | LOW | `:121` | Inline regex in `_shop_key` terwijl de module `_ID_RE`, `_SPLIT_RE`, `SHOP_RE`, `SHOP_ID_RE` wel hoist | OPEN |
| TT-L5 | LOW | `:660/:667/:687` | Dode `custom_attr`-parameterroute en `_spec_from_legacy`; plus ongebruikte imports `GoogleAdsClient`, `GEO_TARGETS`, `get_negatives`, en `_export_rows` is `return results` | OPEN |
| TT-L6 | LOW | `:1079` | Een mislukte campagne-create laat het zojuist aangemaakte budget als wees achter | OPEN |

**Schoon bevonden:** geen dry-run-lek op enig mutatiepad (alle `mutate_*`-aanroepen zitten achter
`if not dry_run` in `_process_row`), en de concurrent-run-lock is echt — atomaire check-and-set
onder `_state_lock`, beide router-ingangen erdoorheen, 409 bij bezet. De bescherming is wel puur
positioneel: geen enkele mutatiehelper toetst de vlag zelf (TT-L7, OPEN).

---

## Healthscore 2.0 — `_service` (1.617) + `_runs` (618) + `_keywords` (729) + `_router` (367)

Pusht naar keywords.api.beslist.nl `POST /sitemap`: replace-per-categorie, **geen DELETE**.
Een verkeerde push is alleen met een tweede push te repareren.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| HS-H1 | HIGH | `keywords:605` | `push()` zonder `raise_for_status()`; aanroeper zet daarna onvoorwaardelijk "ok". Een 500 telt als geslaagde vervanging | GEDAAN |
| HS-H2 | HIGH | `keywords:478` | Snapshotnaam zonder run-id, geopend met `"w"` — push #2 vernietigt de enige undo | GEDAAN |
| HS-H3 | HIGH | `runs:591` | Read-back in dezelfde try als de push; een `get_live`-timeout zet een geslaagde push op 'error' | GEDAAN |
| HS-H4 | HIGH | `router:236` | `_guard_idle` is check-then-act; twee gelijktijdige pushes passeren allebei | GEDAAN |
| HS-H5 | HIGH | `service:538/588/678/948/1001/1051` | `TRUNCATE` + `execute_values` op een lege lijst leegt de tabel en meldt succes | GEDAAN |
| HS-H6 | HIGH | `service:768` + `:1191` | `ORDER BY score DESC, visits DESC` met `percent_rank()`-score: de cap-snede valt in een gelijkspel, elke rebuild wisselt live URL's om | GEDAAN (ook `compute_shadow`) |
| HS-H7 | HIGH | `service:1147` | Maincat-terugval is de KLEMwaarde 120.000 waar de categorie-tweeling 1.000 neemt — een maincat zonder cap-rij is effectief ongecapt | GEDAAN |
| HS-H8 | HIGH | `runs:555` | `if category_ids:` — een lege selectie vervangt élke categorie live | GEDAAN |
| HS-H9 | HIGH | `service:520` + `:555` | Klimatologievenster is `months * 30.5` dagen, dus de eerste en laatste kalendermaand tellen deels mee maar worden als volle maanden gemiddeld. De gevalideerde tweeling pint bewust complete maanden | OPEN |
| HS-H10 | HIGH | `service:859/1012/1065/1119/1147` | De hele maincat-tak is vanuit de router onbereikbaar (`action` accepteert alleen coverage/features/sitemap/shadow), terwijl de UI een maincat-scope aanbiedt die op die tabellen leunt | OPEN |
| HS-H11 | HIGH | `service:1001` + `:588` | `_guard_knee_shrink` bestaat alleen in `scripts/analysis/`; het backendpad truncate dezelfde knietabellen zonder vergelijking | GEDAAN |
| HS-H12 | HIGH | `caps.py:184` vs `service:633` | Twee schrijvers van `pa.hs2_cat_cap` met verschillende logica: `_combine_caps` heeft een seizoens-vooruitblik (`max(idx[m], idx[m+1])`) die het script mist | DEELS (`engine`-kolom; merge open) |
| HS-H13 | HIGH | `service:1051` + `:1107` | Twee incompatibele maincat-sizings (knie vs 1,5x live) truncaten dezelfde tabel; niets zei welke erin stond | GEDAAN (`:maincat_knee` / `:maincat_live`) |
| HS-M1 | MED | `service:419` | `_gather_keywords` mist het domeinfilter uit `_SEO_WHERE`, dus .be- en locale-varianten komen in het keyword-universum en worden tegen de NL Keyword Planner geprijsd | OPEN |
| HS-M2 | MED | `service:466` | Een keyword dat de API niet teruggaf wordt als volume 0 gecachet en 25 dagen vers gehouden; quota-uitputting vergiftigt die rijen | OPEN |
| HS-M3 | MED | `service:711` | `facet_id \|\| facet_value_id \|\| country` zonder scheidingsteken: (12,345) en (123,45) botsen | OPEN |
| HS-M4 | MED | `service:786-801` | De "gegarandeerde" nieuwe-URL-emmer schrijft `deepest_category_id = NULL`, en `build_payload` filtert daarop — de rijen worden stil gedropt in plaats van als skipped gemeld | OPEN |
| HS-M5 | MED | `service:762-783` | Geen guard dat de feature-snapshot voor `as_of` bestaat; de router plant features en sitemap als losse jobs zonder volgorde | OPEN |
| HS-M6 | MED | `service:770-776` | Productpagina's en URL's >255 tekens vullen cap-plekken die ze bij de push weer verliezen | OPEN |
| HS-M7 | MED | `service:306` | `MAX(dv.deepest_subcat_id)` bepaalt de categorie van een URL — het grootste id, niet het recentste of frequentste | OPEN |
| HS-M8 | MED | `service:111/290/405` vs `:1242` | Drie handkopieën van de SEO-join/where, 800 regels boven hun definitie; HS-M1 is die drift die al gebeurd is | OPEN |
| HS-M9 | MED | `service:1217-1219` | De docstring belooft dat de nieuwe-URL-emmer "als unmapped gerapporteerd" wordt; de query telt iets anders (feature-URL's zonder maincat-mapping) | OPEN |
| HS-M10 | MED | `service:856` | `MAINCAT_SENTINELS = (-1, 0)`, maar de UI verbergt óók 11111 (`!Overig`) — de backend sizet en pusht die gewoon | OPEN |
| HS-M11 | MED | `service:1187-1207` | De cap wordt toegepast vóór de filters die bepalen wat er werkelijk verscheept (PLP, `/p/`, ontbrekende heading, te lang) | OPEN |
| HS-M12 | MED | `service:1304` | `compute_shadow` evalueert niet meer wat er verscheept: platte cap, geen seizoenscaps, geen nieuwe-URL-emmer, geen maincat-rollup | OPEN |
| HS-M13 | MED | `service:859` | `refresh_maincat_map` filtert alleen op `deleted_ind = 0`, geen `actual_ind = 1`; dezelfde join in `_refresh_maincat_month/_knee` zou visits kunnen vermenigvuldigen | OPEN |
| HS-M14 | MED | `service:1092` | Volledige live-download per maincat alleen om URL's te tellen — ~500k records over 32 maincats, en `preview` haalt ze even later nog eens | OPEN |
| HS-M15 | MED | `runs:451` | De drop-lijst bouwt de payload-set opnieuw per live-element: 10.158 ms tegen 1,11 ms gehesen | GEDAAN |
| HS-M16 | MED | `runs:449` + `keywords:304` | De live set wordt twee keer opgehaald per categorie (build_payload met preserve, en preview zelf) | OPEN |
| HS-M17 | MED | `runs:602` | Niets wordt gepersisteerd tot de hele push-lus klaar is; een herstart midden in laat de rij op `running` staan zonder reaper | OPEN |
| HS-M18 | MED | `keywords:201` + `:363` | De comment claimt een stabiele tie-break, maar strikt `>` houdt de eerste in SCANvolgorde en de query heeft geen `ORDER BY` | OPEN |
| HS-M19 | MED | `router:94` | `_run_job` zet 'done' zodra de callable terugkeert, ook bij een push waarvan élke categorie faalde | OPEN |
| HS-L1 | LOW | `keywords:535/631/667` | `health()`, `dry_run()` en `maincat_dry_run()` hebben geen aanroepers; `seo_visits_in_maincats()` alleen via die laatste; `MAINCAT_MAP_TABLE` op `:85` wordt nergens gelezen | OPEN |
| HS-L2 | LOW | `router:187-194` | `/run` heeft een letterlijke kopie van `_guard_idle`'s body | GEDAAN |
| HS-L3 | LOW | `runs:451` vs `keywords:583` | Twee drop-tellingen in dezelfde run-rij die kunnen verschillen (rstrip vs rauwe spelling) | OPEN |
| HS-L4 | LOW | `runs:552` | Niets belet dezelfde preview meerdere keren te pushen; elke replay is opnieuw live vervangen | OPEN |
| HS-L5 | LOW | `runs:99/137/167/182` | `_ensure_table()` doet DDL + commit op elk leespad, inclusief de CSV-export | OPEN |
| HS-L6 | LOW | `service:772` | De `ln()` binnen `percent_rank()` is een no-op — rangorde verandert niet, terwijl de comment doet alsof de log de score vormt | OPEN |
| HS-L7 | LOW | `service:259-261` | `_feature_windows` bewaakt niet dat het prior-venster binnen het level-venster past; `momentum` komt dan systematisch positief uit | OPEN |
| HS-L8 | LOW | `service:1225/783` | Dode `isinstance(dict)`-takken (de cursor levert altijd tuples) en een ongebruikte `scored_n` | OPEN |
| HS-L9 | LOW | `service:1421/1119` | Lezers gaan uit van tabellen die alleen schrijvers aanmaken; `get_shadow_history` heeft geen LIMIT | OPEN |

**Schoon bevonden:** de twee bezoekdefinities worden nergens gekruist — SEO-only voor dekking en
URL-score, all-channel voor cap-sizing, en `pa.hs2_cat_month` wordt nooit als dekkingsnoemer
gebruikt. Het `confirm_token`-hek is niet te omzeilen, `push_run` replayt echt de opgeslagen payload
en herbouwt nooit, de snapshot gaat vóór de write, en `MAX_URL_LEN` wordt op drie plaatsen
afgedwongen. `_predictor_window` heeft geen off-by-one.

---

## SEO Priority — `seo_prio_service.py` (1.389) + de `/api/seo-prio/*`-endpoints in `main.py`

Schrijft naar de Taxonomy API (`PUT /api/CategoryFacetSettings`), dus muteert productie-taxonomie.

**Alle drie de gedocumenteerde valkuilen zaten er nog goed in** en zijn expliciet nagelopen: het
legacy PDM-id gaat via `cat_urls.csv` (3.543 rijen, alle slugs uniek, 1:1), urlSlug komt uit
`facet.labels[]` per locale, en `_decide` krijgt de rauwe bool. Geen dry-run-lek, `X-User-Name` op
elke sessie, `_SETTING_CARRY_FIELDS` dekt de swagger exact, en `delete_run` laat de apply-log staan.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| SP-H1 | HIGH | `:882` | Categorie zonder omzet → elk facet 0,00% → `qualifies_off` slaagt altijd op het omzetbeen; en de OFF-kant had geen absolute vloer | GEDAAN |
| SP-H2 | HIGH | `:371` | `_set_status` lekt een poolverbinding (geen `finally`) — heetste DB-call van de tool tegen de gedeelde pool van 60 | GEDAAN |
| SP-H3 | HIGH | `:1301` | Mislukte audit-log doet `rollback()` + `print`; `apply_to_taxonomy` meldt daarna gewoon "N applied" | GEDAAN |
| SP-M1 | MED | `:1062-1067` | Een onleesbare read-back wordt als mislukte schrijfactie geboekt — het logboek zegt dan het tegenovergestelde van wat er in productie staat | OPEN |
| SP-M2 | MED | `:996-998` | `_FACET_ENABLED_CACHE` is een permanente negatieve cache; een 404 tijdens een storing blokkeert dat facet tot een herstart, en de comment beweert het omgekeerde | OPEN |
| SP-M3 | MED | `:259-268` + `:279-294` | Een transiënte taxv2-fout wordt gecachet als "deze categorie heeft geen facetten"; de run eindigt groen | DEELS (retry via `taxv2_client`) |
| SP-M4 | MED | `:261` | Twee facetten met dezelfde nl-NL urlSlug in één categorie: last-wins, terwijl de hidden-facet-fallback op `:345` juist weigert bij meer dan één kandidaat | OPEN |
| SP-M5 | MED | `:1028` | `_parse_target` maakt van een integer `0` een `None` (`str(value or "")`), dus een expliciete "zet UIT" via de API valt terug op het opgeslagen voorstel | OPEN |
| SP-M6 | MED | `:1230` | De audit-log wordt pas geschreven nadat élke categorie klaar is; een herstart of proxy-timeout verliest het spoor van alle PUT's die al landden | OPEN |
| SP-M7 | MED | `:984-989` + `:1110-1112` | De read-merge-write gebruikt een hardgecodeerde allowlist in plaats van terug te echoën wat de GET gaf — de module weet zelf al dat `unitAmount` in de API zit en niet in de swagger | OPEN |
| SP-M8 | MED | `:589` + `:934` | Ongelimiteerde Redshift-fetch (default 2 jaar, hele site) en daarna één INSERT per combo | OPEN |
| SP-M9 | MED | `main.py:3390` + `:740` | Drempels gaan ongevalideerd door; een leeggemaakt invoerveld stuurt `null` en laat de run crashen ná de volledige Redshift-pull | OPEN |
| SP-M10 | MED | `:418-425` | `stop_run` raakt alleen `_RUNS` in geheugen; na een uvicorn-herstart blijft de DB-rij op `running` en pollt de frontend een dode run | OPEN |
| SP-L1 | LOW | `:19` | `import re` zonder enig gebruik; docstring op `:150` verwijst naar een hernoemde functie; `ct["urls"]` wordt geteld en nooit gelezen; `res["body"]` wordt in dry-run teruggegeven en nergens gerenderd | OPEN |

---

## SEO Titles — `seo_titles_service.py` (1.324)

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| ST-H1 | HIGH | `:314` | `_identity_parent` is prefix-gebaseerd en matcht 9 van de 55 parent-slugs; `serie`, `speelgoed_series`, `voertuigmerken`, `nerf_series`, `personage` vallen erbuiten en verdwijnen uit de titel | GEDAAN (21/34-splitsing) |
| ST-H2 | HIGH | `:340` | `child.endswith('_' + p)` schrapt het type-facet — de noun — dat daarna door `!!sub_category!!` vervangen wordt. 99 gepushte blueprints, 70 puur door deze regel | GEDAAN |
| ST-H3 | HIGH | `:954` | Gedeelde cursor zonder rollback + `except: todo.append(u)`; na één DB-fout gaat de hele rest van de batch langs de bescherming van handgeschreven titels heen | GEDAAN (gebatcht, fail-closed) |
| ST-M1 | — | `:431` | *De gemelde dubbele `{phrase}`* | VERVALLEN — zie boven |
| ST-M2 | MED | `:431` | **Nieuw gevonden:** `&#10062;` staat als letterlijke tekst in de template; de site escapet bij het injecteren in de meta-tag, dus Google leest `&#10062;` als tekst. 86.123 van 86.124 descriptions, 0 titles | CODE KLAAR, NIET GEPUSHT |
| ST-M3 | MED | `:407` + `:1149` | Blueprints worden gesleuteld op een gesorteerde key terwijl de store letterlijk en volgordegevoelig matcht: 1.467 van 51.443 gepushte rijen (~3,0k SEO-visits/jaar) kunnen nooit resolven | OPEN |
| ST-M4 | MED | `:992` | De 3-pogingenlus rond `/page-titles` dekt alleen transportfouten — de `return` is onvoorwaardelijk, dus een 502 laat 5.000 rijen op `failed` staan. `_record_exists` doet het wél goed | OPEN |
| ST-M5 | MED | `:1009` | `update_blueprint` zet de status niet terug op `built`, en `publish_built` selecteert alleen `built` — een bewerking aan een gepushte blueprint bereikt de site nooit. Alle 86.124 rijen staan op `pushed` | GEDAAN (`4cdb5bd`) |
| ST-M6 | MED | `:854` + `:360` | Eén run gebruikt twee dependency-snapshots: `_run` laadt `deps` voor `impossible_reason`, `facet_phrase` valt terug op de TTL-cache | OPEN |
| ST-M7 | MED | `:388` | Alleen `geschikte_leeftijd` wordt hard na de noun gezet; andere voorzetselfacetten hangen aan de regeltabel, waardoor "Houten Tweepersoons Met matras Bedden" ontstaat. `ai_titles_service` heeft hier wél een regex voor | OPEN |
| ST-M8 | MED | `:924` + `:955` | Eén commit en één round-trip per record | DEELS (de lookup is gebatcht) |
| ST-M9 | MED | `:1215` | De publish-status meldt `done` ook als élke batch faalde; alleen een raise zet hem op error | OPEN |
| ST-M10 | MED | `:674` | De DDL van `pa.page_titles_existing` mist `cat_name` en `browse_description`, die de leesquery wel selecteert — op een verse DB 500't het "existing"-tabblad | OPEN |
| ST-L1 | LOW | `:632` | `get_store_record()` heeft geen aanroepers; toegevoegd als "de leesroute om een push te verifiëren", maar `publish_built` verifieert niet | OPEN |
| ST-L2 | LOW | `:1158` | De parameter `combos` wordt binnen de push-lus overschreven | OPEN |
| ST-L3 | LOW | — | `pa.facet_position_rules` wordt twee keer geladen met verschillende semantiek (`load_rules` vs `ai_titles._load_facet_position_rules`) en drie verschillende fallbacks (1750 / 10.000.000 / 1500). Twee analysescripts importeren `load_existing_combos`, een naam die niet meer bestaat — die crashen bij het starten | OPEN |
| ST-L4 | LOW | — | `import re` ontbrak volledig in dit bestand | GEDAAN |

**Schoon bevonden:** `facet_phrase` is reproduceerbaar (de eindsort is een totale ordening op
`(order, slug)`), `canon_key`/`parse_url` gaan correct om met trailing slashes, en
`impossible_reason` implementeert de gedocumenteerde ANY-parent-semantiek.

---

## SEO Rulings — `seo_rulings_service.py` (1.074) + router (63)

Vier live checks tegen beslist.nl, elke run naar `pa.seo_rulings_runs` plus een Slack-DM.
**De whitelisted UA staat overal goed:** `_fetch` is het enige pad naar beslist.nl en zet
`User-Agent: Beslist script voor SEO` op élke fetch, inclusief sitemaps, 404-probes en retries.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| SR-H1 | HIGH | `:708` | `failed` telt alleen `failed`/`no_rows`, terwijl `_check_variable` ook `fetch_error` en `skipped` produceert | GEDAAN |
| SR-H2 | HIGH | `:688` | Uitgeputte kandidatenpool → lege findings → `any([])` is False → groen zonder bewijs | GEDAAN |
| SR-H3 | HIGH | `:415` | Elke niet-lege 200 telt als geldige XML-sitemap | GEDAAN |
| SR-H4 | HIGH | `:509` | Check 2 sampelt uit `/api/CategoryFacets`, dat dependent facetten weglaat — precies de klasse die de check moet betrappen | GEDAAN |
| SR-H5 | HIGH | `:81` + `:403` | Bevroren CSS-modulehash als exacte needle, tien regels boven het comment dat uitlegt waarom dat eerder al brak | GEDAAN |
| SR-H6 | HIGH | `:228` | `_iter_live` raadpleegt `_REDIRECT_CACHE` niet; een geredirecte pagina wordt beoordeeld onder de naam van de gesamplede URL | GEDAAN |
| SR-H7 | HIGH | `:222` + `:479` + `:595` | Ongezaaide shuffle plus `ORDER BY random()` maken de run-historie onvergelijkbaar | GEDAAN (seed in `summary`) |
| SR-M1 | MED | `:459` | Een lege pill-group `return`t vóór de legacy-layout geprobeerd is, terwijl CloudFront ze naast elkaar serveert — vals rood | OPEN |
| SR-M2 | MED | `:701` | `!!NR!!` slaagt op élk cijfer in de meta-description; "55 inch" of "WH-1000XM5" maakt hem groen. `!!DISCOUNT!!` en `!!JAAR!!` zijn wél streng | OPEN |
| SR-M3 | MED | `:561` | De meta-description-regex kapt af op de eerste apostrof (`tv's`) en eist `name` vóór `content` | OPEN |
| SR-M4 | MED | `:868` | Check 2 krimpt stil zijn steekproef: `no_priority_facets_found` wordt alleen gezet als de stream vóór de eerste slot al opdroogt. Checks 1 en 3 doen dit wél goed via `_run_slot_check` | OPEN |
| SR-M5 | MED | `:142` | Een transiënte taxv2-blip wordt voor de hele run gecachet als `isEnabled=false`, waardoor die categorie uit elke steekproef verdwijnt | DEELS (retry via `taxv2_client`; de cache-van-de-fout staat nog) |
| SR-M6 | MED | router `:19` | De comment claimt "<12 fetches per run"; realistisch worst case is ~600 seriële requests à 30 s timeout, zonder deadline op de endpoint | OPEN |
| SR-L1 | LOW | `:234` + `:551` | `_pick_one_live` en `_get_priority_facet_combos` hebben geen aanroepers — restanten van het pre-retry-ontwerp | OPEN |
| SR-L2 | LOW | `:790` / `:918` vs `:932` | Caches werden alleen bij binnenkomst geleegd, en Slack ging de deur uit vóór `_persist_run`, waarvan de fout in een warning verdwijnt | DEELS (caches nu ook in de staart; de Slack-volgorde staat nog) |

---

## Facet Watch — `facet_watch_service.py` (988) + router (112)

De nieuwste tool (28-08-2026). **Aantoonbaar read-only:** geen enkele `.post/.put/.delete/.patch`.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| FW-H1 | HIGH | `:526` | Het event draagt een `CategoryId` dat wordt opgeslagen en nooit gelezen; attributie loopt volledig via een live lookup, dus deletes en ontkoppelingen zijn per constructie niet toe te wijzen | GEDAAN |
| FW-H2 | HIGH | `:655` | `_resolve_facets` op de default `max_age_days=7`: een aanhecht-event wordt toegewezen aan de maincats van vóór de aanhechting | GEDAAN |
| FW-H3 | HIGH | `:429` | Een mislukte lookup wordt als `no_maincat` weggeschreven — onzichtbaar voor de overzichten, en het venster van één dag raakt het nooit meer aan | GEDAAN (`lookup_failed` + reparatiestap) |
| FW-H4 | HIGH | `:571` | Alles buiten `FACET_ENTITIES` wordt zonder boekhouding gedropt. **Gemeten over 30 dagen: 3.789 van 70.516 (5,4%)**, waarvan 2.497 `Facet Value Dependency`, 1.200 `Facet Context`, 92 `Category Context` | BOEKHOUDING GEDAAN; ingesten OPEN |
| FW-H5 | HIGH | `:721` | Een afgebroken ingest meldt `status="done"` en `success: True` aan de UI, terwijl de DB-rij correct `stopped` krijgt | GEDAAN |
| FW-M1 | MED | `:792-794` + `:866` | `values_added` telt `Facet Value` én `Facet Value Label` (dus ~1 + n_locales per waarde), `values_deleted` alleen de kale rij — twee kolommen op verschillende schalen | OPEN |
| FW-M2 | MED | `:713` | `events_new` telt upserts, niet nieuwe rijen; het venster overlapt bewust een dag, dus het getal bevat altijd herschrijvingen | OPEN |
| FW-M3 | MED | `:574` | De pagineerguard keert om als `total` ontbreekt (`or 0`): stille afkap na één pagina, gerapporteerd als normale afronding | OPEN |
| FW-M4 | MED | `:726-727` | Faalt `_finish_run` in de except-tak (waarschijnlijk juist bij een DB-storing), dan wordt `_set(status="error")` nooit bereikt en blijft de in-memory status op `running` — de UI weigert daarna elke nieuwe ingest tot een herstart | OPEN |
| FW-M5 | MED | `:751-754` | TOCTOU op de single-run-guard: twee gelijktijdige POSTs zien allebei "idle" | OPEN |
| FW-M6 | MED | `:798` | De kolom `auto_events` is per constructie 0 in de standaardweergave, want de outer `WHERE` heeft de auto-rijen al verwijderd | OPEN |
| FW-M7 | MED | `:593-600` + `:663` | `value_name` blijft leeg voor juist de `Facet Value`-events, want `need_vals` verzamelt alleen waarden zónder facet-id; en `setdefault` laat de OUDSTE FacetId winnen in plaats van de nieuwste | OPEN |
| FW-M8 | MED | `:939-940` | `get_deletions` is een GET die de live API bevraagt, de cachetabel schrijft én de ingest-teller ophoogt — bij elke paginaweergave | OPEN |
| FW-L1 | LOW | `:96` + `:112` + `:768` | `FACET_LEVEL`, `AUTO_FACET_NAMES` en `AUTO_FACET_NAME_PREFIX` worden nergens gelezen (de echte filter is een hardgecodeerde SQL-literal), de docstring verwijst naar een constante die niet bestaat, en `/deletions` + `/main-categories` worden door de frontend nooit aangeroepen | OPEN |

---

## GSD Budgets — `gsd_budgets_service.py` (1.277) + router (123)

Muteert live budgetten en schrijft `pa.gsd_shop_exclusions_joep`. De fixes hier zijn meegekomen
in `03fd95f` uit een parallelle sessie.

**De dry-run-fix van 2026-04-21 staat er nog** (`sync_shop_exclusions` is gegate vóór zijn
DELETE+INSERT), en een uitputtende grep over élke INSERT/DELETE/mutate/write_text/execute_values
vond geen enkel ander dry-run-lek — op de credential-yaml na. `get_rev_click_delta` bewaakt
`None` en deling door nul correct, en `deleted_ind`/`actual_ind` staan overal goed.

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| GB-H1 | HIGH | `:458` | Het kosten-subselect filtert op gisteren, het omzetbeen op zeven dagen, en `marge` is hun verschil tegen absolute eurodrempels. Gemeten NL: `verlagen-25` 0 → 16, `marge>0` 533 → 364 | GEDAAN |
| GB-H2 | HIGH | `:294` | Vier OAuth-secrets op modus 0644 op een vast pad, ook in dry-run | GEDAAN (beide services + het bestaande bestand) |
| GB-H3 | HIGH | `:1179` | SA360-storing wordt `campaign_marge = 0.0`, wat élke poort faalt — een volle resultaattabel met "completed", nul mutaties, niet te onderscheiden van een rustige dag | GEDAAN |
| GB-M1 | MED | `:1047` + `:1271` | `_unregister_active` stond alleen op de happy path; één exceptie liet een spookrun voorgoed in `_active_runs` staan | GEDAAN |
| GB-M2 | MED | `:975` | `run_id = len(_run_history) + 1`, berekend buiten het lock en botsend zodra de historie zijn cap van 50 raakt; `_history_remove` verwijdert dan álle treffers | OPEN |
| GB-M3 | MED | `:434` | `shop_names` uit de querystring rechtstreeks in Redshift-SQL; `''`-verdubbeling verdedigt niet tegen een afsluitende backslash. Elders in het bestand wordt netjes naar int gecast | OPEN |
| GB-M4 | MED | `:864-868` | Exact `-25` en exact `-5` vallen tussen de takken door naar `no_action`, en `marge` is een afgeronde euro dus die waarden zijn bereikbaar | OPEN |
| GB-M5 | MED | `:661` + `:771` | Verse `GoogleAdsClient` + gRPC-kanaal per shop en per mutatie, terwijl SA360 in hetzelfde bestand wél gememoïseerd is | GEDAAN |
| GB-M6 | MED | `:89` | `REV_CLICK_THRESHOLD = 1.38` is een hardgecodeerd eurobedrag over een tariefwijziging heen (cpa_cpc ging op 2026-07-08 van vast naar per-shop ROAS), en `get_rev_click_old` vergelijkt zeven dagen tegen één dag van 28 dagen geleden | OPEN |
| GB-M7 | MED | `:190-198` | Run-historie wordt niet-atomair weggeschreven; `rurl_optimizer_v2_service` doet dit al met tmp + `os.replace` na precies dit incident, `dma_bidding_service` heeft dezelfde ongefixte vorm | OPEN |
| GB-M8 | MED | `:1120` + `:340` | Eén Redshift-roundtrip per kwalificerende shop, en de Sheets-cache haalt beide landen op ook bij een run voor één land; de retry heeft geen backoff | DEELS (rev/click gebatcht) |
| GB-L1 | LOW | `:121` + `:152` | `RunCancelled` wordt nergens geraised of gevangen, en de docstring noemt een `_check_cancel` die niet bestaat. De cancel-vlag zelf wordt wél gelezen — maar niet tijdens de pre-loop-fase, die minuten kan duren | OPEN |
| GB-L2 | LOW | `:1208` | Een dry run telt zijn rijen mee in `summary_counts["budget_changed"]`, dus die meldt hetzelfde aantal als een live run | OPEN |
| GB-L3 | LOW | `:645` | `_coerce_date`'s 8-cijfertak zit niet in een try, dus een onparseerbare waarde geeft een rauwe strptime-fout in plaats van de bedoelde melding | OPEN |
| GB-L4 | LOW | `:605` | `upload_missed_shops` dedupliceert niet: twee runs op één dag zetten dubbele rijen in `pa.jvs_gsd_missed_shops` | OPEN |
| GB-L5 | LOW | router `:82` | `_run_history` wordt geserialiseerd zonder het lock vast te houden | OPEN |

---

## GSD Check — `gsd_check_service.py` (116) + router (50)

| # | Sev | Waar | Bevinding | Status |
|---|-----|------|-----------|--------|
| GC-H1 | HIGH | `:60-73` | `ROW_NUMBER() OVER (PARTITION BY shop_id …)` over de hele `bt.shop_list` (87,8 M rijen); het shopfilter staat in de buitenste WHERE en is niet door de LEFT JOIN te duwen. Gemeten 9,4 s → 1,3 s tweetraps | GEDAAN |
| GC-M1 | MED | `:89` + `:111` | `LIMIT 5000` kapt stil af en het afgekapte aantal wordt als `total` teruggegeven | GEDAAN (`total`/`returned`/`truncated`) |
| GC-M2 | MED | `:57` | Vastgepind op `CURRENT_DATE - 1` terwijl de snapshot van vandaag al bestaat; op 2026-09-02 waren er 8 shops die tussen gisteren en vandaag een GSD-vlag omzetten. GSD Campaigns handelt op de verse stand, deze tool toont hem een dag later. De comment op `:49-55` verwijst bovendien naar de verkeerde tabel | OPEN |
| GC-L1 | LOW | `:31` | `%` en `_` in een zoekterm werken als onge-escapete LIKE-wildcards | OPEN |

---

## Wat hierna moet gebeuren

De gefaseerde uitvoering staat in TASKS onder `2026-09-02 (3)`. Wat uit dít register nog open
staat en nergens anders geagendeerd is:

1. **Ruim zestig MED's en de LOW-categorie hierboven.** Geen van deze is in een fase beland.
   De zwaarste kandidaten voor een volgende ronde, op basis van wat ze kunnen kosten:
   ~~ST-M5~~ (opgelost op 2026-09-02, `4cdb5bd`), ST-M3 (1.467 records met een key die nooit kan resolven), SP-M1 (het
   logboek zegt het tegenovergestelde van wat er in productie staat), FW-M4 (een DB-storing
   wedgt Facet Watch tot een herstart), GB-M3 (SQL-injectie via een querystring) en GB-M2
   (botsende run-id's zodra de historie vol is).
2. **Twee merges wachten op een harness die niet bestaat** — de Healthscore-tweelingen en de
   listing-tree-helpers. Eisen staan in BACKLOG.
3. **De backend is niet herstart,** dus niets van de elf commits draait live op :8003.
