# SEO-facetlinks & dependent facetten

_Onderzoek 2026-08-19. Startvraag was een uitdraai van Parfumerie-facetvalues zonder
SEO-visits; onderweg bleek waarom een deel van die values structureel geen kans maakt._

## TL;DR

1. **`CategoryFacetSettings.seoPriority` = interne linkbuilding**: het facet 'aanzetten' in een
   categorie zorgt dat de facet values in een `<noscript>`-blok gelinkt worden, zodat Googlebot
   (crawlt zonder JS) ze kan volgen. Geen UI-vlag, geen robots-hint.
2. **De site linkt op `isSeoFacet` uit ProductSearch v2, niet op de taxonomie.** Voor
   **dependent (child) facetten** staat `isSeoFacet` altijd `false`, ook als de taxonomie `true`
   zegt. Gevolg: die facet values worden nooit gelinkt voor Google.
3. Dat is **platformbreed**: Collectie (3432, Parfumerie), type_productlijn (3821, Schoenen),
   Modelnaam (5514, Laptops). Geen Parfumerie-dingetje.
4. **Niet zelf te fixen**: ProductSearch v2 is read-only (17 endpoints, allemaal GET) en
   `isSeoFacet` is een afgeleide waarde. De taxonomie- en sync-kant zijn al correct.
5. **Bron van de vlag** (nagemeten 2026-08-25): categorie×facet-seoPriority leeft *uitsluitend* in
   `CategoryFacetSettings`. `GET /api/CategoryFacets` heeft het veld ook, maar geeft altijd `null`.
   Zie §8 — inclusief de checklist die naar IT kan.

## STATUS 2026-09-03: de bug reproduceert niet meer

Nagemeten met exact de URL uit §2 — `parfum_aftershave_422758/c/merk~422868`, dezelfde pagina waar
op 19-08 en 25-08 `isSeoFacet=false` en 0 links stonden:

| Facet | slug | `isSeoFacet` 19-08 | `isSeoFacet` 03-09 | noscript 03-09 |
|---|---|---|---|---|
| **3432 Collectie** | type_parfum | **false** | **true** | kop "Collectie" + 5 links |
| 3441 Inhoud | inhoud_parfum_ml | true | true | ja |
| 6271 Verpakking | verpakking | true | true | ja |
| 6273 Geurfamilie | geurnoot | false | false | nee (seoPriority is daar ook null) |

Tweede geval, Schoenen: op `schoenen_430884/c/populaire_serie~4379309` (parent-waarde gekozen) staat
**3821 `type_productlijn` op `isSeoFacet=true`** met een kop "Type" en 14 facetlinks, waaronder
`…/c/populaire_serie~4379309~~type_productlijn~18049952`.

Twee dingen om niet te verwarren:

- **Een dependent facet linkt alleen op de pagina waar zijn parent-waarde gekozen is.** Op de kále
  `schoenen_430884/`-pagina en op `/c/merk~431107` staat géén Type-blok en 0 `type_productlijn`-links.
  Dat is geen bug maar de definitie van dependent. De crawlketen loopt dus via het parent-facet:
  de kale categorie linkt "Productlijn" (3513 `populaire_serie`), en pas op zo'n waarde-pagina
  verschijnt het kind. Meet je op de kale categorie, dan meet je niets.
- **Modelnaam 5514 (Laptops) is hiermee niet getoetst.** In `computers_19664326_19904517` staat
  *elk* facet op `isSeoFacet=false` en bevat het noscript alleen "Kies categorie" — die categorie
  heeft überhaupt geen SEO-facetten, dus er valt geen dependent-gedrag te zien. Wie dit sluitend wil
  hebben, zoekt een categorie waar de parent (Productlijn 2306) zelf wél gelinkt wordt.

**Joep bevestigt de fix** (03-09-2026): "de bug is idd gefikst" — vandaar de vaste check in SEO
Rulings. De nameting hieronder is dus geen toevalstreffer op één pagina.

**Wat dit betekent voor de ask aan IT** (§8, de checklist-artifact): de twee gevallen die het bewijs
droegen linken nu allebei. Voordat je de vraag intrekt: het is niet bekend *wat* er tussen 25-08 en
03-09 veranderd is, en niemand heeft een release gemeld. Vraag 3 uit de checklist ("bestaat er een
bewuste regel dat child-facetten nooit isSeoFacet krijgen?") is daarmee juist beantwoord met "nee,
blijkbaar niet" — maar zonder te weten waarom het eerst wél zo was.

**Regressiewacht staat sinds vandaag in de tool.** SEO Rulings check 2 heeft een vaste combo
(`PINNED_FACET_COMBO` in `backend/seo_rulings_service.py`): elke run toetst of
`…populaire_serie~4379309~~type_productlijn~18049952` als `<a href>` in het noscript van die pagina
staat. Verdwijnt de link, dan valt de check om — dat is precies de gebeurtenis die dit document
beschrijft. Zie ook LEARNINGS 2026-09-03 voor waarom die match op het noscript-blok gescoped is en
niet op de hele HTML.

## 1. Wat seoPriority doet, en hoe je het meet

Definitie (Joep): `seoPriority=1` op een cat/facet-combinatie linkt de facet values in een
`<noscript>`-blok; Googlebot crawlt zonder JS en ziet daar dus de facetlinks.

Meetrecept:

```bash
# browser-UA is nodig; zonder UA of na veel requests krijg je HTTP 202/405 bot-challenge
curl -s -L -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
  (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36" -H "Accept: text/html" \
  "https://www.beslist.nl/products/parfum_aftershave/parfum_aftershave_422758/c/merk~422868" \
  -o page.html
# pak <noscript>...</noscript> en groepeer de hrefs op facet-slug
```

Let op: **buiten** het noscript staan ook echte `<a href>`-links met `/c/`
(related/populair-blokken). Die zijn crawlbaar maar zijn geen facetlinks — op de gemeten pagina
102 stuks in de HTML tegen 16 in het noscript. Ze verklaren waarom er nog SEO-traffic op
collectie-URL's binnenkomt terwijl het facet niet gelinkt wordt.

## 2. De bug

Gemeten op `/products/parfum_aftershave/parfum_aftershave_422758/c/merk~422868`
(Parfums 9000239 + CHANEL 422868 — een **geregistreerde** parent, dus alle randvoorwaarden vervuld):

| Facet | slug | `isSeoFacet` (Search API) | `settings.seoPriority` (Taxonomy) | in noscript |
|---|---|---|---|---|
| 3039 | geslacht_parfum | true | true | ja (2 links) |
| 3027 | merk | true | true | ja (1 parent-link) |
| 3441 | inhoud_parfum_ml | true | true | ja (9 links) |
| 6271 | verpakking | true | true | ja (4 links) |
| **3432** | **type_parfum** | **false** | **true** | **nee (0 links)** |
| 6273 | geurnoot | false | null | nee |
| 6272 | kenmerken | false | null | nee |

De noscript-inhoud correspondeert 1-op-1 met `isSeoFacet`, niet met de taxonomie-instelling.

### De keten downstream is wél correct

De back-sync schrijft naar legacy MySQL (`beslist` op `dbs-htz-001`, read-only via `t_pdm`).
Daar staat de vlag gewoon goed:

| Laag | Collectie 3432 (Parfums) | type_productlijn 3821 (Sneakers) |
|---|---|---|
| Taxonomy `CategoryFacetSettings.seoPriority` | true | true |
| `tbl_CS_Cat_Column_Order.seo_prio` | **1** (alle 7 categorieën) | **1** (60 rijen) |
| `tbl_CS_Cat_Column_Slots.slot_id` | 8 | 23 |
| ProductSearch v2 `isSeoFacet` | **false** | **false** |
| noscript-facetlinks | geen | geen |

`tbl_CS_Cat_Column_Order` spiegelt de **`seo_prio`-kolom** exact — `verpakking` alleen `seo_prio=1`
in Parfums, `kenmerken` alleen in Aftershaves, `geslacht_parfum` `0` in Eau de Colognes en Body
Mist. Kolommen: `cat_id`, `ent_id` (= facet-id), `type`, `order_number`, `seo_prio`,
`is_plp_facet`. De `cat_id` is de taxonomie-id (29000, 9000238, …), niet de legacy id uit de
URL-slug. `BackSync` draait meerdere keren per dag per maincat en meldt `Completed`.
**Let op (2026-08-25): alleen de `seo_prio`-kolom spiegelt exact.** Lidmaatschap en
`order_number` wijken af — zie §8.

**Conclusie: de breuk zit in de projectie/indexering van ProductSearch v2.**

### Gefalsifieerde hypotheses (niet opnieuw onderzoeken)

- ~~"seoPriority staat op 0 in de children"~~ — staat `true` in alle 7 Parfumerie-categorieën,
  allemaal met dezelfde `updatedAt` 2026-03-25T09:23:50 (één bulk-edit).
- ~~"Het facet is dood / de pagina's bestaan niet"~~ — `/c/type_parfum~<id>` geeft HTTP 200 met
  content en ~2.700 SEO-visits/maand.
- ~~"De back-sync mist de rij, dus de vlag komt niet aan"~~ — de vlag staat downstream op 1,
  inclusief slot. Dit was mijn eerste diagnose en die is fout.
- ~~"Het is by design want child-facetten horen niet gelinkt te worden"~~ — kan niet uit de data
  worden hardgemaakt; de taxonomie zet de vlag expliciet aan en downstream draagt hij hem.

De enige overgebleven correlatie: precies de facetten met een foute `isSeoFacet` missen een rij in
`GET /api/CategoryFacets`. Vermoedelijk joint de indexer daarop in plaats van op de Order-tabel.
`tbl_CS_Cat_Columns` heeft 0 rijen voor deze facet-ids (ander id-domein) en is dus niet de bron.

### Reproduce

```bash
TAX=http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl
curl -s "$TAX/api/CategoryFacetSettings/9000239/3432"      # seoPriority: true
curl -s "$TAX/api/CategoryFacets?categoryId=9000239"       # 3432 ontbreekt
curl -s "https://productsearch-v2.api.beslist.nl/search/products?\
category=parfum_aftershave_422758&filters%5Bmerk%5D%5B0%5D=422868&\
countryLanguage=nl-nl&isBot=false&limit=0"                 # 3432 -> isSeoFacet: false
# legacy: SELECT ent_id, seo_prio FROM beslist.tbl_CS_Cat_Column_Order
#         WHERE cat_id=9000239 AND ent_id=3432;            -> seo_prio = 1
```

## 3. Waar het gefixt moet worden

Eigenaar: ProductSearch v2 / de indexer. Drie kandidaat-fixes:

1. **Indexer/projectie**: `isSeoFacet` afleiden van `seo_prio` (die al klopt) in plaats van van de
   CategoryFacet-link.
2. **Front-end**: de noscript-generator laat de child-values linken zodra de parent-waarde
   gekozen is (conceptueel de juiste plek — een child heeft alleen betekenis onder zijn parent).
3. **Data-workaround**: `POST /api/CategoryFacets` om het facet aan te haken. Dit is de enige knop
   die wij zelf hebben, maar het repareert een indexer-bug met taxonomie-data en kan het facet in
   het gewone filterpaneel zetten. Alleen als snelle test op één categorie voorstellen, niet als fix.

## 4. Tweede, losstaand gat: de dependency-tabel staat stil

`GET /api/Facets/{childId}/value-dependencies` geeft de parent-waardes die het child-facet
vrijgeven. Alle rijen zijn sinds de migratie niet meer aangeraakt — `createdAt == updatedAt`:

| child-facet | rijen | timestamp |
|---|---|---|
| 3432 Collectie | 197 | 2026-01-27T15:52:01 |
| 3821 type_productlijn | 254 | 2026-01-27T15:52:36 |
| 5514 Modelnaam | 1 | 2026-01-27T15:45:34 |

Gevolg: merken die er daarna bijkwamen ontbreken. **Armani** (234 producten in Parfumerie, met
collectie-waarden als *Stronger With You*, *My Way*, *Code*, *Sí*) en **ARMAF BEAUTÉ** (163
producten, *Odyssey Homme*) staan niet geregistreerd → hun Collectie-filter komt nooit boven.
Andersom staan 41 van de 197 registraties leeg (merk zonder collectie-waarden).

Geen write-endpoint (GET-only). De enige refresh-route in de API is een volledige maincat-import
uit het legacy-systeem: `POST /api/Import/sessions {mainCategoryId}` → status pollen →
`POST /api/Import/sessions/{id}/commit`. Dat stageert een diff over de héle maincat — niet blind
afvuren.

## 5. De uitdraai: Parfumerie-facetvalues zonder SEO-visits

Deliverable: `Downloads\claude\Parfumerie_facetvalues_zonder_SEO_visits_20260819.xlsx`
(genereerscripts in de sessie-scratchpad, niet in de repo).

**Populatie**: facet value met `seoPriority=true` **én** het facet met `seoPriority=true` op ≥1 van
de 7 Parfumerie-categorieën = **3.802 values** (Merk 1.974, Collectie 1.729, Inhoud 76,
Verpakking 11, Kenmerken 8, Doelgroep 4).

**Venster**: 19-02-2025 t/m 18-08-2026 (18 mnd), `fct_visits` + `dim_visit`, `is_real_visit=1`,
kanaal via `chan_deriv.ref_channel_derivation_stats` (`deleted_ind=0`), **alle domeinen**.

**Uitkomst**: 1.956 values (51%) zonder SEO-visit; daarvan **1.775 zonder visit op álle kanalen**
(tab 1) en **181 met alleen niet-SEO-traffic** (tab 2, samen 420 visits: SEA 297, Overig 106,
GSAAS 9, DMA paid 4, AI 4). Tab 1 uitgesplitst: 1.011 met 0 producten, 16 die niet meer in de
zoekindex bestaan, 229 mét producten (Merk 215, Inhoud 12, Verpakking 1, Kenmerken 1) — die
laatste groep is de interessantste: pagina met producten, SEO aan, 18 maanden geen enkele visit.

**Meetkeuzes** die je moet kennen om de cijfers te reproduceren:

- Ruim gemeten: een visit op een multi-facet-URL telt mee voor élke facet value in die URL.
- Match op **facet value ID**, niet op slug — slugs zijn hernoemd (`geurnoot` was `geurfamilie`)
  en verschillen per locale.
- URL-scope: alle `/c/`-URL's binnen de Parfumerie-paden, regex `parf(u|ü|.c3.bc)m_aftershave`
  case-insensitive. Het DE-domein `www.shopcaddy.de` gebruikt **percent-encoded Duitse slugs**
  (`parf%c3%bcm_aftershave`, `marke~`, `geschlecht_parf%c3%bcm~`) — een filter op
  `parfum_aftershave` mist die stil.
- Controles: (a) een probe op Parfumerie-specifieke facet-slugs buiten de parfum-paden gaf 0
  visits; (b) de 1.775 "0"-ids komen all-time nergens anders in `dim_visit` voor. Steekproef wees
  wel uit dat één value (V Canto) z'n laatste echte visit had op **2025-02-02**, 17 dagen voor de
  venstergrens — een "laatste visit ooit"-kolom is de logische volgende verrijking.

**Parent-merk in de URL**: Collectie-URL's werken alleen als `merk~<merkId>~~type_parfum~<id>`.
Bepaald via, in deze volgorde: producten (145+47 rijen), historische `dim_visit`-URL's (175),
bezochte URL's (62), GSC-URL's (35), `pa.urls` (21). 485 van de 641 Collectie-rijen hebben zo een
merk; 156 niet (0 producten én nooit een URL). Op naam matchen is **niet** gedaan: dat gaf fouten
als "Beauty Blue Infini" → rommelmerk *"beauty"*.

## 6. API-gotchas uit dit onderzoek

- **ProductSearch v2 is read-only.** Spec staat inline in de root-HTML
  (`<script id="swagger-data">` op `https://productsearch-v2.api.beslist.nl/`): 17 endpoints,
  allemaal GET. `/swagger/v1/swagger.json` bestaat niet (404).
- **Een child-facet krijg je te zien door op de parent te filteren.** `filters[merk][0]=<id>`
  → facet 3432 komt terug mét values, counts en `crawlable`. Eén call per merk (1.222 merken met
  producten, ~30 s met 10 workers) levert de mapping collectie-value → merk-value: 759 van de
  1.729 values. Bonus: met een filter actief is de merk-facetlijst **niet** op 100 getrunceerd.
- **HTTP 400 met errors-payload.** Een ongeldige facet-slug/value geeft 400 met
  `{"errors":[{"errorCode":200|300,...}]}` — `curl` toont die body, `urllib` gooit een `HTTPError`
  (kostte in de eerste run 1.745 "http_error"-statussen die eigenlijk nette antwoorden waren).
  `errorCode 200` = facet onbekend, `300` = value onbekend. Een bestaande value zonder producten
  geeft gewoon `total: 0` — 400-vs-0 is dus een echt onderscheid.
- **Gebruik `limit=0` voor de AND-count.** Geverifieerd: `filters[merk][0]=422995` op
  `parfum_aftershave_23797918` geeft bij limit 0/1/5 total 7 (juist) en bij limit 20 total 5044
  (OR-fallback). Zie ook LEARNINGS/memory over `total`-onbetrouwbaarheid.
- **`pa.urls` (shared Postgres) is een goede URL-bron**: 1.447 collectie-URL's, allemaal met merk.
  In Redshift bestaat `pa.urls` niet.
- **`GET /api/Import/sessions` geeft 404** (geen lijst-endpoint); `GET /api/BackSync/results?
  categoryId=<id>` wel, met per run de zes services: `Category`→`tblCategories_online`,
  `Facet`→`tbl_CS_Cat_Columns`, `FacetValue`→`tbl_CS_Column_Buckets`,
  `Slot`→`tbl_CS_Cat_Column_Slots`, `Order`→`tbl_CS_Cat_Column_Order`,
  `Hide`→`tbl_CS_Column_Cat_Hides`.

## 7. Openstaand

- [x] **Checklist voor IT opgeleverd** (2026-08-25): zes checkpoints van bron naar pagina, met
      commando's, verwachte uitkomst en de drie vragen die alleen de indexer-eigenaar kan
      beantwoorden. Artifact: https://claude.ai/code/artifact/c977b3c0-a1ab-4284-bce3-329919b9a9c1
- [ ] Ticket bij eigenaar ProductSearch v2/indexer: `isSeoFacet=false` terwijl `seo_prio=1`
      (bewijs: §2, checklist hierboven). Vraag welke van de drie fixes uit §3 de juiste plek is.
- [ ] Optioneel als onderbouwing bij het ticket: uitdraai van *alle* categorie×facet-combinaties met
      `seoPriority=true`, zodat er een getal onder de reikwijdte staat i.p.v. drie voorbeelden.
      Moet per categorie itereren (§8).
- [ ] Apart, lagere prioriteit: dependency-registratie bijwerken (§4). Bijlage-idee: volledige
      lijst niet-geregistreerde merken met collectie-waarden op hun producten.
- [ ] Opruimen op basis van de uitdraai: de 1.011 values met 0 producten en de 16 die niet meer in
      de zoekindex bestaan kunnen op `seoPriority=false` (GET-merge-PUT, flat body — zie
      TAXONOMY_API-skill).
- [ ] Optioneel: kolom "laatste echte visit ooit" toevoegen aan de uitdraai.

## 8. Update 2026-08-25 — waar de vlag leeft, en wat "verouderd" wel en niet betekent

Aanleiding: vanuit IT de opmerking dat `tbl_CS_Cat_Column_Order` verouderd is en niet meer gebruikt
wordt. Dat raakt de redenering in §2, dus opnieuw gemeten. De bug zelf reproduceert nog steeds:
dezelfde call geeft `isSeoFacet=false` voor 3432 mét 5 values, terwijl 3027/3039/3441/6271 op
`true` staan.

### De master is `CategoryFacetSettings`

Eén rij per (`categoryId`, `facetId`) — de enige plek waar seoPriority op categorieniveau leeft.

```bash
curl -s "$TAX/api/CategoryFacetSettings?categoryId=9000239"   # hele categorie
curl -s "$TAX/api/CategoryFacetSettings/9000239/3432"         # één combinatie
# schrijven: PUT /api/CategoryFacetSettings (upsert, platte body) — GET-merge-PUT!
```

Vier valkuilen:

1. **`GET /api/CategoryFacets` heeft óók een `seoPriority`-veld en dat is altijd `null`.**
   Geverifieerd op 3039: settings `true`, CategoryFacets `null`, terwijl `displayOrder` daar wél
   klopt. Het is de *link*-tabel (`id`, `categoryId`, `facetId`, `version`) met een half-gevulde
   projectie. Nooit de vlag daaruit lezen.
2. **`GET /api/CategoryFacetSettings` zónder `categoryId` geeft HTTP 400** — geen dump-call, per
   categorie itereren.
3. **Facet-globaal bestaat seoPriority niet.** `GET /api/Facets/{id}` geeft `isEnabled`,
   `isDetail`, `isTopFacet`, `noIndexNoFollow`, `sortMode`, `seoDisplayLimit`. Dus precies twee
   niveaus: categorie×facet en facet value. `GET /api/Facets/{id}/contexts` is leeg voor 3432/3027.
4. **`tbl_CS_Cat_Column_Slots` gaat op `column_id` + `maincat_id`**, niet op `cat_id` (dat geeft
   `Unknown column`). Kolommen: `column_id`, `maincat_id`, `slot_id`, `timestamp`.

### "Verouderd" ≠ "niet bijgehouden"

De `Order`-service draait nog: run `a0ef1552`, maincat 29000, 2026-08-25 10:33 UTC, `Completed`,
63 rijen skipped. Hoogste `timestamp` in `tbl_CS_Cat_Column_Slots`: 2026-08-24 22:18. Of de
indexer hem nog *leest* is de open vraag — en luidt het antwoord nee, dan blijft alleen de
Taxonomy-route over en wijst dat harder naar de ontbrekende `CategoryFacets`-rij, niet zachter.

### De drie lagen zijn het oneens (correctie op §2)

Voor 9000239, gemeten 2026-08-25:

| Laag | rijen | uitschieters |
|---|---|---|
| `CategoryFacetSettings` | 12 | — |
| `GET /api/CategoryFacets` | 10 | 3028 en 3432 ontbreken |
| `tbl_CS_Cat_Column_Order` | 10 | 7526/7527/7528 ontbreken, 3033 zit er alléén hier |

`seo_prio` spiegelt `seoPriority` 1-op-1 (`true`→1, `null`→0), maar `order_number` niet: 3441 is 5
in de taxonomie en 4 in MySQL, 6271 6→5, 6272 7→6, 6273 4→7, 3028 11→8 — terwijl de back-sync
"0 changes" meldt. De formulering "spiegelt exact" uit §2 gold dus alleen voor de seo_prio-kolom.

### Checklist voor IT

Zes checkpoints van bron naar pagina (taxonomie → link-tabel → back-sync → slot → zoekindex →
noscript), elk met commando en verwachte uitkomst, plus de A/B-tabel uit één API-call, de vier
gefalsifieerde hypotheses en de drie kandidaat-fixes:
https://claude.ai/code/artifact/c977b3c0-a1ab-4284-bce3-329919b9a9c1

De drie vragen die erin staan, zijn de hele ask: (1) uit welke bron leidt de indexer `isSeoFacet`
af, (2) is dat een inner join op `CategoryFacets` waar dependent facetten uitvallen, (3) bestaat er
een bewuste regel "child-facetten krijgen nooit isSeoFacet"?
