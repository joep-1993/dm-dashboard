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

`tbl_CS_Cat_Column_Order` spiegelt `CategoryFacetSettings` exact — `verpakking` alleen `seo_prio=1`
in Parfums, `kenmerken` alleen in Aftershaves, `geslacht_parfum` `0` in Eau de Colognes en Body
Mist. Kolommen: `cat_id`, `ent_id` (= facet-id), `type`, `order_number`, `seo_prio`,
`is_plp_facet`. De `cat_id` is de taxonomie-id (29000, 9000238, …), niet de legacy id uit de
URL-slug. `BackSync` draait meerdere keren per dag per maincat en meldt `Completed`.

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

- [ ] Ticket bij eigenaar ProductSearch v2/indexer: `isSeoFacet=false` terwijl `seo_prio=1`
      (bewijs: §2). Vraag welke van de drie fixes uit §3 de juiste plek is.
- [ ] Apart, lagere prioriteit: dependency-registratie bijwerken (§4). Bijlage-idee: volledige
      lijst niet-geregistreerde merken met collectie-waarden op hun producten.
- [ ] Opruimen op basis van de uitdraai: de 1.011 values met 0 producten en de 16 die niet meer in
      de zoekindex bestaan kunnen op `seoPriority=false` (GET-merge-PUT, flat body — zie
      TAXONOMY_API-skill).
- [ ] Optioneel: kolom "laatste echte visit ooit" toevoegen aan de uitdraai.
