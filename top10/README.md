# Top-10 data pijplijn

Bouwt de data achter een "beste X"-pagina voor **elke** Beslist-categorie:
kandidaatproducten, een web-search-review per product, klikdata, een door een
model gescoorde top-10 per zoekterm, prijzen en winkelaanbod — platgeslagen tot
één JSON-bestand dat een paginabouwer kan consumeren.

Overgezet uit de skill `get-top-10-data`, die vastzat aan één categorie
(airfryers) en aan bestanden buiten deze repo. Hier komt de categorie uit
`topic.json` en draait alles op de repo zelf.

## Een categorie draaien

Alles in één keer, met een stop vóór de eerste betaalde stap:

```bash
venv/bin/python top10/scripts/run_all.py "airfryers"              # tot de kostenraming
venv/bin/python top10/scripts/run_all.py --topic airfryers --yes  # ook de betaalde stappen
```

Of stap voor stap — handig als er iets misgaat, want alles cachet per item:

```bash
# 0. categorie opzoeken (toont kandidaten, maakt nog niets aan)
venv/bin/python top10/scripts/resolve_category.py "airfryers"
venv/bin/python top10/scripts/resolve_category.py "airfryers" --create 9005486

# 0b. zoektermen kiezen op zoekvolume           gratis
venv/bin/python top10/scripts/keyword_research.py --topic airfryers
venv/bin/python top10/scripts/keyword_research.py --topic airfryers --apply

# 1. kandidaatproducten per zoekterm            gratis
venv/bin/python top10/scripts/collect_products.py --topic airfryers

# 2. kliks per product (Redshift, 90 dagen)      gratis
venv/bin/python top10/scripts/get_clicks.py --topic airfryers

# 2b. commercieel bewijs: A-label + pixeldata   gratis, maar minutenlang
venv/bin/python top10/scripts/get_tagdata.py --topic airfryers

# 3. review per uniek product                    KOST GELD — eerst proefdraaien
venv/bin/python top10/scripts/run_reviews.py --topic airfryers --limit 2
venv/bin/python top10/scripts/run_reviews.py --topic airfryers

# 4. scoren en rangschikken per zoekterm         kost geld (klein)
venv/bin/python top10/scripts/rank_top10.py --topic airfryers

# 5. prijzen + winkelaanbod                      gratis
venv/bin/python top10/scripts/snapshot_prices.py --topic airfryers

# 6. alles platslaan tot het contractbestand     gratis
venv/bin/python top10/scripts/export_top10_data.py --topic airfryers
```

Alles cachet per item: stap 3 slaat producten met een geldige review over, stap
4 slaat termen over die al een `rank_<slug>.json` hebben, en stap 6 praat met
niets. Een afgebroken run hervat je dus gratis, en na verse prijzen draai je
alleen stap 5 en 6 opnieuw.

## Een nieuwe categorie

Alleen `resolve_category.py` draaien. Geen codewijziging: de categorie zit als
twee Search-API-parameters in `topic.json`, en de zoektermen staan eronder.

## Wat waar staat

| Pad | Wat |
|-----|-----|
| `scripts/run_all.py` | de hele pijplijn, met kostenstop vóór de betaalde stappen |
| `scripts/keyword_research.py` | zoekvolumes via Keyword Planner; kiest de termen |
| `scripts/get_tagdata.py` | `bt.ean_score` (A-label) + `bt.revenue_per_product` (pixel) |
| `shared/topic.py` | topic-configuratie, paden, modelkeuze uit `.env` |
| `shared/taxonomy.py` | categorieboom ophalen, cachen, doorzoeken |
| `shared/llm_websearch.py` | één web-search-call bij OpenAI, met bronnen en kosten |
| `shared/pricing.json` | tarieven per model; ontbreekt een model, dan blijft de kostprijs `null` |
| `topics/<datum>_bestof-<categorie>/topic.json` | de categorie en de zoektermen |
| `topics/.../data/` | alle tussenbestanden en het eindbestand in `data/export/` |
| `cache/categories_nl-NL.json` | de 3.575 categorieën, eenmalig opgehaald |

## Dingen die je moet weten

**De categorie-slug mag je niet zelf bouwen.** De getallen in
`huishoudelijke_apparatuur_19968037_23583843` zijn geen categorie-id's —
Airfryers is id 9005486. De slug komt uit het `urlSlug`-label van de Taxonomy
API; `resolve_category.py` haalt hem op.

**De boom kost één keer twee minuten.** De Taxonomy API heeft geen
tree-endpoint en geeft per call één niveau kinderen, dus 3.575 categorieën =
3.575 calls. Daarna staat hij in `cache/`. Verversen met `--refresh`.

**`products-by-ids` geeft maar één winkel per product** (het beste aanbod),
`/search/product` geeft ze allemaal. Vandaar prijzen in batches van 50 en
aanbiedingen per product apart.

**Bezorgkosten bestaan niet in deze API.** Alleen de vervoerder
(`deliveryCompanies`). `delivery_cost` is daarom altijd `0.0`: geen ontbrekende
waarde maar een niet-bestaande.

**Een productsleutel is niet altijd een EAN.** Producten zonder EAN vallen
terug op hun `groupId`. Behandel die sleutel als ondoorzichtig — niet
per se numeriek, niet per se 13 cijfers.

**Het rankmodel ziet geen prijzen.** Dat is met opzet: dan kan het geen
prijsclaims in de copy laten lekken. De prijs komt er in stap 6 live bij.

**`source_urls_are_real` staat vaak op `false`.** Dat betekent niet "verzonnen"
maar "onbevestigd": het model schreef een URL op die het niet heeft geopend of
geciteerd. `unverified_urls` zegt precies welke.

**Een zoekvolume van 0 en "geen volume" zijn niet hetzelfde.** De gedeelde
`get_search_volumes()` in `backend/keyword_planner_service.py` maakt van een
keyword dat de API niet teruggaf een `0`. Daarop selecteren zou zo'n term stil
onderaan de ranglijst zetten. `keyword_research.py` gebruikt daarom de laag
eronder en houdt "niet teruggekomen" op `null`.

**Keyword Planner voegt enkelvoud en meervoud samen.** "beste airfryer" en
"beste airfryers" krijgen allebei 5.400/maand — dat is één keer 5.400, geen
twee termen. Ze zouden dezelfde pagina opleveren en wel twee ranglijsten
kosten, dus varianten worden samengevoegd tot de sterkste.

**Zoekvolume kiest op vraag, niet op intentie.** "kip airfryer" haalt
9.900/maand maar wie dat zoekt wil een recept, geen top-10 van apparaten. Kijk
de gekozen termen na en sluit zo'n facet uit met
`--skip-facet bereidingsprogramma`.

**De termtekst wordt paginatekst.** Facetwaarden leveren soms kromme
samenstellingen op ("draadloos koptelefoon" in plaats van "draadloze") die wel
volume hebben. Het model krijgt die term letterlijk als paginatitel, dus
corrigeer de tekst in `topic.json` als het in een kop terechtkomt.

**`bt.ean_score` heeft vier labels, geen drie.** Naast `A`/`B`/`C` bestaat
`APlus`. Filteren op `label = 'A'` gooit stil de béste producten weg.

**Elke historische rij in `bt.ean_score` draagt `load_end_date = 9999-12-31`,**
dus een "nog open"-filter levert alle versies op. Erger: één EAN kan meerdere
elkaar tegensprekende rijen hebben op dezelfde nieuwste `load_start_date`, en
dan is een `row_number()`-greep niet-deterministisch — twee runs, twee labels.
`get_tagdata.py` kiest de hoogste `totaal_ean_score` en schrijft in
`score_variants` hoeveel varianten het oneens waren. Bij `> 1`: eerst
controleren voordat je erop stuurt.

**Een lege `session_starts` is informatie, geen ontbrekende waarde.** Alleen
winkels die onze tag draaien rapporteren sessies en shopomzet. `transactions`
is veel breder gevuld omdat dat ook uit attributie komt, dus een
affiliate-winkel kan transacties tonen met nul sessies. Sommeer nooit over
aanbiedingen zonder te tellen hoeveel er rapporteren: `reports_sessions` is die
telling.

**Kale getallen worden geen zoekterm.** Een facet als "Aantal borstels" (2, 4,
6) zou "2 elektrische tandenborstel" opleveren. Die gaan er vóór het
volume-onderzoek uit, want anders dan taalfouten kunnen die nooit iets zijn.

**Facet-uitsluitingen zijn categoriewerk.** `skip_facets` in `topic.json` (of
`--skip-facet`) houdt facetten buiten de termen die een ándere productsoort
beschrijven: bij airfryers `bereidingsprogramma` (recepten), bij
tandenborstels `type_tand` (opzetborstels, flossers).

**Een pagina is een groep termen, niet één term.** `collect_products.py`
voegt termen met een identieke productlijst samen: bij elektrische
tandenborstels vielen vijf generieke termen ("… kopen", "… test", "…
aanbieding", "beste …") op één pagina van 59.880/maand. Zonder dat kreeg je
vier pagina's met dezelfde kop en hetzelfde #1-product — duplicate content, en
vier keer een rank-call. De term met het hoogste volume wordt de primaire, de
rest blijft als `also_targets` aan de pagina hangen. Exact gelijke
verzamelingen, geen gelijkenisdrempel: gemeten liggen de duplicaten op precies
1,00 en het eerstvolgende paar op 0,74.

Gevolg voor `--top N`: dat kiest *termen*, en na samenvoegen houd je minder
pagina's over. Wil je er tien, vraag er dan ruimer op (`--top 16`).

**`display` is de paginatitel, `term` de zoekterm.** Die twee lopen uiteen
zodra een facetwaarde krom samenstelt ("draadloos koptelefoon") of als de term
zelf met "beste" begint. De term blijft staan zoals mensen hem intypen; de kop
corrigeer je in `topic.json`.

**Kosten blijven `null` zolang het model niet in `pricing.json` staat.** Een
geraden tarief is erger dan geen tarief; vul het aan zodra je het echte tarief
hebt.

## Aanroepen als skill

De skill `get-top-10-data` (in `~/.claude/skills/`) wijst naar deze map, dus
"gebruik de top10 skill voor categorie X" komt hier uit.
