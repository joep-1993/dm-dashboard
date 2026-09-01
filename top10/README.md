# Top-10 data pijplijn

Bouwt de data achter een "beste X"-pagina voor **elke** Beslist-categorie:
kandidaatproducten, een web-search-review per product, klikdata, een door een
model gescoorde top-10 per zoekterm, prijzen en winkelaanbod — platgeslagen tot
één JSON-bestand dat een paginabouwer kan consumeren.

Overgezet uit de skill `get-top-10-data`, die vastzat aan één categorie
(airfryers) en aan bestanden buiten deze repo. Hier komt de categorie uit
`topic.json` en draait alles op de repo zelf.

## Een categorie draaien

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
| `scripts/keyword_research.py` | zoekvolumes via Keyword Planner; kiest de termen |
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

**Kosten blijven `null` zolang het model niet in `pricing.json` staat.** Een
geraden tarief is erger dan geen tarief; vul het aan zodra je het echte tarief
hebt.

## Nog niet gedaan

- **Eén commando voor de hele pijplijn** en de skill-beschrijving bijwerken.
