# findfacetvalues

Kandidaat-facetwaarden van één facetsoort uit de producttitels en -beschrijvingen van een
Beslist-categorie, met maandelijks zoekvolume, naar Excel.

Aangestuurd door het slash-command `/findfacetvalues`
(`~/.claude/commands/findfacetvalues.md`). Daar staat ook de schiftregel per facetsoort.

## Waarom drie stappen

De middelste stap is opzettelijk geen code. Of `waterdicht` een waarde van *Opties* is en
`laadregelaar` een waarde van *Type*, is een betekenisoordeel; een woordenlijst krijgt dat
niet betrouwbaar goed voor een open verzameling facetsoorten. `scan.py` doet het mechanische
werk, het model schift, `volumes.py` rekent af.

```
scan.py     categorie + facet resolven, producten ophalen, kandidaten extraheren
            -> scan.json (alles) + review.tsv (de bovenste N)
[schiften]  -> keep.txt, één term per regel
volumes.py  zoekvolume ophalen, filteren op drempel, Excel schrijven
```

## Gebruik

```bash
V=/home/joepvanschagen/projects/dm-dashboard/venv/bin/python
D=/home/joepvanschagen/projects/dm-dashboard/scripts/findfacetvalues

$V $D/scan.py --url "https://www.beslist.nl/products/klussen/klussen_486172_9134130/" \
              --facet "Opties" --workdir ./run --review 600
# ... schift review.tsv naar run/keep.txt ...
$V $D/volumes.py --workdir ./run --min-volume 50
```

`--mode measure` in `scan.py` voor Afmetingen/Maat/Formaat: extraheert maatpatronen
(`60x60 cm`, `100 wp`) in plaats van woordcombinaties.

## Twee dingen die niet vanzelf spreken

**De categorie-ids in de URL kent de Taxonomy API niet.** `klussen_486172_9134130` zijn
legacy-ids. De echte categorie-id (9005004) komt uit de `categories`-lijst die de Search API
bij een `mainCategory`-call teruggeeft, gematcht op `urlName`. Dat doet `common.resolve_category`.

**Keyword Planner laat in grote batches stil rijen weg.** Een batch van 10.000 leverde
9.355 rijen; de ontbrekende keywords zijn niet te onderscheiden van een echte 0 — `wiesbaden`
kwam zo als 0 terug terwijl het 6.600/maand is. `volumes.get_volumes` vraagt daarom op in
batches van 500, houdt bij wat terugkwam, en herhaalt de ontbrekende in kleinere batches tot
alles een waarde heeft. Gebruik die functie, niet `keyword_planner_service.get_search_volumes`
(die heeft `BATCH_SIZE = 10000` en maakt van een ontbrekende rij een 0).

`.cache/` bevat de facetlijst en facetwaarden uit de Taxonomy API, 7 dagen geldig, gitignored.
