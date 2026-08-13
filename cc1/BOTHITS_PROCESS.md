# Bot Hits — CloudFront crawler-log analytics

Runbook voor de **Bot Hits**-tool (`/static/bothits.html`, prefix `/api/bothits`).
Vervangt het oude losse CSV-extractieproces; die scripts staan onderaan onder
"Wat er hiervóór was".

---

## Wat het is

Ruwe CloudFront-accesslogs → vier geaggregeerde tabellen in `pa.*` op de gedeelde
PostgreSQL (10.1.32.9) → een dashboard met splitsing per **bot**, **URL-type**,
**domein**, **datum** en **URL-niveau**.

| Bestand | Rol |
|---|---|
| `backend/bothits_s3.py` | haalt logs uit de S3-bucket naar de staging-map |
| `backend/bothits_ingest.py` | parser + loader + dagelijkse scheduler; ook CLI |
| `backend/bothits_service.py` | querylaag, 5-min in-process cache |
| `backend/bothits_router.py` | FastAPI-routes |
| `frontend/bothits.html` | dashboard (Chart.js 4.4.1, Bootstrap) |
| `scripts/bothits_schema.sql` | DDL, idempotent (`CREATE TABLE IF NOT EXISTS`) |

---

## Waarom de korrel is zoals hij is

Dit is de kern van het ontwerp en het is **gemeten, niet geschat**. Wie de tabellen
wil verbouwen moet deze drie cijfers eerst kennen.

**1. Volledige URL-korrel is 154M rijen over 116 dagen.** Te veel voor een DB die in
z'n geheel 22 GB is.

**2. Naar week of maand aggregeren helpt níet — de compressie is 1,05x.**
Gemeten over 2026-03-10 t/m 03-13: 4.820.092 dagrijen tegen 4.571.527 unieke.
Bots crawlen elke dag een grotendeels ándere set facet-URL's, dus elke dag levert
zijn eigen rijen op ongeacht hoe je de datum bucket. Dag/week/maand ≈ 140M/129M/126M.
**Het onbegrensde is de URL-ruimte, niet de datum-granulariteit.**

**3. 86% van die rijen bestaat niet in `pa.urls`.** Gemeten op 2026-03-10 (niet-product):

| filter | rijen | 116d-schatting |
|---|---|---|
| alles | 1.329.312 | 154M |
| alleen bots-whitelist | 1.302.209 (98,0%) | 151M |
| **alleen `pa.urls`** | **184.576 (13,9%)** | **21,4M** |
| beide | 176.556 (13,3%) | 20,5M |

Dus: **de `pa.urls`-filter doet het zware werk, de bot-whitelist doet de
signaalkwaliteit.** Een bot-whitelist alleen snijdt maar 2% weg, omdat het volume
Googlebot + OpenAI + Apple + Meta ís — precies wat je wilt houden.

Resultaat: URL-detail alleen voor `pa.urls`-leden, de weggelaten staart blijft
volledig telbaar in de cube via `is_known_url` + `facet_depth`, en de luidste
veroorzakers krijgen per dag een naam in `bothits_unknown_daily`.

---

## Omvang en groei — gemeten op 2026-08-11

Na de backfill van alle 116 archiefdatums (2026-02-14 t/m 06-09), met de keep-list op
`beslist.nl` actief:

| tabel | rijen | heap | index | totaal |
|---|---|---|---|---|
| `bothits_url_daily` | 14.257.695 | 1.162 MB | 1.076 MB | **2.238 MB** |
| `bothits_unknown_daily` | 587.507 | 84 MB | 110 MB | 194 MB |
| `bothits_daily` (cube) | 159.210 | 18 MB | 19 MB | 37 MB |
| dimensies + ledger | 186 | — | — | < 1 MB |
| **totaal** | **15.004.598** | | | **2,41 GB** |

Dat is **10,0% van de hele database** (24 GB). Kosten per rij, gemeten en bruikbaar voor
elke volgende schatting: **165 B** voor een URL-rij, **347 B** voor een unknown-rij,
**246 B** voor een cube-rij. De indexen zijn bijna zo groot als de data zelf — reken dus
op ~2 MB op schijf per MB ruwe feiten.

**Groei: gebruik de recente snelheid, niet het gemiddelde.** Het crawlvolume is over deze
periode bijna verdubbeld (eerste 14 dagen 2,26M hits/dag, laatste 14 dagen 4,05M):

| basis | rijen/dag | MB/dag | per jaar |
|---|---|---|---|
| hele periode (116 d) | 129.348 | 21,3 | 47,2M rijen / **7,6 GB** |
| laatste 14 volledige dagen | 176.828 | 28,8 | 64,5M rijen / **10,3 GB** |

Reken dus op **~10 GB per jaar** en 24 GB → ~34 GB als er een jaar bij komt, waarvan
bothits dan ~35% is. Knijpt dat, dan is de goedkoopste knop een retentie op
`bothits_url_daily` (94% van de omvang); de cube kost 0,3 MB/dag en houdt de lange
historie telbaar, dus die kan blijven staan.

## Tabellen

| Tabel | Korrel | Omvang |
|---|---|---|
| `pa.bothits_daily` | datum × host × bot × url_type × facet_depth × known × status × edge | ~2.500 rijen/dag |
| `pa.bothits_url_daily` | datum × url_id × host × bot (alleen `pa.urls`) | ~150k rijen/dag |
| `pa.bothits_unknown_daily` | top-500 per dag per bot-familie, buiten `pa.urls` | ~5.500 rijen/dag |
| `pa.bothits_ingest` | ledger per logdatum (idempotentie + volledigheid) | 1 rij/dag |
| `pa.bothits_host` / `pa.bothits_bot` | dimensies | tientallen |

`bothits_bot.is_tracked` is de URL-niveau-whitelist. Untracked families
(`other-bot`, `Monitoring`, `SEO-tools`, `Social`) tellen **wel** volledig mee in de
cube maar krijgen geen URL-rijen — zo kan een catch-all de feitentabel nooit
stilletjes opblazen. De ingest schrijft dimensierijen met `DO NOTHING`, dus een
handmatige `is_tracked`-wijziging overleeft een re-ingest.

**Sinds 2026-08-13 heeft die kolom een TWEEDE betekenis: het dashboard filtert er ook
op.** `_filters()` in `bothits_service.py` zet `b.is_tracked` in élke query, dus wat op
false staat verdwijnt uit de tegels, de grafieken én de tabel. Joep wil alleen de drie
Google-bots, Applebot, Bing en de grote AI-crawlers zien; van de 31 families in de logs
staan er nu **12 aan en 19 uit**. De cijfers gaan daarmee over 91,2 mln in plaats van
95,1 mln bot-hits (95,9%).

De twaalf: `Googlebot`, `Apple`, `GoogleOther`, `OpenAI`, `ByteDance`, `Perplexity`,
`Bing`, `DuckAssist`, `Amazon`, `Meta-AI`, `Anthropic`, `Google-AI`.

De zestien extra families staan **alleen in de DB** op false, niet in
`UNTRACKED_FAMILIES` in de ingest — dat is bewust. Die set bepaalt of er per-URL-rijen
geschreven worden, en de URL-tabellen mogen hun volle breedte houden: het waren de tabs
die eruit gingen, niet de data. De DB-vlag is dus ruimer dan de Python-set, en dat is
het verschil tussen "wat we bewaren" en "wat we tonen". Een familie terugzetten in het
dashboard is één UPDATE op `pa.bothits_bot`, geen deploy.

Nieuwe bots landen via de ingest op `is_tracked = f not in UNTRACKED_FAMILIES`, dus een
onbekende crawler valt in de catch-all `other-bot` en blijft onzichtbaar; een nieuw
benoemde familie vereist toch al een codewijziging in `CANON_NAMES`.

---

## Nachtelijke ingest: waar hij hoort te draaien (2026-08-13)

**De S3-retentie is een deadline, niet een eigenschap.** De bucket bewaart ~42 dagen,
dus elke dag zonder ophaal kost permanent één logdatum. Zo is het gat 06-10 t/m 07-03
(24 dagen) ontstaan, en zo groeide het tussen 11 en 13 augustus nog met drie dagen
terwijl er een notitie over lag. Dit is de reden dat ophalen een taak moet zijn en geen
gewoonte.

**Wat de ingebouwde scheduler wél en niet doet.** `start_scheduler()` wordt vanuit
`backend/main.py` bij startup gearmeerd en is een `threading.Timer` **in het
uvicorn-proces**. Twee dingen die je daarvan moet weten:

* `_fire()` roept `start_ingest_async("scheduled", on_done)` aan **zonder `before=`**,
  dus er wordt **niets uit S3 gehaald** — hij verwerkt alleen de dropfolder. Aanzetten
  van `BOTHITS_AUTO_INGEST` beschermt het S3-venster dus NIET.
* Hij draait op Joeps werkmachine, die 's nachts uit kan staan. `threading.Timer` wacht
  op `CLOCK_MONOTONIC`, die op Linux niet doortikt tijdens suspend, dus na een nacht
  slapen vuurt hij niet om 04:30 maar zoveel later als de machine weg was — en dat
  schuift elke nacht verder op.

**Daarom verhuist het ophalen naar een externe machine die altijd aan staat**, met een
Windows-taak als planner. De volledige overdrachtsprompt staat in
`Downloads\claude\bothits_nachtelijke_ingest_PROMPT.txt` (bevat geen credentials, alleen
de namen van de sleutels). Kern:

| | |
|---|---|
| planner | Windows-taak om 03:30, "ook als gebruiker niet is aangemeld" + "starten na gemiste start" + 3× retry |
| uitvoering | `scripts/bothits_nightly.py` (fetch + `run_drop`), exitcode 1 bij nul bekende URL's zodat de taak op "mislukt" gaat |
| blik | `BOTHITS_NIGHTLY_DAYS=5` — al geladen datums en al gedownloade bestanden worden overgeslagen, dus een gemiste nacht haalt zichzelf in |
| staging | `BOTHITS_STAGING_RETENTION_DAYS=7`, blijft rond 7 GB |
| eigendom | die machine is de ENIGE ophaler; bij Joep blijft `BOTHITS_AUTO_INGEST=false` |

**`fork` bestaat niet op Windows.** `ingest_date()` vraagt expliciet
`multiprocessing.get_context("fork")`; native Windows-Python kent alleen `spawn` en
gooit daar een `ValueError`. Vandaar dat de aanbevolen route de ingest **in WSL** op die
machine draait en de Windows-taak alleen `wsl.exe` aanroept — dan is de code identiek.
Wie het tóch native wil, moet de pool op spawn zetten mét een `initializer` die per
worker `load_url_ids()` en `load_ip_ranges()` doet; zonder dat is de uitkomst stil
kapot (zie de is_known_url-bevinding in TASKS).

---

## Staging, archief en het herstelpad (2026-08-13)

Verwerkte bronbestanden verhuizen naar `<staging>/_processed/<datum>/`. Drie dingen om
te weten sinds de audit:

* **Retentie.** `BOTHITS_STAGING_RETENTION_DAYS` (default **21**, `0` = nooit) ruimt na
  een run datummappen op die ouder zijn dan de grens. Ze bleven eerder eeuwig staan:
  ~900 MB per logdatum, 30 GB op de meetdag. 21 dagen omdat de bucket ~42 dagen bewaart —
  binnen die termijn is een datum zowel opnieuw te downloaden áls lokaal te herladen.

  **Sinds 2026-08-13 ruimt óók `backfill` op, en dat was de hele bug.** `_prune_archive`
  hing alleen aan `run_drop`, terwijl het werk juist via `backfill` liep — het herstelpad,
  de 30-datum-herlaad, de CLI. De retentie zat er dus wel in en vuurde nooit: gemeten op
  13-08 stonden er 18 datummappen voorbij de grens, **18 GB**, met de oudste op 2026-07-04.
  Als je de staging ziet groeien terwijl retentie "aan" staat, is dit de eerste plek om te
  kijken: welk codepad heeft de laatste runs gedaan.
* **Het archief wordt écht overgeslagen.** De skip in `scan_tree` vergeleek de basename
  met `_processed` en sloeg dus alleen die map zelf over, niet de datum-submappen: elke
  run schuimde het hele archief af (46.097 bestanden op de meetdag, +2.900/dag).
* **Herstel van één datum**, en dit is de procedure die je wilt als een ingest fout ging:

  ```bash
  # opnieuw verwerken UIT HET ARCHIEF, zonder te downloaden. Eén commando.
  python -m backend.bothits_ingest backfill --src ~/bothits_s3 --date 2026-08-12
  ```

  **De ledger-rij hoef je NIET weg te gooien** — dat stond hier tot 2026-08-13 wel, en
  het was onnodig. `--date` vult `only` en `run_backfill` bouwt `todo` dan opnieuw op
  zonder de `already_ingested()`-filter (`bothits_ingest.py:764-765`), dus de datum wordt
  gewoon opgepakt. De ingest is idempotent per logdatum (`DELETE ... WHERE log_date = X`),
  dus de oude feiten worden vervangen. Die DELETE was ook actief schadelijk zolang het
  standaard datumbereik nog aan de ledger hing: hij verschoof stil het venster waarop de
  tool opende. Dat hangt sinds `a5ecefa` aan de cube, maar `/meta` leest nog wél de ledger
  (fase 3 van de audit), dus voeg die stap niet terug toe.

  `--date` zet `include_archived=True`, dus dit leest wél uit `_processed/`. Zonder die
  vlag vindt een gerichte herlaad niets en moet je 900 MB per dag opnieuw uit S3 halen.

**Draai een lange ingest nooit via de server met `--reload`.** Elke `.py`-edit herstart
uvicorn en breekt de run af. Los proces:
`setsid nohup env PYTHONPATH=<repo> ./venv/bin/python <script> > log 2>&1 &`. En het is
ook nog sneller om een andere reden — zie hieronder.

---

## Logs uit S3 halen (knop "Nieuwe logs ophalen", 2026-08-11)

Tot 11-08 was dit handwerk: `~/projects/cloudfront-logs/download_cloudfront_logs.py`
naar `Downloads\Cloudfront`, dan met de verkenner naar de dropfolder. Nu zit het in
`bothits_s3.py`, achter een knop in de kaartkop van **Hits per dag** — naast Refresh,
niet erin: Refresh is een her-query van <1s en dit is minuten per logdatum.

```
GET  /api/bothits/s3/preview?days=3   → per datum files/MB/uren + reden, zonder download
POST /api/bothits/s3/fetch?days=3     → download + ingest, achtergrond
GET  /api/bothits/ingest/status        → dezelfde poller, nu met `phase` en `fetch`
```

`days` telt terug vanaf **gisteren**; vandaag valt er altijd buiten, want die is nooit
24 uur compleet en de ingest zou hem toch weigeren. De download hangt via
`start_ingest_async(before=…, src=…)` aan hetzelfde lock als de dropfolder-ingest, dus
een nachtelijke run kan niet halverwege een download beginnen te parsen.

**Vier gemeten eigenschappen van de bucket** (11-08-2026):

1. **Keys staan onder `cloudfront/<DIST>.<YYYY-MM-DD>-<HH>.<hash>.gz`.** Dus één
   prefix-list per (distributie, datum) haalt precies één dag op — 6 gerichte lists in
   plaats van de volledige bucketscan die het losse script per datum deed (~230
   pagina's, inclusief duizenden `export-2022-*`-objecten die geen CloudFront zijn).
2. **Zes distributies**, gevonden via `Delimiter="."` (één call, `CommonPrefixes`):
   `E14VW8EO449KG7`, `E1M5IC93ZML0R0`, `E2XB3ULGPCQATU`, `E3NSMIRDIMNYHL`,
   `E3QQH7GDBASLV1`, `EKJMSLJWXI3M0`. Niet hardcoded — een zevende wordt opgepikt.
3. **Retentie ≈ 42 dagen.** Oudste key op 11-08 was 2026-06-30; 06-29 en ouder zijn
   leeg. Vandaar `days ≤ 45` en de expliciete status `niet_in_s3` per datum: een lege
   ophaal moet niet als kapotte knop lezen. **Elke dag die je laat liggen verdwijnt.**
4. **Eén dag ≈ 2.900 bestanden en ~900 MB.** Daarom parallelle downloads
   (`BOTHITS_S3_WORKERS`, default 8), overslaan wat er met dezelfde grootte al ligt, en
   een preview die de UI het volume laat quoten vóór de klik.

### Het gat in de historie

| bron | dekking (11-08-2026) |
|---|---|
| geïngest (`pa.bothits_ingest`) | 2026-02-14 t/m **03-16** (23 datums) |
| lokaal archief (`BACKUP_DIR`) | 2026-02-14 t/m **06-09** → 03-17..06-09 nog te backfillen |
| S3 | **06-30** t/m gisteren |

**2026-06-10 t/m 06-29 (20 dagen) zit in geen van beide** en is definitief weg. De
85 dagen 03-17..06-09 staan al op schijf: die horen via `backfill` te gaan, niet via
S3 — dat scheelt ~75 GB download voor data die je al hebt.

### Staging-map, niet de dropfolder

`BOTHITS_S3_DIR` (default `~/bothits_s3`) is WSL-lokaal en bewust **niet** `DROP_DIR`:
die default staat onder OneDrive op `/mnt/c`, en 900 MB per dag door die sync trekken is
precies de I/O-hang uit `onedrive_wsl_file_hang`. De dropfolder blijft bestaan voor het
handmatige pad (dat pad moet juist vanuit Windows te bereiken zijn); `run_drop(src=…)`
nam de map altijd al als argument.

## Draaien

```bash
# volledige backfill uit het archief (slaat al geladen datums over)
python3 -m backend.bothits_ingest backfill

# één datum opnieuw
python3 -m backend.bothits_ingest backfill --date 2026-03-16

# dropfolder verwerken (wat de knop en de nachtelijke job ook doen)
python3 -m backend.bothits_ingest drop

python3 -m backend.bothits_ingest status
```

~55 s per logdatum op 16 cores; 116 dagen ≈ 1 uur 45.

**Elke wijziging in `process_file()` hoort langs `scripts/bothits_parse_fingerprint.py`.**
Dat draait de parser over een vaste set echte `.gz`-bestanden en zet een md5 over de
gesorteerde cube-, known- én unknown-items; identiek vóór en ná betekent dat de hot loop
niet is verschoven. Twee dingen die het harnas zelf moest leren: sorteer de steekproef op
**grootte per distributie** (alfabetisch pak je 20 bestanden van ~480 B met samen 335
regels en nul bot-hits — een groene test die niets aanraakt), en zet bij een baseline altijd
de **commit** waarop hij gemaakt is, want anders meet je een codewijziging en een
datawijziging tegelijk (zie LEARNINGS, de 142.054-vs-142.057-kwestie).

**Sinds 2026-08-13 sneller én betrouwbaarder** — en die twee hangen samen met hoe je hem
start:

* De UA-classificatie zat in de hot loop: per logregel een `unquote()` plus tot 33
  regex-searches, terwijl een logdatum maar ~950 unieke user-agents draagt. Er zit nu een
  memo op de ruwe UA-string met een unie-regex als snelle afwijzing ervoor. Gemeten:
  0 verschillen op 117.492 echte logregels, byte-voor-byte identieke uitkomst
  end-to-end, en **125 s → 29 s** voor 2026-08-12 (2.905 bestanden, 6,9 mln regels).
* De pool krijgt een **expliciete fork-context**. Zonder dat erfde hij de start-methode
  van hoe de app gestart was, en `uvicorn --reload` gebruikt spawn — dan importeren de
  workers de module opnieuw en beginnen ze met een lege `URL_IDS` en `IP_RANGES`. Dat
  heeft 30 logdatums verpest (`known_rows = 0`, `verify_state = 'unchecked'`). De
  tripwire schreeuwt nu in het log als een datum met >100k bot-hits nul bekende URL's
  oplevert.

**Idempotent per logdatum**: elke ingest doet eerst `DELETE ... WHERE log_date = X`.
Dezelfde map twee keer droppen verdubbelt dus niets. (Dat is ook waarom een
`TRUNCATE` nooit nodig is — en die wordt door een hook geblokkeerd op deze
gedeelde DB, terecht.)

### Env

| Var | Default | Betekenis |
|---|---|---|
| `BOTHITS_BACKUP_DIR` | `…/Downloads/claude/bothits_new/backup` | historisch archief |
| `BOTHITS_DROP_DIR` | `…/Downloads/claude/bothits_drop` | nieuwe logs |
| `BOTHITS_AUTO_INGEST` | `false` | nachtelijke run aan/uit |
| `BOTHITS_AUTO_INGEST_AT` | `04:30` | tijdstip |
| `BOTHITS_WORKERS` | `12` | parallelle parsers |
| `BOTHITS_KEEP_SOURCE` | — | `1` = niet naar `_processed/` verplaatsen |
| `BOTHITS_STAGING_RETENTION_DAYS` | `21` | hoe lang `_processed/<datum>/` blijft staan; `0` = nooit opruimen |
| `BOTHITS_UA_MEMO_MAX` | `50000` | grens op de UA→familie-memo (een spoofer kan oneindig unieke UA's sturen) |
| `BOTHITS_S3_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` | — | S3-credentials; zonder deze geeft de knop een 400 i.p.v. een 500 |
| `BOTHITS_S3_BUCKET` / `_PREFIX` / `_REGION` | zie `bothits_s3.py` | logbucket, `cloudfront/`, `eu-west-1` |
| `BOTHITS_S3_DIR` | `~/bothits_s3` | staging voor de download (WSL-lokaal) |
| `BOTHITS_S3_WORKERS` | `8` | parallelle downloads |
| `BOTHITS_RANGE_CACHE` | `~/.cache/bothits/ipranges.json` | gecachete officiële IP-ranges |
| `BOTHITS_RANGE_TTL_H` | `24` | hoe lang die cache geldig is |
| `BOTHITS_KEEP_DOMAINS` | `beslist.nl` | komma-lijst; elk domein dekt zijn subdomeinen. **Alles daarbuiten wordt niet geteld** — zie Domeinfilter |

De scheduler is een `threading.Timer` zoals `gsd_ll_service`, **geen APScheduler** —
en staat default uit. Een tweede uvicorn met een eigen scheduler is precies hoe de
GSD low-linkage spookruns ontstonden. Knop en timer delen één lock.

---

## Valkuilen in het bronarchief

Alle vier gemeten op het echte archief; ze zijn afgedekt in `scan_tree()`, maar wie
de parser aanraakt moet ze kennen.

1. **Mapnamen zijn downloaddatums, geen logdatums.** Map `1-5-2026` bevat logs van
   2026-04-22 t/m 05-01. Lees de datum altijd uit de *bestandsnaam*.
2. **Eén logdatum kan over twee mappen liggen.** `26-3-2026` loopt terug tot
   2026-02-14, dus 2026-03-15/16 zitten óók in `16-3-2026`. Scan de hele boom en
   groepeer op datum, anders laad je een halve dag. *(Dit ging in de eerste testrun
   fout: 3.710.645 regels i.p.v. 6.666.792 — 44% gemist.)*
3. **Dubbele bestanden.** Datums die over twee mappen liggen noemen deels dezelfde
   CloudFront-objecten. Dedupliceren op basename, anders tel je dubbel.
4. **1.039 bestanden zijn uitgepakte kopieën** (magic `#Ver`, geen gzip) naast hun
   `.gz`-tweeling. Alleen `.gz` nemen is veilig: geverifieerd dat **0** platte
   bestanden zonder `.gz`-tweeling bestaan.

Verder: het archief bevat **6 CloudFront-distributies** en **3 domeinen**
(beslist.nl, beslist.be, shopcaddy.de). Het domein komt uit `x-host-header` — de
oude CSV-export gooide die kolom weg, wat die export ongeschikt maakt als bron.

**Onvolledige dagen.** Vijf logdatums missen uren omdat ze de afkapdag van een
download-batch zijn: 2026-03-26 (17/24), 04-13 (8/24), 04-21 (8/24), 05-01 (9/24),
06-09 (9/24). Die staan als `is_complete = false` in de ledger en het dashboard
waarschuwt erover — een halve dag naast hele dagen leest anders als een
verkeersinstorting die nooit gebeurd is.

### Wat "compleet" sinds 2026-08-13 betekent

`is_complete` hing alleen aan `hours_present >= 24`. Nu aan drie dingen:

1. **alle 24 uurbuckets** — vangt een dropfolder die nog volloopt;
2. **`failed_files = 0`** — een afgebroken gzip leverde eerder stil een te korte dag op,
   want `process_file` gaf de regels terug die het vóór de fout had gelezen;
3. **`files >= expected_files`** — wat S3 zei te hebben, uit
   `<staging>/_manifest/<datum>.json` dat `fetch()` achterlaat. `NULL` bij een backfill
   uit het lokale archief; dan gelden alleen 1 en 2.

**Dit is de enige harde maat.** Het bestandsaantal op zichzelf zegt niets: complete dagen
lopen legitiem van 1.591 tot 4.969 bestanden. `raw_lines` is wél stabiel (5,6–7,9 mln per
dag) maar dat weet je pas ná het parsen.

**Een distributie met minder dan 24 uur is GEEN bewijs van verlies** — en die verleiding
is echt, dus hier de meting. Drie van de 21 staging-datums hebben er een: 07-31 (22 uur),
08-10 (23), 08-11 (23), en het is elke keer `E14VW8EO449KG7`, de kleinste distributie met
139 bestanden per dag (≈5,8 per uur). De missende uren zijn 00, 02 en 19. Een uur zonder
één request levert geen logbestand op. Zou je dit tot eis maken, dan valt 14% van de
datums om als "incompleet" en pakt `run_drop` ze daarna nooit meer op. Het staat er als
waarschuwing in (`_warn_thin_distributions`), bewust niet als poort.

**Gevolg voor het opruimen, en dit is de belangrijkste regel:** bronbestanden worden
alleen gearchiveerd ná een compleet geladen datum, en `_prune_archive` wist nooit een
datummap die niet als compleet in de ledger staat. Kan de DB niet gelezen worden, dan
ruimt hij niets op. Opruimen is nooit dringend genoeg om te doen zonder te weten wat er
al veilig binnen is — buiten het S3-venster van ~42 dagen is er geen tweede kopie.

---

## Metriek-valkuil: "verspilling" moet je over `/c/` meten

`pa.urls` bevat **alleen** `/c/`-categorie/facet-URL's (1.028.016 van 1.031.796 met
`/c/`), dus élke productpagina is per definitie "niet in pa.urls". Verspilling over
álle hits meten geeft ~94% en betekent niets — productpagina's zijn gewoon legitieme
pagina's. De tegel **Facet-verspilling** rekent daarom alleen over
`category` + `category_facet` + `category_legacy`.

**Dezelfde val zat sinds 2026-08-13 óók in de Facet-diepte-grafiek**, en die stond er
langer in. `facet_depth()` geeft 0 terug voor álles zonder `/c/` — productpagina's, de
homepage, `robots.txt`, assets — dus die vielen allemaal in de nul-balk. Over het
standaardvenster van 30 dagen was depth 0 **49.225.165 hits waarvan maar 7.682.976
(15,6%) categorie-vormig**. De grafiek las dus als "de meeste crawl gaat naar
categoriepagina's zonder facetten", terwijl 84% van die balk verkeer was dat per
definitie geen facetten *heeft*. De query filtert nu op dezelfde drie url_types en de
kaartkop noemt de reikwijdte. Depth ≥1 verandert nauwelijks maar niet nul: daar valt
1.355 hits weg (`/l/`-URL's met `/c/` erin, plus wat `other`).

Regel voor de volgende die hier iets meet: **elke noemer die over "facetten" of
"verspilling" gaat, hoort beperkt te zijn tot category-URL's.** Er zijn er nu twee, en
ze horen bij elkaar te blijven.

---

## Metriek-valkuil: `status_class = '2xx'` bevat de WAF-challenge (2026-08-13)

AWS WAF antwoordt onbekende bots met een **202 of een 405**, en die 202 landt in de cube
als `status_class = '2xx'`. Dat leest als "pagina bestaat" en dat is het niet.

Zo herken je ze in de ruwe log: `x-edge-result-type = 'Error'`, afgehandeld in
0,001–0,003 s, vaste bodygrootte (~2,45 KB / 590 B / 3.016 B) — de origin is nooit geraakt.
Site-breed is dit 1,90% van alle bot-hits; per bot loopt het van 0% (AdsBot-Google) tot
96,7% (SemrushBot).

**De cube bewaart alleen de statusKLASSE, dus dit is er niet uit te filteren.** Elke
conclusie over levende versus dode URL's moet uit de ruwe logs komen (`sc-status` +
`x-edge-result-type`). Concreet voorbeeld van hoe het misgaat: de cube meldt 28,7% 2xx op
`product_legacy`, maar van 224.242 legacy product-URL's gaf er **niet één ooit een echte
200**.

## Wat de tabellen NIET kunnen: de lange staart (2026-08-13)

`pa.bothits_unknown_daily` houdt **top-500 per dag per bot-familie**. Die 500 plekken gaan
naar de luidruchtige assets, dus een platte verdeling valt er volledig buiten. Gemeten:
nul rijen met `url_type = 'product_legacy'`, terwijl de cube 3,0 mln legacy-hits telt —
want dat zijn 224.242 URL's met gemiddeld 1,14 hit.

**Sinds 2026-08-13 raakt die beperking nog maar de hélft van de URL's-tab.** Die leest nu
uit twee tabellen, samengevoegd en opnieuw gerankt:

| bron | wat erin zit | hoe volledig |
|---|---|---|
| `pa.bothits_url_daily` | URL's die in `pa.urls` staan | **uitputtend** — elke hit, elke dag |
| `pa.bothits_unknown_daily` | de staart daarbuiten | top-500 per dag per bot-familie |

De kolom **"In pa.urls"** zegt per rij welke van de twee je leest, want de benen zijn niet
symmetrisch en dat mag je niet verstoppen. Daarvoor las de tab alléén de onbekende tabel:
op 2026-08-12 zag hij 41.427 van 3,39 mln bot-hits (**1,2%**) en kon hij geen enkele
indexeerbare `/c/`-URL tonen. Dat was een noodgreep uit de periode dat `url_daily` stilstond
(`known_rows = 0`), en die reden verviel met de fork-fix zonder dat de querylaag meeging.

**Productpagina's blijven onzichtbaar in deze tab**, en dat is nu de echte beperking: ze
staan niet in `pa.urls` en gaan ook nooit naar `unknown_daily` (bijna uniek per hit, dus een
top-N erover is ruis). Filter je op PLP, dan zegt de tab dat met zoveel woorden i.p.v. een
lege tabel te tonen. Hun hits tellen wél volledig mee in het Overzicht — dat komt uit de cube.

Terugvalpad is de ruwe log, en dat is betrouwbaar: parsen met `classify_ua`, `url_type`,
`norm_host` en `skip_host` uit `bothits_ingest.py` gaf op 2026-08-12 exact de cube-cijfers
(12.939 legacy-hits, 12.711 4xx). Valideer altijd eerst op één dag tegen de cube.

**Let op de klok**: S3-retentie is ~42 dagen. Referer- en statusniveau-vragen ("wie linkt
hiernaar") kun je alleen stellen zolang de logs er nog zijn.

---

## IP-verificatie (2026-08-11)

`backend/bothits_verify.py` toetst elk bot-IP aan de **officieel gepubliceerde
IP-ranges** van de operator die de user-agent claimt. Uit het oude
`~/bothits_verify.py` is bewust alléén de `RANGE_SOURCES`-tabel overgenomen.

Vier uitkomsten, als dimensie `verify_state` op `pa.bothits_daily` (en als
splitsing "IP-verificatie" in Hits per dag):

| state | betekenis |
|---|---|
| `verified` | IP valt binnen een gepubliceerde range van de geclaimde operator |
| `failed` | operator publiceert ranges, dit IP zit er niet in — **de tripwire** |
| `unverifiable` | operator publiceert geen lijst (Meta, ByteDance, Amazon, CommonCrawl, SEO-tools, other-bot) |
| `unchecked` | lijsten niet op te halen deze run, kapot `c-ip`, of rij van vóór 11-08-2026 |

**Gemeten op 2026-03-10 (3.652.718 bot-hits):** `verified` 87,15%,
`unverifiable` 12,51%, `failed` **0,35%**. Die 12.726 failed-hits zijn OpenAI
12.570, Googlebot 85, Anthropic 67, Bing 4. Per familie: Googlebot, GoogleOther
en Apple **100% verified**, Bing 99,9%, OpenAI 97,6%.

Vier ontwerpkeuzes, elk met de meting erachter:

1. **Geen rDNS.** Dat was in het oude script het dure deel (8s timeout, 64
   threads) en is onnodig: de vier grootste families matchen volledig op range.
   Een dag heeft maar ~17.500 unieke bot-IP's, dus een prefix-check kost niets.
2. **Dimensie, geen filter.** Bij 0,4% spoof zou filteren geen cijfer
   veranderen maar wel data kosten. `failed` is nuttiger als alarm.
3. **Bisect, geen lus.** ~1.460 prefixes × 17.500 IP's = 26M containment-checks
   per dag als je er lineair door loopt; nu O(log n) per IP, plus memoisatie per
   (ip, operator). Ranges worden in de **parent** geladen (`load_ip_ranges()` in
   `ingest_date`) en via fork geërfd, net als `URL_IDS`.
4. **`unchecked` ≠ `failed`.** Een mislukte fetch of een kapot `c-ip`-veld mag
   echte Googlebot nooit als spoof wegzetten. Bij een mislukte fetch valt de
   loader terug op een verouderde cache — ranges verschuiven met weken, niet uren.

Cache: `~/.cache/bothits/ipranges.json`, TTL 24 uur (`BOTHITS_RANGE_CACHE`,
`BOTHITS_RANGE_TTL_H`). Meta's endpoint geeft sinds kort 404 (zij publiceren via
whois op AS32934), en Meta-AI is 7,39% van het verkeer — die categorie is dus
geen afrondingsfout.

De schemawijziging (`verify_state` in de kolommen én in de PK) draait zichzelf
via `SCHEMA_MIGRATE` in `bothits_ingest.py`, met dezelfde catalogus-guard als
`faq_v2_publisher`: een `ALTER TABLE` pakt een AccessExclusiveLock ook als er
niets te doen valt, en dat deadlockt tegen een lopende ingest.

## Domeinfilter — ALLEEN beslist.nl (2026-08-11)

> **Dit is een bewuste inperking, geen bug.** De tool telde alle zes hosts uit de
> logs. Sinds 11-08-2026 gaat er **alleen `beslist.nl` en zijn subdomeinen** in.
> Hieronder staat precies wat eruit is en hoe je het terugzet.

```python
# backend/bothits_ingest.py
KEEP_DOMAINS = tuple(... os.getenv("BOTHITS_KEEP_DOMAINS", "beslist.nl") ...)

def skip_host(h):
    return not any(h == d or h.endswith("." + d) for d in KEEP_DOMAINS)
```

Een **keep-list**, niet een skip-list, want er vielen drie soorten hosts weg die
niets gemeen hebben: andere markten (`beslist.be`, `shopcaddy.de` + hun
`shop.*`-varianten) en de CDN-distributies zelf (`*.cloudfront.net` — geen site
maar de oorsprong). Met een skip-list moet je die alle drie blijven onderhouden en
glipt een nieuwe distributie of markt er stil in.

### Wat eruit is (gemeten over de 24 geladen logdatums, vóór verwijdering)

| host | hits | verkeer | tabelruimte |
|---|---|---|---|
| beslist.be | 32.285.354 | 613,5 GB | ~162 MB |
| shopcaddy.de | 11.041.493 | 176,9 GB | ~7 MB |
| shop.beslist.be | 157.455 | 0,3 GB | ~0 MB |
| shop.shopcaddy.de | 41.460 | 0,1 GB | ~0 MB |
| **samen** | **43.525.762 (45,08%)** | **790,8 GB** | **~169 MB** |
| *(eerder al weg)* `*.cloudfront.net` | 603 | — | ~0 MB |

Verwijderd: 28.116 cube-rijen, 970.396 URL-rijen, 37.405 unknown-rijen, 4
dimensierijen. Wat overblijft: `beslist.nl` (52.546.477 hits) en
`shop.beslist.nl` (477.052).

**Waarom óók de bestaande rijen weg, en niet alleen nieuwe ingests:** anders
verandert de samenstelling van de reeks halverwege. Een grafiek waarin BE t/m een
zekere datum meetelt en daarna niet, leest als een verkeersval die nooit gebeurd
is — dezelfde reden als bij de onvolledige dagen hierboven.

`shopcaddy.de` was trouwens 11,44% van de hits maar slechts 7 MB opslag: `pa.urls`
bevat geen Duitse paden, dus vrijwel elke DE-crawl viel buiten de bekende set en
kreeg geen URL-detail. Opslag zit waar URL-detail zit, niet waar de hits zitten.

### Terugzetten

Twee stappen, en de tweede is de kritische:

```bash
# 1. de markten weer toelaten (in .env)
BOTHITS_KEEP_DOMAINS=beslist.nl,beslist.be,shopcaddy.de

# 2. de datums die je terug wil opnieuw ingesten — zonder dit blijft de historie leeg
python3 -m backend.bothits_ingest backfill --date 2026-03-10 --redo   # per datum
python3 -m backend.bothits_ingest backfill --redo                     # alles in het archief
```

De ingest is idempotent per logdatum (`DELETE ... WHERE log_date = X` vooraf), dus
een re-ingest herstelt exact dezelfde rijen — inclusief de weggehaalde hosts.

**Waar de terugzetbaarheid ophoudt**, en dit is het punt om te bewaken:

| bron | dekking | terugzetbaar? |
|---|---|---|
| lokaal archief `BOTHITS_BACKUP_DIR` | 2026-02-14 t/m 06-09 (102 GB, 229.288 `.gz`) | **ja, zolang die map bestaat** |
| S3-bucket | laatste ~42 dagen | ja, binnen dat venster |
| 2026-06-10 t/m 06-29 | in geen van beide | nee, definitief weg |

Dus: **het archief is de enige kopie van BE/DE-crawlgedrag van feb–juni.**
Wordt die map opgeruimd, dan is die historie niet meer te herstellen — de logs zijn
inmiddels uit S3 verlopen.

`raw_lines` in de ledger blijft het aantal regels in de logbestanden, ook van
weggefilterde hosts: dat is de volledigheidsmaat van een logdatum en moet op het
bestand kloppen, niet op wat wij ervan bewaren.

## Bot-taxonomie

`bot_class` ∈ `ai` / `search` / `seo-tool` / `social` / `monitoring` / `other`.
Volgorde in `BOT_FAMILIES` is functioneel: `Google-AI` en `GoogleOther` moeten vóór
`Googlebot` staan, `Applebot-Extended` vóór `Applebot`, `meta-externalagent` vóór
`facebookexternalhit`.

Twee crawlers die mensen zoeken en niet bestaan:
- **Gemini heeft geen eigen crawler** — Google haalt op met `Google-Extended` en
  `Google-CloudVertexBot` (familie `Google-AI`).
- **Copilot ook niet** — dat rijdt mee op `bingbot`.

`bot_name` wordt via `CANON_BY_LOWER` teruggevouwen op onze eigen spelling. Zonder
dat leveren `DiffBot` en `Diffbot` twee dimensierijen voor één crawler op, want
`re` geeft de tekst terug zoals hij in de user-agent stond.

---

## Wat er hiervóór was

De losse scripts in `~/`: `bothits_filter.py`, `bothits_verify.py`,
`bothits_finalize.py`, `bothits_merge.py`, met output in
`Downloads\claude\bothits_new`. Die filterden op **11 AI-bots** (geen Googlebot of
Bingbot), hielden 7 kolommen over en lieten `x-host-header` vallen. Ze deden ook een
IP-verificatiestap (officiële ranges + rDNS) die de nieuwe pipeline **niet** heeft —
de nieuwe classificatie is puur user-agent-gebaseerd, dus een bot die zich voordoet
als Googlebot telt mee. Als dat gaat knellen: de verificatielogica staat nog in
`~/bothits_verify.py`.

De CSV's blijven staan; het ruwe archief eronder (`backup/`, 102 GB, 229.288 `.gz`,
116 aaneengesloten dagen 2026-02-14 t/m 2026-06-09) is de bron voor deze tool.
