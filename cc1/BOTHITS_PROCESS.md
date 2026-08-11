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

---

## Metriek-valkuil: "verspilling" moet je over `/c/` meten

`pa.urls` bevat **alleen** `/c/`-categorie/facet-URL's (1.028.016 van 1.031.796 met
`/c/`), dus élke productpagina is per definitie "niet in pa.urls". Verspilling over
álle hits meten geeft ~94% en betekent niets — productpagina's zijn gewoon legitieme
pagina's. De tegel **Facet-verspilling** rekent daarom alleen over
`category` + `category_facet` + `category_legacy`.

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
