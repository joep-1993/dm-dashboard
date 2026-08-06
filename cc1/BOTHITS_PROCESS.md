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
