# attic — opgeborgen, niet weggegooid

Wat hier staat is niet in gebruik maar ook niet weg. Opgeborgen op 2026-08-14 om de
projectroot leesbaar te maken; niets is verwijderd. Alles kan terug met één `git mv`.

De reden dat dit een map is en geen `git rm`: bij twijfel niet weggooien. Een map die te
rommelig is, is hinderlijk; een weggegooid bestand dat iets bleek te doen, kost een middag.

## docker/ — `Dockerfile`, `docker-compose.yml`

`CLAUDE.md` zegt het al: dit project draait **zonder Docker** (FastAPI + uvicorn, gestart door
de Windows-launcher `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1`). Geen code
verwijst naar deze twee bestanden; alleen de documentatie doet dat nog.

**Ze werken niet meer vanaf deze plek** en dat is met opzet niet gerepareerd:
`docker-compose.yml` mount `./backend`, `./frontend`, `./themes` en
`../theme_ads/thema_ads_optimized` relatief aan de projectroot. Wil je Docker weer gebruiken,
zet beide bestanden dan terug in de root — dan kloppen die paden weer:

```bash
git mv attic/docker/Dockerfile attic/docker/docker-compose.yml .
```

**Let op:** `README.md` en `docs/START_HERE.md` beschrijven Docker nog uitgebreid als het
deploypad — dat was al in tegenspraak met `CLAUDE.md` voordat deze bestanden verhuisden. Er
staat nu een verwijzing bovenaan die secties; de commando's zelf zijn niet herschreven.

## start-scripts/ — `start.sh`, `run_local.sh`, `start-dm-tools.bat`

Drie manieren om de server met de hand te starten. De echte boot gebruikt ze niet: de
PowerShell-launcher bouwt zijn eigen `nohup setsid uvicorn`-commando (nagekeken op 2026-08-14),
en niets in de repo roept deze scripts aan.

Ze zijn bewaard omdat je ze met de hand kúnt aanroepen. Twee dingen om te weten als je dat doet:
- `run_local.sh` en `start.sh` starten uvicorn **met `--reload`**, en zo draait de server hier
  niet. Zie de memory-note `dm_tools_backend_no_reload`: een backendwijziging vraagt daar een
  kill + relaunch.
- `run_local.sh` verwijst naar `thema_ads_optimized`, dat een **symlink** naar
  `~/projects/theme_ads/` is en in de root blijft staan.

Terugzetten: `git mv attic/start-scripts/<naam> .`

## pycache-root-owned/ — alleen als het lukt

Hier hoort `backend/__pycache__` te staan: 29 `.pyc`-bestanden uit februari–april 2026, eigenaar
**`root:root`**, terwijl alle `.py`-bronnen van `joepvanschagen` zijn. Er heeft dus ooit iets de
backend als root gedraaid.

Verplaatsen lukte niet, en de reden is leerzaam: het verplaatsen van een map werkt zijn eigen
`..`-verwijzing bij, dus de kernel eist schrijfrecht **op die map zelf** — en die is root-owned
met mode 755. Dit heeft jouw sudo nodig:

```bash
sudo mv backend/__pycache__ attic/pycache-root-owned      # of: sudo rm -rf backend/__pycache__
```

Waarom het de moeite is: Python kan die bestanden niet verversen, dus bij elke start wordt de
bytecode opnieuw in het geheugen gecompileerd in plaats van van schijf gelezen. Het is 960 KB en
volledig herbouwbaar — weggooien mag hier dus ook, dit is de enige map in `attic/` waarvoor dat
geldt.

## Wat NIET is opgeborgen, en waarom

| blijft in de root | reden |
|---|---|
| `themes.py` + `themes/` | **in gebruik.** `from themes import …` staat op tien plekken in `backend/thema_ads_service.py` en `thema_ads_router.py`. Een grep op de bestandsnáám vindt dat niet — Python importeert het als módule. |
| `thema_ads_optimized` | symlink naar `~/projects/theme_ads/thema_ads_optimized`, een ander project |
| `suggestions.txt`, `suggestions_new.txt` | Joeps werkvoorraad ("de lijst"), geen rommel |
| `keys.txt`, `.env` | secrets, nooit verplaatsen zonder opdracht |
| `backend/rurl_optimizer*/data/cache` (756 MB) | caches op hun eigen plek. Verplaatsen kost hetzelfde als weggooien (de volgende run haalt alles opnieuw uit Redshift) maar houdt de schijfruimte bezet — dus of laten staan, of bewust opruimen |
