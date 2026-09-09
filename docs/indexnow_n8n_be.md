# IndexNow n8n-flow: beslist.be erbij (09-09-2026)

De dagelijkse flow `indexnow_submitter` deed uitsluitend beslist.nl — de fetch-query
filterde op `dv.url like '%beslist.nl%'`. Sinds .be een eigen, gevalideerde IndexNow-key
heeft, bleef daarmee een volledig eigen quotum van 10.000 URL's per dag ongebruikt.

**Import:** `Downloads/claude/indexnow_submitter_IMPORT_2026-09-09_be.json` (22 nodes).
Basis is `indexnow_submitter_IMPORT_2026-09-08.json` (12 nodes).

## Vorm: een parallelle tak, geen gedeelde keten

De .be-keten is een kopie van de .nl-keten, met eigen nodes (`*_be`). Beide hangen aan
dezelfde `Schedule Trigger` → `get_dates`.

De aantrekkelijkere vorm — één keten met een item per domein — is bewust **niet** gekozen.
`build_tracking_insert1` koppelt de API-response aan de URL-lijst via
`$('has_urls?1').first().json.urls`. Met meerdere domeinen in één keten wordt dat een
koppeling op itemvolgorde, en juist die koppeling is waar deze flow al twee keer stil is
misgegaan (1 sep: fout gelogd als 200; 8 sep: geen spoor bij een storing). Een verkeerde
koppeling zou de URL's van het ene domein wegschrijven als ingezonden voor het andere —
en omdat de dedup alleen op `url` matcht, bereiken die URL's Bing dan nooit meer.

Prijs van deze keuze: negen gedupliceerde nodes, en een fix in de JS moet twee keer
worden toegepast. Bewust geaccepteerd.

## Wat er per node verschilt

| Node | Verschil met de .nl-versie |
|---|---|
| `fetch_urls_from_redshift_be` | `'%beslist.nl%'` → `'%beslist.be%'`. Verder letterlijk gelijk, inclusief de `/p/`-uitsluitlijst, zodat de twee query's diffbaar blijven. |
| `validate_suppliers_be` | `INDEX_PREFIX` → `product_search_v4_be-nl_`. Host-strip is een regex geworden. |
| `submit_to_indexnow_be` | Eigen host, key en keyLocation. |
| `build_summary_be` | Domeinnaam in de kop; `(<3 suppliers)` → `(<2 suppliers)`. |
| overige `*_be` | Alleen node-verwijzingen omgezet naar de .be-tegenhangers. |

De twaalf bestaande .nl-nodes zijn **byte-identiek** gebleven (geverifieerd).

## Wat vooraf is nagemeten

- **Voorraad**: 245.362 unieke .be-URL's met echte visits over 30 dagen (84.487 `/c/` +
  160.875 `/p/`), waarvan **alle** nog nooit ingezonden. Bij C-eerst is dat ~8 dagen aan
  `/c/` en daarna `/p/`.
- **ES-index bestaat**: `product_search_v4_be-nl_*`, met 32 aliassen zonder timestamp —
  evenveel als nl-nl. De node bevraagt `…_<maincat>` zonder wildcard, dus dit was een
  harde voorwaarde.
- **pimId-vorm is gelijk**: in de be-nl-index heet het veld nog steeds
  `nl-nl-gold-<ean>` (geverifieerd op `product_search_v4_be-nl_165`). De sleutelopbouw
  in `parseUrl` kon dus ongewijzigd blijven — dit was de grootste aanname en hij klopt.
- **Validatie werkt echt**: simulatie van `validate_suppliers_be` op 300 echte .be
  `/p/`-URL's → **243 gevalideerd (81%)**, 12 niet te parsen, 3 niet in ES. Zonder deze
  toets was "alle `/p/` stil afgekeurd" een reëel scenario geweest.

## Waarom de host-strip een regex werd

De .nl-node doet `rel.slice(22)` na een `startsWith('https://www.beslist.nl')`. Toevallig
is `https://www.beslist.be` óók 22 tekens, dus een letterlijke kopie zou hebben gewerkt —
maar bij een mismatch geeft `parseUrl` stil `null` terug en wordt **elke** `/p/`-URL
afgekeurd, zonder fout in de log. Vervangen door
`url.replace(/^https?:\/\/[^\/]+/, '')`. De .nl-node is bewust niet aangepast.

## Na de import controleren

1. **Twee Slack-berichten per dag** — bewust: een storing op .be mag het .nl-rapport niet
   onderdrukken. Elk bericht noemt zijn domein in de kop.
2. **Eerste run**: `pa.index_now_joep_runs` moet twee regels per dag krijgen. Controleer
   dat de .be-regel `response_code = 200` heeft, niet 202.
3. **Twee Redshift-query's van `limit 100000`** in één executie in plaats van één — de
   .be-query duurde bij het meten meer dan twee minuten.
4. De workflow staat op `active: false` in het exportbestand; activeren blijft handwerk.

## Wat dit NIET oplost

R-URL's blijven buiten IndexNow, op beide domeinen. Ze leveren 62% van de
Bing-organische entries en krijgen 0% van het quotum.
