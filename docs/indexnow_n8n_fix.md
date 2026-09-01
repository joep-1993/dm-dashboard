# IndexNow-flow: foutafhandeling + Slack-alarm

**Waarom.** `pa.index_now_joep` bevatte 159 dagen lang uitsluitend `response_code = 200`.
Dat bewees niets. De HTTP-node `submit_to_indexnow1` staat op `onError: continueRegularOutput`,
en de code-node erachter schreef `item.json.statusCode || 200`. Bij een mislukte POST ontbreekt
`statusCode`, dus die fallback logde de fout weg als succes — en de Slack-melding zei
"Submission Complete" met een :champagne:. Erger nog: `dedup_and_batch` dedupt op `url` alleen,
dus zo'n rij zou de betreffende URLs voorgoed overslaan; ze zouden Bing nooit meer bereiken.

**Bron van waarheid.** `docs/indexnow_n8n.json` staat in `.gitignore` (de export bevat de
IndexNow-key), dus dít bestand is het vastgelegde verslag. De lokale JSON is bijgewerkt naar de
*echte* live export (12 nodes, met `get_dates` en `validate_suppliers`) met de fix erin; wat er
eerst stond was een oude 11-node-export waarin `fetch_urls_from_redshift` nog een
placeholder-query had — dat is precies hoe deze verwarring ontstond. Importeren in n8n kan met
`Downloads/claude/indexnow_submitter_fixed.json`, of plak de twee blokken hieronder in de
live nodes.

## Wat er verandert

| Node | Verandering |
|---|---|
| `build_tracking_insert1` | Echte statuscode in plaats van `\|\| 200`. Bij niet-2xx wordt er **niets** gelogd (de query wordt `SELECT 1`) zodat de URLs de volgende run opnieuw meegaan, en de fout gaat als `ok:false` + `detail` naar de samenvatting. |
| `build_summary1` | Splitst gelogd van geweigerd, onderscheidt "geen antwoord" van een 4xx, en maakt er bij niet-2xx een alarm van met 🚨 en het aantal niet-gelogde URLs. Alle bestaande regels (facet-bypass, rejected, truncated) blijven staan. |
| `Slack` | Emoji volgt de uitkomst: `{{ $json.ok ? ':champagne:' : ':rotating_light:' }}`. Een mislukking hoort geen champagne te krijgen. |

Geen nieuwe nodes en geen nieuwe verbindingen — de bestaande Slack-node (DM naar j.schagen)
verstuurt het alarm. Wel een restrisico: valt `log_to_tracking_table` zelf om (Redshift weg),
dan stopt de keten vóór Slack en krijg je niets. Een aparte watchdog op "0 rijen vandaag in
`pa.index_now_joep`" dekt dat, maar dat zit niet in deze wijziging.

## Getest (droog, met gemockte n8n-context)

| Scenario | Resultaat |
|---|---|
| `statusCode 200` | INSERT met alle URLs, `ok=true`, gewone melding, champagne |
| `statusCode 403` | `SELECT 1`, 0 rijen, 🚨 "API gaf 403" + "3 URLs NIET gelogd" |
| `statusCode 429` | idem, met 429 |
| error-item zonder `statusCode` | `SELECT 1`, 0 rijen, 🚨 "API gaf geen antwoord" + de foutmelding |
| URL met een apostrof | correct ge-escaped naar `o''brien` |

## Live testen zonder op een echte storing te wachten

Zet in `submit_to_indexnow1` de URL tijdelijk op `https://api.indexnow.org/IndexNowXXX`
(geeft 404) en draai de flow handmatig. Verwacht: een 🚨-DM, `New URLs submitted: 0`, en
**geen** nieuwe rijen voor vandaag in `pa.index_now_joep`. Daarna de URL terugzetten.

## 1. Node `build_tracking_insert1` → JavaScript

```javascript
// Log alleen wat IndexNow echt heeft aangenomen.
//
// Was hier fout (1 sep 2026): `item.json.statusCode || 200`. submit_to_indexnow1 staat op
// onError: continueRegularOutput, dus bij een fout komt er een item ZONDER statusCode
// binnen — en dan schreef die fallback een 403/429/timeout weg als een keurige 200.
// Daardoor stond pa.index_now_joep 159 dagen lang vol 200-en die niets bewezen.
const urls = ($('has_urls?1').first().json.urls) || [];
const item = $input.first();

const code = Number(item.json.statusCode);
const hasCode = Number.isFinite(code);
const ok = hasCode && code >= 200 && code < 300;

const detail = item.json.error
  ? String(item.json.error.message || JSON.stringify(item.json.error)).slice(0, 300)
  : String(item.json.body ?? '').slice(0, 300);

if (!ok || urls.length === 0) {
  // Niet loggen. dedup_and_batch dedupt op url alleen, dus een rij voor een mislukte
  // batch zou die URLs voorgoed overslaan — ze zouden Bing nooit meer bereiken. Door
  // niets te schrijven pakt de run van morgen ze gewoon opnieuw op.
  // log_to_tracking_table voert {{ $json.query }} uit en heeft dus iets geldigs nodig.
  return [{ json: {
    query: 'SELECT 1',
    url_count: 0,
    attempted: urls.length,
    response_code: hasCode ? code : null,
    ok: false,
    detail: detail
  } }];
}

const today = new Date().toISOString().split('T')[0];
const values = urls
  .map(url => `('${url.replace(/'/g, "''")}', '${today}', ${code})`)
  .join(',\n');

return [{ json: {
  query: `INSERT INTO pa.index_now_joep (url, submitted_date, response_code) VALUES ${values}`,
  url_count: urls.length,
  attempted: urls.length,
  response_code: code,
  ok: true,
  detail: ''
} }];
```

## 2. Node `build_summary1` → JavaScript

```javascript
// Slack-samenvatting, met alarm bij alles wat geen 2xx is.
const trackingItems = $('build_tracking_insert1').all();

let totalLogged = 0, totalFailed = 0, responseCode = null, detail = '';
for (const item of trackingItems) {
  const j = item.json;
  if (j.ok) {
    totalLogged += j.url_count || 0;
  } else {
    totalFailed += j.attempted || 0;
    if (!detail) detail = j.detail || '';
  }
  if (j.response_code !== null && j.response_code !== undefined) responseCode = j.response_code;
}

const dedupItem = $('dedup_and_batch').first();
const totalNew = dedupItem.json.total_new || 0;
const totalSkipped = dedupItem.json.total_skipped || 0;

const validateItems = $('validate_suppliers').all();
let totalRejected = 0;
let totalTruncated = 0;
let totalFacetBypass = 0;
for (const item of validateItems) {
  totalRejected += item.json.urls_rejected || 0;
  totalTruncated += item.json.total_truncated || 0;
  totalFacetBypass += item.json.urls_facet_bypassed || 0;
}

// Geen antwoord van de API is een andere storing dan een 4xx — benoem het verschil.
const codeLabel = (responseCode === null || responseCode === undefined) ? 'geen antwoord' : responseCode;
const alarm = totalFailed > 0 || totalLogged === 0;

const lines = [
  alarm
    ? `:rotating_light: *IndexNow submit MISLUKT* — API gaf ${codeLabel}`
    : `*IndexNow Submission Complete*`,
  `• Fetched from Redshift: ${totalSkipped + totalNew}`,
  `• Already submitted (skipped): ${totalSkipped}`,
  `• Facet (/c/) URLs auto-passed: ${totalFacetBypass}`,
  `• /p/ rejected (<3 suppliers): ${totalRejected}`,
  `• Truncated (daily limit, post-validation): ${totalTruncated}`,
  `• New URLs submitted: ${totalLogged}`,
  `• API response: ${codeLabel}`
];
if (alarm) {
  lines.push(`• *${totalFailed} URLs NIET gelogd* — ze gaan de volgende run opnieuw mee`);
  if (detail) lines.push(`• Antwoord: ${detail}`);
}

return [{ json: { message: lines.join('\n'), ok: !alarm } }];
```
