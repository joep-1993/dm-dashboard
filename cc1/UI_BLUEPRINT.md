# dm-tools UI Blueprint

The single source of truth for how a new dm-tools page must look, so every tool
is visually identical. **Starting point: copy `frontend/_tool-template.html`** —
it already implements everything below. This doc is the *why* / the checklist.

Design tokens live in `frontend/css/style.css` `:root` — never hard-code these
hexes inline, use the token or the class that references it:

| Token | Value | Use |
|-------|-------|-----|
| `--color-navbar` | `#5e4a90` (purple) | top navbar only |
| `--color-section` | `#E8E9EB` (light grey) | card/section headers |
| `--color-button` | `#CC5500` (burnt orange) | orange buttons |
| `--color-button-hover` | `#E97451` (coral) | orange button hover |

## Page skeleton

- `<head>`: Bootstrap 5.3 CDN CSS **+** `/static/css/style.css` (in that order).
- **Shared navbar** (`navbar navbar-dark bg-primary sticky-top`) — identical on
  every page. Set `.navbar-brand` text; mark this tool's link
  `nav-dropdown-item active` and its group toggle `nav-dropdown-toggle nav-dropdown-active`.
  **Adding a tool means adding its link to the navbar of *every* page**, not just
  the new one.
- **Apps button** (far-right, `a.btn.btn-light.nav-dashboard-btn`, links to
  `dashboard.html`): icon-only, inline 9-square-grid SVG with `fill="currentColor"`,
  recoloured brand purple via `.nav-dashboard-btn svg { color:#5e4a90 }`. It and
  `.nav-dropdown-toggle` share an explicit `height:2.25rem` + `box-sizing:border-box`
  + centred flex content so text and icon buttons are pixel-identical in height —
  do **not** try to match heights via padding (Bootstrap `.btn` `line-height:1.5`
  vs the native toggles' `normal` makes that unreliable). The apps button is
  deliberately **excluded from the responsive `@media` padding/font rules** (fixed
  `2.75rem`-wide box at every breakpoint). Icon markup is hand-duplicated per page.
- **Fixed width wrapper — two sanctioned widths, decided 2026-07-30:**
  - **Default: `container mt-5 pb-5` › `row` › `col-md-10 mx-auto`** (~950px at a
    1200-1399px window, ~1074px at ≥1400px). Every tool uses this unless it is on
    the list below.
  - **Data-dense exception: `col-lg-11 mx-auto`** (~1045px / ~1184px) for the five
    pages whose content genuinely needs it: **SEO stats, Healthscore, SEO titles,
    DMA Exclusions, Bot Hits**. Bot Hits joined on 2026-08-12 (Joep: "gelijktrekken
    aan SEO stats") — it was the last page still on `container-fluid` with a hand-set
    `max-width: 1500px`, and six tiles on one row plus eight-column tables put it in
    the same bracket as SEO stats. Watch the trap that cost a round here: swapping
    `container-fluid` for a **bare** container centres the page but leaves it wider
    than every other tool, and it *looks* fixed. The width only matches once the
    `row` › `col-lg-11` wrapper is there too — verify by measuring the rendered
    edges at two window sizes, not by comparing the class names.
    SEO stats and Healthscore ran on a *bare* container (~1140px /
    ~1320px) until this date, which is why they read visibly wider than the rest of
    the app; `col-lg-11` brings them into a sanctioned width without squeezing
    them. At `col-md-10`, SEO stats' 8 summary tiles wrap 7+1 and its 10 metric
    pills spill onto two rows.
  - Never `container-fluid` or a bare container.
  - The px figures move with the viewport because `.container` itself does (1140px
    at 1200-1399, 1320px at ≥1400) — quote the class, not the pixels.
  - Measured, not estimated: at a 1500px window `col-md-10` renders 1074px of
    content and `col-lg-11` renders 1184px.
  - `dashboard.html` is deliberately excluded — it is the Apps launcher, a
    `col-lg-4 col-md-6` card grid in a bare container, not a tool page.
- Each logical block is a `card mb-4` with a `card-header` + `card-body`.

## Section / card headers

Use the plain shared header — **grey** `#E8E9EB` from style.css:

```html
<div class="card-header"><h5 class="mb-0">Title</h5></div>
```

Do **not** add an inline `background:#5e4a90` (purple) header. GSD Campaigns
currently does this and is the *only* tool that does — it's the outlier, every
other tool uses the grey default. New tools follow the grey default.

## Tables — match "Campaigns created" in GSD Campaigns

- Wrapper: `<div class="tool-table-wrap">` (1px `#eee` border, rounded, `overflow:auto`)
  so the table sits inside the card body, not edge-to-edge.
- `<table class="table table-sm table-hover tool-table">`, `<thead class="table-light">`.
- Header cells: **grey `#f8f9fa`**, sticky, `padding:6px 14px`, **font-size `1rem`**
  (headers are a touch larger than the `0.9rem` body).
- Body cells: `font-size:0.9rem`, `vertical-align:middle`.
- **Column widths — pick one strategy, and a value must never bleed into the next column:**
  - *Fixed widths (stable on sort):* `table-layout:fixed; width:100%` + an explicit
    width on every column (a `.col-*` class per `<th>`; `width:36px` for the checkbox,
    percentages for the rest). Keeps widths from shifting when you sort a different
    column. Contain values with either *wrap* (`white-space:normal; overflow-wrap:anywhere;
    word-break:break-word;` incl. nested `code`/`a`) or *single-line ellipsis*
    (`white-space:nowrap; overflow:hidden; text-overflow:ellipsis;`). Never `nowrap`
    **without** `overflow:hidden` here — that's the combination that bleeds.
  - *Content widths + horizontal scroll (full values always visible):* `table-layout:auto`
    (`width:auto; min-width:100%`) + `white-space:nowrap` (no truncation), inside a
    horizontally-scrollable wrapper (`.table-responsive` / `overflow-x:auto`). Each column
    sizes to its widest value in the current view and the table scrolls sideways when it
    exceeds the card. Widths may shift between views — that's the trade for showing every
    value in full. If a hover control (e.g. an edit pencil) must stay reachable while
    scrolled, put it in a **right-pinned sticky column** (`position:sticky; right:0` + a
    solid background).
- **Filter box + action buttons go in a `.filter-row` above the table**, not in the
  card header — one `d-flex gap-2 align-items-center mb-3` row with the filter input
  on the left and the action buttons pushed right (`ms-auto` on the first right item),
  so filter and buttons share a single baseline height. Sort arrows use the
  `th.sortable::after` `⇅`/`▲`/`▼` glyph pattern (see "Campaigns created" in GSD Campaigns).
  - **Exception — a table with NO filter puts its export actions in the card header,**
    far right, next to the title (`card-header d-flex justify-content-between
    align-items-center`). Joep, 2026-08-04, on GSD Check and MC ID Finder: a
    `.filter-row` holding nothing but two right-aligned buttons is a bar that exists
    for no reason, and the card title row is already the right home for
    Copy/Export. The `.filter-row` rule stands wherever there IS a filter to pair
    the buttons with — that is what the shared baseline is for.
- **Sortable headers**: add `class="sortable" data-sort="<key>" onclick="sortBy('<key>')"`.
  The `.sortable` CSS shows a `⇅` idle glyph and `▲`/`▼` for the active sort
  direction (toggled by adding `sort-asc` / `sort-desc` to the active `<th>`).
  **Never put `position: relative` on `th.sortable`.** The base `.tool-table th`
  rule sets `position: sticky; top: 0`, and a more specific `position` silently
  beats it — so the sortable columns stop sticking while the plain ones keep
  sticking, which looks like "only Aandeel, In pa.urls and Cache-hit are fixed"
  (Joep, 2026-08-11). The `::after` glyph needs a positioned ancestor and `sticky`
  already is one, so dropping `relative` costs nothing. `seo-stats.html` had it
  right; `_tool-template.html` did not, which is how it spread — still present in
  gsd-campaigns, gsd-tag-toppers, gsd-check, shop-campaigns, mc-id-finder and
  seo-titles.
- **A row that expands: one open at a time.** For a per-row breakdown (Bot Hits'
  bot-family table), make the `<tr>` clickable, add a `▸` caret that rotates on
  `.is-open`, and insert a sibling `<tr class="…-detail"><td colspan="N">` with the
  charts. Close whatever was open first **and destroy its Chart.js instances** —
  three charts per row times 25 rows is 75 live charts that all redraw on a resize.
  Fetch the panel's data from the existing list endpoint with the row's own filter
  value added (`/summary?bot_family=X`), passing the page's current filters along so
  the panel and the page agree about the period. After the await, check the canvas
  still exists: the row may have been closed or re-rendered while the request was in
  flight.
- **A category column of labels is centred** (`text-align: center`) — an outlined
  `.lbl` is a block with its own edges, so left-aligning it against a numeric
  column's right-aligned digits leaves a ragged gutter between them.
- **De basis-CSS staat sinds 2026-08-14 in `css/style.css`**: `.tool-table-wrap`,
  `.tool-table`, de sticky grijze kop (`padding: 6px 14px`, `font-size: 1rem`) en de
  `th.sortable`-glyphs. Een nieuwe tabel heeft de opmaak dus gratis — geen blok kopiëren.
  De elf pagina's die hun eigen `.tool-table`-blok hebben (in acht licht afwijkende
  varianten) houden dat voorlopig: een page-`<style>` laadt later en wint, dus de gedeelde
  basis is puur additief en verandert geen bestaande pagina. Die varianten samenvoegen staat
  als open taak in `cc1/TASKS.md`. Kolombreedtes en pagina-eigen kolomregels blijven per
  pagina — die horen bij de data.
  De **kopkleur** komt niet uit `.tool-table th` maar uit `.table thead th` (het vlakke
  paneel): die is specifieker (0,1,2 tegen 0,1,1) en wint ook vanuit een page-`<style>`. Zet
  er dus geen `background` op — dat is een regel die nooit iets doet.
- **Loading state = skeleton rows, not a spinner.** While a table fetches, draw
  shimmering placeholder rows so it reads as "the table is being drawn". The point
  is **layout stability**: a skeleton row is the same height as a loaded row, so
  the table neither collapses to nothing nor grows when data lands, and on a
  *reload* it holds the current height instead of jumping.

  **The CSS is shared in `css/style.css`** (`.skel-row` / `.skel-bar` /
  `@keyframes skelShimmer`) — do **not** re-declare it per page; it was duplicated
  across seven pages and got consolidated. Only the JS helper is per-page, because
  the column count differs and there is no shared JS bundle:
  ```js
  function skeletonRows(cols, n = 10) {
      const cell = '<td><span class="skel-bar"></span></td>';
      return ('<tr class="skel-row">' + cell.repeat(Math.max(cols, 1)) + '</tr>').repeat(n);
  }
  ```
  `cols` must be the table's real column count (checkbox and action columns
  included) or the shimmer won't line up with the header. Where the header row is
  built at runtime, read it back — `head.querySelectorAll('th').length` — with the
  column-set length as the first-load fallback (see `seo-stats.html`,
  `shop-campaigns.html`). Cap the row count at the page size, and at ~10 when
  "Show all" is selected — don't draw 5,000 skeletons.

  Three rules the sweep in 2026-07-28 turned up the hard way:
  - **Every failure path must replace the skeleton.** A `catch` that only logs, or
    only writes to a summary line elsewhere, leaves the table shimmering forever —
    which reads as "still loading" for a request that already died.
  - **Skeleton where the fetch starts, not where the render happens.** If table B
    is filled by a function that only runs *after* table A's fetch resolves (SEO
    stats: `loadDeltas()` runs at the end of `load()`), B sits blank for that whole
    first request unless you also draw its skeleton up front.
  - **A hidden table + spinner becomes a visible table + skeleton.** GSD Check and
    MC ID Finder used to hide the results card and show a `#loadingArea` spinner;
    they now build the header before the fetch and show the card with skeleton
    rows. That requires any state the header depends on to be assigned *before* the
    fetch (MC ID Finder's `lastMode` had to move up).
  - **A grey overlay + spinner is not a substitute — it hides the drawing effect.**
    SEO stats' Performance-standup card kept a page-local `.loading-overlay`
    (removed 2026-07-30) on the argument that it also greyed the stat tiles, which
    skeleton rows can't. That argument is wrong: **tiles take a skeleton too** —
    keep the label (it is known before the fetch) and shimmer only the value, e.g.
    `<h3><span class="skel-bar" style="width:60%;margin:0 auto;"></span></h3>`.
    Match the skeleton row count to what actually renders (both standup lists
    `slice(0, 3)`, so 3 rows), or the height still jumps on load.

  **When a progress bar is right instead.** Skeletons say "data is arriving now",
  so they only fit a single fetch that returns promptly. Long multi-item runs that
  already report real progress keep their bar — URL Checker, URL Validator, Index
  Checker, Redirect Checker, Redirect Generator, Canonicals, Thema Ads, and Keyword
  Planner's two Google-Ads tables. Those tables are also hidden until the run ends,
  so there is nothing on screen to shimmer.
- **Timestamp columns: convert to Europe/Amsterdam — the DB values are UTC.** The shared
  Postgres runs `TimeZone=Etc/UTC` and our `created_at`/`applied_at` columns are
  `TIMESTAMP` (no tz), so `now()` stores UTC and the backend's `.isoformat()` emits it
  **with no offset**. Slicing that string (`replace("T"," ").slice(0,16)`) therefore shows
  UTC as if it were local — 2h early in summer. This shipped in DMA Exclusions and was
  fixed in `ef5c53e`; copy `fmtTs()` from `dma-exclusions.html` rather than re-deriving it.
  The one thing you must not skip: append `"Z"` **before** `new Date()`, because JS parses
  an offset-less date-time as LOCAL — so the obvious `new Date(s)` is a silent no-op in
  CEST. Format with `toLocaleString("sv-SE", { timeZone: "Europe/Amsterdam", … })` to get
  `YYYY-MM-DD HH:MM` with DST handled. Keep the raw value on a `title` tooltip, name the
  timezone in the `<th>` title, and **sort the raw ISO field, not the formatted string**.

## Pagination — orange arrows, like "Enabled / Paused history"

Put a `.pagination-controls` bar under every paginated table: a "Per page"
select (10 / 25 / 50 / 100 / Show all), prev/next chevron buttons, and an
`X-Y of Z` page-info span. The chevron buttons are
`class="btn btn-outline-secondary btn-page"` — `btn-outline-secondary` maps to
`--color-button` in style.css, which is why the arrows render **orange**. Use the
chevron SVGs from the template (not `<` / `>` text).

## Charts — the SEO stats look is the reference

`seo-stats.html` and `shop-campaigns.html` now render the same chart. Copy that
chrome rather than re-deriving it (Chart.js, `.chart-wrap { position: relative;
height: 420px }`):

- **Filled areas, thin lines.** `fill: true`, `tension: 0.35`, `borderWidth: 1.75`,
  round caps/joins. The fill alpha steps DOWN as series are added — `'2b'` at ≤2,
  `'22'` at ≤4, `'14'` above — because a stack of translucent washes turns to mud.
  The line colour never changes; only the wash under it gets quieter.
- **No permanent points.** `pointRadius: 0`, `pointHoverRadius: 4`,
  `pointHitRadius: 12`, white hover ring. A dot per day over 90 days is what makes
  a chart look busy; index-mode interaction keeps the line hoverable anyway.
- **Recessive grid, no furniture.** Grid `#eef0f2` on both axes, `border: display
  false`, `drawTicks: false`, ticks `#9a9aa6` at 11px with `padding: 8`,
  `maxTicksLimit: 6` on the value axes. `layout.padding` top/right 12.
- **The unit rides on the tick, not on an axis caption** — `€1,2 mln`, `6,7%`,
  `Jul 29`. Turn the axis title off. **Exception**: when one side carries TWO axes
  (Shop-campaigns can put Count + Impressions left, and €, % and € CPC right) the
  tick alone no longer says which gutter is which, so the caption comes back for
  that side only.
- **Chart.js' own legend is OFF.** The summary tiles are the legend — see below.
- **Tooltip is ours, dark**: `#242628` panel, `.ct-dot` with a
  `0 0 0 1px rgba(255,255,255,0.45)` ring so a dark series colour stays visible,
  `.ct-label` nowrap, date heading with the weekday — `2026-07-24 (vrijdag)`.

**A swatch legend goes ABOVE the plot, centred, and is clickable** (Bot Hits' Hits
per dag, 2026-08-11) — `display:flex; flex-wrap:wrap; justify-content:center` in a
`.daily-legend`, one `.leg-item` per series. Click toggles that band via
`chart.setDatasetVisibility(i, …)` + `chart.update()`, never a refetch. The off state
dims the item, strikes the label through, and makes the swatch **hollow**
(`box-shadow: inset 0 0 0 2px currentColor`) — the colour has to stay legible or you
cannot tell which band you are switching back on. Keep the hidden set OUTSIDE the
draw function and keyed per split dimension, so a Refresh or a different split does
not silently restore what the user switched off, and re-apply it as `hidden:` on the
dataset when redrawing.

**Stacked bands are SOLID, and the alpha ladder does not apply to them.** The ladder
above exists for *overlapping* line-areas, where the wash sits under its own line.
In a stacked chart the fill IS the series: at five series the ladder's `'14'` (7,8%
opacity) turned Bot Hits' 96%-of-volume band into a pale field that read as grey
(Joep, 2026-08-11). Stacked → full colour, separated by a **2px white border**, which
is also the dataviz rule for adjacent fills. Consequence for the tooltip: its dot
must read `dataset.backgroundColor`, not `borderColor` — the border is the white
separator there.

**Summary tiles double as the legend and the toggles.** One `selected` Set behind
the tiles and the chart. A tile carries the series colour (a sparkline in SEO
stats; a 7px `.tile-dot` next to the label where tiles are too narrow for one),
gets `.metric-tile` + `.metric-on`/`.metric-off`, and its border and dot say
whether the series is drawn — the value and label are NEVER dimmed, the number is
the tile's whole point. Do not also ship a row of `.metric-toggle` pills for the
same metrics; keep pills only for series that have no tile (SEO stats' two
aggregates). In SEO stats the sparkline area is deliberately not clickable.

**Loading state = a skeleton in the shape of a chart, not a spinner** — same
reasoning as the skeleton table rows. Absolutely positioned inside `.chart-wrap`,
**opaque** (`background: #fff`), a row of bottom-aligned shimmer columns using the
shared `skelShimmer` gradient, with the left/bottom padding left empty where the
axis gutters will be. Opaque matters: Chart.js keeps the PREVIOUS range drawn
until new data lands, and a stale chart behind a translucent veil reads as the
current one. Heights are a FIXED silhouette in the upper half of the range — a
shape that changes per load reads as data, and low values leave the top of the
card empty. Remove it on success AND on error, but **not** when a stale-load guard
bails out: a newer load owns it then. The tiles shimmer on the same fetch (label
and on/off border stay, only the value goes to a `.skel-bar`), because the tiles
are the range's totals and stale numbers above a loading chart look current.

**The first three colours of any table or chart are lichtblauw, roze, lichtgroen**
(Joep, 2026-08-11) — the brand base hues, i.e. `primary-500`, `secondary-500`,
`accent-500` of the Kleursysteem base row:

| # | naam | hex | waar het al zo is |
|---|---|---|---|
| 1 | lichtblauw | `#1f99c4` | `METRICS.seo_visits`, `URLTYPE_STYLE['R-url']`, Bot Hits "in pa.urls" |
| 2 | roze | `#be4693` | `METRICS.seo_omzet`, Bot Hits "niet in pa.urls" |
| 3 | lichtgroen | `#91c34e` | `METRICS.gsaas_visits` |

Measured as a trio (dataviz validator, light, surface `#fff`): CVD-min **7,7 ΔE**
(deutan, lichtblauw↔roze), normal-min **24,1**. The 7,7 sits in the 6–8 band that
is legal ONLY with secondary encoding, so a chart using these **must** carry a
legend or a table beside it — which every chart here does anyway.

Two caveats that are not optional:

* **Three is where this rule stops.** Beyond three series, go back to the search
  described below — `accent-500` (lichtgroen) is exactly the hue that creates this
  palette's worst pairs in a larger set, so "just keep going down the base row" is
  what the 2026-07-29 measurement already disproved (10 of 45 pairs failed).
* **A two-series chart takes the first two, not one plus grey.** Grey is reserved
  for "everything else"/unlisted, so using it as one half of a binary split reads
  as if that half were a residual.
* **The first colour belongs on the biggest series, not on slot 1 of a shared
  map.** Bot Hits inherited its URL-type hues 1-on-1 from `dashDonutUrlType`, which
  put lichtblauw on R-url — 2 hits on a day with 2,1M PLP hits, so the chart's
  first-choice colour never appeared on screen (Joep, 2026-08-11). PLP and R-url
  were swapped there. Consequence to keep in mind: a hue that means one thing in
  tool A now means another in tool B, so if you swap, either swap in both or record
  the divergence where both maps live.
* **"First three" means first three IN THE LEGEND**, which here is the order the
  query layer returns — i.e. by volume. Bot Hits' four dimensions resolved to
  Googlebot/Apple/GoogleOther, search/ai/other, beslist.nl/shop.beslist.nl and
  PLP/C-url/Cat-url (2026-08-12). Re-derive it from the data, don't guess from the
  map's declaration order.

**What the rule costs, measured (2026-08-12).** The trio's own floor is CVD **7,7**
(lichtblauw↔roze, deutan), so *no* set containing all three can score higher — 7,7 is
the ceiling, not a defect to fix. What varies is whether the remaining slots hold it:

| series | best achievable with the trio fixed |
|---|---|
| 4–7 | CVD 7,7 — the trio's own floor, nothing lost |
| 8 | CVD **4,9** — a real FAIL (cta-orange ↔ lichtgroen) |

At eight series the rule and the floor genuinely conflict, and the third brand hue is
what does it (with only lichtblauw+roze fixed, eight still makes 7,7). Two ways out,
both legitimate: fold the smallest series into "Overig" so the set is seven, or add one
new validated hue. Bot Hits' `bot_family` took the second (Joep's call) — see
`PAL.terracotta` in `bothits.html`, the only hue in this codebase that exists in one
tool and not in SEO Stats. **Nine series is not a question to ask**: fold instead.

**Re-measured 2026-08-13, with the Kleursysteem "Base" column in the pool.** Joep asked
for a hue per bot-family (twelve of them) and pointed at the design-system document, so
the whole search was redone over Base (the 500-stops: red `#ef4444`, orange `#f97316`,
yellow `#fdb62b`, green `#22c55e`, teal `#14b8a6`, cyan `#06b6d4`, blue `#3b82f6`,
violet `#a855f7`, pink `#ec4899`, primary `#1f99c4`, secondary `#be4693`, accent
`#84cc16`, gray `#6b7280`) plus the darker stops this codebase already uses. With the
trio fixed:

| named series | best CVD | best normal | verdict |
|---|---|---|---|
| 6 | 7,7 | 17,2 | OK |
| 7 | 7,7 | 16,4 | OK |
| **8** | **7,7** | **15,8** | **OK — the ceiling** |
| 9 | 7,7 | 11,3 | FAIL (normal < 15) |
| 12 | 5,5 | 7,1 | FAIL |

Two things worth keeping:

* **Eight named series is the ceiling, and it is the NORMAL-vision floor that stops
  you, not CVD.** CVD stays at the trio's 7,7 all the way up; normal-vision separation
  is what collapses. Secondary encoding does not excuse that one.
* **A design system's base row is the WRONG place to look for the 7th and 8th hue.**
  Base-only fails even at six (7,7 / 11,3), because every entry sits at stop 500 and
  therefore at near-identical lightness — and lightness is what keeps hues apart for
  full-colour vision. What passes is Base *plus the darker stops of the same families*:
  turquoise `#107063` ≈ teal-800, bordeaux `#722F37` ≈ red-900, navy `#001F3F` ≈
  blue-950, yellow `#936305` ≈ yellow-800. Reach for a darker step of a hue you already
  have before reaching for a new hue.

One value did move to the document's: `PAL.violet` is now `#a855f7` (violet-500) instead
of seo-stats' `#8459cf`. With the old value the weakest pair of the eight sits at 14,1
normal; with the document's it makes 15,8. Recorded divergence, no meaning conflict —
violet carries DuckAssist and facet-depth 4 here, DMA traffic there.

**Count the bands the chart DRAWS, not the entries in the map.** A catch-all "Overig"
band is a series the moment it renders, and `drawDaily()` always draws one. Validating
the eight named families while the chart showed nine bands is how the first candidate
hue got picked — it sat 9,3 ΔE from the grey and became the weakest pair itself. And
measure the old set too: Bot Hits' url_type turned out to *improve* (CVD 2,7 → 5,9),
while the grey ↔ lichtblauw pair at 12,0 normal was pre-existing and is unfixable by
any choice of series hue — only reworking the Overig band touches it.

**Colours: run the validator, never eyeball ΔE.** The palette is SEO stats'
(`seo-stats.html` — the Kleursysteem base row with the bordeaux/navy departures).
A page that needs fewer than ten series picks a SUBSET, and *which* one it drops is
a real decision: dropping accent-500 removes both of that palette's worst pairs.
Which colour lands on which metric is a search, not a preference — the assignment
determines which pairs end up adjacent, and a "logical" mapping can fail a floor
that a computed one clears by 10 ΔE. Use the `dataviz` skill's
`scripts/validate_palette.js`, light mode, surface `#fff`, and check **both**
`--pairs all` (every line is on screen at once) and the default adjacent run.
Record the measured numbers next to `METRICS` so the next change has a baseline.

### Stat tiles — the GSD Budgets card

The canonical KPI tile is `.stat-card` as defined in `gsd-budgets.html`, and it is
**value-first**: the number on top in brand purple, the label under it, an optional
`.detail` line under that. No border — a 12px radius plus a soft purple shadow
(`0 4px 16px rgba(94,74,144,0.14)`) is what separates it from the page.

```css
.stat-card { border-radius: 12px; background: #fff; color: #2d3436; padding: 1.25rem;
             text-align: center; min-width: 140px;
             box-shadow: 0 4px 16px rgba(94,74,144,0.14); }
.stat-card h3 { font-size: 2rem; font-weight: 700; margin: 0; color: #5e4a90; }
.stat-card .label  { font-size: 0.85rem; color: #636e72; }
.stat-card .detail { font-size: 0.75rem; color: #999; margin-top: 0.25rem; }
```

```html
<div class="stat-card"><h3>92.897.176</h3><div class="label">Bot-hits totaal</div>
  <div class="detail">2026-02-14 t/m 2026-03-16</div></div>
```

Three tiles sit in `d-flex flex-wrap gap-3 justify-content-center`; from about five
onwards use a grid (`repeat(auto-fit, minmax(170px, 1fr))`) so the last row is as
wide as the others instead of centred under them. Do **not** put the label above the
value in a small grey uppercase line — that was Bot Hits' own `.tile` variant and it
buried the number, which is the tile's whole point.

Note the collision: **SEO stats also has a `.stat-card`, but that is the chart's
legend-and-toggle tile** (sparkline, `.metric-on`/`.metric-off` border) — same class
name, different job. A page that needs both must rename one.

### Donut / part-to-whole — the SEO stats ring

`dashDonutVisits` / `dashDonutRevenue` / `dashDonutUrlType` in `seo-stats.html` are
the reference. The anatomy:

- **`cutout: '68%'`** with the totals in the hole via an absolutely positioned
  `.donut-center` (`.dc-total` big + `.dc-cap` small uppercase caption).
- **Segments get air**: `borderColor:'#fff'`, `borderWidth:2`, `spacing:2`,
  `borderRadius:6`, `hoverOffset:10`.
- **Chart.js' legend is OFF, no legend row, and no direct labels either** — the ring
  is identified by hover alone (Bot Hits' URL-type ring, 2026-08-11). That went
  through two rejected states, so don't re-derive it: a legend row beside/under the
  ring can *name* a 0,1% share but never *point at* it, and direct labels with leader
  lines (which do both) turned out to be too much chrome around a small ring — six
  labels plus leaders read busier than the data they annotate.
- **What makes hover-only workable is a minimum arc.** A 0,2% slice is a fraction of
  a pixel and `spacing` swallows it entirely, so it is neither visible nor hoverable.
  A small `afterUpdate` plugin lifts every non-empty segment to ~1,8° and takes the
  shortfall proportionally from the segments that have room (Bot Hits'
  `donutMinAngle`: R-url at 0,0003% becomes a 3,8px sliver, the ring still sums to
  exactly 360°). This **deliberately distorts the proportion** — the ring becomes a
  "which types exist and which dominates" picture, not a measuring stick — and that
  trade is only acceptable because the tooltip names the true count and the true
  share. Set `spacing: 0` and let a 1px white border do the separation; 2px of
  spacing on both sides eats a 1,8° arc whole.
- Never print `0%` for a segment that has hits — render `<0,1%`, because `0%` reads
  as empty.
- **Its own DOM tooltip (`.donut-tip`), not the canvas one.** Chart.js paints its
  tooltip INSIDE the canvas while `.donut-center` is an absolute layer on top, so the
  canvas tooltip renders *under* the total. Hence an external node in
  `.donut-wrap` with a higher z-index; flip it below the caret when above would clip.
  It is `nowrap` and centred on the caret, so it **also needs a horizontal clamp** —
  without one a slice near the edge pushes the panel outside the card. Centre it when
  the panel is wider than the wrapper.
- **The hover carries the WoW delta per slice** (2026-08-12): `Mobiel · 44.903 · 71,9%
  · −5,9% vs. 7d`. It is the percentage change of that slice's **value**, not of its
  share — the same operation as the stat tiles above, so one number means one thing on
  the whole screen. Say this out loud in review, because the two readings diverge: when
  every slice moves by the same factor, every delta is identical while the ring itself
  has not changed. The delta describes the slice's volume, the arc describes the mix.
  A slice absent on d-7 renders `n/a`, never `+100%`.
- **The delta cannot come out of `callbacks.label`.** Everything routed through
  `tooltip.body` is escaped into one flat string, so a colour-coded element has to be
  built in the external tooltip from data parked on the chart instance
  (`donuts[which].$dist = dist`, then `dataPoints[0].dataIndex`). `destroy()` takes it
  with the chart, so a stale delta cannot outlive the ring it belonged to.

Reach for the ring only for a real part-to-whole with few, well-separated slices.
For a distribution where two of the parts differ by ~10x, the dataviz skill sends you
to a 100% stacked bar instead — Dagoverzicht's device split is the exception, a donut
on Joep's explicit request (the stacked bars it replaced are in git history).

### Hover block — the dark panel is `.chart-tooltip`

Every chart's own tooltip is off (`tooltip: { enabled: false, external: … }`) and
replaced by the dark panel from `seo-stats.html`: `#242628`, 10px radius,
`.ct-title` (date **with weekday**, `2026-03-10 (dinsdag)`), an optional `.ct-sub`
caption, then a `.ct-row` per series with `.ct-dot` / `.ct-label` / `.ct-val`. The
dot carries a `0 0 0 1px rgba(255,255,255,0.45)` ring so a dark series colour stays
visible on the dark panel.

On a **stacked** chart, add a total row (top border, `rgba(255,255,255,0.14)`) —
when you hover a stack the sum is the thing you are asking for, and Chart.js cannot
produce it. Sort the rows by value descending and drop the zero series; the legend
order is for identity, the tooltip order is for reading.

**Each row carries its share too**, in muted ink after the value (`1.191.326 ·
58%`). Two rules: the denominator is the **hovered point's** total, not the
period's — you are pointing at one day — and a series that the user switched off is
out of that denominator as well, or the percentages add up to something not on
screen. Never render `0%` for a non-zero value; use `<0,1%`.

**A delta inside the dark panel does NOT reuse `.delta-badge`.** The tile pills are
tuned for the white card and `#00854c` / `#c0392b` sit at roughly 2:1 against
`#242628`. Use the `.dt-delta` pair from `seo-stats.html` instead — `#5fce8f` up,
`#ff8b7d` down, `#b9b9c4` for flat and `n/a` — which clears 6,9:1 on that ground. The
judgement is unchanged: green is the good direction, red the bad one, and for a metric
where up is bad you swap the class, never the sign (a rising bounce still prints `+5%`,
only in red).

### Two charts in one row

- **Equal plot heights, or the shorter card ends in dead space.** Bootstrap's `h-100`
  makes both cards as tall as the taller one, so a 300px donut beside a 260px bar
  chart leaves 40px of white under the bar chart (Joep, 2026-08-11). Give both plot
  boxes the same height and keep them in sync when one changes.
- **Fold an invisible tail instead of plotting it.** Bot Hits' facet-depth chart had
  five trailing categories worth 967 hits out of 51M — they render as nothing but
  still claim axis width, which squeezes the columns that do carry data. Fold the
  trailing run into one `N+` bucket, **data-driven** (walk back while a category is
  under ~0,05% of the total, fold when it is more than one) rather than on a
  hard-coded index, and keep the total intact so nothing disappears. Fewer categories
  then also earns wider bars (`categoryPercentage` ~0,82, `barPercentage` ~0,98).

## Buttons

Canonical classes are defined in `style.css` (additive/opt-in). Use them; never
inline the hexes.

| Purpose | Class | Look | Placement |
|---------|-------|------|-----------|
| Run / execute (primary CTA) | `btn btn-run` | **full orange** `#CC5500`, hover coral | **far right** of the section (`d-flex justify-content-end`) |
| Orange non-run action (Export, **"+ Add rule"**) | `btn btn-outline-orange` | orange outline, fills orange on hover | bij de rij die hij aanvult |
| Any other action, **Refresh**, **Preview** | `btn btn-outline-purple` | **purple outline** `#5e4a90`, fills purple with white text on hover | usually right (`ms-auto`) |
| Refresh specifiek | `btn btn-outline-purple` + `↻` glyph | idem, mét het pijltje ervoor | card-header of filterrij, rechts |
| Destructive (Stop / Remove / Cancel) | `btn btn-outline-red` | **red outline**, fills red on hover — *only while available* | — |
| Geen eigen betekenis | `btn btn-outline-secondary` / `-primary` / `-info` / `-success` / `-warning`, `btn-secondary`, `btn-preset` | **neutraal grijze outline**, grijze hover | — |
| Not clickable / unavailable | add `disabled` | **vlak grijs**: paneelvulling `#f4f5f9`, rand `#d6d8d7`, tekst `#9aa0a6` — altijd, ook voor rode knoppen | — |

**De Bootstrap-kleurnamen zijn geen keuze.** `btn-outline-primary` / `-info` / `-success` /
`-warning` zeggen niets over wat de knop doet, dus ze rénderen ook niets: neutraal grijs.
Wil je betekenis, pak dan `btn-run` (uitvoeren), `btn-outline-purple` (secundair) of
`btn-outline-orange` (toevoegen/exporteren). Dit is de reden dat DMA Exclusions' Preview van
`btn-outline-primary` naar `btn-outline-purple` ging (2026-08-14).

**Verzin geen nieuwe knopklasse.** De aliassen die pagina's zelf hebben bedacht
(`.btn-purple-outline`, `.btn-purple-action`, `.btn-hs-outline`, `.btn-tool` → paars;
`.btn-orange-action` → oranje; `.btn-preset`, `.btn-bulk-*` → grijs) zijn in `style.css` aan
de drie groepen gekoppeld zodat ze niet uit de toon vallen, maar dat is opruimwerk en geen
uitbreiding. Ook niet doen: een knop met inline hexes plus `onmouseover`-JS — dat stond op
GSD Campaigns (vijf stuks) en Canonicals (de Delete-knop) en is weg.

**In a filter card the buttons go under the filters, bottom-right** — not in a
column beside them (Bot Hits, 2026-08-11). A filter row is a set of equal columns;
hanging Reset/Toepassen in the last one makes that column mean something different
from its five neighbours and ties the button position to whatever field happens to
sit above it. `<div class="d-flex justify-content-end gap-2 mt-3">` after the
`.row`, secondary left of primary.

**Refresh in a card header needs NO override — an inline `background:#5e4a90` on a
`.card-header` is dead CSS (2026-07-31).** `style.css` has (waarden bijgewerkt na de merge
van 2026-08-14; de kleur is niet langer `--color-section` maar het vlakke paneel):

```css
.card-header,
.card-header.bg-info, .card-header.bg-success, .card-header.bg-primary,
.card-header.bg-secondary, .card-header.bg-warning, .card-header.bg-dark {
    background-color: var(--flat-panel) !important;   /* #f4f5f9 */
    color: var(--flat-text) !important;
    border-bottom: 1px solid var(--flat-border);
}
```

and a stylesheet `!important` **beats an inline declaration**. So every `card-header`
renders in the standard light panel colour no matter what `style="background:#5e4a90;
color:#fff"` says next to it — including SEO stats', which looked purple in the markup for
weeks and grey on screen the whole time. Hetzelfde geldt voor een klasse die het probeert:
Healthscore's `.card-header.hs-head { background: #5e4a90 }` verliest van het `!important`
hierboven. Consequences:

* Leave the Refresh button exactly canonical: `btn btn-sm btn-outline-purple` + `↻`,
  transparent background, purple outline and label, fills purple on hover. Any white base
  or white outline "so it shows on the purple header" is compensating for a colour that
  never paints, and reads as a different button from every other Refresh (Joep, 2026-07-31:
  *"the Refresh button in Performance per day should be transparent (is now white)"*).
* Before styling anything **against** a header colour, check the rendered colour in the
  browser (or a headless screenshot), not the inline style in the HTML.
* **Een knopvariant "voor op donker" is per definitie verdacht.** URL Validator had
  `.btn-outline-purple-on-dark` / `-orange-on-dark`: wit label, transparante vulling, voor
  een donkerpaarse kop. Die kop bestaat niet — de knoppen stonden met wit label op `#f4f5f9`
  (gemeten 2026-08-14). Ze zijn vervangen door de canonieke paarse en oranje outline. Als je
  zo'n variant nodig denkt te hebben: meet eerst de gerénderde achtergrond.
* Corollary for any override you do add: `.card-header .btn-outline-purple` (two classes)
  has the **same specificity** as `.btn-outline-purple:hover`, and a page's `<style>` block
  loads after `style.css`, so a bare background override silently wins on hover too and
  can leave white text on a white fill. Re-assert the hover in the same breath.

**A button whose mode changes must say so in its own label** (added 2026-08-13, SEO
Priority's "Apply to Taxonomy"). Where the same button either previews or writes to a
live external API depending on a nearby switch, the switch alone is not enough signal —
someone flips it, scrolls, and clicks the same green button believing it pushed. Rewrite
the label from the mode: `Apply 12 to Taxonomy` (green, `btn-success`) versus
`Preview 12 (dry run)` (purple outline). Same rule the GSD Tag Toppers run-history
learned the expensive way: a dry run that looks like a real run gets remembered as a real
run. Put the selection count in the label too, and confirm the live path with the actual
numbers (`12 facets → ON, 3 → OFF`), not a generic "are you sure".

**Bulk-select bar goes above the table, not in the card header.** Same reasoning as the
filter row: `.apply-bar` (`d-flex flex-wrap align-items-center gap-2 px-3 py-2`,
`border-bottom:1px solid #eee`, `background:#fbfbfc`) directly under `.card-body.p-0`,
holding the selection count on the left and the mode switch + action button pushed right
(`ms-auto`). A 36px `th.col-check`/`td.col-check` first column carries the checkboxes,
selected rows get a tinted `tr.row-selected`. **Disable a checkbox rather than hiding
it**, and put the reason in its `title` ("facet is not linked to the category", "already
applied") — a row you cannot act on still tells you something, an absent checkbox is just
a hole. Keep the selection keyed on the row's own identity (the same key the backend
looks the row up by), not on its page index, or it scrambles the moment someone sorts.

**Unavailable always wins over colour.** A `disabled` button rendert **vlak grijs**
(paneelvulling `#f4f5f9`, rand `#d6d8d7`, tekst `#9aa0a6`) ongeacht zijn beschikbare
kleur — inclusief rode/destructieve knoppen (Stop, Remove, Cancel). Rood is alleen te zien
als de actie er echt is.

Sinds 2026-08-14 geldt dat voor **elke** knopklasse via één regel in `style.css`:

```css
.btn:disabled, .btn.disabled { background-color: var(--flat-panel) !important; … }
```

`.btn:disabled` is specifieker (0,2,0) dan elke knopkleurklasse (0,1,0), dus dit hoeft
niet per klasse herhaald te worden. Dat verving vier losse disabled-regels met elk hun
eigen grijs (`.btn-run`/`.btn-outline-orange`/`.btn-outline-purple`/`.btn-outline-red`,
plus `#processBtn` en `#processAllBtn`). **Dus: alleen het `disabled`-attribuut omzetten,
nooit met de hand herstylen** — en de oude raad om per id een eigen `:disabled` toe te
voegen is niet meer nodig. Een pagina-eigen `#id:disabled` in een `<style>`-blok wint nog
wél (later in de cascade), dus wie zo'n regel tegenkomt kan hem opruimen.

## Icon-only buttons — never a text glyph

For a small square button whose whole content is one icon — every remove-`×` in
the app — use **`btn btn-outline-red btn-remove-row`** with the SVG below. Both
classes are in `style.css`; `.btn-outline-red` supplies the colour and
`.btn-remove-row` the geometry, so a non-destructive icon button can pair the
same geometry with a different colour.

```html
<button class="btn btn-outline-red btn-remove-row"
        title="Remove this row" aria-label="Remove this row">
  <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12"
       fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
    <path d="M2 2 L10 10 M10 2 L2 10"/></svg>
</button>
```

**Never a text `×` / `&times;`.** It sits high in its line box, so it is
optically above centre no matter what `line-height`, `padding` or `font-size` you
try. This is the bug that kept coming back page by page; the fix is geometric,
not typographic. The rules baked into `.btn-remove-row`: fixed `width` **and**
`height` (square, and can't be squeezed by its content), `padding: 0`,
`inline-flex` centring, `flex: 0 0 auto` for flex rows, and `svg{display:block}`
to kill the inline-baseline gap. Plus, in the markup: an SVG symmetric about its
own viewBox centre, `stroke="currentColor"` so it inherits the hover colour, and
an `aria-label` because there is no text to read.

Do **not** add `btn-sm` — the explicit box sets the size and `btn-sm`'s padding
fights it. Do **not** hand-roll the box inline (`style="width:30px;height:30px;
padding:0;font-size:1.1rem"` was FAQ's old version of exactly this).

**Direction of the hover, and why the class is `-red`:** outline at rest →
**fills solid red on hover**, never the reverse. A `.btn-danger-invert` used to do
it backwards and made identical `×` controls behave oppositely per page; it has
been deleted, so don't reintroduce it. And it is `btn-outline-red`, not
`btn-outline-danger`, because `style.css` themes the Bootstrap name **orange**.

In use: R-Finder filter rows, Redirect Generator + Canonicals rule rows,
Kopteksten + FAQ's Recent Results. The one deliberate exception is an
input-group's clear-the-field `×` (Canonicals' result filter): that is neutral,
not destructive, and a fixed square box would break the input-group's height
matching.

## Info banners — yellow, never grey

A standing note about a section (not a hover hint — that's the "i" button below)
uses the shared **`.info-note`** class from `style.css`:

```html
<div class="info-note mb-3">Auto-Queue: jobs will start automatically…</div>
```

Yellow/amber — `background #fff8ed`, `border #f0d9b5`, text `#8a5a00`, radius
`8px`, `padding 10px 14px`, `font-size 0.88rem`. **Do not use grey**, and in
particular don't reach for `.alert-info` — `style.css` themes it grey
(`rgba(232,233,235,.5)`), so it reads as disabled chrome and gets skipped. Same
reasoning as `.alert-done-yellow` for end-of-run banners, and as "don't use grey
as a label colour".
The class is declared **once** in `css/style.css`; never re-declare it per page
(it was duplicated verbatim in seo-stats / healthscore / shop-campaigns before
being consolidated). In use in SEO stats, Healthscore, Shop-campaigns, Thema Ads.

## Labels / badges — never lean on Bootstrap's colour names

`style.css` re-themes `.bg-success`, `.bg-info` and `.bg-primary:not(.navbar)` to
`var(--color-section)` — **light grey**. There are `.badge.bg-*` rules that put the
colour back, but they are not reliable: an inspector screenshot on 2026-08-07 showed
Bootstrap's `.bg-success` struck through and the badge rendering grey anyway. The same
trap produced an invisible `.badge.bg-info` on the same page earlier that day.

For a label with a colour that has to survive, use a **page-local class**, and prefer
the outlined form used by the OOS / MANUAL labels in DMA Exclusions:

```css
.lbl { background: transparent; font-weight: 700; border: 1px solid currentColor; }
.lbl-green { color: #198754; }   /* one color rule per colour — the border follows */
```

Two notes when converting filled badges to outlined: Bootstrap's amber `#ffc107` is
readable as a fill with dark text but **not** as border-and-text on white (use `#b26a00`,
same hue, enough contrast), and `.badge` itself sets `color:#fff`, so your `.lbl-*` rule
must load after Bootstrap — a page `<style>` block does.

**Category labels in a table column are UPPERCASE and outlined** (Bot Hits' Soort
column, 2026-08-11) — same form as OOS / MANUAL, with the hue as the *text* colour
and `currentColor` as the border:

```css
.lbl { background: transparent; font-weight: 700; border: 1px solid currentColor;
       text-transform: uppercase; letter-spacing: .03em; font-size: 0.7rem;
       padding: 0.2em 0.55em; border-radius: 0.375rem; display: inline-block; }
```

Going outlined has a knock-on effect worth knowing: a *filled* badge can carry a
light hue (with dark text on it), an outlined one cannot — light-on-white is
unreadable. So a chart hue that doubles as a label needs to be mid-to-dark. That
constraint replaced a luminance-based `textOn()` helper in Bot Hits: picking the
readable text colour per background works, but keeping the label palette dark is one
rule instead of a function.

## Info tooltips — the "i" button

For a "what is this?" hint next to a header or field, use the inline
purple-circle **"i"** SVG with a native `<title>` tooltip (no Bootstrap tooltip JS
needed). 16×16, brand purple `#5e4a90`, white glyph, `cursor: help`, baseline
nudge `vertical-align:-2px`:

```html
<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 16 16" style="cursor: help; vertical-align: -2px;"><title id="myTip">Explain this here.</title><circle cx="8" cy="8" r="8" fill="#5e4a90"/><circle cx="8" cy="4.2" r="1.1" fill="#fff"/><rect x="6.9" y="6.5" width="2.2" height="5.8" rx="0.4" fill="#fff"/></svg>
```

Give the `<title>` an `id` and rewrite its text at runtime to update the hint
(e.g. GSD Campaigns' "last successful data load"). In use across GSD Campaigns,
SEO titles / prio / stats, DMA Exclusions, DM Review, Redirect Tool, R-URL Optimizer.

## Het vlakke thema — sinds 2026-08-14 gewoon `style.css`

> **Er is één stylesheet.** `theme-flat.css` was van 13 t/m 14 augustus 2026 een
> override-laag bovenop `style.css`; Joep keurde de proef goed en de twee zijn
> samengevoegd. Dat bestand bestaat niet meer, en de `<link>` is uit alle 35 pagina's.
> De rest van dit document beschrijft dus gewoon wat er in `css/style.css` staat — er is
> geen tweede bestand meer dat het overschrijft.

Naar het voorbeeld `Downloads\claude\2026-08-13 18 11 15.png` (Semrush): vlakke
kaartkoppen i.p.v. de grijze balk, één knopvorm, lichte randen, zachte accentkleur.

**Terugdraaien** is geen `sed` meer maar het git-tag **`ui-voor-flat`**, dat naar de laatste
commit vóór de restyle wijst. Dat is met opzet zo gelaten in de merge-commit vastgelegd.

**Palet, uit de screenshot gemeten met PIL (niet geschat).** De variabelen staan in
`:root` in `style.css`; gebruik ze, schrijf de hex niet uit.

| rol | variabele | kleur |
|---|---|---|
| pagina | `--flat-bg` | `#f4f5f5` |
| paneel / kaartkop / thead / disabled | `--flat-panel` | `#f4f5f9` |
| vlak (kaart, input, outline-knop) | `--flat-surface` | `#ffffff` |
| rand | `--flat-border` | `#d6d8d7` |
| rand van een outline-knop | `--flat-border-strong` | `#c3c7cc` |
| tekst / secundair | `--flat-text` / `--flat-muted` | `#1a1d19` / `#666a6b` |
| accent — alleen LIJNEN | `--flat-accent` | `#8796ef` |
| chip- en hovervulling | `--flat-accent-soft` | `#eaeef9` |
| actie (CTA) | `--color-button` | `#CC5500`, hover `#E97451` |
| secundaire actie | `--color-navbar` | `#5e4a90` |
| radius | `--flat-radius` | `6px` |

**Drie kleuren met elk één betekenis:**

| kleur | betekenis | waar |
|---|---|---|
| **oranje** `#CC5500` | actie | gevulde CTA (`.btn-run` c.s.), en oranje-outline voor "+ Add rule" / Export |
| **paars** `#5e4a90` | secundaire actie | `.btn-outline-purple` (Refresh, Preview, "andere actie"), navbar |
| **blauw** `#8796ef` | selectie en interactie | tab-onderlijn, focusring, chiprand — **nooit een vulling met tekst erop** |

Contrastmetingen, want die stuurden het ontwerp:

* wit op blauw `#8796ef` = **2,75:1** → zakt door AA. Daarom is blauw de kleur van LIJNEN
  en niet van gevulde knoppen. `--flat-accent-solid` (`#5566e0`, 4,81:1) staat er nog als
  gemeten alternatief voor wie de CTA ooit tóch blauw wil.
* wit op oranje `#CC5500` = **4,31:1** → genoeg voor knoptekst (groot/bold), net onder de
  4,5:1 voor gewone tekst. Bestaande huiswaarde; `#B84D00` geeft 5,1:1 als het ooit moet.
* paars `#5e4a90` op wit = **7,35:1**, en wit op paars idem. Dit is dus wél een kleur die
  tekst mag zijn — vandaar dat de secundaire knop zijn label in paars draagt.

**Eén ronde die is teruggedraaid, zodat je hem niet opnieuw voorstelt.** De eerste versie
van het thema maakte álle outline-knoppen neutraal grijs, op het argument dat
paars-versus-oranje alleen kleur was en geen betekenis. Dat is op 2026-08-14 teruggedraaid
(Joep): paars ís de betekenis "secundaire actie", en grijs maakte van elke Refresh op elke
pagina een naamloos knopje. Neutraal grijs is nu alleen nog voor knopklassen die niets
zeggen — de Bootstrap-kleurnamen (`.btn-outline-secondary` c.s.) en de segmented controls.

**Waarom er nog `!important` op kleuren staat.** Niet meer omdat dit een laag is — die is
opgeheven. Er zijn twee echte redenen, en ze staan per blok in `style.css` in het
commentaar:

1. **Bootstrap's eigen utilities** (`.bg-info`, `.bg-secondary`, `.text-white`) zetten hun
   kleur mét `!important`. Zonder het onze valt een `card-header bg-primary` terug op
   Bootstrap-blauw.
2. **Zeventien pagina's** definiëren knopklassen in hun eigen `<style>`, en dat blok laadt
   ná elke stylesheet. Op gelijke specificiteit wint het dus altijd. Het gaat om 77 regels;
   die opruimen staat als open taak in `cc1/TASKS.md`, en daarna kan reden 2 vervallen.

Het staat op **kleur** en bewust **niet** op maatvoering: padding, breedte en `nowrap`
blijven van de pagina, zodat bestaande lay-outs heel blijven.

**Twee dingen bewust niet aangeraakt:** de paarse navbar (staat niet op de screenshot en is
het enige element dat het dashboard herkenbaar maakt), en rood voor destructieve acties
(alleen platter gemaakt — een verwijderknop hoort niet in de accentkleur te verdwijnen).

**Eén cascadeval die je bij het samenvoegen tegenkomt, en die alleen met een meting
zichtbaar is.** `.bg-primary:not(.navbar)` heeft door de `:not()` specificiteit **0,2,0** —
precies gelijk aan `.card-header.bg-primary` — en ze hebben beide `!important`. Met twee
bestanden won de kopbalk op bestandsvolgorde; in één bestand won `.bg-primary`, en dan
kleurt een `card-header bg-primary` weer ouderwets grijs (gemeten op SEO titles). Opgelost
met `:not(.card-header)` erbij, niet door de blokken te herordenen: volgorde-afhankelijkheid
breekt zodra iemand een blok verplaatst.

## Tabs — multi-section cards (see Canonicals)

When one card holds several parallel rule-sets or modes, use Bootstrap tabs:
`ul.nav.nav-tabs` › `li.nav-item` › `button.nav-link` with
`data-bs-toggle="tab" data-bs-target="#pane"`; panes are `div.tab-pane.fade`
(first one also `show active`).

**De vorm staat sinds 2026-08-13 in `css/style.css` — schrijf hem NIET per pagina.**
Tabs zijn tekst op een lijn; de markering is een **onderlijn**, geen kader:

| staat | markering |
|---|---|
| rust | `#3d4348`, weight 500, transparante onderlijn |
| hover | tekst donkerder + **grijze** onderlijn `#c3c7cc` |
| actief | tekst `#1d2129`, weight 600, **brand-paarse** onderlijn `var(--color-navbar)` |

Dit verving het blok `.nav-link { color:#3a3a3a; font-weight:bold }` dat op **elf**
pagina's afzonderlijk stond. Let op de cascade: zo'n blok staat in de page-`<style>` en
laadt dus ná `style.css`. Wie de gedeelde regel aanpast en een pagina-blok laat staan,
ziet op die pagina niets veranderen — daarom zijn ze alle elf weggehaald.

Alleen echte pagina-eigen behoeften blijven lokaal. `thema-ads.html` is het enige
voorbeeld: acht tabs op één regel, dus `flex-wrap: nowrap` + `white-space: nowrap` +
`font-size: 0.9rem`. Kleur, gewicht en onderlijn komen daar wél uit de gedeelde regel.

**Niet paarse links met een donkere active-state.** Bot Hits had
`.nav-link { color:#5e4a90 }` + `.nav-link.active { color:#212529 }`, wat de nadruk
omdraait: de tabs die je kunt aanklikken lichten op en die waar je op staat wordt stil.
Dezelfde regel geldt voor de onderlijn-variant — de tab waar je **op** staat hoort de
opvallendste te zijn (2026-08-11, herbevestigd 2026-08-13).

Eén bewuste afwijking van het voorbeeld waar dit vandaan komt
(`Downloads\claude\2026-08-13 18 11 15.png`, Semrush): de actieve onderlijn is
brand-paars en niet blauw. Blauw is in dit dashboard nergens een accentkleur en zou als
een tweede merk lezen.

## Form controls — inputs, date pickers, checkboxes, radios, selects

Plain Bootstrap 5.3 — **no custom skinning**, so the brand theme in style.css
carries through automatically:

- **Text / number inputs & selects**: `form-control` / `form-select`. Add `-sm`
  inside dense toolbars; set an explicit inline `width` when it shouldn't stretch.
- **Bestandsupload**: `<input type="file" class="form-control">` — en die is **32rem
  (512px) breed**, niet de volle kaartbreedte. Staat als gedeelde regel in `style.css`
  (`input[type="file"].form-control { max-width: 32rem; }`), dus zet er geen eigen breedte
  op. Reden (Joep, 2026-08-14: "die van Auto-Redirects is prima"): een file-input bevat één
  knop plus een bestandsnaam, dus meerekken met de kaart geeft alleen leegte. Gemeten vóór
  de regel: Auto-Redirects 513px (het stond in een `col-md-6`), Kopteksten 939px, Unique
  titles 1042px. Een pagina die om een eigen reden breder moet, zet zelf een `max-width` —
  die wint.
- **Date pickers**: sinds 2026-08-13 zit elke datumkiezer in een **`.date-box`** —
  één omlijsting met een kalendericoon ervoor. Een periode is één ding, dus het hoort
  één control te zijn; twee losse `form-control`-velden met elk een eigen label lazen
  als twee onafhankelijke filters. Doorgevoerd op alle tien de pagina's die een
  datumveld hebben.

  ```html
  <label class="form-label">Date range</label>
  <div class="date-box">
      <input type="date" id="startDate" aria-label="Start date">
      <span class="sep">–</span>
      <input type="date" id="endDate" aria-label="End date">
  </div>
  ```

  Eén datum: dezelfde box met één input, zonder `.sep`. De inputs houden hun eigen
  `id`, `value` en `onchange`, en **`form-control` gaat eraf** — `.date-box` zet de
  rand, de breedte en het lettertype. Bestaande JS (`.value`, flatpickr-init op `#id`)
  hoeft niet mee te veranderen.

  Zes dingen die in de CSS zijn opgelost en die je niet zelf moet overdoen:
  1. **Het icoon is een `::before` met een data-URI**, geen inline `<svg>`. Anders staat
     hetzelfde SVG-blok vijftien keer over tien pagina's.
  2. **`display:flex` + `width:fit-content`**, niet `inline-flex`. Met inline-flex ging
     het label ernaast staan zodra er ruimte was en eronder als die er niet was —
     dezelfde pagina, twee uitkomsten, afhankelijk van de kolombreedte.
  3. **Chrome's eigen kalendericoon** zit ín het veld, dus op pagina's zonder flatpickr
     stonden er ineens twee (bij een bereik drie). Het is niet verborgen maar
     uitgerekt over het hele veld en transparant gemaakt: het glyph verdwijnt en het
     klikvlak wordt juist groter. Met flatpickr is die regel vanzelf inactief, want die
     vervangt de input door `type=text`.
  4. **Bij een bereik staat de eerste datum RECHTS uitgelijnd** (`:has(.sep)`). Beide
     velden zijn 6,6rem maar een datum is maar ~84px breed, dus links uitgelijnd viel
     alle speling vóór het streepje: gemeten 60px links tegen ~15px rechts, en dan kleeft
     het streepje aan de tweede datum. Maak de velden hiervoor **niet** smaller — die
     breedte moet zowel `2026-08-13` (flatpickr) als `13-08-2026` (native) als Chrome's
     eigen icoon aankunnen. Losse datumvelden blijven links uitgelijnd.
  5. **Geen eigen rand, radius of schaduw op de velden — ook niet met een `#id`**
     (2026-08-14). Vijf pagina's hadden nog `#startDate, #endDate { border: 1px solid
     #d9d4e8; border-radius: 10px; padding: … }` uit de tijd van losse datumvelden. Het
     kader zit nu om het HELE bereik, dus dat tekent er een **tweede binnenin**: nagemeten
     in een screenshot van Joep staat de buitenlijn op `#d4d5d5` (de box) en 1px daarnaast
     `#d9d4e8` (de pagina-regel). Ze waren onzichtbaar zolang `style.css`' `border: 0
     !important` meekwam — precies daarom weghalen en niet laten staan: een regel die
     vandaag niets doet is morgen een bug. Weg in seo-stats, bothits, shop-campaigns en
     gsd-campaigns; `accent-color`/`color-scheme` mogen blijven (die gelden voor de native
     picker). Óók weghalen: `#id::-webkit-calendar-picker-indicator`-regels, want een
     id-selector wint van `style.css` en zet Chrome's glyph weer aan naast onze `::before`.
  6. **Focus hoort om de BOX, niet om een veld** (2026-08-14). De velden hebben `border: 0`
     en `box-shadow: none`, dus een `#startDate:focus`-regel is per definitie onzichtbaar —
     die stonden er wel, en deden al niets. Het staat nu als `.date-box:focus-within` in
     `style.css` (paars) én in `theme-flat.css` (blauw accent, met `!important` op de
     border-color omdat de `.date-box`-regel daar ook `!important` is). Plus
     `.date-box input:focus { outline: none }`: zonder dat tekent Chrome zijn eigen ring om
     de border-box van de input en valt dat als een strak zwart kadertje **midden in** de
     box. Die twee horen bij elkaar — alleen de outline weghalen maakt focus onzichtbaar.

  **VELDBREEDTE — hij hangt af van wat er in het veld staat, niet van de pagina**
  (2026-08-14, Joep: de picker in Retrieve URL data mocht smaller). Twee regels in
  `style.css`, en een veld matcht altijd precies één ervan omdat flatpickr de input op
  `type=text` zet:

  | veld | breedte | waarom |
  |---|---|---|
  | `.date-box input.flatpickr-input` | **5,3rem** (84,8px) | er staat altijd exact `YYYY-MM-DD`; gemeten 79,0px bij 13,6px system-ui, dus ~6px lucht |
  | `.date-box input[type="date"]` | **6,6rem** (105,6px) | native: Chrome zet zijn eigen kalendericoon ÍN het veld en toont `13-08-2026` — smaller kapt dat af |

  Dit was eerst één 6,6rem voor allebei, met 26px lucht per veld. Zet er **geen** eigen
  breedte per pagina op; de enige uitzondering is GSD Campaigns, waar de velden een
  uitleg-placeholder dragen ("Leave empty for most recent") en daarom 13,5rem zijn.

  Daarbovenop komt **flatpickr** voor de kalender zelf, en dat is de canonieke look
  (paarse maand-/weekdagkop, paarse geselecteerde dag). This doc previously said "no JS
  date library anywhere", which was already untrue when item 18 asked for GSD Budgets'
  pickers to "match the blueprint" — what was meant was the purple calendar, not the bare
  OS one. To add it to a page:
  1. In `<head>`, **vóór** `style.css`:
     ```html
     <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr@4.6.13/dist/flatpickr.min.css">
     <link href="/static/css/style.css" rel="stylesheet">
     <script src="https://cdn.jsdelivr.net/npm/flatpickr@4.6.13"></script>
     ```
     **De volgorde is niet vrij**: bootstrap → flatpickr → `style.css`. Ons paarse thema
     staat sinds 2026-08-14 in `style.css`, en flatpickr's eigen
     `.flatpickr-calendar { border-radius: 5px }` verslaat het als die stylesheet later
     laadt. Gemeten: op alle acht pagina's stond de kalender daardoor even op 5px in plaats
     van 12px. `style.css` is de override-laag, dus die komt als laatste.
  2. **Niets kopiëren.** Het `.flatpickr-*`-blok stond op zes pagina's byte-identiek en
     staat nu één keer in `style.css`. Wie hier iets aan de kalender wil wijzigen, doet dat
     daar.
  3. Init inside `DOMContentLoaded`, **guarded** so an unreachable CDN degrades to
     the working native input rather than breaking the page:
     ```js
     if (window.flatpickr) {
         const fpOpts = { dateFormat: 'Y-m-d', allowInput: true,
                          disableMobile: true, locale: { firstDayOfWeek: 1 } };
         flatpickr('#startDate', fpOpts);
     }
     ```
  Set any default `.value` **before** the `flatpickr()` call — it adopts the
  input's current value, and flatpickr swaps the field to `type="text"`, so
  assigning a `YYYY-MM-DD` string afterwards is not equivalent. Twee dingen die daarbij
  horen en die je op een native veld niet ziet:
  - **Een `max`/`min`-attribuut op de input doet niets meer.** Dat geldt alleen voor
    `type=date`. Geef de grens mee als optie (`maxDate: 'today'`), anders staat de kalender
    stilzwijgend datums in de toekomst toe (DMA Bidding, 2026-08-14).
  - **Reset via `fp.setDate(waarde, false)`, niet via `.value =`.** Een directe `.value`
    verzet het veld maar niet flatpickr's eigen state, dus de kalender opent daarna nog op
    de oude datum. Het tweede argument `false` onderdrukt `onChange` (R-Finder's
    Reset-knop).

  Still-native (no flatpickr): SEO Priority, Performance Standup. R-Finder en DMA Bidding
  hadden de kale OS-kalender en hebben sinds 2026-08-14 de paarse.
- **A date range loads on change — there is no Load button.** SEO stats and
  Shop-campaigns both put the range INSIDE the chart card (it scopes the page, but
  that is where you look at it) with Refresh on the card's title row, and load on
  change. Four things go with that:
  1. `onchange="autoLoad()"` on the inputs **and** `onChange: autoLoad` on the
     flatpickr instances — flatpickr replaces the native picker, so the input's own
     `onchange` never fires once it is attached. Wiring only one of the two is a
     picker that silently does nothing.
  2. Debounce ~400ms: picking a new start AND end fires twice within a second and
     only the second range matters.
  3. `setDates()` passes `false` to flatpickr so the presets set both fields
     WITHOUT triggering the debounce, then call `load()` once themselves.
  4. A **generation token** (`loadToken`) — two loads can now be in flight at once,
     so every async step that writes shared state checks the token it started with
     and bails. Without it a slow older range paints over a newer one.
- **A preset ends on today − 3, not on today** (Joep, 2026-08-11, Shop-campaigns).
  Clicking 7d on 11 August gives **2 t/m 8 August**. The last three days have not
  settled — conversions and revenue land late — so a range that runs to today sags on
  the right and reads as a decline that did not happen. Two things go with it: the
  date inputs stay unbounded (the lag is a property of the shortcut, not of the tool),
  and **the range the page opens on must use the same end date**, or the page opens on
  one range and its own 30d button produces a different one without anything having
  changed.
- **`ymd()` must build the string from local date parts, never `toISOString()`.**
  `toISOString` converts to UTC, so local midnight in CEST becomes 22:00 the previous
  day and the whole range shifts back one day. Shop-campaigns does it right
  (`getFullYear` / `getMonth` / `getDate`); **`seo-stats.html` still uses
  `toISOString`** — latent, because flatpickr there receives Date objects rather than
  strings, but it is the same off-by-one waiting to happen.
- **Preset group (7d / 14d / 30d / 90d / All)**: `.btn-preset` needs a
  `min-width: 3rem`. With padding alone the cell width follows the label, so "7d"
  and "All" sit in ~39px cells between ~47px ones — each label is dead-centre in
  its own cell, but the cells differ, and that is what reads as "All isn't
  centred" (Joep, 2026-08-06). An "All" preset uses a FIXED start date, not an
  open-ended range: the API wants both bounds.
- **Checkboxes / radios**: `<input class="form-check-input" type="checkbox|radio">`
  in a `.form-check` with a `.form-check-label`. Keep the default Bootstrap accent —
  don't recolour. (Canonicals' bulk-select adds a `canon-select` class alongside
  `form-check-input`; that's a local extension, not the shared default.)

## Status / progress bar

When a process runs, show the status bar (hidden `#progressArea` by default):
a **green** (`#00b894`) striped/animated Bootstrap progress bar with a
label + percent line above it, and a **red-outline Cancel button** below. Same
markup as the run/LL bars in GSD Campaigns. Drive it with `showStatus()` /
`setStatus(pct, text)` / `hideStatus()` and honour `cancelRequested`
(see the template JS).

When the run ends, the bar comes **down** and a Done banner takes its place — see
"Done banner" below.

Two flavours — pick by whether the work is cancellable:

1. **Full status area** (`#progressArea` + red-outline Cancel) for long
   *cancellable* runs, e.g. the GSD run/LL bars and the SEO-Titles Generate run.
2. **Inline bar, no Cancel** for a single blocking action you cannot abort
   mid-flight — e.g. SEO Titles *Publish*, which pushes live to `/page-titles`.
   Place it directly above the result box, same green striped bar, with a
   label/percent row above it:
   ```html
   <div id="xProgress" class="mb-3" style="display:none;">
     <div class="d-flex justify-content-between align-items-center small text-muted mb-1">
       <span id="xProgressLabel">Working…</span><span id="xProgressPct">0%</span>
     </div>
     <div class="progress" style="height:1.1rem;">
       <div id="xProgressBar" class="progress-bar progress-bar-striped progress-bar-animated"
            style="width:0%; background-color:#00b894;" role="progressbar"
            aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"></div>
     </div>
   </div>
   ```

**A Cancel button needs a boundary, and you pick it on consistency — not on how
responsive the button feels** (added 2026-08-12, Bot Hits' "Nieuwe logs ophalen").
Cooperative only: the button sets a server-side flag and the worker checks it where
stopping leaves a whole result behind. In that tool that is *between files* while
downloading (a partial download costs time, never correctness — the ingest demands 24
full hours and a retry skips what is already on disk) and *between log dates* while
parsing, deliberately no finer, because stopping inside one date leaves half a day in
the cube that then counts as ingested. Show the request landed — the button goes
disabled with "Annuleren aangevraagd…" the instant it is clicked, or the user clicks
three more times while the worker finishes its current file. And name the outcome
honestly: a cancelled run says **"Geannuleerd"**, not "Klaar", with the same figures
plus the reassurance that what did get processed is complete.

**Pick the denominator before you pick the markup** (added 2026-08-12, Bot Hits'
"Nieuwe logs ophalen"). Count **units of work**, not volume. That download skips a file
that is already on disk at the right size, which is what makes a retry after an aborted
run cheap — and a skipped file contributes zero bytes, so a bytes-driven bar sits at 0%
through a run that is genuinely working. Files: 120/120. Bytes: 0. Count a **failure as
progress** too (the unit is handled, just not well), or the bar hangs on a finished run.
Keep the volume in the label — it is the unit the user was quoted in the confirm dialog
— but off the axis. And if the denominator does not exist before the work starts,
building it is the first task; a bar without one is not a bar.

### The status-bar lifecycle — three rules (added 2026-07-31)

Learned from SEO Titles' Publish, which sat at **100% / "Pushing AI unique titles…"**
and never produced a banner. Nothing was broken — the run genuinely had ~30 minutes of
work left — but the UI made a working run look hung. Apply these to **every**
status-bar process, not just this one.

1. **A later opaque phase must NOT hold a determinate bar at 100%.** When the measured
   phase (batch pushing) finishes and an unmeasurable one starts (per-URL AI titles,
   dedup refresh), switch the track to **`.progress.indeterminate`** — the shared CSS in
   `style.css` slides a 35% segment across it — and blank the percent readout. Movement
   says "busy", the length claims nothing. A full bar reads as *finished and stuck*,
   which is precisely how it was misread.
2. **Completion must not depend on the client's own request resolving.** A long POST is
   the least reliable thing in the flow: reload the tab, lose the response to a proxy
   timeout, open a second tab, and the `finally` that hides the bar never runs. Poll a
   **server-side status endpoint** and let *whichever notices first* close the run out —
   funnel both paths through one `finalise…()` guarded by a boolean so the banner is
   written once. If the POST does come back it carries more detail (per-batch responses),
   so let it reset the guard and overwrite the poller's simpler banner.
3. **Adopt a run already in progress on page load.** If the status endpoint says
   `running`, show the bar and start polling instead of rendering an idle page. Otherwise
   a reload during a 30-minute push looks like nothing is happening, and the user starts
   a second one.

Corollary for the failure path: a dropped response on a long push does **not** mean the
push failed. Say "request lost — the push may still be running" (tone `warning`), not
"failed" — and only if the poller has not already reported success.

**Never replace a banner with an *indeterminate* bar.** A bar implies "this much
is done"; if you have no real number, an animated bar is a lie and the old text
banner was more honest. (Rule 1 above is the opposite case: an indeterminate bar
*during* a phase that is still running, not in place of a finished run's banner.) To get real numbers from a synchronous POST, keep the
endpoint as-is (FastAPI already runs it via `run_in_executor`, so it's off the
event loop), have the service write progress into a module-level state dict, and
add a `GET /<thing>-status` the frontend polls **while its own POST is still in
flight** — see `seo_titles_service.py` `_pub_state` / `get_publish_status()` and
`publish()` / `pollPublish()` in `seo-titles.html`. Two rules that pattern must
follow: advance the counter only **after** the work is durably committed (so the
bar never runs ahead of reality), and give the endpoint's `except` branch a
`mark_*_error()` call — otherwise a raising handler leaves the bar spinning
forever. Phases that are one opaque call get a **label change at the same
percentage**, not fake movement.

## Done banner — how a run ends

**A finished run never leaves its progress bar on screen.** A bar parked at 100%
reads as "still working"; the moment the run ends, hide the bar (and its
label/percent row) and put a **Done banner** in its place. Origin:
`dma-exclusions.html` `showOosDone()`; also `seo-titles.html` (both the Publish
push and the Retrieve-URL-data run).

The banner is **light yellow**, because `style.css` flattens `.alert-success` and
`.alert-info` to theme grey — a Bootstrap success alert is nearly invisible here
and reads as "nothing happened". Use the shared class (in `style.css`, don't
re-declare it per page):

```css
.alert-done-yellow { background-color:#fff8e1 !important; border-color:#f3e2a0 !important; color:#6b5900 !important; }
```

Markup — one dismissible banner per run, hidden by default, placed exactly where
the progress bar was:

```html
<div id="xDone" class="alert alert-dismissible fade show mb-3" style="display:none;" role="alert">
  <span class="done-text"></span>
  <button type="button" class="btn-close" onclick="hideDoneBanner('xDone')" aria-label="Close"></button>
</div>
```

Drive it with the two helpers from `seo-titles.html` (`showDoneBanner(id, html,
tone)` / `hideDoneBanner(id)`). Tone picks the colour by **outcome**, not by
step:

| Tone | Class | When |
|------|-------|------|
| `done` | `alert-done-yellow` | ran to completion, nothing failed |
| `warning` | `alert-warning` | completed, but some rows failed / were skipped |
| `error` | `alert-danger` | the run itself failed |
| `cancelled` | `alert-info` | user stopped/cancelled mid-run |

Say "Stopped — partial run" for a cancelled run, not "Done". Watch for backends
that land a stopped run as `status="done"` with a separate stop flag
(`seo_titles_service.py` `should_stop`) — check the flag, or the banner lies.

Content shape: a bold `Done — <headline>.` sentence, then ` · `-joined counts with
the **numbers bold** (`<strong>12</strong> failed`). Keep it to the outcome
numbers — if a counters row sits under the banner, that carries the full
breakdown. A raw API response goes in a folded
`<details><summary>Response detail</summary>` inside the banner, never as a bare
`<pre>` dump.

Two rules that are easy to get wrong:

- **Re-show guard.** If a status poll keeps running after the banner appears,
  dismissing it must stick — keep a `xDoneDismissed` flag, set it from the
  `btn-close` handler, and reset it when the next run starts (see
  `genDoneDismissed` in `seo-titles.html`).
- **Show it on the failure path too.** The `catch`/`finally` of the action must
  produce a banner as well, or a failed run just makes the bar vanish with no
  explanation.

**Spinners** are for small inline "busy" hints, not for table or run progress.
The markup has drifted (11 variants across the pages); the canonical form is
`<span class="spinner-border spinner-border-sm d-none" role="status"></span>`
toggled via `.d-none`, next to the button or label it belongs to.

## Footer

Every page ends with:

```html
<footer class="text-center py-4">
    <small class="text-muted">Digital Marketing tools by Joep van Schagen - 2026</small>
</footer>
```

**Every page** — a tool without it ends in whitespace and reads as if the page
failed to finish loading. Bot Hits had none until 2026-08-11. Place it after the
last `.container` and before the `<script>` tags, so it sits outside the content
column and stays centred on the viewport.

## Deploy note

Frontend is static (`StaticFiles` from the `dm-tools` dir) — changes are live on
a browser refresh, no uvicorn restart. Backend is bare uvicorn (no `--reload`),
so *backend* changes still need a manual kill + relaunch.
