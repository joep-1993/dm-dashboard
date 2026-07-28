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
- **Fixed width wrapper — same on every tool:**
  `container mt-5 pb-5` › `row` › `col-md-10 mx-auto`. Do not use `col-lg-11`,
  `container-fluid`, or a bare container (dma-exclusions' `col-lg-11` is a legacy
  outlier — do not copy it).
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
- **Sortable headers**: add `class="sortable" data-sort="<key>" onclick="sortBy('<key>')"`.
  The `.sortable` CSS shows a `⇅` idle glyph and `▲`/`▼` for the active sort
  direction (toggled by adding `sort-asc` / `sort-desc` to the active `<th>`).
- All of this CSS is in the template's `<style>` block — keep it as-is.
- **Loading state = skeleton rows, not a spinner.** While a table fetches, draw
  shimmering placeholder rows so it reads as "the table is being drawn". The point
  is **layout stability**: a skeleton row is the same height as a loaded row, so
  the table neither collapses to nothing nor grows when data lands, and on a
  *reload* it holds the current height instead of jumping. Copy from
  `gsd-campaigns.html` (origin) or `seo-titles.html` — CSS is identical, only the
  column count differs:
  ```css
  .skel-row td { vertical-align: middle; }
  .skel-bar { display:block; height:1.45rem; border-radius:4px;
      background: linear-gradient(90deg,#ececec 25%,#f6f6f6 37%,#ececec 63%);
      background-size:400% 100%; animation: skelShimmer 1.4s ease infinite; }
  @keyframes skelShimmer { 0%{background-position:100% 50%} 100%{background-position:0 50%} }
  ```
  ```js
  function skeletonRows(n = 10) {            // n = rows to draw
      const cell = '<td><span class="skel-bar"></span></td>';
      return ('<tr class="skel-row">' + cell.repeat(COLS) + '</tr>').repeat(n);
  }
  ```
  Set `COLS` to the table's real column count (checkbox and action columns
  included) or the shimmer won't line up with the header. Cap the count at the
  page size, and at ~10 when "Show all" is selected — don't draw 5,000 skeletons.
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

## Buttons

Canonical classes are defined in `style.css` (additive/opt-in). Use them; never
inline the hexes.

| Purpose | Class | Look | Placement |
|---------|-------|------|-----------|
| Run / execute (primary CTA) | `btn btn-run` | **full orange**, hover coral | **far right** of the section (`d-flex justify-content-end`) |
| Orange non-run action (e.g. Export) | `btn btn-outline-orange` | orange outline, fills on hover | — |
| Any other action | `btn btn-outline-purple` | purple outline, fills on hover | — |
| Refresh | `btn btn-outline-purple` + `↻` glyph | purple outline **with arrow icon** | usually right (`ms-auto`) |
| Destructive (Stop / Remove / Cancel) | `btn btn-outline-danger` | **red outline**, fills red on hover — *only while available* | — |
| Not clickable / unavailable | add `disabled` | **grey outline** (`#6c757d`) — always, even for red buttons | — |

**Unavailable always wins over colour.** A `disabled` button must render **grey
outline** (`#6c757d`) regardless of its available-state colour — this includes
red / destructive buttons (Stop, Remove, Cancel). Red is only shown when the
action is actually available. The canonical `.btn-run` / `.btn-outline-orange` /
`.btn-outline-purple` classes already do this via their `:disabled` rule in
style.css — just toggle the `disabled` attribute, don't restyle by hand. A
`btn-outline-danger` or hand-styled red button does **not** get it for free, so
add an explicit `#id:disabled { color:#6c757d; border-color:#6c757d;
background:transparent; opacity:1; }` (see seo-titles.html `#btnStop` /
`#btnRemove`) so red never shows in the unavailable state.

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

## Tabs — multi-section cards (see Canonicals)

When one card holds several parallel rule-sets or modes, use Bootstrap tabs:
`ul.nav.nav-tabs` › `li.nav-item` › `button.nav-link` with
`data-bs-toggle="tab" data-bs-target="#pane"`; panes are `div.tab-pane.fade`
(first one also `show active`). Restyle the links **dark + bold** (not default
blue) with this per-page CSS:

```css
.nav-tabs .nav-link { color:#3a3a3a; font-weight:bold; }
.nav-tabs .nav-link:hover { color:#1a1a1a; }
.nav-tabs .nav-link.active { color:#3a3a3a; font-weight:bold; }
```

## Form controls — inputs, date pickers, checkboxes, radios, selects

Plain Bootstrap 5.3 — **no custom skinning**, so the brand theme in style.css
carries through automatically:

- **Text / number inputs & selects**: `form-control` / `form-select`. Add `-sm`
  inside dense toolbars; set an explicit inline `width` when it shouldn't stretch.
- **Date pickers**: native `<input type="date" class="form-control">` (or
  `form-control-sm`, ~160px wide). No JS date library anywhere.
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

**Never replace a banner with an *indeterminate* bar.** A bar implies "this much
is done"; if you have no real number, an animated bar is a lie and the old text
banner was more honest. To get real numbers from a synchronous POST, keep the
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

## Deploy note

Frontend is static (`StaticFiles` from the `dm-tools` dir) — changes are live on
a browser refresh, no uvicorn restart. Backend is bare uvicorn (no `--reload`),
so *backend* changes still need a manual kill + relaunch.
