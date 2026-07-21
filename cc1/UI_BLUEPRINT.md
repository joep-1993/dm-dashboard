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
