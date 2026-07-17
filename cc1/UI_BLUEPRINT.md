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
| Not clickable | add `disabled` | **grey outline** (`#6c757d`) automatically | — |

The grey-when-disabled behaviour is built into `.btn-run` / `.btn-outline-orange`
/ `.btn-outline-purple` `:disabled` in style.css — just toggle the `disabled`
attribute, don't restyle by hand.

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
