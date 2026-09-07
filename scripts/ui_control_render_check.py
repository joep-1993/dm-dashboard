#!/usr/bin/env python3
"""Render the dashboard's five control types and MEASURE their shape.

WHY THIS EXISTS (2026-09-07): `.form-check-input[type="checkbox"] { border-radius: 50% }`
(f29142f) also matches `.form-switch .form-check-input` — a class plus an attribute is
(0,2,0), exactly as specific as Bootstrap's own switch rule, and style.css loads after
Bootstrap, so it won. Every switch's 2em x 1em track became an ELLIPSE instead of a pill.
That shipped and survived four days, because at 1em tall nobody spots it. A screenshot for
a human to eyeball is therefore NOT the check — that is what already failed. This measures.

WHAT IT MEASURES: each control is rendered alone on a white page at --zoom, screenshotted
with Windows Chrome, and reduced to a silhouette: per pixel row, the span between the first
and last non-white pixel. All five silhouettes are horizontally convex (circle, ellipse,
stadium, rectangle), so a per-row span is exact, and it is immune to what sits INSIDE the
shape — the white checkmark in a checked box and the white knob in a switch track would both
break a naive "count the accent pixels" approach.

    fill ratio = silhouette area / (bbox width * bbox height)

        circle / ellipse    pi/4        = 0.785
        pill (w = 2h)       1-(4-pi)/8  = 0.893
        rectangle                         1.000

0.785 vs 0.893 is a 14% gap on a single scalar, which is why this catches the exact
regression above while tolerating antialiasing.

TWO KINDS OF ASSERTION, deliberately separated:

  * SHAPE RULES are hard-coded from UI_BLUEPRINT §"Form controls" — checkbox and radio are
    round, a switch keeps its pill. These are decisions, so a violation is a FAIL with the
    rule quoted.
  * EVERYTHING ELSE is compared against a recorded baseline (ui_control_render_baseline.json)
    so that any DRIFT in size is caught even where no rule says what the number should be.
    An intended change is re-recorded with --update-baseline, which puts the new number in a
    diff where it can be reviewed.

The date-box is measured against a form-select-sm's height on purpose: TASKS carries an open
item that it stands 1,4px too tall dashboard-wide (the fix is page-local in seo-prio.html).
That is reported as a WARN, not a FAIL — it is a known-open item, and a lint step that cries
about it every run gets ignored.

Reads frontend/css/style.css FROM DISK, not from the running server: this checks what you are
about to commit. Bootstrap comes from the CDN, same version the pages pin.

USAGE
    python scripts/ui_control_render_check.py                 # check, exit 1 on failure
    python scripts/ui_control_render_check.py --zoom 8        # bigger render
    python scripts/ui_control_render_check.py --update-baseline
    python scripts/ui_control_render_check.py --keep          # keep the PNGs and say where

Needs Windows Chrome (WSL has no headless Chromium and CDP is blocked, hence --screenshot).
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image

REPO = Path(__file__).resolve().parents[1]
STYLE_CSS = REPO / "frontend" / "css" / "style.css"
BASELINE = Path(__file__).with_name("ui_control_render_baseline.json")
BOOTSTRAP = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"

CHROME_CANDIDATES = [
    "/mnt/c/Program Files/Google/Chrome/Application/chrome.exe",
    "/mnt/c/Program Files (x86)/Google/Chrome/Application/chrome.exe",
]

# Shape expectations. `ratio` is the fill ratio defined in the docstring; `aspect` is
# width/height. `None` means "no rule, baseline only".
ROUND, PILL, RECT = "round", "pill", "rect"
SHAPE_RATIO = {ROUND: 0.785, PILL: 0.893, RECT: 1.000}
RULE = {
    ROUND: 'UI_BLUEPRINT §"Form controls": "Selectievakjes zijn ROND, net als de radio\'s"',
    PILL: 'UI_BLUEPRINT §"Form controls": "Een .form-switch valt buiten die regel en houdt zijn pil"',
}

CONTROLS = [
    # key            html                                                        shape  aspect
    ("checkbox", '<input class="form-check-input" type="checkbox" checked>', ROUND, 1.0),
    ("radio", '<input class="form-check-input" type="radio" checked>', ROUND, 1.0),
    (
        "switch",
        '<div class="form-check form-switch mb-0">'
        '<input class="form-check-input" type="checkbox" checked></div>',
        PILL,
        2.0,
    ),
    ("select_sm", '<select class="form-select form-select-sm"><option>x</option></select>', None, None),
    (
        "date_box",
        '<div class="date-box"><input type="date" value="2026-09-07">'
        '<span class="sep">-</span><input type="date" value="2026-09-07"></div>',
        None,
        None,
    ),
]

PAGE = """<!doctype html><html><head><meta charset="utf-8">
<link href="{bootstrap}" rel="stylesheet">
<style>
{style}
</style>
<style>
  /* The harness itself must add nothing that changes a control's box. */
  html, body {{ margin: 0; padding: 0; background: #ffffff; }}
  #probe {{ position: absolute; top: 8px; left: 8px; zoom: {zoom}; }}
  /* A select stretches to its container; give it its natural width, as on a real page. */
  #probe > select {{ width: auto; }}
</style></head><body><div id="probe">{control}</div></body></html>
"""


def win_path(p: Path) -> str:
    """/mnt/c/x/y -> C:\\x\\y (Chrome is a Windows binary and only speaks Windows paths)."""
    s = str(p)
    if not s.startswith("/mnt/"):
        raise SystemExit(f"{s} is not under /mnt — Windows Chrome cannot read it.")
    drive = s[5]
    return f"{drive.upper()}:" + s[6:].replace("/", "\\")


def find_chrome() -> str:
    for c in CHROME_CANDIDATES:
        if Path(c).exists():
            return c
    raise SystemExit(
        "Windows Chrome not found. Looked in:\n  " + "\n  ".join(CHROME_CANDIDATES)
    )


def silhouette(png: Path) -> dict:
    """Per-row span between the first and last non-white pixel -> bbox + fill ratio."""
    img = np.asarray(Image.open(png).convert("RGB")).astype(np.int16)
    # Antialiased edges land just off pure white; 250 keeps them, and no control uses a
    # near-white fill as its OUTER extent (the switch's white knob is enclosed by the track).
    non_white = (img < 250).any(axis=2)
    rows = np.flatnonzero(non_white.any(axis=1))
    cols = np.flatnonzero(non_white.any(axis=0))
    if rows.size == 0:
        raise SystemExit(f"{png.name}: nothing rendered — the page came out blank.")
    y0, y1, x0, x1 = rows[0], rows[-1], cols[0], cols[-1]
    # A control clipped by the window edge still produces a plausible-looking bbox and a
    # nonsense fill ratio — that is how the date-box first measured 0.42 instead of ~1.0
    # (window 900x400, box 660x160 at an offset of 240px). Refuse to measure it.
    ih, iw = non_white.shape
    if y0 == 0 or x0 == 0 or y1 == ih - 1 or x1 == iw - 1:
        raise SystemExit(
            f"{png.name}: the rendered control touches the image edge "
            f"(bbox x{x0}-{x1} y{y0}-{y1} in {iw}x{ih}) — it is clipped, so every "
            "measurement below would be wrong. Raise --window-size or lower --zoom."
        )
    area = 0
    for y in range(y0, y1 + 1):
        xs = np.flatnonzero(non_white[y])
        if xs.size:
            area += int(xs[-1] - xs[0] + 1)
    w, h = int(x1 - x0 + 1), int(y1 - y0 + 1)
    return {"w": w, "h": h, "area": area, "ratio": round(area / (w * h), 4)}


def render(chrome: str, workdir: Path, key: str, control: str, zoom: int) -> Path:
    html = workdir / f"{key}.html"
    png = workdir / f"{key}.png"
    html.write_text(
        PAGE.format(
            bootstrap=BOOTSTRAP,
            style=STYLE_CSS.read_text(encoding="utf-8"),
            zoom=zoom,
            control=control,
        ),
        encoding="utf-8",
    )
    subprocess.run(
        [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--hide-scrollbars",
            f"--screenshot={win_path(png)}",
            # Big enough that the widest control (date-box, ~250 CSS px: an icon plus two 6,6rem
            # date inputs) cannot be clipped at any sane zoom. A clipped control still
            # measures, as a WRONG shape — the date-box first came out at ratio 0.42 — so
            # the guard in silhouette() is what actually protects us; this just avoids it.
            f"--window-size={400 * zoom + 64},{100 * zoom + 64}",
            win_path(html),
        ],
        capture_output=True,
        timeout=120,
    )
    if not png.exists():
        raise SystemExit(f"{key}: Chrome produced no screenshot.")
    return png


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zoom", type=int, default=6, help="render zoom (default 6)")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep the PNGs and print the folder")
    args = ap.parse_args()

    chrome = find_chrome()
    # Must live on /mnt/c: Windows Chrome cannot read a WSL-only path.
    workdir = Path(tempfile.mkdtemp(prefix="dmd-ui-check-", dir="/mnt/c/temp")) \
        if Path("/mnt/c/temp").is_dir() else Path(tempfile.mkdtemp(prefix="dmd-ui-check-", dir="/mnt/c"))

    measured: dict[str, dict] = {}
    try:
        for key, control, shape, aspect in CONTROLS:
            png = render(chrome, workdir, key, control, args.zoom)
            m = silhouette(png)
            # Report at 1x so the numbers stay comparable across --zoom values.
            m["w_css"] = round(m["w"] / args.zoom, 2)
            m["h_css"] = round(m["h"] / args.zoom, 2)
            measured[key] = m

        fails, warns = [], []

        # --- Rule assertions: shape is a decision, so violating one is an error. ---
        for key, _control, shape, aspect in CONTROLS:
            if shape is None:
                continue
            m = measured[key]
            want = SHAPE_RATIO[shape]
            if abs(m["ratio"] - want) > 0.035:
                got = min(SHAPE_RATIO, key=lambda s: abs(SHAPE_RATIO[s] - m["ratio"]))
                fails.append(
                    f"{key}: fill ratio {m['ratio']} — expected {shape} ({want}), "
                    f"this reads as {got} ({SHAPE_RATIO[got]}).\n      {RULE[shape]}"
                )
            if aspect and abs(m["w"] / m["h"] - aspect) > 0.12:
                fails.append(
                    f"{key}: aspect {m['w'] / m['h']:.2f} — expected {aspect:.2f} "
                    f"(w={m['w_css']}px h={m['h_css']}px at 1x)"
                )

        # --- The date-box vs form-select-sm height parity (TASKS, open). ---
        dh, sh = measured["date_box"]["h_css"], measured["select_sm"]["h_css"]
        if abs(dh - sh) > 0.5:
            warns.append(
                f"date_box is {dh - sh:+.2f}px vs form-select-sm ({dh} vs {sh}) — known-open "
                "TASKS item; the fix is page-local in seo-prio.html."
            )

        # --- Baseline drift: catches size changes no rule pins down. ---
        if args.update_baseline:
            # Only the zoom-independent fields go in. The raw pixel w/h/area scale with
            # --zoom, so recording them would put numbers in the file that no comparison
            # reads and that differ between two correct runs — a trap for the next reader.
            # `recorded_at_zoom` is provenance only.
            slim = {
                k: {f: m[f] for f in ("w_css", "h_css", "ratio")} for k, m in measured.items()
            }
            BASELINE.write_text(
                json.dumps({"recorded_at_zoom": args.zoom, "controls": slim}, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"baseline written: {BASELINE.relative_to(REPO)}")
        elif BASELINE.exists():
            base = json.loads(BASELINE.read_text(encoding="utf-8"))["controls"]
            for key, m in measured.items():
                b = base.get(key)
                if not b:
                    warns.append(f"{key}: no baseline entry yet — run --update-baseline.")
                    continue
                for field, tol in (("w_css", 0.6), ("h_css", 0.6), ("ratio", 0.02)):
                    if abs(m[field] - b[field]) > tol:
                        fails.append(
                            f"{key}.{field} drifted: {b[field]} -> {m[field]} "
                            f"(tolerance {tol}). If intended: --update-baseline."
                        )
        else:
            warns.append("no baseline file yet — run --update-baseline to record one.")

        print(f"\nRendered at zoom {args.zoom}, sizes in CSS px (1x):\n")
        print(f"  {'control':<12}{'w':>8}{'h':>8}{'ratio':>8}   shape")
        for key, _c, shape, _a in CONTROLS:
            m = measured[key]
            name = shape or "-"
            print(f"  {key:<12}{m['w_css']:>8}{m['h_css']:>8}{m['ratio']:>8}   {name}")

        for w in warns:
            print(f"\nWARN  {w}")
        for f in fails:
            print(f"\nFAIL  {f}")
        if not fails:
            print("\nOK — all shape rules hold" + ("" if BASELINE.exists() else " (no baseline yet)"))
        return 1 if fails else 0
    finally:
        if args.keep:
            print(f"\nPNGs kept in {workdir}  ({win_path(workdir)})")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
