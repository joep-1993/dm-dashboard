"""Read a stored Auto-Redirects run back as selectable redirect rows.

The xlsx bytes in `rurl_run_output` are the only durable record of what a run
produced, so the "doorvoeren" flow — pushing a run's redirects to production
through the Redirect Tool — reads its rows back from there rather than
re-running the optimizer. That also means a run from weeks ago can still be
pushed, exactly like the Export button next to it.

Two output schemas exist and both are read here: v2 writes the user-facing
header (`old url` / `new url` / `score`, see
rurl_optimizer_v2_service._write_xlsx_output) while v1 wrote the raw engine
columns (`original_url` / `redirect_url` / `reliability_score`).
"""
from __future__ import annotations

import io
import logging
import threading
from collections import OrderedDict
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Column aliases per output field: v2's user-facing header first, v1's raw
# engine column second. First hit wins.
_ALIASES: dict[str, tuple[str, ...]] = {
    "old_url": ("old url", "original_url"),
    "new_url": ("new url", "redirect_url"),
    "score": ("score", "reliability_score"),
    "main_category": ("main_category",),
    "deepest_category": ("deepest_category", "redirect_category"),
    "h1": ("h1",),
    "h1_match": ("h1_match", "h1_overlap"),
    "target_products": ("target_products", "search_derived_dom_count"),
    "visits": ("visits",),
    "revenue": ("revenue", "visit_rev"),
    "reason": ("reason",),
}

# Fields rendered as whole numbers in the UI.
_INT_FIELDS = ("score", "h1_match", "target_products", "visits")

# Parsed runs, keyed by task_id. Reading a run is one DB fetch plus an
# openpyxl parse of the whole sheet, and the frontend re-reads the same run
# every time the score threshold moves — so keep the last few around.
_CACHE: "OrderedDict[str, dict]" = OrderedDict()
_CACHE_MAX = 4
_CACHE_LOCK = threading.Lock()

MAX_ROWS = 20000
DEFAULT_ROWS = 5000


def _tier(score: int) -> str:
    """Same boundaries as reliability_scorer.get_reliability_tier."""
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def parse_rows(content: bytes) -> dict:
    """Parse a run's xlsx into pushable rows plus a tally of what was left out.

    Rows without a redirect target (the URLs the optimizer could not resolve)
    and rows without a numeric score are never offered for selection: the
    score gate is the whole point of this screen, and an unscored row cannot
    take part in it. They are counted so nothing vanishes silently.
    """
    import pandas as pd

    df = pd.read_excel(io.BytesIO(content))
    cols = {str(c).strip().lower(): c for c in df.columns}
    pick: dict[str, Any] = {}
    for field, names in _ALIASES.items():
        for name in names:
            if name in cols:
                pick[field] = cols[name]
                break
    if "old_url" not in pick or "new_url" not in pick:
        raise ValueError(
            "run output has no recognisable old/new URL columns "
            f"(found: {', '.join(map(str, df.columns))})"
        )

    out = pd.DataFrame({field: df[src] for field, src in pick.items()})

    for field in ("old_url", "new_url"):
        out[field] = out[field].where(out[field].notna(), "").astype(str).str.strip()
    has_target = out["old_url"].ne("") & out["new_url"].ne("")
    skipped = {"no_target": int((~has_target).sum())}
    out = out[has_target]

    # Non-numeric / empty scores coerce to NaN and drop out below.
    out["score"] = pd.to_numeric(out.get("score"), errors="coerce")
    scored = out["score"].notna()
    skipped["no_score"] = int((~scored).sum())
    out = out[scored]

    # Highest score first — the same order the Export xlsx uses, so a
    # truncated read keeps the rows most likely to be pushed. Dedupe after
    # sorting so a URL that appears twice keeps its best row (the redirect API
    # is keyed on fromUrl, so two rows for one old URL is one write anyway).
    out = out.sort_values("score", ascending=False, kind="mergesort")
    before = len(out)
    out = out.drop_duplicates(subset="old_url", keep="first")
    skipped["duplicate"] = before - len(out)

    out = out.astype(object).where(pd.notna(out), None)
    rows: list[dict] = []
    for rec in out.to_dict("records"):
        for field in _INT_FIELDS:
            v = rec.get(field)
            if isinstance(v, float):
                rec[field] = int(round(v))
        rev = rec.get("revenue")
        if isinstance(rev, float):
            rec["revenue"] = round(rev, 2)
        rec["tier"] = _tier(int(rec["score"]))
        rows.append(rec)

    histogram: dict[int, int] = {}
    for r in rows:
        histogram[r["score"]] = histogram.get(r["score"], 0) + 1

    return {
        "rows": rows,
        "skipped": skipped,
        # Descending, so the frontend can accumulate it into "≥ N" counts in
        # one pass.
        "histogram": [{"score": s, "count": histogram[s]}
                      for s in sorted(histogram, reverse=True)],
    }


def _load_parsed(task_id: str) -> Optional[dict]:
    """Parsed run output, memoized. None when the run has no stored bytes."""
    with _CACHE_LOCK:
        hit = _CACHE.get(task_id)
        if hit is not None:
            _CACHE.move_to_end(task_id)
            return hit

    from backend import rurl_optimizer_persistence as pers
    blob = pers.get_run_output(task_id)
    if not blob:
        return None
    filename, _mime, content = blob
    parsed = parse_rows(content)
    parsed["filename"] = filename

    with _CACHE_LOCK:
        _CACHE[task_id] = parsed
        _CACHE.move_to_end(task_id)
        while len(_CACHE) > _CACHE_MAX:
            _CACHE.popitem(last=False)
    return parsed


def invalidate(task_id: str) -> None:
    """Drop a run from the parse cache (called when its output is deleted)."""
    with _CACHE_LOCK:
        _CACHE.pop(task_id, None)


def run_results(task_id: str, min_score: int = 90,
                limit: int = DEFAULT_ROWS) -> Optional[dict]:
    """Rows of a run at or above `min_score`, plus the full score histogram.

    The histogram always covers the whole run, so the frontend can label its
    score selector with real counts before fetching the rows behind a
    threshold. `limit` caps the rows returned; the caller is told when that
    bit, because a selection can only ever cover the rows it was shown.
    """
    parsed = _load_parsed(task_id)
    if parsed is None:
        return None

    limit = max(1, min(int(limit), MAX_ROWS))
    min_score = max(0, min(int(min_score), 100))
    all_rows = parsed["rows"]
    matched = [r for r in all_rows if r["score"] >= min_score]
    rows = matched[:limit]

    return {
        "task_id": task_id,
        "filename": parsed.get("filename"),
        "total": len(all_rows),
        "min_score": min_score,
        "matched": len(matched),
        "returned": len(rows),
        "truncated": len(matched) > len(rows),
        "limit": limit,
        "histogram": parsed["histogram"],
        "skipped": parsed["skipped"],
        "rows": rows,
    }


# Cap on a combined export, so one careless select-all cannot pull every run
# ever stored into a single workbook.
MAX_EXPORT_RUNS = 50


def export_runs(task_ids: list[str]) -> Optional[tuple[str, str, bytes]]:
    """Return (filename, mime, bytes) for the selected runs, or None if none
    of them has stored output.

    One run streams its own stored file back byte for byte — the same thing the
    per-row Export used to hand you. Several are merged into one workbook with
    a leading `run` column, because rows from different runs are only useful if
    you can still tell which run they came from. v1 and v2 outputs have
    different headers, so the merge is an outer join: a column a run does not
    have stays empty for its rows rather than dropping the run.
    """
    from backend import rurl_optimizer_persistence as pers

    XLSX_MIME = ("application/vnd.openxmlformats-officedocument"
                 ".spreadsheetml.sheet")

    task_ids = list(dict.fromkeys(task_ids))[:MAX_EXPORT_RUNS]
    found: list[tuple[str, str, bytes]] = []
    for tid in task_ids:
        blob = pers.get_run_output(tid)
        if blob:
            filename, _mime, content = blob
            found.append((tid, filename, content))
    if not found:
        return None
    if len(found) == 1:
        _tid, filename, content = found[0]
        return filename, XLSX_MIME, content

    import pandas as pd

    frames = []
    for tid, _filename, content in found:
        try:
            df = pd.read_excel(io.BytesIO(content))
        except Exception as e:
            logger.warning(f"export_runs: unreadable output for {tid}: {e}")
            continue
        df.insert(0, "run", tid)
        frames.append(df)
    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True, sort=False)
    buf = io.BytesIO()
    merged.to_excel(buf, index=False)
    from datetime import datetime
    name = f"rurl_runs_{len(frames)}_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return name, XLSX_MIME, buf.getvalue()
