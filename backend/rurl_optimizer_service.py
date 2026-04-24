"""
Background-task wrapper for the bundled R-URL Optimizer.

The optimizer (backend/rurl_optimizer/) ships as a self-contained CLI. We run
it as a subprocess per task and tail stdout into an in-memory task dict so the
dashboard can poll progress, stream logs, and download the resulting CSV.
"""
from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
import sys
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

PKG_DIR = Path(__file__).parent / "rurl_optimizer"
OUTPUT_DIR = Path("/tmp/rurl-optimizer-output")
INPUT_DIR = Path("/tmp/rurl-optimizer-input")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
INPUT_DIR.mkdir(parents=True, exist_ok=True)

_TASKS: Dict[str, Dict[str, Any]] = {}
_TASKS_LOCK = threading.Lock()
_HISTORY: deque = deque(maxlen=50)

# tqdm writes lines like "Processing:  42%|████▏     | 4200/10000 [00:15<00:20, ...]"
_TQDM_RE = re.compile(r"(?P<pct>\d+)%\|.*?\|\s*(?P<cur>\d+)/(?P<tot>\d+)")


def _set(task_id: str, patch: Dict[str, Any]) -> None:
    with _TASKS_LOCK:
        t = _TASKS.setdefault(task_id, {})
        t.update(patch)


def _get(task_id: str) -> Optional[Dict[str, Any]]:
    with _TASKS_LOCK:
        return dict(_TASKS[task_id]) if task_id in _TASKS else None


def _append_log(task_id: str, line: str) -> None:
    with _TASKS_LOCK:
        t = _TASKS.setdefault(task_id, {})
        log = t.setdefault("log", [])
        log.append(line)
        # cap log to last 500 lines
        if len(log) > 500:
            del log[: len(log) - 500]


def _run_subprocess(task_id: str, argv: list[str], output_path: Path, script: str) -> None:
    _set(task_id, {
        "status": "running",
        "progress": 0,
        "message": "Starting...",
        "started_at": datetime.now().isoformat(),
        "output_path": str(output_path),
        "script": script,
    })
    _append_log(task_id, f"$ {' '.join(shlex.quote(a) for a in argv)}")

    env = os.environ.copy()
    # Ensure the bundled package can import its sibling modules (src/, config).
    env["PYTHONPATH"] = str(PKG_DIR) + os.pathsep + env.get("PYTHONPATH", "")

    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(PKG_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as e:
        _set(task_id, {"status": "failed", "error": f"Failed to start: {e}"})
        return

    _set(task_id, {"pid": proc.pid})

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.rstrip()
        if not line:
            continue
        _append_log(task_id, line)

        m = _TQDM_RE.search(line)
        if m:
            pct = int(m.group("pct"))
            cur = int(m.group("cur"))
            tot = int(m.group("tot"))
            _set(task_id, {
                "progress": pct,
                "message": f"Processing {cur:,}/{tot:,} URLs",
            })
        elif line.startswith("Pre-loading") or "Loading " in line:
            _set(task_id, {"progress": 5, "message": "Loading data..."})
        elif "Data loaded" in line or "Data cached" in line:
            _set(task_id, {"progress": 15, "message": "Data ready"})
        elif line.startswith("Saving to"):
            _set(task_id, {"progress": 98, "message": "Saving output..."})

        # check if task was cancelled
        if _get(task_id) and _get(task_id).get("cancel_requested"):
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            _set(task_id, {"status": "cancelled", "message": "Cancelled by user"})
            _history_append(task_id)
            return

    rc = proc.wait()
    if rc == 0 and output_path.exists():
        _set(task_id, {
            "status": "completed",
            "progress": 100,
            "message": f"Done. Output: {output_path.name}",
            "finished_at": datetime.now().isoformat(),
        })
    else:
        _set(task_id, {
            "status": "failed",
            "error": f"Exit code {rc}",
            "finished_at": datetime.now().isoformat(),
        })
    _history_append(task_id)


def _history_append(task_id: str) -> None:
    t = _get(task_id)
    if not t:
        return
    _HISTORY.appendleft({
        "task_id": task_id,
        "script": t.get("script"),
        "status": t.get("status"),
        "started_at": t.get("started_at"),
        "finished_at": t.get("finished_at"),
        "message": t.get("message"),
        "error": t.get("error"),
        "output_path": t.get("output_path"),
        "params": t.get("params"),
    })


def _normalize_upload(csv_bytes: bytes) -> bytes:
    """
    Ensure the uploaded CSV has a column named 'r_url'. If the file has a
    single column with any other name, rename it. If it has multiple columns,
    leave it untouched (the optimizer expects an 'r_url' column then, which
    the Redshift path already produces).
    """
    import io
    import pandas as pd
    try:
        df = pd.read_csv(io.BytesIO(csv_bytes))
    except Exception:
        return csv_bytes
    if "r_url" in df.columns:
        return csv_bytes
    if len(df.columns) == 1:
        df.columns = ["r_url"]
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")
    return csv_bytes


def _read_url_column(csv_path: Path, url_column: str) -> list[str]:
    """Read only the URL column from a CSV. Returns empty list on any failure."""
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, usecols=[url_column])
        return df[url_column].dropna().astype(str).tolist()
    except Exception:
        return []


def _filter_input_csv(csv_path: Path, url_column: str, skip_urls: set[str]) -> None:
    """Rewrite CSV in place keeping only rows whose URL is NOT in skip_urls."""
    if not skip_urls:
        return
    import pandas as pd
    df = pd.read_csv(csv_path)
    if url_column not in df.columns:
        return
    mask = ~df[url_column].astype(str).isin(skip_urls)
    df[mask].to_csv(csv_path, index=False)


def _fetch_redshift_rurls(lookback_days: int = 365, row_limit: Optional[int] = None) -> bytes:
    """Pull R-URLs with visits + revenue from Redshift for the last N days."""
    from backend.database import get_redshift_connection
    import csv
    import io

    today = datetime.now().strftime("%Y%m%d")
    from datetime import timedelta
    start = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")

    sql = f"""
    SELECT
        dv.main_cat_name AS maincat,
        dv.deepest_subcat_name AS deepest_cat,
        SPLIT_PART(dv.url, '?', 1) AS r_url,
        count(*) AS visits,
        sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) AS visit_rev
    FROM datamart.fct_visits fcv
    JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
    JOIN datamart.dim_date dat ON fcv.dim_date_key = dat.dim_date_key
    JOIN chan_deriv.ref_channel_derivation_stats chan
      ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
    WHERE dv.is_real_visit = 1
      AND fcv.dim_date_key BETWEEN {start} AND {today}
      AND dv.url LIKE '%beslist.nl%'
      AND dv.url LIKE '%/r/%'
    GROUP BY 1, 2, 3
    ORDER BY visits DESC
    """
    if row_limit and row_limit > 0:
        sql += f"\n    LIMIT {int(row_limit)}"

    conn = get_redshift_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["maincat", "deepest_cat", "r_url", "visits", "visit_rev"])
    for r in rows:
        # cursor returns RealDictRow — access by key
        w.writerow([r["maincat"], r["deepest_cat"], r["r_url"], r["visits"], r["visit_rev"]])
    return buf.getvalue().encode("utf-8")


def start_optimize(
    csv_bytes: Optional[bytes],
    filename: Optional[str],
    workers: Optional[int],
    threshold: int,
    multi_facet: bool,
    url_column: str,
    also_global: bool,
    source: str = "upload",
    lookback_days: int = 365,
    row_limit: Optional[int] = None,
    force_reprocess: bool = False,
) -> str:
    task_id = uuid.uuid4().hex[:8]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if source == "redshift":
        filename = f"redshift_{lookback_days}d.csv"
        # Defer the fetch to the worker thread — it can take 30-60s.
        input_path = INPUT_DIR / f"input_{task_id}_{filename}"
    else:
        if not csv_bytes:
            raise ValueError("csv_bytes required when source=upload")
        input_path = INPUT_DIR / f"input_{task_id}_{filename}"
        input_path.write_bytes(_normalize_upload(csv_bytes))

    output_path = OUTPUT_DIR / f"redirects_{task_id}_{ts}.csv"

    argv = [
        sys.executable,
        "main_parallel_v2.py",
        str(input_path),
        "-o", str(output_path),
        "-c", url_column,
        "--threshold", str(threshold),
    ]
    if workers:
        argv += ["-w", str(workers)]
    if multi_facet:
        argv.append("--multi-facet")

    _set(task_id, {
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "log": [],
        "params": {
            "filename": filename,
            "workers": workers,
            "threshold": threshold,
            "multi_facet": multi_facet,
            "url_column": url_column,
            "also_global": also_global,
            "source": source,
            "lookback_days": lookback_days if source == "redshift" else None,
            "row_limit": row_limit if source == "redshift" else None,
            "force_reprocess": force_reprocess,
        },
    })

    def _runner():
        if source == "redshift":
            _set(task_id, {"status": "running", "progress": 1,
                           "message": f"Querying Redshift (last {lookback_days} days)..."})
            _append_log(task_id,
                        f"Fetching R-URLs from Redshift (last {lookback_days} days"
                        + (f", limit {row_limit}" if row_limit else "") + ")...")
            try:
                data = _fetch_redshift_rurls(lookback_days, row_limit)
                input_path.write_bytes(data)
                nrows = data.count(b"\n") - 1
                _append_log(task_id, f"Redshift returned {nrows:,} rows -> {input_path.name}")
            except Exception as e:
                _set(task_id, {"status": "failed", "error": f"Redshift fetch failed: {e}",
                               "finished_at": datetime.now().isoformat()})
                _history_append(task_id)
                return

        # Persistence: filter out URLs already processed (unless forced).
        all_input_urls = _read_url_column(input_path, url_column)
        cached_urls: set[str] = set()
        if not force_reprocess and all_input_urls:
            try:
                from backend import rurl_optimizer_persistence as pers
                cached_urls = pers.already_processed(all_input_urls)
            except Exception as e:
                _append_log(task_id, f"[warn] persistence lookup failed: {e} — processing all URLs")
                cached_urls = set()
        if cached_urls:
            _append_log(task_id,
                        f"Persistence: {len(cached_urls):,} of {len(all_input_urls):,} URLs "
                        f"already processed — skipping them.")
            _filter_input_csv(input_path, url_column, cached_urls)

        # Short-circuit: every URL is cached — write output directly from the cache.
        if cached_urls and len(cached_urls) >= len(all_input_urls):
            try:
                from backend import rurl_optimizer_persistence as pers
                prev_df = pers.load_previous(list(cached_urls))
                prev_df.to_csv(output_path, index=False)
                _set(task_id, {
                    "status": "completed", "progress": 100,
                    "message": f"All {len(cached_urls):,} URLs served from cache",
                    "started_at": datetime.now().isoformat(),
                    "finished_at": datetime.now().isoformat(),
                    "output_path": str(output_path), "script": "cache_only",
                })
                _append_log(task_id, f"Wrote {len(prev_df):,} cached rows to {output_path.name}")
                _history_append(task_id)
            except Exception as e:
                _set(task_id, {"status": "failed", "error": f"cache-only write failed: {e}",
                               "finished_at": datetime.now().isoformat()})
                _history_append(task_id)
            return

        _run_subprocess(task_id, argv, output_path, script="main_parallel_v2")

        # After a successful subprocess run, upsert + combine with cached rows.
        if (_get(task_id) or {}).get("status") == "completed" and output_path.exists():
            try:
                from backend import rurl_optimizer_persistence as pers
                import pandas as pd
                fresh_df = pd.read_csv(output_path)
                n_up = pers.upsert_results(fresh_df)
                _append_log(task_id, f"Persistence: upserted {n_up:,} rows to rurl_processed.")
                if cached_urls:
                    prev_df = pers.load_previous(list(cached_urls))
                    # Align columns — prev_df has only the 5 DB cols; pad missing with NA.
                    for col in fresh_df.columns:
                        if col not in prev_df.columns:
                            prev_df[col] = pd.NA
                    prev_df = prev_df[fresh_df.columns.tolist()]
                    combined = pd.concat([fresh_df, prev_df], ignore_index=True)
                    combined.to_csv(output_path, index=False)
                    _append_log(task_id,
                                f"Persistence: combined output = {len(fresh_df):,} fresh + "
                                f"{len(prev_df):,} cached = {len(combined):,} rows.")
            except Exception as e:
                _append_log(task_id, f"[warn] persistence post-step failed: {e}")
        if also_global and (_get(task_id) or {}).get("status") == "completed":
            # Chain the global R-URL pass. It reads INPUT_FILE from its own
            # module constant, so we run a tiny inline wrapper instead of
            # editing the script.
            global_out = OUTPUT_DIR / f"redirects_global_{task_id}_{ts}.csv"
            argv2 = [
                sys.executable, "-c",
                (
                    "import sys; sys.path.insert(0, '.'); "
                    "import process_global_rurls as g; "
                    f"g.INPUT_FILE = {str(output_path)!r}; "
                    f"g.OUTPUT_FILE = {str(global_out)!r}; "
                    "g.main()"
                ),
            ]
            _append_log(task_id, "--- Running global R-URL pass ---")
            _run_subprocess(task_id, argv2, global_out, script="process_global_rurls")

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def get_status(task_id: str) -> Optional[Dict[str, Any]]:
    return _get(task_id)


def cancel(task_id: str) -> bool:
    t = _get(task_id)
    if not t or t.get("status") not in ("queued", "running"):
        return False
    _set(task_id, {"cancel_requested": True})
    return True


def get_output_path(task_id: str) -> Optional[str]:
    t = _get(task_id)
    if not t:
        return None
    return t.get("output_path")


def get_history() -> list:
    return list(_HISTORY)
