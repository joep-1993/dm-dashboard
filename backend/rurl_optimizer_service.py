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


def start_optimize(
    csv_bytes: bytes,
    filename: str,
    workers: Optional[int],
    threshold: int,
    multi_facet: bool,
    url_column: str,
    also_global: bool,
) -> str:
    task_id = uuid.uuid4().hex[:8]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    input_path = INPUT_DIR / f"input_{task_id}_{filename}"
    input_path.write_bytes(csv_bytes)

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
        },
    })

    def _runner():
        _run_subprocess(task_id, argv, output_path, script="main_parallel_v2")
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
