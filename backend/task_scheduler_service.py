"""
Task Scheduler Service — Windows Task Scheduler integration + database CRUD.

Manages scheduled tasks: create, update, delete, toggle, run manually.
Synchronises with Windows Task Scheduler via schtasks.exe.
"""
import subprocess
import threading
import logging
import re
from datetime import datetime
from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

TASK_PREFIX = "DM-Dashboard-"
SCHTASKS = r"C:\Windows\System32\schtasks.exe"

# In-memory tracking for manual runs (background threads)
_manual_runs = {}
_manual_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Windows Task Scheduler helpers
# ---------------------------------------------------------------------------

def _run_schtasks(args: list[str]) -> subprocess.CompletedProcess:
    """Run schtasks.exe with the given arguments."""
    cmd = [SCHTASKS] + args
    logger.info("schtasks: %s", " ".join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        logger.error("schtasks stderr: %s", result.stderr.strip())
    return result


def create_windows_task(win_task_name: str, command: str, working_dir: str,
                        schedule_type: str, schedule_time: str,
                        schedule_days: str | None = None) -> bool:
    """Create a task in Windows Task Scheduler."""
    # Wrap command with working directory via cmd /c
    if working_dir:
        tr_command = f'cmd /c "cd /d {working_dir} && {command}"'
    else:
        tr_command = command

    args = [
        "/create",
        "/tn", win_task_name,
        "/tr", tr_command,
        "/sc", schedule_type,
        "/st", schedule_time,
        "/f",  # force overwrite
    ]
    if schedule_type == "WEEKLY" and schedule_days:
        args.extend(["/d", schedule_days])

    result = _run_schtasks(args)
    if result.returncode != 0:
        logger.error("schtasks create failed: %s", result.stderr)
    return result.returncode == 0


def delete_windows_task(win_task_name: str) -> bool:
    """Delete a task from Windows Task Scheduler."""
    result = _run_schtasks(["/delete", "/tn", win_task_name, "/f"])
    return result.returncode == 0


def enable_windows_task(win_task_name: str) -> bool:
    result = _run_schtasks(["/change", "/tn", win_task_name, "/enable"])
    return result.returncode == 0


def disable_windows_task(win_task_name: str) -> bool:
    result = _run_schtasks(["/change", "/tn", win_task_name, "/disable"])
    return result.returncode == 0


def trigger_windows_task(win_task_name: str) -> bool:
    result = _run_schtasks(["/run", "/tn", win_task_name])
    return result.returncode == 0


def query_windows_task(win_task_name: str) -> dict | None:
    """Query a single task from Windows Task Scheduler. Returns parsed info."""
    result = _run_schtasks(["/query", "/tn", win_task_name, "/v", "/fo", "LIST"])
    if result.returncode != 0:
        return None
    return _parse_schtasks_list(result.stdout)


def query_all_dashboard_tasks() -> list[dict]:
    """Query all DM-Dashboard-* tasks from Windows Task Scheduler."""
    result = subprocess.run(
        [SCHTASKS, "/query", "/fo", "LIST", "/v"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        return []

    tasks = []
    current = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            if current and current.get("TaskName", "").startswith(f"\\{TASK_PREFIX}"):
                tasks.append(current)
            current = {}
            continue
        if ":" in line:
            key, _, val = line.partition(":")
            current[key.strip()] = val.strip()

    # Don't forget last block
    if current and current.get("TaskName", "").startswith(f"\\{TASK_PREFIX}"):
        tasks.append(current)

    return tasks


def _parse_schtasks_list(output: str) -> dict:
    """Parse schtasks /query /v /fo LIST output into a dictionary."""
    info = {}
    for line in output.splitlines():
        line = line.strip()
        if ":" in line:
            key, _, val = line.partition(":")
            # Re-join if value contained colons (e.g. time 07:00:00)
            info[key.strip()] = val.strip()
    return info


# ---------------------------------------------------------------------------
# Database CRUD
# ---------------------------------------------------------------------------

def list_tasks() -> list[dict]:
    """List all scheduled tasks from the database, enriched with Windows status."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*,
                   r.started_at AS last_run_at,
                   r.status AS last_run_status,
                   r.exit_code AS last_run_exit_code
            FROM pa.scheduled_tasks t
            LEFT JOIN LATERAL (
                SELECT started_at, status, exit_code
                FROM pa.scheduled_task_runs
                WHERE task_id = t.id
                ORDER BY started_at DESC
                LIMIT 1
            ) r ON true
            ORDER BY t.created_at
        """)
        rows = cur.fetchall()
        cur.close()

        tasks = []
        for row in rows:
            task = dict(row)
            # Convert time to string
            if task.get("schedule_time"):
                task["schedule_time"] = task["schedule_time"].strftime("%H:%M")
            # Enrich with Windows info
            win_info = query_windows_task(task["win_task_name"])
            if win_info:
                task["win_status"] = win_info.get("Status", "Unknown")
                task["next_run_time"] = win_info.get("Next Run Time", "N/A")
            else:
                task["win_status"] = "Not Found"
                task["next_run_time"] = "N/A"
            tasks.append(task)

        return tasks
    finally:
        return_db_connection(conn)


def get_task(task_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM pa.scheduled_tasks WHERE id = %s", (task_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            task = dict(row)
            if task.get("schedule_time"):
                task["schedule_time"] = task["schedule_time"].strftime("%H:%M")
            return task
        return None
    finally:
        return_db_connection(conn)


def create_task(data: dict) -> dict:
    """Create a scheduled task in DB and Windows Task Scheduler."""
    task_name = data["task_name"]
    win_task_name = f"{TASK_PREFIX}{task_name}"
    command = data["command"]
    working_dir = data.get("working_directory", r"C:\Users\l.davidowski\dm-dashboard")
    schedule_type = data.get("schedule_type", "DAILY")
    schedule_time = data.get("schedule_time", "07:00")
    schedule_days = data.get("schedule_days")

    # Create in Windows Task Scheduler
    success = create_windows_task(
        win_task_name, command, working_dir,
        schedule_type, schedule_time, schedule_days
    )
    if not success:
        raise RuntimeError(f"Failed to create Windows task '{win_task_name}'")

    # Insert into database
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pa.scheduled_tasks
                (task_name, display_name, description, command, working_directory,
                 schedule_type, schedule_time, schedule_days, is_enabled, win_task_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true, %s)
            RETURNING *
        """, (
            task_name,
            data.get("display_name", task_name),
            data.get("description", ""),
            command,
            working_dir,
            schedule_type,
            schedule_time,
            schedule_days,
            win_task_name,
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        task = dict(row)
        if task.get("schedule_time"):
            task["schedule_time"] = task["schedule_time"].strftime("%H:%M")
        return task
    except Exception:
        conn.rollback()
        # Rollback Windows task
        delete_windows_task(win_task_name)
        raise
    finally:
        return_db_connection(conn)


def update_task(task_id: int, data: dict) -> dict:
    """Update a scheduled task in DB and Windows Task Scheduler."""
    existing = get_task(task_id)
    if not existing:
        raise ValueError("Task not found")

    command = data.get("command", existing["command"])
    working_dir = data.get("working_directory", existing["working_directory"])
    schedule_type = data.get("schedule_type", existing["schedule_type"])
    schedule_time = data.get("schedule_time", existing["schedule_time"])
    schedule_days = data.get("schedule_days", existing.get("schedule_days"))

    # Recreate in Windows (schtasks has no true update)
    win_task_name = existing["win_task_name"]
    delete_windows_task(win_task_name)
    success = create_windows_task(
        win_task_name, command, working_dir,
        schedule_type, schedule_time, schedule_days
    )
    if not success:
        raise RuntimeError(f"Failed to update Windows task '{win_task_name}'")

    if not existing.get("is_enabled", True):
        disable_windows_task(win_task_name)

    # Update database
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE pa.scheduled_tasks SET
                display_name = %s, description = %s, command = %s,
                working_directory = %s, schedule_type = %s, schedule_time = %s,
                schedule_days = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING *
        """, (
            data.get("display_name", existing["display_name"]),
            data.get("description", existing.get("description", "")),
            command, working_dir, schedule_type, schedule_time, schedule_days,
            task_id,
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        task = dict(row)
        if task.get("schedule_time"):
            task["schedule_time"] = task["schedule_time"].strftime("%H:%M")
        return task
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def delete_task(task_id: int) -> bool:
    """Delete a scheduled task from DB and Windows Task Scheduler."""
    existing = get_task(task_id)
    if not existing:
        raise ValueError("Task not found")

    delete_windows_task(existing["win_task_name"])

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pa.scheduled_tasks WHERE id = %s", (task_id,))
        conn.commit()
        cur.close()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def toggle_task(task_id: int) -> dict:
    """Toggle a task between enabled and disabled."""
    existing = get_task(task_id)
    if not existing:
        raise ValueError("Task not found")

    new_state = not existing["is_enabled"]

    if new_state:
        enable_windows_task(existing["win_task_name"])
    else:
        disable_windows_task(existing["win_task_name"])

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE pa.scheduled_tasks
            SET is_enabled = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING *
        """, (new_state, task_id))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        task = dict(row)
        if task.get("schedule_time"):
            task["schedule_time"] = task["schedule_time"].strftime("%H:%M")
        return task
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Manual run
# ---------------------------------------------------------------------------

def run_task_manually(task_id: int) -> int:
    """Run a task manually in a background thread. Returns run_id."""
    existing = get_task(task_id)
    if not existing:
        raise ValueError("Task not found")

    # Create run record
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pa.scheduled_task_runs (task_id, trigger_type, status)
            VALUES (%s, 'manual', 'running')
            RETURNING id
        """, (task_id,))
        run_id = cur.fetchone()["id"]
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)

    # Start background thread
    thread = threading.Thread(
        target=_execute_task,
        args=(run_id, existing["command"], existing["working_directory"]),
        daemon=True
    )
    thread.start()

    with _manual_lock:
        _manual_runs[run_id] = {"thread": thread, "task_id": task_id}

    return run_id


def _execute_task(run_id: int, command: str, working_dir: str):
    """Execute a task command and capture output."""
    try:
        logger.info("Manual run %d: executing %s in %s", run_id, command, working_dir)
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=working_dir,
            timeout=72 * 3600,  # 72 hour max
        )

        output = result.stdout[-50000:] if result.stdout else ""  # last 50K chars
        if result.stderr:
            output += "\n--- STDERR ---\n" + result.stderr[-10000:]

        status = "completed" if result.returncode == 0 else "failed"
        error_msg = result.stderr[:2000] if result.returncode != 0 else None

        _update_run(run_id, status, result.returncode, output, error_msg)
        logger.info("Manual run %d: %s (exit %d)", run_id, status, result.returncode)

    except subprocess.TimeoutExpired:
        _update_run(run_id, "failed", -1, "", "Timeout: task exceeded maximum runtime")
        logger.error("Manual run %d: timeout", run_id)
    except Exception as e:
        _update_run(run_id, "failed", -1, "", str(e))
        logger.error("Manual run %d: error %s", run_id, e)


def _update_run(run_id: int, status: str, exit_code: int,
                output: str, error_msg: str | None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE pa.scheduled_task_runs
            SET status = %s, exit_code = %s, output_log = %s,
                error_message = %s, completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (status, exit_code, output, error_msg, run_id))
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------

def get_task_runs(task_id: int, limit: int = 20) -> list[dict]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, task_id, started_at, completed_at, exit_code,
                   status, trigger_type, error_message
            FROM pa.scheduled_task_runs
            WHERE task_id = %s
            ORDER BY started_at DESC
            LIMIT %s
        """, (task_id, limit))
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    finally:
        return_db_connection(conn)


def get_run_log(run_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, task_id, started_at, completed_at, exit_code,
                   status, trigger_type, error_message, output_log
            FROM pa.scheduled_task_runs
            WHERE id = %s
        """, (run_id,))
        row = cur.fetchone()
        cur.close()
        return dict(row) if row else None
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Import existing tasks
# ---------------------------------------------------------------------------

def import_existing_tasks() -> list[dict]:
    """Import DM-Dashboard-* tasks from Windows into the database."""
    win_tasks = query_all_dashboard_tasks()
    imported = []

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        for wt in win_tasks:
            raw_name = wt.get("TaskName", "").lstrip("\\")
            if not raw_name.startswith(TASK_PREFIX):
                continue

            # Check if already in DB
            cur.execute(
                "SELECT id FROM pa.scheduled_tasks WHERE win_task_name = %s",
                (raw_name,)
            )
            if cur.fetchone():
                continue  # already imported

            task_name = raw_name.replace(TASK_PREFIX, "").lower().replace(" ", "-")
            command = wt.get("Task To Run", "")
            schedule_time = "07:00"

            # Try to parse time from "Start Time"
            start_time_raw = wt.get("Start Time", "")
            time_match = re.search(r"(\d{2}):(\d{2})", start_time_raw)
            if time_match:
                schedule_time = f"{time_match.group(1)}:{time_match.group(2)}"

            # Determine schedule type from "Schedule Type"
            sched_type_raw = wt.get("Schedule Type", "Daily").lower()
            if "week" in sched_type_raw:
                schedule_type = "WEEKLY"
            elif "hour" in sched_type_raw or "minut" in sched_type_raw:
                schedule_type = "HOURLY"
            else:
                schedule_type = "DAILY"

            status_raw = wt.get("Status", "").lower()
            is_enabled = "disabled" not in status_raw

            cur.execute("""
                INSERT INTO pa.scheduled_tasks
                    (task_name, display_name, description, command, working_directory,
                     schedule_type, schedule_time, is_enabled, win_task_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (
                task_name,
                raw_name.replace(TASK_PREFIX, "").replace("-", " ").title(),
                f"Imported from Windows Task Scheduler",
                command,
                r"C:\Users\l.davidowski\dm-dashboard",
                schedule_type,
                schedule_time,
                is_enabled,
                raw_name,
            ))
            row = cur.fetchone()
            imported.append(dict(row))

        conn.commit()
        cur.close()
        return imported
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)
