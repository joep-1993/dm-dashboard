"""One place that knows whether the OpenAI key still has credits.

WHY THIS EXISTS. On 2026-07-31 the key ran out of credits. Every call returned
``429 insufficient_quota / credit_balance_exhausted`` — and nothing stopped, nothing said
so. The AI-titles v3 pipeline catches a failing polish call and falls back to its
deterministic composed H1, so batches kept "succeeding" while quietly producing unpolished
titles ("Hardhout Potdekselplanken" instead of "Hardhouten Potdekselplanken"). The only
way to find out was to read a log line. Joep: signal it in the UI and stop the processes
that depend on it.

HOW IT WORKS. ``install()`` wraps the OpenAI SDK's completion call ONCE, at the class
level, so every client instance in every module goes through it — there are ~14 call sites
across 8 modules and wrapping each one would have left the next new call site unguarded.
A quota error flips a flag in Postgres (shared, so uvicorn, the batch workers and the UI
all see the same state, and it survives a restart); the first successful call clears it.

WHAT CALLERS DO. Generation entry points call ``raise_if_blocked()`` (or check
``status()``) and refuse to start; worker loops check ``is_blocked()`` per item and stop
cleanly, leaving the remaining jobs pending rather than burning them as failed.
"""
import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

FLAG = "openai_quota_exhausted"

# The flag lives in Postgres so every process agrees, but a worker loop checks it once per
# URL — cache it briefly so that costs nothing. Short enough that "credits topped up, press
# clear" feels immediate.
_CACHE_TTL_SEC = 5.0
_cache: Dict[str, Any] = {"at": 0.0, "value": None}
_lock = threading.Lock()
_installed = False


class OpenAIQuotaExhausted(RuntimeError):
    """Raised by raise_if_blocked() so callers can answer 409 instead of 500."""


def _ensure_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.system_flags (
            flag       TEXT PRIMARY KEY,
            detail     JSONB,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
    """)


def _quota_message(exc: Exception) -> Optional[str]:
    """The API's own wording when the key is out of credits, else None.

    Matches on the error CODE/type rather than the prose: OpenAI has reworded this message
    before ("You exceeded your current quota" -> "You have no credits remaining"), but
    `insufficient_quota` / `credit_balance_exhausted` have been stable. A plain rate limit
    (429 without those codes) is NOT this — retrying fixes that one, and blocking the whole
    dashboard over a burst would be worse than the bug this guards.
    """
    blob = ""
    for attr in ("code", "type"):
        blob += f" {getattr(exc, attr, '') or ''}"
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        err = body.get("error") or {}
        blob += f" {err.get('code', '')} {err.get('type', '')}"
    blob += f" {exc}"
    low = blob.lower()
    if "insufficient_quota" in low or "credit_balance_exhausted" in low or \
            "no credits remaining" in low or "exceeded your current quota" in low:
        return str(exc)[:500]
    return None


def status(force: bool = False) -> Dict[str, Any]:
    """{'blocked': bool, 'since': iso|None, 'message': str|None}. Never raises."""
    with _lock:
        if not force and _cache["value"] is not None and (time.time() - _cache["at"]) < _CACHE_TTL_SEC:
            return dict(_cache["value"])
    out: Dict[str, Any] = {"blocked": False, "since": None, "message": None}
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        _ensure_table(cur)
        conn.commit()
        cur.execute("SELECT detail, updated_at FROM pa.system_flags WHERE flag = %s", (FLAG,))
        row = cur.fetchone()
        if row:
            detail = row["detail"] or {}
            if isinstance(detail, str):
                detail = json.loads(detail)
            out = {
                "blocked": True,
                "since": (detail.get("since") or row["updated_at"].isoformat()),
                "message": detail.get("message"),
            }
        cur.close()
    except Exception as ex:
        # A DB hiccup must not make the dashboard think it is blocked.
        logger.warning("openai_guard: status read failed (%s) — reporting not blocked", ex)
    finally:
        if conn is not None:
            return_db_connection(conn)
    with _lock:
        _cache["at"] = time.time()
        _cache["value"] = dict(out)
    return dict(out)


def is_blocked() -> bool:
    return bool(status().get("blocked"))


def raise_if_blocked(what: str = "This action") -> None:
    st = status()
    if st.get("blocked"):
        raise OpenAIQuotaExhausted(
            f"{what} needs the OpenAI API, and the key has no credits "
            f"(since {st.get('since')}). Top up the balance, then press "
            f"\"I topped up\" in the banner (or POST /api/system/openai-status/clear)."
        )


def mark_blocked(message: str) -> None:
    """Record that the key is out of credits. Idempotent — keeps the FIRST timestamp."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        _ensure_table(cur)
        detail = json.dumps({"since": datetime.now().astimezone().isoformat(),
                             "message": message[:500]})
        cur.execute("""
            INSERT INTO pa.system_flags (flag, detail, updated_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (flag) DO NOTHING
        """, (FLAG, detail))
        conn.commit()
        cur.close()
        logger.error("OPENAI QUOTA EXHAUSTED — AI generation blocked: %s", message[:300])
    except Exception as ex:
        logger.error("openai_guard: could not persist the block flag: %s", ex)
    finally:
        if conn is not None:
            return_db_connection(conn)
    with _lock:
        _cache["at"] = 0.0
        _cache["value"] = None


def clear() -> Dict[str, Any]:
    """Credits are back (or a call just succeeded). Removes the flag."""
    conn = None
    removed = 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        _ensure_table(cur)
        cur.execute("DELETE FROM pa.system_flags WHERE flag = %s", (FLAG,))
        removed = cur.rowcount
        conn.commit()
        cur.close()
        if removed:
            logger.info("openai_guard: quota flag cleared — AI generation unblocked")
    except Exception as ex:
        logger.error("openai_guard: could not clear the block flag: %s", ex)
    finally:
        if conn is not None:
            return_db_connection(conn)
    with _lock:
        _cache["at"] = 0.0
        _cache["value"] = None
    return {"cleared": bool(removed)}


def install() -> bool:
    """Wrap the SDK's completion call so every module is covered. Safe to call twice."""
    global _installed
    if _installed:
        return True
    try:
        from openai.resources.chat.completions import Completions
    except Exception as ex:                                    # SDK missing/renamed
        logger.warning("openai_guard: cannot install (%s); flag will stay manual", ex)
        return False

    # The Batch API path (batch_api_service) never touches chat.completions — its quota
    # errors surface on batches.create — so both entry points get the same treatment.
    try:
        from openai.resources.batches import Batches
    except Exception:
        Batches = None

    original = Completions.create

    def guarded(self, *args, **kwargs):
        try:
            result = original(self, *args, **kwargs)
        except Exception as exc:
            msg = _quota_message(exc)
            if msg:
                mark_blocked(msg)
            raise
        # A success means credits are available again. Only touch the DB when the flag is
        # actually set, so the happy path costs one cached read.
        try:
            if is_blocked():
                clear()
        except Exception:
            pass
        return result

    guarded.__wrapped__ = original                             # so it is recognisably a wrapper
    Completions.create = guarded

    hooked = ["chat.completions.create"]
    if Batches is not None:
        batch_original = Batches.create

        def guarded_batch(self, *args, **kwargs):
            try:
                result = batch_original(self, *args, **kwargs)
            except Exception as exc:
                msg = _quota_message(exc)
                if msg:
                    mark_blocked(msg)
                raise
            try:
                if is_blocked():
                    clear()
            except Exception:
                pass
            return result

        guarded_batch.__wrapped__ = batch_original
        Batches.create = guarded_batch
        hooked.append("batches.create")

    _installed = True
    logger.info("openai_guard installed on %s", " + ".join(hooked))
    return True
