"""
Task Scheduler Router — CRUD endpoints for managing scheduled tasks.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.task_scheduler_service import (
    list_tasks,
    get_task,
    create_task,
    update_task,
    delete_task,
    toggle_task,
    run_task_manually,
    get_task_runs,
    get_run_log,
    import_existing_tasks,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/task-scheduler", tags=["task-scheduler"])
executor = ThreadPoolExecutor(max_workers=2)


class TaskCreate(BaseModel):
    task_name: str
    display_name: str
    description: Optional[str] = ""
    command: str
    working_directory: Optional[str] = r"C:\Users\l.davidowski\dm-dashboard"
    schedule_type: Optional[str] = "DAILY"
    schedule_time: Optional[str] = "07:00"
    schedule_days: Optional[str] = None


class TaskUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    command: Optional[str] = None
    working_directory: Optional[str] = None
    schedule_type: Optional[str] = None
    schedule_time: Optional[str] = None
    schedule_days: Optional[str] = None


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "task_scheduler"}


@router.get("/tasks")
async def get_tasks():
    """List all scheduled tasks with live Windows status."""
    try:
        loop = asyncio.get_event_loop()
        tasks = await loop.run_in_executor(executor, list_tasks)
        return {"tasks": tasks}
    except Exception as e:
        logger.error("Error listing tasks: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}")
async def get_task_detail(task_id: int):
    """Get a single task by ID."""
    try:
        loop = asyncio.get_event_loop()
        task = await loop.run_in_executor(executor, get_task, task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks")
async def create_task_endpoint(body: TaskCreate):
    """Create a new scheduled task (DB + Windows Task Scheduler)."""
    try:
        loop = asyncio.get_event_loop()
        task = await loop.run_in_executor(executor, create_task, body.dict())
        return task
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error("Error creating task: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}")
async def update_task_endpoint(task_id: int, body: TaskUpdate):
    """Update an existing scheduled task."""
    try:
        data = {k: v for k, v in body.dict().items() if v is not None}
        loop = asyncio.get_event_loop()
        task = await loop.run_in_executor(executor, update_task, task_id, data)
        return task
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error("Error updating task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}")
async def delete_task_endpoint(task_id: int):
    """Delete a scheduled task (DB + Windows)."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, delete_task, task_id)
        return {"status": "deleted", "task_id": task_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error deleting task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/toggle")
async def toggle_task_endpoint(task_id: int):
    """Enable or disable a scheduled task."""
    try:
        loop = asyncio.get_event_loop()
        task = await loop.run_in_executor(executor, toggle_task, task_id)
        return task
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error toggling task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/run")
async def run_task_endpoint(task_id: int):
    """Trigger a manual run of a scheduled task."""
    try:
        loop = asyncio.get_event_loop()
        run_id = await loop.run_in_executor(executor, run_task_manually, task_id)
        return {"status": "started", "run_id": run_id, "task_id": task_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Error running task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/runs")
async def get_task_runs_endpoint(task_id: int):
    """Get execution history for a task."""
    try:
        loop = asyncio.get_event_loop()
        runs = await loop.run_in_executor(executor, get_task_runs, task_id)
        return {"runs": runs}
    except Exception as e:
        logger.error("Error getting runs for task %d: %s", task_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}/log")
async def get_run_log_endpoint(run_id: int):
    """Get output log for a specific run."""
    try:
        loop = asyncio.get_event_loop()
        run = await loop.run_in_executor(executor, get_run_log, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return run
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting log for run %d: %s", run_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import-existing")
async def import_existing_endpoint():
    """Import existing DM-Dashboard-* tasks from Windows Task Scheduler."""
    try:
        loop = asyncio.get_event_loop()
        imported = await loop.run_in_executor(executor, import_existing_tasks)
        return {"imported": len(imported), "tasks": imported}
    except Exception as e:
        logger.error("Error importing tasks: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
