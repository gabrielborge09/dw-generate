from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import AppConfig, load_config
from .flows import run_full_load_flow, run_incremental_load_flow
from .runtime_log import BRASILIA_TZ, append_runtime_log, read_runtime_logs
from .scheduler import (
    add_scheduler_job,
    delete_scheduler_job,
    init_scheduler_store,
    list_scheduler_jobs,
    list_scheduler_runs,
    run_due_jobs_once,
    set_scheduler_job_enabled,
    trigger_scheduler_job,
)


class SchedulerStartRequest(BaseModel):
    poll_seconds: Optional[int] = Field(default=None, ge=1)
    max_jobs_per_cycle: Optional[int] = Field(default=None, ge=1)


class ForcedRunRequest(BaseModel):
    rows: Optional[int] = Field(default=None, ge=1)
    continue_on_error: bool = False
    skip_validation: bool = False


class JobUpsertRequest(BaseModel):
    name: str = Field(min_length=1)
    mode: str
    schedule_type: str
    interval_unit: Optional[str] = None
    interval_value: Optional[int] = Field(default=None, ge=1)
    daily_at: Optional[str] = None
    daily_repeat: bool = True
    rows: Optional[int] = Field(default=None, ge=1)
    continue_on_error: bool = False
    skip_validation: bool = False
    enabled: bool = True
    replace: bool = False


@dataclass
class _SchedulerLoopState:
    running: bool = False
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    poll_seconds: int = 30
    max_jobs_per_cycle: Optional[int] = None
    last_cycle: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None


class SchedulerApiRuntime:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._state = _SchedulerLoopState(poll_seconds=int(config.scheduler.poll_interval_seconds))
        self._state_lock = threading.Lock()
        self._scheduler_stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._execution_lock = threading.Lock()

        self._manual_tasks_lock = threading.Lock()
        self._manual_tasks: Dict[str, Dict[str, Any]] = {}
        self._current_task_id: Optional[str] = None

        init_scheduler_store(config)
        self._ensure_manual_runs_table()

    def status(self) -> Dict[str, Any]:
        with self._state_lock:
            scheduler_state = {
                "running": self._state.running,
                "started_at": self._state.started_at,
                "stopped_at": self._state.stopped_at,
                "poll_seconds": self._state.poll_seconds,
                "max_jobs_per_cycle": self._state.max_jobs_per_cycle,
                "last_cycle": self._state.last_cycle,
                "last_error": self._state.last_error,
            }
        with self._manual_tasks_lock:
            current_task = self._manual_tasks.get(self._current_task_id) if self._current_task_id else None
        return {
            "server_time_utc": _utc_iso(),
            "server_time_brt": _to_brt_iso(datetime.now(tz=timezone.utc)),
            "scheduler": scheduler_state,
            "execution": {
                "busy": self._execution_lock.locked(),
                "current_task": current_task,
            },
        }

    def start_scheduler(
        self, poll_seconds: Optional[int] = None, max_jobs_per_cycle: Optional[int] = None
    ) -> Dict[str, Any]:
        already_running = False
        with self._state_lock:
            if self._state.running:
                already_running = True
            else:
                self._scheduler_stop_event.clear()
                self._state.running = True
                self._state.started_at = _utc_iso()
                self._state.stopped_at = None
                if poll_seconds is not None:
                    self._state.poll_seconds = int(poll_seconds)
                self._state.max_jobs_per_cycle = (
                    int(max_jobs_per_cycle) if max_jobs_per_cycle is not None else None
                )
                self._state.last_error = None

                self._scheduler_thread = threading.Thread(
                    target=self._scheduler_loop,
                    name="dw-generate-scheduler-loop",
                    daemon=True,
                )
                self._scheduler_thread.start()
        if already_running:
            return self.status()
        return self.status()

    def stop_scheduler(self, timeout_seconds: int = 10) -> Dict[str, Any]:
        with self._state_lock:
            thread = self._scheduler_thread
            running = self._state.running
            if running:
                self._scheduler_stop_event.set()

        if thread and running:
            thread.join(timeout=timeout_seconds)

        with self._state_lock:
            if not self._scheduler_thread or not self._scheduler_thread.is_alive():
                self._state.running = False
                self._state.stopped_at = _utc_iso()
                self._scheduler_thread = None
        return self.status()

    def run_due_once(self, max_jobs: Optional[int] = None) -> Dict[str, Any]:
        if not self._execution_lock.acquire(blocking=False):
            raise RuntimeError("Existe uma execucao em andamento. Tente novamente.")
        try:
            result = run_due_jobs_once(self.config, max_jobs=max_jobs)
            with self._state_lock:
                self._state.last_cycle = result
                self._state.last_error = None
            return result
        finally:
            self._execution_lock.release()

    def trigger_job(self, job_ref: str) -> Dict[str, Any]:
        if not self._execution_lock.acquire(blocking=False):
            raise RuntimeError("Existe uma execucao em andamento. Tente novamente.")
        try:
            return trigger_scheduler_job(self.config, job_ref=job_ref)
        finally:
            self._execution_lock.release()

    def trigger_full(self, request: ForcedRunRequest) -> Dict[str, Any]:
        return self._start_manual_task(mode="full", request=request)

    def trigger_incremental(self, request: ForcedRunRequest) -> Dict[str, Any]:
        return self._start_manual_task(mode="incremental", request=request)

    def get_manual_task(self, task_id: str) -> Dict[str, Any]:
        with self._manual_tasks_lock:
            task = self._manual_tasks.get(task_id)
            if not task:
                raise ValueError(f"Task nao encontrada: {task_id}")
            return task

    def list_manual_runs(self, limit: int = 50) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        with self._connect_scheduler_db() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    mode,
                    started_at,
                    finished_at,
                    status,
                    continue_on_error,
                    skip_validation,
                    rows_override,
                    error_text
                FROM scheduler_manual_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return {
            "manual_runs": [
                {
                    "id": row["id"],
                    "mode": row["mode"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "status": row["status"],
                    "continue_on_error": bool(int(row["continue_on_error"])),
                    "skip_validation": bool(int(row["skip_validation"])),
                    "rows_override": row["rows_override"],
                    "error_text": row["error_text"],
                }
                for row in rows
            ]
        }

    def _scheduler_loop(self) -> None:
        while not self._scheduler_stop_event.is_set():
            try:
                if self._execution_lock.acquire(blocking=False):
                    try:
                        with self._state_lock:
                            max_jobs = self._state.max_jobs_per_cycle
                        result = run_due_jobs_once(self.config, max_jobs=max_jobs)
                        with self._state_lock:
                            self._state.last_cycle = result
                            self._state.last_error = None
                    finally:
                        self._execution_lock.release()
                else:
                    with self._state_lock:
                        self._state.last_cycle = {
                            "checked_at": _utc_iso(),
                            "due_jobs": 0,
                            "executed": [],
                            "skipped": "busy",
                        }
            except Exception as exc:  # noqa: BLE001
                with self._state_lock:
                    self._state.last_error = str(exc)

            with self._state_lock:
                poll_seconds = self._state.poll_seconds
            if self._scheduler_stop_event.wait(timeout=poll_seconds):
                break

        with self._state_lock:
            self._state.running = False
            self._state.stopped_at = _utc_iso()
            self._scheduler_thread = None

    def _start_manual_task(self, mode: str, request: ForcedRunRequest) -> Dict[str, Any]:
        if not self._execution_lock.acquire(blocking=False):
            raise RuntimeError("Existe uma execucao em andamento. Tente novamente.")

        task_id = str(uuid.uuid4())
        task = {
            "task_id": task_id,
            "mode": mode,
            "status": "running",
            "started_at": _utc_iso(),
            "finished_at": None,
            "error_text": None,
            "rows_override": request.rows,
            "continue_on_error": request.continue_on_error,
            "skip_validation": request.skip_validation,
        }
        with self._manual_tasks_lock:
            self._manual_tasks[task_id] = task
            self._current_task_id = task_id

        self._insert_manual_run(task_id=task_id, mode=mode, request=request)

        thread = threading.Thread(
            target=self._run_manual_task,
            args=(task_id, mode, request),
            name=f"dw-generate-manual-{mode}-{task_id[:8]}",
            daemon=True,
        )
        thread.start()
        return task

    def _run_manual_task(self, task_id: str, mode: str, request: ForcedRunRequest) -> None:
        status = "success"
        error_text: Optional[str] = None
        result_payload: Dict[str, Any] = {}

        try:
            if mode == "full":
                result_payload = run_full_load_flow(
                    config=self.config,
                    rows_override=request.rows,
                    validate_before_execute=not request.skip_validation,
                    stop_on_error=not request.continue_on_error,
                )
            else:
                result_payload = run_incremental_load_flow(
                    config=self.config,
                    validate_before_execute=not request.skip_validation,
                    stop_on_error=not request.continue_on_error,
                )

            if _count_flow_errors(result_payload) > 0:
                status = "error"
                error_text = "Execucao finalizada com erros no retorno do fluxo."
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error_text = str(exc)
            result_payload = {"exception": str(exc)}
        finally:
            finished_at = _utc_iso()
            with self._manual_tasks_lock:
                task = self._manual_tasks[task_id]
                task["status"] = status
                task["finished_at"] = finished_at
                task["error_text"] = error_text
                if self._current_task_id == task_id:
                    self._current_task_id = None

            self._update_manual_run(
                task_id=task_id,
                status=status,
                finished_at=finished_at,
                error_text=error_text,
                result_payload=result_payload,
            )
            try:
                append_runtime_log(
                    config=self.config,
                    event_type="manual_run",
                    payload={
                        "task_id": task_id,
                        "mode": mode,
                        "status": status,
                        "started_at": task["started_at"],
                        "finished_at": finished_at,
                        "error_text": error_text,
                    },
                )
            except Exception:  # noqa: BLE001
                pass
            self._execution_lock.release()

    def _ensure_manual_runs_table(self) -> None:
        with self._connect_scheduler_db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS scheduler_manual_runs (
                    id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental')),
                    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'error')),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    continue_on_error INTEGER NOT NULL DEFAULT 0,
                    skip_validation INTEGER NOT NULL DEFAULT 0,
                    rows_override INTEGER,
                    error_text TEXT,
                    result_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_scheduler_manual_runs_started
                ON scheduler_manual_runs(started_at DESC);
                """
            )
            conn.commit()

    def _insert_manual_run(self, task_id: str, mode: str, request: ForcedRunRequest) -> None:
        with self._connect_scheduler_db() as conn:
            conn.execute(
                """
                INSERT INTO scheduler_manual_runs (
                    id,
                    mode,
                    status,
                    started_at,
                    continue_on_error,
                    skip_validation,
                    rows_override
                ) VALUES (?, ?, 'running', ?, ?, ?, ?)
                """,
                (
                    task_id,
                    mode,
                    _utc_iso(),
                    int(request.continue_on_error),
                    int(request.skip_validation),
                    request.rows,
                ),
            )
            conn.commit()

    def _update_manual_run(
        self,
        task_id: str,
        status: str,
        finished_at: str,
        error_text: Optional[str],
        result_payload: Dict[str, Any],
    ) -> None:
        with self._connect_scheduler_db() as conn:
            conn.execute(
                """
                UPDATE scheduler_manual_runs
                SET status = ?,
                    finished_at = ?,
                    error_text = ?,
                    result_json = ?
                WHERE id = ?
                """,
                (
                    status,
                    finished_at,
                    error_text,
                    json.dumps(result_payload, ensure_ascii=False),
                    task_id,
                ),
            )
            conn.commit()

    def _connect_scheduler_db(self) -> sqlite3.Connection:
        db_path = _resolve_scheduler_db_path(self.config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn


def create_app(
    config_path: str | Path = "config.yaml",
    env_file: str | Path = ".env",
    *,
    config: Optional[AppConfig] = None,
) -> FastAPI:
    app_config = config or load_config(Path(config_path), dotenv_path=Path(env_file))
    runtime = SchedulerApiRuntime(app_config)

    app = FastAPI(title="dw-generate Control API", version="0.2.0")
    app.state.runtime = runtime
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    web_dir = Path(__file__).resolve().parent / "web"
    app.mount("/web", StaticFiles(directory=web_dir), name="web")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(web_dir / "index.html")

    @app.get("/api/health")
    def health() -> Dict[str, str]:
        return {"status": "ok", "time_utc": _utc_iso()}

    @app.get("/api/status")
    def api_status() -> Dict[str, Any]:
        return runtime.status()

    @app.post("/api/scheduler/start")
    def api_scheduler_start(payload: SchedulerStartRequest) -> Dict[str, Any]:
        return runtime.start_scheduler(
            poll_seconds=payload.poll_seconds,
            max_jobs_per_cycle=payload.max_jobs_per_cycle,
        )

    @app.post("/api/scheduler/stop")
    def api_scheduler_stop() -> Dict[str, Any]:
        return runtime.stop_scheduler()

    @app.post("/api/scheduler/run-once")
    def api_scheduler_run_once(
        max_jobs: Optional[int] = Query(default=None, ge=1),
    ) -> Dict[str, Any]:
        try:
            return runtime.run_due_once(max_jobs=max_jobs)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/jobs")
    def api_jobs(
        include_disabled: bool = Query(default=True),
    ) -> Dict[str, Any]:
        return list_scheduler_jobs(app_config, include_disabled=include_disabled)

    @app.post("/api/jobs")
    def api_jobs_upsert(payload: JobUpsertRequest) -> Dict[str, Any]:
        mode = payload.mode.strip().lower()
        if mode not in {"full", "incremental"}:
            raise HTTPException(status_code=400, detail="mode deve ser 'full' ou 'incremental'.")

        name = payload.name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="name nao pode ser vazio.")

        schedule_type = payload.schedule_type.strip().lower()
        if schedule_type not in {"interval", "daily"}:
            raise HTTPException(status_code=400, detail="schedule_type deve ser 'interval' ou 'daily'.")

        kwargs: Dict[str, Any] = {
            "config": app_config,
            "name": name,
            "mode": mode,
            "rows_override": payload.rows,
            "continue_on_error": payload.continue_on_error,
            "skip_validation": payload.skip_validation,
            "enabled": payload.enabled,
            "replace": payload.replace,
        }

        if schedule_type == "interval":
            if payload.interval_value is None:
                raise HTTPException(status_code=400, detail="interval_value e obrigatorio para schedule interval.")
            interval_unit = (payload.interval_unit or "").strip().lower()
            if interval_unit not in {"minutes", "hours"}:
                raise HTTPException(status_code=400, detail="interval_unit deve ser 'minutes' ou 'hours'.")
            if interval_unit == "minutes":
                kwargs["every_minutes"] = payload.interval_value
            else:
                kwargs["every_hours"] = payload.interval_value
        else:
            if not payload.daily_at:
                raise HTTPException(status_code=400, detail="daily_at e obrigatorio para schedule daily.")
            kwargs["daily_at"] = payload.daily_at.strip()
            kwargs["daily_repeat"] = bool(payload.daily_repeat)

        try:
            return add_scheduler_job(**kwargs)
        except ValueError as exc:
            error_message = str(exc)
            status_code = 409 if "Ja existe job" in error_message else 400
            raise HTTPException(status_code=status_code, detail=error_message) from exc

    @app.post("/api/jobs/{job_ref}/trigger")
    def api_job_trigger(job_ref: str) -> Dict[str, Any]:
        try:
            return runtime.trigger_job(job_ref)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/jobs/{job_ref}/enable")
    def api_job_enable(job_ref: str) -> Dict[str, Any]:
        try:
            return set_scheduler_job_enabled(app_config, job_ref=job_ref, enabled=True)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/jobs/{job_ref}/disable")
    def api_job_disable(job_ref: str) -> Dict[str, Any]:
        try:
            return set_scheduler_job_enabled(app_config, job_ref=job_ref, enabled=False)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.delete("/api/jobs/{job_ref}")
    def api_job_delete(job_ref: str) -> Dict[str, Any]:
        try:
            return delete_scheduler_job(app_config, job_ref=job_ref)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/execute/full")
    def api_execute_full(payload: ForcedRunRequest) -> Dict[str, Any]:
        try:
            return runtime.trigger_full(payload)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/execute/incremental")
    def api_execute_incremental(payload: ForcedRunRequest) -> Dict[str, Any]:
        try:
            return runtime.trigger_incremental(payload)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/tasks/{task_id}")
    def api_task(task_id: str) -> Dict[str, Any]:
        try:
            return runtime.get_manual_task(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/logs")
    def api_logs(
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> Dict[str, Any]:
        return {"logs": read_runtime_logs(app_config, limit=limit)}

    @app.get("/api/history")
    def api_history(
        limit: int = Query(default=30, ge=1, le=500),
    ) -> Dict[str, Any]:
        scheduler_runs = list_scheduler_runs(app_config, limit=limit)
        manual_runs = runtime.list_manual_runs(limit=limit)
        return {
            "scheduler_runs": scheduler_runs["runs"],
            "manual_runs": manual_runs["manual_runs"],
        }

    @app.on_event("shutdown")
    def on_shutdown() -> None:
        runtime.stop_scheduler(timeout_seconds=5)

    return app


def run_api_server(
    config_path: str | Path,
    env_file: str | Path,
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> None:
    import uvicorn

    cfg = load_config(Path(config_path), dotenv_path=Path(env_file))
    app = create_app(config=cfg)
    uvicorn.run(
        app,
        host=host or cfg.api.host,
        port=int(port or cfg.api.port),
        log_level="info",
    )


def _resolve_scheduler_db_path(config: AppConfig) -> Path:
    configured = (config.scheduler.db_path or "").strip()
    if configured:
        path = Path(configured)
    else:
        path = config.workspace_dir / "runtime" / "scheduler.db"
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _utc_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_brt_iso(value: datetime) -> str:
    return value.astimezone(BRASILIA_TZ).replace(microsecond=0).isoformat()


def _count_flow_errors(payload: Any) -> int:
    if isinstance(payload, dict):
        total = 0
        for key, value in payload.items():
            if key in {"errors", "validation_errors"} and isinstance(value, list):
                total += len(value)
            else:
                total += _count_flow_errors(value)
        return total
    if isinstance(payload, list):
        return sum(_count_flow_errors(item) for item in payload)
    return 0
