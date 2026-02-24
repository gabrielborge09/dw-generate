from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..core.config import AppConfig, load_config
from ..core.runtime_log import read_runtime_logs
from ..scheduler_modules.service import (
    add_scheduler_job,
    delete_scheduler_job,
    list_scheduler_jobs,
    list_scheduler_runs,
    set_scheduler_job_enabled,
)
from .runtime import SchedulerApiRuntime, utc_iso
from .schemas import ForcedRunRequest, JobUpsertRequest, SchedulerStartRequest


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

    web_dir = Path(__file__).resolve().parent.parent / "web"
    app.mount("/web", StaticFiles(directory=web_dir), name="web")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(web_dir / "index.html")

    @app.get("/api/health")
    def health() -> Dict[str, str]:
        return {"status": "ok", "time_utc": utc_iso()}

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
