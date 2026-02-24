from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from ..core.config import AppConfig


def init_scheduler_store(config: AppConfig) -> Dict[str, str]:
    db_path = _scheduler_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect_scheduler(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS scheduler_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental')),
                schedule_type TEXT NOT NULL CHECK (schedule_type IN ('interval', 'daily')),
                interval_seconds INTEGER,
                daily_time TEXT,
                daily_repeat INTEGER NOT NULL DEFAULT 1,
                rows_override INTEGER,
                continue_on_error INTEGER NOT NULL DEFAULT 0,
                skip_validation INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                next_run_at TEXT NOT NULL,
                last_run_at TEXT,
                last_status TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scheduler_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL CHECK (status IN ('running', 'success', 'error')),
                result_json TEXT,
                error_text TEXT,
                FOREIGN KEY (job_id) REFERENCES scheduler_jobs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_due
            ON scheduler_jobs(enabled, next_run_at);

            CREATE INDEX IF NOT EXISTS idx_scheduler_runs_job
            ON scheduler_runs(job_id, started_at DESC);
            """
        )
        _ensure_scheduler_schema(conn)
        conn.commit()
    return {"scheduler_db": str(db_path)}


def _scheduler_db_path(config: AppConfig) -> Path:
    configured = (config.scheduler.db_path or "").strip()
    if configured:
        path = Path(configured)
    else:
        path = config.workspace_dir / "runtime" / "scheduler.db"
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _connect_scheduler(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _find_job_by_ref(conn: sqlite3.Connection, job_ref: str) -> Optional[sqlite3.Row]:
    if job_ref.isdigit():
        by_id = conn.execute(
            "SELECT * FROM scheduler_jobs WHERE id = ?",
            (int(job_ref),),
        ).fetchone()
        if by_id is not None:
            return by_id
    return conn.execute(
        "SELECT * FROM scheduler_jobs WHERE name = ?",
        (job_ref,),
    ).fetchone()


def _serialize_job(row: sqlite3.Row) -> Dict[str, object]:
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "mode": row["mode"],
        "schedule_type": row["schedule_type"],
        "interval_seconds": row["interval_seconds"],
        "daily_time": row["daily_time"],
        "daily_repeat": _as_bool(_row_get(row, "daily_repeat", 1)),
        "rows_override": row["rows_override"],
        "continue_on_error": _as_bool(row["continue_on_error"]),
        "skip_validation": _as_bool(row["skip_validation"]),
        "enabled": _as_bool(row["enabled"]),
        "next_run_at": row["next_run_at"],
        "last_run_at": row["last_run_at"],
        "last_status": row["last_status"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _as_bool(value: Any) -> bool:
    return bool(int(value))


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    keys = set(row.keys())
    if key in keys:
        return row[key]
    return default


def _ensure_scheduler_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "scheduler_jobs")
    if "daily_repeat" not in columns:
        conn.execute(
            "ALTER TABLE scheduler_jobs ADD COLUMN daily_repeat INTEGER NOT NULL DEFAULT 1"
        )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}
