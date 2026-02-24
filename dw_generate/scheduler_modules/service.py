from __future__ import annotations

import json
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..core.config import AppConfig
from ..core.runtime_log import BRASILIA_TZ, append_runtime_log
from ..etl.flows import run_full_load_flow, run_incremental_load_flow
from .store import (
    _as_bool,
    _connect_scheduler,
    _find_job_by_ref,
    _row_get,
    _scheduler_db_path,
    _serialize_job,
    init_scheduler_store,
)


_DAILY_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


def add_scheduler_job(
    config: AppConfig,
    name: str,
    mode: str,
    *,
    every_minutes: Optional[int] = None,
    every_hours: Optional[int] = None,
    daily_at: Optional[str] = None,
    daily_repeat: bool = True,
    rows_override: Optional[int] = None,
    continue_on_error: bool = False,
    skip_validation: bool = False,
    enabled: bool = True,
    replace: bool = False,
) -> Dict[str, object]:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"full", "incremental"}:
        raise ValueError("mode deve ser 'full' ou 'incremental'.")

    schedule_type, interval_seconds, normalized_daily_at, daily_first_run_at = _parse_schedule_args(
        every_minutes=every_minutes,
        every_hours=every_hours,
        daily_at=daily_at,
    )
    effective_daily_repeat = bool(daily_repeat) if schedule_type == "daily" else True

    now = _utcnow()
    if daily_first_run_at is not None:
        if daily_first_run_at <= now:
            raise ValueError("Para daily com data/hora, informe um horario futuro.")
        next_run_at = daily_first_run_at
    else:
        next_run_at = _compute_next_run(
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            daily_time=normalized_daily_at,
            reference=now,
        )

    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)

    with _connect_scheduler(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM scheduler_jobs WHERE name = ?",
            (name,),
        ).fetchone()

        if existing and not replace:
            raise ValueError(
                f"Ja existe job com nome '{name}'. Use --replace para atualizar."
            )

        params = (
            normalized_mode,
            schedule_type,
            interval_seconds,
            normalized_daily_at,
            int(effective_daily_repeat),
            rows_override,
            int(continue_on_error),
            int(skip_validation),
            int(enabled),
            _to_iso(next_run_at),
            _to_iso(now),
            _to_iso(now),
            name,
        )

        if existing:
            conn.execute(
                """
                UPDATE scheduler_jobs
                SET mode = ?,
                    schedule_type = ?,
                    interval_seconds = ?,
                    daily_time = ?,
                    daily_repeat = ?,
                    rows_override = ?,
                    continue_on_error = ?,
                    skip_validation = ?,
                    enabled = ?,
                    next_run_at = ?,
                    updated_at = ?
                WHERE name = ?
                """,
                (
                    normalized_mode,
                    schedule_type,
                    interval_seconds,
                    normalized_daily_at,
                    int(effective_daily_repeat),
                    rows_override,
                    int(continue_on_error),
                    int(skip_validation),
                    int(enabled),
                    _to_iso(next_run_at),
                    _to_iso(now),
                    name,
                ),
            )
            job_id = int(existing["id"])
            action = "updated"
        else:
            conn.execute(
                """
                INSERT INTO scheduler_jobs (
                    mode,
                    schedule_type,
                    interval_seconds,
                    daily_time,
                    daily_repeat,
                    rows_override,
                    continue_on_error,
                    skip_validation,
                    enabled,
                    next_run_at,
                    created_at,
                    updated_at,
                    name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            job_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            action = "created"
        conn.commit()

        row = conn.execute(
            "SELECT * FROM scheduler_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()

    return {"action": action, "job": _serialize_job(row)}


def list_scheduler_jobs(config: AppConfig, include_disabled: bool = True) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    query = "SELECT * FROM scheduler_jobs"
    params: Tuple[object, ...] = ()
    if not include_disabled:
        query += " WHERE enabled = ?"
        params = (1,)
    query += " ORDER BY next_run_at ASC, id ASC"

    with _connect_scheduler(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return {"jobs": [_serialize_job(row) for row in rows]}


def set_scheduler_job_enabled(config: AppConfig, job_ref: str, enabled: bool) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    now = _utcnow()
    with _connect_scheduler(db_path) as conn:
        row = _find_job_by_ref(conn, job_ref)
        if row is None:
            raise ValueError(f"Job nao encontrado: {job_ref}")

        next_run_at = row["next_run_at"]
        if enabled and not _as_bool(row["enabled"]):
            next_run = _compute_next_run(
                schedule_type=row["schedule_type"],
                interval_seconds=row["interval_seconds"],
                daily_time=row["daily_time"],
                reference=now,
            )
            next_run_at = _to_iso(next_run)

        conn.execute(
            """
            UPDATE scheduler_jobs
            SET enabled = ?,
                next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (int(enabled), next_run_at, _to_iso(now), row["id"]),
        )
        conn.commit()

        updated = conn.execute(
            "SELECT * FROM scheduler_jobs WHERE id = ?",
            (row["id"],),
        ).fetchone()
    return {"job": _serialize_job(updated)}


def trigger_scheduler_job(config: AppConfig, job_ref: str) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    with _connect_scheduler(db_path) as conn:
        row = _find_job_by_ref(conn, job_ref)
        if row is None:
            raise ValueError(f"Job nao encontrado: {job_ref}")
        execution = _execute_job(conn, config, row)
        conn.commit()
    return {"triggered": execution}


def delete_scheduler_job(config: AppConfig, job_ref: str) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    with _connect_scheduler(db_path) as conn:
        row = _find_job_by_ref(conn, job_ref)
        if row is None:
            raise ValueError(f"Job nao encontrado: {job_ref}")

        job_id = int(row["id"])
        job_name = str(row["name"])

        conn.execute(
            "DELETE FROM scheduler_runs WHERE job_id = ?",
            (job_id,),
        )
        conn.execute(
            "DELETE FROM scheduler_jobs WHERE id = ?",
            (job_id,),
        )
        conn.commit()

    return {"deleted": {"id": job_id, "name": job_name}}


def run_due_jobs_once(config: AppConfig, max_jobs: Optional[int] = None) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    now = _utcnow()
    limit_clause = ""
    params: Tuple[object, ...] = (_to_iso(now),)
    if max_jobs is not None and max_jobs > 0:
        limit_clause = " LIMIT ?"
        params = (_to_iso(now), int(max_jobs))

    with _connect_scheduler(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM scheduler_jobs
            WHERE enabled = 1
              AND next_run_at <= ?
            ORDER BY next_run_at ASC, id ASC
            {limit_clause}
            """,
            params,
        ).fetchall()

        executions: List[Dict[str, object]] = []
        for row in rows:
            executions.append(_execute_job(conn, config, row))
        conn.commit()

    return {
        "checked_at": _to_iso(now),
        "due_jobs": len(rows),
        "executed": executions,
    }


def start_scheduler_loop(
    config: AppConfig,
    *,
    poll_interval_seconds: Optional[int] = None,
    max_cycles: Optional[int] = None,
    max_jobs_per_cycle: Optional[int] = None,
) -> Dict[str, object]:
    init_scheduler_store(config)
    poll_seconds = (
        int(poll_interval_seconds)
        if poll_interval_seconds is not None
        else int(config.scheduler.poll_interval_seconds)
    )
    if poll_seconds < 1:
        raise ValueError("poll_interval_seconds deve ser >= 1.")

    cycles = 0
    total_executed = 0
    last_cycle: Dict[str, object] = {"checked_at": _to_iso(_utcnow()), "due_jobs": 0, "executed": []}

    try:
        while True:
            last_cycle = run_due_jobs_once(config, max_jobs=max_jobs_per_cycle)
            total_executed += len(last_cycle["executed"])
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        pass

    return {
        "cycles": cycles,
        "total_executed": total_executed,
        "last_cycle": last_cycle,
    }


def list_scheduler_runs(config: AppConfig, limit: int = 20) -> Dict[str, object]:
    init_scheduler_store(config)
    db_path = _scheduler_db_path(config)
    max_limit = max(1, min(int(limit), 500))
    with _connect_scheduler(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.job_id,
                j.name AS job_name,
                r.started_at,
                r.finished_at,
                r.status,
                r.error_text
            FROM scheduler_runs AS r
            INNER JOIN scheduler_jobs AS j ON j.id = r.job_id
            ORDER BY r.started_at DESC, r.id DESC
            LIMIT ?
            """,
            (max_limit,),
        ).fetchall()
    return {
        "runs": [
            {
                "id": int(row["id"]),
                "job_id": int(row["job_id"]),
                "job_name": row["job_name"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "status": row["status"],
                "error_text": row["error_text"],
            }
            for row in rows
        ]
    }


def _execute_job(conn: sqlite3.Connection, config: AppConfig, job_row: sqlite3.Row) -> Dict[str, object]:
    started_at = _utcnow()
    run_insert = conn.execute(
        """
        INSERT INTO scheduler_runs (job_id, started_at, status)
        VALUES (?, ?, 'running')
        """,
        (job_row["id"], _to_iso(started_at)),
    )
    run_id = int(run_insert.lastrowid)

    mode = str(job_row["mode"]).lower()
    rows_override = job_row["rows_override"]
    continue_on_error = _as_bool(job_row["continue_on_error"])
    skip_validation = _as_bool(job_row["skip_validation"])
    daily_repeat = _as_bool(_row_get(job_row, "daily_repeat", 1))

    status = "success"
    error_text = ""
    result_payload: Dict[str, object] | None = None

    try:
        if mode == "full":
            result_payload = run_full_load_flow(
                config=config,
                rows_override=rows_override if rows_override is None else int(rows_override),
                validate_before_execute=not skip_validation,
                stop_on_error=not continue_on_error,
            )
        else:
            result_payload = run_incremental_load_flow(
                config=config,
                validate_before_execute=not skip_validation,
                stop_on_error=not continue_on_error,
            )

        error_count = _count_flow_errors(result_payload)
        if error_count > 0:
            status = "error"
            error_text = f"Fluxo finalizado com {error_count} erro(s) no resultado."
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error_text = str(exc)
        result_payload = {"exception": str(exc)}

    finished_at = _utcnow()
    schedule_type = str(job_row["schedule_type"]).lower()
    enabled_after = int(job_row["enabled"])
    if schedule_type == "daily" and not daily_repeat:
        next_run = finished_at
        enabled_after = 0
    else:
        next_run = _compute_next_run(
            schedule_type=job_row["schedule_type"],
            interval_seconds=job_row["interval_seconds"],
            daily_time=job_row["daily_time"],
            reference=finished_at,
        )

    conn.execute(
        """
        UPDATE scheduler_runs
        SET finished_at = ?,
            status = ?,
            result_json = ?,
            error_text = ?
        WHERE id = ?
        """,
        (
            _to_iso(finished_at),
            status,
            json.dumps(result_payload, ensure_ascii=False),
            error_text or None,
            run_id,
        ),
    )

    conn.execute(
        """
        UPDATE scheduler_jobs
        SET last_run_at = ?,
            last_status = ?,
            last_error = ?,
            next_run_at = ?,
            enabled = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            _to_iso(finished_at),
            status,
            error_text or None,
            _to_iso(next_run),
            enabled_after,
            _to_iso(finished_at),
            job_row["id"],
        ),
    )

    execution = {
        "run_id": run_id,
        "job_id": int(job_row["id"]),
        "job_name": job_row["name"],
        "mode": mode,
        "started_at": _to_iso(started_at),
        "finished_at": _to_iso(finished_at),
        "status": status,
        "error_text": error_text or None,
        "next_run_at": _to_iso(next_run),
        "daily_repeat": daily_repeat,
        "enabled_after_run": bool(enabled_after),
    }
    try:
        append_runtime_log(
            config=config,
            event_type="scheduler_job_run",
            payload=execution,
        )
    except Exception:  # noqa: BLE001
        # Falha de log nunca deve interromper processamento da carga.
        pass
    return execution


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


def _parse_schedule_args(
    *,
    every_minutes: Optional[int],
    every_hours: Optional[int],
    daily_at: Optional[str],
) -> Tuple[str, Optional[int], Optional[str], Optional[datetime]]:
    options_used = sum(
        1
        for value in (every_minutes, every_hours, daily_at)
        if value is not None and str(value).strip() != ""
    )
    if options_used != 1:
        raise ValueError(
            "Informe exatamente um tipo de agenda: --every-minutes, --every-hours ou --daily-at."
        )

    if every_minutes is not None:
        minutes = int(every_minutes)
        if minutes < 1:
            raise ValueError("--every-minutes deve ser >= 1.")
        return "interval", minutes * 60, None, None

    if every_hours is not None:
        hours = int(every_hours)
        if hours < 1:
            raise ValueError("--every-hours deve ser >= 1.")
        return "interval", hours * 3600, None, None

    normalized_daily_at = str(daily_at).strip()
    if not _DAILY_TIME_RE.fullmatch(normalized_daily_at):
        parsed = _parse_daily_datetime(normalized_daily_at)
        if parsed is None:
            raise ValueError("--daily-at deve ser HH:MM ou YYYY-MM-DDTHH:MM (BRT).")
        return "daily", None, parsed.strftime("%H:%M"), parsed
    return "daily", None, normalized_daily_at, None


def _parse_daily_datetime(value: str) -> Optional[datetime]:
    text = value.strip()
    if not text:
        return None

    normalized = text.replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BRASILIA_TZ)
    else:
        parsed = parsed.astimezone(BRASILIA_TZ)

    return parsed.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _compute_next_run(
    *,
    schedule_type: str,
    interval_seconds: Optional[int],
    daily_time: Optional[str],
    reference: datetime,
) -> datetime:
    normalized_schedule_type = str(schedule_type).lower()
    if normalized_schedule_type == "interval":
        if interval_seconds is None:
            raise ValueError("interval_seconds nao pode ser nulo para schedule interval.")
        return reference + timedelta(seconds=int(interval_seconds))

    if normalized_schedule_type == "daily":
        if not daily_time:
            raise ValueError("daily_time nao pode ser nulo para schedule daily.")
        hour, minute = [int(item) for item in daily_time.split(":", 1)]
        reference_br = reference.astimezone(BRASILIA_TZ)
        candidate_br = reference_br.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        if candidate_br <= reference_br:
            candidate_br += timedelta(days=1)
        return candidate_br.astimezone(timezone.utc)

    raise ValueError(f"schedule_type invalido: {schedule_type}")


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc).replace(microsecond=0)


def _to_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
