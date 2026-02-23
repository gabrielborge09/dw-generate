from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from .config import AppConfig


BRASILIA_TZ = timezone(timedelta(hours=-3), name="BRT")


def append_runtime_log(config: AppConfig, event_type: str, payload: Dict[str, Any]) -> None:
    path = _runtime_log_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)

    now_utc = _utc_now()
    entry = {
        "timestamp_utc": _to_iso(now_utc),
        "timestamp_brt": _to_iso(now_utc.astimezone(BRASILIA_TZ)),
        "event_type": event_type,
        "payload": payload,
    }
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, ensure_ascii=False) + "\n")


def read_runtime_logs(config: AppConfig, limit: int = 200) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 1000))
    path = _runtime_log_path(config)
    if not path.exists():
        return []

    lines = path.read_text(encoding="utf-8").splitlines()
    selected = lines[-safe_limit:]
    parsed: List[Dict[str, Any]] = []
    for line in selected:
        text = line.strip()
        if not text:
            continue
        try:
            parsed.append(json.loads(text))
        except json.JSONDecodeError:
            parsed.append(
                {
                    "timestamp_utc": _to_iso(_utc_now()),
                    "timestamp_brt": _to_iso(_utc_now().astimezone(BRASILIA_TZ)),
                    "event_type": "invalid_log_line",
                    "payload": {"raw": text},
                }
            )
    parsed.reverse()
    return parsed


def _runtime_log_path(config: AppConfig) -> Path:
    return config.workspace_dir / "runtime" / "execution.log"


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(microsecond=0)


def _to_iso(value: datetime) -> str:
    return value.isoformat()
