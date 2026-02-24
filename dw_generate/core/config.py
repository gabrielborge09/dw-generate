from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv


@dataclass
class SourceConfig:
    url: str
    schemas: Optional[List[str]] = None
    admin_url: Optional[str] = None


@dataclass
class TargetConfig:
    admin_url: str
    layers: Dict[str, str]


@dataclass
class NormalizationConfig:
    enabled: bool = True
    require_create_table: bool = True
    require_insert_select: bool = True
    require_explicit_alias: bool = True
    forbid_select_star: bool = True
    forbid_destructive_commands: bool = True
    alias_regex: str = r"^[a-z][a-z0-9_]*$"
    min_normalized_columns_per_select: int = 1
    passthrough_aliases: List[str] = field(default_factory=list)
    required_functions_any: List[str] = field(
        default_factory=lambda: [
            "NULLIF",
            "LTRIM",
            "RTRIM",
            "TRY_CONVERT",
            "TRY_CAST",
            "CAST",
            "CONVERT",
            "COALESCE",
            "UPPER",
            "LOWER",
            "REPLACE",
            "TRANSLATE",
        ]
    )


@dataclass
class SchedulerConfig:
    db_path: Optional[str] = None
    poll_interval_seconds: int = 30


@dataclass
class ApiConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass
class AppConfig:
    source: SourceConfig
    target: TargetConfig
    workspace_dir: Path
    sample_rows: int = 3
    normalization: NormalizationConfig = field(default_factory=NormalizationConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    api: ApiConfig = field(default_factory=ApiConfig)


def _validate_layers(layers: Dict[str, str]) -> Dict[str, str]:
    required = ["stage", "warehouse", "gold"]
    missing = [item for item in required if item not in layers]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Camadas obrigatorias ausentes em target.layers: {missing_str}")
    return layers


_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def load_config(path: str | Path, dotenv_path: str | Path | None = ".env") -> AppConfig:
    _load_dotenv_if_exists(dotenv_path)
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as file:
        raw = _resolve_env_in_value(yaml.safe_load(file) or {})

    source_raw = raw.get("source") or {}
    target_raw = raw.get("target") or {}
    normalization_raw = raw.get("normalization") or {}
    scheduler_raw = raw.get("scheduler") or {}
    api_raw = raw.get("api") or {}

    source = SourceConfig(
        url=source_raw["url"],
        schemas=source_raw.get("schemas"),
        admin_url=source_raw.get("admin_url"),
    )

    target = TargetConfig(
        admin_url=target_raw["admin_url"],
        layers=_validate_layers(target_raw["layers"]),
    )

    workspace_dir = Path(raw.get("workspace_dir", "workspace"))
    sample_rows = int(raw.get("sample_rows", 3))
    normalization = NormalizationConfig(
        enabled=bool(normalization_raw.get("enabled", True)),
        require_create_table=bool(normalization_raw.get("require_create_table", True)),
        require_insert_select=bool(normalization_raw.get("require_insert_select", True)),
        require_explicit_alias=bool(normalization_raw.get("require_explicit_alias", True)),
        forbid_select_star=bool(normalization_raw.get("forbid_select_star", True)),
        forbid_destructive_commands=bool(
            normalization_raw.get("forbid_destructive_commands", True)
        ),
        alias_regex=str(normalization_raw.get("alias_regex", r"^[a-z][a-z0-9_]*$")),
        min_normalized_columns_per_select=int(
            normalization_raw.get("min_normalized_columns_per_select", 1)
        ),
        passthrough_aliases=list(normalization_raw.get("passthrough_aliases", [])),
        required_functions_any=list(normalization_raw.get("required_functions_any", []))
        or NormalizationConfig().required_functions_any,
    )
    scheduler = SchedulerConfig(
        db_path=scheduler_raw.get("db_path"),
        poll_interval_seconds=int(scheduler_raw.get("poll_interval_seconds", 30)),
    )
    api = ApiConfig(
        host=str(api_raw.get("host", "127.0.0.1")),
        port=int(api_raw.get("port", 8000)),
    )

    return AppConfig(
        source=source,
        target=target,
        workspace_dir=workspace_dir,
        sample_rows=sample_rows,
        normalization=normalization,
        scheduler=scheduler,
        api=api,
    )


def _load_dotenv_if_exists(dotenv_path: str | Path | None) -> None:
    if dotenv_path is None:
        return
    env_path = Path(dotenv_path)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)


def _resolve_env_in_value(value: Any) -> Any:
    if isinstance(value, str):
        return _resolve_env_in_text(value)
    if isinstance(value, list):
        return [_resolve_env_in_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_env_in_value(item) for key, item in value.items()}
    return value


def _resolve_env_in_text(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        env_name = match.group(1)
        env_value = os.environ.get(env_name)
        if env_value is None:
            raise ValueError(f"Variavel de ambiente nao definida: {env_name}")
        return env_value

    return _ENV_VAR_RE.sub(replace, text)
