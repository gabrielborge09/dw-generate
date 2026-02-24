from __future__ import annotations

from typing import Dict, Optional

from ..core.config import AppConfig
from .discovery import ensure_sql_output_folders, snapshot_source_tables
from .executor import apply_sql_scripts
from .layers import ensure_layer_databases


def run_full_load_flow(
    config: AppConfig,
    rows_override: Optional[int] = None,
    validate_before_execute: bool = True,
    stop_on_error: bool = True,
) -> Dict[str, object]:
    layers_result = ensure_layer_databases(config)
    snapshot_result = snapshot_source_tables(config, rows_override=rows_override)
    dirs_result = ensure_sql_output_folders(config)
    apply_result = apply_sql_scripts(
        config=config,
        stop_on_error=stop_on_error,
        validate_before_execute=validate_before_execute,
        load_mode="full",
    )
    return {
        "flow": "full",
        "layers": layers_result,
        "snapshot": snapshot_result,
        "sql_dirs": dirs_result,
        "apply": apply_result,
    }


def run_incremental_load_flow(
    config: AppConfig,
    validate_before_execute: bool = True,
    stop_on_error: bool = True,
) -> Dict[str, object]:
    layers_result = ensure_layer_databases(config)
    dirs_result = ensure_sql_output_folders(config)
    apply_result = apply_sql_scripts(
        config=config,
        stop_on_error=stop_on_error,
        validate_before_execute=validate_before_execute,
        load_mode="incremental",
    )
    return {
        "flow": "incremental",
        "layers": layers_result,
        "sql_dirs": dirs_result,
        "apply": apply_result,
    }
