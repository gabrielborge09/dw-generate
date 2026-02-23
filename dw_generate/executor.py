from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List

import sqlparse

from .config import AppConfig
from .db import make_engine
from .layers import layer_urls
from .normalization import split_sql_server_batches, validate_sql_server_script


def _split_sql_statements(sql_text: str) -> List[str]:
    return [item.strip() for item in sqlparse.split(sql_text) if item.strip()]


@dataclass
class InsertSelectParts:
    target_table: str
    insert_columns: List[str]
    select_clause: str
    from_clause: str


_INSERT_SELECT_RE = re.compile(
    r"""
    ^\s*INSERT\s+INTO\s+(?P<target>[^\s(]+)\s*
    \((?P<insert_columns>.*?)\)\s*
    SELECT\s+(?P<select_clause>.*?)
    \s+FROM\s+(?P<from_clause>.*)\s*;?\s*$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


def apply_sql_scripts(
    config: AppConfig,
    stop_on_error: bool = True,
    validate_before_execute: bool = True,
    load_mode: str = "raw",
) -> Dict[str, object]:
    output_base = config.workspace_dir / "sql_output"
    urls = layer_urls(config)

    report: Dict[str, object] = {"executed": [], "errors": [], "validation_errors": []}
    for layer_name in ("stage", "warehouse", "gold"):
        layer_dir = output_base / layer_name
        if not layer_dir.exists():
            continue

        sql_files = sorted(layer_dir.glob("*.sql"))
        if not sql_files:
            continue

        engine = make_engine(urls[layer_name])
        dialect = engine.dialect.name
        for sql_file in sql_files:
            sql_text = sql_file.read_text(encoding="utf-8")
            if dialect.startswith("mssql") and validate_before_execute:
                validation = validate_sql_server_script(
                    sql_text, config.normalization, layer_name=layer_name
                )
                if not validation.valid:
                    report["validation_errors"].append(
                        {
                            "layer": layer_name,
                            "file": str(sql_file),
                            "errors": validation.errors,
                        }
                    )
                    if stop_on_error:
                        return report
                    continue

            statements = _split_mssql_or_default(sql_text, dialect)
            try:
                with engine.begin() as conn:
                    if dialect.startswith("mssql") and load_mode in {"full", "incremental"}:
                        executed_statements = _execute_load_mode_mssql(
                            conn=conn,
                            statements=statements,
                            load_mode=load_mode,
                            incremental_column="dt_insercao",
                        )
                    else:
                        for statement in statements:
                            conn.exec_driver_sql(statement)
                        executed_statements = len(statements)
                report["executed"].append(
                    {
                        "layer": layer_name,
                        "file": str(sql_file),
                        "statements": executed_statements,
                        "load_mode": load_mode,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                error = {"layer": layer_name, "file": str(sql_file), "error": str(exc)}
                report["errors"].append(error)
                if stop_on_error:
                    return report

    return report


def _split_mssql_or_default(sql_text: str, dialect: str) -> List[str]:
    if dialect.startswith("mssql"):
        return split_sql_server_batches(sql_text)
    return _split_sql_statements(sql_text)


def _execute_load_mode_mssql(conn, statements: List[str], load_mode: str, incremental_column: str) -> int:
    expanded_statements: List[str] = []
    for statement in statements:
        split_items = _split_sql_statements(statement)
        if split_items:
            expanded_statements.extend(split_items)
        else:
            expanded_statements.append(statement)

    non_insert_statements: List[str] = []
    insert_parts: InsertSelectParts | None = None

    for statement in expanded_statements:
        parsed = _parse_insert_select(statement)
        if parsed is None:
            non_insert_statements.append(statement)
            continue
        if insert_parts is None:
            insert_parts = parsed
        else:
            # Mantem comportamento previsivel para scripts fora do padrao atual.
            non_insert_statements.append(statement)

    executed_count = 0
    for statement in non_insert_statements:
        conn.exec_driver_sql(statement)
        executed_count += 1

    if insert_parts is None:
        return executed_count

    _ensure_incremental_column(conn, insert_parts.target_table, incremental_column)

    if load_mode == "full":
        conn.exec_driver_sql(f"DELETE FROM {insert_parts.target_table}")
        executed_count += 1

    load_insert_sql = _build_load_insert_sql(
        insert_parts=insert_parts,
        mode=load_mode,
        incremental_column=incremental_column,
    )
    conn.exec_driver_sql(load_insert_sql)
    executed_count += 1
    return executed_count


def _parse_insert_select(statement: str) -> InsertSelectParts | None:
    match = _INSERT_SELECT_RE.match(statement.strip())
    if not match:
        return None

    target_table = match.group("target").strip()
    insert_columns = [item.strip() for item in match.group("insert_columns").split(",") if item.strip()]
    select_clause = match.group("select_clause").strip()
    from_clause = match.group("from_clause").strip().rstrip(";")

    if not target_table or not insert_columns or not select_clause or not from_clause:
        return None

    return InsertSelectParts(
        target_table=target_table,
        insert_columns=insert_columns,
        select_clause=select_clause,
        from_clause=from_clause,
    )


def _build_load_insert_sql(insert_parts: InsertSelectParts, mode: str, incremental_column: str) -> str:
    columns = list(insert_parts.insert_columns)
    if not _has_column(columns, incremental_column):
        columns.append(incremental_column)

    select_clause = insert_parts.select_clause
    if not _has_alias(select_clause, incremental_column):
        select_clause = (
            f"{select_clause.rstrip()}\n"
            f"    , TRY_CONVERT(DATETIME2, {incremental_column}) AS {incremental_column}"
        )

    from_clause = insert_parts.from_clause
    if mode == "incremental":
        predicate = (
            f"COALESCE(TRY_CONVERT(DATETIME2, {incremental_column}), CAST('1900-01-01' AS DATETIME2)) "
            f"> COALESCE((SELECT MAX(t.{incremental_column}) FROM {insert_parts.target_table} AS t), "
            f"CAST('1900-01-01' AS DATETIME2))"
        )
        from_clause = _append_predicate(from_clause, predicate)

    cols_sql = ", ".join(columns)
    return (
        f"INSERT INTO {insert_parts.target_table} ({cols_sql})\n"
        f"SELECT {select_clause}\n"
        f"FROM {from_clause};"
    )


def _append_predicate(from_clause: str, predicate: str) -> str:
    if re.search(r"\bWHERE\b", from_clause, re.IGNORECASE):
        return f"{from_clause} AND {predicate}"
    return f"{from_clause} WHERE {predicate}"


def _has_column(columns: List[str], column_name: str) -> bool:
    normalized = {_normalize_identifier(item) for item in columns}
    return _normalize_identifier(column_name) in normalized


def _has_alias(select_clause: str, alias_name: str) -> bool:
    pattern = re.compile(rf"\bAS\s+{re.escape(alias_name)}\b", re.IGNORECASE)
    return bool(pattern.search(select_clause))


def _ensure_incremental_column(conn, target_table: str, incremental_column: str) -> None:
    constraint_name = _default_constraint_name(target_table, incremental_column)
    sql = f"""
IF COL_LENGTH('{target_table}', '{incremental_column}') IS NULL
BEGIN
    ALTER TABLE {target_table}
    ADD {incremental_column} DATETIME2 NOT NULL
    CONSTRAINT {constraint_name} DEFAULT SYSUTCDATETIME();
END
"""
    conn.exec_driver_sql(sql)


def _default_constraint_name(target_table: str, incremental_column: str) -> str:
    raw = re.sub(r"[\[\].]", "_", target_table)
    raw = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    raw = raw.strip("_")
    base = f"DF_{raw}_{incremental_column}"
    if len(base) <= 120:
        return base
    return base[:120]


def _normalize_identifier(name: str) -> str:
    text = name.strip().strip("[]").strip().lower()
    return text
