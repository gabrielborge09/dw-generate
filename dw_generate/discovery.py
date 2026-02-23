from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from .config import AppConfig
from .db import format_schema_table, make_engine


def _safe_file_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def list_source_tables(engine: Engine, schemas: Optional[List[str]] = None) -> List[Dict[str, str]]:
    inspector = inspect(engine)
    dialect = engine.dialect.name
    result: List[Dict[str, str]] = []

    if schemas:
        target_schemas = schemas
    else:
        default_schema = inspector.default_schema_name
        if default_schema:
            target_schemas = [default_schema]
        else:
            target_schemas = inspector.get_schema_names()

    for schema in target_schemas:
        if dialect.startswith("postgres") and schema in {"pg_catalog", "information_schema"}:
            continue
        if dialect.startswith("mssql") and schema.lower() in {"sys", "information_schema"}:
            continue
        tables = inspector.get_table_names(schema=schema)
        for table in tables:
            result.append({"schema": schema, "table": table})

    return result


def sample_table_rows(
    engine: Engine, schema: Optional[str], table: str, rows: int
) -> Tuple[List[str], List[Tuple]]:
    qualified = format_schema_table(engine.dialect.name, schema, table)
    dialect = engine.dialect.name

    if dialect.startswith("mssql"):
        sql = f"SELECT TOP ({rows}) * FROM {qualified}"
    else:
        sql = f"SELECT * FROM {qualified} LIMIT {rows}"

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        headers = list(result.keys())
        data = [tuple(row) for row in result.fetchall()]

    return headers, data


def write_csv(path: Path, headers: Iterable[str], rows: Iterable[Tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(list(headers))
        writer.writerows(rows)


def snapshot_source_tables(config: AppConfig, rows_override: Optional[int] = None) -> Dict[str, object]:
    rows_count = rows_override if rows_override is not None else config.sample_rows
    engine = make_engine(config.source.url)
    inspector = inspect(engine)

    workspace = config.workspace_dir
    samples_dir = workspace / "samples"
    manifest_dir = workspace / "manifests"
    sql_context_dir = workspace / "sql_context"

    samples_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    sql_context_dir.mkdir(parents=True, exist_ok=True)

    tables = list_source_tables(engine, config.source.schemas)
    manifest: Dict[str, object] = {
        "sample_rows": rows_count,
        "source_url_masked": _mask_url(config.source.url),
        "tables": [],
    }

    for item in tables:
        schema = item["schema"]
        table = item["table"]
        headers, rows = sample_table_rows(engine, schema, table, rows_count)
        column_metadata = _get_column_metadata(inspector, schema, table)

        source_key = f"{schema}.{table}" if schema else table
        csv_name = _safe_file_name(f"{source_key}.csv")
        prompt_name = _safe_file_name(f"{source_key}.md")

        csv_path = samples_dir / csv_name
        write_csv(csv_path, headers, rows)

        prompt_path = sql_context_dir / prompt_name
        prompt_path.write_text(
            _build_sql_context(source_key, csv_path.relative_to(workspace), column_metadata),
            encoding="utf-8",
        )

        manifest["tables"].append(
            {
                "source_key": source_key,
                "schema": schema,
                "table": table,
                "sample_file": str(csv_path),
                "columns": headers,
                "column_metadata": column_metadata,
                "row_count": len(rows),
                "context_file": str(prompt_path),
            }
        )

    manifest_path = manifest_dir / "tables_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"tables": len(tables), "manifest_path": str(manifest_path), "samples_dir": str(samples_dir)}


def ensure_sql_output_folders(config: AppConfig) -> Dict[str, str]:
    base = config.workspace_dir / "sql_output"
    paths = {
        "stage": base / "stage",
        "warehouse": base / "warehouse",
        "gold": base / "gold",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return {key: str(value) for key, value in paths.items()}


def _build_sql_context(
    source_key: str, sample_relative_path: Path, column_metadata: List[Dict[str, object]]
) -> str:
    columns_text = "\n".join(
        [
            f"  - {col['name']} ({col['type']}, nullable={str(col['nullable']).lower()})"
            for col in column_metadata
        ]
    )
    return (
        f"# Contexto da tabela: {source_key}\n\n"
        f"- Arquivo de amostra: `{sample_relative_path.as_posix()}`\n"
        "- Banco alvo: SQL Server.\n"
        "- Produza SQL para `stage`, `warehouse` e `gold`.\n"
        "- Use DDL idempotente: `IF OBJECT_ID(...) IS NULL CREATE TABLE ...`.\n"
        "- Inclua carga full com `INSERT INTO ... SELECT ...` e campos normalizados.\n"
        "- Inclua `dt_insercao DATETIME2 NOT NULL` nas tabelas destino.\n"
        "- Inclua `dt_insercao` no INSERT/SELECT para suportar carga incremental.\n"
        "- Toda coluna no SELECT deve usar alias explicito com `AS` em snake_case.\n"
        "- Evite `SELECT *`.\n"
        "- Salve os scripts em `workspace/sql_output/<camada>/`.\n"
        "\n## Colunas de origem\n"
        f"{columns_text}\n"
    )


def _mask_url(url: str) -> str:
    # Evita gravar senha em texto plano nos artefatos gerados.
    if "@" not in url or "://" not in url:
        return url
    left, right = url.split("://", 1)
    if "@" not in right or ":" not in right.split("@", 1)[0]:
        return url
    creds, host = right.split("@", 1)
    user = creds.split(":", 1)[0]
    return f"{left}://{user}:***@{host}"


def _get_column_metadata(inspector, schema: Optional[str], table: str) -> List[Dict[str, object]]:
    columns = inspector.get_columns(table_name=table, schema=schema)
    result: List[Dict[str, object]] = []
    for col in columns:
        result.append(
            {
                "name": col.get("name"),
                "type": str(col.get("type")),
                "nullable": bool(col.get("nullable", True)),
            }
        )
    return result
