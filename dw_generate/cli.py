from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import load_config
from .discovery import ensure_sql_output_folders, snapshot_source_tables
from .executor import apply_sql_scripts
from .flows import run_full_load_flow, run_incremental_load_flow
from .layers import ensure_layer_databases


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dw-generate",
        description="Automacao para acelerar a criacao de DW por camadas (stage, warehouse, gold).",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Caminho para o arquivo de configuracao YAML.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Arquivo .env usado para resolver variaveis no config.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init-layers",
        help="Valida e cria os bancos de camada (stage, warehouse, gold) se nao existirem.",
    )

    snapshot_cmd = subparsers.add_parser(
        "snapshot-source",
        help="Lista tabelas da base origem e gera amostras com cabecalho + primeiras linhas.",
    )
    snapshot_cmd.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Quantidade de linhas por tabela. Sobrescreve sample_rows do config.",
    )

    subparsers.add_parser(
        "prepare-sql-dirs",
        help="Cria pastas para depositar scripts SQL por camada.",
    )

    apply_cmd = subparsers.add_parser(
        "apply-sql",
        help="Executa scripts .sql em workspace/sql_output/<camada>/ nos bancos corretos.",
    )
    apply_cmd.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua executando outros arquivos mesmo se houver erro.",
    )
    apply_cmd.add_argument(
        "--skip-validation",
        action="store_true",
        help="Pula a validacao de normalizacao antes da execucao dos SQLs.",
    )
    apply_cmd.add_argument(
        "--mode",
        choices=["raw", "full", "incremental"],
        default="raw",
        help="Modo de carga: raw (script original), full (refresh completo), incremental.",
    )

    run_full_cmd = subparsers.add_parser(
        "run-full",
        help="Executa fluxo completo de carga full (init-layers + snapshot + apply em modo full).",
    )
    run_full_cmd.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Quantidade de linhas por tabela para snapshot.",
    )
    run_full_cmd.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua executando outros arquivos mesmo se houver erro no apply.",
    )
    run_full_cmd.add_argument(
        "--skip-validation",
        action="store_true",
        help="Pula validacao de normalizacao durante apply.",
    )

    run_incr_cmd = subparsers.add_parser(
        "run-incremental",
        help="Executa fluxo de carga incremental baseado em dt_insercao.",
    )
    run_incr_cmd.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua executando outros arquivos mesmo se houver erro no apply.",
    )
    run_incr_cmd.add_argument(
        "--skip-validation",
        action="store_true",
        help="Pula validacao de normalizacao durante apply.",
    )

    subparsers.add_parser(
        "init-project",
        help="Cria um config.yaml inicial a partir do template se ainda nao existir.",
    )

    return parser


def _write_default_config_if_missing(path: Path) -> str:
    if path.exists():
        return f"Arquivo ja existe: {path}"

    template = """source:
  url: "${DW_SOURCE_URL}"
  admin_url: "${DW_SOURCE_ADMIN_URL}"
  schemas: ["${DW_SOURCE_SCHEMA}"]

target:
  admin_url: "${DW_TARGET_ADMIN_URL}"
  layers:
    stage: "${DW_STAGE_DB}"
    warehouse: "${DW_WAREHOUSE_DB}"
    gold: "${DW_GOLD_DB}"

workspace_dir: "${DW_WORKSPACE_DIR}"
sample_rows: "${DW_SAMPLE_ROWS}"

normalization:
  enabled: true
  require_create_table: true
  require_insert_select: true
  require_explicit_alias: true
  forbid_select_star: true
  forbid_destructive_commands: true
  alias_regex: "^[a-z][a-z0-9_]*$"
  min_normalized_columns_per_select: 1
  passthrough_aliases: ["id", "dt_carga", "dt_insercao"]
  required_functions_any:
    - "NULLIF"
    - "LTRIM"
    - "RTRIM"
    - "TRY_CONVERT"
    - "TRY_CAST"
    - "CAST"
    - "CONVERT"
    - "COALESCE"
    - "UPPER"
    - "LOWER"
    - "REPLACE"
    - "TRANSLATE"
"""
    path.write_text(template, encoding="utf-8")
    return f"Arquivo criado: {path}"


def _write_dotenv_example_if_missing(path: Path) -> str:
    if path.exists():
        return f"Arquivo ja existe: {path}"

    template = """# Exemplo SQL Server com autenticacao Windows
# Ajuste host, instancia, bancos e credenciais conforme seu ambiente.
DW_SOURCE_URL=mssql+pyodbc://@SERVER\\INSTANCE/NOME_BASE_ORIGEM?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes
DW_SOURCE_ADMIN_URL=mssql+pyodbc://@SERVER\\INSTANCE/master?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes
DW_TARGET_ADMIN_URL=mssql+pyodbc://@SERVER\\INSTANCE/master?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes

DW_SOURCE_SCHEMA=dbo
DW_STAGE_DB=dw_stage
DW_WAREHOUSE_DB=dw_warehouse
DW_GOLD_DB=dw_gold

DW_WORKSPACE_DIR=workspace
DW_SAMPLE_ROWS=3
"""
    path.write_text(template, encoding="utf-8")
    return f"Arquivo criado: {path}"


def _write_dotenv_if_missing(path: Path, template_path: Path) -> str:
    if path.exists():
        return f"Arquivo ja existe: {path}"
    if not template_path.exists():
        return f"Template nao encontrado: {template_path}"
    path.write_text(template_path.read_text(encoding="utf-8"), encoding="utf-8")
    return f"Arquivo criado: {path}"


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (Path.cwd() / config_path).resolve()

    env_file_path = Path(args.env_file)
    if not env_file_path.is_absolute():
        env_file_path = (config_path.parent / env_file_path).resolve()

    if args.command == "init-project":
        messages = [
            _write_default_config_if_missing(config_path),
            _write_dotenv_example_if_missing(Path(".env.example")),
            _write_dotenv_if_missing(Path(".env"), Path(".env.example")),
        ]
        print("\n".join(messages))
        return

    config = load_config(config_path, dotenv_path=env_file_path)

    if args.command == "init-layers":
        result = ensure_layer_databases(config)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "snapshot-source":
        result = snapshot_source_tables(config, rows_override=args.rows)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "prepare-sql-dirs":
        result = ensure_sql_output_folders(config)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "apply-sql":
        result = apply_sql_scripts(
            config,
            stop_on_error=not args.continue_on_error,
            validate_before_execute=not args.skip_validation,
            load_mode=args.mode,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "run-full":
        result = run_full_load_flow(
            config=config,
            rows_override=args.rows,
            validate_before_execute=not args.skip_validation,
            stop_on_error=not args.continue_on_error,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "run-incremental":
        result = run_incremental_load_flow(
            config=config,
            validate_before_execute=not args.skip_validation,
            stop_on_error=not args.continue_on_error,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    parser.error(f"Comando nao suportado: {args.command}")


if __name__ == "__main__":
    main()
