from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import load_config
from .discovery import ensure_sql_output_folders, snapshot_source_tables
from .executor import apply_sql_scripts
from .flows import run_full_load_flow, run_incremental_load_flow
from .layers import ensure_layer_databases
from .scheduler import (
    add_scheduler_job,
    init_scheduler_store,
    list_scheduler_jobs,
    list_scheduler_runs,
    run_due_jobs_once,
    set_scheduler_job_enabled,
    start_scheduler_loop,
    trigger_scheduler_job,
)


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

    subparsers.add_parser(
        "scheduler-init",
        help="Inicializa o banco SQLite do scheduler.",
    )

    scheduler_add_cmd = subparsers.add_parser(
        "scheduler-add",
        help="Cria ou atualiza um job de agendamento para run-full ou run-incremental.",
    )
    scheduler_add_cmd.add_argument("--name", required=True, help="Nome unico do job.")
    scheduler_add_cmd.add_argument(
        "--mode",
        required=True,
        choices=["full", "incremental"],
        help="Tipo de carga executada pelo job.",
    )
    schedule_group = scheduler_add_cmd.add_mutually_exclusive_group(required=True)
    schedule_group.add_argument(
        "--every-minutes",
        type=int,
        help="Executa em intervalo fixo de minutos.",
    )
    schedule_group.add_argument(
        "--every-hours",
        type=int,
        help="Executa em intervalo fixo de horas.",
    )
    schedule_group.add_argument(
        "--daily-at",
        help="Executa diariamente em HH:MM (BRT) ou inicia em YYYY-MM-DDTHH:MM (BRT).",
    )
    scheduler_add_cmd.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Rows para snapshot quando mode=full (opcional).",
    )
    scheduler_add_cmd.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua em caso de erro durante apply.",
    )
    scheduler_add_cmd.add_argument(
        "--skip-validation",
        action="store_true",
        help="Pula validacao de normalizacao durante apply.",
    )
    scheduler_add_cmd.add_argument(
        "--disabled",
        action="store_true",
        help="Cria job desabilitado.",
    )
    scheduler_add_cmd.add_argument(
        "--replace",
        action="store_true",
        help="Atualiza o job se nome ja existir.",
    )

    scheduler_list_cmd = subparsers.add_parser(
        "scheduler-list",
        help="Lista jobs do scheduler.",
    )
    scheduler_list_cmd.add_argument(
        "--only-enabled",
        action="store_true",
        help="Mostra apenas jobs habilitados.",
    )

    scheduler_enable_cmd = subparsers.add_parser(
        "scheduler-enable",
        help="Habilita um job por nome ou id.",
    )
    scheduler_enable_cmd.add_argument("job", help="Nome ou id do job.")

    scheduler_disable_cmd = subparsers.add_parser(
        "scheduler-disable",
        help="Desabilita um job por nome ou id.",
    )
    scheduler_disable_cmd.add_argument("job", help="Nome ou id do job.")

    scheduler_trigger_cmd = subparsers.add_parser(
        "scheduler-trigger",
        help="Executa um job imediatamente por nome ou id.",
    )
    scheduler_trigger_cmd.add_argument("job", help="Nome ou id do job.")

    scheduler_run_once_cmd = subparsers.add_parser(
        "scheduler-run-once",
        help="Roda um ciclo do scheduler e executa jobs vencidos.",
    )
    scheduler_run_once_cmd.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Limita quantidade de jobs executados no ciclo.",
    )

    scheduler_start_cmd = subparsers.add_parser(
        "scheduler-start",
        help="Inicia loop continuo de agendamento e execucao.",
    )
    scheduler_start_cmd.add_argument(
        "--poll-seconds",
        type=int,
        default=None,
        help="Intervalo entre ciclos (default vem do config).",
    )
    scheduler_start_cmd.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Limita ciclos (util para teste).",
    )
    scheduler_start_cmd.add_argument(
        "--max-jobs-per-cycle",
        type=int,
        default=None,
        help="Limita jobs por ciclo.",
    )

    scheduler_runs_cmd = subparsers.add_parser(
        "scheduler-runs",
        help="Lista historico de execucoes do scheduler.",
    )
    scheduler_runs_cmd.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Quantidade maxima de execucoes retornadas.",
    )

    api_serve_cmd = subparsers.add_parser(
        "api-serve",
        help="Inicia API HTTP de controle (scheduler + execucao + front).",
    )
    api_serve_cmd.add_argument(
        "--host",
        default=None,
        help="Host de bind da API. Se omitido, usa api.host do config.",
    )
    api_serve_cmd.add_argument(
        "--port",
        type=int,
        default=None,
        help="Porta da API. Se omitido, usa api.port do config.",
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

scheduler:
  db_path: "workspace/runtime/scheduler.db"
  poll_interval_seconds: 30

api:
  host: "127.0.0.1"
  port: 8000
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

    if args.command == "scheduler-init":
        result = init_scheduler_store(config)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-add":
        result = add_scheduler_job(
            config=config,
            name=args.name,
            mode=args.mode,
            every_minutes=args.every_minutes,
            every_hours=args.every_hours,
            daily_at=args.daily_at,
            rows_override=args.rows,
            continue_on_error=args.continue_on_error,
            skip_validation=args.skip_validation,
            enabled=not args.disabled,
            replace=args.replace,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-list":
        result = list_scheduler_jobs(config, include_disabled=not args.only_enabled)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-enable":
        result = set_scheduler_job_enabled(config, job_ref=args.job, enabled=True)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-disable":
        result = set_scheduler_job_enabled(config, job_ref=args.job, enabled=False)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-trigger":
        result = trigger_scheduler_job(config, job_ref=args.job)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-run-once":
        result = run_due_jobs_once(config, max_jobs=args.max_jobs)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-start":
        result = start_scheduler_loop(
            config=config,
            poll_interval_seconds=args.poll_seconds,
            max_cycles=args.max_cycles,
            max_jobs_per_cycle=args.max_jobs_per_cycle,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "scheduler-runs":
        result = list_scheduler_runs(config, limit=args.limit)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    if args.command == "api-serve":
        from .api import run_api_server

        run_api_server(
            config_path=config_path,
            env_file=env_file_path,
            host=args.host,
            port=args.port,
        )
        return

    parser.error(f"Comando nao suportado: {args.command}")


if __name__ == "__main__":
    main()
