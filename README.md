# dw-generate

Automacao em Python para acelerar a criacao de Data Warehouse em SQL Server por camadas (`stage`, `warehouse`, `gold`).

## O que o projeto faz

1. Valida/cria bancos de camada (`stage`, `warehouse`, `gold`).
2. Lista tabelas da base origem e gera amostras com cabecalho.
3. Gera contexto tecnico por tabela para apoiar construcao de SQL.
4. Executa scripts SQL por camada com validacoes de qualidade.
5. Suporta carga `full` e `incremental` com base na coluna `dt_insercao`.

## Criar estagios do projeto

Use o comando abaixo para criar/validar os bancos de camada do DW:

```bash
python -m dw_generate --config config.yaml --env-file .env init-layers
```

Camadas criadas/validadas:
- `stage`
- `warehouse`
- `gold`

## Project Flow

1. Configurar conexoes e variaveis (`config.yaml` + `.env`)
2. Criar estagios (`init-layers`)
3. Descobrir tabelas e amostras (`snapshot-source`)
4. Preparar diretorios SQL (`prepare-sql-dirs`)
5. Gerar scripts SQL por camada
6. Executar carga (`apply-sql`, `run-full` ou `run-incremental`)
7. Operar por agendamento/API (scheduler + front)

## Repository Structure

```text
dw-generate/
  dw_generate/
    api.py
    cli.py
    config.py
    discovery.py
    executor.py
    flows.py
    layers.py
    normalization.py
    runtime_log.py
    scheduler.py
    web/
      index.html
      app.js
      styles.css
  workspace/
    manifests/
    samples/
    sql_context/
    sql_output/
    runtime/
  config.yaml
  .env
  README.md
```

## Data Generation

O script de geração de dados utiliza o Mockaroo para criar dados sintéticos para o banco de dados.

Observacao:
- para ambiente de teste, use um script externo de geracao de massa e carregue os dados na base origem.
- o fluxo principal deste repositorio e orientado a tabelas reais existentes no SQL Server.

## ETL Process (resume)

- **Extract**: leitura das tabelas da base origem SQL Server.
- **Transform**: normalizacao dos campos conforme regras SQL por camada.
- **Load**: carga em `stage`, `warehouse` e `gold`, com modo `full` ou `incremental`.
- **Control**: scheduler, API e logs para rastreabilidade operacional.

## Data Warehouse

Explicação: o esquema em estrela inclui dimensões e tabelas de fatos com base no nível de detalhamento do negócio definido em seus scripts SQL.

Modelo de referencia (exemplo):
- `dim_cliente`
- `dim_produto`
- `dim_tempo`
- `fato_pedidos`

Observacao:
- o projeto nao fixa um unico star schema.
- o desenho final das dimensoes/fatos depende das tabelas de origem e dos scripts gerados para cada dominio.

## Requisitos

- Python 3.10+
- ODBC Driver 18 for SQL Server
- Dependencias Python:
  - `SQLAlchemy`
  - `PyYAML`
  - `sqlparse`
  - `pyodbc`
  - `python-dotenv`
  - `fastapi`
  - `uvicorn`

## Instalar

```bash
pip install -r requirements.txt
```

## Configuracao por `.env`

O projeto usa `config.yaml` com placeholders `${VAR}` e resolve via `.env`.

1. Gerar arquivos base:

```bash
python -m dw_generate init-project
```

Esse comando cria:
- `config.yaml`
- `.env.example`
- `.env` (se ainda nao existir)

2. Ajustar `.env` para seu ambiente real (servidor, instancia, base origem, bases de camada, schema).

## Fluxo de operacao (producao)

1. Criar/validar bancos de camada:

```bash
python -m dw_generate --config config.yaml --env-file .env init-layers
```

2. Gerar snapshots das tabelas da base origem:

```bash
python -m dw_generate --config config.yaml --env-file .env snapshot-source --rows 3
```

3. Criar pastas de scripts SQL por camada:

```bash
python -m dw_generate --config config.yaml --env-file .env prepare-sql-dirs
```

4. Executar scripts SQL:

```bash
python -m dw_generate --config config.yaml --env-file .env apply-sql --mode raw
```

Opcional para pular validacao (nao recomendado):

```bash
python -m dw_generate --config config.yaml --env-file .env apply-sql --skip-validation
```

## Novos comandos de carga

### Carga full (fluxo completo)

Executa: `init-layers` + `snapshot-source` + `prepare-sql-dirs` + `apply-sql` em modo `full`.

```bash
python -m dw_generate --config config.yaml --env-file .env run-full --rows 3
```

No modo `full`, o executor:
- garante coluna `dt_insercao` nas tabelas destino;
- limpa a tabela destino antes da recarga;
- recarrega todos os dados.

### Carga incremental

Executa: `init-layers` + `prepare-sql-dirs` + `apply-sql` em modo `incremental`.

```bash
python -m dw_generate --config config.yaml --env-file .env run-incremental
```

No modo `incremental`, o executor:
- garante coluna `dt_insercao` nas tabelas destino;
- insere apenas registros onde `source.dt_insercao > MAX(target.dt_insercao)`.

## Scheduler + execucao automatica

O scheduler usa SQLite local para registrar jobs e execucoes.

1. Inicializar store do scheduler:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-init
```

2. Criar job incremental a cada 30 minutos:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-add --name incr_30m --mode incremental --every-minutes 30
```

3. Criar job full diario as 02:00 (BRT):

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-add --name full_diario --mode full --daily-at 02:00
```

Para daily sem repeticao (executa uma vez e desabilita):

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-add --name full_unica --mode full --daily-at 2026-02-23T22:00 --daily-once
```

4. Listar jobs:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-list
```

5. Executar jobs vencidos uma vez (modo batch):

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-run-once
```

6. Iniciar loop continuo de agendamento:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-start
```

7. Disparo manual de um job:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-trigger incr_30m
```

8. Historico de execucoes:

```bash
python -m dw_generate --config config.yaml --env-file .env scheduler-runs --limit 50
```

## API de controle + Front

Suba a API (e o front) com:

```bash
python -m dw_generate --config config.yaml --env-file .env api-serve
```

Depois acesse:
- `http://127.0.0.1:8000/` (front)
- `http://127.0.0.1:8000/api/health`

Endpoints principais:
- `GET /api/status` - status geral (scheduler + execucao atual)
- `POST /api/scheduler/start` - inicia loop do scheduler
- `POST /api/scheduler/stop` - para loop do scheduler
- `POST /api/scheduler/run-once` - executa jobs vencidos uma vez
- `POST /api/jobs` - cria/atualiza job (configuracao completa)
  - `daily_at` aceita `HH:MM` ou `YYYY-MM-DDTHH:MM` (BRT)
  - `daily_repeat`: `true` para repetir diariamente, `false` para executar uma vez
- `POST /api/execute/full` - forca carga full
- `POST /api/execute/incremental` - forca carga incremental
- `GET /api/jobs` - lista jobs
- `POST /api/jobs/{job}/trigger` - dispara job manualmente
- `DELETE /api/jobs/{job}` - exclui job e historico associado
- `GET /api/history` - historico de runs do scheduler e execucoes manuais
- `GET /api/logs` - logs persistidos de execucao (scheduler + manual)

## Coluna tecnica de insercao

- O projeto passa a usar `dt_insercao DATETIME2` como coluna tecnica de controle.
- Recomenda-se que os scripts SQL incluam `dt_insercao` no `CREATE TABLE` e no `INSERT/SELECT`.

## Validacoes rigidas de normalizacao

Antes de executar cada `.sql`, o projeto valida:

- exige `CREATE TABLE`
- exige `INSERT ... SELECT` (ou `MERGE ... USING`)
- bloqueia `SELECT *`
- exige alias explicito com `AS`
- exige alias em snake_case
- bloqueia comandos destrutivos (`DROP TABLE`, `TRUNCATE TABLE`, `DELETE FROM`)
- bloqueia sintaxe nao SQL Server (`CREATE TABLE IF NOT EXISTS`, `LIMIT`, cast `::`)
- exige colunas transformadas/normalizadas conforme `normalization` no `config.yaml`

## Estrutura de pastas gerada

```text
workspace/
  samples/
  manifests/
    tables_manifest.json
  sql_context/
  sql_output/
    stage/
    warehouse/
    gold/
```
