# dw-generate

Automacao em Python para acelerar a criacao de Data Warehouse em SQL Server por camadas (`stage`, `warehouse`, `gold`).

## O que o projeto faz

1. Valida/cria bancos de camada (`stage`, `warehouse`, `gold`).
2. Lista tabelas da base origem e gera amostras com cabecalho.
3. Gera contexto tecnico por tabela para apoiar construcao de SQL.
4. Executa scripts SQL por camada com validacoes de qualidade.
5. Suporta carga `full` e `incremental` com base na coluna `dt_insercao`.

## Requisitos

- Python 3.10+
- ODBC Driver 18 for SQL Server
- Dependencias Python:
  - `SQLAlchemy`
  - `PyYAML`
  - `sqlparse`
  - `pyodbc`
  - `python-dotenv`

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


