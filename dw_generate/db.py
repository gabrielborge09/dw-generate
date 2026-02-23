from __future__ import annotations

import re
import warnings
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import SAWarning


warnings.filterwarnings(
    "ignore",
    message=r"Unrecognized server version info .*",
    category=SAWarning,
)


def make_engine(url: str) -> Engine:
    return create_engine(url, future=True)


def validate_identifier(name: str) -> str:
    if not isinstance(name, str):
        raise ValueError("Identificador deve ser string.")
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Identificador vazio.")
    if re.search(r"[\x00-\x1F]", cleaned):
        raise ValueError(f"Identificador invalido: {name!r}")
    return cleaned


def quote_identifier(name: str, dialect_name: str) -> str:
    safe_name = validate_identifier(name)
    if dialect_name.startswith("postgres"):
        return f'"{safe_name.replace(chr(34), chr(34) * 2)}"'
    if dialect_name.startswith("mssql"):
        return f"[{safe_name.replace(']', ']]')}]"
    if dialect_name.startswith("mysql"):
        return f"`{safe_name.replace('`', '``')}`"
    if dialect_name.startswith("sqlite"):
        return f'"{safe_name.replace(chr(34), chr(34) * 2)}"'
    return f'"{safe_name.replace(chr(34), chr(34) * 2)}"'


def database_exists(engine: Engine, database_name: str) -> bool:
    database_name = validate_identifier(database_name)
    dialect = engine.dialect.name

    with engine.connect() as conn:
        if dialect.startswith("postgres"):
            row = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": database_name},
            ).first()
            return row is not None
        if dialect.startswith("mssql"):
            row = conn.execute(
                text("SELECT 1 FROM sys.databases WHERE name = :name"),
                {"name": database_name},
            ).first()
            return row is not None
        if dialect.startswith("mysql"):
            row = conn.execute(
                text("SHOW DATABASES LIKE :name"),
                {"name": database_name},
            ).first()
            return row is not None
        if dialect.startswith("sqlite"):
            return True

    raise NotImplementedError(
        f"Dialeto {dialect!r} nao suportado para validacao de banco."
    )


def create_database(engine: Engine, database_name: str) -> None:
    database_name = validate_identifier(database_name)
    dialect = engine.dialect.name
    quoted = quote_identifier(database_name, dialect)

    with engine.connect() as conn:
        if dialect.startswith("postgres"):
            conn.execution_options(isolation_level="AUTOCOMMIT").exec_driver_sql(
                f"CREATE DATABASE {quoted}"
            )
            return
        if dialect.startswith("mssql"):
            conn.execution_options(isolation_level="AUTOCOMMIT").exec_driver_sql(
                f"CREATE DATABASE {quoted}"
            )
            return
        if dialect.startswith("mysql"):
            conn.exec_driver_sql(f"CREATE DATABASE {quoted}")
            return
        if dialect.startswith("sqlite"):
            return

    raise NotImplementedError(
        f"Dialeto {dialect!r} nao suportado para criacao de banco."
    )


def build_database_url(admin_url: str, database_name: str) -> str:
    database_name = validate_identifier(database_name)
    url = make_url(admin_url)
    updated: URL = url.set(database=database_name)
    return str(updated)


def get_database_name_from_url(url: str) -> str:
    parsed = make_url(url)
    if not parsed.database:
        raise ValueError(f"URL sem nome de banco: {url!r}")
    return validate_identifier(parsed.database)


def format_schema_table(dialect_name: str, schema: Optional[str], table: str) -> str:
    table_ident = quote_identifier(table, dialect_name)
    if schema:
        schema_ident = quote_identifier(schema, dialect_name)
        return f"{schema_ident}.{table_ident}"
    return table_ident
