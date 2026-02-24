from __future__ import annotations

from typing import Dict

from ..core.config import AppConfig
from ..core.db import build_database_url, create_database, database_exists, make_engine


def ensure_layer_databases(config: AppConfig) -> Dict[str, str]:
    admin_engine = make_engine(config.target.admin_url)
    status: Dict[str, str] = {}

    for layer_name, database_name in config.target.layers.items():
        if database_exists(admin_engine, database_name):
            status[layer_name] = "exists"
            continue
        create_database(admin_engine, database_name)
        status[layer_name] = "created"

    return status


def layer_urls(config: AppConfig) -> Dict[str, str]:
    return {
        layer: build_database_url(config.target.admin_url, database)
        for layer, database in config.target.layers.items()
    }
