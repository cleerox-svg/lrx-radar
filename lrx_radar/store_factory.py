from __future__ import annotations

from pathlib import Path
from typing import Any

from lrx_radar.storage import RadarStore
from lrx_radar.storage_postgres import PostgresRadarStore


def build_store(
    *,
    database_path: str | Path | None = None,
    database_url: str | None = None,
) -> Any:
    if database_url and database_url.lower().startswith(("postgres://", "postgresql://")):
        return PostgresRadarStore(database_url=database_url)
    if database_path is None:
        raise RuntimeError("database_path is required when DATABASE_URL is not PostgreSQL.")
    return RadarStore(database_path=database_path)
