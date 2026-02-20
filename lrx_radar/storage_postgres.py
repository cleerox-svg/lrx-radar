from __future__ import annotations

import json
from datetime import date, datetime
from threading import Lock
from typing import Any

from lrx_radar.schemas import ProcessedScan


class PostgresRadarStore:
    """PostgreSQL storage with range partitioning on captured day."""

    def __init__(self, database_url: str) -> None:
        try:
            import psycopg  # type: ignore
            from psycopg.rows import dict_row  # type: ignore
        except ImportError as exc:  # pragma: no cover - executed only in PostgreSQL mode
            raise RuntimeError("PostgreSQL mode requires psycopg to be installed.") from exc

        self._psycopg = psycopg
        self._dict_row = dict_row
        self._conn = psycopg.connect(database_url, row_factory=dict_row)
        self._lock = Lock()
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scans (
                        event_id TEXT NOT NULL,
                        captured_day DATE NOT NULL,
                        schema_version TEXT NOT NULL,
                        scan_id TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        captured_at TIMESTAMPTZ NOT NULL,
                        ingested_at TIMESTAMPTZ NOT NULL,
                        azimuth_deg DOUBLE PRECISION NOT NULL,
                        azimuth_rad DOUBLE PRECISION NOT NULL,
                        range_m DOUBLE PRECISION NOT NULL,
                        intensity_dbz DOUBLE PRECISION NOT NULL,
                        normalized_intensity DOUBLE PRECISION NOT NULL,
                        quality_score DOUBLE PRECISION NOT NULL,
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        tags_json JSONB NOT NULL,
                        PRIMARY KEY (event_id, captured_day)
                    ) PARTITION BY RANGE (captured_day)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scans_default
                    PARTITION OF scans DEFAULT
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dead_letters (
                        id BIGSERIAL PRIMARY KEY,
                        event_id TEXT NOT NULL,
                        scan_id TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        error_message TEXT NOT NULL,
                        failed_at TIMESTAMPTZ NOT NULL,
                        retry_attempts INTEGER NOT NULL
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scans_captured_at ON scans (captured_at DESC)"
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_scans_quality ON scans (quality_score)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_scans_source ON scans (source_id)")
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dead_letters_failed_at ON dead_letters (failed_at DESC)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letters (source_id)"
                )
            self._ensure_partition_for_day(date.today())
            self._conn.commit()

    def _ensure_partition_for_day(self, day: date) -> None:
        month_start = day.replace(day=1)
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1)
        partition_name = f"scans_{month_start.year}{month_start.month:02d}"
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF scans
                FOR VALUES FROM (%s) TO (%s)
                """,
                (month_start.isoformat(), month_end.isoformat()),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def insert_scan(self, scan: ProcessedScan) -> bool:
        captured_day = scan.captured_at.date()
        with self._lock:
            self._ensure_partition_for_day(captured_day)
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO scans (
                        event_id, captured_day, schema_version, scan_id, source_id,
                        captured_at, ingested_at, azimuth_deg, azimuth_rad, range_m,
                        intensity_dbz, normalized_intensity, quality_score,
                        latitude, longitude, tags_json
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s::jsonb
                    )
                    ON CONFLICT (event_id, captured_day) DO NOTHING
                    """,
                    (
                        scan.event_id,
                        captured_day,
                        scan.schema_version,
                        scan.scan_id,
                        scan.source_id,
                        scan.captured_at,
                        scan.ingested_at,
                        scan.azimuth_deg,
                        scan.azimuth_rad,
                        scan.range_m,
                        scan.intensity_dbz,
                        scan.normalized_intensity,
                        scan.quality_score,
                        scan.location.latitude if scan.location else None,
                        scan.location.longitude if scan.location else None,
                        json.dumps(scan.tags),
                    ),
                )
                inserted = cursor.rowcount > 0
            self._conn.commit()
            return inserted

    def count_scans(self) -> int:
        return self.count_filtered_scans()

    def count_filtered_scans(
        self,
        source_id: str | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> int:
        where_clauses, params = self._scan_filters(
            source_id=source_id, min_quality=min_quality, max_quality=max_quality
        )
        query = "SELECT COUNT(*) AS c FROM scans"
        if where_clauses:
            query = f"{query} WHERE {' AND '.join(where_clauses)}"
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                return int(row["c"])

    def list_scans(
        self,
        limit: int = 50,
        offset: int = 0,
        source_id: str | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> list[dict[str, Any]]:
        where_clauses, params = self._scan_filters(
            source_id=source_id, min_quality=min_quality, max_quality=max_quality
        )
        query = """
            SELECT
                event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                quality_score, latitude, longitude, tags_json
            FROM scans
        """
        if where_clauses:
            query = f"{query} WHERE {' AND '.join(where_clauses)}"
        query = f"{query} ORDER BY captured_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                return [self._row_to_dict(row) for row in cursor.fetchall()]

    def count_alerts(self, quality_below: float = 0.45, source_id: str | None = None) -> int:
        where_clauses = ["quality_score < %s"]
        params: list[Any] = [quality_below]
        if source_id:
            where_clauses.append("source_id = %s")
            params.append(source_id)
        query = f"SELECT COUNT(*) AS c FROM scans WHERE {' AND '.join(where_clauses)}"
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                return int(row["c"])

    def list_alerts(
        self,
        quality_below: float = 0.45,
        limit: int = 20,
        offset: int = 0,
        source_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where_clauses = ["quality_score < %s"]
        params: list[Any] = [quality_below]
        if source_id:
            where_clauses.append("source_id = %s")
            params.append(source_id)
        query = f"""
            SELECT
                event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                quality_score, latitude, longitude, tags_json
            FROM scans
            WHERE {' AND '.join(where_clauses)}
            ORDER BY quality_score ASC, captured_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                return [self._row_to_dict(row) for row in cursor.fetchall()]

    def list_scans_in_window(
        self,
        *,
        window_start_iso: str,
        source_id: str | None = None,
        limit: int = 5_000,
        require_location: bool = False,
    ) -> list[dict[str, Any]]:
        where_clauses = ["captured_at >= %s"]
        params: list[Any] = [window_start_iso]
        if source_id:
            where_clauses.append("source_id = %s")
            params.append(source_id)
        if require_location:
            where_clauses.append("latitude IS NOT NULL")
            where_clauses.append("longitude IS NOT NULL")
        query = f"""
            SELECT
                event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                quality_score, latitude, longitude, tags_json
            FROM scans
            WHERE {' AND '.join(where_clauses)}
            ORDER BY captured_at ASC
            LIMIT %s
        """
        params.append(limit)
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                return [self._row_to_dict(row) for row in cursor.fetchall()]

    def count_dead_letters(self, source_id: str | None = None) -> int:
        query = "SELECT COUNT(*) AS c FROM dead_letters"
        params: list[Any] = []
        if source_id:
            query = f"{query} WHERE source_id = %s"
            params.append(source_id)
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                return int(row["c"])

    def insert_dead_letter(
        self,
        *,
        event_id: str,
        scan_id: str,
        source_id: str,
        payload_json: str,
        error_message: str,
        failed_at: datetime,
        retry_attempts: int,
    ) -> None:
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dead_letters (
                        event_id, scan_id, source_id, payload_json, error_message, failed_at, retry_attempts
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id,
                        scan_id,
                        source_id,
                        payload_json,
                        error_message[:1024],
                        failed_at,
                        retry_attempts,
                    ),
                )
            self._conn.commit()

    def list_dead_letters(
        self, limit: int = 20, offset: int = 0, source_id: str | None = None
    ) -> list[dict[str, Any]]:
        query = """
            SELECT id, event_id, scan_id, source_id, payload_json, error_message, failed_at, retry_attempts
            FROM dead_letters
        """
        params: list[Any] = []
        if source_id:
            query = f"{query} WHERE source_id = %s"
            params.append(source_id)
        query = f"{query} ORDER BY failed_at DESC, id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self._lock:
            with self._conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return [self._dead_letter_row_to_dict(row) for row in rows]

    @staticmethod
    def _scan_filters(
        *,
        source_id: str | None,
        min_quality: float | None,
        max_quality: float | None,
    ) -> tuple[list[str], list[Any]]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if source_id:
            where_clauses.append("source_id = %s")
            params.append(source_id)
        if min_quality is not None:
            where_clauses.append("quality_score >= %s")
            params.append(min_quality)
        if max_quality is not None:
            where_clauses.append("quality_score <= %s")
            params.append(max_quality)
        return where_clauses, params

    @staticmethod
    def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
        tags_value = row["tags_json"]
        if isinstance(tags_value, str):
            tags = json.loads(tags_value)
        else:
            tags = list(tags_value)
        latitude = row["latitude"]
        longitude = row["longitude"]
        return {
            "event_id": row["event_id"],
            "schema_version": row["schema_version"],
            "scan_id": row["scan_id"],
            "source_id": row["source_id"],
            "captured_at": row["captured_at"].isoformat(),
            "ingested_at": row["ingested_at"].isoformat(),
            "azimuth_deg": row["azimuth_deg"],
            "azimuth_rad": row["azimuth_rad"],
            "range_m": row["range_m"],
            "intensity_dbz": row["intensity_dbz"],
            "normalized_intensity": row["normalized_intensity"],
            "quality_score": row["quality_score"],
            "location": (
                None
                if latitude is None or longitude is None
                else {"latitude": latitude, "longitude": longitude}
            ),
            "tags": tags,
        }

    @staticmethod
    def _dead_letter_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "event_id": row["event_id"],
            "scan_id": row["scan_id"],
            "source_id": row["source_id"],
            "payload_json": row["payload_json"],
            "error_message": row["error_message"],
            "failed_at": row["failed_at"].isoformat(),
            "retry_attempts": row["retry_attempts"],
        }
