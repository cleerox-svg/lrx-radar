from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from lrx_radar.schemas import ProcessedScan


class RadarStore:
    """SQLite-backed storage for processed radar scans."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.database_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = Lock()
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scans (
                    event_id TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    scan_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    azimuth_deg REAL NOT NULL,
                    azimuth_rad REAL NOT NULL,
                    range_m REAL NOT NULL,
                    intensity_dbz REAL NOT NULL,
                    normalized_intensity REAL NOT NULL,
                    quality_score REAL NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    tags_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    scan_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    failed_at TEXT NOT NULL,
                    retry_attempts INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scans_captured_at ON scans (captured_at DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scans_quality ON scans (quality_score)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scans_source ON scans (source_id)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dead_letters_failed_at ON dead_letters (failed_at DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letters (source_id)"
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def insert_scan(self, scan: ProcessedScan) -> bool:
        payload = (
            scan.event_id,
            scan.schema_version,
            scan.scan_id,
            scan.source_id,
            scan.captured_at.isoformat(),
            scan.ingested_at.isoformat(),
            scan.azimuth_deg,
            scan.azimuth_rad,
            scan.range_m,
            scan.intensity_dbz,
            scan.normalized_intensity,
            scan.quality_score,
            scan.location.latitude if scan.location else None,
            scan.location.longitude if scan.location else None,
            json.dumps(scan.tags),
        )
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO scans (
                        event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                        azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                        quality_score, latitude, longitude, tags_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def count_scans(self) -> int:
        return self.count_filtered_scans()

    def count_filtered_scans(
        self,
        source_id: str | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> int:
        where_clauses, where_params = self._scan_filters(
            source_id=source_id, min_quality=min_quality, max_quality=max_quality
        )
        query = "SELECT COUNT(*) AS c FROM scans"
        if where_clauses:
            query = f"{query} WHERE {' AND '.join(where_clauses)}"
        with self._lock:
            cursor = self._conn.execute(query, tuple(where_params))
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
        where_clauses, where_params = self._scan_filters(
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
        query = f"{query} ORDER BY captured_at DESC LIMIT ? OFFSET ?"
        params = [*where_params, limit, offset]
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def count_alerts(self, quality_below: float = 0.45, source_id: str | None = None) -> int:
        where_clauses = ["quality_score < ?"]
        params: list[Any] = [quality_below]
        if source_id:
            where_clauses.append("source_id = ?")
            params.append(source_id)
        query = f"SELECT COUNT(*) AS c FROM scans WHERE {' AND '.join(where_clauses)}"
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
            row = cursor.fetchone()
            return int(row["c"])

    def list_alerts(
        self,
        quality_below: float = 0.45,
        limit: int = 20,
        offset: int = 0,
        source_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where_clauses = ["quality_score < ?"]
        params: list[Any] = [quality_below]
        if source_id:
            where_clauses.append("source_id = ?")
            params.append(source_id)
        query = f"""
            SELECT
                event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                quality_score, latitude, longitude, tags_json
            FROM scans
            WHERE {' AND '.join(where_clauses)}
            ORDER BY quality_score ASC, captured_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def count_dead_letters(self, source_id: str | None = None) -> int:
        query = "SELECT COUNT(*) AS c FROM dead_letters"
        params: list[Any] = []
        if source_id:
            query = f"{query} WHERE source_id = ?"
            params.append(source_id)
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
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
            self._conn.execute(
                """
                INSERT INTO dead_letters (
                    event_id, scan_id, source_id, payload_json, error_message, failed_at, retry_attempts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    scan_id,
                    source_id,
                    payload_json,
                    error_message[:1024],
                    failed_at.isoformat(),
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
            query = f"{query} WHERE source_id = ?"
            params.append(source_id)
        query = f"{query} ORDER BY failed_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
            return [self._dead_letter_row_to_dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        latitude = row["latitude"]
        longitude = row["longitude"]
        return {
            "event_id": row["event_id"],
            "schema_version": row["schema_version"],
            "scan_id": row["scan_id"],
            "source_id": row["source_id"],
            "captured_at": row["captured_at"],
            "ingested_at": row["ingested_at"],
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
            "tags": json.loads(row["tags_json"]),
        }

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
            where_clauses.append("source_id = ?")
            params.append(source_id)
        if min_quality is not None:
            where_clauses.append("quality_score >= ?")
            params.append(min_quality)
        if max_quality is not None:
            where_clauses.append("quality_score <= ?")
            params.append(max_quality)
        return where_clauses, params

    @staticmethod
    def _dead_letter_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "event_id": row["event_id"],
            "scan_id": row["scan_id"],
            "source_id": row["source_id"],
            "payload_json": row["payload_json"],
            "error_message": row["error_message"],
            "failed_at": row["failed_at"],
            "retry_attempts": row["retry_attempts"],
        }
