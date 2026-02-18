from __future__ import annotations

import json
import sqlite3
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
                "CREATE INDEX IF NOT EXISTS idx_scans_captured_at ON scans (captured_at DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scans_quality ON scans (quality_score)"
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
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) AS c FROM scans")
            row = cursor.fetchone()
            return int(row["c"])

    def list_scans(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                """
                SELECT
                    event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                    azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                    quality_score, latitude, longitude, tags_json
                FROM scans
                ORDER BY captured_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def list_alerts(self, quality_below: float = 0.45, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                """
                SELECT
                    event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
                    azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
                    quality_score, latitude, longitude, tags_json
                FROM scans
                WHERE quality_score < ?
                ORDER BY quality_score ASC, captured_at DESC
                LIMIT ?
                """,
                (quality_below, limit),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]

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
