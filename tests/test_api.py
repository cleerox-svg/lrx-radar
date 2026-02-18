from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from lrx_radar.app import create_app


def build_payload(size: int = 2) -> dict[str, object]:
    now = datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc)
    scans: list[dict[str, object]] = []
    for index in range(size):
        scans.append(
            {
                "schema_version": "1.0",
                "scan_id": f"scan-{index}",
                "source_id": "station-alpha",
                "captured_at": now.isoformat(),
                "azimuth_deg": float(index * 45),
                "range_m": 15_000 + (index * 1_000),
                "intensity_dbz": -12.0 + (index * 3.0),
                "quality_hint": 0.7,
                "tags": ["integration"],
            }
        )
    return {"source_batch_id": "batch-1", "scans": scans}


def wait_for_processing(client: TestClient, expected: int, timeout_seconds: float = 3.0) -> dict[str, object]:
    end = time.time() + timeout_seconds
    while time.time() < end:
        metrics = client.get("/api/v1/metrics").json()
        if int(metrics["processed"]) + int(metrics["duplicates"]) >= expected:
            return metrics
        time.sleep(0.05)
    raise AssertionError("processing did not complete before timeout")


def test_ingest_and_list_scans(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "test.db")

    with TestClient(app) as client:
        response = client.post("/api/v1/ingest", json=build_payload(size=2))
        assert response.status_code == 202
        assert response.json()["accepted"] == 2

        wait_for_processing(client, expected=2)
        scans_response = client.get("/api/v1/scans?limit=5")
        assert scans_response.status_code == 200
        scans = scans_response.json()["items"]
        assert len(scans) == 2
        assert scans[0]["source_id"] == "station-alpha"

        dashboard_response = client.get("/api/v1/dashboard?limit=10")
        assert dashboard_response.status_code == 200
        dashboard = dashboard_response.json()
        assert dashboard["metrics"]["stored_scans"] == 2


def test_duplicate_scan_detection(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "dedupe.db")
    payload = build_payload(size=1)

    with TestClient(app) as client:
        first = client.post("/api/v1/ingest", json=payload)
        second = client.post("/api/v1/ingest", json=payload)
        assert first.status_code == 202
        assert second.status_code == 202

        metrics = wait_for_processing(client, expected=2)
        assert metrics["stored_scans"] == 1
        assert metrics["duplicates"] >= 1
