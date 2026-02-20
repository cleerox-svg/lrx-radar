from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from lrx_radar.app import create_app

AUTH_HEADERS = {"x-api-key": "dev-admin-key"}


def build_payload(
    size: int = 2, *, source_id: str = "station-alpha", scan_prefix: str = "scan"
) -> dict[str, object]:
    now = datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc)
    scans: list[dict[str, object]] = []
    for index in range(size):
        scans.append(
            {
                "schema_version": "1.0",
                "scan_id": f"{scan_prefix}-{index}",
                "source_id": source_id,
                "captured_at": now.isoformat(),
                "azimuth_deg": float(index * 45),
                "range_m": 15_000 + (index * 1_000),
                "intensity_dbz": -12.0 + (index * 3.0),
                "quality_hint": 0.7,
                "tags": ["integration"],
            }
        )
    return {"source_batch_id": f"batch-{scan_prefix}", "scans": scans}


def wait_for_processing(client: TestClient, expected: int, timeout_seconds: float = 3.0) -> dict[str, object]:
    end = time.time() + timeout_seconds
    while time.time() < end:
        metrics = client.get("/api/v1/metrics", headers=AUTH_HEADERS).json()
        terminal = int(metrics["processed"]) + int(metrics["duplicates"]) + int(
            metrics["dead_lettered"]
        )
        if terminal >= expected:
            return metrics
        time.sleep(0.05)
    raise AssertionError("processing did not complete before timeout")


def test_ingest_and_list_scans(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "test.db")

    with TestClient(app) as client:
        response = client.post("/api/v1/ingest", json=build_payload(size=2), headers=AUTH_HEADERS)
        assert response.status_code == 202
        assert response.json()["accepted"] == 2

        wait_for_processing(client, expected=2)
        scans_response = client.get("/api/v1/scans?limit=5", headers=AUTH_HEADERS)
        assert scans_response.status_code == 200
        scans_payload = scans_response.json()
        scans = scans_payload["items"]
        assert scans_payload["total"] == 2
        assert len(scans) == 2
        assert scans[0]["source_id"] == "station-alpha"

        dashboard_response = client.get("/api/v1/dashboard?limit=10", headers=AUTH_HEADERS)
        assert dashboard_response.status_code == 200
        dashboard = dashboard_response.json()
        assert dashboard["metrics"]["stored_scans"] == 2
        assert "dead_letters" in dashboard
        assert "trends" in dashboard
        assert "geospatial" in dashboard


def test_duplicate_scan_detection(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "dedupe.db")
    payload = build_payload(size=1)

    with TestClient(app) as client:
        first = client.post("/api/v1/ingest", json=payload, headers=AUTH_HEADERS)
        second = client.post("/api/v1/ingest", json=payload, headers=AUTH_HEADERS)
        assert first.status_code == 202
        assert second.status_code == 202

        metrics = wait_for_processing(client, expected=2)
        assert metrics["stored_scans"] == 1
        assert metrics["duplicates"] >= 1
        dead_letters = client.get("/api/v1/dead-letters", headers=AUTH_HEADERS).json()
        assert dead_letters["total"] == 0


def test_pagination_and_source_filtering(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "filters.db")
    payload_a = build_payload(size=5, source_id="station-alpha", scan_prefix="alpha")
    payload_b = build_payload(size=4, source_id="station-beta", scan_prefix="beta")

    with TestClient(app) as client:
        assert client.post("/api/v1/ingest", json=payload_a, headers=AUTH_HEADERS).status_code == 202
        assert client.post("/api/v1/ingest", json=payload_b, headers=AUTH_HEADERS).status_code == 202
        wait_for_processing(client, expected=9)

        first_page = client.get("/api/v1/scans?limit=3&offset=0", headers=AUTH_HEADERS).json()
        second_page = client.get("/api/v1/scans?limit=3&offset=3", headers=AUTH_HEADERS).json()
        assert first_page["total"] == 9
        assert second_page["total"] == 9
        assert len(first_page["items"]) == 3
        assert len(second_page["items"]) == 3

        beta_only = client.get(
            "/api/v1/scans?limit=10&offset=0&source_id=station-beta",
            headers=AUTH_HEADERS,
        ).json()
        assert beta_only["total"] == 4
        assert all(item["source_id"] == "station-beta" for item in beta_only["items"])

        alerts = client.get(
            "/api/v1/alerts?quality_below=1&limit=2&offset=0&source_id=station-alpha",
            headers=AUTH_HEADERS,
        ).json()
        assert alerts["total"] == 5
        assert len(alerts["items"]) == 2


def test_retry_exhaustion_moves_to_dead_letter(tmp_path: Path, monkeypatch) -> None:
    app = create_app(database_path=tmp_path / "dlq.db", max_retries=1, retry_delay_seconds=0.0)

    def broken_normalize(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("forced normalize failure")

    monkeypatch.setattr("lrx_radar.service.normalize_scan", broken_normalize)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/ingest",
            json=build_payload(size=1, scan_prefix="broken"),
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 202

        metrics = wait_for_processing(client, expected=1)
        assert metrics["processed"] == 0
        assert metrics["retried"] >= 1
        assert metrics["dead_lettered"] == 1
        assert metrics["rejected"] == 1

        dead_letters = client.get(
            "/api/v1/dead-letters?limit=5&offset=0",
            headers=AUTH_HEADERS,
        ).json()
        assert dead_letters["total"] == 1
        assert "forced normalize failure" in dead_letters["items"][0]["error_message"]


def test_auth_required(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "auth.db")
    with TestClient(app) as client:
        response = client.get("/api/v1/scans?limit=5")
        assert response.status_code == 401


def test_source_quota_enforced(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "quota.db", default_quota_per_minute=1)
    with TestClient(app) as client:
        first = client.post(
            "/api/v1/ingest",
            headers=AUTH_HEADERS,
            json=build_payload(size=1, source_id="station-alpha", scan_prefix="quota-a"),
        )
        second = client.post(
            "/api/v1/ingest",
            headers=AUTH_HEADERS,
            json=build_payload(size=1, source_id="station-alpha", scan_prefix="quota-b"),
        )
        assert first.status_code == 202
        assert second.status_code == 429


def test_trend_and_geo_analytics(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "analytics.db")
    payload = build_payload(size=3, source_id="station-alpha", scan_prefix="analytic")
    for idx, scan in enumerate(payload["scans"]):  # type: ignore[index]
        scan["location"] = {"latitude": 14.61 + (idx * 0.01), "longitude": 121.02 + (idx * 0.01)}
    with TestClient(app) as client:
        assert client.post("/api/v1/ingest", json=payload, headers=AUTH_HEADERS).status_code == 202
        wait_for_processing(client, expected=3)

        trends = client.get(
            "/api/v1/analytics/trends?window_minutes=240&bucket_minutes=10",
            headers=AUTH_HEADERS,
        ).json()
        assert "items" in trends
        assert len(trends["items"]) >= 1

        geo = client.get(
            "/api/v1/analytics/geospatial?window_minutes=240&limit=50",
            headers=AUTH_HEADERS,
        ).json()
        assert len(geo["points"]) == 3
