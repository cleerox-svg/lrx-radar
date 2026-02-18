from __future__ import annotations

from datetime import datetime, timezone

from lrx_radar.pipeline import compute_event_id, normalize_scan
from lrx_radar.schemas import RadarScanIn


def build_scan() -> RadarScanIn:
    return RadarScanIn(
        schema_version="1.0",
        scan_id="scan-001",
        source_id="station-alpha",
        captured_at=datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),
        azimuth_deg=180.0,
        range_m=25_000.0,
        intensity_dbz=12.5,
        quality_hint=0.8,
        tags=["nominal", "Nominal"],
    )


def test_compute_event_id_is_deterministic() -> None:
    scan = build_scan()
    assert compute_event_id(scan) == compute_event_id(scan)


def test_normalize_scan_sets_expected_ranges() -> None:
    scan = build_scan()
    normalized = normalize_scan(scan)

    assert normalized.event_id
    assert 0.0 <= normalized.normalized_intensity <= 1.0
    assert 0.0 <= normalized.quality_score <= 1.0
    assert normalized.azimuth_rad == 3.141593
    assert normalized.tags == ["nominal"]
