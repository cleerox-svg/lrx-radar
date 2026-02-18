from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone

from lrx_radar.schemas import ProcessedScan, RadarScanIn


MAX_RANGE_METERS = 500_000.0
MIN_DBZ = -32.0
MAX_DBZ = 95.0
DBZ_SPAN = MAX_DBZ - MIN_DBZ


def compute_event_id(scan: RadarScanIn) -> str:
    canonical = (
        f"{scan.schema_version}|{scan.scan_id}|{scan.source_id}|"
        f"{scan.captured_at.isoformat()}|{scan.azimuth_deg:.6f}|"
        f"{scan.range_m:.3f}|{scan.intensity_dbz:.3f}"
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_intensity(intensity_dbz: float) -> float:
    return max(0.0, min(1.0, round((intensity_dbz - MIN_DBZ) / DBZ_SPAN, 6)))


def derive_quality_score(scan: RadarScanIn, normalized_intensity: float) -> float:
    hint = 0.65 if scan.quality_hint is None else scan.quality_hint
    range_factor = max(0.0, 1.0 - (scan.range_m / MAX_RANGE_METERS))
    score = (hint * 0.50) + (range_factor * 0.25) + (normalized_intensity * 0.25)
    return max(0.0, min(1.0, round(score, 3)))


def normalize_scan(scan: RadarScanIn, ingested_at: datetime | None = None) -> ProcessedScan:
    now = datetime.now(timezone.utc) if ingested_at is None else ingested_at.astimezone(timezone.utc)
    normalized_intensity = normalize_intensity(scan.intensity_dbz)
    return ProcessedScan(
        event_id=compute_event_id(scan),
        schema_version=scan.schema_version,
        scan_id=scan.scan_id,
        source_id=scan.source_id,
        captured_at=scan.captured_at.astimezone(timezone.utc),
        ingested_at=now,
        azimuth_deg=scan.azimuth_deg,
        azimuth_rad=round(math.radians(scan.azimuth_deg), 6),
        range_m=scan.range_m,
        intensity_dbz=scan.intensity_dbz,
        normalized_intensity=normalized_intensity,
        quality_score=derive_quality_score(scan, normalized_intensity),
        location=scan.location,
        tags=scan.tags,
    )
