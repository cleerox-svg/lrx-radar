from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GeoPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)


class RadarScanIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    scan_id: str = Field(min_length=3, max_length=128)
    source_id: str = Field(min_length=2, max_length=64)
    captured_at: datetime
    azimuth_deg: float = Field(ge=0.0, lt=360.0)
    range_m: float = Field(gt=0.0, le=500_000.0)
    intensity_dbz: float = Field(ge=-32.0, le=95.0)
    quality_hint: float | None = Field(default=None, ge=0.0, le=1.0)
    location: GeoPoint | None = None
    tags: list[str] = Field(default_factory=list)

    @field_validator("captured_at")
    @classmethod
    def normalize_datetime(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("tags")
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for raw_tag in value:
            tag = raw_tag.strip().lower()
            if not tag:
                continue
            if len(tag) > 24:
                raise ValueError("tag length cannot exceed 24 characters")
            if tag not in seen:
                deduped.append(tag)
                seen.add(tag)
        if len(deduped) > 10:
            raise ValueError("a scan cannot have more than 10 tags")
        return deduped


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_batch_id: str | None = Field(default=None, max_length=128)
    scans: list[RadarScanIn] = Field(min_length=1, max_length=500)


class IngestResponse(BaseModel):
    status: Literal["accepted"] = "accepted"
    accepted: int
    queue_depth: int


class ProcessedScan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=64, max_length=64)
    schema_version: Literal["1.0"] = "1.0"
    scan_id: str
    source_id: str
    captured_at: datetime
    ingested_at: datetime
    azimuth_deg: float
    azimuth_rad: float
    range_m: float
    intensity_dbz: float
    normalized_intensity: float = Field(ge=0.0, le=1.0)
    quality_score: float = Field(ge=0.0, le=1.0)
    location: GeoPoint | None = None
    tags: list[str] = Field(default_factory=list)


class DeadLetterRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    event_id: str = Field(min_length=64, max_length=64)
    scan_id: str
    source_id: str
    payload_json: str
    error_message: str
    failed_at: datetime
    retry_attempts: int = Field(ge=0)


class PipelineMetrics(BaseModel):
    queue_depth: int = 0
    accepted: int = 0
    processed: int = 0
    duplicates: int = 0
    retried: int = 0
    dead_lettered: int = 0
    rejected: int = 0
    dead_letter_depth: int = 0
    last_error: str | None = None
    last_processed_at: datetime | None = None
