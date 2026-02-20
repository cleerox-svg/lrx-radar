from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from lrx_radar.broker import AbstractBroker, BrokerMessage
from lrx_radar.pipeline import compute_event_id, normalize_scan
from lrx_radar.schemas import IngestRequest, PipelineMetrics, RadarScanIn


class IngestionService:
    """Queue-driven ingestion service with pluggable broker backends."""

    def __init__(
        self,
        store: Any,
        broker: AbstractBroker,
        *,
        max_retries: int = 2,
        retry_delay_seconds: float = 0.2,
    ) -> None:
        self.store = store
        self.broker = broker
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self._worker_task: asyncio.Task[None] | None = None
        self._metrics = PipelineMetrics()
        self._metrics_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        if self._worker_task is None:
            await self.broker.close()
            self.store.close()
            return
        await self.broker.publish_many([{"type": "control", "action": "stop"}])
        await self._worker_task
        self._worker_task = None
        await self.broker.close()
        self.store.close()

    async def enqueue(self, request: IngestRequest) -> int:
        ingestion_time = datetime.now(timezone.utc)
        payloads = [
            {
                "type": "scan",
                "scan": scan.model_dump(mode="json"),
                "ingested_at": ingestion_time.isoformat(),
                "retry_attempt": 0,
                "event_id": compute_event_id(scan),
            }
            for scan in request.scans
        ]
        await self.broker.publish_many(payloads)
        async with self._metrics_lock:
            self._metrics.accepted += len(payloads)
            self._metrics.queue_depth = await self.broker.depth()
        return len(payloads)

    async def metrics_snapshot(self) -> PipelineMetrics:
        async with self._metrics_lock:
            snapshot = PipelineMetrics(**self._metrics.model_dump())
        snapshot.queue_depth = await self.broker.depth()
        snapshot.dead_letter_depth = self.store.count_dead_letters()
        return snapshot

    async def wait_for_idle(self, timeout_seconds: float = 2.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if await self.broker.depth() <= 0:
                return
            await asyncio.sleep(0.05)
        raise TimeoutError("Broker did not drain before timeout")

    def list_scans(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        source_id: str | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> list[dict[str, Any]]:
        return self.store.list_scans(
            limit=limit,
            offset=offset,
            source_id=source_id,
            min_quality=min_quality,
            max_quality=max_quality,
        )

    def count_scans(
        self,
        *,
        source_id: str | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> int:
        return self.store.count_filtered_scans(
            source_id=source_id,
            min_quality=min_quality,
            max_quality=max_quality,
        )

    def list_alerts(
        self,
        *,
        quality_below: float = 0.45,
        limit: int = 20,
        offset: int = 0,
        source_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.store.list_alerts(
            quality_below=quality_below, limit=limit, offset=offset, source_id=source_id
        )

    def count_alerts(self, *, quality_below: float = 0.45, source_id: str | None = None) -> int:
        return self.store.count_alerts(quality_below=quality_below, source_id=source_id)

    def list_dead_letters(
        self, *, limit: int = 20, offset: int = 0, source_id: str | None = None
    ) -> list[dict[str, Any]]:
        return self.store.list_dead_letters(limit=limit, offset=offset, source_id=source_id)

    def count_dead_letters(self, *, source_id: str | None = None) -> int:
        return self.store.count_dead_letters(source_id=source_id)

    def total_scans(self) -> int:
        return self.store.count_scans()

    def trend_buckets(
        self,
        *,
        window_minutes: int,
        bucket_minutes: int,
        source_id: str | None = None,
        max_points: int = 5_000,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=window_minutes)
        scans = self.store.list_scans_in_window(
            window_start_iso=start.isoformat(),
            source_id=source_id,
            limit=max_points,
            require_location=False,
        )
        bucket_seconds = max(60, bucket_minutes * 60)
        buckets: dict[int, dict[str, float]] = {}
        for scan in scans:
            captured = datetime.fromisoformat(scan["captured_at"])
            bucket_epoch = int(captured.timestamp() // bucket_seconds * bucket_seconds)
            bucket = buckets.setdefault(
                bucket_epoch,
                {"count": 0.0, "quality_sum": 0.0, "intensity_sum": 0.0},
            )
            bucket["count"] += 1.0
            bucket["quality_sum"] += float(scan["quality_score"])
            bucket["intensity_sum"] += float(scan["intensity_dbz"])
        items: list[dict[str, Any]] = []
        for bucket_epoch in sorted(buckets):
            data = buckets[bucket_epoch]
            count = int(data["count"])
            bucket_time = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
            items.append(
                {
                    "bucket_start": bucket_time.isoformat(),
                    "count": count,
                    "avg_quality": round(data["quality_sum"] / count, 4),
                    "avg_intensity_dbz": round(data["intensity_sum"] / count, 3),
                }
            )
        return {"window_minutes": window_minutes, "bucket_minutes": bucket_minutes, "items": items}

    def geospatial_overlay(
        self,
        *,
        window_minutes: int,
        source_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=window_minutes)
        scans = self.store.list_scans_in_window(
            window_start_iso=start.isoformat(),
            source_id=source_id,
            limit=limit,
            require_location=True,
        )
        points = [
            {
                "event_id": scan["event_id"],
                "source_id": scan["source_id"],
                "captured_at": scan["captured_at"],
                "quality_score": scan["quality_score"],
                "intensity_dbz": scan["intensity_dbz"],
                "latitude": scan["location"]["latitude"],
                "longitude": scan["location"]["longitude"],
            }
            for scan in scans
            if scan.get("location")
        ]
        source_counts = Counter(point["source_id"] for point in points)
        return {
            "window_minutes": window_minutes,
            "points": points,
            "source_mix": [
                {"source_id": source_id_value, "count": count}
                for source_id_value, count in source_counts.most_common(8)
            ],
        }

    async def _worker_loop(self) -> None:
        while True:
            message = await self.broker.get(timeout_seconds=1.0)
            if message is None:
                continue
            try:
                payload = message.payload
                if payload.get("type") == "control" and payload.get("action") == "stop":
                    await self.broker.ack(message)
                    return
                if payload.get("type") != "scan":
                    await self.broker.ack(message)
                    continue
                scan = RadarScanIn.model_validate(payload["scan"])
                ingested_at = datetime.fromisoformat(payload["ingested_at"])
                normalized = normalize_scan(scan=scan, ingested_at=ingested_at)
                inserted = self.store.insert_scan(normalized)
                await self.broker.ack(message)
                async with self._metrics_lock:
                    if inserted:
                        self._metrics.processed += 1
                    else:
                        self._metrics.duplicates += 1
                    self._metrics.last_processed_at = datetime.now(timezone.utc)
            except Exception as exc:  # pragma: no cover - defensive handling for worker stability
                await self._handle_failed_message(message=message, error=exc)
            finally:
                async with self._metrics_lock:
                    self._metrics.queue_depth = await self.broker.depth()

    async def _handle_failed_message(self, *, message: BrokerMessage, error: Exception) -> None:
        payload = message.payload
        retry_attempt = int(payload.get("retry_attempt", 0))
        failure_message = f"{type(error).__name__}: {error}"
        if retry_attempt < self.max_retries:
            retry_payload = dict(payload)
            retry_payload["retry_attempt"] = retry_attempt + 1
            delay_seconds = (
                self.retry_delay_seconds * (2**retry_attempt) if self.retry_delay_seconds > 0 else 0.0
            )
            await self.broker.requeue(
                message=message,
                payload=retry_payload,
                delay_seconds=delay_seconds,
            )
            async with self._metrics_lock:
                self._metrics.retried += 1
                self._metrics.last_error = failure_message
            return

        scan_payload = payload.get("scan", {})
        self.store.insert_dead_letter(
            event_id=str(payload.get("event_id", "")),
            scan_id=str(scan_payload.get("scan_id", "unknown")),
            source_id=str(scan_payload.get("source_id", "unknown")),
            payload_json=json.dumps(payload, sort_keys=True, default=str),
            error_message=failure_message,
            failed_at=datetime.now(timezone.utc),
            retry_attempts=retry_attempt,
        )
        await self.broker.ack(message)
        async with self._metrics_lock:
            self._metrics.dead_lettered += 1
            self._metrics.rejected += 1
            self._metrics.last_error = failure_message
            self._metrics.last_processed_at = datetime.now(timezone.utc)
