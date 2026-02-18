from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lrx_radar.pipeline import compute_event_id, normalize_scan
from lrx_radar.schemas import IngestRequest, PipelineMetrics, RadarScanIn
from lrx_radar.storage import RadarStore


@dataclass(slots=True)
class QueuedScan:
    scan: RadarScanIn
    ingested_at: datetime
    retry_attempt: int = 0
    event_id: str = ""


class IngestionService:
    """Queue-driven ingestion service that decouples API receipt from processing."""

    def __init__(
        self, store: RadarStore, *, max_retries: int = 2, retry_delay_seconds: float = 0.2
    ) -> None:
        self.store = store
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.queue: asyncio.Queue[QueuedScan | object] = asyncio.Queue()
        self._stop_token = object()
        self._worker_task: asyncio.Task[None] | None = None
        self._metrics = PipelineMetrics()
        self._metrics_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        if self._worker_task is None:
            self.store.close()
            return
        await self.queue.put(self._stop_token)
        await self._worker_task
        self._worker_task = None
        self.store.close()

    async def enqueue(self, request: IngestRequest) -> int:
        accepted = 0
        ingestion_time = datetime.now(timezone.utc)
        for scan in request.scans:
            self.queue.put_nowait(
                QueuedScan(
                    scan=scan,
                    ingested_at=ingestion_time,
                    retry_attempt=0,
                    event_id=compute_event_id(scan),
                )
            )
            accepted += 1

        async with self._metrics_lock:
            self._metrics.accepted += accepted
            self._metrics.queue_depth = self.queue.qsize()

        return accepted

    async def metrics_snapshot(self) -> PipelineMetrics:
        async with self._metrics_lock:
            snapshot = PipelineMetrics(**self._metrics.model_dump())
            snapshot.queue_depth = self.queue.qsize()
        snapshot.dead_letter_depth = self.store.count_dead_letters()
        return snapshot

    async def wait_for_idle(self, timeout_seconds: float = 2.0) -> None:
        await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)

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

    async def _worker_loop(self) -> None:
        while True:
            queue_item = await self.queue.get()
            try:
                if queue_item is self._stop_token:
                    return
                item = queue_item
                normalized = normalize_scan(scan=item.scan, ingested_at=item.ingested_at)
                inserted = self.store.insert_scan(normalized)
                async with self._metrics_lock:
                    if inserted:
                        self._metrics.processed += 1
                    else:
                        self._metrics.duplicates += 1
                    self._metrics.last_processed_at = datetime.now(timezone.utc)
            except Exception as exc:  # pragma: no cover - defensive handling for worker stability
                await self._handle_failed_item(item=item, error=exc)
            finally:
                self.queue.task_done()
                async with self._metrics_lock:
                    self._metrics.queue_depth = self.queue.qsize()

    async def _handle_failed_item(self, *, item: QueuedScan, error: Exception) -> None:
        failure_message = f"{type(error).__name__}: {error}"
        if item.retry_attempt < self.max_retries:
            next_attempt = item.retry_attempt + 1
            if self.retry_delay_seconds > 0:
                # Exponential backoff helps avoid hot-loop failures.
                await asyncio.sleep(self.retry_delay_seconds * (2 ** item.retry_attempt))
            self.queue.put_nowait(
                QueuedScan(
                    scan=item.scan,
                    ingested_at=item.ingested_at,
                    retry_attempt=next_attempt,
                    event_id=item.event_id,
                )
            )
            async with self._metrics_lock:
                self._metrics.retried += 1
                self._metrics.last_error = failure_message
            return

        self.store.insert_dead_letter(
            event_id=item.event_id,
            scan_id=item.scan.scan_id,
            source_id=item.scan.source_id,
            payload_json=item.scan.model_dump_json(),
            error_message=failure_message,
            failed_at=datetime.now(timezone.utc),
            retry_attempts=item.retry_attempt,
        )
        async with self._metrics_lock:
            self._metrics.dead_lettered += 1
            self._metrics.rejected += 1
            self._metrics.last_error = failure_message
            self._metrics.last_processed_at = datetime.now(timezone.utc)
