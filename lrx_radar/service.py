from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from lrx_radar.pipeline import normalize_scan
from lrx_radar.schemas import IngestRequest, PipelineMetrics, RadarScanIn
from lrx_radar.storage import RadarStore


class IngestionService:
    """Queue-driven ingestion service that decouples API receipt from processing."""

    def __init__(self, store: RadarStore) -> None:
        self.store = store
        self.queue: asyncio.Queue[tuple[RadarScanIn, datetime] | object] = asyncio.Queue()
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
            self.queue.put_nowait((scan, ingestion_time))
            accepted += 1

        async with self._metrics_lock:
            self._metrics.accepted += accepted
            self._metrics.queue_depth = self.queue.qsize()

        return accepted

    async def metrics_snapshot(self) -> PipelineMetrics:
        async with self._metrics_lock:
            snapshot = PipelineMetrics(**self._metrics.model_dump())
            snapshot.queue_depth = self.queue.qsize()
            return snapshot

    async def wait_for_idle(self, timeout_seconds: float = 2.0) -> None:
        await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)

    def list_scans(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.store.list_scans(limit=limit)

    def list_alerts(self, quality_below: float = 0.45, limit: int = 20) -> list[dict[str, Any]]:
        return self.store.list_alerts(quality_below=quality_below, limit=limit)

    def total_scans(self) -> int:
        return self.store.count_scans()

    async def _worker_loop(self) -> None:
        while True:
            queue_item = await self.queue.get()
            try:
                if queue_item is self._stop_token:
                    return
                scan, ingestion_time = queue_item  # type: ignore[misc]
                normalized = normalize_scan(scan=scan, ingested_at=ingestion_time)
                inserted = self.store.insert_scan(normalized)
                async with self._metrics_lock:
                    if inserted:
                        self._metrics.processed += 1
                    else:
                        self._metrics.duplicates += 1
                    self._metrics.last_processed_at = datetime.now(timezone.utc)
            except Exception as exc:  # pragma: no cover - protective catch for long-lived worker
                async with self._metrics_lock:
                    self._metrics.rejected += 1
                    self._metrics.last_error = str(exc)
            finally:
                self.queue.task_done()
                async with self._metrics_lock:
                    self._metrics.queue_depth = self.queue.qsize()
