from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from lrx_radar.schemas import IngestRequest, IngestResponse
from lrx_radar.service import IngestionService
from lrx_radar.storage import RadarStore


def create_app(
    database_path: str | Path | None = None,
    *,
    max_retries: int = 2,
    retry_delay_seconds: float = 0.2,
) -> FastAPI:
    static_dir = Path(__file__).resolve().parent / "web"
    resolved_database = (
        Path(database_path)
        if database_path is not None
        else Path(__file__).resolve().parent.parent / "data" / "radar.db"
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = RadarStore(resolved_database)
        service = IngestionService(
            store=store, max_retries=max_retries, retry_delay_seconds=retry_delay_seconds
        )
        await service.start()
        app.state.ingestion_service = service
        yield
        await service.stop()

    app = FastAPI(
        title="LRX Radar",
        description="Radar ingestion and dashboard service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/v1/health")
    async def health() -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        metrics = await service.metrics_snapshot()
        return {"status": "ok", "queue_depth": metrics.queue_depth}

    @app.post("/api/v1/ingest", response_model=IngestResponse, status_code=202)
    async def ingest(payload: IngestRequest) -> IngestResponse:
        service: IngestionService = app.state.ingestion_service
        accepted = await service.enqueue(payload)
        metrics = await service.metrics_snapshot()
        return IngestResponse(accepted=accepted, queue_depth=metrics.queue_depth)

    @app.get("/api/v1/scans")
    async def scans(
        limit: int = Query(default=25, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
        min_quality: float | None = Query(default=None, ge=0.0, le=1.0),
        max_quality: float | None = Query(default=None, ge=0.0, le=1.0),
    ) -> dict[str, object]:
        if min_quality is not None and max_quality is not None and min_quality > max_quality:
            raise HTTPException(status_code=400, detail="min_quality cannot exceed max_quality")
        service: IngestionService = app.state.ingestion_service
        return {
            "items": service.list_scans(
                limit=limit,
                offset=offset,
                source_id=source_id,
                min_quality=min_quality,
                max_quality=max_quality,
            ),
            "total": service.count_scans(
                source_id=source_id, min_quality=min_quality, max_quality=max_quality
            ),
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/v1/alerts")
    async def alerts(
        quality_below: float = Query(default=0.45, ge=0.0, le=1.0),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
    ) -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        return {
            "items": service.list_alerts(
                quality_below=quality_below,
                limit=limit,
                offset=offset,
                source_id=source_id,
            ),
            "total": service.count_alerts(quality_below=quality_below, source_id=source_id),
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/v1/dead-letters")
    async def dead_letters(
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
    ) -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        return {
            "items": service.list_dead_letters(limit=limit, offset=offset, source_id=source_id),
            "total": service.count_dead_letters(source_id=source_id),
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/v1/metrics")
    async def metrics() -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        snapshot = await service.metrics_snapshot()
        return {
            **snapshot.model_dump(),
            "stored_scans": service.count_scans(),
        }

    @app.get("/api/v1/dashboard")
    async def dashboard(limit: int = Query(default=15, ge=5, le=100)) -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        snapshot = await service.metrics_snapshot()
        return {
            "metrics": {**snapshot.model_dump(), "stored_scans": service.count_scans()},
            "scans": service.list_scans(limit=limit, offset=0),
            "alerts": service.list_alerts(limit=min(limit, 20), offset=0),
            "dead_letters": service.list_dead_letters(limit=5, offset=0),
        }

    return app


app = create_app()
