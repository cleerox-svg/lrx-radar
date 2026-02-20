from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from lrx_radar.auth import (
    ROLE_ADMIN,
    ROLE_INGEST,
    ROLE_READ,
    AuthManager,
    Principal,
    SourceQuotaManager,
    api_key_from_header,
    parse_source_quota_overrides,
)
from lrx_radar.broker import build_broker
from lrx_radar.schemas import IngestRequest, IngestResponse
from lrx_radar.service import IngestionService
from lrx_radar.store_factory import build_store


def create_app(
    database_path: str | Path | None = None,
    *,
    database_url: str | None = None,
    broker_backend: str | None = None,
    sqs_queue_url: str | None = None,
    aws_region: str | None = None,
    api_key_config: str | None = None,
    default_quota_per_minute: int | None = None,
    source_quota_config: str | None = None,
    max_retries: int = 2,
    retry_delay_seconds: float = 0.2,
) -> FastAPI:
    static_dir = Path(__file__).resolve().parent / "web"
    resolved_database_path = (
        Path(database_path)
        if database_path is not None
        else Path(__file__).resolve().parent.parent / "data" / "radar.db"
    )
    resolved_database_url = database_url or os.getenv("DATABASE_URL")
    resolved_broker_backend = broker_backend or os.getenv("BROKER_BACKEND", "memory")
    resolved_sqs_queue_url = sqs_queue_url or os.getenv("SQS_QUEUE_URL")
    resolved_aws_region = aws_region or os.getenv("AWS_REGION")
    resolved_api_key_config = api_key_config or os.getenv("API_KEY_CONFIG")
    resolved_default_quota = (
        default_quota_per_minute
        if default_quota_per_minute is not None
        else int(os.getenv("INGEST_QUOTA_PER_MINUTE", "2000"))
    )
    resolved_source_quota_config = source_quota_config or os.getenv("SOURCE_QUOTAS")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = build_store(
            database_path=resolved_database_path,
            database_url=resolved_database_url,
        )
        broker = build_broker(
            backend=resolved_broker_backend,
            sqs_queue_url=resolved_sqs_queue_url,
            aws_region=resolved_aws_region,
        )
        service = IngestionService(
            store=store,
            broker=broker,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
        auth_manager = AuthManager(resolved_api_key_config)
        quota_manager = SourceQuotaManager(
            default_per_minute=resolved_default_quota,
            per_source_overrides=parse_source_quota_overrides(resolved_source_quota_config),
        )
        await service.start()
        app.state.ingestion_service = service
        app.state.auth_manager = auth_manager
        app.state.quota_manager = quota_manager
        yield
        await service.stop()

    app = FastAPI(
        title="LRX Radar",
        description="Radar ingestion and dashboard service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def principal_from_key(api_key: str | None = Depends(api_key_from_header)) -> Principal:
        auth_manager: AuthManager = app.state.auth_manager
        return auth_manager.authenticate(api_key)

    def require_roles(*roles: str):
        def dependency(principal: Principal = Depends(principal_from_key)) -> Principal:
            auth_manager: AuthManager = app.state.auth_manager
            auth_manager.authorize(principal, roles)
            return principal

        return dependency

    def validate_source_scope(principal: Principal, source_id: str | None) -> None:
        auth_manager: AuthManager = app.state.auth_manager
        if source_id is None:
            if not principal.is_admin and "*" not in principal.source_scopes:
                raise HTTPException(
                    status_code=400,
                    detail="source_id is required for scoped API keys.",
                )
            return
        auth_manager.enforce_source_scope(principal, source_id)

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/v1/health")
    async def health() -> dict[str, object]:
        service: IngestionService = app.state.ingestion_service
        metrics = await service.metrics_snapshot()
        return {"status": "ok", "queue_depth": metrics.queue_depth}

    @app.get("/api/v1/auth/me")
    async def me(principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN))) -> dict[str, object]:
        return {
            "role": principal.role,
            "source_scopes": principal.source_scopes,
            "api_key_preview": f"{principal.api_key[:4]}***",
        }

    @app.post("/api/v1/ingest", response_model=IngestResponse, status_code=202)
    async def ingest(
        payload: IngestRequest,
        principal: Principal = Depends(require_roles(ROLE_INGEST, ROLE_ADMIN)),
    ) -> IngestResponse:
        auth_manager: AuthManager = app.state.auth_manager
        quota_manager: SourceQuotaManager = app.state.quota_manager
        for scan in payload.scans:
            auth_manager.enforce_source_scope(principal, scan.source_id)
        quota_manager.consume(principal, payload.scans)
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
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        if min_quality is not None and max_quality is not None and min_quality > max_quality:
            raise HTTPException(status_code=400, detail="min_quality cannot exceed max_quality")
        validate_source_scope(principal, source_id)
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
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        validate_source_scope(principal, source_id)
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
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        validate_source_scope(principal, source_id)
        service: IngestionService = app.state.ingestion_service
        return {
            "items": service.list_dead_letters(limit=limit, offset=offset, source_id=source_id),
            "total": service.count_dead_letters(source_id=source_id),
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/v1/metrics")
    async def metrics(
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        _ = principal
        service: IngestionService = app.state.ingestion_service
        snapshot = await service.metrics_snapshot()
        return {
            **snapshot.model_dump(),
            "stored_scans": service.count_scans(),
        }

    @app.get("/api/v1/analytics/trends")
    async def trends(
        window_minutes: int = Query(default=120, ge=5, le=1440),
        bucket_minutes: int = Query(default=5, ge=1, le=60),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        validate_source_scope(principal, source_id)
        service: IngestionService = app.state.ingestion_service
        return service.trend_buckets(
            window_minutes=window_minutes,
            bucket_minutes=bucket_minutes,
            source_id=source_id,
        )

    @app.get("/api/v1/analytics/geospatial")
    async def geospatial(
        window_minutes: int = Query(default=180, ge=5, le=1440),
        limit: int = Query(default=300, ge=10, le=2000),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        validate_source_scope(principal, source_id)
        service: IngestionService = app.state.ingestion_service
        return service.geospatial_overlay(
            window_minutes=window_minutes,
            source_id=source_id,
            limit=limit,
        )

    @app.get("/api/v1/dashboard")
    async def dashboard(
        limit: int = Query(default=15, ge=5, le=100),
        trend_window_minutes: int = Query(default=120, ge=5, le=1440),
        trend_bucket_minutes: int = Query(default=5, ge=1, le=60),
        geo_window_minutes: int = Query(default=180, ge=5, le=1440),
        source_id: str | None = Query(default=None, min_length=2, max_length=64),
        principal: Principal = Depends(require_roles(ROLE_READ, ROLE_INGEST, ROLE_ADMIN)),
    ) -> dict[str, object]:
        validate_source_scope(principal, source_id)
        service: IngestionService = app.state.ingestion_service
        snapshot = await service.metrics_snapshot()
        return {
            "metrics": {**snapshot.model_dump(), "stored_scans": service.count_scans()},
            "scans": service.list_scans(limit=limit, offset=0, source_id=source_id),
            "alerts": service.list_alerts(limit=min(limit, 20), offset=0, source_id=source_id),
            "dead_letters": service.list_dead_letters(limit=5, offset=0, source_id=source_id),
            "trends": service.trend_buckets(
                window_minutes=trend_window_minutes,
                bucket_minutes=trend_bucket_minutes,
                source_id=source_id,
            ),
            "geospatial": service.geospatial_overlay(
                window_minutes=geo_window_minutes,
                source_id=source_id,
                limit=min(limit * 20, 400),
            ),
        }

    return app


app = create_app()
