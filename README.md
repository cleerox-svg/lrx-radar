# LRX-Radar

LRX-Radar is a lightweight radar ingestion and observability application with:

- pluggable brokered ingestion (in-memory or SQS)
- PostgreSQL-ready partitioned storage (with SQLite fallback for local/dev)
- API-key authn/authz and per-source ingest quotas
- dashboard geospatial overlays and time-window trend analytics

This scaffold is intended to accelerate both UX and backend iteration.

## Architecture

```
Client UI (static dashboard)
        |
        v
 FastAPI /api/v1/ingest  ---->  Broker (memory/SQS) ----> processing worker
        |                                                  |
        |                                                  v
        +-------------------- /api/v1/* metrics/scans <--- PostgreSQL (partitioned) / SQLite
```

### Backend pipeline stages

1. **Receive** - request accepted at `POST /api/v1/ingest`
2. **Validate** - strict Pydantic input models (`schema_version`, range checks, tags)
3. **Normalize** - azimuth radians, normalized intensity, derived quality score
4. **Authorize** - API key role checks + source-scoped access controls
5. **Quota** - per-source per-key ingest quotas (429 on exceed)
6. **Idempotency** - deterministic event hash (`event_id`) dedupe
7. **Store** - PostgreSQL partitioned writes (or SQLite fallback)
8. **Retry** - exponential backoff retries for transient processing failures
9. **Quarantine** - failed records are persisted to a dead-letter table after retry exhaustion
10. **Observe** - pipeline metrics + trend + geospatial analytics

## UI/UX highlights

- Design token based styling (spacing, color, typography, radius)
- Clean dashboard hierarchy with cards, ingestion form, trend charts, geo overlays, scans, alerts, and DLQ view
- Empty/error/loading states for better operability
- Theme toggle with persisted preference (light/dark)
- Server-side scans/alerts filters and pagination controls
- Accessibility basics: semantic structure, labels, focus styles, `aria-live` status text

## Project layout

```
lrx_radar/
  app.py          # FastAPI app + API routes + static mounting
  schemas.py      # Input/output contracts (Pydantic)
  pipeline.py     # Normalization + quality scoring + event IDs
  service.py      # Async queue worker and runtime metrics
  storage.py           # SQLite persistence and query helpers
  storage_postgres.py  # PostgreSQL partitioned persistence
  broker.py            # In-memory + SQS broker backends
  auth.py              # API keys, roles, and source quotas
  store_factory.py     # SQLite/PostgreSQL store selection
  web/
    index.html    # Dashboard UI
    styles.css    # Design tokens + responsive styles
    app.js        # API integration + client-side rendering
tests/
  test_pipeline.py
  test_api.py
requirements.txt
```

## Quickstart

### Prerequisites

- Python 3.10+

### Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

For local test tooling as well:

```bash
pip install -r requirements-dev.txt
```

### Run

```bash
uvicorn lrx_radar.app:app --reload
```

Open `http://127.0.0.1:8000`.

Default API key for local use: `dev-admin-key`

## Deployment notes (Bun lockfile error fix)

If your GitHub-connected deploy platform reports a lockfile error like
`bun.lockb is missing`, force Python build detection instead of Bun/Node.
This repository now includes:

- `Dockerfile` - explicit Python runtime + Uvicorn start command
- `nixpacks.toml` - explicit Nixpacks Python setup/install/start phases
- `Procfile` - explicit web process for platforms that honor Procfiles

These files prevent accidental Bun detection caused by JavaScript assets in
`lrx_radar/web/`.

If you deploy on Railway, `railway.toml` is included to force Dockerfile builds.

For Cloudflare builds, set `PYTHON_VERSION=3.12` in project environment
variables (or ensure `.python-version`/`.tool-versions` is respected).

If Cloudflare runs a deploy command (`npx wrangler deploy`), keep `wrangler.jsonc`
in repo root so Wrangler can publish static assets from `dist/`.

### Cloudflare-only deployment (frontend + backend in one Worker)

This repository includes a Worker entrypoint at `src/worker.js` that serves both:

- static UI assets
- API endpoints under `/api/v1/*`

Use these commands in Cloudflare Worker Git build settings:

**Build command**

```bash
mkdir -p dist && cp -r lrx_radar/web/* dist/
```

**Deploy command**

```bash
npx wrangler deploy --config wrangler.jsonc
```

Optional (for persistent storage): add a D1 binding named `DB`.

Optional (for queued ingestion): add a Queue producer binding named
`INGEST_QUEUE` and configure this same Worker as consumer.

## Runtime configuration

- `DATABASE_URL` - use `postgres://...` or `postgresql://...` for PostgreSQL mode
- `BROKER_BACKEND` - `memory` (default) or `sqs`
- `SQS_QUEUE_URL` - required when `BROKER_BACKEND=sqs`
- `API_KEY_CONFIG` - API key config string, e.g. `dev-admin-key:admin:*,reader:read:station-`
- `INGEST_QUOTA_PER_MINUTE` - default per-source quota per key (default `2000`)
- `SOURCE_QUOTAS` - source-specific overrides, e.g. `station-alpha=500,station-beta=1200`
- `CORS_ALLOWED_ORIGINS` - comma-separated origins allowed by browser clients (default `*`)

## API overview

- `GET /api/v1/health` - service status
- `POST /api/v1/ingest` - queue a batch of radar scans
- `GET /api/v1/scans?limit=25&offset=0&source_id=&min_quality=` - paginated, filterable scans
- `GET /api/v1/alerts?quality_below=0.45&limit=20&offset=0` - paginated, filterable alerts
- `GET /api/v1/dead-letters?limit=20&offset=0` - failed records that exhausted retries
- `GET /api/v1/metrics` - ingestion pipeline metrics
- `GET /api/v1/analytics/trends?window_minutes=120&bucket_minutes=5` - time-window trends
- `GET /api/v1/analytics/geospatial?window_minutes=180&limit=300` - geo overlay points
- `GET /api/v1/dashboard` - combined payload for UI
- `GET /api/v1/auth/me` - current key role/scopes

### Ingest payload example

```json
{
  "source_batch_id": "batch-123",
  "scans": [
    {
      "schema_version": "1.0",
      "scan_id": "scan-001",
      "source_id": "station-alpha",
      "captured_at": "2026-02-18T12:00:00Z",
      "azimuth_deg": 180.0,
      "range_m": 15000.0,
      "intensity_dbz": 8.3,
      "quality_hint": 0.7,
      "tags": ["nominal"]
    }
  ]
}
```

## Testing

```bash
pytest
```
