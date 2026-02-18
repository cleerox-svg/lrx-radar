# LRX-Radar

LRX-Radar is a lightweight radar ingestion and observability application with:

- a queue-driven backend pipeline for validation, normalization, dedupe, and storage
- a dashboard UI for ingestion monitoring, low-quality alerting, and fast smoke testing

This scaffold is intended to accelerate both UX and backend iteration.

## Architecture

```
Client UI (static dashboard)
        |
        v
 FastAPI /api/v1/ingest  ---->  asyncio.Queue  ---->  processing worker
        |                                                  |
        |                                                  v
        +-------------------- /api/v1/* metrics/scans <--- SQLite store (idempotent)
```

### Backend pipeline stages

1. **Receive** - request accepted at `POST /api/v1/ingest`
2. **Validate** - strict Pydantic input models (`schema_version`, range checks, tags)
3. **Normalize** - azimuth radians, normalized intensity, derived quality score
4. **Idempotency** - deterministic event hash (`event_id`) and SQLite primary key dedupe
5. **Store** - indexed scan records for dashboard/API reads
6. **Observe** - pipeline metrics (accepted, processed, duplicates, queue depth, errors)

## UI/UX highlights

- Design token based styling (spacing, color, typography, radius)
- Clean dashboard hierarchy with cards, ingestion form, recent scans, and alerts
- Empty/error/loading states for better operability
- Theme toggle with persisted preference (light/dark)
- Accessibility basics: semantic structure, labels, focus styles, `aria-live` status text

## Project layout

```
lrx_radar/
  app.py          # FastAPI app + API routes + static mounting
  schemas.py      # Input/output contracts (Pydantic)
  pipeline.py     # Normalization + quality scoring + event IDs
  service.py      # Async queue worker and runtime metrics
  storage.py      # SQLite persistence and query helpers
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
```

### Run

```bash
uvicorn lrx_radar.app:app --reload
```

Open `http://127.0.0.1:8000`.

## API overview

- `GET /api/v1/health` - service status
- `POST /api/v1/ingest` - queue a batch of radar scans
- `GET /api/v1/scans?limit=25` - latest stored scans
- `GET /api/v1/alerts?quality_below=0.45&limit=20` - quality alerts
- `GET /api/v1/metrics` - ingestion pipeline metrics
- `GET /api/v1/dashboard` - combined payload for UI

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

## Next improvement ideas

- Replace SQLite with PostgreSQL + partitioning when throughput grows
- Add dead-letter queue and retry policies for invalid or failed records
- Add charting (time-series and geospatial heatmap) to the dashboard
- Add authn/authz and per-source quotas before multi-tenant rollout
