# f1-dashboard
F1 race analytics pipeline on GCP: PySpark + medallion architecture (bronze/silver/gold) on BigQuery, built from OpenF1 data.
=======
# F1 Race Analytics Pipeline

> **Medallion architecture** (Bronze → Silver → Gold) on GCP,  
> powered by PySpark, Dataproc Serverless, and BigQuery.

---

## Architecture overview

```
OpenF1 API  ──►  Python Ingestion  ──►  GCS Bronze  ──►  PySpark Silver  ──►  PySpark Gold  ──►  BigQuery  ──►  Looker Studio
                 (rate-limited,              (raw JSON,                  (Parquet,               (partitioned/
                  idempotent)                 partitioned by             cleaned,                  clustered)
                                             season / session)           typed, joined)
```

| Layer  | Storage                              | Format  | Partitioning                  |
|--------|--------------------------------------|---------|-------------------------------|
| Bronze | `gs://<bucket>/bronze/<endpoint>/`  | JSON    | `season=YYYY/session_key=N`   |
| Silver | `gs://<bucket>/silver/<table>/`     | Parquet | `season=YYYY/session_key=N`   |
| Gold   | BigQuery `f1_analytics.*`           | BQ      | `race_date` (partition key)   |

---

## Phases

| # | Phase | Status |
|---|-------|--------|
| 1 | **Bronze – Raw ingestion (OpenF1 → GCS)** | ✅ Done |
| 2 | **Silver – PySpark cleaning & Parquet transformation** | ✅ Done |
| 3 | **Gold – BigQuery race strategy analytics tables** | ✅ Done |
| 4 | **Interactive Dashboard (Web App & Looker Studio)** | ✅ Done |
| 5 | Streaming replay (Pub/Sub + Structured Streaming) | ⏳ Optional |
| 6 | Unit tests & test coverage (27/27 passing) | ✅ Done |

---

## Phase 1 – Bronze Ingestion

### What it does
- Fetches 7 OpenF1 endpoints per race session: `sessions`, `drivers`, `laps`,
  `stints`, `pit`, `weather`, `car_data`
- Writes raw JSON to `gs://<bucket>/bronze/<endpoint>/season=YYYY/session_key=N/`
- Skips already-downloaded sessions (idempotent)
- Respects the OpenF1 rate limit: 30 req/min, 3 req/s (dual token-bucket)
- Retries with exponential back-off + jitter on 429 / 5xx
- Logs row counts per file in structured JSON

### Quick start

```bash
# 1. Clone and enter the repo
git clone <repo>
cd f1-pipeline

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env and fill in GCP_PROJECT_ID, GCS_BUCKET, DATAPROC_SA

# 5. Run locally (no GCS, writes to ./local_bronze/)
LOCAL_MODE=1 python -m ingestion.ingest --log-level INFO

# 6. Dry-run (fetch API but write nothing)
LOCAL_MODE=1 python -m ingestion.ingest --dry-run

# 7. Run against GCS (needs GOOGLE_APPLICATION_CREDENTIALS or ADC)
python -m ingestion.ingest

# 8. Test a single session
LOCAL_MODE=1 python -m ingestion.ingest --session-key 9158

# 9. Run tests
pytest tests/ -v
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | repo root | Path to `config.yaml` |
| `--season` | from config | Override seasons, comma-separated (e.g. `2023,2024`) |
| `--session-key` | all | Pull a single session key only |
| `--overwrite` | false | Re-fetch and overwrite existing files |
| `--local` | false | Force local mode (write to `./local_bronze/`) |
| `--dry-run` | false | Fetch but write nothing |
| `--log-level` | INFO | `DEBUG` / `INFO` / `WARNING` |

### Configuration (`config.yaml`)

Key parameters:

```yaml
openf1:
  rate_limit:
    requests_per_minute: 30   # OpenF1 limit
    requests_per_second:  3   # burst guard

max_sessions_per_season: 3    # start small, scale later
car_data_row_cap: 50000       # telemetry can be enormous
session_type_filter: "Race"   # "Race" | "Qualifying" | null
```

### Bronze path convention

```
gs://<bucket>/bronze/laps/season=2024/session_key=9158/laps_20240313T120000Z.json
```

This Hive-style partitioning lets Spark and BigQuery partition-prune automatically.

### Rate limiting design

Two independent token buckets:

| Bucket | Capacity | Refill period |
|--------|----------|---------------|
| Per-second | 3 tokens | 1 s |
| Per-minute | 30 tokens | 60 s |

Every `acquire()` call must pass **both** checks. The per-second check runs first (cheapest). This prevents bursting beyond 3 req/s even when the per-minute budget has tokens remaining.

---

## Repository structure

```
f1-pipeline/
├── ingestion/
│   ├── __init__.py
│   ├── config_loader.py      # env-var substitution
│   ├── logger.py             # structured JSON logger + IngestionTimer
│   ├── rate_limiter.py       # dual token-bucket (30 rpm / 3 rps)
│   ├── openf1_client.py      # API client, pagination, retry/backoff
│   ├── gcs_uploader.py       # GCS upload + local dry-run mode
│   └── ingest.py             # Phase 1 orchestrator
├── spark_jobs/               # Phase 2 (bronze → silver)
├── sql/                      # Phase 3 (BigQuery DDL + views)
├── tests/
│   ├── test_rate_limiter.py
│   ├── test_openf1_client.py
│   └── test_gcs_uploader.py
├── docs/                     # architecture diagram (Phase 6)
├── config.yaml               # central config (no secrets)
├── requirements.txt
├── pyproject.toml            # ruff + pytest config
├── .env.example              # template for secrets
├── .gitignore
└── .github/workflows/ci.yml  # lint + test on push/PR
```

---

## Design decisions

### Why raw JSON in Bronze?
Bronze is the **single source of truth** – never transform before landing.
If a Silver job has a bug, re-running it from Bronze is cheap.

### Why Hive-style path partitioning?
`season=YYYY/session_key=N` lets Spark and BigQuery's external tables
auto-discover partitions without a metastore. A full-season re-ingest
touches only one partition prefix.

### Why a dual token-bucket?
A single RPM bucket would allow burst spikes (30 requests in 1 second).
The per-second bucket acts as a burst guard without requiring a sliding
window, which is heavier to implement correctly with threads.

### Why idempotent by default?
Cloud jobs fail. Restarting an ingestion job without idempotency would
duplicate records and inflate costs. The existence check on the blob
(or local filesystem) makes re-runs safe.

---

## GCP cost estimate (Phase 1 only)

| Resource | Volume | Est. cost/run |
|----------|--------|---------------|
| GCS writes (3 races × 7 endpoints) | ~50 MB | < $0.01 |
| GCS storage (30 days) | ~200 MB | < $0.01 |
| Egress (OpenF1 → GCS via Dataproc) | ~50 MB | $0.00 (same region) |
| **Total Phase 1** | | **< $0.02 / run** |

> Car-data (`car_data_row_cap: 50000`) is the biggest variable.  
> A full race without cap can be 500k+ rows.

---

## Contributing

1. Run `ruff check .` before committing.
2. All new logic needs a corresponding test in `tests/`.
3. Never hardcode project IDs, bucket names, or credentials.
4. Each phase is reviewed and merged before the next begins.

