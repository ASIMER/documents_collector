# Document Collection Pipeline

A production-ready data pipeline for collecting, transforming, and storing legal documents from open data sources for LLM training. Built with **Airflow 2.10.4**, **Python 3.12**, and optimized for scalability.

## Features

- **Airflow 2.10.4** with Python 3.12 (stable, production-ready)
- **Smart filtering** - Pre-download document checking (90%+ API call reduction for incremental runs)
- **Hive-style partitioning** with dual-write (by revision date + by collection date)
- **SCD Type 2** history tracking for documents and dictionaries
- **UV package manager** integration for 10-100x faster builds (optional)
- **Pluggable collector pattern** for easy integration of new data sources
- **MinIO S3-compatible storage** for raw and processed documents
- **PostgreSQL** for metadata with full change history
- **Quality checks** and automated reporting

## Architecture

```
┌──────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│     COLLECT      │────▶│  TRANSFORM  │────▶│    LOAD      │────▶│   QUALITY   │
│  (Source → Raw)  │     │ (Text → MD) │     │ (Meta → DB)  │     │   CHECK     │
└──────────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
   ▲ pluggable              │                      │                    │
   │ collector               ▼                      ▼                    ▼
   │                   MinIO: processed/        PostgreSQL          Logs/Report
   │
   ├── RadaCollector (Ukrainian Parliament)
   ├── (Future collectors...)
   └── ...
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design decisions.

## Quick Start

### Prerequisites

- Docker & Docker Compose installed
- (Optional) Make for simplified commands

### 1. Clone and Start

```bash
git clone <repository-url>
cd documents_collector

# Using Makefile (recommended)
make up

# Or using docker-compose directly
cd docker && docker-compose up -d
```

Wait 60-90 seconds for all services to initialize.

### 2. Access Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **PostgreSQL**: localhost:5432 (pipeline_user/pipeline_password)

### 3. Run Pipeline

**Option A: Airflow UI (Recommended)**

1. Open http://localhost:8080
2. Find DAG: `document_pipeline`
3. Click "Trigger DAG" (play icon)
4. Set parameters:
   - `source`: `rada` (default)
   - `doc_limit`: `5` for testing, `0` for full run (~514 documents)
5. Click "Trigger"

**Option B: CLI**

```bash
# Using Makefile
make run-pipeline SOURCE=rada

# Or using docker-compose exec
cd docker
docker-compose exec airflow-scheduler \
  python /opt/airflow/scripts/run_pipeline.py full --source rada --limit 5
```

### 4. Verify Results

```bash
# Using Makefile
make check-db
make check-minio

# Or manually
cd docker

# Check documents in PostgreSQL
docker-compose exec postgres psql -U pipeline_user -d pipeline \
  -c "SELECT COUNT(*) FROM documents WHERE is_current=TRUE;"

# Check files in MinIO
docker-compose exec -T minio mc ls local/raw/by_revision/source=rada/
```

Expected results:
- PostgreSQL: 5 documents (or 514 if full run)
- MinIO: 30 files for 5 documents (6 file types × 5 docs)

## Smart Filtering

The pipeline includes intelligent pre-download filtering using bulk database queries to compare `revision_date` values. This dramatically reduces API load for incremental runs (90%+ reduction typical). Filtering is enabled by default and can be disabled via `skip_existing: false` in config.

See [ARCHITECTURE.md](ARCHITECTURE.md) for implementation details.

## Common Commands

```bash
make up              # Start all services
make down            # Stop all services
make logs            # View all logs
make logs-scheduler  # View Airflow scheduler logs
make run-pipeline    # Run pipeline via CLI (SOURCE=rada)
make check-db        # Check database status
make check-minio     # Check MinIO buckets
make restart         # Restart all services
make clean           # Clean up containers and volumes
```

See [Makefile](Makefile) for all available commands.

## Project Structure

```
documents_collector/
├── Makefile              # Common operations
├── README.md             # This file
├── ARCHITECTURE.md       # System design and patterns
├── API.md                # Rada API integration guide
│
├── docker/
│   ├── Dockerfile        # Airflow 2.10.4 + Python 3.12
│   ├── docker-compose.yaml
│   └── init-db.sql       # Database schema with SCD Type 2
│
├── configs/
│   └── config.yaml       # Global infrastructure config
│
├── dags/
│   └── rada_data_source/
│       ├── document_pipeline_dag.py  # Airflow DAG
│       ├── config.yml                # DAG-specific config
│       └── README.md                 # DAG documentation
│
├── pipeline/
│   ├── collectors/       # Pluggable data source collectors
│   │   ├── base.py       # BaseCollector ABC
│   │   └── rada.py       # RadaCollector
│   ├── tasks/            # Airflow task implementations
│   │   ├── collect.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   ├── quality.py
│   │   └── maintenance.py
│   ├── storage.py        # MinIO client (dual-write)
│   ├── db.py             # SQLAlchemy models (SCD Type 2)
│   └── utils.py
│
└── scripts/
    └── run_pipeline.py   # CLI entry point
```

## Configuration

The project uses split configuration for flexibility:

**Global Config** (`/configs/config.yaml`):
- MinIO connection (endpoint, credentials, buckets)
- PostgreSQL connection (host, port, database, user)
- Pipeline defaults (batch size, retention days)
- Logging configuration

**DAG Config** (`/dags/rada_data_source/config.yml`):
- Source identification (name, collector class)
- API settings (base URL, auth, rate limits)
- Dictionary list
- Pipeline overrides

This separation makes it easy to add new data sources without duplicating infrastructure settings.

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System design, patterns, and technical decisions
- **[API.md](API.md)** - Rada API documentation and integration guide
- **[DAG README](dags/rada_data_source/README.md)** - Pipeline task details and workflows

## Package Management

Using pip for simplicity. UV integration is optional for faster Docker builds (10-100x faster lock resolution).

## Roadmap

### Planned Features
- [ ] Additional data sources (courts, EU legislation)
- [ ] NeMo-Curator integration for text quality filtering
- [ ] Data quality dashboard
- [ ] Automated testing suite
- [ ] Alerting on failures
- [ ] Metrics to Prometheus

### Scalability
- [ ] Distributed execution (CeleryExecutor)
- [ ] Snowflake/Redshift integration (architecture already prepared)
- [ ] Apache Spark for large-scale processing

## Development

### Adding New Data Source

1. Create collector: `pipeline/collectors/newsource.py`
2. Implement `BaseCollector` interface
3. Create DAG: `dags/newsource_data_source/pipeline_dag.py`
4. Add config: `dags/newsource_data_source/config.yml`
5. Register in `pipeline/collectors/__init__.py`

See [ARCHITECTURE.md](ARCHITECTURE.md) Section 2 for collector pattern details.

### Local Development Without Docker

```bash
# Install dependencies
uv sync  # or: pip install -r requirements.txt

# Run pipeline locally
uv run scripts/run_pipeline.py full --source rada --limit 5

# Run specific step
uv run scripts/run_pipeline.py collect-dicts --source rada
```

## Monitoring

### Service Health

```bash
# Check all services
cd docker && docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Check Airflow DAG status
docker-compose exec airflow-scheduler airflow dags list
```

### Data Validation

```bash
# PostgreSQL: Check document count
docker-compose exec postgres psql -U pipeline_user -d pipeline \
  -c "SELECT source, COUNT(*) FROM documents WHERE is_current=TRUE GROUP BY source;"

# PostgreSQL: Latest collection run
docker-compose exec postgres psql -U pipeline_user -d pipeline \
  -c "SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT 1;"

# MinIO: Check file count
docker-compose exec -T minio mc find local/raw/by_revision --name "*.txt" | wc -l
docker-compose exec -T minio mc find local/processed/by_revision --name "*.md" | wc -l
```

## Troubleshooting

### Services Not Starting

```bash
# Check logs
cd docker
docker-compose logs postgres
docker-compose logs minio
docker-compose logs airflow-scheduler

# Restart services
docker-compose restart

# Full restart
docker-compose down && docker-compose up -d
```

### DAG Not Appearing

```bash
# Check for import errors
docker-compose exec airflow-scheduler airflow dags list-import-errors

# Manually parse DAG
docker-compose exec airflow-scheduler \
  python /opt/airflow/dags/rada_data_source/document_pipeline_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Database Connection Errors

```bash
# Test connection
docker-compose exec postgres pg_isready -U airflow

# Check pipeline database
docker-compose exec postgres psql -U pipeline_user -d pipeline -c "\dt"
```

### MinIO Bucket Errors

```bash
# List buckets
docker-compose exec -T minio mc ls local/

# Create missing bucket
docker-compose exec -T minio mc mb local/pipeline-temp
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Airflow 2.10.4** | Stable, production-ready version with reliable DAG discovery |
| **Python 3.12** | Modern Python features, better performance |
| **Pre-download filtering** | Filter before API calls, not after; 90%+ reduction in incremental runs |
| **Bulk DB query** | Single query vs N queries; uses existing index; ~10ms for 200 docs |
| **revision_date comparison** | Available without download; reliable change detection |
| **UV (optional)** | 10-100x faster package resolution, better Docker caching |
| **Dual-write partitioning** | `by_revision` for analytics, `by_collection` for debugging |
| **SCD Type 2** | Full history tracking without data loss, time-travel queries |
| **LocalExecutor** | Linear pipeline, fewer services, simpler deployment |
| **PostgreSQL + MinIO** | Separation: fast SQL queries + S3-like for large files |
| **Hive-style partitions** | Compatible with Athena/Spark/Snowflake, scalable |
| **Configuration split** | Global infrastructure + DAG-specific for easy source addition |
