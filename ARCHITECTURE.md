# Document Collection Pipeline Architecture

## 1. General Overview

A data pipeline for collecting, transforming, and storing text documents from open sources for LLM model training.

The system is designed as an **extensible framework**: a specific data source (Ukrainian Parliament, courts, EU, etc.) is a pluggable collector with its own collection logic, while the core (storage, transformation, quality checks, orchestration) is shared.

```
┌──────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│     COLLECT      │────▶│  TRANSFORM  │────▶│    LOAD      │────▶│   QUALITY   │
│  (Source → Raw)  │     │ (Text → MD) │     │ (Meta → DB)  │     │   CHECK     │
└──────────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
   ▲ pluggable              │                      │                    │
   │ collector               ▼                      ▼                    ▼
   │                   MinIO: processed/        PostgreSQL          Logs/Report
   │
   ├── RadaCollector
   ├── (FutureCollector)
   └── ...
```

---

## 2. Extensibility — Collector Pattern

### 2.1 Concept

Each data source is a separate collector class that implements the base interface. The pipeline core knows nothing about the specific API—it works with unified structures.

```python
# pipeline/collectors/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CollectedDocument:
    """Unified document structure from any collector."""
    source_id: str           # unique ID within source (dokid for Rada)
    source_reg: str          # registration number (nreg for Rada)
    title: str               # title
    text: str | None         # full document text (plain text)
    status_id: int | None    # status
    type_ids: list[int]      # document types
    org_id: int | None       # publisher organization
    doc_date: date | None    # document date
    revision_date: date | None  # current revision date
    raw_metadata: dict       # full JSON from API (for raw storage)


@dataclass
class DictionaryEntry:
    """Dictionary entry."""
    dict_type: str    # 'status', 'type', 'theme', ...
    entry_id: int
    entry_name: str


class BaseCollector(ABC):
    """Base collector interface."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique source name: 'rada', 'court', 'eu', ..."""
        ...

    @abstractmethod
    def collect_dictionaries(self) -> list[DictionaryEntry]:
        """Collect source dictionaries."""
        ...

    @abstractmethod
    def collect_document_list(self) -> list[dict]:
        """Collect document list (minimal metadata for filtering)."""
        ...

    @abstractmethod
    def collect_document(self, doc_meta: dict) -> CollectedDocument:
        """Collect single document (metadata + text)."""
        ...
```

### 2.2 Concrete Collector — Ukrainian Parliament (Verkhovna Rada)

```python
# pipeline/collectors/rada.py

class RadaCollector(BaseCollector):

    source_name = "rada"

    def __init__(self, config: dict):
        self.base_url = config["api"]["base_url"]
        self.session = self._create_session(config)
        self.rate_limiter = RateLimiter(config["api"]["rate_limit"])

    def collect_dictionaries(self) -> list[DictionaryEntry]:
        # Two-step operation: JSON metadata → CSV by link
        # Details in API.md
        ...

    def collect_document_list(self) -> list[dict]:
        # GET /laws/main/r/page{N}.json
        # Returns all documents with pagination
        ...

    def collect_document(self, doc_meta: dict) -> CollectedDocument:
        # Metadata from doc_meta (already obtained from list)
        # Text: GET /laws/show/{nreg}.txt
        ...
```

### 2.3 Collector Registration

```python
# pipeline/collectors/__init__.py

REGISTRY: dict[str, type[BaseCollector]] = {
    "rada": RadaCollector,
}

def get_collector(source_name: str, config: dict) -> BaseCollector:
    cls = REGISTRY[source_name]
    return cls(config)
```

### 2.4 Adding New Source in the Future

```python
# pipeline/collectors/court.py
class CourtCollector(BaseCollector):
    source_name = "court"
    ...

# pipeline/collectors/__init__.py — add line:
REGISTRY["court"] = CourtCollector
```

### 2.5 Entry Point

```python
# scripts/run_pipeline.py

@click.command()
@click.option("--source", required=True, help="Source name: rada, court, ...")
@click.option("--config", default="configs/config.yaml")
@click.option("--step", default=None, help="Run specific step only")
def main(source, config, step):
    cfg = load_config(config)
    collector = get_collector(source, cfg)
    run_pipeline(collector, cfg, step)
```

```bash
# Usage
python scripts/run_pipeline.py --source rada
python scripts/run_pipeline.py --source rada --step collect_texts
python scripts/run_pipeline.py --source court  # in the future
```

### 2.6 Dictionaries vs Documents — Core Concepts

**Dictionaries** (Reference Data):
- Statuses: Document lifecycle states (draft, active, archived, etc.) — 8 values (IDs 0-7)
- Themes: Thematic categories (economy, education, healthcare, etc.) — variable count
- Types: Document classifications (law, resolution, decision, etc.) — inferred from data
- Organizations: Publishing entities (Parliament, Cabinet, etc.) — not available via API

**Documents** (Actual Data):
- Legislative texts (laws, resolutions, decisions, notifications)
- Contains: title, text, dates, status, types, organization
- References dictionaries via foreign keys (status_id, type_ids, org_id)

**Why Load Dictionaries First?**
- Documents have foreign key references to dictionaries
- Database integrity: cannot insert document with invalid status_id
- Ensures data quality and referential integrity
- Dictionary entries are stable (rarely change)
- Documents are volatile (updated frequently)

**Loading Order**:
```
1. collect_dictionaries    → PostgreSQL (status, theme entries)
2. snapshot_dictionaries   → MinIO backup (JSON snapshots)
3. collect_document_list   → In-memory/XCom (dokid list)
4. collect_document_texts  → MinIO raw/ (text files)
5. load_metadata_to_db     → PostgreSQL (documents reference dictionaries)
```

**Example Reference**:
```sql
-- Dictionary entry
INSERT INTO dictionaries VALUES ('rada', 'status', 5, 'Active');

-- Document referencing dictionary
INSERT INTO documents (source, source_id, status_id, ...)
VALUES ('rada', '551704', 5, ...);  -- status_id=5 must exist in dictionaries
```

### 2.7 Future Data Warehouse Migration

The current architecture is designed for easy migration to cloud data warehouses:

**PostgreSQL → Snowflake/Redshift**:
- `pipeline` database can be replaced with Snowflake tables
- SCD Type 2 is standard pattern in data warehouses
- Alternatively: use dbt snapshots for history tracking
- Foreign keys become soft references (validation in application layer)

**MinIO → AWS S3/Azure Blob**:
- Hive-style partitioning is compatible with Athena, Redshift Spectrum, Snowflake External Tables
- `by_revision` partition for analytics queries (time-series analysis)
- `by_collection` partition for lineage tracking (audit logs)
- S3 API compatibility makes migration seamless

**Benefits of Current Design**:
- Local PostgreSQL + MinIO: easy development and testing
- Production: swap to managed services without code changes
- Hive partitioning: optimized for analytical queries
- SCD Type 2: industry-standard history tracking

---

## 3. Infrastructure (Docker Compose)

### 3.1 Services

```
┌──────────────────────────────────────────────────────┐
│                Docker Compose Network                 │
│                                                       │
│   ┌─────────────┐  ┌──────────┐  ┌────────────────┐  │
│   │  PostgreSQL  │  │  MinIO   │  │    Airflow      │  │
│   │    :5432     │  │ :9000 API│  │  :8080 Web UI   │  │
│   │              │  │ :9001 UI │  │                  │  │
│   │ airflow DB   │  │          │  │  webserver       │  │
│   │ pipeline DB  │  │ raw/     │  │  scheduler       │  │
│   │              │  │ processed│  │  (LocalExecutor)  │  │
│   └─────────────┘  └──────────┘  └────────────────┘  │
└──────────────────────────────────────────────────────┘
```

### 3.2 Service Descriptions

| Service | Image | Ports | Purpose |
|--------|-------|-------|-------------|
| `postgres` | `postgres:15` | `5432` | Airflow metadata (`airflow` DB) + pipeline data (`pipeline` DB) |
| `minio` | `bitnami/minio:latest` | `9000`, `9001` | S3-compatible storage for raw/processed documents |
| `airflow-webserver` | Custom (base: `apache/airflow:2.10.4`) | `8080` | Web UI for DAG monitoring |
| `airflow-scheduler` | Custom (base: `apache/airflow:2.10.4`) | — | LocalExecutor — scheduler and executor |
| `airflow-init` | Custom (base: `apache/airflow:2.10.4`) | — | One-time DB initialization + admin user |

### 3.3 Why LocalExecutor?

- Pipeline is linear sequence of tasks, no massive parallelization needed
- Fewer services (no Redis/RabbitMQ/Worker)
- Simpler for review and reproducibility

### 3.4 Volumes

```yaml
volumes:
  postgres-data:          # Persistent storage PostgreSQL
  minio-data:             # Persistent storage MinIO

# Bind mounts (for development):
# ./dags       → /opt/airflow/dags
# ./pipeline   → /opt/airflow/pipeline
# ./configs    → /opt/airflow/configs
# ./outputs/logs → /opt/airflow/logs
```

### 3.5 Network

All services in single Docker bridge network `pipeline-network`. Services address each other by hostname: `postgres`, `minio`.

---

## 4. Data Flow — 10 Pipeline Steps

### Step 1: Collect Dictionaries → PostgreSQL

```
Collector.collect_dictionaries()
  → RadaCollector: GET /open/data/stan.json → CSV link → download → parse
  → UPSERT → PostgreSQL: dictionaries table
```

For Rada: two-step operation (JSON metadata → CSV). Details in API.md.

Available dictionaries (verified): `stan` (statuses), `temy` (themes).
Unavailable: `typs` (404), `orgs` (404).

### Step 2: Snapshot Dictionaries → MinIO

```
Save dictionaries to MinIO for backup and auditing
  → MinIO: dictionaries/snapshots/source={source}/date={YYYY-MM-DD}/
```

### Step 3: Collect Document List → XCom / in-memory

```
Collector.collect_document_list()
  → RadaCollector: GET /laws/main/r/page{N}.json
  → List (dokid, nreg, nazva, status, types, dates, revision_date)
  → Save to MinIO temp (for debugging)
  → Pass to filtering step
```

Current volume: 514 documents on single page. Code supports pagination.

### Step 4: Filter Documents → Filtered List

```
task_filter_documents_for_collection()
  → Bulk DB query: SELECT source_id, revision_date, content_hash
                   FROM documents WHERE source='rada' AND is_current=TRUE
  → Compare API revision_date vs DB revision_date
  → Filter:
     • New document: dokid not in DB → download
     • Changed document: revision_date differs → download
     • Unchanged document: revision_date same → skip
  → Save filtering report to MinIO temp
  → Return filtered list (only new/changed documents)
```

**Performance Impact:**
- Incremental runs (no changes): 0 downloads → 100% API call reduction
- Incremental runs (10% changes): 20 downloads instead of 200 → 90% reduction
- First run (cold start): All documents downloaded → no impact
- Bulk DB query: ~10ms for 200 documents

**Key Features:**
- Uses existing index: `idx_documents_source_id_current`
- Fail-safe: Falls back to downloading all if DB connection fails
- Can be disabled: Set `skip_existing: false` in config
- Edge cases handled: Missing revision_date in API or DB

### Step 5: Collect Document Texts → MinIO raw/

```
Collector.collect_document(doc_meta)
  → RadaCollector: GET /laws/show/{nreg}.txt
  → Rate limit: random 5-7 sec between requests
  → Save → MinIO: raw/{source}/{dokid}.txt
  → Save metadata → MinIO: raw/{source}/{dokid}.meta.json
```

**Note**: This step now receives **filtered list** instead of full list, significantly reducing API load.

**Idempotency:**
- Check if file exists in MinIO
- Support `If-Modified-Since` header
- Skip if not changed

### Step 6: Transform Text → Markdown → MinIO processed/

```
Read raw text from MinIO: raw/{source}/{dokid}.txt
  → Add YAML frontmatter (title, date, source, status, types)
  → Clean up (excess blank lines, spaces)
  → Save → MinIO: processed/{source}/{dokid}.md
```

**Example Result:**

```markdown
---
source: rada
dokid: 551704
nreg: n0044500-26
title: "On Accounting Price of Banking Metals"
date: 2026-01-30
status: 0
types: [95]
org: 70
---

NATIONAL BANK OF UKRAINE
NOTIFICATION
30.01.2026

Accounting price of banking metals as of 30.01.2026
...
```

**Why Markdown and not plain text:**
- YAML frontmatter — structured metadata with text (single file)
- Standard format for LLM training datasets
- Easy to parse programmatically

**Why markdownify / beautifulsoup4 not needed:**
API with `OpenData` token returns `.txt` as plain text without HTML tags. HTML conversion not needed.

### Step 7: Load Metadata → PostgreSQL

```
Read collected metadata
  → UPSERT with SCD Type 2 (INSERT for new, UPDATE+INSERT for changed)
  → PostgreSQL: documents table
  → PostgreSQL: document_types junction table
```

### Step 8: Quality Checks → Logs/Report

| Check | Level |
|-----------|--------|
| `title` not empty | ERROR |
| Text present (`has_text`) | WARNING |
| `status_id` exists in dictionary | WARNING |
| Markdown not empty | ERROR |
| Min text length > 50 characters | WARNING |
| Duplicate source_id | ERROR (handled by UPSERT) |

Result: generate report with metrics (count collected, skipped, errors, etc.).

### Step 9: Generate Collection Report → MinIO temp

```
Aggregate metrics from all steps
  → Documents found, new, updated, unchanged, failed
  → Filtering metrics (if filtering enabled)
  → Quality score
  → Save report to MinIO temp
```

### Step 10: Cleanup Temporary Files → MinIO temp

```
Delete files older than retention period (default: 7 days)
  → Clean up old filtering reports, batch files, quality reports
  → Keep MinIO temp bucket size manageable
```

---

## 5. Database Schema (PostgreSQL)

### 5.1 Two Databases in One Postgres

| Database | Purpose | User |
|------|-------------|------|
| `airflow` | Airflow metadata (auto-created) | `airflow` |
| `pipeline` | Pipeline document metadata | `pipeline_user` |

### 5.2 Database Diagram (ER Diagram)

```mermaid
erDiagram
    dictionaries ||--o{ documents : "referenced by"
    documents ||--o{ document_types : "has many"
    collection_runs ||--o{ documents : "tracks"
    dictionary_snapshots ||--o{ dictionaries : "snapshots of"

    dictionaries {
        serial id PK
        varchar source
        varchar dict_type "status|theme|type"
        integer entry_id
        text entry_name
        timestamp valid_from "SCD Type 2"
        timestamp valid_to "SCD Type 2"
        boolean is_current "SCD Type 2"
        varchar content_hash "SHA256"
        timestamp created_at
        timestamp updated_at
    }

    documents {
        serial id PK
        varchar source
        varchar source_id "dokid"
        varchar source_reg "nreg"
        text title
        integer status_id FK "→dictionaries"
        integer org_id
        date doc_date
        date revision_date
        varchar raw_path_revision
        varchar raw_path_collection
        varchar processed_path_revision
        varchar processed_path_collection
        boolean has_text
        integer text_length
        integer word_count
        timestamp valid_from "SCD Type 2"
        timestamp valid_to "SCD Type 2"
        boolean is_current "SCD Type 2"
        varchar content_hash "SHA256"
        timestamp collected_at
        timestamp transformed_at
        timestamp created_at
        timestamp updated_at
    }

    document_types {
        serial id PK
        integer document_id FK "→documents"
        integer type_id FK "→dictionaries"
        timestamp valid_from "SCD Type 2"
        timestamp valid_to "SCD Type 2"
        boolean is_current "SCD Type 2"
        timestamp created_at
    }

    collection_runs {
        serial id PK
        varchar run_id UK "airflow_run_id"
        varchar source
        timestamp started_at
        timestamp completed_at
        varchar status "running|success|failed"
        integer documents_found
        integer documents_new
        integer documents_updated
        integer documents_unchanged
        integer documents_failed
        integer documents_with_text
        bigint total_text_length
        bigint total_word_count
        text error_message
        timestamp created_at
        timestamp updated_at
    }

    dictionary_snapshots {
        serial id PK
        varchar source
        date snapshot_date
        varchar minio_path
        text[] dict_types "array"
        integer entry_count
        timestamp created_at
    }
```

**Legend:**
- **PK** = Primary Key
- **FK** = Foreign Key
- **UK** = Unique Key
- **SCD Type 2** = Slowly Changing Dimension Type 2 (history tracking fields)

**Key Relationships:**
1. `documents.status_id` → `dictionaries.entry_id` (WHERE dict_type='status')
2. `document_types.document_id` → `documents.id` (many-to-many junction)
3. `document_types.type_id` → `dictionaries.entry_id` (WHERE dict_type='type')
4. `collection_runs.source` groups metrics per data source
5. `dictionary_snapshots.minio_path` points to MinIO backup files

### 5.3 Tables

```sql
-- ═══════════════════════════════════════════════════
-- Dictionaries (statuses, types, themes, ...)
-- ═══════════════════════════════════════════════════

CREATE TABLE dictionaries (
    source      VARCHAR(50)  NOT NULL,    -- 'rada', 'court', ...
    dict_type   VARCHAR(50)  NOT NULL,    -- 'status', 'type', 'theme'
    entry_id    INTEGER      NOT NULL,
    entry_name  TEXT         NOT NULL,

    -- SCD Type 2 columns
    valid_from      TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP    NULL,          -- NULL = current record
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    content_hash    VARCHAR(64)  NOT NULL,      -- SHA256 for change detection

    PRIMARY KEY (source, dict_type, entry_id, valid_from)
);

CREATE UNIQUE INDEX idx_dictionaries_current
    ON dictionaries(source, dict_type, entry_id)
    WHERE is_current = TRUE;

CREATE INDEX idx_dictionaries_history
    ON dictionaries(source, dict_type, entry_id, valid_from, valid_to);


-- ═══════════════════════════════════════════════════
-- Documents (main metadata table)
-- ═══════════════════════════════════════════════════

CREATE TABLE documents (
    id                  SERIAL PRIMARY KEY,
    source              VARCHAR(50)  NOT NULL,       -- 'rada', 'court', ...
    source_id           VARCHAR(100) NOT NULL,       -- dokid (as string for universality)
    source_reg          VARCHAR(255),                -- nreg, registration number
    title               TEXT,
    status_id           INTEGER,
    org_id              INTEGER,
    doc_date            DATE,                        -- document date
    revision_date       DATE,                        -- current revision date

    -- Storage paths (dual-write)
    raw_path_by_revision       VARCHAR(500),        -- MinIO: raw/by_revision/...
    raw_path_by_collection     VARCHAR(500),        -- MinIO: raw/by_collection/...
    processed_path_by_revision VARCHAR(500),        -- MinIO: processed/by_revision/...
    processed_path_by_collection VARCHAR(500),      -- MinIO: processed/by_collection/...

    has_text            BOOLEAN      DEFAULT FALSE,
    text_length         INTEGER      DEFAULT 0,      -- chars
    word_count          INTEGER      DEFAULT 0,
    collected_at        TIMESTAMP    DEFAULT NOW(),
    transformed_at      TIMESTAMP,

    -- SCD Type 2 columns
    valid_from      TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP    NULL,          -- NULL = current record
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    content_hash    VARCHAR(64)  NOT NULL       -- SHA256(title, status_id, revision_date)
);

CREATE UNIQUE INDEX idx_documents_current
    ON documents(source, source_id)
    WHERE is_current = TRUE;

CREATE INDEX idx_documents_history
    ON documents(source, source_id, valid_from, valid_to);

CREATE INDEX idx_documents_status ON documents(status_id);
CREATE INDEX idx_documents_doc_date ON documents(doc_date);


-- ═══════════════════════════════════════════════════
-- Junction: document ↔ types (many-to-many)
-- ═══════════════════════════════════════════════════

CREATE TABLE document_types (
    id          SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    type_id     INTEGER NOT NULL,

    -- SCD Type 2 columns
    valid_from      TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP    NULL,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,

    UNIQUE (document_id, type_id, valid_from)
);

CREATE INDEX idx_document_types_doc ON document_types(document_id);
CREATE INDEX idx_document_types_current ON document_types(document_id, type_id)
    WHERE is_current = TRUE;


-- ═══════════════════════════════════════════════════
-- Collection runs (pipeline execution tracking)
-- ═══════════════════════════════════════════════════

CREATE TABLE collection_runs (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255) NOT NULL UNIQUE,
    source          VARCHAR(50)  NOT NULL,
    started_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    status          VARCHAR(20)  DEFAULT 'running',
    documents_found     INTEGER DEFAULT 0,
    documents_new       INTEGER DEFAULT 0,
    documents_updated   INTEGER DEFAULT 0,
    documents_unchanged INTEGER DEFAULT 0,
    documents_failed    INTEGER DEFAULT 0
);

CREATE INDEX idx_collection_runs_source ON collection_runs(source);
CREATE INDEX idx_collection_runs_started ON collection_runs(started_at DESC);


-- ═══════════════════════════════════════════════════
-- Dictionary snapshots (daily backups to MinIO)
-- ═══════════════════════════════════════════════════

CREATE TABLE dictionary_snapshots (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,
    snapshot_date   DATE         NOT NULL,
    minio_path      VARCHAR(500) NOT NULL,
    dict_types      TEXT[]       NOT NULL,
    entry_count     INTEGER      NOT NULL,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),

    UNIQUE(source, snapshot_date, minio_path)
);

CREATE INDEX idx_snapshots_source_date ON dictionary_snapshots(source, snapshot_date DESC);
```

### 5.4 Why `source` in Each Table?

- One pipeline — many sources
- Filtering: `SELECT * FROM documents WHERE source = 'rada'`
- Isolation: sources don't overlap by `source_id`
- Scalability: add new collector → data immediately in same schema

### 5.5 SCD Type 2 UPSERT Strategy

**For documents:**
```python
def upsert_document_scd2(session, source, source_id, data, paths):
    """UPSERT with SCD Type 2 history tracking."""
    # 1. Compute content hash
    content_hash = compute_hash(data['title'], data['status_id'], data['revision_date'])

    # 2. SELECT current record
    current = session.query(Document).filter_by(
        source=source, source_id=source_id, is_current=True
    ).first()

    # 3. If no existing → INSERT new
    if not current:
        new_doc = Document(source=source, source_id=source_id,
                          content_hash=content_hash, is_current=True, **data)
        session.add(new_doc)
        return (new_doc.id, "insert")

    # 4. If existing with same hash → unchanged
    if current.content_hash == content_hash:
        return (current.id, "unchanged")

    # 5. If existing with different hash → UPDATE + INSERT
    # Close old record
    current.valid_to = datetime.now()
    current.is_current = False

    # Insert new record
    new_doc = Document(source=source, source_id=source_id,
                      content_hash=content_hash, is_current=True,
                      valid_from=datetime.now(), **data)
    session.add(new_doc)

    return (new_doc.id, "update")
```

**Historical Queries:**
```sql
-- Get current state
SELECT * FROM documents WHERE is_current = TRUE;

-- Get document state at specific date
SELECT * FROM documents
WHERE source = 'rada' AND source_id = '551704'
  AND valid_from <= '2026-01-15'
  AND (valid_to IS NULL OR valid_to > '2026-01-15');

-- Count versions per document
SELECT source, source_id, COUNT(*) as versions
FROM documents
GROUP BY source, source_id
HAVING COUNT(*) > 1;
```

---

## 6. MinIO — Storage Structure

### 6.1 Bucket Structure

```
MinIO
├── raw/                              # Raw data from API
│   ├── by_revision/                  # For analytics (partitioned by revision_date)
│   │   └── source={source}/
│   │       └── year={YYYY}/month={MM}/day={DD}/
│   │           ├── {source_id}.txt           # Document text
│   │           └── {source_id}.meta.json     # Metadata (full JSON from API)
│   │
│   └── by_collection/                # For debugging (partitioned by collection_date)
│       └── date={YYYY-MM-DD}/
│           └── source={source}/
│               ├── {source_id}.txt
│               └── {source_id}.meta.json
│
├── processed/                        # Transformed documents
│   ├── by_revision/
│   │   └── source={source}/year={YYYY}/month={MM}/day={DD}/
│   │       └── {source_id}.md            # Markdown with YAML frontmatter
│   │
│   └── by_collection/
│       └── date={YYYY-MM-DD}/source={source}/
│           └── {source_id}.md
│
├── dictionaries/                     # Dictionary snapshots
│   └── snapshots/
│       └── source={source}/date={YYYY-MM-DD}/
│           ├── status.json
│           └── theme.json
│
└── pipeline-temp/                    # Temporary files (7-day retention)
    └── date={YYYY-MM-DD}/
        └── run_id={airflow_run_id}/
            └── task_{task_name}/
                └── {filename}
```

### 6.2 Dual-Write Strategy

Each document is saved to **two locations**:

**Primary Path (by_revision)** — For analytics/LLM training:
- Partitioned by `revision_date` (when document was last updated)
- Hive-style: `source=rada/year=2026/month=01/day=30/`
- Optimized for time-series analysis
- Compatible with Athena, Spark, Snowflake External Tables

**Secondary Path (by_collection)** — For debugging/auditing:
- Partitioned by `collection_date` (when pipeline ran)
- Hive-style: `date=2026-02-01/source=rada/`
- Useful for tracking "what was collected on specific date"
- Audit trail for pipeline runs

**Example for Rada:**
```
# By revision (analytics)
raw/by_revision/source=rada/year=2026/month=01/day=30/551704.txt
raw/by_revision/source=rada/year=2026/month=01/day=30/551704.meta.json
processed/by_revision/source=rada/year=2026/month=01/day=30/551704.md

# By collection (debugging)
raw/by_collection/date=2026-02-01/source=rada/551704.txt
raw/by_collection/date=2026-02-01/source=rada/551704.meta.json
processed/by_collection/date=2026-02-01/source=rada/551704.md
```

### 6.3 Temporary Files Path

```
pipeline-temp/date={YYYY-MM-DD}/run_id={airflow_run_id}/task_{task_name}/{filename}
```

**Files stored**:
- `task_collect_list/document_ids.json` - List of dokids to process
- `task_collect_texts/batch_*.json` - Batch processing status
- `task_collect_texts/failed_documents.json` - Failed documents
- `task_transform/transform_report.json` - Transformation stats
- `task_quality/quality_report.json` - Final QA metrics

**Retention**: 7 days (configurable via `cleanup_temp_files` task)

### 6.4 Why MinIO?

- Recommended in task FAQ
- S3-compatible API (easy migration to AWS S3)
- Web console for viewing (port 9001)
- File versioning
- Data scientists familiar with S3-like storage
- Hive-style partitioning for analytical queries

---

## 7. Airflow DAG

### 7.1 Configuration

| Parameter | Value |
|----------|----------|
| DAG ID | `document_pipeline` |
| Schedule | `@daily` |
| Executor | LocalExecutor |
| Max Active Runs | 1 |
| Catchup | False |

### 7.2 Task Sequence

```
collect_dictionaries → snapshot_dictionaries
                    ↘
                     [snapshot_dictionaries, collect_texts]
                                                ↓
collect_document_list → filter_documents → collect_document_texts →
transform_to_markdown → load_metadata_to_db → run_quality_checks →
generate_report → cleanup_temp_files
```

The filtering step performs bulk DB query and skips unchanged documents, significantly reducing API load for incremental runs.

### 7.3 Task → Module Mapping

| Task | Module | Function |
|------|--------|----------|
| `collect_dictionaries` | `pipeline.tasks.collect` | `task_collect_dictionaries(source, config_path)` |
| `snapshot_dictionaries` | `pipeline.tasks.collect` | `task_snapshot_dictionaries(source, config_path)` |
| `collect_document_list` | `pipeline.tasks.collect` | `task_collect_document_list(source, config_path)` |
| `filter_documents_for_collection` | `pipeline.tasks.collect` | `task_filter_documents_for_collection(source, config_path, doc_list)` |
| `collect_document_texts` | `pipeline.tasks.collect` | `task_collect_document_texts(source, config_path, filtered_list)` |
| `transform_to_markdown` | `pipeline.tasks.transform` | `task_transform_all(source, config_path)` |
| `load_metadata_to_db` | `pipeline.tasks.load` | `task_load_metadata(source, config_path)` |
| `run_quality_checks` | `pipeline.tasks.quality` | `task_quality_checks(source, config_path)` |
| `generate_report` | `pipeline.tasks.quality` | `task_generate_report(source, config_path)` |
| `cleanup_temp_files` | `pipeline.tasks.maintenance` | `task_cleanup_temp(config_path, retention_days=7)` |

### 7.4 Data Passing Between Tasks

- **Small data** (dokid list, counters) → XCom
- **Large data** (texts, JSON) → MinIO with naming convention

### 7.5 DAG Params

```python
dag = DAG(
    dag_id="document_pipeline",
    params={"source": Param("rada", type="string", description="Data source name")},
    ...
)

# In each task:
source = context["params"]["source"]
collector = get_collector(source, config)
```

This allows running the same DAG for different sources via Airflow UI (trigger with params).

---

## 8. Project Structure

```
document-pipeline/
│
├── pyproject.toml                        # Dependencies (UV - optional, using pip now)
├── README.md
├── ARCHITECTURE.md                       # This file
├── API.md                                # Rada API documentation
├── .env.example
│
├── configs/
│   └── config.yaml                       # Global pipeline configuration
│
├── dags/
│   └── rada_data_source/
│       ├── document_pipeline_dag.py      # Airflow DAG
│       ├── config.yml                    # DAG-specific configuration
│       └── README.md                     # DAG documentation
│
├── pipeline/                             # Python package — core
│   ├── __init__.py
│   ├── collectors/                       # Collectors (pluggable)
│   │   ├── __init__.py                   #   REGISTRY + get_collector()
│   │   ├── base.py                       #   BaseCollector ABC
│   │   └── rada.py                       #   RadaCollector
│   ├── tasks/                            # Airflow task implementations
│   │   ├── __init__.py
│   │   ├── collect.py                    #   Collection tasks
│   │   ├── transform.py                  #   Transformation tasks
│   │   ├── load.py                       #   Database loading tasks
│   │   ├── quality.py                    #   Quality check tasks
│   │   └── maintenance.py                #   Cleanup tasks
│   ├── storage.py                        # MinIO client with dual-write
│   ├── db.py                             # SQLAlchemy models + SCD Type 2
│   └── utils.py                          # Rate limiter, config loader, logging
│
├── scripts/
│   └── run_pipeline.py                   # CLI entry point (standalone)
│
├── docker/
│   ├── Dockerfile                        # Custom Airflow image (2.10.4 + Python 3.12)
│   ├── docker-compose.yaml
│   └── init-db.sql                       # CREATE DATABASE pipeline; CREATE TABLES
│
└── outputs/
    └── logs/                             # Pipeline logs
```

---

## 9. Configuration Files

### 9.1 Global Configuration

**File**: `/configs/config.yaml`

**Purpose**: Infrastructure settings shared across all data sources

**Contains**:
- MinIO connection (endpoint, credentials, bucket names)
- PostgreSQL connection (host, port, database, credentials)
- Pipeline defaults (batch size, retention days, min text length)
- Logging configuration

### 9.2 DAG-Specific Configuration

**File**: `/dags/rada_data_source/config.yml`

**Purpose**: Settings specific to Rada data source

**Contains**:
- Source identification (name, display name, collector class)
- API configuration (base URL, auth method, token, rate limits)
- Dictionary list (which dictionaries to collect)
- Pipeline overrides (source-specific behavior)

### 9.3 Configuration Merging

DAG loads both configs and merges them:
1. Load global infrastructure config from `/configs/config.yaml`
2. Load DAG-specific config from `./config.yml` (relative to DAG file)
3. Merge: DAG config takes precedence for overlapping keys
4. Pass merged config to all tasks

**Benefits**:
- Separation of concerns: infrastructure vs source logic
- Easy to add new sources: create new DAG folder with config.yml
- No duplication: infrastructure settings defined once
- Override mechanism: DAG can override global defaults

---

## 10. Docker Setup

### 10.1 Dockerfile (Custom Airflow)

```dockerfile
FROM apache/airflow:2.10.4-python3.12

# Copy UV binary (for future use)
USER root
COPY --from=ghcr.io/astral-sh/uv:0.8.18 /uv /uvx /usr/local/bin/

# Install additional dependencies
USER airflow
RUN pip install --no-cache-dir \
    minio>=7.2 \
    "psycopg[binary]>=3.1" \
    click>=8.1

WORKDIR /opt/airflow
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
```

### 10.2 docker-compose.yaml (Key Parts)

```yaml
x-airflow-common: &airflow-common
  build:
    context: ..
    dockerfile: docker/Dockerfile
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    PYTHONPATH: /opt/airflow
    RADA_API_TOKEN: f822823f-ff12-4583-8160-0fd9db40507f
    DB_HOST: postgres
    DB_USER: pipeline_user
    DB_PASSWORD: pipeline_password
    MINIO_ENDPOINT: minio:9000
    MINIO_ACCESS_KEY: minioadmin
    MINIO_SECRET_KEY: minioadmin
  volumes:
    - ../dags:/opt/airflow/dags
    - ../pipeline:/opt/airflow/pipeline
    - ../configs:/opt/airflow/configs
    - ../outputs/logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy
    minio:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      retries: 5
    ports:
      - "5432:5432"

  minio:
    image: bitnami/minio:latest
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_DEFAULT_BUCKETS: "raw,processed,dictionaries,pipeline-temp"
    volumes:
      - minio-data:/bitnami/minio/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      retries: 5
    ports:
      - "9000:9000"
      - "9001:9001"

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: >
      -c "
        airflow db migrate &&
        airflow users create
          --username admin
          --password admin
          --firstname Admin
          --lastname User
          --role Admin
          --email admin@example.com || true
      "
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-data:
  minio-data:
```

### 10.3 init-db.sql

```sql
-- Create separate database for pipeline
CREATE USER pipeline_user WITH PASSWORD 'pipeline_password';
CREATE DATABASE pipeline OWNER pipeline_user;

\c pipeline

-- Tables (DDL from section 5.2)
CREATE TABLE dictionaries (...);
CREATE TABLE documents (...);
CREATE TABLE document_types (...);
CREATE TABLE collection_runs (...);
CREATE TABLE dictionary_snapshots (...);
-- Indexes
```

---

## 11. Python Dependencies

### 11.1 Package Manager: UV (Optional)

**Why UV over pip/Poetry?**

| Feature | pip | Poetry | UV |
|---------|-----|--------|-----|
| Installation speed | Baseline | 2-5x faster | **10-100x faster** |
| Lock file support | ❌ | ✅ | ✅ |
| Cache efficiency | Basic | Good | **Shared global cache** |
| Parallel downloads | Limited | Yes | **Highly parallel** |
| Python version management | ❌ | Via pyenv | ✅ Built-in |
| Docker optimization | Manual | Manual | **Native cache mounts** |

**Note**: Currently using `pip` for simplicity. UV integration is optional and can be added later for production deployments.

### 11.2 Current Dependencies

**Core runtime dependencies** (installed via pip in Dockerfile):
```dockerfile
RUN pip install --no-cache-dir \
    minio>=7.2 \
    "psycopg[binary]>=3.1" \
    click>=8.1
```

**Included with Airflow 2.10.4 base image**:
- `requests>=2.31`
- `pyyaml>=6.0`
- `sqlalchemy>=2.0`
- `pandas>=2.2`
- `apache-airflow==2.10.4`

### 11.3 Removed Dependencies

**What was removed compared to initial design:**
- `markdownify` — not needed (API returns plain text, not HTML)
- `beautifulsoup4` — not needed (no HTML parsing required)
- `psycopg2-binary` — replaced with modern `psycopg[binary]>=3.1` (psycopg3)

### 11.4 Environment

- **Python Version**: 3.12 (Airflow 2.10.4 base image)
- **Airflow Version**: 2.10.4 (stable, production-ready)
- **Base Image**: `apache/airflow:2.10.4-python3.12`

---

## 12. Idempotency & Optimization

| Step | Strategy |
|------|-----------|
| Collect list | Fetch all documents from API (no filtering at this stage) |
| **Filter documents** | **Bulk DB query + revision_date comparison → skip unchanged** |
| Collect text | Check file in MinIO + `If-Modified-Since` header (secondary check) |
| Transform | Check `{id}.md` exists in MinIO `processed/` |
| Load metadata | SCD Type 2 UPSERT (INSERT for new, UPDATE+INSERT for changed, nothing for unchanged) |
| Dictionaries | SCD Type 2 UPSERT by `(source, dict_type, entry_id)` |

**Filtering Optimization**:

The filtering step provides significant performance improvements:

1. **Bulk DB Query**: Single query fetches all existing documents instead of N individual queries
   ```sql
   SELECT source_id, revision_date, content_hash
   FROM documents
   WHERE source='rada' AND source_id IN (...) AND is_current=TRUE
   -- Uses index: idx_documents_source_id_current
   -- Performance: ~10ms for 200 documents
   ```

2. **Smart Comparison**:
   - Compare `revision_date` from API vs DB
   - If dates match → document unchanged → skip download
   - If dates differ → document changed → download
   - If not in DB → new document → download

3. **Performance Impact**:
   - **First run (cold start)**: No filtering overhead, all documents downloaded
   - **Incremental run (no changes)**: 100% API call reduction (0 downloads)
   - **Incremental run (10% changes)**: 90% API call reduction (20 downloads instead of 200)
   - **Time savings**: ~1 second instead of ~30 seconds for unchanged documents

4. **Configuration**:
   - Enabled by default: `skip_existing: true` in `config.yaml`
   - Can be disabled for force refresh: `skip_existing: false`
   - Fail-safe: Falls back to downloading all if DB connection fails

---

## 13. Key Design Decisions (Decision Log)

| # | Decision | Rationale |
|---|---------|---------------|
| 1 | API instead of web scraping | FAQ allows it; structured data; official API |
| 2 | `.txt` for text, not `.json` with `stru` | With "OpenData" token, `stru` field not returned |
| 3 | PostgreSQL for metadata + MinIO for content | Separation: fast SQL queries + S3-like for large files |
| 4 | Markdown with YAML frontmatter | Standard format for LLM datasets; metadata + text in one file |
| 5 | Pluggable collector pattern | Extensibility to new sources without changing core |
| 6 | Source-agnostic naming | DB `pipeline` (not `rada_data`); `document_pipeline` DAG |
| 7 | LocalExecutor | Linear pipeline; fewer services; simpler |
| 8 | One Postgres instance, two DBs | Fewer Docker services; complete data isolation |
| 9 | No markdownify/beautifulsoup | API gives plain text; HTML conversion not needed |
| 10 | Two-step dictionary loading | API returns metadata → then CSV by link |
| 11 | Dual-write partitioning | `by_revision` for analytics, `by_collection` for debugging |
| 12 | SCD Type 2 | Full history tracking without data loss, time-travel queries |
| 13 | Hive-style partitions | Compatible with Athena/Spark/Snowflake, scalable |
| 14 | Airflow 2.10.4 (not 3.1.7rc1) | Stable version, DAG discovery works reliably |
| 15 | Configuration split | Global infrastructure + DAG-specific for easy source addition |
| 16 | Pre-download filtering | Filter before API calls, not after; 90%+ reduction in incremental runs |
| 17 | Bulk DB query for filtering | Single query vs N queries; uses existing index; ~10ms for 200 docs |
| 18 | revision_date as change indicator | Available without download; reliable change detection; part of hash |
