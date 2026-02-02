# Rada Document Collection Pipeline

## Overview

This DAG orchestrates the complete document collection pipeline for Ukrainian Parliament (Verkhovna Rada) open data. It collects, transforms, and stores legislative documents with full history tracking using SCD Type 2 and dual-write Hive-style partitioning.

### What Are Dictionaries vs Documents?

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
- Dictionary entries are stable (rarely change), documents are volatile (updated frequently)

## DAG Configuration

- **DAG ID**: `document_pipeline`
- **Schedule**: `@daily`
- **Max Active Runs**: 1
- **Catchup**: False
- **Tags**: document-collection, etl, llm-data

## Parameters

- `source`: Data source name (default: "rada")
- `doc_limit`: Document limit for testing (default: 5, set to 0 for full run)

## Pipeline Architecture

The pipeline consists of **two parallel branches** that join at the transform step, followed by sequential processing:

```
        ┌─────────────────┐              ┌─────────────────┐
        │ Collect Dicts   │              │ Collect List    │
        └────────┬────────┘              └────────┬────────┘
                 │                                │
                 ▼                                ▼
        ┌─────────────────┐              ┌─────────────────┐
        │ Snapshot Dicts  │              │ Filter Docs     │
        └────────┬────────┘              └────────┬────────┘
                 │                                │
                 │                                ▼
                 │                       ┌─────────────────┐
                 │                       │ Collect Texts   │
                 │                       └────────┬────────┘
                 │                                │
                 └────────────┬───────────────────┘
                              │ (both branches join here)
                              ▼
                    ┌──────────────────┐
                    │ Transform to MD  │
                    └─────────┬────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Load Metadata DB │
                    └─────────┬────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Quality Checks   │
                    └─────────┬────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Generate Report  │
                    └─────────┬────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Cleanup Temp     │
                    └──────────────────┘
```

**Left branch**: Dictionary reference data (required for FK validation)
**Right branch**: Document collection with smart filtering
**Join point**: Transform waits for both branches to complete

## Task Descriptions

### 1. collect_dictionaries

**Purpose**: Collect reference dictionaries from Rada API

**Output**: Dictionary entries in PostgreSQL `dictionaries` table with SCD Type 2

**Details**: Collects `stan` (statuses) and `temy` (themes) dictionaries via two-step process (JSON metadata → CSV download). Uses windows-1251 encoding for CSV files.

### 2. snapshot_dictionaries

**Purpose**: Backup current dictionary state to MinIO

**Output**: JSON snapshots in `dictionaries/snapshots/source=rada/date={date}/`

**Details**: Saves current dictionary state as separate JSON files. Records snapshot metadata in database for tracking.

### 3. collect_document_list

**Purpose**: Retrieve list of recently updated documents from API

**Output**: List of document metadata (via XCom), temp file in MinIO

**Details**: Calls `/laws/main/r/page{N}.json` with pagination. Extracts metadata including `revision_date` for filtering. Applies `doc_limit` for testing.

### 4. filter_documents_for_collection

**Purpose**: Filter documents based on database state to skip unchanged documents

**Output**: Filtered document list (via XCom), filtering report in MinIO temp

**Details**: Performs bulk DB query to compare `revision_date` values. Returns only new or changed documents. Saves filtering report with metrics.

### 5. collect_document_texts

**Purpose**: Download full text for filtered documents with dual-write partitioning

**Output**: Raw text and metadata in MinIO `raw` bucket (both partitions)

**Details**: Receives filtered list from previous task. Rate limited (5-7s pause). Dual-write to `by_revision` and `by_collection` partitions. Logs failures.

### 6. transform_to_markdown

**Purpose**: Convert raw text to Markdown with YAML frontmatter

**Output**: Markdown files in MinIO `processed` bucket (both partitions)

**Details**: Adds YAML frontmatter with metadata. Cleans whitespace. Dual-write to both partitions.

### 7. load_metadata_to_db

**Purpose**: Load document metadata to PostgreSQL with SCD Type 2

**Output**: Records in `documents` and `document_types` tables

**Details**: Implements SCD Type 2 logic (insert/update/unchanged). Computes content hash for change detection. Records metrics in `collection_runs` table.

### 8. run_quality_checks

**Purpose**: Validate data quality

**Output**: Quality check results (via XCom)

**Checks**: Title not empty, text exists, valid status ID, minimum text length, no overlapping SCD periods.

### 9. generate_report

**Purpose**: Generate collection report with metrics

**Output**: JSON report in MinIO temp file

**Includes**: Collection metrics, statistics, duration, quality summary, error details.

### 10. cleanup_temp_files

**Purpose**: Remove old temporary files

**Output**: Deleted objects count

**Details**: Deletes files older than 7 days from `pipeline-temp` bucket. Parses dates from partition paths.

## Dependencies

### Task Dependencies (Airflow DAG)

```python
# Branch 1: Dictionaries
collect_dicts >> snapshot_dicts

# Branch 2: Documents
collect_list >> filter_docs >> collect_texts

# Join point: Both branches must complete before transform
[snapshot_dicts, collect_texts] >> transform

# Sequential processing
transform >> load_metadata >> quality >> report >> cleanup
```

### Visualization

```
Branch 1 (Dicts):    collect_dicts ──→ snapshot_dicts ──┐
                                                        │
                                                        ├──→ transform ──→ ...
                                                        │
Branch 2 (Docs):     collect_list ──→ filter_docs ──→ collect_texts ──┘
```

### Dependency Rationale

**Parallel Execution**:
- `collect_dicts` and `collect_list` can run in parallel (independent data sources)
- This reduces total pipeline time by ~30 seconds

**Sequential in Branch 1**:
- `snapshot_dicts` waits for `collect_dicts` (needs data to snapshot)

**Sequential in Branch 2**:
- `filter_docs` waits for `collect_list` (needs document list from XCom)
- `collect_texts` waits for `filter_docs` (needs filtered list from XCom)

**Join Point**:
- `transform` waits for **both** `snapshot_dicts` and `collect_texts`
- Dictionaries must be available before transforming documents (for FK validation)
- Document texts must be collected before transformation

**Sequential Processing**:
- All tasks after `transform` run sequentially
- Each task depends on previous task completion
- `cleanup` runs last to preserve temp files for debugging

## Data Locations

### PostgreSQL Tables

- **documents**: Current and historical document records
  - ~514 current records (is_current=TRUE)
  - Historical versions when documents update
- **dictionaries**: Current and historical dictionary entries
  - ~8 status entries
  - ~N theme entries
- **document_types**: Many-to-many document-type relationships
- **collection_runs**: DAG execution metrics
- **dictionary_snapshots**: Snapshot metadata

### MinIO Buckets

#### raw/
```
raw/by_revision/source=rada/year=2026/month=01/day=30/
  ├── 551704.txt
  ├── 551704.meta.json
  ├── 551702.txt
  └── 551702.meta.json

raw/by_collection/date=2026-02-01/source=rada/
  ├── 551704.txt
  ├── 551704.meta.json
  ├── 551702.txt
  └── 551702.meta.json
```

#### processed/
```
processed/by_revision/source=rada/year=2026/month=01/day=30/
  ├── 551704.md
  └── 551702.md

processed/by_collection/date=2026-02-01/source=rada/
  ├── 551704.md
  └── 551702.md
```

#### dictionaries/
```
dictionaries/snapshots/source=rada/date=2026-02-01/
  ├── status.json
  └── theme.json
```

#### pipeline-temp/
```
pipeline-temp/date=2026-02-01/run_id={airflow_run_id}/
  ├── task_collect_list/
  │   └── document_list.json
  ├── task_filter_documents/
  │   └── filtering_report.json
  ├── task_collect_texts/
  │   └── failed_documents.json
  └── task_report/
      └── collection_report.json
```

## Partitioning Examples

### By revision (for analytics)

Path pattern: `source={source}/year={YYYY}/month={MM}/day={DD}/{source_id}.{ext}`

**Use cases**:
- LLM training datasets
- Time-series analysis
- Historical document snapshots
- Athena/Spark queries

**Example**:
```
raw/by_revision/source=rada/year=2026/month=01/day=30/551704.txt
```

### By collection (for debugging)

Path pattern: `date={YYYY-MM-DD}/source={source}/{source_id}.{ext}`

**Use cases**:
- Pipeline debugging
- Audit trails
- Data lineage tracking
- Daily collection reports

**Example**:
```
raw/by_collection/date=2026-02-01/source=rada/551704.txt
```

## SCD Type 2 Examples

### Current state query

Get all current documents:
```sql
SELECT * FROM documents WHERE is_current = TRUE;
```

### Historical query

Get document state at specific date:
```sql
SELECT * FROM documents
WHERE source = 'rada' AND source_id = '551704'
  AND valid_from <= '2026-01-15'
  AND (valid_to IS NULL OR valid_to > '2026-01-15');
```

### Version count

Count document versions:
```sql
SELECT source_id, COUNT(*) as versions
FROM documents
WHERE source = 'rada'
GROUP BY source_id
HAVING COUNT(*) > 1
ORDER BY versions DESC;
```

### Recent updates

Find documents updated in last 7 days:
```sql
SELECT source_id, title, valid_from, valid_to
FROM documents
WHERE source = 'rada'
  AND valid_from > NOW() - INTERVAL '7 days'
ORDER BY valid_from DESC;
```

### History for specific document

```sql
SELECT
    source_id,
    title,
    status_id,
    is_current,
    valid_from,
    valid_to
FROM documents
WHERE source = 'rada' AND source_id = '551704'
ORDER BY valid_from DESC;
```

## Configuration

Filtering is enabled by default. To force re-download all documents:
```yaml
# configs/config.yaml or dags/rada_data_source/config.yml
pipeline:
  skip_existing: false
```
