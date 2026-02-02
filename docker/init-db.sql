-- ============================================================================
-- Database Initialization for Document Collection Pipeline
-- Creates pipeline database with SCD Type 2 schema
-- ============================================================================

-- Create pipeline user and database
CREATE USER pipeline_user WITH PASSWORD 'pipeline_password';
CREATE DATABASE pipeline OWNER pipeline_user;

-- Connect to pipeline database
\c pipeline

-- Grant all privileges to pipeline_user
GRANT ALL PRIVILEGES ON DATABASE pipeline TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA public TO pipeline_user;

-- ============================================================================
-- Table: dictionaries (SCD Type 2)
-- Purpose: Store reference data (status, types, themes) with full history
-- ============================================================================

CREATE TABLE dictionaries (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,       -- 'rada', 'court', etc.
    dict_type       VARCHAR(50)  NOT NULL,       -- 'status', 'type', 'theme'
    entry_id        INTEGER      NOT NULL,       -- ID from source system
    entry_name      TEXT         NOT NULL,       -- Name of the entry

    -- SCD Type 2 columns
    valid_from      TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP    NULL,           -- NULL = current record
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    content_hash    VARCHAR(64)  NOT NULL,       -- SHA256(entry_name) for change detection

    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

-- Unique constraint: only one current record per (source, dict_type, entry_id)
CREATE UNIQUE INDEX idx_dictionaries_current
    ON dictionaries(source, dict_type, entry_id)
    WHERE is_current = TRUE;

-- Index for historical queries
CREATE INDEX idx_dictionaries_history
    ON dictionaries(source, dict_type, entry_id, valid_from, valid_to);

-- Index for entry lookups
CREATE INDEX idx_dictionaries_entry
    ON dictionaries(source, dict_type, entry_id) WHERE is_current = TRUE;


-- ============================================================================
-- Table: documents (SCD Type 2)
-- Purpose: Store document metadata with full history tracking
-- ============================================================================

CREATE TABLE documents (
    id                      SERIAL PRIMARY KEY,
    source                  VARCHAR(50)  NOT NULL,       -- 'rada', 'court', etc.
    source_id               VARCHAR(100) NOT NULL,       -- dokid (as string for universality)
    source_reg              VARCHAR(255),                -- nreg, registration number

    -- Document metadata
    title                   TEXT,
    status_id               INTEGER,                     -- FK to dictionaries (status)
    org_id                  INTEGER,                     -- Organization ID
    doc_date                DATE,                        -- Document date
    revision_date           DATE,                        -- Current revision date

    -- Storage paths
    raw_path_revision       VARCHAR(500),                -- MinIO: raw/by_revision/source={s}/year={y}/month={m}/day={d}/{id}.txt
    raw_path_collection     VARCHAR(500),                -- MinIO: raw/by_collection/date={date}/source={s}/{id}.txt
    processed_path_revision VARCHAR(500),                -- MinIO: processed/by_revision/...
    processed_path_collection VARCHAR(500),              -- MinIO: processed/by_collection/...

    -- Content metrics
    has_text                BOOLEAN      DEFAULT FALSE,
    text_length             INTEGER      DEFAULT 0,      -- Characters
    word_count              INTEGER      DEFAULT 0,

    -- SCD Type 2 columns
    valid_from              TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to                TIMESTAMP    NULL,           -- NULL = current record
    is_current              BOOLEAN      NOT NULL DEFAULT TRUE,
    content_hash            VARCHAR(64)  NOT NULL,       -- SHA256(title, status_id, revision_date) for change detection

    -- Timestamps
    collected_at            TIMESTAMP    DEFAULT NOW(),
    transformed_at          TIMESTAMP,
    created_at              TIMESTAMP    DEFAULT NOW(),
    updated_at              TIMESTAMP    DEFAULT NOW()
);

-- Unique constraint: only one current record per (source, source_id)
CREATE UNIQUE INDEX idx_documents_current
    ON documents(source, source_id)
    WHERE is_current = TRUE;

-- Index for historical queries
CREATE INDEX idx_documents_history
    ON documents(source, source_id, valid_from, valid_to);

-- Index for queries by source
CREATE INDEX idx_documents_source
    ON documents(source) WHERE is_current = TRUE;

-- Index for queries by status
CREATE INDEX idx_documents_status
    ON documents(status_id) WHERE is_current = TRUE;

-- Index for queries by date
CREATE INDEX idx_documents_doc_date
    ON documents(doc_date) WHERE is_current = TRUE;

-- Index for queries by revision date
CREATE INDEX idx_documents_revision_date
    ON documents(revision_date) WHERE is_current = TRUE;


-- ============================================================================
-- Table: document_types (SCD Type 2)
-- Purpose: Many-to-many relationship between documents and types
-- ============================================================================

CREATE TABLE document_types (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    type_id         INTEGER NOT NULL,

    -- SCD Type 2 columns
    valid_from      TIMESTAMP    NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP    NULL,           -- NULL = current record
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,

    created_at      TIMESTAMP    DEFAULT NOW()
);

-- Unique constraint: only one current record per (document_id, type_id)
CREATE UNIQUE INDEX idx_document_types_current
    ON document_types(document_id, type_id)
    WHERE is_current = TRUE;

-- Index for queries by document
CREATE INDEX idx_document_types_doc
    ON document_types(document_id) WHERE is_current = TRUE;

-- Index for queries by type
CREATE INDEX idx_document_types_type
    ON document_types(type_id) WHERE is_current = TRUE;


-- ============================================================================
-- Table: collection_runs
-- Purpose: Track DAG execution history and metrics
-- ============================================================================

CREATE TABLE collection_runs (
    id                  SERIAL PRIMARY KEY,
    run_id              VARCHAR(255) NOT NULL UNIQUE,   -- Airflow run_id
    source              VARCHAR(50)  NOT NULL,          -- Data source
    started_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMP,
    status              VARCHAR(20)  DEFAULT 'running', -- 'running', 'success', 'failed'

    -- Metrics
    documents_found     INTEGER DEFAULT 0,              -- Total documents from API
    documents_new       INTEGER DEFAULT 0,              -- New inserts
    documents_updated   INTEGER DEFAULT 0,              -- Updates (SCD Type 2)
    documents_unchanged INTEGER DEFAULT 0,              -- No changes detected
    documents_failed    INTEGER DEFAULT 0,              -- Failed to process

    -- Quality metrics
    documents_with_text INTEGER DEFAULT 0,
    total_text_length   BIGINT  DEFAULT 0,
    total_word_count    BIGINT  DEFAULT 0,

    -- Error tracking
    error_message       TEXT,

    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Index for queries by source
CREATE INDEX idx_collection_runs_source
    ON collection_runs(source, started_at DESC);

-- Index for queries by status
CREATE INDEX idx_collection_runs_status
    ON collection_runs(status, started_at DESC);


-- ============================================================================
-- Table: dictionary_snapshots
-- Purpose: Track MinIO snapshot locations for dictionaries
-- ============================================================================

CREATE TABLE dictionary_snapshots (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,          -- Data source
    snapshot_date   DATE         NOT NULL,          -- Date of snapshot
    minio_path      VARCHAR(500) NOT NULL,          -- MinIO path to snapshot file
    dict_types      TEXT[]       NOT NULL,          -- Array of dictionary types in snapshot
    entry_count     INTEGER      DEFAULT 0,         -- Total entries in snapshot

    created_at      TIMESTAMP    DEFAULT NOW()
);

-- Unique constraint: one snapshot per source per date
CREATE UNIQUE INDEX idx_dictionary_snapshots_unique
    ON dictionary_snapshots(source, snapshot_date);

-- Index for queries by source
CREATE INDEX idx_dictionary_snapshots_source
    ON dictionary_snapshots(source, snapshot_date DESC);


-- ============================================================================
-- Grant permissions to pipeline_user
-- ============================================================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO pipeline_user;

-- Grant permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON FUNCTIONS TO pipeline_user;


-- ============================================================================
-- Success message
-- ============================================================================

SELECT 'Pipeline database initialized successfully with SCD Type 2 schema' AS status;
