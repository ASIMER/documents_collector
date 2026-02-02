"""Database models and operations with SCD Type 2 support.

This module provides:
- SQLAlchemy models for all tables
- SCD Type 2 UPSERT operations
- Database connection management
- Transaction handling
"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker

from pipeline.utils import compute_content_hash

logger = logging.getLogger(__name__)

Base = declarative_base()


# ============================================================================
# SQLAlchemy Models
# ============================================================================


class Dictionary(Base):
    """Dictionary entries with SCD Type 2 history tracking."""

    __tablename__ = "dictionaries"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    dict_type = Column(String(50), nullable=False)
    entry_id = Column(Integer, nullable=False)
    entry_name = Column(Text, nullable=False)

    # SCD Type 2 columns
    valid_from = Column(TIMESTAMP, nullable=False, default=datetime.now)
    valid_to = Column(TIMESTAMP, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)
    content_hash = Column(String(64), nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.now)
    updated_at = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now)


class Document(Base):
    """Documents with SCD Type 2 history tracking."""

    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    source_id = Column(String(100), nullable=False)
    source_reg = Column(String(255))

    # Document metadata
    title = Column(Text)
    status_id = Column(Integer)
    org_id = Column(Integer)
    doc_date = Column(Date)
    revision_date = Column(Date)

    # Storage paths
    raw_path_revision = Column(String(500))
    raw_path_collection = Column(String(500))
    processed_path_revision = Column(String(500))
    processed_path_collection = Column(String(500))

    # Content metrics
    has_text = Column(Boolean, default=False)
    text_length = Column(Integer, default=0)
    word_count = Column(Integer, default=0)

    # SCD Type 2 columns
    valid_from = Column(TIMESTAMP, nullable=False, default=datetime.now)
    valid_to = Column(TIMESTAMP, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)
    content_hash = Column(String(64), nullable=False)

    # Timestamps
    collected_at = Column(TIMESTAMP, default=datetime.now)
    transformed_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, default=datetime.now)
    updated_at = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now)

    # Relationships
    types = relationship("DocumentType", back_populates="document", cascade="all, delete-orphan")


class DocumentType(Base):
    """Document types junction table with SCD Type 2."""

    __tablename__ = "document_types"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    type_id = Column(Integer, nullable=False)

    # SCD Type 2 columns
    valid_from = Column(TIMESTAMP, nullable=False, default=datetime.now)
    valid_to = Column(TIMESTAMP, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

    created_at = Column(TIMESTAMP, default=datetime.now)

    # Relationships
    document = relationship("Document", back_populates="types")


class CollectionRun(Base):
    """Collection run tracking."""

    __tablename__ = "collection_runs"

    id = Column(Integer, primary_key=True)
    run_id = Column(String(255), nullable=False, unique=True)
    source = Column(String(50), nullable=False)
    started_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    completed_at = Column(TIMESTAMP)
    status = Column(String(20), default="running")

    # Metrics
    documents_found = Column(Integer, default=0)
    documents_new = Column(Integer, default=0)
    documents_updated = Column(Integer, default=0)
    documents_unchanged = Column(Integer, default=0)
    documents_failed = Column(Integer, default=0)

    # Quality metrics
    documents_with_text = Column(Integer, default=0)
    total_text_length = Column(Integer, default=0)
    total_word_count = Column(Integer, default=0)

    # Error tracking
    error_message = Column(Text)

    created_at = Column(TIMESTAMP, default=datetime.now)
    updated_at = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now)


class DictionarySnapshot(Base):
    """Dictionary snapshot tracking."""

    __tablename__ = "dictionary_snapshots"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    minio_path = Column(String(500), nullable=False)
    dict_types = Column(ARRAY(Text), nullable=False)
    entry_count = Column(Integer, default=0)

    created_at = Column(TIMESTAMP, default=datetime.now)


# ============================================================================
# Database Connection Management
# ============================================================================


def create_db_engine(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
):
    """Create SQLAlchemy engine.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        SQLAlchemy engine
    """
    # Use postgresql:// which auto-detects psycopg2 or psycopg3
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string, pool_pre_ping=True)
    logger.info(f"Connected to PostgreSQL: {host}:{port}/{database}")
    return engine


def create_session_factory(engine):
    """Create session factory.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Session factory
    """
    return sessionmaker(bind=engine)


# ============================================================================
# SCD Type 2 UPSERT Operations
# ============================================================================


def upsert_dictionary_scd2(
    session: Session,
    source: str,
    dict_type: str,
    entry_id: int,
    entry_name: str,
) -> tuple[int, str]:
    """Upsert dictionary entry with SCD Type 2 logic.

    Args:
        session: SQLAlchemy session
        source: Data source name
        dict_type: Dictionary type
        entry_id: Entry ID
        entry_name: Entry name

    Returns:
        Tuple of (dictionary_id, action) where action is 'insert', 'update', or 'unchanged'
    """
    # Compute content hash
    content_hash = compute_content_hash(entry_name)

    # Get current record
    current = (
        session.query(Dictionary)
        .filter(
            Dictionary.source == source,
            Dictionary.dict_type == dict_type,
            Dictionary.entry_id == entry_id,
            Dictionary.is_current == True,
        )
        .first()
    )

    if current is None:
        # No existing record - insert new
        new_dict = Dictionary(
            source=source,
            dict_type=dict_type,
            entry_id=entry_id,
            entry_name=entry_name,
            content_hash=content_hash,
            valid_from=datetime.now(),
            is_current=True,
        )
        session.add(new_dict)
        session.flush()
        logger.debug(f"Inserted new dictionary entry: {source}/{dict_type}/{entry_id}")
        return new_dict.id, "insert"

    if current.content_hash == content_hash:
        # No change detected
        return current.id, "unchanged"

    # Change detected - close old record and insert new
    now = datetime.now()
    current.valid_to = now
    current.is_current = False
    current.updated_at = now

    new_dict = Dictionary(
        source=source,
        dict_type=dict_type,
        entry_id=entry_id,
        entry_name=entry_name,
        content_hash=content_hash,
        valid_from=now,
        is_current=True,
    )
    session.add(new_dict)
    session.flush()
    logger.debug(f"Updated dictionary entry: {source}/{dict_type}/{entry_id}")
    return new_dict.id, "update"


def upsert_document_scd2(
    session: Session,
    source: str,
    source_id: str,
    data: dict[str, Any],
    paths: Any = None,
) -> tuple[int, str]:
    """Upsert document with SCD Type 2 logic.

    Args:
        session: SQLAlchemy session
        source: Data source name
        source_id: Document ID in source system
        data: Document data dictionary
        paths: Optional DocumentPaths object with storage paths

    Returns:
        Tuple of (document_id, action) where action is 'insert', 'update', or 'unchanged'
    """
    # Compute content hash from key fields
    content_hash = compute_content_hash(
        data.get("title"),
        data.get("status_id"),
        data.get("revision_date"),
    )

    # Get current record
    current = (
        session.query(Document)
        .filter(
            Document.source == source,
            Document.source_id == source_id,
            Document.is_current == True,
        )
        .first()
    )

    if current is None:
        # No existing record - insert new
        new_doc = Document(
            source=source,
            source_id=source_id,
            source_reg=data.get("source_reg"),
            title=data.get("title"),
            status_id=data.get("status_id"),
            org_id=data.get("org_id"),
            doc_date=data.get("doc_date"),
            revision_date=data.get("revision_date"),
            raw_path_revision=paths.raw_by_revision if paths else None,
            raw_path_collection=paths.raw_by_collection if paths else None,
            processed_path_revision=paths.processed_by_revision if paths else None,
            processed_path_collection=paths.processed_by_collection if paths else None,
            has_text=data.get("has_text", False),
            text_length=data.get("text_length", 0),
            word_count=data.get("word_count", 0),
            content_hash=content_hash,
            valid_from=datetime.now(),
            is_current=True,
            collected_at=data.get("collected_at", datetime.now()),
            transformed_at=data.get("transformed_at"),
        )
        session.add(new_doc)
        session.flush()
        logger.debug(f"Inserted new document: {source}/{source_id}")
        return new_doc.id, "insert"

    if current.content_hash == content_hash:
        # No change detected - update paths if provided
        if paths:
            current.raw_path_revision = paths.raw_by_revision
            current.raw_path_collection = paths.raw_by_collection
            current.processed_path_revision = paths.processed_by_revision
            current.processed_path_collection = paths.processed_by_collection
            current.updated_at = datetime.now()
            session.flush()
        return current.id, "unchanged"

    # Change detected - close old record and insert new
    now = datetime.now()
    current.valid_to = now
    current.is_current = False
    current.updated_at = now

    new_doc = Document(
        source=source,
        source_id=source_id,
        source_reg=data.get("source_reg"),
        title=data.get("title"),
        status_id=data.get("status_id"),
        org_id=data.get("org_id"),
        doc_date=data.get("doc_date"),
        revision_date=data.get("revision_date"),
        raw_path_revision=paths.raw_by_revision if paths else None,
        raw_path_collection=paths.raw_by_collection if paths else None,
        processed_path_revision=paths.processed_by_revision if paths else None,
        processed_path_collection=paths.processed_by_collection if paths else None,
        has_text=data.get("has_text", False),
        text_length=data.get("text_length", 0),
        word_count=data.get("word_count", 0),
        content_hash=content_hash,
        valid_from=now,
        is_current=True,
        collected_at=data.get("collected_at", datetime.now()),
        transformed_at=data.get("transformed_at"),
    )
    session.add(new_doc)
    session.flush()
    logger.debug(f"Updated document: {source}/{source_id}")
    return new_doc.id, "update"


def upsert_document_types_scd2(
    session: Session,
    document_id: int,
    type_ids: list[int],
) -> None:
    """Upsert document types with SCD Type 2 logic.

    Args:
        session: SQLAlchemy session
        document_id: Document ID
        type_ids: List of type IDs
    """
    # Get current types for document
    current_types = (
        session.query(DocumentType)
        .filter(
            DocumentType.document_id == document_id,
            DocumentType.is_current == True,
        )
        .all()
    )

    current_type_ids = {dt.type_id for dt in current_types}
    new_type_ids = set(type_ids)

    # Types to remove
    removed_type_ids = current_type_ids - new_type_ids
    if removed_type_ids:
        now = datetime.now()
        for doc_type in current_types:
            if doc_type.type_id in removed_type_ids:
                doc_type.valid_to = now
                doc_type.is_current = False

    # Types to add
    added_type_ids = new_type_ids - current_type_ids
    for type_id in added_type_ids:
        new_doc_type = DocumentType(
            document_id=document_id,
            type_id=type_id,
            valid_from=datetime.now(),
            is_current=True,
        )
        session.add(new_doc_type)

    session.flush()
    if removed_type_ids or added_type_ids:
        logger.debug(
            f"Updated document types for doc {document_id}: "
            f"added={added_type_ids}, removed={removed_type_ids}"
        )


def get_existing_documents(
    session: Session,
    source: str,
    source_ids: list[str]
) -> dict[str, tuple[Date | None, str]]:
    """Bulk query to check document existence in database.

    Args:
        session: SQLAlchemy session
        source: Data source name
        source_ids: List of source document IDs

    Returns:
        dict: {source_id: (revision_date, content_hash)}
    """
    if not source_ids:
        return {}

    results = (
        session.query(
            Document.source_id,
            Document.revision_date,
            Document.content_hash
        )
        .filter(
            Document.source == source,
            Document.source_id.in_(source_ids),
            Document.is_current == True
        )
        .all()
    )

    return {
        source_id: (revision_date, content_hash)
        for source_id, revision_date, content_hash in results
    }
