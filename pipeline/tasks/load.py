"""Database loading tasks for Airflow DAG.

This module provides:
- Load document metadata to PostgreSQL with SCD Type 2
- Load document types
- Track collection run metrics
"""

import json
import logging
from datetime import date, datetime

from pipeline.db import (
    CollectionRun,
    create_db_engine,
    create_session_factory,
    upsert_document_scd2,
    upsert_document_types_scd2,
)
from pipeline.storage import StorageClient
from pipeline.utils import parse_date, parse_types

logger = logging.getLogger(__name__)


def task_load_metadata_scd2(
    source: str,
    config: dict,
    document_list: list[dict],
    **context,
) -> dict:
    """Load document metadata to PostgreSQL with SCD Type 2.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        document_list: List of document metadata
        **context: Airflow context

    Returns:
        Dictionary with loading metrics
    """
    # Deserialize document_list if it comes as string from XCom
    if isinstance(document_list, str):
        logger.debug(f"document_list is string, deserializing...")
        try:
            # Try JSON first
            document_list = json.loads(document_list)
        except json.JSONDecodeError:
            # Try Python literal_eval (for repr strings)
            import ast
            document_list = ast.literal_eval(document_list)

    logger.info(f"Starting metadata loading for {len(document_list)} documents")

    # Connect to database
    db_config = config["database"]
    engine = create_db_engine(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["name"],
        user=db_config["user"],
        password=db_config["password"],
    )
    SessionFactory = create_session_factory(engine)

    # Connect to storage
    minio_config = config["minio"]
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config["secure"],
    )

    # Loading metrics
    run_id = context["run_id"]
    collection_date = date.today()
    insert_count = 0
    update_count = 0
    unchanged_count = 0
    failed_count = 0

    # Create collection run record
    with SessionFactory() as session:
        collection_run = CollectionRun(
            run_id=run_id,
            source=source,
            started_at=datetime.now(),
            status="running",
            documents_found=len(document_list),
        )
        session.add(collection_run)
        session.commit()

        # Load each document
        for idx, doc_meta in enumerate(document_list, 1):
            dokid = str(doc_meta.get("dokid"))
            nreg = doc_meta.get("nreg")

            try:
                # Parse dates
                doc_date = parse_date(doc_meta.get("orgdat"))
                revision_date = parse_date(doc_meta.get("poddat")) or doc_date or collection_date

                # Get storage paths
                paths = storage.get_document_paths(
                    source=source,
                    source_id=dokid,
                    revision_date=revision_date,
                    collection_date=collection_date,
                )

                # Check if text exists
                has_text = storage.object_exists("raw", paths.raw_by_revision)
                text_length = 0
                word_count = 0

                if has_text:
                    try:
                        text_content = storage.get_object("raw", paths.raw_by_revision).decode(
                            "utf-8"
                        )
                        text_length = len(text_content)
                        word_count = len(text_content.split())
                    except Exception:
                        pass

                # Prepare document data
                doc_data = {
                    "source_reg": nreg,
                    "title": doc_meta.get("nazva", ""),
                    "status_id": doc_meta.get("status"),
                    "org_id": doc_meta.get("orgid") or doc_meta.get("org"),
                    "doc_date": doc_date,
                    "revision_date": revision_date,
                    "has_text": has_text,
                    "text_length": text_length,
                    "word_count": word_count,
                }

                # Upsert document with SCD Type 2
                document_id, action = upsert_document_scd2(
                    session=session,
                    source=source,
                    source_id=dokid,
                    data=doc_data,
                    paths=paths,
                )

                # Track action
                if action == "insert":
                    insert_count += 1
                elif action == "update":
                    update_count += 1
                else:
                    unchanged_count += 1

                # Upsert document types
                type_ids = parse_types(doc_meta.get("types"))
                if type_ids:
                    upsert_document_types_scd2(
                        session=session,
                        document_id=document_id,
                        type_ids=type_ids,
                    )

                # Commit in batches
                if idx % 50 == 0:
                    session.commit()
                    logger.info(
                        f"Progress: {idx}/{len(document_list)} "
                        f"(insert={insert_count}, update={update_count}, "
                        f"unchanged={unchanged_count})"
                    )

            except Exception as e:
                logger.error(f"Failed to load document {dokid}: {e}")
                failed_count += 1

        # Final commit
        session.commit()

        # Update collection run
        collection_run.completed_at = datetime.now()
        collection_run.status = "success" if failed_count == 0 else "partial"
        collection_run.documents_new = insert_count
        collection_run.documents_updated = update_count
        collection_run.documents_unchanged = unchanged_count
        collection_run.documents_failed = failed_count

        session.commit()

    logger.info(
        f"Metadata loading complete: "
        f"inserted={insert_count}, updated={update_count}, "
        f"unchanged={unchanged_count}, failed={failed_count}"
    )

    return {
        "total": len(document_list),
        "inserted": insert_count,
        "updated": update_count,
        "unchanged": unchanged_count,
        "failed": failed_count,
    }
