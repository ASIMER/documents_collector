"""Collection tasks for Airflow DAG.

This module provides:
- Dictionary collection and database loading
- Dictionary snapshot to MinIO
- Document list collection
- Document text collection with dual-write partitioning
"""

import json
import logging
from datetime import date, datetime

from pipeline.collectors import get_collector
from pipeline.db import (
    DictionarySnapshot,
    create_db_engine,
    create_session_factory,
    get_existing_documents,
    upsert_dictionary_scd2,
)
from pipeline.storage import StorageClient
from pipeline.utils import parse_date

logger = logging.getLogger(__name__)


def task_collect_dictionaries(source: str, config: dict, **context) -> dict:
    """Collect dictionaries from source API and load to PostgreSQL with SCD Type 2.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        **context: Airflow context

    Returns:
        Dictionary with metrics
    """
    logger.info(f"Starting dictionary collection for source: {source}")

    # Get collector
    collector = get_collector(source, config)

    # Collect dictionaries
    entries = collector.collect_dictionaries()
    logger.info(f"Collected {len(entries)} dictionary entries")

    # Load to database with SCD Type 2
    db_config = config["database"]
    engine = create_db_engine(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["name"],
        user=db_config["user"],
        password=db_config["password"],
    )
    SessionFactory = create_session_factory(engine)

    insert_count = 0
    update_count = 0
    unchanged_count = 0

    with SessionFactory() as session:
        for entry in entries:
            _, action = upsert_dictionary_scd2(
                session=session,
                source=source,
                dict_type=entry.dict_type,
                entry_id=entry.entry_id,
                entry_name=entry.entry_name,
            )

            if action == "insert":
                insert_count += 1
            elif action == "update":
                update_count += 1
            else:
                unchanged_count += 1

        session.commit()

    logger.info(
        f"Dictionary collection complete: "
        f"inserted={insert_count}, updated={update_count}, unchanged={unchanged_count}"
    )

    return {
        "total": len(entries),
        "inserted": insert_count,
        "updated": update_count,
        "unchanged": unchanged_count,
    }


def task_snapshot_dictionaries(source: str, config: dict, **context) -> dict:
    """Save dictionary snapshot to MinIO.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        **context: Airflow context

    Returns:
        Dictionary with snapshot info
    """
    logger.info(f"Starting dictionary snapshot for source: {source}")

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

    # Query current dictionaries
    from pipeline.db import Dictionary

    with SessionFactory() as session:
        dictionaries = (
            session.query(Dictionary)
            .filter(Dictionary.source == source, Dictionary.is_current == True)
            .all()
        )

        # Group by dict_type
        dict_data = {}
        for d in dictionaries:
            if d.dict_type not in dict_data:
                dict_data[d.dict_type] = []
            dict_data[d.dict_type].append(
                {
                    "entry_id": d.entry_id,
                    "entry_name": d.entry_name,
                    "valid_from": d.valid_from.isoformat(),
                }
            )

        # Save to MinIO
        minio_config = config["minio"]
        storage = StorageClient(
            endpoint=minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config["secure"],
        )

        snapshot_date = date.today()
        dict_types = list(dict_data.keys())
        total_entries = sum(len(entries) for entries in dict_data.values())

        # Save each dictionary type to separate file
        snapshot_paths = []
        for dict_type, entries in dict_data.items():
            snapshot_path = f"snapshots/source={source}/date={snapshot_date}/{dict_type}.json"
            snapshot_content = json.dumps(entries, indent=2, ensure_ascii=False)

            storage._put_object("dictionaries", snapshot_path, snapshot_content.encode("utf-8"))
            snapshot_paths.append(snapshot_path)
            logger.info(f"Saved snapshot: dictionaries/{snapshot_path}")

        # Record snapshot in database (check for existing first)
        existing_snapshot = (
            session.query(DictionarySnapshot)
            .filter(
                DictionarySnapshot.source == source,
                DictionarySnapshot.snapshot_date == snapshot_date,
            )
            .first()
        )

        if existing_snapshot:
            # Update existing snapshot
            existing_snapshot.minio_path = f"snapshots/source={source}/date={snapshot_date}/"
            existing_snapshot.dict_types = dict_types
            existing_snapshot.entry_count = total_entries
            logger.info(f"Updated existing snapshot for {snapshot_date}")
        else:
            # Create new snapshot
            snapshot_record = DictionarySnapshot(
                source=source,
                snapshot_date=snapshot_date,
                minio_path=f"snapshots/source={source}/date={snapshot_date}/",
                dict_types=dict_types,
                entry_count=total_entries,
            )
            session.add(snapshot_record)
            logger.info(f"Created new snapshot for {snapshot_date}")

        session.commit()

        logger.info(
            f"Dictionary snapshot complete: {len(dict_types)} types, {total_entries} entries"
        )

        return {
            "snapshot_date": snapshot_date.isoformat(),
            "dict_types": dict_types,
            "entry_count": total_entries,
            "snapshot_paths": snapshot_paths,
        }


def task_collect_document_list(
    source: str, config: dict, doc_limit: int = 0, **context
) -> list[dict]:
    """Collect list of documents from source API.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        doc_limit: Document limit for testing (0 = no limit)
        **context: Airflow context

    Returns:
        List of document metadata dictionaries
    """
    # Convert doc_limit to int (comes as string from Airflow template)
    doc_limit = int(doc_limit) if doc_limit else 0

    logger.info(f"Starting document list collection for source: {source}")
    if doc_limit > 0:
        logger.warning(f"⚠️ TESTING MODE: Document limit = {doc_limit}")

    # Get collector
    collector = get_collector(source, config)

    # Collect document list
    documents = collector.collect_document_list()

    # Apply limit if specified
    if doc_limit > 0 and len(documents) > doc_limit:
        logger.warning(f"⚠️ Limiting documents from {len(documents)} to {doc_limit}")
        documents = documents[:doc_limit]

    logger.info(f"Collected {len(documents)} documents")

    # Save to temp file for next task
    run_id = context["run_id"]
    minio_config = config["minio"]
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config["secure"],
    )

    temp_path = storage.save_temp_file(
        run_id=run_id,
        task_name="collect_list",
        filename="document_list.json",
        content=json.dumps(documents, ensure_ascii=False),
    )

    logger.info(f"Saved document list to: pipeline-temp/{temp_path}")

    return documents


def task_filter_documents_for_collection(
    source: str,
    config: dict,
    **context,
) -> dict:
    """Filter documents based on database state to skip unchanged documents.

    Compares revision_date from API with database records using bulk query.
    Returns only new or changed documents for download.

    Args:
        source: Data source name
        config: Pipeline configuration dict
        **context: Airflow context with run_id and task_instance

    Returns:
        dict: {"filtered_documents": [...], "metrics": {...}}
    """
    run_id = context["run_id"]

    # Get document_list from XCom (previous task: collect_document_list)
    ti = context["ti"]
    document_list = ti.xcom_pull(task_ids="collect_document_list")

    if document_list is None:
        logger.error("No document_list found in XCom from collect_document_list task")
        raise ValueError("document_list is None - check collect_document_list task output")

    minio_config = config.get("minio", {})
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config.get("secure", False),
    )

    # Check if filtering is enabled
    skip_existing = config.get("skip_existing", True)
    if not skip_existing:
        logger.warning(
            "⚠️  skip_existing=False, downloading all documents without filtering"
        )
        return {
            "filtered_documents": document_list,
            "metrics": {
                "total": len(document_list),
                "filtering_enabled": False,
            },
        }

    # Initialize metrics
    metrics = {
        "total": len(document_list),
        "new": 0,
        "changed": 0,
        "unchanged": 0,
        "skipped": 0,
        "filtering_enabled": True,
    }

    # Handle empty document list
    if not document_list:
        logger.info("No documents to filter")
        return {
            "filtered_documents": [],
            "metrics": metrics,
        }

    # Extract source_ids from document_list
    source_ids = [str(doc.get("dokid")) for doc in document_list if doc.get("dokid")]
    logger.info(f"Checking {len(source_ids)} documents against database")

    # Query database for existing documents
    try:
        db_config = config["database"]
        engine = create_db_engine(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["name"],
            user=db_config["user"],
            password=db_config["password"],
        )
        SessionFactory = create_session_factory(engine)
        with SessionFactory() as session:
            existing_docs = get_existing_documents(session, source, source_ids)
            logger.info(f"Found {len(existing_docs)} existing documents in database")
    except Exception as e:
        logger.error(f"Failed to query database: {e}")
        logger.warning("Falling back to downloading all documents (fail-safe)")
        return {
            "filtered_documents": document_list,
            "metrics": {
                "total": len(document_list),
                "filtering_enabled": False,
                "error": str(e),
            },
        }

    # Filter documents based on revision_date comparison
    filtered_documents = []

    for doc in document_list:
        dokid = str(doc.get("dokid"))
        if not dokid:
            # No dokid, skip this document
            continue

        # Document is new (not in database)
        if dokid not in existing_docs:
            filtered_documents.append(doc)
            metrics["new"] += 1
            logger.debug(f"Document {dokid} is NEW")
            continue

        # Document exists in database - compare revision_date
        db_revision_date, db_content_hash = existing_docs[dokid]

        # Get revision_date from API
        # poddat is the primary field, fallback to orgdat (doc_date)
        api_revision_value = doc.get("poddat") or doc.get("orgdat")
        api_revision_date = parse_date(api_revision_value)

        # Edge case: If both API dates are None, treat as changed
        if api_revision_date is None:
            filtered_documents.append(doc)
            metrics["changed"] += 1
            logger.debug(
                f"Document {dokid} has no revision_date in API, treating as CHANGED"
            )
            continue

        # Edge case: If DB revision_date is None, treat as changed (update DB)
        if db_revision_date is None:
            filtered_documents.append(doc)
            metrics["changed"] += 1
            logger.debug(
                f"Document {dokid} has no revision_date in DB, treating as CHANGED"
            )
            continue

        # Compare revision dates
        if api_revision_date != db_revision_date:
            filtered_documents.append(doc)
            metrics["changed"] += 1
            logger.debug(
                f"Document {dokid} CHANGED: API={api_revision_date} vs DB={db_revision_date}"
            )
        else:
            metrics["unchanged"] += 1
            metrics["skipped"] += 1
            logger.debug(f"Document {dokid} UNCHANGED, skipping")

    # Save filtering report to MinIO temp
    filtering_report = {
        "run_id": run_id,
        "source": source,
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics,
        "filtered_document_ids": [doc.get("dokid") for doc in filtered_documents],
    }

    temp_path = storage.save_temp_file(
        run_id=run_id,
        task_name="filter_documents",
        filename="filtering_report.json",
        content=json.dumps(filtering_report, ensure_ascii=False, default=str),
    )

    logger.info(
        f"Document filtering complete: "
        f"total={metrics['total']}, "
        f"new={metrics['new']}, "
        f"changed={metrics['changed']}, "
        f"unchanged={metrics['unchanged']}, "
        f"skipped={metrics['skipped']}"
    )
    logger.info(
        f"Will download {len(filtered_documents)} documents "
        f"({metrics['new']} new + {metrics['changed']} changed)"
    )
    logger.info(f"Saved filtering report to: pipeline-temp/{temp_path}")

    return {
        "filtered_documents": filtered_documents,
        "metrics": metrics,
    }


def task_collect_document_texts(
    source: str,
    config: dict,
    document_list: list[dict],
    **context,
) -> dict:
    """Collect document texts and save to MinIO with dual-write partitioning.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        document_list: List of document metadata from previous task
        **context: Airflow context

    Returns:
        Dictionary with collection metrics
    """
    # Deserialize document_list if it comes as string from XCom
    if isinstance(document_list, str):
        logger.debug(f"document_list is string, deserializing... (first 100 chars: {document_list[:100]})")
        try:
            # Try JSON first
            document_list = json.loads(document_list)
        except json.JSONDecodeError:
            # Try Python literal_eval (for repr strings)
            import ast
            try:
                document_list = ast.literal_eval(document_list)
            except (ValueError, SyntaxError) as e:
                logger.error(f"Failed to deserialize document_list: {e}")
                logger.error(f"document_list type: {type(document_list)}")
                logger.error(f"document_list value (first 500 chars): {document_list[:500]}")
                raise

    logger.info(f"Starting document text collection for {len(document_list)} documents")

    # Get collector and storage
    collector = get_collector(source, config)
    minio_config = config["minio"]
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config["secure"],
    )

    # Ensure buckets exist
    storage.ensure_buckets(["raw", "processed", "dictionaries", "pipeline-temp"])

    # Collection metrics
    run_id = context["run_id"]
    collection_date = date.today()
    success_count = 0
    failed_count = 0
    failed_docs = []

    # Collect each document
    for idx, doc_meta in enumerate(document_list, 1):
        dokid = doc_meta.get("dokid")
        nreg = doc_meta.get("nreg")

        try:
            # Collect document
            collected = collector.collect_document(doc_meta)

            if collected.text is None:
                logger.warning(f"No text for document {dokid} ({nreg})")
                failed_count += 1
                failed_docs.append({"dokid": dokid, "nreg": nreg, "error": "No text"})
                continue

            # Prepare content and metadata
            text_content = collected.text
            metadata_content = json.dumps(collected.raw_metadata, ensure_ascii=False)

            # Get revision date (use doc_date as fallback)
            revision_date = collected.revision_date or collected.doc_date or collection_date

            # Save to MinIO with dual-write
            paths = storage.save_document_dual(
                source=source,
                source_id=collected.source_id,
                content=text_content,
                metadata=metadata_content,
                revision_date=revision_date,
                collection_date=collection_date,
            )

            success_count += 1

            if idx % 10 == 0:
                logger.info(
                    f"Progress: {idx}/{len(document_list)} "
                    f"(success={success_count}, failed={failed_count})"
                )

        except Exception as e:
            logger.error(f"Failed to collect document {dokid}: {e}")
            failed_count += 1
            failed_docs.append({"dokid": dokid, "nreg": nreg, "error": str(e)})

    # Save failed documents to temp file
    if failed_docs:
        storage.save_temp_file(
            run_id=run_id,
            task_name="collect_texts",
            filename="failed_documents.json",
            content=json.dumps(failed_docs, indent=2, ensure_ascii=False),
        )

    logger.info(
        f"Document text collection complete: success={success_count}, failed={failed_count}"
    )

    return {
        "total": len(document_list),
        "success": success_count,
        "failed": failed_count,
        "failed_docs": failed_docs[:10],  # First 10 failures only
    }
