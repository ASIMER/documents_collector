"""Quality check and reporting tasks for Airflow DAG.

This module provides:
- Data quality validation
- Report generation with metrics
"""

import json
import logging
from datetime import datetime

from pipeline.db import CollectionRun, create_db_engine, create_session_factory
from pipeline.storage import StorageClient

logger = logging.getLogger(__name__)


def task_quality_checks(source: str, config: dict, **context) -> dict:
    """Run data quality checks on collected documents.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        **context: Airflow context

    Returns:
        Dictionary with quality check results
    """
    logger.info(f"Starting quality checks for source: {source}")

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

    # Quality check results
    checks = {
        "empty_title": 0,
        "missing_text": 0,
        "invalid_status": 0,
        "short_text": 0,
        "total_documents": 0,
    }

    from pipeline.db import Dictionary, Document

    with SessionFactory() as session:
        # Get current documents
        documents = (
            session.query(Document).filter(Document.source == source, Document.is_current == True).all()
        )

        checks["total_documents"] = len(documents)

        # Get valid status IDs
        valid_statuses = set(
            session.query(Dictionary.entry_id)
            .filter(Dictionary.source == source, Dictionary.dict_type == "status", Dictionary.is_current == True)
            .distinct()
        )
        valid_status_ids = {status[0] for status in valid_statuses}

        # Run checks
        for doc in documents:
            # Check: title not empty
            if not doc.title or not doc.title.strip():
                checks["empty_title"] += 1
                logger.warning(f"Empty title: {doc.source_id}")

            # Check: text exists
            if not doc.has_text:
                checks["missing_text"] += 1
                logger.debug(f"Missing text: {doc.source_id}")

            # Check: status_id in dictionary
            if doc.status_id is not None and doc.status_id not in valid_status_ids:
                checks["invalid_status"] += 1
                logger.warning(f"Invalid status {doc.status_id}: {doc.source_id}")

            # Check: minimum text length
            min_length = config["pipeline"].get("min_text_length", 50)
            if doc.text_length > 0 and doc.text_length < min_length:
                checks["short_text"] += 1
                logger.debug(f"Short text ({doc.text_length} chars): {doc.source_id}")

    # Calculate quality score
    total = checks["total_documents"]
    if total > 0:
        quality_score = (
            (total - checks["empty_title"] - checks["invalid_status"]) / total * 100
        )
    else:
        quality_score = 0

    checks["quality_score"] = round(quality_score, 2)

    logger.info(
        f"Quality checks complete: "
        f"score={quality_score:.2f}%, "
        f"empty_title={checks['empty_title']}, "
        f"missing_text={checks['missing_text']}, "
        f"invalid_status={checks['invalid_status']}"
    )

    return checks


def task_generate_report(source: str, config: dict, **context) -> dict:
    """Generate collection report with metrics.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        **context: Airflow context

    Returns:
        Dictionary with report data
    """
    logger.info(f"Generating collection report for source: {source}")

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

    # Get collection run metrics
    run_id = context["run_id"]

    from pipeline.db import Document

    with SessionFactory() as session:
        # Get collection run
        collection_run = (
            session.query(CollectionRun).filter(CollectionRun.run_id == run_id).first()
        )

        if not collection_run:
            logger.error(f"Collection run not found: {run_id}")
            return {}

        # Get document statistics
        documents = (
            session.query(Document).filter(Document.source == source, Document.is_current == True).all()
        )

        total_documents = len(documents)
        documents_with_text = sum(1 for d in documents if d.has_text)
        total_text_length = sum(d.text_length for d in documents)
        total_word_count = sum(d.word_count for d in documents)

        # Calculate averages
        avg_text_length = total_text_length / documents_with_text if documents_with_text > 0 else 0
        avg_word_count = total_word_count / documents_with_text if documents_with_text > 0 else 0

        # Build report
        report = {
            "run_id": run_id,
            "source": source,
            "timestamp": datetime.now().isoformat(),
            "collection": {
                "documents_found": collection_run.documents_found,
                "documents_new": collection_run.documents_new,
                "documents_updated": collection_run.documents_updated,
                "documents_unchanged": collection_run.documents_unchanged,
                "documents_failed": collection_run.documents_failed,
            },
            "statistics": {
                "total_documents": total_documents,
                "documents_with_text": documents_with_text,
                "documents_without_text": total_documents - documents_with_text,
                "total_text_length": total_text_length,
                "total_word_count": total_word_count,
                "avg_text_length": round(avg_text_length, 2),
                "avg_word_count": round(avg_word_count, 2),
            },
            "duration": {
                "started_at": collection_run.started_at.isoformat(),
                "completed_at": collection_run.completed_at.isoformat()
                if collection_run.completed_at
                else None,
            },
        }

        # Save report to temp file
        minio_config = config["minio"]
        storage = StorageClient(
            endpoint=minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config["secure"],
        )

        report_path = storage.save_temp_file(
            run_id=run_id,
            task_name="report",
            filename="collection_report.json",
            content=json.dumps(report, indent=2, ensure_ascii=False),
        )

        logger.info(f"Report saved to: pipeline-temp/{report_path}")
        logger.info(
            f"Collection summary: "
            f"{total_documents} documents, "
            f"{documents_with_text} with text, "
            f"avg {avg_word_count:.0f} words"
        )

        return report
