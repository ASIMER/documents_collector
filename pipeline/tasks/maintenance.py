"""Maintenance tasks for Airflow DAG.

This module provides:
- Cleanup of old temporary files
- Storage maintenance operations
"""

import logging

from pipeline.storage import StorageClient

logger = logging.getLogger(__name__)


def task_cleanup_temp(
    config: dict,
    retention_days: int = 7,
    dry_run: bool = False,
    **context,
) -> dict:
    """Cleanup old temporary files from MinIO.

    Args:
        config: Configuration dictionary (merged global + DAG config)
        retention_days: Number of days to retain files (default: 7)
        dry_run: If True, only log what would be deleted
        **context: Airflow context

    Returns:
        Dictionary with cleanup metrics
    """
    logger.info(f"Starting temp file cleanup (retention: {retention_days} days, dry_run: {dry_run})")

    # Get retention from config if not specified
    if retention_days == 7:  # Default value
        retention_days = config["pipeline"].get("temp_retention_days", 7)

    # Connect to storage
    minio_config = config["minio"]
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config["secure"],
    )

    # Cleanup old files
    deleted_count = storage.cleanup_old_temp_files(
        retention_days=retention_days,
        dry_run=dry_run,
    )

    logger.info(f"Temp file cleanup complete: deleted {deleted_count} objects")

    return {
        "retention_days": retention_days,
        "deleted_count": deleted_count,
        "dry_run": dry_run,
    }
