"""MinIO storage client with Hive-style partitioning and dual-write support.

This module provides:
- Hive-style date partitioning (by_revision and by_collection)
- Dual-write: save documents to both partitioning schemes
- Temporary files management with auto-cleanup
- S3-compatible object storage operations
"""

import io
import logging
from dataclasses import dataclass
from datetime import date, timedelta

from minio import Minio
from minio.deleteobjects import DeleteObject

logger = logging.getLogger(__name__)


@dataclass
class DocumentPaths:
    """All storage paths for a document (dual-write partitioning)."""

    # Raw document paths
    raw_by_revision: str  # raw/by_revision/source={s}/year={y}/month={m}/day={d}/{id}.txt
    raw_by_collection: str  # raw/by_collection/date={date}/source={s}/{id}.txt
    raw_meta_by_revision: str  # raw/by_revision/.../...meta.json
    raw_meta_by_collection: str  # raw/by_collection/.../...meta.json

    # Processed document paths
    processed_by_revision: str  # processed/by_revision/source={s}/year={y}/month={m}/day={d}/{id}.md
    processed_by_collection: str  # processed/by_collection/date={date}/source={s}/{id}.md


class StorageClient:
    """MinIO storage client with partitioning and dual-write support."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ):
        """Initialize MinIO client.

        Args:
            endpoint: MinIO endpoint (host:port)
            access_key: Access key
            secret_key: Secret key
            secure: Use HTTPS (default: False for local development)
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        logger.info(f"Connected to MinIO at {endpoint}")

    def ensure_buckets(self, bucket_names: list[str]) -> None:
        """Ensure buckets exist, create if missing.

        Args:
            bucket_names: List of bucket names to create
        """
        for bucket_name in bucket_names:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.debug(f"Bucket exists: {bucket_name}")

    def get_document_paths(
        self,
        source: str,
        source_id: str,
        revision_date: date,
        collection_date: date,
    ) -> DocumentPaths:
        """Generate all storage paths for a document (dual-write).

        Args:
            source: Data source name (e.g., 'rada')
            source_id: Document ID in source system
            revision_date: Document revision date (for by_revision partitioning)
            collection_date: Collection date (for by_collection partitioning)

        Returns:
            DocumentPaths with all path combinations
        """
        # By revision partitioning
        rev_year = revision_date.year
        rev_month = f"{revision_date.month:02d}"
        rev_day = f"{revision_date.day:02d}"

        raw_by_revision = (
            f"by_revision/source={source}/year={rev_year}/month={rev_month}/day={rev_day}/{source_id}.txt"
        )
        raw_meta_by_revision = (
            f"by_revision/source={source}/year={rev_year}/month={rev_month}/day={rev_day}/{source_id}.meta.json"
        )
        processed_by_revision = (
            f"by_revision/source={source}/year={rev_year}/month={rev_month}/day={rev_day}/{source_id}.md"
        )

        # By collection partitioning
        col_date = collection_date.isoformat()
        raw_by_collection = f"by_collection/date={col_date}/source={source}/{source_id}.txt"
        raw_meta_by_collection = f"by_collection/date={col_date}/source={source}/{source_id}.meta.json"
        processed_by_collection = f"by_collection/date={col_date}/source={source}/{source_id}.md"

        return DocumentPaths(
            raw_by_revision=raw_by_revision,
            raw_by_collection=raw_by_collection,
            raw_meta_by_revision=raw_meta_by_revision,
            raw_meta_by_collection=raw_meta_by_collection,
            processed_by_revision=processed_by_revision,
            processed_by_collection=processed_by_collection,
        )

    def save_document_dual(
        self,
        source: str,
        source_id: str,
        content: str | bytes,
        metadata: str | bytes,
        revision_date: date,
        collection_date: date,
    ) -> DocumentPaths:
        """Save document to both partitioning schemes (dual-write).

        Args:
            source: Data source name
            source_id: Document ID
            content: Document content (text or binary)
            metadata: Document metadata (JSON)
            revision_date: Document revision date
            collection_date: Collection date

        Returns:
            DocumentPaths with all saved paths
        """
        paths = self.get_document_paths(source, source_id, revision_date, collection_date)

        # Convert content to bytes if string
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = content

        if isinstance(metadata, str):
            metadata_bytes = metadata.encode("utf-8")
        else:
            metadata_bytes = metadata

        # Dual-write: save to both partitioning schemes
        for bucket, path, data in [
            ("raw", paths.raw_by_revision, content_bytes),
            ("raw", paths.raw_by_collection, content_bytes),
            ("raw", paths.raw_meta_by_revision, metadata_bytes),
            ("raw", paths.raw_meta_by_collection, metadata_bytes),
        ]:
            self._put_object(bucket, path, data)

        logger.debug(f"Saved document {source_id} with dual-write partitioning")
        return paths

    def save_processed_dual(
        self,
        source: str,
        source_id: str,
        content: str | bytes,
        revision_date: date,
        collection_date: date,
    ) -> tuple[str, str]:
        """Save processed document to both partitioning schemes.

        Args:
            source: Data source name
            source_id: Document ID
            content: Processed content (Markdown)
            revision_date: Document revision date
            collection_date: Collection date

        Returns:
            Tuple of (path_by_revision, path_by_collection)
        """
        paths = self.get_document_paths(source, source_id, revision_date, collection_date)

        # Convert content to bytes if string
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = content

        # Dual-write: save to both partitioning schemes
        self._put_object("processed", paths.processed_by_revision, content_bytes)
        self._put_object("processed", paths.processed_by_collection, content_bytes)

        logger.debug(f"Saved processed document {source_id} with dual-write partitioning")
        return paths.processed_by_revision, paths.processed_by_collection

    def get_object(self, bucket: str, object_name: str) -> bytes:
        """Get object from storage.

        Args:
            bucket: Bucket name
            object_name: Object path

        Returns:
            Object content as bytes
        """
        response = self.client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists in storage.

        Args:
            bucket: Bucket name
            object_name: Object path

        Returns:
            True if object exists
        """
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except Exception:
            return False

    def get_temp_path(self, run_id: str, task_name: str, filename: str) -> str:
        """Generate path for temporary task file.

        Args:
            run_id: Airflow run ID
            task_name: Task name
            filename: File name

        Returns:
            Temporary file path: date={date}/run_id={run_id}/task_{task_name}/{filename}
        """
        today = date.today().isoformat()
        return f"date={today}/run_id={run_id}/task_{task_name}/{filename}"

    def save_temp_file(
        self,
        run_id: str,
        task_name: str,
        filename: str,
        content: str | bytes,
    ) -> str:
        """Save temporary task file.

        Args:
            run_id: Airflow run ID
            task_name: Task name
            filename: File name
            content: File content

        Returns:
            Saved file path
        """
        path = self.get_temp_path(run_id, task_name, filename)

        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = content

        self._put_object("pipeline-temp", path, content_bytes)
        logger.debug(f"Saved temp file: {path}")
        return path

    def cleanup_old_temp_files(self, retention_days: int = 7, dry_run: bool = False) -> int:
        """Delete temporary files older than retention period.

        Args:
            retention_days: Number of days to retain files
            dry_run: If True, only log what would be deleted

        Returns:
            Number of deleted objects
        """
        cutoff_date = date.today() - timedelta(days=retention_days)
        logger.info(f"Cleaning up temp files older than {cutoff_date} (retention: {retention_days} days)")

        # List all objects in pipeline-temp bucket
        try:
            objects = self.client.list_objects("pipeline-temp", recursive=True)
        except Exception as e:
            logger.error(f"Failed to list temp files: {e}")
            return 0

        # Find old objects by parsing date from path
        objects_to_delete = []
        for obj in objects:
            # Extract date from path: date=YYYY-MM-DD/...
            if obj.object_name.startswith("date="):
                date_str = obj.object_name.split("/")[0].replace("date=", "")
                try:
                    obj_date = date.fromisoformat(date_str)
                    if obj_date < cutoff_date:
                        objects_to_delete.append(obj.object_name)
                except ValueError:
                    logger.warning(f"Invalid date format in path: {obj.object_name}")

        if not objects_to_delete:
            logger.info("No old temp files to delete")
            return 0

        if dry_run:
            logger.info(f"[DRY RUN] Would delete {len(objects_to_delete)} objects")
            return len(objects_to_delete)

        # Delete objects
        delete_object_list = [DeleteObject(name) for name in objects_to_delete]
        errors = self.client.remove_objects("pipeline-temp", delete_object_list)

        error_count = sum(1 for _ in errors)
        deleted_count = len(objects_to_delete) - error_count

        logger.info(f"Deleted {deleted_count} old temp files ({error_count} errors)")
        return deleted_count

    def _put_object(self, bucket: str, object_name: str, data: bytes) -> None:
        """Put object to storage (internal helper).

        Args:
            bucket: Bucket name
            object_name: Object path
            data: Object content as bytes
        """
        data_stream = io.BytesIO(data)
        self.client.put_object(
            bucket,
            object_name,
            data_stream,
            length=len(data),
        )
