"""Transformation tasks for Airflow DAG.

This module provides:
- Text to Markdown transformation with YAML frontmatter
- Dual-write to both partitioning schemes
"""

import json
import logging
from datetime import date

from pipeline.storage import StorageClient

logger = logging.getLogger(__name__)


def task_transform_all(
    source: str,
    config: dict,
    document_list: list[dict],
    **context,
) -> dict:
    """Transform all documents from raw text to Markdown with YAML frontmatter.

    Args:
        source: Data source name
        config: Configuration dictionary (merged global + DAG config)
        document_list: List of document metadata
        **context: Airflow context

    Returns:
        Dictionary with transformation metrics
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

    logger.info(f"Starting Markdown transformation for {len(document_list)} documents")

    # Connect to storage
    minio_config = config["minio"]
    storage = StorageClient(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=minio_config["secure"],
    )

    # Transformation metrics
    success_count = 0
    failed_count = 0
    skipped_count = 0
    collection_date = date.today()

    for idx, doc_meta in enumerate(document_list, 1):
        dokid = str(doc_meta.get("dokid"))
        nreg = doc_meta.get("nreg")

        try:
            # Get revision date from metadata
            from pipeline.utils import parse_date

            revision_date = parse_date(doc_meta.get("poddat")) or collection_date

            # Get document paths
            paths = storage.get_document_paths(
                source=source,
                source_id=dokid,
                revision_date=revision_date,
                collection_date=collection_date,
            )

            # Read raw text from by_revision partition
            try:
                raw_text = storage.get_object("raw", paths.raw_by_revision).decode("utf-8")
            except Exception as e:
                logger.warning(f"Failed to read raw text for {dokid}: {e}")
                skipped_count += 1
                continue

            if not raw_text.strip():
                logger.warning(f"Empty text for {dokid}")
                skipped_count += 1
                continue

            # Transform to Markdown with YAML frontmatter
            markdown = _create_markdown_with_frontmatter(doc_meta, raw_text)

            # Save to processed bucket with dual-write
            storage.save_processed_dual(
                source=source,
                source_id=dokid,
                content=markdown,
                revision_date=revision_date,
                collection_date=collection_date,
            )

            success_count += 1

            if idx % 10 == 0:
                logger.info(
                    f"Progress: {idx}/{len(document_list)} "
                    f"(success={success_count}, skipped={skipped_count})"
                )

        except Exception as e:
            logger.error(f"Failed to transform document {dokid}: {e}")
            failed_count += 1

    logger.info(
        f"Markdown transformation complete: "
        f"success={success_count}, skipped={skipped_count}, failed={failed_count}"
    )

    return {
        "total": len(document_list),
        "success": success_count,
        "skipped": skipped_count,
        "failed": failed_count,
    }


def _create_markdown_with_frontmatter(doc_meta: dict, text: str) -> str:
    """Create Markdown document with YAML frontmatter.

    Args:
        doc_meta: Document metadata
        text: Document text

    Returns:
        Markdown content with frontmatter
    """
    from pipeline.utils import parse_types

    # Extract metadata
    dokid = doc_meta.get("dokid")
    nreg = doc_meta.get("nreg")
    title = doc_meta.get("nazva", "")
    status = doc_meta.get("status")
    types = parse_types(doc_meta.get("types"))
    org = doc_meta.get("orgid") or doc_meta.get("org")
    orgdat = doc_meta.get("orgdat")
    poddat = doc_meta.get("poddat")

    # Format dates
    from pipeline.utils import parse_date

    doc_date = parse_date(orgdat)
    revision_date = parse_date(poddat)

    # Build YAML frontmatter
    frontmatter_lines = [
        "---",
        f'source: "{source if "source" in locals() else "rada"}"',
        f"dokid: {dokid}",
        f'nreg: "{nreg}"',
        f'title: "{title}"',
    ]

    if doc_date:
        frontmatter_lines.append(f"doc_date: {doc_date.isoformat()}")

    if revision_date:
        frontmatter_lines.append(f"revision_date: {revision_date.isoformat()}")

    if status is not None:
        frontmatter_lines.append(f"status: {status}")

    if types:
        types_str = ", ".join(str(t) for t in types)
        frontmatter_lines.append(f"types: [{types_str}]")

    if org is not None:
        frontmatter_lines.append(f"org: {org}")

    frontmatter_lines.append("---")
    frontmatter_lines.append("")  # Empty line after frontmatter

    # Clean text
    cleaned_text = _clean_text(text)

    # Combine frontmatter and text
    markdown = "\n".join(frontmatter_lines) + cleaned_text

    return markdown


def _clean_text(text: str) -> str:
    """Clean document text.

    Args:
        text: Raw text

    Returns:
        Cleaned text
    """
    # Remove excessive whitespace
    lines = []
    prev_empty = False

    for line in text.split("\n"):
        stripped = line.strip()

        # Skip multiple consecutive empty lines
        if not stripped:
            if not prev_empty:
                lines.append("")
            prev_empty = True
        else:
            lines.append(line.rstrip())
            prev_empty = False

    # Join lines and ensure single trailing newline
    cleaned = "\n".join(lines).strip() + "\n"

    return cleaned
