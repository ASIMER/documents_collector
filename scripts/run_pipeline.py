#!/usr/bin/env python3
"""CLI entry point for document collection pipeline.

This script provides a command-line interface to run the pipeline
without Airflow, useful for development and testing.
"""

import logging
import sys
from pathlib import Path

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.tasks.collect import (
    task_collect_dictionaries,
    task_collect_document_list,
    task_collect_document_texts,
    task_filter_documents_for_collection,
    task_snapshot_dictionaries,
)
from pipeline.tasks.load import task_load_metadata_scd2
from pipeline.tasks.maintenance import task_cleanup_temp
from pipeline.tasks.quality import task_generate_report, task_quality_checks
from pipeline.tasks.transform import task_transform_all
from pipeline.utils import load_config, merge_configs, setup_logging

logger = logging.getLogger(__name__)


def get_merged_config(global_config_path: str, source: str) -> dict:
    """Load and merge global + DAG-specific config for CLI.

    Args:
        global_config_path: Path to global config
        source: Data source name

    Returns:
        Merged configuration dictionary
    """
    # Try to find DAG-specific config
    dag_config_path = project_root / "dags" / f"{source}_data_source" / "config.yml"

    if dag_config_path.exists():
        logger.debug(f"Loading DAG config from: {dag_config_path}")
        return merge_configs(
            global_config_path=global_config_path,
            dag_config_path=str(dag_config_path),
            source_name=source,
        )
    else:
        # Fallback to global config only
        logger.warning(f"DAG config not found at {dag_config_path}, using global config only")
        return load_config(global_config_path)


@click.group()
@click.option(
    "--config",
    default="configs/config.yaml",
    type=click.Path(exists=True),
    help="Path to configuration file",
)
@click.option("--log-level", default="INFO", help="Logging level")
@click.pass_context
def cli(ctx, config, log_level):
    """Document Collection Pipeline CLI.

    Run individual pipeline steps or the full pipeline.
    """
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["log_level"] = log_level

    # Setup logging
    setup_logging(level=log_level)


@cli.command()
@click.option("--source", required=True, help="Data source name (rada, court, etc.)")
@click.option("--limit", default=0, type=int, help="Limit number of documents (0 = no limit)")
@click.pass_context
def full(ctx, source, limit):
    """Run full pipeline end-to-end."""
    from datetime import datetime

    global_config_path = ctx.obj["config"]
    config = get_merged_config(global_config_path, source)
    run_id = f"cli_{source}_{int(datetime.now().timestamp())}"

    logger.info(f"Starting full pipeline for source: {source}")
    logger.info(f"Run ID: {run_id}")
    if limit > 0:
        logger.info(f"Document limit: {limit}")

    # Create context
    context = {"run_id": run_id}

    try:
        # Step 1: Collect dictionaries
        logger.info("Step 1/10: Collecting dictionaries...")
        task_collect_dictionaries(source, config, **context)

        # Step 2: Snapshot dictionaries
        logger.info("Step 2/10: Snapshotting dictionaries...")
        task_snapshot_dictionaries(source, config, **context)

        # Step 3: Collect document list
        logger.info("Step 3/10: Collecting document list...")
        document_list = task_collect_document_list(source, config, **context)

        # Apply limit if specified (before filtering)
        if limit > 0:
            logger.info(f"Applying limit: {limit} of {len(document_list)} documents")
            document_list = document_list[:limit]

        # Step 4: Filter documents based on database state
        logger.info("Step 4/10: Filtering documents...")

        # Create mock task instance for CLI (emulate XCom)
        class MockTaskInstance:
            def __init__(self, data):
                self._data = data

            def xcom_pull(self, task_ids):
                return self._data

        # Add mock task instance to context
        context_with_ti = {**context, "ti": MockTaskInstance(document_list)}

        filter_result = task_filter_documents_for_collection(source, config, **context_with_ti)
        filtered_documents = filter_result['filtered_documents']
        filter_metrics = filter_result['metrics']
        logger.info(
            f"Filtering complete: {len(filtered_documents)} documents to download "
            f"(new={filter_metrics.get('new', 0)}, changed={filter_metrics.get('changed', 0)}, "
            f"skipped={filter_metrics.get('skipped', 0)})"
        )

        # Step 5: Collect document texts
        logger.info("Step 5/10: Collecting document texts...")
        task_collect_document_texts(source, config, filtered_documents, **context)

        # Step 6: Transform to Markdown
        logger.info("Step 6/10: Transforming to Markdown...")
        task_transform_all(source, config, filtered_documents, **context)

        # Step 7: Load metadata
        logger.info("Step 7/10: Loading metadata to database...")
        task_load_metadata_scd2(source, config, filtered_documents, **context)

        # Step 8: Quality checks
        logger.info("Step 8/10: Running quality checks...")
        task_quality_checks(source, config, **context)

        # Step 9: Generate report
        logger.info("Step 9/10: Generating report...")
        task_generate_report(source, config, **context)

        # Step 10: Cleanup (skip in CLI)
        logger.info("Step 10/10: Cleanup (skipped in CLI mode)")

        logger.info("Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option("--source", required=True, help="Data source name")
@click.pass_context
def collect_dicts(ctx, source):
    """Collect dictionaries from source."""
    global_config_path = ctx.obj["config"]
    config = get_merged_config(global_config_path, source)
    context = {"run_id": f"cli_{source}"}

    logger.info("Collecting dictionaries...")
    result = task_collect_dictionaries(source, config, **context)
    logger.info(f"Result: {result}")


@cli.command()
@click.option("--source", required=True, help="Data source name")
@click.option("--limit", default=0, type=int, help="Limit number of documents (0 = no limit)")
@click.pass_context
def collect_docs(ctx, source, limit):
    """Collect documents from source."""
    from datetime import datetime

    global_config_path = ctx.obj["config"]
    config = get_merged_config(global_config_path, source)
    run_id = f"cli_{source}_{int(datetime.now().timestamp())}"
    context = {"run_id": run_id}

    logger.info("Collecting document list...")
    document_list = task_collect_document_list(source, config, **context)

    # Apply limit if specified
    if limit > 0:
        logger.info(f"Applying limit: {limit} of {len(document_list)} documents")
        document_list = document_list[:limit]

    logger.info("Collecting document texts...")
    result = task_collect_document_texts(source, config, document_list, **context)
    logger.info(f"Result: {result}")


@cli.command()
@click.option("--retention-days", default=7, help="Days to retain temp files")
@click.option("--dry-run", is_flag=True, help="Only show what would be deleted")
@click.pass_context
def cleanup(ctx, retention_days, dry_run):
    """Cleanup old temporary files."""
    global_config_path = ctx.obj["config"]
    # Cleanup doesn't need source-specific config, use global
    config = load_config(global_config_path)
    context = {"run_id": "cli_cleanup"}

    logger.info("Running cleanup...")
    result = task_cleanup_temp(config, retention_days, dry_run, **context)
    logger.info(f"Result: {result}")


@cli.command()
@click.option("--source", required=True, help="Data source name")
@click.pass_context
def report(ctx, source):
    """Generate collection report."""

    global_config_path = ctx.obj["config"]
    config = get_merged_config(global_config_path, source)
    run_id = f"cli_{source}"
    context = {"run_id": run_id}

    logger.info("Generating report...")
    result = task_generate_report(source, config, **context)
    logger.info(f"Result: {result}")


if __name__ == "__main__":
    from datetime import datetime

    cli()
