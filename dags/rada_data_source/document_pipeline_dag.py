"""Document Collection Pipeline DAG for Airflow 3.x.

This DAG orchestrates the complete document collection pipeline:
1. Collect dictionaries from source API
2. Snapshot dictionaries to MinIO
3. Collect document list
4. Filter documents based on database state (skip unchanged)
5. Collect document texts with dual-write partitioning
6. Transform texts to Markdown
7. Load metadata to PostgreSQL with SCD Type 2
8. Run quality checks
9. Generate collection report
10. Cleanup old temporary files

The filtering step (4) significantly optimizes incremental runs by comparing
revision_date from API with database records, skipping documents that haven't
changed. This typically reduces API load by 90%+ on incremental collections.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# Import task functions
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
from pipeline.utils import merge_configs

# Configuration
GLOBAL_CONFIG_PATH = "/opt/airflow/configs/config.yaml"
DAG_CONFIG_PATH = Path(__file__).parent / "config.yml"
SOURCE_NAME = "rada"

CONFIG = merge_configs(
    global_config_path=GLOBAL_CONFIG_PATH,
    dag_config_path=str(DAG_CONFIG_PATH),
    source_name=SOURCE_NAME,
)

# Read DAG documentation from README.md
DAG_DIR = Path(__file__).parent
README_PATH = DAG_DIR / "README.md"

if README_PATH.exists():
    with open(README_PATH, "r", encoding="utf-8") as f:
        DAG_DOCS = f.read()
else:
    DAG_DOCS = "Documentation not found. See README.md in DAG directory."

# Default arguments for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    dag_id="document_pipeline",
    default_args=default_args,
    description="Document Collection Pipeline with SCD Type 2 and Hive-style partitioning",
    schedule="@daily",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "source": Param(
            "rada",
            type="string",
            description="Data source name (rada, court, eu, etc.)",
        ),
        "doc_limit": Param(
            CONFIG.get("pipeline", {}).get("doc_limit", 5),
            type="integer",
            description="Document limit for testing (0 = no limit, full run)",
        ),
    },
    tags=["document-collection", "etl", "llm-data"],
    doc_md=DAG_DOCS,  # Add comprehensive documentation from README.md
)

with dag:
    # Task 1: Collect dictionaries
    collect_dicts = PythonOperator(
        task_id="collect_dictionaries",
        python_callable=task_collect_dictionaries,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
        },
        doc_md="Collects reference dictionaries (statuses, themes) with SCD Type 2. See DAG README for details.",
    )

    # Task 2: Snapshot dictionaries to MinIO
    snapshot_dicts = PythonOperator(
        task_id="snapshot_dictionaries",
        python_callable=task_snapshot_dictionaries,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
        },
        doc_md="Saves current dictionary state to MinIO for versioning and backup. See DAG README for details.",
    )

    # Task 3: Collect document list
    collect_list = PythonOperator(
        task_id="collect_document_list",
        python_callable=task_collect_document_list,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
            "doc_limit": "{{ params.doc_limit }}",
        },
        do_xcom_push=True,
        doc_md="Retrieves list of recently updated documents from source API. See DAG README for details.",
    )

    # Task 4: Filter documents based on database state
    filter_docs = PythonOperator(
        task_id="filter_documents_for_collection",
        python_callable=task_filter_documents_for_collection,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
        },
        do_xcom_push=True,
        doc_md="Filters documents based on DB state. Skips unchanged documents to optimize API usage and reduce processing time for incremental runs.",
    )

    # Task 5: Collect document texts (now uses filtered list)
    collect_texts = PythonOperator(
        task_id="collect_document_texts",
        python_callable=task_collect_document_texts,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
            "document_list": "{{ task_instance.xcom_pull(task_ids='filter_documents_for_collection')['filtered_documents'] }}",
        },
        execution_timeout=timedelta(hours=2),  # Allow up to 2 hours for large collections
        doc_md="Downloads filtered documents only (new and changed). Significantly reduces API load for incremental runs.",
    )

    # Task 5: Transform to Markdown
    transform = PythonOperator(
        task_id="transform_to_markdown",
        python_callable=task_transform_all,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
            "document_list": "{{ task_instance.xcom_pull(task_ids='collect_document_list') }}",
        },
        doc_md="Transforms raw text to Markdown with YAML frontmatter and dual-write partitioning. See DAG README for details.",
    )

    # Task 6: Load metadata to database
    load_metadata = PythonOperator(
        task_id="load_metadata_to_db",
        python_callable=task_load_metadata_scd2,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
            "document_list": "{{ task_instance.xcom_pull(task_ids='collect_document_list') }}",
        },
        doc_md="Loads document metadata to PostgreSQL with SCD Type 2 history tracking. See DAG README for details.",
    )

    # Task 7: Quality checks
    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=task_quality_checks,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
        },
        doc_md="Validates data quality (title, text existence, status ID, text length). See DAG README for details.",
    )

    # Task 8: Generate report
    report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        op_kwargs={
            "source": "{{ params.source }}",
            "config": CONFIG,
        },
        doc_md="Generates collection report with metrics and statistics. See DAG README for details.",
    )

    # Task 9: Cleanup temporary files
    cleanup = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=task_cleanup_temp,
        op_kwargs={
            "config": CONFIG,
            "retention_days": 7,
            "dry_run": False,
        },
        doc_md="Removes temporary files older than retention period (default: 7 days). See DAG README for details.",
    )

    # Define task dependencies
    # Parallel: collect_dicts -> snapshot_dicts AND collect_list (can run simultaneously)
    # Then sequential: collect_list -> filter_docs -> collect_texts
    # Then: [snapshot_dicts, collect_texts] -> transform -> load_metadata -> quality -> report
    # Finally: cleanup (independent, runs last)

    collect_dicts >> snapshot_dicts
    collect_list >> filter_docs >> collect_texts
    [snapshot_dicts, collect_texts] >> transform
    transform >> load_metadata >> quality >> report >> cleanup
