# ============================================================================
# Document Collection Pipeline - Makefile
# ============================================================================
# Simplified commands for common operations
# ============================================================================

.PHONY: help setup up down restart logs logs-scheduler logs-webserver \
        run-pipeline check-db check-minio clean

# Default target
help:
	@echo "Document Collection Pipeline - Available Commands:"
	@echo ""
	@echo "  make setup           - Create directories with correct permissions"
	@echo "  make up              - Start all Docker services"
	@echo "  make down            - Stop all Docker services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - View logs from all services"
	@echo "  make logs-scheduler  - View Airflow scheduler logs"
	@echo "  make logs-webserver  - View Airflow webserver logs"
	@echo "  make run-pipeline    - Run pipeline via CLI (SOURCE=rada)"
	@echo "  make check-db        - Check PostgreSQL database status"
	@echo "  make check-minio     - Check MinIO buckets"
	@echo "  make clean           - Clean temporary files and volumes"
	@echo ""
	@echo "Example usage:"
	@echo "  make up"
	@echo "  make run-pipeline SOURCE=rada"
	@echo "  make check-db"

# ────────────────────────────────────────────────────────────────────────────
# Setup
# ────────────────────────────────────────────────────────────────────────────

setup:
	@echo "Creating directories with correct permissions..."
	@mkdir -p outputs/logs
	@mkdir -p data
	@chmod -R 777 outputs/logs
	@chmod -R 777 data
	@echo "Directories created successfully"
	@echo ""
	@echo "Directory structure:"
	@ls -la outputs/
	@ls -la data/

# ────────────────────────────────────────────────────────────────────────────
# Docker Operations
# ────────────────────────────────────────────────────────────────────────────

up: setup
	@echo "Starting Docker services..."
	cd docker && docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo ""
	@echo "Services status:"
	cd docker && docker-compose ps

down:
	@echo "Stopping Docker services..."
	cd docker && docker-compose down

restart:
	@echo "Restarting Docker services..."
	cd docker && docker-compose restart
	@sleep 10
	@echo ""
	@echo "Services status:"
	cd docker && docker-compose ps

# ────────────────────────────────────────────────────────────────────────────
# Logs
# ────────────────────────────────────────────────────────────────────────────

logs:
	cd docker && docker-compose logs -f

logs-scheduler:
	cd docker && docker-compose logs -f airflow-scheduler

logs-webserver:
	cd docker && docker-compose logs -f airflow-webserver

# ────────────────────────────────────────────────────────────────────────────
# Pipeline Operations
# ────────────────────────────────────────────────────────────────────────────

SOURCE ?= rada
LIMIT ?= 5

run-pipeline:
	@echo "Running pipeline for source: $(SOURCE) (limit: $(LIMIT))"
	cd docker && docker-compose exec airflow-scheduler \
		python /opt/airflow/scripts/run_pipeline.py full --source $(SOURCE) --limit $(LIMIT)

# ────────────────────────────────────────────────────────────────────────────
# Health Checks
# ────────────────────────────────────────────────────────────────────────────

check-db:
	@echo "PostgreSQL Database Status:"
	@echo ""
	cd docker && docker-compose exec postgres psql -U pipeline_user -d pipeline \
		-c "SELECT 'Documents (current):' as metric, COUNT(*)::text as count FROM documents WHERE is_current=TRUE \
		    UNION ALL \
		    SELECT 'Documents (total):', COUNT(*)::text FROM documents \
		    UNION ALL \
		    SELECT 'Dictionaries:', COUNT(*)::text FROM dictionaries WHERE is_current=TRUE \
		    UNION ALL \
		    SELECT 'Collection runs:', COUNT(*)::text FROM collection_runs;"

check-minio:
	@echo "MinIO Buckets:"
	@echo ""
	cd docker && docker-compose exec -T minio mc ls local/

# ────────────────────────────────────────────────────────────────────────────
# Cleanup
# ────────────────────────────────────────────────────────────────────────────

clean:
	@echo "WARNING: This will remove all containers, volumes, and data!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	cd docker && docker-compose down -v
	@echo "Cleaned up all Docker resources"
