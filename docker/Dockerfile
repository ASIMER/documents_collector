# Base image with Airflow 2.10.4 and Python 3.12
FROM apache/airflow:2.10.4-python3.12

# Copy UV binary from official image (as root)
USER root
COPY --from=ghcr.io/astral-sh/uv:0.8.18 /uv /uvx /usr/local/bin/

# Switch to airflow user for package installation
USER airflow

# Install additional Python dependencies using UV
# UV automatically detects the correct site-packages location
RUN uv pip install --no-cache \
    minio>=7.2 \
    "psycopg[binary]>=3.1" \
    click>=8.1

# Set working directory
WORKDIR /opt/airflow

# Set environment variable to ensure pipeline modules are importable
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
