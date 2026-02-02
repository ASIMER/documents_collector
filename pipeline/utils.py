"""Utility functions for the document collection pipeline.

This module provides:
- RateLimiter: Enforce rate limits on API requests
- Configuration loading with environment variable substitution
- Date parsing utilities
- Logging setup
"""

import logging
import os
import random
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml


class RateLimiter:
    """Rate limiter for API requests with random pause intervals."""

    def __init__(self, min_pause: float = 5.0, max_pause: float = 7.0):
        """Initialize rate limiter.

        Args:
            min_pause: Minimum pause duration in seconds
            max_pause: Maximum pause duration in seconds
        """
        self.min_pause = min_pause
        self.max_pause = max_pause
        self.last_request_time: float | None = None

    def wait(self) -> None:
        """Wait for rate limit interval before next request."""
        if self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            pause = random.uniform(self.min_pause, self.max_pause)
            if elapsed < pause:
                sleep_time = pause - elapsed
                time.sleep(sleep_time)
        self.last_request_time = time.time()

    def wait_and_execute(self, func: callable, *args: Any, **kwargs: Any) -> Any:
        """Wait for rate limit, then execute function.

        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function

        Returns:
            Result of function execution
        """
        self.wait()
        return func(*args, **kwargs)


def load_config(config_path: str | Path) -> dict:
    """Load configuration from YAML file with environment variable substitution.

    Replaces ${VAR_NAME} or ${VAR_NAME:-default} patterns with environment variables.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configuration dictionary with substituted values
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Substitute environment variables
    content = _substitute_env_vars(content)

    # Parse YAML
    config = yaml.safe_load(content)
    return config


def _substitute_env_vars(text: str) -> str:
    """Substitute ${VAR} or ${VAR:-default} patterns with environment variables.

    Args:
        text: Text containing environment variable patterns

    Returns:
        Text with substituted values
    """
    import re

    # Pattern: ${VAR_NAME} or ${VAR_NAME:-default_value}
    pattern = r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]+))?\}"

    def replace(match: re.Match) -> str:
        var_name = match.group(1)
        default_value = match.group(3)
        return os.environ.get(var_name, default_value or "")

    return re.sub(pattern, replace, text)


def parse_date(date_value: int | str | None) -> date | None:
    """Parse date from various formats.

    Args:
        date_value: Date as YYYYMMDD integer, ISO string, or None

    Returns:
        date object or None if input is None/0
    """
    if date_value is None or date_value == 0:
        return None

    if isinstance(date_value, int):
        # Parse YYYYMMDD format
        date_str = str(date_value)
        if len(date_str) == 8:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return date(year, month, day)
        return None

    if isinstance(date_value, str):
        # Try parsing ISO format
        try:
            return datetime.fromisoformat(date_value).date()
        except ValueError:
            return None

    return None


def parse_types(types_value: int | str | None) -> list[int]:
    """Parse document types field which can be int or pipe-separated string.

    Args:
        types_value: Single type ID or pipe-separated string "95|6|168"

    Returns:
        List of type IDs
    """
    if types_value is None:
        return []

    if isinstance(types_value, int):
        return [types_value]

    if isinstance(types_value, str):
        return [int(t.strip()) for t in types_value.split("|") if t.strip()]

    return []


def setup_logging(
    level: str = "INFO",
    log_format: str | None = None,
    log_file: str | Path | None = None,
) -> None:
    """Setup logging configuration for the pipeline.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_format: Log message format string
        log_file: Optional log file path
    """
    if log_format is None:
        log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[],
    )

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.root.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.root.addHandler(file_handler)


def substitute_env_vars(config: dict) -> dict:
    """Substitute environment variables in a config dictionary.

    Args:
        config: Configuration dictionary

    Returns:
        Configuration with substituted values
    """
    import json

    # Convert to JSON string, substitute, then parse back
    config_str = json.dumps(config)
    config_str = _substitute_env_vars(config_str)
    return json.loads(config_str)


def merge_configs(global_config_path: str, dag_config_path: str, source_name: str) -> dict:
    """Merge global and DAG-specific configs.

    Args:
        global_config_path: Path to global infrastructure config
        dag_config_path: Path to DAG-specific config
        source_name: Data source name (e.g., "rada")

    Returns:
        Merged config dict with sources[source_name] structure
    """
    global_config_path = Path(global_config_path)
    dag_config_path = Path(dag_config_path)

    if not global_config_path.exists():
        raise FileNotFoundError(f"Global config not found: {global_config_path}")
    if not dag_config_path.exists():
        raise FileNotFoundError(f"DAG config not found: {dag_config_path}")

    # Load global config
    with open(global_config_path, "r", encoding="utf-8") as f:
        global_cfg = yaml.safe_load(f)

    # Load DAG config
    with open(dag_config_path, "r", encoding="utf-8") as f:
        dag_cfg = yaml.safe_load(f)

    # Apply env var substitution to both
    global_cfg = substitute_env_vars(global_cfg)
    dag_cfg = substitute_env_vars(dag_cfg)

    # Merge pipeline settings (DAG overrides global)
    merged_pipeline = {
        **global_cfg.get("pipeline", {}),
        **dag_cfg.get("pipeline", {}),
    }

    # Create merged structure
    merged = {
        **global_cfg,  # minio, database, logging
        "pipeline": merged_pipeline,
        "sources": {
            source_name: {
                "source": dag_cfg["source"],
                "api": dag_cfg["api"],
                "dictionaries": dag_cfg["dictionaries"],
            }
        }
    }

    return merged


def compute_content_hash(*values: Any) -> str:
    """Compute SHA256 hash of concatenated values for change detection.

    Args:
        *values: Values to hash (converted to strings)

    Returns:
        Hexadecimal hash string
    """
    import hashlib

    # Convert all values to strings and concatenate
    content = "|".join(str(v) if v is not None else "" for v in values)

    # Compute SHA256 hash
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
