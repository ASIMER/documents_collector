"""Collector registry and factory.

This module provides:
- Collector registry for pluggable collectors
- Factory function to get collector by source name
"""

from typing import Type

from pipeline.collectors.base import BaseCollector

# Collector registry: source_name -> collector_class
REGISTRY: dict[str, Type[BaseCollector]] = {}


def register_collector(source_name: str):
    """Decorator to register a collector class.

    Args:
        source_name: Source name (e.g., 'rada')
    """

    def decorator(cls: Type[BaseCollector]):
        REGISTRY[source_name] = cls
        return cls

    return decorator


def get_collector(source_name: str, config: dict) -> BaseCollector:
    """Get collector instance by source name.

    Args:
        source_name: Source name (e.g., 'rada')
        config: Configuration dictionary

    Returns:
        Collector instance

    Raises:
        KeyError: If collector not found in registry
    """
    if source_name not in REGISTRY:
        available = ", ".join(REGISTRY.keys())
        raise KeyError(
            f"Collector '{source_name}' not found in registry. "
            f"Available collectors: {available}"
        )

    collector_class = REGISTRY[source_name]
    return collector_class(config)


# Import collectors to register them
from pipeline.collectors.rada import RadaCollector  # noqa: E402, F401
