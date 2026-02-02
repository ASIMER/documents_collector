"""Base collector interface and data structures.

This module defines:
- BaseCollector ABC: Interface for all data source collectors
- CollectedDocument: Unified document structure
- DictionaryEntry: Unified dictionary entry structure
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date


@dataclass
class CollectedDocument:
    """Unified document structure from any collector."""

    source_id: str  # Unique ID in source system (dokid for Rada)
    source_reg: str  # Registration number (nreg for Rada)
    title: str  # Document title
    text: str | None  # Full document text (plain text)
    status_id: int | None  # Status/state
    type_ids: list[int]  # Document types
    org_id: int | None  # Organization ID
    doc_date: date | None  # Document date
    revision_date: date | None  # Current revision date
    raw_metadata: dict  # Full JSON from API (for raw storage)


@dataclass
class DictionaryEntry:
    """Dictionary entry structure."""

    dict_type: str  # 'status', 'type', 'theme', etc.
    entry_id: int  # Entry ID
    entry_name: str  # Entry name/label


class BaseCollector(ABC):
    """Base interface for data source collectors.

    Each collector implements data collection logic for a specific source
    (e.g., Rada, Court, EU) while the pipeline core remains source-agnostic.
    """

    def __init__(self, config: dict):
        """Initialize collector with configuration.

        Args:
            config: Configuration dictionary for this source
        """
        self.config = config

        # Extract source name from config
        if "sources" in config:
            # Nested structure - find the source name
            self._extracted_source_name = list(config["sources"].keys())[0]
        elif "source" in config and "name" in config["source"]:
            # Flat structure - source.name field
            self._extracted_source_name = config["source"]["name"]
        else:
            self._extracted_source_name = None

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique source name: 'rada', 'court', 'eu', etc."""
        ...

    @abstractmethod
    def collect_dictionaries(self) -> list[DictionaryEntry]:
        """Collect reference dictionaries from source.

        Returns:
            List of dictionary entries
        """
        ...

    @abstractmethod
    def collect_document_list(self) -> list[dict]:
        """Collect list of documents (minimal metadata for filtering).

        Returns:
            List of document metadata dictionaries
        """
        ...

    @abstractmethod
    def collect_document(self, doc_meta: dict) -> CollectedDocument:
        """Collect single document (metadata + full text).

        Args:
            doc_meta: Document metadata from document list

        Returns:
            Collected document with text
        """
        ...
