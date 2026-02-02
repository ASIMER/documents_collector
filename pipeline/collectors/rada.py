"""Rada (Verkhovna Rada) collector implementation.

This module implements data collection from the Ukrainian Parliament's open data API.
API documentation: https://data.rada.gov.ua
"""

import logging

import requests

from pipeline.collectors import register_collector
from pipeline.collectors.base import BaseCollector, CollectedDocument, DictionaryEntry
from pipeline.utils import RateLimiter, parse_date, parse_types

logger = logging.getLogger(__name__)


@register_collector("rada")
class RadaCollector(BaseCollector):
    """Collector for Verkhovna Rada (Ukrainian Parliament) open data API."""

    source_name = "rada"

    def __init__(self, config: dict):
        """Initialize Rada collector.

        Args:
            config: Configuration dictionary with rada source settings
        """
        super().__init__(config)

        # Support both nested and flat config structures
        if "sources" in config and "rada" in config["sources"]:
            # Nested structure: config["sources"]["rada"]["api"]
            source_config = config["sources"]["rada"]
            api_config = source_config["api"]
            self.dictionary_configs = source_config.get("dictionaries", [])
        else:
            # Flat structure: config["api"]
            api_config = config["api"]
            self.dictionary_configs = config.get("dictionaries", [])

        # Extract API configuration
        self.base_url = api_config["base_url"]
        self.timeout = api_config.get("timeout", 30)

        # Authentication configuration
        self.auth_method = api_config.get("auth_method", "opendata")
        self.token = api_config.get("token", "")
        self.fallback_to_opendata = api_config.get("fallback_to_opendata", True)
        self.user_agent_opendata = api_config.get("user_agent_opendata", "OpenData")

        # Rate limiting
        rate_limit_config = api_config.get("rate_limit", {})
        self.rate_limiter = RateLimiter(
            min_pause=rate_limit_config.get("min_pause", 5.0),
            max_pause=rate_limit_config.get("max_pause", 7.0),
        )

        # Pagination configuration
        pagination_config = api_config.get("pagination", {})
        self.max_pages = pagination_config.get("max_pages", 100)

        # HTTP session
        self.session = requests.Session()

        # Determine initial use_opendata state
        self.use_opendata = self.auth_method == "opendata" or not self.token

        logger.info(
            f"Initialized RadaCollector (auth_method={self.auth_method}, "
            f"use_opendata={self.use_opendata})"
        )

    def _get_headers(self) -> dict[str, str]:
        """Get HTTP headers for API requests.

        Returns:
            Headers dictionary with User-Agent
        """
        if self.use_opendata or not self.token:
            return {"User-Agent": self.user_agent_opendata}
        return {"User-Agent": self.token}

    def _make_request(self, url: str, method: str = "GET") -> requests.Response:
        """Make rate-limited HTTP request.

        Args:
            url: Request URL
            method: HTTP method

        Returns:
            Response object

        Raises:
            requests.RequestException: On request failure
        """
        self.rate_limiter.wait()

        headers = self._get_headers()

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response

        except requests.RequestException as e:
            # If token auth failed and fallback enabled, try OpenData
            if not self.use_opendata and self.fallback_to_opendata:
                logger.warning(f"Token auth failed, falling back to OpenData: {e}")
                self.use_opendata = True
                return self._make_request(url, method)
            raise

    def collect_dictionaries(self) -> list[DictionaryEntry]:
        """Collect dictionaries from Rada API.

        Two-step process:
        1. GET /open/data/{dict_name}.json -> get metadata with CSV link
        2. Download CSV from link
        3. Parse CSV (windows-1251, tab-separated, no headers)

        Returns:
            List of dictionary entries
        """
        all_entries = []

        for dict_config in self.dictionary_configs:
            dict_name = dict_config["name"]
            dict_type = dict_config["dict_type"]

            logger.info(f"Collecting dictionary: {dict_name} ({dict_type})")

            try:
                # Step 1: Get metadata JSON
                meta_url = f"{self.base_url}/open/data/{dict_name}.json"
                meta_response = self._make_request(meta_url)
                meta_data = meta_response.json()

                # Step 2: Extract CSV link
                if not meta_data.get("item") or len(meta_data["item"]) == 0:
                    logger.warning(f"No items in dictionary metadata: {dict_name}")
                    continue

                csv_link = meta_data["item"][0]["link"]
                logger.debug(f"Downloading CSV from: {csv_link}")

                # Step 3: Download CSV
                csv_response = self._make_request(csv_link)

                # IMPORTANT: CSV encoding is windows-1251, not UTF-8
                csv_response.encoding = "windows-1251"
                csv_text = csv_response.text

                # Step 4: Parse CSV (tab-separated, no headers)
                entries = self._parse_dictionary_csv(csv_text, dict_type)
                all_entries.extend(entries)

                logger.info(f"Collected {len(entries)} entries from {dict_name}")

            except Exception as e:
                logger.error(f"Failed to collect dictionary {dict_name}: {e}")
                # Continue with other dictionaries

        return all_entries

    def _parse_dictionary_csv(self, csv_text: str, dict_type: str) -> list[DictionaryEntry]:
        """Parse dictionary CSV file.

        Format: tab-separated, no headers, structure: id\tname

        Args:
            csv_text: CSV file content
            dict_type: Dictionary type

        Returns:
            List of dictionary entries
        """
        entries = []

        for line in csv_text.strip().split("\n"):
            if not line.strip():
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            try:
                entry_id = int(parts[0].strip())
                entry_name = parts[1].strip()

                entries.append(
                    DictionaryEntry(
                        dict_type=dict_type,
                        entry_id=entry_id,
                        entry_name=entry_name,
                    )
                )
            except ValueError as e:
                logger.warning(f"Failed to parse dictionary line: {line} ({e})")

        return entries

    def collect_document_list(self) -> list[dict]:
        """Collect list of recently updated documents.

        GET /laws/main/r/page{N}.json with pagination support.

        Returns:
            List of document metadata dictionaries
        """
        all_documents = []
        page = 1

        logger.info("Collecting document list from Rada API")

        while page <= self.max_pages:
            url = f"{self.base_url}/laws/main/r/page{page}.json"

            try:
                response = self._make_request(url)
                data = response.json()

                # Extract documents from response
                documents = data.get("list", [])
                if not documents:
                    logger.debug(f"No documents on page {page}, stopping pagination")
                    break

                all_documents.extend(documents)

                # Check if we've collected all documents
                total_count = data.get("max", 0)
                collected_count = len(all_documents)

                logger.info(
                    f"Collected page {page}: {len(documents)} documents "
                    f"(total: {collected_count}/{total_count})"
                )

                if collected_count >= total_count:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Failed to collect page {page}: {e}")
                break

        logger.info(f"Collected {len(all_documents)} documents total")
        return all_documents

    def collect_document(self, doc_meta: dict) -> CollectedDocument:
        """Collect single document (metadata + full text).

        Args:
            doc_meta: Document metadata from document list

        Returns:
            Collected document with text
        """
        nreg = doc_meta.get("nreg")
        dokid = doc_meta.get("dokid")

        logger.debug(f"Collecting document: {dokid} ({nreg})")

        # Get document text
        text = self._fetch_document_text(nreg)

        # Parse fields
        source_id = str(dokid)
        source_reg = str(nreg)
        title = doc_meta.get("nazva", "")
        status_id = doc_meta.get("status")
        type_ids = parse_types(doc_meta.get("types"))
        org_id = doc_meta.get("orgid") or doc_meta.get("org")

        # Parse dates (YYYYMMDD format)
        doc_date = parse_date(doc_meta.get("orgdat"))
        revision_date = parse_date(doc_meta.get("poddat"))

        return CollectedDocument(
            source_id=source_id,
            source_reg=source_reg,
            title=title,
            text=text,
            status_id=status_id,
            type_ids=type_ids,
            org_id=org_id,
            doc_date=doc_date,
            revision_date=revision_date,
            raw_metadata=doc_meta,
        )

    def _fetch_document_text(self, nreg: str) -> str | None:
        """Fetch document text from API.

        GET /laws/show/{nreg}.txt

        Args:
            nreg: Registration number

        Returns:
            Document text or None if not available
        """
        url = f"{self.base_url}/laws/show/{nreg}.txt"

        try:
            response = self._make_request(url)

            # Text encoding is UTF-8
            text = response.text.strip()

            if not text:
                logger.warning(f"Empty text for document {nreg}")
                return None

            return text

        except requests.RequestException as e:
            logger.error(f"Failed to fetch text for {nreg}: {e}")
            return None
