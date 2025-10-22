from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from bs4 import BeautifulSoup

from .. import logging as _logging  # noqa: F401
from ..config import DatasetConfig, get_default_datasets
from ..utils import http
from ..database import Database

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DiscoveredFile:
    file_url: str
    dataset_key: str
    expected_format: str


class Catalogue:
    def __init__(self, db: Database, datasets: dict[str, DatasetConfig] | None = None) -> None:
        self.db = db
        self.datasets = datasets or get_default_datasets()

    async def discover(self, source_html: str | None = None, base_url: str | None = None) -> list[DiscoveredFile]:
        html = source_html
        if html is None:
            response = await http.get("https://rrc.texas.gov/resource-center/research/data-sets-available-for-download/")
            html = response.text
            base_url = str(response.url)
        soup = BeautifulSoup(html, "lxml")
        discovered: list[DiscoveredFile] = []
        for link in soup.find_all("a"):
            href = link.get("href")
            text = link.text.lower()
            if not href:
                continue
            url = href
            if base_url and url.startswith("/"):
                from urllib.parse import urljoin

                url = urljoin(base_url, url)
            for dataset in self.datasets.values():
                if any(pattern in text or pattern in url.lower() for pattern in dataset.source_patterns):
                    discovered.append(
                        DiscoveredFile(file_url=url, dataset_key=dataset.dataset_key, expected_format=dataset.format)
                    )
                    break
        self._persist(discovered)
        return discovered

    def _persist(self, files: Iterable[DiscoveredFile]) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta_file (
                  file_url TEXT PRIMARY KEY,
                  dataset_key TEXT NOT NULL,
                  expected_format TEXT,
                  discovered_at TEXT,
                  last_checked_at TEXT,
                  last_modified TEXT,
                  etag TEXT,
                  size_bytes INTEGER,
                  status TEXT
                )
                """
            )
            for file in files:
                conn.execute(
                    """
                    INSERT INTO meta_file (file_url, dataset_key, expected_format, discovered_at, status)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(file_url) DO UPDATE SET last_checked_at = excluded.discovered_at
                    """,
                    (file.file_url, file.dataset_key, file.expected_format, datetime.utcnow().isoformat(), "seen"),
                )
            conn.commit()
