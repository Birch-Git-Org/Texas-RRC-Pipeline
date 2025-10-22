from __future__ import annotations

import gzip
import logging
from datetime import datetime
from pathlib import Path

import httpx

from ..config import RAW_DIR, ensure_data_dirs
from ..database import Database
from ..utils import hashing
from ..utils import http as http_utils

logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self, db: Database) -> None:
        self.db = db
        ensure_data_dirs()

    async def fetch(self, url: str, dataset_key: str) -> Path:
        response = await http_utils.get(url)
        content = response.content
        is_gzip = False
        if url.endswith(".gz") or response.headers.get("Content-Encoding") == "gzip":
            content = gzip.decompress(content)
            is_gzip = True
        sha = hashing.sha256_bytes(content)
        month = datetime.utcnow().strftime("%Y-%m")
        dest_dir = RAW_DIR / dataset_key / month
        dest_dir.mkdir(parents=True, exist_ok=True)
        ext = Path(url).suffix or ".dat"
        dest = dest_dir / f"{sha}{ext}"
        dest.write_bytes(content)
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_file (dataset_key, file_url, sha256, stored_path, is_gzip, downloaded_at, http_status, content_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dataset_key,
                    url,
                    sha,
                    str(dest),
                    int(is_gzip),
                    datetime.utcnow().isoformat(),
                    response.status_code,
                    response.headers.get("Content-Type"),
                ),
            )
            conn.commit()
        return dest
