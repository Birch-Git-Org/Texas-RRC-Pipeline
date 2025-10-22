from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DB_PATH, ensure_data_dirs


class Database:
    def __init__(self, path: Path | None = None) -> None:
        ensure_data_dirs()
        self.path = path or DB_PATH
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        from .normalize.ddl import ensure_schema

        with self.connect() as conn:
            ensure_schema(conn)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
