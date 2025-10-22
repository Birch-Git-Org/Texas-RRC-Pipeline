from __future__ import annotations

from pathlib import Path
import sqlite3

DDL_PATH = Path(__file__).with_suffix(".sql")


def ensure_schema(conn: sqlite3.Connection) -> None:
    with DDL_PATH.open("r", encoding="utf8") as fh:
        conn.executescript(fh.read())
        conn.commit()
