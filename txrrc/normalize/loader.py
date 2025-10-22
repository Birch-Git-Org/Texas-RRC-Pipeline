from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime

import sqlite3

from ..config import DatasetConfig, get_default_datasets
from ..database import Database

logger = logging.getLogger(__name__)


class Loader:
    def __init__(self, db: Database, datasets: dict[str, DatasetConfig] | None = None) -> None:
        self.db = db
        self.datasets = datasets or get_default_datasets()

    def load(self, dataset_key: str, rows: Iterable[dict[str, object]], raw_file_id: int | None = None) -> None:
        dataset = self.datasets[dataset_key]
        stage_table = f"stg_{dataset_key}"
        payload = list(rows)
        with self.db.connect() as conn:
            self._ensure_staging(conn, stage_table, dataset)
            conn.execute(f"DELETE FROM {stage_table}")
            if payload:
                placeholders = ",".join([":" + key for key in dataset.mapping.keys()])
                columns = ",".join(dataset.mapping.keys())
                conn.executemany(
                    f"INSERT INTO {stage_table} ({columns}) VALUES ({placeholders})",
                    payload,
                )
            self._upsert(conn, dataset, stage_table)
            self._record_lineage(conn, dataset, stage_table, raw_file_id, len(payload))
            conn.commit()

    def _ensure_staging(self, conn: sqlite3.Connection, table: str, dataset: DatasetConfig) -> None:
        cols = ",".join(f"{column} TEXT" for column in dataset.mapping.keys())
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")

    def _upsert(self, conn: sqlite3.Connection, dataset: DatasetConfig, stage_table: str) -> None:
        target = dataset.target_table
        columns = list(dataset.mapping.values())
        stage_columns = list(dataset.mapping.keys())
        assignments = ",".join(f"{col}=excluded.{col}" for col in columns if col not in dataset.keys)
        column_list = ",".join(columns)
        stage_select = ",".join(stage_columns)
        if assignments:
            sql = (
                f"INSERT INTO {target} ({column_list}) SELECT {stage_select} FROM {stage_table} "
                f"ON CONFLICT({','.join(dataset.keys)}) DO UPDATE SET {assignments}"
            )
        else:
            sql = f"INSERT OR IGNORE INTO {target} ({column_list}) SELECT {stage_select} FROM {stage_table}"
        conn.execute(sql)

    def _record_lineage(
        self,
        conn: sqlite3.Connection,
        dataset: DatasetConfig,
        stage_table: str,
        raw_file_id: int | None,
        count: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO lineage_ingest (
              raw_file_id, dataset_key, stage_table, loaded_tables, record_count, error_count, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                raw_file_id,
                dataset.dataset_key,
                stage_table,
                dataset.target_table,
                count,
                0,
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )
