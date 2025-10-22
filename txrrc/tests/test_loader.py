from __future__ import annotations

from pathlib import Path

from txrrc.config import DatasetConfig, get_default_datasets
from txrrc.database import Database
from txrrc.normalize.loader import Loader


def test_loader_upsert(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    db = Database(db_path)
    datasets = get_default_datasets()
    loader = Loader(db, datasets)
    rows = [
        {
            "api14": "42-001-00001",
            "api10": "4200100001",
            "district": "01",
            "county_code": "001",
            "field_code": "10001",
            "field_name": "TEST FIELD",
            "well_name": "TEST WELL",
            "operator_number": "123456",
            "operator_name": "OPERATOR",
            "orig_completion_date": "2020-01-01",
            "total_depth": "1000",
        }
    ]
    loader.load("wellbore", rows)
    loader.load("wellbore", rows)
    with db.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM well").fetchone()[0]
        assert count == 1
