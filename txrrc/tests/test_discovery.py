from __future__ import annotations

import asyncio
from pathlib import Path

from txrrc.config import get_default_datasets
from txrrc.database import Database
from txrrc.discovery.catalogue import Catalogue


HTML = """
<html><body>
<a href="https://example.com/wellbore.dbf">Wellbore DBF900</a>
<a href="https://example.com/production.csv">Production Ledger</a>
</body></html>
"""


def test_discovery(tmp_path: Path) -> None:
    db = Database(tmp_path / "db.sqlite")
    catalogue = Catalogue(db, get_default_datasets())
    files = asyncio.run(catalogue.discover(source_html=HTML, base_url="https://example.com"))
    assert any(f.dataset_key == "wellbore" for f in files)
    assert any(f.dataset_key == "production" for f in files)
