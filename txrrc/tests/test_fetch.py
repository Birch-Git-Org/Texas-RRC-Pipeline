from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from txrrc.database import Database
from txrrc.fetch.downloader import Downloader


class DummyResponse:
    def __init__(self, content: bytes, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}


@pytest.mark.asyncio
async def test_downloader_handles_gzip(monkeypatch, tmp_path: Path) -> None:
    db = Database(tmp_path / "db.sqlite")
    downloader = Downloader(db)

    async def fake_get(url: str):  # type: ignore[no-untyped-def]
        payload = gzip.compress(b"col1,col2\n1,2\n")
        return DummyResponse(payload, headers={"Content-Encoding": "gzip"})

    monkeypatch.setattr("txrrc.utils.http.get", fake_get)
    path = await downloader.fetch("https://example.com/data.csv.gz", "production")
    assert Path(path).exists()
    assert Path(path).read_text() == "col1,col2\n1,2\n"
