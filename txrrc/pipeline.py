from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Iterable

from .config import get_default_datasets
from .database import Database
from .discovery.catalogue import Catalogue
from .fetch.downloader import Downloader
from .normalize.loader import Loader
from .parse.router import ParseRouter


def discover(db: Database | None = None) -> list[dict[str, str]]:
    database = db or Database()
    catalogue = Catalogue(database)
    return asyncio.run(catalogue.discover())


def fetch(dataset_key: str, url: str, db: Database | None = None) -> str:
    database = db or Database()
    downloader = Downloader(database)
    path = asyncio.run(downloader.fetch(url, dataset_key))
    return str(path)


def parse(dataset_key: str, data: bytes) -> Iterable[dict[str, object]]:
    router = ParseRouter()
    return router.parse(dataset_key, data)


def load(dataset_key: str, rows: Iterable[dict[str, object]], db: Database | None = None) -> None:
    database = db or Database()
    loader = Loader(database)
    loader.load(dataset_key, rows)


def refresh_all() -> None:
    db = Database()
    datasets = get_default_datasets()
    catalogue = Catalogue(db, datasets)
    discovered = asyncio.run(catalogue.discover())
    downloader = Downloader(db)
    router = ParseRouter(datasets)
    loader = Loader(db, datasets)
    for item in discovered:
        path = asyncio.run(downloader.fetch(item.file_url, item.dataset_key))
        data = Path(path).read_bytes()
        rows = list(router.parse(item.dataset_key, data))
        loader.load(item.dataset_key, rows)
