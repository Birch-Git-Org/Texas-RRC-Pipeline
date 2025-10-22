from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from .config import ensure_data_dirs
from .database import Database
from .discovery.catalogue import Catalogue
from .fetch.downloader import Downloader
from .normalize.loader import Loader
from .parse.router import ParseRouter
from .pipeline import refresh_all as pipeline_refresh_all
from .scheduler.loop import run_scheduler

app = typer.Typer(help="TXRRC data pipeline")


@app.command()
def discover() -> None:
    db = Database()
    catalogue = Catalogue(db)
    typer.echo(asyncio.run(catalogue.discover()))


@app.command()
def fetch(dataset_key: str, url: str) -> None:
    db = Database()
    downloader = Downloader(db)
    path = asyncio.run(downloader.fetch(url, dataset_key))
    typer.echo(path)


@app.command()
def parse(dataset_key: str, file_path: Path) -> None:
    router = ParseRouter()
    data = file_path.read_bytes()
    rows = list(router.parse(dataset_key, data))
    typer.echo(rows)


@app.command()
def load(dataset_key: str, file_path: Path) -> None:
    db = Database()
    loader = Loader(db)
    data = file_path.read_bytes()
    router = ParseRouter()
    rows = list(router.parse(dataset_key, data))
    loader.load(dataset_key, rows)
    typer.echo("loaded")


@app.command("refresh-all")
def refresh_all() -> None:
    pipeline_refresh_all()


@app.command()
def scheduler(interval: int = typer.Option(21600)) -> None:
    ensure_data_dirs()
    asyncio.run(run_scheduler(interval))


@app.command()
def api(port: int = 8080) -> None:
    from .api import serve

    serve(port)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
