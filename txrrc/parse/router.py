from __future__ import annotations

from typing import Iterable

from ..config import DatasetConfig, get_default_datasets, load_layout
from .csv_parser import parse_csv
from .ebcdic_parser import parse_ebcdic_fixed
from .fixed_parser import parse_ascii_fixed
from .json_parser import parse_json


class ParseRouter:
    def __init__(self, datasets: dict[str, DatasetConfig] | None = None) -> None:
        self.datasets = datasets or get_default_datasets()

    def parse(self, dataset_key: str, data: bytes) -> Iterable[dict[str, object]]:
        dataset = self.datasets[dataset_key]
        if dataset.format == "csv":
            return parse_csv(data, dataset.mapping.keys())
        if dataset.format == "json":
            return parse_json(data, dataset.mapping.keys())
        if dataset.format == "ascii_fixed":
            layout = load_layout(dataset.layout or "")
            return parse_ascii_fixed(data, layout)
        if dataset.format == "ebcdic_fixed":
            layout = load_layout(dataset.layout or "")
            return parse_ebcdic_fixed(data, layout)
        raise ValueError(f"Unknown format {dataset.format}")
