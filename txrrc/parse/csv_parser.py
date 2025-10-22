from __future__ import annotations

import io
from typing import Iterable

import pandas as pd


def parse_csv(data: bytes, columns: Iterable[str]) -> Iterable[dict[str, object]]:
    df = pd.read_csv(io.BytesIO(data))
    subset = [column for column in columns if column in df.columns]
    df = df[subset]
    for record in df.to_dict(orient="records"):
        yield record
