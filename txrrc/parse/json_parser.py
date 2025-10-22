from __future__ import annotations

import json
from typing import Iterable


def parse_json(data: bytes, columns: Iterable[str]) -> Iterable[dict[str, object]]:
    payload = json.loads(data)
    if isinstance(payload, dict):
        items = payload.get("records") or payload.get("data") or [payload]
    else:
        items = payload
    for item in items:
        yield {column: item.get(column) for column in columns}
