from __future__ import annotations

from typing import Iterable


def parse_fixed_width(lines: Iterable[str], layout: dict) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    for line in lines:
        record: dict[str, str | None] = {}
        for field in layout.get("fields", []):
            start = field["start"] - 1
            end = start + field["length"]
            raw = line[start:end]
            value = raw.strip() or None
            if field.get("type") == "date" and value:
                from .dates import normalize_yyyymmdd

                value = normalize_yyyymmdd(raw)
            elif field.get("type", "string").startswith("numeric"):
                decimals = field.get("implied", 0)
                from .ebcdic import normalize_numeric

                value = normalize_numeric(raw, decimals)
            record[field["name"]] = value
        rows.append(record)
    return rows
