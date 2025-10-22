from __future__ import annotations

from typing import Iterable

from ..utils.ebcdic import decode_lines, parse_record


def parse_ebcdic_fixed(data: bytes, layout: dict) -> Iterable[dict[str, object]]:
    line_length = layout.get("line_length")
    if not line_length:
        raise ValueError("layout must define line_length")
    lines = decode_lines(data, int(line_length))
    records = parse_record(lines, layout)
    root_key = layout.get("root_key")
    grouped: list[dict[str, object]] = []
    current_root: dict[str, object] | None = None
    for record in records:
        if record["record_type"] == layout.get("root_record"):
            current_root = {key: record.get(key) for key in layout.get("root_fields", [])}
            grouped.append(current_root)
        elif current_root is not None:
            for field, alias in layout.get("child_fields", {}).items():
                if field in record and record[field] is not None:
                    current_root[alias] = record[field]
    return grouped
