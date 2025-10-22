from __future__ import annotations

from typing import Iterable


def ebcdic_to_ascii(data: bytes) -> str:
    for encoding in ("cp500", "cp037"):
        try:
            return data.decode(encoding, errors="strict")
        except Exception:
            continue
    return data.decode("cp500", errors="replace")


def decode_lines(data: bytes, line_length: int) -> list[str]:
    text = ebcdic_to_ascii(data)
    return [text[i : i + line_length] for i in range(0, len(text), line_length) if text[i : i + line_length].strip()]


def implied_decimal(value: str, decimals: int) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        integer = int(raw)
    except ValueError:
        return None
    if decimals:
        return f"{integer / (10**decimals):.{decimals}f}"
    return str(integer)


def normalize_numeric(value: str, decimals: int | None = None) -> str | None:
    raw = value.strip()
    if not raw or set(raw) == {"0"}:
        return None
    if decimals is not None:
        return implied_decimal(raw, decimals)
    try:
        return str(int(raw))
    except ValueError:
        try:
            return str(float(raw))
        except ValueError:
            return None


def parse_record(lines: Iterable[str], layout: dict) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    for line in lines:
        record_type = line[:2]
        spec = layout["records"].get(record_type)
        if not spec:
            continue
        record: dict[str, str | None] = {"record_type": record_type}
        for field in spec.get("fields", []):
            start = field["start"] - 1
            end = start + field["length"]
            raw = line[start:end]
            kind = field.get("type", "string")
            if kind == "date":
                from .dates import normalize_yyyymmdd

                record[field["name"]] = normalize_yyyymmdd(raw)
            elif kind.startswith("numeric"):
                decimals = field.get("implied", 0)
                record[field["name"]] = normalize_numeric(raw, decimals)
            else:
                record[field["name"]] = raw.strip() or None
        records.append(record)
    return records
