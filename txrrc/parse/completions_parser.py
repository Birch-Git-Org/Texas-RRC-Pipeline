from __future__ import annotations

import io
import json
import re
import zipfile
from collections.abc import Iterable
from datetime import datetime


def parse_completions_zip(data: bytes) -> Iterable[dict[str, object]]:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for name in sorted(n for n in archive.namelist() if n.lower().endswith(".dat")):
            text = archive.read(name).decode("utf-8", errors="replace")
            records = _parse_records(text)
            structured = [
                {
                    "type": label,
                    "values": [value if value != "" else None for value in values],
                }
                for label, values in records
            ]
            packet_record = _find_record(structured, "PACKET")
            tracking_number = _extract_tracking_number(name, packet_record)
            header_record = _find_record(structured, tracking_number) if tracking_number else None
            packet_number = _get_value(packet_record, 1)
            submitted_date_raw = _get_value(packet_record, 2)
            submitted_date = _normalize_date(submitted_date_raw)
            forms_summary = _get_value(header_record, 0)
            attachments = _get_value(header_record, 1)
            result = {
                "tracking_number": tracking_number,
                "packet_number": packet_number,
                "district": _extract_district(name),
                "status": _extract_status(name),
                "submitted_date": submitted_date,
                "forms_summary": forms_summary,
                "attachments": attachments,
                "payload_json": json.dumps(
                    {
                        "source": name,
                        "records": structured,
                    },
                    ensure_ascii=False,
                ),
            }
            yield result


def _parse_records(text: str) -> list[tuple[str, list[str]]]:
    records: list[tuple[str, list[str]]] = []
    current: str | None = None
    allowed = set("- /&")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "{" not in line:
            if current is not None:
                current += line
            continue
        prefix = line.split("{", 1)[0]
        if prefix and all(ch.isalnum() or ch in allowed for ch in prefix):
            if current:
                _append_record(current, records)
            current = line
        else:
            if current:
                current += line
            else:
                current = line
    if current:
        _append_record(current, records)
    return records


def _append_record(record: str, records: list[tuple[str, list[str]]]) -> None:
    parts = record.split("{")
    label = parts[0].strip()
    values = [part.strip() for part in parts[1:]]
    records.append((label, values))


def _find_record(structured: list[dict[str, object]], record_type: str | None) -> dict[str, object] | None:
    if not record_type:
        return None
    for record in structured:
        if record.get("type") == record_type:
            return record
    return None


def _get_value(record: dict[str, object] | None, index: int) -> str | None:
    if not record:
        return None
    values = record.get("values")
    if not isinstance(values, list):
        return None
    if index >= len(values):
        return None
    value = values[index]
    if isinstance(value, str):
        value = value.strip()
    return value or None


def _extract_tracking_number(path: str, packet_record: dict[str, object] | None) -> str | None:
    if packet_record:
        from_packet = _get_value(packet_record, 0)
        if from_packet:
            return from_packet
    match = re.search(r"(\d+)", path)
    if match:
        return match.group(1)
    return None


def _extract_status(path: str) -> str | None:
    filename = path.rsplit("/", 1)[-1]
    parts = filename.split("_")
    if parts:
        last = parts[-1]
        if "." in last:
            last = last.split(".", 1)[0]
        return last or None
    return None


def _extract_district(path: str) -> str | None:
    if "/" in path:
        return path.split("/", 1)[0] or None
    return None


def _normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return value
