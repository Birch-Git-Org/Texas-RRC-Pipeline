from __future__ import annotations

import json
from pathlib import Path

from txrrc.parse.completions_parser import parse_completions_zip


def test_parse_completions_sample() -> None:
    data = Path("samples/completions-data/02-22-2024.zip").read_bytes()
    rows = list(parse_completions_zip(data))
    assert rows
    sample = next(row for row in rows if row["tracking_number"] == "310032")
    assert sample["district"] == "06"
    assert sample["status"] == "Submitted"
    assert sample["submitted_date"] == "2024-02-22"
    payload = json.loads(sample["payload_json"])
    assert payload["source"].endswith("packetData_310032_Submitted.dat")
    assert payload["records"][0]["type"] == "310032"
    assert any(record["type"] == "G-1" for record in payload["records"])
