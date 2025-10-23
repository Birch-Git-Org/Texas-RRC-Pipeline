from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("yaml")

from txrrc.config import load_layout
from txrrc.parse.ebcdic_parser import parse_ebcdic_fixed


def test_parse_ebcdic_records(tmp_path: Path) -> None:
    layout = load_layout("wellbore_dbf900.yaml")
    line = [" "] * layout["line_length"]
    # record type 01
    line[0:2] = list("01")
    line[2:2 + 14] = list("42-001-00001") + [" "] * (14 - len("42-001-00001"))
    line[16:16 + 10] = list("4200100001")
    line[26:26 + 2] = list("01")
    joined = "".join(line)
    encoded = joined.encode("cp500")
    result = list(parse_ebcdic_fixed(encoded, layout))
    assert result
    record = result[0]
    assert record["api10"] == "4200100001"
