from __future__ import annotations

from txrrc.parse.fixed_parser import parse_ascii_fixed


def test_fixed_width_parser() -> None:
    layout = {
        "fields": [
            {"name": "permit_number", "start": 1, "length": 6},
            {"name": "api10", "start": 7, "length": 10},
        ]
    }
    line = "1234564200100001"
    data = (line + "\n").encode("ascii")
    rows = list(parse_ascii_fixed(data, layout))
    assert rows[0]["permit_number"] == "123456"
    assert rows[0]["api10"] == "4200100001"
