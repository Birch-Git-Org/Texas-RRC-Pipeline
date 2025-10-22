from __future__ import annotations

from typing import Iterable

from ..utils.fixedwidth import parse_fixed_width


def parse_ascii_fixed(data: bytes, layout: dict) -> Iterable[dict[str, object]]:
    text = data.decode("ascii", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    return parse_fixed_width(lines, layout)
