from __future__ import annotations

from typing import Any


def validate_numeric(value: Any, minimum: int | None = None, maximum: int | None = None) -> bool:
    if value is None:
        return True
    try:
        number = int(value)
    except (ValueError, TypeError):
        return False
    if minimum is not None and number < minimum:
        return False
    if maximum is not None and number > maximum:
        return False
    return True
