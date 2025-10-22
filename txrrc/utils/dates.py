from __future__ import annotations

from datetime import date, datetime


def normalize_yyyymmdd(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not value or value in {"00000000", "99999999"}:
        return None
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def month_start(value: str | date) -> str:
    if isinstance(value, str):
        dt = datetime.strptime(value, "%Y-%m-%d")
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    else:  # assume YYYY-MM string
        dt = datetime.strptime(str(value), "%Y-%m")
    return dt.replace(day=1).date().isoformat()
