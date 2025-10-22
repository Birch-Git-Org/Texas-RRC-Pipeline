from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict[str, Any] = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.__dict__:
            for key, value in record.__dict__.items():
                if key.startswith("_"):
                    continue
                if key in payload:
                    continue
                if key in {"args", "msg", "message", "levelno", "levelname"}:
                    continue
                payload[key] = value
        return json.dumps(payload, default=str)


def configure_logging(level: str | int = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.handlers = [handler]


configure_logging()
