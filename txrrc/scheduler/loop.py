from __future__ import annotations

import asyncio
import logging
from random import random

from ..pipeline import refresh_all

logger = logging.getLogger(__name__)


async def run_scheduler(interval: int) -> None:
    while True:
        try:
            refresh_all()
        except Exception:  # pragma: no cover
            logger.exception("scheduler iteration failed")
        await asyncio.sleep(interval + int(random() * 5))
