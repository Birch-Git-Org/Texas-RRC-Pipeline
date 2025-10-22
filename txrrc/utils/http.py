from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


@asynccontextmanager
def _client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
        yield client


@retry(wait=wait_exponential(multiplier=0.5, min=1, max=30), stop=stop_after_attempt(5))
async def _request(method: str, url: str, **kwargs: Any) -> httpx.Response:
    async with _client() as client:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()
        return response


async def get(url: str, **kwargs: Any) -> httpx.Response:
    return await _request("GET", url, **kwargs)


async def head(url: str, **kwargs: Any) -> httpx.Response:
    return await _request("HEAD", url, **kwargs)


def run_sync(async_fn, *args, **kwargs):  # type: ignore[no-untyped-def]
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        return asyncio.ensure_future(async_fn(*args, **kwargs))
    return asyncio.run(async_fn(*args, **kwargs))
