from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from time import monotonic

import httpx


logger = logging.getLogger(__name__)
REQUEST_RETRY_ATTEMPTS = 3


class RateLimitedAsyncClient:
    def __init__(self, base_url: str, rate_limit_seconds: float, headers: Mapping[str, str] | None = None) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=30.0, headers=dict(headers or {}))
        self._rate_limit_seconds = rate_limit_seconds
        self._last_request_at = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def get(self, url: str, params: Mapping[str, str | int | float] | None = None) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(REQUEST_RETRY_ATTEMPTS):
            now = monotonic()
            elapsed = now - self._last_request_at
            if elapsed < self._rate_limit_seconds:
                await asyncio.sleep(self._rate_limit_seconds - elapsed)
            try:
                response = await self._client.get(url, params=params)
                self._last_request_at = monotonic()
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                self._last_request_at = monotonic()
                status_code = exc.response.status_code
                if not (status_code == 429 or 500 <= status_code < 600) or attempt == REQUEST_RETRY_ATTEMPTS - 1:
                    raise
                delay = min(2**attempt, 10)
                logger.warning(
                    "HTTP GET %s failed with status %d; retrying in %d seconds (attempt %d/%d)",
                    url,
                    status_code,
                    delay,
                    attempt + 1,
                    REQUEST_RETRY_ATTEMPTS,
                )
                await asyncio.sleep(delay)
            except httpx.RequestError as exc:
                last_exc = exc
                self._last_request_at = monotonic()
                if attempt == REQUEST_RETRY_ATTEMPTS - 1:
                    raise
                delay = min(2**attempt, 10)
                logger.warning(
                    "HTTP GET %s failed with %s; retrying in %d seconds (attempt %d/%d)",
                    url,
                    exc.__class__.__name__,
                    delay,
                    attempt + 1,
                    REQUEST_RETRY_ATTEMPTS,
                )
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc
