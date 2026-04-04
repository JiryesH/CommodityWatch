from __future__ import annotations

from datetime import date

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


class FREDClient:
    def __init__(self) -> None:
        settings = get_settings()
        if settings.fred_api_key is None or not settings.fred_api_key.get_secret_value():
            raise ValueError("CW_FRED_API_KEY is required for FRED ingestion.")
        self._api_key = settings.fred_api_key.get_secret_value()
        self._client = RateLimitedAsyncClient("https://api.stlouisfed.org", settings.fred_rate_limit_seconds)

    async def close(self) -> None:
        await self._client.close()

    async def get_series_observations(
        self,
        series_id: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> dict:
        params: dict[str, str] = {
            "api_key": self._api_key,
            "file_type": "json",
            "series_id": series_id,
            "sort_order": "asc",
        }
        if start_date is not None:
            params["observation_start"] = start_date.isoformat()
        if end_date is not None:
            params["observation_end"] = end_date.isoformat()
        if realtime_start is not None:
            params["realtime_start"] = realtime_start.isoformat()
        if realtime_end is not None:
            params["realtime_end"] = realtime_end.isoformat()
        return (await self._client.get("/fred/series/observations", params=params)).json()

    async def get_series_release(self, series_id: str) -> dict:
        params = {
            "api_key": self._api_key,
            "file_type": "json",
            "series_id": series_id,
        }
        return (await self._client.get("/fred/series/release", params=params)).json()

    async def get_release_dates(self, release_id: int, *, limit: int = 120) -> dict:
        params = {
            "api_key": self._api_key,
            "file_type": "json",
            "release_id": release_id,
            "limit": str(limit),
            "sort_order": "desc",
        }
        return (await self._client.get("/fred/release/dates", params=params)).json()
