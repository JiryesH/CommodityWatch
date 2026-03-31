from __future__ import annotations

from datetime import date
from urllib.parse import quote

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


class EIAClient:
    def __init__(self) -> None:
        settings = get_settings()
        if settings.eia_api_key is None or not settings.eia_api_key.get_secret_value():
            raise ValueError("CW_EIA_API_KEY is required for EIA ingestion.")
        self._api_key = settings.eia_api_key.get_secret_value()
        self._client = RateLimitedAsyncClient("https://api.eia.gov/v2", settings.eia_rate_limit_seconds)

    async def close(self) -> None:
        await self._client.close()

    async def get_series_data(
        self,
        series_id: str,
        start: date | None = None,
        end: date | None = None,
        length: int | None = None,
        sort_desc: bool = True,
    ) -> dict:
        params: dict[str, str | int] = {"api_key": self._api_key}
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()
        if length:
            params["length"] = length
        params["sort[0][column]"] = "period"
        params["sort[0][direction]"] = "desc" if sort_desc else "asc"
        response = await self._client.get(f"/seriesid/{quote(series_id, safe='')}", params=params)
        payload = response.json()
        if payload.get("error"):
            error = payload["error"]
            if isinstance(error, dict):
                message = error.get("message") or error.get("code") or str(error)
            else:
                message = str(error)
            raise ValueError(f"EIA API error for {series_id}: {message}")
        return payload
