from __future__ import annotations

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


class EmberClient:
    def __init__(self) -> None:
        settings = get_settings()
        if settings.ember_api_key is None or not settings.ember_api_key.get_secret_value():
            raise ValueError("CW_EMBER_API_KEY is required for Ember ingestion.")
        self._api_key = settings.ember_api_key.get_secret_value()
        self._client = RateLimitedAsyncClient("https://api.ember-energy.org", settings.ember_rate_limit_seconds)

    async def close(self) -> None:
        await self._client.close()

    async def get_monthly_demand(
        self,
        *,
        entity: str | None = None,
        entity_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        is_aggregate_entity: bool | None = None,
    ) -> dict:
        params: dict[str, str] = {"api_key": self._api_key}
        if entity is not None:
            params["entity"] = entity
        if entity_code is not None:
            params["entity_code"] = entity_code
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        if is_aggregate_entity is not None:
            params["is_aggregate_entity"] = "true" if is_aggregate_entity else "false"
        return (await self._client.get("/v1/electricity-demand/monthly", params=params)).json()
