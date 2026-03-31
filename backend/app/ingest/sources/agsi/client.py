from __future__ import annotations

from datetime import date

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


class AGSIClient:
    def __init__(self) -> None:
        settings = get_settings()
        if settings.agsi_api_key is None or not settings.agsi_api_key.get_secret_value():
            raise ValueError("CW_AGSI_API_KEY is required for AGSI ingestion.")
        self._client = RateLimitedAsyncClient(
            "https://agsi.gie.eu",
            settings.agsi_rate_limit_seconds,
            headers={"x-key": settings.agsi_api_key.get_secret_value()},
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_data(
        self,
        *,
        type_: str | None = None,
        country: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        page: int = 1,
        size: int = 300,
    ) -> dict:
        params: dict[str, str | int] = {"page": page, "size": size}
        if type_:
            params["type"] = type_
        if country:
            params["country"] = country
        if from_date:
            params["from"] = from_date.isoformat()
        if to_date:
            params["to"] = to_date.isoformat()
        response = await self._client.get("/api", params=params)
        return response.json()

    async def get_all_data(
        self,
        *,
        type_: str | None = None,
        country: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        size: int = 300,
    ) -> dict:
        page = 1
        combined: dict | None = None
        while True:
            payload = await self.get_data(
                type_=type_,
                country=country,
                from_date=from_date,
                to_date=to_date,
                page=page,
                size=size,
            )
            if combined is None:
                combined = payload
                combined["data"] = list(payload.get("data", []))
            else:
                combined["data"].extend(payload.get("data", []))
            last_page = int(payload.get("last_page", page))
            if page >= last_page:
                break
            page += 1
        return combined or {"data": []}
