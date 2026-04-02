from __future__ import annotations

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


GLD_ARCHIVE_URL = "https://api.spdrgoldshares.com/api/v1/historical-archive?exchange=NYSE&lang=en&product=gld"
SLV_URL = "https://www.ishares.com/us/products/239855/ishares-silver-trust-fund"
IAU_URL = "https://www.ishares.com/us/products/239561/ishares-gold-trust-fund"


class ETFHoldingsClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://www.ishares.com",
            rate_limit_seconds=settings.exchange_scrape_rate_limit_seconds,
            headers={"User-Agent": "CommodityWatch/1.0"},
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_text(self, url: str) -> str:
        response = await self._client.get(url)
        return response.text

    async def get_bytes(self, url: str) -> bytes:
        response = await self._client.get(url)
        return response.content
