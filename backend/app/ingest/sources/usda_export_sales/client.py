from __future__ import annotations

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


STATIC_REPORTS_BASE = "https://apps.fas.usda.gov/esrqs/StaticReports"


class USDAExportSalesClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            STATIC_REPORTS_BASE,
            settings.usda_psd_rate_limit_seconds,
            headers={"User-Agent": "CommodityWatch/1.0"},
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_commodity_summary(self) -> bytes:
        return (await self._client.get("/CWRCommoditySummary.xml")).content

    async def get_weekly_highlights(self) -> bytes:
        return (await self._client.get("/WeeklyHighlightsReport.xml")).content
