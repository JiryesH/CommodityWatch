from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urlencode

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient
from app.ingest.sources.usda_wasde.parsers import parse_available_release_months, parse_release_listing


PUBLICATION_PATH = "/publication/world-agricultural-supply-and-demand-estimates"


@dataclass(slots=True)
class WASDEReleaseRef:
    released_on: date
    workbook_url: str
    source_url: str
    month_key: str
    pdf_url: str | None = None
    title: str | None = None

    @property
    def release_key(self) -> str:
        return self.released_on.isoformat()


class USDAWASDEClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://esmis.nal.usda.gov",
            rate_limit_seconds=settings.usda_psd_rate_limit_seconds,
            headers={"User-Agent": "CommodityWatch/1.0"},
        )

    async def close(self) -> None:
        await self._client.close()

    async def list_available_release_months(self) -> list[str]:
        response = await self._client.get(PUBLICATION_PATH)
        return parse_available_release_months(response.text)

    async def list_releases_for_month(self, month_key: str) -> list[WASDEReleaseRef]:
        response = await self._client.get(
            f"{PUBLICATION_PATH}?{urlencode({'date': month_key})}"
        )
        return parse_release_listing(response.text, month_key=month_key)

    async def get_workbook(self, release: WASDEReleaseRef) -> bytes:
        response = await self._client.get(release.workbook_url)
        return response.content


def release_datetime(released_on: date) -> datetime:
    from datetime import time, timezone
    from zoneinfo import ZoneInfo

    release_local = datetime.combine(released_on, time(12, 0), tzinfo=ZoneInfo("America/New_York"))
    return release_local.astimezone(timezone.utc)
