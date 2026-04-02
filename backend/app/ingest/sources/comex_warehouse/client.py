from __future__ import annotations

import httpx

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


COMEX_REPORT_URLS = {
    "GOLD": "https://www.cmegroup.com/delivery_reports/Gold_Stocks.xls",
    "SILVER": "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls",
}
BLOCKED_MARKERS = (
    "This IP address is blocked due to suspected web scraping activity",
    "Access Denied",
    "Reference #",
)


class COMEXWarehouseAccessBlockedError(RuntimeError):
    pass


class COMEXWarehouseClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://www.cmegroup.com",
            rate_limit_seconds=settings.exchange_scrape_rate_limit_seconds,
            headers={
                "User-Agent": "CommodityWatch/1.0",
                "Accept": "application/vnd.ms-excel,application/octet-stream,text/html;q=0.8,*/*;q=0.5",
                "Referer": "https://www.cmegroup.com/clearing/operations-and-deliveries/nymex-delivery-notices.html",
            },
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_report(self, symbol: str) -> bytes:
        url = COMEX_REPORT_URLS[symbol]
        try:
            response = await self._client.get(url)
        except httpx.HTTPStatusError as exc:
            response = exc.response
            if response.status_code != 403:
                raise
        raw = response.content
        text_preview = raw[:4000].decode("utf-8", errors="ignore")
        if response.status_code == 403 or any(marker in text_preview for marker in BLOCKED_MARKERS):
            raise COMEXWarehouseAccessBlockedError(f"CME blocked {symbol} warehouse report from this environment.")
        if not _looks_like_spreadsheet(raw):
            raise COMEXWarehouseAccessBlockedError(f"CME blocked {symbol} warehouse report from this environment.")
        return raw


def _looks_like_spreadsheet(raw: bytes) -> bool:
    prefix = raw[:16]
    return (
        prefix.startswith(b"\xd0\xcf\x11\xe0")
        or prefix.startswith(b"PK")
        or prefix.lstrip(b"\xef\xbb\xbf").startswith(b"<?xml")
    )
