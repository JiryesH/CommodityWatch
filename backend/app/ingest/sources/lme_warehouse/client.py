from __future__ import annotations

from datetime import date, timedelta

import httpx

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


REPORT_URL_PREFIX = "https://www.lme.com/-/media/files/data/stocks-breakdown"
MONTH_SLUGS = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "may",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "oct",
    11: "nov",
    12: "dec",
}


class LMEWarehouseAccessBlockedError(RuntimeError):
    pass


class LMEReportNotFoundError(FileNotFoundError):
    pass


def build_report_filename(report_date: date) -> str:
    return f"metals-reports-{report_date.day:02d}-{MONTH_SLUGS[report_date.month]}-{report_date.year}.xls"


def build_report_url(report_date: date) -> str:
    return f"{REPORT_URL_PREFIX}/{build_report_filename(report_date)}"


class LMEWarehouseClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://www.lme.com",
            rate_limit_seconds=settings.exchange_scrape_rate_limit_seconds,
            headers={
                "User-Agent": "CommodityWatch/1.0",
                "Accept": "application/vnd.ms-excel,application/octet-stream,text/html;q=0.8,*/*;q=0.5",
            },
        )

    async def close(self) -> None:
        await self._client.close()

    async def has_report(self, report_date: date) -> bool:
        response = await self._request_report(report_date, method="HEAD")
        if _is_excel_response(response):
            return True
        if _is_missing_redirect(response) or response.status_code == 404:
            return False
        if _is_access_blocked(response):
            raise LMEWarehouseAccessBlockedError("LME direct report URL is blocked from this environment.")
        if response.status_code == 405 or response.status_code == 200:
            # Some report URLs respond to HEAD without a usable content type.
            response = await self._request_report(report_date, method="GET")
            if _is_access_blocked(response):
                raise LMEWarehouseAccessBlockedError("LME direct report URL is blocked from this environment.")
            return _is_excel_response(response)
        return False

    async def get_report(self, report_date: date) -> bytes:
        response = await self._request_report(report_date, method="GET")
        if _is_access_blocked(response):
            raise LMEWarehouseAccessBlockedError("LME direct report URL is blocked from this environment.")
        if not _is_excel_response(response):
            raise LMEReportNotFoundError(f"No LME workbook published for {report_date.isoformat()}")
        return response.content

    async def find_latest_available_report(self, *, as_of: date, lookback_days: int = 10) -> date | None:
        for day_offset in range(lookback_days + 1):
            candidate = as_of - timedelta(days=day_offset)
            if candidate.weekday() >= 5:
                continue
            if await self.has_report(candidate):
                return candidate
        return None

    async def _request_report(self, report_date: date, *, method: str) -> httpx.Response:
        try:
            return await self._client.request(method, build_report_url(report_date))
        except httpx.HTTPStatusError as exc:
            return exc.response


def _is_missing_redirect(response) -> bool:
    if response.status_code not in {301, 302, 303, 307, 308}:
        return False
    location = (response.headers.get("location") or "").lower()
    return "/system-pages/404-page" in location


def _is_excel_response(response) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    return response.status_code == 200 and (
        "application/vnd.ms-excel" in content_type or "application/octet-stream" in content_type
    )


def _is_access_blocked(response) -> bool:
    if (response.headers.get("cf-mitigated") or "").lower() == "challenge":
        return True
    return response.status_code in {401, 403}
