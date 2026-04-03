from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx
import yaml

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


CONTRACTS_PATH = Path(__file__).with_name("contracts.yml")


class ICECertifiedAccessBlockedError(RuntimeError):
    pass


class ICECertifiedStructureChangedError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ICEContractDefinition:
    source_series_key: str
    name: str
    report_id: int | None
    report_url: str | None
    expected_report_name: str | None
    expected_exchange: str | None
    expected_category: str | None
    unit_native_code: str
    availability_note: str | None = None


@dataclass(frozen=True, slots=True)
class ICEReportMetadata:
    report_id: int
    name: str
    report_template_url: str
    exchange: str
    category_name: str
    active: bool
    recaptcha_required: bool


def load_contracts() -> list[ICEContractDefinition]:
    raw_contracts = yaml.safe_load(CONTRACTS_PATH.read_text(encoding="utf-8")) or []
    return [
        ICEContractDefinition(
            source_series_key=_clean_text(item.get("source_series_key")),
            name=_clean_text(item.get("name")),
            report_id=_optional_int(item.get("report_id")),
            report_url=_optional_text(item.get("report_url")),
            expected_report_name=_optional_text(item.get("expected_report_name")),
            expected_exchange=_optional_text(item.get("expected_exchange")),
            expected_category=_optional_text(item.get("expected_category")),
            unit_native_code=_clean_text(item.get("unit_native_code")),
            availability_note=_optional_text(item.get("availability_note")),
        )
        for item in raw_contracts
    ]


class ICECertifiedClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://www.ice.com",
            rate_limit_seconds=settings.exchange_scrape_rate_limit_seconds,
            headers={"User-Agent": "CommodityWatch/1.0", "Accept": "application/json"},
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_metadata(self, report_id: int) -> ICEReportMetadata:
        response = await self._client.get(f"/marketdata/api/reports/metadata/{report_id}")
        payload = response.json()
        return ICEReportMetadata(
            report_id=int(payload["id"]),
            name=_clean_text(payload.get("name")),
            report_template_url=_clean_text(payload.get("reportTemplateURL")),
            exchange=_clean_text(payload.get("exchange")),
            category_name=_clean_text(payload.get("categoryName")),
            active=bool(payload["active"]),
            recaptcha_required=bool(payload["recaptchaRequired"]),
        )

    async def get_criteria(self, report_id: int) -> dict:
        try:
            response = await self._client.get(f"/marketdata/api/reports/{report_id}/criteria")
        except httpx.HTTPStatusError as exc:
            response = exc.response
        payload = response.json()
        if response.status_code == 409 or payload.get("status") == 409:
            raise ICECertifiedAccessBlockedError(f"ICE report {report_id} requires recaptcha validation before data access.")
        return payload


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split())


def _optional_text(value: object) -> str | None:
    cleaned = _clean_text(value)
    return cleaned or None


def _optional_int(value: object) -> int | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    return int(cleaned)
