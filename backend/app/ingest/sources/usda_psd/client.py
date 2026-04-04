from __future__ import annotations

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


SOAP_NAMESPACE = "http://www.fas.usda.gov/wsfaspsd/"


def _soap_envelope(method: str, body: str) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
        "<soap:Body>"
        f'<{method} xmlns="{SOAP_NAMESPACE}">{body}</{method}>'
        "</soap:Body>"
        "</soap:Envelope>"
    )


class USDAPSDClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient(
            "https://apps.fas.usda.gov/PSDExternalAPIService",
            settings.usda_psd_rate_limit_seconds,
            headers={"User-Agent": "CommodityWatch/1.0"},
        )

    async def close(self) -> None:
        await self._client.close()

    async def get_data_by_commodity(self, commodity_code: str) -> bytes:
        envelope = _soap_envelope("getDatabyCommodity", f"<strCommodityCode>{commodity_code}</strCommodityCode>")
        response = await self._client.post(
            "/svcPSD_AMIS.asmx",
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": '"http://www.fas.usda.gov/wsfaspsd/getDatabyCommodity"',
            },
            content=envelope,
        )
        return response.content
