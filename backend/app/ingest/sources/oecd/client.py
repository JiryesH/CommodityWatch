from __future__ import annotations

from datetime import date

from app.core.config import get_settings
from app.ingest.common.http import RateLimitedAsyncClient


class OECDClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._client = RateLimitedAsyncClient("https://sdmx.oecd.org", settings.oecd_rate_limit_seconds)

    async def close(self) -> None:
        await self._client.close()

    async def get_cli_snapshot(
        self,
        ref_areas: list[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> bytes:
        if not ref_areas:
            raise ValueError("At least one OECD reference area is required.")

        ref_area_key = "+".join(sorted({str(value).strip().upper() for value in ref_areas if str(value).strip()}))
        if not ref_area_key:
            raise ValueError("At least one valid OECD reference area is required.")

        params = {
            "dimensionAtObservation": "AllDimensions",
            "format": "csvfilewithlabels",
        }
        if start_date is not None:
            params["startPeriod"] = start_date.strftime("%Y-%m")
        if end_date is not None:
            params["endPeriod"] = end_date.strftime("%Y-%m")

        response = await self._client.get(
            f"/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/{ref_area_key}.M.LI.IX._Z.AA.IX._Z.H",
            params=params,
        )
        return response.content
