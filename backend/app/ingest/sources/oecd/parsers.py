from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO


@dataclass(frozen=True, slots=True)
class ParsedOECDCLIObservation:
    source_series_key: str
    reference_area: str
    period_start_at: datetime
    period_end_at: datetime
    value: float
    source_item_ref: str


def _parse_month_period(raw: str) -> tuple[datetime, datetime]:
    cleaned = str(raw or "").strip()
    if len(cleaned) != 7:
        raise ValueError(f"Unsupported OECD TIME_PERIOD value: {raw!r}")
    year = int(cleaned[:4])
    month = int(cleaned[5:7])
    period_start = datetime(year, month, 1, tzinfo=timezone.utc)
    next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return period_start, next_month - timedelta(seconds=1)


def parse_oecd_cli_csv(raw: bytes) -> list[ParsedOECDCLIObservation]:
    reader = csv.DictReader(StringIO(raw.decode("utf-8-sig")))
    parsed: list[ParsedOECDCLIObservation] = []
    for row in reader:
        ref_area = str(row.get("REF_AREA") or "").strip().upper()
        period = str(row.get("TIME_PERIOD") or "").strip()
        raw_value = str(row.get("OBS_VALUE") or "").strip()
        obs_status = str(row.get("OBS_STATUS") or "A").strip().upper()
        frequency = str(row.get("FREQ") or "").strip().upper()
        measure = str(row.get("MEASURE") or "").strip().upper()
        unit_measure = str(row.get("UNIT_MEASURE") or "").strip().upper()

        if not ref_area or not period or not raw_value:
            continue
        if frequency != "M" or measure != "LI" or unit_measure != "IX":
            continue
        if obs_status and obs_status != "A":
            continue

        period_start_at, period_end_at = _parse_month_period(period)
        value = float(raw_value)
        parsed.append(
            ParsedOECDCLIObservation(
                source_series_key=ref_area,
                reference_area=str(row.get("Reference area") or ref_area).strip() or ref_area,
                period_start_at=period_start_at,
                period_end_at=period_end_at,
                value=value,
                source_item_ref=f"{ref_area}:{period}",
            )
        )
    return parsed
