from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(slots=True)
class ParsedObservation:
    period_start_at: datetime
    period_end_at: datetime
    value: float
    source_item_ref: str | None = None


def _parse_number(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = str(raw).strip()
    if not cleaned:
        return None
    if cleaned.upper() in {"NA", "N/A", "NULL", "NONE"}:
        return None
    return float(cleaned.replace(",", ""))


def _as_utc(datetime_value: datetime) -> datetime:
    if datetime_value.tzinfo is None:
        return datetime_value.replace(tzinfo=timezone.utc)
    return datetime_value.astimezone(timezone.utc)


def _parse_period(raw: str, frequency: str) -> datetime:
    cleaned = str(raw).strip()
    if frequency == "hourly":
        if len(cleaned) == 13:
            return _as_utc(datetime.strptime(cleaned, "%Y-%m-%dT%H"))
        return _as_utc(datetime.fromisoformat(cleaned))
    if frequency in {"daily", "weekly"}:
        return _as_utc(datetime.fromisoformat(cleaned))
    if frequency == "monthly":
        monthly_raw = cleaned if len(cleaned) > 7 else f"{cleaned}-01"
        return _as_utc(datetime.fromisoformat(monthly_raw))
    raise ValueError(f"Unsupported EIA frequency: {frequency}")


def _period_start(period_end_at: datetime, frequency: str) -> datetime:
    if frequency == "weekly":
        return period_end_at - timedelta(days=6)
    if frequency == "hourly":
        return period_end_at
    return period_end_at


def parse_eia_response(payload: dict, frequency: str) -> list[ParsedObservation]:
    response = payload.get("response", payload)
    rows = response.get("data", [])
    parsed: list[ParsedObservation] = []
    for row in rows:
        period_raw = row.get("period") or row.get("date") or row.get("period_start")
        if not period_raw:
            continue
        value = _parse_number(row.get("value"))
        if value is None:
            continue
        period_end_at = _parse_period(str(period_raw), frequency)
        parsed.append(
            ParsedObservation(
                period_start_at=_period_start(period_end_at, frequency),
                period_end_at=period_end_at,
                value=value,
                source_item_ref=(
                    row.get("series-description")
                    or row.get("seriesDescription")
                    or row.get("series_description")
                    or row.get("description")
                ),
            )
        )
    return parsed
