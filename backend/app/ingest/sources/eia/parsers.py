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
    if raw in (None, "", "NA", "null"):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    return float(str(raw).replace(",", ""))


def _parse_period(raw: str, frequency: str) -> datetime:
    if frequency in {"daily", "weekly"}:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    if frequency == "monthly":
        return datetime.fromisoformat(f"{raw}-01").replace(tzinfo=timezone.utc)
    raise ValueError(f"Unsupported EIA frequency: {frequency}")


def _period_start(period_end_at: datetime, frequency: str) -> datetime:
    if frequency == "weekly":
        return period_end_at - timedelta(days=6)
    return period_end_at


def parse_eia_response(payload: dict, frequency: str) -> list[ParsedObservation]:
    response = payload.get("response", payload)
    rows = response.get("data", [])
    parsed: list[ParsedObservation] = []
    for row in rows:
        period_raw = row.get("period")
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
                source_item_ref=row.get("series-description") or row.get("seriesDescription"),
            )
        )
    return parsed

