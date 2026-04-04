from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


@dataclass(frozen=True, slots=True)
class ParsedEmberObservation:
    period_start_at: datetime
    period_end_at: datetime
    value_twh: float
    source_item_ref: str
    entity: str
    entity_code: str | None

    def to_item(self) -> dict[str, object]:
        return {
            "period_start_at": self.period_start_at.isoformat(),
            "period_end_at": self.period_end_at.isoformat(),
            "value_twh": self.value_twh,
            "source_item_ref": self.source_item_ref,
            "entity": self.entity,
            "entity_code": self.entity_code,
        }


def _month_end(value: date) -> date:
    if value.month == 12:
        return date(value.year, 12, 31)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)


def parse_ember_stats_timestamp(payload: dict) -> datetime | None:
    raw = ((payload.get("stats") or {}).get("timestamp"))
    if not raw:
        return None
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_ember_monthly_demand(payload: dict) -> list[ParsedEmberObservation]:
    parsed: list[ParsedEmberObservation] = []
    for row in payload.get("data") or []:
        raw_date = row.get("date")
        raw_value = row.get("demand_twh")
        if raw_date is None or raw_value is None:
            continue
        month_date = date.fromisoformat(f"{raw_date}-01")
        period_start_at = datetime(month_date.year, month_date.month, 1, tzinfo=timezone.utc)
        period_end_at = datetime.combine(_month_end(month_date), datetime.max.time(), tzinfo=timezone.utc).replace(microsecond=0)
        parsed.append(
            ParsedEmberObservation(
                period_start_at=period_start_at,
                period_end_at=period_end_at,
                value_twh=float(raw_value),
                source_item_ref=f"{row.get('entity_code') or row.get('entity')}:{raw_date}",
                entity=str(row.get("entity") or ""),
                entity_code=row.get("entity_code"),
            )
        )
    return parsed
