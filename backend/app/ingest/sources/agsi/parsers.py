from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ParsedAGSIObservation:
    period_start_at: datetime
    period_end_at: datetime
    value: float
    percent_full: float | None
    source_item_ref: str | None


def parse_agsi_payload(payload: dict) -> list[ParsedAGSIObservation]:
    rows = payload.get("data", [])
    parsed: list[ParsedAGSIObservation] = []
    for row in rows:
        gas_day = row.get("gasDayStart")
        gas_in_storage = row.get("gasInStorage")
        if not gas_day or gas_in_storage in (None, "", "NA"):
            continue
        period = datetime.fromisoformat(str(gas_day)).replace(tzinfo=timezone.utc)
        parsed.append(
            ParsedAGSIObservation(
                period_start_at=period,
                period_end_at=period,
                value=float(str(gas_in_storage).replace(",", "")),
                percent_full=float(str(row["full"]).replace(",", "")) if row.get("full") not in (None, "", "NA") else None,
                source_item_ref=row.get("url"),
            )
        )
    return parsed

