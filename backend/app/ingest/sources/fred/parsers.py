from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


FRED_LIVE_VINTAGE_COUNT = 3


@dataclass(frozen=True, slots=True)
class FREDRelease:
    release_id: int
    name: str
    link: str | None


@dataclass(frozen=True, slots=True)
class ParsedFREDObservation:
    period_start_at: datetime
    period_end_at: datetime
    value: float
    source_item_ref: str


def _parse_value(raw: str | float | int | None) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = str(raw).strip()
    if not cleaned or cleaned == ".":
        return None
    return float(cleaned)


def parse_fred_release(payload: dict) -> FREDRelease:
    releases = payload.get("releases") or []
    if not releases:
        raise ValueError("FRED series release payload did not include any releases.")
    first = releases[0]
    return FREDRelease(
        release_id=int(first["id"]),
        name=str(first["name"]),
        link=first.get("link"),
    )


def parse_fred_release_dates(payload: dict) -> list[date]:
    parsed: list[date] = []
    for row in payload.get("release_dates") or []:
        raw = row.get("date")
        if not raw:
            continue
        parsed.append(date.fromisoformat(str(raw)))
    return parsed


def selected_vintage_dates(
    release_dates: list[date],
    *,
    run_mode: str,
    start_date: date | None,
) -> list[date]:
    if not release_dates:
        return []
    if run_mode == "backfill":
        if start_date is None:
            return sorted(set(release_dates))
        cutoff = start_date - timedelta(days=31)
        return sorted({released_on for released_on in release_dates if released_on >= cutoff})
    return sorted(set(release_dates[:FRED_LIVE_VINTAGE_COUNT]))


def parse_fred_observations(payload: dict, frequency: str) -> list[ParsedFREDObservation]:
    parsed: list[ParsedFREDObservation] = []
    for row in payload.get("observations") or []:
        raw_date = row.get("date")
        value = _parse_value(row.get("value"))
        if not raw_date or value is None:
            continue
        point_date = date.fromisoformat(str(raw_date))
        if frequency == "monthly":
            period_start = datetime(point_date.year, point_date.month, 1, tzinfo=timezone.utc)
            next_month = date(point_date.year + 1, 1, 1) if point_date.month == 12 else date(point_date.year, point_date.month + 1, 1)
            period_end = datetime.combine(next_month, datetime.min.time(), tzinfo=timezone.utc) - timedelta(seconds=1)
        else:
            period_start = datetime.combine(point_date, datetime.min.time(), tzinfo=timezone.utc)
            period_end = period_start
        parsed.append(
            ParsedFREDObservation(
                period_start_at=period_start,
                period_end_at=period_end,
                value=value,
                source_item_ref=str(raw_date),
            )
        )
    return parsed
