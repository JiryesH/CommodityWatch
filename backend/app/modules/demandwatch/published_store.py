from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import StatisticsError, fmean, pstdev
from typing import Any


UTC = timezone.utc
SCHEMA_VERSION = 1
MIN_BACKFILL_DAYS = 365 * 3
SURPRISE_ZSCORE_THRESHOLD = 1.0
WEEK_ENDING_WEEKDAYS = {
    "eia": 4,
    "usda_export_sales": 3,
}
EXPECTED_BACKFILL_SOURCES = frozenset(
    {
        "eia",
        "fred",
        "usda_psd",
        "ember",
    }
)
PETROLEUM_SURPRISE_CODES = frozenset(
    {
        "EIA_US_TOTAL_PRODUCT_SUPPLIED",
        "EIA_GASOLINE_US_PRODUCT_SUPPLIED",
        "EIA_DISTILLATE_US_PRODUCT_SUPPLIED",
    }
)
WASDE_REVISION_SURPRISE_CODES = frozenset(
    {
        "USDA_US_CORN_TOTAL_USE_WASDE",
        "USDA_US_SOYBEAN_TOTAL_USE_WASDE",
    }
)


def utcnow() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _month_last_day(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _quarter_last_day(year: int, quarter: int) -> date:
    month = quarter * 3
    return _month_last_day(year, month)


def _parse_release_month(raw: object) -> tuple[int, int] | None:
    normalized = str(raw or "").strip()
    if len(normalized) != 7 or normalized[4] != "-":
        return None
    try:
        return int(normalized[:4]), int(normalized[5:7])
    except ValueError:
        return None


def _age_in_days(now: datetime, value: datetime | None) -> int | None:
    if value is None:
        return None
    return max(0, int((now - value).total_seconds() // 86400))


def _stale_after_days(frequency: str) -> int:
    normalized = str(frequency or "").strip().lower()
    if normalized == "hourly":
        return 2
    if normalized == "daily":
        return 7
    if normalized == "weekly":
        return 30
    if normalized == "monthly":
        return 90
    if normalized == "quarterly":
        return 180
    if normalized == "annual":
        return 400
    return 90


def _history_window_for_surprise(frequency: str) -> int:
    normalized = str(frequency or "").strip().lower()
    if normalized == "hourly":
        return 24 * 14
    if normalized == "daily":
        return 60
    if normalized == "weekly":
        return 52
    if normalized == "monthly":
        return 12
    if normalized == "quarterly":
        return 8
    return 12


def _trend_direction(value: float | None, *, flat_threshold: float = 0.01) -> str:
    if value is None:
        return "unknown"
    if value > flat_threshold:
        return "up"
    if value < -flat_threshold:
        return "down"
    return "flat"


def _safe_pct_change(current: float | None, base: float | None) -> float | None:
    if current is None or base in (None, 0):
        return None
    return ((current - base) / abs(base)) * 100.0


def _series_backfill_expected(series: "DemandSeriesDefinition") -> bool:
    return series.source_slug in EXPECTED_BACKFILL_SOURCES


def _week_ending_weekday(series: "DemandSeriesDefinition") -> int | None:
    return WEEK_ENDING_WEEKDAYS.get(series.source_slug)


@dataclass(frozen=True, slots=True)
class DemandUnitDefinition:
    code: str
    name: str
    symbol: str | None


@dataclass(frozen=True, slots=True)
class DemandVerticalDefinition:
    code: str
    name: str
    commodity_code: str
    sector: str
    nav_label: str | None
    short_label: str | None
    description: str | None
    display_order: int
    active: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DemandSeriesDefinition:
    id: str
    indicator_id: str
    code: str
    name: str
    description: str | None
    vertical_code: str
    tier: str
    coverage_status: str
    display_order: int
    notes: str | None
    measure_family: str
    frequency: str
    commodity_code: str | None
    geography_code: str | None
    source_slug: str
    source_name: str | None
    source_legal_status: str | None
    source_url: str | None
    source_series_key: str | None
    native_unit_code: str | None
    native_unit_symbol: str | None
    canonical_unit_code: str | None
    canonical_unit_symbol: str | None
    default_observation_kind: str
    visibility_tier: str
    active: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DemandObservation:
    id: str
    series_id: str
    period_start_at: datetime
    period_end_at: datetime
    normalized_period_start_at: datetime
    normalized_period_end_at: datetime
    period_label: str
    release_date: datetime | None
    vintage_at: datetime
    value_native: float
    unit_native_code: str
    value_canonical: float
    unit_canonical_code: str
    observation_kind: str
    revision_sequence: int
    is_latest: bool
    source_release_id: str | None
    source_url: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DemandLatestMetrics:
    series_id: str
    latest_observation_id: str | None
    latest_period_start_at: datetime | None
    latest_period_end_at: datetime | None
    latest_period_label: str | None
    latest_release_date: datetime | None
    latest_vintage_at: datetime | None
    latest_source_url: str | None
    latest_value: float | None
    unit_code: str | None
    unit_symbol: str | None
    prior_value: float | None
    change_abs: float | None
    change_pct: float | None
    yoy_value: float | None
    yoy_abs: float | None
    yoy_pct: float | None
    moving_average_4w: float | None
    trend_3m_abs: float | None
    trend_3m_pct: float | None
    trend_3m_direction: str
    freshness_state: str
    stale: bool
    stale_reason: str | None
    release_age_days: int | None
    period_age_days: int | None
    surprise_flag: bool
    surprise_direction: str | None
    surprise_score: float | None
    surprise_reason: str | None
    observation_count: int
    latest_observation_count: int
    latest_revision_sequence: int | None
    vintage_count: int
    history_days: int
    backfill_expected: bool
    backfill_complete: bool
    canonical_units_ok: bool
    canonical_unit_reason: str | None
    date_convention_ok: bool
    date_convention_reason: str | None


class PublishedDemandRepository:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._units_by_code: dict[str, DemandUnitDefinition] = {}
        self._verticals_by_code: dict[str, DemandVerticalDefinition] = {}
        self._series_by_id: dict[str, DemandSeriesDefinition] = {}
        self._observations_by_series_id: dict[str, list[DemandObservation]] = {}
        self._latest_metrics_by_series_id: dict[str, DemandLatestMetrics] = {}
        self._load_from_db()

    def _load_from_db(self) -> None:
        if not self.database_path.exists():
            raise FileNotFoundError(f"DemandWatch published database not found: {self.database_path}")

        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            for row in connection.execute(
                """
                SELECT code, name, symbol
                FROM published_demand_units
                ORDER BY code
                """
            ).fetchall():
                self._units_by_code[str(row["code"])] = DemandUnitDefinition(
                    code=str(row["code"]),
                    name=str(row["name"]),
                    symbol=row["symbol"],
                )

            for row in connection.execute(
                """
                SELECT
                    code,
                    name,
                    commodity_code,
                    sector,
                    nav_label,
                    short_label,
                    description,
                    display_order,
                    active,
                    metadata_json
                FROM published_demand_verticals
                ORDER BY display_order, code
                """
            ).fetchall():
                self._verticals_by_code[str(row["code"])] = DemandVerticalDefinition(
                    code=str(row["code"]),
                    name=str(row["name"]),
                    commodity_code=str(row["commodity_code"]),
                    sector=str(row["sector"]),
                    nav_label=row["nav_label"],
                    short_label=row["short_label"],
                    description=row["description"],
                    display_order=int(row["display_order"]),
                    active=bool(row["active"]),
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )

            for row in connection.execute(
                """
                SELECT
                    id,
                    indicator_id,
                    code,
                    name,
                    description,
                    vertical_code,
                    tier,
                    coverage_status,
                    display_order,
                    notes,
                    measure_family,
                    frequency,
                    commodity_code,
                    geography_code,
                    source_slug,
                    source_name,
                    source_legal_status,
                    source_url,
                    source_series_key,
                    native_unit_code,
                    native_unit_symbol,
                    canonical_unit_code,
                    canonical_unit_symbol,
                    default_observation_kind,
                    visibility_tier,
                    active,
                    metadata_json
                FROM published_demand_series
                ORDER BY vertical_code, display_order, code
                """
            ).fetchall():
                series = DemandSeriesDefinition(
                    id=str(row["id"]),
                    indicator_id=str(row["indicator_id"]),
                    code=str(row["code"]),
                    name=str(row["name"]),
                    description=row["description"],
                    vertical_code=str(row["vertical_code"]),
                    tier=str(row["tier"]),
                    coverage_status=str(row["coverage_status"]),
                    display_order=int(row["display_order"]),
                    notes=row["notes"],
                    measure_family=str(row["measure_family"]),
                    frequency=str(row["frequency"]),
                    commodity_code=row["commodity_code"],
                    geography_code=row["geography_code"],
                    source_slug=str(row["source_slug"]),
                    source_name=row["source_name"],
                    source_legal_status=row["source_legal_status"],
                    source_url=row["source_url"],
                    source_series_key=row["source_series_key"],
                    native_unit_code=row["native_unit_code"],
                    native_unit_symbol=row["native_unit_symbol"],
                    canonical_unit_code=row["canonical_unit_code"],
                    canonical_unit_symbol=row["canonical_unit_symbol"],
                    default_observation_kind=str(row["default_observation_kind"]),
                    visibility_tier=str(row["visibility_tier"]),
                    active=bool(row["active"]),
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )
                self._series_by_id[series.id] = series

            for row in connection.execute(
                """
                SELECT
                    id,
                    series_id,
                    period_start_at,
                    period_end_at,
                    normalized_period_start_at,
                    normalized_period_end_at,
                    period_label,
                    release_date,
                    vintage_at,
                    value_native,
                    unit_native_code,
                    value_canonical,
                    unit_canonical_code,
                    observation_kind,
                    revision_sequence,
                    is_latest,
                    source_release_id,
                    source_url,
                    metadata_json
                FROM published_demand_observations
                ORDER BY series_id, normalized_period_end_at, vintage_at
                """
            ).fetchall():
                observation = DemandObservation(
                    id=str(row["id"]),
                    series_id=str(row["series_id"]),
                    period_start_at=datetime.fromisoformat(str(row["period_start_at"])),
                    period_end_at=datetime.fromisoformat(str(row["period_end_at"])),
                    normalized_period_start_at=datetime.fromisoformat(str(row["normalized_period_start_at"])),
                    normalized_period_end_at=datetime.fromisoformat(str(row["normalized_period_end_at"])),
                    period_label=str(row["period_label"]),
                    release_date=datetime.fromisoformat(str(row["release_date"])) if row["release_date"] else None,
                    vintage_at=datetime.fromisoformat(str(row["vintage_at"])),
                    value_native=float(row["value_native"]),
                    unit_native_code=str(row["unit_native_code"]),
                    value_canonical=float(row["value_canonical"]),
                    unit_canonical_code=str(row["unit_canonical_code"]),
                    observation_kind=str(row["observation_kind"]),
                    revision_sequence=int(row["revision_sequence"]),
                    is_latest=bool(row["is_latest"]),
                    source_release_id=row["source_release_id"],
                    source_url=row["source_url"],
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )
                self._observations_by_series_id.setdefault(observation.series_id, []).append(observation)

            for row in connection.execute(
                """
                SELECT
                    series_id,
                    latest_observation_id,
                    latest_period_start_at,
                    latest_period_end_at,
                    latest_period_label,
                    latest_release_date,
                    latest_vintage_at,
                    latest_source_url,
                    latest_value,
                    unit_code,
                    unit_symbol,
                    prior_value,
                    change_abs,
                    change_pct,
                    yoy_value,
                    yoy_abs,
                    yoy_pct,
                    moving_average_4w,
                    trend_3m_abs,
                    trend_3m_pct,
                    trend_3m_direction,
                    freshness_state,
                    stale,
                    stale_reason,
                    release_age_days,
                    period_age_days,
                    surprise_flag,
                    surprise_direction,
                    surprise_score,
                    surprise_reason,
                    observation_count,
                    latest_observation_count,
                    latest_revision_sequence,
                    vintage_count,
                    history_days,
                    backfill_expected,
                    backfill_complete,
                    canonical_units_ok,
                    canonical_unit_reason,
                    date_convention_ok,
                    date_convention_reason
                FROM published_demand_latest_metrics
                ORDER BY series_id
                """
            ).fetchall():
                self._latest_metrics_by_series_id[str(row["series_id"])] = DemandLatestMetrics(
                    series_id=str(row["series_id"]),
                    latest_observation_id=row["latest_observation_id"],
                    latest_period_start_at=datetime.fromisoformat(str(row["latest_period_start_at"]))
                    if row["latest_period_start_at"]
                    else None,
                    latest_period_end_at=datetime.fromisoformat(str(row["latest_period_end_at"]))
                    if row["latest_period_end_at"]
                    else None,
                    latest_period_label=row["latest_period_label"],
                    latest_release_date=datetime.fromisoformat(str(row["latest_release_date"]))
                    if row["latest_release_date"]
                    else None,
                    latest_vintage_at=datetime.fromisoformat(str(row["latest_vintage_at"]))
                    if row["latest_vintage_at"]
                    else None,
                    latest_source_url=row["latest_source_url"],
                    latest_value=float(row["latest_value"]) if row["latest_value"] is not None else None,
                    unit_code=row["unit_code"],
                    unit_symbol=row["unit_symbol"],
                    prior_value=float(row["prior_value"]) if row["prior_value"] is not None else None,
                    change_abs=float(row["change_abs"]) if row["change_abs"] is not None else None,
                    change_pct=float(row["change_pct"]) if row["change_pct"] is not None else None,
                    yoy_value=float(row["yoy_value"]) if row["yoy_value"] is not None else None,
                    yoy_abs=float(row["yoy_abs"]) if row["yoy_abs"] is not None else None,
                    yoy_pct=float(row["yoy_pct"]) if row["yoy_pct"] is not None else None,
                    moving_average_4w=float(row["moving_average_4w"]) if row["moving_average_4w"] is not None else None,
                    trend_3m_abs=float(row["trend_3m_abs"]) if row["trend_3m_abs"] is not None else None,
                    trend_3m_pct=float(row["trend_3m_pct"]) if row["trend_3m_pct"] is not None else None,
                    trend_3m_direction=str(row["trend_3m_direction"]),
                    freshness_state=str(row["freshness_state"]),
                    stale=bool(row["stale"]),
                    stale_reason=row["stale_reason"],
                    release_age_days=int(row["release_age_days"]) if row["release_age_days"] is not None else None,
                    period_age_days=int(row["period_age_days"]) if row["period_age_days"] is not None else None,
                    surprise_flag=bool(row["surprise_flag"]),
                    surprise_direction=row["surprise_direction"],
                    surprise_score=float(row["surprise_score"]) if row["surprise_score"] is not None else None,
                    surprise_reason=row["surprise_reason"],
                    observation_count=int(row["observation_count"]),
                    latest_observation_count=int(row["latest_observation_count"]),
                    latest_revision_sequence=int(row["latest_revision_sequence"])
                    if row["latest_revision_sequence"] is not None
                    else None,
                    vintage_count=int(row["vintage_count"]),
                    history_days=int(row["history_days"]),
                    backfill_expected=bool(row["backfill_expected"]),
                    backfill_complete=bool(row["backfill_complete"]),
                    canonical_units_ok=bool(row["canonical_units_ok"]),
                    canonical_unit_reason=row["canonical_unit_reason"],
                    date_convention_ok=bool(row["date_convention_ok"]),
                    date_convention_reason=row["date_convention_reason"],
                )
        finally:
            connection.close()


def normalize_period_bounds(
    series: DemandSeriesDefinition,
    *,
    period_start_at: datetime,
    period_end_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> tuple[datetime, datetime]:
    metadata = metadata or {}
    frequency = str(series.frequency or "").strip().lower()
    period_start_at = _as_utc(period_start_at) or period_start_at
    period_end_at = _as_utc(period_end_at) or period_end_at

    if frequency == "hourly":
        normalized_end = period_end_at.replace(minute=0, second=0, microsecond=0)
        return normalized_end, normalized_end

    if frequency == "daily":
        normalized_day = period_end_at.date()
        return (
            datetime.combine(normalized_day, time(0, 0), tzinfo=UTC),
            datetime.combine(normalized_day, time(23, 59, 59), tzinfo=UTC),
        )

    if frequency == "weekly":
        normalized_day = period_end_at.date()
        return (
            datetime.combine(normalized_day - timedelta(days=6), time(0, 0), tzinfo=UTC),
            datetime.combine(normalized_day, time(23, 59, 59), tzinfo=UTC),
        )

    if frequency == "monthly":
        release_month = _parse_release_month(metadata.get("release_month"))
        if release_month is not None:
            year, month = release_month
        else:
            year, month = period_end_at.year, period_end_at.month
        month_end = _month_last_day(year, month)
        return (
            datetime(year, month, 1, tzinfo=UTC),
            datetime.combine(month_end, time(23, 59, 59), tzinfo=UTC),
        )

    if frequency == "quarterly":
        quarter = ((period_end_at.month - 1) // 3) + 1
        quarter_end = _quarter_last_day(period_end_at.year, quarter)
        quarter_start_month = ((quarter - 1) * 3) + 1
        return (
            datetime(period_end_at.year, quarter_start_month, 1, tzinfo=UTC),
            datetime.combine(quarter_end, time(23, 59, 59), tzinfo=UTC),
        )

    if frequency == "annual":
        return (
            datetime(period_end_at.year, 1, 1, tzinfo=UTC),
            datetime(period_end_at.year, 12, 31, 23, 59, 59, tzinfo=UTC),
        )

    return period_start_at, period_end_at


def format_period_label(series: DemandSeriesDefinition, normalized_period_end_at: datetime) -> str:
    frequency = str(series.frequency or "").strip().lower()
    if frequency == "weekly":
        return f"Week ending {normalized_period_end_at.date().isoformat()}"
    if frequency == "monthly":
        return normalized_period_end_at.strftime("%b %Y")
    if frequency == "quarterly":
        quarter = ((normalized_period_end_at.month - 1) // 3) + 1
        return f"Q{quarter} {normalized_period_end_at.year}"
    if frequency == "annual":
        return str(normalized_period_end_at.year)
    if frequency == "hourly":
        return normalized_period_end_at.strftime("%Y-%m-%d %H:%M UTC")
    return normalized_period_end_at.date().isoformat()


def build_observation(
    series: DemandSeriesDefinition,
    *,
    observation_id: str,
    period_start_at: datetime,
    period_end_at: datetime,
    release_date: datetime | None,
    vintage_at: datetime,
    value_native: float,
    unit_native_code: str,
    value_canonical: float,
    unit_canonical_code: str,
    observation_kind: str,
    revision_sequence: int,
    is_latest: bool,
    source_release_id: str | None,
    source_url: str | None,
    metadata: dict[str, Any] | None = None,
) -> DemandObservation:
    normalized_start_at, normalized_end_at = normalize_period_bounds(
        series,
        period_start_at=period_start_at,
        period_end_at=period_end_at,
        metadata=metadata,
    )
    return DemandObservation(
        id=observation_id,
        series_id=series.id,
        period_start_at=_as_utc(period_start_at) or period_start_at,
        period_end_at=_as_utc(period_end_at) or period_end_at,
        normalized_period_start_at=normalized_start_at,
        normalized_period_end_at=normalized_end_at,
        period_label=format_period_label(series, normalized_end_at),
        release_date=_as_utc(release_date),
        vintage_at=_as_utc(vintage_at) or vintage_at,
        value_native=float(value_native),
        unit_native_code=str(unit_native_code),
        value_canonical=float(value_canonical),
        unit_canonical_code=str(unit_canonical_code),
        observation_kind=str(observation_kind),
        revision_sequence=int(revision_sequence),
        is_latest=bool(is_latest),
        source_release_id=source_release_id,
        source_url=source_url,
        metadata=dict(metadata or {}),
    )


def latest_vintage_observations(observations: list[DemandObservation]) -> list[DemandObservation]:
    grouped: dict[datetime, list[DemandObservation]] = {}
    for observation in observations:
        grouped.setdefault(observation.normalized_period_end_at, []).append(observation)

    latest_points: list[DemandObservation] = []
    for key in sorted(grouped.keys()):
        candidates = grouped[key]
        latest = next((item for item in candidates if item.is_latest), None)
        if latest is None:
            latest = max(candidates, key=lambda item: (item.vintage_at, item.revision_sequence))
        latest_points.append(latest)
    return latest_points


def _previous_year_observation(
    series: DemandSeriesDefinition,
    observations: list[DemandObservation],
    current: DemandObservation,
) -> DemandObservation | None:
    normalized = str(series.frequency or "").strip().lower()
    target_candidates: list[datetime] = []
    if normalized == "hourly":
        target_candidates = [
            current.normalized_period_end_at - timedelta(days=364),
            current.normalized_period_end_at - timedelta(days=365),
        ]
    elif normalized == "daily":
        target_candidates = [current.normalized_period_end_at - timedelta(days=365)]
    elif normalized == "weekly":
        target_candidates = [current.normalized_period_end_at - timedelta(days=364)]
    elif normalized == "monthly":
        target_candidates = [
            datetime.combine(
                _month_last_day(current.normalized_period_end_at.year - 1, current.normalized_period_end_at.month),
                time(23, 59, 59),
                tzinfo=UTC,
            )
        ]
    elif normalized == "quarterly":
        quarter = ((current.normalized_period_end_at.month - 1) // 3) + 1
        target_candidates = [
            datetime.combine(
                _quarter_last_day(current.normalized_period_end_at.year - 1, quarter),
                time(23, 59, 59),
                tzinfo=UTC,
            )
        ]
    elif normalized == "annual":
        target_candidates = [datetime(current.normalized_period_end_at.year - 1, 12, 31, 23, 59, 59, tzinfo=UTC)]

    if not target_candidates:
        return None

    by_end = {item.normalized_period_end_at: item for item in observations}
    for candidate in target_candidates:
        if candidate in by_end:
            return by_end[candidate]

    if normalized == "weekly":
        target_date = target_candidates[0].date()
        eligible = [
            item
            for item in observations
            if abs((item.normalized_period_end_at.date() - target_date).days) <= 3
        ]
        if eligible:
            return min(eligible, key=lambda item: abs((item.normalized_period_end_at.date() - target_date).days))

    return None


def _surprise_baseline(values: list[float], current_value: float) -> tuple[float | None, bool]:
    if len(values) < 4:
        return None, False
    try:
        stddev = pstdev(values)
    except StatisticsError:
        return None, False
    if stddev == 0:
        return None, False
    score = (current_value - fmean(values)) / stddev
    return score, abs(score) >= SURPRISE_ZSCORE_THRESHOLD


def compute_latest_metrics(
    series: DemandSeriesDefinition,
    observations: list[DemandObservation],
    *,
    now: datetime | None = None,
) -> DemandLatestMetrics:
    now = _as_utc(now) or utcnow()
    latest_points = latest_vintage_observations(observations)
    latest = latest_points[-1] if latest_points else None
    prior = latest_points[-2] if len(latest_points) >= 2 else None
    yoy = _previous_year_observation(series, latest_points, latest) if latest is not None else None
    history_days = 0
    if latest_points:
        history_days = max(0, int((latest_points[-1].normalized_period_end_at - latest_points[0].normalized_period_end_at).days))

    release_age_days = _age_in_days(now, latest.release_date if latest else None)
    period_age_days = _age_in_days(now, latest.normalized_period_end_at if latest else None)
    warning_after_days = _stale_after_days(series.frequency)
    freshness_state = "unknown"
    stale = False
    stale_reason = None
    if latest is not None:
        reference_age = release_age_days if release_age_days is not None else period_age_days
        if reference_age is None:
            freshness_state = "unknown"
        elif reference_age > warning_after_days * 2:
            freshness_state = "historical"
            stale = True
            stale_reason = f"Latest data is {reference_age} days old."
        elif reference_age > warning_after_days:
            freshness_state = "stale"
            stale = True
            stale_reason = f"Latest data is {reference_age} days old."
        else:
            freshness_state = "fresh"

    moving_average_4w = None
    if str(series.frequency).lower() == "weekly" and len(latest_points) >= 4:
        moving_average_4w = fmean(point.value_canonical for point in latest_points[-4:])

    trend_3m_abs = None
    trend_3m_pct = None
    trend_3m_direction = "unknown"
    if str(series.frequency).lower() == "monthly" and len(latest_points) >= 6:
        latest_window = latest_points[-3:]
        prior_window = latest_points[-6:-3]
        latest_avg = fmean(point.value_canonical for point in latest_window)
        prior_avg = fmean(point.value_canonical for point in prior_window)
        trend_3m_abs = latest_avg - prior_avg
        trend_3m_pct = _safe_pct_change(latest_avg, prior_avg)
        trend_3m_direction = _trend_direction(trend_3m_pct if trend_3m_pct is not None else trend_3m_abs)

    canonical_units_ok = True
    canonical_unit_reason = None
    if series.canonical_unit_code:
        mismatches = [point.unit_canonical_code for point in latest_points if point.unit_canonical_code != series.canonical_unit_code]
        if mismatches:
            canonical_units_ok = False
            canonical_unit_reason = (
                f"Canonical unit mismatch: expected {series.canonical_unit_code}, got {sorted(set(mismatches))!r}."
            )

    date_convention_ok = True
    date_convention_reason = None
    if latest is not None and str(series.frequency).lower() == "weekly":
        expected_weekday = _week_ending_weekday(series)
        latest_weekday = latest.normalized_period_end_at.date().weekday()
        if expected_weekday is not None and latest_weekday != expected_weekday:
            date_convention_ok = False
            date_convention_reason = (
                f"Expected week-ending weekday {expected_weekday}, found {latest_weekday} "
                f"on {latest.normalized_period_end_at.date().isoformat()}."
            )
        elif latest.normalized_period_start_at.date() != latest.normalized_period_end_at.date() - timedelta(days=6):
            date_convention_ok = False
            date_convention_reason = "Weekly periods must span seven calendar days ending on the published week-ending date."

    yoy_abs = (latest.value_canonical - yoy.value_canonical) if latest and yoy else None
    yoy_pct = _safe_pct_change(latest.value_canonical if latest else None, yoy.value_canonical if yoy else None)
    change_abs = (latest.value_canonical - prior.value_canonical) if latest and prior else None
    change_pct = _safe_pct_change(latest.value_canonical if latest else None, prior.value_canonical if prior else None)

    surprise_flag = False
    surprise_direction = None
    surprise_score = None
    surprise_reason = None
    if latest is not None:
        if series.code in PETROLEUM_SURPRISE_CODES and yoy_abs is not None and abs(yoy_abs) >= 500:
            surprise_flag = True
            surprise_direction = "positive" if yoy_abs > 0 else "negative"
            surprise_score = abs(yoy_abs) / 500.0
            surprise_reason = f"{abs(yoy_abs):,.0f} {latest.unit_canonical_code} versus year-ago exceeds the 500 kb/d design threshold."
        elif series.code in WASDE_REVISION_SURPRISE_CODES and change_abs is not None and abs(change_abs) >= 50:
            surprise_flag = True
            surprise_direction = "positive" if change_abs > 0 else "negative"
            surprise_score = abs(change_abs) / 50.0
            surprise_reason = (
                f"{abs(change_abs):,.1f} {latest.unit_canonical_code} versus prior release exceeds the 50 million-bushel revision threshold."
            )
        else:
            history_window = _history_window_for_surprise(series.frequency)
            baseline_values: list[float] = []
            current_baseline = None
            if yoy_pct is not None:
                comparable_yoy: list[float] = []
                for point in latest_points[:-1]:
                    prior_year_point = _previous_year_observation(series, latest_points, point)
                    point_yoy_pct = _safe_pct_change(point.value_canonical, prior_year_point.value_canonical if prior_year_point else None)
                    if point_yoy_pct is not None:
                        comparable_yoy.append(point_yoy_pct)
                baseline_values = comparable_yoy[-history_window:]
                current_baseline = yoy_pct
            elif change_abs is not None:
                comparable_changes: list[float] = []
                for index in range(1, len(latest_points) - 1):
                    comparable_changes.append(latest_points[index].value_canonical - latest_points[index - 1].value_canonical)
                baseline_values = comparable_changes[-history_window:]
                current_baseline = change_abs

            if current_baseline is not None:
                surprise_score, surprise_flag = _surprise_baseline(baseline_values, current_baseline)
                if surprise_flag and surprise_score is not None:
                    surprise_direction = "positive" if current_baseline > 0 else "negative"
                    surprise_reason = f"Latest reading is {abs(surprise_score):.2f} standard deviations from its recent trend."

    return DemandLatestMetrics(
        series_id=series.id,
        latest_observation_id=latest.id if latest else None,
        latest_period_start_at=latest.normalized_period_start_at if latest else None,
        latest_period_end_at=latest.normalized_period_end_at if latest else None,
        latest_period_label=latest.period_label if latest else None,
        latest_release_date=latest.release_date if latest else None,
        latest_vintage_at=latest.vintage_at if latest else None,
        latest_source_url=latest.source_url if latest else series.source_url,
        latest_value=latest.value_canonical if latest else None,
        unit_code=series.canonical_unit_code,
        unit_symbol=series.canonical_unit_symbol or series.canonical_unit_code,
        prior_value=prior.value_canonical if prior else None,
        change_abs=change_abs,
        change_pct=change_pct,
        yoy_value=yoy.value_canonical if yoy else None,
        yoy_abs=yoy_abs,
        yoy_pct=yoy_pct,
        moving_average_4w=moving_average_4w,
        trend_3m_abs=trend_3m_abs,
        trend_3m_pct=trend_3m_pct,
        trend_3m_direction=trend_3m_direction,
        freshness_state=freshness_state,
        stale=stale,
        stale_reason=stale_reason,
        release_age_days=release_age_days,
        period_age_days=period_age_days,
        surprise_flag=surprise_flag,
        surprise_direction=surprise_direction,
        surprise_score=surprise_score,
        surprise_reason=surprise_reason,
        observation_count=len(observations),
        latest_observation_count=len(latest_points),
        latest_revision_sequence=latest.revision_sequence if latest else None,
        vintage_count=len(observations),
        history_days=history_days,
        backfill_expected=_series_backfill_expected(series),
        backfill_complete=(not _series_backfill_expected(series)) or history_days >= MIN_BACKFILL_DAYS,
        canonical_units_ok=canonical_units_ok,
        canonical_unit_reason=canonical_unit_reason,
        date_convention_ok=date_convention_ok,
        date_convention_reason=date_convention_reason,
    )


def build_latest_metrics_map(
    series_by_id: dict[str, DemandSeriesDefinition],
    observations_by_series_id: dict[str, list[DemandObservation]],
    *,
    now: datetime | None = None,
) -> dict[str, DemandLatestMetrics]:
    return {
        series_id: compute_latest_metrics(series, observations_by_series_id.get(series_id, []), now=now)
        for series_id, series in series_by_id.items()
    }


def _audit_status_for_series(series: DemandSeriesDefinition, metrics: DemandLatestMetrics) -> tuple[str, list[str]]:
    reasons: list[str] = []
    coverage_status = str(series.coverage_status)

    if coverage_status == "blocked":
        if series.notes:
            reasons.append(series.notes)
        return "blocked", reasons

    if coverage_status in {"planned", "needs_verification"}:
        if series.notes:
            reasons.append(series.notes)
        else:
            reasons.append("Indicator is deferred pending source/legal review.")
        return "deferred", reasons

    if metrics.observation_count <= 0:
        reasons.append("No observations have been published.")
    if metrics.latest_release_date is None and metrics.latest_vintage_at is not None:
        reasons.append("Latest observation is missing a source release timestamp.")
    if metrics.stale and metrics.stale_reason:
        reasons.append(metrics.stale_reason)
    if metrics.backfill_expected and not metrics.backfill_complete:
        reasons.append(
            f"Backfill window is {metrics.history_days} days; DemandWatch MVP expects at least {MIN_BACKFILL_DAYS} days."
        )
    if not metrics.canonical_units_ok and metrics.canonical_unit_reason:
        reasons.append(metrics.canonical_unit_reason)
    if not metrics.date_convention_ok and metrics.date_convention_reason:
        reasons.append(metrics.date_convention_reason)

    return ("live" if not reasons else "partial"), reasons


def build_demandwatch_coverage_audit(
    repository: PublishedDemandRepository | "DemandStoreBundle",
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _as_utc(now) or utcnow()
    verticals = repository._verticals_by_code
    series_by_id = repository._series_by_id
    metrics_by_series_id = repository._latest_metrics_by_series_id
    if not metrics_by_series_id:
        metrics_by_series_id = build_latest_metrics_map(series_by_id, repository._observations_by_series_id, now=now)

    vertical_entries: list[dict[str, Any]] = []
    summary_counts = {"live": 0, "partial": 0, "deferred": 0, "blocked": 0}

    for vertical in sorted(verticals.values(), key=lambda item: (item.display_order, item.code)):
        grouped: dict[str, list[dict[str, Any]]] = {"live": [], "partial": [], "deferred": [], "blocked": []}
        related_series = sorted(
            (item for item in series_by_id.values() if item.vertical_code == vertical.code),
            key=lambda item: (item.display_order, item.code),
        )
        for series in related_series:
            metrics = metrics_by_series_id[series.id]
            audit_status, reasons = _audit_status_for_series(series, metrics)
            grouped[audit_status].append(
                {
                    "series_id": series.id,
                    "indicator_id": series.indicator_id,
                    "code": series.code,
                    "name": series.name,
                    "tier": series.tier,
                    "coverage_status": series.coverage_status,
                    "source_slug": series.source_slug,
                    "source_name": series.source_name,
                    "source_legal_status": series.source_legal_status,
                    "source_url": metrics.latest_source_url or series.source_url,
                    "frequency": series.frequency,
                    "commodity_code": series.commodity_code,
                    "geography_code": series.geography_code,
                    "latest_period_label": metrics.latest_period_label,
                    "latest_period_end_at": _format_timestamp(metrics.latest_period_end_at),
                    "latest_release_date": _format_timestamp(metrics.latest_release_date),
                    "latest_vintage_at": _format_timestamp(metrics.latest_vintage_at),
                    "latest_value": metrics.latest_value,
                    "unit_code": metrics.unit_code,
                    "unit_symbol": metrics.unit_symbol,
                    "yoy_pct": metrics.yoy_pct,
                    "yoy_abs": metrics.yoy_abs,
                    "moving_average_4w": metrics.moving_average_4w,
                    "trend_3m_pct": metrics.trend_3m_pct,
                    "trend_3m_direction": metrics.trend_3m_direction,
                    "freshness_state": metrics.freshness_state,
                    "stale": metrics.stale,
                    "surprise_flag": metrics.surprise_flag,
                    "surprise_reason": metrics.surprise_reason,
                    "history_days": metrics.history_days,
                    "backfill_complete": metrics.backfill_complete,
                    "observation_count": metrics.observation_count,
                    "latest_observation_count": metrics.latest_observation_count,
                    "vintage_count": metrics.vintage_count,
                    "reasons": reasons,
                }
            )
            summary_counts[audit_status] += 1

        vertical_entries.append(
            {
                "code": vertical.code,
                "name": vertical.name,
                "commodity_code": vertical.commodity_code,
                "sector": vertical.sector,
                "counts": {status: len(grouped[status]) for status in ("live", "partial", "deferred", "blocked")},
                "live": grouped["live"],
                "partial": grouped["partial"],
                "deferred": grouped["deferred"],
                "blocked": grouped["blocked"],
            }
        )

    return {
        "generated_at": now.isoformat(),
        "summary": {
            "vertical_count": len(vertical_entries),
            "series_count": len(series_by_id),
            "status_counts": summary_counts,
        },
        "verticals": vertical_entries,
    }


def demandwatch_coverage_audit_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# DemandWatch Coverage Audit",
        "",
        f"Generated at: {audit['generated_at']}",
        "",
        "## Summary",
        "",
        f"- Verticals: {audit['summary']['vertical_count']}",
        f"- Indicators: {audit['summary']['series_count']}",
        f"- Live: {audit['summary']['status_counts']['live']}",
        f"- Partial: {audit['summary']['status_counts']['partial']}",
        f"- Deferred: {audit['summary']['status_counts']['deferred']}",
        f"- Blocked: {audit['summary']['status_counts']['blocked']}",
        "",
        "## By Vertical",
        "",
        "| Vertical | Live | Partial | Deferred | Blocked |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for vertical in audit["verticals"]:
        counts = vertical["counts"]
        lines.append(
            f"| {vertical['name']} | {counts['live']} | {counts['partial']} | {counts['deferred']} | {counts['blocked']} |"
        )

    lines.extend(
        [
            "",
            "## Indicator Detail",
            "",
            "| Vertical | Status | Code | Tier | Latest | Freshness | Reasons |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for vertical in audit["verticals"]:
        for status in ("live", "partial", "deferred", "blocked"):
            for item in vertical[status]:
                reason_text = "; ".join(item["reasons"]) or "-"
                lines.append(
                    "| {vertical} | {status} | {code} | {tier} | {latest} | {freshness} | {reasons} |".format(
                        vertical=vertical["name"],
                        status=status,
                        code=item["code"],
                        tier=item["tier"],
                        latest=item["latest_period_label"] or "-",
                        freshness=item["freshness_state"],
                        reasons=reason_text,
                    )
                )
    return "\n".join(lines)


@dataclass(slots=True)
class DemandStoreBundle:
    units_by_code: dict[str, DemandUnitDefinition]
    verticals_by_code: dict[str, DemandVerticalDefinition]
    series_by_id: dict[str, DemandSeriesDefinition]
    observations_by_series_id: dict[str, list[DemandObservation]]
    latest_metrics_by_series_id: dict[str, DemandLatestMetrics]

    @property
    def _units_by_code(self) -> dict[str, DemandUnitDefinition]:
        return self.units_by_code

    @property
    def _verticals_by_code(self) -> dict[str, DemandVerticalDefinition]:
        return self.verticals_by_code

    @property
    def _series_by_id(self) -> dict[str, DemandSeriesDefinition]:
        return self.series_by_id

    @property
    def _observations_by_series_id(self) -> dict[str, list[DemandObservation]]:
        return self.observations_by_series_id

    @property
    def _latest_metrics_by_series_id(self) -> dict[str, DemandLatestMetrics]:
        return self.latest_metrics_by_series_id


def _remove_existing_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def write_published_demand_store(bundle: DemandStoreBundle, output_path: Path) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        connection = sqlite3.connect(temp_path)
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = DELETE;
                DROP TABLE IF EXISTS published_demand_meta;
                DROP TABLE IF EXISTS published_demand_units;
                DROP TABLE IF EXISTS published_demand_verticals;
                DROP TABLE IF EXISTS published_demand_series;
                DROP TABLE IF EXISTS published_demand_observations;
                DROP TABLE IF EXISTS published_demand_latest_metrics;

                CREATE TABLE published_demand_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE published_demand_units (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    symbol TEXT
                );

                CREATE TABLE published_demand_verticals (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    commodity_code TEXT NOT NULL,
                    sector TEXT NOT NULL,
                    nav_label TEXT,
                    short_label TEXT,
                    description TEXT,
                    display_order INTEGER NOT NULL,
                    active INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE published_demand_series (
                    id TEXT PRIMARY KEY,
                    indicator_id TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    vertical_code TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    coverage_status TEXT NOT NULL,
                    display_order INTEGER NOT NULL,
                    notes TEXT,
                    measure_family TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    commodity_code TEXT,
                    geography_code TEXT,
                    source_slug TEXT NOT NULL,
                    source_name TEXT,
                    source_legal_status TEXT,
                    source_url TEXT,
                    source_series_key TEXT,
                    native_unit_code TEXT,
                    native_unit_symbol TEXT,
                    canonical_unit_code TEXT,
                    canonical_unit_symbol TEXT,
                    default_observation_kind TEXT NOT NULL,
                    visibility_tier TEXT NOT NULL,
                    active INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE published_demand_observations (
                    id TEXT PRIMARY KEY,
                    series_id TEXT NOT NULL,
                    period_start_at TEXT NOT NULL,
                    period_end_at TEXT NOT NULL,
                    normalized_period_start_at TEXT NOT NULL,
                    normalized_period_end_at TEXT NOT NULL,
                    period_label TEXT NOT NULL,
                    release_date TEXT,
                    vintage_at TEXT NOT NULL,
                    value_native REAL NOT NULL,
                    unit_native_code TEXT NOT NULL,
                    value_canonical REAL NOT NULL,
                    unit_canonical_code TEXT NOT NULL,
                    observation_kind TEXT NOT NULL,
                    revision_sequence INTEGER NOT NULL,
                    is_latest INTEGER NOT NULL,
                    source_release_id TEXT,
                    source_url TEXT,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE published_demand_latest_metrics (
                    series_id TEXT PRIMARY KEY,
                    latest_observation_id TEXT,
                    latest_period_start_at TEXT,
                    latest_period_end_at TEXT,
                    latest_period_label TEXT,
                    latest_release_date TEXT,
                    latest_vintage_at TEXT,
                    latest_source_url TEXT,
                    latest_value REAL,
                    unit_code TEXT,
                    unit_symbol TEXT,
                    prior_value REAL,
                    change_abs REAL,
                    change_pct REAL,
                    yoy_value REAL,
                    yoy_abs REAL,
                    yoy_pct REAL,
                    moving_average_4w REAL,
                    trend_3m_abs REAL,
                    trend_3m_pct REAL,
                    trend_3m_direction TEXT NOT NULL,
                    freshness_state TEXT NOT NULL,
                    stale INTEGER NOT NULL,
                    stale_reason TEXT,
                    release_age_days INTEGER,
                    period_age_days INTEGER,
                    surprise_flag INTEGER NOT NULL,
                    surprise_direction TEXT,
                    surprise_score REAL,
                    surprise_reason TEXT,
                    observation_count INTEGER NOT NULL,
                    latest_observation_count INTEGER NOT NULL,
                    latest_revision_sequence INTEGER,
                    vintage_count INTEGER NOT NULL,
                    history_days INTEGER NOT NULL,
                    backfill_expected INTEGER NOT NULL,
                    backfill_complete INTEGER NOT NULL,
                    canonical_units_ok INTEGER NOT NULL,
                    canonical_unit_reason TEXT,
                    date_convention_ok INTEGER NOT NULL,
                    date_convention_reason TEXT
                );
                """
            )

            published_at = max(
                (
                    metrics.latest_vintage_at
                    for metrics in bundle.latest_metrics_by_series_id.values()
                    if metrics.latest_vintage_at is not None
                ),
                default=utcnow(),
            )
            connection.executemany(
                "INSERT INTO published_demand_meta (key, value) VALUES (?, ?)",
                [
                    ("schema_version", str(SCHEMA_VERSION)),
                    ("published_at", published_at.isoformat()),
                    ("series_count", str(len(bundle.series_by_id))),
                ],
            )

            connection.executemany(
                "INSERT INTO published_demand_units (code, name, symbol) VALUES (?, ?, ?)",
                [
                    (
                        unit.code,
                        unit.name,
                        unit.symbol,
                    )
                    for unit in sorted(bundle.units_by_code.values(), key=lambda item: item.code)
                ],
            )

            connection.executemany(
                """
                INSERT INTO published_demand_verticals (
                    code,
                    name,
                    commodity_code,
                    sector,
                    nav_label,
                    short_label,
                    description,
                    display_order,
                    active,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        vertical.code,
                        vertical.name,
                        vertical.commodity_code,
                        vertical.sector,
                        vertical.nav_label,
                        vertical.short_label,
                        vertical.description,
                        vertical.display_order,
                        1 if vertical.active else 0,
                        json.dumps(vertical.metadata, sort_keys=True),
                    )
                    for vertical in sorted(bundle.verticals_by_code.values(), key=lambda item: (item.display_order, item.code))
                ],
            )

            connection.executemany(
                """
                INSERT INTO published_demand_series (
                    id,
                    indicator_id,
                    code,
                    name,
                    description,
                    vertical_code,
                    tier,
                    coverage_status,
                    display_order,
                    notes,
                    measure_family,
                    frequency,
                    commodity_code,
                    geography_code,
                    source_slug,
                    source_name,
                    source_legal_status,
                    source_url,
                    source_series_key,
                    native_unit_code,
                    native_unit_symbol,
                    canonical_unit_code,
                    canonical_unit_symbol,
                    default_observation_kind,
                    visibility_tier,
                    active,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        series.id,
                        series.indicator_id,
                        series.code,
                        series.name,
                        series.description,
                        series.vertical_code,
                        series.tier,
                        series.coverage_status,
                        series.display_order,
                        series.notes,
                        series.measure_family,
                        series.frequency,
                        series.commodity_code,
                        series.geography_code,
                        series.source_slug,
                        series.source_name,
                        series.source_legal_status,
                        series.source_url,
                        series.source_series_key,
                        series.native_unit_code,
                        series.native_unit_symbol,
                        series.canonical_unit_code,
                        series.canonical_unit_symbol,
                        series.default_observation_kind,
                        series.visibility_tier,
                        1 if series.active else 0,
                        json.dumps(series.metadata, sort_keys=True),
                    )
                    for series in sorted(
                        bundle.series_by_id.values(),
                        key=lambda item: (item.vertical_code, item.display_order, item.code),
                    )
                ],
            )

            observation_rows = []
            for series_id, observations in sorted(bundle.observations_by_series_id.items()):
                del series_id
                for observation in observations:
                    observation_rows.append(
                        (
                            observation.id,
                            observation.series_id,
                            observation.period_start_at.isoformat(),
                            observation.period_end_at.isoformat(),
                            observation.normalized_period_start_at.isoformat(),
                            observation.normalized_period_end_at.isoformat(),
                            observation.period_label,
                            observation.release_date.isoformat() if observation.release_date else None,
                            observation.vintage_at.isoformat(),
                            observation.value_native,
                            observation.unit_native_code,
                            observation.value_canonical,
                            observation.unit_canonical_code,
                            observation.observation_kind,
                            observation.revision_sequence,
                            1 if observation.is_latest else 0,
                            observation.source_release_id,
                            observation.source_url,
                            json.dumps(observation.metadata, sort_keys=True),
                        )
                    )
            connection.executemany(
                """
                INSERT INTO published_demand_observations (
                    id,
                    series_id,
                    period_start_at,
                    period_end_at,
                    normalized_period_start_at,
                    normalized_period_end_at,
                    period_label,
                    release_date,
                    vintage_at,
                    value_native,
                    unit_native_code,
                    value_canonical,
                    unit_canonical_code,
                    observation_kind,
                    revision_sequence,
                    is_latest,
                    source_release_id,
                    source_url,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                observation_rows,
            )

            connection.executemany(
                """
                INSERT INTO published_demand_latest_metrics (
                    series_id,
                    latest_observation_id,
                    latest_period_start_at,
                    latest_period_end_at,
                    latest_period_label,
                    latest_release_date,
                    latest_vintage_at,
                    latest_source_url,
                    latest_value,
                    unit_code,
                    unit_symbol,
                    prior_value,
                    change_abs,
                    change_pct,
                    yoy_value,
                    yoy_abs,
                    yoy_pct,
                    moving_average_4w,
                    trend_3m_abs,
                    trend_3m_pct,
                    trend_3m_direction,
                    freshness_state,
                    stale,
                    stale_reason,
                    release_age_days,
                    period_age_days,
                    surprise_flag,
                    surprise_direction,
                    surprise_score,
                    surprise_reason,
                    observation_count,
                    latest_observation_count,
                    latest_revision_sequence,
                    vintage_count,
                    history_days,
                    backfill_expected,
                    backfill_complete,
                    canonical_units_ok,
                    canonical_unit_reason,
                    date_convention_ok,
                    date_convention_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        metrics.series_id,
                        metrics.latest_observation_id,
                        _format_timestamp(metrics.latest_period_start_at),
                        _format_timestamp(metrics.latest_period_end_at),
                        metrics.latest_period_label,
                        _format_timestamp(metrics.latest_release_date),
                        _format_timestamp(metrics.latest_vintage_at),
                        metrics.latest_source_url,
                        metrics.latest_value,
                        metrics.unit_code,
                        metrics.unit_symbol,
                        metrics.prior_value,
                        metrics.change_abs,
                        metrics.change_pct,
                        metrics.yoy_value,
                        metrics.yoy_abs,
                        metrics.yoy_pct,
                        metrics.moving_average_4w,
                        metrics.trend_3m_abs,
                        metrics.trend_3m_pct,
                        metrics.trend_3m_direction,
                        metrics.freshness_state,
                        1 if metrics.stale else 0,
                        metrics.stale_reason,
                        metrics.release_age_days,
                        metrics.period_age_days,
                        1 if metrics.surprise_flag else 0,
                        metrics.surprise_direction,
                        metrics.surprise_score,
                        metrics.surprise_reason,
                        metrics.observation_count,
                        metrics.latest_observation_count,
                        metrics.latest_revision_sequence,
                        metrics.vintage_count,
                        metrics.history_days,
                        1 if metrics.backfill_expected else 0,
                        1 if metrics.backfill_complete else 0,
                        1 if metrics.canonical_units_ok else 0,
                        metrics.canonical_unit_reason,
                        1 if metrics.date_convention_ok else 0,
                        metrics.date_convention_reason,
                    )
                    for metrics in sorted(bundle.latest_metrics_by_series_id.values(), key=lambda item: item.series_id)
                ],
            )
            connection.commit()
        finally:
            connection.close()

        _remove_existing_file(output_path)
        os.replace(temp_path, output_path)
    except Exception:
        _remove_existing_file(temp_path)
        raise

    return {
        "database_path": str(output_path),
        "series_count": len(bundle.series_by_id),
        "observation_count": sum(len(points) for points in bundle.observations_by_series_id.values()),
        "vertical_count": len(bundle.verticals_by_code),
    }
