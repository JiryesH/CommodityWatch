from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.common import APIModel


class IndicatorListItem(APIModel):
    id: UUID
    code: str
    name: str
    modules: list[str]
    commodity_code: str | None
    geography_code: str | None
    measure_family: str
    frequency: str
    native_unit: str | None
    canonical_unit: str | None
    is_seasonal: bool
    is_derived: bool
    visibility_tier: str
    latest_release_at: datetime | None


class IndicatorListResponse(APIModel):
    items: list[IndicatorListItem]
    next_cursor: str | None = None


class IndicatorSeriesMeta(APIModel):
    id: UUID
    code: str
    name: str
    modules: list[str]
    commodity_code: str | None
    geography_code: str | None
    frequency: str
    measure_family: str
    unit: str | None


class IndicatorSeriesPoint(APIModel):
    period_start_at: datetime
    period_end_at: datetime
    release_date: datetime | None
    vintage_at: datetime
    value: float
    unit: str
    observation_kind: str
    revision_sequence: int


class SeasonalRangePoint(APIModel):
    period_index: int
    p10: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    p90: float | None = None
    mean: float | None = None
    stddev: float | None = None


class IndicatorDataMetadata(APIModel):
    latest_release_id: UUID | None = None
    latest_release_at: datetime | None = None
    source_url: str | None = None


class IndicatorDataResponse(APIModel):
    indicator: IndicatorSeriesMeta
    series: list[IndicatorSeriesPoint]
    seasonal_range: list[SeasonalRangePoint]
    metadata: IndicatorDataMetadata


class LatestPoint(APIModel):
    period_end_at: datetime
    release_date: datetime | None
    value: float
    unit: str
    change_from_prior_abs: float | None
    change_from_prior_pct: float | None
    deviation_from_seasonal_abs: float | None
    deviation_from_seasonal_zscore: float | None
    revision_sequence: int


class IndicatorLatestResponse(APIModel):
    indicator: dict[str, UUID | str]
    latest: LatestPoint


class SnapshotCard(APIModel):
    indicator_id: UUID
    code: str
    name: str
    commodity_code: str | None
    geography_code: str | None
    latest_value: float
    unit: str
    frequency: str | None = None
    change_abs: float | None
    deviation_abs: float | None
    signal: str
    sparkline: list[float]
    last_updated_at: datetime
    stale: bool = False
    seasonal_low: float | None = None
    seasonal_high: float | None = None
    seasonal_median: float | None = None
    seasonal_samples: int | None = None
    seasonal_p10: float | None = None
    seasonal_p25: float | None = None
    seasonal_p75: float | None = None
    seasonal_p90: float | None = None


class SnapshotResponse(APIModel):
    module: str
    generated_at: datetime
    expires_at: datetime
    cards: list[SnapshotCard]
