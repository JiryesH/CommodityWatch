from __future__ import annotations

from datetime import datetime

from app.schemas.common import APIModel


class DemandMacroStripItem(APIModel):
    id: str
    code: str
    label: str
    descriptor: str | None = None
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    display_value: str | None = None
    change_label: str | None = None
    trend: str
    freshness: str
    freshness_state: str
    latest_period_label: str | None = None
    latest_release_date: datetime | None = None
    source_url: str | None = None


class DemandMacroStripResponse(APIModel):
    generated_at: datetime
    items: list[DemandMacroStripItem]


class DemandScorecardItem(APIModel):
    id: str
    code: str
    label: str
    nav_label: str
    short_label: str
    sector: str
    scorecard_label: str
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    display_value: str | None = None
    yoy_value: float | None = None
    yoy_label: str | None = None
    trend: str
    latest_period_label: str | None = None
    freshness: str
    freshness_state: str
    stale: bool
    source_url: str | None = None
    primary_series_code: str


class DemandScorecardResponse(APIModel):
    generated_at: datetime
    items: list[DemandScorecardItem]


class DemandMoverItem(APIModel):
    vertical_id: str
    vertical_code: str
    code: str
    title: str
    tier: str
    tier_label: str
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    display_value: str | None = None
    change_label: str | None = None
    surprise_label: str | None = None
    trend: str
    freshness: str
    freshness_state: str
    latest_period_label: str | None = None
    latest_release_date: datetime | None = None
    source_url: str | None = None


class DemandMoversResponse(APIModel):
    generated_at: datetime
    items: list[DemandMoverItem]


class DemandFactItem(APIModel):
    label: str
    value: str
    note: str


class DemandIndicatorCard(APIModel):
    series_id: str
    indicator_id: str
    code: str
    title: str
    tier: str
    tier_label: str
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    display_value: str | None = None
    change_label: str | None = None
    detail: str | None = None
    trend: str
    sparkline: list[float]
    freshness: str
    freshness_state: str
    latest_period_label: str | None = None
    latest_release_date: datetime | None = None
    latest_vintage_at: datetime | None = None
    source_url: str | None = None
    coverage_status: str
    vintage_count: int


class DemandIndicatorTableRow(APIModel):
    series_id: str
    indicator_id: str
    code: str
    label: str
    tier: str
    tier_label: str
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    latest_display: str | None = None
    change_display: str | None = None
    yoy_display: str | None = None
    freshness: str
    freshness_state: str
    trend: str
    latest_period_label: str | None = None
    latest_release_date: datetime | None = None
    source_url: str | None = None
    vintage_count: int


class DemandVerticalSection(APIModel):
    id: str
    title: str
    description: str
    indicators: list[DemandIndicatorCard]
    table_rows: list[DemandIndicatorTableRow]


class DemandNextReleaseItem(APIModel):
    release_slug: str
    release_name: str
    source_slug: str
    source_name: str
    cadence: str
    schedule_timezone: str
    vertical_ids: list[str]
    vertical_codes: list[str]
    series_codes: list[str]
    scheduled_for: datetime | None = None
    latest_release_at: datetime | None = None
    source_url: str | None = None
    is_estimated: bool
    notes: list[str]


class DemandVerticalDetailResponse(APIModel):
    generated_at: datetime
    id: str
    code: str
    label: str
    nav_label: str
    short_label: str
    sector: str
    description: str | None = None
    summary: str
    scorecard: DemandScorecardItem
    facts: list[DemandFactItem]
    sections: list[DemandVerticalSection]
    calendar: list[DemandNextReleaseItem]
    notes: list[str]


class DemandIndicatorTableSection(APIModel):
    id: str
    title: str
    rows: list[DemandIndicatorTableRow]


class DemandIndicatorTableResponse(APIModel):
    generated_at: datetime
    vertical_id: str
    vertical_code: str
    sections: list[DemandIndicatorTableSection]


class DemandCoverageAuditSummary(APIModel):
    vertical_count: int
    series_count: int
    status_counts: dict[str, int]


class DemandCoverageAuditItem(APIModel):
    series_id: str
    indicator_id: str
    code: str
    name: str
    tier: str
    coverage_status: str
    source_slug: str
    source_name: str | None = None
    source_legal_status: str | None = None
    source_url: str | None = None
    frequency: str
    commodity_code: str | None = None
    geography_code: str | None = None
    latest_period_label: str | None = None
    latest_period_end_at: str | None = None
    latest_release_date: str | None = None
    latest_vintage_at: str | None = None
    latest_value: float | None = None
    unit_code: str | None = None
    unit_symbol: str | None = None
    yoy_pct: float | None = None
    yoy_abs: float | None = None
    moving_average_4w: float | None = None
    trend_3m_pct: float | None = None
    trend_3m_direction: str
    freshness_state: str
    stale: bool
    surprise_flag: bool
    surprise_reason: str | None = None
    history_days: int
    backfill_complete: bool
    observation_count: int
    latest_observation_count: int
    vintage_count: int
    reasons: list[str]


class DemandCoverageAuditVertical(APIModel):
    id: str
    code: str
    name: str
    commodity_code: str
    sector: str
    counts: dict[str, int]
    live: list[DemandCoverageAuditItem]
    partial: list[DemandCoverageAuditItem]
    deferred: list[DemandCoverageAuditItem]
    blocked: list[DemandCoverageAuditItem]


class DemandCoverageNotesResponse(APIModel):
    generated_at: datetime
    markdown: str
    summary: DemandCoverageAuditSummary
    verticals: list[DemandCoverageAuditVertical]


class DemandNextReleasesResponse(APIModel):
    generated_at: datetime
    items: list[DemandNextReleaseItem]


class DemandBootstrapVerticalError(APIModel):
    vertical_id: str
    message: str


class DemandBootstrapResponse(APIModel):
    module: str
    generated_at: datetime
    expires_at: datetime
    macro_strip: DemandMacroStripResponse
    scorecard: DemandScorecardResponse
    movers: DemandMoversResponse
    coverage_notes: DemandCoverageNotesResponse
    vertical_details: list[DemandVerticalDetailResponse]
    vertical_errors: list[DemandBootstrapVerticalError]
    next_release_dates: DemandNextReleasesResponse
