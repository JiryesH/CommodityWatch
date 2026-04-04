from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.modules.demandwatch.published_store import (
    DemandSeriesDefinition,
    DemandUnitDefinition,
    build_observation,
    compute_latest_metrics,
)


def _series(
    *,
    code: str,
    frequency: str,
    source_slug: str,
    coverage_status: str = "live",
) -> DemandSeriesDefinition:
    units = {
        "kb_d": DemandUnitDefinition(code="kb_d", name="Thousand Barrels per Day", symbol="kb/d"),
        "index": DemandUnitDefinition(code="index", name="Index", symbol="index"),
    }
    unit = units["kb_d" if code.startswith("EIA_") else "index"]
    return DemandSeriesDefinition(
        id=f"{code.lower()}-series",
        indicator_id=f"{code.lower()}-indicator",
        code=code,
        name=code.replace("_", " ").title(),
        description=None,
        vertical_code="crude_products" if code.startswith("EIA_") else "base_metals",
        tier="t1_direct" if code.startswith("EIA_") else "t4_end_use",
        coverage_status=coverage_status,
        display_order=10,
        notes=None,
        measure_family="flow" if code.startswith("EIA_") else "macro",
        frequency=frequency,
        commodity_code="crude_products" if code.startswith("EIA_") else "base_metals",
        geography_code="US",
        source_slug=source_slug,
        source_name=source_slug.upper(),
        source_legal_status="public_domain",
        source_url="https://example.com/source",
        source_series_key=code,
        native_unit_code=unit.code,
        native_unit_symbol=unit.symbol,
        canonical_unit_code=unit.code,
        canonical_unit_symbol=unit.symbol,
        default_observation_kind="actual",
        visibility_tier="public",
        active=True,
        metadata={},
    )


def test_compute_weekly_demand_metrics_supports_yoy_4w_average_and_surprise_flags() -> None:
    series = _series(code="EIA_US_TOTAL_PRODUCT_SUPPLIED", frequency="weekly", source_slug="eia")
    observations = [
        build_observation(
            series,
            observation_id="2025-03-28",
            period_start_at=datetime(2025, 3, 22, tzinfo=UTC),
            period_end_at=datetime(2025, 3, 28, tzinfo=UTC),
            release_date=datetime(2025, 4, 2, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2025, 4, 2, 14, 30, tzinfo=UTC),
            value_native=9000.0,
            unit_native_code="kb_d",
            value_canonical=9000.0,
            unit_canonical_code="kb_d",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2025-04-02",
            source_url="https://example.com/eia/2025-04-02",
            metadata={},
        ),
        build_observation(
            series,
            observation_id="2026-03-06",
            period_start_at=datetime(2026, 2, 28, tzinfo=UTC),
            period_end_at=datetime(2026, 3, 6, tzinfo=UTC),
            release_date=datetime(2026, 3, 11, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 11, 14, 30, tzinfo=UTC),
            value_native=9300.0,
            unit_native_code="kb_d",
            value_canonical=9300.0,
            unit_canonical_code="kb_d",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2026-03-11",
            source_url="https://example.com/eia/2026-03-11",
            metadata={},
        ),
        build_observation(
            series,
            observation_id="2026-03-13",
            period_start_at=datetime(2026, 3, 7, tzinfo=UTC),
            period_end_at=datetime(2026, 3, 13, tzinfo=UTC),
            release_date=datetime(2026, 3, 18, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 18, 14, 30, tzinfo=UTC),
            value_native=9400.0,
            unit_native_code="kb_d",
            value_canonical=9400.0,
            unit_canonical_code="kb_d",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2026-03-18",
            source_url="https://example.com/eia/2026-03-18",
            metadata={},
        ),
        build_observation(
            series,
            observation_id="2026-03-20",
            period_start_at=datetime(2026, 3, 14, tzinfo=UTC),
            period_end_at=datetime(2026, 3, 20, tzinfo=UTC),
            release_date=datetime(2026, 3, 25, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 25, 14, 30, tzinfo=UTC),
            value_native=9500.0,
            unit_native_code="kb_d",
            value_canonical=9500.0,
            unit_canonical_code="kb_d",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2026-03-25",
            source_url="https://example.com/eia/2026-03-25",
            metadata={},
        ),
        build_observation(
            series,
            observation_id="2026-03-27",
            period_start_at=datetime(2026, 3, 21, tzinfo=UTC),
            period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
            release_date=datetime(2026, 4, 1, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 4, 1, 14, 30, tzinfo=UTC),
            value_native=9600.0,
            unit_native_code="kb_d",
            value_canonical=9600.0,
            unit_canonical_code="kb_d",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2026-04-01",
            source_url="https://example.com/eia/2026-04-01",
            metadata={},
        ),
    ]

    metrics = compute_latest_metrics(series, observations, now=datetime(2026, 4, 4, 12, 0, tzinfo=UTC))

    assert metrics.latest_period_label == "Week ending 2026-03-27"
    assert metrics.yoy_abs == 600.0
    assert round(metrics.yoy_pct or 0.0, 2) == 6.67
    assert metrics.moving_average_4w == 9450.0
    assert metrics.freshness_state == "fresh"
    assert metrics.date_convention_ok is True
    assert metrics.surprise_flag is True
    assert metrics.surprise_reason is not None


def test_compute_monthly_demand_metrics_supports_three_month_trend() -> None:
    series = _series(code="FRED_HOUST", frequency="monthly", source_slug="fred")
    observations = []
    monthly_values = [
        ("2025-09", 99.0),
        ("2025-10", 100.0),
        ("2025-11", 101.0),
        ("2025-12", 102.0),
        ("2026-01", 103.0),
        ("2026-02", 104.0),
        ("2026-03", 105.0),
    ]
    for index, (release_month, value) in enumerate(monthly_values, start=1):
        year, month = (int(part) for part in release_month.split("-", 1))
        observations.append(
            build_observation(
                series,
                observation_id=f"obs-{release_month}",
                period_start_at=datetime(year, month, 1, tzinfo=UTC),
                period_end_at=datetime(year, month, 18, 12, 30, tzinfo=UTC),
                release_date=datetime(year, month, 18, 13, 30, tzinfo=UTC),
                vintage_at=datetime(year, month, 18, 13, 30, tzinfo=UTC),
                value_native=value,
                unit_native_code="index",
                value_canonical=value,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=index,
                is_latest=True,
                source_release_id=f"release-{release_month}",
                source_url=f"https://example.com/fred/{release_month}",
                metadata={},
            )
        )
    observations.append(
        build_observation(
            series,
            observation_id="obs-2025-03",
            period_start_at=datetime(2025, 3, 1, tzinfo=UTC),
            period_end_at=datetime(2025, 3, 17, 12, 30, tzinfo=UTC),
            release_date=datetime(2025, 3, 17, 13, 30, tzinfo=UTC),
            vintage_at=datetime(2025, 3, 17, 13, 30, tzinfo=UTC),
            value_native=97.0,
            unit_native_code="index",
            value_canonical=97.0,
            unit_canonical_code="index",
            observation_kind="actual",
            revision_sequence=1,
            is_latest=True,
            source_release_id="release-2025-03",
            source_url="https://example.com/fred/2025-03",
            metadata={},
        )
    )

    metrics = compute_latest_metrics(series, observations, now=datetime(2026, 4, 4, 12, 0, tzinfo=UTC))

    assert metrics.latest_period_label == "Mar 2026"
    assert round(metrics.yoy_pct or 0.0, 2) == round(((105.0 - 97.0) / 97.0) * 100.0, 2)
    assert metrics.trend_3m_abs == 3.0
    assert round(metrics.trend_3m_pct or 0.0, 2) == round(((104.0 - 101.0) / 101.0) * 100.0, 2)
    assert metrics.trend_3m_direction == "up"
