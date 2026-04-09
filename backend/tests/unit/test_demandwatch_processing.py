from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.modules.demandwatch.presentation import DemandReleaseSchedule
from app.modules.demandwatch.published_store import (
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
    build_latest_metrics_map,
    build_observation,
    compute_latest_metrics,
    write_published_demand_store,
)
from app.processing import demandwatch as demandwatch_processing
from app.processing.demandwatch import DemandWatchSetupError


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


def _public_bundle(*, latest_value: float, latest_vintage_at: datetime) -> DemandStoreBundle:
    series = _series(
        code="EIA_US_TOTAL_PRODUCT_SUPPLIED",
        frequency="weekly",
        source_slug="eia",
    )
    observations = {
        series.id: [
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
                observation_id="2026-03-27",
                period_start_at=datetime(2026, 3, 21, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
                release_date=latest_vintage_at,
                vintage_at=latest_vintage_at,
                value_native=latest_value,
                unit_native_code="kb_d",
                value_canonical=latest_value,
                unit_canonical_code="kb_d",
                observation_kind="actual",
                revision_sequence=2,
                is_latest=True,
                source_release_id="release-2026-04-01",
                source_url="https://example.com/eia/2026-04-01",
                metadata={},
            ),
        ]
    }
    return DemandStoreBundle(
        units_by_code={
            "kb_d": DemandUnitDefinition(code="kb_d", name="Thousand Barrels per Day", symbol="kb/d"),
        },
        verticals_by_code={
            "crude_products": DemandVerticalDefinition(
                code="crude_products",
                name="Crude Oil + Refined Products",
                commodity_code="crude_products",
                sector="energy",
                nav_label="Crude",
                short_label="Crude + Products",
                description=None,
                display_order=10,
                active=True,
                metadata={},
            )
        },
        series_by_id={series.id: series},
        observations_by_series_id=observations,
        latest_metrics_by_series_id=build_latest_metrics_map(
            {series.id: series},
            observations,
            now=latest_vintage_at,
        ),
    )


def _public_schedules() -> list[DemandReleaseSchedule]:
    return [
        DemandReleaseSchedule(
            release_slug="demand_eia_wpsr",
            release_name="DemandWatch EIA Weekly Petroleum Status Report",
            source_slug="eia",
            source_name="EIA",
            cadence="weekly",
            schedule_timezone="America/New_York",
            schedule_rule="FREQ=WEEKLY;BYDAY=WE;BYHOUR=10;BYMINUTE=30",
            default_local_time=time(10, 30),
            is_calendar_driven=False,
            source_url="https://www.eia.gov/petroleum/supply/weekly/",
            latest_release_at=datetime(2026, 4, 1, 14, 30, tzinfo=UTC),
            vertical_codes=("crude_products",),
            series_codes=("EIA_US_TOTAL_PRODUCT_SUPPLIED",),
        )
    ]


class _FakeSnapshotSession:
    def __init__(self) -> None:
        self.merged = []

    async def merge(self, value) -> None:
        self.merged.append(value)

    async def flush(self) -> None:
        return None


class _FakeCacheLookupResult:
    def __init__(self, cached) -> None:
        self._cached = cached

    def scalar_one_or_none(self):
        return self._cached


class _FakeCacheLookupSession:
    def __init__(self, cached) -> None:
        self.cached = cached

    async def execute(self, _query) -> _FakeCacheLookupResult:
        return _FakeCacheLookupResult(self.cached)


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


def test_load_demandwatch_public_read_model_reloads_replaced_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demandwatch" / "published.sqlite"
    initial_generated_at = datetime(2026, 4, 1, 14, 30, tzinfo=UTC)
    refreshed_generated_at = datetime(2026, 4, 8, 14, 30, tzinfo=UTC)

    write_published_demand_store(
        _public_bundle(latest_value=9600.0, latest_vintage_at=initial_generated_at),
        output_path,
    )
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)
    monkeypatch.setattr(demandwatch_processing, "utcnow", lambda: refreshed_generated_at)
    demandwatch_processing.clear_demandwatch_public_read_model_cache()

    initial_model = demandwatch_processing.load_demandwatch_public_read_model()
    assert initial_model.generated_at == initial_generated_at
    assert initial_model.bundle.latest_metrics_by_series_id["eia_us_total_product_supplied-series"].latest_value == 9600.0

    write_published_demand_store(
        _public_bundle(latest_value=9800.0, latest_vintage_at=refreshed_generated_at),
        output_path,
    )

    refreshed_model = demandwatch_processing.load_demandwatch_public_read_model()
    assert refreshed_model.generated_at == refreshed_generated_at
    assert refreshed_model.bundle.latest_metrics_by_series_id["eia_us_total_product_supplied-series"].latest_value == 9800.0


@pytest.mark.asyncio
async def test_recompute_demandwatch_snapshot_uses_published_generated_at(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demandwatch" / "published.sqlite"
    published_at = datetime(2026, 4, 8, 14, 30, tzinfo=UTC)
    write_published_demand_store(
        _public_bundle(latest_value=9800.0, latest_vintage_at=published_at),
        output_path,
    )
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)
    monkeypatch.setattr(demandwatch_processing, "utcnow", lambda: published_at)

    async def fake_schedules(_session) -> list[DemandReleaseSchedule]:
        return _public_schedules()

    monkeypatch.setattr(demandwatch_processing, "load_demandwatch_release_schedules", fake_schedules)
    demandwatch_processing.clear_demandwatch_public_read_model_cache()
    session = _FakeSnapshotSession()

    payload = await demandwatch_processing.recompute_demandwatch_snapshot(session)

    assert payload["generated_at"] == published_at.isoformat()
    assert payload["macro_strip"]["generated_at"] == published_at.isoformat()
    assert payload["scorecard"]["generated_at"] == published_at.isoformat()
    assert payload["coverage_notes"]["generated_at"] == published_at.isoformat()
    assert payload["next_release_dates"]["generated_at"] == published_at.isoformat()
    assert payload["movers"]["items"]
    assert session.merged[0].payload["generated_at"] == published_at.isoformat()
    assert session.merged[0].expires_at > session.merged[0].as_of


def test_load_demandwatch_public_read_model_fails_cleanly_when_artifact_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demandwatch" / "missing.sqlite"
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)
    demandwatch_processing.clear_demandwatch_public_read_model_cache()

    with pytest.raises(DemandWatchSetupError, match="published store is unavailable") as exc_info:
        demandwatch_processing.load_demandwatch_public_read_model()

    assert "app.modules.demandwatch.cli publish" in str(exc_info.value)


def test_load_demandwatch_public_read_model_fails_cleanly_when_artifact_is_stale_beyond_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demandwatch" / "published.sqlite"
    published_at = datetime(2026, 4, 1, 14, 30, tzinfo=UTC)
    write_published_demand_store(
        _public_bundle(latest_value=9800.0, latest_vintage_at=published_at),
        output_path,
    )
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)
    monkeypatch.setattr(demandwatch_processing, "DEMANDWATCH_PUBLISHED_STORE_MAX_AGE", timedelta(hours=1))
    monkeypatch.setattr(demandwatch_processing, "utcnow", lambda: published_at + timedelta(hours=2))
    demandwatch_processing.clear_demandwatch_public_read_model_cache()

    with pytest.raises(DemandWatchSetupError, match="published store at .* is stale") as exc_info:
        demandwatch_processing.load_demandwatch_public_read_model()

    assert "maximum allowed age is 1h" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_demandwatch_snapshot_payload_recomputes_when_cached_snapshot_mixes_generated_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published_at = datetime(2026, 4, 8, 14, 30, tzinfo=UTC)
    bundle = _public_bundle(latest_value=9800.0, latest_vintage_at=published_at)
    consistent_payload = demandwatch_processing.build_demandwatch_snapshot_payload(
        bundle,
        _public_schedules(),
        generated_at=published_at,
        expires_at=published_at + timedelta(minutes=5),
    )
    cached_payload = {
        **consistent_payload,
        "scorecard": {
            **consistent_payload["scorecard"],
            "generated_at": (published_at - timedelta(minutes=5)).isoformat(),
        },
    }
    cached_snapshot = SimpleNamespace(
        expires_at=published_at + timedelta(minutes=5),
        payload=cached_payload,
    )
    session = _FakeCacheLookupSession(cached_snapshot)
    read_model = demandwatch_processing.DemandWatchPublicReadModel(
        bundle=bundle,
        generated_at=published_at,
        database_path=Path("/tmp/demandwatch-published.sqlite"),
    )
    recompute_calls = 0

    async def fake_assert_registry_seeded(_session) -> None:
        return None

    def fake_load_read_model():
        return read_model

    async def fake_recompute(_session) -> dict[str, object]:
        nonlocal recompute_calls
        recompute_calls += 1
        return consistent_payload

    monkeypatch.setattr(demandwatch_processing, "assert_demandwatch_registry_seeded", fake_assert_registry_seeded)
    monkeypatch.setattr(demandwatch_processing, "load_demandwatch_public_read_model", fake_load_read_model)
    monkeypatch.setattr(demandwatch_processing, "recompute_demandwatch_snapshot", fake_recompute)
    monkeypatch.setattr(demandwatch_processing, "utcnow", lambda: published_at)

    payload = await demandwatch_processing.get_demandwatch_snapshot_payload(session)

    assert payload == consistent_payload
    assert recompute_calls == 1


@pytest.mark.asyncio
async def test_recompute_demandwatch_snapshot_preserves_key_field_parity_from_published_bundle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demandwatch" / "published.sqlite"
    published_at = datetime(2026, 4, 8, 14, 30, tzinfo=UTC)
    bundle = _public_bundle(latest_value=9800.0, latest_vintage_at=published_at)
    series = bundle.series_by_id["eia_us_total_product_supplied-series"]
    metrics = bundle.latest_metrics_by_series_id[series.id]
    write_published_demand_store(bundle, output_path)
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)
    monkeypatch.setattr(demandwatch_processing, "utcnow", lambda: published_at)

    async def fake_schedules(_session) -> list[DemandReleaseSchedule]:
        return _public_schedules()

    monkeypatch.setattr(demandwatch_processing, "load_demandwatch_release_schedules", fake_schedules)
    demandwatch_processing.clear_demandwatch_public_read_model_cache()
    session = _FakeSnapshotSession()

    payload = await demandwatch_processing.recompute_demandwatch_snapshot(session)

    scorecard_item = payload["scorecard"]["items"][0]
    vertical_detail = payload["vertical_details"][0]
    detail_row = vertical_detail["sections"][0]["table_rows"][0]

    assert scorecard_item["primary_series_code"] == series.code
    assert scorecard_item["latest_value"] == metrics.latest_value
    assert scorecard_item["source_url"] == metrics.latest_source_url
    assert vertical_detail["scorecard"]["primary_series_code"] == series.code
    assert detail_row["code"] == series.code
    assert detail_row["latest_value"] == metrics.latest_value
    assert detail_row["latest_release_date"] == metrics.latest_release_date.isoformat()
    assert payload["coverage_notes"]["summary"]["series_count"] == len(bundle.series_by_id)
