from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from app.modules.demandwatch.operations import (
    DemandWatchSourceSpec,
    _run_source_with_retries,
    build_demandwatch_canonical_unit_audit,
    build_demandwatch_source_health,
)
from app.modules.demandwatch.published_store import (
    DemandLatestMetrics,
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
)


NOW = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)


class _FakeRunSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _series(
    *,
    series_id: str,
    code: str,
    vertical_code: str,
    release_slug: str,
    source_slug: str,
    canonical_unit_code: str,
) -> DemandSeriesDefinition:
    return DemandSeriesDefinition(
        id=series_id,
        indicator_id=f"{series_id}-indicator",
        code=code,
        name=code,
        description=None,
        vertical_code=vertical_code,
        tier="t1_direct",
        coverage_status="live",
        display_order=10,
        notes=None,
        measure_family="flow",
        frequency="weekly",
        commodity_code=vertical_code,
        geography_code="US",
        source_slug=source_slug,
        source_name=source_slug.upper(),
        source_legal_status="public_domain",
        source_url="https://example.com/source",
        source_series_key=code,
        native_unit_code=canonical_unit_code,
        native_unit_symbol=canonical_unit_code,
        canonical_unit_code=canonical_unit_code,
        canonical_unit_symbol=canonical_unit_code,
        default_observation_kind="actual",
        visibility_tier="public",
        active=True,
        metadata={"release_slug": release_slug},
    )


def _metrics(
    series_id: str,
    *,
    latest_value: float,
    unit_code: str,
    stale: bool,
    release_date: datetime,
    canonical_units_ok: bool = True,
    canonical_unit_reason: str | None = None,
) -> DemandLatestMetrics:
    return DemandLatestMetrics(
        series_id=series_id,
        latest_observation_id=f"{series_id}-obs",
        latest_period_start_at=datetime(2026, 3, 28, tzinfo=UTC),
        latest_period_end_at=datetime(2026, 4, 3, 23, 59, 59, tzinfo=UTC),
        latest_period_label="Week ending 2026-04-03",
        latest_release_date=release_date,
        latest_vintage_at=release_date,
        latest_source_url="https://example.com/release",
        latest_value=latest_value,
        unit_code=unit_code,
        unit_symbol=unit_code,
        prior_value=latest_value - 1,
        change_abs=1.0,
        change_pct=1.0,
        yoy_value=latest_value - 2,
        yoy_abs=2.0,
        yoy_pct=2.0,
        moving_average_4w=latest_value,
        trend_3m_abs=None,
        trend_3m_pct=None,
        trend_3m_direction="flat",
        freshness_state="stale" if stale else "fresh",
        stale=stale,
        stale_reason="Latest data is stale." if stale else None,
        release_age_days=120 if stale else 3,
        period_age_days=120 if stale else 5,
        surprise_flag=False,
        surprise_direction=None,
        surprise_score=None,
        surprise_reason=None,
        observation_count=12,
        latest_observation_count=12,
        latest_revision_sequence=1,
        vintage_count=12,
        history_days=365 * 4,
        backfill_expected=True,
        backfill_complete=True,
        canonical_units_ok=canonical_units_ok,
        canonical_unit_reason=canonical_unit_reason,
        date_convention_ok=True,
        date_convention_reason=None,
    )


def _bundle(*, eia_unit: str = "kb_d", eia_unit_ok: bool = True) -> DemandStoreBundle:
    units = {
        "kb_d": DemandUnitDefinition(code="kb_d", name="Thousand barrels per day", symbol="kb/d"),
        "index": DemandUnitDefinition(code="index", name="Index", symbol="index"),
        "mmt": DemandUnitDefinition(code="mmt", name="Million metric tons", symbol="mmt"),
    }
    verticals = {
        "crude_products": DemandVerticalDefinition(
            code="crude_products",
            name="Crude & Products",
            commodity_code="crude_products",
            sector="energy",
            nav_label=None,
            short_label=None,
            description=None,
            display_order=10,
            active=True,
            metadata={},
        ),
        "base_metals": DemandVerticalDefinition(
            code="base_metals",
            name="Base Metals",
            commodity_code="base_metals",
            sector="metals",
            nav_label=None,
            short_label=None,
            description=None,
            display_order=20,
            active=True,
            metadata={},
        ),
    }
    eia_series = _series(
        series_id="eia-series",
        code="EIA_US_TOTAL_PRODUCT_SUPPLIED",
        vertical_code="crude_products",
        release_slug="demand_eia_wpsr",
        source_slug="eia",
        canonical_unit_code=eia_unit,
    )
    fred_series = _series(
        series_id="fred-series",
        code="FRED_US_INDUSTRIAL_PRODUCTION",
        vertical_code="base_metals",
        release_slug="demand_fred_g17",
        source_slug="fred",
        canonical_unit_code="index",
    )
    metrics = {
        eia_series.id: _metrics(
            eia_series.id,
            latest_value=9_400.0,
            unit_code=eia_unit,
            stale=True,
            release_date=datetime(2025, 11, 1, tzinfo=UTC),
            canonical_units_ok=eia_unit_ok,
            canonical_unit_reason=None if eia_unit_ok else "Canonical unit mismatch.",
        ),
        fred_series.id: _metrics(
            fred_series.id,
            latest_value=101.0,
            unit_code="index",
            stale=False,
            release_date=datetime(2026, 4, 5, tzinfo=UTC),
        ),
    }
    return DemandStoreBundle(
        units_by_code=units,
        verticals_by_code=verticals,
        series_by_id={eia_series.id: eia_series, fred_series.id: fred_series},
        observations_by_series_id={},
        latest_metrics_by_series_id=metrics,
    )


def test_source_health_flags_stale_failed_feeds_and_preserves_healthy_feeds() -> None:
    bundle = _bundle()
    runs = [
        SimpleNamespace(
            job_name="demand_eia_wpsr",
            status="failed",
            finished_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
            error_text="Bad payload",
            metadata_={"failure_category": "parse_error"},
        ),
        SimpleNamespace(
            job_name="demand_fred_g17",
            status="success",
            finished_at=datetime(2026, 4, 8, 9, 0, tzinfo=UTC),
            error_text=None,
            metadata_={},
        ),
    ]

    health = build_demandwatch_source_health(bundle, runs, now=NOW)
    items = {item["name"]: item for item in health["items"]}

    assert items["demand_eia_wpsr"]["status"] == "failing"
    assert items["demand_eia_wpsr"]["parse_failures_30d"] == 1
    assert items["demand_fred_g17"]["status"] == "healthy"


def test_canonical_unit_audit_flags_series_that_break_explicit_policy() -> None:
    audit = build_demandwatch_canonical_unit_audit(_bundle(eia_unit="mmt", eia_unit_ok=False))
    eia_item = next(item for item in audit["items"] if item["code"] == "EIA_US_TOTAL_PRODUCT_SUPPLIED")

    assert audit["summary"]["violations"] == 1
    assert eia_item["status"] == "violation"
    assert any("Expected canonical unit kb_d" in reason for reason in eia_item["reasons"])


@pytest.mark.asyncio
async def test_run_source_with_retries_retries_retryable_failures_and_switches_run_mode(monkeypatch) -> None:
    request = httpx.Request("GET", "https://example.com/feed")
    retryable_error = httpx.HTTPStatusError(
        "temporary upstream failure",
        request=request,
        response=httpx.Response(503, request=request),
    )
    results = iter(
        [
            retryable_error,
            SimpleNamespace(fetched_items=3, inserted_rows=2, updated_rows=1, quarantined_rows=0),
        ]
    )
    runner_calls: list[str] = []
    recorded_failures: list[dict[str, object]] = []
    sleep_calls: list[float] = []

    async def runner(_session, *, run_mode, start_date, end_date):
        runner_calls.append(run_mode)
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    async def fake_record_failed_ingest_run(_session, spec, **kwargs) -> None:
        recorded_failures.append(
            {
                "spec_name": spec.name,
                "failure_category": kwargs["failure_context"]["failure_category"],
                "run_mode": kwargs["run_mode"],
            }
        )

    async def fake_latest_job_run(_session, _job_name):
        return None

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(
        "app.modules.demandwatch.operations._record_failed_ingest_run",
        fake_record_failed_ingest_run,
    )
    monkeypatch.setattr(
        "app.modules.demandwatch.operations._latest_job_run",
        fake_latest_job_run,
    )

    outcome = await _run_source_with_retries(
        _FakeRunSession(),
        DemandWatchSourceSpec(
            name="demand_fred_g17",
            source_slug="fred",
            release_slug="demand_fred_g17",
            display_name="FRED G.17",
            runner=runner,
        ),
        operation="refresh",
        run_mode="manual",
        from_date=None,
        to_date=None,
        max_attempts=3,
        manifest={"signature": "test-manifest"},
        sleep_fn=fake_sleep,
    )

    assert outcome.status == "success"
    assert outcome.attempts == 2
    assert outcome.fetched_items == 3
    assert outcome.inserted_rows == 2
    assert outcome.updated_rows == 1
    assert runner_calls == ["manual", "retry"]
    assert recorded_failures == [
        {
            "spec_name": "demand_fred_g17",
            "failure_category": "http_error",
            "run_mode": "manual",
        }
    ]
    assert sleep_calls == [1]


@pytest.mark.asyncio
async def test_run_source_with_retries_marks_partial_success_when_latest_run_is_partial(monkeypatch) -> None:
    session = _FakeRunSession()

    async def runner(_session, *, run_mode, start_date, end_date):
        return SimpleNamespace(fetched_items=2, inserted_rows=1, updated_rows=0, quarantined_rows=0)

    async def fake_latest_job_run(_session, _job_name):
        return SimpleNamespace(status="partial")

    async def unexpected_record_failed_ingest_run(*_args, **_kwargs) -> None:
        raise AssertionError("failed ingest runs should not be recorded for partial-success outcomes")

    monkeypatch.setattr(
        "app.modules.demandwatch.operations._latest_job_run",
        fake_latest_job_run,
    )
    monkeypatch.setattr(
        "app.modules.demandwatch.operations._record_failed_ingest_run",
        unexpected_record_failed_ingest_run,
    )

    outcome = await _run_source_with_retries(
        session,
        DemandWatchSourceSpec(
            name="demand_usda_export_sales",
            source_slug="usda_export_sales",
            release_slug="demand_usda_export_sales",
            display_name="USDA Export Sales",
            runner=runner,
        ),
        operation="refresh",
        run_mode="manual",
        from_date=None,
        to_date=None,
        max_attempts=2,
        manifest={"signature": "test-manifest"},
    )

    assert outcome.status == "partial"
    assert outcome.attempts == 1
    assert outcome.fetched_items == 2
    assert outcome.inserted_rows == 1
    assert session.commits == 1
    assert session.rollbacks == 0
