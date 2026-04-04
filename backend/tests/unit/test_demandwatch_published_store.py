from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.modules.demandwatch.published_store import (
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
    PublishedDemandRepository,
    build_demandwatch_coverage_audit,
    build_latest_metrics_map,
    build_observation,
    demandwatch_coverage_audit_markdown,
    write_published_demand_store,
)


def _bundle() -> DemandStoreBundle:
    units = {
        "kb_d": DemandUnitDefinition(code="kb_d", name="Thousand Barrels per Day", symbol="kb/d"),
        "index": DemandUnitDefinition(code="index", name="Index", symbol="index"),
    }
    verticals = {
        "crude_products": DemandVerticalDefinition(
            code="crude_products",
            name="Crude Oil + Refined Products",
            commodity_code="crude_products",
            sector="energy",
            nav_label="Crude",
            short_label="Crude",
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
            nav_label="Metals",
            short_label="Metals",
            description=None,
            display_order=20,
            active=True,
            metadata={},
        ),
    }
    series = {
        "live-series": DemandSeriesDefinition(
            id="live-series",
            indicator_id="live-indicator",
            code="EIA_US_TOTAL_PRODUCT_SUPPLIED",
            name="EIA US Total Product Supplied",
            description=None,
            vertical_code="crude_products",
            tier="t1_direct",
            coverage_status="live",
            display_order=10,
            notes=None,
            measure_family="flow",
            frequency="weekly",
            commodity_code="crude_products",
            geography_code="US",
            source_slug="eia",
            source_name="EIA",
            source_legal_status="public_domain",
            source_url="https://example.com/eia",
            source_series_key="PET.WRPUPUS2.W",
            native_unit_code="kb_d",
            native_unit_symbol="kb/d",
            canonical_unit_code="kb_d",
            canonical_unit_symbol="kb/d",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "partial-series": DemandSeriesDefinition(
            id="partial-series",
            indicator_id="partial-indicator",
            code="FRED_HOUST",
            name="FRED Housing Starts",
            description=None,
            vertical_code="base_metals",
            tier="t4_end_use",
            coverage_status="live",
            display_order=10,
            notes=None,
            measure_family="macro",
            frequency="monthly",
            commodity_code="base_metals",
            geography_code="US",
            source_slug="fred",
            source_name="FRED",
            source_legal_status="public_domain",
            source_url="https://example.com/fred",
            source_series_key="HOUST",
            native_unit_code="index",
            native_unit_symbol="index",
            canonical_unit_code="index",
            canonical_unit_symbol="index",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "deferred-series": DemandSeriesDefinition(
            id="deferred-series",
            indicator_id="deferred-indicator",
            code="CHINA_CRUDE_IMPORTS_MONTHLY",
            name="China Crude Imports",
            description=None,
            vertical_code="crude_products",
            tier="t3_trade",
            coverage_status="needs_verification",
            display_order=90,
            notes="Direct republication terms are unresolved.",
            measure_family="flow",
            frequency="monthly",
            commodity_code="crude_oil",
            geography_code="CHINA",
            source_slug="china_customs",
            source_name="China Customs",
            source_legal_status="needs_verification",
            source_url=None,
            source_series_key=None,
            native_unit_code="index",
            native_unit_symbol="index",
            canonical_unit_code="index",
            canonical_unit_symbol="index",
            default_observation_kind="actual",
            visibility_tier="internal",
            active=False,
            metadata={},
        ),
        "blocked-series": DemandSeriesDefinition(
            id="blocked-series",
            indicator_id="blocked-indicator",
            code="SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW",
            name="S&P Global Eurozone PMI Raw",
            description=None,
            vertical_code="base_metals",
            tier="t6_macro",
            coverage_status="blocked",
            display_order=100,
            notes="Raw PMI values are off-limits without a licence.",
            measure_family="signal",
            frequency="monthly",
            commodity_code="base_metals",
            geography_code="EU",
            source_slug="spglobal_pmi",
            source_name="S&P Global",
            source_legal_status="off_limits",
            source_url=None,
            source_series_key=None,
            native_unit_code="index",
            native_unit_symbol="index",
            canonical_unit_code="index",
            canonical_unit_symbol="index",
            default_observation_kind="signal",
            visibility_tier="internal",
            active=False,
            metadata={},
        ),
    }
    observations = {
        "live-series": [
            build_observation(
                series["live-series"],
                observation_id="2023-03-24",
                period_start_at=datetime(2023, 3, 18, tzinfo=UTC),
                period_end_at=datetime(2023, 3, 24, tzinfo=UTC),
                release_date=datetime(2023, 3, 29, 14, 30, tzinfo=UTC),
                vintage_at=datetime(2023, 3, 29, 14, 30, tzinfo=UTC),
                value_native=9000.0,
                unit_native_code="kb_d",
                value_canonical=9000.0,
                unit_canonical_code="kb_d",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="release-2023-03-29",
                source_url="https://example.com/eia/2023-03-29",
                metadata={},
            ),
            build_observation(
                series["live-series"],
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
        ],
        "partial-series": [
            build_observation(
                series["partial-series"],
                observation_id="2026-03",
                period_start_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 18, 12, 30, tzinfo=UTC),
                release_date=datetime(2026, 3, 18, 13, 30, tzinfo=UTC),
                vintage_at=datetime(2026, 3, 18, 13, 30, tzinfo=UTC),
                value_native=105.0,
                unit_native_code="index",
                value_canonical=105.0,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="release-2026-03",
                source_url="https://example.com/fred/2026-03",
                metadata={},
            ),
        ],
    }
    latest_metrics = build_latest_metrics_map(series, observations, now=datetime(2026, 4, 4, 12, 0, tzinfo=UTC))
    return DemandStoreBundle(
        units_by_code=units,
        verticals_by_code=verticals,
        series_by_id=series,
        observations_by_series_id=observations,
        latest_metrics_by_series_id=latest_metrics,
    )


def test_demandwatch_coverage_audit_groups_series_by_status() -> None:
    audit = build_demandwatch_coverage_audit(_bundle(), now=datetime(2026, 4, 4, 12, 0, tzinfo=UTC))

    assert audit["summary"]["status_counts"] == {
        "live": 1,
        "partial": 1,
        "deferred": 1,
        "blocked": 1,
    }
    crude = next(item for item in audit["verticals"] if item["code"] == "crude_products")
    metals = next(item for item in audit["verticals"] if item["code"] == "base_metals")

    assert crude["counts"]["live"] == 1
    assert crude["counts"]["deferred"] == 1
    assert metals["counts"]["partial"] == 1
    assert metals["counts"]["blocked"] == 1
    assert metals["partial"][0]["reasons"]

    markdown = demandwatch_coverage_audit_markdown(audit)
    assert "# DemandWatch Coverage Audit" in markdown
    assert "SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW" in markdown


def test_demandwatch_published_store_round_trips(tmp_path: Path) -> None:
    bundle = _bundle()
    output_path = tmp_path / "demandwatch.db"

    summary = write_published_demand_store(bundle, output_path)
    repository = PublishedDemandRepository(output_path)

    assert summary["series_count"] == 4
    assert len(repository._series_by_id) == 4
    assert sum(len(points) for points in repository._observations_by_series_id.values()) == 3
    live_metrics = repository._latest_metrics_by_series_id["live-series"]
    assert live_metrics.latest_period_label == "Week ending 2026-03-27"
    assert live_metrics.backfill_complete is True
