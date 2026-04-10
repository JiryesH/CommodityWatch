from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
import sys

from httpx import ASGITransport, AsyncClient
import pytest
import pytest_asyncio


BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.api.routers import demandwatch as demandwatch_router
from app.api.routers import snapshots as snapshots_router
from app.db.session import get_db_session
from app.main import app
from app.modules.demandwatch.presentation import DemandReleaseSchedule, build_demandwatch_bootstrap_payload
from app.modules.demandwatch.published_store import (
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
    build_latest_metrics_map,
    build_observation,
)
from app.processing.demandwatch import DemandWatchPublicReadModel, DemandWatchSetupError


def _bundle() -> DemandStoreBundle:
    units = {
        "kb_d": DemandUnitDefinition(code="kb_d", name="Thousand Barrels per Day", symbol="kb/d"),
        "gw": DemandUnitDefinition(code="gw", name="Gigawatt", symbol="GW"),
        "mbu": DemandUnitDefinition(code="mbu", name="Million Bushels", symbol="mbu"),
        "index": DemandUnitDefinition(code="index", name="Index", symbol="index"),
        "k_units_saar": DemandUnitDefinition(code="k_units_saar", name="Thousands SAAR", symbol="k"),
    }
    verticals = {
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
        ),
        "electricity": DemandVerticalDefinition(
            code="electricity",
            name="Electricity / Power",
            commodity_code="electricity",
            sector="energy",
            nav_label="Power",
            short_label="Electricity",
            description=None,
            display_order=20,
            active=True,
            metadata={},
        ),
        "grains_oilseeds": DemandVerticalDefinition(
            code="grains_oilseeds",
            name="Grains & Oilseeds",
            commodity_code="grains_oilseeds",
            sector="agriculture",
            nav_label="Grains",
            short_label="Grains",
            description=None,
            display_order=30,
            active=True,
            metadata={},
        ),
        "base_metals": DemandVerticalDefinition(
            code="base_metals",
            name="Base Metals",
            commodity_code="base_metals",
            sector="metals",
            nav_label="Metals",
            short_label="Base Metals",
            description=None,
            display_order=40,
            active=True,
            metadata={},
        ),
    }
    series = {
        "crude-live": DemandSeriesDefinition(
            id="crude-live",
            indicator_id="crude-live-indicator",
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
        "crude-throughput": DemandSeriesDefinition(
            id="crude-throughput",
            indicator_id="crude-throughput-indicator",
            code="EIA_CRUDE_US_REFINERY_INPUTS",
            name="EIA US Refinery Inputs",
            description=None,
            vertical_code="crude_products",
            tier="t2_throughput",
            coverage_status="live",
            display_order=20,
            notes=None,
            measure_family="flow",
            frequency="weekly",
            commodity_code="crude_oil",
            geography_code="US",
            source_slug="eia",
            source_name="EIA",
            source_legal_status="public_domain",
            source_url="https://example.com/eia",
            source_series_key="PET.WCRRIUS2.W",
            native_unit_code="kb_d",
            native_unit_symbol="kb/d",
            canonical_unit_code="kb_d",
            canonical_unit_symbol="kb/d",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "electricity-live": DemandSeriesDefinition(
            id="electricity-live",
            indicator_id="electricity-live-indicator",
            code="EIA_US_ELECTRICITY_GRID_LOAD",
            name="EIA US Grid Load",
            description=None,
            vertical_code="electricity",
            tier="t1_direct",
            coverage_status="live",
            display_order=10,
            notes=None,
            measure_family="capacity",
            frequency="hourly",
            commodity_code="electricity",
            geography_code="US",
            source_slug="eia",
            source_name="EIA",
            source_legal_status="public_domain",
            source_url="https://example.com/grid",
            source_series_key="EBA.US48-ALL.D.H",
            native_unit_code="gw",
            native_unit_symbol="GW",
            canonical_unit_code="gw",
            canonical_unit_symbol="GW",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "grains-live": DemandSeriesDefinition(
            id="grains-live",
            indicator_id="grains-live-indicator",
            code="USDA_US_CORN_TOTAL_USE_WASDE",
            name="USDA US Corn Total Use",
            description=None,
            vertical_code="grains_oilseeds",
            tier="t1_direct",
            coverage_status="live",
            display_order=10,
            notes=None,
            measure_family="flow",
            frequency="monthly",
            commodity_code="corn",
            geography_code="US",
            source_slug="usda_psd",
            source_name="USDA PSD",
            source_legal_status="public_domain",
            source_url="https://example.com/wasde",
            source_series_key="0440000",
            native_unit_code="mbu",
            native_unit_symbol="mbu",
            canonical_unit_code="mbu",
            canonical_unit_symbol="mbu",
            default_observation_kind="estimate",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "metals-live": DemandSeriesDefinition(
            id="metals-live",
            indicator_id="metals-live-indicator",
            code="FRED_US_INDUSTRIAL_PRODUCTION",
            name="FRED US Industrial Production",
            description=None,
            vertical_code="base_metals",
            tier="t6_macro",
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
            source_series_key="INDPRO",
            native_unit_code="index",
            native_unit_symbol="index",
            canonical_unit_code="index",
            canonical_unit_symbol="index",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "metals-end-use": DemandSeriesDefinition(
            id="metals-end-use",
            indicator_id="metals-end-use-indicator",
            code="FRED_US_HOUSING_STARTS",
            name="FRED US Housing Starts",
            description=None,
            vertical_code="base_metals",
            tier="t4_end_use",
            coverage_status="live",
            display_order=20,
            notes=None,
            measure_family="macro",
            frequency="monthly",
            commodity_code="base_metals",
            geography_code="US",
            source_slug="fred",
            source_name="FRED",
            source_legal_status="public_domain",
            source_url="https://example.com/fred/housing",
            source_series_key="HOUST",
            native_unit_code="k_units_saar",
            native_unit_symbol="k",
            canonical_unit_code="k_units_saar",
            canonical_unit_symbol="k",
            default_observation_kind="actual",
            visibility_tier="public",
            active=True,
            metadata={},
        ),
        "crude-deferred": DemandSeriesDefinition(
            id="crude-deferred",
            indicator_id="crude-deferred-indicator",
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
            native_unit_code="mbu",
            native_unit_symbol="mbu",
            canonical_unit_code="mbu",
            canonical_unit_symbol="mbu",
            default_observation_kind="actual",
            visibility_tier="internal",
            active=False,
            metadata={},
        ),
    }

    observations = {
        "crude-live": [
            build_observation(
                series["crude-live"],
                observation_id="crude-2023",
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
                source_release_id="release-2023",
                source_url="https://example.com/eia/2023",
                metadata={},
            ),
            build_observation(
                series["crude-live"],
                observation_id="crude-2025",
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
                source_release_id="release-2025",
                source_url="https://example.com/eia/2025",
                metadata={},
            ),
            build_observation(
                series["crude-live"],
                observation_id="crude-2026-03-06",
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
                series["crude-live"],
                observation_id="crude-2026-03-13",
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
                series["crude-live"],
                observation_id="crude-2026-03-20",
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
                series["crude-live"],
                observation_id="crude-2026-03-27",
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
        "crude-throughput": [
            build_observation(
                series["crude-throughput"],
                observation_id="throughput-2023",
                period_start_at=datetime(2023, 3, 18, tzinfo=UTC),
                period_end_at=datetime(2023, 3, 24, tzinfo=UTC),
                release_date=datetime(2023, 3, 29, 14, 30, tzinfo=UTC),
                vintage_at=datetime(2023, 3, 29, 14, 30, tzinfo=UTC),
                value_native=15000.0,
                unit_native_code="kb_d",
                value_canonical=15000.0,
                unit_canonical_code="kb_d",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="throughput-2023",
                source_url="https://example.com/eia/throughput/2023",
                metadata={},
            ),
            build_observation(
                series["crude-throughput"],
                observation_id="throughput-2026",
                period_start_at=datetime(2026, 3, 21, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
                release_date=datetime(2026, 4, 1, 14, 30, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 1, 14, 30, tzinfo=UTC),
                value_native=16300.0,
                unit_native_code="kb_d",
                value_canonical=16300.0,
                unit_canonical_code="kb_d",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="throughput-2026",
                source_url="https://example.com/eia/throughput/2026",
                metadata={},
            ),
        ],
        "electricity-live": [
            build_observation(
                series["electricity-live"],
                observation_id="grid-2023",
                period_start_at=datetime(2023, 4, 4, 10, 0, tzinfo=UTC),
                period_end_at=datetime(2023, 4, 4, 10, 0, tzinfo=UTC),
                release_date=datetime(2023, 4, 4, 10, 0, tzinfo=UTC),
                vintage_at=datetime(2023, 4, 4, 10, 0, tzinfo=UTC),
                value_native=360.0,
                unit_native_code="gw",
                value_canonical=360.0,
                unit_canonical_code="gw",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="grid-2023",
                source_url="https://example.com/grid/2023",
                metadata={},
            ),
            build_observation(
                series["electricity-live"],
                observation_id="grid-2026",
                period_start_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                period_end_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                release_date=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                value_native=428.0,
                unit_native_code="gw",
                value_canonical=428.0,
                unit_canonical_code="gw",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="grid-2026",
                source_url="https://example.com/grid/2026",
                metadata={},
            ),
        ],
        "grains-live": [
            build_observation(
                series["grains-live"],
                observation_id="corn-2023",
                period_start_at=datetime(2023, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2023, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2023, 4, 12, 16, 0, tzinfo=UTC),
                vintage_at=datetime(2023, 4, 12, 16, 0, tzinfo=UTC),
                value_native=14520.0,
                unit_native_code="mbu",
                value_canonical=14520.0,
                unit_canonical_code="mbu",
                observation_kind="estimate",
                revision_sequence=1,
                is_latest=True,
                source_release_id="corn-2023",
                source_url="https://example.com/wasde/2023",
                metadata={"release_month": "2023-03"},
            ),
            build_observation(
                series["grains-live"],
                observation_id="corn-2026-prior",
                period_start_at=datetime(2026, 2, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 2, 28, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2026, 3, 12, 16, 0, tzinfo=UTC),
                vintage_at=datetime(2026, 3, 12, 16, 0, tzinfo=UTC),
                value_native=14770.0,
                unit_native_code="mbu",
                value_canonical=14770.0,
                unit_canonical_code="mbu",
                observation_kind="estimate",
                revision_sequence=1,
                is_latest=True,
                source_release_id="corn-2026-prior",
                source_url="https://example.com/wasde/2026-prior",
                metadata={"release_month": "2026-02"},
            ),
            build_observation(
                series["grains-live"],
                observation_id="corn-2026",
                period_start_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2026, 4, 12, 16, 0, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 12, 16, 0, tzinfo=UTC),
                value_native=14890.0,
                unit_native_code="mbu",
                value_canonical=14890.0,
                unit_canonical_code="mbu",
                observation_kind="estimate",
                revision_sequence=2,
                is_latest=True,
                source_release_id="corn-2026",
                source_url="https://example.com/wasde/2026",
                metadata={"release_month": "2026-03"},
            ),
        ],
        "metals-live": [
            build_observation(
                series["metals-live"],
                observation_id="indpro-2023",
                period_start_at=datetime(2023, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2023, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2023, 4, 17, 13, 15, tzinfo=UTC),
                vintage_at=datetime(2023, 4, 17, 13, 15, tzinfo=UTC),
                value_native=98.0,
                unit_native_code="index",
                value_canonical=98.0,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="indpro-2023",
                source_url="https://example.com/fred/2023",
                metadata={"release_month": "2023-03"},
            ),
            build_observation(
                series["metals-live"],
                observation_id="indpro-2025",
                period_start_at=datetime(2025, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2025, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2025, 4, 17, 13, 15, tzinfo=UTC),
                vintage_at=datetime(2025, 4, 17, 13, 15, tzinfo=UTC),
                value_native=102.6,
                unit_native_code="index",
                value_canonical=102.6,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="indpro-2025",
                source_url="https://example.com/fred/2025",
                metadata={"release_month": "2025-03"},
            ),
            build_observation(
                series["metals-live"],
                observation_id="indpro-2026-initial",
                period_start_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2026, 4, 17, 13, 15, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 17, 13, 15, tzinfo=UTC),
                value_native=103.6,
                unit_native_code="index",
                value_canonical=103.6,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=False,
                source_release_id="indpro-2026-initial",
                source_url="https://example.com/fred/2026-initial",
                metadata={"release_month": "2026-03"},
            ),
            build_observation(
                series["metals-live"],
                observation_id="indpro-2026-revision",
                period_start_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2026, 4, 18, 13, 15, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 18, 13, 15, tzinfo=UTC),
                value_native=103.8,
                unit_native_code="index",
                value_canonical=103.8,
                unit_canonical_code="index",
                observation_kind="actual",
                revision_sequence=2,
                is_latest=True,
                source_release_id="indpro-2026-revision",
                source_url="https://example.com/fred/2026-revision",
                metadata={"release_month": "2026-03"},
            ),
        ],
        "metals-end-use": [
            build_observation(
                series["metals-end-use"],
                observation_id="housing-2023",
                period_start_at=datetime(2023, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2023, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2023, 4, 18, 13, 30, tzinfo=UTC),
                vintage_at=datetime(2023, 4, 18, 13, 30, tzinfo=UTC),
                value_native=1330.0,
                unit_native_code="k_units_saar",
                value_canonical=1330.0,
                unit_canonical_code="k_units_saar",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="housing-2023",
                source_url="https://example.com/housing/2023",
                metadata={"release_month": "2023-03"},
            ),
            build_observation(
                series["metals-end-use"],
                observation_id="housing-2026",
                period_start_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_end_at=datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC),
                release_date=datetime(2026, 4, 18, 13, 30, tzinfo=UTC),
                vintage_at=datetime(2026, 4, 18, 13, 30, tzinfo=UTC),
                value_native=1520.0,
                unit_native_code="k_units_saar",
                value_canonical=1520.0,
                unit_canonical_code="k_units_saar",
                observation_kind="actual",
                revision_sequence=1,
                is_latest=True,
                source_release_id="housing-2026",
                source_url="https://example.com/housing/2026",
                metadata={"release_month": "2026-03"},
            ),
        ],
    }

    latest_metrics = build_latest_metrics_map(series, observations, now=datetime(2026, 4, 20, 12, 0, tzinfo=UTC))
    return DemandStoreBundle(
        units_by_code=units,
        verticals_by_code=verticals,
        series_by_id=series,
        observations_by_series_id=observations,
        latest_metrics_by_series_id=latest_metrics,
    )


def _schedules() -> list[DemandReleaseSchedule]:
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
            latest_release_at=datetime(2026, 4, 15, 14, 30, tzinfo=UTC),
            vertical_codes=("crude_products", "grains_oilseeds"),
            series_codes=("EIA_US_TOTAL_PRODUCT_SUPPLIED", "EIA_CRUDE_US_REFINERY_INPUTS", "EIA_US_ETHANOL_PRODUCTION"),
        ),
        DemandReleaseSchedule(
            release_slug="demand_eia_grid_monitor",
            release_name="DemandWatch EIA Grid Monitor",
            source_slug="eia",
            source_name="EIA",
            cadence="daily",
            schedule_timezone="America/New_York",
            schedule_rule="FREQ=DAILY;BYHOUR=18;BYMINUTE=0",
            default_local_time=time(18, 0),
            is_calendar_driven=False,
            source_url="https://www.eia.gov/electricity/gridmonitor/",
            latest_release_at=datetime(2026, 4, 20, 10, 0, tzinfo=UTC),
            vertical_codes=("electricity",),
            series_codes=("EIA_US_ELECTRICITY_GRID_LOAD",),
        ),
        DemandReleaseSchedule(
            release_slug="demand_usda_wasde",
            release_name="DemandWatch USDA WASDE",
            source_slug="usda_psd",
            source_name="USDA PSD",
            cadence="monthly",
            schedule_timezone="America/New_York",
            schedule_rule="FREQ=MONTHLY;BYHOUR=12;BYMINUTE=0",
            default_local_time=time(12, 0),
            is_calendar_driven=True,
            source_url="https://www.usda.gov/oce/commodity/wasde",
            latest_release_at=datetime(2026, 4, 12, 16, 0, tzinfo=UTC),
            vertical_codes=("grains_oilseeds",),
            series_codes=("USDA_US_CORN_TOTAL_USE_WASDE",),
        ),
        DemandReleaseSchedule(
            release_slug="demand_fred_g17",
            release_name="DemandWatch FRED G.17 Industrial Production",
            source_slug="fred",
            source_name="FRED",
            cadence="monthly",
            schedule_timezone="America/Chicago",
            schedule_rule="FREQ=MONTHLY;BYHOUR=08;BYMINUTE=15",
            default_local_time=time(8, 15),
            is_calendar_driven=False,
            source_url="https://www.federalreserve.gov/releases/g17/",
            latest_release_at=datetime(2026, 4, 18, 13, 15, tzinfo=UTC),
            vertical_codes=("base_metals",),
            series_codes=("FRED_US_INDUSTRIAL_PRODUCTION", "FRED_US_HOUSING_STARTS"),
        ),
    ]


@pytest.fixture
def demand_bundle() -> DemandStoreBundle:
    return _bundle()


@pytest.fixture
def demand_schedules() -> list[DemandReleaseSchedule]:
    return _schedules()


@pytest_asyncio.fixture
async def contract_client(
    monkeypatch: pytest.MonkeyPatch,
    demand_bundle: DemandStoreBundle,
    demand_schedules: list[DemandReleaseSchedule],
) -> AsyncIterator[AsyncClient]:
    async def fake_snapshot(_session) -> dict:
        return build_demandwatch_bootstrap_payload(
            demand_bundle,
            demand_schedules,
            now=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            expires_at=datetime(2026, 4, 20, 12, 5, tzinfo=UTC),
        )

    async def fake_session() -> AsyncIterator[object]:
        yield object()

    monkeypatch.setattr(demandwatch_router, "get_demandwatch_snapshot_payload", fake_snapshot)
    monkeypatch.setattr(snapshots_router, "get_demandwatch_snapshot_payload", fake_snapshot)
    monkeypatch.setattr(
        demandwatch_router,
        "load_demandwatch_public_read_model",
        lambda: DemandWatchPublicReadModel(
            bundle=demand_bundle,
            generated_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            database_path=BACKEND_ROOT / "artifacts" / "demandwatch" / "published.sqlite",
        ),
    )
    app.dependency_overrides[get_db_session] = fake_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http_client:
        yield http_client
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_macro_strip_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/demandwatch/macro-strip")
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["code"] == "FRED_US_INDUSTRIAL_PRODUCTION"
    assert payload["items"][0]["source_label"] == "FRED"
    assert payload["items"][-1]["code"] == "EIA_US_ELECTRICITY_GRID_LOAD"


@pytest.mark.asyncio
async def test_scorecard_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/demandwatch/scorecard")
    assert response.status_code == 200
    payload = response.json()
    assert [item["id"] for item in payload["items"]] == [
        "crude-products",
        "electricity",
        "grains",
        "base-metals",
    ]
    assert payload["items"][0]["source_label"] == "EIA"


@pytest.mark.asyncio
async def test_vertical_detail_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/demandwatch/verticals/base-metals")
    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "base-metals"
    assert payload["sections"][0]["id"] == "macro"
    assert payload["scorecard"]["source_label"] == "FRED"
    assert payload["sections"][0]["indicators"][0]["source_label"] == "FRED"
    assert any(row["code"] == "FRED_US_INDUSTRIAL_PRODUCTION" for row in payload["sections"][0]["table_rows"])


@pytest.mark.asyncio
async def test_concept_detail_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get(
        "/api/demandwatch/verticals/base-metals/concepts/FRED_US_INDUSTRIAL_PRODUCTION"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["vertical_id"] == "base-metals"
    assert payload["code"] == "FRED_US_INDUSTRIAL_PRODUCTION"
    assert payload["title"] == "Industrial production"
    assert payload["source_label"] == "FRED"
    assert payload["cadence"] == "Monthly"
    assert payload["change_label"] == "+1.2"
    assert payload["yoy_label"] == "+1.2% YoY"
    assert payload["history"][-1]["display_value"] == "103.8"
    assert payload["observations"][0]["display_value"] == "103.8"
    assert payload["calendar"][0]["release_slug"] == "demand_fred_g17"


@pytest.mark.asyncio
async def test_vertical_detail_returns_503_when_snapshot_is_missing_a_known_vertical(
    monkeypatch: pytest.MonkeyPatch,
    demand_bundle: DemandStoreBundle,
    demand_schedules: list[DemandReleaseSchedule],
) -> None:
    async def fake_snapshot(_session) -> dict:
        payload = build_demandwatch_bootstrap_payload(
            demand_bundle,
            demand_schedules,
            now=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            expires_at=datetime(2026, 4, 20, 12, 5, tzinfo=UTC),
        )
        payload["vertical_details"] = [
            item for item in payload["vertical_details"] if item["id"] != "base-metals"
        ]
        return payload

    async def fake_session() -> AsyncIterator[object]:
        yield object()

    monkeypatch.setattr(demandwatch_router, "get_demandwatch_snapshot_payload", fake_snapshot)
    app.dependency_overrides[get_db_session] = fake_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http_client:
        response = await http_client.get("/api/demandwatch/verticals/base-metals")
        assert response.status_code == 503
        assert response.json()["detail"] == "DemandWatch vertical detail is unavailable."
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_coverage_notes_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/demandwatch/coverage-notes")
    assert response.status_code == 200
    payload = response.json()
    assert payload["markdown"].startswith("# DemandWatch Coverage Audit")
    crude = next(item for item in payload["verticals"] if item["code"] == "crude_products")
    assert crude["deferred"][0]["code"] == "CHINA_CRUDE_IMPORTS_MONTHLY"


@pytest.mark.asyncio
async def test_next_release_dates_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/demandwatch/next-release-dates", params={"vertical": "grains"})
    assert response.status_code == 200
    payload = response.json()
    assert {item["release_slug"] for item in payload["items"]} == {"demand_eia_wpsr", "demand_usda_wasde"}
    assert all("grains" in item["vertical_ids"] for item in payload["items"])


@pytest.mark.asyncio
async def test_snapshot_contract(contract_client: AsyncClient) -> None:
    response = await contract_client.get("/api/snapshot/demandwatch")
    assert response.status_code == 200
    payload = response.json()
    assert payload["module"] == "demandwatch"
    assert payload["macro_strip"]["items"][0]["code"] == "FRED_US_INDUSTRIAL_PRODUCTION"
    assert payload["macro_strip"]["items"][0]["source_label"] == "FRED"
    assert [item["id"] for item in payload["vertical_details"]] == [
        "crude-products",
        "electricity",
        "grains",
        "base-metals",
    ]
    assert payload["vertical_errors"] == []


@pytest.mark.asyncio
async def test_public_snapshot_setup_error_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    async def failing_snapshot(_session) -> dict:
        raise DemandWatchSetupError("DemandWatch published store is unavailable.")

    async def fake_session() -> AsyncIterator[object]:
        yield object()

    monkeypatch.setattr(demandwatch_router, "get_demandwatch_snapshot_payload", failing_snapshot)
    monkeypatch.setattr(snapshots_router, "get_demandwatch_snapshot_payload", failing_snapshot)
    app.dependency_overrides[get_db_session] = fake_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http_client:
        response = await http_client.get("/api/demandwatch/macro-strip")
        assert response.status_code == 503
        assert response.json()["detail"] == "DemandWatch published store is unavailable."

        snapshot_response = await http_client.get("/api/snapshot/demandwatch")
        assert snapshot_response.status_code == 503
        assert snapshot_response.json()["detail"] == "DemandWatch published store is unavailable."
    app.dependency_overrides.clear()
