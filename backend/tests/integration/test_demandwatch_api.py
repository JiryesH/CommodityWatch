from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta
from httpx import ASGITransport, AsyncClient
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.demand import DemandSeries, DemandVertical
from app.db.models.indicators import Indicator
from app.db.models.observations import Observation
from app.db.models.reference import Commodity, Geography, UnitDefinition
from app.db.models.sources import ReleaseDefinition, Source, SourceRelease
from app.db.session import get_db_session
from app.main import app
from app.processing import demandwatch as demandwatch_processing
from app.processing.demandwatch import publish_demandwatch_store


UTC = timezone.utc


def _latest_weekday(now: datetime, weekday: int) -> datetime:
    days_since = (now.weekday() - weekday) % 7
    if days_since == 0:
        days_since = 7
    target = (now - timedelta(days=days_since)).date()
    return datetime(target.year, target.month, target.day, tzinfo=UTC)


def _month_end(month_start: datetime) -> datetime:
    next_month = month_start + relativedelta(months=+1)
    final_day = (next_month - timedelta(days=1)).date()
    return datetime(final_day.year, final_day.month, final_day.day, 23, 59, 59, tzinfo=UTC)


def _month_anchor(now: datetime, months_back: int) -> datetime:
    target = datetime(now.year, now.month, 1, tzinfo=UTC) - relativedelta(months=months_back)
    return _month_end(target)


def _release_at(target_date: datetime, hour: int, minute: int) -> datetime:
    return datetime(target_date.year, target_date.month, target_date.day, hour, minute, tzinfo=UTC)


@pytest_asyncio.fixture
async def demandwatch_seeded_session(seeded_session: AsyncSession) -> AsyncIterator[AsyncSession]:
    now = datetime.now(UTC)
    latest_friday = _latest_weekday(now, 4)
    latest_thursday = _latest_weekday(now, 3)
    latest_hour = (now - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)

    seeded_session.add_all(
        [
            Commodity(code="crude_products", name="Crude Products", sector="energy", is_active=True, metadata_={}),
            Commodity(code="gasoline", name="Gasoline", sector="energy", is_active=True, metadata_={}),
            Commodity(code="distillates", name="Distillates", sector="energy", is_active=True, metadata_={}),
            Commodity(code="electricity", name="Electricity", sector="energy", is_active=True, metadata_={}),
            Commodity(code="grains_oilseeds", name="Grains & Oilseeds", sector="agriculture", is_active=True, metadata_={}),
            Commodity(code="corn", name="Corn", sector="agriculture", is_active=True, metadata_={}),
            Commodity(code="soybeans", name="Soybeans", sector="agriculture", is_active=True, metadata_={}),
            Commodity(code="wheat", name="Wheat", sector="agriculture", is_active=True, metadata_={}),
            Commodity(code="base_metals", name="Base Metals", sector="metals", is_active=True, metadata_={}),
            Geography(code="WORLD", name="World", geo_type="global", is_active=True, metadata_={}),
            Geography(code="CHINA", name="China", geo_type="country", is_active=True, metadata_={}),
            UnitDefinition(code="kb_d", name="Thousand Barrels per Day", dimension="flow", symbol="kb/d", metadata_={}),
            UnitDefinition(code="pct", name="Percent", dimension="ratio", symbol="%", metadata_={}),
            UnitDefinition(code="gw", name="Gigawatt", dimension="capacity", symbol="GW", metadata_={}),
            UnitDefinition(code="twh", name="Terawatt Hour", dimension="flow", symbol="TWh", metadata_={}),
            UnitDefinition(code="mbu", name="Million Bushels", dimension="flow", symbol="mbu", metadata_={}),
            UnitDefinition(code="mmt", name="Million Metric Tons", dimension="flow", symbol="mmt", metadata_={}),
            UnitDefinition(code="index", name="Index", dimension="index", symbol="index", metadata_={}),
            UnitDefinition(code="k_units_saar", name="Thousands SAAR", dimension="count", symbol="k", metadata_={}),
            Source(
                slug="eia",
                name="U.S. Energy Information Administration",
                source_type="api",
                legal_status="public_domain",
                homepage_url="https://www.eia.gov/",
                docs_url="https://www.eia.gov/opendata/",
                default_timezone="America/New_York",
                attribution_text="EIA",
                rate_limit_notes="test",
                active=True,
            ),
            Source(
                slug="fred",
                name="Federal Reserve Economic Data",
                source_type="api",
                legal_status="public_domain",
                homepage_url="https://fred.stlouisfed.org/",
                docs_url="https://fred.stlouisfed.org/docs/api/",
                default_timezone="America/Chicago",
                attribution_text="FRED",
                rate_limit_notes="test",
                active=True,
            ),
            Source(
                slug="usda_psd",
                name="USDA PSD Online",
                source_type="api",
                legal_status="public_domain",
                homepage_url="https://apps.fas.usda.gov/psdonline/",
                docs_url="https://apps.fas.usda.gov/",
                default_timezone="America/New_York",
                attribution_text="USDA FAS PSD",
                rate_limit_notes="test",
                active=True,
            ),
            Source(
                slug="usda_export_sales",
                name="USDA Export Sales Reporting",
                source_type="csv",
                legal_status="public_domain",
                homepage_url="https://apps.fas.usda.gov/export-sales/esrd1.html",
                docs_url="https://apps.fas.usda.gov/export-sales/esrd1.html",
                default_timezone="America/New_York",
                attribution_text="USDA Export Sales",
                rate_limit_notes="test",
                active=True,
            ),
            Source(
                slug="ember",
                name="Ember",
                source_type="api",
                legal_status="cc_by",
                homepage_url="https://ember-energy.org/",
                docs_url="https://ember-energy.org/data-catalogue/",
                default_timezone="UTC",
                attribution_text="Ember",
                rate_limit_notes="test",
                active=True,
            ),
            Source(
                slug="china_customs",
                name="China Customs",
                source_type="html",
                legal_status="needs_verification",
                homepage_url="http://english.customs.gov.cn/",
                docs_url=None,
                default_timezone="Asia/Shanghai",
                attribution_text="China Customs",
                rate_limit_notes="blocked",
                active=True,
            ),
            Source(
                slug="spglobal_pmi",
                name="S&P Global PMI",
                source_type="html",
                legal_status="off_limits",
                homepage_url="https://www.spglobal.com/marketintelligence/en/mi/research-analysis/pmi.html",
                docs_url="https://www.spglobal.com/marketintelligence/en/solutions/purchasing-managers-indexes.html",
                default_timezone="Europe/London",
                attribution_text="S&P Global",
                rate_limit_notes="blocked",
                active=True,
            ),
        ]
    )
    await seeded_session.flush()

    sources = {
        source.slug: source
        for source in (
            await seeded_session.execute(select(Source).where(Source.slug.in_(("eia", "fred", "usda_psd", "usda_export_sales", "ember", "china_customs", "spglobal_pmi"))))
        ).scalars()
    }

    seeded_session.add_all(
        [
            ReleaseDefinition(
                source_id=sources["eia"].id,
                slug="demand_eia_wpsr",
                name="DemandWatch EIA Weekly Petroleum Status Report",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="crude_products",
                geography_code="US",
                cadence="weekly",
                schedule_timezone="America/New_York",
                schedule_rule="FREQ=WEEKLY;BYDAY=WE;BYHOUR=10;BYMINUTE=30",
                default_local_time=time(10, 30),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://www.eia.gov/petroleum/supply/weekly/"},
            ),
            ReleaseDefinition(
                source_id=sources["eia"].id,
                slug="demand_eia_grid_monitor",
                name="DemandWatch EIA Grid Monitor",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="electricity",
                geography_code="US",
                cadence="daily",
                schedule_timezone="America/New_York",
                schedule_rule="FREQ=DAILY;BYHOUR=18;BYMINUTE=0",
                default_local_time=time(18, 0),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://www.eia.gov/electricity/gridmonitor/"},
            ),
            ReleaseDefinition(
                source_id=sources["fred"].id,
                slug="demand_fred_g17",
                name="DemandWatch FRED G.17 Industrial Production",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="base_metals",
                geography_code="US",
                cadence="monthly",
                schedule_timezone="America/Chicago",
                schedule_rule="FREQ=MONTHLY;BYHOUR=08;BYMINUTE=15",
                default_local_time=time(8, 15),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://www.federalreserve.gov/releases/g17/"},
            ),
            ReleaseDefinition(
                source_id=sources["fred"].id,
                slug="demand_fred_new_residential_construction",
                name="DemandWatch FRED New Residential Construction",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="base_metals",
                geography_code="US",
                cadence="monthly",
                schedule_timezone="America/New_York",
                schedule_rule="FREQ=MONTHLY;BYHOUR=08;BYMINUTE=30",
                default_local_time=time(8, 30),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://www.census.gov/construction/nrc/index.html"},
            ),
            ReleaseDefinition(
                source_id=sources["usda_psd"].id,
                slug="demand_usda_wasde",
                name="DemandWatch USDA WASDE",
                release_kind="report",
                module_code="demandwatch",
                commodity_code="grains_oilseeds",
                geography_code="WORLD",
                cadence="monthly",
                schedule_timezone="America/New_York",
                schedule_rule="FREQ=MONTHLY;BYHOUR=12;BYMINUTE=0",
                default_local_time=time(12, 0),
                is_calendar_driven=True,
                active=True,
                metadata_={"landing_url": "https://www.usda.gov/oce/commodity/wasde"},
            ),
            ReleaseDefinition(
                source_id=sources["usda_export_sales"].id,
                slug="demand_usda_export_sales",
                name="DemandWatch USDA Export Sales Report",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="grains_oilseeds",
                geography_code="US",
                cadence="weekly",
                schedule_timezone="America/New_York",
                schedule_rule="FREQ=WEEKLY;BYDAY=TH;BYHOUR=08;BYMINUTE=30",
                default_local_time=time(8, 30),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://apps.fas.usda.gov/export-sales/esrd1.html"},
            ),
            ReleaseDefinition(
                source_id=sources["ember"].id,
                slug="demand_ember_monthly_electricity",
                name="DemandWatch Ember Monthly Electricity",
                release_kind="data_release",
                module_code="demandwatch",
                commodity_code="electricity",
                geography_code="WORLD",
                cadence="monthly",
                schedule_timezone="UTC",
                schedule_rule="FREQ=MONTHLY;BYHOUR=08;BYMINUTE=0",
                default_local_time=time(8, 0),
                is_calendar_driven=False,
                active=True,
                metadata_={"landing_url": "https://ember-energy.org/data-catalogue/monthly-electricity-data/"},
            ),
        ]
    )
    await seeded_session.flush()

    releases = {
        release.slug: release
        for release in (
            await seeded_session.execute(
                select(ReleaseDefinition).where(
                    ReleaseDefinition.slug.in_(
                        (
                            "demand_eia_wpsr",
                            "demand_eia_grid_monitor",
                            "demand_fred_g17",
                            "demand_fred_new_residential_construction",
                            "demand_usda_wasde",
                            "demand_usda_export_sales",
                            "demand_ember_monthly_electricity",
                        )
                    )
                )
            )
        ).scalars()
    }

    seeded_session.add_all(
        [
            DemandVertical(
                code="crude_products",
                name="Crude Oil + Refined Products",
                commodity_code="crude_products",
                sector="energy",
                nav_label="Crude",
                short_label="Crude + Products",
                description="Weekly petroleum demand and refinery-throughput indicators anchored on EIA releases.",
                display_order=10,
                active=True,
                metadata_={},
            ),
            DemandVertical(
                code="electricity",
                name="Electricity / Power",
                commodity_code="electricity",
                sector="energy",
                nav_label="Power",
                short_label="Electricity",
                description="Direct electricity demand coverage from EIA and Ember.",
                display_order=20,
                active=True,
                metadata_={},
            ),
            DemandVertical(
                code="grains_oilseeds",
                name="Grains & Oilseeds",
                commodity_code="grains_oilseeds",
                sector="agriculture",
                nav_label="Grains",
                short_label="Grains",
                description="USDA demand estimates, export flow data, and corn ethanol throughput.",
                display_order=30,
                active=True,
                metadata_={},
            ),
            DemandVertical(
                code="base_metals",
                name="Base Metals",
                commodity_code="base_metals",
                sector="metals",
                nav_label="Metals",
                short_label="Base Metals",
                description="Macro-demand proxies for industrial metals.",
                display_order=40,
                active=True,
                metadata_={},
            ),
        ]
    )
    await seeded_session.flush()

    indicator_specs = [
        ("EIA_US_TOTAL_PRODUCT_SUPPLIED", "EIA US Total Product Supplied", "flow", "weekly", "crude_products", "US", "eia", "PET.WRPUPUS2.W", "kb_d", "kb_d", "actual", "crude_products", "t1_direct", "live", 10, "demand_eia_wpsr"),
        ("EIA_GASOLINE_US_PRODUCT_SUPPLIED", "EIA US Gasoline Product Supplied", "flow", "weekly", "gasoline", "US", "eia", "PET.WGFUPUS2.W", "kb_d", "kb_d", "actual", "crude_products", "t1_direct", "live", 20, "demand_eia_wpsr"),
        ("EIA_DISTILLATE_US_PRODUCT_SUPPLIED", "EIA US Distillate Product Supplied", "flow", "weekly", "distillates", "US", "eia", "PET.WDIUPUS2.W", "kb_d", "kb_d", "actual", "crude_products", "t1_direct", "live", 30, "demand_eia_wpsr"),
        ("EIA_CRUDE_US_REFINERY_INPUTS", "EIA US Refinery Crude Inputs", "flow", "weekly", "crude_oil", "US", "eia", "PET.WCRRIUS2.W", "kb_d", "kb_d", "actual", "crude_products", "t2_throughput", "live", 40, "demand_eia_wpsr"),
        ("EIA_CRUDE_US_REFINERY_UTILISATION", "EIA US Refinery Utilisation", "utilisation", "weekly", "crude_products", "US", "eia", "PET.WPULEUS3.W", "pct", "pct", "actual", "crude_products", "t2_throughput", "live", 50, "demand_eia_wpsr"),
        ("EIA_US_ELECTRICITY_GRID_LOAD", "EIA US Grid Load", "capacity", "hourly", "electricity", "US", "eia", "EBA.US48-ALL.D.H", "gw", "gw", "actual", "electricity", "t1_direct", "live", 10, "demand_eia_grid_monitor"),
        ("EMBER_GLOBAL_ELECTRICITY_DEMAND", "Ember Global Electricity Demand", "flow", "monthly", "electricity", "WORLD", "ember", "WORLD", "twh", "twh", "actual", "electricity", "t1_direct", "live", 20, "demand_ember_monthly_electricity"),
        ("EMBER_CHINA_ELECTRICITY_DEMAND", "Ember China Electricity Demand", "flow", "monthly", "electricity", "CHINA", "ember", "CHN", "twh", "twh", "actual", "electricity", "t1_direct", "live", 30, "demand_ember_monthly_electricity"),
        ("USDA_US_CORN_TOTAL_USE_WASDE", "USDA US Corn Total Use", "flow", "monthly", "corn", "US", "usda_psd", "0440000", "mbu", "mbu", "estimate", "grains_oilseeds", "t1_direct", "live", 10, "demand_usda_wasde"),
        ("USDA_US_SOYBEAN_TOTAL_USE_WASDE", "USDA US Soybean Total Use", "flow", "monthly", "soybeans", "US", "usda_psd", "2222000", "mbu", "mbu", "estimate", "grains_oilseeds", "t1_direct", "live", 20, "demand_usda_wasde"),
        ("USDA_US_WHEAT_TOTAL_USE_WASDE", "USDA US Wheat Total Use", "flow", "monthly", "wheat", "US", "usda_psd", "0410000", "mbu", "mbu", "estimate", "grains_oilseeds", "t1_direct", "live", 30, "demand_usda_wasde"),
        ("USDA_US_CORN_EXPORT_SALES", "USDA US Corn Export Sales", "flow", "weekly", "corn", "US", "usda_export_sales", "401", "mmt", "mmt", "actual", "grains_oilseeds", "t3_trade", "live", 40, "demand_usda_export_sales"),
        ("EIA_US_ETHANOL_PRODUCTION", "EIA US Ethanol Production", "flow", "weekly", "corn", "US", "eia", "PET.W_EPOOXE_YIR_NUS_MBBLD.W", "kb_d", "kb_d", "actual", "grains_oilseeds", "t2_throughput", "live", 50, "demand_eia_wpsr"),
        ("FRED_US_INDUSTRIAL_PRODUCTION", "FRED US Industrial Production Index", "macro", "monthly", "base_metals", "US", "fred", "INDPRO", "index", "index", "actual", "base_metals", "t6_macro", "live", 10, "demand_fred_g17"),
        ("FRED_US_MANUFACTURING_PRODUCTION", "FRED US Manufacturing Production Index", "macro", "monthly", "base_metals", "US", "fred", "IPMAN", "index", "index", "actual", "base_metals", "t6_macro", "live", 20, "demand_fred_g17"),
        ("FRED_US_HOUSING_STARTS", "FRED US Housing Starts", "macro", "monthly", "base_metals", "US", "fred", "HOUST", "k_units_saar", "k_units_saar", "actual", "base_metals", "t4_end_use", "live", 30, "demand_fred_new_residential_construction"),
        ("FRED_US_BUILDING_PERMITS", "FRED US Building Permits", "macro", "monthly", "base_metals", "US", "fred", "PERMIT", "k_units_saar", "k_units_saar", "actual", "base_metals", "t5_leading", "live", 40, "demand_fred_new_residential_construction"),
        ("CHINA_CRUDE_IMPORTS_MONTHLY", "China Crude Oil Imports", "flow", "monthly", "crude_oil", "CHINA", "china_customs", None, "mmt", "mmt", "actual", "crude_products", "t3_trade", "needs_verification", 90, None),
        ("SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW", "S&P Global Eurozone Manufacturing PMI Raw", "signal", "monthly", "base_metals", "WORLD", "spglobal_pmi", None, "index", "index", "signal", "base_metals", "t6_macro", "blocked", 100, None),
    ]

    indicators: dict[str, Indicator] = {}
    demand_series: list[DemandSeries] = []
    for (
        code,
        name,
        measure_family,
        frequency,
        commodity_code,
        geography_code,
        source_slug,
        source_series_key,
        native_unit_code,
        canonical_unit_code,
        observation_kind,
        vertical_code,
        tier,
        coverage_status,
        display_order,
        release_slug,
    ) in indicator_specs:
        indicator = Indicator(
            code=code,
            name=name,
            description=name,
            measure_family=measure_family,
            frequency=frequency,
            commodity_code=commodity_code,
            geography_code=geography_code,
            source_id=sources[source_slug].id,
            source_series_key=source_series_key,
            native_unit_code=native_unit_code,
            canonical_unit_code=canonical_unit_code,
            default_observation_kind=observation_kind,
            is_seasonal=False,
            is_derived=False,
            visibility_tier="public" if coverage_status == "live" else "internal",
            active=coverage_status == "live",
            metadata_={"release_slug": release_slug} if release_slug else {},
        )
        seeded_session.add(indicator)
        await seeded_session.flush()
        indicators[code] = indicator
        demand_series.append(
            DemandSeries(
                indicator_id=indicator.id,
                vertical_code=vertical_code,
                release_definition_id=releases[release_slug].id if release_slug else None,
                indicator_tier=tier,
                coverage_status=coverage_status,
                display_order=display_order,
                notes=(
                    "Direct republication terms are unresolved."
                    if coverage_status == "needs_verification"
                    else "Raw PMI values are off-limits without a licence."
                    if coverage_status == "blocked"
                    else None
                ),
                metadata_={},
            )
        )
    seeded_session.add_all(demand_series)
    await seeded_session.flush()

    release_cache: dict[str, SourceRelease] = {}

    def upsert_release(release_slug: str, key_suffix: str, released_at: datetime, source_url: str) -> SourceRelease:
        cache_key = f"{release_slug}:{key_suffix}"
        if cache_key in release_cache:
            return release_cache[cache_key]
        source_release = SourceRelease(
            source_id=releases[release_slug].source_id,
            release_definition_id=releases[release_slug].id,
            release_key=cache_key,
            release_name=f"{releases[release_slug].name} ({key_suffix})",
            scheduled_at=released_at,
            released_at=released_at,
            period_start_at=None,
            period_end_at=None,
            release_timezone=releases[release_slug].schedule_timezone,
            source_url=source_url,
            status="observed",
            primary_artifact_id=None,
            notes=None,
            metadata_={},
        )
        seeded_session.add(source_release)
        release_cache[cache_key] = source_release
        return source_release

    def add_observation(
        indicator_code: str,
        period_start_at: datetime,
        period_end_at: datetime,
        release_slug: str,
        key_suffix: str,
        released_at: datetime,
        value: float,
        unit_code: str,
        *,
        observation_kind: str = "actual",
        vintage_at: datetime | None = None,
        is_latest: bool = True,
        revision_sequence: int = 1,
        metadata: dict | None = None,
        source_url: str | None = None,
    ) -> None:
        source_release = upsert_release(
            release_slug,
            key_suffix,
            released_at,
            source_url or f"https://example.com/{release_slug}/{key_suffix}",
        )
        seeded_session.add(
            Observation(
                indicator_id=indicators[indicator_code].id,
                period_start_at=period_start_at,
                period_end_at=period_end_at,
                release_id=source_release.id,
                release_date=released_at,
                vintage_at=vintage_at or released_at,
                observation_kind=observation_kind,
                value_native=value,
                unit_native_code=unit_code,
                value_canonical=value,
                unit_canonical_code=unit_code,
                metadata_=metadata or {},
                is_latest=is_latest,
                revision_sequence=revision_sequence,
            )
        )

    old_week_end = latest_friday - timedelta(days=7 * 157)
    old_week_start = old_week_end - timedelta(days=6)
    year_ago_week_end = latest_friday - timedelta(days=364)
    year_ago_week_start = year_ago_week_end - timedelta(days=6)
    recent_week_ends = [latest_friday - timedelta(days=7 * offset) for offset in range(0, 4)]
    recent_week_ends.reverse()
    recent_week_starts = [week_end - timedelta(days=6) for week_end in recent_week_ends]

    weekly_series_values = {
        "EIA_US_TOTAL_PRODUCT_SUPPLIED": [9300.0, 9400.0, 9500.0, 9600.0],
        "EIA_GASOLINE_US_PRODUCT_SUPPLIED": [8600.0, 8700.0, 8800.0, 8900.0],
        "EIA_DISTILLATE_US_PRODUCT_SUPPLIED": [3900.0, 3950.0, 4020.0, 4100.0],
        "EIA_CRUDE_US_REFINERY_INPUTS": [15800.0, 16000.0, 16150.0, 16300.0],
        "EIA_CRUDE_US_REFINERY_UTILISATION": [86.0, 86.8, 87.2, 88.4],
        "EIA_US_ETHANOL_PRODUCTION": [1010.0, 1030.0, 1050.0, 1080.0],
    }
    weekly_old_values = {
        "EIA_US_TOTAL_PRODUCT_SUPPLIED": 9000.0,
        "EIA_GASOLINE_US_PRODUCT_SUPPLIED": 8400.0,
        "EIA_DISTILLATE_US_PRODUCT_SUPPLIED": 3700.0,
        "EIA_CRUDE_US_REFINERY_INPUTS": 15000.0,
        "EIA_CRUDE_US_REFINERY_UTILISATION": 84.0,
        "EIA_US_ETHANOL_PRODUCTION": 960.0,
    }
    weekly_yoy_values = {
        "EIA_US_TOTAL_PRODUCT_SUPPLIED": 9000.0,
        "EIA_GASOLINE_US_PRODUCT_SUPPLIED": 8700.0,
        "EIA_DISTILLATE_US_PRODUCT_SUPPLIED": 4000.0,
        "EIA_CRUDE_US_REFINERY_INPUTS": 15900.0,
        "EIA_CRUDE_US_REFINERY_UTILISATION": 86.8,
        "EIA_US_ETHANOL_PRODUCTION": 1045.0,
    }

    for code, latest_values in weekly_series_values.items():
        add_observation(
            code,
            old_week_start,
            old_week_end,
            "demand_eia_wpsr",
            f"{code.lower()}-old",
            _release_at(old_week_end + timedelta(days=5), 14, 30),
            weekly_old_values[code],
            indicators[code].canonical_unit_code,
        )
        add_observation(
            code,
            year_ago_week_start,
            year_ago_week_end,
            "demand_eia_wpsr",
            f"{code.lower()}-yoy",
            _release_at(year_ago_week_end + timedelta(days=5), 14, 30),
            weekly_yoy_values[code],
            indicators[code].canonical_unit_code,
        )
        for index, (period_start, period_end, value) in enumerate(zip(recent_week_starts, recent_week_ends, latest_values, strict=True), start=1):
            add_observation(
                code,
                period_start,
                period_end,
                "demand_eia_wpsr",
                f"{code.lower()}-{period_end.date().isoformat()}",
                _release_at(period_end + timedelta(days=5), 14, 30),
                value,
                indicators[code].canonical_unit_code,
                revision_sequence=index,
            )

    old_export_end = latest_thursday - timedelta(days=7 * 157)
    old_export_start = old_export_end - timedelta(days=6)
    add_observation(
        "USDA_US_CORN_EXPORT_SALES",
        old_export_start,
        old_export_end,
        "demand_usda_export_sales",
        "corn-export-old",
        _release_at(old_export_end, 12, 30),
        0.75,
        "mmt",
    )
    add_observation(
        "USDA_US_CORN_EXPORT_SALES",
        latest_thursday - timedelta(days=6),
        latest_thursday,
        "demand_usda_export_sales",
        "corn-export-latest",
        _release_at(latest_thursday, 12, 30),
        1.21,
        "mmt",
    )

    add_observation(
        "EIA_US_ELECTRICITY_GRID_LOAD",
        latest_hour - timedelta(days=365 * 3),
        latest_hour - timedelta(days=365 * 3),
        "demand_eia_grid_monitor",
        "grid-load-old",
        latest_hour - timedelta(days=365 * 3),
        360.0,
        "gw",
    )
    add_observation(
        "EIA_US_ELECTRICITY_GRID_LOAD",
        latest_hour - timedelta(days=364),
        latest_hour - timedelta(days=364),
        "demand_eia_grid_monitor",
        "grid-load-yoy",
        latest_hour - timedelta(days=364),
        419.0,
        "gw",
    )
    add_observation(
        "EIA_US_ELECTRICITY_GRID_LOAD",
        latest_hour,
        latest_hour,
        "demand_eia_grid_monitor",
        "grid-load-latest",
        latest_hour,
        428.0,
        "gw",
    )

    monthly_latest_fred = _month_anchor(now, 2)
    monthly_release_fred = _release_at(datetime(monthly_latest_fred.year, monthly_latest_fred.month, 17, tzinfo=UTC) + relativedelta(months=+1), 13, 15)
    monthly_release_fred_revision = monthly_release_fred + timedelta(days=1)
    fred_months = [_month_anchor(now, offset) for offset in range(2, 8)]
    fred_values = {
        "FRED_US_INDUSTRIAL_PRODUCTION": [103.8, 103.0, 102.7, 102.3, 101.9, 101.6],
        "FRED_US_MANUFACTURING_PRODUCTION": [101.4, 101.1, 100.8, 100.5, 100.1, 99.8],
        "FRED_US_HOUSING_STARTS": [1520.0, 1490.0, 1460.0, 1440.0, 1410.0, 1390.0],
        "FRED_US_BUILDING_PERMITS": [1580.0, 1560.0, 1530.0, 1510.0, 1490.0, 1475.0],
    }
    fred_yoy_values = {
        "FRED_US_INDUSTRIAL_PRODUCTION": 102.6,
        "FRED_US_MANUFACTURING_PRODUCTION": 100.0,
        "FRED_US_HOUSING_STARTS": 1445.0,
        "FRED_US_BUILDING_PERMITS": 1505.0,
    }
    fred_old_values = {
        "FRED_US_INDUSTRIAL_PRODUCTION": 98.0,
        "FRED_US_MANUFACTURING_PRODUCTION": 97.0,
        "FRED_US_HOUSING_STARTS": 1330.0,
        "FRED_US_BUILDING_PERMITS": 1410.0,
    }

    for code, values in fred_values.items():
        old_period_end = monthly_latest_fred - relativedelta(years=3)
        add_observation(
            code,
            old_period_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            old_period_end,
            "demand_fred_g17" if code.startswith("FRED_US_I") or code == "FRED_US_MANUFACTURING_PRODUCTION" else "demand_fred_new_residential_construction",
            f"{code.lower()}-old",
            monthly_release_fred - relativedelta(years=3),
            fred_old_values[code],
            indicators[code].canonical_unit_code,
            metadata={"release_month": old_period_end.strftime("%Y-%m")},
        )
        yoy_period_end = monthly_latest_fred - relativedelta(years=1)
        add_observation(
            code,
            yoy_period_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            yoy_period_end,
            "demand_fred_g17" if code.startswith("FRED_US_I") or code == "FRED_US_MANUFACTURING_PRODUCTION" else "demand_fred_new_residential_construction",
            f"{code.lower()}-yoy",
            monthly_release_fred - relativedelta(years=1),
            fred_yoy_values[code],
            indicators[code].canonical_unit_code,
            metadata={"release_month": yoy_period_end.strftime("%Y-%m")},
        )
        for value_index, (period_end, value) in enumerate(zip(fred_months, values, strict=True), start=1):
            release_slug = (
                "demand_fred_g17"
                if code.startswith("FRED_US_I") or code == "FRED_US_MANUFACTURING_PRODUCTION"
                else "demand_fred_new_residential_construction"
            )
            release_at = monthly_release_fred - relativedelta(months=(monthly_latest_fred.month - period_end.month) % 12)
            if period_end == monthly_latest_fred and code == "FRED_US_INDUSTRIAL_PRODUCTION":
                add_observation(
                    code,
                    period_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                    period_end,
                    release_slug,
                    f"{code.lower()}-{period_end.date().isoformat()}-initial",
                    release_at,
                    103.6,
                    indicators[code].canonical_unit_code,
                    metadata={"release_month": period_end.strftime("%Y-%m")},
                    is_latest=False,
                    revision_sequence=1,
                )
                add_observation(
                    code,
                    period_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                    period_end,
                    release_slug,
                    f"{code.lower()}-{period_end.date().isoformat()}-revision",
                    monthly_release_fred_revision,
                    value,
                    indicators[code].canonical_unit_code,
                    metadata={"release_month": period_end.strftime("%Y-%m")},
                    revision_sequence=2,
                )
            else:
                add_observation(
                    code,
                    period_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                    period_end,
                    release_slug,
                    f"{code.lower()}-{period_end.date().isoformat()}",
                    release_at,
                    value,
                    indicators[code].canonical_unit_code,
                    metadata={"release_month": period_end.strftime("%Y-%m")},
                    revision_sequence=value_index,
                )

    latest_wasde_period = _month_anchor(now, 1)
    wasde_release = _release_at(datetime(latest_wasde_period.year, latest_wasde_period.month, 12, tzinfo=UTC), 16, 0)
    prior_wasde_period = latest_wasde_period - relativedelta(months=1)
    wasde_codes = {
        "USDA_US_CORN_TOTAL_USE_WASDE": (14520.0, 14770.0, 14890.0),
        "USDA_US_SOYBEAN_TOTAL_USE_WASDE": (2230.0, 2330.0, 2360.0),
        "USDA_US_WHEAT_TOTAL_USE_WASDE": (1980.0, 2050.0, 2075.0),
    }
    for code, (old_value, prior_value, latest_value) in wasde_codes.items():
        old_period = latest_wasde_period - relativedelta(years=3)
        add_observation(
            code,
            old_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            old_period,
            "demand_usda_wasde",
            f"{code.lower()}-old",
            wasde_release - relativedelta(years=3),
            old_value,
            "mbu",
            observation_kind="estimate",
            metadata={"release_month": old_period.strftime("%Y-%m")},
        )
        add_observation(
            code,
            prior_wasde_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            prior_wasde_period,
            "demand_usda_wasde",
            f"{code.lower()}-prior",
            wasde_release - relativedelta(months=1),
            prior_value,
            "mbu",
            observation_kind="estimate",
            metadata={"release_month": prior_wasde_period.strftime("%Y-%m")},
        )
        add_observation(
            code,
            latest_wasde_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            latest_wasde_period,
            "demand_usda_wasde",
            f"{code.lower()}-latest",
            wasde_release,
            latest_value,
            "mbu",
            observation_kind="estimate",
            metadata={"release_month": latest_wasde_period.strftime("%Y-%m")},
        )

    latest_ember_period = _month_anchor(now, 2)
    latest_ember_release = _release_at(datetime(latest_ember_period.year, latest_ember_period.month, 25, tzinfo=UTC) + relativedelta(months=+1), 8, 0)
    ember_values = {
        "EMBER_GLOBAL_ELECTRICITY_DEMAND": (2400.0, 2500.0, 2550.0),
        "EMBER_CHINA_ELECTRICITY_DEMAND": (620.0, 640.0, 670.0),
    }
    for code, (old_value, yoy_value, latest_value) in ember_values.items():
        old_period = latest_ember_period - relativedelta(years=3)
        yoy_period = latest_ember_period - relativedelta(years=1)
        add_observation(
            code,
            old_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            old_period,
            "demand_ember_monthly_electricity",
            f"{code.lower()}-old",
            latest_ember_release - relativedelta(years=3),
            old_value,
            "twh",
            metadata={"release_month": old_period.strftime("%Y-%m")},
        )
        add_observation(
            code,
            yoy_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            yoy_period,
            "demand_ember_monthly_electricity",
            f"{code.lower()}-yoy",
            latest_ember_release - relativedelta(years=1),
            yoy_value,
            "twh",
            metadata={"release_month": yoy_period.strftime("%Y-%m")},
        )
        add_observation(
            code,
            latest_ember_period.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            latest_ember_period,
            "demand_ember_monthly_electricity",
            f"{code.lower()}-latest",
            latest_ember_release,
            latest_value,
            "twh",
            metadata={"release_month": latest_ember_period.strftime("%Y-%m")},
        )

    await seeded_session.commit()
    yield seeded_session


@pytest_asyncio.fixture
async def demandwatch_client(
    demandwatch_seeded_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> AsyncIterator[AsyncClient]:
    output_path = tmp_path / "demandwatch" / "published.sqlite"
    await publish_demandwatch_store(demandwatch_seeded_session, output_path)
    demandwatch_processing.clear_demandwatch_public_read_model_cache()
    monkeypatch.setattr(demandwatch_processing, "_demandwatch_public_artifact_path", lambda: output_path)

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield demandwatch_seeded_session

    app.dependency_overrides[get_db_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http_client:
        yield http_client
    app.dependency_overrides.clear()
    demandwatch_processing.clear_demandwatch_public_read_model_cache()


@pytest.mark.asyncio
async def test_demandwatch_macro_strip_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/macro-strip")
    assert response.status_code == 200
    payload = response.json()
    by_code = {item["code"]: item for item in payload["items"]}
    assert by_code["FRED_US_INDUSTRIAL_PRODUCTION"]["label"] == "US Industrial Production"
    assert by_code["EIA_US_ELECTRICITY_GRID_LOAD"]["display_value"].endswith("GW")
    assert by_code["EMBER_CHINA_ELECTRICITY_DEMAND"]["source_url"] is not None
    assert (
        by_code["EMBER_CHINA_ELECTRICITY_DEMAND"]["source_url"].startswith("https://example.com/")
        or "ember-energy.org" in by_code["EMBER_CHINA_ELECTRICITY_DEMAND"]["source_url"]
    )


@pytest.mark.asyncio
async def test_demandwatch_scorecard_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/scorecard")
    assert response.status_code == 200
    payload = response.json()
    assert [item["id"] for item in payload["items"]] == [
        "crude-products",
        "electricity",
        "grains",
        "base-metals",
    ]
    crude = payload["items"][0]
    assert crude["primary_series_code"] == "EIA_US_TOTAL_PRODUCT_SUPPLIED"
    assert crude["yoy_label"] is not None


@pytest.mark.asyncio
async def test_demandwatch_movers_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/movers", params={"limit": 5})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 5
    assert payload["items"][0]["latest_release_date"] is not None
    assert payload["items"][0]["tier_label"].startswith("T")


@pytest.mark.asyncio
async def test_demandwatch_vertical_detail_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/verticals/crude-products")
    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "crude-products"
    assert payload["scorecard"]["primary_series_code"] == "EIA_US_TOTAL_PRODUCT_SUPPLIED"
    assert [section["id"] for section in payload["sections"]] == ["direct", "throughput"]
    assert any("China" in note for note in payload["notes"])


@pytest.mark.asyncio
async def test_demandwatch_indicator_table_endpoint_exposes_fred_vintages(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/verticals/base-metals/indicator-table")
    assert response.status_code == 200
    payload = response.json()
    rows = {
        row["code"]: row
        for section in payload["sections"]
        for row in section["rows"]
    }
    assert rows["FRED_US_INDUSTRIAL_PRODUCTION"]["vintage_count"] >= 3
    assert rows["FRED_US_HOUSING_STARTS"]["latest_display"].endswith("m")


@pytest.mark.asyncio
async def test_demandwatch_coverage_notes_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/coverage-notes")
    assert response.status_code == 200
    payload = response.json()
    assert payload["markdown"].startswith("# DemandWatch Coverage Audit")
    crude = next(item for item in payload["verticals"] if item["code"] == "crude_products")
    metals = next(item for item in payload["verticals"] if item["code"] == "base_metals")
    assert crude["deferred"][0]["code"] == "CHINA_CRUDE_IMPORTS_MONTHLY"
    assert metals["blocked"][0]["code"] == "SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW"


@pytest.mark.asyncio
async def test_demandwatch_next_release_dates_endpoint(demandwatch_client: AsyncClient) -> None:
    response = await demandwatch_client.get("/api/demandwatch/next-release-dates", params={"vertical": "grains"})
    assert response.status_code == 200
    payload = response.json()
    slugs = {item["release_slug"] for item in payload["items"]}
    assert slugs == {"demand_usda_wasde", "demand_usda_export_sales", "demand_eia_wpsr"}
    assert all("grains" in item["vertical_ids"] for item in payload["items"])
    assert all(item["scheduled_for"] is not None for item in payload["items"])


@pytest.mark.asyncio
async def test_demandwatch_public_reads_share_one_published_generated_at(
    demandwatch_client: AsyncClient,
) -> None:
    snapshot_response = await demandwatch_client.get("/api/snapshot/demandwatch")
    assert snapshot_response.status_code == 200
    snapshot_payload = snapshot_response.json()

    detail_response = await demandwatch_client.get("/api/demandwatch/verticals/crude-products")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()

    table_response = await demandwatch_client.get("/api/demandwatch/verticals/crude-products/indicator-table")
    assert table_response.status_code == 200
    table_payload = table_response.json()

    coverage_response = await demandwatch_client.get("/api/demandwatch/coverage-notes")
    assert coverage_response.status_code == 200
    coverage_payload = coverage_response.json()

    assert detail_payload["generated_at"] == snapshot_payload["generated_at"]
    assert table_payload["generated_at"] == snapshot_payload["generated_at"]
    assert coverage_payload["generated_at"] == snapshot_payload["generated_at"]
    assert snapshot_payload["next_release_dates"]["generated_at"] == snapshot_payload["generated_at"]


@pytest.mark.asyncio
async def test_demandwatch_snapshot_endpoint_returns_full_bootstrap_payload(
    demandwatch_client: AsyncClient,
) -> None:
    response = await demandwatch_client.get("/api/snapshot/demandwatch")
    assert response.status_code == 200
    payload = response.json()

    assert payload["module"] == "demandwatch"
    assert payload["generated_at"] == payload["macro_strip"]["generated_at"]
    assert payload["generated_at"] == payload["scorecard"]["generated_at"]
    assert payload["generated_at"] == payload["movers"]["generated_at"]
    assert payload["generated_at"] == payload["coverage_notes"]["generated_at"]
    assert payload["generated_at"] == payload["next_release_dates"]["generated_at"]
    assert all(item["generated_at"] == payload["generated_at"] for item in payload["vertical_details"])
    assert payload["expires_at"] is not None
    assert [item["id"] for item in payload["scorecard"]["items"]] == [
        "crude-products",
        "electricity",
        "grains",
        "base-metals",
    ]
    assert [item["id"] for item in payload["vertical_details"]] == [
        "crude-products",
        "electricity",
        "grains",
        "base-metals",
    ]
    assert payload["vertical_errors"] == []
    assert payload["coverage_notes"]["summary"]["status_counts"]["blocked"] == 1
    assert {item["release_slug"] for item in payload["next_release_dates"]["items"]} == {
        "demand_eia_wpsr",
        "demand_eia_grid_monitor",
        "demand_fred_g17",
        "demand_fred_new_residential_construction",
        "demand_usda_wasde",
        "demand_usda_export_sales",
        "demand_ember_monthly_electricity",
    }
