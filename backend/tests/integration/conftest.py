from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.getenv("CW_TEST_DATABASE_URL")

if TEST_DATABASE_URL:
    import pytest_asyncio
else:  # pragma: no cover
    pytest_asyncio = pytest


pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="CW_TEST_DATABASE_URL is not configured")


if TEST_DATABASE_URL:
    from app.db.base import Base
    from app.db.models.indicators import Indicator, IndicatorModule, ModuleSnapshotCache, SeasonalRange
    from app.db.models.observations import Observation
    from app.db.models.reference import AppModule, Commodity, Geography, UnitDefinition
    from app.main import app
else:  # pragma: no cover
    Base = None
    Indicator = IndicatorModule = ModuleSnapshotCache = SeasonalRange = None
    Observation = None
    AppModule = Commodity = Geography = UnitDefinition = None
    app = None


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(TEST_DATABASE_URL, future=True)
    session_factory = async_sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        yield session
        await session.rollback()

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def seeded_session(db_session: AsyncSession) -> AsyncIterator[AsyncSession]:
    indicator_id = uuid4()
    hidden_indicator_id = uuid4()
    now = datetime.now(timezone.utc)
    db_session.add_all(
        [
            AppModule(code="inventorywatch", name="InventoryWatch"),
            Commodity(code="crude_oil", name="Crude Oil", sector="energy", is_active=True, metadata_={}),
            Geography(code="US", name="United States", geo_type="country", is_active=True, metadata_={}),
            UnitDefinition(code="kb", name="Thousand Barrels", dimension="volume_stock", symbol="kb", metadata_={}),
            Indicator(
                id=indicator_id,
                code="EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR",
                name="EIA US Commercial Crude Stocks excl SPR",
                measure_family="stock",
                frequency="weekly",
                commodity_code="crude_oil",
                geography_code="US",
                native_unit_code="kb",
                canonical_unit_code="kb",
                default_observation_kind="actual",
                is_seasonal=True,
                is_derived=False,
                visibility_tier="public",
                metadata_={"release_slug": "eia_wpsr"},
            ),
            IndicatorModule(indicator_id=indicator_id, module_code="inventorywatch", is_primary=True),
            Indicator(
                id=hidden_indicator_id,
                code="EIA_CRUDE_US_COMMERCIAL_STOCKS_ZZ_NO_HISTORY",
                name="EIA US Commercial Crude Stocks no history",
                measure_family="stock",
                frequency="weekly",
                commodity_code="crude_oil",
                geography_code="US",
                native_unit_code="kb",
                canonical_unit_code="kb",
                default_observation_kind="actual",
                is_seasonal=True,
                is_derived=False,
                visibility_tier="public",
                metadata_={"release_slug": "eia_wpsr"},
            ),
            IndicatorModule(indicator_id=hidden_indicator_id, module_code="inventorywatch", is_primary=True),
            Observation(
                indicator_id=indicator_id,
                period_start_at=datetime(2026, 3, 14, tzinfo=timezone.utc),
                period_end_at=datetime(2026, 3, 20, tzinfo=timezone.utc),
                release_id=None,
                release_date=now,
                vintage_at=now,
                observation_kind="actual",
                value_native=438940.0,
                unit_native_code="kb",
                value_canonical=438940.0,
                unit_canonical_code="kb",
                metadata_={},
            ),
            Observation(
                indicator_id=indicator_id,
                period_start_at=datetime(2026, 3, 7, tzinfo=timezone.utc),
                period_end_at=datetime(2026, 3, 13, tzinfo=timezone.utc),
                release_id=None,
                release_date=now - timedelta(days=7),
                vintage_at=now - timedelta(days=7),
                observation_kind="actual",
                value_native=442280.0,
                unit_native_code="kb",
                value_canonical=442280.0,
                unit_canonical_code="kb",
                metadata_={},
            ),
            SeasonalRange(
                indicator_id=indicator_id,
                profile_name="inventorywatch_5y",
                period_type="week_of_year",
                period_index=12,
                sample_size=5,
                p10=430000.0,
                p25=435000.0,
                p50=440000.0,
                p75=445000.0,
                p90=450000.0,
                mean_value=440500.0,
                stddev_value=5000.0,
                metadata_={},
            ),
            ModuleSnapshotCache(
                module_code="inventorywatch",
                snapshot_key="default",
                as_of=now,
                payload={
                    "module": "inventorywatch",
                    "generated_at": now.isoformat(),
                    "expires_at": (now + timedelta(minutes=5)).isoformat(),
                    "cards": [
                        {
                            "indicator_id": str(indicator_id),
                            "code": "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR",
                            "name": "EIA US Commercial Crude Stocks excl SPR",
                            "commodity_code": "crude_oil",
                            "geography_code": "US",
                            "latest_value": 438940.0,
                            "unit": "kb",
                            "change_abs": -3340.0,
                            "deviation_abs": -1060.0,
                            "signal": "tightening",
                            "sparkline": [442280.0, 438940.0],
                            "last_updated_at": now.isoformat(),
                            "stale": False
                        }
                    ]
                },
                expires_at=now - timedelta(minutes=5),
            ),
        ]
    )
    await db_session.commit()
    yield db_session


@pytest_asyncio.fixture
async def client(seeded_session: AsyncSession) -> AsyncIterator[AsyncClient]:
    async def override_session() -> AsyncIterator[AsyncSession]:
        yield seeded_session

    from app.db.session import get_db_session

    app.dependency_overrides[get_db_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http_client:
        yield http_client
    app.dependency_overrides.clear()
