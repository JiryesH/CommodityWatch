from __future__ import annotations

import asyncio
from datetime import timedelta, time
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.auth import BillingWebhookEvent, Subscription, User, UserSession
from app.db.models.content import CalendarEvent, CalendarEventChange, CalendarReviewItem, Headline, HeadlineIndicatorLink
from app.db.models.indicators import Indicator, IndicatorDependency, IndicatorModule, ModuleSnapshotCache, SeasonalRange
from app.db.models.observations import AppEvent, Observation
from app.db.models.reference import AppModule, Commodity, CommodityUnitConvention, Geography, UnitDefinition
from app.db.models.sources import IngestArtifact, IngestRun, ReleaseDefinition, Source, SourceRelease
from app.db.session import get_session_factory


BASE_DIR = Path(__file__).resolve().parents[1]
SEED_DIR = BASE_DIR / "seed"


def load_yaml(path: Path) -> list[dict[str, Any]]:
    loaded = yaml.safe_load(path.read_text())
    if not loaded:
        return []
    if not isinstance(loaded, list):
        raise ValueError(f"Expected list payload in {path}")
    return loaded


async def upsert_rows(session: AsyncSession, model: Any, rows: list[dict[str, Any]], conflict_cols: list[str]) -> None:
    if not rows:
        return
    table = model.__table__
    stmt = insert(table).values(rows)
    update_cols = {
        column.name: getattr(stmt.excluded, column.name)
        for column in table.columns
        if column.name not in conflict_cols
    }
    await session.execute(stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols))


async def seed_reference_tables(session: AsyncSession) -> None:
    await upsert_rows(session, AppModule, load_yaml(SEED_DIR / "app_modules.yml"), ["code"])
    await upsert_rows(session, Commodity, load_yaml(SEED_DIR / "commodities.yml"), ["code"])
    await upsert_rows(session, Geography, load_yaml(SEED_DIR / "geographies.yml"), ["code"])
    await upsert_rows(session, UnitDefinition, load_yaml(SEED_DIR / "units.yml"), ["code"])
    await upsert_rows(
        session,
        CommodityUnitConvention,
        load_yaml(SEED_DIR / "commodity_unit_conventions.yml"),
        ["commodity_code", "measure_family"],
    )


async def seed_sources(session: AsyncSession) -> dict[str, Any]:
    await upsert_rows(session, Source, load_yaml(SEED_DIR / "sources.yml"), ["slug"])
    result = await session.execute(select(Source.slug, Source.id))
    return {slug: source_id for slug, source_id in result.all()}


async def seed_release_definitions(session: AsyncSession, source_ids: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for item in load_yaml(SEED_DIR / "release_definitions.yml"):
        rows.append(
            {
                "slug": item["slug"],
                "source_id": source_ids[item["source_slug"]],
                "name": item["name"],
                "release_kind": item["release_kind"],
                "module_code": item.get("module_code"),
                "commodity_code": item.get("commodity_code"),
                "geography_code": item.get("geography_code"),
                "cadence": item["cadence"],
                "schedule_timezone": item["schedule_timezone"],
                "schedule_rule": item["schedule_rule"],
                "default_local_time": time.fromisoformat(item["default_local_time"]) if item.get("default_local_time") else None,
                "is_calendar_driven": item.get("is_calendar_driven", False),
                "active": item.get("active", True),
                "metadata": item.get("metadata", {}),
            }
        )
    await upsert_rows(session, ReleaseDefinition, rows, ["slug"])
    result = await session.execute(select(ReleaseDefinition.slug, ReleaseDefinition.id))
    return {slug: release_id for slug, release_id in result.all()}


async def seed_indicators(session: AsyncSession, source_ids: dict[str, Any]) -> None:
    indicator_files = sorted((SEED_DIR / "indicators").glob("*.yml"))
    rows: list[dict[str, Any]] = []
    module_rows: list[dict[str, Any]] = []

    for path in indicator_files:
        for item in load_yaml(path):
            rows.append(
                {
                    "code": item["code"],
                    "name": item["name"],
                    "description": item.get("description"),
                    "measure_family": item["measure_family"],
                    "frequency": item["frequency"],
                    "commodity_code": item.get("commodity_code"),
                    "geography_code": item.get("geography_code"),
                    "source_id": source_ids[item["source_slug"]],
                    "source_series_key": item.get("source_series_key"),
                    "native_unit_code": item.get("native_unit_code"),
                    "canonical_unit_code": item.get("canonical_unit_code"),
                    "default_observation_kind": item["default_observation_kind"],
                    "publication_lag": timedelta(days=item.get("publication_lag_days", 0)),
                    "seasonal_profile": item.get("seasonal_profile"),
                    "is_seasonal": item.get("is_seasonal", False),
                    "is_derived": item.get("is_derived", False),
                    "formula": item.get("formula"),
                    "visibility_tier": item.get("visibility_tier", "public"),
                    "active": item.get("active", True),
                    "metadata": item.get("metadata", {}),
                }
            )

    await upsert_rows(session, Indicator, rows, ["code"])
    result = await session.execute(select(Indicator.code, Indicator.id))
    indicator_ids = {code: indicator_id for code, indicator_id in result.all()}

    for path in indicator_files:
        for item in load_yaml(path):
            indicator_id = indicator_ids[item["code"]]
            for module_code in item.get("modules", []):
                module_rows.append(
                    {
                        "indicator_id": indicator_id,
                        "module_code": module_code,
                        "is_primary": module_code == item.get("primary_module"),
                    }
                )

    await session.execute(delete(IndicatorModule))
    await upsert_rows(session, IndicatorModule, module_rows, ["indicator_id", "module_code"])


async def main() -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await seed_reference_tables(session)
        source_ids = await seed_sources(session)
        await seed_release_definitions(session, source_ids)
        await seed_indicators(session, source_ids)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main())
