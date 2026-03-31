from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator, IndicatorModule
from app.db.models.observations import Observation


@dataclass(slots=True)
class IndicatorFilters:
    module: str | None = None
    commodity: str | None = None
    geography: str | None = None
    frequency: str | None = None
    measure_family: str | None = None
    visibility: str | None = "public"
    active: bool = True


def encode_cursor(code: str, indicator_id: uuid.UUID) -> str:
    payload = json.dumps({"code": code, "id": str(indicator_id)}).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("utf-8")


def decode_cursor(cursor: str | None) -> tuple[str, uuid.UUID] | None:
    if not cursor:
        return None
    payload = json.loads(base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8"))
    return payload["code"], uuid.UUID(payload["id"])


def apply_filters(stmt: Select, filters: IndicatorFilters) -> Select:
    if filters.module:
        stmt = stmt.where(IndicatorModule.module_code == filters.module)
    if filters.commodity:
        stmt = stmt.where(Indicator.commodity_code == filters.commodity)
    if filters.geography:
        stmt = stmt.where(Indicator.geography_code == filters.geography)
    if filters.frequency:
        stmt = stmt.where(Indicator.frequency == filters.frequency)
    if filters.measure_family:
        stmt = stmt.where(Indicator.measure_family == filters.measure_family)
    if filters.visibility:
        stmt = stmt.where(Indicator.visibility_tier == filters.visibility)
    stmt = stmt.where(Indicator.active.is_(filters.active))
    return stmt


async def list_indicators(
    session: AsyncSession,
    filters: IndicatorFilters,
    limit: int = 200,
    cursor: str | None = None,
) -> tuple[list[dict], str | None]:
    latest_release_subquery = (
        select(
            Observation.indicator_id.label("indicator_id"),
            func.max(Observation.release_date).label("latest_release_at"),
        )
        .where(Observation.is_latest.is_(True))
        .group_by(Observation.indicator_id)
        .subquery()
    )

    stmt = (
        select(
            Indicator,
            IndicatorModule.module_code,
            latest_release_subquery.c.latest_release_at,
        )
        .join(IndicatorModule, IndicatorModule.indicator_id == Indicator.id)
        .outerjoin(latest_release_subquery, latest_release_subquery.c.indicator_id == Indicator.id)
        .order_by(Indicator.code.asc(), Indicator.id.asc())
    )
    stmt = apply_filters(stmt, filters)

    decoded = decode_cursor(cursor)
    if decoded:
        code, indicator_id = decoded
        stmt = stmt.where(or_(Indicator.code > code, and_(Indicator.code == code, Indicator.id > indicator_id)))

    result = await session.execute(stmt.limit(limit + 1))
    rows = result.all()

    grouped: dict[uuid.UUID, dict] = {}
    for indicator, module_code, latest_release_at in rows[:limit]:
        entry = grouped.setdefault(
            indicator.id,
            {
                "id": indicator.id,
                "code": indicator.code,
                "name": indicator.name,
                "modules": [],
                "commodity_code": indicator.commodity_code,
                "geography_code": indicator.geography_code,
                "measure_family": indicator.measure_family.value,
                "frequency": indicator.frequency.value,
                "native_unit": indicator.native_unit_code,
                "canonical_unit": indicator.canonical_unit_code,
                "is_seasonal": indicator.is_seasonal,
                "is_derived": indicator.is_derived,
                "visibility_tier": indicator.visibility_tier.value,
                "latest_release_at": latest_release_at,
            },
        )
        entry["modules"].append(module_code.value if hasattr(module_code, "value") else module_code)

    next_cursor = None
    if len(rows) > limit:
        next_indicator = rows[limit][0]
        next_cursor = encode_cursor(next_indicator.code, next_indicator.id)

    return list(grouped.values()), next_cursor


async def get_indicator(session: AsyncSession, indicator_id: uuid.UUID) -> Indicator | None:
    result = await session.execute(select(Indicator).where(Indicator.id == indicator_id))
    return result.scalar_one_or_none()


async def get_indicator_by_code(session: AsyncSession, code: str) -> Indicator | None:
    result = await session.execute(select(Indicator).where(Indicator.code == code))
    return result.scalar_one_or_none()


async def get_indicator_modules(session: AsyncSession, indicator_id: uuid.UUID) -> list[str]:
    result = await session.execute(
        select(IndicatorModule.module_code).where(IndicatorModule.indicator_id == indicator_id).order_by(
            IndicatorModule.module_code.asc()
        )
    )
    return [module.value if hasattr(module, "value") else module for module in result.scalars().all()]


async def list_module_indicators(session: AsyncSession, module_code: str) -> list[Indicator]:
    result = await session.execute(
        select(Indicator)
        .join(IndicatorModule, IndicatorModule.indicator_id == Indicator.id)
        .where(IndicatorModule.module_code == module_code, Indicator.active.is_(True))
        .order_by(Indicator.code.asc())
    )
    return list(result.scalars().all())
