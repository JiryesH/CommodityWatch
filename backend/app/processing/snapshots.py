from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator, IndicatorModule, ModuleSnapshotCache, SeasonalRange
from app.db.models.observations import Observation
from app.modules.inventorywatch.trigger_rules import classify_inventory_signal
from app.repositories.observations import get_latest_and_prior


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def period_index_for(indicator: Indicator, observation: Observation) -> tuple[str, int]:
    if indicator.frequency.value == "weekly":
        return "week_of_year", min(observation.period_end_at.isocalendar().week, 52)
    if indicator.frequency.value == "monthly":
        return "month_of_year", observation.period_end_at.month
    if indicator.frequency.value == "daily":
        return "day_of_year", observation.period_end_at.timetuple().tm_yday
    raise ValueError(f"Unsupported frequency for snapshot: {indicator.frequency.value}")


async def recent_series(session: AsyncSession, indicator_id: UUID, points: int = 12) -> list[Observation]:
    result = await session.execute(
        select(Observation)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(points)
    )
    observations = list(result.scalars().all())
    observations.reverse()
    return observations


async def seasonal_context(
    session: AsyncSession,
    indicator: Indicator,
    latest: Observation,
    seasonal: SeasonalRange | None,
) -> dict[str, float | int | None]:
    window_start = datetime(latest.period_end_at.year - 4, 1, 1, tzinfo=timezone.utc)
    _period_type, target_period_index = period_index_for(indicator, latest)
    result = await session.execute(
        select(Observation)
        .where(
            Observation.indicator_id == indicator.id,
            Observation.is_latest.is_(True),
            Observation.period_end_at >= window_start,
        )
        .order_by(Observation.period_end_at.asc())
    )
    matching_values = [
        float(point.value_canonical)
        for point in result.scalars().all()
        if period_index_for(indicator, point)[1] == target_period_index
    ]

    return {
        "seasonal_low": min(matching_values) if matching_values else None,
        "seasonal_high": max(matching_values) if matching_values else None,
        "seasonal_median": float(seasonal.p50) if seasonal and seasonal.p50 is not None else None,
        "seasonal_samples": len(matching_values),
        "seasonal_p10": float(seasonal.p10) if seasonal and seasonal.p10 is not None else None,
        "seasonal_p25": float(seasonal.p25) if seasonal and seasonal.p25 is not None else None,
        "seasonal_p75": float(seasonal.p75) if seasonal and seasonal.p75 is not None else None,
        "seasonal_p90": float(seasonal.p90) if seasonal and seasonal.p90 is not None else None,
    }


async def build_inventorywatch_snapshot_payload(session: AsyncSession) -> dict:
    now = utcnow()
    result = await session.execute(
        select(Indicator)
        .join(IndicatorModule, IndicatorModule.indicator_id == Indicator.id)
        .where(IndicatorModule.module_code == "inventorywatch", Indicator.active.is_(True))
        .order_by(Indicator.code.asc())
    )
    indicators = list(result.scalars().all())

    cards = []
    for indicator in indicators:
        latest, prior = await get_latest_and_prior(session, indicator.id)
        if latest is None:
            continue
        period_type, period_index = period_index_for(indicator, latest)
        seasonal_result = await session.execute(
            select(SeasonalRange).where(
                SeasonalRange.indicator_id == indicator.id,
                SeasonalRange.period_type == period_type,
                SeasonalRange.period_index == period_index,
            )
        )
        seasonal = seasonal_result.scalar_one_or_none()
        context = await seasonal_context(session, indicator, latest, seasonal)
        deviation_abs = None
        deviation_zscore = None
        if seasonal and seasonal.p50 is not None:
            deviation_abs = float(latest.value_canonical) - float(seasonal.p50)
            if seasonal.stddev_value not in (None, 0):
                deviation_zscore = deviation_abs / float(seasonal.stddev_value)
        sparkline = [float(item.value_canonical) for item in await recent_series(session, indicator.id)]
        cards.append(
            {
                "indicator_id": str(latest.indicator_id),
                "code": indicator.code,
                "name": indicator.name,
                "commodity_code": indicator.commodity_code,
                "geography_code": indicator.geography_code,
                "latest_value": float(latest.value_canonical),
                "unit": latest.unit_canonical_code,
                "frequency": indicator.frequency.value,
                "change_abs": (float(latest.value_canonical) - float(prior.value_canonical)) if prior else None,
                "deviation_abs": deviation_abs,
                "signal": classify_inventory_signal(deviation_zscore),
                "sparkline": sparkline,
                "last_updated_at": latest.vintage_at.isoformat(),
                "stale": latest.release_date is None or (now - latest.release_date) > timedelta(days=14),
                "seasonal_low": context["seasonal_low"],
                "seasonal_high": context["seasonal_high"],
                "seasonal_median": context["seasonal_median"],
                "seasonal_samples": context["seasonal_samples"],
                "seasonal_p10": context["seasonal_p10"],
                "seasonal_p25": context["seasonal_p25"],
                "seasonal_p75": context["seasonal_p75"],
                "seasonal_p90": context["seasonal_p90"],
            }
        )

    return {
        "module": "inventorywatch",
        "generated_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=300)).isoformat(),
        "cards": cards,
    }


async def recompute_inventorywatch_snapshot(session: AsyncSession) -> dict:
    payload = await build_inventorywatch_snapshot_payload(session)
    as_of = utcnow()
    expires_at = as_of + timedelta(seconds=300)
    await session.merge(
        ModuleSnapshotCache(
            module_code="inventorywatch",
            snapshot_key="default",
            as_of=as_of,
            payload=payload,
            expires_at=expires_at,
        )
    )
    await session.flush()
    return payload


async def get_snapshot_payload(session: AsyncSession) -> dict:
    result = await session.execute(
        select(ModuleSnapshotCache).where(
            ModuleSnapshotCache.module_code == "inventorywatch",
            ModuleSnapshotCache.snapshot_key == "default",
        )
    )
    cached = result.scalar_one_or_none()
    if cached and cached.expires_at > utcnow():
        return cached.payload
    return await recompute_inventorywatch_snapshot(session)
