from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime
from math import floor
from statistics import mean, pstdev
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator, IndicatorModule, SeasonalRange
from app.db.models.observations import Observation


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct
    lower = floor(rank)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = rank - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * weight)


def seasonal_period(observation: Observation, frequency: str) -> tuple[str, int]:
    if frequency == "weekly":
        return "week_of_year", min(observation.period_end_at.isocalendar().week, 52)
    if frequency == "monthly":
        return "month_of_year", observation.period_end_at.month
    if frequency == "daily":
        return "day_of_year", observation.period_end_at.timetuple().tm_yday
    raise ValueError(f"Unsupported seasonal frequency: {frequency}")


async def compute_seasonal_ranges(
    session: AsyncSession,
    indicator_scope: str | None = None,
    indicator_ids: list[UUID] | None = None,
    exclude_years: set[int] | None = None,
    profile_name: str = "inventorywatch_5y",
) -> None:
    exclude_years = exclude_years or set()

    stmt = select(Indicator).where(Indicator.is_seasonal.is_(True))
    if indicator_ids:
        stmt = stmt.where(Indicator.id.in_(indicator_ids))
    if indicator_scope:
        stmt = stmt.join(IndicatorModule, IndicatorModule.indicator_id == Indicator.id).where(
            IndicatorModule.module_code == indicator_scope
        )
    result = await session.execute(stmt.order_by(Indicator.code.asc()))
    indicators = list(result.scalars().unique().all())

    for indicator in indicators:
        obs_result = await session.execute(
            select(Observation)
            .where(Observation.indicator_id == indicator.id, Observation.is_latest.is_(True))
            .order_by(Observation.period_end_at.asc())
        )
        observations = list(obs_result.scalars().all())
        observations = [obs for obs in observations if obs.period_end_at.year not in exclude_years]
        buckets: dict[tuple[str, int], list[float]] = defaultdict(list)

        for observation in observations:
            period_type, period_index = seasonal_period(observation, indicator.frequency.value)
            buckets[(period_type, period_index)].append(float(observation.value_canonical))

        await session.execute(
            delete(SeasonalRange).where(
                SeasonalRange.indicator_id == indicator.id,
                SeasonalRange.profile_name == profile_name,
            )
        )

        if not buckets:
            continue

        years = sorted({obs.period_end_at.year for obs in observations})
        for (period_type, period_index), values in sorted(buckets.items(), key=lambda item: item[0][1]):
            session.add(
                SeasonalRange(
                    indicator_id=indicator.id,
                    profile_name=profile_name,
                    period_type=period_type,
                    period_index=period_index,
                    sample_size=len(values),
                    range_start_year=years[0] if years else None,
                    range_end_year=years[-1] if years else None,
                    p10=percentile(values, 0.10),
                    p25=percentile(values, 0.25),
                    p50=percentile(values, 0.50),
                    p75=percentile(values, 0.75),
                    p90=percentile(values, 0.90),
                    mean_value=mean(values) if values else None,
                    stddev_value=pstdev(values) if len(values) > 1 else 0.0,
                    metadata_={"excluded_years": sorted(exclude_years)},
                )
            )

    await session.flush()


async def run_cli(indicator_scope: str | None, exclude_years: set[int]) -> None:
    from app.db.session import get_session_factory

    session_factory = get_session_factory()
    async with session_factory() as session:
        await compute_seasonal_ranges(session, indicator_scope=indicator_scope, exclude_years=exclude_years)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recompute seasonal ranges.")
    parser.add_argument("--indicator-scope", default=None)
    parser.add_argument("--exclude-year", action="append", default=[])
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exclude_years = {int(year) for year in args.exclude_year}
    asyncio.run(run_cli(args.indicator_scope, exclude_years))


if __name__ == "__main__":
    main()
