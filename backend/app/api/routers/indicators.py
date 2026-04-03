from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import SessionDep
from app.db.models.indicators import Indicator, IndicatorModule, SeasonalRange
from app.db.models.observations import Observation
from app.db.models.sources import SourceRelease
from app.processing.snapshots import period_index_for, public_inventorywatch_seasonality_allowed, seasonal_context
from app.repositories.indicators import IndicatorFilters, decode_cursor, encode_cursor, get_indicator, get_indicator_modules, list_indicators
from app.repositories.observations import collapse_vintages, downsample_observations, fetch_observations, get_latest_and_prior
from app.schemas.indicators import (
    IndicatorDataMetadata,
    IndicatorDataResponse,
    IndicatorLatestResponse,
    IndicatorListResponse,
    IndicatorListItem,
    IndicatorSeriesMeta,
    IndicatorSeriesPoint,
    LatestPoint,
    SeasonalRangePoint,
)


router = APIRouter(prefix="/api/indicators", tags=["indicators"])


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def resolve_downsample(downsample: str, points: list[Observation]) -> str:
    if downsample != "auto":
        return downsample
    if len(points) > 500:
        return "weekly"
    return "raw"


async def list_inventorywatch_indicators(
    session: AsyncSession,
    filters: IndicatorFilters,
    limit: int,
    cursor: str | None,
) -> tuple[list[dict], str | None]:
    seasonal_point_count_subquery = (
        select(func.count(SeasonalRange.id))
        .where(
            SeasonalRange.indicator_id == Indicator.id,
            SeasonalRange.profile_name == func.coalesce(Indicator.seasonal_profile, "inventorywatch_5y"),
        )
        .correlate(Indicator)
        .scalar_subquery()
    )
    latest_release_subquery = (
        select(
            Observation.indicator_id.label("indicator_id"),
            func.max(Observation.release_date).label("latest_release_at"),
        )
        .where(Observation.is_latest.is_(True))
        .group_by(Observation.indicator_id)
        .subquery()
    )
    observation_stats_subquery = (
        select(
            Observation.indicator_id.label("indicator_id"),
            func.count(Observation.id).label("observation_count"),
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
            observation_stats_subquery.c.observation_count,
            seasonal_point_count_subquery.label("seasonal_point_count"),
        )
        .join(IndicatorModule, IndicatorModule.indicator_id == Indicator.id)
        .join(observation_stats_subquery, observation_stats_subquery.c.indicator_id == Indicator.id)
        .outerjoin(latest_release_subquery, latest_release_subquery.c.indicator_id == Indicator.id)
        .order_by(Indicator.code.asc(), Indicator.id.asc())
    )

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

    decoded = decode_cursor(cursor)
    if decoded:
        code, indicator_id = decoded
        stmt = stmt.where(or_(Indicator.code > code, and_(Indicator.code == code, Indicator.id > indicator_id)))

    result = await session.execute(stmt.limit(limit + 1))
    rows = result.all()

    grouped: dict[UUID, dict] = {}
    for indicator, module_code, latest_release_at, observation_count, seasonal_point_count in rows[:limit]:
        public_is_seasonal = public_inventorywatch_seasonality_allowed(
            indicator,
            int(observation_count or 0),
            int(seasonal_point_count or 0),
        )
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
                "is_seasonal": public_is_seasonal,
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


@router.get("", response_model=IndicatorListResponse)
async def list_indicator_metadata(
    session: SessionDep,
    module: str | None = None,
    commodity: str | None = None,
    geography: str | None = None,
    frequency: str | None = None,
    measure_family: str | None = None,
    visibility: str = "public",
    active: bool = True,
    limit: int = Query(default=200, le=500),
    cursor: str | None = None,
) -> IndicatorListResponse:
    filters = IndicatorFilters(
        module=module,
        commodity=commodity,
        geography=geography,
        frequency=frequency,
        measure_family=measure_family,
        visibility=visibility,
        active=active,
    )
    if module == "inventorywatch":
        items, next_cursor = await list_inventorywatch_indicators(session, filters, limit, cursor)
    else:
        items, next_cursor = await list_indicators(session, filters, limit=limit, cursor=cursor)
    return IndicatorListResponse(items=[IndicatorListItem.model_validate(item) for item in items], next_cursor=next_cursor)


@router.get("/{indicator_id}/data", response_model=IndicatorDataResponse)
async def indicator_data(
    indicator_id: UUID,
    session: SessionDep,
    start_date: date | None = None,
    end_date: date | None = None,
    downsample: str = "auto",
    vintage: str = "latest",
    as_of: datetime | None = None,
    include_seasonal: bool = True,
    seasonal_profile: str | None = None,
    limit_points: int = Query(default=2000, le=5000),
) -> IndicatorDataResponse:
    indicator = await get_indicator(session, indicator_id)
    if indicator is None:
        raise HTTPException(status_code=404, detail="Indicator not found.")

    start_date = start_date or (utc_today() - timedelta(days=365))
    end_date = end_date or utc_today()
    observations = await fetch_observations(session, indicator_id, start_date, end_date)
    collapsed = collapse_vintages(observations, vintage=vintage, as_of=as_of)
    sampled = downsample_observations(collapsed, resolve_downsample(downsample, collapsed))
    sampled = sampled[-limit_points:]
    modules = await get_indicator_modules(session, indicator.id)

    if not sampled:
        raise HTTPException(status_code=404, detail="No observations found.")

    latest = sampled[-1] if sampled else None
    latest_release = None
    latest_release_at = None
    source_url = None
    if latest and latest.release_id:
        latest_release = latest.release_id
        latest_release_at = latest.release_date
        source_release = await session.scalar(select(SourceRelease).where(SourceRelease.id == latest.release_id))
        source_url = source_release.source_url if source_release else None
    if source_url is None:
        source_url = indicator.metadata_.get("source_url")

    public_is_seasonal = False
    seasonal_rows: list[SeasonalRange] = []
    profile = seasonal_profile or indicator.seasonal_profile or "inventorywatch_5y"
    if latest:
        seasonal_result = await session.execute(
            select(SeasonalRange)
            .where(SeasonalRange.indicator_id == indicator.id, SeasonalRange.profile_name == profile)
            .order_by(SeasonalRange.period_index.asc())
        )
        seasonal_rows = list(seasonal_result.scalars().all())
        period_type, period_index = period_index_for(indicator, latest)
        seasonal = next(
            (item for item in seasonal_rows if item.period_type == period_type and item.period_index == period_index),
            None,
        )
        context = await seasonal_context(session, indicator, latest, seasonal)
        public_is_seasonal = public_inventorywatch_seasonality_allowed(
            indicator,
            len(sampled),
            len(seasonal_rows),
            int(context["seasonal_samples"] or 0),
        )

    seasonal_points: list[SeasonalRangePoint] = []
    if include_seasonal and public_is_seasonal:
        seasonal_points = [
            SeasonalRangePoint(
                period_index=item.period_index,
                p10=float(item.p10) if item.p10 is not None else None,
                p25=float(item.p25) if item.p25 is not None else None,
                p50=float(item.p50) if item.p50 is not None else None,
                p75=float(item.p75) if item.p75 is not None else None,
                p90=float(item.p90) if item.p90 is not None else None,
                mean=float(item.mean_value) if item.mean_value is not None else None,
                stddev=float(item.stddev_value) if item.stddev_value is not None else None,
            )
            for item in seasonal_rows
        ]

    return IndicatorDataResponse(
        indicator=IndicatorSeriesMeta(
            id=indicator.id,
            code=indicator.code,
            name=indicator.name,
            description=indicator.description,
            modules=modules,
            commodity_code=indicator.commodity_code,
            geography_code=indicator.geography_code,
            frequency=indicator.frequency.value,
            measure_family=indicator.measure_family.value,
            unit=indicator.canonical_unit_code,
            period_type=indicator.metadata_.get("period_type"),
            marketing_year_start_month=indicator.metadata_.get("marketing_year_start_month"),
            is_seasonal=public_is_seasonal,
        ),
        series=[
            IndicatorSeriesPoint(
                period_start_at=item.period_start_at,
                period_end_at=item.period_end_at,
                release_date=item.release_date,
                vintage_at=item.vintage_at,
                value=float(item.value_canonical),
                unit=item.unit_canonical_code,
                observation_kind=item.observation_kind.value,
                revision_sequence=item.revision_sequence,
            )
            for item in sampled
        ],
        seasonal_range=seasonal_points,
        metadata=IndicatorDataMetadata(
            latest_release_id=latest_release,
            latest_release_at=latest_release_at,
            source_url=source_url,
        ),
    )


@router.get("/{indicator_id}/latest", response_model=IndicatorLatestResponse)
async def indicator_latest(indicator_id: UUID, session: SessionDep) -> IndicatorLatestResponse:
    indicator = await get_indicator(session, indicator_id)
    if indicator is None:
        raise HTTPException(status_code=404, detail="Indicator not found.")

    latest, prior = await get_latest_and_prior(session, indicator_id)
    if latest is None:
        raise HTTPException(status_code=404, detail="No observations found.")

    deviation_abs = None
    deviation_zscore = None
    if indicator.is_seasonal:
        profile = indicator.seasonal_profile or "inventorywatch_5y"
        seasonal_rows_result = await session.execute(
            select(SeasonalRange)
            .where(SeasonalRange.indicator_id == indicator.id, SeasonalRange.profile_name == profile)
            .order_by(SeasonalRange.period_index.asc())
        )
        seasonal_rows = list(seasonal_rows_result.scalars().all())
        period_type, period_index = period_index_for(indicator, latest)
        seasonal = next(
            (item for item in seasonal_rows if item.period_type == period_type and item.period_index == period_index),
            None,
        )
        if seasonal and seasonal.p50 is not None:
            context = await seasonal_context(session, indicator, latest, seasonal)
            if public_inventorywatch_seasonality_allowed(
                indicator,
                1,
                len(seasonal_rows),
                int(context["seasonal_samples"] or 0),
            ):
                deviation_abs = float(latest.value_canonical) - float(seasonal.p50)
                if seasonal.stddev_value not in (None, 0):
                    deviation_zscore = deviation_abs / float(seasonal.stddev_value)
    change_abs = (float(latest.value_canonical) - float(prior.value_canonical)) if prior else None
    change_pct = ((change_abs / float(prior.value_canonical)) * 100) if prior and prior.value_canonical else None
    return IndicatorLatestResponse(
        indicator={"id": indicator.id, "code": indicator.code},
        latest=LatestPoint(
            period_end_at=latest.period_end_at,
            release_date=latest.release_date,
            value=float(latest.value_canonical),
            unit=latest.unit_canonical_code,
            change_from_prior_abs=change_abs,
            change_from_prior_pct=change_pct,
            deviation_from_seasonal_abs=deviation_abs,
            deviation_from_seasonal_zscore=deviation_zscore,
            revision_sequence=latest.revision_sequence,
        ),
    )
