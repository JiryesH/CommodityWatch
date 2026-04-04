from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.demand import DemandSeries, DemandVertical
from app.db.models.indicators import Indicator, ModuleSnapshotCache
from app.db.models.observations import Observation
from app.db.models.reference import UnitDefinition
from app.db.models.sources import ReleaseDefinition, Source, SourceRelease
from app.modules.demandwatch.published_store import (
    DemandObservation,
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
    build_latest_metrics_map,
    build_observation,
    utcnow,
    write_published_demand_store,
)


async def load_demandwatch_bundle(session: AsyncSession) -> DemandStoreBundle:
    unit_result = await session.execute(select(UnitDefinition).order_by(UnitDefinition.code.asc()))
    units_by_code = {
        unit.code: DemandUnitDefinition(code=unit.code, name=unit.name, symbol=unit.symbol)
        for unit in unit_result.scalars().all()
    }

    vertical_result = await session.execute(select(DemandVertical).order_by(DemandVertical.display_order.asc(), DemandVertical.code.asc()))
    verticals_by_code = {
        vertical.code: DemandVerticalDefinition(
            code=vertical.code,
            name=vertical.name,
            commodity_code=vertical.commodity_code,
            sector=vertical.sector.value if hasattr(vertical.sector, "value") else str(vertical.sector),
            nav_label=vertical.nav_label,
            short_label=vertical.short_label,
            description=vertical.description,
            display_order=int(vertical.display_order),
            active=bool(vertical.active),
            metadata=dict(vertical.metadata_ or {}),
        )
        for vertical in vertical_result.scalars().all()
    }

    series_result = await session.execute(
        select(DemandSeries, Indicator, Source, ReleaseDefinition)
        .join(Indicator, Indicator.id == DemandSeries.indicator_id)
        .outerjoin(Source, Source.id == Indicator.source_id)
        .outerjoin(ReleaseDefinition, ReleaseDefinition.id == DemandSeries.release_definition_id)
        .order_by(DemandSeries.vertical_code.asc(), DemandSeries.display_order.asc(), Indicator.code.asc())
    )
    series_by_id: dict[str, DemandSeriesDefinition] = {}
    series_by_indicator_id: dict[object, DemandSeriesDefinition] = {}
    for demand_series, indicator, source, release_definition in series_result.all():
        native_unit = units_by_code.get(indicator.native_unit_code) if indicator.native_unit_code else None
        canonical_unit = units_by_code.get(indicator.canonical_unit_code) if indicator.canonical_unit_code else None
        if source is None or source.legal_status is None:
            source_legal_status = None
        else:
            source_legal_status = source.legal_status.value if hasattr(source.legal_status, "value") else str(source.legal_status)
        series = DemandSeriesDefinition(
            id=str(demand_series.id),
            indicator_id=str(indicator.id),
            code=indicator.code,
            name=indicator.name,
            description=indicator.description,
            vertical_code=demand_series.vertical_code,
            tier=demand_series.indicator_tier.value
            if hasattr(demand_series.indicator_tier, "value")
            else str(demand_series.indicator_tier),
            coverage_status=demand_series.coverage_status.value
            if hasattr(demand_series.coverage_status, "value")
            else str(demand_series.coverage_status),
            display_order=int(demand_series.display_order),
            notes=demand_series.notes,
            measure_family=indicator.measure_family.value if hasattr(indicator.measure_family, "value") else str(indicator.measure_family),
            frequency=indicator.frequency.value if hasattr(indicator.frequency, "value") else str(indicator.frequency),
            commodity_code=indicator.commodity_code,
            geography_code=indicator.geography_code,
            source_slug=source.slug if source is not None else "",
            source_name=source.name if source is not None else None,
            source_legal_status=source_legal_status,
            source_url=(
                (indicator.metadata_ or {}).get("source_url")
                or ((release_definition.metadata_ or {}).get("landing_url") if release_definition is not None else None)
                or (source.homepage_url if source is not None else None)
            ),
            source_series_key=indicator.source_series_key,
            native_unit_code=indicator.native_unit_code,
            native_unit_symbol=native_unit.symbol if native_unit is not None else indicator.native_unit_code,
            canonical_unit_code=indicator.canonical_unit_code,
            canonical_unit_symbol=canonical_unit.symbol if canonical_unit is not None else indicator.canonical_unit_code,
            default_observation_kind=(
                indicator.default_observation_kind.value
                if hasattr(indicator.default_observation_kind, "value")
                else str(indicator.default_observation_kind)
            ),
            visibility_tier=indicator.visibility_tier.value if hasattr(indicator.visibility_tier, "value") else str(indicator.visibility_tier),
            active=bool(indicator.active),
            metadata={
                **dict(indicator.metadata_ or {}),
                **dict(demand_series.metadata_ or {}),
                "coverage_note": demand_series.notes,
            },
        )
        series_by_id[series.id] = series
        series_by_indicator_id[indicator.id] = series

    observation_result = await session.execute(
        select(Observation, SourceRelease)
        .join(DemandSeries, DemandSeries.indicator_id == Observation.indicator_id)
        .outerjoin(SourceRelease, SourceRelease.id == Observation.release_id)
        .order_by(Observation.indicator_id.asc(), Observation.period_end_at.asc(), Observation.vintage_at.asc())
    )
    observations_by_series_id: dict[str, list[DemandObservation]] = {}
    for observation, source_release in observation_result.all():
        series = series_by_indicator_id.get(observation.indicator_id)
        if series is None:
            continue
        observations_by_series_id.setdefault(series.id, []).append(
            build_observation(
                series,
                observation_id=str(observation.id),
                period_start_at=observation.period_start_at,
                period_end_at=observation.period_end_at,
                release_date=observation.release_date,
                vintage_at=observation.vintage_at,
                value_native=float(observation.value_native),
                unit_native_code=observation.unit_native_code,
                value_canonical=float(observation.value_canonical),
                unit_canonical_code=observation.unit_canonical_code,
                observation_kind=(
                    observation.observation_kind.value
                    if hasattr(observation.observation_kind, "value")
                    else str(observation.observation_kind)
                ),
                revision_sequence=int(observation.revision_sequence),
                is_latest=bool(observation.is_latest),
                source_release_id=str(observation.release_id) if observation.release_id else None,
                source_url=(source_release.source_url if source_release is not None else None) or series.source_url,
                metadata=dict(observation.metadata_ or {}),
            )
        )

    latest_metrics_by_series_id = build_latest_metrics_map(series_by_id, observations_by_series_id)
    return DemandStoreBundle(
        units_by_code=units_by_code,
        verticals_by_code=verticals_by_code,
        series_by_id=series_by_id,
        observations_by_series_id=observations_by_series_id,
        latest_metrics_by_series_id=latest_metrics_by_series_id,
    )


def build_demandwatch_snapshot_payload(bundle: DemandStoreBundle) -> dict[str, Any]:
    generated_at = utcnow()
    vertical_cards: list[dict[str, Any]] = []
    for vertical in sorted(bundle.verticals_by_code.values(), key=lambda item: (item.display_order, item.code)):
        related_series = [
            series
            for series in sorted(bundle.series_by_id.values(), key=lambda item: (item.display_order, item.code))
            if series.vertical_code == vertical.code and series.coverage_status == "live"
        ]
        primary_series = next((series for series in related_series if series.tier == "t1_direct"), None)
        if primary_series is None and related_series:
            primary_series = related_series[0]
        if primary_series is None:
            continue
        metrics = bundle.latest_metrics_by_series_id[primary_series.id]
        vertical_cards.append(
            {
                "vertical_code": vertical.code,
                "vertical_name": vertical.name,
                "series_code": primary_series.code,
                "series_name": primary_series.name,
                "tier": primary_series.tier,
                "latest_value": metrics.latest_value,
                "unit_symbol": metrics.unit_symbol,
                "yoy_pct": metrics.yoy_pct,
                "yoy_abs": metrics.yoy_abs,
                "moving_average_4w": metrics.moving_average_4w,
                "trend_3m_direction": metrics.trend_3m_direction,
                "latest_period_label": metrics.latest_period_label,
                "freshness_state": metrics.freshness_state,
                "surprise_flag": metrics.surprise_flag,
            }
        )

    movers = [
        {
            "vertical_code": series.vertical_code,
            "code": series.code,
            "name": series.name,
            "tier": series.tier,
            "latest_period_label": metrics.latest_period_label,
            "latest_release_date": metrics.latest_release_date.isoformat() if metrics.latest_release_date else None,
            "latest_value": metrics.latest_value,
            "unit_symbol": metrics.unit_symbol,
            "change_abs": metrics.change_abs,
            "yoy_pct": metrics.yoy_pct,
            "surprise_flag": metrics.surprise_flag,
            "surprise_reason": metrics.surprise_reason,
        }
        for series, metrics in (
            (bundle.series_by_id[series_id], bundle.latest_metrics_by_series_id[series_id])
            for series_id in bundle.series_by_id
        )
        if metrics.latest_release_date is not None
    ]
    movers.sort(key=lambda item: item["latest_release_date"] or "", reverse=True)

    return {
        "module": "demandwatch",
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(seconds=300)).isoformat(),
        "scorecard": vertical_cards,
        "movers": movers[:10],
    }


async def recompute_demandwatch_snapshot(session: AsyncSession) -> dict[str, Any]:
    bundle = await load_demandwatch_bundle(session)
    payload = build_demandwatch_snapshot_payload(bundle)
    as_of = utcnow()
    expires_at = as_of + timedelta(seconds=300)
    await session.merge(
        ModuleSnapshotCache(
            module_code="demandwatch",
            snapshot_key="default",
            as_of=as_of,
            payload=payload,
            expires_at=expires_at,
        )
    )
    await session.flush()
    return payload


async def publish_demandwatch_store(session: AsyncSession, output_path: Path) -> dict[str, Any]:
    bundle = await load_demandwatch_bundle(session)
    return write_published_demand_store(bundle, output_path)
