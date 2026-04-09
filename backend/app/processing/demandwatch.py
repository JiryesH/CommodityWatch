from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.demand import DemandSeries, DemandVertical
from app.db.models.indicators import Indicator, ModuleSnapshotCache
from app.db.models.observations import Observation
from app.db.models.reference import UnitDefinition
from app.db.models.sources import ReleaseDefinition, Source, SourceRelease
from app.modules.demandwatch.presentation import (
    DemandReleaseSchedule,
    build_demandwatch_bootstrap_payload,
)
from app.modules.demandwatch.published_store import (
    DemandObservation,
    DemandSeriesDefinition,
    DemandStoreBundle,
    DemandUnitDefinition,
    DemandVerticalDefinition,
    PublishedDemandRepository,
    build_latest_metrics_map,
    build_observation,
    utcnow,
    write_published_demand_store,
)


SETUP_INSTRUCTIONS = "Run `alembic upgrade head` and `python scripts/seed_reference_data.py` against the backend database."
PUBLISHED_STORE_SETUP_INSTRUCTIONS = (
    "Run `python -m app.modules.demandwatch.cli publish` to create the DemandWatch published store."
)
DEMANDWATCH_SNAPSHOT_TTL = timedelta(seconds=300)
DEMANDWATCH_SNAPSHOT_MOVERS_LIMIT = 50
DEMANDWATCH_PUBLISHED_STORE_MAX_AGE = timedelta(days=14)


@dataclass(frozen=True, slots=True)
class DemandWatchPublicReadModel:
    bundle: DemandStoreBundle
    generated_at: datetime
    database_path: Path


@dataclass(slots=True)
class _CachedDemandWatchReadModel:
    database_path: Path
    signature: tuple[int, int, int]
    read_model: DemandWatchPublicReadModel


_demandwatch_public_read_model_cache: _CachedDemandWatchReadModel | None = None
_demandwatch_public_read_model_lock = Lock()


class DemandWatchSetupError(RuntimeError):
    pass


def _demandwatch_public_artifact_path() -> Path:
    return get_settings().artifact_root / "demandwatch" / "published.sqlite"


def _published_store_signature(path: Path) -> tuple[int, int, int]:
    stat_result = path.stat()
    return (int(stat_result.st_ino), int(stat_result.st_size), int(stat_result.st_mtime_ns))


def clear_demandwatch_public_read_model_cache() -> None:
    global _demandwatch_public_read_model_cache
    with _demandwatch_public_read_model_lock:
        _demandwatch_public_read_model_cache = None


def _published_store_unavailable(path: Path, exc: Exception) -> DemandWatchSetupError:
    return DemandWatchSetupError(
        f"DemandWatch published store is unavailable at {path}. "
        f"{PUBLISHED_STORE_SETUP_INSTRUCTIONS} "
        f"Details: {exc}"
    )


def _published_store_invalid(path: Path, exc: Exception) -> DemandWatchSetupError:
    return DemandWatchSetupError(
        f"DemandWatch published store at {path} is invalid. "
        f"{PUBLISHED_STORE_SETUP_INSTRUCTIONS} "
        f"Details: {exc}"
    )


def _format_duration_label(value: timedelta) -> str:
    total_hours = max(0, int(value.total_seconds() // 3600))
    if total_hours >= 48 and total_hours % 24 == 0:
        return f"{total_hours // 24}d"
    return f"{total_hours}h"


def _published_store_stale(path: Path, *, published_at: datetime, now: datetime | None = None) -> DemandWatchSetupError:
    reference_now = now or utcnow()
    age = max(reference_now - published_at, timedelta())
    return DemandWatchSetupError(
        f"DemandWatch published store at {path} is stale. "
        f"Published at {published_at.isoformat()} ({_format_duration_label(age)} old); "
        f"maximum allowed age is {_format_duration_label(DEMANDWATCH_PUBLISHED_STORE_MAX_AGE)}. "
        f"{PUBLISHED_STORE_SETUP_INSTRUCTIONS}"
    )


def _normalized_iso_timestamp(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value
    return None


def load_demandwatch_public_read_model() -> DemandWatchPublicReadModel:
    path = _demandwatch_public_artifact_path()
    try:
        signature = _published_store_signature(path)
    except FileNotFoundError as exc:
        raise _published_store_unavailable(path, exc) from exc

    global _demandwatch_public_read_model_cache
    with _demandwatch_public_read_model_lock:
        cached = _demandwatch_public_read_model_cache
        if cached is not None and cached.database_path == path and cached.signature == signature:
            return cached.read_model

        try:
            repository = PublishedDemandRepository(path)
        except FileNotFoundError as exc:
            raise _published_store_unavailable(path, exc) from exc
        except (sqlite3.DatabaseError, ValueError) as exc:
            raise _published_store_invalid(path, exc) from exc

        if repository.published_at is None:
            raise _published_store_invalid(path, ValueError("published_at metadata is missing."))
        if utcnow() - repository.published_at > DEMANDWATCH_PUBLISHED_STORE_MAX_AGE:
            raise _published_store_stale(path, published_at=repository.published_at)

        read_model = DemandWatchPublicReadModel(
            bundle=repository.to_bundle(),
            generated_at=repository.published_at,
            database_path=path,
        )
        _demandwatch_public_read_model_cache = _CachedDemandWatchReadModel(
            database_path=path,
            signature=signature,
            read_model=read_model,
        )
        return read_model


async def assert_demandwatch_registry_seeded(session: AsyncSession) -> None:
    try:
        vertical_count = await session.scalar(select(func.count()).select_from(DemandVertical))
        series_count = await session.scalar(select(func.count()).select_from(DemandSeries))
    except ProgrammingError as exc:
        raise DemandWatchSetupError(
            "DemandWatch tables are missing from the backend database. "
            f"{SETUP_INSTRUCTIONS}"
        ) from exc

    if int(vertical_count or 0) <= 0 or int(series_count or 0) <= 0:
        raise DemandWatchSetupError(
            "DemandWatch reference data is missing from the backend database. "
            f"{SETUP_INSTRUCTIONS}"
        )


async def load_demandwatch_bundle(session: AsyncSession) -> DemandStoreBundle:
    await assert_demandwatch_registry_seeded(session)

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


async def load_demandwatch_release_schedules(session: AsyncSession) -> list[DemandReleaseSchedule]:
    release_rows = (
        await session.execute(
            select(ReleaseDefinition, Source)
            .join(Source, Source.id == ReleaseDefinition.source_id)
            .where(ReleaseDefinition.module_code == "demandwatch", ReleaseDefinition.active.is_(True))
            .order_by(ReleaseDefinition.slug.asc())
        )
    ).all()

    if not release_rows:
        return []

    release_ids = [release.id for release, _source in release_rows]
    latest_release_rows = (
        await session.execute(
            select(
                SourceRelease.release_definition_id,
                func.max(SourceRelease.released_at).label("latest_release_at"),
            )
            .where(SourceRelease.release_definition_id.in_(release_ids))
            .group_by(SourceRelease.release_definition_id)
        )
    ).all()
    latest_release_by_definition_id = {
        release_definition_id: latest_release_at
        for release_definition_id, latest_release_at in latest_release_rows
    }

    series_rows = (
        await session.execute(
            select(ReleaseDefinition.id, DemandSeries.vertical_code, Indicator.code)
            .join(DemandSeries, DemandSeries.release_definition_id == ReleaseDefinition.id)
            .join(Indicator, Indicator.id == DemandSeries.indicator_id)
            .where(
                ReleaseDefinition.id.in_(release_ids),
                Indicator.active.is_(True),
                DemandSeries.coverage_status == "live",
            )
            .order_by(ReleaseDefinition.id.asc(), DemandSeries.display_order.asc(), Indicator.code.asc())
        )
    ).all()

    grouped_verticals: dict[object, set[str]] = {}
    grouped_series_codes: dict[object, list[str]] = {}
    for release_definition_id, vertical_code, series_code in series_rows:
        grouped_verticals.setdefault(release_definition_id, set()).add(vertical_code)
        grouped_series_codes.setdefault(release_definition_id, []).append(series_code)

    schedules: list[DemandReleaseSchedule] = []
    for release, source in release_rows:
        series_codes = tuple(dict.fromkeys(grouped_series_codes.get(release.id, [])))
        vertical_codes = tuple(sorted(grouped_verticals.get(release.id, set())))
        if not series_codes or not vertical_codes:
            continue
        schedules.append(
            DemandReleaseSchedule(
                release_slug=release.slug,
                release_name=release.name,
                source_slug=source.slug,
                source_name=source.name,
                cadence=release.cadence,
                schedule_timezone=release.schedule_timezone,
                schedule_rule=release.schedule_rule,
                default_local_time=release.default_local_time,
                is_calendar_driven=bool(release.is_calendar_driven),
                source_url=(release.metadata_ or {}).get("landing_url") or source.homepage_url,
                latest_release_at=latest_release_by_definition_id.get(release.id),
                vertical_codes=vertical_codes,
                series_codes=series_codes,
            )
        )
    return schedules


def build_demandwatch_snapshot_payload(
    bundle: DemandStoreBundle,
    schedules: list[DemandReleaseSchedule],
    *,
    generated_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> dict[str, Any]:
    payload_generated_at = generated_at or utcnow()
    payload = build_demandwatch_bootstrap_payload(
        bundle,
        schedules,
        now=payload_generated_at,
        expires_at=expires_at or (utcnow() + DEMANDWATCH_SNAPSHOT_TTL),
        movers_limit=DEMANDWATCH_SNAPSHOT_MOVERS_LIMIT,
    )
    return jsonable_encoder(payload)


def demandwatch_snapshot_payload_is_complete(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    required_fields = {
        "module",
        "generated_at",
        "expires_at",
        "macro_strip",
        "scorecard",
        "movers",
        "coverage_notes",
        "vertical_details",
        "vertical_errors",
        "next_release_dates",
    }
    if payload.get("module") != "demandwatch" or not required_fields.issubset(payload):
        return False

    generated_at = _normalized_iso_timestamp(payload.get("generated_at"))
    expires_at = _normalized_iso_timestamp(payload.get("expires_at"))
    if generated_at is None or expires_at is None:
        return False

    section_specs = (
        ("macro_strip", "items"),
        ("scorecard", "items"),
        ("movers", "items"),
        ("coverage_notes", "verticals"),
        ("next_release_dates", "items"),
    )
    for section_name, items_key in section_specs:
        section = payload.get(section_name)
        if not isinstance(section, dict):
            return False
        if _normalized_iso_timestamp(section.get("generated_at")) != generated_at:
            return False
        if not isinstance(section.get(items_key), list):
            return False

    coverage_notes = payload["coverage_notes"]
    if not isinstance(coverage_notes.get("summary"), dict):
        return False

    vertical_details = payload.get("vertical_details")
    if not isinstance(vertical_details, list):
        return False
    for detail in vertical_details:
        if not isinstance(detail, dict):
            return False
        if _normalized_iso_timestamp(detail.get("generated_at")) != generated_at:
            return False
        if not isinstance(detail.get("sections"), list):
            return False

    vertical_errors = payload.get("vertical_errors")
    if not isinstance(vertical_errors, list):
        return False
    for item in vertical_errors:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("vertical_id"), str) or not isinstance(item.get("message"), str):
            return False

    return True


async def recompute_demandwatch_snapshot(session: AsyncSession) -> dict[str, Any]:
    read_model = load_demandwatch_public_read_model()
    schedules = await load_demandwatch_release_schedules(session)
    as_of = utcnow()
    expires_at = as_of + DEMANDWATCH_SNAPSHOT_TTL
    payload = build_demandwatch_snapshot_payload(
        read_model.bundle,
        schedules,
        generated_at=read_model.generated_at,
        expires_at=expires_at,
    )
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


async def get_demandwatch_snapshot_payload(session: AsyncSession) -> dict[str, Any]:
    await assert_demandwatch_registry_seeded(session)
    read_model = load_demandwatch_public_read_model()

    result = await session.execute(
        select(ModuleSnapshotCache).where(
            ModuleSnapshotCache.module_code == "demandwatch",
            ModuleSnapshotCache.snapshot_key == "default",
        )
    )
    cached = result.scalar_one_or_none()
    cached_generated_at = None
    if isinstance(getattr(cached, "payload", None), dict):
        raw_generated_at = cached.payload.get("generated_at")
        if isinstance(raw_generated_at, datetime):
            cached_generated_at = raw_generated_at.isoformat()
        elif isinstance(raw_generated_at, str):
            cached_generated_at = raw_generated_at
    if (
        cached
        and cached.expires_at > utcnow()
        and demandwatch_snapshot_payload_is_complete(cached.payload)
        and cached_generated_at == read_model.generated_at.isoformat()
    ):
        return cached.payload
    return await recompute_demandwatch_snapshot(session)


async def publish_demandwatch_store(session: AsyncSession, output_path: Path) -> dict[str, Any]:
    bundle = await load_demandwatch_bundle(session)
    return write_published_demand_store(bundle, output_path)
