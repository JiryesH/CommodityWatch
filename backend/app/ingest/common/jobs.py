from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.db.models.sources import IngestRun, QuarantinedObservation, ReleaseDefinition, Source, SourceRelease


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestJobResult:
    fetched_items: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    quarantined_rows: int = 0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def get_source_bundle(session: AsyncSession, source_slug: str, release_slug: str) -> tuple[Source, ReleaseDefinition]:
    source = await session.scalar(select(Source).where(Source.slug == source_slug))
    release = await session.scalar(select(ReleaseDefinition).where(ReleaseDefinition.slug == release_slug))
    if source is None or release is None:
        raise ValueError(f"Missing source or release definition for {source_slug}/{release_slug}")
    return source, release


async def get_release_indicators(session: AsyncSession, release_slug: str) -> list[Indicator]:
    result = await session.execute(select(Indicator).where(Indicator.active.is_(True)).order_by(Indicator.code.asc()))
    return [indicator for indicator in result.scalars().all() if indicator.metadata_.get("release_slug") == release_slug]


async def create_ingest_run(
    session: AsyncSession,
    job_name: str,
    source_id,
    release_definition_id,
    run_mode: str,
    metadata: dict | None = None,
) -> IngestRun:
    run = IngestRun(
        job_name=job_name,
        source_id=source_id,
        release_definition_id=release_definition_id,
        run_mode=run_mode,
        status="running",
        metadata_=metadata or {},
    )
    session.add(run)
    await session.flush()
    return run


def _default_scheduled_at(
    release_definition: ReleaseDefinition,
    released_at: datetime,
) -> datetime | None:
    if release_definition.default_local_time is None:
        return None
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    local_date = released_at.astimezone(local_tz).date()
    return datetime.combine(local_date, release_definition.default_local_time or time(0, 0), tzinfo=local_tz).astimezone(
        timezone.utc
    )


async def upsert_source_release(
    session: AsyncSession,
    *,
    source: Source,
    release_definition: ReleaseDefinition,
    release_key: str,
    release_name: str,
    released_at: datetime,
    period_start_at: datetime | None,
    period_end_at: datetime | None,
    artifact_id=None,
    source_url: str | None = None,
    notes: str | None = None,
    metadata: dict | None = None,
    scheduled_at: datetime | None = None,
) -> SourceRelease:
    existing = await session.scalar(
        select(SourceRelease).where(SourceRelease.source_id == source.id, SourceRelease.release_key == release_key)
    )
    if existing:
        existing.release_name = release_name
        existing.scheduled_at = scheduled_at or existing.scheduled_at or _default_scheduled_at(release_definition, released_at)
        existing.released_at = released_at
        existing.period_start_at = period_start_at
        existing.period_end_at = period_end_at
        existing.release_timezone = release_definition.schedule_timezone
        existing.status = "observed"
        existing.primary_artifact_id = artifact_id
        existing.source_url = source_url
        existing.notes = notes
        existing.metadata_ = metadata or existing.metadata_
        return existing

    release = SourceRelease(
        source_id=source.id,
        release_definition_id=release_definition.id,
        release_key=release_key,
        release_name=release_name,
        scheduled_at=scheduled_at or _default_scheduled_at(release_definition, released_at),
        released_at=released_at,
        period_start_at=period_start_at,
        period_end_at=period_end_at,
        release_timezone=release_definition.schedule_timezone,
        status="observed",
        primary_artifact_id=artifact_id,
        source_url=source_url,
        notes=notes,
        metadata_=metadata or {},
    )
    session.add(release)
    await session.flush()
    return release


def indicator_bounds(indicator: Indicator) -> tuple[float | None, float | None]:
    bounds = indicator.metadata_.get("sanity_bounds") or {}
    lower = bounds.get("min")
    upper = bounds.get("max")
    try:
        lower_value = float(lower) if lower is not None else None
    except (TypeError, ValueError):
        lower_value = None
    try:
        upper_value = float(upper) if upper is not None else None
    except (TypeError, ValueError):
        upper_value = None
    return lower_value, upper_value


def value_within_bounds(indicator: Indicator, value: float) -> tuple[bool, float | None, float | None]:
    lower_bound, upper_bound = indicator_bounds(indicator)
    if lower_bound is not None and value < lower_bound:
        return False, lower_bound, upper_bound
    if upper_bound is not None and value > upper_bound:
        return False, lower_bound, upper_bound
    return True, lower_bound, upper_bound


async def quarantine_value(
    session: AsyncSession,
    *,
    run: IngestRun,
    source: Source,
    indicator: Indicator,
    period_end_at: datetime | None,
    value: float,
    unit_native_code: str | None,
    reason: str,
    artifact_uri: str | None = None,
    payload: dict | None = None,
) -> None:
    lower_bound, upper_bound = indicator_bounds(indicator)
    run.quarantined_rows += 1
    logger.warning(
        "QUARANTINE %s %s value=%s bounds=(%s,%s) period_end_at=%s",
        indicator.code,
        reason,
        value,
        lower_bound,
        upper_bound,
        period_end_at.isoformat() if period_end_at else None,
    )
    session.add(
        QuarantinedObservation(
            ingest_run_id=run.id,
            indicator_id=indicator.id,
            source_id=source.id,
            period_end_at=period_end_at,
            value_native=value,
            unit_native_code=unit_native_code,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            reason=reason,
            artifact_uri=artifact_uri,
            payload=payload or {},
        )
    )
    await session.flush()
