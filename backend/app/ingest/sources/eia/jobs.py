from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.indicators import Indicator
from app.db.models.sources import IngestRun, ReleaseDefinition, Source, SourceRelease
from app.ingest.common.archive import archive_payload
from app.ingest.sources.eia.client import EIAClient
from app.ingest.sources.eia.parsers import parse_eia_response
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, upsert_observation_revision


RETRY_ATTEMPTS = 4
RETRY_INTERVAL_SECONDS = 15 * 60
DEV_RETRY_INTERVAL_SECONDS = 30


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EIAJobResult:
    fetched_items: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def retry_interval_seconds(run_mode: str) -> int:
    if run_mode == "manual" or not get_settings().is_production:
        return DEV_RETRY_INTERVAL_SECONDS
    return RETRY_INTERVAL_SECONDS


async def get_source_bundle(session: AsyncSession, release_slug: str) -> tuple[Source, ReleaseDefinition]:
    source = await session.scalar(select(Source).where(Source.slug == "eia"))
    release = await session.scalar(select(ReleaseDefinition).where(ReleaseDefinition.slug == release_slug))
    if source is None or release is None:
        raise ValueError(f"Missing source or release definition for {release_slug}")
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
) -> IngestRun:
    run = IngestRun(
        job_name=job_name,
        source_id=source_id,
        release_definition_id=release_definition_id,
        run_mode=run_mode,
        status="running",
    )
    session.add(run)
    await session.flush()
    return run


async def upsert_source_release(
    session: AsyncSession,
    source: Source,
    release_definition: ReleaseDefinition,
    period_end_at: datetime,
    released_at: datetime,
    artifact_id,
) -> SourceRelease:
    release_key = f"{release_definition.slug}:{period_end_at.date().isoformat()}"
    existing = await session.scalar(
        select(SourceRelease).where(SourceRelease.source_id == source.id, SourceRelease.release_key == release_key)
    )
    if existing:
        existing.released_at = released_at
        existing.status = "observed"
        existing.primary_artifact_id = artifact_id
        return existing

    local_tz = ZoneInfo(release_definition.schedule_timezone)
    scheduled_local = datetime.combine(released_at.astimezone(local_tz).date(), release_definition.default_local_time or time(0, 0), tzinfo=local_tz)
    source_release = SourceRelease(
        source_id=source.id,
        release_definition_id=release_definition.id,
        release_key=release_key,
        release_name=f"{release_definition.name} ({period_end_at.date().isoformat()})",
        scheduled_at=scheduled_local.astimezone(timezone.utc),
        released_at=released_at,
        period_start_at=period_end_at - timedelta(days=6),
        period_end_at=period_end_at,
        release_timezone=release_definition.schedule_timezone,
        status="observed",
        primary_artifact_id=artifact_id,
    )
    session.add(source_release)
    await session.flush()
    return source_release


async def _run_eia_release(
    session: AsyncSession,
    release_slug: str,
    job_name: str,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    source, release_definition = await get_source_bundle(session, release_slug)
    run = await create_ingest_run(session, job_name, source.id, release_definition.id, run_mode)
    indicators = await get_release_indicators(session, release_slug)
    client = EIAClient()
    released_at = utcnow()
    counters = EIAJobResult()
    total_indicators = len(indicators)

    try:
        logger.info("EIA job %s starting with %d indicators", job_name, total_indicators)
        grouped: dict[datetime, list[tuple[Indicator, object, object]]] = defaultdict(list)
        for index, indicator in enumerate(indicators, start=1):
            logger.info(
                "EIA job %s fetching %d/%d: %s (%s)",
                job_name,
                index,
                total_indicators,
                indicator.code,
                indicator.source_series_key,
            )
            payload = await client.get_series_data(
                indicator.source_series_key,
                start=start_date,
                end=end_date,
                length=None if start_date else 8,
                sort_desc=not bool(start_date),
            )
            artifact = await archive_payload(session, source, job_name, payload)
            parsed = parse_eia_response(payload, indicator.frequency.value)
            logger.info(
                "EIA job %s received %d rows for %s",
                job_name,
                len(parsed),
                indicator.code,
            )
            counters.fetched_items += len(parsed)
            for item in parsed:
                grouped[item.period_end_at].append((indicator, item, artifact))

        for period_end_at, items in grouped.items():
            source_release = await upsert_source_release(
                session,
                source,
                release_definition,
                period_end_at,
                released_at,
                items[0][2].id,
            )
            for indicator, parsed, _artifact in items:
                observation, changed = await upsert_observation_revision(
                    session,
                    ObservationInput(
                        indicator_id=indicator.id,
                        period_start_at=parsed.period_start_at,
                        period_end_at=parsed.period_end_at,
                        release_id=source_release.id,
                        release_date=released_at,
                        vintage_at=released_at,
                        observation_kind=indicator.default_observation_kind.value,
                        value_native=parsed.value,
                        unit_native_code=indicator.native_unit_code,
                        value_canonical=parsed.value,
                        unit_canonical_code=indicator.canonical_unit_code,
                        source_item_ref=parsed.source_item_ref,
                        provenance_note=f"EIA API series {indicator.source_series_key}",
                        metadata={"run_mode": run_mode},
                    ),
                )
                if changed:
                    if observation.revision_sequence > 1:
                        counters.updated_rows += 1
                    else:
                        counters.inserted_rows += 1
                    await emit_observation_event(session, indicator, observation)

        run.status = "success"
        run.fetched_items = counters.fetched_items
        run.inserted_rows = counters.inserted_rows
        run.updated_rows = counters.updated_rows
        run.finished_at = utcnow()
        await process_pending_events(session)
        logger.info(
            "EIA job %s finished (fetched=%d inserted=%d updated=%d)",
            job_name,
            counters.fetched_items,
            counters.inserted_rows,
            counters.updated_rows,
        )
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("EIA job %s failed: %s", job_name, exc)
        raise
    finally:
        await client.close()


async def _retrying_job(
    session: AsyncSession,
    release_slug: str,
    job_name: str,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    last_exc: Exception | None = None
    retry_interval = retry_interval_seconds(run_mode)
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await _run_eia_release(
                session,
                release_slug=release_slug,
                job_name=job_name,
                run_mode=run_mode if attempt == 0 or run_mode == "backfill" else "retry",
                start_date=start_date,
                end_date=end_date,
            )
        except (StatementError, TypeError, ValueError):
            raise
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            status_code = exc.response.status_code
            if 400 <= status_code < 500 and status_code != 429:
                raise
            if attempt == RETRY_ATTEMPTS - 1:
                break
            await session.rollback()
            logger.warning(
                "EIA job %s attempt %d/%d failed with HTTP %d; retrying in %d seconds",
                job_name,
                attempt + 1,
                RETRY_ATTEMPTS,
                status_code,
                retry_interval,
            )
            await asyncio.sleep(retry_interval)
        except Exception as exc:  # pragma: no cover
            last_exc = exc
            if attempt == RETRY_ATTEMPTS - 1:
                break
            await session.rollback()
            logger.warning(
                "EIA job %s attempt %d/%d failed with %s; retrying in %d seconds",
                job_name,
                attempt + 1,
                RETRY_ATTEMPTS,
                exc.__class__.__name__,
                retry_interval,
            )
            await asyncio.sleep(retry_interval)
    assert last_exc is not None
    raise last_exc


async def fetch_eia_wpsr(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    return await _retrying_job(session, "eia_wpsr", "eia_wpsr", run_mode=run_mode, start_date=start_date, end_date=end_date)


async def fetch_eia_wngs(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    return await _retrying_job(session, "eia_wngs", "eia_wngs", run_mode=run_mode, start_date=start_date, end_date=end_date)
