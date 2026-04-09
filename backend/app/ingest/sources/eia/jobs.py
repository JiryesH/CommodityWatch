from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import delete, select, update
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.indicators import Indicator
from app.db.models.observations import AppEvent, Observation
from app.db.models.sources import IngestRun, ReleaseDefinition, Source, SourceRelease
from app.ingest.common.archive import archive_payload
from app.modules.demandwatch.reliability import annotate_run_failure, bump_run_metadata_counter, dedupe_records
from app.ingest.sources.eia.client import EIAClient
from app.ingest.sources.eia.parsers import ParsedObservation, parse_eia_response
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, upsert_observation_revision


RETRY_ATTEMPTS = 4
RETRY_INTERVAL_SECONDS = 15 * 60
DEV_RETRY_INTERVAL_SECONDS = 30


logger = logging.getLogger(__name__)
EIA_LIVE_FALLBACK_DAYS = {
    "hourly": 14,
    "daily": 365 * 2,
    "weekly": 365 * 8,
    "monthly": 365 * 15,
}
EIA_INCREMENTAL_OVERLAP_DAYS = {
    "hourly": 2,
    "daily": 7,
    "weekly": 14,
    "monthly": 31,
}
EIA_STALE_PERIOD_AGE_DAYS = 120
EIA_STALE_RELEASE_GAP_DAYS = 45


@dataclass(slots=True)
class EIAJobResult:
    fetched_items: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0


@dataclass(frozen=True, slots=True)
class EIASeriesRequest:
    start: date | None
    end: date | None
    length: int | None
    sort_desc: bool


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def retry_interval_seconds(run_mode: str) -> int:
    if run_mode == "manual" or not get_settings().is_production:
        return DEV_RETRY_INTERVAL_SECONDS
    return RETRY_INTERVAL_SECONDS


def _frequency_value(indicator: Indicator) -> str:
    return indicator.frequency.value if hasattr(indicator.frequency, "value") else str(indicator.frequency)


def resolve_eia_live_start_date(
    indicator: Indicator,
    latest_period_end_at: datetime | None,
    *,
    observed_at: datetime,
) -> date:
    frequency = _frequency_value(indicator)
    if latest_period_end_at is None:
        fallback_days = EIA_LIVE_FALLBACK_DAYS.get(frequency, 365)
        return (observed_at - timedelta(days=fallback_days)).date()
    overlap_days = EIA_INCREMENTAL_OVERLAP_DAYS.get(frequency, 14)
    return max(date(2000, 1, 1), (latest_period_end_at - timedelta(days=overlap_days)).date())


def build_eia_series_request(
    indicator: Indicator,
    *,
    run_mode: str,
    start_date: date | None,
    end_date: date | None,
    latest_period_end_at: datetime | None,
    observed_at: datetime,
) -> EIASeriesRequest:
    if start_date is not None:
        return EIASeriesRequest(start=start_date, end=end_date, length=None, sort_desc=False)
    if run_mode == "backfill":
        return EIASeriesRequest(start=None, end=end_date, length=None, sort_desc=False)
    return EIASeriesRequest(
        start=resolve_eia_live_start_date(indicator, latest_period_end_at, observed_at=observed_at),
        end=end_date,
        length=None,
        sort_desc=False,
    )


def derive_eia_release_timestamp(
    indicator: Indicator,
    release_definition: ReleaseDefinition,
    *,
    period_end_at: datetime,
    observed_at: datetime,
) -> datetime:
    lag = indicator.publication_lag or timedelta()
    scheduled_base = period_end_at + lag
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    local_date = scheduled_base.astimezone(local_tz).date()
    scheduled_local = datetime.combine(local_date, release_definition.default_local_time or time(0, 0), tzinfo=local_tz)
    released_at = scheduled_local.astimezone(timezone.utc)
    return min(released_at, observed_at)


def is_invalid_recent_stale_window(
    period_end_at: datetime,
    release_date: datetime | None,
    *,
    observed_at: datetime,
    max_period_age_days: int = EIA_STALE_PERIOD_AGE_DAYS,
    max_release_gap_days: int = EIA_STALE_RELEASE_GAP_DAYS,
) -> bool:
    if release_date is None:
        return False
    return (
        (observed_at - period_end_at) > timedelta(days=max_period_age_days)
        and (release_date - period_end_at) > timedelta(days=max_release_gap_days)
    )


def invalid_recent_stale_observation_ids(
    observations: list[Observation],
    *,
    observed_at: datetime,
) -> list[object]:
    return [
        observation.id
        for observation in observations
        if is_invalid_recent_stale_window(observation.period_end_at, observation.release_date, observed_at=observed_at)
    ]


def latest_eia_payload_period_end_at(parsed: list[ParsedObservation]) -> datetime | None:
    if not parsed:
        return None
    return max(item.period_end_at for item in parsed)


def is_stale_live_payload(
    latest_period_end_at: datetime | None,
    *,
    observed_at: datetime,
    max_period_age_days: int = EIA_STALE_PERIOD_AGE_DAYS,
) -> bool:
    if latest_period_end_at is None:
        return False
    return (observed_at - latest_period_end_at) > timedelta(days=max_period_age_days)


def filter_eia_observations_to_request_window(
    parsed: list[ParsedObservation],
    *,
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[ParsedObservation], list[ParsedObservation]]:
    accepted: list[ParsedObservation] = []
    rejected: list[ParsedObservation] = []
    for item in parsed:
        period_date = item.period_end_at.date()
        if start_date is not None and period_date < start_date:
            rejected.append(item)
            continue
        if end_date is not None and period_date > end_date:
            rejected.append(item)
            continue
        accepted.append(item)
    return accepted, rejected


async def get_recent_latest_observations(
    session: AsyncSession,
    indicator_id,
    *,
    limit: int = 16,
) -> list[Observation]:
    result = await session.execute(
        select(Observation)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(limit)
    )
    return list(result.scalars().all())


async def get_indicator_observations(
    session: AsyncSession,
    indicator_id,
) -> list[Observation]:
    result = await session.execute(
        select(Observation)
        .where(Observation.indicator_id == indicator_id)
        .order_by(Observation.period_end_at.asc(), Observation.revision_sequence.asc())
    )
    return list(result.scalars().all())


async def latest_indicator_period_end_at(session: AsyncSession, indicator_id) -> datetime | None:
    result = await session.execute(
        select(Observation.period_end_at)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def invalid_stale_history_observation_ids(
    observations: list[Observation],
    *,
    observed_at: datetime,
) -> list[object]:
    return [
        observation.id
        for observation in observations
        if not observation.is_latest
        and is_invalid_recent_stale_window(observation.period_end_at, observation.release_date, observed_at=observed_at)
    ]


async def purge_invalid_recent_stale_observations(
    session: AsyncSession,
    indicator: Indicator,
    *,
    observed_at: datetime,
) -> int:
    stale_ids = invalid_recent_stale_observation_ids(
        await get_recent_latest_observations(session, indicator.id),
        observed_at=observed_at,
    )
    if not stale_ids:
        return 0
    await session.execute(
        update(Observation)
        .where(Observation.id.in_(stale_ids))
        .values(is_latest=False)
    )
    await session.flush()
    logger.warning(
        "EIA job demoted %d invalid stale latest observation(s) for %s before live refresh",
        len(stale_ids),
        indicator.code,
    )
    return len(stale_ids)


async def purge_invalid_stale_history(
    session: AsyncSession,
    indicator: Indicator,
    *,
    observed_at: datetime,
) -> int:
    stale_ids = invalid_stale_history_observation_ids(
        await get_indicator_observations(session, indicator.id),
        observed_at=observed_at,
    )
    if not stale_ids:
        return 0
    await session.execute(delete(AppEvent).where(AppEvent.observation_id.in_(stale_ids)))
    await session.execute(delete(Observation).where(Observation.id.in_(stale_ids)))
    await session.flush()
    logger.warning(
        "EIA job removed %d invalid stale historical observation(s) for %s before live refresh",
        len(stale_ids),
        indicator.code,
    )
    return len(stale_ids)


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
    indicator: Indicator,
    period_end_at: datetime,
    released_at: datetime,
    artifact_id,
    source_url: str | None = None,
) -> SourceRelease:
    frequency = _frequency_value(indicator)
    if frequency == "hourly":
        release_key_suffix = period_end_at.strftime("%Y-%m-%dT%H")
        period_start_at = period_end_at
    elif frequency == "daily":
        release_key_suffix = period_end_at.date().isoformat()
        period_start_at = period_end_at
    elif frequency == "monthly":
        release_key_suffix = period_end_at.strftime("%Y-%m")
        period_start_at = period_end_at.replace(day=1)
    else:
        release_key_suffix = period_end_at.date().isoformat()
        period_start_at = period_end_at - timedelta(days=6)
    release_key = f"{release_definition.slug}:{release_key_suffix}"
    existing = await session.scalar(
        select(SourceRelease).where(SourceRelease.source_id == source.id, SourceRelease.release_key == release_key)
    )
    if existing:
        existing.released_at = released_at
        existing.status = "observed"
        existing.primary_artifact_id = artifact_id
        existing.period_start_at = period_start_at
        existing.period_end_at = period_end_at
        existing.source_url = source_url or existing.source_url
        return existing

    local_tz = ZoneInfo(release_definition.schedule_timezone)
    scheduled_local = datetime.combine(released_at.astimezone(local_tz).date(), release_definition.default_local_time or time(0, 0), tzinfo=local_tz)
    source_release = SourceRelease(
        source_id=source.id,
        release_definition_id=release_definition.id,
        release_key=release_key,
        release_name=f"{release_definition.name} ({release_key_suffix})",
        scheduled_at=scheduled_local.astimezone(timezone.utc),
        released_at=released_at,
        period_start_at=period_start_at,
        period_end_at=period_end_at,
        release_timezone=release_definition.schedule_timezone,
        status="observed",
        primary_artifact_id=artifact_id,
        source_url=source_url,
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
    observed_at = utcnow()
    counters = EIAJobResult()
    total_indicators = len(indicators)
    repaired_windows: dict[str, int] = {}
    cleaned_history: dict[str, int] = {}
    stale_live_indicators: list[str] = []
    failure_stage = "initializing"

    try:
        logger.info("EIA job %s starting with %d indicators", job_name, total_indicators)
        grouped: dict[datetime, list[tuple[Indicator, object, object]]] = defaultdict(list)
        for index, indicator in enumerate(indicators, start=1):
            latest_period_end_at = None
            if start_date is None and run_mode != "backfill":
                repaired_count = await purge_invalid_recent_stale_observations(
                    session,
                    indicator,
                    observed_at=observed_at,
                )
                if repaired_count:
                    repaired_windows[indicator.code] = repaired_count
                cleaned_count = await purge_invalid_stale_history(
                    session,
                    indicator,
                    observed_at=observed_at,
                )
                if cleaned_count:
                    cleaned_history[indicator.code] = cleaned_count
                latest_period_end_at = await latest_indicator_period_end_at(session, indicator.id)
            request = build_eia_series_request(
                indicator,
                run_mode=run_mode,
                start_date=start_date,
                end_date=end_date,
                latest_period_end_at=latest_period_end_at,
                observed_at=observed_at,
            )
            logger.info(
                "EIA job %s fetching %d/%d: %s (%s) start=%s end=%s",
                job_name,
                index,
                total_indicators,
                indicator.code,
                indicator.source_series_key,
                request.start.isoformat() if request.start else None,
                request.end.isoformat() if request.end else None,
            )
            failure_stage = f"fetch_series:{indicator.code}"
            payload = await client.get_series_data(
                indicator.source_series_key,
                start=request.start,
                end=request.end,
                length=request.length,
                sort_desc=request.sort_desc,
            )
            artifact = await archive_payload(session, source, job_name, payload)
            failure_stage = f"parse_series:{indicator.code}"
            parsed = parse_eia_response(payload, indicator.frequency.value)
            parsed, duplicate_count = dedupe_records(
                parsed,
                key=lambda item: (item.period_start_at, item.period_end_at, item.source_item_ref),
            )
            if duplicate_count:
                bump_run_metadata_counter(run, "duplicate_observations_dropped", duplicate_count)
                logger.warning(
                    "EIA job %s dropped %d duplicate parsed rows for %s",
                    job_name,
                    duplicate_count,
                    indicator.code,
                )
            if start_date is None and run_mode != "backfill":
                latest_payload_period = latest_eia_payload_period_end_at(parsed)
                if is_stale_live_payload(latest_payload_period, observed_at=observed_at):
                    stale_live_indicators.append(indicator.code)
                    logger.warning(
                        "EIA job %s skipping stale live payload for %s: latest_period_end_at=%s",
                        job_name,
                        indicator.code,
                        latest_payload_period.isoformat() if latest_payload_period else None,
                    )
                    continue
            accepted, rejected = filter_eia_observations_to_request_window(
                parsed,
                start_date=request.start,
                end_date=request.end,
            )
            logger.info(
                "EIA job %s received %d rows for %s (%d accepted, %d rejected)",
                job_name,
                len(parsed),
                indicator.code,
                len(accepted),
                len(rejected),
            )
            counters.fetched_items += len(accepted)
            for item in accepted:
                grouped[item.period_end_at].append((indicator, item, artifact))

        for period_end_at, items in sorted(grouped.items(), key=lambda item: item[0]):
            release_timestamp = derive_eia_release_timestamp(
                items[0][0],
                release_definition,
                period_end_at=period_end_at,
                observed_at=observed_at,
            )
            source_release = await upsert_source_release(
                session,
                source,
                release_definition,
                items[0][0],
                period_end_at,
                release_timestamp,
                items[0][2].id,
                source_url=(release_definition.metadata_ or {}).get("landing_url"),
            )
            for indicator, parsed, _artifact in items:
                observation, changed = await upsert_observation_revision(
                    session,
                    ObservationInput(
                        indicator_id=indicator.id,
                        period_start_at=parsed.period_start_at,
                        period_end_at=parsed.period_end_at,
                        release_id=source_release.id,
                        release_date=release_timestamp,
                        vintage_at=release_timestamp,
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

        run.status = "partial" if stale_live_indicators else "success"
        run.fetched_items = counters.fetched_items
        run.inserted_rows = counters.inserted_rows
        run.updated_rows = counters.updated_rows
        if stale_live_indicators or repaired_windows or cleaned_history:
            run.metadata_ = {
                **(run.metadata_ or {}),
                "repaired_invalid_windows": repaired_windows,
                "cleaned_invalid_history": cleaned_history,
                "skipped_stale_live_indicators": stale_live_indicators,
            }
        if stale_live_indicators:
            run.error_text = "; ".join(f"{code}: stale live payload skipped" for code in stale_live_indicators)
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
        annotate_run_failure(run, exc, stage=failure_stage)
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("EIA job %s failed during %s: %s", job_name, failure_stage, exc)
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


async def fetch_demand_eia_wpsr(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    return await _retrying_job(
        session,
        "demand_eia_wpsr",
        "demand_eia_wpsr",
        run_mode=run_mode,
        start_date=start_date,
        end_date=end_date,
    )


async def fetch_demand_eia_grid_monitor(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> EIAJobResult:
    return await _retrying_job(
        session,
        "demand_eia_grid_monitor",
        "demand_eia_grid_monitor",
        run_mode=run_mode,
        start_date=start_date,
        end_date=end_date,
    )
