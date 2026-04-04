from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.db.models.observations import Observation
from app.ingest.common.archive import archive_payload
from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_release_indicators, get_source_bundle, upsert_source_release, utcnow
from app.ingest.sources.fred.client import FREDClient
from app.ingest.sources.fred.parsers import parse_fred_observations, parse_fred_release, parse_fred_release_dates, selected_vintage_dates
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, upsert_observation_revision


FRED_LIVE_OVERLAP_DAYS = {
    "monthly": 90,
    "weekly": 21,
    "daily": 7,
}


def _frequency_value(indicator: Indicator) -> str:
    return indicator.frequency.value if hasattr(indicator.frequency, "value") else str(indicator.frequency)


async def _latest_indicator_period_end_at(session: AsyncSession, indicator_id) -> datetime | None:
    result = await session.execute(
        select(Observation.period_end_at)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def _window_start(indicator: Indicator, latest_period_end_at: datetime | None, start_date: date | None) -> date | None:
    if start_date is not None:
        return start_date
    if latest_period_end_at is None:
        return None
    overlap_days = FRED_LIVE_OVERLAP_DAYS.get(_frequency_value(indicator), 30)
    return (latest_period_end_at - timedelta(days=overlap_days)).date()


def _release_datetime_for_date(release_definition, released_on: date) -> datetime:
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    return datetime.combine(released_on, release_definition.default_local_time or time(0, 0), tzinfo=local_tz).astimezone(timezone.utc)


async def _run_fred_release(
    session: AsyncSession,
    *,
    release_slug: str,
    job_name: str,
    run_mode: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "fred", release_slug)
    indicators = await get_release_indicators(session, release_slug)
    if not indicators:
        raise ValueError(f"No active FRED indicators found for release {release_slug}")

    client = FREDClient()
    run = await create_ingest_run(
        session,
        job_name,
        source.id,
        release_definition.id,
        run_mode,
        metadata={"source_url": (release_definition.metadata_ or {}).get("landing_url")},
    )
    counters = IngestJobResult()
    release_cache: dict[date, object] = {}

    try:
        release_ref: FREDRelease | None = None
        release_dates: list[date] = []

        for indicator in indicators:
            if release_ref is None:
                release_ref = parse_fred_release(await client.get_series_release(str(indicator.source_series_key)))
                release_dates = parse_fred_release_dates(await client.get_release_dates(release_ref.release_id))
                run.metadata_ = {
                    **(run.metadata_ or {}),
                    "fred_release_id": release_ref.release_id,
                    "fred_release_name": release_ref.name,
                    "fred_release_link": release_ref.link,
                }

            latest_period_end_at = None if run_mode == "backfill" else await _latest_indicator_period_end_at(session, indicator.id)
            observation_start = _window_start(indicator, latest_period_end_at, start_date)
            vintage_dates = selected_vintage_dates(release_dates, run_mode=run_mode, start_date=observation_start or start_date)
            for released_on in vintage_dates:
                payload = await client.get_series_observations(
                    str(indicator.source_series_key),
                    start_date=observation_start,
                    end_date=end_date,
                    realtime_start=released_on,
                    realtime_end=released_on,
                )
                artifact = await archive_payload(session, source, f"{job_name}_{released_on.isoformat()}", payload)
                parsed = parse_fred_observations(payload, _frequency_value(indicator))
                counters.fetched_items += len(parsed)

                for item in parsed:
                    if observation_start is not None and item.period_end_at.date() < observation_start:
                        continue
                    if end_date is not None and item.period_end_at.date() > end_date:
                        continue

                    released_at = _release_datetime_for_date(release_definition, released_on)
                    source_release = release_cache.get(released_on)
                    if source_release is None:
                        source_release = await upsert_source_release(
                            session,
                            source=source,
                            release_definition=release_definition,
                            release_key=f"{release_slug}:{released_on.isoformat()}",
                            release_name=f"{release_definition.name} ({released_on.isoformat()})",
                            released_at=released_at,
                            period_start_at=item.period_start_at,
                            period_end_at=item.period_end_at,
                            artifact_id=artifact.id,
                            source_url=release_ref.link or f"https://fred.stlouisfed.org/series/{indicator.source_series_key}",
                            metadata={
                                "fred_release_id": release_ref.release_id,
                                "fred_release_name": release_ref.name,
                                "series_id": indicator.source_series_key,
                            },
                        )
                        release_cache[released_on] = source_release
                    run.source_release_id = source_release.id

                    observation, changed = await upsert_observation_revision(
                        session,
                        ObservationInput(
                            indicator_id=indicator.id,
                            period_start_at=item.period_start_at,
                            period_end_at=item.period_end_at,
                            release_id=source_release.id,
                            release_date=released_at,
                            vintage_at=released_at,
                            observation_kind=indicator.default_observation_kind.value,
                            value_native=item.value,
                            unit_native_code=indicator.native_unit_code,
                            value_canonical=item.value,
                            unit_canonical_code=indicator.canonical_unit_code,
                            source_item_ref=item.source_item_ref,
                            provenance_note=f"FRED series {indicator.source_series_key}",
                            metadata={"fred_release_id": release_ref.release_id, "run_mode": run_mode},
                        ),
                    )
                    if not changed:
                        continue
                    if observation.revision_sequence > 1:
                        counters.updated_rows += 1
                    else:
                        counters.inserted_rows += 1
                    await emit_observation_event(
                        session,
                        indicator,
                        observation,
                        event_type="demand.observation_upserted",
                        producer_module_code="demandwatch",
                    )

        run.status = "success"
        run.fetched_items = counters.fetched_items
        run.inserted_rows = counters.inserted_rows
        run.updated_rows = counters.updated_rows
        run.finished_at = utcnow()
        await process_pending_events(session)
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        raise
    finally:
        await client.close()


async def fetch_demand_fred_g17(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    return await _run_fred_release(
        session,
        release_slug="demand_fred_g17",
        job_name="demand_fred_g17",
        run_mode=run_mode,
        start_date=start_date,
        end_date=end_date,
    )


async def fetch_demand_fred_new_residential_construction(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    return await _run_fred_release(
        session,
        release_slug="demand_fred_new_residential_construction",
        job_name="demand_fred_new_residential_construction",
        run_mode=run_mode,
        start_date=start_date,
        end_date=end_date,
    )
