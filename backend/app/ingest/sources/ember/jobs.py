from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.observations import Observation
from app.ingest.common.archive import archive_payload
from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_release_indicators, get_source_bundle, upsert_source_release, utcnow
from app.ingest.sources.ember.client import EmberClient
from app.ingest.sources.ember.parsers import parse_ember_monthly_demand, parse_ember_stats_timestamp
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, upsert_observation_revision


async def _latest_indicator_period_end_at(session: AsyncSession, indicator_id) -> datetime | None:
    result = await session.execute(
        select(Observation.period_end_at)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def _release_datetime(release_definition, indicator, period_end_at: datetime) -> datetime:
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    release_day = (period_end_at + (indicator.publication_lag or timedelta())).date()
    return datetime.combine(release_day, release_definition.default_local_time or time(0, 0), tzinfo=local_tz).astimezone(
        timezone.utc
    )


async def fetch_demand_ember_monthly_electricity(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "ember", "demand_ember_monthly_electricity")
    indicators = await get_release_indicators(session, "demand_ember_monthly_electricity")
    client = EmberClient()
    run = await create_ingest_run(
        session,
        "demand_ember_monthly_electricity",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"landing_url": (release_definition.metadata_ or {}).get("landing_url")},
    )
    counters = IngestJobResult()
    release_cache: dict[str, object] = {}

    try:
        for indicator in indicators:
            metadata = indicator.metadata_ or {}
            latest_period_end_at = None if run_mode == "backfill" else await _latest_indicator_period_end_at(session, indicator.id)
            query_start = start_date.isoformat()[:7] if start_date else (
                latest_period_end_at.date().replace(day=1).isoformat()[:7] if latest_period_end_at else None
            )
            payload = await client.get_monthly_demand(
                entity=metadata.get("source_entity"),
                entity_code=metadata.get("source_entity_code"),
                start_date=query_start,
                end_date=end_date.isoformat()[:7] if end_date else None,
                is_aggregate_entity=metadata.get("source_is_aggregate_entity"),
            )
            artifact = await archive_payload(session, source, "demand_ember_monthly_electricity", payload)
            stats_timestamp = parse_ember_stats_timestamp(payload)
            parsed = parse_ember_monthly_demand(payload)
            counters.fetched_items += len(parsed)

            for item in parsed:
                if latest_period_end_at is not None and item.period_end_at < latest_period_end_at:
                    continue
                release_key_suffix = item.period_end_at.strftime("%Y-%m")
                released_at = _release_datetime(release_definition, indicator, item.period_end_at)
                source_release = release_cache.get(release_key_suffix)
                if source_release is None:
                    source_release = await upsert_source_release(
                        session,
                        source=source,
                        release_definition=release_definition,
                        release_key=f"demand_ember_monthly_electricity:{release_key_suffix}",
                        release_name=f"{release_definition.name} ({release_key_suffix})",
                        released_at=released_at,
                        period_start_at=item.period_start_at,
                        period_end_at=item.period_end_at,
                        artifact_id=artifact.id,
                        source_url=(release_definition.metadata_ or {}).get("landing_url"),
                        metadata={
                            "ember_stats_timestamp": stats_timestamp.isoformat() if stats_timestamp else None,
                            "entity": item.entity,
                            "entity_code": item.entity_code,
                        },
                    )
                    release_cache[release_key_suffix] = source_release
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
                        value_native=item.value_twh,
                        unit_native_code=indicator.native_unit_code,
                        value_canonical=item.value_twh,
                        unit_canonical_code=indicator.canonical_unit_code,
                        source_item_ref=item.source_item_ref,
                        provenance_note=f"Ember API monthly demand for {item.entity}",
                        metadata={"ember_stats_timestamp": stats_timestamp.isoformat() if stats_timestamp else None},
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
