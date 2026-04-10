from __future__ import annotations

import logging
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.observations import Observation
from app.ingest.common.archive import archive_blob
from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_release_indicators, get_source_bundle, upsert_source_release, utcnow
from app.ingest.sources.oecd.client import OECDClient
from app.ingest.sources.oecd.parsers import parse_oecd_cli_csv
from app.modules.demandwatch.reliability import annotate_run_failure, bump_run_metadata_counter, dedupe_records
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, upsert_observation_revision


logger = logging.getLogger(__name__)
DEFAULT_OECD_LOOKBACK_DAYS = 365 * 3


async def _latest_indicator_period_end_at(session: AsyncSession, indicator_id) -> date | None:
    result = await session.execute(
        select(Observation.period_end_at)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(1)
    )
    value = result.scalar_one_or_none()
    return None if value is None else value.date()


async def fetch_demand_oecd_cli(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "oecd", "demand_oecd_cli")
    indicators = await get_release_indicators(session, "demand_oecd_cli")
    indicators_by_series = {str(indicator.source_series_key): indicator for indicator in indicators if indicator.source_series_key}
    if not indicators_by_series:
        raise ValueError("DemandWatch OECD CLI indicators are missing source_series_key values.")

    run = await create_ingest_run(
        session,
        "demand_oecd_cli",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"landing_url": (release_definition.metadata_ or {}).get("landing_url")},
    )
    client = OECDClient()
    counters = IngestJobResult()
    failure_stage = "initializing"

    try:
        requested_start = start_date
        if requested_start is None:
            if run_mode == "backfill":
                requested_start = utcnow().date() - timedelta(days=DEFAULT_OECD_LOOKBACK_DAYS)
            else:
                latest_periods = [
                    latest_period
                    for indicator in indicators
                    if (latest_period := await _latest_indicator_period_end_at(session, indicator.id)) is not None
                ]
                requested_start = (
                    min(latest_periods).replace(day=1) if latest_periods else utcnow().date() - timedelta(days=DEFAULT_OECD_LOOKBACK_DAYS)
                )

        failure_stage = "fetch_oecd_cli_snapshot"
        raw = await client.get_cli_snapshot(
            list(indicators_by_series),
            start_date=requested_start,
            end_date=end_date,
        )
        artifact = await archive_blob(
            session,
            source,
            "demand_oecd_cli",
            raw,
            extension="csv",
            content_type="text/csv",
            metadata={
                "ref_areas": sorted(indicators_by_series),
                "start_date": requested_start.isoformat() if requested_start else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
        )

        failure_stage = "parse_oecd_cli_snapshot"
        parsed = parse_oecd_cli_csv(raw)
        parsed, duplicate_count = dedupe_records(
            parsed,
            key=lambda item: (item.source_series_key, item.period_end_at, item.source_item_ref),
        )
        if duplicate_count:
            bump_run_metadata_counter(run, "duplicate_observations_dropped", duplicate_count)
            logger.warning("OECD CLI job dropped %d duplicate parsed rows", duplicate_count)

        relevant_items = [item for item in parsed if item.source_series_key in indicators_by_series]
        if not relevant_items:
            raise ValueError("OECD CLI response did not include any seeded reference areas.")

        fetched_at = utcnow()
        source_release = await upsert_source_release(
            session,
            source=source,
            release_definition=release_definition,
            release_key=f"demand_oecd_cli:{fetched_at.strftime('%Y%m%dT%H%M%SZ')}",
            release_name=f"{release_definition.name} ({fetched_at.date().isoformat()})",
            released_at=fetched_at,
            period_start_at=min(item.period_start_at for item in relevant_items),
            period_end_at=max(item.period_end_at for item in relevant_items),
            artifact_id=artifact.id,
            source_url=(release_definition.metadata_ or {}).get("landing_url"),
            metadata={
                "ref_areas": sorted(indicators_by_series),
                "snapshot_vintage_at": fetched_at.isoformat(),
                "stores_latest_snapshot_only": True,
            },
        )
        run.source_release_id = source_release.id

        for item in relevant_items:
            if end_date is not None and item.period_end_at.date() > end_date:
                continue
            indicator = indicators_by_series[item.source_series_key]
            counters.fetched_items += 1
            observation, changed = await upsert_observation_revision(
                session,
                ObservationInput(
                    indicator_id=indicator.id,
                    period_start_at=item.period_start_at,
                    period_end_at=item.period_end_at,
                    release_id=source_release.id,
                    release_date=fetched_at,
                    vintage_at=fetched_at,
                    observation_kind=indicator.default_observation_kind.value,
                    value_native=item.value,
                    unit_native_code=indicator.native_unit_code,
                    value_canonical=item.value,
                    unit_canonical_code=indicator.canonical_unit_code,
                    source_item_ref=item.source_item_ref,
                    provenance_note=f"OECD CLI SDMX snapshot for {item.reference_area}",
                    metadata={
                        "reference_area": item.reference_area,
                        "ref_area_code": item.source_series_key,
                        "stores_latest_snapshot_only": True,
                    },
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
        run.finished_at = fetched_at
        await process_pending_events(session)
        return counters
    except Exception as exc:
        run.status = "failed"
        annotate_run_failure(run, exc, stage=failure_stage)
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("OECD CLI job failed during %s: %s", failure_stage, exc)
        raise
    finally:
        await client.close()
