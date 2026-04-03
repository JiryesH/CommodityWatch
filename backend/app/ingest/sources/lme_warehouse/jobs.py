from __future__ import annotations

import logging
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.ingest.common.archive import archive_blob, archive_payload
from app.ingest.common.jobs import (
    IngestJobResult,
    create_ingest_run,
    get_release_indicators,
    get_source_bundle,
    quarantine_value,
    upsert_source_release,
    utcnow,
    value_within_bounds,
)
from app.ingest.sources.lme_warehouse.client import (
    LMEReportNotFoundError,
    LMEWarehouseAccessBlockedError,
    LMEWarehouseClient,
    build_report_url,
)
from app.ingest.sources.lme_warehouse.parsers import ParsedLMEObservation, parse_lme_workbook
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


logger = logging.getLogger(__name__)


LATEST_PUBLIC_LOOKBACK_DAYS = 7
LME_WAREHOUSE_COVERAGE_NOTE = (
    "Backfills iterate public business-day workbook URLs; current reports may still be login-gated."
)


def _release_datetime(report_date: date) -> datetime:
    release_local = datetime.combine(report_date, time(18, 0), tzinfo=ZoneInfo("Europe/London"))
    return release_local.astimezone(timezone.utc)


async def fetch_lme_warehouse(
    session: AsyncSession,
    run_mode: str = "live",
    report_date: date | None = None,
) -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "lme", "lme_warehouse")
    indicators = await get_release_indicators(session, "lme_warehouse")
    indicators_by_key: dict[str, Indicator] = {str(indicator.source_series_key): indicator for indicator in indicators}
    run = await create_ingest_run(
        session,
        "lme_warehouse",
        source.id,
        release_definition.id,
        run_mode,
        metadata={
            "requested_report_date": report_date.isoformat() if report_date else None,
            "coverage_note": LME_WAREHOUSE_COVERAGE_NOTE,
        },
    )
    client = LMEWarehouseClient()
    counters = IngestJobResult()

    try:
        target_date = report_date
        if target_date is None:
            target_date = await client.find_latest_available_report(as_of=date.today(), lookback_days=LATEST_PUBLIC_LOOKBACK_DAYS)
            if target_date is None:
                message = "No publicly accessible LME workbook found in the recent lookback window; current reports may require login."
                logger.warning(message)
                run.status = "partial"
                run.error_text = message
                run.finished_at = utcnow()
                return counters

        source_url = build_report_url(target_date)
        try:
            raw_workbook = await client.get_report(target_date)
        except LMEReportNotFoundError:
            logger.info("No LME workbook published for %s", target_date.isoformat())
            run.status = "success"
            run.finished_at = utcnow()
            return counters

        raw_artifact = await archive_blob(
            session,
            source,
            "lme_warehouse",
            raw_workbook,
            extension="xls",
            content_type="application/vnd.ms-excel",
            metadata={"report_date": target_date.isoformat(), "source_url": source_url},
        )
        parsed_items = parse_lme_workbook(raw_workbook, report_date=target_date, source_url=source_url)
        structured_artifact = await archive_payload(
            session,
            source,
            "lme_warehouse",
            {"items": [item.to_item() for item in parsed_items]},
        )

        released_at = _release_datetime(target_date)
        source_release = await upsert_source_release(
            session,
            source=source,
            release_definition=release_definition,
            release_key=f"{release_definition.slug}:{target_date.isoformat()}",
            release_name=f"{release_definition.name} ({target_date.isoformat()})",
            released_at=released_at,
            period_start_at=released_at,
            period_end_at=released_at,
            artifact_id=raw_artifact.id,
            source_url=source_url,
            metadata={"structured_artifact_uri": structured_artifact.storage_uri},
        )
        run.source_release_id = source_release.id

        for parsed in parsed_items:
            await _upsert_parsed_item(
                session,
                parsed=parsed,
                indicators_by_key=indicators_by_key,
                source_release_id=source_release.id,
                released_at=released_at,
                run_mode=run_mode,
                counters=counters,
                source=source,
                run=run,
                artifact_uri=raw_artifact.storage_uri,
            )

        run.status = "partial" if counters.quarantined_rows else "success"
        run.fetched_items = counters.fetched_items
        run.inserted_rows = counters.inserted_rows
        run.updated_rows = counters.updated_rows
        run.quarantined_rows = counters.quarantined_rows
        run.finished_at = utcnow()
        await process_pending_events(session)
        return counters
    except LMEWarehouseAccessBlockedError as exc:
        logger.warning("LME warehouse job blocked: %s", exc)
        run.status = "partial"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("LME warehouse job failed: %s", exc)
        raise
    finally:
        await client.close()


async def _upsert_parsed_item(
    session: AsyncSession,
    *,
    parsed: ParsedLMEObservation,
    indicators_by_key: dict[str, Indicator],
    source_release_id,
    released_at: datetime,
    run_mode: str,
    counters: IngestJobResult,
    source,
    run,
    artifact_uri: str,
) -> None:
    counters.fetched_items += 1
    indicator = indicators_by_key.get(parsed.source_series_key)
    if indicator is None:
        logger.warning("Skipping unseeded LME metal %s", parsed.source_series_key)
        return

    is_valid, lower_bound, upper_bound = value_within_bounds(indicator, parsed.total)
    if not is_valid:
        await quarantine_value(
            session,
            run=run,
            source=source,
            indicator=indicator,
            period_end_at=released_at,
            value=parsed.total,
            unit_native_code="tonnes",
            reason=f"outside_bounds[{lower_bound},{upper_bound}]",
            artifact_uri=artifact_uri,
            payload=parsed.to_item(),
        )
        counters.quarantined_rows += 1
        return

    observation, changed = await upsert_observation_revision(
        session,
        ObservationInput(
            indicator_id=indicator.id,
            period_start_at=derive_period_start(released_at, indicator.frequency.value),
            period_end_at=released_at,
            release_id=source_release_id,
            release_date=released_at,
            vintage_at=released_at,
            observation_kind=indicator.default_observation_kind.value,
            value_native=parsed.total,
            unit_native_code="tonnes",
            value_canonical=parsed.total,
            unit_canonical_code=indicator.canonical_unit_code,
            source_item_ref=parsed.source_item_ref,
            provenance_note=f"LME warehouse workbook {parsed.source_url}",
            metadata={
                "on_warrant": parsed.on_warrant,
                "cancelled_tonnage": parsed.cancelled,
                "metal": parsed.metal,
                "run_mode": run_mode,
                **parsed.metadata,
            },
        ),
    )
    if not changed:
        return
    if observation.revision_sequence > 1:
        counters.updated_rows += 1
    else:
        counters.inserted_rows += 1
    await emit_observation_event(session, indicator, observation)
