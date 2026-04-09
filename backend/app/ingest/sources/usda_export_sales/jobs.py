from __future__ import annotations

import logging
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.ingest.common.archive import archive_blob, archive_payload
from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_release_indicators, get_source_bundle, upsert_source_release, utcnow
from app.modules.demandwatch.reliability import annotate_run_failure, bump_run_metadata_counter, dedupe_records
from app.ingest.sources.usda_export_sales.client import STATIC_REPORTS_BASE, USDAExportSalesClient
from app.ingest.sources.usda_export_sales.parsers import parse_export_sales_release_info, parse_export_sales_summary
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


logger = logging.getLogger(__name__)


def _release_datetime(release_definition, released_on: date) -> datetime:
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    return datetime.combine(released_on, release_definition.default_local_time or time(0, 0), tzinfo=local_tz).astimezone(
        ZoneInfo("UTC")
    )


async def fetch_demand_usda_export_sales(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    del start_date, end_date

    source, release_definition = await get_source_bundle(session, "usda_export_sales", "demand_usda_export_sales")
    indicators = await get_release_indicators(session, "demand_usda_export_sales")
    indicators_by_series = {str(indicator.source_series_key): indicator for indicator in indicators if indicator.source_series_key}
    if not indicators_by_series:
        raise ValueError("DemandWatch USDA export sales indicators are missing source_series_key values.")

    run = await create_ingest_run(
        session,
        "demand_usda_export_sales",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"landing_url": (release_definition.metadata_ or {}).get("landing_url")},
    )
    client = USDAExportSalesClient()
    counters = IngestJobResult()
    failure_stage = "initializing"

    try:
        failure_stage = "fetch_summary"
        summary_raw = await client.get_commodity_summary()
        failure_stage = "fetch_highlights"
        highlights_raw = await client.get_weekly_highlights()
        summary_artifact = await archive_blob(
            session,
            source,
            "demand_usda_export_sales_summary",
            summary_raw,
            extension="xml",
            content_type="application/xml",
            metadata={"source_url": f"{STATIC_REPORTS_BASE}/CWRCommoditySummary.xml"},
        )
        highlights_artifact = await archive_blob(
            session,
            source,
            "demand_usda_export_sales_highlights",
            highlights_raw,
            extension="xml",
            content_type="application/xml",
            metadata={"source_url": f"{STATIC_REPORTS_BASE}/WeeklyHighlightsReport.xml"},
        )
        failure_stage = "parse_highlights"
        release_info = parse_export_sales_release_info(highlights_raw)
        failure_stage = "parse_summary"
        summary_items = parse_export_sales_summary(summary_raw)
        summary_items, duplicate_count = dedupe_records(
            summary_items,
            key=lambda item: (item.source_series_key, item.period_ending_on, item.marketing_year, item.source_item_ref),
        )
        if duplicate_count:
            bump_run_metadata_counter(run, "duplicate_observations_dropped", duplicate_count)
            logger.warning(
                "USDA export sales job dropped %d duplicate parsed rows",
                duplicate_count,
            )
        relevant_items = [item for item in summary_items if item.source_series_key in indicators_by_series]
        await archive_payload(
            session,
            source,
            "demand_usda_export_sales",
            {
                "released_on": release_info.released_on.isoformat(),
                "period_ending_on": release_info.period_ending_on.isoformat() if release_info.period_ending_on else None,
                "items": [item.to_item() for item in relevant_items],
            },
        )

        released_at = _release_datetime(release_definition, release_info.released_on)
        source_release = await upsert_source_release(
            session,
            source=source,
            release_definition=release_definition,
            release_key=f"demand_usda_export_sales:{release_info.released_on.isoformat()}",
            release_name=f"{release_definition.name} ({release_info.released_on.isoformat()})",
            released_at=released_at,
            period_start_at=(
                datetime.combine(release_info.period_ending_on, time(0, 0), tzinfo=ZoneInfo("UTC"))
                if release_info.period_ending_on
                else released_at
            ),
            period_end_at=(
                datetime.combine(release_info.period_ending_on, time(0, 0), tzinfo=ZoneInfo("UTC"))
                if release_info.period_ending_on
                else released_at
            ),
            artifact_id=summary_artifact.id,
            source_url=f"{STATIC_REPORTS_BASE}/CWRCommoditySummary.xml",
            metadata={
                "highlights_artifact_uri": highlights_artifact.storage_uri,
                "highlights_url": f"{STATIC_REPORTS_BASE}/WeeklyHighlightsReport.xml",
            },
        )
        run.source_release_id = source_release.id

        for item in relevant_items:
            indicator = indicators_by_series[item.source_series_key]
            period_end_at = datetime.combine(item.period_ending_on, time(0, 0), tzinfo=ZoneInfo("UTC"))
            counters.fetched_items += 1
            observation, changed = await upsert_observation_revision(
                session,
                ObservationInput(
                    indicator_id=indicator.id,
                    period_start_at=derive_period_start(period_end_at, indicator.frequency.value),
                    period_end_at=period_end_at,
                    release_id=source_release.id,
                    release_date=released_at,
                    vintage_at=released_at,
                    observation_kind=indicator.default_observation_kind.value,
                    value_native=item.value_mmt,
                    unit_native_code=indicator.native_unit_code,
                    value_canonical=item.value_mmt,
                    unit_canonical_code=indicator.canonical_unit_code,
                    source_item_ref=item.source_item_ref,
                    provenance_note="USDA Export Sales static report XML",
                    metadata={
                        "marketing_year": item.marketing_year,
                        "raw_net_sales_kt": item.raw_net_sales_kt,
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
        run.finished_at = utcnow()
        await process_pending_events(session)
        return counters
    except Exception as exc:
        run.status = "failed"
        annotate_run_failure(run, exc, stage=failure_stage)
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("USDA export sales job failed during %s: %s", failure_stage, exc)
        raise
    finally:
        await client.close()
