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
from app.ingest.sources.comex_warehouse.client import (
    COMEX_REPORT_URLS,
    COMEXWarehouseAccessBlockedError,
    COMEXWarehouseClient,
)
from app.ingest.sources.comex_warehouse.parsers import ParsedCOMEXObservation, parse_comex_workbook
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


logger = logging.getLogger(__name__)


def _release_datetime(report_date: date) -> datetime:
    release_local = datetime.combine(report_date, time(17, 0), tzinfo=ZoneInfo("America/New_York"))
    return release_local.astimezone(timezone.utc)


async def fetch_comex_warehouse(session: AsyncSession, run_mode: str = "live") -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "comex", "comex_warehouse")
    indicators = await get_release_indicators(session, "comex_warehouse")
    indicators_by_key: dict[str, Indicator] = {str(indicator.source_series_key): indicator for indicator in indicators}
    run = await create_ingest_run(session, "comex_warehouse", source.id, release_definition.id, run_mode)
    client = COMEXWarehouseClient()
    counters = IngestJobResult()

    try:
        parsed_items: list[ParsedCOMEXObservation] = []
        blocked_symbols: list[str] = []
        raw_artifacts: dict[str, str] = {}
        for symbol, url in COMEX_REPORT_URLS.items():
            try:
                raw = await client.get_report(symbol)
            except COMEXWarehouseAccessBlockedError:
                blocked_symbols.append(symbol)
                continue

            artifact = await archive_blob(
                session,
                source,
                f"comex_warehouse_{symbol.lower()}",
                raw,
                extension="xls",
                content_type="application/vnd.ms-excel",
                metadata={"symbol": symbol, "source_url": url},
            )
            raw_artifacts[symbol] = artifact.storage_uri
            parsed_items.append(parse_comex_workbook(raw, symbol=symbol, source_url=url))

        if not parsed_items:
            message = (
                "CME warehouse workbooks are currently blocked from this environment."
                if blocked_symbols
                else "No COMEX warehouse workbooks were parsed."
            )
            logger.warning(message)
            run.status = "partial"
            run.error_text = message
            run.finished_at = utcnow()
            return counters

        structured_artifact = await archive_payload(
            session,
            source,
            "comex_warehouse",
            {"items": [item.to_item() for item in parsed_items]},
        )

        grouped: dict[str, list[ParsedCOMEXObservation]] = {}
        for parsed in parsed_items:
            grouped.setdefault(parsed.report_date.isoformat(), []).append(parsed)

        for observation_key, rows in grouped.items():
            report_date = rows[0].report_date
            released_at = _release_datetime(report_date)
            source_release = await upsert_source_release(
                session,
                source=source,
                release_definition=release_definition,
                release_key=f"{release_definition.slug}:{observation_key}",
                release_name=f"{release_definition.name} ({observation_key})",
                released_at=released_at,
                period_start_at=released_at,
                period_end_at=released_at,
                artifact_id=structured_artifact.id,
                source_url=rows[0].source_url,
                metadata={"structured_artifact_uri": structured_artifact.storage_uri},
            )
            run.source_release_id = source_release.id

            for parsed in rows:
                counters.fetched_items += 1
                indicator = indicators_by_key.get(parsed.source_series_key)
                if indicator is None:
                    logger.warning("Skipping unseeded COMEX symbol %s", parsed.source_series_key)
                    continue

                is_valid, lower_bound, upper_bound = value_within_bounds(indicator, parsed.total)
                if not is_valid:
                    await quarantine_value(
                        session,
                        run=run,
                        source=source,
                        indicator=indicator,
                        period_end_at=released_at,
                        value=parsed.total,
                        unit_native_code=indicator.native_unit_code,
                        reason=f"outside_bounds[{lower_bound},{upper_bound}]",
                        artifact_uri=raw_artifacts.get(parsed.source_series_key, structured_artifact.storage_uri),
                        payload=parsed.to_item(),
                    )
                    counters.quarantined_rows += 1
                    continue

                observation, changed = await upsert_observation_revision(
                    session,
                    ObservationInput(
                        indicator_id=indicator.id,
                        period_start_at=derive_period_start(released_at, indicator.frequency.value),
                        period_end_at=released_at,
                        release_id=source_release.id,
                        release_date=released_at,
                        vintage_at=released_at,
                        observation_kind=indicator.default_observation_kind.value,
                        value_native=parsed.total,
                        unit_native_code=indicator.native_unit_code,
                        value_canonical=parsed.total,
                        unit_canonical_code=indicator.canonical_unit_code,
                        source_item_ref=parsed.source_item_ref,
                        provenance_note=f"COMEX warehouse workbook {parsed.source_url}",
                        metadata={"registered": parsed.registered, "eligible": parsed.eligible, "run_mode": run_mode},
                    ),
                )
                if not changed:
                    continue
                if observation.revision_sequence > 1:
                    counters.updated_rows += 1
                else:
                    counters.inserted_rows += 1
                await emit_observation_event(session, indicator, observation)

        run.status = "partial" if blocked_symbols or counters.quarantined_rows else "success"
        run.error_text = (
            f"Blocked symbols: {', '.join(sorted(blocked_symbols))}" if blocked_symbols else None
        )
        run.fetched_items = counters.fetched_items
        run.inserted_rows = counters.inserted_rows
        run.updated_rows = counters.updated_rows
        run.quarantined_rows = counters.quarantined_rows
        run.finished_at = utcnow()
        await process_pending_events(session)
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("COMEX warehouse job failed: %s", exc)
        raise
    finally:
        await client.close()
