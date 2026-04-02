from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, time, timezone
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
from app.ingest.sources.etf_holdings.client import ETFHoldingsClient, GLD_ARCHIVE_URL, IAU_URL, SLV_URL
from app.ingest.sources.etf_holdings.parsers import (
    ParsedETFObservation,
    parse_gld_archive,
    parse_ishares_current_holdings,
)
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


logger = logging.getLogger(__name__)


def _release_datetime(observation_date) -> datetime:
    release_local = datetime.combine(observation_date, time(20, 0), tzinfo=ZoneInfo("America/New_York"))
    return release_local.astimezone(timezone.utc)


async def fetch_etf_holdings(session: AsyncSession, run_mode: str = "live") -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "etf", "etf_holdings")
    indicators = await get_release_indicators(session, "etf_holdings")
    indicators_by_key: dict[str, Indicator] = {str(indicator.source_series_key): indicator for indicator in indicators}
    run = await create_ingest_run(session, "etf_holdings", source.id, release_definition.id, run_mode)
    client = ETFHoldingsClient()
    counters = IngestJobResult()

    try:
        gld_archive = await client.get_bytes(GLD_ARCHIVE_URL)
        gld_raw_artifact = await archive_blob(
            session,
            source,
            "etf_holdings_gld",
            gld_archive,
            extension="xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            metadata={"symbol": "GLD", "source_url": GLD_ARCHIVE_URL},
        )
        gld_points = parse_gld_archive(gld_archive, source_url=GLD_ARCHIVE_URL)
        latest_gld = gld_points[-1]

        slv_html = await client.get_text(SLV_URL)
        slv_raw_artifact = await archive_blob(
            session,
            source,
            "etf_holdings_slv",
            slv_html.encode("utf-8"),
            extension="html",
            content_type="text/html",
            metadata={"symbol": "SLV", "source_url": SLV_URL},
        )
        slv_point = parse_ishares_current_holdings(slv_html, symbol="SLV", source_url=SLV_URL)

        iau_html = await client.get_text(IAU_URL)
        iau_raw_artifact = await archive_blob(
            session,
            source,
            "etf_holdings_iau",
            iau_html.encode("utf-8"),
            extension="html",
            content_type="text/html",
            metadata={"symbol": "IAU", "source_url": IAU_URL},
        )
        iau_point = parse_ishares_current_holdings(iau_html, symbol="IAU", source_url=IAU_URL)

        parsed_items = [latest_gld, slv_point, iau_point]
        structured_artifact = await archive_payload(
            session,
            source,
            "etf_holdings",
            {"items": [item.to_item() for item in parsed_items]},
        )

        grouped: dict[str, list[tuple[ParsedETFObservation, str]]] = defaultdict(list)
        grouped[latest_gld.observation_date.isoformat()].append((latest_gld, gld_raw_artifact.storage_uri))
        grouped[slv_point.observation_date.isoformat()].append((slv_point, slv_raw_artifact.storage_uri))
        grouped[iau_point.observation_date.isoformat()].append((iau_point, iau_raw_artifact.storage_uri))

        for observation_key, rows in grouped.items():
            released_at = _release_datetime(rows[0][0].observation_date)
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
                source_url=rows[0][0].source_url,
                metadata={
                    "symbols": [item.source_series_key for item, _artifact_uri in rows],
                    "structured_artifact_uri": structured_artifact.storage_uri,
                },
            )
            run.source_release_id = source_release.id

            for parsed, artifact_uri in rows:
                counters.fetched_items += 1
                indicator = indicators_by_key.get(parsed.source_series_key)
                if indicator is None:
                    logger.warning("Skipping unseeded ETF symbol %s", parsed.source_series_key)
                    continue

                is_valid, lower_bound, upper_bound = value_within_bounds(indicator, parsed.value)
                if not is_valid:
                    await quarantine_value(
                        session,
                        run=run,
                        source=source,
                        indicator=indicator,
                        period_end_at=released_at,
                        value=parsed.value,
                        unit_native_code="tonnes",
                        reason=f"outside_bounds[{lower_bound},{upper_bound}]",
                        artifact_uri=artifact_uri,
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
                        value_native=parsed.value,
                        unit_native_code="tonnes",
                        value_canonical=parsed.value,
                        unit_canonical_code=indicator.canonical_unit_code,
                        source_item_ref=parsed.source_item_ref,
                        provenance_note=f"ETF holdings source {parsed.source_url}",
                        metadata={"run_mode": run_mode, **parsed.metadata},
                    ),
                )
                if not changed:
                    continue
                if observation.revision_sequence > 1:
                    counters.updated_rows += 1
                else:
                    counters.inserted_rows += 1
                await emit_observation_event(session, indicator, observation)

        run.status = "partial" if counters.quarantined_rows else "success"
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
        logger.exception("ETF holdings job failed: %s", exc)
        raise
    finally:
        await client.close()
