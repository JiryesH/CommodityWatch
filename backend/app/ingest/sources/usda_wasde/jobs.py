from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.indicators import Indicator
from app.db.models.sources import SourceRelease
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
from app.ingest.sources.usda_wasde.client import USDAWASDEClient, release_datetime
from app.ingest.sources.usda_wasde.parsers import ParsedWASDEObservation, parse_wasde_workbook
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


RETRY_ATTEMPTS = 4
RETRY_INTERVAL_SECONDS = 15 * 60
DEV_RETRY_INTERVAL_SECONDS = 30


logger = logging.getLogger(__name__)


def retry_interval_seconds(run_mode: str) -> int:
    if run_mode == "manual" or not get_settings().is_production:
        return DEV_RETRY_INTERVAL_SECONDS
    return RETRY_INTERVAL_SECONDS


def _release_schedule_dates(indicators: list[Indicator]) -> set[date]:
    scheduled: set[date] = set()
    for indicator in indicators:
        release_schedule = indicator.metadata_.get("release_schedule") or {}
        for raw in release_schedule.get("dates") or []:
            try:
                scheduled.add(date.fromisoformat(str(raw)))
            except ValueError:
                continue
    return scheduled


async def _latest_release_in_db(session: AsyncSession, source_id) -> date | None:
    latest = await session.scalar(
        select(SourceRelease)
        .where(SourceRelease.source_id == source_id)
        .order_by(SourceRelease.released_at.desc().nullslast(), SourceRelease.created_at.desc())
        .limit(1)
    )
    if latest is None or latest.released_at is None:
        return None
    return latest.released_at.date()


async def _candidate_release_months(
    client: USDAWASDEClient,
    *,
    run_mode: str,
    latest_db_release: date | None,
    requested_months: list[str] | None = None,
) -> list[str]:
    if requested_months:
        return requested_months
    available_months = await client.list_available_release_months()
    if run_mode == "backfill":
        return available_months
    if latest_db_release is None:
        return available_months[:2]
    threshold = latest_db_release.strftime("%Y-%m")
    return [month_key for month_key in available_months if month_key >= threshold][:3]


async def _fetch_candidate_releases(
    client: USDAWASDEClient,
    *,
    run_mode: str,
    latest_db_release: date | None,
    requested_months: list[str] | None = None,
) -> list:
    month_keys = await _candidate_release_months(
        client,
        run_mode=run_mode,
        latest_db_release=latest_db_release,
        requested_months=requested_months,
    )
    releases = []
    for month_key in month_keys:
        releases.extend(await client.list_releases_for_month(month_key))
    unique = {release.release_key: release for release in releases}
    ordered = sorted(unique.values(), key=lambda item: item.released_on)
    if run_mode != "backfill":
        return ordered[-1:] if ordered else []
    return ordered


async def _load_release(
    session: AsyncSession,
    *,
    release,
    source,
    release_definition,
    indicators_by_key: dict[str, Indicator],
    run,
    counters: IngestJobResult,
    client: USDAWASDEClient,
    run_mode: str,
) -> None:
    workbook = await client.get_workbook(release)
    raw_artifact = await archive_blob(
        session,
        source,
        "usda_wasde",
        workbook,
        extension="xls",
        content_type="application/vnd.ms-excel",
        metadata={
            "release_date": release.released_on.isoformat(),
            "workbook_url": release.workbook_url,
            "source_url": release.source_url,
            "month_key": release.month_key,
        },
    )
    parsed_items = parse_wasde_workbook(workbook, release_date=release.released_on)
    structured_artifact = await archive_payload(
        session,
        source,
        "usda_wasde",
        {"items": [item.to_item() for item in parsed_items]},
    )

    released_at = release_datetime(release.released_on)
    source_release = await upsert_source_release(
        session,
        source=source,
        release_definition=release_definition,
        release_key=f"{release_definition.slug}:{release.release_key}",
        release_name=f"{release_definition.name} ({release.released_on.isoformat()})",
        released_at=released_at,
        period_start_at=released_at,
        period_end_at=released_at,
        artifact_id=raw_artifact.id,
        source_url=release.source_url,
        notes=release.title,
        metadata={
            "pdf_url": release.pdf_url,
            "structured_artifact_uri": structured_artifact.storage_uri,
            "workbook_url": release.workbook_url,
            "month_key": release.month_key,
        },
    )
    run.source_release_id = source_release.id

    for parsed in parsed_items:
        counters.fetched_items += 1
        indicator = indicators_by_key.get(parsed.source_series_key)
        if indicator is None:
            logger.warning("Skipping unseeded WASDE series %s", parsed.source_series_key)
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
                unit_native_code=parsed.unit_native_code,
                reason=f"outside_bounds[{lower_bound},{upper_bound}]",
                artifact_uri=raw_artifact.storage_uri,
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
                unit_native_code=parsed.unit_native_code,
                value_canonical=parsed.value,
                unit_canonical_code=indicator.canonical_unit_code,
                source_item_ref=parsed.source_item_ref,
                provenance_note=f"WASDE workbook {release.workbook_url}",
                metadata={
                    "market_year": parsed.market_year,
                    "marketing_year_label": parsed.marketing_year_label,
                    "sheet_name": parsed.sheet_name,
                    "run_mode": run_mode,
                },
            ),
        )
        if not changed:
            continue
        if observation.revision_sequence > 1:
            counters.updated_rows += 1
        else:
            counters.inserted_rows += 1
        await emit_observation_event(session, indicator, observation)


async def _run_usda_wasde(
    session: AsyncSession,
    *,
    run_mode: str = "live",
    requested_months: list[str] | None = None,
) -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "usda_psd", "usda_wasde")
    indicators = await get_release_indicators(session, "usda_wasde")
    indicators_by_key = {str(indicator.source_series_key): indicator for indicator in indicators}
    run = await create_ingest_run(
        session,
        "usda_wasde",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"requested_months": requested_months or []},
    )
    client = USDAWASDEClient()
    counters = IngestJobResult()
    try:
        latest_db_release = await _latest_release_in_db(session, source.id)
        releases = await _fetch_candidate_releases(
            client,
            run_mode=run_mode,
            latest_db_release=latest_db_release,
            requested_months=requested_months,
        )
        logger.info("USDA WASDE job evaluating %d release(s)", len(releases))

        for release in releases:
            await _load_release(
                session,
                release=release,
                source=source,
                release_definition=release_definition,
                indicators_by_key=indicators_by_key,
                run=run,
                counters=counters,
                client=client,
                run_mode=run_mode,
            )

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
        logger.exception("USDA WASDE job failed: %s", exc)
        raise
    finally:
        await client.close()


async def fetch_usda_wasde(
    session: AsyncSession,
    run_mode: str = "live",
    release_months: list[str] | None = None,
) -> IngestJobResult:
    indicators = await get_release_indicators(session, "usda_wasde")
    scheduled_dates = _release_schedule_dates(indicators)
    retry_interval = retry_interval_seconds(run_mode)
    attempts = RETRY_ATTEMPTS if run_mode != "backfill" else 1
    now_et = datetime.now(ZoneInfo("America/New_York")).date()
    should_retry_for_today = now_et in scheduled_dates and not release_months
    latest_seen_release = None

    for attempt in range(attempts):
        latest_seen_release = await _latest_release_in_db(session, (await get_source_bundle(session, "usda_psd", "usda_wasde"))[0].id)
        result = await _run_usda_wasde(
            session,
            run_mode=run_mode if attempt == 0 or run_mode == "backfill" else "retry",
            requested_months=release_months,
        )
        latest_after = await _latest_release_in_db(session, (await get_source_bundle(session, "usda_psd", "usda_wasde"))[0].id)
        if (
            not should_retry_for_today
            or latest_after == now_et
            or attempt == attempts - 1
            or latest_after != latest_seen_release
        ):
            return result
        await session.rollback()
        logger.warning(
            "USDA WASDE release for %s not visible yet; retrying in %d seconds (attempt %d/%d)",
            now_et.isoformat(),
            retry_interval,
            attempt + 1,
            attempts,
        )
        await asyncio.sleep(retry_interval)

    return IngestJobResult()
