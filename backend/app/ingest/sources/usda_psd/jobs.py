from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from app.ingest.common.archive import archive_blob, archive_payload
from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_release_indicators, get_source_bundle, upsert_source_release, utcnow
from app.modules.demandwatch.reliability import annotate_run_failure, bump_run_metadata_counter, dedupe_records
from app.ingest.sources.usda_psd.client import USDAPSDClient
from app.ingest.sources.usda_psd.parsers import parse_psd_commodity_response
from app.ingest.sources.usda_wasde.client import USDAWASDEClient, release_datetime
from app.processing.events import emit_observation_event, process_pending_events
from app.repositories.observations import ObservationInput, derive_period_start, upsert_observation_revision


logger = logging.getLogger(__name__)


async def fetch_demand_usda_psd(
    session: AsyncSession,
    run_mode: str = "live",
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestJobResult:
    del start_date, end_date

    source, release_definition = await get_source_bundle(session, "usda_psd", "demand_usda_wasde")
    indicators = await get_release_indicators(session, "demand_usda_wasde")
    indicators_by_series = {str(indicator.source_series_key): indicator for indicator in indicators if indicator.source_series_key}
    if not indicators_by_series:
        raise ValueError("DemandWatch USDA PSD indicators are missing source_series_key values.")

    run = await create_ingest_run(
        session,
        "demand_usda_wasde",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"landing_url": (release_definition.metadata_ or {}).get("landing_url")},
    )
    psd_client = USDAPSDClient()
    wasde_client = USDAWASDEClient()
    counters = IngestJobResult()
    release_cache: dict[str, object] = {}
    failure_stage = "initializing"

    try:
        failure_stage = "fetch_release_months"
        month_keys = await wasde_client.list_available_release_months()
        selected_months = set(month_keys if run_mode == "backfill" else month_keys[:2])
        releases = []
        for month_key in selected_months:
            failure_stage = f"fetch_release_manifest:{month_key}"
            releases.extend(await wasde_client.list_releases_for_month(month_key))
        releases_by_month = {release.month_key: release for release in releases}

        structured_rows: list[dict[str, object]] = []
        for commodity_code, indicator in indicators_by_series.items():
            failure_stage = f"fetch_psd_payload:{commodity_code}"
            raw = await psd_client.get_data_by_commodity(commodity_code)
            raw_artifact = await archive_blob(
                session,
                source,
                "demand_usda_psd",
                raw,
                extension="xml",
                content_type="application/xml",
                metadata={"commodity_code": commodity_code},
            )
            failure_stage = f"parse_psd_payload:{commodity_code}"
            parsed = parse_psd_commodity_response(raw, commodity_code=commodity_code)
            parsed, duplicate_count = dedupe_records(
                parsed,
                key=lambda item: (item.source_series_key, item.release_month, item.market_year, item.source_item_ref),
            )
            if duplicate_count:
                bump_run_metadata_counter(run, "duplicate_observations_dropped", duplicate_count)
                logger.warning(
                    "USDA PSD job dropped %d duplicate parsed rows for commodity %s",
                    duplicate_count,
                    commodity_code,
                )
            for item in parsed:
                release = releases_by_month.get(item.release_month)
                if release is None:
                    continue
                structured_rows.append(item.to_item())
                counters.fetched_items += 1
                released_at = release_datetime(release.released_on)
                source_release = release_cache.get(release.release_key)
                if source_release is None:
                    source_release = await upsert_source_release(
                        session,
                        source=source,
                        release_definition=release_definition,
                        release_key=f"demand_usda_wasde:{release.release_key}",
                        release_name=f"{release_definition.name} ({release.released_on.isoformat()})",
                        released_at=released_at,
                        period_start_at=released_at,
                        period_end_at=released_at,
                        artifact_id=raw_artifact.id,
                        source_url=release.source_url,
                        metadata={
                            "commodity_codes": sorted(indicators_by_series.keys()),
                            "workbook_url": release.workbook_url,
                            "pdf_url": release.pdf_url,
                            "release_month": item.release_month,
                        },
                    )
                    release_cache[release.release_key] = source_release
                run.source_release_id = source_release.id

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
                        value_native=item.value_mbu,
                        unit_native_code=indicator.native_unit_code,
                        value_canonical=item.value_mbu,
                        unit_canonical_code=indicator.canonical_unit_code,
                        source_item_ref=item.source_item_ref,
                        provenance_note=f"USDA PSD SOAP commodity {commodity_code}",
                        metadata={
                            "market_year": item.market_year,
                            "release_month": item.release_month,
                            "raw_unit_description": item.raw_unit_description,
                            "raw_value": item.raw_value,
                            "attribute_description": item.attribute_description,
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
                await emit_observation_event(
                    session,
                    indicator,
                    observation,
                    event_type="demand.observation_upserted",
                    producer_module_code="demandwatch",
                )

        await archive_payload(session, source, "demand_usda_psd", {"items": structured_rows})
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
        logger.exception("USDA PSD job failed during %s: %s", failure_stage, exc)
        raise
    finally:
        await psd_client.close()
        await wasde_client.close()
