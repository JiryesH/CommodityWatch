from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.ingest.common.jobs import IngestJobResult, create_ingest_run, get_source_bundle, utcnow
from app.ingest.sources.ice_certified.client import (
    ICECertifiedAccessBlockedError,
    ICECertifiedClient,
    ICECertifiedStructureChangedError,
    load_contracts,
)


logger = logging.getLogger(__name__)
ICE_CERTIFIED_COVERAGE_NOTE = (
    "ICE_NO11 remains unresolved and is suppressed until a stable public report id is available."
)


def _normalized_text(value: object | None) -> str:
    return " ".join(str(value or "").split()).casefold()


def _metadata_matches(expected: str | None, actual: str | None) -> bool:
    if expected is None:
        return True
    return _normalized_text(expected) == _normalized_text(actual)


async def fetch_ice_certified(session: AsyncSession, run_mode: str = "live") -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "ice", "ice_certified")
    run = await create_ingest_run(
        session,
        "ice_certified",
        source.id,
        release_definition.id,
        run_mode,
        metadata={"coverage_note": ICE_CERTIFIED_COVERAGE_NOTE},
    )
    client = ICECertifiedClient()
    counters = IngestJobResult()

    try:
        blocked_contracts: list[str] = []
        unresolved_contracts: list[str] = []
        validated_contracts: list[str] = []

        for contract in load_contracts():
            if contract.report_id is None:
                unresolved_contracts.append(
                    contract.source_series_key
                    if contract.availability_note is None
                    else f"{contract.source_series_key} ({contract.availability_note})"
                )
                continue
            metadata = await client.get_metadata(contract.report_id)
            if not _metadata_matches(contract.expected_report_name, metadata.name):
                raise ICECertifiedStructureChangedError(
                    f"ICE metadata name mismatch for {contract.source_series_key}: expected {contract.expected_report_name!r}, got {metadata.name!r}"
                )
            if not _metadata_matches(contract.expected_exchange, metadata.exchange):
                raise ICECertifiedStructureChangedError(
                    f"ICE exchange mismatch for {contract.source_series_key}: expected {contract.expected_exchange!r}, got {metadata.exchange!r}"
                )
            if not _metadata_matches(contract.expected_category, metadata.category_name):
                raise ICECertifiedStructureChangedError(
                    f"ICE category mismatch for {contract.source_series_key}: expected {contract.expected_category!r}, got {metadata.category_name!r}"
                )

            try:
                await client.get_criteria(contract.report_id)
            except ICECertifiedAccessBlockedError:
                blocked_contracts.append(contract.source_series_key)
                continue
            validated_contracts.append(contract.source_series_key)

        if blocked_contracts or unresolved_contracts:
            parts = []
            if blocked_contracts:
                parts.append(f"recaptcha-gated: {', '.join(sorted(blocked_contracts))}")
            if unresolved_contracts:
                parts.append(f"unresolved-report-ids: {', '.join(sorted(unresolved_contracts))}")
            run.status = "partial"
            run.error_text = "; ".join(parts)
        else:
            run.status = "success"
        run.metadata_ = {
            **(run.metadata_ or {}),
            "validated_contracts": validated_contracts,
            "blocked_contracts": blocked_contracts,
            "unresolved_contracts": unresolved_contracts,
        }
        run.fetched_items = len(validated_contracts)
        run.finished_at = utcnow()
        counters.fetched_items = len(validated_contracts)
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("ICE certified job failed: %s", exc)
        raise
    finally:
        await client.close()
