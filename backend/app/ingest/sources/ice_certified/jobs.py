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


async def fetch_ice_certified(session: AsyncSession, run_mode: str = "live") -> IngestJobResult:
    source, release_definition = await get_source_bundle(session, "ice", "ice_certified")
    run = await create_ingest_run(session, "ice_certified", source.id, release_definition.id, run_mode)
    client = ICECertifiedClient()
    counters = IngestJobResult()

    try:
        blocked_contracts: list[str] = []
        unresolved_contracts: list[str] = []
        validated_contracts: list[str] = []

        for contract in load_contracts():
            if contract.report_id is None:
                unresolved_contracts.append(contract.source_series_key)
                continue
            metadata = await client.get_metadata(contract.report_id)
            if contract.expected_report_name and metadata.name != contract.expected_report_name:
                raise ICECertifiedStructureChangedError(
                    f"ICE metadata name mismatch for {contract.source_series_key}: expected {contract.expected_report_name!r}, got {metadata.name!r}"
                )
            if contract.expected_exchange and metadata.exchange != contract.expected_exchange:
                raise ICECertifiedStructureChangedError(
                    f"ICE exchange mismatch for {contract.source_series_key}: expected {contract.expected_exchange!r}, got {metadata.exchange!r}"
                )
            if contract.expected_category and metadata.category_name != contract.expected_category:
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
        run.fetched_items = len(validated_contracts)
        run.finished_at = utcnow()
        return counters
    except Exception as exc:
        run.status = "failed"
        run.error_text = str(exc)
        run.finished_at = utcnow()
        logger.exception("ICE certified job failed: %s", exc)
        raise
    finally:
        await client.close()
