from __future__ import annotations

from datetime import datetime

from .http import CurlHttpClient
from .storage import CalendarRepository
from .types import AdapterRunStats


class CalendarIngestionService:
    def __init__(self, repository: CalendarRepository, client: CurlHttpClient):
        self.repository = repository
        self.client = client

    def run_adapter(self, adapter, *, as_of: datetime | None = None) -> AdapterRunStats:
        run_id = self.repository.create_ingest_run(adapter.slug, adapter.primary_url)
        try:
            candidates = adapter.collect(self.client, as_of=as_of)
            stats = self.repository.upsert_events(
                source_slug=adapter.slug,
                ingestion_pattern=adapter.pattern,
                candidates=candidates,
                detected_at=as_of,
            )
            self.repository.finish_ingest_run(
                run_id,
                status="succeeded",
                fetched_records=stats["fetched"],
                inserted_records=stats["inserted"],
                updated_records=stats["updated"],
                flagged_records=stats["flagged"],
            )
            return AdapterRunStats(
                source_slug=adapter.slug,
                fetched=stats["fetched"],
                inserted=stats["inserted"],
                updated=stats["updated"],
                flagged=stats["flagged"],
            )
        except Exception as exc:
            self.repository.finish_ingest_run(
                run_id,
                status="failed",
                fetched_records=0,
                inserted_records=0,
                updated_records=0,
                flagged_records=0,
                error_message=str(exc),
            )
            self.repository.log_failure(
                adapter.slug,
                str(exc),
                details={"source_url": adapter.primary_url},
                run_id=run_id,
            )
            return AdapterRunStats(
                source_slug=adapter.slug,
                fetched=0,
                inserted=0,
                updated=0,
                flagged=0,
                failed=True,
                error=str(exc),
            )

    def run_many(self, adapters, *, as_of: datetime | None = None) -> list[AdapterRunStats]:
        return [self.run_adapter(adapter, as_of=as_of) for adapter in adapters]
