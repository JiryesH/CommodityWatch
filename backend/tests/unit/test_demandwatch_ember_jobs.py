from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.ingest.sources.ember import jobs as ember_jobs


@pytest.mark.asyncio
async def test_fetch_demand_ember_monthly_electricity_marks_missing_api_key_partial(monkeypatch) -> None:
    run = SimpleNamespace(
        status=None,
        error_text=None,
        metadata_=None,
        finished_at=None,
        source_release_id=None,
        fetched_items=0,
        inserted_rows=0,
        updated_rows=0,
    )

    async def fake_get_source_bundle(_session, _source_slug, _release_slug):
        return SimpleNamespace(id="source-id"), SimpleNamespace(id="release-id", metadata_={})

    async def fake_get_release_indicators(_session, _release_slug):
        return []

    async def fake_create_ingest_run(_session, *_args, **_kwargs):
        return run

    def fake_ember_client():
        raise ValueError("CW_EMBER_API_KEY is required for Ember ingestion.")

    monkeypatch.setattr(ember_jobs, "get_source_bundle", fake_get_source_bundle)
    monkeypatch.setattr(ember_jobs, "get_release_indicators", fake_get_release_indicators)
    monkeypatch.setattr(ember_jobs, "create_ingest_run", fake_create_ingest_run)
    monkeypatch.setattr(ember_jobs, "EmberClient", fake_ember_client)

    result = await ember_jobs.fetch_demand_ember_monthly_electricity(SimpleNamespace(), run_mode="manual")

    assert result.fetched_items == 0
    assert run.status == "partial"
    assert run.error_text == "CW_EMBER_API_KEY is required for Ember ingestion."
    assert run.metadata_ == {"skipped_reason": "missing_api_key"}
    assert run.finished_at is not None
