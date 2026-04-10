from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from app.ingest.sources.oecd import jobs as oecd_jobs


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "oecd" / "oecd_cli_sample.csv"


@pytest.mark.asyncio
async def test_fetch_demand_oecd_cli_upserts_snapshot_rows(monkeypatch) -> None:
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
    source = SimpleNamespace(id=uuid.uuid4())
    release_definition = SimpleNamespace(
        id=uuid.uuid4(),
        name="DemandWatch OECD Composite Leading Indicators",
        metadata_={"landing_url": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/"},
    )
    indicators = [
        SimpleNamespace(
            id=uuid.uuid4(),
            code="OECD_JAPAN_COMPOSITE_LEADING_INDICATOR",
            source_series_key="JPN",
            default_observation_kind=SimpleNamespace(value="actual"),
            native_unit_code="index",
            canonical_unit_code="index",
        ),
        SimpleNamespace(
            id=uuid.uuid4(),
            code="OECD_SOUTH_KOREA_COMPOSITE_LEADING_INDICATOR",
            source_series_key="KOR",
            default_observation_kind=SimpleNamespace(value="actual"),
            native_unit_code="index",
            canonical_unit_code="index",
        ),
        SimpleNamespace(
            id=uuid.uuid4(),
            code="OECD_INDIA_COMPOSITE_LEADING_INDICATOR",
            source_series_key="IND",
            default_observation_kind=SimpleNamespace(value="actual"),
            native_unit_code="index",
            canonical_unit_code="index",
        ),
    ]
    observation_inputs = []
    source_release_calls = []
    emitted = []
    client_calls = []
    fixed_now = datetime(2026, 4, 9, 12, 0, tzinfo=UTC)

    async def fake_get_source_bundle(_session, _source_slug, _release_slug):
        return source, release_definition

    async def fake_get_release_indicators(_session, _release_slug):
        return indicators

    async def fake_create_ingest_run(_session, *_args, **_kwargs):
        return run

    class FakeOECDClient:
        async def get_cli_snapshot(self, ref_areas, *, start_date=None, end_date=None):
            client_calls.append((tuple(ref_areas), start_date, end_date))
            return FIXTURE_PATH.read_bytes()

        async def close(self):
            return None

    async def fake_archive_blob(_session, _source, _job_name, _raw, **_kwargs):
        return SimpleNamespace(id=uuid.uuid4(), storage_uri="/tmp/oecd.csv")

    async def fake_upsert_source_release(_session, **kwargs):
        source_release_calls.append(kwargs)
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_upsert_observation_revision(_session, observation_input):
        observation_inputs.append(observation_input)
        return SimpleNamespace(revision_sequence=1), True

    async def fake_emit_observation_event(_session, indicator, observation, **_kwargs):
        emitted.append((indicator.code, observation.revision_sequence))

    async def fake_process_pending_events(_session):
        return None

    monkeypatch.setattr(oecd_jobs, "get_source_bundle", fake_get_source_bundle)
    monkeypatch.setattr(oecd_jobs, "get_release_indicators", fake_get_release_indicators)
    monkeypatch.setattr(oecd_jobs, "create_ingest_run", fake_create_ingest_run)
    monkeypatch.setattr(oecd_jobs, "OECDClient", FakeOECDClient)
    monkeypatch.setattr(oecd_jobs, "archive_blob", fake_archive_blob)
    monkeypatch.setattr(oecd_jobs, "upsert_source_release", fake_upsert_source_release)
    monkeypatch.setattr(oecd_jobs, "upsert_observation_revision", fake_upsert_observation_revision)
    monkeypatch.setattr(oecd_jobs, "emit_observation_event", fake_emit_observation_event)
    monkeypatch.setattr(oecd_jobs, "process_pending_events", fake_process_pending_events)
    monkeypatch.setattr(oecd_jobs, "utcnow", lambda: fixed_now)

    result = await oecd_jobs.fetch_demand_oecd_cli(
        SimpleNamespace(),
        run_mode="backfill",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
    )

    assert client_calls == [(("JPN", "KOR", "IND"), date(2026, 1, 1), date(2026, 3, 31))]
    assert result.fetched_items == 6
    assert result.inserted_rows == 6
    assert result.updated_rows == 0
    assert len(source_release_calls) == 1
    assert source_release_calls[0]["metadata"]["stores_latest_snapshot_only"] is True
    assert len(observation_inputs) == 6
    assert {item.source_item_ref for item in observation_inputs} == {
        "IND:2025-12",
        "IND:2026-01",
        "JPN:2026-02",
        "JPN:2026-03",
        "KOR:2025-12",
        "KOR:2026-01",
    }
    assert run.status == "success"
    assert run.finished_at == fixed_now
    assert len(emitted) == 6
