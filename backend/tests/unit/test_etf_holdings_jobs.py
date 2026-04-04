from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from app.ingest.sources.etf_holdings import jobs as etf_jobs
from app.ingest.sources.etf_holdings.parsers import ParsedETFObservation


UTC = timezone.utc


def make_indicator(indicator_id: str, code: str, symbol: str, min_value: float) -> SimpleNamespace:
    return SimpleNamespace(
        id=indicator_id,
        code=code,
        source_series_key=symbol,
        metadata_={"sanity_bounds": {"min": min_value, "max": 2000}},
        frequency=SimpleNamespace(value="daily"),
        default_observation_kind=SimpleNamespace(value="actual"),
        canonical_unit_code="tonnes",
    )


@pytest.mark.asyncio
async def test_fetch_etf_holdings_accepts_early_gld_history_and_stays_partial_by_source(monkeypatch) -> None:
    run = SimpleNamespace(
        id="run-id",
        metadata_={},
        status=None,
        error_text=None,
        source_release_id=None,
        fetched_items=0,
        inserted_rows=0,
        updated_rows=0,
        quarantined_rows=0,
        finished_at=None,
    )
    source = SimpleNamespace(id="source-id")
    release_definition = SimpleNamespace(id="release-id", slug="etf_holdings", name="ETF Holdings")
    indicators = [
        make_indicator("gld-id", "ETF_GLD_HOLDINGS", "GLD", 1),
        make_indicator("iau-id", "ETF_IAU_HOLDINGS", "IAU", 1),
    ]
    inserted_payloads: list[object] = []
    quarantines: list[object] = []

    class FakeClient:
        async def get_bytes(self, url: str) -> bytes:
            assert url == etf_jobs.GLD_ARCHIVE_URL
            return b"gld-bytes"

        async def get_text(self, url: str) -> str:
            if url == etf_jobs.SLV_URL:
                raise RuntimeError("SLV unavailable")
            assert url == etf_jobs.IAU_URL
            return "<html>iau</html>"

        async def close(self) -> None:
            return None

    async def fake_get_source_bundle(session, source_slug: str, release_slug: str):
        assert source_slug == "etf"
        assert release_slug == "etf_holdings"
        return source, release_definition

    async def fake_get_release_indicators(session, release_slug: str):
        assert release_slug == "etf_holdings"
        return indicators

    async def fake_create_ingest_run(*args, **kwargs):
        return run

    async def fake_archive_blob(*args, **kwargs):
        return SimpleNamespace(storage_uri="archive://raw")

    async def fake_archive_payload(*args, **kwargs):
        return SimpleNamespace(id="structured-id", storage_uri="archive://structured")

    def fake_parse_gld_archive(payload: bytes, *, source_url: str):
        assert payload == b"gld-bytes"
        return [
            ParsedETFObservation(
                source_series_key="GLD",
                value=8.09,
                observation_date=date(2004, 11, 18),
                source_item_ref="GLD:launch",
                source_url=source_url,
                metadata={"provider": "SPDR"},
            )
        ]

    def fake_parse_ishares_current_holdings(payload: str, *, symbol: str, source_url: str):
        assert symbol == "IAU"
        return ParsedETFObservation(
            source_series_key="IAU",
            value=475.02,
            observation_date=date(2026, 4, 2),
            source_item_ref="IAU:Tonnes in Trust",
            source_url=source_url,
            metadata={"provider": "iShares"},
        )

    async def fake_upsert_source_release(*args, **kwargs):
        return SimpleNamespace(id="source-release-id")

    async def fake_upsert_observation_revision(session, payload):
        inserted_payloads.append(payload)
        return SimpleNamespace(revision_sequence=1), True

    async def fake_quarantine_value(*args, **kwargs):
        quarantines.append(kwargs)

    async def fake_emit(*args, **kwargs):
        return None

    async def fake_process_pending(*args, **kwargs):
        return None

    monkeypatch.setattr(etf_jobs, "ETFHoldingsClient", lambda: FakeClient())
    monkeypatch.setattr(etf_jobs, "get_source_bundle", fake_get_source_bundle)
    monkeypatch.setattr(etf_jobs, "get_release_indicators", fake_get_release_indicators)
    monkeypatch.setattr(etf_jobs, "create_ingest_run", fake_create_ingest_run)
    monkeypatch.setattr(etf_jobs, "archive_blob", fake_archive_blob)
    monkeypatch.setattr(etf_jobs, "archive_payload", fake_archive_payload)
    monkeypatch.setattr(etf_jobs, "parse_gld_archive", fake_parse_gld_archive)
    monkeypatch.setattr(etf_jobs, "parse_ishares_current_holdings", fake_parse_ishares_current_holdings)
    monkeypatch.setattr(etf_jobs, "upsert_source_release", fake_upsert_source_release)
    monkeypatch.setattr(etf_jobs, "upsert_observation_revision", fake_upsert_observation_revision)
    monkeypatch.setattr(etf_jobs, "quarantine_value", fake_quarantine_value)
    monkeypatch.setattr(etf_jobs, "emit_observation_event", fake_emit)
    monkeypatch.setattr(etf_jobs, "process_pending_events", fake_process_pending)
    monkeypatch.setattr(etf_jobs, "utcnow", lambda: datetime(2026, 4, 3, 12, 0, tzinfo=UTC))

    counters = await etf_jobs.fetch_etf_holdings(SimpleNamespace(), run_mode="live")

    assert counters.inserted_rows == 2
    assert counters.quarantined_rows == 0
    assert run.status == "partial"
    assert run.error_text == "SLV: SLV unavailable"
    assert [payload.indicator_id for payload in inserted_payloads] == ["gld-id", "iau-id"]
    assert inserted_payloads[0].value_native == 8.09
    assert quarantines == []

