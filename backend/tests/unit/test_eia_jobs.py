from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.ingest.sources.eia import jobs as eia_jobs


UTC = timezone.utc
NOW = datetime(2026, 4, 3, 12, 0, tzinfo=UTC)


def make_indicator(*, frequency: str = "weekly", publication_lag_days: int = 4) -> SimpleNamespace:
    return SimpleNamespace(
        id="indicator-id",
        code="EIA_TEST",
        frequency=SimpleNamespace(value=frequency),
        publication_lag=timedelta(days=publication_lag_days),
    )


def test_build_eia_series_request_uses_incremental_overlap_from_latest_period() -> None:
    request = eia_jobs.build_eia_series_request(
        make_indicator(),
        run_mode="live",
        start_date=None,
        end_date=None,
        latest_period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
        observed_at=NOW,
    )

    assert request.start == date(2026, 3, 13)
    assert request.end is None
    assert request.length is None
    assert request.sort_desc is False


def test_build_eia_series_request_uses_long_fallback_window_when_history_is_empty() -> None:
    request = eia_jobs.build_eia_series_request(
        make_indicator(),
        run_mode="live",
        start_date=None,
        end_date=None,
        latest_period_end_at=None,
        observed_at=NOW,
    )

    assert request.start == date(2018, 4, 5)
    assert request.length is None


def test_invalid_recent_stale_observation_ids_flags_misdated_fresh_windows() -> None:
    observations = [
        SimpleNamespace(
            id="bad-window",
            period_end_at=datetime(2016, 9, 30, tzinfo=UTC),
            release_date=datetime(2026, 4, 3, tzinfo=UTC),
        ),
        SimpleNamespace(
            id="good-window",
            period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
            release_date=datetime(2026, 4, 2, tzinfo=UTC),
        ),
    ]

    assert eia_jobs.invalid_recent_stale_observation_ids(observations, observed_at=NOW) == ["bad-window"]


def test_is_stale_live_payload_detects_discontinued_series() -> None:
    assert eia_jobs.is_stale_live_payload(datetime(2020, 4, 3, tzinfo=UTC), observed_at=NOW) is True
    assert eia_jobs.is_stale_live_payload(datetime(2026, 3, 27, tzinfo=UTC), observed_at=NOW) is False


@pytest.mark.asyncio
async def test_purge_invalid_recent_stale_observations_demotes_bad_latest_rows(monkeypatch) -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.statements: list[object] = []
            self.flushed = False

        async def execute(self, statement) -> None:
            self.statements.append(statement)

        async def flush(self) -> None:
            self.flushed = True

    async def fake_recent_latest(_session, _indicator_id, *, limit: int = 16):
        assert limit == 16
        return [
            SimpleNamespace(
                id="bad-window",
                period_end_at=datetime(2020, 4, 3, tzinfo=UTC),
                release_date=datetime(2026, 4, 3, tzinfo=UTC),
            )
        ]

    monkeypatch.setattr(eia_jobs, "get_recent_latest_observations", fake_recent_latest)

    session = FakeSession()
    deleted = await eia_jobs.purge_invalid_recent_stale_observations(
        session,
        SimpleNamespace(id="indicator-id", code="EIA_TEST"),
        observed_at=NOW,
    )

    assert deleted == 1
    assert len(session.statements) == 1
    assert session.statements[0].__visit_name__ == "update"
    assert session.flushed is True


@pytest.mark.asyncio
async def test_purge_invalid_stale_history_deletes_nonlatest_rows_and_events(monkeypatch) -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.statements: list[object] = []
            self.flushed = False

        async def execute(self, statement):
            self.statements.append(statement)
            return None

        async def flush(self) -> None:
            self.flushed = True

    async def fake_observations(_session, _indicator_id):
        return [
            SimpleNamespace(
                id="bad-history",
                is_latest=False,
                period_end_at=datetime(2020, 4, 3, tzinfo=UTC),
                release_date=datetime(2026, 4, 3, tzinfo=UTC),
            ),
            SimpleNamespace(
                id="current-latest",
                is_latest=True,
                period_end_at=datetime(2026, 3, 27, tzinfo=UTC),
                release_date=datetime(2026, 4, 3, tzinfo=UTC),
            ),
        ]

    monkeypatch.setattr(eia_jobs, "get_indicator_observations", fake_observations)

    session = FakeSession()
    deleted = await eia_jobs.purge_invalid_stale_history(
        session,
        SimpleNamespace(id="indicator-id", code="EIA_TEST"),
        observed_at=NOW,
    )

    assert deleted == 1
    assert len(session.statements) == 2
    assert session.statements[0].__visit_name__ == "delete"
    assert session.statements[1].__visit_name__ == "delete"
    assert session.flushed is True
