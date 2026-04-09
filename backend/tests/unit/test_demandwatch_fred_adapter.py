from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import httpx

from app.ingest.sources.fred import jobs as fred_jobs
from app.ingest.sources.fred.parsers import parse_fred_observations, parse_fred_release, parse_fred_release_dates, selected_vintage_dates


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "fred"


def _load_json(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def test_parse_fred_release_metadata() -> None:
    release = parse_fred_release(_load_json("fred_indpro_release.json"))

    assert release.release_id == 10
    assert release.name == "Industrial Production and Capacity Utilization"
    assert release.link == "https://fred.stlouisfed.org/release?rid=10"


def test_parse_fred_release_dates() -> None:
    release_dates = parse_fred_release_dates(_load_json("fred_indpro_release_dates.json"))

    assert release_dates == [date(2026, 4, 16), date(2026, 3, 17), date(2026, 2, 18)]


def test_parse_fred_monthly_observations_uses_month_end() -> None:
    parsed = parse_fred_observations(_load_json("fred_indpro_observations.json"), "monthly")

    assert len(parsed) == 2
    assert parsed[0].period_start_at == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert parsed[0].period_end_at == datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc)
    assert parsed[1].value == 101.8


def test_selected_vintage_dates_keeps_recent_live_releases_and_respects_backfill_cutoff() -> None:
    release_dates = [
        date(2026, 4, 16),
        date(2026, 3, 17),
        date(2026, 2, 18),
        date(2026, 1, 16),
        date(2025, 12, 17),
    ]

    assert selected_vintage_dates(release_dates, run_mode="live", start_date=None) == [
        date(2026, 2, 18),
        date(2026, 3, 17),
        date(2026, 4, 16),
    ]
    assert selected_vintage_dates(release_dates, run_mode="backfill", start_date=date(2026, 2, 1)) == [
        date(2026, 1, 16),
        date(2026, 2, 18),
        date(2026, 3, 17),
        date(2026, 4, 16),
    ]


def test_retryable_fred_vintage_errors_only_soft_fail_outside_backfill() -> None:
    request = httpx.Request("GET", "https://fred.stlouisfed.org")
    retryable_http = httpx.HTTPStatusError(
        "server error",
        request=request,
        response=httpx.Response(500, request=request),
    )

    assert fred_jobs._should_soft_fail_vintage_error(exc=retryable_http, run_mode="manual") is True
    assert fred_jobs._should_soft_fail_vintage_error(exc=retryable_http, run_mode="backfill") is False
    assert fred_jobs._should_soft_fail_vintage_error(exc=ValueError("bad payload"), run_mode="manual") is False
