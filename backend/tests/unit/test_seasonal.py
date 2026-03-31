from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from app.processing.seasonal import percentile, seasonal_period


def test_percentile_linear_interpolation() -> None:
    values = [100.0, 110.0, 120.0, 130.0, 140.0]
    assert percentile(values, 0.50) == 120.0
    assert percentile(values, 0.10) == 104.0


def test_seasonal_period_for_weekly_and_daily() -> None:
    weekly_observation = SimpleNamespace(period_end_at=datetime(2026, 3, 20, tzinfo=timezone.utc))
    daily_observation = SimpleNamespace(period_end_at=datetime(2026, 3, 20, tzinfo=timezone.utc))

    assert seasonal_period(weekly_observation, "weekly") == ("week_of_year", 12)
    assert seasonal_period(daily_observation, "daily") == ("day_of_year", 79)

