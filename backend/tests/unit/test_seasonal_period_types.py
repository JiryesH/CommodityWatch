from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from app.processing.seasonal import indicator_period_type, seasonal_period_for_indicator


def test_indicator_period_type_respects_metadata_period_type() -> None:
    indicator = SimpleNamespace(
        metadata_={"period_type": "marketing_month"},
        frequency=SimpleNamespace(value="monthly"),
    )
    assert indicator_period_type(indicator) == "marketing_year_month"


def test_seasonal_period_for_indicator_uses_marketing_year_start() -> None:
    indicator = SimpleNamespace(
        metadata_={"period_type": "marketing_month", "marketing_year_start_month": 9},
        frequency=SimpleNamespace(value="monthly"),
    )
    observation = SimpleNamespace(period_end_at=datetime(2026, 3, 10, tzinfo=timezone.utc))
    assert seasonal_period_for_indicator(indicator, observation) == ("marketing_year_month", 7)
