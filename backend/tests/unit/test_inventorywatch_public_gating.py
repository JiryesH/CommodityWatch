from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.processing.snapshots import public_inventorywatch_seasonality_allowed


def test_public_inventorywatch_seasonality_requires_real_history() -> None:
    seasonal_indicator = SimpleNamespace(is_seasonal=True, frequency="weekly")
    current_only_indicator = SimpleNamespace(is_seasonal=False, frequency="weekly")

    assert public_inventorywatch_seasonality_allowed(current_only_indicator, observation_count=10, seasonal_samples=10) is False
    assert public_inventorywatch_seasonality_allowed(seasonal_indicator, observation_count=0, seasonal_samples=10) is False
    assert public_inventorywatch_seasonality_allowed(
        seasonal_indicator,
        observation_count=10,
        seasonal_point_count=25,
        seasonal_samples=4,
    ) is False
    assert public_inventorywatch_seasonality_allowed(
        seasonal_indicator,
        observation_count=2,
        seasonal_point_count=26,
        seasonal_samples=2,
    ) is False
    assert public_inventorywatch_seasonality_allowed(
        seasonal_indicator,
        observation_count=3,
        seasonal_point_count=26,
        seasonal_samples=None,
    ) is True
    assert public_inventorywatch_seasonality_allowed(
        seasonal_indicator,
        observation_count=1,
        seasonal_point_count=26,
        seasonal_samples=3,
    ) is True
