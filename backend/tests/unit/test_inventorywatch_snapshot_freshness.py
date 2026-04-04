from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.processing.snapshots import inventorywatch_release_is_aged


def test_inventorywatch_release_aging_is_cadence_aware() -> None:
    now = datetime(2026, 4, 3, 12, 0, tzinfo=UTC)

    assert inventorywatch_release_is_aged("weekly", now - timedelta(days=30), now=now) is True
    assert inventorywatch_release_is_aged("monthly", now - timedelta(days=30), now=now) is False
    assert inventorywatch_release_is_aged("monthly", now - timedelta(days=90), now=now) is True
