from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from app.db.models.observations import Observation
from app.repositories.observations import ObservationInput, upsert_observation_revision


def test_revision_detection_marks_prior_observation_not_latest() -> None:
    indicator_id = uuid4()
    existing = Observation(
        indicator_id=indicator_id,
        period_start_at=datetime(2026, 3, 14, tzinfo=timezone.utc),
        period_end_at=datetime(2026, 3, 20, tzinfo=timezone.utc),
        release_id=None,
        release_date=datetime(2026, 3, 25, tzinfo=timezone.utc),
        vintage_at=datetime(2026, 3, 25, tzinfo=timezone.utc),
        observation_kind="actual",
        value_native=100.0,
        unit_native_code="kb",
        value_canonical=100.0,
        unit_canonical_code="kb",
        is_latest=True,
        revision_sequence=1,
        metadata_={},
    )

    exact_vintage_result = MagicMock()
    exact_vintage_result.scalar_one_or_none.return_value = None
    latest_result = MagicMock()
    latest_result.scalar_one_or_none.return_value = existing
    session = AsyncMock()
    session.execute.side_effect = [exact_vintage_result, latest_result]
    session.flush = AsyncMock()
    session.add = MagicMock()

    observation, changed = asyncio.run(
        upsert_observation_revision(
            session,
            ObservationInput(
                indicator_id=indicator_id,
                period_start_at=existing.period_start_at,
                period_end_at=existing.period_end_at,
                release_id=None,
                release_date=existing.release_date,
                vintage_at=datetime(2026, 3, 26, tzinfo=timezone.utc),
                observation_kind="actual",
                value_native=95.0,
                unit_native_code="kb",
                value_canonical=95.0,
                unit_canonical_code="kb",
            ),
        )
    )

    assert changed is True
    assert existing.is_latest is False
    assert observation.revision_sequence == 2


def test_duplicate_vintage_is_idempotent_noop() -> None:
    indicator_id = uuid4()
    existing = Observation(
        indicator_id=indicator_id,
        period_start_at=datetime(2026, 4, 1, 21, 0, tzinfo=timezone.utc),
        period_end_at=datetime(2026, 4, 1, 21, 0, tzinfo=timezone.utc),
        release_id=None,
        release_date=datetime(2026, 4, 1, 21, 0, tzinfo=timezone.utc),
        vintage_at=datetime(2026, 4, 1, 21, 0, tzinfo=timezone.utc),
        observation_kind="actual",
        value_native=31533900.538,
        unit_native_code="toz",
        value_canonical=31533900.538,
        unit_canonical_code="toz",
        is_latest=True,
        revision_sequence=1,
        metadata_={},
    )

    exact_vintage_result = MagicMock()
    exact_vintage_result.scalar_one_or_none.return_value = existing
    session = AsyncMock()
    session.execute.return_value = exact_vintage_result
    session.flush = AsyncMock()
    session.add = MagicMock()

    observation, changed = asyncio.run(
        upsert_observation_revision(
            session,
            ObservationInput(
                indicator_id=indicator_id,
                period_start_at=existing.period_start_at,
                period_end_at=existing.period_end_at,
                release_id=None,
                release_date=existing.release_date,
                vintage_at=existing.vintage_at,
                observation_kind="actual",
                value_native=existing.value_native,
                unit_native_code=existing.unit_native_code,
                value_canonical=existing.value_canonical,
                unit_canonical_code=existing.unit_canonical_code,
            ),
        )
    )

    assert changed is False
    assert observation is existing
    session.add.assert_not_called()
    session.flush.assert_not_called()
