from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.db.models.observations import AppEvent, Observation
from app.processing.snapshots import recompute_inventorywatch_snapshot


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def emit_observation_event(
    session: AsyncSession,
    indicator: Indicator,
    observation: Observation,
    event_type: str = "inventory.observation_upserted",
) -> None:
    payload = {
        "indicator_id": str(indicator.id),
        "indicator_code": indicator.code,
        "commodity_code": indicator.commodity_code,
        "geography_code": indicator.geography_code,
        "period_end_at": observation.period_end_at.isoformat(),
        "value": float(observation.value_canonical),
        "revision_sequence": observation.revision_sequence,
    }
    idempotency_key = f"{event_type}:{indicator.id}:{observation.period_end_at.date()}:{observation.revision_sequence}"
    session.add(
        AppEvent(
            idempotency_key=idempotency_key,
            event_type=event_type,
            producer_module_code="inventorywatch",
            aggregate_type="observation",
            aggregate_id=observation.id,
            commodity_code=indicator.commodity_code,
            geography_code=indicator.geography_code,
            indicator_id=indicator.id,
            observation_id=observation.id,
            payload=payload,
        )
    )


async def process_pending_events(session: AsyncSession, limit: int = 100) -> int:
    result = await session.execute(
        select(AppEvent)
        .where(AppEvent.status.in_(["pending", "failed"]), AppEvent.available_at <= utcnow())
        .order_by(AppEvent.available_at.asc())
        .limit(limit)
    )
    events = list(result.scalars().all())
    if not events:
        return 0

    touched_inventorywatch = False
    for event in events:
        event.status = "processing"
        try:
            if event.event_type.startswith("inventory."):
                touched_inventorywatch = True
            event.status = "processed"
            event.processed_at = utcnow()
        except Exception as exc:  # pragma: no cover
            event.status = "failed"
            event.error_text = str(exc)

    if touched_inventorywatch:
        await recompute_inventorywatch_snapshot(session)
    return len(events)

