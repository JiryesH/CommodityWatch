from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import ModuleSnapshotCache


async def write_worker_heartbeat(session: AsyncSession) -> None:
    now = datetime.now(timezone.utc)
    cached = await session.scalar(
        select(ModuleSnapshotCache).where(
            ModuleSnapshotCache.module_code == "inventorywatch",
            ModuleSnapshotCache.snapshot_key == "__worker_heartbeat__",
        )
    )
    payload = {"service": "worker", "heartbeat_at": now.isoformat()}
    if cached is None:
        await session.merge(
            ModuleSnapshotCache(
                module_code="inventorywatch",
                snapshot_key="__worker_heartbeat__",
                as_of=now,
                payload=payload,
                expires_at=now + timedelta(hours=24),
            )
        )
    else:
        cached.as_of = now
        cached.payload = payload
        cached.expires_at = now + timedelta(hours=24)

