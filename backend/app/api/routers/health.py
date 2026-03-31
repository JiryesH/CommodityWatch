from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import select

from app.api.deps import SessionDep
from app.core.config import get_settings
from app.db.models.indicators import ModuleSnapshotCache
from app.db.session import check_database


router = APIRouter(prefix="/api/health", tags=["health"])


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@router.get("")
async def health(session: SessionDep) -> dict:
    db_ok = await check_database()
    return {"status": "ok" if db_ok else "degraded", "database": db_ok}


@router.get("/live")
async def live() -> dict:
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> dict:
    db_ok = await check_database()
    return {"status": "ok" if db_ok else "degraded", "database": db_ok}


@router.get("/dependencies")
async def dependencies(session: SessionDep) -> dict:
    db_ok = await check_database()
    heartbeat = await session.scalar(
        select(ModuleSnapshotCache).where(
            ModuleSnapshotCache.module_code == "inventorywatch",
            ModuleSnapshotCache.snapshot_key == "__worker_heartbeat__",
        )
    )
    settings = get_settings()
    worker_ok = False
    heartbeat_at = None
    if heartbeat:
        heartbeat_at = heartbeat.as_of
        worker_ok = (utcnow() - heartbeat.as_of).total_seconds() <= settings.worker_heartbeat_grace_seconds
    return {
        "status": "ok" if db_ok and worker_ok else "degraded",
        "database": db_ok,
        "worker": {"ok": worker_ok, "heartbeat_at": heartbeat_at},
    }
