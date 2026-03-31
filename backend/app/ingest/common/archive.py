from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.sources import IngestArtifact, Source


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def archive_payload(
    session: AsyncSession,
    source: Source,
    job_name: str,
    payload: dict,
    content_type: str = "application/json",
) -> IngestArtifact:
    settings = get_settings()
    now = utcnow()
    source_dir = settings.artifact_root / source.slug / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
    source_dir.mkdir(parents=True, exist_ok=True)
    file_path = source_dir / f"{job_name}-{now.strftime('%H%M%S%f')}.json"
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    file_path.write_bytes(raw)

    artifact = IngestArtifact(
        source_id=source.id,
        storage_uri=str(Path(file_path).resolve()),
        content_type=content_type,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        metadata_={"job_name": job_name},
    )
    session.add(artifact)
    await session.flush()
    return artifact
