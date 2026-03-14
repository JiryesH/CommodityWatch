from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def stable_hash(*parts: str) -> str:
    digest = sha1()
    for part in parts:
        digest.update(part.encode("utf-8"))
        digest.update(b"\x1f")
    return digest.hexdigest()


@dataclass(frozen=True)
class CandidateEvent:
    name: str
    organiser: str
    cadence: str
    commodity_sectors: tuple[str, ...]
    event_date: datetime
    calendar_url: str
    redistribution_ok: bool
    source_label: str
    notes: str | None
    is_confirmed: bool
    source_item_key: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    review_reasons: tuple[str, ...] = ()

    def natural_key_hash(self) -> str:
        return stable_hash(
            self.organiser.strip().lower(),
            self.event_date.astimezone(timezone.utc).isoformat(),
            self.name.strip().lower(),
        )


@dataclass(frozen=True)
class AdapterRunStats:
    source_slug: str
    fetched: int
    inserted: int
    updated: int
    flagged: int
    failed: bool = False
    error: str | None = None
