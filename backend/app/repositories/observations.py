from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.db.models.observations import Observation


@dataclass(slots=True)
class ObservationInput:
    indicator_id: uuid.UUID
    period_start_at: datetime
    period_end_at: datetime
    release_id: uuid.UUID | None
    release_date: datetime | None
    vintage_at: datetime
    observation_kind: str
    value_native: float
    unit_native_code: str
    value_canonical: float
    unit_canonical_code: str
    currency_code: str | None = None
    source_item_ref: str | None = None
    provenance_note: str | None = None
    metadata: dict = field(default_factory=dict)


def coerce_utc_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


async def fetch_observations(
    session: AsyncSession,
    indicator_id: uuid.UUID,
    start_date: date | datetime,
    end_date: date | datetime,
) -> list[Observation]:
    stmt = (
        select(Observation)
        .where(
            Observation.indicator_id == indicator_id,
            Observation.period_end_at >= coerce_utc_datetime(start_date),
            Observation.period_end_at <= coerce_utc_datetime(end_date),
        )
        .order_by(Observation.period_end_at.asc(), Observation.vintage_at.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


def collapse_vintages(
    observations: list[Observation],
    vintage: str = "latest",
    as_of: datetime | None = None,
) -> list[Observation]:
    grouped: dict[tuple[datetime, datetime, str], list[Observation]] = {}
    for observation in observations:
        key = (observation.period_start_at, observation.period_end_at, observation.observation_kind.value)
        grouped.setdefault(key, []).append(observation)

    collapsed: list[Observation] = []
    for key in sorted(grouped.keys(), key=lambda item: item[1]):
        candidates = grouped[key]
        if vintage == "first":
            collapsed.append(min(candidates, key=lambda item: (item.revision_sequence, item.vintage_at)))
            continue
        if vintage == "as_of" and as_of:
            eligible = [item for item in candidates if item.vintage_at <= as_of]
            if eligible:
                collapsed.append(max(eligible, key=lambda item: (item.vintage_at, item.revision_sequence)))
            continue
        latest_candidates = [item for item in candidates if item.is_latest]
        if latest_candidates:
            collapsed.append(latest_candidates[0])
        else:
            collapsed.append(max(candidates, key=lambda item: (item.vintage_at, item.revision_sequence)))
    return collapsed


def downsample_observations(observations: list[Observation], mode: str) -> list[Observation]:
    if mode in {"raw", "auto"} or not observations:
        return observations

    buckets: dict[tuple[int, int], Observation] = {}
    if mode == "weekly":
        for observation in observations:
            iso_year, iso_week, _ = observation.period_end_at.isocalendar()
            buckets[(iso_year, iso_week)] = observation
    elif mode == "monthly":
        for observation in observations:
            buckets[(observation.period_end_at.year, observation.period_end_at.month)] = observation
    elif mode == "quarterly":
        for observation in observations:
            quarter = ((observation.period_end_at.month - 1) // 3) + 1
            buckets[(observation.period_end_at.year, quarter)] = observation
    elif mode == "daily":
        return observations
    else:
        return observations

    return [buckets[key] for key in sorted(buckets.keys())]


async def get_latest_observation(session: AsyncSession, indicator_id: uuid.UUID) -> Observation | None:
    result = await session.execute(
        select(Observation)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_latest_and_prior(session: AsyncSession, indicator_id: uuid.UUID) -> tuple[Observation | None, Observation | None]:
    result = await session.execute(
        select(Observation)
        .where(Observation.indicator_id == indicator_id, Observation.is_latest.is_(True))
        .order_by(Observation.period_end_at.desc())
        .limit(2)
    )
    observations = list(result.scalars().all())
    latest = observations[0] if observations else None
    prior = observations[1] if len(observations) > 1 else None
    return latest, prior


async def upsert_observation_revision(session: AsyncSession, payload: ObservationInput) -> tuple[Observation, bool]:
    exact_vintage_result = await session.execute(
        select(Observation)
        .where(
            Observation.indicator_id == payload.indicator_id,
            Observation.period_start_at == payload.period_start_at,
            Observation.period_end_at == payload.period_end_at,
            Observation.observation_kind == payload.observation_kind,
            Observation.vintage_at == payload.vintage_at,
        )
        .limit(1)
    )
    exact_vintage = exact_vintage_result.scalar_one_or_none()

    if exact_vintage is not None:
        return exact_vintage, False

    existing_result = await session.execute(
        select(Observation)
        .where(
            Observation.indicator_id == payload.indicator_id,
            Observation.period_start_at == payload.period_start_at,
            Observation.period_end_at == payload.period_end_at,
            Observation.observation_kind == payload.observation_kind,
            Observation.is_latest.is_(True),
        )
        .limit(1)
    )
    existing = existing_result.scalar_one_or_none()

    if existing and existing.value_native == payload.value_native and existing.value_canonical == payload.value_canonical:
        return existing, False

    revision_sequence = 1
    supersedes_id = None
    if existing:
        existing.is_latest = False
        revision_sequence = existing.revision_sequence + 1
        supersedes_id = existing.id

    observation = Observation(
        indicator_id=payload.indicator_id,
        period_start_at=payload.period_start_at,
        period_end_at=payload.period_end_at,
        release_id=payload.release_id,
        release_date=payload.release_date,
        vintage_at=payload.vintage_at,
        observation_kind=payload.observation_kind,
        value_native=payload.value_native,
        unit_native_code=payload.unit_native_code,
        value_canonical=payload.value_canonical,
        unit_canonical_code=payload.unit_canonical_code,
        currency_code=payload.currency_code,
        is_latest=True,
        revision_sequence=revision_sequence,
        supersedes_observation_id=supersedes_id,
        source_item_ref=payload.source_item_ref,
        provenance_note=payload.provenance_note,
        metadata_=payload.metadata,
    )
    session.add(observation)
    await session.flush()
    return observation, True


async def get_indicator_lookup(session: AsyncSession, indicator_id: uuid.UUID) -> Indicator | None:
    result = await session.execute(select(Indicator).where(Indicator.id == indicator_id))
    return result.scalar_one_or_none()


def derive_period_start(period_end_at: datetime, frequency: str) -> datetime:
    if frequency == "weekly":
        return period_end_at - timedelta(days=6)
    if frequency == "monthly":
        return period_end_at.replace(day=1)
    if frequency == "daily":
        return period_end_at
    return period_end_at
