from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, TypedDict

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    create_engine,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

from .types import CandidateEvent, stable_hash, utc_now


PUBLISH_STATUS_PUBLISHED = "published"
PUBLISH_STATUS_PENDING_REVIEW = "pending_review"
PUBLISH_STATUS_REJECTED = "rejected"

REVIEW_STATUS_PENDING = "pending"
REVIEW_STATUS_APPROVED = "approved"
REVIEW_STATUS_REJECTED = "rejected"

metadata = MetaData()
json_type = JSON().with_variant(JSONB, "postgresql")


calendar_events = Table(
    "calendar_events",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("source_slug", String(64), nullable=False, index=True),
    Column("source_item_key", String(255), nullable=True),
    Column("natural_key_hash", String(40), nullable=False, unique=True),
    Column("name", String(255), nullable=False),
    Column("organiser", String(255), nullable=False),
    Column("cadence", String(32), nullable=False),
    Column("commodity_sectors", json_type, nullable=False),
    Column("event_date", DateTime(timezone=True), nullable=False, index=True),
    Column("calendar_url", Text, nullable=False),
    Column("redistribution_ok", Boolean, nullable=False, default=False),
    Column("source_label", String(255), nullable=False),
    Column("notes", Text, nullable=True),
    Column("is_confirmed", Boolean, nullable=False, default=False),
    Column("ingestion_pattern", String(32), nullable=False),
    Column("publish_status", String(32), nullable=False, default=PUBLISH_STATUS_PENDING_REVIEW),
    Column("requires_review", Boolean, nullable=False, default=False),
    Column("review_reasons", json_type, nullable=False),
    Column("manual_publish_override", Boolean, nullable=False, default=False),
    Column("raw_payload", json_type, nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("source_slug", "source_item_key", name="uq_calendar_events_source_item"),
)

calendar_event_changes = Table(
    "calendar_event_changes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", String(64), nullable=False, index=True),
    Column("field_name", String(64), nullable=False),
    Column("previous_value", String(255), nullable=True),
    Column("new_value", String(255), nullable=True),
    Column("detected_at", DateTime(timezone=True), nullable=False),
    Column("requires_review", Boolean, nullable=False, default=False),
)

calendar_review_items = Table(
    "calendar_review_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", String(64), nullable=False, index=True),
    Column("change_id", Integer, nullable=True),
    Column("reason", String(64), nullable=False),
    Column("status", String(32), nullable=False, default=REVIEW_STATUS_PENDING),
    Column("resolution_notes", Text, nullable=True),
    Column("manual_publish_override", Boolean, nullable=False, default=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("resolved_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("event_id", "reason", "status", name="uq_calendar_review_event_reason_status"),
)

calendar_ingest_runs = Table(
    "calendar_ingest_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_slug", String(64), nullable=False, index=True),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    Column("status", String(32), nullable=False),
    Column("source_url", Text, nullable=True),
    Column("fetched_records", Integer, nullable=False, default=0),
    Column("inserted_records", Integer, nullable=False, default=0),
    Column("updated_records", Integer, nullable=False, default=0),
    Column("flagged_records", Integer, nullable=False, default=0),
    Column("error_message", Text, nullable=True),
)

calendar_adapter_failures = Table(
    "calendar_adapter_failures",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_slug", String(64), nullable=False, index=True),
    Column("failed_at", DateTime(timezone=True), nullable=False),
    Column("error_message", Text, nullable=False),
    Column("details", json_type, nullable=False),
    Column("run_id", Integer, nullable=True),
    Column("digest_sent_at", DateTime(timezone=True), nullable=True),
)


class CalendarEventRecord(TypedDict):
    id: str
    name: str
    organiser: str
    cadence: str
    commodity_sectors: list[str]
    event_date: str
    calendar_url: str
    redistribution_ok: bool
    source_label: str
    notes: str | None
    is_confirmed: bool
    source_slug: str
    ingestion_pattern: str
    publish_status: str
    review_reasons: list[str]
    updated_at: str | None


def create_calendar_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True)


def _assume_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _assume_utc(value).isoformat()


def _normalize_event_row(row: dict[str, Any]) -> CalendarEventRecord:
    return {
        "id": str(row["id"]),
        "name": str(row["name"]),
        "organiser": str(row["organiser"]),
        "cadence": str(row["cadence"]),
        "commodity_sectors": list(row["commodity_sectors"] or []),
        "event_date": _normalize_datetime(row["event_date"]) or "",
        "calendar_url": str(row["calendar_url"]),
        "redistribution_ok": bool(row["redistribution_ok"]),
        "source_label": str(row["source_label"]),
        "notes": row["notes"],
        "is_confirmed": bool(row["is_confirmed"]),
        "source_slug": str(row["source_slug"]),
        "ingestion_pattern": str(row["ingestion_pattern"]),
        "publish_status": str(row["publish_status"]),
        "review_reasons": list(row["review_reasons"] or []),
        "updated_at": _normalize_datetime(row["updated_at"]),
    }


class CalendarRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def ensure_schema(self) -> None:
        metadata.create_all(self.engine)
        review_view = """
        CREATE VIEW IF NOT EXISTS calendar_review_queue AS
        SELECT
            review.id AS review_item_id,
            review.event_id,
            review.reason,
            review.status,
            review.created_at,
            review.resolution_notes,
            event.name,
            event.organiser,
            event.cadence,
            event.event_date,
            event.calendar_url,
            event.redistribution_ok,
            event.source_label,
            event.notes,
            event.is_confirmed,
            event.source_slug,
            event.ingestion_pattern,
            event.review_reasons
        FROM calendar_review_items AS review
        JOIN calendar_events AS event
          ON event.id = review.event_id
        WHERE review.status = 'pending'
        ORDER BY review.created_at DESC
        """
        with self.engine.begin() as connection:
            connection.execute(text(review_view))

    def create_ingest_run(self, source_slug: str, source_url: str | None) -> int:
        now = utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                insert(calendar_ingest_runs).values(
                    source_slug=source_slug,
                    started_at=now,
                    status="running",
                    source_url=source_url,
                    fetched_records=0,
                    inserted_records=0,
                    updated_records=0,
                    flagged_records=0,
                )
            )
        return int(result.inserted_primary_key[0])

    def finish_ingest_run(
        self,
        run_id: int,
        *,
        status: str,
        fetched_records: int,
        inserted_records: int,
        updated_records: int,
        flagged_records: int,
        error_message: str | None = None,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                update(calendar_ingest_runs)
                .where(calendar_ingest_runs.c.id == run_id)
                .values(
                    finished_at=utc_now(),
                    status=status,
                    fetched_records=fetched_records,
                    inserted_records=inserted_records,
                    updated_records=updated_records,
                    flagged_records=flagged_records,
                    error_message=error_message,
                )
            )

    def log_failure(
        self,
        source_slug: str,
        error_message: str,
        *,
        details: dict[str, Any] | None = None,
        run_id: int | None = None,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                insert(calendar_adapter_failures).values(
                    source_slug=source_slug,
                    failed_at=utc_now(),
                    error_message=error_message,
                    details=details or {},
                    run_id=run_id,
                )
            )

    def list_events(
        self,
        *,
        from_date: date | None,
        to_date: date | None,
        sectors: list[str] | None = None,
    ) -> list[CalendarEventRecord]:
        conditions = [
            calendar_events.c.publish_status == PUBLISH_STATUS_PUBLISHED,
            calendar_events.c.is_confirmed.is_(True),
            (
                (calendar_events.c.redistribution_ok.is_(True))
                | (calendar_events.c.manual_publish_override.is_(True))
            ),
        ]
        if from_date is not None:
            conditions.append(
                calendar_events.c.event_date
                >= datetime(from_date.year, from_date.month, from_date.day, tzinfo=timezone.utc)
            )
        if to_date is not None:
            conditions.append(
                calendar_events.c.event_date
                <= datetime(to_date.year, to_date.month, to_date.day, 23, 59, 59, tzinfo=timezone.utc)
            )

        query = (
            select(calendar_events)
            .where(and_(*conditions))
            .order_by(calendar_events.c.event_date, calendar_events.c.name)
        )

        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings().all()]

        normalized = [_normalize_event_row(row) for row in rows]
        if not sectors:
            return normalized

        selected = set(sectors)
        return [
            row
            for row in normalized
            if any(sector in selected for sector in row["commodity_sectors"])
        ]

    def list_pending_failures(self, *, since_hours: int = 24) -> list[dict[str, Any]]:
        window_start = utc_now() - timedelta(hours=since_hours)
        query = (
            select(calendar_adapter_failures)
            .where(
                and_(
                    calendar_adapter_failures.c.failed_at >= window_start,
                    calendar_adapter_failures.c.digest_sent_at.is_(None),
                )
            )
            .order_by(calendar_adapter_failures.c.failed_at.desc())
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings().all()]

    def mark_failures_digested(self, failure_ids: list[int]) -> None:
        if not failure_ids:
            return
        with self.engine.begin() as connection:
            connection.execute(
                update(calendar_adapter_failures)
                .where(calendar_adapter_failures.c.id.in_(failure_ids))
                .values(digest_sent_at=utc_now())
            )

    def upsert_events(
        self,
        *,
        source_slug: str,
        ingestion_pattern: str,
        candidates: list[CandidateEvent],
        detected_at: datetime | None = None,
    ) -> dict[str, int]:
        timestamp = detected_at or utc_now()
        inserted_count = 0
        updated_count = 0
        flagged_count = 0

        with self.engine.begin() as connection:
            for candidate in candidates:
                review_reasons = self._build_review_reasons(
                    candidate=candidate,
                    ingestion_pattern=ingestion_pattern,
                )
                existing = None
                if candidate.source_item_key:
                    existing = (
                        connection.execute(
                            select(calendar_events).where(
                                and_(
                                    calendar_events.c.source_slug == source_slug,
                                    calendar_events.c.source_item_key == candidate.source_item_key,
                                )
                            )
                        )
                        .mappings()
                        .first()
                    )

                if existing is None:
                    existing = (
                        connection.execute(
                            select(calendar_events).where(
                                calendar_events.c.natural_key_hash == candidate.natural_key_hash()
                            )
                        )
                        .mappings()
                        .first()
                    )

                if existing is None:
                    publish_status = self._resolve_publish_status(
                        review_reasons=review_reasons,
                        manual_publish_override=False,
                    )
                    event_id = self._build_event_id(source_slug, candidate)
                    connection.execute(
                        insert(calendar_events).values(
                            id=event_id,
                            source_slug=source_slug,
                            source_item_key=candidate.source_item_key,
                            natural_key_hash=candidate.natural_key_hash(),
                            name=candidate.name,
                            organiser=candidate.organiser,
                            cadence=candidate.cadence,
                            commodity_sectors=list(candidate.commodity_sectors),
                            event_date=_assume_utc(candidate.event_date),
                            calendar_url=candidate.calendar_url,
                            redistribution_ok=candidate.redistribution_ok,
                            source_label=candidate.source_label,
                            notes=candidate.notes,
                            is_confirmed=candidate.is_confirmed,
                            ingestion_pattern=ingestion_pattern,
                            publish_status=publish_status,
                            requires_review=bool(review_reasons),
                            review_reasons=review_reasons,
                            manual_publish_override=False,
                            raw_payload=candidate.raw_payload,
                            first_seen_at=timestamp,
                            last_seen_at=timestamp,
                            published_at=timestamp if publish_status == PUBLISH_STATUS_PUBLISHED else None,
                            created_at=timestamp,
                            updated_at=timestamp,
                        )
                    )
                    self._ensure_review_items(
                        connection,
                        event_id=event_id,
                        review_reasons=review_reasons,
                        detected_at=timestamp,
                    )
                    inserted_count += 1
                    continue

                event_id = str(existing["id"])
                previous_event_date = _assume_utc(existing["event_date"])
                current_event_date = _assume_utc(candidate.event_date)
                date_changed = previous_event_date != current_event_date
                date_shifted = previous_event_date.date() != current_event_date.date()
                if date_shifted and existing["publish_status"] == PUBLISH_STATUS_PUBLISHED:
                    review_reasons = sorted(set(review_reasons) | {"date_changed"})
                    flagged_count += 1

                publish_status = self._resolve_publish_status(
                    review_reasons=review_reasons,
                    manual_publish_override=bool(existing["manual_publish_override"]),
                )

                if date_changed:
                    change_result = connection.execute(
                        insert(calendar_event_changes).values(
                            event_id=event_id,
                            field_name="event_date",
                            previous_value=previous_event_date.isoformat(),
                            new_value=current_event_date.isoformat(),
                            detected_at=timestamp,
                            requires_review=date_shifted and "date_changed" in review_reasons,
                        )
                    )
                    change_id = int(change_result.inserted_primary_key[0])
                else:
                    change_id = None

                connection.execute(
                    update(calendar_events)
                    .where(calendar_events.c.id == event_id)
                    .values(
                        source_item_key=candidate.source_item_key,
                        natural_key_hash=candidate.natural_key_hash(),
                        name=candidate.name,
                        organiser=candidate.organiser,
                        cadence=candidate.cadence,
                        commodity_sectors=list(candidate.commodity_sectors),
                        event_date=_assume_utc(candidate.event_date),
                        calendar_url=candidate.calendar_url,
                        redistribution_ok=candidate.redistribution_ok,
                        source_label=candidate.source_label,
                        notes=candidate.notes,
                        is_confirmed=candidate.is_confirmed,
                        ingestion_pattern=ingestion_pattern,
                        publish_status=publish_status,
                        requires_review=bool(review_reasons),
                        review_reasons=review_reasons,
                        raw_payload=candidate.raw_payload,
                        last_seen_at=timestamp,
                        published_at=existing["published_at"]
                        or (timestamp if publish_status == PUBLISH_STATUS_PUBLISHED else None),
                        updated_at=timestamp,
                    )
                )
                self._ensure_review_items(
                    connection,
                    event_id=event_id,
                    review_reasons=review_reasons,
                    detected_at=timestamp,
                    change_id=change_id,
                )
                updated_count += 1

        return {
            "fetched": len(candidates),
            "inserted": inserted_count,
            "updated": updated_count,
            "flagged": flagged_count,
        }

    def _ensure_review_items(
        self,
        connection,
        *,
        event_id: str,
        review_reasons: list[str],
        detected_at: datetime,
        change_id: int | None = None,
    ) -> None:
        for reason in review_reasons:
            existing = (
                connection.execute(
                    select(calendar_review_items.c.id).where(
                        and_(
                            calendar_review_items.c.event_id == event_id,
                            calendar_review_items.c.reason == reason,
                            calendar_review_items.c.status == REVIEW_STATUS_PENDING,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if existing is not None:
                continue
            connection.execute(
                insert(calendar_review_items).values(
                    event_id=event_id,
                    change_id=change_id,
                    reason=reason,
                    status=REVIEW_STATUS_PENDING,
                    created_at=detected_at,
                    manual_publish_override=False,
                )
            )

    @staticmethod
    def _resolve_publish_status(*, review_reasons: list[str], manual_publish_override: bool) -> str:
        if manual_publish_override:
            return PUBLISH_STATUS_PUBLISHED
        if review_reasons:
            return PUBLISH_STATUS_PENDING_REVIEW
        return PUBLISH_STATUS_PUBLISHED

    @staticmethod
    def _build_review_reasons(*, candidate: CandidateEvent, ingestion_pattern: str) -> list[str]:
        reasons = set(candidate.review_reasons)
        if not candidate.redistribution_ok:
            reasons.add("redistribution_unconfirmed")
        if ingestion_pattern == "pdf":
            reasons.add("pdf_review")
        if ingestion_pattern == "press_release":
            reasons.add("press_release_review")
        return sorted(reasons)

    @staticmethod
    def _build_event_id(source_slug: str, candidate: CandidateEvent) -> str:
        unique_part = candidate.source_item_key or candidate.natural_key_hash()
        return f"cw_{stable_hash(source_slug, unique_part)[:24]}"
