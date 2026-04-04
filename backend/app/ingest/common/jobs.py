from __future__ import annotations

import argparse
import asyncio
import ast
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.indicators import Indicator
from app.db.models.sources import IngestRun, QuarantinedObservation, ReleaseDefinition, Source, SourceRelease


logger = logging.getLogger(__name__)
ALEMBIC_VERSION_NUM_MAX_LENGTH = 32
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "db" / "migrations" / "versions"

DEFAULT_REQUIRED_INGEST_TABLES: tuple[str, ...] = (
    "ingest_runs",
    "source_releases",
    "observations",
    "quarantined_observations",
)
QUARANTINE_REQUIRED_TABLES: tuple[str, ...] = ("quarantined_observations",)


@dataclass(slots=True)
class IngestJobResult:
    fetched_items: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    quarantined_rows: int = 0


class IngestSchemaPreflightError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MigrationRevision:
    path: Path
    revision: str | None
    down_revisions: tuple[str, ...]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def missing_required_tables(
    available_tables: Iterable[str],
    *,
    required_tables: Iterable[str] = DEFAULT_REQUIRED_INGEST_TABLES,
) -> list[str]:
    available = {str(table).strip() for table in available_tables}
    required = {str(table).strip() for table in required_tables}
    return sorted(table for table in required if table not in available)


def build_schema_preflight_error(missing_tables: Sequence[str]) -> IngestSchemaPreflightError:
    missing = ", ".join(sorted({str(table).strip() for table in missing_tables}))
    return IngestSchemaPreflightError(
        "Missing required ingest tables: "
        f"{missing}. Run `alembic upgrade head` against the configured backend database "
        "before running InventoryWatch refresh."
    )


def iter_migration_files(migrations_dir: Path = MIGRATIONS_DIR) -> list[Path]:
    return sorted(path for path in migrations_dir.glob("*.py") if path.name != "__init__.py")


def _read_migration_assignment(migration_path: Path, target_name: str):
    tree = ast.parse(migration_path.read_text(encoding="utf-8"), filename=str(migration_path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == target_name:
                return ast.literal_eval(node.value)
    return None


def read_migration_revision(migration_path: Path) -> str | None:
    value = _read_migration_assignment(migration_path, "revision")
    return None if value is None else str(value)


def read_migration_down_revisions(migration_path: Path) -> tuple[str, ...]:
    value = _read_migration_assignment(migration_path, "down_revision")
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(str(item) for item in value if item is not None)
    return (str(value),)


def read_migration_metadata(migration_path: Path) -> MigrationRevision:
    return MigrationRevision(
        path=migration_path,
        revision=read_migration_revision(migration_path),
        down_revisions=read_migration_down_revisions(migration_path),
    )


def find_oversized_migration_revisions(
    *,
    migrations_dir: Path = MIGRATIONS_DIR,
    max_length: int = ALEMBIC_VERSION_NUM_MAX_LENGTH,
) -> list[str]:
    violations: list[str] = []
    for migration_path in iter_migration_files(migrations_dir):
        revision = read_migration_revision(migration_path)
        if revision is None:
            continue
        if len(revision) > max_length:
            violations.append(
                f"{migration_path.name}: revision '{revision}' is {len(revision)} chars (max {max_length})"
            )
    return violations


def assert_migration_revision_lengths(
    *,
    migrations_dir: Path = MIGRATIONS_DIR,
    max_length: int = ALEMBIC_VERSION_NUM_MAX_LENGTH,
) -> None:
    violations = find_oversized_migration_revisions(migrations_dir=migrations_dir, max_length=max_length)
    if not violations:
        return
    details = "; ".join(violations)
    raise IngestSchemaPreflightError(
        "Alembic migration revision identifiers exceed the configured "
        f"`alembic_version.version_num` limit ({max_length}): {details}"
    )


def find_migration_graph_issues(*, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    revisions_by_id: dict[str, MigrationRevision] = {}
    seen_as_parent: set[str] = set()
    issues: list[str] = []

    for migration in (read_migration_metadata(path) for path in iter_migration_files(migrations_dir)):
        if migration.revision is None:
            issues.append(f"{migration.path.name}: missing revision identifier")
            continue
        if migration.revision in revisions_by_id:
            duplicate = revisions_by_id[migration.revision]
            issues.append(
                f"{migration.path.name}: duplicate revision '{migration.revision}' also defined by {duplicate.path.name}"
            )
            continue
        revisions_by_id[migration.revision] = migration
        seen_as_parent.update(migration.down_revisions)

    missing_parents = sorted(parent for parent in seen_as_parent if parent not in revisions_by_id)
    issues.extend(f"missing down_revision target '{parent}'" for parent in missing_parents)

    heads = sorted(revision for revision in revisions_by_id if revision not in seen_as_parent)
    if len(heads) != 1:
        issues.append(f"expected exactly one migration head, found {len(heads)}: {', '.join(heads) or 'none'}")

    return issues


def assert_migration_graph(*, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    issues = find_migration_graph_issues(migrations_dir=migrations_dir)
    if not issues:
        return
    raise IngestSchemaPreflightError("Alembic migration graph is invalid: " + "; ".join(issues))


async def list_database_tables(session: AsyncSession) -> list[str]:
    connection = await session.connection()

    def _table_names(sync_connection) -> list[str]:
        return inspect(sync_connection).get_table_names()

    return list(await connection.run_sync(_table_names))


async def assert_required_ingest_schema(
    session: AsyncSession,
    *,
    required_tables: Sequence[str] = DEFAULT_REQUIRED_INGEST_TABLES,
) -> None:
    normalized = tuple(sorted({str(table).strip() for table in required_tables}))
    cache_key = ("inventorywatch_schema_preflight", normalized)
    if session.info.get(cache_key):
        return

    missing = missing_required_tables(await list_database_tables(session), required_tables=normalized)
    if missing:
        raise build_schema_preflight_error(missing)
    session.info[cache_key] = True


async def assert_quarantine_schema(session: AsyncSession) -> None:
    await assert_required_ingest_schema(session, required_tables=QUARANTINE_REQUIRED_TABLES)


async def get_source_bundle(session: AsyncSession, source_slug: str, release_slug: str) -> tuple[Source, ReleaseDefinition]:
    source = await session.scalar(select(Source).where(Source.slug == source_slug))
    release = await session.scalar(select(ReleaseDefinition).where(ReleaseDefinition.slug == release_slug))
    if source is None or release is None:
        raise ValueError(f"Missing source or release definition for {source_slug}/{release_slug}")
    return source, release


async def get_release_indicators(session: AsyncSession, release_slug: str) -> list[Indicator]:
    result = await session.execute(select(Indicator).where(Indicator.active.is_(True)).order_by(Indicator.code.asc()))
    return [indicator for indicator in result.scalars().all() if indicator.metadata_.get("release_slug") == release_slug]


async def create_ingest_run(
    session: AsyncSession,
    job_name: str,
    source_id,
    release_definition_id,
    run_mode: str,
    metadata: dict | None = None,
) -> IngestRun:
    run = IngestRun(
        job_name=job_name,
        source_id=source_id,
        release_definition_id=release_definition_id,
        run_mode=run_mode,
        status="running",
        metadata_=metadata or {},
    )
    session.add(run)
    await session.flush()
    return run


def _default_scheduled_at(
    release_definition: ReleaseDefinition,
    released_at: datetime,
) -> datetime | None:
    if release_definition.default_local_time is None:
        return None
    local_tz = ZoneInfo(release_definition.schedule_timezone)
    local_date = released_at.astimezone(local_tz).date()
    return datetime.combine(local_date, release_definition.default_local_time or time(0, 0), tzinfo=local_tz).astimezone(
        timezone.utc
    )


async def upsert_source_release(
    session: AsyncSession,
    *,
    source: Source,
    release_definition: ReleaseDefinition,
    release_key: str,
    release_name: str,
    released_at: datetime,
    period_start_at: datetime | None,
    period_end_at: datetime | None,
    artifact_id=None,
    source_url: str | None = None,
    notes: str | None = None,
    metadata: dict | None = None,
    scheduled_at: datetime | None = None,
) -> SourceRelease:
    existing = await session.scalar(
        select(SourceRelease).where(SourceRelease.source_id == source.id, SourceRelease.release_key == release_key)
    )
    if existing:
        existing.release_name = release_name
        existing.scheduled_at = scheduled_at or existing.scheduled_at or _default_scheduled_at(release_definition, released_at)
        existing.released_at = released_at
        existing.period_start_at = period_start_at
        existing.period_end_at = period_end_at
        existing.release_timezone = release_definition.schedule_timezone
        existing.status = "observed"
        existing.primary_artifact_id = artifact_id
        existing.source_url = source_url
        existing.notes = notes
        existing.metadata_ = metadata or existing.metadata_
        return existing

    release = SourceRelease(
        source_id=source.id,
        release_definition_id=release_definition.id,
        release_key=release_key,
        release_name=release_name,
        scheduled_at=scheduled_at or _default_scheduled_at(release_definition, released_at),
        released_at=released_at,
        period_start_at=period_start_at,
        period_end_at=period_end_at,
        release_timezone=release_definition.schedule_timezone,
        status="observed",
        primary_artifact_id=artifact_id,
        source_url=source_url,
        notes=notes,
        metadata_=metadata or {},
    )
    session.add(release)
    await session.flush()
    return release


def indicator_bounds(indicator: Indicator) -> tuple[float | None, float | None]:
    bounds = indicator.metadata_.get("sanity_bounds") or {}
    lower = bounds.get("min")
    upper = bounds.get("max")
    try:
        lower_value = float(lower) if lower is not None else None
    except (TypeError, ValueError):
        lower_value = None
    try:
        upper_value = float(upper) if upper is not None else None
    except (TypeError, ValueError):
        upper_value = None
    return lower_value, upper_value


def value_within_bounds(indicator: Indicator, value: float) -> tuple[bool, float | None, float | None]:
    lower_bound, upper_bound = indicator_bounds(indicator)
    if lower_bound is not None and value < lower_bound:
        return False, lower_bound, upper_bound
    if upper_bound is not None and value > upper_bound:
        return False, lower_bound, upper_bound
    return True, lower_bound, upper_bound


async def quarantine_value(
    session: AsyncSession,
    *,
    run: IngestRun,
    source: Source,
    indicator: Indicator,
    period_end_at: datetime | None,
    value: float,
    unit_native_code: str | None,
    reason: str,
    artifact_uri: str | None = None,
    payload: dict | None = None,
) -> None:
    await assert_quarantine_schema(session)
    lower_bound, upper_bound = indicator_bounds(indicator)
    run.quarantined_rows += 1
    logger.warning(
        "QUARANTINE %s %s value=%s bounds=(%s,%s) period_end_at=%s",
        indicator.code,
        reason,
        value,
        lower_bound,
        upper_bound,
        period_end_at.isoformat() if period_end_at else None,
    )
    session.add(
        QuarantinedObservation(
            ingest_run_id=run.id,
            indicator_id=indicator.id,
            source_id=source.id,
            period_end_at=period_end_at,
            value_native=value,
            unit_native_code=unit_native_code,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            reason=reason,
            artifact_uri=artifact_uri,
            payload=payload or {},
        )
    )
    await session.flush()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="InventoryWatch ingest utilities.")
    subparsers = parser.add_subparsers(dest="command")
    check_schema = subparsers.add_parser("check-schema", help="Verify that required ingest tables are present.")
    check_schema.add_argument(
        "--table",
        dest="tables",
        action="append",
        help="Require a specific table. Repeat to check multiple tables. Defaults to the core ingest tables.",
    )
    subparsers.add_parser(
        "validate-migrations",
        help="Verify Alembic revision identifiers fit the configured alembic_version limit.",
    )
    return parser


async def run_schema_preflight(tables: Sequence[str] | None = None) -> None:
    from app.db.session import get_session_factory

    session_factory = get_session_factory()
    async with session_factory() as session:
        await assert_required_ingest_schema(session, required_tables=tuple(tables or DEFAULT_REQUIRED_INGEST_TABLES))


def run_migration_preflight() -> None:
    assert_migration_revision_lengths()
    assert_migration_graph()


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "check-schema":
        asyncio.run(run_schema_preflight(args.tables))
        return
    if args.command == "validate-migrations":
        run_migration_preflight()
        return
    raise SystemExit("Expected a subcommand. Use `check-schema` or `validate-migrations`.")


if __name__ == "__main__":
    main()
