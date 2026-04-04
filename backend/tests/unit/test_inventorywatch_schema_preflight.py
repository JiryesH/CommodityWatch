from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.ingest.common import jobs


def test_missing_required_tables_reports_sorted_difference() -> None:
    missing = jobs.missing_required_tables(
        ["observations", "ingest_runs"],
        required_tables=("quarantined_observations", "observations", "ingest_runs"),
    )

    assert missing == ["quarantined_observations"]


@pytest.mark.asyncio
async def test_assert_required_ingest_schema_raises_helpful_error_for_missing_quarantine_table(monkeypatch) -> None:
    session = SimpleNamespace(info={})

    async def fake_list_database_tables(_session) -> list[str]:
        return ["ingest_runs", "observations", "source_releases"]

    monkeypatch.setattr(jobs, "list_database_tables", fake_list_database_tables)

    with pytest.raises(jobs.IngestSchemaPreflightError, match="quarantined_observations"):
        await jobs.assert_required_ingest_schema(session)


@pytest.mark.asyncio
async def test_assert_required_ingest_schema_caches_success(monkeypatch) -> None:
    session = SimpleNamespace(info={})
    calls = 0

    async def fake_list_database_tables(_session) -> list[str]:
        nonlocal calls
        calls += 1
        return list(jobs.DEFAULT_REQUIRED_INGEST_TABLES)

    monkeypatch.setattr(jobs, "list_database_tables", fake_list_database_tables)

    await jobs.assert_required_ingest_schema(session)
    await jobs.assert_required_ingest_schema(session)

    assert calls == 1


def test_all_migration_revisions_fit_alembic_version_limit() -> None:
    assert jobs.find_oversized_migration_revisions() == []


def test_inventorywatch_migration_graph_has_single_head_and_resolved_parents() -> None:
    assert jobs.find_migration_graph_issues() == []


def test_assert_migration_revision_lengths_reports_violations(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "versions"
    migrations_dir.mkdir()
    (migrations_dir / "0001_ok.py").write_text('revision = "0001_ok"\n', encoding="utf-8")
    (migrations_dir / "0002_too_long.py").write_text(
        'revision = "0002_add_quarantined_observations"\n',
        encoding="utf-8",
    )

    with pytest.raises(jobs.IngestSchemaPreflightError, match="0002_too_long.py"):
        jobs.assert_migration_revision_lengths(migrations_dir=migrations_dir)


def test_assert_migration_graph_reports_missing_parent_and_multiple_heads(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "versions"
    migrations_dir.mkdir()
    (migrations_dir / "0001_root.py").write_text(
        'revision = "0001_root"\n'
        'down_revision = None\n',
        encoding="utf-8",
    )
    (migrations_dir / "0002_bad.py").write_text(
        'revision = "0002_bad"\n'
        'down_revision = "0099_missing"\n',
        encoding="utf-8",
    )

    with pytest.raises(jobs.IngestSchemaPreflightError, match="missing down_revision target '0099_missing'"):
        jobs.assert_migration_graph(migrations_dir=migrations_dir)


def test_assert_migration_graph_reports_duplicate_revisions(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "versions"
    migrations_dir.mkdir()
    (migrations_dir / "0001_root.py").write_text(
        'revision = "0001_root"\n'
        'down_revision = None\n',
        encoding="utf-8",
    )
    (migrations_dir / "0002_duplicate.py").write_text(
        'revision = "0001_root"\n'
        'down_revision = "0001_root"\n',
        encoding="utf-8",
    )

    with pytest.raises(jobs.IngestSchemaPreflightError, match="duplicate revision '0001_root'"):
        jobs.assert_migration_graph(migrations_dir=migrations_dir)
