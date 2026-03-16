from __future__ import annotations

from types import SimpleNamespace

import calendar_pipeline.cli as cli
from calendar_pipeline.types import AdapterRunStats


def patch_cli_dependencies(monkeypatch, stats: list[AdapterRunStats]) -> None:
    monkeypatch.setattr(cli, "create_calendar_engine", lambda _: object())
    monkeypatch.setattr(cli, "CalendarRepository", lambda _: SimpleNamespace(ensure_schema=lambda: None))
    monkeypatch.setattr(cli, "CurlHttpClient", lambda: object())
    monkeypatch.setattr(
        cli,
        "default_adapters",
        lambda: [
            SimpleNamespace(slug="alpha", pattern="html"),
            SimpleNamespace(slug="beta", pattern="json"),
        ],
    )

    class FakeCalendarIngestionService:
        def __init__(self, repository, client):
            self.repository = repository
            self.client = client

        def run_many(self, adapters, *, as_of=None):
            return stats

    monkeypatch.setattr(cli, "CalendarIngestionService", FakeCalendarIngestionService)


def test_run_command_returns_zero_when_all_adapters_succeed(monkeypatch) -> None:
    patch_cli_dependencies(
        monkeypatch,
        [
            AdapterRunStats(source_slug="alpha", fetched=2, inserted=1, updated=0, flagged=0),
            AdapterRunStats(source_slug="beta", fetched=1, inserted=0, updated=1, flagged=0),
        ],
    )

    assert cli.main(["--database-url", "sqlite:///ignored.db", "run"]) == 0


def test_run_command_returns_one_when_any_adapter_fails(monkeypatch) -> None:
    patch_cli_dependencies(
        monkeypatch,
        [
            AdapterRunStats(source_slug="alpha", fetched=2, inserted=1, updated=0, flagged=0),
            AdapterRunStats(
                source_slug="beta",
                fetched=0,
                inserted=0,
                updated=0,
                flagged=0,
                failed=True,
                error="boom",
            ),
        ],
    )

    assert cli.main(["--database-url", "sqlite:///ignored.db", "run"]) == 1
