from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

from .adapters import default_adapters
from .digest import FailureDigestService
from .http import CurlHttpClient
from .service import CalendarIngestionService
from .storage import CalendarRepository, create_calendar_engine


DEFAULT_CALENDAR_DATABASE_URL = "sqlite:///data/calendarwatch.db"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CalendarWatch data pipeline")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("CALENDAR_DATABASE_URL", DEFAULT_CALENDAR_DATABASE_URL),
        help="Database URL for CalendarWatch storage",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create or migrate the CalendarWatch schema")

    list_parser = subparsers.add_parser("list-sources", help="List available adapters")
    list_parser.set_defaults(command="list-sources")

    run_parser = subparsers.add_parser("run", help="Run one or more adapters")
    run_parser.add_argument("--source", action="append", help="Adapter slug to run. Repeat to run multiple.")
    run_parser.add_argument("--as-of", help="ISO 8601 datetime to use as the adapter clock")

    digest_parser = subparsers.add_parser("send-failure-digest", help="Post recent adapter failures")
    digest_parser.add_argument("--endpoint-url", default=os.environ.get("CALENDAR_FAILURE_DIGEST_URL"))
    digest_parser.add_argument("--hours", type=int, default=24)

    return parser


def parse_as_of(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("--as-of must include a timezone offset")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    repository = CalendarRepository(create_calendar_engine(args.database_url))
    repository.ensure_schema()

    if args.command == "init-db":
        return 0

    adapters = default_adapters()
    if args.command == "list-sources":
        print(json.dumps([{"slug": adapter.slug, "pattern": adapter.pattern} for adapter in adapters], indent=2))
        return 0

    client = CurlHttpClient()

    if args.command == "run":
        selected = set(args.source or [])
        chosen_adapters = [adapter for adapter in adapters if not selected or adapter.slug in selected]
        if not chosen_adapters:
            raise SystemExit("No adapters selected")
        service = CalendarIngestionService(repository, client)
        stats = service.run_many(chosen_adapters, as_of=parse_as_of(args.as_of))
        print(json.dumps([stat.__dict__ for stat in stats], indent=2))
        return 1 if any(stat.failed for stat in stats) else 0

    if args.command == "send-failure-digest":
        if not args.endpoint_url:
            raise SystemExit("--endpoint-url or CALENDAR_FAILURE_DIGEST_URL is required")
        digest = FailureDigestService(repository, client)
        print(json.dumps(digest.send(args.endpoint_url, since_hours=args.hours), indent=2))
        return 0

    raise SystemExit(f"Unknown command: {args.command}")
