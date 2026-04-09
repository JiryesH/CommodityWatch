from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from app.modules.demandwatch.operations import (
    audit_failures_at_or_above,
    default_demandwatch_audit_json_path,
    default_demandwatch_audit_markdown_path,
    default_demandwatch_publish_path,
    demandwatch_operational_audit_markdown,
    list_demandwatch_sources,
    run_demandwatch_audit,
    run_demandwatch_backfill,
    run_demandwatch_publish,
    run_demandwatch_refresh,
    write_json_artifact,
    write_markdown_artifact,
)
from app.processing.demandwatch import DemandWatchSetupError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DemandWatch operations CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    source_choices = list_demandwatch_sources()

    refresh = subparsers.add_parser("refresh", help="Run DemandWatch source refreshes.")
    refresh.add_argument("--source", dest="sources", action="append", choices=source_choices, default=None)
    refresh.add_argument("--run-mode", choices=["live", "manual"], default="manual")
    refresh.add_argument("--max-attempts", type=int, default=3)
    refresh.add_argument("--continue-on-error", action="store_true")
    refresh.add_argument("--json-output", type=Path, default=None)

    backfill = subparsers.add_parser("backfill", help="Run DemandWatch source backfills.")
    backfill.add_argument("--source", dest="sources", action="append", choices=source_choices, default=None)
    backfill.add_argument("--from", dest="from_date", default=None)
    backfill.add_argument("--to", dest="to_date", default=date.today().isoformat())
    backfill.add_argument("--max-attempts", type=int, default=3)
    backfill.add_argument("--continue-on-error", action="store_true")
    backfill.add_argument("--json-output", type=Path, default=None)

    publish = subparsers.add_parser("publish", help="Publish the DemandWatch SQLite store.")
    publish.add_argument("--output", type=Path, default=default_demandwatch_publish_path())
    publish.add_argument("--json-output", type=Path, default=None)

    audit = subparsers.add_parser("audit", help="Build a DemandWatch operational audit.")
    audit.add_argument("--json-output", type=Path, default=default_demandwatch_audit_json_path())
    audit.add_argument("--markdown-output", type=Path, default=default_demandwatch_audit_markdown_path())
    audit.add_argument("--fail-on", choices=["healthy", "degraded", "failing"], default="failing")

    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


async def _dispatch(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    if args.command == "refresh":
        payload = await run_demandwatch_refresh(
            sources=args.sources,
            run_mode=args.run_mode,
            continue_on_error=bool(args.continue_on_error),
            max_attempts=int(args.max_attempts),
        )
        return (1 if payload["summary"]["failed_sources"] else 0), payload

    if args.command == "backfill":
        to_date = date.fromisoformat(args.to_date)
        from_date = date.fromisoformat(args.from_date) if args.from_date else None
        payload = await run_demandwatch_backfill(
            sources=args.sources,
            from_date=from_date,
            to_date=to_date,
            continue_on_error=bool(args.continue_on_error),
            max_attempts=int(args.max_attempts),
        )
        return (1 if payload["summary"]["failed_sources"] else 0), payload

    if args.command == "publish":
        payload = await run_demandwatch_publish(output_path=args.output)
        return 0, payload

    if args.command == "audit":
        payload = await run_demandwatch_audit()
        failures = audit_failures_at_or_above(payload["audit"], args.fail_on)
        return (1 if failures else 0), payload

    raise ValueError(f"Unsupported DemandWatch command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        exit_code, payload = asyncio.run(_dispatch(args))
    except DemandWatchSetupError as exc:
        print(str(exc))
        return 1

    if getattr(args, "json_output", None):
        write_json_artifact(args.json_output, payload)
    if args.command == "audit" and getattr(args, "markdown_output", None):
        write_markdown_artifact(args.markdown_output, demandwatch_operational_audit_markdown(payload["audit"]))

    if getattr(args, "json_output", None) is None:
        _print_json(payload)

    return exit_code
