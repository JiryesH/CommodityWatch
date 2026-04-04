#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import get_session_factory
from app.modules.demandwatch.published_store import (
    PublishedDemandRepository,
    build_demandwatch_coverage_audit,
    demandwatch_coverage_audit_markdown,
)
from app.processing.demandwatch import publish_demandwatch_store


def validate_published_store(output_path: Path, summary: dict, repository: PublishedDemandRepository | None = None) -> None:
    resolved_output = output_path.expanduser().resolve()
    expected_path = Path(summary["database_path"]).expanduser().resolve()
    if expected_path != resolved_output:
        raise RuntimeError(f"Published store path mismatch: expected {resolved_output}, got {expected_path}")
    if not resolved_output.exists():
        raise FileNotFoundError(f"Published store was not created: {resolved_output}")
    if resolved_output.stat().st_size <= 0:
        raise RuntimeError(f"Published store is empty: {resolved_output}")
    if int(summary.get("series_count") or 0) <= 0:
        raise RuntimeError("Published store contains no DemandWatch series.")

    repo = repository or PublishedDemandRepository(resolved_output)
    actual_series_count = len(repo._series_by_id)
    actual_observation_count = sum(len(points) for points in repo._observations_by_series_id.values())
    if actual_series_count != int(summary["series_count"]):
        raise RuntimeError(
            f"Published store series count mismatch: summary={summary['series_count']} actual={actual_series_count}"
        )
    if actual_observation_count != int(summary["observation_count"]):
        raise RuntimeError(
            f"Published store observation count mismatch: summary={summary['observation_count']} actual={actual_observation_count}"
        )


async def _publish(output_path: Path) -> dict:
    session_factory = get_session_factory()
    async with session_factory() as session:
        summary = await publish_demandwatch_store(session, output_path)
        await session.rollback()
        return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish DemandWatch SQLite read model from the backend database.")
    parser.add_argument(
        "--output",
        default=str(APP_ROOT / "data" / "demandwatch.db"),
        help="Published DemandWatch SQLite read-model path",
    )
    parser.add_argument(
        "--audit-json",
        default=None,
        help="Write a machine-readable coverage audit JSON report to this path",
    )
    parser.add_argument(
        "--audit-markdown",
        default=None,
        help="Write a human-readable coverage audit markdown report to this path",
    )
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    summary = asyncio.run(_publish(output_path))
    repository = PublishedDemandRepository(output_path)
    validate_published_store(output_path, summary, repository)
    audit = build_demandwatch_coverage_audit(repository)

    if args.audit_json:
        audit_json_path = Path(args.audit_json).expanduser().resolve()
        audit_json_path.parent.mkdir(parents=True, exist_ok=True)
        audit_json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    if args.audit_markdown:
        audit_markdown_path = Path(args.audit_markdown).expanduser().resolve()
        audit_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        audit_markdown_path.write_text(demandwatch_coverage_audit_markdown(audit), encoding="utf-8")

    print(
        json.dumps(
            {
                "publish": summary,
                "coverage_audit": audit["summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
