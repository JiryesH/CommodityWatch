#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from inventory_watch_published_db import (
    InventoryCoverageThresholds,
    PublishedInventoryRepository,
    build_inventory_coverage_audit,
    inventory_coverage_audit_markdown,
    inventory_coverage_blocking_issues,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit a published InventoryWatch SQLite read model.")
    parser.add_argument(
        "--database",
        default=str(APP_ROOT / "data" / "inventorywatch.db"),
        help="Published InventoryWatch SQLite read-model path",
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
    parser.add_argument(
        "--min-seasonal-points",
        type=int,
        default=InventoryCoverageThresholds.min_seasonal_points,
        help="Minimum seasonal point count before a seasonal series is considered credible",
    )
    parser.add_argument(
        "--stale-after-days",
        type=int,
        default=InventoryCoverageThresholds.stale_after_days,
        help="Release age in days before coverage is flagged as stale",
    )
    parser.add_argument(
        "--dead-after-days",
        type=int,
        default=InventoryCoverageThresholds.dead_after_days,
        help="Release age in days before coverage is flagged as dead",
    )
    parser.add_argument(
        "--fail-on-weak-coverage",
        action="store_true",
        help="Exit non-zero when the coverage audit finds blocking coverage issues",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    thresholds = InventoryCoverageThresholds(
        min_seasonal_points=args.min_seasonal_points,
        stale_after_days=args.stale_after_days,
        dead_after_days=args.dead_after_days,
    )
    repository = PublishedInventoryRepository(Path(args.database).expanduser().resolve())
    audit = build_inventory_coverage_audit(repository, thresholds=thresholds)

    if args.audit_json:
        audit_json_path = Path(args.audit_json).expanduser().resolve()
        audit_json_path.parent.mkdir(parents=True, exist_ok=True)
        audit_json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    if args.audit_markdown:
        audit_markdown_path = Path(args.audit_markdown).expanduser().resolve()
        audit_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        audit_markdown_path.write_text(inventory_coverage_audit_markdown(audit), encoding="utf-8")

    print(json.dumps(audit["summary"], indent=2, sort_keys=True))
    if args.fail_on_weak_coverage:
        blocking = inventory_coverage_blocking_issues(audit)
        if blocking:
            print(
                "InventoryWatch coverage audit found blocking issues for: "
                + ", ".join(item["code"] for item in blocking),
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
