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
    publish_inventory_store,
)


def validate_published_store(output_path: Path, summary: dict, repository: PublishedInventoryRepository | None = None) -> None:
    resolved_output = output_path.expanduser().resolve()
    expected_path = Path(summary["database_path"]).expanduser().resolve()
    if expected_path != resolved_output:
        raise RuntimeError(f"Published store path mismatch: expected {resolved_output}, got {expected_path}")
    if not resolved_output.exists():
        raise FileNotFoundError(f"Published store was not created: {resolved_output}")
    if resolved_output.stat().st_size <= 0:
        raise RuntimeError(f"Published store is empty: {resolved_output}")
    if int(summary.get("indicator_count") or 0) <= 0:
        raise RuntimeError("Published store contains no indicators.")
    if int(summary.get("observation_count") or 0) <= 0:
        raise RuntimeError("Published store contains no observations.")

    repo = repository or PublishedInventoryRepository(resolved_output)
    actual_indicator_count = len(repo._indicators_by_id)
    actual_observation_count = sum(len(points) for points in repo._observations_by_id.values())
    actual_seasonal_profile_count = len(repo._seasonal_cache)

    if actual_indicator_count != int(summary["indicator_count"]):
        raise RuntimeError(
            f"Published store indicator count mismatch: summary={summary['indicator_count']} actual={actual_indicator_count}"
        )
    if actual_observation_count != int(summary["observation_count"]):
        raise RuntimeError(
            f"Published store observation count mismatch: summary={summary['observation_count']} actual={actual_observation_count}"
        )
    if actual_seasonal_profile_count != int(summary["seasonal_profile_count"]):
        raise RuntimeError(
            f"Published store seasonal profile count mismatch: summary={summary['seasonal_profile_count']} actual={actual_seasonal_profile_count}"
        )


def coverage_blocking_message(audit: dict) -> str | None:
    blocking = inventory_coverage_blocking_issues(audit)
    if not blocking:
        return None
    return "InventoryWatch coverage audit found blocking issues for: " + ", ".join(item["code"] for item in blocking)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish InventoryWatch local read model")
    parser.add_argument(
        "--data-root",
        default=str(APP_ROOT / "backend"),
        help="InventoryWatch backend artifact root",
    )
    parser.add_argument(
        "--output",
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
        help="Fallback release-age threshold in days for cadences without explicit schedule-aware rules",
    )
    parser.add_argument(
        "--dead-after-days",
        type=int,
        default=InventoryCoverageThresholds.dead_after_days,
        help="Fallback period-age threshold in days for cadences without explicit dead-series rules",
    )
    parser.add_argument(
        "--fail-on-weak-coverage",
        action="store_true",
        help="Exit non-zero when the coverage audit finds blocking coverage issues",
    )
    args = parser.parse_args()

    thresholds = InventoryCoverageThresholds(
        min_seasonal_points=args.min_seasonal_points,
        stale_after_days=args.stale_after_days,
        dead_after_days=args.dead_after_days,
    )
    summary = publish_inventory_store(
        Path(args.data_root).expanduser().resolve(),
        Path(args.output).expanduser().resolve(),
    )
    published_repo = PublishedInventoryRepository(Path(args.output).expanduser().resolve())
    validate_published_store(Path(args.output), summary, published_repo)

    audit = build_inventory_coverage_audit(published_repo, thresholds=thresholds)
    if args.audit_json:
        audit_json_path = Path(args.audit_json).expanduser().resolve()
        audit_json_path.parent.mkdir(parents=True, exist_ok=True)
        audit_json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    if args.audit_markdown:
        audit_markdown_path = Path(args.audit_markdown).expanduser().resolve()
        audit_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        audit_markdown_path.write_text(inventory_coverage_audit_markdown(audit), encoding="utf-8")

    report = {
        "publish": summary,
        "coverage_audit": audit["summary"],
    }
    print(json.dumps(report, indent=2, sort_keys=True))

    warning = coverage_blocking_message(audit)
    if warning:
        print(warning, file=sys.stderr)
    if args.fail_on_weak_coverage and warning:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
