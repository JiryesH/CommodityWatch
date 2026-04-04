#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.modules.demandwatch.published_store import (
    PublishedDemandRepository,
    build_demandwatch_coverage_audit,
    demandwatch_coverage_audit_markdown,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit a published DemandWatch SQLite read model.")
    parser.add_argument(
        "--database",
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    repository = PublishedDemandRepository(Path(args.database).expanduser().resolve())
    audit = build_demandwatch_coverage_audit(repository)

    if args.audit_json:
        audit_json_path = Path(args.audit_json).expanduser().resolve()
        audit_json_path.parent.mkdir(parents=True, exist_ok=True)
        audit_json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    if args.audit_markdown:
        audit_markdown_path = Path(args.audit_markdown).expanduser().resolve()
        audit_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        audit_markdown_path.write_text(demandwatch_coverage_audit_markdown(audit), encoding="utf-8")

    print(json.dumps(audit["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
