#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.ingest.backfill import run_backfill
from app.ingest.backfill import describe_backfill_scope


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill historical USDA WASDE releases from the public archive.")
    parser.add_argument(
        "--from",
        dest="from_date",
        default="2000-01-01",
        help="Start date for archive backfill. Defaults to 2000-01-01 to cover a deeper WASDE history window.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=date.today().isoformat(),
        help="End date for archive backfill. Defaults to today.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    print(
        f"USDA WASDE backfill scope: {describe_backfill_scope('usda_wasde', date.fromisoformat(args.from_date), date.fromisoformat(args.to_date))}",
        file=sys.stderr,
    )
    asyncio.run(
        run_backfill(
            "usda_wasde",
            date.fromisoformat(args.from_date),
            date.fromisoformat(args.to_date),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
