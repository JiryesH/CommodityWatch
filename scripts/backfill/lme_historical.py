#!/usr/bin/env python3
"""Backfill public LME warehouse stock history from direct report workbooks.

Source:
  London Metal Exchange public stock breakdown workbooks under
  https://www.lme.com/-/media/files/data/stocks-breakdown/

Terms:
  Public LME workbooks remain subject to the LME site's terms and conditions.
  This script is intended for low-volume internal historical backfill only.

Notes:
  As of 2026-03-31, direct public workbook URLs appear available for the
  historical archive, while the latest current reports may require LME login.
"""

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
    parser = argparse.ArgumentParser(description="Backfill public LME warehouse stock history.")
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat())
    return parser


def main() -> int:
    args = build_parser().parse_args()
    print(
        f"LME warehouse backfill scope: {describe_backfill_scope('lme_warehouse', date.fromisoformat(args.from_date), date.fromisoformat(args.to_date))}",
        file=sys.stderr,
    )
    asyncio.run(run_backfill("lme_warehouse", date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
