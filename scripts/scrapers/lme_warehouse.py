#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import get_session_factory
from app.ingest.sources.lme_warehouse.client import LMEWarehouseClient
from app.ingest.sources.lme_warehouse.jobs import fetch_lme_warehouse
from app.ingest.sources.lme_warehouse.parsers import parse_lme_workbook
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


async def dry_run(report_date: date | None) -> int:
    client = LMEWarehouseClient()
    try:
        target_date = report_date or await client.find_latest_available_report(as_of=date.today(), lookback_days=7)
        if target_date is None:
            print("No recent public LME workbook is accessible. Current reports may require LME login.", file=sys.stderr)
            return 78
        raw = await client.get_report(target_date)
        parsed = parse_lme_workbook(raw, report_date=target_date, source_url="")
        print(json.dumps([item.to_item() for item in parsed], indent=2, sort_keys=True))
        return 0
    finally:
        await client.close()


async def run_job(report_date: date | None) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await fetch_lme_warehouse(session, run_mode="manual", report_date=report_date)
        await compute_seasonal_ranges(session, indicator_scope="inventorywatch")
        await recompute_inventorywatch_snapshot(session)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch LME warehouse stocks.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", dest="report_date")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report_date = date.fromisoformat(args.report_date) if args.report_date else None
    if args.dry_run:
        return asyncio.run(dry_run(report_date))
    asyncio.run(run_job(report_date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
