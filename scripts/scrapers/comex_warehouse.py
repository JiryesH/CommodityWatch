#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import get_session_factory
from app.ingest.sources.comex_warehouse.client import COMEXWarehouseClient, COMEXWarehouseAccessBlockedError
from app.ingest.sources.comex_warehouse.jobs import fetch_comex_warehouse
from app.ingest.sources.comex_warehouse.parsers import parse_comex_workbook
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


async def dry_run() -> int:
    client = COMEXWarehouseClient()
    try:
        parsed = []
        for symbol in ("GOLD", "SILVER"):
            raw = await client.get_report(symbol)
            parsed.append(parse_comex_workbook(raw, symbol=symbol, source_url="").to_item())
        print(json.dumps(parsed, indent=2, sort_keys=True))
        return 0
    except COMEXWarehouseAccessBlockedError as exc:
        print(str(exc), file=sys.stderr)
        return 78
    finally:
        await client.close()


async def run_job() -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await fetch_comex_warehouse(session, run_mode="manual")
        await compute_seasonal_ranges(session, indicator_scope="inventorywatch")
        await recompute_inventorywatch_snapshot(session)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch COMEX warehouse stocks.")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.dry_run:
        return asyncio.run(dry_run())
    asyncio.run(run_job())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
