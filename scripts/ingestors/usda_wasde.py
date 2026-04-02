#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import get_session_factory
from app.ingest.sources.usda_wasde.jobs import fetch_usda_wasde
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


async def run_cli(release_months: list[str] | None = None) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await fetch_usda_wasde(session, run_mode="manual", release_months=release_months)
        await compute_seasonal_ranges(session, indicator_scope="inventorywatch")
        await recompute_inventorywatch_snapshot(session)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest USDA WASDE releases into the backend store.")
    parser.add_argument("--release-month", action="append", dest="release_months", default=[])
    return parser


def main() -> int:
    args = build_parser().parse_args()
    asyncio.run(run_cli(args.release_months or None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
