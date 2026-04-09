from __future__ import annotations

import argparse
import asyncio
from datetime import date

from app.modules.demandwatch.backfill import demandwatch_default_from_date
from app.modules.demandwatch.operations import list_demandwatch_sources, run_demandwatch_backfill


async def run_demandwatch_backfill_single(source: str, from_date: date, to_date: date) -> dict:
    return await run_demandwatch_backfill(
        sources=[source],
        from_date=from_date,
        to_date=to_date,
        continue_on_error=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a DemandWatch source backfill.")
    parser.add_argument("--source", required=True, choices=list_demandwatch_sources())
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat())
    return parser


def main() -> None:
    args = build_parser().parse_args()
    to_date = date.fromisoformat(args.to_date)
    from_date = date.fromisoformat(args.from_date) if args.from_date else demandwatch_default_from_date(to_date)
    result = asyncio.run(run_demandwatch_backfill_single(args.source, from_date, to_date))
    if result["summary"]["failed_sources"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
