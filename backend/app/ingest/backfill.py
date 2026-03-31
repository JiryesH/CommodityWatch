from __future__ import annotations

import argparse
import asyncio
from datetime import date

from app.db.session import get_session_factory
from app.ingest.sources.agsi.jobs import fetch_agsi_daily
from app.ingest.sources.eia.jobs import fetch_eia_wngs, fetch_eia_wpsr
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


def yearly_chunks(start_date: date, end_date: date) -> list[tuple[date, date]]:
    chunks = []
    year = start_date.year
    while year <= end_date.year:
        chunk_start = date(year, 1, 1) if year != start_date.year else start_date
        chunk_end = date(year, 12, 31) if year != end_date.year else end_date
        chunks.append((chunk_start, chunk_end))
        year += 1
    return chunks


async def run_backfill(source: str, from_date: date, to_date: date) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        for chunk_start, chunk_end in yearly_chunks(from_date, to_date):
            if source == "eia_wpsr":
                await fetch_eia_wpsr(session, run_mode="backfill", start_date=chunk_start, end_date=chunk_end)
            elif source == "eia_wngs":
                await fetch_eia_wngs(session, run_mode="backfill", start_date=chunk_start, end_date=chunk_end)
            elif source == "agsi":
                await fetch_agsi_daily(session, run_mode="backfill", start_date=chunk_start, end_date=chunk_end)
            else:
                raise ValueError(f"Unsupported source: {source}")
            await session.commit()

        await compute_seasonal_ranges(session, indicator_scope="inventorywatch")
        await recompute_inventorywatch_snapshot(session)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a source backfill.")
    parser.add_argument("--source", required=True, choices=["eia_wpsr", "eia_wngs", "agsi"])
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat())
    return parser


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(run_backfill(args.source, date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)))


if __name__ == "__main__":
    main()

