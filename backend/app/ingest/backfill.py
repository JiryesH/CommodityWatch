from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta

from app.db.session import get_session_factory
from app.ingest.sources.agsi.jobs import fetch_agsi_daily
from app.ingest.sources.lme_warehouse.jobs import fetch_lme_warehouse
from app.ingest.sources.eia.jobs import fetch_eia_wngs, fetch_eia_wpsr
from app.ingest.sources.usda_wasde.jobs import fetch_usda_wasde
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


BACKFILL_COVERAGE_NOTES: dict[str, str] = {
    "usda_wasde": (
        "USDA WASDE backfills use the public archive and are seeded from 2000-01-01 by default; "
        "actual depth depends on what the USDA archive currently exposes."
    ),
    "lme_warehouse": (
        "LME warehouse backfills iterate public business-day workbook URLs; current reports may still be login-gated."
    ),
}


def yearly_chunks(start_date: date, end_date: date) -> list[tuple[date, date]]:
    chunks = []
    year = start_date.year
    while year <= end_date.year:
        chunk_start = date(year, 1, 1) if year != start_date.year else start_date
        chunk_end = date(year, 12, 31) if year != end_date.year else end_date
        chunks.append((chunk_start, chunk_end))
        year += 1
    return chunks


def monthly_chunks(start_date: date, end_date: date) -> list[str]:
    months: list[str] = []
    cursor = date(start_date.year, start_date.month, 1)
    limit = date(end_date.year, end_date.month, 1)
    while cursor <= limit:
        months.append(cursor.strftime("%Y-%m"))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def business_days(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def describe_backfill_scope(source: str, from_date: date, to_date: date) -> str:
    note = BACKFILL_COVERAGE_NOTES.get(source)
    window = f"{from_date.isoformat()} -> {to_date.isoformat()}"
    if note is None:
        return window
    return f"{window} | {note}"


async def run_backfill(source: str, from_date: date, to_date: date) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        if source == "usda_wasde":
            months = monthly_chunks(from_date, to_date)
            await fetch_usda_wasde(session, run_mode="backfill", release_months=months)
            await session.commit()
        elif source == "lme_warehouse":
            for report_date in business_days(from_date, to_date):
                await fetch_lme_warehouse(session, run_mode="backfill", report_date=report_date)
                await session.commit()
        else:
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
    parser.add_argument(
        "--source",
        required=True,
        choices=["eia_wpsr", "eia_wngs", "agsi", "usda_wasde", "lme_warehouse"],
    )
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat())
    return parser


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(run_backfill(args.source, date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)))


if __name__ == "__main__":
    main()
