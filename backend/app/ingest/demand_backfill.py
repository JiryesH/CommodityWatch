from __future__ import annotations

import argparse
import asyncio
from datetime import date

from app.db.session import get_session_factory
from app.ingest.backfill import monthly_chunks, yearly_chunks
from app.ingest.sources.eia.jobs import fetch_demand_eia_grid_monitor, fetch_demand_eia_wpsr
from app.ingest.sources.ember.jobs import fetch_demand_ember_monthly_electricity
from app.ingest.sources.fred.jobs import fetch_demand_fred_g17, fetch_demand_fred_new_residential_construction
from app.ingest.sources.usda_export_sales.jobs import fetch_demand_usda_export_sales
from app.ingest.sources.usda_psd.jobs import fetch_demand_usda_psd
from app.modules.demandwatch.backfill import demandwatch_default_from_date
from app.processing.demandwatch import recompute_demandwatch_snapshot


async def run_demandwatch_backfill(source: str, from_date: date, to_date: date) -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        if source == "demand_eia_wpsr":
            for chunk_start, chunk_end in yearly_chunks(from_date, to_date):
                await fetch_demand_eia_wpsr(session, run_mode="backfill", start_date=chunk_start, end_date=chunk_end)
                await session.commit()
        elif source == "demand_eia_grid_monitor":
            for month_key in monthly_chunks(from_date, to_date):
                start = date.fromisoformat(f"{month_key}-01")
                if start.month == 12:
                    end = date(start.year, 12, 31)
                else:
                    end = date(start.year, start.month + 1, 1) - date.resolution
                await fetch_demand_eia_grid_monitor(session, run_mode="backfill", start_date=start, end_date=min(end, to_date))
                await session.commit()
        elif source == "demand_fred_g17":
            await fetch_demand_fred_g17(session, run_mode="backfill", start_date=from_date, end_date=to_date)
            await session.commit()
        elif source == "demand_fred_new_residential_construction":
            await fetch_demand_fred_new_residential_construction(
                session,
                run_mode="backfill",
                start_date=from_date,
                end_date=to_date,
            )
            await session.commit()
        elif source == "demand_usda_wasde":
            await fetch_demand_usda_psd(session, run_mode="backfill", start_date=from_date, end_date=to_date)
            await session.commit()
        elif source == "demand_usda_export_sales":
            await fetch_demand_usda_export_sales(session, run_mode="backfill", start_date=from_date, end_date=to_date)
            await session.commit()
        elif source == "demand_ember_monthly_electricity":
            await fetch_demand_ember_monthly_electricity(
                session,
                run_mode="backfill",
                start_date=from_date,
                end_date=to_date,
            )
            await session.commit()
        else:
            raise ValueError(f"Unsupported DemandWatch source: {source}")

        await recompute_demandwatch_snapshot(session)
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a DemandWatch source backfill.")
    parser.add_argument(
        "--source",
        required=True,
        choices=[
            "demand_eia_wpsr",
            "demand_eia_grid_monitor",
            "demand_fred_g17",
            "demand_fred_new_residential_construction",
            "demand_usda_wasde",
            "demand_usda_export_sales",
            "demand_ember_monthly_electricity",
        ],
    )
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat())
    return parser


def main() -> None:
    args = build_parser().parse_args()
    to_date = date.fromisoformat(args.to_date)
    from_date = date.fromisoformat(args.from_date) if args.from_date else demandwatch_default_from_date(to_date)
    asyncio.run(run_demandwatch_backfill(args.source, from_date, to_date))


if __name__ == "__main__":
    main()
