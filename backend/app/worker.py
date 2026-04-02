from __future__ import annotations

import argparse
import asyncio
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import get_session_factory
from app.ingest.common.scheduler import write_worker_heartbeat
from app.ingest.registry import JOB_REGISTRY
from app.processing.seasonal import compute_seasonal_ranges
from app.processing.snapshots import recompute_inventorywatch_snapshot


logger = logging.getLogger(__name__)


async def execute_registered_job(job_name: str, run_mode: str = "live") -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            logger.info("Starting worker job %s", job_name)
            result = await JOB_REGISTRY[job_name](session, run_mode=run_mode)
            await write_worker_heartbeat(session)
            await session.commit()
            if result is not None and hasattr(result, "fetched_items"):
                logger.info(
                    "Completed worker job %s (fetched=%s inserted=%s updated=%s)",
                    job_name,
                    getattr(result, "fetched_items", 0),
                    getattr(result, "inserted_rows", 0),
                    getattr(result, "updated_rows", 0),
                )
            else:
                logger.info("Completed worker job %s", job_name)
        except Exception:
            await session.rollback()
            raise


async def execute_seasonal_job() -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            logger.info("Starting worker job seasonal_ranges")
            await compute_seasonal_ranges(session, indicator_scope="inventorywatch")
            await recompute_inventorywatch_snapshot(session)
            await write_worker_heartbeat(session)
            await session.commit()
            logger.info("Completed worker job seasonal_ranges")
        except Exception:
            await session.rollback()
            raise


async def heartbeat_job() -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await write_worker_heartbeat(session)
        await session.commit()


def build_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(day_of_week="wed", hour=10, minute=35, timezone=ZoneInfo("America/New_York")),
        args=["eia_wpsr"],
        id="eia_wpsr",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(day_of_week="thu", hour=10, minute=35, timezone=ZoneInfo("America/New_York")),
        args=["eia_wngs"],
        id="eia_wngs",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(hour=18, minute=0, timezone=ZoneInfo("Europe/Brussels")),
        args=["agsi_daily"],
        id="agsi_daily",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(day="8-14", hour=12, minute=5, timezone=ZoneInfo("America/New_York")),
        args=["usda_wasde"],
        id="usda_wasde",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(hour=18, minute=0, timezone=ZoneInfo("Europe/London")),
        args=["lme_warehouse"],
        id="lme_warehouse",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(hour=17, minute=0, timezone=ZoneInfo("America/New_York")),
        args=["comex_warehouse"],
        id="comex_warehouse",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(hour=20, minute=0, timezone=ZoneInfo("America/New_York")),
        args=["etf_holdings"],
        id="etf_holdings",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_registered_job,
        CronTrigger(hour=17, minute=0, timezone=ZoneInfo("America/New_York")),
        args=["ice_certified"],
        id="ice_certified",
        replace_existing=True,
    )
    scheduler.add_job(
        execute_seasonal_job,
        CronTrigger(day_of_week="sun", hour=2, minute=0, timezone=ZoneInfo("UTC")),
        id="seasonal_ranges",
        replace_existing=True,
    )
    scheduler.add_job(
        heartbeat_job,
        CronTrigger(minute="*/5", timezone=ZoneInfo("UTC")),
        id="worker_heartbeat",
        replace_existing=True,
    )
    return scheduler


async def serve_worker() -> None:
    scheduler = build_scheduler()
    scheduler.start()
    await heartbeat_job()
    await asyncio.Event().wait()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Contango worker")
    subparsers = parser.add_subparsers(dest="command")
    run_once = subparsers.add_parser("run-once")
    run_once.add_argument("--job", required=True, choices=[*JOB_REGISTRY.keys(), "seasonal_ranges"])
    return parser


def main() -> None:
    configure_logging(get_settings().log_level)
    args = build_parser().parse_args()
    if args.command == "run-once":
        if args.job == "seasonal_ranges":
            asyncio.run(execute_seasonal_job())
        else:
            asyncio.run(execute_registered_job(args.job, run_mode="manual"))
        return
    asyncio.run(serve_worker())


if __name__ == "__main__":
    main()
