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
from app.ingest.sources.ice_certified.client import (
    ICECertifiedAccessBlockedError,
    ICECertifiedClient,
    load_contracts,
)
from app.ingest.sources.ice_certified.jobs import fetch_ice_certified


async def dry_run() -> int:
    client = ICECertifiedClient()
    try:
        rows = []
        blocked = False
        for contract in load_contracts():
            row = {"source_series_key": contract.source_series_key, "report_id": contract.report_id}
            if contract.report_id is None:
                row["status"] = "unresolved"
                row["note"] = contract.availability_note
                blocked = True
                rows.append(row)
                continue
            metadata = await client.get_metadata(contract.report_id)
            row.update(
                {
                    "status": "ok",
                    "name": metadata.name,
                    "exchange": metadata.exchange,
                    "category": metadata.category_name,
                    "recaptcha_required": metadata.recaptcha_required,
                }
            )
            if metadata.recaptcha_required:
                row["status"] = "blocked"
                blocked = True
            rows.append(row)
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 78 if blocked else 0
    except ICECertifiedAccessBlockedError as exc:
        print(str(exc), file=sys.stderr)
        return 78
    finally:
        await client.close()


async def run_job() -> None:
    session_factory = get_session_factory()
    async with session_factory() as session:
        await fetch_ice_certified(session, run_mode="manual")
        await session.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe ICE certified stock feeds.")
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
