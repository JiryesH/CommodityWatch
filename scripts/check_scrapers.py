#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import sys
from datetime import date
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parent.parent
BACKEND_ROOT = APP_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.ingest.sources.comex_warehouse.client import COMEXWarehouseAccessBlockedError, COMEXWarehouseClient
from app.ingest.sources.comex_warehouse.parsers import parse_comex_workbook
from app.ingest.sources.etf_holdings.client import ETFHoldingsClient, GLD_ARCHIVE_URL, IAU_URL, SLV_URL
from app.ingest.sources.etf_holdings.parsers import parse_gld_archive, parse_ishares_current_holdings
from app.ingest.sources.ice_certified.client import ICECertifiedClient, load_contracts
from app.ingest.sources.lme_warehouse.client import LMEWarehouseClient
from app.ingest.sources.lme_warehouse.parsers import parse_lme_workbook


async def check_etf() -> dict[str, str]:
    client = ETFHoldingsClient()
    try:
        gld = parse_gld_archive(await client.get_bytes(GLD_ARCHIVE_URL), source_url=GLD_ARCHIVE_URL)[-1]
        slv = parse_ishares_current_holdings(await client.get_text(SLV_URL), symbol="SLV", source_url=SLV_URL)
        iau = parse_ishares_current_holdings(await client.get_text(IAU_URL), symbol="IAU", source_url=IAU_URL)
        return {"status": "PASS", "detail": f"GLD {gld.observation_date}, SLV {slv.observation_date}, IAU {iau.observation_date}"}
    finally:
        await client.close()


async def check_lme() -> dict[str, str]:
    client = LMEWarehouseClient()
    try:
        target_date = await client.find_latest_available_report(as_of=date.today(), lookback_days=7)
        if target_date is None:
            archive_date = date(2023, 3, 31)
            raw = await client.get_report(archive_date)
            parse_lme_workbook(raw, report_date=archive_date, source_url="")
            return {
                "status": "BLOCKED",
                "detail": "No recent public workbook found; archive sample 2023-03-31 parsed successfully.",
            }
        raw = await client.get_report(target_date)
        parse_lme_workbook(raw, report_date=target_date, source_url="")
        return {"status": "PASS", "detail": f"Latest public workbook {target_date.isoformat()} parsed."}
    finally:
        await client.close()


async def check_comex() -> dict[str, str]:
    client = COMEXWarehouseClient()
    try:
        gold_raw = await client.get_report("GOLD")
        silver_raw = await client.get_report("SILVER")
        parse_comex_workbook(gold_raw, symbol="GOLD", source_url="")
        parse_comex_workbook(silver_raw, symbol="SILVER", source_url="")
        return {"status": "PASS", "detail": "Gold and silver workbooks downloaded and parsed."}
    except COMEXWarehouseAccessBlockedError as exc:
        return {"status": "BLOCKED", "detail": str(exc)}
    finally:
        await client.close()


async def check_ice() -> dict[str, str]:
    client = ICECertifiedClient()
    try:
        blocked = []
        unresolved = []
        for contract in load_contracts():
            if contract.report_id is None:
                unresolved.append(contract.source_series_key)
                continue
            metadata = await client.get_metadata(contract.report_id)
            if metadata.recaptcha_required:
                blocked.append(contract.source_series_key)
        if blocked or unresolved:
            details = []
            if blocked:
                details.append(f"recaptcha-gated: {', '.join(blocked)}")
            if unresolved:
                details.append(f"unresolved: {', '.join(unresolved)}")
            return {"status": "BLOCKED", "detail": "; ".join(details)}
        return {"status": "PASS", "detail": "All ICE report definitions are accessible without recaptcha."}
    finally:
        await client.close()


async def main_async() -> int:
    checks = {}
    for name, fn in {
        "etf_holdings": check_etf,
        "lme_warehouse": check_lme,
        "comex_warehouse": check_comex,
        "ice_certified": check_ice,
    }.items():
        try:
            checks[name] = await fn()
        except Exception as exc:
            checks[name] = {"status": "FAIL", "detail": str(exc)}
    print(json.dumps(checks, indent=2, sort_keys=True))
    return 0 if all(item["status"] == "PASS" for item in checks.values()) else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
