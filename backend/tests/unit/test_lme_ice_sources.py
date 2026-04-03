from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import textwrap

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))


def load_module(module_name: str, relative_path: str):
    module_path = BACKEND_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def stub_package(package_name: str, relative_path: str) -> None:
    package = ModuleType(package_name)
    package.__path__ = [str(BACKEND_ROOT / relative_path)]
    sys.modules[package_name] = package


stub_package("app.ingest.sources.ice_certified", "app/ingest/sources/ice_certified")
stub_package("app.ingest.sources.lme_warehouse", "app/ingest/sources/lme_warehouse")

ice_client = load_module("test_ice_certified_client", "app/ingest/sources/ice_certified/client.py")
ice_jobs = load_module("test_ice_certified_jobs", "app/ingest/sources/ice_certified/jobs.py")
lme_client = load_module("test_lme_warehouse_client", "app/ingest/sources/lme_warehouse/client.py")
lme_parsers = load_module("test_lme_warehouse_parsers", "app/ingest/sources/lme_warehouse/parsers.py")

load_contracts = ice_client.load_contracts
_metadata_matches = ice_jobs._metadata_matches
LMEWarehouseClient = lme_client.LMEWarehouseClient
EXPECTED_HEADERS = lme_parsers.EXPECTED_HEADERS
EXPECTED_METALS = lme_parsers.EXPECTED_METALS
parse_lme_workbook = lme_parsers.parse_lme_workbook


class _FakeSheet:
    def __init__(self, rows: list[list[object]]) -> None:
        self.name = "Metals Totals Report"
        self._rows = rows
        self.nrows = len(rows)

    def row_values(self, row_index: int) -> list[object]:
        return self._rows[row_index]


class _FakeBook:
    def __init__(self, sheet: _FakeSheet) -> None:
        self._sheet = sheet

    def sheet_names(self) -> list[str]:
        return [self._sheet.name]

    def sheet_by_name(self, sheet_name: str) -> _FakeSheet:
        assert sheet_name == self._sheet.name
        return self._sheet


def test_parse_lme_workbook_normalizes_labels_and_humanizes_metals(monkeypatch) -> None:
    rows: list[list[object]] = [
        [expected[0] for expected in EXPECTED_HEADERS],
    ]
    for index, (source_series_key, metal_label) in enumerate(EXPECTED_METALS.items(), start=1):
        label = metal_label if source_series_key != "LME_ALUMINIUM_WAREHOUSE_STOCKS" else "  Primary   Aluminium  "
        rows.append(["", label, "", "", "", "", "", ""])
        rows.append(["Total", "", "", "", "", 1000.0 + index, 200.0 + index, 50.0 + index])

    monkeypatch.setattr(lme_parsers.xlrd, "open_workbook", lambda file_contents: _FakeBook(_FakeSheet(rows)))

    parsed = parse_lme_workbook(b"ignored", report_date=date(2026, 3, 31), source_url="https://example.com/report.xls")

    assert [item.source_series_key for item in parsed] == list(EXPECTED_METALS)
    assert parsed[0].metal == "Copper"
    assert parsed[1].metal == "Primary Aluminium"
    assert parsed[1].metadata["raw_label"] == "primary aluminium"


@pytest.mark.asyncio
async def test_lme_has_report_falls_back_to_get_when_head_is_ambiguous(monkeypatch) -> None:
    client = LMEWarehouseClient.__new__(LMEWarehouseClient)
    calls: list[str] = []

    async def fake_request(report_date, *, method: str):
        calls.append(method)
        if method == "HEAD":
            return SimpleNamespace(status_code=200, headers={"content-type": "text/html"}, content=b"")
        return SimpleNamespace(status_code=200, headers={"content-type": "application/vnd.ms-excel"}, content=b"excel")

    monkeypatch.setattr(client, "_request_report", fake_request)

    assert await client.has_report(date(2026, 3, 31)) is True
    assert calls == ["HEAD", "GET"]


@pytest.mark.asyncio
async def test_fetch_ice_certified_marks_unresolved_contracts_partial(monkeypatch) -> None:
    run = SimpleNamespace(id=1, metadata_={}, status=None, error_text=None, fetched_items=None, finished_at=None)
    source = SimpleNamespace(id=11)
    release_definition = SimpleNamespace(id=22)

    class _FakeClient:
        async def get_metadata(self, report_id: int):
            assert report_id == 41
            return SimpleNamespace(
                name="Certified Stock Reports - Cocoa",
                exchange="ICE Futures U.S.",
                category_name="Certified Stock",
            )

        async def get_criteria(self, report_id: int):
            assert report_id == 41
            return {"status": 200}

        async def close(self):
            return None

    fake_contracts = [
        SimpleNamespace(
            source_series_key="ICE_COCOA",
            report_id=41,
            expected_report_name="Certified Stock Reports - Cocoa",
            expected_exchange="ICE Futures U.S.",
            expected_category="Certified Stock",
            availability_note=None,
        ),
        SimpleNamespace(
            source_series_key="ICE_NO11",
            report_id=None,
            expected_report_name=None,
            expected_exchange="ICE Futures U.S.",
            expected_category="Certified Stock",
            availability_note="Exact public report id unresolved as of 2026-03-31.",
        ),
    ]

    async def fake_get_source_bundle(session, source_slug: str, release_slug: str):
        assert source_slug == "ice"
        assert release_slug == "ice_certified"
        return source, release_definition

    async def fake_create_ingest_run(*args, **kwargs):
        return run

    monkeypatch.setattr(ice_jobs, "get_source_bundle", fake_get_source_bundle)
    monkeypatch.setattr(ice_jobs, "create_ingest_run", fake_create_ingest_run)
    monkeypatch.setattr(ice_jobs, "load_contracts", lambda: fake_contracts)
    monkeypatch.setattr(ice_jobs, "utcnow", lambda: "2026-03-31T00:00:00Z")
    monkeypatch.setattr(ice_jobs, "ICECertifiedClient", lambda: _FakeClient())

    counters = await ice_jobs.fetch_ice_certified(SimpleNamespace(), run_mode="live")

    assert counters.fetched_items == 1
    assert run.fetched_items == 1
    assert run.status == "partial"
    assert "unresolved-report-ids" in run.error_text
    assert run.metadata_["validated_contracts"] == ["ICE_COCOA"]
    assert run.metadata_["unresolved_contracts"] == [
        "ICE_NO11 (Exact public report id unresolved as of 2026-03-31.)"
    ]


def test_load_contracts_strips_whitespace_and_blanks(tmp_path: Path, monkeypatch) -> None:
    contracts_path = tmp_path / "contracts.yml"
    contracts_path.write_text(
        textwrap.dedent(
            """
            - source_series_key: " ICE_COCOA "
              name: " ICE Cocoa Certified Stocks "
              report_id: "41"
              report_url: " "
              expected_report_name: " Certified Stock Reports - Cocoa "
              expected_exchange: " ICE Futures U.S. "
              expected_category: " Certified Stock "
              unit_native_code: " tonnes "
              availability_note: "  Extra spacing  "
            """
        ).strip()
    )
    monkeypatch.setattr(ice_client, "CONTRACTS_PATH", contracts_path)

    [contract] = load_contracts()

    assert contract.source_series_key == "ICE_COCOA"
    assert contract.name == "ICE Cocoa Certified Stocks"
    assert contract.report_id == 41
    assert contract.report_url is None
    assert contract.expected_report_name == "Certified Stock Reports - Cocoa"
    assert contract.expected_exchange == "ICE Futures U.S."
    assert contract.expected_category == "Certified Stock"
    assert contract.unit_native_code == "tonnes"
    assert contract.availability_note == "Extra spacing"


def test_metadata_matching_normalizes_spacing_and_case() -> None:
    assert _metadata_matches("Certified Stock Reports - Cocoa", " certified  stock reports - cocoa ")
    assert _metadata_matches("ICE Futures U.S.", " ICE Futures U.S. ")
    assert _metadata_matches(None, "anything at all")
