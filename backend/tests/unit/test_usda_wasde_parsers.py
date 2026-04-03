from __future__ import annotations

import importlib.util
import sys
from datetime import date
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType


PARSER_PATH = Path(__file__).resolve().parents[2] / "app" / "ingest" / "sources" / "usda_wasde" / "parsers.py"
SPEC = importlib.util.spec_from_file_location("test_usda_wasde_parsers_module", PARSER_PATH)
assert SPEC is not None and SPEC.loader is not None
PARSERS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PARSERS
SPEC.loader.exec_module(PARSERS)

_extract_market_year_label = PARSERS._extract_market_year_label
_market_year_start = PARSERS._market_year_start
_normalize_entity_label = PARSERS._normalize_entity_label
parse_available_release_months = PARSERS.parse_available_release_months
parse_release_listing = PARSERS.parse_release_listing


@dataclass(slots=True)
class _FakeWASDEReleaseRef:
    released_on: date
    workbook_url: str
    source_url: str
    month_key: str
    pdf_url: str | None = None
    title: str | None = None


def _install_fake_client_module() -> None:
    package_names = [
        "app",
        "app.ingest",
        "app.ingest.sources",
        "app.ingest.sources.usda_wasde",
    ]
    for name in package_names:
        module = sys.modules.get(name)
        if module is None:
            module = ModuleType(name)
            module.__path__ = []  # type: ignore[attr-defined]
            sys.modules[name] = module
    fake_client = ModuleType("app.ingest.sources.usda_wasde.client")
    fake_client.WASDEReleaseRef = _FakeWASDEReleaseRef
    sys.modules["app.ingest.sources.usda_wasde.client"] = fake_client


def test_parse_available_release_months() -> None:
    html = """
    <select name="date">
      <option value="All">- Any -</option>
      <option value="2026-03">March 2026</option>
      <option value="2026-02">February 2026</option>
    </select>
    """
    assert parse_available_release_months(html) == ["2026-03", "2026-02"]


def test_parse_release_listing_extracts_workbook_release() -> None:
    _install_fake_client_module()
    html = """
    <div class="release">
      <a href="/sites/default/release-files/795813/wasde0326.xls">
        <span class="usa-sr-only"><time datetime="2026-03-10T12:00:00Z">Mar 10 2026</time></span>
      </a>
      <a href="/sites/default/release-files/795813/wasde0326.pdf">PDF</a>
      <a href="/publication/world-agricultural-supply-and-demand-estimates/2026-03-10">View</a>
    </div>
    """
    releases = parse_release_listing(html, month_key="2026-03")
    assert len(releases) == 1
    assert releases[0].released_on == date(2026, 3, 10)
    assert releases[0].workbook_url.endswith("wasde0326.xls")
    assert releases[0].pdf_url and releases[0].pdf_url.endswith("wasde0326.pdf")


def test_parse_release_listing_accepts_xlsx_workbook_release() -> None:
    _install_fake_client_module()
    html = """
    <div class="release">
      <a href="/sites/default/release-files/795813/wasde0326.xlsx">
        <span class="usa-sr-only"><time datetime="2026-03-10T12:00:00Z">Mar 10 2026</time></span>
      </a>
      <a href="/sites/default/release-files/795813/wasde0326.pdf">PDF</a>
      <a href="/publication/world-agricultural-supply-and-demand-estimates/2026-03-10">View</a>
    </div>
    """
    releases = parse_release_listing(html, month_key="2026-03")
    assert len(releases) == 1
    assert releases[0].workbook_url.endswith("wasde0326.xlsx")


def test_market_year_helpers() -> None:
    assert _extract_market_year_label("2025/26 Proj.") == "2025/26"
    assert _market_year_start("2025/26") == 2025
    assert _normalize_entity_label("World  3/") == "World"
