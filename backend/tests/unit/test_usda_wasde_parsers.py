from __future__ import annotations

from datetime import date

from app.ingest.sources.usda_wasde.parsers import (
    _extract_market_year_label,
    _market_year_start,
    _normalize_entity_label,
    parse_available_release_months,
    parse_release_listing,
)


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


def test_market_year_helpers() -> None:
    assert _extract_market_year_label("2025/26 Proj.") == "2025/26"
    assert _market_year_start("2025/26") == 2025
    assert _normalize_entity_label("World  3/") == "World"
