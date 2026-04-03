from __future__ import annotations

import importlib.util
import sys
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


PARSER_PATH = Path(__file__).resolve().parents[2] / "app" / "ingest" / "sources" / "etf_holdings" / "parsers.py"
SPEC = importlib.util.spec_from_file_location("test_etf_holdings_parsers_module", PARSER_PATH)
assert SPEC is not None and SPEC.loader is not None
PARSERS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PARSERS
SPEC.loader.exec_module(PARSERS)

parse_gld_archive = PARSERS.parse_gld_archive
parse_ishares_current_holdings = PARSERS.parse_ishares_current_holdings


def test_parse_ishares_current_holdings() -> None:
    html = """
    <div class="product-data-item">
      <div class="caption">
        Tonnes in Trust
        <div class="as-of-date">as of Mar 30, 2026</div>
      </div>
      <div class="data">475.02</div>
    </div>
    """
    parsed = parse_ishares_current_holdings(html, symbol="IAU", source_url="https://example.com")
    assert parsed.source_series_key == "IAU"
    assert parsed.value == 475.02
    assert parsed.observation_date.isoformat() == "2026-03-30"


def test_parse_ishares_current_holdings_accepts_full_month_date() -> None:
    html = """
    <div class="product-data-item">
      <div class="caption">
        Tonnes in Trust
        <div class="as-of-date">as of March 30, 2026</div>
      </div>
      <div class="data">475.02</div>
    </div>
    """
    parsed = parse_ishares_current_holdings(html, symbol="IAU", source_url="https://example.com")
    assert parsed.observation_date.isoformat() == "2026-03-30"


def test_parse_gld_archive() -> None:
    shared_strings = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="4">
      <si><t>Date</t></si>
      <si><t>Tonnes of Gold</t></si>
      <si><t>30-Mar-2026</t></si>
      <si><t>31-Mar-2026</t></si>
    </sst>
    """
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="1">
          <c r="A1" t="s"><v>0</v></c>
          <c r="J1" t="s"><v>1</v></c>
        </row>
        <row r="2">
          <c r="A2" t="s"><v>2</v></c>
          <c r="J2"><v>867.12</v></c>
        </row>
        <row r="3">
          <c r="A3" t="s"><v>3</v></c>
          <c r="J3"><v>868.34</v></c>
        </row>
      </sheetData>
    </worksheet>
    """
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as workbook:
        workbook.writestr("xl/sharedStrings.xml", shared_strings)
        workbook.writestr("xl/worksheets/sheet2.xml", sheet_xml)

    parsed = parse_gld_archive(buffer.getvalue(), source_url="https://example.com/archive.xlsx")
    assert len(parsed) == 2
    assert parsed[-1].observation_date.isoformat() == "2026-03-31"
    assert parsed[-1].value == 868.34


def test_parse_gld_archive_detects_non_default_columns() -> None:
    shared_strings = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="4">
      <si><t>Date</t></si>
      <si><t>Tonnes of Gold</t></si>
      <si><t>29-Mar-2026</t></si>
      <si><t>30-Mar-2026</t></si>
    </sst>
    """
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="1">
          <c r="B1" t="s"><v>0</v></c>
          <c r="F1" t="s"><v>1</v></c>
        </row>
        <row r="2">
          <c r="B2" t="s"><v>2</v></c>
          <c r="F2"><v>867.12</v></c>
        </row>
        <row r="3">
          <c r="B3" t="s"><v>3</v></c>
          <c r="F3"><v>868.34</v></c>
        </row>
      </sheetData>
    </worksheet>
    """
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as workbook:
        workbook.writestr("xl/sharedStrings.xml", shared_strings)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    parsed = parse_gld_archive(buffer.getvalue(), source_url="https://example.com/archive.xlsx")
    assert [item.observation_date.isoformat() for item in parsed] == ["2026-03-29", "2026-03-30"]
    assert parsed[-1].value == 868.34
