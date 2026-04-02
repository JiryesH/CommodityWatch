from __future__ import annotations

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from app.ingest.sources.etf_holdings.parsers import parse_gld_archive, parse_ishares_current_holdings


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
