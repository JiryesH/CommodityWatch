from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from bs4 import BeautifulSoup


XLSX_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


class ETFHoldingsStructureChangedError(RuntimeError):
    pass


@dataclass(slots=True)
class ParsedETFObservation:
    source_series_key: str
    value: float
    observation_date: date
    source_item_ref: str
    source_url: str
    metadata: dict[str, str | float]

    def to_item(self) -> dict[str, str | float]:
        return {
            "source_series_key": self.source_series_key,
            "value": self.value,
            "observation_date": self.observation_date.isoformat(),
            "release_date": self.observation_date.isoformat(),
            "updated_at": self.observation_date.isoformat(),
            "source_item_ref": self.source_item_ref,
            "source_url": self.source_url,
            **self.metadata,
        }


def parse_ishares_current_holdings(html: str, *, symbol: str, source_url: str) -> ParsedETFObservation:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("div.product-data-item")
    if not items:
        raise ETFHoldingsStructureChangedError(f"Missing product data items for {symbol}")

    tonnes_value = None
    as_of_date = None
    for item in items:
        caption = item.select_one("div.caption")
        data = item.select_one("div.data")
        if caption is None or data is None:
            continue
        caption_text = " ".join(caption.stripped_strings)
        if "Tonnes in Trust" not in caption_text:
            continue
        tonnes_value = _parse_number(data.get_text(" ", strip=True))
        date_match = re.search(r"as of\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4})", caption_text)
        if date_match:
            as_of_date = datetime.strptime(date_match.group(1), "%b %d, %Y").date()
        break

    if tonnes_value is None or as_of_date is None:
        raise ETFHoldingsStructureChangedError(f"Missing Tonnes in Trust block for {symbol}")

    return ParsedETFObservation(
        source_series_key=symbol,
        value=tonnes_value,
        observation_date=as_of_date,
        source_item_ref=f"{symbol}:Tonnes in Trust",
        source_url=source_url,
        metadata={"provider": "iShares", "metric": "Tonnes in Trust"},
    )


def parse_gld_archive(raw: bytes, *, source_url: str) -> list[ParsedETFObservation]:
    with ZipFile(BytesIO(raw)) as workbook:
        shared = _parse_shared_strings(workbook)
        sheet = ET.fromstring(workbook.read("xl/worksheets/sheet2.xml"))

    rows = sheet.find("x:sheetData", XLSX_NS)
    if rows is None:
        raise ETFHoldingsStructureChangedError("Missing GLD historical sheet data")

    header_map: dict[str, str] = {}
    observations: list[ParsedETFObservation] = []
    for row_idx, row in enumerate(rows.findall("x:row", XLSX_NS)):
        values = _xlsx_row_values(row, shared)
        if row_idx == 0:
            header_map = {cell_ref: value for cell_ref, value in values.items()}
            continue
        raw_date = values.get("A")
        raw_tonnes = values.get("J")
        if raw_date in (None, "", "US Holiday") or raw_tonnes in (None, "", "US Holiday"):
            continue
        try:
            observation_date = datetime.strptime(str(raw_date), "%d-%b-%Y").date()
            tonnes_value = float(str(raw_tonnes))
        except ValueError:
            continue
        observations.append(
            ParsedETFObservation(
                source_series_key="GLD",
                value=tonnes_value,
                observation_date=observation_date,
                source_item_ref=f"US GLD Historical Archive!J{row_idx + 1}",
                source_url=source_url,
                metadata={"provider": "SPDR", "metric": "Tonnes of Gold"},
            )
        )

    if not observations:
        raise ETFHoldingsStructureChangedError("No GLD archive observations found")
    return observations


def _parse_shared_strings(workbook: ZipFile) -> list[str]:
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall("x:si", XLSX_NS):
        values.append("".join(text.text or "" for text in item.findall(".//x:t", XLSX_NS)))
    return values


def _xlsx_row_values(row: ET.Element, shared: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for cell in row.findall("x:c", XLSX_NS):
        cell_ref = cell.get("r") or ""
        column = re.sub(r"\d+", "", cell_ref)
        value_node = cell.find("x:v", XLSX_NS)
        if not column or value_node is None:
            continue
        value = value_node.text or ""
        if cell.get("t") == "s" and value:
            values[column] = shared[int(value)]
        else:
            values[column] = value
    return values


def _parse_number(raw: str) -> float:
    return float(str(raw).replace(",", "").strip())
