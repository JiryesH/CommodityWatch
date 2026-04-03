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
        if not _caption_is_tonnes_in_trust(caption_text):
            continue
        tonnes_value = _parse_number(data.get_text(" ", strip=True))
        as_of_date = _parse_caption_date(caption_text)
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
    observations: list[ParsedETFObservation] = []
    with ZipFile(BytesIO(raw)) as workbook:
        shared = _parse_shared_strings(workbook)
        sheet_names = sorted(
            name for name in workbook.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        )
        for sheet_name in sheet_names:
            sheet = ET.fromstring(workbook.read(sheet_name))
            rows = sheet.find("x:sheetData", XLSX_NS)
            if rows is None:
                continue
            observations = _parse_gld_sheet(rows, shared, source_url=source_url, sheet_name=sheet_name.replace(".xml", ""))
            if observations:
                break

    if not observations:
        raise ETFHoldingsStructureChangedError("No GLD archive observations found")
    return observations


def _parse_shared_strings(workbook: ZipFile) -> list[str]:
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall("x:si", XLSX_NS):
        values.append("".join(text.text or "" for text in item.findall(".//x:t", XLSX_NS)))
    return values


def _parse_gld_sheet(rows: ET.Element, shared: list[str], *, source_url: str, sheet_name: str) -> list[ParsedETFObservation]:
    header_columns: dict[str, int] | None = None
    observations: list[ParsedETFObservation] = []

    for row_idx, row in enumerate(rows.findall("x:row", XLSX_NS)):
        values = _xlsx_row_values(row, shared)
        if header_columns is None:
            computed = _gld_header_columns(values)
            if not computed:
                continue
            header_columns = computed
            continue

        raw_date = _cell_by_label(values, header_columns, "date")
        raw_tonnes = _cell_by_label(values, header_columns, "tonnes of gold")
        if raw_tonnes is None:
            raw_tonnes = _cell_by_label(values, header_columns, "tonnes")
        if raw_tonnes is None:
            raw_tonnes = _cell_by_label(values, header_columns, "holdings")
        if raw_date in (None, "", "US Holiday") or raw_tonnes in (None, "", "US Holiday"):
            continue
        try:
            observation_date = _parse_gld_date(str(raw_date))
            tonnes_value = float(str(raw_tonnes).replace(",", ""))
        except ValueError:
            continue
        observations.append(
            ParsedETFObservation(
                source_series_key="GLD",
                value=tonnes_value,
                observation_date=observation_date,
                source_item_ref=f"US GLD Historical Archive!{sheet_name}!{row_idx + 1}",
                source_url=source_url,
                metadata={"provider": "SPDR", "metric": "Tonnes of Gold"},
            )
        )
    return observations


def _gld_header_columns(values: dict[str, str]) -> dict[str, int]:
    normalized = {str(value).strip().lower(): _xlsx_column_index(column) for column, value in values.items() if value}
    header_columns: dict[str, int] = {}
    for label in ("date", "tonnes of gold", "tonnes", "holdings"):
        if label in normalized:
            header_columns[label] = normalized[label]
    return header_columns


def _cell_by_label(values: dict[str, str], header_columns: dict[str, int], label: str) -> str | None:
    index = header_columns.get(label)
    if index is None:
        return None
    return values.get(_xlsx_column_name(index))


def _parse_gld_date(raw: str) -> date:
    normalized = raw.strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported GLD archive date: {raw}")


def _xlsx_column_index(column: str) -> int:
    index = 0
    for char in column:
        if not char.isalpha():
            continue
        index = (index * 26) + (ord(char.upper()) - 64)
    return index - 1


def _xlsx_column_name(index: int) -> str:
    name = ""
    value = index + 1
    while value:
        value, remainder = divmod(value - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _caption_is_tonnes_in_trust(caption_text: str) -> bool:
    return bool(re.search(r"\bTonnes in Trust\b", caption_text, re.IGNORECASE))


def _parse_caption_date(caption_text: str) -> date | None:
    match = re.search(r"as of\s+(.+)$", caption_text, re.IGNORECASE)
    if match is None:
        return None
    raw = match.group(1).strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


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
