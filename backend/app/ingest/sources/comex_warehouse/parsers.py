from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import xlrd


DATE_PATTERNS = (
    re.compile(r"\b([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b"),
    re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b"),
)


class COMEXWarehouseStructureChangedError(RuntimeError):
    pass


@dataclass(slots=True)
class ParsedCOMEXObservation:
    source_series_key: str
    total: float
    registered: float
    eligible: float
    report_date: date
    source_item_ref: str
    source_url: str
    metadata: dict[str, Any]

    def to_item(self) -> dict[str, Any]:
        return {
            "source_series_key": self.source_series_key,
            "value": self.total,
            "total": self.total,
            "registered": self.registered,
            "eligible": self.eligible,
            "observation_date": self.report_date.isoformat(),
            "release_date": self.report_date.isoformat(),
            "updated_at": self.report_date.isoformat(),
            "source_item_ref": self.source_item_ref,
            "source_url": self.source_url,
            **self.metadata,
        }


def parse_comex_workbook(raw: bytes, *, symbol: str, source_url: str) -> ParsedCOMEXObservation:
    workbook = xlrd.open_workbook(file_contents=raw)
    report_date = _extract_report_date(workbook)

    for sheet in workbook.sheets():
        total_today_index = _find_total_today_column(sheet)
        if total_today_index is None:
            continue
        registered = 0.0
        eligible = 0.0
        last_row_index = None
        for row_index in range(sheet.nrows):
            label = str(sheet.cell_value(row_index, 0) or "").strip().casefold()
            if label == "registered":
                registered += _row_numeric(sheet.row_values(row_index), total_today_index)
                last_row_index = row_index
            elif label == "eligible":
                eligible += _row_numeric(sheet.row_values(row_index), total_today_index)
                last_row_index = row_index
        if last_row_index is None:
            continue
        total = registered + eligible
        return ParsedCOMEXObservation(
            source_series_key=symbol,
            total=total,
            registered=registered,
            eligible=eligible,
            report_date=report_date,
            source_item_ref=f"{sheet.name}!{_excel_column_name(total_today_index + 1)}{last_row_index + 1}",
            source_url=source_url,
            metadata={"sheet_name": sheet.name},
        )

    raise COMEXWarehouseStructureChangedError(
        f"Unable to locate Eligible/Registered totals in COMEX {symbol} workbook"
    )


def _extract_report_date(workbook: xlrd.book.Book) -> date:
    for sheet in workbook.sheets():
        for row_index in range(min(sheet.nrows, 25)):
            for value in sheet.row_values(row_index):
                raw = str(value or "")
                for pattern in DATE_PATTERNS:
                    match = pattern.search(raw)
                    if not match:
                        continue
                    try:
                        if "/" in match.group(1):
                            return datetime.strptime(match.group(1), "%m/%d/%Y").date()
                        return datetime.strptime(match.group(1), "%b %d, %Y").date()
                    except ValueError:
                        continue
    raise COMEXWarehouseStructureChangedError("Unable to determine COMEX warehouse report date from workbook")


def _find_total_today_column(sheet: xlrd.sheet.Sheet) -> int | None:
    for row_index in range(min(sheet.nrows, 80)):
        row = [str(value or "").strip().casefold() for value in sheet.row_values(row_index)]
        for column_index, cell in enumerate(row):
            if cell == "total today":
                return column_index
    return None


def _row_numeric(row: list[Any], index: int) -> float:
    value = row[index]
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value or "").replace(",", "").strip()
    if not raw:
        raise COMEXWarehouseStructureChangedError("Expected numeric COMEX total cell but found blank")
    return float(raw)


def _excel_column_name(column_number: int) -> str:
    result = []
    current = column_number
    while current:
        current, remainder = divmod(current - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))
