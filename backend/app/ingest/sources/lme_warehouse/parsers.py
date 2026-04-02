from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import xlrd


EXPECTED_SHEET = "Metals Totals Report"
EXPECTED_HEADERS = (
    ("Country", "Country/Region"),
    ("Location",),
    ("Opening Stock",),
    ("Delivered In",),
    ("Delivered Out",),
    ("Closing Stock",),
    ("Open Tonnage",),
    ("Cancelled Tonnage",),
)
EXPECTED_METALS = {
    "COPPER": "Copper",
    "ALUMINIUM": "Primary Aluminium",
    "ZINC": "Special High Grade Zinc",
    "NICKEL": "Nickel",
    "TIN": "Tin",
    "LEAD": "Lead",
}


class LMEStructureChangedError(RuntimeError):
    pass


@dataclass(slots=True)
class ParsedLMEObservation:
    source_series_key: str
    metal: str
    on_warrant: float
    cancelled: float
    total: float
    report_date: date
    source_item_ref: str
    source_url: str
    metadata: dict[str, Any]

    def to_item(self) -> dict[str, Any]:
        return {
            "source_series_key": self.source_series_key,
            "metal": self.metal,
            "on_warrant": self.on_warrant,
            "cancelled": self.cancelled,
            "total": self.total,
            "observation_date": self.report_date.isoformat(),
            "release_date": self.report_date.isoformat(),
            "updated_at": self.report_date.isoformat(),
            "source_item_ref": self.source_item_ref,
            "source_url": self.source_url,
            **self.metadata,
        }


def parse_lme_workbook(raw: bytes, *, report_date: date, source_url: str) -> list[ParsedLMEObservation]:
    book = xlrd.open_workbook(file_contents=raw)
    if EXPECTED_SHEET not in book.sheet_names():
        raise LMEStructureChangedError(f"Missing expected LME sheet: {EXPECTED_SHEET}")

    sheet = book.sheet_by_name(EXPECTED_SHEET)
    header_row_index = _find_header_row(sheet)

    current_label: str | None = None
    parsed: dict[str, ParsedLMEObservation] = {}
    label_lookup = {label.casefold(): source_series_key for source_series_key, label in EXPECTED_METALS.items()}

    for row_index in range(header_row_index + 1, sheet.nrows):
        row = sheet.row_values(row_index)
        label = _clean_text(row[1] if len(row) > 1 else "")
        row_lead = _clean_text(row[0] if row else "")
        if label and not row_lead:
            current_label = label
        if row_lead != "Total":
            continue
        if current_label is None:
            continue
        source_series_key = label_lookup.get(current_label.casefold())
        if source_series_key is None:
            continue
        total = _numeric(row[5])
        on_warrant = _numeric(row[6])
        cancelled = _numeric(row[7])
        parsed[source_series_key] = ParsedLMEObservation(
            source_series_key=source_series_key,
            metal=source_series_key,
            on_warrant=on_warrant,
            cancelled=cancelled,
            total=total,
            report_date=report_date,
            source_item_ref=f"{sheet.name}!F{row_index + 1}",
            source_url=source_url,
            metadata={"sheet_name": sheet.name, "metal_label": current_label},
        )

    missing = [key for key in EXPECTED_METALS if key not in parsed]
    if missing:
        raise LMEStructureChangedError(
            f"Missing expected LME metals in workbook: {', '.join(sorted(missing))}"
        )
    return [parsed[key] for key in EXPECTED_METALS]


def _find_header_row(sheet: xlrd.sheet.Sheet) -> int:
    for row_index in range(sheet.nrows):
        row = tuple(_clean_text(value) for value in sheet.row_values(row_index)[: len(EXPECTED_HEADERS)])
        if len(row) != len(EXPECTED_HEADERS):
            continue
        if all(cell in expected_values for cell, expected_values in zip(row, EXPECTED_HEADERS, strict=True)):
            return row_index
    raise LMEStructureChangedError("LME workbook header row did not match the expected fingerprint")


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _numeric(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    raw = _clean_text(value).replace(",", "")
    if not raw:
        raise LMEStructureChangedError("Expected numeric LME workbook cell but found blank")
    return float(raw)
