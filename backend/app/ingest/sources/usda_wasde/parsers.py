from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urljoin

import xlrd
from bs4 import BeautifulSoup


PUBLICATION_URL = "https://esmis.nal.usda.gov/publication/world-agricultural-supply-and-demand-estimates"
EXPECTED_WORKBOOK_SHEETS = {"Page 11", "Page 12", "Page 14", "Page 15", "Page 19", "Page 23", "Page 28"}


class USDAWASDEStructureChangedError(RuntimeError):
    pass


@dataclass(slots=True)
class ParsedWASDEObservation:
    source_series_key: str
    value: float
    unit_native_code: str
    release_date: date
    market_year: int
    marketing_year_label: str
    source_item_ref: str
    sheet_name: str
    metadata: dict[str, Any]

    def to_item(self) -> dict[str, Any]:
        return {
            "source_series_key": self.source_series_key,
            "value": self.value,
            "unit_native_code": self.unit_native_code,
            "observation_date": self.release_date.isoformat(),
            "release_date": self.release_date.isoformat(),
            "updated_at": self.release_date.isoformat(),
            "market_year": self.market_year,
            "marketing_year_label": self.marketing_year_label,
            "source_item_ref": self.source_item_ref,
            "sheet_name": self.sheet_name,
            **self.metadata,
        }


@dataclass(frozen=True, slots=True)
class _USSectionSpec:
    source_series_key: str
    sheet_name: str
    section_title: str
    row_label: str
    unit_native_code: str
    next_section_title: str | None = None
    multiplier: float = 1.0


@dataclass(frozen=True, slots=True)
class _WorldSectionSpec:
    source_series_key: str
    sheet_name: str
    entity_label: str
    unit_native_code: str


US_SECTION_SPECS = (
    _USSectionSpec(
        source_series_key="0410000:9000000",
        sheet_name="Page 11",
        section_title="U.S. Wheat Supply and Use",
        row_label="Ending Stocks",
        unit_native_code="mbu",
        next_section_title="U.S. Wheat by Class: Supply and Use",
    ),
    _USSectionSpec(
        source_series_key="0440000:9000000",
        sheet_name="Page 12",
        section_title="CORN",
        row_label="Ending Stocks",
        unit_native_code="mbu",
    ),
    _USSectionSpec(
        source_series_key="0422110:9000000",
        sheet_name="Page 14",
        section_title="TOTAL RICE",
        row_label="Ending Stocks",
        unit_native_code="kcwt",
        next_section_title="LONG-GRAIN RICE",
        multiplier=1000.0,
    ),
    _USSectionSpec(
        source_series_key="2222000:9000000",
        sheet_name="Page 15",
        section_title="SOYBEANS",
        row_label="Ending Stocks",
        unit_native_code="mbu",
        next_section_title="SOYBEAN OIL",
    ),
)

WORLD_SECTION_SPECS = (
    _WorldSectionSpec(
        source_series_key="0410000:0000999",
        sheet_name="Page 19",
        entity_label="World",
        unit_native_code="mmt",
    ),
    _WorldSectionSpec(
        source_series_key="0440000:0000999",
        sheet_name="Page 23",
        entity_label="World",
        unit_native_code="mmt",
    ),
    _WorldSectionSpec(
        source_series_key="2222000:0000999",
        sheet_name="Page 28",
        entity_label="World",
        unit_native_code="mmt",
    ),
)


def parse_available_release_months(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", attrs={"name": "date"})
    if select is None:
        raise USDAWASDEStructureChangedError("Missing WASDE month filter select")

    months: list[str] = []
    for option in select.find_all("option"):
        value = (option.get("value") or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}", value):
            months.append(value)
    if not months:
        raise USDAWASDEStructureChangedError("No WASDE release months found in archive page")
    return months


def parse_release_listing(html: str, *, month_key: str) -> list[Any]:
    from app.ingest.sources.usda_wasde.client import WASDEReleaseRef

    soup = BeautifulSoup(html, "html.parser")
    workbook_links = soup.find_all("a", href=re.compile(r"wasde\d+\.xls$", re.IGNORECASE))
    releases: list[WASDEReleaseRef] = []
    seen_urls: set[str] = set()

    for link in workbook_links:
        href = (link.get("href") or "").strip()
        workbook_url = urljoin(PUBLICATION_URL, href)
        if workbook_url in seen_urls:
            continue
        time_tag = link.find("time")
        if time_tag is None:
            container = link.parent
            while container is not None and time_tag is None:
                time_tag = container.find("time")
                container = container.parent
        if time_tag is None or not time_tag.get("datetime"):
            raise USDAWASDEStructureChangedError("Missing release time for WASDE workbook link")
        released_on = datetime_from_html(time_tag["datetime"]).date()

        container = link.parent
        detail_href = None
        pdf_href = None
        while container is not None and (detail_href is None or pdf_href is None):
            detail_link = container.find(
                "a",
                href=re.compile(r"/publication/world-agricultural-supply-and-demand-estimates/\d{4}-\d{2}-\d{2}$"),
            )
            pdf_link = container.find("a", href=re.compile(r"wasde\d+\.pdf$", re.IGNORECASE))
            if detail_link is not None and detail_href is None:
                detail_href = detail_link.get("href")
            if pdf_link is not None and pdf_href is None:
                pdf_href = pdf_link.get("href")
            container = container.parent

        releases.append(
            WASDEReleaseRef(
                released_on=released_on,
                workbook_url=workbook_url,
                source_url=urljoin(PUBLICATION_URL, detail_href or workbook_url),
                month_key=month_key,
                pdf_url=urljoin(PUBLICATION_URL, pdf_href) if pdf_href else None,
                title="World Agricultural Supply and Demand Estimates",
            )
        )
        seen_urls.add(workbook_url)

    if not releases:
        raise USDAWASDEStructureChangedError(f"No WASDE workbook links found for month {month_key}")
    return sorted(releases, key=lambda item: item.released_on, reverse=True)


def parse_wasde_workbook(raw: bytes, *, release_date: date) -> list[ParsedWASDEObservation]:
    book = xlrd.open_workbook(file_contents=raw)
    missing_sheets = EXPECTED_WORKBOOK_SHEETS.difference(book.sheet_names())
    if missing_sheets:
        raise USDAWASDEStructureChangedError(
            f"Missing expected WASDE workbook sheets: {', '.join(sorted(missing_sheets))}"
        )

    observations = [
        *_parse_us_sections(book, release_date),
        *_parse_world_sections(book, release_date),
    ]
    if len(observations) != 7:
        raise USDAWASDEStructureChangedError(f"Expected 7 WASDE observations, found {len(observations)}")
    return observations


def _parse_us_sections(book: xlrd.book.Book, release_date: date) -> list[ParsedWASDEObservation]:
    month_label = release_date.strftime("%b")
    parsed: list[ParsedWASDEObservation] = []
    for spec in US_SECTION_SPECS:
        sheet = book.sheet_by_name(spec.sheet_name)
        section_row = _find_row(sheet, spec.section_title)
        header_row = _find_projection_header_row(sheet, start=section_row)
        end_row = _find_row(sheet, spec.next_section_title, start=header_row + 1) if spec.next_section_title else sheet.nrows
        month_row = _find_month_header_row(sheet, start=header_row)
        current_col = _find_column_by_month(sheet, month_row, month_label)
        target_row = _find_row(sheet, spec.row_label, start=header_row + 1, end=end_row)
        value = _numeric_cell(sheet.cell_value(target_row, current_col)) * spec.multiplier
        market_year_label = _extract_market_year_label(str(sheet.cell_value(header_row, current_col)))
        parsed.append(
            ParsedWASDEObservation(
                source_series_key=spec.source_series_key,
                value=value,
                unit_native_code=spec.unit_native_code,
                release_date=release_date,
                market_year=_market_year_start(market_year_label),
                marketing_year_label=market_year_label,
                source_item_ref=f"{spec.sheet_name}!{_excel_column_name(current_col)}{target_row + 1}",
                sheet_name=spec.sheet_name,
                metadata={"section_title": spec.section_title, "release_month": month_label},
            )
        )
    return parsed


def _parse_world_sections(book: xlrd.book.Book, release_date: date) -> list[ParsedWASDEObservation]:
    month_label = release_date.strftime("%b").lower()[:3]
    parsed: list[ParsedWASDEObservation] = []
    for spec in WORLD_SECTION_SPECS:
        sheet = book.sheet_by_name(spec.sheet_name)
        section_row = _find_projection_section_row(sheet)
        end_col = _find_column(sheet, section_row, "Ending Stocks")

        current_entity = ""
        current_row_index = None
        current_market_year_label = _extract_market_year_label(str(sheet.cell_value(section_row, 0)))
        for row_idx in range(section_row + 1, sheet.nrows):
            entity_value = _clean_text(sheet.cell_value(row_idx, 0))
            month_value = _clean_text(sheet.cell_value(row_idx, 1)).lower()[:3]
            if entity_value:
                current_entity = _normalize_entity_label(entity_value)
            if month_value == month_label and current_entity == spec.entity_label:
                current_row_index = row_idx
                break

        if current_row_index is None:
            raise USDAWASDEStructureChangedError(
                f"Missing {spec.entity_label} {release_date.strftime('%b')} row on {spec.sheet_name}"
            )

        value = _numeric_cell(sheet.cell_value(current_row_index, end_col))
        parsed.append(
            ParsedWASDEObservation(
                source_series_key=spec.source_series_key,
                value=value,
                unit_native_code=spec.unit_native_code,
                release_date=release_date,
                market_year=_market_year_start(current_market_year_label),
                marketing_year_label=current_market_year_label,
                source_item_ref=f"{spec.sheet_name}!{_excel_column_name(end_col)}{current_row_index + 1}",
                sheet_name=spec.sheet_name,
                metadata={"entity_label": spec.entity_label, "release_month": release_date.strftime("%b")},
            )
        )
    return parsed


def _find_row(sheet: xlrd.sheet.Sheet, text: str, *, start: int = 0, end: int | None = None) -> int:
    end = sheet.nrows if end is None else min(end, sheet.nrows)
    target = _normalize_entity_label(text)
    for row_idx in range(start, end):
        value = _normalize_entity_label(_clean_text(sheet.cell_value(row_idx, 0)))
        if value == target:
            return row_idx
    raise USDAWASDEStructureChangedError(f"Missing row '{text}' on sheet {sheet.name}")


def _find_projection_section_row(sheet: xlrd.sheet.Sheet) -> int:
    for row_idx in range(sheet.nrows):
        value = _clean_text(sheet.cell_value(row_idx, 0))
        if re.fullmatch(r"\d{4}/\d{2}\s+Proj\.", value):
            return row_idx
    raise USDAWASDEStructureChangedError(f"Missing current projection section on sheet {sheet.name}")


def _find_projection_header_row(sheet: xlrd.sheet.Sheet, *, start: int) -> int:
    for row_idx in range(start, min(start + 8, sheet.nrows)):
        for col_idx in range(sheet.ncols):
            value = _clean_text(sheet.cell_value(row_idx, col_idx))
            if re.fullmatch(r"\d{4}/\d{2}\s+Proj\.", value):
                return row_idx
    raise USDAWASDEStructureChangedError(f"Missing current projection header row on sheet {sheet.name}")


def _find_month_header_row(sheet: xlrd.sheet.Sheet, *, start: int) -> int:
    for row_idx in range(start, min(start + 3, sheet.nrows)):
        for col_idx in range(sheet.ncols):
            value = _clean_text(sheet.cell_value(row_idx, col_idx)).lower()[:3]
            if value in {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}:
                return row_idx
    raise USDAWASDEStructureChangedError(f"Missing current release month row on sheet {sheet.name}")


def _find_column_by_month(sheet: xlrd.sheet.Sheet, row_idx: int, month_label: str) -> int:
    normalized = month_label.lower()[:3]
    for col_idx in range(sheet.ncols - 1, -1, -1):
        value = _clean_text(sheet.cell_value(row_idx, col_idx)).lower()[:3]
        if value == normalized:
            return col_idx
    raise USDAWASDEStructureChangedError(f"Missing release month column '{month_label}' on sheet {sheet.name}")


def _find_column(sheet: xlrd.sheet.Sheet, row_idx: int, header_text: str) -> int:
    target = _normalize_entity_label(header_text)
    for col_idx in range(sheet.ncols):
        value = _normalize_entity_label(_clean_text(sheet.cell_value(row_idx, col_idx)))
        if value == target:
            return col_idx
    raise USDAWASDEStructureChangedError(f"Missing column '{header_text}' on sheet {sheet.name}")


def _numeric_cell(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = _clean_text(value).replace(",", "")
    if not text:
        raise USDAWASDEStructureChangedError("Expected numeric cell, found blank")
    return float(text)


def _clean_text(value: Any) -> str:
    return str(value or "").replace("\n", " ").strip()


def _normalize_entity_label(value: str) -> str:
    collapsed = re.sub(r"\s+", " ", value).strip()
    collapsed = re.sub(r"\s+\d+/$", "", collapsed)
    return collapsed


def _extract_market_year_label(raw: str) -> str:
    match = re.search(r"(\d{4}/\d{2})", raw)
    if match is None:
        raise USDAWASDEStructureChangedError(f"Unable to parse market year label from '{raw}'")
    return match.group(1)


def _market_year_start(label: str) -> int:
    return int(label.split("/", 1)[0])


def _excel_column_name(col_idx: int) -> str:
    name = ""
    value = col_idx + 1
    while value:
        value, remainder = divmod(value - 1, 26)
        name = chr(65 + remainder) + name
    return name


def datetime_from_html(raw: str):
    from datetime import datetime

    value = raw.strip()
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)
