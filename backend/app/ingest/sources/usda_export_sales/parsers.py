from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from xml.etree import ElementTree as ET


DATE_PATTERN = re.compile(r"([A-Z][a-z]+ \d{1,2}, \d{4})")
PERIOD_END_PATTERN = re.compile(r"For Period Ending ([A-Z][a-z]+ \d{1,2}, \d{4})")


@dataclass(frozen=True, slots=True)
class ExportSalesReleaseInfo:
    released_on: date
    period_ending_on: date | None


@dataclass(frozen=True, slots=True)
class ParsedExportSalesObservation:
    source_series_key: str
    period_ending_on: date
    raw_net_sales_kt: float
    value_mmt: float
    marketing_year: str
    source_item_ref: str

    def to_item(self) -> dict[str, object]:
        return {
            "source_series_key": self.source_series_key,
            "period_ending_on": self.period_ending_on.isoformat(),
            "raw_net_sales_kt": self.raw_net_sales_kt,
            "value_mmt": self.value_mmt,
            "marketing_year": self.marketing_year,
            "source_item_ref": self.source_item_ref,
        }


def _parse_long_date(raw: str) -> date:
    month_name, day_value, year_value = raw.replace(",", "").split()
    months = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }
    return date(int(year_value), months[month_name], int(day_value))


def _parse_period_ending_date(raw: str) -> date:
    normalized = str(raw or "").strip()
    if not normalized:
        raise ValueError("USDA export sales summary row is missing PeriodEndingDate.")
    if "/" in normalized:
        month_value, day_value, year_value = normalized.split("/")
        return date(int(year_value), int(month_value), int(day_value))
    return date.fromisoformat(normalized)


def parse_export_sales_release_info(raw: bytes) -> ExportSalesReleaseInfo:
    root = ET.fromstring(raw)
    text = next((node.attrib.get("Textbox1", "") for node in root.iter() if node.tag.endswith("Details")), "")
    date_matches = DATE_PATTERN.findall(text)
    if not date_matches:
        raise ValueError("Weekly export sales highlights payload did not contain a release date.")
    period_match = PERIOD_END_PATTERN.search(text)
    period_ending_on = _parse_long_date(period_match.group(1)) if period_match else None
    released_on = None
    if period_ending_on is not None:
        for match in date_matches:
            candidate = _parse_long_date(match)
            if candidate > period_ending_on:
                released_on = candidate
                break
    if released_on is None:
        released_on = _parse_long_date(date_matches[-1])
    return ExportSalesReleaseInfo(
        released_on=released_on,
        period_ending_on=period_ending_on,
    )


def parse_export_sales_summary(raw: bytes) -> list[ParsedExportSalesObservation]:
    root = ET.fromstring(raw)
    parsed: list[ParsedExportSalesObservation] = []
    for node in root.iter():
        if not node.tag.endswith("Details"):
            continue
        attrs = node.attrib
        commodity_code = attrs.get("CommodityCode")
        period_ending = attrs.get("PeriodEndingDate")
        net_sales = attrs.get("NetSales")
        marketing_year = attrs.get("MarketingYear")
        if not commodity_code or not period_ending or net_sales is None or not marketing_year:
            continue
        raw_net_sales_kt = float(net_sales)
        parsed.append(
            ParsedExportSalesObservation(
                source_series_key=commodity_code,
                period_ending_on=_parse_period_ending_date(period_ending),
                raw_net_sales_kt=raw_net_sales_kt,
                value_mmt=raw_net_sales_kt / 1000.0,
                marketing_year=marketing_year,
                source_item_ref=f"{commodity_code}:{period_ending}",
            )
        )
    return parsed
