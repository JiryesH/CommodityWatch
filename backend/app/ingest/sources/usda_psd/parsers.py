from __future__ import annotations

from dataclasses import dataclass
from xml.etree import ElementTree as ET


PSD_TOTAL_USE_LABELS = {
    "domestic consumption",
    "domestic use",
    "total consumption",
    "total use",
}
MILLION_BUSHELS_PER_THOUSAND_MT = {
    "0440000": 39.3680 / 1000.0,
    "2222000": 36.7437 / 1000.0,
    "0410000": 36.7437 / 1000.0,
}


@dataclass(frozen=True, slots=True)
class ParsedPSDObservation:
    source_series_key: str
    release_month: str
    market_year: int
    raw_value: float
    value_mbu: float
    raw_unit_description: str
    attribute_description: str
    source_item_ref: str

    def to_item(self) -> dict[str, object]:
        return {
            "source_series_key": self.source_series_key,
            "release_month": self.release_month,
            "market_year": self.market_year,
            "raw_value": self.raw_value,
            "value_mbu": self.value_mbu,
            "raw_unit_description": self.raw_unit_description,
            "attribute_description": self.attribute_description,
            "source_item_ref": self.source_item_ref,
        }


def _normalize_label(raw: str | None) -> str:
    return " ".join((raw or "").strip().lower().split())


def parse_psd_commodity_response(raw: bytes, *, commodity_code: str) -> list[ParsedPSDObservation]:
    root = ET.fromstring(raw)
    parsed: list[ParsedPSDObservation] = []
    mbu_factor = MILLION_BUSHELS_PER_THOUSAND_MT.get(commodity_code)
    if mbu_factor is None:
        raise ValueError(f"Unsupported PSD commodity conversion for {commodity_code}")

    for node in root.iter():
        if node.tag.endswith("Commodity"):
            item = {child.tag.split("}", 1)[-1]: (child.text or "").strip() for child in list(node)}
            if item.get("Commodity_code") != commodity_code:
                continue
            if item.get("Country_Code") != "US":
                continue
            attribute = _normalize_label(item.get("Attribute_Description"))
            if attribute not in PSD_TOTAL_USE_LABELS:
                continue
            value = float(item["Value"])
            market_year = int(item["Market_Year"])
            calendar_year = int(item["Calendar_Year"])
            month = int(item["Month"])
            parsed.append(
                ParsedPSDObservation(
                    source_series_key=commodity_code,
                    release_month=f"{calendar_year:04d}-{month:02d}",
                    market_year=market_year,
                    raw_value=value,
                    value_mbu=value * mbu_factor,
                    raw_unit_description=item.get("Unit_Description", ""),
                    attribute_description=item.get("Attribute_Description", ""),
                    source_item_ref=f"{commodity_code}:US:{market_year}:{calendar_year:04d}-{month:02d}",
                )
            )
    return parsed
