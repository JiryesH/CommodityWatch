from __future__ import annotations

from app.db.models.enums import DemandCoverageStatus, LegalStatus


SAFE_MVP_LEGAL_STATUSES = frozenset(
    {
        LegalStatus.PUBLIC_DOMAIN.value,
        LegalStatus.CC_BY.value,
        LegalStatus.PUBLIC_REGISTERED.value,
    }
)
DEMANDWATCH_CANONICAL_UNIT_BY_SERIES_CODE: dict[str, str] = {
    "EIA_US_TOTAL_PRODUCT_SUPPLIED": "kb_d",
    "EIA_GASOLINE_US_PRODUCT_SUPPLIED": "kb_d",
    "EIA_DISTILLATE_US_PRODUCT_SUPPLIED": "kb_d",
    "EIA_CRUDE_US_REFINERY_INPUTS": "kb_d",
    "EIA_CRUDE_US_REFINERY_UTILISATION": "pct",
    "FRED_US_VEHICLE_MILES_TRAVELED": "m_vehicle_miles",
    "EIA_US_ELECTRICITY_GRID_LOAD": "gw",
    "EMBER_GLOBAL_ELECTRICITY_DEMAND": "twh",
    "EMBER_CHINA_ELECTRICITY_DEMAND": "twh",
    "USDA_US_CORN_TOTAL_USE_WASDE": "mbu",
    "USDA_US_SOYBEAN_TOTAL_USE_WASDE": "mbu",
    "USDA_US_WHEAT_TOTAL_USE_WASDE": "mbu",
    "USDA_US_CORN_EXPORT_SALES": "mmt",
    "USDA_US_SOYBEAN_EXPORT_SALES": "mmt",
    "USDA_US_WHEAT_EXPORT_SALES": "mmt",
    "EIA_US_ETHANOL_PRODUCTION": "kb_d",
    "FRED_US_INDUSTRIAL_PRODUCTION": "index",
    "FRED_US_MANUFACTURING_PRODUCTION": "index",
    "FRED_US_MANUFACTURING_CAPACITY_UTILISATION": "pct",
    "FRED_US_HOUSING_STARTS": "k_units_saar",
    "FRED_US_TOTAL_VEHICLE_SALES": "m_units_saar",
    "FRED_US_BUILDING_PERMITS": "k_units_saar",
    "OECD_JAPAN_COMPOSITE_LEADING_INDICATOR": "index",
    "OECD_SOUTH_KOREA_COMPOSITE_LEADING_INDICATOR": "index",
    "OECD_INDIA_COMPOSITE_LEADING_INDICATOR": "index",
    "CHINA_CRUDE_IMPORTS_MONTHLY": "mmt",
    "CHINA_REFINERY_THROUGHPUT_MONTHLY": "mmt",
    "WORLDSTEEL_GLOBAL_CRUDE_STEEL_PRODUCTION": "mmt",
}


def _enum_value(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)


def is_safe_demand_source_for_mvp(legal_status: object) -> bool:
    return _enum_value(legal_status) in SAFE_MVP_LEGAL_STATUSES


def is_demand_series_ingestable(coverage_status: object, legal_status: object) -> bool:
    return (
        _enum_value(coverage_status) == DemandCoverageStatus.LIVE.value
        and is_safe_demand_source_for_mvp(legal_status)
    )


def expected_canonical_unit_for_series(code: object) -> str | None:
    return DEMANDWATCH_CANONICAL_UNIT_BY_SERIES_CODE.get(str(code))
