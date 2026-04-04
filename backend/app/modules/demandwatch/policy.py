from __future__ import annotations

from app.db.models.enums import DemandCoverageStatus, LegalStatus


SAFE_MVP_LEGAL_STATUSES = frozenset(
    {
        LegalStatus.PUBLIC_DOMAIN.value,
        LegalStatus.CC_BY.value,
    }
)


def _enum_value(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)


def is_safe_demand_source_for_mvp(legal_status: object) -> bool:
    return _enum_value(legal_status) in SAFE_MVP_LEGAL_STATUSES


def is_demand_series_ingestable(coverage_status: object, legal_status: object) -> bool:
    return (
        _enum_value(coverage_status) == DemandCoverageStatus.LIVE.value
        and is_safe_demand_source_for_mvp(legal_status)
    )
