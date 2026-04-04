from __future__ import annotations

from app.db.models.enums import DemandCoverageStatus, LegalStatus
from app.modules.demandwatch import policy


def test_safe_mvp_demand_sources_are_limited_to_cleared_legal_statuses() -> None:
    assert policy.is_safe_demand_source_for_mvp(LegalStatus.PUBLIC_DOMAIN) is True
    assert policy.is_safe_demand_source_for_mvp("cc_by") is True
    assert policy.is_safe_demand_source_for_mvp(LegalStatus.NEEDS_VERIFICATION) is False
    assert policy.is_safe_demand_source_for_mvp(LegalStatus.OFF_LIMITS) is False


def test_ingestable_demand_series_must_be_live_and_legally_cleared() -> None:
    assert policy.is_demand_series_ingestable(DemandCoverageStatus.LIVE, LegalStatus.PUBLIC_DOMAIN) is True
    assert policy.is_demand_series_ingestable(DemandCoverageStatus.LIVE, LegalStatus.CC_BY) is True
    assert policy.is_demand_series_ingestable(DemandCoverageStatus.PLANNED, LegalStatus.PUBLIC_DOMAIN) is False
    assert policy.is_demand_series_ingestable(DemandCoverageStatus.LIVE, LegalStatus.NEEDS_VERIFICATION) is False
    assert policy.is_demand_series_ingestable(DemandCoverageStatus.BLOCKED, LegalStatus.OFF_LIMITS) is False
