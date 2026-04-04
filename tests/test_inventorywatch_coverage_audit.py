from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from inventory_watch_local_api import InventoryIndicatorDefinition, InventoryObservation
from inventory_watch_published_db import (
    InventoryCoverageThresholds,
    build_inventory_coverage_audit,
    inventory_coverage_audit_markdown,
    inventory_coverage_blocking_issues,
)
from scripts.audit_inventorywatch_coverage import coverage_blocking_message as audit_coverage_blocking_message
from scripts.publish_inventorywatch_store import coverage_blocking_message as publish_coverage_blocking_message
from scripts.publish_inventorywatch_store import validate_published_store


UTC = timezone.utc
NOW = datetime(2026, 4, 2, 12, 0, tzinfo=UTC)


def make_indicator(
    code: str,
    *,
    source_slug: str = "usda",
    frequency: str = "weekly",
    is_seasonal: bool = False,
    visibility_tier: str = "public",
    metadata: dict[str, object] | None = None,
) -> InventoryIndicatorDefinition:
    merged_metadata: dict[str, object] = {"source_label": source_slug.upper()}
    if metadata:
        merged_metadata.update(metadata)
    return InventoryIndicatorDefinition(
        id=code,
        code=code,
        name=code.replace("_", " ").title(),
        description=None,
        measure_family="inventory",
        frequency=frequency,
        commodity_code="wheat",
        geography_code=None,
        source_slug=source_slug,
        source_series_key=code.upper(),
        native_unit_code="kt",
        canonical_unit_code="kt",
        default_observation_kind="actual",
        seasonal_profile="inventorywatch_5y" if is_seasonal else None,
        is_seasonal=is_seasonal,
        is_derived=False,
        visibility_tier=visibility_tier,
        metadata=merged_metadata,
    )


def make_observation(
    *,
    period_days_ago: int | None = None,
    release_days_ago: int | None = None,
    value: float = 100.0,
    frequency: str = "weekly",
    period_end_at: datetime | None = None,
    release_date: datetime | None = None,
) -> InventoryObservation:
    resolved_period_end_at = period_end_at or NOW - timedelta(days=period_days_ago or 0)
    resolved_release_date = release_date
    if resolved_release_date is None and release_days_ago is not None:
        resolved_release_date = NOW - timedelta(days=release_days_ago)
    period_start_at = resolved_period_end_at - (timedelta(days=6) if frequency == "weekly" else timedelta(days=0))
    vintage_at = resolved_release_date or NOW
    return InventoryObservation(
        period_start_at=period_start_at,
        period_end_at=resolved_period_end_at,
        release_date=resolved_release_date,
        vintage_at=vintage_at,
        value=value,
        unit="kt",
        observation_kind="actual",
    )


class FakeRepository:
    def __init__(
        self,
        indicators: list[InventoryIndicatorDefinition],
        observations: dict[str, list[InventoryObservation]],
        seasonal_cache: dict[tuple[str, str], list[dict[str, float | int | None]]],
    ) -> None:
        self._indicators_by_id = {indicator.id: indicator for indicator in indicators}
        self._observations_by_id = observations
        self._seasonal_cache = seasonal_cache

    def _seasonal_range(
        self,
        indicator: InventoryIndicatorDefinition,
        profile_name: str | None,
    ) -> list[dict[str, float | int | None]]:
        resolved_profile = profile_name or indicator.seasonal_profile or "inventorywatch_5y"
        return self._seasonal_cache.get((indicator.id, resolved_profile), [])


def test_coverage_audit_reports_structural_gaps_and_suitability() -> None:
    good = make_indicator("good_weekly", is_seasonal=True)
    thin = make_indicator("thin_weekly", is_seasonal=True)
    empty = make_indicator("empty_weekly", is_seasonal=False)
    hidden = make_indicator("hidden_weekly", is_seasonal=False, visibility_tier="internal")

    repository = FakeRepository(
        [good, thin, empty, hidden],
        {
            good.id: [
                make_observation(period_days_ago=21, release_days_ago=3, value=100.0),
                make_observation(period_days_ago=14, release_days_ago=2, value=101.0),
                make_observation(period_days_ago=7, release_days_ago=1, value=102.0),
            ],
            thin.id: [
                make_observation(period_days_ago=14, release_days_ago=2, value=50.0),
                make_observation(period_days_ago=7, release_days_ago=1, value=51.0),
            ],
            hidden.id: [
                make_observation(period_days_ago=14, release_days_ago=2, value=60.0),
            ],
        },
        {
            (good.id, "inventorywatch_5y"): [
                {"period_index": 1, "p50": 90.0},
                {"period_index": 2, "p50": 91.0},
                {"period_index": 3, "p50": 92.0},
                {"period_index": 4, "p50": 93.0},
                {"period_index": 5, "p50": 94.0},
                {"period_index": 6, "p50": 95.0},
                {"period_index": 7, "p50": 96.0},
                {"period_index": 8, "p50": 97.0},
                {"period_index": 9, "p50": 98.0},
                {"period_index": 10, "p50": 99.0},
                {"period_index": 11, "p50": 100.0},
                {"period_index": 12, "p50": 101.0},
                {"period_index": 13, "p50": 102.0},
                {"period_index": 14, "p50": 103.0},
                {"period_index": 15, "p50": 104.0},
                {"period_index": 16, "p50": 105.0},
                {"period_index": 17, "p50": 106.0},
                {"period_index": 18, "p50": 107.0},
                {"period_index": 19, "p50": 108.0},
                {"period_index": 20, "p50": 109.0},
                {"period_index": 21, "p50": 110.0},
                {"period_index": 22, "p50": 111.0},
                {"period_index": 23, "p50": 112.0},
                {"period_index": 24, "p50": 113.0},
                {"period_index": 25, "p50": 114.0},
                {"period_index": 26, "p50": 115.0},
            ],
            (thin.id, "inventorywatch_5y"): [
                {"period_index": 1, "p50": 45.0},
                {"period_index": 2, "p50": 46.0},
            ],
        },
    )

    audit = build_inventory_coverage_audit(repository, now=NOW, thresholds=InventoryCoverageThresholds(min_seasonal_points=12))

    assert audit["summary"]["indicator_count"] == 4
    assert audit["summary"]["public_indicator_count"] == 3
    assert audit["summary"]["indicators_with_observations"] == 3
    assert audit["summary"]["total_observations"] == 6
    assert audit["summary"]["indicators_with_usable_seasonal_ranges"] == 1
    assert audit["summary"]["public_display_suitable_count"] == 1
    assert audit["summary"]["suppressed_indicator_count"] == 3
    assert audit["summary"]["error_indicator_count"] == 2
    assert audit["summary"]["public_error_indicator_count"] == 2
    assert audit["summary"]["non_public_error_indicator_count"] == 0
    assert audit["summary"]["public_warning_indicator_count"] == 0
    assert audit["summary"]["non_public_warning_indicator_count"] == 0
    assert audit["summary"]["public_issue_code_counts"] == {
        "no_observations": 1,
        "thin_seasonal_history": 1,
    }
    assert audit["summary"]["non_public_issue_code_counts"] == {
        "non_public_indicator": 1,
    }

    good_row = next(item for item in audit["indicators"] if item["code"] == "good_weekly")
    thin_row = next(item for item in audit["indicators"] if item["code"] == "thin_weekly")
    empty_row = next(item for item in audit["indicators"] if item["code"] == "empty_weekly")
    hidden_row = next(item for item in audit["indicators"] if item["code"] == "hidden_weekly")

    assert good_row["public_display_suitable"] is True
    assert good_row["public_display_status"] == "eligible"
    assert good_row["earliest_period_end_at"] == "2026-03-12T12:00:00+00:00"
    assert good_row["latest_period_end_at"] == "2026-03-26T12:00:00+00:00"
    assert good_row["seasonal_point_count"] == 26

    assert thin_row["public_display_suitable"] is False
    assert thin_row["public_display_status"] == "suppressed"
    assert any(issue["code"] == "thin_seasonal_history" for issue in thin_row["issues"])

    assert empty_row["public_display_suitable"] is False
    assert any(issue["code"] == "no_observations" for issue in empty_row["issues"])

    assert hidden_row["public_display_suitable"] is False
    assert hidden_row["public_display_status"] == "suppressed"
    assert hidden_row["issues"][0]["code"] == "non_public_indicator"
    assert hidden_row["issues"][0]["scope"] == "non_public"

    markdown = inventory_coverage_audit_markdown(audit)
    assert "# InventoryWatch Coverage Audit" in markdown
    assert "## Public Product" in markdown
    assert "## Non-Public / Suppressed" in markdown
    assert "good_weekly" in markdown
    assert "| Code | Source | Obs |" in markdown


def test_monthly_calendar_schedule_is_not_flagged_stale_before_due_but_warns_after_missed_release() -> None:
    indicator = make_indicator(
        "monthly_wasde",
        frequency="monthly",
        metadata={
            "release_schedule": {
                "type": "monthly_calendar",
                "time_local": "12:00",
                "timezone": "America/New_York",
                "dates": ["2026-03-10", "2026-04-09"],
            }
        },
    )
    latest = make_observation(
        frequency="monthly",
        period_end_at=datetime(2026, 3, 10, tzinfo=UTC),
        release_date=datetime(2026, 3, 10, tzinfo=UTC),
        value=200.0,
    )
    repository = FakeRepository([indicator], {indicator.id: [latest]}, {})

    before_due = build_inventory_coverage_audit(
        repository,
        now=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
    )
    before_row = before_due["indicators"][0]

    assert before_due["summary"]["public_warning_indicator_count"] == 0
    assert before_due["summary"]["public_issue_code_counts"] == {}
    assert before_row["freshness_state"] == "fresh"
    assert before_row["public_display_status"] == "eligible"
    assert before_row["last_expected_release_at"] == "2026-03-10T16:00:00+00:00"
    assert before_row["next_expected_release_at"] == "2026-04-09T16:00:00+00:00"
    assert all(issue["code"] != "stale_release" for issue in before_row["issues"])

    after_missed_release = build_inventory_coverage_audit(
        repository,
        now=datetime(2026, 4, 11, 18, 0, tzinfo=UTC),
    )
    after_row = after_missed_release["indicators"][0]

    assert after_missed_release["summary"]["public_warning_indicator_count"] == 1
    assert after_missed_release["summary"]["public_issue_code_counts"] == {"stale_release": 1}
    assert after_row["freshness_state"] == "stale"
    assert after_row["public_display_status"] == "stale"
    assert any(issue["code"] == "stale_release" for issue in after_row["issues"])
    assert any("2026-04-09" in issue["message"] for issue in after_row["issues"] if issue["code"] == "stale_release")


def test_non_public_warnings_are_counted_separately_from_public_product_health() -> None:
    public_indicator = make_indicator("good_daily", source_slug="etf", frequency="daily")
    internal_indicator = make_indicator("hidden_daily", source_slug="lme", frequency="daily", visibility_tier="internal")
    repository = FakeRepository(
        [public_indicator, internal_indicator],
        {
            public_indicator.id: [
                make_observation(period_days_ago=1, release_days_ago=1, value=10.0, frequency="daily"),
            ],
            internal_indicator.id: [
                make_observation(period_days_ago=487, release_days_ago=487, value=10.0, frequency="daily"),
            ],
        },
        {},
    )
    audit = build_inventory_coverage_audit(repository, now=NOW)
    internal_row = next(item for item in audit["indicators"] if item["code"] == "hidden_daily")

    assert audit["summary"]["warning_indicator_count"] == 1
    assert audit["summary"]["public_warning_indicator_count"] == 0
    assert audit["summary"]["non_public_warning_indicator_count"] == 1
    assert audit["summary"]["public_issue_code_counts"] == {}
    assert audit["summary"]["non_public_issue_code_counts"] == {
        "non_public_indicator": 1,
        "old_period": 1,
        "stale_release": 1,
    }
    assert internal_row["freshness_state"] == "historical"
    assert internal_row["public_display_status"] == "suppressed"
    assert [issue["scope"] for issue in internal_row["issues"]] == ["non_public", "non_public", "non_public"]
    assert inventory_coverage_blocking_issues(audit) == []
    assert publish_coverage_blocking_message(audit) is None
    assert audit_coverage_blocking_message(audit) is None


def test_non_public_errors_do_not_block_public_strict_mode(tmp_path: Path) -> None:
    indicator = make_indicator("empty_internal", is_seasonal=True, visibility_tier="internal")
    repository = FakeRepository([indicator], {indicator.id: []}, {})
    audit = build_inventory_coverage_audit(repository, now=NOW)

    assert audit["summary"]["public_error_indicator_count"] == 0
    assert audit["summary"]["non_public_error_indicator_count"] == 1
    assert inventory_coverage_blocking_issues(audit) == []
    assert publish_coverage_blocking_message(audit) is None
    assert audit_coverage_blocking_message(audit) is None

    output_path = tmp_path / "inventorywatch.db"
    output_path.write_bytes(b"sqlite")
    summary = {
        "database_path": str(output_path),
        "indicator_count": 1,
        "observation_count": 1,
        "seasonal_profile_count": 0,
    }

    with pytest.raises(RuntimeError, match="Published store path mismatch"):
        validate_published_store(tmp_path / "other.db", summary)


def test_coverage_blocking_message_is_consistent_between_publish_and_audit_scripts() -> None:
    indicator = make_indicator("empty_weekly", is_seasonal=True)
    repository = FakeRepository([indicator], {indicator.id: []}, {})
    audit = build_inventory_coverage_audit(repository, now=NOW)

    expected = "InventoryWatch coverage audit found blocking issues for: empty_weekly"

    assert publish_coverage_blocking_message(audit) == expected
    assert audit_coverage_blocking_message(audit) == expected
