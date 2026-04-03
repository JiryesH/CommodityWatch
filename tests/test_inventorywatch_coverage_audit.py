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
) -> InventoryIndicatorDefinition:
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
        metadata={"source_label": source_slug.upper()},
    )


def make_observation(
    *,
    period_days_ago: int,
    release_days_ago: int | None,
    value: float,
    frequency: str = "weekly",
) -> InventoryObservation:
    period_end_at = NOW - timedelta(days=period_days_ago)
    release_date = None if release_days_ago is None else NOW - timedelta(days=release_days_ago)
    period_start_at = period_end_at - (timedelta(days=6) if frequency == "weekly" else timedelta(days=0))
    vintage_at = release_date or NOW
    return InventoryObservation(
        period_start_at=period_start_at,
        period_end_at=period_end_at,
        release_date=release_date,
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

    markdown = inventory_coverage_audit_markdown(audit)
    assert "# InventoryWatch Coverage Audit" in markdown
    assert "good_weekly" in markdown
    assert "| Code | Source | Obs |" in markdown


def test_blocking_issues_and_validation_failures_are_operator_visible(tmp_path: Path) -> None:
    indicator = make_indicator("empty_weekly", is_seasonal=True)
    repository = FakeRepository([indicator], {indicator.id: []}, {})
    audit = build_inventory_coverage_audit(repository, now=NOW)

    blocking = inventory_coverage_blocking_issues(audit)
    assert [item["code"] for item in blocking] == ["empty_weekly"]

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
