from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inventory_watch_local_api import (
    InventoryIndicatorDefinition,
    InventoryObservation,
    LocalInventoryRepository,
    normalize_source_series_key,
    parse_optional_timestamp,
    source_label_for_indicator,
)


SCHEMA_VERSION = 1
AUDIT_STALE_AFTER_DAYS = 14
AUDIT_DEAD_AFTER_DAYS = 90


def _expanded_profile_names(profile_name: str | None) -> set[str]:
    resolved = str(profile_name or "inventorywatch_5y").strip() or "inventorywatch_5y"
    names = {resolved}
    if "_ex_2020" not in resolved.lower():
        names.add(f"{resolved}_ex_2020")
    return names


def _remove_existing_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _required_seasonal_points(indicator: InventoryIndicatorDefinition) -> int:
    if not indicator.is_seasonal:
        return 0
    frequency = str(indicator.frequency or "").lower()
    if frequency == "daily":
        return 180
    if frequency == "weekly":
        return 26
    if frequency == "quarterly":
        return 8
    if frequency == "monthly":
        return 12
    return 12


@dataclass(frozen=True)
class InventoryCoverageThresholds:
    min_seasonal_points: int = 12
    stale_after_days: int = AUDIT_STALE_AFTER_DAYS
    dead_after_days: int = AUDIT_DEAD_AFTER_DAYS


def _inventory_coverage_issue(severity: str, code: str, message: str) -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
    }


def _indicator_release_age_days(now: datetime, latest_release_at: datetime | None) -> int | None:
    if latest_release_at is None:
        return None
    return max(0, int((now - latest_release_at).total_seconds() // 86400))


def _indicator_period_age_days(now: datetime, latest_period_end_at: datetime | None) -> int | None:
    if latest_period_end_at is None:
        return None
    return max(0, int((now - latest_period_end_at).total_seconds() // 86400))


def build_inventory_coverage_audit(
    repository: LocalInventoryRepository,
    *,
    now: datetime | None = None,
    thresholds: InventoryCoverageThresholds | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or InventoryCoverageThresholds()
    now = now or _utcnow()

    indicator_rows: list[dict[str, Any]] = []
    summary = {
        "indicator_count": 0,
        "public_indicator_count": 0,
        "indicators_with_observations": 0,
        "total_observations": 0,
        "indicators_with_usable_seasonal_ranges": 0,
        "public_display_suitable_count": 0,
        "suppressed_indicator_count": 0,
        "warning_indicator_count": 0,
        "error_indicator_count": 0,
    }
    issue_counts: dict[str, int] = {"info": 0, "warning": 0, "error": 0}
    issue_code_counts: dict[str, int] = {}

    for indicator in sorted(repository._indicators_by_id.values(), key=lambda item: item.code):
        observations = list(repository._observations_by_id.get(indicator.id, []))
        observation_count = len(observations)
        earliest_period_end_at = observations[0].period_end_at if observations else None
        latest_period_end_at = observations[-1].period_end_at if observations else None
        latest_release_at = observations[-1].release_date if observations else None
        release_age_days = _indicator_release_age_days(now, latest_release_at)
        period_age_days = _indicator_period_age_days(now, latest_period_end_at)
        seasonal_points = list(repository._seasonal_range(indicator, indicator.seasonal_profile)) if indicator.is_seasonal else []
        seasonal_point_count = len(seasonal_points)
        required_seasonal_points = max(_required_seasonal_points(indicator), thresholds.min_seasonal_points)
        issues: list[dict[str, str]] = []

        if indicator.visibility_tier != "public":
            issues.append(
                _inventory_coverage_issue(
                    "info",
                    "non_public_indicator",
                    "Indicator is not public and should remain hidden from public surfaces.",
                )
            )
        if observation_count == 0:
            issues.append(
                _inventory_coverage_issue(
                    "error",
                    "no_observations",
                    "No observations are published for this indicator.",
                )
            )
        if indicator.is_seasonal and seasonal_point_count < required_seasonal_points:
            issues.append(
                _inventory_coverage_issue(
                    "error",
                    "thin_seasonal_history",
                    (
                        f"Seasonal history is too thin for public display: "
                        f"{seasonal_point_count} points, need at least {required_seasonal_points}."
                    ),
                )
            )
        if observation_count > 0 and latest_release_at is None:
            issues.append(
                _inventory_coverage_issue(
                    "error",
                    "missing_release_date",
                    "The latest observation does not include a release date.",
                )
            )
        if release_age_days is not None and release_age_days > thresholds.stale_after_days:
            issues.append(
                _inventory_coverage_issue(
                    "warning",
                    "stale_release",
                    f"Latest release is {release_age_days} days old.",
                )
            )
        if period_age_days is not None and period_age_days > thresholds.dead_after_days:
            issues.append(
                _inventory_coverage_issue(
                    "warning",
                    "old_period",
                    f"Latest period end is {period_age_days} days old.",
                )
            )

        public_display_suitable = indicator.visibility_tier == "public" and not any(
            issue["severity"] == "error" for issue in issues
        )
        if observation_count == 0:
            public_display_status = "empty"
        elif indicator.visibility_tier != "public":
            public_display_status = "suppressed"
        elif any(issue["severity"] == "error" for issue in issues):
            public_display_status = "suppressed"
        elif release_age_days is not None and release_age_days > thresholds.stale_after_days:
            public_display_status = "stale"
        elif period_age_days is not None and period_age_days > thresholds.dead_after_days:
            public_display_status = "historical"
        else:
            public_display_status = "eligible"

        summary["indicator_count"] += 1
        if indicator.visibility_tier == "public":
            summary["public_indicator_count"] += 1
        if observation_count > 0:
            summary["indicators_with_observations"] += 1
            summary["total_observations"] += observation_count
        if indicator.is_seasonal and seasonal_point_count >= required_seasonal_points:
            summary["indicators_with_usable_seasonal_ranges"] += 1
        if public_display_suitable:
            summary["public_display_suitable_count"] += 1
        else:
            summary["suppressed_indicator_count"] += 1

        for issue in issues:
            issue_counts[issue["severity"]] = issue_counts.get(issue["severity"], 0) + 1
            issue_code_counts[issue["code"]] = issue_code_counts.get(issue["code"], 0) + 1

        summary["warning_indicator_count"] += 1 if any(issue["severity"] == "warning" for issue in issues) else 0
        summary["error_indicator_count"] += 1 if any(issue["severity"] == "error" for issue in issues) else 0

        indicator_rows.append(
            {
                "indicator_id": indicator.id,
                "code": indicator.code,
                "name": indicator.name,
                "source_slug": indicator.source_slug,
                "source_label": source_label_for_indicator(indicator),
                "visibility_tier": indicator.visibility_tier,
                "observation_count": observation_count,
                "earliest_period_end_at": _format_timestamp(earliest_period_end_at),
                "latest_period_end_at": _format_timestamp(latest_period_end_at),
                "latest_release_at": _format_timestamp(latest_release_at),
                "release_age_days": release_age_days,
                "period_age_days": period_age_days,
                "seasonal_point_count": seasonal_point_count,
                "seasonal_profile_count": len(
                    {
                        profile_name
                        for (indicator_id, profile_name) in repository._seasonal_cache
                        if indicator_id == indicator.id
                    }
                ),
                "required_seasonal_points": required_seasonal_points,
                "freshness_state": (
                    "fresh"
                    if release_age_days is not None and release_age_days <= thresholds.stale_after_days
                    else "stale"
                    if release_age_days is not None
                    else "unknown"
                ),
                "stale": release_age_days is None or release_age_days > thresholds.stale_after_days,
                "public_display_suitable": public_display_suitable,
                "public_display_status": public_display_status,
                "issues": issues,
            }
        )

    return {
        "generated_at": now.isoformat(),
        "thresholds": {
            "min_seasonal_points": thresholds.min_seasonal_points,
            "stale_after_days": thresholds.stale_after_days,
            "dead_after_days": thresholds.dead_after_days,
        },
        "summary": {
            **summary,
            "issue_counts": issue_counts,
            "issue_code_counts": issue_code_counts,
        },
        "indicators": indicator_rows,
    }


def inventory_coverage_blocking_issues(audit: dict[str, Any]) -> list[dict[str, Any]]:
    blocking: list[dict[str, Any]] = []
    for item in audit["indicators"]:
        errors = [issue for issue in item.get("issues", []) if issue.get("severity") == "error"]
        if errors:
            blocking.append(
                {
                    "indicator_id": item["indicator_id"],
                    "code": item["code"],
                    "name": item["name"],
                    "issues": errors,
                }
            )
    return blocking


def inventory_coverage_audit_markdown(audit: dict[str, Any]) -> str:
    summary = audit["summary"]
    lines = [
        "# InventoryWatch Coverage Audit",
        "",
        f"Generated at: {audit['generated_at']}",
        "",
        "## Summary",
        "",
        f"- Indicators: {summary['indicator_count']}",
        f"- Public indicators: {summary['public_indicator_count']}",
        f"- Indicators with observations: {summary['indicators_with_observations']}",
        f"- Total observations: {summary['total_observations']}",
        f"- Indicators with usable seasonal ranges: {summary['indicators_with_usable_seasonal_ranges']}",
        f"- Public-display suitable indicators: {summary['public_display_suitable_count']}",
        f"- Suppressed indicators: {summary['suppressed_indicator_count']}",
        f"- Error indicators: {summary['error_indicator_count']}",
        f"- Warning indicators: {summary['warning_indicator_count']}",
        "",
        "## Indicators",
        "",
        "| Code | Source | Obs | Earliest period | Latest period | Seasonal points | Freshness | Status | Public |",
        "| --- | --- | ---: | --- | --- | ---: | --- | --- | --- |",
    ]
    for item in audit["indicators"]:
        lines.append(
            "| {code} | {source} | {obs} | {earliest} | {latest} | {seasonal} | {freshness} | {status} | {public} |".format(
                code=item["code"],
                source=item["source_label"],
                obs=item["observation_count"],
                earliest=item["earliest_period_end_at"] or "-",
                latest=item["latest_period_end_at"] or "-",
                seasonal=item["seasonal_point_count"],
                freshness=item["freshness_state"],
                status=item["public_display_status"],
                public="yes" if item["public_display_suitable"] else "no",
            )
        )
    return "\n".join(lines)


class PublishedInventoryRepository(LocalInventoryRepository):
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.data_root = database_path.parent
        self._indicators_by_id: dict[str, InventoryIndicatorDefinition] = {}
        self._indicators_by_code: dict[str, InventoryIndicatorDefinition] = {}
        self._indicators_by_source_key: dict[tuple[str, str], InventoryIndicatorDefinition] = {}
        self._observations_by_id: dict[str, list[InventoryObservation]] = {}
        self._seasonal_cache: dict[tuple[str, str], list[dict[str, float | int | None]]] = {}
        self._quarantined_observations = []
        self._load_from_db()

    def _load_from_db(self) -> None:
        if not self.database_path.exists():
            raise FileNotFoundError(f"InventoryWatch published database not found: {self.database_path}")

        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            indicator_rows = connection.execute(
                """
                SELECT
                    id,
                    code,
                    name,
                    description,
                    measure_family,
                    frequency,
                    commodity_code,
                    geography_code,
                    source_slug,
                    source_series_key,
                    native_unit_code,
                    canonical_unit_code,
                    default_observation_kind,
                    seasonal_profile,
                    is_seasonal,
                    is_derived,
                    visibility_tier,
                    metadata_json
                FROM published_inventory_indicators
                ORDER BY code
                """
            ).fetchall()

            for row in indicator_rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                indicator = InventoryIndicatorDefinition(
                    id=str(row["id"]),
                    code=str(row["code"]),
                    name=str(row["name"]),
                    description=row["description"],
                    measure_family=str(row["measure_family"]),
                    frequency=str(row["frequency"]),
                    commodity_code=row["commodity_code"],
                    geography_code=row["geography_code"],
                    source_slug=str(row["source_slug"]),
                    source_series_key=normalize_source_series_key(row["source_series_key"]),
                    native_unit_code=row["native_unit_code"],
                    canonical_unit_code=row["canonical_unit_code"],
                    default_observation_kind=str(row["default_observation_kind"]),
                    seasonal_profile=row["seasonal_profile"],
                    is_seasonal=bool(row["is_seasonal"]),
                    is_derived=bool(row["is_derived"]),
                    visibility_tier=str(row["visibility_tier"]),
                    metadata=metadata,
                )
                self._indicators_by_id[indicator.id] = indicator
                self._indicators_by_code[indicator.code] = indicator
                self._indicators_by_source_key[(indicator.source_slug, indicator.source_series_key)] = indicator

            observation_rows = connection.execute(
                """
                SELECT
                    indicator_id,
                    period_start_at,
                    period_end_at,
                    release_date,
                    vintage_at,
                    value,
                    unit,
                    observation_kind,
                    revision_sequence
                FROM published_inventory_observations
                ORDER BY indicator_id, period_end_at
                """
            ).fetchall()

            observations_by_id: dict[str, list[InventoryObservation]] = {}
            for row in observation_rows:
                observation = InventoryObservation(
                    period_start_at=parse_optional_timestamp(row["period_start_at"]) or parse_optional_timestamp(row["period_end_at"]),
                    period_end_at=parse_optional_timestamp(row["period_end_at"]) or parse_optional_timestamp(row["period_start_at"]),
                    release_date=parse_optional_timestamp(row["release_date"]),
                    vintage_at=parse_optional_timestamp(row["vintage_at"]) or parse_optional_timestamp(row["period_end_at"]),
                    value=float(row["value"]),
                    unit=str(row["unit"] or ""),
                    observation_kind=str(row["observation_kind"]),
                    revision_sequence=int(row["revision_sequence"]),
                )
                if observation.period_start_at is None or observation.period_end_at is None or observation.vintage_at is None:
                    continue
                observations_by_id.setdefault(str(row["indicator_id"]), []).append(observation)
            self._observations_by_id = observations_by_id

            seasonal_rows = connection.execute(
                """
                SELECT
                    indicator_id,
                    profile_name,
                    period_index,
                    p10,
                    p25,
                    p50,
                    p75,
                    p90,
                    mean_value,
                    stddev_value
                FROM published_inventory_seasonal_ranges
                ORDER BY indicator_id, profile_name, period_index
                """
            ).fetchall()

            for row in seasonal_rows:
                cache_key = (str(row["indicator_id"]), str(row["profile_name"]))
                self._seasonal_cache.setdefault(cache_key, []).append(
                    {
                        "period_index": int(row["period_index"]),
                        "p10": float(row["p10"]) if row["p10"] is not None else None,
                        "p25": float(row["p25"]) if row["p25"] is not None else None,
                        "p50": float(row["p50"]) if row["p50"] is not None else None,
                        "p75": float(row["p75"]) if row["p75"] is not None else None,
                        "p90": float(row["p90"]) if row["p90"] is not None else None,
                        "mean": float(row["mean_value"]) if row["mean_value"] is not None else None,
                        "stddev": float(row["stddev_value"]) if row["stddev_value"] is not None else None,
                    }
                )
        except sqlite3.Error as exc:
            raise RuntimeError(f"Unable to load InventoryWatch published database: {self.database_path}") from exc
        finally:
            connection.close()


def publish_inventory_store(data_root: Path, output_path: Path) -> dict[str, Any]:
    repository = LocalInventoryRepository(data_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        connection = sqlite3.connect(temp_path)
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = DELETE;
                DROP TABLE IF EXISTS published_inventory_meta;
                DROP TABLE IF EXISTS published_inventory_indicators;
                DROP TABLE IF EXISTS published_inventory_observations;
                DROP TABLE IF EXISTS published_inventory_seasonal_ranges;

                CREATE TABLE published_inventory_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE published_inventory_indicators (
                    id TEXT PRIMARY KEY,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    measure_family TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    commodity_code TEXT,
                    geography_code TEXT,
                    source_slug TEXT NOT NULL,
                    source_series_key TEXT NOT NULL,
                    native_unit_code TEXT,
                    canonical_unit_code TEXT,
                    default_observation_kind TEXT NOT NULL,
                    seasonal_profile TEXT,
                    is_seasonal INTEGER NOT NULL,
                    is_derived INTEGER NOT NULL,
                    visibility_tier TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE published_inventory_observations (
                    indicator_id TEXT NOT NULL,
                    period_start_at TEXT NOT NULL,
                    period_end_at TEXT NOT NULL,
                    release_date TEXT,
                    vintage_at TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    observation_kind TEXT NOT NULL,
                    revision_sequence INTEGER NOT NULL,
                    PRIMARY KEY (indicator_id, period_end_at)
                );

                CREATE TABLE published_inventory_seasonal_ranges (
                    indicator_id TEXT NOT NULL,
                    profile_name TEXT NOT NULL,
                    period_index INTEGER NOT NULL,
                    p10 REAL,
                    p25 REAL,
                    p50 REAL,
                    p75 REAL,
                    p90 REAL,
                    mean_value REAL,
                    stddev_value REAL,
                    PRIMARY KEY (indicator_id, profile_name, period_index)
                );
                """
            )

            connection.executemany(
                """
                INSERT INTO published_inventory_meta (key, value) VALUES (?, ?)
                """,
                [
                    ("schema_version", str(SCHEMA_VERSION)),
                    ("published_at", repository.snapshot_payload(limit=1, include_sparklines=False)["generated_at"]),
                    ("source_data_root", str(data_root)),
                ],
            )

            connection.executemany(
                """
                INSERT INTO published_inventory_indicators (
                    id,
                    code,
                    name,
                    description,
                    measure_family,
                    frequency,
                    commodity_code,
                    geography_code,
                    source_slug,
                    source_series_key,
                    native_unit_code,
                    canonical_unit_code,
                    default_observation_kind,
                    seasonal_profile,
                    is_seasonal,
                    is_derived,
                    visibility_tier,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        indicator.id,
                        indicator.code,
                        indicator.name,
                        indicator.description,
                        indicator.measure_family,
                        indicator.frequency,
                        indicator.commodity_code,
                        indicator.geography_code,
                        indicator.source_slug,
                        indicator.source_series_key,
                        indicator.native_unit_code,
                        indicator.canonical_unit_code,
                        indicator.default_observation_kind,
                        indicator.seasonal_profile,
                        1 if indicator.is_seasonal else 0,
                        1 if indicator.is_derived else 0,
                        indicator.visibility_tier,
                        json.dumps(indicator.metadata, sort_keys=True),
                    )
                    for indicator in sorted(repository._indicators_by_id.values(), key=lambda item: item.code)
                ],
            )

            observation_rows = []
            seasonal_rows = []
            seasonal_profile_count = 0
            for indicator in sorted(repository._indicators_by_id.values(), key=lambda item: item.code):
                for point in repository._observations_by_id.get(indicator.id, []):
                    observation_rows.append(
                        (
                            indicator.id,
                            point.period_start_at.isoformat(),
                            point.period_end_at.isoformat(),
                            point.release_date.isoformat() if point.release_date else None,
                            point.vintage_at.isoformat(),
                            point.value,
                            point.unit,
                            point.observation_kind,
                            point.revision_sequence,
                        )
                    )

                for profile_name in sorted(_expanded_profile_names(indicator.seasonal_profile)):
                    seasonal_points = repository._seasonal_range(indicator, profile_name)
                    if not seasonal_points:
                        continue
                    seasonal_profile_count += 1
                    for seasonal_point in seasonal_points:
                        seasonal_rows.append(
                            (
                                indicator.id,
                                profile_name,
                                int(seasonal_point["period_index"]),
                                seasonal_point["p10"],
                                seasonal_point["p25"],
                                seasonal_point["p50"],
                                seasonal_point["p75"],
                                seasonal_point["p90"],
                                seasonal_point["mean"],
                                seasonal_point["stddev"],
                            )
                        )

            connection.executemany(
                """
                INSERT INTO published_inventory_observations (
                    indicator_id,
                    period_start_at,
                    period_end_at,
                    release_date,
                    vintage_at,
                    value,
                    unit,
                    observation_kind,
                    revision_sequence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                observation_rows,
            )

            connection.executemany(
                """
                INSERT INTO published_inventory_seasonal_ranges (
                    indicator_id,
                    profile_name,
                    period_index,
                    p10,
                    p25,
                    p50,
                    p75,
                    p90,
                    mean_value,
                    stddev_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                seasonal_rows,
            )
            connection.commit()
        finally:
            connection.close()

        _remove_existing_file(output_path)
        os.replace(temp_path, output_path)
    except Exception:
        _remove_existing_file(temp_path)
        raise

    return {
        "database_path": str(output_path),
        "indicator_count": len(repository._indicators_by_id),
        "observation_count": sum(len(points) for points in repository._observations_by_id.values()),
        "seasonal_profile_count": seasonal_profile_count,
    }
