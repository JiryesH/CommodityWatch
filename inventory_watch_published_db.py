from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from inventory_watch_local_api import (
    InventoryIndicatorDefinition,
    InventoryObservation,
    LocalInventoryRepository,
    normalize_source_series_key,
    parse_optional_timestamp,
)


SCHEMA_VERSION = 1


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
                    for seasonal_point in repository._seasonal_range(indicator, profile_name):
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
        "seasonal_profile_count": len(repository._seasonal_cache),
    }
