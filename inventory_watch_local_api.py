from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

from inventory_watch_signals import classify_inventory_signal, compute_inventory_alerts, revision_flag


UTC = timezone.utc
LOCAL_MODULE_CODE = "inventorywatch"
REVISION_TOLERANCE_RATIO = 0.001
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class InventoryIndicatorDefinition:
    id: str
    code: str
    name: str
    description: str | None
    measure_family: str
    frequency: str
    commodity_code: str | None
    geography_code: str | None
    source_slug: str
    source_series_key: str
    native_unit_code: str | None
    canonical_unit_code: str | None
    default_observation_kind: str
    seasonal_profile: str | None
    is_seasonal: bool
    is_derived: bool
    visibility_tier: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InventoryObservation:
    period_start_at: datetime
    period_end_at: datetime
    release_date: datetime | None
    vintage_at: datetime
    value: float
    unit: str
    observation_kind: str
    revision_sequence: int = 1


@dataclass(frozen=True)
class QuarantinedObservation:
    indicator_id: str
    code: str
    period_end_at: datetime
    value: float
    lower_bound: float | None
    upper_bound: float | None
    source_slug: str
    artifact_path: str


def utcnow() -> datetime:
    return datetime.now(UTC)


def release_aged_after(frequency: str | None) -> timedelta:
    normalized = str(frequency or "").lower()
    if normalized == "daily":
        return timedelta(days=7)
    if normalized == "monthly":
        return timedelta(days=75)
    if normalized == "quarterly":
        return timedelta(days=200)
    if normalized == "annual":
        return timedelta(days=550)
    return timedelta(days=21)


def release_is_aged(frequency: str | None, release_date: datetime | None, *, now: datetime | None = None) -> bool:
    if release_date is None:
        return True
    reference_now = now or utcnow()
    return (reference_now - release_date) > release_aged_after(frequency)


def required_public_seasonal_points(indicator: InventoryIndicatorDefinition) -> int:
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


def public_seasonality_allowed(
    indicator: InventoryIndicatorDefinition,
    observation_count: int,
    seasonal_point_count: int | None = None,
    seasonal_samples: int | None = None,
) -> bool:
    if not indicator.is_seasonal or observation_count <= 0:
        return False
    if seasonal_point_count is not None and seasonal_point_count < required_public_seasonal_points(indicator):
        return False
    if seasonal_samples is None:
        return observation_count >= 3
    return seasonal_samples >= 3


def normalize_source_series_key(raw_value: str | None) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""

    parts = value.split(".")
    if len(parts) >= 3:
        value = ".".join(parts[1:-1])
    elif len(parts) == 2:
        value = parts[1]

    for suffix in (".W", ".D", ".M"):
        if value.endswith(suffix):
            value = value[: -len(suffix)]

    return value.strip().upper()


def parse_numeric(raw_value: Any) -> float | None:
    if raw_value in (None, "", "NA", "null"):
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    return float(str(raw_value).replace(",", ""))


def parse_period_end(raw_value: str, frequency: str) -> datetime:
    if frequency in {"daily", "weekly"}:
        return datetime.fromisoformat(raw_value).replace(tzinfo=UTC)
    if frequency == "quarterly":
        normalized = str(raw_value).strip()
        if len(normalized) == 7 and normalized[4] == "-":
            month = int(normalized[5:7])
            quarter_end_month = ((month - 1) // 3 + 1) * 3
            quarter_end = datetime.fromisoformat(f"{normalized[:4]}-{quarter_end_month:02d}-01").replace(tzinfo=UTC)
            if quarter_end_month == 12:
                return quarter_end.replace(day=31)
            return (quarter_end.replace(month=quarter_end_month + 1) - timedelta(days=1)).replace(tzinfo=UTC)
        return datetime.fromisoformat(normalized).replace(tzinfo=UTC)
    if frequency == "monthly":
        normalized = str(raw_value).strip()
        if len(normalized) == 7 and normalized[4] == "-":
            return datetime.fromisoformat(f"{normalized}-01").replace(tzinfo=UTC)
        return datetime.fromisoformat(normalized).replace(tzinfo=UTC)
    raise ValueError(f"Unsupported InventoryWatch frequency: {frequency}")


def period_start_for(period_end_at: datetime, frequency: str) -> datetime:
    if frequency == "weekly":
        return period_end_at - timedelta(days=6)
    if frequency == "monthly":
        return period_end_at.replace(day=1)
    if frequency == "quarterly":
        quarter_start_month = ((period_end_at.month - 1) // 3) * 3 + 1
        return period_end_at.replace(month=quarter_start_month, day=1)
    return period_end_at


def parse_optional_timestamp(raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    if " " in normalized and "T" not in normalized:
        normalized = normalized.replace(" ", "T")

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def artifact_timestamp(path: Path) -> datetime:
    try:
        year = int(path.parts[-4])
        month = int(path.parts[-3])
        day = int(path.parts[-2])
        time_token = path.stem.rsplit("-", 1)[-1]
        hour = int(time_token[0:2]) if len(time_token) >= 2 else 0
        minute = int(time_token[2:4]) if len(time_token) >= 4 else 0
        second = int(time_token[4:6]) if len(time_token) >= 6 else 0
        microsecond = int(time_token[6:].ljust(6, "0")[:6]) if len(time_token) > 6 else 0
        return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)
    except (IndexError, ValueError):
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def week_of_year(value: datetime) -> int:
    return min(value.isocalendar().week, 52)


def day_of_year(value: datetime) -> int:
    return value.timetuple().tm_yday


def merge_indicator_metadata(raw_indicator: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(raw_indicator.get("metadata") or {})
    for key in (
        "source_url",
        "source_label",
        "period_type",
        "color_convention",
        "sanity_bounds",
        "release_schedule",
        "days_of_supply",
        "alerts_enabled",
        "marketing_year_start_month",
        "days_of_supply_indicator_id",
        "paired_demand_indicator_id",
    ):
        if raw_indicator.get(key) is not None and key not in metadata:
            metadata[key] = raw_indicator.get(key)
    return metadata


def indicator_period_type(indicator: InventoryIndicatorDefinition) -> str:
    raw_value = str(indicator.metadata.get("period_type") or "").strip().lower()
    if raw_value in {"weekly", "monthly", "daily", "marketing_month", "quarterly"}:
        return raw_value
    if indicator.frequency == "daily":
        return "daily"
    if indicator.frequency == "monthly":
        return "monthly"
    if indicator.frequency == "quarterly":
        return "quarterly"
    return "weekly"


def marketing_year_start_month(indicator: InventoryIndicatorDefinition) -> int:
    raw_value = indicator.metadata.get("marketing_year_start_month")
    try:
        month = int(raw_value)
    except (TypeError, ValueError):
        month = 1
    return month if 1 <= month <= 12 else 1


def period_index_for_indicator(indicator: InventoryIndicatorDefinition, period_end_at: datetime) -> int:
    period_type = indicator_period_type(indicator)
    if period_type == "daily":
        return day_of_year(period_end_at)
    if period_type == "monthly":
        return period_end_at.month
    if period_type == "quarterly":
        return ((period_end_at.month - 1) // 3) + 1
    if period_type == "marketing_month":
        return ((period_end_at.month - marketing_year_start_month(indicator)) % 12) + 1
    return week_of_year(period_end_at)


def sanity_bounds_for_indicator(indicator: InventoryIndicatorDefinition) -> tuple[float | None, float | None]:
    bounds = indicator.metadata.get("sanity_bounds")
    if not isinstance(bounds, dict):
        return None, None
    lower = parse_numeric(bounds.get("min"))
    upper = parse_numeric(bounds.get("max"))
    return lower, upper


def release_schedule_for_indicator(indicator: InventoryIndicatorDefinition) -> dict[str, Any] | None:
    schedule = indicator.metadata.get("release_schedule")
    return schedule if isinstance(schedule, dict) else None


def alerts_enabled_for_indicator(indicator: InventoryIndicatorDefinition) -> bool:
    if "alerts_enabled" not in indicator.metadata:
        return True
    return bool(indicator.metadata.get("alerts_enabled"))


def days_of_supply_enabled_for_indicator(indicator: InventoryIndicatorDefinition) -> bool:
    return bool(indicator.metadata.get("days_of_supply"))


def color_convention_for_indicator(indicator: InventoryIndicatorDefinition) -> str:
    value = str(indicator.metadata.get("color_convention") or "").strip().lower()
    if value in {"standard", "inverse", "none"}:
        return value
    return "standard"


def period_index_for(frequency: str, period_end_at: datetime) -> int:
    if frequency == "daily":
        return day_of_year(period_end_at)
    if frequency == "monthly":
        return period_end_at.month
    if frequency == "quarterly":
        return ((period_end_at.month - 1) // 3) + 1
    return week_of_year(period_end_at)


def quantile(sorted_values: list[float], percentile: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    position = (len(sorted_values) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(sorted_values[lower])

    weight = position - lower
    return float(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * weight)


def population_stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None

    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return variance**0.5


def source_label_for_indicator(indicator: InventoryIndicatorDefinition) -> str:
    explicit = str(indicator.metadata.get("source_label") or "").strip()
    if explicit:
        return explicit
    if indicator.source_slug == "agsi":
        return "GIE / AGSI+"
    if indicator.source_slug == "eia":
        return "EIA"
    if indicator.source_slug == "lme":
        return "LME"
    if indicator.source_slug in {"usda", "usda_psd"}:
        return "USDA WASDE"
    if indicator.source_slug == "comex":
        return "COMEX / CME"
    if indicator.source_slug == "etf":
        return "ETF Holdings"
    if indicator.source_slug == "ice":
        return "ICE"
    return "CommodityWatch"


def source_url_for_indicator(indicator: InventoryIndicatorDefinition) -> str | None:
    explicit = str(indicator.metadata.get("source_url") or "").strip()
    if explicit:
        return explicit
    release_slug = str(indicator.metadata.get("release_slug") or "")
    if indicator.source_slug == "agsi":
        return "https://agsi.gie.eu/"
    if release_slug == "eia_wngs":
        return "https://www.eia.gov/naturalgas/storagedashboard/"
    if release_slug == "eia_wpsr":
        return "https://www.eia.gov/petroleum/supply/weekly/"
    return None


class LocalInventoryRepository:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self._indicators_by_id: dict[str, InventoryIndicatorDefinition] = {}
        self._indicators_by_code: dict[str, InventoryIndicatorDefinition] = {}
        self._indicators_by_source_key: dict[tuple[str, str], InventoryIndicatorDefinition] = {}
        self._observations_by_id: dict[str, list[InventoryObservation]] = {}
        self._seasonal_cache: dict[tuple[str, str], list[dict[str, float | int | None]]] = {}
        self._quarantined_observations: list[QuarantinedObservation] = []
        self._load()

    @property
    def has_data(self) -> bool:
        return any(self._observations_by_id.values())

    def _load(self) -> None:
        if yaml is None:
            raise RuntimeError("PyYAML is required for InventoryWatch local fallback support.")

        seed_path = self.data_root / "seed" / "indicators" / "inventorywatch.yml"
        if not seed_path.exists():
            raise FileNotFoundError(f"Inventory indicator seed not found: {seed_path}")

        raw_indicators = yaml.safe_load(seed_path.read_text(encoding="utf-8")) or []
        if not isinstance(raw_indicators, list):
            raise ValueError(f"Inventory indicator seed is invalid: {seed_path}")

        for raw_indicator in raw_indicators:
            metadata = merge_indicator_metadata(raw_indicator)
            indicator = InventoryIndicatorDefinition(
                id=str(raw_indicator.get("code") or ""),
                code=str(raw_indicator.get("code") or ""),
                name=str(raw_indicator.get("name") or raw_indicator.get("code") or ""),
                description=raw_indicator.get("description"),
                measure_family=str(raw_indicator.get("measure_family") or ""),
                frequency=str(raw_indicator.get("frequency") or "weekly"),
                commodity_code=raw_indicator.get("commodity_code"),
                geography_code=raw_indicator.get("geography_code"),
                source_slug=str(raw_indicator.get("source_slug") or ""),
                source_series_key=normalize_source_series_key(raw_indicator.get("source_series_key")),
                native_unit_code=raw_indicator.get("native_unit_code"),
                canonical_unit_code=raw_indicator.get("canonical_unit_code"),
                default_observation_kind=str(raw_indicator.get("default_observation_kind") or "actual"),
                seasonal_profile=raw_indicator.get("seasonal_profile"),
                is_seasonal=bool(raw_indicator.get("is_seasonal")),
                is_derived=bool(raw_indicator.get("is_derived")),
                visibility_tier=str(raw_indicator.get("visibility_tier") or "public"),
                metadata=metadata,
            )
            if not indicator.id:
                continue
            self._indicators_by_id[indicator.id] = indicator
            self._indicators_by_code[indicator.code] = indicator
            self._indicators_by_source_key[(indicator.source_slug, indicator.source_series_key)] = indicator

        observations_by_period: dict[str, dict[datetime, InventoryObservation]] = defaultdict(dict)
        self._load_eia_artifacts(observations_by_period)
        self._load_agsi_artifacts(observations_by_period)
        self._load_structured_value_artifacts(observations_by_period, source_slug="lme", value_key="total")
        self._load_structured_value_artifacts(observations_by_period, source_slug="usda", value_key="value")
        self._load_structured_value_artifacts(observations_by_period, source_slug="usda_psd", value_key="value")
        self._load_structured_value_artifacts(observations_by_period, source_slug="comex", value_key="value")
        self._load_structured_value_artifacts(observations_by_period, source_slug="etf", value_key="value")
        self._load_structured_value_artifacts(observations_by_period, source_slug="ice", value_key="value")
        self._observations_by_id = {
            indicator_id: sorted(points.values(), key=lambda point: point.period_end_at)
            for indicator_id, points in observations_by_period.items()
        }

    def _within_sanity_bounds(
        self,
        indicator: InventoryIndicatorDefinition,
        value: float,
        period_end_at: datetime,
        artifact_path: Path,
    ) -> bool:
        lower_bound, upper_bound = sanity_bounds_for_indicator(indicator)
        if lower_bound is not None and value < lower_bound:
            self._quarantined_observations.append(
                QuarantinedObservation(
                    indicator_id=indicator.id,
                    code=indicator.code,
                    period_end_at=period_end_at,
                    value=value,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    source_slug=indicator.source_slug,
                    artifact_path=str(artifact_path),
                )
            )
            LOGGER.warning("QUARANTINE %s %s below minimum bound (%s < %s)", indicator.code, period_end_at.date(), value, lower_bound)
            return False
        if upper_bound is not None and value > upper_bound:
            self._quarantined_observations.append(
                QuarantinedObservation(
                    indicator_id=indicator.id,
                    code=indicator.code,
                    period_end_at=period_end_at,
                    value=value,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    source_slug=indicator.source_slug,
                    artifact_path=str(artifact_path),
                )
            )
            LOGGER.warning("QUARANTINE %s %s above maximum bound (%s > %s)", indicator.code, period_end_at.date(), value, upper_bound)
            return False
        return True

    def _store_observation(
        self,
        observations_by_period: dict[str, dict[datetime, InventoryObservation]],
        indicator: InventoryIndicatorDefinition,
        observation: InventoryObservation,
        artifact_path: Path,
        *,
        revision_tolerance_ratio: float = REVISION_TOLERANCE_RATIO,
    ) -> None:
        if not self._within_sanity_bounds(indicator, observation.value, observation.period_end_at, artifact_path):
            return

        existing = observations_by_period[indicator.id].get(observation.period_end_at)
        if existing is None:
            observations_by_period[indicator.id][observation.period_end_at] = observation
            return

        if observation.vintage_at < existing.vintage_at:
            return

        change_ratio = 0.0
        if existing.value:
            change_ratio = abs(observation.value - existing.value) / abs(existing.value)
        elif observation.value != existing.value:
            change_ratio = 1.0

        revision_sequence_value = existing.revision_sequence
        if change_ratio > revision_tolerance_ratio:
            revision_sequence_value += 1

        observations_by_period[indicator.id][observation.period_end_at] = InventoryObservation(
            period_start_at=observation.period_start_at,
            period_end_at=observation.period_end_at,
            release_date=observation.release_date,
            vintage_at=observation.vintage_at,
            value=observation.value,
            unit=observation.unit,
            observation_kind=observation.observation_kind,
            revision_sequence=revision_sequence_value,
        )

    def _load_eia_artifacts(self, observations_by_period: dict[str, dict[datetime, InventoryObservation]]) -> None:
        artifact_root = self.data_root / "artifacts" / "eia"
        if not artifact_root.exists():
            return

        for artifact_path in sorted(artifact_root.rglob("*.json"), key=artifact_timestamp):
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            data = payload.get("response", {}).get("data", [])
            request_params = payload.get("request", {}).get("params", {}) or {}
            requested_series = request_params.get("facets[series][]")
            if isinstance(requested_series, list):
                raw_series_key = requested_series[0] if requested_series else None
            else:
                raw_series_key = requested_series
            if not raw_series_key and data:
                raw_series_key = data[0].get("series")

            indicator = self._indicators_by_source_key.get(("eia", normalize_source_series_key(raw_series_key)))
            if indicator is None:
                continue

            vintage_at = artifact_timestamp(artifact_path)
            for row in data:
                period_raw = row.get("period")
                value = parse_numeric(row.get("value"))
                if not period_raw or value is None:
                    continue

                period_end_at = parse_period_end(str(period_raw), indicator.frequency)
                observation = InventoryObservation(
                    period_start_at=period_start_for(period_end_at, indicator.frequency),
                    period_end_at=period_end_at,
                    release_date=vintage_at,
                    vintage_at=vintage_at,
                    value=value,
                    unit=indicator.canonical_unit_code or indicator.native_unit_code or "",
                    observation_kind=indicator.default_observation_kind,
                )
                self._store_observation(observations_by_period, indicator, observation, artifact_path)

    def _load_agsi_artifacts(self, observations_by_period: dict[str, dict[datetime, InventoryObservation]]) -> None:
        artifact_root = self.data_root / "artifacts" / "agsi"
        if not artifact_root.exists():
            return

        for artifact_path in sorted(artifact_root.rglob("*.json"), key=artifact_timestamp):
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            for row in payload.get("data", []):
                indicator = self._indicators_by_source_key.get(("agsi", normalize_source_series_key(row.get("code"))))
                if indicator is None:
                    continue

                value = parse_numeric(row.get("gasInStorage"))
                period_end_raw = row.get("gasDayEnd")
                if value is None or not period_end_raw:
                    continue

                period_end_at = parse_period_end(str(period_end_raw), indicator.frequency)
                vintage_at = parse_optional_timestamp(row.get("updatedAt")) or artifact_timestamp(artifact_path)
                period_start_at = parse_optional_timestamp(row.get("gasDayStart")) or period_start_for(
                    period_end_at, indicator.frequency
                )
                observation = InventoryObservation(
                    period_start_at=period_start_at,
                    period_end_at=period_end_at,
                    release_date=vintage_at,
                    vintage_at=vintage_at,
                    value=value,
                    unit=indicator.canonical_unit_code or indicator.native_unit_code or "",
                    observation_kind=indicator.default_observation_kind,
                )
                self._store_observation(observations_by_period, indicator, observation, artifact_path, revision_tolerance_ratio=1.0)

    def _load_structured_value_artifacts(
        self,
        observations_by_period: dict[str, dict[datetime, InventoryObservation]],
        *,
        source_slug: str,
        value_key: str,
    ) -> None:
        artifact_root = self.data_root / "artifacts" / source_slug
        if not artifact_root.exists():
            return

        for artifact_path in sorted(artifact_root.rglob("*.json"), key=artifact_timestamp):
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            rows = payload.get("items") or payload.get("data") or []
            if not isinstance(rows, list):
                continue

            for row in rows:
                raw_source_key = row.get("source_series_key") or row.get("series_key") or row.get("metal") or row.get("code")
                indicator = self._indicators_by_source_key.get((source_slug, normalize_source_series_key(raw_source_key)))
                if indicator is None:
                    continue

                value = parse_numeric(row.get(value_key))
                if value is None and value_key != "value":
                    value = parse_numeric(row.get("value"))
                if value is None and value_key != "total":
                    value = parse_numeric(row.get("total"))
                period_raw = row.get("date") or row.get("observation_date") or row.get("period") or row.get("period_end_at")
                if value is None or not period_raw:
                    continue

                period_end_at = parse_period_end(str(period_raw), indicator.frequency)
                vintage_at = parse_optional_timestamp(row.get("updated_at")) or parse_optional_timestamp(row.get("release_date")) or artifact_timestamp(artifact_path)
                release_date = parse_optional_timestamp(row.get("release_date")) or vintage_at
                observation = InventoryObservation(
                    period_start_at=period_start_for(period_end_at, indicator.frequency),
                    period_end_at=period_end_at,
                    release_date=release_date,
                    vintage_at=vintage_at,
                    value=value,
                    unit=indicator.canonical_unit_code or indicator.native_unit_code or "",
                    observation_kind=indicator.default_observation_kind,
                )
                self._store_observation(observations_by_period, indicator, observation, artifact_path)

    def find_indicator(self, indicator_id: str) -> InventoryIndicatorDefinition | None:
        return self._indicators_by_id.get(indicator_id) or self._indicators_by_code.get(indicator_id)

    def _latest_and_prior(self, indicator_id: str) -> tuple[InventoryObservation | None, InventoryObservation | None]:
        series = self._observations_by_id.get(indicator_id, [])
        if not series:
            return None, None
        latest = series[-1]
        prior = series[-2] if len(series) > 1 else None
        return latest, prior

    def _trailing_changes(self, indicator_id: str, points: int = 4) -> list[float]:
        series = self._observations_by_id.get(indicator_id, [])
        trailing = series[-points:]
        return [trailing[index].value - trailing[index - 1].value for index in range(1, len(trailing))]

    def _deviation_metrics(
        self,
        indicator: InventoryIndicatorDefinition,
        latest: InventoryObservation,
    ) -> tuple[dict[str, float | int | None] | None, dict[str, float | int | None], float | None, float | None]:
        seasonal_point = self._seasonal_point(indicator, latest, indicator.seasonal_profile)
        seasonal_context = self._seasonal_context(indicator, latest, indicator.seasonal_profile)
        deviation_abs = None
        deviation_zscore = None
        if seasonal_point and seasonal_point.get("p50") is not None:
            deviation_abs = latest.value - float(seasonal_point["p50"])
            if seasonal_point.get("stddev") not in (None, 0):
                deviation_zscore = deviation_abs / float(seasonal_point["stddev"])
        return seasonal_point, seasonal_context, deviation_abs, deviation_zscore

    def _public_seasonality_state(
        self,
        indicator: InventoryIndicatorDefinition,
        latest: InventoryObservation | None,
    ) -> bool:
        if latest is None:
            return False
        seasonal_points = self._seasonal_range(indicator, indicator.seasonal_profile)
        seasonal_context = self._seasonal_context(indicator, latest, indicator.seasonal_profile)
        observation_count = len(self._observations_by_id.get(indicator.id, []))
        return public_seasonality_allowed(
            indicator,
            observation_count,
            len(seasonal_points),
            int(seasonal_context.get("seasonal_samples") or 0),
        )

    def _seasonal_range(
        self,
        indicator: InventoryIndicatorDefinition,
        profile_name: str | None,
    ) -> list[dict[str, float | int | None]]:
        resolved_profile = profile_name or indicator.seasonal_profile or "inventorywatch_5y"
        cache_key = (indicator.id, resolved_profile)
        if cache_key in self._seasonal_cache:
            return self._seasonal_cache[cache_key]

        series = self._observations_by_id.get(indicator.id, [])
        if not series:
            self._seasonal_cache[cache_key] = []
            return []

        latest_year = series[-1].period_end_at.year
        min_year = latest_year - 4
        exclude_2020 = "_EX_2020" in resolved_profile.upper()
        grouped: dict[int, list[float]] = defaultdict(list)

        for point in series:
            year = point.period_end_at.year
            if year < min_year:
                continue
            if exclude_2020 and year == 2020:
                continue
            grouped[period_index_for_indicator(indicator, point.period_end_at)].append(point.value)

        seasonal_points: list[dict[str, float | int | None]] = []
        for period_index in sorted(grouped):
            values = sorted(grouped[period_index])
            mean_value = sum(values) / len(values) if values else None
            seasonal_points.append(
                {
                    "period_index": period_index,
                    "p10": quantile(values, 0.10),
                    "p25": quantile(values, 0.25),
                    "p50": quantile(values, 0.50),
                    "p75": quantile(values, 0.75),
                    "p90": quantile(values, 0.90),
                    "mean": mean_value,
                    "stddev": population_stddev(values),
                }
            )

        self._seasonal_cache[cache_key] = seasonal_points
        return seasonal_points

    def _seasonal_point(
        self,
        indicator: InventoryIndicatorDefinition,
        observation: InventoryObservation,
        profile_name: str | None = None,
    ) -> dict[str, float | int | None] | None:
        points = self._seasonal_range(indicator, profile_name)
        index = period_index_for_indicator(indicator, observation.period_end_at)
        return next((point for point in points if point["period_index"] == index), None)

    def _seasonal_context(
        self,
        indicator: InventoryIndicatorDefinition,
        observation: InventoryObservation,
        profile_name: str | None = None,
    ) -> dict[str, float | int | None]:
        seasonal_point = self._seasonal_point(indicator, observation, profile_name)
        latest_year = observation.period_end_at.year
        min_year = latest_year - 4
        exclude_2020 = "_EX_2020" in str(profile_name or indicator.seasonal_profile or "").upper()
        target_period_index = period_index_for_indicator(indicator, observation.period_end_at)
        matching_values = [
            point.value
            for point in self._observations_by_id.get(indicator.id, [])
            if point.period_end_at.year >= min_year
            and period_index_for_indicator(indicator, point.period_end_at) == target_period_index
            and not (exclude_2020 and point.period_end_at.year == 2020)
        ]

        if matching_values:
            seasonal_low = float(min(matching_values))
            seasonal_high = float(max(matching_values))
            seasonal_samples = len(matching_values)
        else:
            seasonal_low = None
            seasonal_high = None
            seasonal_samples = 0

        seasonal_median = None
        if seasonal_point and seasonal_point.get("p50") is not None:
            seasonal_median = float(seasonal_point["p50"])
        elif matching_values:
            seasonal_median = quantile(sorted(matching_values), 0.50)

        return {
            "seasonal_low": seasonal_low,
            "seasonal_high": seasonal_high,
            "seasonal_median": seasonal_median,
            "seasonal_samples": seasonal_samples,
            "seasonal_p10": float(seasonal_point["p10"]) if seasonal_point and seasonal_point.get("p10") is not None else None,
            "seasonal_p25": float(seasonal_point["p25"]) if seasonal_point and seasonal_point.get("p25") is not None else None,
            "seasonal_p75": float(seasonal_point["p75"]) if seasonal_point and seasonal_point.get("p75") is not None else None,
            "seasonal_p90": float(seasonal_point["p90"]) if seasonal_point and seasonal_point.get("p90") is not None else None,
        }

    def snapshot_payload(
        self,
        *,
        commodity: str | None = None,
        geography: str | None = None,
        limit: int = 20,
        include_sparklines: bool = True,
    ) -> dict[str, Any]:
        now = utcnow()
        cards: list[dict[str, Any]] = []

        for indicator in sorted(self._indicators_by_id.values(), key=lambda item: item.code):
            if indicator.visibility_tier != "public":
                continue
            if commodity and indicator.commodity_code != commodity:
                continue
            if geography and indicator.geography_code != geography:
                continue

            latest, prior = self._latest_and_prior(indicator.id)
            if latest is None:
                continue

            _seasonal_point, seasonal_context, deviation_abs, deviation_zscore = self._deviation_metrics(indicator, latest)
            has_public_seasonality = self._public_seasonality_state(indicator, latest)
            if not has_public_seasonality:
                deviation_abs = None
                deviation_zscore = None
                seasonal_context = {
                    "seasonal_low": None,
                    "seasonal_high": None,
                    "seasonal_median": None,
                    "seasonal_samples": None,
                    "seasonal_p10": None,
                    "seasonal_p25": None,
                    "seasonal_p75": None,
                    "seasonal_p90": None,
                }
            sparkline_source = self._observations_by_id.get(indicator.id, [])
            alerts = compute_inventory_alerts(
                commodity_code=indicator.commodity_code,
                latest_value=latest.value,
                deviation_zscore=deviation_zscore,
                seasonal_context=seasonal_context,
                trailing_changes=self._trailing_changes(indicator.id),
                alerts_enabled=alerts_enabled_for_indicator(indicator),
            )
            cards.append(
                {
                    "indicator_id": indicator.id,
                    "code": indicator.code,
                    "name": indicator.name,
                    "description": indicator.description,
                    "commodity_code": indicator.commodity_code,
                    "geography_code": indicator.geography_code,
                    "latest_value": latest.value,
                    "unit": latest.unit,
                    "frequency": indicator.frequency,
                    "change_abs": (latest.value - prior.value) if prior else None,
                    "deviation_abs": deviation_abs,
                    "signal": classify_inventory_signal(deviation_zscore),
                    "sparkline": [point.value for point in sparkline_source[-12:]] if include_sparklines else [],
                    "latest_period_end_at": latest.period_end_at.isoformat(),
                    "latest_release_date": latest.release_date.isoformat() if latest.release_date else None,
                    "commoditywatch_updated_at": latest.vintage_at.isoformat(),
                    "last_updated_at": latest.vintage_at.isoformat(),
                    "stale": release_is_aged(indicator.frequency, latest.release_date, now=now),
                    "source_label": source_label_for_indicator(indicator),
                    "source_url": source_url_for_indicator(indicator),
                    "is_seasonal": has_public_seasonality,
                    "period_type": indicator_period_type(indicator),
                    "marketing_year_start_month": marketing_year_start_month(indicator),
                    "color_convention": color_convention_for_indicator(indicator),
                    "release_schedule": release_schedule_for_indicator(indicator),
                    "days_of_supply": days_of_supply_enabled_for_indicator(indicator),
                    "alerts_enabled": alerts_enabled_for_indicator(indicator),
                    "alerts": alerts,
                    "seasonal_low": seasonal_context["seasonal_low"],
                    "seasonal_high": seasonal_context["seasonal_high"],
                    "seasonal_median": seasonal_context["seasonal_median"],
                    "seasonal_samples": seasonal_context["seasonal_samples"],
                    "seasonal_p10": seasonal_context["seasonal_p10"],
                    "seasonal_p25": seasonal_context["seasonal_p25"],
                    "seasonal_p75": seasonal_context["seasonal_p75"],
                    "seasonal_p90": seasonal_context["seasonal_p90"],
                }
            )

        return {
            "module": LOCAL_MODULE_CODE,
            "generated_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=300)).isoformat(),
            "cards": cards[:limit],
        }

    def indicator_latest_payload(self, indicator_id: str) -> dict[str, Any]:
        indicator = self.find_indicator(indicator_id)
        if indicator is None:
            raise KeyError("Indicator not found.")

        latest, prior = self._latest_and_prior(indicator.id)
        if latest is None:
            raise LookupError("No observations found.")

        _seasonal_point, seasonal_context, deviation_abs, deviation_zscore = self._deviation_metrics(indicator, latest)
        has_public_seasonality = self._public_seasonality_state(indicator, latest)
        if not has_public_seasonality:
            deviation_abs = None
            deviation_zscore = None
            seasonal_context = {
                "seasonal_low": None,
                "seasonal_high": None,
                "seasonal_median": None,
                "seasonal_samples": None,
                "seasonal_p10": None,
                "seasonal_p25": None,
                "seasonal_p75": None,
                "seasonal_p90": None,
            }

        change_abs = (latest.value - prior.value) if prior else None
        change_pct = ((change_abs / prior.value) * 100) if prior and prior.value else None
        alerts = compute_inventory_alerts(
            commodity_code=indicator.commodity_code,
            latest_value=latest.value,
            deviation_zscore=deviation_zscore,
            seasonal_context=seasonal_context,
            trailing_changes=self._trailing_changes(indicator.id),
            alerts_enabled=alerts_enabled_for_indicator(indicator),
        )
        return {
            "indicator": {
                "id": indicator.id,
                "code": indicator.code,
            },
            "latest": {
                "period_end_at": latest.period_end_at.isoformat(),
                "release_date": latest.release_date.isoformat() if latest.release_date else None,
                "commoditywatch_updated_at": latest.vintage_at.isoformat(),
                "value": latest.value,
                "unit": latest.unit,
                "change_from_prior_abs": change_abs,
                "change_from_prior_pct": change_pct,
                "deviation_from_seasonal_abs": deviation_abs,
                "deviation_from_seasonal_zscore": deviation_zscore,
                "revision_sequence": latest.revision_sequence,
                "revision_flag": revision_flag(latest.revision_sequence),
                "alerts": alerts,
            },
        }

    def indicator_data_payload(
        self,
        indicator_id: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        include_seasonal: bool = True,
        seasonal_profile: str | None = None,
        limit_points: int = 2000,
    ) -> dict[str, Any]:
        indicator = self.find_indicator(indicator_id)
        if indicator is None:
            raise KeyError("Indicator not found.")

        all_points = self._observations_by_id.get(indicator.id, [])
        effective_end = end_date or utcnow().date()
        effective_start = start_date
        series = [
            point
            for point in all_points
            if (effective_start is None or effective_start <= point.period_end_at.date())
            and point.period_end_at.date() <= effective_end
        ]
        series = series[-limit_points:]
        latest, _prior = self._latest_and_prior(indicator.id)
        has_public_seasonality = self._public_seasonality_state(indicator, latest)

        return {
            "indicator": {
                "id": indicator.id,
                "code": indicator.code,
                "name": indicator.name,
                "description": indicator.description,
                "modules": [LOCAL_MODULE_CODE],
                "commodity_code": indicator.commodity_code,
                "geography_code": indicator.geography_code,
                "frequency": indicator.frequency,
                "measure_family": indicator.measure_family,
                "unit": indicator.canonical_unit_code,
                "period_type": indicator_period_type(indicator),
                "marketing_year_start_month": marketing_year_start_month(indicator),
                "is_seasonal": has_public_seasonality,
                "color_convention": color_convention_for_indicator(indicator),
                "days_of_supply": days_of_supply_enabled_for_indicator(indicator),
                "alerts_enabled": alerts_enabled_for_indicator(indicator),
                "release_schedule": release_schedule_for_indicator(indicator),
            },
            "series": [
                {
                    "period_start_at": point.period_start_at.isoformat(),
                    "period_end_at": point.period_end_at.isoformat(),
                    "release_date": point.release_date.isoformat() if point.release_date else None,
                    "vintage_at": point.vintage_at.isoformat(),
                    "value": point.value,
                    "unit": point.unit,
                    "observation_kind": point.observation_kind,
                    "revision_sequence": point.revision_sequence,
                    "revision_flag": revision_flag(point.revision_sequence),
                }
                for point in series
            ],
            "seasonal_range": self._seasonal_range(indicator, seasonal_profile) if include_seasonal and has_public_seasonality else [],
            "metadata": {
                "latest_release_id": None,
                "latest_release_at": latest.release_date.isoformat() if latest and latest.release_date else None,
                "latest_period_end_at": latest.period_end_at.isoformat() if latest else None,
                "latest_vintage_at": latest.vintage_at.isoformat() if latest else None,
                "source_url": source_url_for_indicator(indicator),
                "source_label": source_label_for_indicator(indicator),
                "quarantined_observation_count": len(
                    [entry for entry in self._quarantined_observations if entry.indicator_id == indicator.id]
                ),
            },
        }

    def list_indicators_payload(
        self,
        *,
        module: str | None = None,
        commodity: str | None = None,
        geography: str | None = None,
        frequency: str | None = None,
        measure_family: str | None = None,
        visibility: str = "public",
        active: bool = True,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if cursor:
            return {"items": [], "next_cursor": None}
        if module and module != LOCAL_MODULE_CODE:
            return {"items": [], "next_cursor": None}
        if not active:
            return {"items": [], "next_cursor": None}

        items: list[dict[str, Any]] = []
        for indicator in sorted(self._indicators_by_id.values(), key=lambda item: item.code):
            if indicator.visibility_tier != visibility:
                continue
            if commodity and indicator.commodity_code != commodity:
                continue
            if geography and indicator.geography_code != geography:
                continue
            if frequency and indicator.frequency != frequency:
                continue
            if measure_family and indicator.measure_family != measure_family:
                continue

            latest, _prior = self._latest_and_prior(indicator.id)
            has_public_seasonality = self._public_seasonality_state(indicator, latest)
            items.append(
                {
                    "id": indicator.id,
                    "code": indicator.code,
                    "name": indicator.name,
                    "modules": [LOCAL_MODULE_CODE],
                    "commodity_code": indicator.commodity_code,
                    "geography_code": indicator.geography_code,
                    "measure_family": indicator.measure_family,
                    "frequency": indicator.frequency,
                    "native_unit": indicator.native_unit_code,
                    "canonical_unit": indicator.canonical_unit_code,
                    "is_seasonal": has_public_seasonality,
                    "is_derived": indicator.is_derived,
                    "visibility_tier": indicator.visibility_tier,
                    "latest_release_at": latest.release_date.isoformat() if latest and latest.release_date else None,
                    "period_type": indicator_period_type(indicator),
                    "marketing_year_start_month": marketing_year_start_month(indicator),
                    "release_schedule": release_schedule_for_indicator(indicator),
                }
            )

        return {
            "items": items[:limit],
            "next_cursor": None,
        }
