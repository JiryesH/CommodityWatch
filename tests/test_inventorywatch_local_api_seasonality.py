from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inventory_watch_local_api import InventoryIndicatorDefinition, InventoryObservation, LocalInventoryRepository


UTC = timezone.utc
NOW = datetime(2026, 4, 3, 12, 0, tzinfo=UTC)


def make_indicator(
    code: str,
    *,
    is_seasonal: bool,
    source_slug: str = "eia",
) -> InventoryIndicatorDefinition:
    return InventoryIndicatorDefinition(
        id=code,
        code=code,
        name=code.replace("_", " ").title(),
        description=None,
        measure_family="inventory",
        frequency="weekly",
        commodity_code="crude_oil",
        geography_code="US",
        source_slug=source_slug,
        source_series_key=code,
        native_unit_code="kb",
        canonical_unit_code="kb",
        default_observation_kind="actual",
        seasonal_profile="inventorywatch_5y" if is_seasonal else None,
        is_seasonal=is_seasonal,
        is_derived=False,
        visibility_tier="public",
        metadata={"source_label": "EIA"},
    )


def make_observation(
    *,
    period_end_at: datetime,
    release_date: datetime,
    vintage_at: datetime,
    value: float,
) -> InventoryObservation:
    return InventoryObservation(
        period_start_at=period_end_at - timedelta(days=6),
        period_end_at=period_end_at,
        release_date=release_date,
        vintage_at=vintage_at,
        value=value,
        unit="kb",
        observation_kind="actual",
    )


def build_repository(
    *,
    current_indicator: InventoryIndicatorDefinition,
    current_observations: list[InventoryObservation],
    seasonal_indicator: InventoryIndicatorDefinition | None = None,
    seasonal_observations: list[InventoryObservation] | None = None,
    seasonal_points: list[dict[str, float | int | None]] | None = None,
) -> LocalInventoryRepository:
    repository = LocalInventoryRepository.__new__(LocalInventoryRepository)
    indicators = [current_indicator]
    if seasonal_indicator is not None:
        indicators.append(seasonal_indicator)

    repository._indicators_by_id = {indicator.id: indicator for indicator in indicators}
    repository._indicators_by_code = {indicator.code: indicator for indicator in indicators}
    repository._observations_by_id = {current_indicator.id: current_observations}
    if seasonal_indicator is not None and seasonal_observations is not None:
        repository._observations_by_id[seasonal_indicator.id] = seasonal_observations
    repository._seasonal_cache = {}
    if seasonal_indicator is None and seasonal_points is not None:
        repository._seasonal_cache[(current_indicator.id, "inventorywatch_5y")] = seasonal_points
    if seasonal_indicator is not None and seasonal_points is not None:
        repository._seasonal_cache[(seasonal_indicator.id, "inventorywatch_5y")] = seasonal_points
    repository._quarantined_observations = []
    return repository


def build_contract_repository() -> LocalInventoryRepository:
    current_indicator = make_indicator("EIA_CURRENT_ONLY_STOCKS", is_seasonal=False)
    seasonal_indicator = make_indicator("EIA_SEASONAL_PUBLIC_STOCKS", is_seasonal=True)

    current_observations = [
        make_observation(
            period_end_at=datetime(2026, 3, 13, tzinfo=UTC),
            release_date=datetime(2026, 3, 19, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 19, 15, 5, tzinfo=UTC),
            value=442_280,
        ),
        make_observation(
            period_end_at=datetime(2026, 3, 20, tzinfo=UTC),
            release_date=datetime(2026, 3, 26, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 26, 15, 5, tzinfo=UTC),
            value=438_940,
        ),
    ]

    seasonal_observations = [
        make_observation(
            period_end_at=datetime(2024, 3, 22, tzinfo=UTC),
            release_date=datetime(2024, 3, 27, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2024, 3, 27, 15, 5, tzinfo=UTC),
            value=615_000,
        ),
        make_observation(
            period_end_at=datetime(2025, 3, 21, tzinfo=UTC),
            release_date=datetime(2025, 3, 26, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2025, 3, 26, 15, 5, tzinfo=UTC),
            value=620_000,
        ),
        make_observation(
            period_end_at=datetime(2026, 3, 13, tzinfo=UTC),
            release_date=datetime(2026, 3, 19, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 19, 15, 5, tzinfo=UTC),
            value=631_000,
        ),
        make_observation(
            period_end_at=datetime(2026, 3, 20, tzinfo=UTC),
            release_date=datetime(2026, 3, 26, 14, 30, tzinfo=UTC),
            vintage_at=datetime(2026, 3, 26, 15, 20, tzinfo=UTC),
            value=628_000,
        ),
    ]

    seasonal_points = [
        {
            "period_index": period_index,
            "p10": 610_000 + period_index,
            "p25": 615_000 + period_index,
            "p50": 620_000 + period_index,
            "p75": 625_000 + period_index,
            "p90": 630_000 + period_index,
            "mean": 620_500 + period_index,
            "stddev": 4_000,
        }
        for period_index in range(1, 27)
    ]

    return build_repository(
        current_indicator=current_indicator,
        current_observations=current_observations,
        seasonal_indicator=seasonal_indicator,
        seasonal_observations=seasonal_observations,
        seasonal_points=seasonal_points,
    )


def build_thin_seasonality_repository() -> LocalInventoryRepository:
    indicator = make_indicator("EIA_THIN_SEASONAL_STOCKS", is_seasonal=True)
    observations = [
        make_observation(
            period_end_at=NOW - timedelta(days=14),
            release_date=NOW - timedelta(days=9),
            vintage_at=NOW - timedelta(days=9, hours=-1),
            value=420_000,
        ),
        make_observation(
            period_end_at=NOW - timedelta(days=7),
            release_date=NOW - timedelta(days=2),
            vintage_at=NOW - timedelta(days=2) + timedelta(hours=1),
            value=430_000,
        ),
        make_observation(
            period_end_at=NOW,
            release_date=NOW + timedelta(days=5),
            vintage_at=NOW + timedelta(days=5, hours=1),
            value=440_000,
        ),
    ]
    seasonal_points = [
        {
            "period_index": 12,
            "p10": 390_000,
            "p25": 400_000,
            "p50": 410_000,
            "p75": 420_000,
            "p90": 430_000,
            "mean": 410_000,
            "stddev": 10_000,
        },
        {
            "period_index": 13,
            "p10": 391_000,
            "p25": 401_000,
            "p50": 411_000,
            "p75": 421_000,
            "p90": 431_000,
            "mean": 411_000,
            "stddev": 10_000,
        },
    ]
    return build_repository(
        current_indicator=indicator,
        current_observations=observations,
        seasonal_points=seasonal_points,
    )


def test_public_payload_suppresses_thin_seasonality() -> None:
    repository = build_thin_seasonality_repository()

    snapshot = repository.snapshot_payload(limit=5)
    latest = repository.indicator_latest_payload("EIA_THIN_SEASONAL_STOCKS")
    data = repository.indicator_data_payload("EIA_THIN_SEASONAL_STOCKS", include_seasonal=True)

    assert snapshot["cards"][0]["is_seasonal"] is False
    assert snapshot["cards"][0]["seasonal_median"] is None
    assert snapshot["cards"][0]["seasonal_p10"] is None
    assert snapshot["cards"][0]["deviation_abs"] is None

    assert latest["latest"]["deviation_from_seasonal_abs"] is None
    assert latest["latest"]["deviation_from_seasonal_zscore"] is None

    assert data["indicator"]["is_seasonal"] is False
    assert data["seasonal_range"] == []


def test_snapshot_payload_preserves_date_semantics_for_current_and_seasonal_series() -> None:
    repository = build_contract_repository()

    snapshot = repository.snapshot_payload(limit=10, include_sparklines=False)
    by_code = {card["code"]: card for card in snapshot["cards"]}

    current_only = by_code["EIA_CURRENT_ONLY_STOCKS"]
    assert current_only["is_seasonal"] is False
    assert current_only["seasonal_median"] is None
    assert current_only["latest_period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert current_only["latest_release_date"] == "2026-03-26T14:30:00+00:00"
    assert current_only["commoditywatch_updated_at"] == "2026-03-26T15:05:00+00:00"

    seasonal = by_code["EIA_SEASONAL_PUBLIC_STOCKS"]
    assert seasonal["is_seasonal"] is True
    assert seasonal["seasonal_median"] is not None
    assert seasonal["seasonal_p10"] is not None
    assert seasonal["seasonal_p90"] is not None
    assert seasonal["latest_period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert seasonal["latest_release_date"] == "2026-03-26T14:30:00+00:00"
    assert seasonal["commoditywatch_updated_at"] == "2026-03-26T15:20:00+00:00"


def test_latest_payload_preserves_date_semantics_for_current_and_seasonal_series() -> None:
    repository = build_contract_repository()

    current_only = repository.indicator_latest_payload("EIA_CURRENT_ONLY_STOCKS")
    assert current_only["latest"]["period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert current_only["latest"]["release_date"] == "2026-03-26T14:30:00+00:00"
    assert current_only["latest"]["commoditywatch_updated_at"] == "2026-03-26T15:05:00+00:00"
    assert current_only["latest"]["deviation_from_seasonal_abs"] is None
    assert current_only["latest"]["deviation_from_seasonal_zscore"] is None

    seasonal = repository.indicator_latest_payload("EIA_SEASONAL_PUBLIC_STOCKS")
    assert seasonal["latest"]["period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert seasonal["latest"]["release_date"] == "2026-03-26T14:30:00+00:00"
    assert seasonal["latest"]["commoditywatch_updated_at"] == "2026-03-26T15:20:00+00:00"
    assert seasonal["latest"]["deviation_from_seasonal_abs"] is not None
    assert seasonal["latest"]["deviation_from_seasonal_zscore"] is not None


def test_data_payload_preserves_date_semantics_for_current_and_seasonal_series() -> None:
    repository = build_contract_repository()

    current_only = repository.indicator_data_payload("EIA_CURRENT_ONLY_STOCKS", include_seasonal=True)
    assert current_only["indicator"]["is_seasonal"] is False
    assert current_only["seasonal_range"] == []
    assert current_only["metadata"]["latest_period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert current_only["metadata"]["latest_vintage_at"] == "2026-03-26T15:05:00+00:00"

    seasonal = repository.indicator_data_payload("EIA_SEASONAL_PUBLIC_STOCKS", include_seasonal=True)
    assert seasonal["indicator"]["is_seasonal"] is True
    assert len(seasonal["seasonal_range"]) == 26
    assert seasonal["metadata"]["latest_period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert seasonal["metadata"]["latest_release_at"] == "2026-03-26T14:30:00+00:00"
    assert seasonal["metadata"]["latest_vintage_at"] == "2026-03-26T15:20:00+00:00"
