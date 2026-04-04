from __future__ import annotations

from pathlib import Path

import inventory_watch_published_db as published_db
from inventory_watch_published_db import PublishedInventoryRepository, publish_inventory_store
from tests.test_inventorywatch_local_api_seasonality import build_contract_repository


def normalize_cards(payload: dict) -> dict[str, dict]:
    return {card["code"]: card for card in payload["cards"]}


def test_published_store_preserves_inventorywatch_contract_fields(tmp_path: Path, monkeypatch) -> None:
    repository = build_contract_repository()
    output_path = tmp_path / "inventorywatch.db"

    monkeypatch.setattr(published_db, "LocalInventoryRepository", lambda _data_root: repository)
    summary = publish_inventory_store(tmp_path / "fixture-root", output_path)

    assert summary["indicator_count"] == 2
    assert summary["observation_count"] == 6
    assert summary["seasonal_profile_count"] >= 2

    published_repository = PublishedInventoryRepository(output_path)

    local_snapshot = normalize_cards(repository.snapshot_payload(limit=10, include_sparklines=False))
    published_snapshot = normalize_cards(published_repository.snapshot_payload(limit=10, include_sparklines=False))

    for code in ("EIA_CURRENT_ONLY_STOCKS", "EIA_SEASONAL_PUBLIC_STOCKS"):
        assert published_snapshot[code]["latest_period_end_at"] == local_snapshot[code]["latest_period_end_at"]
        assert published_snapshot[code]["latest_release_date"] == local_snapshot[code]["latest_release_date"]
        assert published_snapshot[code]["commoditywatch_updated_at"] == local_snapshot[code]["commoditywatch_updated_at"]
        assert published_snapshot[code]["is_seasonal"] == local_snapshot[code]["is_seasonal"]
        assert published_snapshot[code]["seasonal_median"] == local_snapshot[code]["seasonal_median"]

    published_current = published_repository.indicator_latest_payload("EIA_CURRENT_ONLY_STOCKS")
    assert published_current["latest"]["deviation_from_seasonal_abs"] is None
    assert published_current["latest"]["commoditywatch_updated_at"] == "2026-03-26T15:05:00+00:00"

    published_seasonal = published_repository.indicator_latest_payload("EIA_SEASONAL_PUBLIC_STOCKS")
    assert published_seasonal["latest"]["deviation_from_seasonal_abs"] is not None
    assert published_seasonal["latest"]["commoditywatch_updated_at"] == "2026-03-26T15:20:00+00:00"

    published_data = published_repository.indicator_data_payload("EIA_SEASONAL_PUBLIC_STOCKS", include_seasonal=True)
    assert published_data["indicator"]["is_seasonal"] is True
    assert len(published_data["seasonal_range"]) == 26
    assert published_data["metadata"]["latest_period_end_at"] == "2026-03-20T00:00:00+00:00"
    assert published_data["metadata"]["latest_vintage_at"] == "2026-03-26T15:20:00+00:00"
