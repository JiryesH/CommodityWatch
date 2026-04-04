from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_list_indicators_endpoint(client) -> None:
    response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    assert response.status_code == 200
    payload = response.json()
    assert [item["code"] for item in payload["items"]] == [
        "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR",
        "EIA_CRUDE_US_TOTAL_STOCKS_SEASONAL_PUBLIC",
    ]

    by_code = {item["code"]: item for item in payload["items"]}
    assert by_code["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]["is_seasonal"] is False
    assert by_code["EIA_CRUDE_US_TOTAL_STOCKS_SEASONAL_PUBLIC"]["is_seasonal"] is True


@pytest.mark.asyncio
async def test_indicator_data_endpoint(client) -> None:
    list_response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    items = {item["code"]: item for item in list_response.json()["items"]}
    indicator_id = items["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]["id"]

    response = await client.get(f"/api/indicators/{indicator_id}/data")
    assert response.status_code == 200
    payload = response.json()
    assert payload["indicator"]["code"] == "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"
    assert len(payload["series"]) >= 1
    assert payload["indicator"]["is_seasonal"] is False
    assert payload["seasonal_range"] == []
    assert payload["metadata"]["latest_period_end_at"].startswith("2026-03-20T00:00:00")
    assert payload["metadata"]["latest_vintage_at"] is not None


@pytest.mark.asyncio
async def test_indicator_latest_endpoint_suppresses_weak_seasonality(client) -> None:
    list_response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    items = {item["code"]: item for item in list_response.json()["items"]}
    indicator_id = items["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]["id"]

    response = await client.get(f"/api/indicators/{indicator_id}/latest")
    assert response.status_code == 200
    payload = response.json()
    assert payload["latest"]["deviation_from_seasonal_abs"] is None
    assert payload["latest"]["deviation_from_seasonal_zscore"] is None
    assert payload["latest"]["period_end_at"].startswith("2026-03-20T00:00:00")
    assert payload["latest"]["release_date"] != payload["latest"]["commoditywatch_updated_at"]


@pytest.mark.asyncio
async def test_seasonal_indicator_endpoints_expose_public_seasonal_fields(client) -> None:
    list_response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    items = {item["code"]: item for item in list_response.json()["items"]}
    indicator_id = items["EIA_CRUDE_US_TOTAL_STOCKS_SEASONAL_PUBLIC"]["id"]

    data_response = await client.get(f"/api/indicators/{indicator_id}/data")
    assert data_response.status_code == 200
    data_payload = data_response.json()
    assert data_payload["indicator"]["is_seasonal"] is True
    assert len(data_payload["seasonal_range"]) == 26
    assert data_payload["metadata"]["latest_period_end_at"].startswith("2026-03-20T00:00:00")
    assert data_payload["metadata"]["latest_vintage_at"] is not None

    latest_response = await client.get(f"/api/indicators/{indicator_id}/latest")
    assert latest_response.status_code == 200
    latest_payload = latest_response.json()
    assert latest_payload["latest"]["deviation_from_seasonal_abs"] is not None
    assert latest_payload["latest"]["deviation_from_seasonal_zscore"] is not None
    assert latest_payload["latest"]["period_end_at"].startswith("2026-03-20T00:00:00")
    assert latest_payload["latest"]["release_date"] != latest_payload["latest"]["commoditywatch_updated_at"]


@pytest.mark.asyncio
async def test_snapshot_endpoint_preserves_date_semantics_and_seasonal_gating(client) -> None:
    response = await client.get("/api/snapshot/inventorywatch")
    assert response.status_code == 200
    payload = response.json()
    assert payload["module"] == "inventorywatch"
    by_code = {card["code"]: card for card in payload["cards"]}

    current_only = by_code["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]
    assert current_only["is_seasonal"] is False
    assert current_only["seasonal_median"] is None
    assert current_only["latest_period_end_at"].startswith("2026-03-20T00:00:00")
    assert current_only["latest_release_date"] != current_only["commoditywatch_updated_at"]

    seasonal = by_code["EIA_CRUDE_US_TOTAL_STOCKS_SEASONAL_PUBLIC"]
    assert seasonal["is_seasonal"] is True
    assert seasonal["seasonal_median"] is not None
    assert seasonal["seasonal_p10"] is not None
    assert seasonal["seasonal_p90"] is not None
    assert seasonal["latest_period_end_at"].startswith("2026-03-20T00:00:00")
    assert seasonal["latest_release_date"] != seasonal["commoditywatch_updated_at"]
