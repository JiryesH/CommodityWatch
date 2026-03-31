from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(not os.getenv("CW_TEST_DATABASE_URL"), reason="CW_TEST_DATABASE_URL is not configured")


@pytest.mark.asyncio
async def test_list_indicators_endpoint(client) -> None:
    response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["code"] == "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"


@pytest.mark.asyncio
async def test_indicator_data_endpoint(client) -> None:
    list_response = await client.get("/api/indicators", params={"module": "inventorywatch"})
    indicator_id = list_response.json()["items"][0]["id"]

    response = await client.get(f"/api/indicators/{indicator_id}/data")
    assert response.status_code == 200
    payload = response.json()
    assert payload["indicator"]["code"] == "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"
    assert len(payload["series"]) >= 1
    assert len(payload["seasonal_range"]) >= 1


@pytest.mark.asyncio
async def test_snapshot_endpoint(client) -> None:
    response = await client.get("/api/snapshot/inventorywatch")
    assert response.status_code == 200
    payload = response.json()
    assert payload["module"] == "inventorywatch"
    assert payload["cards"][0]["code"] == "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"
