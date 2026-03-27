from __future__ import annotations

import json
from pathlib import Path

import headline_associations


CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "shared" / "commodity-series-contract.json"
)
EXPECTED_SECTOR_ORDER = [
    "Energy",
    "Metals and Mining",
    "Agriculture",
    "Fertilizers and Agricultural Chemicals",
    "Livestock, Dairy, and Seafood",
    "Forest and Wood Products",
]


def load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_headline_rule_inventory_matches_contract() -> None:
    contract = load_contract()
    expected = {
        series_key
        for series_key, metadata in contract["series"].items()
        if metadata["supports_related_headlines"]
    }

    assert set(headline_associations.RAW_SERIES_HEADLINE_RULES) == expected


def test_series_contract_uses_valid_taxonomy_and_grouped_references() -> None:
    contract = load_contract()
    sectors = contract["sectors"]
    known_series = set(contract["series"])

    assert [sector["label"] for sector in sectors] == EXPECTED_SECTOR_ORDER
    assert [sector["order"] for sector in sectors] == list(range(1, len(sectors) + 1))

    subsector_keys = set()
    for sector in sectors:
        subsectors = sector["subsectors"]
        assert [subsector["order"] for subsector in subsectors] == list(range(1, len(subsectors) + 1))
        for subsector in subsectors:
            subsector_keys.add((sector["id"], subsector["id"]))

    for metadata in contract["series"].values():
        assert (metadata["sectorId"], metadata["subsectorId"]) in subsector_keys

    for grouped_card in contract["grouped_cards"].values():
        assert (grouped_card["sectorId"], grouped_card["subsectorId"]) in subsector_keys
        assert grouped_card["defaultSeriesKey"] in grouped_card["seriesKeys"]
        assert grouped_card["seriesKeys"]
        assert len(grouped_card["seriesKeys"]) == len(set(grouped_card["seriesKeys"]))
        assert set(grouped_card["seriesKeys"]) <= known_series

    assert contract["dashboard"]["default_home_series_keys"] == [
        "crude_oil_wti",
        "crude_oil_brent",
        "natural_gas_henry_hub",
        "natural_gas_ttf",
        "gold_worldbank_monthly",
        "copper_worldbank_monthly",
        "wheat_global_monthly_proxy",
        "corn_global_monthly_proxy",
        "coffee_arabica_monthly_proxy",
        "sugar_no11_world_monthly_proxy",
        "iron_ore_62pct_china_monthly",
        "thermal_coal_newcastle",
    ]
