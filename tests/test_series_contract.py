from __future__ import annotations

import json
from pathlib import Path

import headline_associations


CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "shared" / "commodity-series-contract.json"
)
VALID_GROUPS = {"energy", "metals", "agri"}


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


def test_series_contract_uses_valid_groups_and_grouped_references() -> None:
    contract = load_contract()
    known_series = set(contract["series"])

    assert {metadata["group"] for metadata in contract["series"].values()} <= VALID_GROUPS

    for series_keys in contract["grouped_cards"].values():
        assert series_keys
        assert len(series_keys) == len(set(series_keys))
        assert set(series_keys) <= known_series
