from __future__ import annotations

from pathlib import Path

import yaml

from app.modules.demandwatch import policy


SEED_DIR = Path(__file__).resolve().parents[2] / "seed"


def load_yaml(name: str):
    return yaml.safe_load((SEED_DIR / name).read_text(encoding="utf-8"))


def test_live_demandwatch_series_are_mvp_safe_and_fully_registered() -> None:
    indicators = load_yaml("indicators/demandwatch.yml")
    sources = {item["slug"]: item for item in load_yaml("sources.yml")}
    releases = {item["slug"] for item in load_yaml("release_definitions.yml")}
    verticals = {item["code"] for item in load_yaml("demand_verticals.yml")}
    commodities = {item["code"] for item in load_yaml("commodities.yml")}
    units = {item["code"] for item in load_yaml("units.yml")}

    live_items = [item for item in indicators if item["coverage_status"] == "live"]
    assert live_items, "DemandWatch should seed at least one live MVP series."

    for item in live_items:
        assert item["active"] is True
        assert "demandwatch" in item["modules"]
        assert item["primary_module"] == "demandwatch"
        assert item["demand_vertical_code"] in verticals
        assert item["commodity_code"] in commodities
        assert item["native_unit_code"] in units
        assert item["canonical_unit_code"] in units
        assert item["release_slug"] in releases
        assert policy.is_demand_series_ingestable(item["coverage_status"], sources[item["source_slug"]]["legal_status"]) is True


def test_restricted_demandwatch_series_are_seeded_as_inactive_placeholders() -> None:
    indicators = load_yaml("indicators/demandwatch.yml")
    restricted = [item for item in indicators if item["coverage_status"] in {"blocked", "needs_verification"}]

    assert {item["code"] for item in restricted} >= {
        "CHINA_CRUDE_IMPORTS_MONTHLY",
        "IEA_GLOBAL_OIL_DEMAND_TABLE",
        "SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW",
    }

    for item in restricted:
        assert item["active"] is False
        assert item["visibility_tier"] == "internal"


def test_unresolved_demand_sources_are_marked_conservatively() -> None:
    sources = {item["slug"]: item for item in load_yaml("sources.yml")}

    assert sources["china_customs"]["legal_status"] == "needs_verification"
    assert sources["china_nbs"]["legal_status"] == "needs_verification"
    assert sources["iea"]["legal_status"] == "needs_verification"
    assert sources["worldsteel"]["legal_status"] == "needs_verification"
    assert sources["spglobal_pmi"]["legal_status"] == "off_limits"
