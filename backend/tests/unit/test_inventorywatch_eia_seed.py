from __future__ import annotations

from pathlib import Path

import yaml


SEED_PATH = Path(__file__).resolve().parents[2] / "seed" / "indicators" / "inventorywatch.yml"


def test_inventorywatch_eia_seed_uses_current_verified_series_ids() -> None:
    items = yaml.safe_load(SEED_PATH.read_text(encoding="utf-8"))
    by_code = {item["code"]: item for item in items}

    assert by_code["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]["source_series_key"] == "PET.WCESTUS1.W"
    assert by_code["EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR"]["visibility_tier"] == "public"

    assert by_code["EIA_PROPANE_US_TOTAL_STOCKS"]["source_series_key"] == "PET.WPRSTUS1.W"
    assert by_code["EIA_PROPANE_US_TOTAL_STOCKS"]["visibility_tier"] == "public"
