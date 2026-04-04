from __future__ import annotations

from pathlib import Path

import pytest

from app.ingest.sources.usda_psd.parsers import parse_psd_commodity_response


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "usda_psd" / "usda_psd_total_use_sample.xml"


def test_parse_usda_psd_total_use_rows_with_unit_conversion() -> None:
    raw = FIXTURE_PATH.read_bytes()

    corn = parse_psd_commodity_response(raw, commodity_code="0440000")
    soybeans = parse_psd_commodity_response(raw, commodity_code="2222000")
    wheat = parse_psd_commodity_response(raw, commodity_code="0410000")

    assert corn[0].release_month == "2026-03"
    assert corn[0].value_mbu == pytest.approx(1539.2888)
    assert soybeans[0].value_mbu == pytest.approx(631.256766)
    assert wheat[0].value_mbu == pytest.approx(1131.70596)
