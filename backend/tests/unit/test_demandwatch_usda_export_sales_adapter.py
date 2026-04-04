from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.ingest.sources.usda_export_sales.parsers import parse_export_sales_release_info, parse_export_sales_summary


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "usda_export_sales"


def test_parse_usda_export_sales_release_info() -> None:
    info = parse_export_sales_release_info((FIXTURE_ROOT / "usda_export_sales_weekly_highlights_sample.xml").read_bytes())

    assert info.released_on == date(2026, 4, 2)
    assert info.period_ending_on == date(2026, 3, 26)


def test_parse_usda_export_sales_summary_rows() -> None:
    parsed = parse_export_sales_summary((FIXTURE_ROOT / "usda_export_sales_cwr_summary_sample.xml").read_bytes())

    corn = next(item for item in parsed if item.source_series_key == "401" and item.period_ending_on == date(2026, 3, 26))
    assert corn.marketing_year == "Sep 2025/Aug 2026"
    assert corn.raw_net_sales_kt == pytest.approx(1149.424)
    assert corn.value_mmt == pytest.approx(1.149424)
