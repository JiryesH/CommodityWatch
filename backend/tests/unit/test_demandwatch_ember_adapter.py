from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.ingest.sources.ember.parsers import parse_ember_monthly_demand, parse_ember_stats_timestamp


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "ember" / "ember_monthly_demand_sample.json"


def test_parse_ember_stats_timestamp() -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    parsed = parse_ember_stats_timestamp(payload)

    assert parsed == datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc)


def test_parse_ember_monthly_demand_rows() -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    parsed = parse_ember_monthly_demand(payload)

    assert len(parsed) == 2
    assert parsed[0].source_item_ref == "World:2026-01"
    assert parsed[0].period_end_at == datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc)
    assert parsed[1].entity_code == "CHN"
    assert parsed[1].value_twh == 712.3
