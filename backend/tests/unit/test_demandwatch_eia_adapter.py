from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from app.ingest.sources.eia.parsers import parse_eia_response


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"


def _load_json(relative_path: str) -> dict:
    return json.loads((FIXTURE_ROOT / relative_path).read_text(encoding="utf-8"))


def _load_demandwatch_seed() -> list[dict]:
    seed_path = Path(__file__).resolve().parents[2] / "seed" / "indicators" / "demandwatch.yml"
    return yaml.safe_load(seed_path.read_text(encoding="utf-8"))


def test_parse_hourly_eia_grid_monitor_payload() -> None:
    payload = _load_json("eia/eia_grid_monitor_sample.json")

    parsed = parse_eia_response(payload, "hourly")

    assert len(parsed) == 2
    assert parsed[-1].period_end_at == datetime(2026, 4, 2, 23, 0, tzinfo=timezone.utc)
    assert parsed[-1].period_start_at == datetime(2026, 4, 2, 23, 0, tzinfo=timezone.utc)
    assert parsed[-1].value == 487321.0


def test_parse_weekly_eia_wpsr_demand_payload() -> None:
    payload = _load_json("eia/eia_wpsr_demand_sample.json")

    parsed = parse_eia_response(payload, "weekly")

    assert len(parsed) == 2
    assert parsed[-1].period_end_at == datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc)
    assert parsed[-1].period_start_at == datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc)
    assert parsed[-1].value == 20345.0


def test_live_demandwatch_eia_series_seed_source_keys() -> None:
    live_eia_items = [
        item
        for item in _load_demandwatch_seed()
        if item.get("coverage_status") == "live" and item.get("source_slug") == "eia"
    ]

    assert live_eia_items
    assert all(item.get("source_series_key") for item in live_eia_items)
