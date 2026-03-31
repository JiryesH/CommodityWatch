from __future__ import annotations

import json
from pathlib import Path

from app.ingest.sources.agsi.parsers import parse_agsi_payload


def test_parse_agsi_payload() -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "agsi" / "agsi_daily_sample.json"
    payload = json.loads(fixture.read_text())
    parsed = parse_agsi_payload(payload)

    assert len(parsed) == 2
    assert parsed[0].value == 780.5
    assert parsed[0].percent_full == 34.2
    assert parsed[0].period_end_at.isoformat() == "2026-03-25T00:00:00+00:00"

