from __future__ import annotations

import json
from pathlib import Path

from app.ingest.sources.eia.parsers import parse_eia_response


def test_parse_eia_response() -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "eia" / "eia_series_sample.json"
    payload = json.loads(fixture.read_text())
    parsed = parse_eia_response(payload, "weekly")

    assert len(parsed) == 2
    assert parsed[0].value == 438940.0
    assert parsed[0].period_end_at.isoformat() == "2026-03-20T00:00:00+00:00"
    assert parsed[0].period_start_at.isoformat() == "2026-03-14T00:00:00+00:00"

