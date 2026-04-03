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


def test_parse_eia_response_skips_blank_values_and_accepts_aliases() -> None:
    payload = {
        "response": {
            "data": [
                {
                    "period": "2026-03-20T00:00:00",
                    "value": "438,940",
                    "seriesDescription": "Weekly U.S. Ending Stocks excluding SPR of Crude Oil",
                },
                {"date": "2026-03-13", "value": "NA", "series_description": "ignored"},
                {"period": "2026-03-06", "value": "", "description": "ignored"},
            ]
        }
    }
    parsed = parse_eia_response(payload, "weekly")

    assert len(parsed) == 1
    assert parsed[0].value == 438940.0
    assert parsed[0].source_item_ref == "Weekly U.S. Ending Stocks excluding SPR of Crude Oil"
    assert parsed[0].period_start_at.isoformat() == "2026-03-14T00:00:00+00:00"
