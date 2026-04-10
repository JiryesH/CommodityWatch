from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.ingest.sources.oecd.parsers import parse_oecd_cli_csv


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "oecd" / "oecd_cli_sample.csv"


def test_parse_oecd_cli_csv_rows() -> None:
    parsed = parse_oecd_cli_csv(FIXTURE_PATH.read_bytes())

    assert len(parsed) == 6
    assert parsed[0].source_series_key == "JPN"
    assert parsed[0].reference_area == "Japan"
    assert parsed[0].period_start_at == datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    assert parsed[0].period_end_at == datetime(2026, 2, 28, 23, 59, 59, tzinfo=timezone.utc)
    assert parsed[0].value == 100.168


def test_parse_oecd_cli_csv_skips_non_normal_or_blank_rows() -> None:
    raw = b"""STRUCTURE,REF_AREA,FREQ,MEASURE,UNIT_MEASURE,TIME_PERIOD,OBS_VALUE,OBS_STATUS,Reference area\nDATAFLOW,JPN,M,LI,IX,2026-03,100.25,A,Japan\nDATAFLOW,JPN,M,LI,IX,2026-04,,A,Japan\nDATAFLOW,JPN,M,LI,IX,2026-05,100.30,E,Japan\n"""

    parsed = parse_oecd_cli_csv(raw)

    assert len(parsed) == 1
    assert parsed[0].source_item_ref == "JPN:2026-03"
