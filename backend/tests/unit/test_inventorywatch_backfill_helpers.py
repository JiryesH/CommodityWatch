from __future__ import annotations

from datetime import date
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.ingest.backfill import business_days, describe_backfill_scope, monthly_chunks, yearly_chunks


def test_describe_backfill_scope_mentions_source_specific_coverage() -> None:
    usda_note = describe_backfill_scope("usda_wasde", date(2000, 1, 1), date(2000, 3, 31))
    lme_note = describe_backfill_scope("lme_warehouse", date(2026, 3, 1), date(2026, 3, 31))

    assert "2000-01-01 -> 2000-03-31" in usda_note
    assert "USDA WASDE backfills use the public archive" in usda_note
    assert "2026-03-01 -> 2026-03-31" in lme_note
    assert "LME warehouse backfills iterate public business-day workbook URLs" in lme_note


def test_backfill_chunk_helpers_remain_stable() -> None:
    assert yearly_chunks(date(2024, 12, 31), date(2026, 1, 2)) == [
        (date(2024, 12, 31), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 2)),
    ]
    assert monthly_chunks(date(2026, 2, 15), date(2026, 4, 2)) == ["2026-02", "2026-03", "2026-04"]
    assert business_days(date(2026, 4, 2), date(2026, 4, 6)) == [
        date(2026, 4, 2),
        date(2026, 4, 3),
        date(2026, 4, 6),
    ]
