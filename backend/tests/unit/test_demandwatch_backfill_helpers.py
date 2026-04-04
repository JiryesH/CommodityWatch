from __future__ import annotations

from datetime import date
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_ROOT))

from app.modules.demandwatch.backfill import demandwatch_default_from_date, describe_demandwatch_backfill_scope


def test_demandwatch_default_backfill_window_targets_three_years() -> None:
    assert demandwatch_default_from_date(date(2026, 4, 4)) == date(2023, 4, 4)
    assert demandwatch_default_from_date(date(2024, 2, 29)) == date(2021, 2, 28)


def test_demandwatch_backfill_scope_mentions_partial_export_sales_history() -> None:
    note = describe_demandwatch_backfill_scope("demand_usda_export_sales", date(2023, 4, 4), date(2026, 4, 4))

    assert "2023-04-04 -> 2026-04-04" in note
    assert "historical backfill remains partial" in note
