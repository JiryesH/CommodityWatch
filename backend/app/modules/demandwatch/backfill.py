from __future__ import annotations

from datetime import date


MIN_DEMANDWATCH_BACKFILL_YEARS = 3
DEMANDWATCH_BACKFILL_NOTES: dict[str, str] = {
    "demand_eia_wpsr": "EIA WPSR history is public and should be backfilled at least three years for YoY comparisons.",
    "demand_eia_grid_monitor": "EIA Grid Monitor supports broad hourly history; MVP target is a minimum three-year backfill.",
    "demand_fred_g17": "FRED backfills should store release vintages where available, not just the latest values.",
    "demand_fred_new_residential_construction": "FRED housing backfills should store release vintages where available, not just the latest values.",
    "demand_usda_wasde": "USDA PSD/WASDE history is public and should be backfilled to support multi-year revisions and YoY comparisons.",
    "demand_usda_export_sales": (
        "The current USDA export-sales adapter still relies on the live static report feed, so historical backfill remains partial "
        "until the archive route is wired."
    ),
    "demand_ember_monthly_electricity": "Ember monthly electricity history is suitable for multi-year backfill and monthly trend calculations.",
}


def demandwatch_default_from_date(to_date: date) -> date:
    target_year = to_date.year - MIN_DEMANDWATCH_BACKFILL_YEARS
    try:
        return date(target_year, to_date.month, to_date.day)
    except ValueError:
        return date(target_year, to_date.month, 28)


def describe_demandwatch_backfill_scope(source: str, from_date: date, to_date: date) -> str:
    note = DEMANDWATCH_BACKFILL_NOTES.get(source)
    window = f"{from_date.isoformat()} -> {to_date.isoformat()}"
    if note is None:
        return window
    return f"{window} | {note}"
