from __future__ import annotations

from datetime import date

import server


def test_merge_demandwatch_release_dates_dedupes_and_maps_release_metadata(monkeypatch) -> None:
    existing_events = [
        {
            "id": "cw_energy_1",
            "name": "EIA Weekly Petroleum Status Report",
            "organiser": "U.S. Energy Information Administration",
            "cadence": "weekly",
            "commodity_sectors": ["energy"],
            "event_date": "2026-03-18T14:30:00+00:00",
            "calendar_url": "https://www.eia.gov/petroleum/supply/weekly/schedule.php",
            "redistribution_ok": True,
            "source_label": "EIA",
            "notes": "Fixture publishable event",
            "is_confirmed": True,
            "source_slug": "eia",
            "ingestion_pattern": "html",
            "publish_status": "published",
            "review_reasons": [],
            "updated_at": "2026-03-12T00:00:00+00:00",
        }
    ]

    monkeypatch.setattr(
        server,
        "fetch_json_response",
        lambda _url, timeout=2.0: {
            "generated_at": "2026-03-12T00:00:00Z",
            "items": [
                {
                    "release_slug": "demand_eia_wpsr",
                    "release_name": "EIA Weekly Petroleum Status Report",
                    "source_slug": "eia",
                    "source_name": "EIA",
                    "cadence": "weekly",
                    "vertical_codes": ["crude_products", "grains_oilseeds"],
                    "scheduled_for": "2026-03-18T14:30:00Z",
                    "source_url": "https://www.eia.gov/petroleum/supply/weekly/",
                    "is_estimated": False,
                    "notes": [],
                },
                {
                    "release_slug": "demand_usda_export_sales",
                    "release_name": "USDA Export Sales Report",
                    "source_slug": "usda_export_sales",
                    "source_name": "USDA Export Sales",
                    "cadence": "weekly",
                    "vertical_codes": ["grains_oilseeds"],
                    "scheduled_for": "2026-03-19T12:30:00Z",
                    "source_url": "https://apps.fas.usda.gov/export-sales/esrd1.html",
                    "is_estimated": False,
                    "notes": [],
                },
                {
                    "release_slug": "demand_fred_motor_vehicle_sales",
                    "release_name": "US Total Vehicle Sales",
                    "source_slug": "fred",
                    "source_name": "FRED",
                    "cadence": "monthly",
                    "vertical_codes": ["base_metals"],
                    "scheduled_for": "2026-03-20T12:30:00Z",
                    "source_url": "https://fred.stlouisfed.org/series/TOTALSA",
                    "is_estimated": True,
                    "notes": [
                        "Next release time is estimated from cadence or latest observed release.",
                        "Calendar-driven release; confirm against CalendarWatch before publication.",
                    ],
                },
            ],
        },
    )

    merged = server._merge_demandwatch_release_dates(
        existing_events,
        demandwatch_api_base_url="http://127.0.0.1:8000/api",
        from_date=date(2026, 3, 1),
        to_date=date(2026, 3, 31),
        sectors=None,
    )

    assert [event["id"] for event in merged] == [
        "cw_energy_1",
        "demand_usda_export_sales:2026-03-19",
        "demand_fred_motor_vehicle_sales:2026-03-20",
    ]

    export_sales_event = next(event for event in merged if event["id"] == "demand_usda_export_sales:2026-03-19")
    vehicle_sales_event = next(event for event in merged if event["id"] == "demand_fred_motor_vehicle_sales:2026-03-20")

    assert export_sales_event["commodity_sectors"] == ["agriculture"]
    assert export_sales_event["source_label"] == "USDA FAS"
    assert export_sales_event["is_confirmed"] is True

    assert vehicle_sales_event["commodity_sectors"] == ["metals"]
    assert vehicle_sales_event["source_label"] == "FRED"
    assert vehicle_sales_event["is_confirmed"] is False
    assert "estimated" in (vehicle_sales_event["notes"] or "").lower()


def test_merge_demandwatch_release_dates_honors_sector_filter(monkeypatch) -> None:
    monkeypatch.setattr(
        server,
        "fetch_json_response",
        lambda _url, timeout=2.0: {
            "generated_at": "2026-03-12T00:00:00Z",
            "items": [
                {
                    "release_slug": "demand_usda_export_sales",
                    "release_name": "USDA Export Sales Report",
                    "source_slug": "usda_export_sales",
                    "source_name": "USDA Export Sales",
                    "cadence": "weekly",
                    "vertical_codes": ["grains_oilseeds"],
                    "scheduled_for": "2026-03-19T12:30:00Z",
                    "source_url": "https://apps.fas.usda.gov/export-sales/esrd1.html",
                    "is_estimated": False,
                    "notes": [],
                },
                {
                    "release_slug": "demand_fred_motor_vehicle_sales",
                    "release_name": "US Total Vehicle Sales",
                    "source_slug": "fred",
                    "source_name": "FRED",
                    "cadence": "monthly",
                    "vertical_codes": ["base_metals"],
                    "scheduled_for": "2026-03-20T12:30:00Z",
                    "source_url": "https://fred.stlouisfed.org/series/TOTALSA",
                    "is_estimated": True,
                    "notes": [],
                },
            ],
        },
    )

    merged = server._merge_demandwatch_release_dates(
        [],
        demandwatch_api_base_url="http://127.0.0.1:8000/api",
        from_date=date(2026, 3, 1),
        to_date=date(2026, 3, 31),
        sectors=["agriculture"],
    )

    assert [event["id"] for event in merged] == ["demand_usda_export_sales:2026-03-19"]


def test_merge_demandwatch_release_dates_dedupes_against_known_events_outside_sector_filter(monkeypatch) -> None:
    known_events = [
        {
            "id": "cw_energy_1",
            "name": "EIA Weekly Petroleum Status Report",
            "organiser": "U.S. Energy Information Administration",
            "cadence": "weekly",
            "commodity_sectors": ["energy"],
            "event_date": "2026-03-18T14:30:00+00:00",
            "calendar_url": "https://www.eia.gov/petroleum/supply/weekly/schedule.php",
            "redistribution_ok": True,
            "source_label": "EIA",
            "notes": "Fixture publishable event",
            "is_confirmed": True,
            "source_slug": "eia",
            "ingestion_pattern": "html",
            "publish_status": "published",
            "review_reasons": [],
            "updated_at": "2026-03-12T00:00:00+00:00",
        }
    ]

    monkeypatch.setattr(
        server,
        "fetch_json_response",
        lambda _url, timeout=2.0: {
            "generated_at": "2026-03-12T00:00:00Z",
            "items": [
                {
                    "release_slug": "demand_eia_wpsr",
                    "release_name": "EIA Weekly Petroleum Status Report",
                    "source_slug": "eia",
                    "source_name": "EIA",
                    "cadence": "weekly",
                    "vertical_codes": ["crude_products", "grains_oilseeds"],
                    "scheduled_for": "2026-03-18T14:30:00Z",
                    "source_url": "https://www.eia.gov/petroleum/supply/weekly/",
                    "is_estimated": False,
                    "notes": [],
                }
            ],
        },
    )

    merged = server._merge_demandwatch_release_dates(
        [],
        demandwatch_api_base_url="http://127.0.0.1:8000/api",
        from_date=date(2026, 3, 1),
        to_date=date(2026, 3, 31),
        sectors=["agriculture"],
        known_events=known_events,
    )

    assert merged == []
