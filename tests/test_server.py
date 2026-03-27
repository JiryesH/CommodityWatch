from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator
from urllib.request import urlopen

import pytest
from sqlalchemy import insert

from calendar_pipeline.storage import CalendarRepository, calendar_events, create_calendar_engine
from server import AppConfig, build_config, create_server


def create_fixture_database(database_path: Path) -> None:
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            CREATE TABLE published_series (
                series_key TEXT PRIMARY KEY,
                target_concept TEXT NOT NULL,
                actual_series_name TEXT NOT NULL,
                benchmark_series TEXT,
                match_type TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_series_code TEXT NOT NULL,
                source_url TEXT,
                frequency TEXT NOT NULL,
                unit TEXT,
                currency TEXT,
                geography TEXT,
                active INTEGER NOT NULL,
                notes TEXT,
                updated_at TEXT
            );

            CREATE TABLE published_latest_observations (
                series_key TEXT PRIMARY KEY,
                target_concept TEXT NOT NULL,
                actual_series_name TEXT NOT NULL,
                benchmark_series TEXT,
                match_type TEXT NOT NULL,
                observation_date TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                currency TEXT,
                frequency TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_series_code TEXT NOT NULL,
                source_url TEXT,
                geography TEXT,
                updated_at TEXT,
                notes TEXT
            );

            CREATE TABLE published_observations (
                series_key TEXT NOT NULL,
                target_concept TEXT NOT NULL,
                actual_series_name TEXT NOT NULL,
                benchmark_series TEXT,
                match_type TEXT NOT NULL,
                observation_date TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                currency TEXT,
                frequency TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_series_code TEXT NOT NULL,
                source_url TEXT,
                geography TEXT,
                release_date TEXT,
                retrieved_at TEXT,
                raw_artifact_id INTEGER,
                inserted_at TEXT,
                updated_at TEXT,
                notes TEXT
            );
            """
        )

        connection.executemany(
            """
            INSERT INTO published_series (
                series_key,
                target_concept,
                actual_series_name,
                benchmark_series,
                match_type,
                source_name,
                source_series_code,
                source_url,
                frequency,
                unit,
                currency,
                geography,
                active,
                notes,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "lng_asia_japan_import_proxy",
                    "JKM LNG",
                    "Global price of LNG, Asia",
                    "JKM LNG",
                    "related",
                    "FRED",
                    "PNGASJPUSDM",
                    "https://fred.example/lng",
                    "monthly",
                    "USD per MMBtu",
                    "USD",
                    "Asia",
                    1,
                    "Related published series",
                    "2026-03-08T10:30:00Z",
                ),
                (
                    "gold_worldbank_monthly",
                    "Gold",
                    "Gold",
                    "Gold",
                    "exact",
                    "FRED",
                    "PGOLDUSDM",
                    "https://fred.example/gold",
                    "monthly",
                    "USD per troy ounce",
                    "USD",
                    "Global",
                    1,
                    "Exact published series",
                    "2026-03-08T10:30:00Z",
                ),
            ],
        )

        connection.executemany(
            """
            INSERT INTO published_latest_observations (
                series_key,
                target_concept,
                actual_series_name,
                benchmark_series,
                match_type,
                observation_date,
                value,
                unit,
                currency,
                frequency,
                source_name,
                source_series_code,
                source_url,
                geography,
                updated_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "lng_asia_japan_import_proxy",
                    "JKM LNG",
                    "Global price of LNG, Asia",
                    "JKM LNG",
                    "related",
                    "2026-02-01",
                    10.435,
                    "USD per MMBtu",
                    "USD",
                    "monthly",
                    "FRED",
                    "PNGASJPUSDM",
                    "https://fred.example/lng",
                    "Asia",
                    "2026-03-08T10:30:00Z",
                    "Related published series",
                ),
                (
                    "gold_worldbank_monthly",
                    "Gold",
                    "Gold",
                    "Gold",
                    "exact",
                    "2026-02-01",
                    2250.1,
                    "USD per troy ounce",
                    "USD",
                    "monthly",
                    "FRED",
                    "PGOLDUSDM",
                    "https://fred.example/gold",
                    "Global",
                    "2026-03-08T10:30:00Z",
                    "Exact published series",
                ),
            ],
        )

        connection.executemany(
            """
            INSERT INTO published_observations (
                series_key,
                target_concept,
                actual_series_name,
                benchmark_series,
                match_type,
                observation_date,
                value,
                unit,
                currency,
                frequency,
                source_name,
                source_series_code,
                source_url,
                geography,
                release_date,
                retrieved_at,
                raw_artifact_id,
                inserted_at,
                updated_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "lng_asia_japan_import_proxy",
                    "JKM LNG",
                    "Global price of LNG, Asia",
                    "JKM LNG",
                    "related",
                    "2025-12-01",
                    8.2,
                    "USD per MMBtu",
                    "USD",
                    "monthly",
                    "FRED",
                    "PNGASJPUSDM",
                    "https://fred.example/lng",
                    "Asia",
                    "2025-12-01",
                    "2026-03-08T10:30:00Z",
                    1,
                    "2026-03-08T10:30:00Z",
                    "2026-03-08T10:30:00Z",
                    "Related published series",
                ),
                (
                    "lng_asia_japan_import_proxy",
                    "JKM LNG",
                    "Global price of LNG, Asia",
                    "JKM LNG",
                    "related",
                    "2026-01-01",
                    9.8,
                    "USD per MMBtu",
                    "USD",
                    "monthly",
                    "FRED",
                    "PNGASJPUSDM",
                    "https://fred.example/lng",
                    "Asia",
                    "2026-01-01",
                    "2026-03-08T10:30:00Z",
                    2,
                    "2026-03-08T10:30:00Z",
                    "2026-03-08T10:30:00Z",
                    "Related published series",
                ),
                (
                    "lng_asia_japan_import_proxy",
                    "JKM LNG",
                    "Global price of LNG, Asia",
                    "JKM LNG",
                    "related",
                    "2026-02-01",
                    10.435,
                    "USD per MMBtu",
                    "USD",
                    "monthly",
                    "FRED",
                    "PNGASJPUSDM",
                    "https://fred.example/lng",
                    "Asia",
                    "2026-02-01",
                    "2026-03-08T10:30:00Z",
                    3,
                    "2026-03-08T10:30:00Z",
                    "2026-03-08T10:30:00Z",
                    "Related published series",
                ),
                (
                    "gold_worldbank_monthly",
                    "Gold",
                    "Gold",
                    "Gold",
                    "exact",
                    "2026-01-01",
                    2210.0,
                    "USD per troy ounce",
                    "USD",
                    "monthly",
                    "FRED",
                    "PGOLDUSDM",
                    "https://fred.example/gold",
                    "Global",
                    "2026-01-01",
                    "2026-03-08T10:30:00Z",
                    4,
                    "2026-03-08T10:30:00Z",
                    "2026-03-08T10:30:00Z",
                    "Exact published series",
                ),
                (
                    "gold_worldbank_monthly",
                    "Gold",
                    "Gold",
                    "Gold",
                    "exact",
                    "2026-02-01",
                    2250.1,
                    "USD per troy ounce",
                    "USD",
                    "monthly",
                    "FRED",
                    "PGOLDUSDM",
                    "https://fred.example/gold",
                    "Global",
                    "2026-02-01",
                    "2026-03-08T10:30:00Z",
                    5,
                    "2026-03-08T10:30:00Z",
                    "2026-03-08T10:30:00Z",
                    "Exact published series",
                ),
            ],
        )

        connection.commit()
    finally:
        connection.close()


def create_fixture_calendar_database(database_path: Path) -> None:
    repository = CalendarRepository(create_calendar_engine(f"sqlite:///{database_path}"))
    repository.ensure_schema()

    with repository.engine.begin() as connection:
        connection.execute(
            insert(calendar_events),
            [
                {
                    "id": "cw_energy_1",
                    "source_slug": "eia",
                    "source_item_key": "wpsr-2026-03-13",
                    "natural_key_hash": "energy-hash-1",
                    "name": "EIA Weekly Petroleum Status Report",
                    "organiser": "U.S. Energy Information Administration",
                    "cadence": "weekly",
                    "commodity_sectors": ["energy"],
                    "event_date": datetime.fromisoformat("2026-03-18T14:30:00+00:00"),
                    "calendar_url": "https://www.eia.gov/petroleum/supply/weekly/schedule.php",
                    "redistribution_ok": True,
                    "source_label": "EIA",
                    "notes": "Fixture publishable event",
                    "is_confirmed": True,
                    "ingestion_pattern": "html",
                    "publish_status": "published",
                    "requires_review": False,
                    "review_reasons": [],
                    "manual_publish_override": False,
                    "raw_payload": {"fixture": True},
                    "first_seen_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "last_seen_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "published_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "created_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "updated_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                },
                {
                    "id": "cw_macro_1",
                    "source_slug": "ons_rss",
                    "source_item_key": "ons-cpi-2026-03",
                    "natural_key_hash": "macro-hash-1",
                    "name": "ONS UK CPI Release",
                    "organiser": "Office for National Statistics",
                    "cadence": "monthly",
                    "commodity_sectors": ["macro"],
                    "event_date": datetime.fromisoformat("2026-03-25T07:00:00+00:00"),
                    "calendar_url": "https://www.ons.gov.uk/releases/consumerpriceinflationukfebruary2026",
                    "redistribution_ok": False,
                    "source_label": "ONS",
                    "notes": "Fixture gated event",
                    "is_confirmed": True,
                    "ingestion_pattern": "structured_feed",
                    "publish_status": "pending_review",
                    "requires_review": True,
                    "review_reasons": ["redistribution_unconfirmed"],
                    "manual_publish_override": False,
                    "raw_payload": {"fixture": True},
                    "first_seen_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "last_seen_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "published_at": None,
                    "created_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                    "updated_at": datetime.fromisoformat("2026-03-12T00:00:00+00:00"),
                },
            ],
        )


@contextmanager
def running_server(database_path: Path) -> Iterator[str]:
    calendar_database_path = database_path.parent / "calendarwatch.db"
    config = AppConfig(
        app_root=Path(__file__).resolve().parents[1],
        backend_root=database_path.parent,
        database_url=f"sqlite:///{database_path}",
        calendar_database_url=f"sqlite:///{calendar_database_path}",
        host="127.0.0.1",
        port=0,
    )
    server = create_server(config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get_json(url: str) -> dict:
    with urlopen(url) as response:
        assert response.status == 200
        return json.load(response)


def get_text(url: str) -> str:
    with urlopen(url) as response:
        assert response.status == 200
        return response.read().decode("utf-8")


def assert_product_nav(html: str, *, current_href: str) -> None:
    for href in ("/", "/headline-watch/", "/price-watch/", "/calendar-watch/"):
        assert f'href="{href}"' in html
    assert html.count('class="site-tab"') == 4
    assert f'href="{current_href}" aria-current="page"' in html
    assert "CommodityWatch<span class=\"dot\"></span>" in html


def test_root_route_serves_home_shell(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(base_url)

    assert "<title>CommodityWatch | Home</title>" in html
    assert "dashboard/app.js" in html
    assert_product_nav(html, current_href="/")


def test_headline_watch_route_serves_page(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/headline-watch/")

    assert "<title>CommodityWatch | HeadlineWatch</title>" in html
    assert_product_nav(html, current_href="/headline-watch/")


def test_price_watch_route_serves_page(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/price-watch/")

    assert "<title>CommodityWatch | PriceWatch</title>" in html
    assert_product_nav(html, current_href="/price-watch/")


def test_calendar_watch_route_serves_page(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/calendar-watch/")

    assert "<title>CommodityWatch | CalendarWatch</title>" in html
    assert_product_nav(html, current_href="/calendar-watch/")


def test_static_routes_disable_browser_caching(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        with urlopen(f"{base_url}/price-watch/") as response:
            assert response.status == 200
            assert response.headers["Cache-Control"] == "no-store"


def test_health_route_reports_missing_database_without_blocking_ui(tmp_path: Path) -> None:
    missing_database_path = tmp_path / "missing.db"
    config = AppConfig(
        app_root=Path(__file__).resolve().parents[1],
        backend_root=tmp_path,
        database_url=f"sqlite:///{missing_database_path}",
        calendar_database_url=f"sqlite:///{tmp_path / 'calendarwatch.db'}",
        host="127.0.0.1",
        port=0,
    )
    server = create_server(config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        health = get_json(f"{base_url}/api/health")
        html = get_text(base_url)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert health["data"]["commodity_api_available"] is False
    assert "Database file not found" in health["data"]["commodity_api_error"]
    assert "HeadlineWatch" in html


def test_build_config_rejects_invalid_port_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "not-a-number")

    with pytest.raises(ValueError, match="Invalid PORT"):
        build_config(tmp_path)


def test_calendar_route_returns_only_publishable_confirmed_events(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    calendar_database_path = tmp_path / "calendarwatch.db"
    create_fixture_database(database_path)
    create_fixture_calendar_database(calendar_database_path)

    with running_server(database_path) as base_url:
        payload = get_json(f"{base_url}/api/calendar?from=2026-03-01&to=2026-03-31")
        sector_payload = get_json(f"{base_url}/api/calendar?from=2026-03-01&to=2026-03-31&sectors=energy")
        macro_payload = get_json(f"{base_url}/api/calendar?from=2026-03-01&to=2026-03-31&sectors=macro")

    assert [event["id"] for event in payload["data"]] == ["cw_energy_1"]
    assert [event["id"] for event in sector_payload["data"]] == ["cw_energy_1"]
    assert macro_payload["data"] == []


def test_series_route_returns_published_catalog(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        payload = get_json(f"{base_url}/api/commodities/series")

    names = [row["actual_series_name"] for row in payload["data"]]
    assert names == ["Global price of LNG, Asia", "Gold"]
    assert payload["data"][0]["match_type"] == "related"
    assert payload["data"][0]["target_concept"] == "JKM LNG"


def test_latest_route_includes_previous_value_and_delta(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        payload = get_json(f"{base_url}/api/commodities/latest")

    lng_row = next(row for row in payload["data"] if row["series_key"] == "lng_asia_japan_import_proxy")
    assert lng_row["actual_series_name"] == "Global price of LNG, Asia"
    assert lng_row["previous_value"] == 9.8
    assert round(lng_row["delta_value"], 3) == 0.635
    assert round(lng_row["delta_pct"], 3) == round((0.635 / 9.8) * 100, 3)


def test_history_route_honors_date_filters(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        payload = get_json(
            f"{base_url}/api/commodities/lng_asia_japan_import_proxy/history?start=2026-01-01&end=2026-02-01"
        )

    assert [row["observation_date"] for row in payload["data"]] == ["2026-01-01", "2026-02-01"]
    assert all(row["series_key"] == "lng_asia_japan_import_proxy" for row in payload["data"])


def test_series_detail_route_returns_series_and_latest(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        payload = get_json(f"{base_url}/api/commodities/gold_worldbank_monthly")

    assert payload["data"]["series"]["actual_series_name"] == "Gold"
    assert payload["data"]["latest"]["observation_date"] == "2026-02-01"
