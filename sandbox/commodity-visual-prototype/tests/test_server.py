from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib.request import urlopen

from server import AppConfig, create_server


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


@contextmanager
def running_server(database_path: Path) -> Iterator[str]:
    config = AppConfig(
        app_root=Path(__file__).resolve().parents[1],
        backend_root=database_path.parent,
        database_url=f"sqlite:///{database_path}",
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

