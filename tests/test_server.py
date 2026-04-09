from __future__ import annotations

import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import pytest
from sqlalchemy import insert

from calendar_pipeline.storage import CalendarRepository, calendar_events, create_calendar_engine
from inventory_watch_published_db import publish_inventory_store
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


def create_fixture_inventory_archive(data_root: Path) -> None:
    seed_dir = data_root / "seed" / "indicators"
    eia_dir = data_root / "artifacts" / "eia" / "2026" / "03" / "28"
    agsi_dir = data_root / "artifacts" / "agsi" / "2026" / "03" / "28"
    seed_dir.mkdir(parents=True, exist_ok=True)
    eia_dir.mkdir(parents=True, exist_ok=True)
    agsi_dir.mkdir(parents=True, exist_ok=True)

    (seed_dir / "inventorywatch.yml").write_text(
        """
- code: EIA_NATURAL_GAS_US_WORKING_STORAGE
  name: EIA US Natural Gas Working Gas in Storage
  description: Weekly working natural gas in underground storage for the Lower 48.
  measure_family: stock
  frequency: weekly
  commodity_code: natural_gas
  geography_code: US
  source_slug: eia
  source_series_key: NG.NW2_EPG0_SWO_R48_BCF.W
  native_unit_code: bcf
  canonical_unit_code: bcf
  default_observation_kind: actual
  seasonal_profile: inventorywatch_5y
  is_seasonal: true
  is_derived: false
  visibility_tier: public
  modules: [inventorywatch]
  primary_module: inventorywatch
  metadata:
    release_slug: eia_wngs
- code: GIE_NATURAL_GAS_EU_TOTAL_STORAGE
  name: GIE AGSI+ EU Total Gas Storage
  description: Daily EU aggregate gas in storage from GIE AGSI+.
  measure_family: stock
  frequency: daily
  commodity_code: natural_gas
  geography_code: EU
  source_slug: agsi
  source_series_key: eu
  native_unit_code: twh
  canonical_unit_code: twh
  default_observation_kind: actual
  seasonal_profile: inventorywatch_5y_daily
  is_seasonal: true
  is_derived: false
  visibility_tier: public
  modules: [inventorywatch]
  primary_module: inventorywatch
  metadata:
    release_slug: agsi_daily
    metric: gasInStorage
""".strip(),
        encoding="utf-8",
    )

    (eia_dir / "eia_wngs-080629655129.json").write_text(
        json.dumps(
            {
                "request": {
                    "params": {
                        "facets[series][]": "NW2_EPG0_SWO_R48_BCF",
                    }
                },
                "response": {
                    "data": [
                        {"period": "2026-03-20", "value": 1829},
                        {"period": "2026-03-13", "value": 1883},
                        {"period": "2026-03-06", "value": 1914},
                        {"period": "2026-02-27", "value": 1942},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    (agsi_dir / "agsi_daily-081136914796.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "code": "eu",
                        "gasDayStart": "2026-03-26",
                        "gasDayEnd": "2026-03-27",
                        "gasInStorage": "323.3437",
                        "updatedAt": "2026-03-27 19:00:42",
                    },
                    {
                        "code": "eu",
                        "gasDayStart": "2026-03-25",
                        "gasDayEnd": "2026-03-26",
                        "gasInStorage": "324.9758",
                        "updatedAt": "2026-03-27 19:00:39",
                    },
                    {
                        "code": "eu",
                        "gasDayStart": "2026-03-24",
                        "gasDayEnd": "2026-03-25",
                        "gasInStorage": "325.0861",
                        "updatedAt": "2026-03-27 19:00:36",
                    },
                    {
                        "code": "eu",
                        "gasDayStart": "2025-03-26",
                        "gasDayEnd": "2025-03-27",
                        "gasInStorage": "355.0000",
                        "updatedAt": "2025-03-27 19:00:36",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


@contextmanager
def running_inventory_backend() -> Iterator[str]:
    class InventoryBackendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)

            if parsed.path == "/api/health":
                payload = {"status": "ok", "database": True}
            elif parsed.path == "/api/snapshot/inventorywatch":
                query = parse_qs(parsed.query)
                payload = {
                    "module": "inventorywatch",
                    "generated_at": "2026-03-28T08:00:00Z",
                    "expires_at": "2026-03-28T08:05:00Z",
                    "cards": [
                        {
                            "indicator_id": "test-indicator",
                            "code": "EIA_TEST",
                            "name": "Fixture Inventory Indicator",
                            "commodity_code": "natural_gas",
                            "geography_code": None,
                            "latest_value": 123.4,
                            "unit": "kb",
                            "frequency": "weekly",
                            "change_abs": -8.0,
                            "deviation_abs": -12.0,
                            "signal": "tightening",
                            "sparkline": [131.4, 123.4],
                            "latest_period_end_at": "2026-03-21T00:00:00Z",
                            "latest_release_date": "2026-03-26T14:30:00Z",
                            "commoditywatch_updated_at": "2026-03-26T15:05:00Z",
                            "stale": False,
                        }
                    ],
                    "path": self.path,
                    "query": query,
                }
            elif parsed.path.endswith("/latest"):
                payload = {
                    "indicator": {"id": "test-indicator", "code": "EIA_TEST"},
                    "latest": {
                        "period_end_at": "2026-03-21T00:00:00Z",
                        "release_date": "2026-03-26T14:30:00Z",
                        "commoditywatch_updated_at": "2026-03-26T15:05:00Z",
                        "value": 123.4,
                        "unit": "kb",
                        "change_from_prior_abs": -8.0,
                        "change_from_prior_pct": -6.1,
                        "deviation_from_seasonal_abs": -12.0,
                        "deviation_from_seasonal_zscore": -1.5,
                        "revision_sequence": 1,
                    },
                    "path": self.path,
                }
            elif parsed.path.endswith("/data"):
                payload = {
                    "indicator": {
                        "id": "test-indicator",
                        "code": "EIA_TEST",
                        "name": "Fixture Inventory Indicator",
                        "description": None,
                        "modules": ["inventorywatch"],
                        "commodity_code": "natural_gas",
                        "geography_code": None,
                        "frequency": "weekly",
                        "measure_family": "stocks",
                        "unit": "kb",
                    },
                    "series": [],
                    "seasonal_range": [],
                    "metadata": {
                        "latest_release_id": None,
                        "latest_release_at": "2026-03-26T14:30:00Z",
                        "latest_period_end_at": "2026-03-21T00:00:00Z",
                        "latest_vintage_at": "2026-03-26T15:05:00Z",
                        "source_url": "https://inventory.example/test",
                    },
                    "path": self.path,
                }
            elif parsed.path == "/api/indicators":
                payload = {
                    "items": [],
                    "next_cursor": None,
                    "path": self.path,
                }
            else:
                payload = {"detail": f"Unhandled fixture path: {self.path}"}
                self.send_response(404)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                body = json.dumps(payload).encode("utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), InventoryBackendHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@contextmanager
def running_demandwatch_backend(next_releases_payload: dict | None = None) -> Iterator[str]:
    payload = next_releases_payload or {"generated_at": "2026-03-12T00:00:00Z", "items": []}

    class DemandWatchBackendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)

            if parsed.path == "/api/health":
                response_payload = {"ok": True}
                status = 200
            elif parsed.path == "/api/demandwatch/next-release-dates":
                response_payload = payload
                status = 200
            else:
                response_payload = {"detail": f"Unhandled fixture path: {self.path}"}
                status = 404

            body = json.dumps(response_payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), DemandWatchBackendHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@contextmanager
def running_server(
    database_path: Path,
    *,
    inventory_api_base_url: str = "http://127.0.0.1:8000/api",
    demandwatch_api_base_url: str = "http://127.0.0.1:8000/api",
    inventory_published_db_path: Path | None = None,
    inventory_data_root: Path | None = None,
    inventory_browse_mode: str = "auto",
) -> Iterator[str]:
    calendar_database_path = database_path.parent / "calendarwatch.db"
    config = AppConfig(
        app_root=Path(__file__).resolve().parents[1],
        backend_root=database_path.parent,
        database_url=f"sqlite:///{database_path}",
        calendar_database_url=f"sqlite:///{calendar_database_path}",
        inventory_published_db_path=inventory_published_db_path,
        inventory_data_root=inventory_data_root,
        inventory_api_base_url=inventory_api_base_url,
        demandwatch_api_base_url=demandwatch_api_base_url,
        inventory_browse_mode=inventory_browse_mode,
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
    for href in ("/", "/headline-watch/", "/price-watch/", "/calendar-watch/", "/inventory-watch/", "/demand-watch/"):
        assert f'href="{href}"' in html
    assert html.count('class="site-tab"') == 6
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


def test_headline_watch_imports_category_contract_constants(tmp_path: Path) -> None:
    html = (Path(__file__).resolve().parents[1] / "headline-watch" / "index.html").read_text(encoding="utf-8")

    taxonomy_import = re.search(
        r'import\s*\{(?P<names>.*?)\}\s*from\s*"\.\./shared/headline-taxonomy\.js";',
        html,
        re.DOTALL,
    )

    assert taxonomy_import is not None
    assert "CANONICAL_CATEGORIES" in taxonomy_import.group("names")


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


def test_inventory_watch_route_serves_page(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/inventory-watch/")

    assert "<title>CommodityWatch | InventoryWatch</title>" in html
    assert "/inventory-watch/app.js" in html
    assert_product_nav(html, current_href="/inventory-watch/")


def test_demand_watch_route_serves_page(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/demand-watch/")

    assert "<title>CommodityWatch | DemandWatch</title>" in html
    assert 'id="demand-root"' in html
    assert 'src="app.js"' in html
    assert_product_nav(html, current_href="/demand-watch/")


def test_inventory_watch_nested_route_serves_inventory_shell(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path) as base_url:
        html = get_text(f"{base_url}/inventory-watch/natural-gas/test-indicator/")

    assert "<title>CommodityWatch | InventoryWatch</title>" in html
    assert "/inventory-watch/styles.css" in html
    assert_product_nav(html, current_href="/inventory-watch/")


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
        inventory_api_base_url="http://127.0.0.1:65534/api",
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
    assert health["data"]["inventory_api_available"] is False
    assert "HeadlineWatch" in html


def test_inventory_api_routes_proxy_backend_contracts(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_inventory_backend() as inventory_api_base_url:
        with running_server(
            database_path,
            inventory_api_base_url=inventory_api_base_url,
            inventory_browse_mode="remote",
        ) as base_url:
            snapshot_payload = get_json(f"{base_url}/api/snapshot/inventorywatch?commodity=natural_gas&limit=7")
            latest_payload = get_json(f"{base_url}/api/indicators/test-indicator/latest")
            data_payload = get_json(f"{base_url}/api/indicators/test-indicator/data?include_seasonal=true")
            health_payload = get_json(f"{base_url}/api/health")

    assert snapshot_payload["module"] == "inventorywatch"
    assert snapshot_payload["path"] == "/api/snapshot/inventorywatch?commodity=natural_gas&limit=7"
    assert snapshot_payload["cards"][0]["latest_period_end_at"] == "2026-03-21T00:00:00Z"
    assert snapshot_payload["cards"][0]["latest_release_date"] == "2026-03-26T14:30:00Z"
    assert snapshot_payload["cards"][0]["commoditywatch_updated_at"] == "2026-03-26T15:05:00Z"
    assert latest_payload["latest"]["unit"] == "kb"
    assert latest_payload["latest"]["commoditywatch_updated_at"] == "2026-03-26T15:05:00Z"
    assert latest_payload["path"] == "/api/indicators/test-indicator/latest"
    assert data_payload["indicator"]["name"] == "Fixture Inventory Indicator"
    assert data_payload["metadata"]["latest_period_end_at"] == "2026-03-21T00:00:00Z"
    assert data_payload["metadata"]["latest_vintage_at"] == "2026-03-26T15:05:00Z"
    assert data_payload["path"] == "/api/indicators/test-indicator/data?include_seasonal=true"
    assert health_payload["data"]["inventory_api_available"] is True
    assert health_payload["data"]["inventory_api_base_url"] == inventory_api_base_url
    assert health_payload["data"]["inventory_browse_mode"] == "remote"
    assert health_payload["data"]["inventory_api_mode"] == "remote-proxy"


def test_inventory_api_prefers_local_archive_for_browsing_when_both_sources_exist(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    inventory_data_root = tmp_path / "inventory-backend"
    create_fixture_database(database_path)
    create_fixture_inventory_archive(inventory_data_root)

    with running_inventory_backend() as inventory_api_base_url:
        with running_server(
            database_path,
            inventory_api_base_url=inventory_api_base_url,
            inventory_data_root=inventory_data_root,
        ) as base_url:
            snapshot_payload = get_json(f"{base_url}/api/snapshot/inventorywatch")
            latest_payload = get_json(f"{base_url}/api/indicators/EIA_NATURAL_GAS_US_WORKING_STORAGE/latest")
            health_payload = get_json(f"{base_url}/api/health")

    assert [card["code"] for card in snapshot_payload["cards"]] == [
        "EIA_NATURAL_GAS_US_WORKING_STORAGE",
        "GIE_NATURAL_GAS_EU_TOTAL_STORAGE",
    ]
    assert latest_payload["indicator"]["id"] == "EIA_NATURAL_GAS_US_WORKING_STORAGE"
    assert health_payload["data"]["inventory_browse_mode"] == "auto"
    assert health_payload["data"]["inventory_api_mode"] == "local-published"


def test_inventory_api_uses_published_local_store_when_available(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    inventory_data_root = tmp_path / "inventory-backend"
    inventory_published_db_path = tmp_path / "inventorywatch.db"
    create_fixture_database(database_path)
    create_fixture_inventory_archive(inventory_data_root)
    publish_inventory_store(inventory_data_root, inventory_published_db_path)

    with running_server(
        database_path,
        inventory_api_base_url="http://127.0.0.1:65534/api",
        inventory_published_db_path=inventory_published_db_path,
    ) as base_url:
        snapshot_payload = get_json(f"{base_url}/api/snapshot/inventorywatch")
        latest_payload = get_json(f"{base_url}/api/indicators/EIA_NATURAL_GAS_US_WORKING_STORAGE/latest")
        health_payload = get_json(f"{base_url}/api/health")

    assert snapshot_payload["module"] == "inventorywatch"
    assert snapshot_payload["cards"][0]["latest_period_end_at"].startswith("2026-03-20T00:00:00")
    assert snapshot_payload["cards"][0]["latest_release_date"] is not None
    assert latest_payload["indicator"]["id"] == "EIA_NATURAL_GAS_US_WORKING_STORAGE"
    assert latest_payload["latest"]["commoditywatch_updated_at"] is not None
    assert health_payload["data"]["inventory_api_mode"] == "local-published"
    assert health_payload["data"]["inventory_local_store_kind"] == "published-db"
    assert health_payload["data"]["inventory_published_db_path"] == str(inventory_published_db_path)


def test_inventory_api_routes_fallback_to_local_archive_when_backend_unavailable(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    inventory_data_root = tmp_path / "inventory-backend"
    create_fixture_database(database_path)
    create_fixture_inventory_archive(inventory_data_root)

    with running_server(
        database_path,
        inventory_api_base_url="http://127.0.0.1:65534/api",
        inventory_data_root=inventory_data_root,
    ) as base_url:
        snapshot_payload = get_json(f"{base_url}/api/snapshot/inventorywatch")
        latest_payload = get_json(f"{base_url}/api/indicators/EIA_NATURAL_GAS_US_WORKING_STORAGE/latest")
        data_payload = get_json(f"{base_url}/api/indicators/GIE_NATURAL_GAS_EU_TOTAL_STORAGE/data?limit_points=10")
        health_payload = get_json(f"{base_url}/api/health")

    assert snapshot_payload["module"] == "inventorywatch"
    assert [card["code"] for card in snapshot_payload["cards"]] == [
        "EIA_NATURAL_GAS_US_WORKING_STORAGE",
        "GIE_NATURAL_GAS_EU_TOTAL_STORAGE",
    ]
    assert latest_payload["indicator"]["id"] == "EIA_NATURAL_GAS_US_WORKING_STORAGE"
    assert latest_payload["latest"]["unit"] == "bcf"
    assert data_payload["indicator"]["name"] == "GIE AGSI+ EU Total Gas Storage"
    assert len(data_payload["series"]) == 3
    assert health_payload["data"]["inventory_api_available"] is True
    assert health_payload["data"]["inventory_api_mode"] == "local-published"
    assert health_payload["data"]["inventory_archive_available"] is True
    assert health_payload["data"]["inventory_archive_has_data"] is True


def test_inventory_api_remote_mode_requires_backend_even_when_local_archive_exists(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    inventory_data_root = tmp_path / "inventory-backend"
    create_fixture_database(database_path)
    create_fixture_inventory_archive(inventory_data_root)

    with running_server(
        database_path,
        inventory_api_base_url="http://127.0.0.1:65534/api",
        inventory_data_root=inventory_data_root,
        inventory_browse_mode="remote",
    ) as base_url:
        with pytest.raises(HTTPError) as error:
            urlopen(f"{base_url}/api/snapshot/inventorywatch")
        health_payload = get_json(f"{base_url}/api/health")

    assert error.value.code == 502
    payload = json.loads(error.value.read().decode("utf-8"))
    assert "InventoryWatch API unavailable" in payload["detail"]
    assert health_payload["data"]["inventory_browse_mode"] == "remote"
    assert health_payload["data"]["inventory_api_available"] is False
    assert health_payload["data"]["inventory_api_mode"] == "unavailable"
    assert health_payload["data"]["inventory_archive_has_data"] is True


def test_inventory_api_unavailable_returns_top_level_detail_when_no_remote_or_local(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    create_fixture_database(database_path)

    with running_server(database_path, inventory_api_base_url="http://127.0.0.1:65534/api") as base_url:
        with pytest.raises(HTTPError) as error:
            urlopen(f"{base_url}/api/snapshot/inventorywatch")

    assert error.value.code == 502
    payload = json.loads(error.value.read().decode("utf-8"))
    assert "InventoryWatch API unavailable" in payload["detail"]
    assert payload["target_url"] == "http://127.0.0.1:65534/api/snapshot/inventorywatch"


def test_build_config_rejects_invalid_port_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "not-a-number")

    with pytest.raises(ValueError, match="Invalid PORT"):
        build_config(tmp_path)


def test_build_config_prefers_local_synced_commodity_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    local_app_root = tmp_path / "commoditywatch"
    local_data_dir = local_app_root / "data"
    sibling_backend_dir = tmp_path / "Commodity Prices" / "data"
    local_data_dir.mkdir(parents=True)
    sibling_backend_dir.mkdir(parents=True)

    create_fixture_database(local_data_dir / "commodities.db")
    create_fixture_database(sibling_backend_dir / "commodities.db")

    monkeypatch.delenv("COMMODITY_BACKEND_ROOT", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    config = build_config(local_app_root)

    assert config.backend_root == local_app_root.resolve()
    assert str((local_data_dir / "commodities.db").resolve()) in config.database_url


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


def test_calendar_route_merges_demandwatch_release_dates_without_duplicate_events(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    calendar_database_path = tmp_path / "calendarwatch.db"
    create_fixture_database(database_path)
    create_fixture_calendar_database(calendar_database_path)

    next_releases_payload = {
        "generated_at": "2026-03-12T00:00:00Z",
        "items": [
            {
                "release_slug": "demand_eia_wpsr",
                "release_name": "EIA Weekly Petroleum Status Report",
                "source_slug": "eia",
                "source_name": "EIA",
                "cadence": "weekly",
                "schedule_timezone": "America/New_York",
                "vertical_ids": ["crude-products", "grains"],
                "vertical_codes": ["crude_products", "grains_oilseeds"],
                "series_codes": ["EIA_US_TOTAL_PRODUCT_SUPPLIED", "EIA_US_ETHANOL_PRODUCTION"],
                "scheduled_for": "2026-03-18T14:30:00Z",
                "latest_release_at": "2026-03-11T14:30:00Z",
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
                "schedule_timezone": "America/New_York",
                "vertical_ids": ["grains"],
                "vertical_codes": ["grains_oilseeds"],
                "series_codes": ["USDA_US_CORN_EXPORT_SALES", "USDA_US_SOYBEAN_EXPORT_SALES", "USDA_US_WHEAT_EXPORT_SALES"],
                "scheduled_for": "2026-03-19T12:30:00Z",
                "latest_release_at": "2026-03-12T12:30:00Z",
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
                "schedule_timezone": "America/New_York",
                "vertical_ids": ["base-metals"],
                "vertical_codes": ["base_metals"],
                "series_codes": ["FRED_US_TOTAL_VEHICLE_SALES"],
                "scheduled_for": "2026-03-20T12:30:00Z",
                "latest_release_at": "2026-02-27T13:30:00Z",
                "source_url": "https://fred.stlouisfed.org/series/TOTALSA",
                "is_estimated": True,
                "notes": [
                    "Next release time is estimated from cadence or latest observed release.",
                    "Calendar-driven release; confirm against CalendarWatch before publication.",
                ],
            },
        ],
    }

    with running_demandwatch_backend(next_releases_payload) as demandwatch_api_base_url:
        with running_server(database_path, demandwatch_api_base_url=demandwatch_api_base_url) as base_url:
            payload = get_json(f"{base_url}/api/calendar?from=2026-03-01&to=2026-03-31")
            agriculture_payload = get_json(f"{base_url}/api/calendar?from=2026-03-01&to=2026-03-31&sectors=agriculture")

    assert [event["id"] for event in payload["data"]] == [
        "cw_energy_1",
        "demand_usda_export_sales:2026-03-19",
        "demand_fred_motor_vehicle_sales:2026-03-20",
    ]

    export_sales_event = next(event for event in payload["data"] if event["id"] == "demand_usda_export_sales:2026-03-19")
    vehicle_sales_event = next(event for event in payload["data"] if event["id"] == "demand_fred_motor_vehicle_sales:2026-03-20")

    assert export_sales_event["commodity_sectors"] == ["agriculture"]
    assert export_sales_event["source_label"] == "USDA FAS"
    assert export_sales_event["is_confirmed"] is True
    assert export_sales_event["source_slug"] == "usda_export_sales"

    assert vehicle_sales_event["commodity_sectors"] == ["metals"]
    assert vehicle_sales_event["source_label"] == "FRED"
    assert vehicle_sales_event["is_confirmed"] is False
    assert "estimated" in (vehicle_sales_event["notes"] or "").lower()

    assert [event["id"] for event in agriculture_payload["data"]] == ["demand_usda_export_sales:2026-03-19"]


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
