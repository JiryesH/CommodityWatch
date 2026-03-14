from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError
from urllib.request import urlopen

from headline_associations import RelatedHeadlineService
from server import AppConfig, create_server


SERIES_ROWS = [
    {
        "series_key": "crude_oil_brent",
        "target_concept": "Brent Crude Oil",
        "actual_series_name": "Crude Oil Prices: Brent - Europe",
        "benchmark_series": "Brent",
    },
    {
        "series_key": "crude_oil_wti",
        "target_concept": "WTI Crude Oil",
        "actual_series_name": "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
        "benchmark_series": "WTI",
    },
    {
        "series_key": "crude_oil_dubai",
        "target_concept": "Dubai / Oman Crude Oil",
        "actual_series_name": "Global price of Dubai Crude",
        "benchmark_series": "Dubai / Oman",
    },
    {
        "series_key": "natural_gas_henry_hub",
        "target_concept": "Henry Hub Natural Gas",
        "actual_series_name": "Natural Gas Spot Price at Henry Hub",
        "benchmark_series": "Henry Hub",
    },
    {
        "series_key": "natural_gas_ttf",
        "target_concept": "TTF Natural Gas",
        "actual_series_name": "Dutch TTF Natural Gas Forward",
        "benchmark_series": "TTF",
    },
    {
        "series_key": "lng_asia_japan_import_proxy",
        "target_concept": "JKM LNG",
        "actual_series_name": "Global price of LNG, Asia",
        "benchmark_series": "JKM LNG",
    },
    {
        "series_key": "gold_worldbank_monthly",
        "target_concept": "Gold",
        "actual_series_name": "Gold",
        "benchmark_series": "Gold",
    },
    {
        "series_key": "copper_worldbank_monthly",
        "target_concept": "Copper",
        "actual_series_name": "Global price of Copper",
        "benchmark_series": "Copper",
    },
    {
        "series_key": "wheat_global_monthly_proxy",
        "target_concept": "Wheat",
        "actual_series_name": "Global wheat price",
        "benchmark_series": "Wheat",
    },
    {
        "series_key": "heating_oil_no2_nyharbor",
        "target_concept": "Diesel",
        "actual_series_name": "No. 2 Heating Oil Spot Price",
        "benchmark_series": "Heating Oil",
    },
]


def write_feed_fixture(feed_path: Path) -> None:
    payload = {
        "metadata": {"generated_at": "2026-03-09T10:30:00+00:00"},
        "articles": [
            {
                "id": "brent-1",
                "title": "Brent widens premium as Atlantic crude tightens",
                "description": "Spot traders tracked Brent crude gains through the European session.",
                "link": "https://example.test/brent-1",
                "published": "2026-03-09T10:30:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Crude",
                "categories": ["Oil - Crude"],
            },
            {
                "id": "wti-1",
                "title": "WTI breaks through $90/bl intraday",
                "description": "US crude futures rallied after supply outages intensified.",
                "link": "https://example.test/wti-1",
                "published": "2026-03-09T09:30:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Crude",
                "categories": ["Oil - Crude"],
            },
            {
                "id": "dubai-1",
                "title": "Platts cash Dubai differential surges past $10/b premium",
                "description": "Middle East sour crude values remained elevated.",
                "link": "https://example.test/dubai-1",
                "published": "2026-03-09T08:30:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Crude",
                "categories": ["Oil - Crude"],
            },
            {
                "id": "henry-1",
                "title": "US gas futures gains capped by bearish demand",
                "description": "Henry Hub prices eased as traders weighed storage builds and weather.",
                "link": "https://example.test/henry-1",
                "published": "2026-03-09T07:30:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Natural Gas",
                "categories": ["Natural Gas"],
            },
            {
                "id": "ttf-1",
                "title": "ICIS TTF Early Day assessments: 9 March 2026",
                "description": "Dutch hub pricing opened firmer on prompt demand.",
                "link": "https://example.test/ttf-1",
                "published": "2026-03-09T06:30:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Natural Gas",
                "categories": ["Natural Gas", "LNG"],
            },
            {
                "id": "brent-2",
                "title": "FACTBOX: Oil futures cross $110/b as Middle East conflict hits production",
                "description": "Brent futures led gains during Asian trading after the latest disruption.",
                "link": "https://example.test/brent-2",
                "published": "2026-03-08T12:00:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Shipping",
                "categories": ["Shipping"],
            },
            {
                "id": "generic-crude",
                "title": "Crude summary: benchmark prices climb again",
                "description": "Oil markets rallied on supply concerns, but no single marker dominated.",
                "link": "https://example.test/generic-crude",
                "published": "2026-03-08T11:00:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Crude",
                "categories": ["Oil - Crude"],
            },
            {
                "id": "oman-only",
                "title": "Drone attacks test Oman's bid as Hormuz bypass",
                "description": "Oman export routes remain under pressure after another security incident.",
                "link": "https://example.test/oman-only",
                "published": "2026-03-08T10:00:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Crude",
                "categories": ["Oil - Crude"],
            },
            {
                "id": "golden-pass",
                "title": "Golden Pass steps closer to LNG with FERC request",
                "description": "US LNG export capacity could expand if construction continues on schedule.",
                "link": "https://example.test/golden-pass",
                "published": "2026-03-08T09:00:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "LNG",
                "categories": ["LNG"],
            },
            {
                "id": "lng-1",
                "title": "Bangladesh rushes to secure lost April LNG supply",
                "description": "",
                "link": "https://example.test/lng-1",
                "published": "2026-03-09T06:14:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "LNG",
                "categories": ["LNG"],
            },
            {
                "id": "lng-2",
                "title": "The US LNG arb is wide open to Asia - or is it?",
                "description": "",
                "link": "https://example.test/lng-2",
                "published": "2026-03-05T15:54:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "LNG",
                "categories": ["LNG"],
            },
            {
                "id": "lng-3",
                "title": "NWE LNG swings to premium over TTF as markets rally on Middle East conflict",
                "description": "",
                "link": "https://example.test/lng-3",
                "published": "2026-03-02T19:22:48+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Natural Gas",
                "categories": ["Natural Gas", "LNG"],
            },
            {
                "id": "lng-false-positive",
                "title": "Asia's EVA prices surge to two-year highs on feedstock gains while output uncertainty looms",
                "description": "Prices in China and southeast Asia are at levels last seen in 2024.",
                "link": "https://example.test/lng-false-positive",
                "published": "2026-03-09T11:05:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Chemicals",
                "categories": ["Chemicals"],
            },
            {
                "id": "copper-1",
                "title": "Australia's BHP to invest in copper projects",
                "description": "",
                "link": "https://example.test/copper-1",
                "published": "2026-02-17T06:00:00+00:00",
                "source": "Argus Media",
                "feed": "Argus Media",
                "category": "Metals",
                "categories": ["Metals"],
            },
            {
                "id": "copper-2",
                "title": "Rio Tinto raises global copper output in 2025",
                "description": "",
                "link": "https://example.test/copper-2",
                "published": "2026-01-21T03:48:00+00:00",
                "source": "Argus Media",
                "feed": "Argus Media",
                "category": "Metals",
                "categories": ["Metals"],
            },
            {
                "id": "wheat-1",
                "title": "Canada wheat exports resume record pace",
                "description": "",
                "link": "https://example.test/wheat-1",
                "published": "2026-01-23T22:25:00+00:00",
                "source": "Argus Media",
                "feed": "Argus Media",
                "category": "Agriculture",
                "categories": ["Agriculture"],
            },
            {
                "id": "diesel-1",
                "title": "Domestic heating oil, diesel demand diverges in Germany",
                "description": "",
                "link": "https://example.test/diesel-1",
                "published": "2026-02-09T10:35:00+00:00",
                "source": "ICIS",
                "feed": "ICIS",
                "category": "Oil - Refined Products",
                "categories": ["Oil - Refined Products"],
            },
        ],
    }
    feed_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def create_series_database(database_path: Path) -> None:
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
                    row["series_key"],
                    row["target_concept"],
                    row["actual_series_name"],
                    row["benchmark_series"],
                    "exact",
                    "FRED",
                    f"code-{row['series_key']}",
                    f"https://fred.example/{row['series_key']}",
                    "daily",
                    "USD",
                    "USD",
                    "Global",
                    1,
                    "Fixture series",
                    "2026-03-09T10:30:00Z",
                )
                for row in SERIES_ROWS
            ],
        )
        connection.commit()
    finally:
        connection.close()


@contextmanager
def running_server(database_path: Path, feed_path: Path) -> Iterator[str]:
    calendar_database_path = database_path.parent / "calendarwatch.db"
    config = AppConfig(
        app_root=Path(__file__).resolve().parents[1],
        backend_root=Path(__file__).resolve().parents[1],
        database_url=f"sqlite:///{database_path}",
        calendar_database_url=f"sqlite:///{calendar_database_path}",
        host="127.0.0.1",
        port=0,
        headline_feed_path=feed_path,
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


def test_related_headline_service_matches_benchmarks_and_respects_active_series(tmp_path: Path) -> None:
    feed_path = tmp_path / "feed.json"
    write_feed_fixture(feed_path)
    service = RelatedHeadlineService(feed_path)

    brent_titles = [row["title"] for row in service.list_related(SERIES_ROWS[0])]
    wti_titles = [row["title"] for row in service.list_related(SERIES_ROWS[1])]
    dubai_titles = [row["title"] for row in service.list_related(SERIES_ROWS[2])]
    henry_titles = [row["title"] for row in service.list_related(SERIES_ROWS[3])]
    ttf_titles = [row["title"] for row in service.list_related(SERIES_ROWS[4])]
    lng_titles = [row["title"] for row in service.list_related(SERIES_ROWS[5])]
    copper_titles = [row["title"] for row in service.list_related(SERIES_ROWS[7])]
    wheat_titles = [row["title"] for row in service.list_related(SERIES_ROWS[8])]
    diesel_titles = [row["title"] for row in service.list_related(SERIES_ROWS[9])]

    assert brent_titles == [
        "Brent widens premium as Atlantic crude tightens",
        "FACTBOX: Oil futures cross $110/b as Middle East conflict hits production",
    ]
    assert wti_titles == ["WTI breaks through $90/bl intraday"]
    assert dubai_titles == ["Platts cash Dubai differential surges past $10/b premium"]
    assert henry_titles == ["US gas futures gains capped by bearish demand"]
    assert ttf_titles == [
        "ICIS TTF Early Day assessments: 9 March 2026",
        "NWE LNG swings to premium over TTF as markets rally on Middle East conflict",
    ]
    assert lng_titles == [
        "Bangladesh rushes to secure lost April LNG supply",
        "Golden Pass steps closer to LNG with FERC request",
        "The US LNG arb is wide open to Asia - or is it?",
        "NWE LNG swings to premium over TTF as markets rally on Middle East conflict",
    ]
    assert copper_titles == [
        "Australia's BHP to invest in copper projects",
        "Rio Tinto raises global copper output in 2025",
    ]
    assert wheat_titles == ["Canada wheat exports resume record pace"]
    assert diesel_titles == ["Domestic heating oil, diesel demand diverges in Germany"]

    assert "WTI breaks through $90/bl intraday" not in brent_titles
    assert "Drone attacks test Oman's bid as Hormuz bypass" not in dubai_titles
    assert "Crude summary: benchmark prices climb again" not in brent_titles


def test_related_headline_service_returns_empty_for_unmatched_series_and_blocks_gold_false_positive(
    tmp_path: Path,
) -> None:
    feed_path = tmp_path / "feed.json"
    write_feed_fixture(feed_path)
    service = RelatedHeadlineService(feed_path)

    gold_titles = [row["title"] for row in service.list_related(SERIES_ROWS[6])]
    assert "Golden Pass steps closer to LNG with FERC request" not in gold_titles
    assert "Asia's EVA prices surge to two-year highs on feedstock gains while output uncertainty looms" not in gold_titles
    assert gold_titles == []


def test_related_headlines_endpoint_returns_latest_rows_and_meta(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    feed_path = tmp_path / "feed.json"
    create_series_database(database_path)
    write_feed_fixture(feed_path)

    with running_server(database_path, feed_path) as base_url:
        payload = get_json(f"{base_url}/api/commodities/crude_oil_brent/headlines?limit=1")

    assert payload["meta"]["series_key"] == "crude_oil_brent"
    assert payload["meta"]["limit"] == 1
    assert payload["data"] == [
        {
            "id": "brent-1",
            "title": "Brent widens premium as Atlantic crude tightens",
            "source": "ICIS",
            "published": "2026-03-09T10:30:00+00:00",
            "link": "https://example.test/brent-1",
        }
    ]


def test_related_headlines_endpoint_404s_for_unknown_series(tmp_path: Path) -> None:
    database_path = tmp_path / "commodities.db"
    feed_path = tmp_path / "feed.json"
    create_series_database(database_path)
    write_feed_fixture(feed_path)

    with running_server(database_path, feed_path) as base_url:
        try:
            urlopen(f"{base_url}/api/commodities/not-a-series/headlines")
        except HTTPError as error:
            assert error.code == 404
            payload = json.load(error)
        else:  # pragma: no cover
            raise AssertionError("Expected a 404 response for an unknown series")

    assert payload["data"]["error"] == "Series not found: not-a-series"
