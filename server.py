#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal, TypedDict
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from calendar_pipeline.storage import CalendarRepository, create_calendar_engine
from feed_io import preferred_headline_feed_path
from headline_associations import RelatedHeadlineService, parse_headline_limit
from inventory_watch_local_api import LocalInventoryRepository
from inventory_watch_published_db import PublishedInventoryRepository


APP_ROOT = Path(__file__).resolve().parent
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
DEFAULT_DATABASE_URL = "sqlite:///data/commodities.db"
DEFAULT_CALENDAR_DATABASE_URL = "sqlite:///data/calendarwatch.db"
MatchType = Literal["exact", "related"]
InventoryBrowseMode = Literal["auto", "local", "remote"]


class PublishedSeriesRecord(TypedDict):
    series_key: str
    target_concept: str
    actual_series_name: str
    benchmark_series: str | None
    match_type: MatchType
    source_name: str
    source_series_code: str
    source_url: str | None
    frequency: str
    unit: str | None
    currency: str | None
    geography: str | None
    active: bool
    notes: str | None
    updated_at: str | None


class PublishedLatestRecord(TypedDict):
    series_key: str
    target_concept: str
    actual_series_name: str
    benchmark_series: str | None
    match_type: MatchType
    observation_date: str
    value: float
    unit: str | None
    currency: str | None
    frequency: str
    source_name: str
    source_series_code: str
    source_url: str | None
    geography: str | None
    updated_at: str | None
    notes: str | None
    previous_value: float | None
    delta_value: float | None
    delta_pct: float | None


class PublishedObservationRecord(TypedDict):
    series_key: str
    target_concept: str
    actual_series_name: str
    benchmark_series: str | None
    match_type: MatchType
    observation_date: str
    value: float
    unit: str | None
    currency: str | None
    frequency: str
    source_name: str
    source_series_code: str
    source_url: str | None
    geography: str | None
    release_date: str | None
    retrieved_at: str | None
    raw_artifact_id: str | int | None
    inserted_at: str | None
    updated_at: str | None
    notes: str | None


@dataclass(frozen=True)
class AppConfig:
    app_root: Path
    backend_root: Path
    database_url: str
    calendar_database_url: str
    inventory_published_db_path: Path | None = None
    inventory_data_root: Path | None = None
    inventory_api_base_url: str = "http://127.0.0.1:8000/api"
    inventory_browse_mode: InventoryBrowseMode = "auto"
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    headline_feed_path: Path | None = None


def load_env_file(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def _read_int_env(
    name: str,
    default: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    try:
        parsed = int(raw_value.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid {name}: expected an integer") from exc

    if min_value is not None and parsed < min_value:
        raise ValueError(f"Invalid {name}: expected a value >= {min_value}")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"Invalid {name}: expected a value <= {max_value}")
    return parsed


def parse_inventory_browse_mode(raw_value: str | None) -> InventoryBrowseMode:
    normalized = (raw_value or "auto").strip().lower()
    if normalized in {"", "auto"}:
        return "auto"
    if normalized in {"local", "published", "local-published", "local-first", "local_first"}:
        return "local"
    if normalized in {"remote", "proxy", "remote-proxy", "remote_proxy"}:
        return "remote"
    raise ValueError("Invalid INVENTORYWATCH_BROWSE_MODE: expected auto, local, or remote")


def has_published_commodity_views(database_path: Path) -> bool:
    if not database_path.exists() or not database_path.is_file():
        return False
    try:
        if database_path.stat().st_size <= 0:
            return False
    except OSError:
        return False

    try:
        connection = sqlite3.connect(database_path)
        try:
            rows = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type IN ('table', 'view')
                  AND name IN ('published_series', 'published_latest_observations', 'published_observations')
                """
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return False

    return {row[0] for row in rows} == {
        "published_series",
        "published_latest_observations",
        "published_observations",
    }


def find_backend_root(app_root: Path) -> Path:
    configured_root = os.environ.get("COMMODITY_BACKEND_ROOT")
    if configured_root:
        candidate = Path(configured_root).expanduser().resolve()
        if candidate.exists():
            return candidate

    for parent in [app_root, *app_root.parents]:
        candidate = (parent / "Commodity Prices").resolve()
        if (candidate / "data" / "commodities.db").exists():
            return candidate

    return app_root


def resolve_database_url(raw_database_url: str, backend_root: Path) -> str:
    url = make_url(raw_database_url)

    if (
        url.drivername == "sqlite"
        and url.database
        and url.database != ":memory:"
        and not os.path.isabs(url.database)
    ):
        sqlite_path = (backend_root / url.database).resolve()
        return str(url.set(database=str(sqlite_path)))

    return str(url)


def build_config(app_root: Path = APP_ROOT) -> AppConfig:
    load_env_file(app_root / ".env")

    local_commodity_db_path = (app_root / "data" / "commodities.db").resolve()
    if (
        "COMMODITY_BACKEND_ROOT" not in os.environ
        and "DATABASE_URL" not in os.environ
        and has_published_commodity_views(local_commodity_db_path)
    ):
        backend_root = app_root.resolve()
    else:
        backend_root = find_backend_root(app_root)
    configured_inventory_published_db_path = os.environ.get("INVENTORYWATCH_PUBLISHED_DB_PATH")
    inventory_published_db_path = None
    if configured_inventory_published_db_path:
        candidate_inventory_published_db_path = Path(configured_inventory_published_db_path).expanduser()
        if not candidate_inventory_published_db_path.is_absolute():
            candidate_inventory_published_db_path = (app_root / candidate_inventory_published_db_path).resolve()
        else:
            candidate_inventory_published_db_path = candidate_inventory_published_db_path.resolve()
        inventory_published_db_path = candidate_inventory_published_db_path
    else:
        default_inventory_published_db_path = (app_root / "data" / "inventorywatch.db").resolve()
        inventory_published_db_path = default_inventory_published_db_path

    configured_inventory_data_root = os.environ.get("INVENTORYWATCH_DATA_ROOT")
    inventory_data_root = None
    if configured_inventory_data_root:
        candidate_inventory_data_root = Path(configured_inventory_data_root).expanduser().resolve()
        if candidate_inventory_data_root.exists():
            inventory_data_root = candidate_inventory_data_root
    else:
        default_inventory_data_root = (app_root / "backend").resolve()
        if default_inventory_data_root.exists():
            inventory_data_root = default_inventory_data_root

    raw_database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    database_url = resolve_database_url(raw_database_url, backend_root)
    raw_calendar_database_url = os.environ.get("CALENDAR_DATABASE_URL", DEFAULT_CALENDAR_DATABASE_URL)
    calendar_database_url = resolve_database_url(raw_calendar_database_url, app_root)
    configured_inventory_api_base_url = os.environ.get("INVENTORYWATCH_API_BASE_URL")
    next_public_api_base_url = os.environ.get("NEXT_PUBLIC_API_BASE_URL")
    raw_inventory_api_base_url = configured_inventory_api_base_url or (
        next_public_api_base_url
        if next_public_api_base_url and next_public_api_base_url.startswith(("http://", "https://"))
        else "http://127.0.0.1:8000/api"
    )
    inventory_api_base_url = raw_inventory_api_base_url.rstrip("/")
    inventory_browse_mode = parse_inventory_browse_mode(os.environ.get("INVENTORYWATCH_BROWSE_MODE"))
    host = os.environ.get("HOST", DEFAULT_HOST)
    port = _read_int_env("PORT", DEFAULT_PORT, min_value=0, max_value=65535)
    return AppConfig(
        app_root=app_root,
        backend_root=backend_root,
        database_url=database_url,
        calendar_database_url=calendar_database_url,
        inventory_published_db_path=inventory_published_db_path,
        inventory_data_root=inventory_data_root,
        inventory_api_base_url=inventory_api_base_url,
        inventory_browse_mode=inventory_browse_mode,
        host=host,
        port=port,
        headline_feed_path=preferred_headline_feed_path(app_root),
    )


def create_db_engine(database_url: str) -> Engine:
    url = make_url(database_url)

    if url.drivername == "sqlite" and url.database and url.database != ":memory:":
        database_path = Path(url.database)
        if not database_path.exists():
            raise FileNotFoundError(f"Database file not found: {database_path}")

    return create_engine(database_url, future=True)


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        normalized_row: dict[str, Any] = {}
        for key, value in row.items():
            if key == "active" and value is not None:
                normalized_row[key] = bool(value)
                continue

            normalized_row[key] = normalize_scalar(value)

        normalized_rows.append(normalized_row)

    return normalized_rows


def require_iso_date(value: str | None, field_name: str) -> str | None:
    if value is None or value == "":
        return None

    try:
        return date.fromisoformat(value.strip()).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: expected YYYY-MM-DD") from exc


def require_iso_date_param(value: str | None, field_name: str) -> date | None:
    if value is None or value == "":
        return None

    normalized = value.strip()
    if not normalized:
        return None
    if "T" in normalized:
        try:
            normalized = datetime.fromisoformat(normalized.replace("Z", "+00:00")).date().isoformat()
        except ValueError as exc:
            raise ValueError(f"Invalid {field_name}: expected ISO date or datetime") from exc

    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: expected YYYY-MM-DD") from exc


def parse_int_param(
    value: str | None,
    field_name: str,
    default: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    if value is None or value == "":
        return default

    try:
        parsed = int(value.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: expected an integer") from exc

    if min_value is not None and parsed < min_value:
        raise ValueError(f"Invalid {field_name}: expected a value >= {min_value}")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"Invalid {field_name}: expected a value <= {max_value}")
    return parsed


def parse_bool_param(value: str | None, field_name: str, default: bool) -> bool:
    if value is None or value == "":
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid {field_name}: expected true or false")


class CommodityRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def list_series(self) -> list[PublishedSeriesRecord]:
        query = text(
            """
            SELECT
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
            FROM published_series
            ORDER BY actual_series_name
            """
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings().all()]
        return normalize_rows(rows)  # type: ignore[return-value]

    def get_series(self, series_key: str) -> PublishedSeriesRecord | None:
        query = text(
            """
            SELECT
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
            FROM published_series
            WHERE series_key = :series_key
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, {"series_key": series_key}).mappings().first()
        if row is None:
            return None
        return normalize_rows([dict(row)])[0]  # type: ignore[return-value]

    def list_latest(self) -> list[PublishedLatestRecord]:
        query = text(
            """
            SELECT
                latest.series_key,
                latest.target_concept,
                latest.actual_series_name,
                latest.benchmark_series,
                latest.match_type,
                latest.observation_date,
                latest.value,
                latest.unit,
                latest.currency,
                latest.frequency,
                latest.source_name,
                latest.source_series_code,
                latest.source_url,
                latest.geography,
                latest.updated_at,
                latest.notes,
                previous.value AS previous_value,
                CASE
                    WHEN previous.value IS NULL THEN NULL
                    ELSE latest.value - previous.value
                END AS delta_value,
                CASE
                    WHEN previous.value IS NULL OR previous.value = 0 THEN NULL
                    ELSE ((latest.value - previous.value) / previous.value) * 100.0
                END AS delta_pct
            FROM published_latest_observations AS latest
            LEFT JOIN published_observations AS previous
              ON previous.series_key = latest.series_key
             AND previous.observation_date = (
                SELECT MAX(history.observation_date)
                FROM published_observations AS history
                WHERE history.series_key = latest.series_key
                  AND history.observation_date < latest.observation_date
             )
            ORDER BY latest.actual_series_name
            """
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings().all()]
        return normalize_rows(rows)  # type: ignore[return-value]

    def get_latest(self, series_key: str) -> PublishedLatestRecord | None:
        query = text(
            """
            SELECT
                latest.series_key,
                latest.target_concept,
                latest.actual_series_name,
                latest.benchmark_series,
                latest.match_type,
                latest.observation_date,
                latest.value,
                latest.unit,
                latest.currency,
                latest.frequency,
                latest.source_name,
                latest.source_series_code,
                latest.source_url,
                latest.geography,
                latest.updated_at,
                latest.notes,
                previous.value AS previous_value,
                CASE
                    WHEN previous.value IS NULL THEN NULL
                    ELSE latest.value - previous.value
                END AS delta_value,
                CASE
                    WHEN previous.value IS NULL OR previous.value = 0 THEN NULL
                    ELSE ((latest.value - previous.value) / previous.value) * 100.0
                END AS delta_pct
            FROM published_latest_observations AS latest
            LEFT JOIN published_observations AS previous
              ON previous.series_key = latest.series_key
             AND previous.observation_date = (
                SELECT MAX(history.observation_date)
                FROM published_observations AS history
                WHERE history.series_key = latest.series_key
                  AND history.observation_date < latest.observation_date
             )
            WHERE latest.series_key = :series_key
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, {"series_key": series_key}).mappings().first()
        if row is None:
            return None
        return normalize_rows([dict(row)])[0]  # type: ignore[return-value]

    def get_history(
        self,
        series_key: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> list[PublishedObservationRecord]:
        query = text(
            """
            SELECT
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
            FROM published_observations
            WHERE series_key = :series_key
              AND (:start IS NULL OR observation_date >= :start)
              AND (:end IS NULL OR observation_date <= :end)
            ORDER BY observation_date
            """
        )
        params = {
            "series_key": series_key,
            "start": require_iso_date(start, "start"),
            "end": require_iso_date(end, "end"),
        }
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query, params).mappings().all()]
        return normalize_rows(rows)  # type: ignore[return-value]


class CommodityRepositoryProvider:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._repository: CommodityRepository | None = None

    def get_repository(self) -> CommodityRepository:
        if self._repository is None:
            self._repository = CommodityRepository(create_db_engine(self.database_url))
        return self._repository


class RelatedHeadlineServiceProvider:
    def __init__(self, feed_path: Path):
        self.feed_path = feed_path
        self._service: RelatedHeadlineService | None = None

    def get_service(self) -> RelatedHeadlineService:
        if self._service is None:
            self._service = RelatedHeadlineService(self.feed_path)
        return self._service


class CalendarRepositoryProvider:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._repository: CalendarRepository | None = None

    def get_repository(self) -> CalendarRepository:
        if self._repository is None:
            repository = CalendarRepository(create_calendar_engine(self.database_url))
            repository.ensure_schema()
            self._repository = repository
        return self._repository


class InventoryRepositoryProvider:
    def __init__(self, published_db_path: Path | None, data_root: Path | None) -> None:
        self._published_db_path = published_db_path
        self._data_root = data_root
        self._repository: LocalInventoryRepository | PublishedInventoryRepository | None = None
        self._error: Exception | None = None
        self._source_kind: str | None = None

    def get_repository(self) -> LocalInventoryRepository | PublishedInventoryRepository:
        if self._repository is not None:
            return self._repository
        if self._error is not None:
            raise self._error

        published_db_error: Exception | None = None
        try:
            if self._published_db_path is not None and self._published_db_path.exists():
                try:
                    self._repository = PublishedInventoryRepository(self._published_db_path)
                    self._source_kind = "published-db"
                    return self._repository
                except Exception as exc:
                    published_db_error = exc

            if self._data_root is None:
                if published_db_error is not None:
                    raise published_db_error
                raise FileNotFoundError("InventoryWatch local store not configured.")

            self._repository = LocalInventoryRepository(self._data_root)
            self._source_kind = "artifact-archive"
        except Exception as exc:
            self._error = exc
            raise
        return self._repository

    def get_status(self) -> tuple[bool, bool, str | None, str | None]:
        try:
            repository = self.get_repository()
        except Exception as exc:
            return False, False, str(exc), self._source_kind
        return True, repository.has_data, None, self._source_kind


def json_payload(data: Any, **meta: Any) -> bytes:
    body: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    if meta:
        body["meta"] = meta
    return json.dumps(body, indent=2).encode("utf-8")


def send_json(handler: SimpleHTTPRequestHandler, status: HTTPStatus, data: Any, **meta: Any) -> None:
    payload = json_payload(data, **meta)
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def send_raw_json(handler: SimpleHTTPRequestHandler, status: HTTPStatus, data: Any) -> None:
    payload = json.dumps(data).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def fetch_proxy_response(target_url: str) -> tuple[int, bytes, str]:
    request = Request(
        target_url,
        method="GET",
        headers={
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=5) as response:
            return (
                response.status,
                response.read(),
                response.headers.get("Content-Type", "application/json; charset=utf-8"),
            )
    except HTTPError as exc:
        return (
            exc.code,
            exc.read(),
            exc.headers.get("Content-Type", "application/json; charset=utf-8"),
        )


def relay_proxy_response(handler: SimpleHTTPRequestHandler, target_url: str) -> None:
    status_code, body, content_type = fetch_proxy_response(target_url)
    handler.send_response(status_code)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def check_inventory_api(base_url: str) -> tuple[bool, str | None]:
    target_url = f"{base_url.rstrip('/')}/health"
    request = Request(
        target_url,
        method="GET",
        headers={
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=3) as response:
            if 200 <= response.status < 400:
                return True, None
            return False, f"InventoryWatch API health check returned {response.status}"
    except HTTPError as exc:
        return False, f"InventoryWatch API health check returned {exc.code}"
    except URLError as exc:
        return False, f"InventoryWatch API unavailable: {exc.reason}"


def resolve_inventory_browse_source(
    browse_mode: InventoryBrowseMode,
    *,
    inventory_archive_has_data: bool,
    inventory_api_available: bool,
) -> str:
    if browse_mode == "local":
        return "local-published" if inventory_archive_has_data else "unavailable"
    if browse_mode == "remote":
        return "remote-proxy" if inventory_api_available else "unavailable"
    if inventory_archive_has_data:
        return "local-published"
    if inventory_api_available:
        return "remote-proxy"
    return "unavailable"


def make_handler(
    repository_provider: CommodityRepositoryProvider,
    calendar_repository_provider: CalendarRepositoryProvider,
    inventory_repository_provider: InventoryRepositoryProvider,
    config: AppConfig,
):
    headline_service_provider = RelatedHeadlineServiceProvider(
        config.headline_feed_path or preferred_headline_feed_path(config.app_root)
    )

    class CommodityWatchRequestHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._sent_cache_control = False
            super().__init__(*args, directory=str(config.app_root), **kwargs)

        def send_header(self, keyword: str, value: str) -> None:
            if keyword.lower() == "cache-control":
                self._sent_cache_control = True
            super().send_header(keyword, value)

        def end_headers(self) -> None:
            if not self._sent_cache_control:
                super().send_header("Cache-Control", "no-store")
            super().end_headers()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)

            if parsed.path.startswith("/api/snapshot") or parsed.path.startswith("/api/indicators"):
                self.handle_inventory_api(parsed)
                return

            if parsed.path == "/api/calendar":
                try:
                    self.handle_calendar_api(parsed)
                except ValueError as exc:
                    send_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                except Exception as exc:  # pragma: no cover
                    send_json(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
                return

            if parsed.path.startswith("/api/commodities"):
                try:
                    self.handle_commodity_api(parsed)
                except ValueError as exc:
                    send_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                except FileNotFoundError as exc:
                    send_json(self, HTTPStatus.NOT_FOUND, {"error": str(exc)})
                except Exception as exc:  # pragma: no cover
                    send_json(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
                return

            if parsed.path == "/api/health":
                commodity_api_error: str | None = None
                commodity_api_available = True
                try:
                    repository_provider.get_repository()
                except FileNotFoundError as exc:
                    commodity_api_available = False
                    commodity_api_error = str(exc)

                calendar_api_error: str | None = None
                calendar_api_available = True
                try:
                    calendar_repository_provider.get_repository()
                except Exception as exc:
                    calendar_api_available = False
                    calendar_api_error = str(exc)

                inventory_archive_available, inventory_archive_has_data, inventory_archive_error, inventory_local_store_kind = (
                    inventory_repository_provider.get_status()
                )
                remote_inventory_api_available = False
                remote_inventory_api_error: str | None = None
                should_check_remote_inventory = config.inventory_browse_mode == "remote" or (
                    config.inventory_browse_mode == "auto" and not inventory_archive_has_data
                )
                if should_check_remote_inventory:
                    remote_inventory_api_available, remote_inventory_api_error = check_inventory_api(
                        config.inventory_api_base_url
                    )

                inventory_api_mode = resolve_inventory_browse_source(
                    config.inventory_browse_mode,
                    inventory_archive_has_data=inventory_archive_has_data,
                    inventory_api_available=remote_inventory_api_available,
                )
                inventory_api_available = inventory_api_mode != "unavailable"
                inventory_api_error = (
                    None
                    if inventory_api_available
                    else remote_inventory_api_error or inventory_archive_error or "InventoryWatch unavailable."
                )

                send_json(
                    self,
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "app_root": str(config.app_root),
                        "backend_root": str(config.backend_root),
                        "database_url": config.database_url,
                        "calendar_database_url": config.calendar_database_url,
                        "commodity_api_available": commodity_api_available,
                        "commodity_api_error": commodity_api_error,
                        "calendar_api_available": calendar_api_available,
                        "calendar_api_error": calendar_api_error,
                        "inventory_api_base_url": config.inventory_api_base_url,
                        "inventory_browse_mode": config.inventory_browse_mode,
                        "inventory_api_available": inventory_api_available,
                        "inventory_api_error": inventory_api_error,
                        "inventory_api_mode": inventory_api_mode,
                        "inventory_remote_api_available": remote_inventory_api_available,
                        "inventory_remote_api_error": remote_inventory_api_error,
                        "inventory_archive_available": inventory_archive_available,
                        "inventory_archive_has_data": inventory_archive_has_data,
                        "inventory_archive_error": inventory_archive_error,
                        "inventory_local_store_kind": inventory_local_store_kind,
                        "inventory_published_db_path": str(config.inventory_published_db_path)
                        if config.inventory_published_db_path is not None
                        else None,
                    },
                )
                return

            if parsed.path in {"", "/"}:
                self.path = "/index.html"
                return super().do_GET()

            if parsed.path == "/inventory-watch" or (
                parsed.path.startswith("/inventory-watch/")
                and "." not in parsed.path.rstrip("/").rsplit("/", 1)[-1]
            ):
                self.path = "/inventory-watch/index.html"
                return super().do_GET()

            return super().do_GET()

        def handle_inventory_api(self, parsed) -> None:
            inventory_archive_available, inventory_archive_has_data, inventory_archive_error, _inventory_local_store_kind = (
                inventory_repository_provider.get_status()
            )
            if config.inventory_browse_mode in {"auto", "local"} and inventory_archive_has_data:
                if self.handle_local_inventory_api(parsed, require_data=True):
                    return

            if config.inventory_browse_mode == "local":
                send_raw_json(
                    self,
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "detail": inventory_archive_error or "InventoryWatch local archive has no data.",
                    },
                )
                return

            suffix = parsed.path.removeprefix("/api")
            target_url = f"{config.inventory_api_base_url}{suffix}"
            if parsed.query:
                target_url = f"{target_url}?{parsed.query}"
            try:
                relay_proxy_response(self, target_url)
                return
            except URLError as exc:
                if (
                    config.inventory_browse_mode == "auto"
                    and inventory_archive_available
                    and inventory_archive_has_data
                    and self.handle_local_inventory_api(parsed, require_data=True)
                ):
                    return
                send_raw_json(
                    self,
                    HTTPStatus.BAD_GATEWAY,
                    {
                        "detail": f"InventoryWatch API unavailable: {exc.reason}",
                        "target_url": target_url,
                    },
                )

        def handle_local_inventory_api(self, parsed, *, require_data: bool = False) -> bool:
            try:
                repository = inventory_repository_provider.get_repository()
            except Exception:
                return False
            if require_data and not repository.has_data:
                return False

            query = parse_qs(parsed.query)

            try:
                if parsed.path == "/api/snapshot/inventorywatch":
                    payload = repository.snapshot_payload(
                        commodity=query.get("commodity", [None])[0],
                        geography=query.get("geography", [None])[0],
                        limit=parse_int_param(query.get("limit", [None])[0], "limit", 20, min_value=1, max_value=100),
                        include_sparklines=parse_bool_param(
                            query.get("include_sparklines", [None])[0], "include_sparklines", True
                        ),
                    )
                    send_raw_json(self, HTTPStatus.OK, payload)
                    return True

                if parsed.path == "/api/indicators":
                    payload = repository.list_indicators_payload(
                        module=query.get("module", [None])[0],
                        commodity=query.get("commodity", [None])[0],
                        geography=query.get("geography", [None])[0],
                        frequency=query.get("frequency", [None])[0],
                        measure_family=query.get("measure_family", [None])[0],
                        visibility=query.get("visibility", ["public"])[0] or "public",
                        active=parse_bool_param(query.get("active", [None])[0], "active", True),
                        limit=parse_int_param(query.get("limit", [None])[0], "limit", 200, min_value=1, max_value=500),
                        cursor=query.get("cursor", [None])[0],
                    )
                    send_raw_json(self, HTTPStatus.OK, payload)
                    return True

                if not parsed.path.startswith("/api/indicators/"):
                    return False

                suffix = unquote(parsed.path.removeprefix("/api/indicators/"))
                if suffix.endswith("/latest"):
                    indicator_id = suffix.removesuffix("/latest").strip("/")
                    if not indicator_id:
                        raise ValueError("indicator_id is required")
                    send_raw_json(self, HTTPStatus.OK, repository.indicator_latest_payload(indicator_id))
                    return True

                if suffix.endswith("/data"):
                    indicator_id = suffix.removesuffix("/data").strip("/")
                    if not indicator_id:
                        raise ValueError("indicator_id is required")
                    payload = repository.indicator_data_payload(
                        indicator_id,
                        start_date=require_iso_date_param(query.get("start_date", [None])[0], "start_date"),
                        end_date=require_iso_date_param(query.get("end_date", [None])[0], "end_date"),
                        include_seasonal=parse_bool_param(
                            query.get("include_seasonal", [None])[0], "include_seasonal", True
                        ),
                        seasonal_profile=query.get("seasonal_profile", [None])[0],
                        limit_points=parse_int_param(
                            query.get("limit_points", [None])[0], "limit_points", 2000, min_value=1, max_value=5000
                        ),
                    )
                    send_raw_json(self, HTTPStatus.OK, payload)
                    return True
            except ValueError as exc:
                send_raw_json(self, HTTPStatus.BAD_REQUEST, {"detail": str(exc)})
                return True
            except KeyError as exc:
                message = str(exc.args[0]) if exc.args else str(exc)
                send_raw_json(self, HTTPStatus.NOT_FOUND, {"detail": message})
                return True
            except LookupError as exc:
                send_raw_json(self, HTTPStatus.NOT_FOUND, {"detail": str(exc)})
                return True

            return False

        def handle_commodity_api(self, parsed) -> None:
            repository = repository_provider.get_repository()
            headline_service = headline_service_provider.get_service()

            if parsed.path == "/api/commodities/series":
                send_json(self, HTTPStatus.OK, repository.list_series())
                return

            if parsed.path == "/api/commodities/latest":
                send_json(self, HTTPStatus.OK, repository.list_latest())
                return

            if not parsed.path.startswith("/api/commodities/"):
                send_json(self, HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
                return

            suffix = unquote(parsed.path.removeprefix("/api/commodities/"))

            if suffix.endswith("/history"):
                series_key = suffix.removesuffix("/history")
                if not series_key:
                    raise ValueError("series_key is required")
                query = parse_qs(parsed.query)
                history = repository.get_history(
                    series_key,
                    start=query.get("start", [None])[0],
                    end=query.get("end", [None])[0],
                )
                send_json(
                    self,
                    HTTPStatus.OK,
                    history,
                    series_key=series_key,
                    start=query.get("start", [None])[0],
                    end=query.get("end", [None])[0],
                )
                return

            if suffix.endswith("/headlines"):
                series_key = suffix.removesuffix("/headlines")
                if not series_key:
                    raise ValueError("series_key is required")

                series = repository.get_series(series_key)
                if series is None:
                    send_json(self, HTTPStatus.NOT_FOUND, {"error": f"Series not found: {series_key}"})
                    return

                query = parse_qs(parsed.query)
                limit = parse_headline_limit(query.get("limit", [None])[0])
                send_json(
                    self,
                    HTTPStatus.OK,
                    headline_service.list_related(series, limit=limit),
                    series_key=series_key,
                    limit=limit,
                )
                return

            series_key = suffix.strip("/")
            if not series_key:
                send_json(self, HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
                return

            series = repository.get_series(series_key)
            if series is None:
                send_json(self, HTTPStatus.NOT_FOUND, {"error": f"Series not found: {series_key}"})
                return

            latest = repository.get_latest(series_key)
            send_json(self, HTTPStatus.OK, {"series": series, "latest": latest})

        def handle_calendar_api(self, parsed) -> None:
            repository = calendar_repository_provider.get_repository()
            query = parse_qs(parsed.query)
            sectors_param = query.get("sectors", [None])[0]
            sectors = [sector.strip() for sector in (sectors_param or "").split(",") if sector.strip()]
            from_date = require_iso_date_param(query.get("from", [None])[0], "from")
            to_date = require_iso_date_param(query.get("to", [None])[0], "to")
            events = repository.list_events(from_date=from_date, to_date=to_date, sectors=sectors or None)
            send_json(
                self,
                HTTPStatus.OK,
                events,
                from_date=from_date.isoformat() if from_date else None,
                to_date=to_date.isoformat() if to_date else None,
                sectors=sectors,
            )

        def log_message(self, format: str, *args: Any) -> None:
            return

    return CommodityWatchRequestHandler


def create_server(config: AppConfig) -> ThreadingHTTPServer:
    handler = make_handler(
        CommodityRepositoryProvider(config.database_url),
        CalendarRepositoryProvider(config.calendar_database_url),
        InventoryRepositoryProvider(config.inventory_published_db_path, config.inventory_data_root),
        config,
    )
    return ThreadingHTTPServer((config.host, config.port), handler)


def serve(config: AppConfig) -> None:
    server = create_server(config)
    print(f"Serving CommodityWatch on http://{config.host}:{server.server_port}")
    print("Pages: /, /headline-watch/, /price-watch/, /calendar-watch/, /inventory-watch/")
    print("API:   /api/health")
    print("API:   /api/calendar")
    print("API:   /api/commodities/series, /api/commodities/latest")
    print("API:   /api/commodities/<series_key>, /api/commodities/<series_key>/history")
    print("API:   /api/commodities/<series_key>/headlines")
    print("API:   /api/indicators, /api/indicators/<indicator_id>/data, /api/indicators/<indicator_id>/latest")
    print("API:   /api/snapshot/inventorywatch")
    print(f"Commodity backend: {config.database_url}")
    print(f"Calendar backend:  {config.calendar_database_url}")
    print(f"Inventory backend: {config.inventory_api_base_url}")
    print(f"Inventory browse:  {config.inventory_browse_mode}")
    print(f"Inventory store:   {config.inventory_published_db_path or 'not configured'}")
    print(f"Inventory data:    {config.inventory_data_root or 'not configured'}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    env_config = build_config()
    parser = argparse.ArgumentParser(description="Serve the CommodityWatch product UI and APIs")
    parser.add_argument("--host", default=env_config.host)
    parser.add_argument("--port", type=int, default=env_config.port)
    args = parser.parse_args()

    config = replace(env_config, host=args.host, port=args.port)
    serve(config)


if __name__ == "__main__":
    main()
