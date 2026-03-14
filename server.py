#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal, TypedDict
from urllib.parse import parse_qs, unquote, urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from calendar_pipeline.storage import CalendarRepository, create_calendar_engine
from feed_io import preferred_headline_feed_path
from headline_associations import RelatedHeadlineService, parse_headline_limit


APP_ROOT = Path(__file__).resolve().parent
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
DEFAULT_DATABASE_URL = "sqlite:///data/commodities.db"
DEFAULT_CALENDAR_DATABASE_URL = "sqlite:///data/calendarwatch.db"
MatchType = Literal["exact", "related"]


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

    backend_root = find_backend_root(app_root)
    raw_database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    database_url = resolve_database_url(raw_database_url, backend_root)
    raw_calendar_database_url = os.environ.get("CALENDAR_DATABASE_URL", DEFAULT_CALENDAR_DATABASE_URL)
    calendar_database_url = resolve_database_url(raw_calendar_database_url, app_root)
    host = os.environ.get("HOST", DEFAULT_HOST)
    port = int(os.environ.get("PORT", str(DEFAULT_PORT)))
    return AppConfig(
        app_root=app_root,
        backend_root=backend_root,
        database_url=database_url,
        calendar_database_url=calendar_database_url,
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
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: expected YYYY-MM-DD") from exc


def require_iso_date_param(value: str | None, field_name: str) -> date | None:
    if value is None or value == "":
        return None

    normalized = value.strip()
    if "T" in normalized:
        try:
            normalized = datetime.fromisoformat(normalized.replace("Z", "+00:00")).date().isoformat()
        except ValueError as exc:
            raise ValueError(f"Invalid {field_name}: expected ISO date or datetime") from exc

    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: expected YYYY-MM-DD") from exc


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


def make_handler(
    repository_provider: CommodityRepositoryProvider,
    calendar_repository_provider: CalendarRepositoryProvider,
    config: AppConfig,
):
    headline_service_provider = RelatedHeadlineServiceProvider(
        config.headline_feed_path or preferred_headline_feed_path(config.app_root)
    )

    class ContangoRequestHandler(SimpleHTTPRequestHandler):
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
                    },
                )
                return

            if parsed.path in {"", "/"}:
                self.path = "/index.html"
                return super().do_GET()

            return super().do_GET()

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

    return ContangoRequestHandler


def create_server(config: AppConfig) -> ThreadingHTTPServer:
    handler = make_handler(
        CommodityRepositoryProvider(config.database_url),
        CalendarRepositoryProvider(config.calendar_database_url),
        config,
    )
    return ThreadingHTTPServer((config.host, config.port), handler)


def serve(config: AppConfig) -> None:
    server = create_server(config)
    print(f"Serving Contango on http://{config.host}:{server.server_port}")
    print("Pages: /, /headline-watch/, /price-watch/, /calendar-watch/")
    print("API:   /api/health")
    print("API:   /api/calendar")
    print("API:   /api/commodities/series, /api/commodities/latest")
    print("API:   /api/commodities/<series_key>, /api/commodities/<series_key>/history")
    print("API:   /api/commodities/<series_key>/headlines")
    print(f"Commodity backend: {config.database_url}")
    print(f"Calendar backend:  {config.calendar_database_url}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    env_config = build_config()
    parser = argparse.ArgumentParser(description="Serve the Contango product UI and APIs")
    parser.add_argument("--host", default=env_config.host)
    parser.add_argument("--port", type=int, default=env_config.port)
    args = parser.parse_args()

    config = replace(env_config, host=args.host, port=args.port)
    serve(config)


if __name__ == "__main__":
    main()
