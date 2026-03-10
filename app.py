#!/usr/bin/env python3
"""
Control API for orchestration of project scripts.

Run:
    python app.py --host 127.0.0.1 --port 8081

Endpoints:
    GET  /api/health
    GET  /api/jobs
    GET  /api/jobs/<id>
    POST /api/jobs/scrape
    POST /api/jobs/sentiment
    POST /api/jobs/ner
    POST /api/jobs/pipeline

All POST endpoints accept JSON bodies and return a job object immediately.
Jobs run in a background thread; poll /api/jobs/<id> for completion.
"""

from __future__ import annotations

import argparse
import copy
import json
import threading
import traceback
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import rss_scraper
import sentiment_finbert
import ner_spacy
from feed_io import ensure_feed_metadata
from ner_spacy import NERConfig, SpacyNERExtractor, log_ner_rollup
from sentiment_finbert import SentimentConfig, FinBERTScorer, log_sentiment_rollup


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_COMPLETED_JOB_RETENTION = 200
COMPLETED_JOB_STATUSES = frozenset({"succeeded", "failed"})


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_path(raw_path: str | None, fallback: Path) -> Path:
    if not raw_path:
        return fallback
    p = Path(raw_path)
    if not p.is_absolute():
        p = ROOT_DIR / p
    return p


def to_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def to_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def build_sentiment_config(payload: dict[str, Any], enabled: bool) -> SentimentConfig:
    return SentimentConfig(
        enabled=enabled,
        model_name=str(payload.get("sentiment_model", "ProsusAI/finbert")),
        batch_size=to_int(payload.get("sentiment_batch_size"), 32),
        max_length=to_int(payload.get("sentiment_max_length"), 128),
        use_description=to_bool(payload.get("sentiment_use_description"), False),
        force_rescore=to_bool(payload.get("sentiment_force_rescore"), False),
    )


def build_ner_config(payload: dict[str, Any], enabled: bool) -> NERConfig:
    return NERConfig(
        enabled=enabled,
        model_name=str(payload.get("ner_model", "en_core_web_lg")),
        batch_size=to_int(payload.get("ner_batch_size"), 64),
        use_description=to_bool(payload.get("ner_use_description"), False),
        force_rescore=to_bool(payload.get("ner_force_rescore"), False),
        max_entities=to_int(payload.get("ner_max_entities"), 18),
    )


def run_scrape_job(payload: dict[str, Any]) -> dict[str, Any]:
    sentiment_enabled = to_bool(payload.get("sentiment"), False)
    ner_enabled = to_bool(payload.get("ner"), False)
    include_rss = to_bool(payload.get("rss"), True)
    include_argus = to_bool(payload.get("argus"), True)
    if not include_rss and not include_argus:
        raise ValueError("At least one source must be enabled (rss or argus).")

    sentiment_config = (
        build_sentiment_config(payload, enabled=True) if sentiment_enabled else None
    )
    ner_config = build_ner_config(payload, enabled=True) if ner_enabled else None

    articles, stats, sentiment_stats, ner_stats = rss_scraper.run_once(
        sentiment_config=sentiment_config,
        ner_config=ner_config,
        include_rss=include_rss,
        include_argus=include_argus,
        argus_max_pages=to_int(
            payload.get("argus_max_pages"), rss_scraper.DEFAULT_ARGUS_MAX_PAGES
        ),
        argus_timeout=to_int(
            payload.get("argus_timeout"), rss_scraper.DEFAULT_ARGUS_TIMEOUT
        ),
        argus_include_lead=to_bool(payload.get("argus_include_lead"), False),
        argus_pause=to_float(payload.get("argus_pause"), rss_scraper.DEFAULT_ARGUS_PAUSE),
    )

    return {
        "article_count": len(articles),
        "fetch": stats,
        "sentiment": sentiment_stats,
        "ner": ner_stats,
        "feed_path": str(rss_scraper.OUTPUT_FILE),
    }


def run_pipeline_job(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    merged.setdefault("sentiment", True)
    merged.setdefault("ner", True)
    return run_scrape_job(merged)


def run_sentiment_job(payload: dict[str, Any]) -> dict[str, Any]:
    input_path = resolve_path(payload.get("input"), rss_scraper.OUTPUT_FILE)
    output_path = resolve_path(payload.get("output"), input_path)

    data = sentiment_finbert.load_feed(input_path)
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = SentimentConfig(
        enabled=True,
        model_name=str(payload.get("model", "ProsusAI/finbert")),
        batch_size=to_int(payload.get("batch_size"), 32),
        max_length=to_int(payload.get("max_length"), 128),
        use_description=to_bool(payload.get("use_description"), False),
        force_rescore=to_bool(payload.get("force_rescore"), False),
    )

    scorer = FinBERTScorer(config)
    stats = scorer.score_incremental(articles)

    metadata = ensure_feed_metadata(data)
    metadata["sentiment"] = stats

    sentiment_finbert.save_feed(output_path, data)
    if stats.get("scored", 0) > 0:
        log_sentiment_rollup(articles)

    return {
        "article_count": len(articles),
        "sentiment": stats,
        "input_path": str(input_path),
        "output_path": str(output_path),
    }


def run_ner_job(payload: dict[str, Any]) -> dict[str, Any]:
    input_path = resolve_path(payload.get("input"), rss_scraper.OUTPUT_FILE)
    output_path = resolve_path(payload.get("output"), input_path)

    data = ner_spacy.load_feed(input_path)
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = NERConfig(
        enabled=True,
        model_name=str(payload.get("model", "en_core_web_lg")),
        batch_size=to_int(payload.get("batch_size"), 64),
        use_description=to_bool(payload.get("use_description"), False),
        force_rescore=to_bool(payload.get("force_rescore"), False),
        max_entities=to_int(payload.get("max_entities"), 18),
    )

    extractor = SpacyNERExtractor(config)
    stats = extractor.extract_incremental(articles)

    metadata = ensure_feed_metadata(data)
    metadata["ner"] = stats

    ner_spacy.save_feed(output_path, data)
    if stats.get("extracted", 0) > 0:
        log_ner_rollup(articles)

    return {
        "article_count": len(articles),
        "ner": stats,
        "input_path": str(input_path),
        "output_path": str(output_path),
    }


class JobStore:
    def __init__(self, *, retain_completed: int = DEFAULT_COMPLETED_JOB_RETENTION):
        self._retain_completed = max(0, retain_completed)
        self._jobs: dict[str, dict[str, Any]] = {}
        self._counter = count(1)
        self._completion_counter = count(1)
        self._lock = threading.Lock()

    def create_job(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            job_id = str(next(self._counter))
            job = {
                "id": job_id,
                "kind": kind,
                "status": "queued",
                "created_at": utc_now_iso(),
                "payload": payload,
            }
            self._jobs[job_id] = job
            return self._snapshot_job(job)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return self._snapshot_job(job)

    def list_jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda job: int(job["id"]), reverse=True)
            return [self._snapshot_job(job) for job in jobs]

    def counts(self) -> dict[str, int]:
        with self._lock:
            return {
                "queued": sum(1 for job in self._jobs.values() if job["status"] == "queued"),
                "running": sum(1 for job in self._jobs.values() if job["status"] == "running"),
            }

    def mark_running(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["status"] = "running"
            job["started_at"] = utc_now_iso()

    def mark_succeeded(self, job_id: str, result: dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["status"] = "succeeded"
            job["result"] = result
            job["finished_at"] = utc_now_iso()
            job["_completed_order"] = next(self._completion_counter)
            self._prune_completed_locked()

    def mark_failed(self, job_id: str, exc: Exception, traceback_text: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["status"] = "failed"
            job["error"] = {
                "message": str(exc),
                "traceback": traceback_text,
            }
            job["finished_at"] = utc_now_iso()
            job["_completed_order"] = next(self._completion_counter)
            self._prune_completed_locked()

    def _prune_completed_locked(self) -> None:
        completed_jobs = sorted(
            (
                (job_id, job)
                for job_id, job in self._jobs.items()
                if job.get("status") in COMPLETED_JOB_STATUSES
            ),
            key=lambda item: int(item[1].get("_completed_order", 0)),
        )
        excess = len(completed_jobs) - self._retain_completed
        if excess <= 0:
            return

        for job_id, _ in completed_jobs[:excess]:
            self._jobs.pop(job_id, None)

    @staticmethod
    def _snapshot_job(job: dict[str, Any]) -> dict[str, Any]:
        return {
            key: copy.deepcopy(value)
            for key, value in job.items()
            if not key.startswith("_")
        }


def job_summary(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "kind": job["kind"],
        "status": job["status"],
        "created_at": job["created_at"],
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
    }


def job_detail(job: dict[str, Any]) -> dict[str, Any]:
    detail = dict(job_summary(job))
    if "result" in job:
        detail["result"] = job["result"]
    if "error" in job:
        detail["error"] = job["error"]
    return detail


JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


class ControlApi:
    def __init__(
        self,
        *,
        job_store: JobStore | None = None,
        job_handlers: dict[str, tuple[str, JobHandler]] | None = None,
        runner_lock: threading.Lock | None = None,
    ):
        self.job_store = job_store or JobStore()
        self.job_handlers = job_handlers or {
            "/api/jobs/scrape": ("scrape", run_scrape_job),
            "/api/jobs/sentiment": ("sentiment", run_sentiment_job),
            "/api/jobs/ner": ("ner", run_ner_job),
            "/api/jobs/pipeline": ("pipeline", run_pipeline_job),
        }
        self.runner_lock = runner_lock or threading.Lock()

    def submit_job(
        self,
        kind: str,
        payload: dict[str, Any],
        fn: JobHandler,
    ) -> dict[str, Any]:
        job = self.job_store.create_job(kind, payload)
        thread = threading.Thread(
            target=self.run_job_worker,
            args=(job["id"], payload, fn),
            daemon=True,
        )
        thread.start()
        return job

    def run_job_worker(
        self,
        job_id: str,
        payload: dict[str, Any],
        fn: JobHandler,
    ) -> None:
        self.job_store.mark_running(job_id)

        try:
            with self.runner_lock:
                result = fn(payload)
            self.job_store.mark_succeeded(job_id, result)
        except Exception as exc:
            self.job_store.mark_failed(job_id, exc, traceback.format_exc(limit=10))


def submit_job(kind: str, payload: dict[str, Any], fn: JobHandler) -> dict[str, Any]:
    return DEFAULT_CONTROL_API.submit_job(kind, payload, fn)


def run_job_worker(job_id: str, payload: dict[str, Any], fn: JobHandler) -> None:
    DEFAULT_CONTROL_API.run_job_worker(job_id, payload, fn)


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(body)


def read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or 0)
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("JSON body must be an object.")
    return data


def make_handler(control_api: ControlApi):
    class ApiHandler(BaseHTTPRequestHandler):
        server_version = "ContangoControlAPI/1.0"

        def do_OPTIONS(self) -> None:  # noqa: N802
            json_response(self, HTTPStatus.OK, {"ok": True})

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path

            if path == "/api/health":
                json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "time": utc_now_iso(),
                        "jobs": control_api.job_store.counts(),
                    },
                )
                return

            if path == "/api/jobs":
                payload = [job_summary(job) for job in control_api.job_store.list_jobs()]
                json_response(self, HTTPStatus.OK, {"jobs": payload})
                return

            if path.startswith("/api/jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                job = control_api.job_store.get_job(job_id)
                if not job:
                    json_response(self, HTTPStatus.NOT_FOUND, {"error": "Job not found."})
                    return
                json_response(self, HTTPStatus.OK, {"job": job_detail(job)})
                return

            json_response(self, HTTPStatus.NOT_FOUND, {"error": "Route not found."})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                payload = read_json_body(self)
            except Exception as exc:
                json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"Invalid JSON body: {exc}"},
                )
                return

            route = control_api.job_handlers.get(path)
            if route is None:
                json_response(self, HTTPStatus.NOT_FOUND, {"error": "Route not found."})
                return

            kind, fn = route
            job = control_api.submit_job(kind, payload, fn)
            json_response(self, HTTPStatus.ACCEPTED, {"job": job_summary(job)})

        def log_message(self, format: str, *args: Any) -> None:
            return

    return ApiHandler


DEFAULT_CONTROL_API = ControlApi()
ApiHandler = make_handler(DEFAULT_CONTROL_API)


def create_server(
    host: str = "127.0.0.1",
    port: int = 8081,
    *,
    control_api: ControlApi | None = None,
) -> ThreadingHTTPServer:
    handler = make_handler(control_api or DEFAULT_CONTROL_API)
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Contango script control API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()

    server = create_server(args.host, args.port)
    print(f"Control API listening on http://{args.host}:{args.port}")
    print("Routes: GET /api/health, GET /api/jobs, GET /api/jobs/<id>,")
    print(
        "        POST /api/jobs/scrape, /api/jobs/sentiment, /api/jobs/ner, /api/jobs/pipeline"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
