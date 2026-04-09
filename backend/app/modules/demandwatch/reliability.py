from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Hashable, Sequence
from json import JSONDecodeError
from typing import Any, TypeVar
from xml.etree.ElementTree import ParseError as XMLParseError

import httpx

from app.core.config import get_settings


T = TypeVar("T")
MANIFEST_VERSION = 1
PARSE_FAILURE_TYPES = (
    JSONDecodeError,
    KeyError,
    TypeError,
    ValueError,
    XMLParseError,
    IndexError,
)


def demandwatch_retry_delay_seconds(attempt_index: int, *, run_mode: str) -> int:
    settings = get_settings()
    normalized = str(run_mode or "").strip().lower()
    if normalized == "backfill":
        base_delay = 4 if settings.is_production else 1
        max_delay = 45 if settings.is_production else 3
    else:
        base_delay = 15 if settings.is_production else 1
        max_delay = 120 if settings.is_production else 5
    return min(base_delay * (2**attempt_index), max_delay)


def classify_demandwatch_failure(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return "http_error"
    if isinstance(exc, (httpx.RequestError, TimeoutError, ConnectionError)):
        return "network_error"
    if isinstance(exc, PARSE_FAILURE_TYPES):
        return "parse_error"
    return "unexpected_error"


def is_retryable_demandwatch_failure(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 429 or 500 <= status_code < 600
    return isinstance(exc, (httpx.RequestError, TimeoutError, ConnectionError))


def build_failure_context(
    exc: Exception,
    *,
    stage: str | None = None,
    attempt: int | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "failure_category": classify_demandwatch_failure(exc),
        "exception_type": exc.__class__.__name__,
        "retryable": is_retryable_demandwatch_failure(exc),
    }
    if stage:
        context["failure_stage"] = stage
    if attempt is not None:
        context["attempt"] = int(attempt)
    if isinstance(exc, httpx.HTTPStatusError):
        context["http_status"] = int(exc.response.status_code)
    return context


def merge_run_metadata(existing: dict[str, Any] | None, **values: Any) -> dict[str, Any]:
    metadata = dict(existing or {})
    for key, value in values.items():
        if value is not None:
            metadata[key] = value
    return metadata


def bump_run_metadata_counter(run: Any, key: str, increment: int) -> None:
    metadata = dict(getattr(run, "metadata_", {}) or {})
    metadata[key] = int(metadata.get(key, 0) or 0) + int(increment)
    setattr(run, "metadata_", metadata)


def annotate_run_failure(
    run: Any,
    exc: Exception,
    *,
    stage: str | None = None,
    attempt: int | None = None,
) -> None:
    setattr(
        run,
        "metadata_",
        merge_run_metadata(
            getattr(run, "metadata_", {}) or {},
            **build_failure_context(exc, stage=stage, attempt=attempt),
        ),
    )


def dedupe_records(
    records: Sequence[T],
    *,
    key: Callable[[T], Hashable],
) -> tuple[list[T], int]:
    unique: list[T] = []
    seen: set[Hashable] = set()
    duplicate_count = 0
    for item in records:
        item_key = key(item)
        if item_key in seen:
            duplicate_count += 1
            continue
        seen.add(item_key)
        unique.append(item)
    return unique, duplicate_count


def build_demandwatch_operation_manifest(
    *,
    operation: str,
    sources: Sequence[str],
    run_mode: str,
    from_date=None,
    to_date=None,
    output_path: str | None = None,
    continue_on_error: bool = False,
    max_attempts: int = 1,
) -> dict[str, Any]:
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "operation": str(operation),
        "sources": sorted({str(source) for source in sources}),
        "run_mode": str(run_mode),
        "from_date": from_date.isoformat() if from_date is not None else None,
        "to_date": to_date.isoformat() if to_date is not None else None,
        "output_path": str(output_path) if output_path else None,
        "continue_on_error": bool(continue_on_error),
        "max_attempts": int(max_attempts),
    }
    digest = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    return {
        **manifest,
        "signature": digest,
    }
