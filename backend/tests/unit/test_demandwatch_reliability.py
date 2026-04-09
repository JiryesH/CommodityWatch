from __future__ import annotations

import httpx

from app.modules.demandwatch.reliability import (
    build_demandwatch_operation_manifest,
    classify_demandwatch_failure,
    dedupe_records,
    is_retryable_demandwatch_failure,
)


def test_operation_manifest_is_deterministic_and_source_order_independent() -> None:
    manifest_a = build_demandwatch_operation_manifest(
        operation="refresh",
        sources=["demand_fred_g17", "demand_eia_wpsr"],
        run_mode="manual",
        continue_on_error=True,
        max_attempts=3,
    )
    manifest_b = build_demandwatch_operation_manifest(
        operation="refresh",
        sources=["demand_eia_wpsr", "demand_fred_g17"],
        run_mode="manual",
        continue_on_error=True,
        max_attempts=3,
    )

    assert manifest_a["sources"] == ["demand_eia_wpsr", "demand_fred_g17"]
    assert manifest_a["signature"] == manifest_b["signature"]


def test_failure_classification_distinguishes_parse_and_retryable_network_errors() -> None:
    request = httpx.Request("GET", "https://example.com")
    retryable_http = httpx.HTTPStatusError(
        "server error",
        request=request,
        response=httpx.Response(503, request=request),
    )

    assert classify_demandwatch_failure(ValueError("bad payload")) == "parse_error"
    assert classify_demandwatch_failure(retryable_http) == "http_error"
    assert is_retryable_demandwatch_failure(retryable_http) is True
    assert is_retryable_demandwatch_failure(httpx.ConnectError("offline", request=request)) is True


def test_dedupe_records_keeps_first_occurrence_and_counts_duplicates() -> None:
    records, duplicate_count = dedupe_records(
        [
            {"key": "a", "value": 1},
            {"key": "a", "value": 2},
            {"key": "b", "value": 3},
        ],
        key=lambda item: item["key"],
    )

    assert duplicate_count == 1
    assert records == [{"key": "a", "value": 1}, {"key": "b", "value": 3}]
