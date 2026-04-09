from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.sources import IngestRun
from app.db.session import get_session_factory
from app.ingest.backfill import monthly_chunks, yearly_chunks
from app.ingest.common.jobs import create_ingest_run, get_source_bundle, utcnow
from app.ingest.sources.eia.jobs import fetch_demand_eia_grid_monitor, fetch_demand_eia_wpsr
from app.ingest.sources.ember.jobs import fetch_demand_ember_monthly_electricity
from app.ingest.sources.fred.jobs import (
    fetch_demand_fred_g17,
    fetch_demand_fred_motor_vehicle_sales,
    fetch_demand_fred_new_residential_construction,
    fetch_demand_fred_traffic_volume_trends,
)
from app.ingest.sources.usda_export_sales.jobs import fetch_demand_usda_export_sales
from app.ingest.sources.usda_psd.jobs import fetch_demand_usda_psd
from app.modules.demandwatch.backfill import (
    demandwatch_default_from_date,
    describe_demandwatch_backfill_scope,
)
from app.modules.demandwatch.policy import expected_canonical_unit_for_series
from app.modules.demandwatch.published_store import (
    DemandStoreBundle,
    _audit_status_for_series,
    build_demandwatch_coverage_audit,
    demandwatch_coverage_audit_markdown,
)
from app.modules.demandwatch.reliability import (
    build_demandwatch_operation_manifest,
    build_failure_context,
    classify_demandwatch_failure,
    demandwatch_retry_delay_seconds,
    is_retryable_demandwatch_failure,
)
from app.processing.demandwatch import (
    DemandWatchSetupError,
    SETUP_INSTRUCTIONS,
    assert_demandwatch_registry_seeded,
    load_demandwatch_bundle,
    publish_demandwatch_store,
    recompute_demandwatch_snapshot,
)


logger = logging.getLogger(__name__)
UTC = timezone.utc
DEFAULT_RETRY_ATTEMPTS = 3
SOURCE_HEALTH_WINDOW_DAYS = 30
AUDIT_FAILURE_RANK = {
    "healthy": 0,
    "deferred": 0,
    "degraded": 1,
    "failing": 2,
}


def _enum_value(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)


@dataclass(frozen=True, slots=True)
class DemandWatchSourceSpec:
    name: str
    source_slug: str
    release_slug: str
    display_name: str
    runner: Callable[..., Awaitable[object]]
    backfill_window: str = "single"
    uses_internal_retries: bool = False


@dataclass(slots=True)
class DemandWatchSourceOutcome:
    name: str
    display_name: str
    run_mode: str
    requested_from_date: str | None = None
    requested_to_date: str | None = None
    attempts: int = 0
    status: str = "success"
    fetched_items: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    quarantined_rows: int = 0
    failure_category: str | None = None
    error_text: str | None = None
    scope_note: str | None = None

    def to_item(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "run_mode": self.run_mode,
            "requested_from_date": self.requested_from_date,
            "requested_to_date": self.requested_to_date,
            "attempts": self.attempts,
            "status": self.status,
            "fetched_items": self.fetched_items,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "quarantined_rows": self.quarantined_rows,
            "failure_category": self.failure_category,
            "error_text": self.error_text,
            "scope_note": self.scope_note,
        }


DEMANDWATCH_SOURCE_SPECS: tuple[DemandWatchSourceSpec, ...] = (
    DemandWatchSourceSpec(
        name="demand_eia_wpsr",
        source_slug="eia",
        release_slug="demand_eia_wpsr",
        display_name="EIA WPSR",
        runner=fetch_demand_eia_wpsr,
        backfill_window="yearly",
        uses_internal_retries=True,
    ),
    DemandWatchSourceSpec(
        name="demand_eia_grid_monitor",
        source_slug="eia",
        release_slug="demand_eia_grid_monitor",
        display_name="EIA Grid Monitor",
        runner=fetch_demand_eia_grid_monitor,
        backfill_window="monthly",
        uses_internal_retries=True,
    ),
    DemandWatchSourceSpec(
        name="demand_fred_g17",
        source_slug="fred",
        release_slug="demand_fred_g17",
        display_name="FRED G.17",
        runner=fetch_demand_fred_g17,
    ),
    DemandWatchSourceSpec(
        name="demand_fred_new_residential_construction",
        source_slug="fred",
        release_slug="demand_fred_new_residential_construction",
        display_name="FRED Housing",
        runner=fetch_demand_fred_new_residential_construction,
    ),
    DemandWatchSourceSpec(
        name="demand_fred_motor_vehicle_sales",
        source_slug="fred",
        release_slug="demand_fred_motor_vehicle_sales",
        display_name="FRED Vehicle Sales",
        runner=fetch_demand_fred_motor_vehicle_sales,
    ),
    DemandWatchSourceSpec(
        name="demand_fred_traffic_volume_trends",
        source_slug="fred",
        release_slug="demand_fred_traffic_volume_trends",
        display_name="FRED Traffic Volume Trends",
        runner=fetch_demand_fred_traffic_volume_trends,
    ),
    DemandWatchSourceSpec(
        name="demand_usda_wasde",
        source_slug="usda_psd",
        release_slug="demand_usda_wasde",
        display_name="USDA PSD/WASDE",
        runner=fetch_demand_usda_psd,
    ),
    DemandWatchSourceSpec(
        name="demand_usda_export_sales",
        source_slug="usda_export_sales",
        release_slug="demand_usda_export_sales",
        display_name="USDA Export Sales",
        runner=fetch_demand_usda_export_sales,
    ),
    DemandWatchSourceSpec(
        name="demand_ember_monthly_electricity",
        source_slug="ember",
        release_slug="demand_ember_monthly_electricity",
        display_name="Ember Monthly Electricity",
        runner=fetch_demand_ember_monthly_electricity,
    ),
)
DEMANDWATCH_SOURCE_SPEC_BY_NAME = {spec.name: spec for spec in DEMANDWATCH_SOURCE_SPECS}


def list_demandwatch_sources() -> list[str]:
    return [spec.name for spec in DEMANDWATCH_SOURCE_SPECS]


def default_demandwatch_publish_path() -> Path:
    return get_settings().artifact_root / "demandwatch" / "published.sqlite"


def default_demandwatch_audit_json_path() -> Path:
    return get_settings().artifact_root / "demandwatch" / "audit.json"


def default_demandwatch_audit_markdown_path() -> Path:
    return get_settings().artifact_root / "demandwatch" / "audit.md"


def resolve_demandwatch_source_specs(sources: list[str] | None) -> list[DemandWatchSourceSpec]:
    if not sources:
        return list(DEMANDWATCH_SOURCE_SPECS)

    resolved: list[DemandWatchSourceSpec] = []
    seen: set[str] = set()
    for source in sources:
        spec = DEMANDWATCH_SOURCE_SPEC_BY_NAME.get(str(source))
        if spec is None:
            raise ValueError(f"Unsupported DemandWatch source: {source}")
        if spec.name in seen:
            continue
        resolved.append(spec)
        seen.add(spec.name)
    return resolved


def _result_value(result: object, field: str) -> int:
    return int(getattr(result, field, 0) or 0)


def _merge_result_into_outcome(outcome: DemandWatchSourceOutcome, result: object) -> None:
    outcome.fetched_items += _result_value(result, "fetched_items")
    outcome.inserted_rows += _result_value(result, "inserted_rows")
    outcome.updated_rows += _result_value(result, "updated_rows")
    outcome.quarantined_rows += _result_value(result, "quarantined_rows")


def _backfill_windows(spec: DemandWatchSourceSpec, from_date: date, to_date: date) -> list[tuple[date, date]]:
    if spec.backfill_window == "yearly":
        return yearly_chunks(from_date, to_date)
    if spec.backfill_window == "monthly":
        windows: list[tuple[date, date]] = []
        for month_key in monthly_chunks(from_date, to_date):
            window_start = date.fromisoformat(f"{month_key}-01")
            if window_start.month == 12:
                window_end = date(window_start.year, 12, 31)
            else:
                window_end = date(window_start.year, window_start.month + 1, 1) - date.resolution
            windows.append((window_start, min(window_end, to_date)))
        return windows
    return [(from_date, to_date)]


async def _latest_job_run(session: AsyncSession, job_name: str) -> IngestRun | None:
    result = await session.execute(
        select(IngestRun)
        .where(IngestRun.job_name == job_name)
        .order_by(IngestRun.started_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _record_failed_ingest_run(
    session: AsyncSession,
    spec: DemandWatchSourceSpec,
    *,
    run_mode: str,
    operation: str,
    manifest: dict[str, Any],
    from_date: date | None,
    to_date: date | None,
    failure_context: dict[str, Any],
    error_text: str,
) -> None:
    source, release_definition = await get_source_bundle(session, spec.source_slug, spec.release_slug)
    run = await create_ingest_run(
        session,
        spec.name,
        source.id,
        release_definition.id,
        run_mode,
        metadata={
            **failure_context,
            "operation": operation,
            "operation_signature": manifest["signature"],
            "requested_from_date": from_date.isoformat() if from_date else None,
            "requested_to_date": to_date.isoformat() if to_date else None,
        },
    )
    run.status = "failed"
    run.error_text = error_text
    run.finished_at = utcnow()
    await session.flush()


async def _run_source_with_retries(
    session: AsyncSession,
    spec: DemandWatchSourceSpec,
    *,
    operation: str,
    run_mode: str,
    from_date: date | None,
    to_date: date | None,
    max_attempts: int,
    manifest: dict[str, Any],
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> DemandWatchSourceOutcome:
    outcome = DemandWatchSourceOutcome(
        name=spec.name,
        display_name=spec.display_name,
        run_mode=run_mode,
        requested_from_date=from_date.isoformat() if from_date else None,
        requested_to_date=to_date.isoformat() if to_date else None,
        scope_note=describe_demandwatch_backfill_scope(spec.name, from_date, to_date)
        if operation == "backfill" and from_date and to_date
        else None,
    )
    effective_attempts = 1 if spec.uses_internal_retries else max(1, int(max_attempts))
    windows = _backfill_windows(spec, from_date, to_date) if operation == "backfill" and from_date and to_date else [(None, None)]

    for attempt_index in range(effective_attempts):
        actual_run_mode = run_mode if attempt_index == 0 or run_mode == "backfill" else "retry"
        outcome.attempts = attempt_index + 1
        outcome.status = "success"
        outcome.failure_category = None
        outcome.error_text = None
        try:
            logger.info(
                "DemandWatch feed run starting",
                extra={
                    "operation": operation,
                    "source_name": spec.name,
                    "source_display_name": spec.display_name,
                    "attempt": attempt_index + 1,
                    "max_attempts": effective_attempts,
                    "run_mode": actual_run_mode,
                    "operation_signature": manifest["signature"],
                    "requested_from_date": outcome.requested_from_date,
                    "requested_to_date": outcome.requested_to_date,
                },
            )
            for window_start, window_end in windows:
                result = await spec.runner(
                    session,
                    run_mode=actual_run_mode,
                    start_date=window_start,
                    end_date=window_end,
                )
                _merge_result_into_outcome(outcome, result)
                await session.commit()
                latest_run = await _latest_job_run(session, spec.name)
                if latest_run is not None and _enum_value(latest_run.status) == "partial":
                    outcome.status = "partial"
            logger.info(
                "DemandWatch feed run finished",
                extra={
                    "operation": operation,
                    "source_name": spec.name,
                    "status": outcome.status,
                    "fetched_items": outcome.fetched_items,
                    "inserted_rows": outcome.inserted_rows,
                    "updated_rows": outcome.updated_rows,
                    "quarantined_rows": outcome.quarantined_rows,
                    "operation_signature": manifest["signature"],
                },
            )
            return outcome
        except Exception as exc:
            failure_context = build_failure_context(exc, stage="source_run", attempt=attempt_index + 1)
            await session.rollback()
            await _record_failed_ingest_run(
                session,
                spec,
                run_mode=actual_run_mode,
                operation=operation,
                manifest=manifest,
                from_date=from_date,
                to_date=to_date,
                failure_context=failure_context,
                error_text=str(exc),
            )
            await session.commit()
            outcome.status = "failed"
            outcome.failure_category = str(failure_context["failure_category"])
            outcome.error_text = str(exc)
            if attempt_index == effective_attempts - 1 or not is_retryable_demandwatch_failure(exc):
                logger.exception(
                    "DemandWatch feed run failed",
                    extra={
                        "operation": operation,
                        "source_name": spec.name,
                        "failure_category": classify_demandwatch_failure(exc),
                        "operation_signature": manifest["signature"],
                    },
                )
                return outcome
            delay_seconds = demandwatch_retry_delay_seconds(attempt_index, run_mode=run_mode)
            logger.warning(
                "DemandWatch feed run retrying after failure",
                extra={
                    "operation": operation,
                    "source_name": spec.name,
                    "attempt": attempt_index + 1,
                    "retry_delay_seconds": delay_seconds,
                    "failure_category": classify_demandwatch_failure(exc),
                    "operation_signature": manifest["signature"],
                },
            )
            await sleep_fn(delay_seconds)

    return outcome


async def assert_demandwatch_ingest_registry(
    session: AsyncSession,
    specs: list[DemandWatchSourceSpec],
) -> None:
    await assert_demandwatch_registry_seeded(session)

    missing: list[str] = []
    for spec in specs:
        try:
            await get_source_bundle(session, spec.source_slug, spec.release_slug)
        except ValueError:
            missing.append(f"{spec.source_slug}/{spec.release_slug}")

    if missing:
        missing_text = ", ".join(missing)
        raise DemandWatchSetupError(
            "DemandWatch source registry is incomplete in the backend database. "
            f"Missing: {missing_text}. {SETUP_INSTRUCTIONS}"
        )


async def _load_demandwatch_runs(session: AsyncSession) -> list[IngestRun]:
    result = await session.execute(
        select(IngestRun)
        .where(IngestRun.job_name.in_(list_demandwatch_sources()))
        .order_by(IngestRun.started_at.desc())
    )
    return list(result.scalars().all())


def build_demandwatch_source_health(
    bundle: DemandStoreBundle,
    runs: list[object],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    items: list[dict[str, Any]] = []
    summary_counts = {"healthy": 0, "degraded": 0, "failing": 0, "deferred": 0}

    for spec in DEMANDWATCH_SOURCE_SPECS:
        related_series = [
            series
            for series in bundle.series_by_id.values()
            if series.metadata.get("release_slug") == spec.release_slug
        ]
        related_statuses = []
        for series in related_series:
            metrics = bundle.latest_metrics_by_series_id[series.id]
            audit_status, reasons = _audit_status_for_series(series, metrics)
            related_statuses.append((series, metrics, audit_status, reasons))

        counts = {"live": 0, "partial": 0, "deferred": 0, "blocked": 0}
        stale_series_count = 0
        unit_issue_count = 0
        latest_release_dates: list[datetime] = []
        latest_vintage_dates: list[datetime] = []
        for _series, metrics, audit_status, _reasons in related_statuses:
            counts[audit_status] += 1
            if audit_status in {"live", "partial"} and metrics.stale:
                stale_series_count += 1
            if not metrics.canonical_units_ok:
                unit_issue_count += 1
            if metrics.latest_release_date is not None:
                latest_release_dates.append(metrics.latest_release_date)
            if metrics.latest_vintage_at is not None:
                latest_vintage_dates.append(metrics.latest_vintage_at)

        source_runs = [run for run in runs if str(getattr(run, "job_name", "")) == spec.name]
        last_run = source_runs[0] if source_runs else None
        last_success = next(
            (
                run
                for run in source_runs
                if _enum_value(getattr(run, "status", "")) in {"success", "partial"}
            ),
            None,
        )
        last_failure = next(
            (run for run in source_runs if _enum_value(getattr(run, "status", "")) == "failed"),
            None,
        )
        consecutive_failures = 0
        for run in source_runs:
            if _enum_value(getattr(run, "status", "")) in {"success", "partial"}:
                break
            if _enum_value(getattr(run, "status", "")) == "failed":
                consecutive_failures += 1

        health_status = "healthy"
        reasons: list[str] = []
        active_series_count = counts["live"] + counts["partial"]
        if active_series_count <= 0:
            health_status = "deferred"
            reasons.append("No live DemandWatch indicators are currently assigned to this feed.")
        else:
            if stale_series_count >= active_series_count and stale_series_count > 0:
                health_status = "failing"
                reasons.append(f"All {active_series_count} active series are stale.")
            elif consecutive_failures >= 2:
                health_status = "failing"
                reasons.append(f"{consecutive_failures} consecutive ingest runs have failed.")
            elif last_run is not None and _enum_value(getattr(last_run, "status", "")) == "failed" and last_success is None:
                health_status = "failing"
                reasons.append("No successful ingest run has been recorded for this feed.")
            elif stale_series_count > 0 or counts["partial"] > 0 or unit_issue_count > 0:
                health_status = "degraded"

            if stale_series_count > 0:
                reasons.append(f"{stale_series_count} active series are stale.")
            if counts["partial"] > 0:
                reasons.append(f"{counts['partial']} active series have partial coverage.")
            if unit_issue_count > 0:
                reasons.append(f"{unit_issue_count} series failed canonical-unit validation.")
            if last_run is not None and _enum_value(getattr(last_run, "status", "")) == "partial":
                reasons.append("Latest ingest run finished with partial status.")
            if last_failure is not None and _enum_value(getattr(last_failure, "status", "")) == "failed":
                failure_category = ((getattr(last_failure, "metadata_", {}) or {}).get("failure_category"))
                if failure_category:
                    reasons.append(f"Most recent failed run was classified as {failure_category}.")

        summary_counts[health_status] += 1
        parse_failure_count_30d = 0
        health_window_start = now - timedelta(days=SOURCE_HEALTH_WINDOW_DAYS)
        for run in source_runs:
            finished_at = getattr(run, "finished_at", None)
            metadata = getattr(run, "metadata_", {}) or {}
            if (
                metadata.get("failure_category") == "parse_error"
                and finished_at is not None
                and finished_at >= health_window_start
            ):
                parse_failure_count_30d += 1

        items.append(
            {
                "name": spec.name,
                "display_name": spec.display_name,
                "source_slug": spec.source_slug,
                "release_slug": spec.release_slug,
                "status": health_status,
                "series_counts": counts,
                "active_series_count": active_series_count,
                "stale_series_count": stale_series_count,
                "unit_issue_count": unit_issue_count,
                "latest_release_date": max(latest_release_dates).isoformat() if latest_release_dates else None,
                "latest_vintage_at": max(latest_vintage_dates).isoformat() if latest_vintage_dates else None,
                "last_run_status": _enum_value(getattr(last_run, "status", "")) if last_run is not None else None,
                "last_run_finished_at": getattr(last_run, "finished_at", None).isoformat()
                if last_run is not None and getattr(last_run, "finished_at", None) is not None
                else None,
                "last_success_at": getattr(last_success, "finished_at", None).isoformat()
                if last_success is not None and getattr(last_success, "finished_at", None) is not None
                else None,
                "last_failure_at": getattr(last_failure, "finished_at", None).isoformat()
                if last_failure is not None and getattr(last_failure, "finished_at", None) is not None
                else None,
                "last_failure_category": ((getattr(last_failure, "metadata_", {}) or {}).get("failure_category"))
                if last_failure is not None
                else None,
                "last_error_text": getattr(last_failure, "error_text", None) if last_failure is not None else None,
                "consecutive_failures": consecutive_failures,
                "parse_failures_30d": parse_failure_count_30d,
                "reasons": reasons,
            }
        )

    return {
        "generated_at": now.isoformat(),
        "summary": {
            "feed_count": len(items),
            "status_counts": summary_counts,
        },
        "items": items,
    }


def build_demandwatch_canonical_unit_audit(bundle: DemandStoreBundle) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    summary = {"ok": 0, "violations": 0, "unregistered": 0}

    for series in sorted(bundle.series_by_id.values(), key=lambda item: (item.vertical_code, item.display_order, item.code)):
        metrics = bundle.latest_metrics_by_series_id[series.id]
        expected_unit = expected_canonical_unit_for_series(series.code)
        reasons: list[str] = []
        status = "ok"
        if expected_unit is None:
            status = "unregistered"
            reasons.append("Series is missing an explicit canonical-unit policy entry.")
        elif series.canonical_unit_code != expected_unit:
            status = "violation"
            reasons.append(
                f"Expected canonical unit {expected_unit}, found {series.canonical_unit_code or 'none'}."
            )
        if series.canonical_unit_code and series.canonical_unit_code not in bundle.units_by_code:
            status = "violation"
            reasons.append(f"Canonical unit {series.canonical_unit_code} is missing from unit definitions.")
        if not metrics.canonical_units_ok and metrics.canonical_unit_reason:
            status = "violation"
            reasons.append(metrics.canonical_unit_reason)

        if status == "ok":
            summary["ok"] += 1
        elif status == "unregistered":
            summary["unregistered"] += 1
        else:
            summary["violations"] += 1

        items.append(
            {
                "series_id": series.id,
                "code": series.code,
                "name": series.name,
                "vertical_code": series.vertical_code,
                "measure_family": series.measure_family,
                "frequency": series.frequency,
                "status": status,
                "expected_unit_code": expected_unit,
                "configured_unit_code": series.canonical_unit_code,
                "configured_unit_symbol": series.canonical_unit_symbol,
                "reasons": reasons,
            }
        )

    return {
        "summary": summary,
        "items": items,
    }


async def build_demandwatch_operational_audit(
    session: AsyncSession,
    *,
    bundle: DemandStoreBundle | None = None,
    now: datetime | None = None,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    bundle = bundle or await load_demandwatch_bundle(session)
    runs = await _load_demandwatch_runs(session)
    coverage = build_demandwatch_coverage_audit(bundle, now=now)
    source_health = build_demandwatch_source_health(bundle, runs, now=now)
    canonical_units = build_demandwatch_canonical_unit_audit(bundle)
    return {
        "generated_at": now.isoformat(),
        "manifest": manifest,
        "source_health": source_health,
        "canonical_units": canonical_units,
        "coverage": coverage,
    }


def demandwatch_operational_audit_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# DemandWatch Operational Audit",
        "",
        f"Generated at: {audit['generated_at']}",
    ]
    manifest = audit.get("manifest") or {}
    if manifest:
        lines.extend(
            [
                "",
                "## Run Manifest",
                "",
                f"- Operation: {manifest.get('operation')}",
                f"- Signature: `{manifest.get('signature')}`",
                f"- Sources: {', '.join(manifest.get('sources') or []) or '-'}",
                f"- Run mode: {manifest.get('run_mode')}",
            ]
        )
        if manifest.get("from_date") or manifest.get("to_date"):
            lines.append(
                f"- Window: {manifest.get('from_date') or '-'} -> {manifest.get('to_date') or '-'}"
            )

    source_health = audit["source_health"]
    lines.extend(
        [
            "",
            "## Source Health",
            "",
            f"- Healthy: {source_health['summary']['status_counts']['healthy']}",
            f"- Degraded: {source_health['summary']['status_counts']['degraded']}",
            f"- Failing: {source_health['summary']['status_counts']['failing']}",
            f"- Deferred: {source_health['summary']['status_counts']['deferred']}",
            "",
            "| Feed | Status | Active Series | Stale | Last Run | Reasons |",
            "| --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for item in source_health["items"]:
        lines.append(
            "| {feed} | {status} | {active} | {stale} | {last_run} | {reasons} |".format(
                feed=item["display_name"],
                status=item["status"],
                active=item["active_series_count"],
                stale=item["stale_series_count"],
                last_run=item["last_run_status"] or "-",
                reasons="; ".join(item["reasons"]) or "-",
            )
        )

    canonical_units = audit["canonical_units"]
    lines.extend(
        [
            "",
            "## Canonical Units",
            "",
            f"- OK: {canonical_units['summary']['ok']}",
            f"- Violations: {canonical_units['summary']['violations']}",
            f"- Missing policy: {canonical_units['summary']['unregistered']}",
        ]
    )
    if canonical_units["summary"]["violations"] or canonical_units["summary"]["unregistered"]:
        lines.extend(
            [
                "",
                "| Series | Status | Expected | Configured | Reasons |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for item in canonical_units["items"]:
            if item["status"] == "ok":
                continue
            lines.append(
                "| {code} | {status} | {expected} | {configured} | {reasons} |".format(
                    code=item["code"],
                    status=item["status"],
                    expected=item["expected_unit_code"] or "-",
                    configured=item["configured_unit_code"] or "-",
                    reasons="; ".join(item["reasons"]) or "-",
                )
            )

    lines.extend(
        [
            "",
            demandwatch_coverage_audit_markdown(audit["coverage"]),
        ]
    )
    return "\n".join(lines)


def audit_failures_at_or_above(audit: dict[str, Any], level: str) -> list[dict[str, Any]]:
    threshold = AUDIT_FAILURE_RANK.get(str(level), 99)
    return [
        item
        for item in audit["source_health"]["items"]
        if AUDIT_FAILURE_RANK.get(item["status"], 99) >= threshold
    ]


async def _run_ingest_operation(
    *,
    operation: str,
    run_mode: str,
    sources: list[str] | None,
    from_date: date | None = None,
    to_date: date | None = None,
    continue_on_error: bool = False,
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
) -> dict[str, Any]:
    resolved_specs = resolve_demandwatch_source_specs(sources)
    manifest = build_demandwatch_operation_manifest(
        operation=operation,
        sources=[spec.name for spec in resolved_specs],
        run_mode=run_mode,
        from_date=from_date,
        to_date=to_date,
        continue_on_error=continue_on_error,
        max_attempts=max_attempts,
    )
    session_factory = get_session_factory()
    outcomes: list[DemandWatchSourceOutcome] = []
    async with session_factory() as session:
        await assert_demandwatch_ingest_registry(session, resolved_specs)
        for spec in resolved_specs:
            outcome = await _run_source_with_retries(
                session,
                spec,
                operation=operation,
                run_mode=run_mode,
                from_date=from_date,
                to_date=to_date,
                max_attempts=max_attempts,
                manifest=manifest,
            )
            outcomes.append(outcome)
            if outcome.status == "failed" and not continue_on_error:
                break

        try:
            await recompute_demandwatch_snapshot(session)
        except DemandWatchSetupError:
            # Public reads refresh from the published artifact; ingest can complete before publish runs.
            pass
        await session.commit()
        bundle = await load_demandwatch_bundle(session)
        audit = await build_demandwatch_operational_audit(session, bundle=bundle, manifest=manifest)

    status_counts = {"success": 0, "partial": 0, "failed": 0}
    for outcome in outcomes:
        status_counts[outcome.status] = status_counts.get(outcome.status, 0) + 1

    return {
        "operation": operation,
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest": manifest,
        "sources": [outcome.to_item() for outcome in outcomes],
        "summary": {
            "requested_source_count": len(resolved_specs),
            "completed_source_count": len(outcomes),
            "status_counts": status_counts,
            "failed_sources": [outcome.name for outcome in outcomes if outcome.status == "failed"],
            "fetched_items": sum(outcome.fetched_items for outcome in outcomes),
            "inserted_rows": sum(outcome.inserted_rows for outcome in outcomes),
            "updated_rows": sum(outcome.updated_rows for outcome in outcomes),
            "quarantined_rows": sum(outcome.quarantined_rows for outcome in outcomes),
        },
        "audit": audit,
    }


async def run_demandwatch_refresh(
    *,
    sources: list[str] | None = None,
    run_mode: str = "manual",
    continue_on_error: bool = False,
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
) -> dict[str, Any]:
    return await _run_ingest_operation(
        operation="refresh",
        run_mode=run_mode,
        sources=sources,
        continue_on_error=continue_on_error,
        max_attempts=max_attempts,
    )


async def run_demandwatch_backfill(
    *,
    sources: list[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    continue_on_error: bool = False,
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
) -> dict[str, Any]:
    to_date = to_date or date.today()
    from_date = from_date or demandwatch_default_from_date(to_date)
    return await _run_ingest_operation(
        operation="backfill",
        run_mode="backfill",
        sources=sources,
        from_date=from_date,
        to_date=to_date,
        continue_on_error=continue_on_error,
        max_attempts=max_attempts,
    )


async def run_demandwatch_publish(
    *,
    output_path: Path | None = None,
) -> dict[str, Any]:
    output_path = output_path or default_demandwatch_publish_path()
    manifest = build_demandwatch_operation_manifest(
        operation="publish",
        sources=list_demandwatch_sources(),
        run_mode="manual",
        output_path=str(output_path),
    )
    session_factory = get_session_factory()
    async with session_factory() as session:
        await assert_demandwatch_registry_seeded(session)
        publish_result = await publish_demandwatch_store(session, output_path)
        await recompute_demandwatch_snapshot(session)
        await session.commit()
        bundle = await load_demandwatch_bundle(session)
        audit = await build_demandwatch_operational_audit(session, bundle=bundle, manifest=manifest)
    return {
        "operation": "publish",
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest": manifest,
        "published_store": publish_result,
        "audit": audit,
    }


async def run_demandwatch_audit() -> dict[str, Any]:
    manifest = build_demandwatch_operation_manifest(
        operation="audit",
        sources=list_demandwatch_sources(),
        run_mode="manual",
    )
    session_factory = get_session_factory()
    async with session_factory() as session:
        await assert_demandwatch_registry_seeded(session)
        audit = await build_demandwatch_operational_audit(session, manifest=manifest)
    return {
        "operation": "audit",
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest": manifest,
        "audit": audit,
    }


def write_json_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown_artifact(path: Path, markdown: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
