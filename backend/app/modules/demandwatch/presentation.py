from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from app.modules.demandwatch.published_store import (
    DemandLatestMetrics,
    DemandSeriesDefinition,
    DemandStoreBundle,
    build_demandwatch_coverage_audit,
    demandwatch_coverage_audit_markdown,
    latest_vintage_observations,
    utcnow,
)


TIER_LABELS = {
    "t1_direct": "T1 · Direct",
    "t2_throughput": "T2 · Throughput",
    "t3_trade": "T3 · Trade Flow",
    "t4_end_use": "T4 · End-Use",
    "t5_leading": "T5 · Leading",
    "t6_macro": "T6 · Macro",
    "t7_weather": "T7 · Weather",
}

VERTICAL_PUBLIC_IDS = {
    "crude_products": "crude-products",
    "electricity": "electricity",
    "grains_oilseeds": "grains",
    "base_metals": "base-metals",
}

VERTICAL_CODE_BY_PUBLIC_ID = {public_id: code for code, public_id in VERTICAL_PUBLIC_IDS.items()}

SERIES_TITLE_OVERRIDES = {
    "EIA_US_TOTAL_PRODUCT_SUPPLIED": "Total product supplied",
    "EIA_GASOLINE_US_PRODUCT_SUPPLIED": "Gasoline product supplied",
    "EIA_DISTILLATE_US_PRODUCT_SUPPLIED": "Distillate product supplied",
    "EIA_CRUDE_US_REFINERY_INPUTS": "Refinery crude inputs",
    "EIA_CRUDE_US_REFINERY_UTILISATION": "Refinery utilisation",
    "FRED_US_VEHICLE_MILES_TRAVELED": "Vehicle miles traveled",
    "EIA_US_ELECTRICITY_GRID_LOAD": "US grid load",
    "EMBER_GLOBAL_ELECTRICITY_DEMAND": "Global electricity demand",
    "EMBER_CHINA_ELECTRICITY_DEMAND": "China electricity demand",
    "USDA_US_CORN_TOTAL_USE_WASDE": "Corn total use",
    "USDA_US_SOYBEAN_TOTAL_USE_WASDE": "Soybean total use",
    "USDA_US_WHEAT_TOTAL_USE_WASDE": "Wheat total use",
    "USDA_US_CORN_EXPORT_SALES": "Corn export sales",
    "USDA_US_SOYBEAN_EXPORT_SALES": "Soybean export sales",
    "USDA_US_WHEAT_EXPORT_SALES": "Wheat export sales",
    "EIA_US_ETHANOL_PRODUCTION": "Ethanol production",
    "FRED_US_INDUSTRIAL_PRODUCTION": "Industrial production",
    "FRED_US_MANUFACTURING_PRODUCTION": "Manufacturing production",
    "FRED_US_MANUFACTURING_CAPACITY_UTILISATION": "Manufacturing capacity utilisation",
    "FRED_US_HOUSING_STARTS": "Housing starts",
    "FRED_US_TOTAL_VEHICLE_SALES": "Total vehicle sales",
    "FRED_US_BUILDING_PERMITS": "Building permits",
}

RELEASE_TITLE_OVERRIDES = {
    "demand_eia_wpsr": "EIA Weekly Petroleum Status Report",
    "demand_eia_grid_monitor": "EIA Grid Monitor",
    "demand_fred_g17": "Federal Reserve G.17 Industrial Production",
    "demand_fred_new_residential_construction": "US New Residential Construction",
    "demand_fred_motor_vehicle_sales": "US Total Vehicle Sales",
    "demand_fred_traffic_volume_trends": "US Traffic Volume Trends",
    "demand_usda_wasde": "USDA WASDE Monthly Report",
    "demand_usda_export_sales": "USDA Export Sales Report",
    "demand_ember_monthly_electricity": "Ember Monthly Electricity Demand",
}

SCORECARD_LABELS = {
    "crude_products": "US product supplied",
    "electricity": "US grid load",
    "grains_oilseeds": "USDA total use",
    "base_metals": "US industrial production",
}


@dataclass(frozen=True, slots=True)
class DemandSectionConfig:
    id: str
    title: str
    description: str
    tiers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DemandVerticalPresentation:
    public_id: str
    notes: tuple[str, ...]
    macro_priority: int | None
    macro_label: str | None
    macro_descriptor: str | None
    primary_series_code: str
    sections: tuple[DemandSectionConfig, ...]


VERTICAL_PRESENTATION = {
    "crude_products": DemandVerticalPresentation(
        public_id="crude-products",
        notes=(
            "Global oil demand tables remain restricted. DemandWatch sticks to public-domain EIA series and marks restricted tables as deferred or blocked.",
            "China direct customs and NBS demand series remain outside ingestion until republication terms are verified.",
        ),
        macro_priority=None,
        macro_label=None,
        macro_descriptor=None,
        primary_series_code="EIA_US_TOTAL_PRODUCT_SUPPLIED",
        sections=(
            DemandSectionConfig(
                id="direct",
                title="Direct Consumption",
                description="Measured demand anchors from the public-domain EIA petroleum releases.",
                tiers=("t1_direct",),
            ),
            DemandSectionConfig(
                id="throughput",
                title="Throughput Proxies",
                description="Refinery runs confirm whether crude demand is translating into system pull.",
                tiers=("t2_throughput",),
            ),
            DemandSectionConfig(
                id="context",
                title="Trade · End-Use Context",
                description="Demand gaps are called out explicitly rather than filled with restricted data.",
                tiers=("t3_trade", "t4_end_use", "t5_leading", "t6_macro"),
            ),
        ),
    ),
    "electricity": DemandVerticalPresentation(
        public_id="electricity",
        notes=(
            "Power is the cleanest launch vertical because both EIA and Ember provide legally safe direct-demand coverage.",
            "HDD and CDD remain read-only WeatherWatch context. DemandWatch does not store its own weather series.",
        ),
        macro_priority=40,
        macro_label="US Grid Load",
        macro_descriptor="EIA real-time power demand",
        primary_series_code="EIA_US_ELECTRICITY_GRID_LOAD",
        sections=(
            DemandSectionConfig(
                id="direct",
                title="Direct Load",
                description="Direct electricity demand is the highest-signal launch page because measurement is timely and explicit.",
                tiers=("t1_direct",),
            ),
            DemandSectionConfig(
                id="context",
                title="Context",
                description="Weather stays linked from WeatherWatch while structural demand context remains read-only here.",
                tiers=("t2_throughput", "t5_leading", "t6_macro", "t7_weather"),
            ),
        ),
    ),
    "grains_oilseeds": DemandVerticalPresentation(
        public_id="grains",
        notes=(
            "USDA remains the MVP anchor because WASDE, PSD, and Export Sales are public domain and machine-readable.",
            "Fertiliser demand stays nested under grains at launch because direct public-domain consumption data remains thin.",
        ),
        macro_priority=None,
        macro_label=None,
        macro_descriptor=None,
        primary_series_code="USDA_US_CORN_TOTAL_USE_WASDE",
        sections=(
            DemandSectionConfig(
                id="direct",
                title="Direct Demand Estimates",
                description="Monthly USDA demand estimates provide the cleanest public-domain agricultural anchor.",
                tiers=("t1_direct",),
            ),
            DemandSectionConfig(
                id="flow",
                title="Trade · Throughput",
                description="Weekly export sales and ethanol runs keep the grains page event-driven.",
                tiers=("t2_throughput", "t3_trade", "t5_leading"),
            ),
        ),
    ),
    "base_metals": DemandVerticalPresentation(
        public_id="base-metals",
        notes=(
            "Base-metals demand stays anchored to public-domain macro releases rather than raw PMI republication.",
            "China metals demand series remain deferred until republication terms are legally confirmed.",
        ),
        macro_priority=10,
        macro_label="US Industrial Production",
        macro_descriptor="Federal Reserve G.17",
        primary_series_code="FRED_US_INDUSTRIAL_PRODUCTION",
        sections=(
            DemandSectionConfig(
                id="macro",
                title="Macro Backbone",
                description="Public-domain Federal Reserve and Census releases carry the base-metals demand page.",
                tiers=("t6_macro",),
            ),
            DemandSectionConfig(
                id="end-use",
                title="End-Use · Leading",
                description="Construction-linked demand proxies sit underneath the macro stack.",
                tiers=("t4_end_use", "t5_leading", "t2_throughput", "t3_trade"),
            ),
        ),
    ),
}


MACRO_STRIP_SERIES = (
    ("FRED_US_INDUSTRIAL_PRODUCTION", "indpro", "US Industrial Production", "Federal Reserve G.17"),
    ("FRED_US_MANUFACTURING_PRODUCTION", "manufacturing", "US Manufacturing Production", "Federal Reserve G.17"),
    ("FRED_US_HOUSING_STARTS", "housing", "US Housing Starts", "Census / FRED"),
    ("EIA_US_ELECTRICITY_GRID_LOAD", "grid-load", "US Grid Load", "EIA real-time demand"),
    ("EMBER_CHINA_ELECTRICITY_DEMAND", "china-power", "China Power Demand", "Ember monthly demand"),
)

WEEKDAY_BY_RULE = {
    "MO": 0,
    "TU": 1,
    "WE": 2,
    "TH": 3,
    "FR": 4,
    "SA": 5,
    "SU": 6,
}


@dataclass(frozen=True, slots=True)
class DemandReleaseSchedule:
    release_slug: str
    release_name: str
    source_slug: str
    source_name: str
    cadence: str
    schedule_timezone: str
    schedule_rule: str
    default_local_time: time | None
    is_calendar_driven: bool
    source_url: str | None
    latest_release_at: datetime | None
    vertical_codes: tuple[str, ...]
    series_codes: tuple[str, ...]


def resolve_vertical_code(value: str) -> str | None:
    if value in VERTICAL_PUBLIC_IDS:
        return value
    return VERTICAL_CODE_BY_PUBLIC_ID.get(value)


def public_vertical_id(code: str) -> str:
    return VERTICAL_PUBLIC_IDS.get(code, code.replace("_", "-"))


def _clean_series_title(series: DemandSeriesDefinition) -> str:
    if series.code in SERIES_TITLE_OVERRIDES:
        return SERIES_TITLE_OVERRIDES[series.code]
    name = series.name
    for prefix in ("EIA ", "FRED ", "USDA ", "Ember "):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break
    return name


def _series_trend(metrics: DemandLatestMetrics) -> str:
    if metrics.trend_3m_direction != "unknown":
        return metrics.trend_3m_direction
    if metrics.yoy_pct is not None:
        if metrics.yoy_pct > 1.0:
            return "up"
        if metrics.yoy_pct < -1.0:
            return "down"
        return "flat"
    if metrics.change_pct is not None:
        if metrics.change_pct > 0.5:
            return "up"
        if metrics.change_pct < -0.5:
            return "down"
        return "flat"
    if metrics.surprise_flag and metrics.surprise_direction == "positive":
        return "up"
    if metrics.surprise_flag and metrics.surprise_direction == "negative":
        return "down"
    return "flat"


def _format_signed_number(value: float, decimals: int = 1) -> str:
    return f"{value:+,.{decimals}f}"


def _format_value(value: float | None, unit_code: str | None, unit_symbol: str | None = None) -> str | None:
    if value is None:
        return None

    normalized_unit = str(unit_code or "").lower()
    if normalized_unit == "kb_d":
        if abs(value) >= 1000:
            return f"{value / 1000.0:,.1f} mb/d"
        return f"{value:,.0f} kb/d"
    if normalized_unit == "pct":
        return f"{value:,.1f}%"
    if normalized_unit == "mbu":
        if abs(value) >= 1000:
            return f"{value / 1000.0:,.2f} bn bu"
        return f"{value:,.0f} mbu"
    if normalized_unit == "mmt":
        return f"{value:,.2f} mmt"
    if normalized_unit == "gw":
        return f"{value:,.0f} GW"
    if normalized_unit == "twh":
        return f"{value:,.1f} TWh"
    if normalized_unit == "m_vehicle_miles":
        return f"{value:,.0f}m mi"
    if normalized_unit == "m_units_saar":
        return f"{value:,.2f}m SAAR"
    if normalized_unit == "k_units_saar":
        return f"{value / 1000.0:,.2f}m" if abs(value) >= 1000 else f"{value:,.0f}k"
    if normalized_unit == "index":
        return f"{value:,.1f}"
    if unit_symbol:
        return f"{value:,.1f} {unit_symbol}"
    return f"{value:,.1f}"


def _format_abs_change(value: float | None, unit_code: str | None, unit_symbol: str | None = None) -> str | None:
    if value is None:
        return None
    normalized_unit = str(unit_code or "").lower()
    if normalized_unit == "pct":
        return f"{value:+.1f}pp"
    if normalized_unit == "kb_d":
        if abs(value) >= 1000:
            return f"{value / 1000.0:+.1f} mb/d"
        return f"{value:+,.0f} kb/d"
    if normalized_unit == "mbu":
        if abs(value) >= 1000:
            return f"{value / 1000.0:+.2f} bn bu"
        return f"{value:+,.0f} mbu"
    if normalized_unit == "mmt":
        return f"{value:+,.2f} mmt"
    if normalized_unit == "gw":
        return f"{value:+,.0f} GW"
    if normalized_unit == "twh":
        return f"{value:+,.1f} TWh"
    if normalized_unit == "m_vehicle_miles":
        return f"{value:+,.0f}m mi"
    if normalized_unit == "m_units_saar":
        return f"{value:+,.2f}m SAAR"
    if normalized_unit == "k_units_saar":
        return f"{value / 1000.0:+.2f}m" if abs(value) >= 1000 else f"{value:+,.0f}k"
    if normalized_unit == "index":
        return _format_signed_number(value, 1)
    if unit_symbol:
        return f"{value:+,.1f} {unit_symbol}"
    return _format_signed_number(value, 1)


def _format_yoy_label(metrics: DemandLatestMetrics) -> str | None:
    if metrics.yoy_pct is not None:
        return f"{metrics.yoy_pct:+.1f}% YoY"
    if metrics.yoy_abs is not None:
        return f"{_format_abs_change(metrics.yoy_abs, metrics.unit_code, metrics.unit_symbol)} YoY"
    return None


def _freshness_label(metrics: DemandLatestMetrics) -> str:
    age = metrics.release_age_days if metrics.release_age_days is not None else metrics.period_age_days
    if age is None:
        return "Unknown"
    if age == 0:
        return "Today"
    if age < 7:
        return f"{age}d ago"
    if age < 60:
        return f"{max(1, age // 7)}w ago"
    return f"{max(1, age // 30)}mo ago"


def _series_for_vertical(bundle: DemandStoreBundle, vertical_code: str, *, live_only: bool) -> list[DemandSeriesDefinition]:
    return [
        series
        for series in sorted(bundle.series_by_id.values(), key=lambda item: (item.vertical_code, item.display_order, item.code))
        if series.vertical_code == vertical_code and (not live_only or series.coverage_status == "live")
    ]


def _metrics(bundle: DemandStoreBundle, series: DemandSeriesDefinition) -> DemandLatestMetrics:
    return bundle.latest_metrics_by_series_id[series.id]


def _primary_series(bundle: DemandStoreBundle, vertical_code: str) -> DemandSeriesDefinition | None:
    presentation = VERTICAL_PRESENTATION.get(vertical_code)
    if presentation is not None:
        explicit = next(
            (
                series
                for series in _series_for_vertical(bundle, vertical_code, live_only=True)
                if series.code == presentation.primary_series_code
            ),
            None,
        )
        if explicit is not None:
            return explicit

    live_series = _series_for_vertical(bundle, vertical_code, live_only=True)
    return next((series for series in live_series if series.tier == "t1_direct"), live_series[0] if live_series else None)


def _sparkline(bundle: DemandStoreBundle, series: DemandSeriesDefinition, limit: int = 7) -> list[float]:
    latest_points = latest_vintage_observations(bundle.observations_by_series_id.get(series.id, []))
    return [point.value_canonical for point in latest_points[-limit:]]


def _detail_text(metrics: DemandLatestMetrics) -> str | None:
    if metrics.moving_average_4w is not None:
        formatted = _format_value(metrics.moving_average_4w, metrics.unit_code, metrics.unit_symbol)
        if formatted:
            return f"4-week average at {formatted}"
    if metrics.trend_3m_pct is not None:
        return f"3-month trend {_format_signed_number(metrics.trend_3m_pct, 1)}%"
    if metrics.surprise_reason:
        return metrics.surprise_reason
    if metrics.latest_period_label:
        return f"Latest data {metrics.latest_period_label}"
    return None


def _indicator_card(bundle: DemandStoreBundle, series: DemandSeriesDefinition) -> dict[str, object]:
    metrics = _metrics(bundle, series)
    return {
        "series_id": series.id,
        "indicator_id": series.indicator_id,
        "code": series.code,
        "title": _clean_series_title(series),
        "tier": series.tier,
        "tier_label": TIER_LABELS.get(series.tier, series.tier),
        "latest_value": metrics.latest_value,
        "unit_code": metrics.unit_code,
        "unit_symbol": metrics.unit_symbol,
        "display_value": _format_value(metrics.latest_value, metrics.unit_code, metrics.unit_symbol),
        "change_label": _format_yoy_label(metrics) or _format_abs_change(metrics.change_abs, metrics.unit_code, metrics.unit_symbol),
        "detail": _detail_text(metrics),
        "trend": _series_trend(metrics),
        "sparkline": _sparkline(bundle, series),
        "freshness": _freshness_label(metrics),
        "freshness_state": metrics.freshness_state,
        "latest_period_label": metrics.latest_period_label,
        "latest_release_date": metrics.latest_release_date,
        "latest_vintage_at": metrics.latest_vintage_at,
        "source_url": metrics.latest_source_url or series.source_url,
        "coverage_status": series.coverage_status,
        "vintage_count": metrics.vintage_count,
    }


def _indicator_table_row(bundle: DemandStoreBundle, series: DemandSeriesDefinition) -> dict[str, object]:
    metrics = _metrics(bundle, series)
    return {
        "series_id": series.id,
        "indicator_id": series.indicator_id,
        "code": series.code,
        "label": _clean_series_title(series),
        "tier": series.tier,
        "tier_label": TIER_LABELS.get(series.tier, series.tier),
        "latest_value": metrics.latest_value,
        "unit_code": metrics.unit_code,
        "unit_symbol": metrics.unit_symbol,
        "latest_display": _format_value(metrics.latest_value, metrics.unit_code, metrics.unit_symbol),
        "change_display": _format_abs_change(metrics.change_abs, metrics.unit_code, metrics.unit_symbol),
        "yoy_display": _format_yoy_label(metrics),
        "freshness": _freshness_label(metrics),
        "freshness_state": metrics.freshness_state,
        "trend": _series_trend(metrics),
        "latest_period_label": metrics.latest_period_label,
        "latest_release_date": metrics.latest_release_date,
        "source_url": metrics.latest_source_url or series.source_url,
        "vintage_count": metrics.vintage_count,
    }


def _scorecard_item(bundle: DemandStoreBundle, vertical_code: str) -> dict[str, object] | None:
    vertical = bundle.verticals_by_code.get(vertical_code)
    series = _primary_series(bundle, vertical_code)
    if vertical is None or series is None:
        return None
    metrics = _metrics(bundle, series)
    public_id = public_vertical_id(vertical_code)
    return {
        "id": public_id,
        "code": vertical.code,
        "label": vertical.name,
        "nav_label": vertical.nav_label or public_id.replace("-", " ").title(),
        "short_label": vertical.short_label or vertical.name,
        "sector": vertical.sector,
        "scorecard_label": SCORECARD_LABELS.get(vertical_code, _clean_series_title(series)),
        "latest_value": metrics.latest_value,
        "unit_code": metrics.unit_code,
        "unit_symbol": metrics.unit_symbol,
        "display_value": _format_value(metrics.latest_value, metrics.unit_code, metrics.unit_symbol),
        "yoy_value": metrics.yoy_pct,
        "yoy_label": _format_yoy_label(metrics),
        "trend": _series_trend(metrics),
        "latest_period_label": metrics.latest_period_label,
        "freshness": _freshness_label(metrics),
        "freshness_state": metrics.freshness_state,
        "stale": metrics.stale,
        "source_url": metrics.latest_source_url or series.source_url,
        "primary_series_code": series.code,
    }


def build_macro_strip_payload(bundle: DemandStoreBundle, *, now: datetime | None = None) -> dict[str, object]:
    generated_at = now or utcnow()
    items: list[dict[str, object]] = []
    series_by_code = {series.code: series for series in bundle.series_by_id.values()}

    for code, item_id, label, descriptor in MACRO_STRIP_SERIES:
        series = series_by_code.get(code)
        if series is None or series.coverage_status != "live":
            continue
        metrics = _metrics(bundle, series)
        items.append(
            {
                "id": item_id,
                "code": series.code,
                "label": label,
                "descriptor": descriptor,
                "latest_value": metrics.latest_value,
                "unit_code": metrics.unit_code,
                "unit_symbol": metrics.unit_symbol,
                "display_value": _format_value(metrics.latest_value, metrics.unit_code, metrics.unit_symbol),
                "change_label": _format_yoy_label(metrics) or _format_abs_change(
                    metrics.change_abs,
                    metrics.unit_code,
                    metrics.unit_symbol,
                ),
                "trend": _series_trend(metrics),
                "freshness": _freshness_label(metrics),
                "freshness_state": metrics.freshness_state,
                "latest_period_label": metrics.latest_period_label,
                "latest_release_date": metrics.latest_release_date,
                "source_url": metrics.latest_source_url or series.source_url,
            }
        )
    return {
        "generated_at": generated_at,
        "items": items,
    }


def build_scorecard_payload(bundle: DemandStoreBundle, *, now: datetime | None = None) -> dict[str, object]:
    generated_at = now or utcnow()
    items = [
        item
        for vertical_code in sorted(
            bundle.verticals_by_code,
            key=lambda code: (bundle.verticals_by_code[code].display_order, code),
        )
        if (item := _scorecard_item(bundle, vertical_code)) is not None
    ]
    return {
        "generated_at": generated_at,
        "items": items,
    }


def build_movers_payload(bundle: DemandStoreBundle, *, limit: int = 10, now: datetime | None = None) -> dict[str, object]:
    generated_at = now or utcnow()
    items: list[dict[str, object]] = []
    for series in _series_for_vertical(bundle, "crude_products", live_only=True) + _series_for_vertical(
        bundle,
        "electricity",
        live_only=True,
    ) + _series_for_vertical(bundle, "grains_oilseeds", live_only=True) + _series_for_vertical(
        bundle,
        "base_metals",
        live_only=True,
    ):
        metrics = _metrics(bundle, series)
        if metrics.latest_release_date is None and metrics.latest_vintage_at is None:
            continue
        vertical_id = public_vertical_id(series.vertical_code)
        surprise_label = None
        if metrics.surprise_flag and metrics.surprise_reason:
            surprise_label = metrics.surprise_reason
        items.append(
            {
                "vertical_id": vertical_id,
                "vertical_code": series.vertical_code,
                "code": series.code,
                "title": _clean_series_title(series),
                "tier": series.tier,
                "tier_label": TIER_LABELS.get(series.tier, series.tier),
                "latest_value": metrics.latest_value,
                "unit_code": metrics.unit_code,
                "unit_symbol": metrics.unit_symbol,
                "display_value": _format_value(metrics.latest_value, metrics.unit_code, metrics.unit_symbol),
                "change_label": _format_yoy_label(metrics) or _format_abs_change(
                    metrics.change_abs,
                    metrics.unit_code,
                    metrics.unit_symbol,
                ),
                "surprise_label": surprise_label,
                "trend": _series_trend(metrics),
                "freshness": _freshness_label(metrics),
                "freshness_state": metrics.freshness_state,
                "latest_period_label": metrics.latest_period_label,
                "latest_release_date": metrics.latest_release_date or metrics.latest_vintage_at,
                "source_url": metrics.latest_source_url or series.source_url,
            }
        )
    items.sort(key=lambda item: item["latest_release_date"] or datetime(1970, 1, 1, tzinfo=UTC), reverse=True)
    return {
        "generated_at": generated_at,
        "items": items[:limit],
    }


def _sorted_vertical_codes(bundle: DemandStoreBundle) -> list[str]:
    return [
        code
        for code, vertical in sorted(
            bundle.verticals_by_code.items(),
            key=lambda item: (item[1].display_order, item[0]),
        )
        if vertical.active
    ]


def build_demandwatch_bootstrap_payload(
    bundle: DemandStoreBundle,
    schedules: list[DemandReleaseSchedule],
    *,
    now: datetime | None = None,
    expires_at: datetime | None = None,
    movers_limit: int = 6,
) -> dict[str, object]:
    generated_at = now or utcnow()
    snapshot_expires_at = expires_at or (generated_at + timedelta(seconds=300))
    macro_strip = build_macro_strip_payload(bundle, now=generated_at)
    scorecard = build_scorecard_payload(bundle, now=generated_at)
    movers = build_movers_payload(bundle, limit=movers_limit, now=generated_at)
    coverage_notes = build_coverage_notes_payload(bundle, now=generated_at)
    next_release_dates = build_next_release_payload(schedules, now=generated_at)
    vertical_details: list[dict[str, object]] = []
    vertical_errors: list[dict[str, str]] = []

    for vertical_code in _sorted_vertical_codes(bundle):
        release_items = [
            item
            for item in next_release_dates["items"]
            if vertical_code in item["vertical_codes"]
        ]
        try:
            vertical_details.append(
                build_vertical_detail_payload(
                    bundle,
                    vertical_code,
                    release_items,
                    now=generated_at,
                )
            )
        except Exception as exc:
            # Keep the page bootable when one vertical's detail assembly fails.
            vertical_errors.append(
                {
                    "vertical_id": public_vertical_id(vertical_code),
                    "message": str(exc) or "DemandWatch vertical detail is unavailable.",
                }
            )

    return {
        "module": "demandwatch",
        "generated_at": generated_at,
        "expires_at": snapshot_expires_at,
        "macro_strip": macro_strip,
        "scorecard": scorecard,
        "movers": movers,
        "coverage_notes": coverage_notes,
        "vertical_details": vertical_details,
        "vertical_errors": vertical_errors,
        "next_release_dates": next_release_dates,
    }


def _coverage_vertical(bundle: DemandStoreBundle, vertical_code: str) -> dict[str, object]:
    audit = build_demandwatch_coverage_audit(bundle)
    return next(item for item in audit["verticals"] if item["code"] == vertical_code)


def _coverage_summary_fact(bundle: DemandStoreBundle, vertical_code: str) -> tuple[str, str]:
    coverage = _coverage_vertical(bundle, vertical_code)
    counts = coverage["counts"]
    value = f"{counts['live']} live"
    parts = [value]
    if counts["partial"]:
        parts.append(f"{counts['partial']} partial")
    if counts["deferred"]:
        parts.append(f"{counts['deferred']} deferred")
    if counts["blocked"]:
        parts.append(f"{counts['blocked']} blocked")

    note = "MVP safe-source coverage only."
    for status in ("partial", "deferred", "blocked"):
        if coverage[status]:
            reasons = coverage[status][0]["reasons"]
            if reasons:
                note = reasons[0]
                break
    return " / ".join(parts), note


def _vertical_facts(
    bundle: DemandStoreBundle,
    vertical_code: str,
    release_items: list[dict[str, object]],
) -> list[dict[str, str]]:
    primary = _primary_series(bundle, vertical_code)
    if primary is None:
        return []
    primary_metrics = _metrics(bundle, primary)
    cadence_value = str(primary.frequency).title()
    cadence_note = release_items[0]["release_name"] if release_items else (primary.source_name or "Primary release")
    coverage_value, coverage_note = _coverage_summary_fact(bundle, vertical_code)

    surprise_series = next(
        (
            series
            for series in _series_for_vertical(bundle, vertical_code, live_only=True)
            if _metrics(bundle, series).surprise_flag
        ),
        None,
    )

    if vertical_code == "crude_products":
        return [
            {"label": "Primary cadence", "value": cadence_value, "note": str(cadence_note)},
            {
                "label": "Surprise flag",
                "value": _format_abs_change(primary_metrics.yoy_abs, primary_metrics.unit_code, primary_metrics.unit_symbol)
                or "None",
                "note": primary_metrics.surprise_reason or "No active surprise flag on the primary demand anchor.",
            },
            {"label": "Coverage", "value": coverage_value, "note": coverage_note},
        ]

    if vertical_code == "electricity":
        ember_series = next(
            (series for series in _series_for_vertical(bundle, vertical_code, live_only=True) if series.source_slug == "ember"),
            None,
        )
        return [
            {"label": "Primary cadence", "value": cadence_value, "note": str(cadence_note)},
            {
                "label": "Global edge",
                "value": ember_series.source_name if ember_series is not None else "Deferred",
                "note": "Ember remains the legally safe CC BY source for global and China power demand.",
            },
            {
                "label": "Weather link",
                "value": "Deferred",
                "note": "WeatherWatch remains the read-only source of truth for HDD and CDD context.",
            },
        ]

    if vertical_code == "grains_oilseeds":
        export_release = next((item for item in release_items if item["release_slug"] == "demand_usda_export_sales"), None)
        return [
            {"label": "Primary cadence", "value": cadence_value, "note": str(cadence_note)},
            {
                "label": "Fast release",
                "value": "Weekly",
                "note": str(export_release["release_name"]) if export_release is not None else "USDA Export Sales provides the fast weekly pulse.",
            },
            {"label": "Coverage", "value": coverage_value, "note": coverage_note},
        ]

    if vertical_code == "base_metals":
        return [
            {"label": "Primary cadence", "value": cadence_value, "note": str(cadence_note)},
            {
                "label": "PMI handling",
                "value": "Blocked",
                "note": "DemandWatch does not republish raw PMI numbers; the page uses public-domain macro releases instead.",
            },
            {"label": "Coverage", "value": coverage_value, "note": coverage_note},
        ]

    return [
        {"label": "Primary cadence", "value": cadence_value, "note": str(cadence_note)},
        {
            "label": "Surprise flag",
            "value": _clean_series_title(surprise_series) if surprise_series is not None else "None",
            "note": _metrics(bundle, surprise_series).surprise_reason if surprise_series is not None else "No active surprise flag.",
        },
        {"label": "Coverage", "value": coverage_value, "note": coverage_note},
    ]


def _vertical_summary(bundle: DemandStoreBundle, vertical_code: str) -> str:
    primary = _primary_series(bundle, vertical_code)
    if primary is None:
        vertical = bundle.verticals_by_code[vertical_code]
        return f"{vertical.name} has no live DemandWatch series yet."

    primary_metrics = _metrics(bundle, primary)
    primary_title = _clean_series_title(primary)
    primary_value = _format_value(primary_metrics.latest_value, primary_metrics.unit_code, primary_metrics.unit_symbol) or "n/a"
    primary_change = _format_yoy_label(primary_metrics) or _format_abs_change(
        primary_metrics.change_abs,
        primary_metrics.unit_code,
        primary_metrics.unit_symbol,
    )

    if vertical_code == "electricity":
        china_series = next(
            (
                series
                for series in _series_for_vertical(bundle, vertical_code, live_only=True)
                if series.code == "EMBER_CHINA_ELECTRICITY_DEMAND"
            ),
            None,
        )
        if china_series is not None:
            china_metrics = _metrics(bundle, china_series)
            china_change = _format_yoy_label(china_metrics) or _format_abs_change(
                china_metrics.change_abs,
                china_metrics.unit_code,
                china_metrics.unit_symbol,
            )
            return (
                f"Power demand is anchored by {primary_title.lower()} at {primary_value}. "
                f"China electricity demand from Ember remains {china_change or 'tracked'}."
            )

    if vertical_code == "grains_oilseeds":
        export_series = next(
            (
                series
                for series in _series_for_vertical(bundle, vertical_code, live_only=True)
                if series.code == "USDA_US_CORN_EXPORT_SALES"
            ),
            None,
        )
        if export_series is not None:
            export_metrics = _metrics(bundle, export_series)
            export_change = _format_yoy_label(export_metrics) or _format_abs_change(
                export_metrics.change_abs,
                export_metrics.unit_code,
                export_metrics.unit_symbol,
            )
            return (
                f"Grains demand is anchored by {primary_title.lower()} at {primary_value}. "
                f"Export sales are {export_change or 'part of the weekly pulse'}."
            )

    if vertical_code == "base_metals":
        housing_series = next(
            (
                series
                for series in _series_for_vertical(bundle, vertical_code, live_only=True)
                if series.code == "FRED_US_HOUSING_STARTS"
            ),
            None,
        )
        if housing_series is not None:
            housing_metrics = _metrics(bundle, housing_series)
            housing_change = _format_yoy_label(housing_metrics) or _format_abs_change(
                housing_metrics.change_abs,
                housing_metrics.unit_code,
                housing_metrics.unit_symbol,
            )
            return (
                f"Base-metals demand is running through the public macro stack: {primary_title.lower()} is {primary_change or 'updated'}, "
                f"and housing starts are {housing_change or 'still part of the construction signal'}."
            )

    return f"{primary_title} is {primary_value}. Latest change: {primary_change or 'not yet available'}."


def _build_section_payloads(bundle: DemandStoreBundle, vertical_code: str) -> list[dict[str, object]]:
    presentation = VERTICAL_PRESENTATION.get(vertical_code)
    if presentation is None:
        return []

    live_series = _series_for_vertical(bundle, vertical_code, live_only=True)
    sections: list[dict[str, object]] = []
    for section in presentation.sections:
        section_series = [series for series in live_series if series.tier in section.tiers]
        if not section_series:
            continue
        sections.append(
            {
                "id": section.id,
                "title": section.title,
                "description": section.description,
                "indicators": [_indicator_card(bundle, series) for series in section_series],
                "table_rows": [_indicator_table_row(bundle, series) for series in section_series],
            }
        )
    return sections


def build_indicator_table_payload(bundle: DemandStoreBundle, vertical_code: str, *, now: datetime | None = None) -> dict[str, object]:
    generated_at = now or utcnow()
    sections = _build_section_payloads(bundle, vertical_code)
    return {
        "generated_at": generated_at,
        "vertical_id": public_vertical_id(vertical_code),
        "vertical_code": vertical_code,
        "sections": [
            {
                "id": section["id"],
                "title": section["title"],
                "rows": section["table_rows"],
            }
            for section in sections
        ],
    }


def build_vertical_detail_payload(
    bundle: DemandStoreBundle,
    vertical_code: str,
    release_items: list[dict[str, object]],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or utcnow()
    vertical = bundle.verticals_by_code[vertical_code]
    scorecard = _scorecard_item(bundle, vertical_code)
    if scorecard is None:
        raise KeyError(vertical_code)

    notes = list(VERTICAL_PRESENTATION.get(vertical_code, DemandVerticalPresentation(public_vertical_id(vertical_code), (), None, None, None, "", ())).notes)
    coverage = _coverage_vertical(bundle, vertical_code)
    for status in ("partial", "deferred", "blocked"):
        for item in coverage[status]:
            for reason in item["reasons"]:
                if reason not in notes:
                    notes.append(reason)

    return {
        "generated_at": generated_at,
        "id": public_vertical_id(vertical_code),
        "code": vertical.code,
        "label": vertical.name,
        "nav_label": vertical.nav_label or public_vertical_id(vertical_code).replace("-", " ").title(),
        "short_label": vertical.short_label or vertical.name,
        "sector": vertical.sector,
        "description": vertical.description,
        "summary": _vertical_summary(bundle, vertical_code),
        "scorecard": scorecard,
        "facts": _vertical_facts(bundle, vertical_code, release_items),
        "sections": _build_section_payloads(bundle, vertical_code),
        "calendar": release_items,
        "notes": notes,
    }


def build_coverage_notes_payload(bundle: DemandStoreBundle, *, now: datetime | None = None) -> dict[str, object]:
    generated_at = now or utcnow()
    audit = build_demandwatch_coverage_audit(bundle, now=generated_at)
    for vertical in audit["verticals"]:
        vertical["id"] = public_vertical_id(vertical["code"])
    return {
        "generated_at": generated_at,
        "markdown": demandwatch_coverage_audit_markdown(audit),
        "summary": audit["summary"],
        "verticals": audit["verticals"],
    }


def _schedule_rule_parts(schedule_rule: str) -> dict[str, str]:
    return {
        key: value
        for part in (schedule_rule or "").split(";")
        if "=" in part
        for key, value in [part.split("=", 1)]
    }


def _clean_release_name(release_slug: str, release_name: str) -> str:
    override = RELEASE_TITLE_OVERRIDES.get(release_slug)
    if override is not None:
        return override
    normalized = release_name.removeprefix("DemandWatch ").strip()
    return normalized or release_name


def _month_last_day(target_year: int, target_month: int) -> int:
    if target_month == 12:
        return 31
    next_month = date(target_year, target_month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _next_monthly_release(
    schedule: DemandReleaseSchedule,
    now_local: datetime,
    parts: dict[str, str],
) -> tuple[datetime | None, bool]:
    release_time = schedule.default_local_time or time(0, 0)
    estimated = False

    if schedule.is_calendar_driven and schedule.latest_release_at is not None:
        latest_local = schedule.latest_release_at.astimezone(now_local.tzinfo)
        candidate = latest_local + relativedelta(months=+1)
        candidate = candidate.replace(
            hour=release_time.hour,
            minute=release_time.minute,
            second=release_time.second,
            microsecond=0,
        )
        while candidate <= now_local:
            candidate += relativedelta(months=+1)
        return candidate.astimezone(UTC), True

    monthday_raw = parts.get("BYMONTHDAY")
    if monthday_raw:
        target_day = int(monthday_raw.split(",")[0])
    elif schedule.latest_release_at is not None:
        target_day = schedule.latest_release_at.astimezone(now_local.tzinfo).day
        estimated = True
    else:
        target_day = 12 if schedule.is_calendar_driven else 1
        estimated = True

    candidate_year = now_local.year
    candidate_month = now_local.month
    while True:
        last_day = _month_last_day(candidate_year, candidate_month)
        day = min(target_day, last_day)
        candidate = datetime(
            candidate_year,
            candidate_month,
            day,
            release_time.hour,
            release_time.minute,
            release_time.second,
            tzinfo=now_local.tzinfo,
        )
        if candidate > now_local:
            return candidate.astimezone(UTC), estimated or schedule.is_calendar_driven
        next_month = datetime(candidate_year, candidate_month, 1, tzinfo=now_local.tzinfo) + relativedelta(months=+1)
        candidate_year = next_month.year
        candidate_month = next_month.month


def next_release_at(schedule: DemandReleaseSchedule, *, now: datetime | None = None) -> tuple[datetime | None, bool]:
    current = now or utcnow()
    current = current if current.tzinfo is not None else current.replace(tzinfo=UTC)
    local_tz = ZoneInfo(schedule.schedule_timezone)
    now_local = current.astimezone(local_tz)
    parts = _schedule_rule_parts(schedule.schedule_rule)
    cadence = str(schedule.cadence or "").lower()
    release_time = schedule.default_local_time or time(
        int(parts.get("BYHOUR", "0")),
        int(parts.get("BYMINUTE", "0")),
    )

    if cadence == "daily":
        candidate = now_local.replace(
            hour=release_time.hour,
            minute=release_time.minute,
            second=release_time.second,
            microsecond=0,
        )
        if candidate <= now_local:
            candidate += timedelta(days=1)
        return candidate.astimezone(UTC), False

    if cadence == "weekly":
        weekday = WEEKDAY_BY_RULE.get(parts.get("BYDAY", ""))
        if weekday is None:
            return None, True
        candidate = now_local.replace(
            hour=release_time.hour,
            minute=release_time.minute,
            second=release_time.second,
            microsecond=0,
        )
        candidate += timedelta(days=(weekday - candidate.weekday()) % 7)
        if candidate <= now_local:
            candidate += timedelta(days=7)
        return candidate.astimezone(UTC), False

    if cadence == "monthly":
        return _next_monthly_release(schedule, now_local, parts)

    return None, True


def build_next_release_payload(
    schedules: list[DemandReleaseSchedule],
    *,
    vertical_code: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or utcnow()
    items: list[dict[str, object]] = []
    for schedule in schedules:
        if vertical_code is not None and vertical_code not in schedule.vertical_codes:
            continue
        scheduled_for, estimated = next_release_at(schedule, now=generated_at)
        notes: list[str] = []
        if estimated:
            notes.append("Next release time is estimated from cadence or latest observed release.")
        if schedule.is_calendar_driven:
            notes.append("Calendar-driven release; confirm against CalendarWatch before publication.")
        items.append(
            {
                "release_slug": schedule.release_slug,
                "release_name": _clean_release_name(schedule.release_slug, schedule.release_name),
                "source_slug": schedule.source_slug,
                "source_name": schedule.source_name,
                "cadence": schedule.cadence,
                "schedule_timezone": schedule.schedule_timezone,
                "vertical_ids": [public_vertical_id(code) for code in schedule.vertical_codes],
                "vertical_codes": list(schedule.vertical_codes),
                "series_codes": list(schedule.series_codes),
                "scheduled_for": scheduled_for,
                "latest_release_at": schedule.latest_release_at,
                "source_url": schedule.source_url,
                "is_estimated": estimated,
                "notes": notes,
            }
        )

    items.sort(key=lambda item: item["scheduled_for"] or datetime.max.replace(tzinfo=UTC))
    return {
        "generated_at": generated_at,
        "items": items,
    }
