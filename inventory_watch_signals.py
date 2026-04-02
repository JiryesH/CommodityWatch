from __future__ import annotations

from typing import Any


ENERGY_GAS_COMMODITIES = {
    "crude_oil",
    "gasoline",
    "distillates",
    "propane",
    "jet_fuel",
    "natural_gas",
}


def classify_inventory_signal(deviation_zscore: float | None) -> str:
    if deviation_zscore is None:
        return "neutral"
    if deviation_zscore <= -1.0:
        return "tightening"
    if deviation_zscore >= 1.0:
        return "loosening"
    return "neutral"


def revision_flag(revision_sequence: int | None) -> bool:
    return bool(revision_sequence and revision_sequence > 1)


def compute_inventory_alerts(
    *,
    commodity_code: str | None,
    latest_value: float,
    deviation_zscore: float | None,
    seasonal_context: dict[str, Any],
    trailing_changes: list[float],
    alerts_enabled: bool = True,
) -> list[dict[str, str]]:
    if not alerts_enabled:
        return []

    alerts: list[dict[str, str]] = []

    def add_alert(kind: str, label: str, tone: str) -> None:
        if any(alert["kind"] == kind for alert in alerts):
            return
        alerts.append({"kind": kind, "label": label, "tone": tone})

    p10 = seasonal_context.get("seasonal_p10")
    p90 = seasonal_context.get("seasonal_p90")
    seasonal_low = seasonal_context.get("seasonal_low")
    seasonal_high = seasonal_context.get("seasonal_high")
    seasonal_samples = int(seasonal_context.get("seasonal_samples") or 0)

    if (deviation_zscore is not None and deviation_zscore <= -1.5) or (p10 is not None and latest_value < float(p10)):
        add_alert("tight", "TIGHT", "tight")

    if (deviation_zscore is not None and deviation_zscore >= 1.5) or (p90 is not None and latest_value > float(p90)):
        add_alert("ample", "AMPLE", "ample")

    if seasonal_samples >= 3 and seasonal_low is not None and latest_value <= float(seasonal_low):
        add_alert("five-year-low", "5Y LOW", "tight")

    if seasonal_samples >= 3 and seasonal_high is not None and latest_value >= float(seasonal_high):
        add_alert("five-year-high", "5Y HIGH", "ample")

    if commodity_code in ENERGY_GAS_COMMODITIES and len(trailing_changes) >= 3:
        trailing_window = trailing_changes[-3:]
        if all(change < 0 for change in trailing_window):
            add_alert("drawing", "DRAWING", "watch")
        if all(change > 0 for change in trailing_window):
            add_alert("building", "BUILDING", "cool")

    return alerts
