from __future__ import annotations


def classify_inventory_signal(deviation_zscore: float | None) -> str:
    if deviation_zscore is None:
        return "neutral"
    if deviation_zscore <= -1.0:
        return "tightening"
    if deviation_zscore >= 1.0:
        return "loosening"
    return "neutral"

