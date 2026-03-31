from __future__ import annotations


def require_key(payload: dict, key: str) -> None:
    if key not in payload:
        raise ValueError(f"Missing required key: {key}")

