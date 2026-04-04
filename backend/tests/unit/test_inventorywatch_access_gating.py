from __future__ import annotations

from types import SimpleNamespace

from app.api.routers import indicators


def test_is_public_indicator_accessible_requires_public_visibility_and_module_membership() -> None:
    public_indicator = SimpleNamespace(visibility_tier="public")
    internal_indicator = SimpleNamespace(visibility_tier="internal")

    assert indicators.is_public_indicator_accessible(public_indicator, ["inventorywatch"]) is True
    assert indicators.is_public_indicator_accessible(public_indicator, []) is False
    assert indicators.is_public_indicator_accessible(internal_indicator, ["inventorywatch"]) is False
