from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType
from typing import Any


APP_ROOT = Path(__file__).resolve().parent
HEADLINE_TAXONOMY_CONTRACT_PATH = APP_ROOT / "shared" / "headline-taxonomy.json"


def _read_contract(path: Path = HEADLINE_TAXONOMY_CONTRACT_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    canonical_categories = payload.get("canonical_categories")
    if not isinstance(canonical_categories, list) or not canonical_categories:
        raise ValueError("headline taxonomy contract must define canonical_categories")
    if len(canonical_categories) != len(set(canonical_categories)):
        raise ValueError("headline taxonomy contract canonical_categories must be unique")

    canonical_category_set = set(canonical_categories)

    short_labels = payload.get("short_labels")
    if not isinstance(short_labels, dict):
        raise ValueError("headline taxonomy contract must define short_labels")
    if not set(short_labels).issubset(canonical_category_set):
        raise ValueError("headline taxonomy short_labels contains unknown categories")

    color_classes = payload.get("color_classes")
    if not isinstance(color_classes, dict):
        raise ValueError("headline taxonomy contract must define color_classes")
    if set(color_classes) != canonical_category_set:
        raise ValueError("headline taxonomy color_classes must cover every canonical category exactly once")

    sector_map = payload.get("sector_map")
    if not isinstance(sector_map, dict) or "energy" not in sector_map:
        raise ValueError("headline taxonomy contract must define sector_map including energy")
    for categories in sector_map.values():
        if not isinstance(categories, list):
            raise ValueError("headline taxonomy sector_map values must be arrays")
        if not set(categories).issubset(canonical_category_set):
            raise ValueError("headline taxonomy sector_map contains unknown categories")

    dashboard_category_tags = payload.get("dashboard_category_tags")
    if not isinstance(dashboard_category_tags, dict):
        raise ValueError("headline taxonomy contract must define dashboard_category_tags")
    if set(dashboard_category_tags) != canonical_category_set:
        raise ValueError("headline taxonomy dashboard_category_tags must cover every canonical category exactly once")

    return payload


HEADLINE_TAXONOMY_CONTRACT = _read_contract()
CANONICAL_CATEGORIES = tuple(HEADLINE_TAXONOMY_CONTRACT["canonical_categories"])
CATEGORY_PRIORITY = {category: index for index, category in enumerate(CANONICAL_CATEGORIES)}
CATEGORY_SHORT_LABELS = MappingProxyType(dict(HEADLINE_TAXONOMY_CONTRACT["short_labels"]))
CATEGORY_COLOR_CLASSES = MappingProxyType(dict(HEADLINE_TAXONOMY_CONTRACT["color_classes"]))
SECTOR_CATEGORY_MAP = MappingProxyType(
    {
        sector_id: tuple(categories)
        for sector_id, categories in HEADLINE_TAXONOMY_CONTRACT["sector_map"].items()
    }
)
DASHBOARD_CATEGORY_TAGS = MappingProxyType(
    {
        category: MappingProxyType(dict(metadata))
        for category, metadata in HEADLINE_TAXONOMY_CONTRACT["dashboard_category_tags"].items()
    }
)
ALWAYS_RELEVANT_CATEGORIES = tuple(
    category
    for category, metadata in DASHBOARD_CATEGORY_TAGS.items()
    if metadata.get("alwaysRelevant")
)
ENERGY_CATEGORIES = SECTOR_CATEGORY_MAP["energy"]
