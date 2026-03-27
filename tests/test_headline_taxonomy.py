from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import classifier
import headline_taxonomy


def test_shared_headline_taxonomy_contract_stays_aligned() -> None:
    assert classifier.CANONICAL_CATEGORIES == headline_taxonomy.CANONICAL_CATEGORIES
    assert headline_taxonomy.ENERGY_CATEGORIES == headline_taxonomy.SECTOR_CATEGORY_MAP["energy"]
    assert headline_taxonomy.ALWAYS_RELEVANT_CATEGORIES == ("General", "Shipping")
    assert set(headline_taxonomy.CATEGORY_COLOR_CLASSES) == set(headline_taxonomy.CANONICAL_CATEGORIES)
    assert set(headline_taxonomy.DASHBOARD_CATEGORY_TAGS) == set(headline_taxonomy.CANONICAL_CATEGORIES)
