"""
Contango — Commodity Article Classifier
========================================
Classifies commodity news articles into frontend filter categories using a
deterministic keyword index derived from commodity_taxonomy.json plus curated
supplement rules for missing or ambiguous market language.

Usage as a module (called by rss_scraper.py at scrape time):
    from classifier import classify_category
    category = classify_category(title, description)   # returns str or None

Debugging:
    from classifier import explain_classification
    debug = explain_classification(title, description)

Usage as a standalone script (re-classify all ICIS articles in feed.json):
    python classifier.py                  # re-classify ICIS articles in data/feed.json
    python classifier.py --all            # re-classify every article regardless of source
    python classifier.py --input path/to/feed.json --output path/to/feed.json
    python classifier.py --dry-run        # print changes without writing
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("classifier")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TAXONOMY_PATH = Path(__file__).parent / "commodity_taxonomy.json"
DEFAULT_FEED_PATH = Path(__file__).parent / "data" / "feed.json"

# Canonical category contract (V1 scope)
CANONICAL_CATEGORIES: tuple[str, ...] = (
    "Oil - Crude",
    "Oil - Refined Products",
    "Natural Gas",
    "LNG",
    "Coal",
    "Electric Power",
    "Energy Transition",
    "Chemicals",
    "Metals",
    "Agriculture",
    "Fertilizers",
    "Shipping",
    "General",
)

MAX_CATEGORIES_PER_ARTICLE = 2

LEGACY_CATEGORY_MAP: dict[str, str] = {
    "oil": "Oil - Crude",
    "blog": "General",
    "crude": "Oil - Crude",
    "refined products": "Oil - Refined Products",
    "power": "Electric Power",
    "fertilizer": "Fertilizers",
    "fertiliser": "Fertilizers",
}

_CANONICAL_BY_LOWER = {cat.lower(): cat for cat in CANONICAL_CATEGORIES}
_CATEGORY_PRIORITY = {cat: idx for idx, cat in enumerate(CANONICAL_CATEGORIES)}
_CATEGORY_SPLIT_RE = re.compile(r"\s*(?:,|;|\|)\s*")

# ---------------------------------------------------------------------------
# Taxonomy → frontend filter mapping
# ---------------------------------------------------------------------------

_TAXONOMY_FILTER_MAP: dict[tuple[str, Optional[str]], str] = {
    ("Agriculture", "Fertilizers"): "Fertilizers",
    ("Agriculture", "Grains and Oilseeds"): "Agriculture",
    ("Agriculture", "Proteins and Feed"): "Agriculture",
    ("Agriculture", "Sugar"): "Agriculture",
    ("Chemicals", None): "Chemicals",
    ("Energy", "Biofuels"): "Energy Transition",
    ("Energy", "Coal and Coke"): "Coal",
    ("Energy", "Crude Oil"): "Oil - Crude",
    ("Energy", "Hydrogen and Ammonia"): "Energy Transition",
    ("Energy", "Natural Gas and LNG"): "Natural Gas",
    ("Energy", "Power"): "Electric Power",
    ("Energy", "Refined Products"): "Oil - Refined Products",
    ("Environmental Markets", None): "Energy Transition",
    ("Metals", None): "Metals",
}

# Commodity names/aliases (lowercase) within the "Natural Gas and LNG"
# subcategory whose primary classification should be "LNG" not "Natural Gas".
_LNG_NAMES: frozenset[str] = frozenset(
    {
        "liquefied natural gas",
        "lng",
        "bio-lng",
        "japan korea marker",
        "jkm",
    }
)

# Explicit remaps where taxonomy structure is broader than the frontend
# contract. Unqualified ammonia is treated as fertilizer-market coverage unless
# low-carbon / hydrogen qualifiers are present. Biomethane / RNG are handled as
# transition fuels rather than conventional gas.
_TAXONOMY_TERM_CATEGORY_OVERRIDES: dict[str, str] = {
    "ammonia": "Fertilizers",
    "japan korea ammonia price": "Fertilizers",
    "us gulf ammonia price": "Fertilizers",
    "biomethane": "Energy Transition",
    "renewable natural gas": "Energy Transition",
    "rng": "Energy Transition",
}

# Terms that are too ambiguous in the taxonomy-derived index to keep as
# standalone commodity signals.
_BLOCKED_TAXONOMY_TERMS: frozenset[str] = frozenset(
    {
        "dubai",
        "scrap",
        "jet",
        "dawn",
    }
)

# Some acronyms are safe enough to match case-insensitively; others remain
# strict because lowercase forms collide with common language.
_CASE_FLEXIBLE_ACRONYMS: frozenset[str] = frozenset(
    {
        "lng",
        "mdi",
        "cbam",
        "vlcc",
        "ccgt",
        "ccgts",
        "rfcc",
        "saf",
    }
)

# ---------------------------------------------------------------------------
# Supplement keywords
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KeywordSpec:
    keyword: str
    category: str
    weight: float = 1.0
    source: str = "supplement"
    case_sensitive: bool = False
    fields: tuple[str, ...] = ("title", "description")
    notes: str = ""


_SUPPLEMENT_KEYWORDS: tuple[KeywordSpec, ...] = (
    # Oil - Crude
    KeywordSpec("crude oil", "Oil - Crude", weight=1.35),
    KeywordSpec("crude", "Oil - Crude", weight=0.9),
    KeywordSpec("oil prices", "Oil - Crude", weight=0.75, notes="background"),
    KeywordSpec("opec", "Oil - Crude", weight=1.8),
    KeywordSpec("opec+", "Oil - Crude", weight=2.0),
    KeywordSpec("condensate", "Oil - Crude", weight=1.7),
    KeywordSpec("trans mountain", "Oil - Crude", weight=2.0),
    KeywordSpec("dubai crude", "Oil - Crude", weight=1.7),
    KeywordSpec("dubai benchmark", "Oil - Crude", weight=1.5),
    # Oil - Refined Products
    KeywordSpec("refined products", "Oil - Refined Products", weight=1.35),
    KeywordSpec("refinery", "Oil - Refined Products", weight=1.1),
    KeywordSpec("refining", "Oil - Refined Products", weight=1.0),
    KeywordSpec("refining margins", "Oil - Refined Products", weight=1.7),
    KeywordSpec("refinery throughput", "Oil - Refined Products", weight=2.0),
    KeywordSpec("crude throughput", "Oil - Refined Products", weight=2.0),
    KeywordSpec("refinery runs", "Oil - Refined Products", weight=1.9),
    KeywordSpec("refinery run", "Oil - Refined Products", weight=1.8),
    KeywordSpec("run rates", "Oil - Refined Products", weight=1.25, notes="context"),
    KeywordSpec("high sulphur fuel oil", "Oil - Refined Products", weight=1.9),
    KeywordSpec("very low sulphur fuel oil", "Oil - Refined Products", weight=1.9),
    KeywordSpec("low sulphur fuel oil", "Oil - Refined Products", weight=1.8),
    # Natural Gas
    KeywordSpec("natural gas", "Natural Gas", weight=1.25),
    KeywordSpec(
        "gas",
        "Natural Gas",
        weight=0.85,
        fields=("title",),
        notes="title-only",
    ),
    KeywordSpec("pipeline gas", "Natural Gas", weight=1.5),
    KeywordSpec("gas prices", "Natural Gas", weight=0.9, notes="background"),
    KeywordSpec("gas demand", "Natural Gas", weight=1.6),
    KeywordSpec("residential gas", "Natural Gas", weight=1.8),
    KeywordSpec("gas grid", "Natural Gas", weight=1.8),
    KeywordSpec("gas-grid", "Natural Gas", weight=1.8),
    KeywordSpec("gas exchange", "Natural Gas", weight=1.8),
    KeywordSpec("injection season", "Natural Gas", weight=2.0),
    KeywordSpec("gas injection", "Natural Gas", weight=1.7),
    KeywordSpec("gas supply", "Natural Gas", weight=1.5),
    KeywordSpec("oil and gas supply", "Natural Gas", weight=1.8),
    # LNG
    KeywordSpec("lng", "LNG", weight=1.3),
    KeywordSpec("liquefied natural gas", "LNG", weight=1.5),
    KeywordSpec("lng shortage", "LNG", weight=1.9),
    KeywordSpec("lng shortages", "LNG", weight=1.9),
    KeywordSpec("lng imports", "LNG", weight=1.8),
    KeywordSpec("lng tanker", "LNG", weight=1.9),
    KeywordSpec("lng tankers", "LNG", weight=1.9),
    KeywordSpec("lng supply", "LNG", weight=1.7),
    # Coal
    KeywordSpec("coal", "Coal", weight=1.0),
    KeywordSpec("coking coal", "Coal", weight=1.5),
    KeywordSpec("thermal coal", "Coal", weight=1.5),
    # Electric Power
    KeywordSpec("electricity", "Electric Power", weight=0.95),
    KeywordSpec("power prices", "Electric Power", weight=1.1),
    KeywordSpec("power grid", "Electric Power", weight=1.4),
    KeywordSpec("power system", "Electric Power", weight=1.9),
    KeywordSpec("power-system", "Electric Power", weight=1.9),
    KeywordSpec("power import", "Electric Power", weight=1.8),
    KeywordSpec("power imports", "Electric Power", weight=1.8),
    KeywordSpec("power mix", "Electric Power", weight=1.6),
    KeywordSpec("power grid operator", "Electric Power", weight=1.8),
    KeywordSpec("ccgt", "Electric Power", weight=1.8),
    KeywordSpec("ccgts", "Electric Power", weight=1.8),
    KeywordSpec(
        "combined cycle gas turbine",
        "Electric Power",
        weight=2.0,
    ),
    KeywordSpec(
        "combined-cycle gas turbine",
        "Electric Power",
        weight=2.0,
    ),
    # Energy Transition
    KeywordSpec("renewables", "Energy Transition", weight=1.4),
    KeywordSpec("renewable energy", "Energy Transition", weight=1.6),
    KeywordSpec("energy transition", "Energy Transition", weight=1.6),
    KeywordSpec("solar", "Energy Transition", weight=1.0),
    KeywordSpec("wind power", "Energy Transition", weight=1.45),
    KeywordSpec("wind energy", "Energy Transition", weight=1.45),
    KeywordSpec("wind farm", "Energy Transition", weight=1.45),
    KeywordSpec("offshore wind", "Energy Transition", weight=1.6),
    KeywordSpec("onshore wind", "Energy Transition", weight=1.6),
    KeywordSpec("solar power", "Energy Transition", weight=1.5),
    KeywordSpec("solar capacity", "Energy Transition", weight=1.5),
    KeywordSpec("clean energy", "Energy Transition", weight=1.5),
    KeywordSpec("green energy", "Energy Transition", weight=1.5),
    KeywordSpec("carbon credits", "Energy Transition", weight=1.6),
    KeywordSpec("carbon allowances", "Energy Transition", weight=1.7),
    KeywordSpec("emissions allowances", "Energy Transition", weight=1.7),
    KeywordSpec("emissions trading", "Energy Transition", weight=1.8),
    KeywordSpec("carbon market", "Energy Transition", weight=1.8),
    KeywordSpec("carbon price", "Energy Transition", weight=1.6),
    KeywordSpec("carbon tax", "Energy Transition", weight=2.0),
    KeywordSpec("carbon taxes", "Energy Transition", weight=2.0),
    KeywordSpec("carbon leakage", "Energy Transition", weight=1.8),
    KeywordSpec("cbam", "Energy Transition", weight=2.0),
    KeywordSpec(
        "carbon border adjustment mechanism",
        "Energy Transition",
        weight=2.0,
    ),
    KeywordSpec("decarbonisation", "Energy Transition", weight=1.6),
    KeywordSpec("decarbonization", "Energy Transition", weight=1.6),
    KeywordSpec("battery project", "Energy Transition", weight=1.9),
    KeywordSpec("battery projects", "Energy Transition", weight=1.9),
    KeywordSpec("battery storage", "Energy Transition", weight=1.9),
    KeywordSpec("corporate renewables", "Energy Transition", weight=1.9),
    KeywordSpec("renewables contracts", "Energy Transition", weight=2.0),
    KeywordSpec("saf", "Energy Transition", weight=2.3),
    KeywordSpec("biomethane", "Energy Transition", weight=1.8),
    KeywordSpec("biometano", "Energy Transition", weight=1.8),
    KeywordSpec("biocarbon", "Energy Transition", weight=1.9),
    KeywordSpec("blue hydrogen", "Energy Transition", weight=2.2),
    KeywordSpec("liquid hydrogen", "Energy Transition", weight=2.0),
    KeywordSpec("hydrogen hubs", "Energy Transition", weight=2.0),
    KeywordSpec("chemical recycling", "Energy Transition", weight=2.1),
    KeywordSpec("recycling plant", "Energy Transition", weight=1.8),
    KeywordSpec("heating mandate", "Energy Transition", weight=1.1),
    KeywordSpec("heating law", "Energy Transition", weight=1.1),
    # Chemicals
    KeywordSpec("petrochemicals", "Chemicals", weight=1.5),
    KeywordSpec("petrochemical supplies", "Chemicals", weight=1.7),
    KeywordSpec("olefins", "Chemicals", weight=1.9),
    KeywordSpec("polyolefins", "Chemicals", weight=2.0),
    KeywordSpec("polymers", "Chemicals", weight=1.8),
    KeywordSpec("polymer", "Chemicals", weight=1.35),
    KeywordSpec("plastics", "Chemicals", weight=1.4),
    KeywordSpec("chemical spot prices", "Chemicals", weight=2.0),
    KeywordSpec("chemical prices", "Chemicals", weight=1.8),
    KeywordSpec("chemical market", "Chemicals", weight=1.6),
    KeywordSpec("chemicals market", "Chemicals", weight=1.7),
    KeywordSpec("mdi", "Chemicals", weight=2.0),
    KeywordSpec(
        "methylene diphenyl diisocyanate",
        "Chemicals",
        weight=2.1,
    ),
    KeywordSpec("butadiene", "Chemicals", weight=1.9),
    KeywordSpec("bd prices", "Chemicals", weight=2.0),
    KeywordSpec("bd spot prices", "Chemicals", weight=2.1),
    KeywordSpec("bd market", "Chemicals", weight=1.8),
    KeywordSpec("pp prices", "Chemicals", weight=1.9),
    KeywordSpec("pp output", "Chemicals", weight=1.9),
    KeywordSpec("pp homopolymer", "Chemicals", weight=2.0),
    KeywordSpec("pp copolymer", "Chemicals", weight=2.0),
    KeywordSpec("pe market", "Chemicals", weight=1.9),
    KeywordSpec("pe prices", "Chemicals", weight=1.9),
    KeywordSpec("abs market", "Chemicals", weight=1.8),
    KeywordSpec("abs imports", "Chemicals", weight=1.8),
    KeywordSpec("glycol ethers", "Chemicals", weight=2.0),
    KeywordSpec("methacrylic acid", "Chemicals", weight=2.1),
    KeywordSpec("methacrylates", "Chemicals", weight=1.8),
    # Metals
    KeywordSpec("al price", "Metals", weight=1.9, fields=("title",)),
    KeywordSpec("al prices", "Metals", weight=1.9, fields=("title",)),
    KeywordSpec("al price forecasts", "Metals", weight=2.0, fields=("title",)),
    KeywordSpec("feti", "Metals", weight=1.8),
    KeywordSpec("iron ore", "Metals", weight=1.5),
    KeywordSpec("scrap metal", "Metals", weight=1.8),
    KeywordSpec("scrap export", "Metals", weight=1.7),
    KeywordSpec("scrap export tender", "Metals", weight=2.0),
    KeywordSpec("steel scrap", "Metals", weight=1.8),
    KeywordSpec("scrap imports", "Metals", weight=1.6),
    KeywordSpec("scrap import", "Metals", weight=1.6),
    KeywordSpec("scrap prices", "Metals", weight=1.6),
    KeywordSpec("scrap market", "Metals", weight=1.6),
    KeywordSpec("steel", "Metals", weight=1.05),
    KeywordSpec("aluminium", "Metals", weight=1.15),
    KeywordSpec("aluminum", "Metals", weight=1.15),
    KeywordSpec("antimony", "Metals", weight=1.8),
    KeywordSpec("copper", "Metals", weight=1.15),
    KeywordSpec("tungsten", "Metals", weight=1.8),
    KeywordSpec("critical minerals", "Metals", weight=1.8),
    KeywordSpec("stainless steel", "Metals", weight=1.4),
    # Agriculture
    KeywordSpec("soybean", "Agriculture", weight=1.4),
    KeywordSpec("soybean sales", "Agriculture", weight=1.8),
    KeywordSpec("barley", "Agriculture", weight=1.5),
    KeywordSpec("barley tender", "Agriculture", weight=1.9),
    KeywordSpec("protein co-products", "Agriculture", weight=1.2),
    KeywordSpec("animal feed", "Agriculture", weight=1.0),
    KeywordSpec("grain-based ethanol", "Agriculture", weight=1.1),
    KeywordSpec("grains", "Agriculture", weight=1.2),
    KeywordSpec("oilseeds", "Agriculture", weight=1.2),
    KeywordSpec("vegetable oil", "Agriculture", weight=1.3),
    KeywordSpec("palm oil", "Agriculture", weight=1.3),
    # Fertilizers
    KeywordSpec("fertilizer", "Fertilizers", weight=1.2),
    KeywordSpec("fertiliser", "Fertilizers", weight=1.2),
    KeywordSpec("ammonia futures", "Fertilizers", weight=2.0),
    KeywordSpec("ammonia production", "Fertilizers", weight=1.8),
    KeywordSpec("ammonia production costs", "Fertilizers", weight=2.0),
    KeywordSpec("ammonia imports", "Fertilizers", weight=1.7),
    KeywordSpec("ammonia market", "Fertilizers", weight=1.7),
    KeywordSpec("sulphur", "Fertilizers", weight=1.2),
    KeywordSpec("sulphuric acid", "Fertilizers", weight=1.8),
    # Shipping
    KeywordSpec("shipping rates", "Shipping", weight=1.8),
    KeywordSpec("freight rates", "Shipping", weight=1.8),
    KeywordSpec("tanker rates", "Shipping", weight=1.7),
    KeywordSpec("tanker", "Shipping", weight=1.15),
    KeywordSpec("tankers", "Shipping", weight=1.3),
    KeywordSpec("sanctioned tankers", "Shipping", weight=1.9),
    KeywordSpec("tanker operators", "Shipping", weight=1.8),
    KeywordSpec("sanctioned ships", "Shipping", weight=1.8),
    KeywordSpec("oil tankers", "Shipping", weight=1.8),
    KeywordSpec("vlcc", "Shipping", weight=1.3),
    KeywordSpec("bunker fuel", "Shipping", weight=1.5),
    KeywordSpec("charter rate", "Shipping", weight=1.6),
    KeywordSpec("trade route", "Shipping", weight=1.1),
    KeywordSpec("trade routes", "Shipping", weight=1.1),
    KeywordSpec("shipping disruption", "Shipping", weight=1.9),
    KeywordSpec("shipping disruptions", "Shipping", weight=1.9),
    KeywordSpec("jebel ali", "Shipping", weight=0.4),
    KeywordSpec("khor fakkan", "Shipping", weight=0.4),
    KeywordSpec("fujairah", "Shipping", weight=0.4),
    KeywordSpec("maritime", "Shipping", weight=1.5),
    KeywordSpec("vessel", "Shipping", weight=1.3),
    KeywordSpec("vessels", "Shipping", weight=1.3),
    # Crude / refined phrase refinements
    KeywordSpec("oil and gas supply", "Oil - Crude", weight=1.8),
    KeywordSpec("oil and gas supply outlook", "Oil - Crude", weight=2.4),
    KeywordSpec("oil security", "Oil - Crude", weight=1.8),
    KeywordSpec("russian oil", "Oil - Crude", weight=1.5),
    KeywordSpec("upstream output", "Oil - Crude", weight=1.9),
    KeywordSpec("upstream production", "Oil - Crude", weight=1.9),
    KeywordSpec("refiners", "Oil - Refined Products", weight=1.8),
    KeywordSpec("refiner", "Oil - Refined Products", weight=1.7),
    KeywordSpec("lpg production", "Oil - Refined Products", weight=1.9),
    KeywordSpec("rfcc", "Oil - Refined Products", weight=1.8),
    KeywordSpec("fluid catalytic cracker", "Oil - Refined Products", weight=1.9),
)

_GENERAL_TITLE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bmarkets?\s+summary\b", re.IGNORECASE), "market summary"),
    (re.compile(r"\bsnapshot\b", re.IGNORECASE), "snapshot"),
    (re.compile(r"\broundup\b", re.IGNORECASE), "roundup"),
    (re.compile(r"\blatest news\b", re.IGNORECASE), "latest news"),
    (re.compile(r"^\s*interactive:", re.IGNORECASE), "interactive"),
)

_FIELD_WEIGHTS: dict[str, float] = {
    "title": 3.0,
    "description": 1.6,
}

_PRIMARY_SCORE_MIN = 2.2
_SECONDARY_SCORE_MIN = 2.8
_SECONDARY_SCORE_RATIO = 0.55
_SECONDARY_TITLE_ONLY_SUPPRESSION_RATIO = 1.45
_SECONDARY_TITLE_ONLY_SUPPRESSION_CATEGORIES: frozenset[str] = frozenset(
    {
        "LNG",
        "Fertilizers",
    }
)

_NORMALIZE_KEEP_SCORE = 1.5
_NORMALIZE_ADD_SCORE = 3.0
_NORMALIZE_OVERRIDE_PRIMARY_SCORE = 4.5
_NORMALIZE_OVERRIDE_SECONDARY_SCORE = 3.2
_NORMALIZE_CONTRADICTION_SCORE_MAX = 1.1

# ---------------------------------------------------------------------------
# Keyword index — built once, cached for the process lifetime
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeywordRule:
    keyword: str
    category: str
    pattern: re.Pattern[str]
    weight: float
    specificity: float
    source: str
    case_sensitive: bool
    fields: tuple[str, ...]
    notes: str = ""


@dataclass
class MatchEvidence:
    keyword: str
    matched_text: str
    category: str
    field: str
    score: float
    weight: float
    specificity: float
    source: str
    start: int
    end: int
    notes: str = ""


@dataclass
class CategoryEvidence:
    category: str
    score: float = 0.0
    title_score: float = 0.0
    description_score: float = 0.0
    title_matches: int = 0
    description_matches: int = 0
    top_match_score: float = 0.0
    matches: list[MatchEvidence] = field(default_factory=list)


_KeywordIndex = list[KeywordRule]

_KEYWORD_INDEX: Optional[_KeywordIndex] = None


def iter_raw_category_tokens(raw_categories: Any) -> list[str]:
    """Flatten category input into stripped string tokens."""
    if raw_categories is None:
        return []
    if isinstance(raw_categories, str):
        text = raw_categories.strip()
        if not text:
            return []
        return [p.strip() for p in _CATEGORY_SPLIT_RE.split(text) if p.strip()]
    if isinstance(raw_categories, (list, tuple, set)):
        out: list[str] = []
        for item in raw_categories:
            out.extend(iter_raw_category_tokens(item))
        return out
    return iter_raw_category_tokens(str(raw_categories))


# Backward-compatible alias for internal callers.
_iter_raw_category_tokens = iter_raw_category_tokens


def normalize_category_token(raw_category: str) -> Optional[str]:
    """Map a raw category token to a canonical category label."""
    token = " ".join(str(raw_category or "").split())
    if not token:
        return None

    canonical = _CANONICAL_BY_LOWER.get(token.lower())
    if canonical:
        return canonical

    return LEGACY_CATEGORY_MAP.get(token.lower())


def normalize_categories(
    raw_categories: Any,
    *,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> tuple[list[str], list[str]]:
    """
    Normalize raw category input into canonical category labels.

    Returns:
        (normalized_categories, unknown_tokens)
    """
    normalized: list[str] = []
    unknown: list[str] = []

    for token in _iter_raw_category_tokens(raw_categories):
        canonical = normalize_category_token(token)
        if canonical is None:
            if token not in unknown:
                unknown.append(token)
            continue
        if canonical not in normalized:
            normalized.append(canonical)

    normalized.sort(key=lambda cat: _CATEGORY_PRIORITY[cat])
    if max_categories and max_categories > 0:
        normalized = normalized[:max_categories]

    return normalized, unknown


def merge_category_lists(
    *raw_category_sets: Any,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> list[str]:
    """Merge category sources into one canonical, deduplicated list."""
    merged: list[str] = []
    for raw in raw_category_sets:
        normalized, _ = normalize_categories(raw, max_categories=None)
        for cat in normalized:
            if cat not in merged:
                merged.append(cat)

    merged.sort(key=lambda cat: _CATEGORY_PRIORITY[cat])
    if max_categories and max_categories > 0:
        merged = merged[:max_categories]
    return merged


def _contains_alpha(text: str) -> bool:
    return any(ch.isalpha() for ch in text)


def _is_short_acronym(text: str) -> bool:
    letters = [ch for ch in text if ch.isalpha()]
    return bool(letters) and len(letters) <= 6 and all(ch.isupper() for ch in letters)


def _should_use_case_sensitive_keyword(keyword: str) -> bool:
    return _is_short_acronym(keyword) and keyword.lower() not in _CASE_FLEXIBLE_ACRONYMS


def _compile_pattern(keyword: str, *, case_sensitive: bool) -> re.Pattern[str]:
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(
        r"(?<![A-Za-z0-9])" + re.escape(keyword) + r"(?![A-Za-z0-9])",
        flags,
    )


def _keyword_specificity(keyword: str) -> float:
    token_count = len(re.findall(r"[A-Za-z0-9]+", keyword))
    specificity = 1.0
    if token_count >= 2:
        specificity += 0.4
    if len(keyword) >= 12:
        specificity += 0.2
    if "-" in keyword or "/" in keyword:
        specificity += 0.1
    if _is_short_acronym(keyword):
        specificity += 0.15
    return min(specificity, 1.9)


def _keyword_fields(notes: str, fields: tuple[str, ...]) -> tuple[str, ...]:
    if "title-only" in notes:
        return ("title",)
    return fields


def _build_rule(
    keyword: str,
    category: str,
    *,
    weight: float,
    source: str,
    case_sensitive: bool,
    fields: tuple[str, ...] = ("title", "description"),
    notes: str = "",
) -> KeywordRule:
    return KeywordRule(
        keyword=keyword,
        category=category,
        pattern=_compile_pattern(keyword, case_sensitive=case_sensitive),
        weight=weight,
        specificity=_keyword_specificity(keyword),
        source=source,
        case_sensitive=case_sensitive,
        fields=_keyword_fields(notes, fields),
        notes=notes,
    )


def build_keyword_index() -> _KeywordIndex:
    """
    Build and return the keyword classification index.

    The index keeps explicit rule metadata so matches can be scored and traced
    later instead of only checking category presence.
    """
    entries: _KeywordIndex = []
    seen: dict[tuple[str, str], int] = {}

    def _add(
        keyword: str,
        category: str,
        *,
        weight: float,
        source: str,
        case_sensitive: bool = False,
        fields: tuple[str, ...] = ("title", "description"),
        notes: str = "",
    ) -> None:
        cleaned = keyword.strip()
        if not cleaned or len(cleaned) < 2:
            return
        key = (cleaned.lower(), category)
        rule = _build_rule(
            cleaned,
            category,
            weight=weight,
            source=source,
            case_sensitive=case_sensitive,
            fields=fields,
            notes=notes,
        )
        existing_index = seen.get(key)
        if existing_index is not None:
            existing_rule = entries[existing_index]
            existing_strength = existing_rule.weight * existing_rule.specificity
            new_strength = rule.weight * rule.specificity
            if new_strength <= existing_strength:
                return
            entries[existing_index] = rule
            return
        seen[key] = len(entries)
        entries.append(rule)

    if TAXONOMY_PATH.exists():
        try:
            with open(TAXONOMY_PATH, encoding="utf-8") as handle:
                taxonomy = json.load(handle)

            for category_block in taxonomy.get("categories", []):
                taxonomy_category = category_block["name"]
                for subcategory in category_block.get("subcategories", []):
                    taxonomy_subcategory = subcategory["name"]

                    filter_category = _TAXONOMY_FILTER_MAP.get(
                        (taxonomy_category, taxonomy_subcategory)
                    )
                    if filter_category is None:
                        filter_category = _TAXONOMY_FILTER_MAP.get(
                            (taxonomy_category, None)
                        )
                    if filter_category is None:
                        continue

                    _add(
                        taxonomy_subcategory,
                        filter_category,
                        weight=0.55,
                        source="taxonomy:subcategory",
                    )

                    for commodity in subcategory.get("commodities", []):
                        default_category = filter_category
                        if (
                            taxonomy_category == "Energy"
                            and taxonomy_subcategory == "Natural Gas and LNG"
                        ):
                            commodity_name = commodity["name"].lower()
                            alias_names = [alias.lower() for alias in commodity.get("aliases", [])]
                            if commodity_name in _LNG_NAMES or any(
                                alias in _LNG_NAMES for alias in alias_names
                            ):
                                default_category = "LNG"

                        terms = (
                            [commodity["name"]]
                            + commodity.get("aliases", [])
                            + commodity.get("variants", [])
                        )
                        for term in terms:
                            normalized_term = term.strip()
                            lowered = normalized_term.lower()
                            if lowered in _BLOCKED_TAXONOMY_TERMS:
                                continue

                            category = _TAXONOMY_TERM_CATEGORY_OVERRIDES.get(
                                lowered,
                                default_category,
                            )
                            weight = 1.0 if " " in normalized_term or len(normalized_term) > 6 else 0.9
                            _add(
                                normalized_term,
                                category,
                                weight=weight,
                                source="taxonomy",
                                case_sensitive=_should_use_case_sensitive_keyword(normalized_term),
                            )

        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Could not load taxonomy for classification: %s", exc)
    else:  # pragma: no cover - defensive logging
        logger.warning(
            "Taxonomy not found at %s; articles will remain unclassified.",
            TAXONOMY_PATH,
        )

    for spec in _SUPPLEMENT_KEYWORDS:
        _add(
            spec.keyword,
            spec.category,
            weight=spec.weight,
            source=spec.source,
            case_sensitive=spec.case_sensitive,
            fields=spec.fields,
            notes=spec.notes,
        )

    entries.sort(
        key=lambda rule: (
            -rule.weight * rule.specificity,
            -len(rule.keyword),
            _CATEGORY_PRIORITY[rule.category],
            rule.keyword,
        )
    )

    logger.info("Keyword index built: %s rules", len(entries))
    return entries


def _get_keyword_index() -> _KeywordIndex:
    """Return the cached keyword index, building it on first call."""
    global _KEYWORD_INDEX
    if _KEYWORD_INDEX is None:
        _KEYWORD_INDEX = build_keyword_index()
    return _KEYWORD_INDEX


def _score_match(rule: KeywordRule, field_name: str) -> float:
    return round(rule.weight * rule.specificity * _FIELD_WEIGHTS[field_name], 4)


def _collect_match_evidence(title: str, description: str) -> list[MatchEvidence]:
    matches: list[MatchEvidence] = []
    text_by_field = {
        "title": str(title or ""),
        "description": str(description or ""),
    }

    for rule in _get_keyword_index():
        for field_name, field_text in text_by_field.items():
            if not field_text or field_name not in rule.fields:
                continue
            match = rule.pattern.search(field_text)
            if not match:
                continue
            matches.append(
                MatchEvidence(
                    keyword=rule.keyword,
                    matched_text=match.group(0),
                    category=rule.category,
                    field=field_name,
                    score=_score_match(rule, field_name),
                    weight=rule.weight,
                    specificity=rule.specificity,
                    source=rule.source,
                    start=match.start(),
                    end=match.end(),
                    notes=rule.notes,
                )
            )
    matches.sort(
        key=lambda match: (
            match.field != "title",
            -match.score,
            _CATEGORY_PRIORITY[match.category],
            match.keyword,
        )
    )
    return matches


def _accumulate_category_evidence(
    matches: list[MatchEvidence],
) -> dict[str, CategoryEvidence]:
    scores: dict[str, CategoryEvidence] = {}
    for match in matches:
        evidence = scores.setdefault(match.category, CategoryEvidence(category=match.category))
        evidence.score += match.score
        if match.field == "title":
            evidence.title_score += match.score
            evidence.title_matches += 1
        else:
            evidence.description_score += match.score
            evidence.description_matches += 1
        evidence.top_match_score = max(evidence.top_match_score, match.score)
        evidence.matches.append(match)
    return scores


def _rank_category_evidence(
    evidence_by_category: dict[str, CategoryEvidence],
) -> list[CategoryEvidence]:
    return sorted(
        evidence_by_category.values(),
        key=lambda evidence: (
            -evidence.score,
            -evidence.title_score,
            -evidence.top_match_score,
            _CATEGORY_PRIORITY[evidence.category],
        ),
    )


def _is_broad_general_story(
    title: str,
    ranked_evidence: list[CategoryEvidence],
) -> tuple[bool, Optional[str]]:
    if not title:
        return False, None

    strong_categories = [evidence for evidence in ranked_evidence if evidence.score >= 2.0]
    title_score_total = sum(evidence.title_score for evidence in ranked_evidence)
    lower_title = title.lower()

    for pattern, reason in _GENERAL_TITLE_PATTERNS:
        if not pattern.search(title):
            continue
        if reason in {"market summary", "snapshot", "roundup", "latest news"}:
            if title_score_total < 2.0:
                return True, reason
        if reason == "interactive" and len(strong_categories) >= 3:
            return True, reason

    if "market summary" in lower_title and title_score_total < 2.0:
        return True, "market summary"

    return False, None


def _should_keep_secondary(
    primary: CategoryEvidence,
    candidate: CategoryEvidence,
) -> bool:
    if candidate.score < _SECONDARY_SCORE_MIN:
        return False
    if (
        candidate.category in _SECONDARY_TITLE_ONLY_SUPPRESSION_CATEGORIES
        and candidate.description_matches == 0
        and candidate.title_matches == 1
        and primary.score >= candidate.score * _SECONDARY_TITLE_ONLY_SUPPRESSION_RATIO
    ):
        return False
    if candidate.title_score > 0:
        return True
    if candidate.score >= primary.score * _SECONDARY_SCORE_RATIO:
        return True
    if candidate.description_matches >= 2 and candidate.score >= _SECONDARY_SCORE_MIN + 0.4:
        return True
    return False


def _select_categories_from_evidence(
    ranked_evidence: list[CategoryEvidence],
    *,
    max_categories: Optional[int],
) -> list[str]:
    if not ranked_evidence or ranked_evidence[0].score < _PRIMARY_SCORE_MIN:
        return []

    selected = [ranked_evidence[0].category]
    if max_categories == 1:
        return selected

    for candidate in ranked_evidence[1:]:
        if not _should_keep_secondary(ranked_evidence[0], candidate):
            continue
        selected.append(candidate.category)
        if max_categories and max_categories > 0 and len(selected) >= max_categories:
            break

    return selected


def explain_classification(
    title: str,
    description: str = "",
    *,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> dict[str, Any]:
    """
    Return category predictions plus keyword and score provenance.

    This helper is intended for debugging and tests. The stable public
    classifiers still return only category labels.
    """
    matches = _collect_match_evidence(title, description)
    evidence_by_category = _accumulate_category_evidence(matches)
    ranked = _rank_category_evidence(evidence_by_category)
    is_general, general_reason = _is_broad_general_story(title, ranked)
    categories = [] if is_general else _select_categories_from_evidence(ranked, max_categories=max_categories)

    return {
        "categories": categories,
        "ranked_categories": [evidence.category for evidence in ranked],
        "scores": {
            evidence.category: {
                "score": round(evidence.score, 4),
                "title_score": round(evidence.title_score, 4),
                "description_score": round(evidence.description_score, 4),
                "title_matches": evidence.title_matches,
                "description_matches": evidence.description_matches,
                "top_match_score": round(evidence.top_match_score, 4),
                "keywords": [match.keyword for match in evidence.matches],
            }
            for evidence in ranked
        },
        "matches": [
            {
                "keyword": match.keyword,
                "matched_text": match.matched_text,
                "category": match.category,
                "field": match.field,
                "score": round(match.score, 4),
                "weight": match.weight,
                "specificity": round(match.specificity, 4),
                "source": match.source,
                "notes": match.notes,
                "span": [match.start, match.end],
            }
            for match in matches
        ],
        "is_general_story": is_general,
        "general_reason": general_reason,
    }


def classify_categories(
    title: str,
    description: str = "",
    *,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> list[str]:
    """Classify article text into canonical category labels."""
    return explain_classification(
        title,
        description,
        max_categories=max_categories,
    )["categories"]


def _classifier_ranked_categories(debug: dict[str, Any]) -> list[str]:
    ranked = list(debug.get("ranked_categories") or [])
    if ranked:
        return ranked
    return list(debug.get("categories") or [])


def _reconcile_existing_and_classifier(
    existing_categories: list[str],
    debug: dict[str, Any],
    *,
    max_categories: Optional[int],
) -> tuple[list[str], bool]:
    predicted = list(debug.get("categories") or [])
    if not predicted:
        return existing_categories, False

    scores: dict[str, dict[str, Any]] = debug.get("scores") or {}
    ranked_categories = _classifier_ranked_categories(debug)
    informative_existing = [cat for cat in existing_categories if cat != "General"]
    if not informative_existing:
        return predicted, True

    supported_existing = [
        cat
        for cat in informative_existing
        if float((scores.get(cat) or {}).get("score", 0.0)) >= _NORMALIZE_KEEP_SCORE
    ]

    classifier_only = [
        cat
        for cat in ranked_categories
        if cat in predicted
        and float((scores.get(cat) or {}).get("score", 0.0)) >= _NORMALIZE_ADD_SCORE
    ]

    if set(predicted).intersection(informative_existing):
        reconciled = [
            cat
            for cat in ranked_categories
            if cat in set(supported_existing).union(classifier_only)
        ]
        if not reconciled:
            return existing_categories, False
        if max_categories and max_categories > 0:
            reconciled = reconciled[:max_categories]
        return reconciled, reconciled != existing_categories

    primary_score = float((scores.get(predicted[0]) or {}).get("score", 0.0))
    secondary_score = 0.0
    if len(predicted) > 1:
        secondary_score = float((scores.get(predicted[1]) or {}).get("score", 0.0))

    strongest_existing_score = max(
        (float((scores.get(cat) or {}).get("score", 0.0)) for cat in informative_existing),
        default=0.0,
    )
    title_supported = bool((scores.get(predicted[0]) or {}).get("title_matches"))

    strong_override = (
        title_supported
        and primary_score >= _NORMALIZE_OVERRIDE_PRIMARY_SCORE
        and strongest_existing_score <= _NORMALIZE_CONTRADICTION_SCORE_MAX
    )
    if strong_override:
        if len(predicted) > 1 and secondary_score < _NORMALIZE_OVERRIDE_SECONDARY_SCORE:
            predicted = predicted[:1]
        return predicted, True

    return existing_categories, False


def normalize_article_categories(
    article: dict[str, Any],
    *,
    classify_fallback: bool = True,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> dict[str, Any]:
    """
    Enforce canonical category fields on an article.

    - Reads `categories` and legacy `category`
    - Rewrites legacy labels to canonical labels
    - Uses scored classifier fallback when categories are empty or only General
    - May conservatively augment or override stale informative labels when the
      article text strongly contradicts them
    - Writes deterministic `categories` array and `category` primary label
    """
    merged = merge_category_lists(
        article.get("categories"),
        article.get("category"),
        max_categories=None,
    )

    _, unknown_from_categories = normalize_categories(
        article.get("categories"),
        max_categories=None,
    )
    _, unknown_from_category = normalize_categories(
        article.get("category"),
        max_categories=None,
    )
    unknown_tokens = list(dict.fromkeys(unknown_from_categories + unknown_from_category))

    has_informative_category = any(cat != "General" for cat in merged)

    used_classifier = False
    if classify_fallback:
        debug = explain_classification(
            article.get("title", ""),
            article.get("description", ""),
            max_categories=max_categories,
        )
        if not has_informative_category and debug["categories"]:
            merged = list(debug["categories"])
            used_classifier = True
        elif has_informative_category:
            merged, used_classifier = _reconcile_existing_and_classifier(
                merged,
                debug,
                max_categories=max_categories,
            )

    if max_categories and max_categories > 0:
        merged = merged[:max_categories]

    if not merged:
        merged = ["General"]

    article["categories"] = merged
    article["category"] = merged[0]

    return {
        "categories": merged,
        "unknown_tokens": unknown_tokens,
        "used_classifier": used_classifier,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def classify_category(title: str, description: str = "") -> Optional[str]:
    """
    Classify a commodity article into up to two frontend filter categories.

    Returns None if the classifier abstains, allowing callers to preserve an
    external fallback category such as "General".
    """
    matched = classify_categories(
        title,
        description,
        max_categories=MAX_CATEGORIES_PER_ARTICLE,
    )
    return ", ".join(matched) if matched else None


# ---------------------------------------------------------------------------
# Standalone script — re-classify articles in feed.json
# ---------------------------------------------------------------------------


def _reclassify_feed(
    input_path: Path,
    output_path: Path,
    all_sources: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Load feed.json, re-classify articles, and write back.

    By default only ICIS articles are reclassified. Pass all_sources=True to
    reclassify every article regardless of source.
    """
    if not input_path.exists():
        logger.error("Feed file not found: %s", input_path)
        return

    with open(input_path, encoding="utf-8") as handle:
        feed = json.load(handle)

    articles: list[dict[str, Any]] = feed.get("articles", [])
    changed = 0
    unchanged = 0
    skipped = 0

    for article in articles:
        source = str(article.get("source", ""))
        title = str(article.get("title", ""))
        old_categories = merge_category_lists(
            article.get("categories"),
            article.get("category"),
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        if not old_categories:
            old_categories = ["General"]

        if not all_sources and source != "ICIS":
            skipped += 1
            continue

        classified = classify_categories(
            title,
            article.get("description", ""),
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        new_categories = classified if classified else ["General"]

        if new_categories != old_categories:
            if not dry_run:
                article["categories"] = new_categories
                article["category"] = new_categories[0]
            changed += 1
            logger.debug('  "%s" -> %r -> %r', title[:60], old_categories, new_categories)
        else:
            if not dry_run:
                article["categories"] = old_categories
                article["category"] = old_categories[0]
            unchanged += 1

    total_processed = changed + unchanged
    print(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"Processed {total_processed} articles "
        f"({'all sources' if all_sources else 'ICIS only'}): "
        f"{changed} reclassified, {unchanged} unchanged, {skipped} skipped"
    )

    if not dry_run:
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(feed, handle, indent=2, ensure_ascii=False)
        print(f"Saved -> {output_path}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Re-classify commodity articles in feed.json using the keyword index."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_FEED_PATH),
        help=f"Path to feed.json (default: {DEFAULT_FEED_PATH})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path (default: same as --input, overwrites in place)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_sources",
        help="Re-classify all articles regardless of source (default: ICIS only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing to disk",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log each reclassification at DEBUG level",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    _reclassify_feed(
        input_path=input_path,
        output_path=output_path,
        all_sources=args.all_sources,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
