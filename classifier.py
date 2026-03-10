"""
Contango — Commodity Article Classifier
========================================
Classifies commodity news articles into frontend filter categories using
keyword matching against commodity_taxonomy.json, plus a curated supplement
list for common terms not directly derivable from taxonomy commodity names.

Usage as a module (called by rss_scraper.py at scrape time):
    from classifier import classify_category
    category = classify_category(title, description)   # returns str or None

Usage as a standalone script (re-classify all ICIS articles in feed.json):
    python classifier.py                  # re-classify ICIS articles in data/feed.json
    python classifier.py --all            # re-classify every article regardless of source
    python classifier.py --input path/to/feed.json --output path/to/feed.json
    python classifier.py --dry-run        # print changes without writing

How it works:
    1. Loads commodity_taxonomy.json (categories → subcategories → commodities).
    2. Builds a list of (compiled_regex, filter_category) tuples from every
       commodity name, alias, and variant in the taxonomy.
    3. Appends supplement keywords for common shorthand, plural forms, and terms
       not represented in the taxonomy (e.g. "crude", "steel", "tanker").
    4. Sorts the list longest-keyword-first so specific phrases win over shorter
       ones (e.g. "liquefied natural gas" matches before "gas").
    5. Scans the article title + description for the first two distinct category
       matches and returns them joined by ", " (e.g. "Oil - Crude, Shipping").
    6. Returns None if no keywords matched — callers keep the article's original
       category (typically "General" for unclassified ICIS articles).

Extending the classifier:
    - To cover new filter pills: add an entry to _TAXONOMY_FILTER_MAP.
    - To fix a missed or wrong classification: add a line to _SUPPLEMENT_KEYWORDS.
      Supplement keywords are processed after taxonomy entries, so they can
      override taxonomy defaults for ambiguous terms.
    - To add an entirely new source or classification strategy: subclass or
      extend the KeywordClassifier class rather than editing the module globals.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
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
# Maps (taxonomy_category, taxonomy_subcategory) → frontend filter-pill value.
# Use None as the subcategory to match every subcategory under that category.
#
# To add a new filter pill (e.g. "Construction Materials"):
#   1. Add the pill to index.html
#   2. Add the (category, subcategory) → "PillLabel" entries here

_TAXONOMY_FILTER_MAP: dict[tuple[str, Optional[str]], str] = {
    ("Agriculture", "Fertilizers"):           "Fertilizers",
    ("Agriculture", "Grains and Oilseeds"):   "Agriculture",
    ("Agriculture", "Proteins and Feed"):     "Agriculture",
    ("Agriculture", "Sugar"):                 "Agriculture",
    ("Chemicals",   None):                    "Chemicals",
    # Construction Materials has no filter pill yet — add mapping when pill is added
    ("Energy",      "Biofuels"):              "Energy Transition",
    ("Energy",      "Coal and Coke"):         "Coal",
    ("Energy",      "Crude Oil"):             "Oil - Crude",
    ("Energy",      "Hydrogen and Ammonia"):  "Energy Transition",
    ("Energy",      "Natural Gas and LNG"):   "Natural Gas",   # LNG split below
    ("Energy",      "Power"):                 "Electric Power",
    ("Energy",      "Refined Products"):      "Oil - Refined Products",
    ("Environmental Markets", None):          "Energy Transition",
    ("Metals",      None):                    "Metals",
}

# Commodity names/aliases (lowercase) within the "Natural Gas and LNG"
# subcategory whose primary classification should be "LNG" not "Natural Gas".
_LNG_NAMES: frozenset[str] = frozenset({
    "liquefied natural gas", "lng", "bio-lng", "japan korea marker", "jkm",
})

# ---------------------------------------------------------------------------
# Supplement keywords
# ---------------------------------------------------------------------------
# Common shorthand, plural forms, spelling variants, and terms (e.g. Shipping)
# that aren't directly derivable from taxonomy commodity names.
#
# Format: (keyword_lowercase, frontend_filter_value)
# Processed AFTER taxonomy entries — supplements can override taxonomy defaults
# for ambiguous terms (later entries are added with deduplication so first wins
# for taxonomy; supplements only fill gaps).
#
# Keep entries grouped by filter category for readability.

_SUPPLEMENT_KEYWORDS: list[tuple[str, str]] = [
    # ── Oil – Crude ──────────────────────────────────────────────────────────
    ("crude oil",              "Oil - Crude"),
    ("crude",                  "Oil - Crude"),
    ("petroleum",              "Oil - Crude"),
    ("oil barrel",             "Oil - Crude"),
    ("oil prices",             "Oil - Crude"),
    # ── Oil – Refined Products ───────────────────────────────────────────────
    ("refined products",       "Oil - Refined Products"),
    ("refinery",               "Oil - Refined Products"),
    ("refining",               "Oil - Refined Products"),
    ("refining margins",       "Oil - Refined Products"),
    # ── Natural Gas ──────────────────────────────────────────────────────────
    ("natural gas",            "Natural Gas"),
    ("pipeline gas",           "Natural Gas"),
    ("gas prices",             "Natural Gas"),
    # ── LNG ──────────────────────────────────────────────────────────────────
    ("lng",                    "LNG"),
    ("liquefied natural gas",  "LNG"),
    # ── Coal ─────────────────────────────────────────────────────────────────
    ("coal",                   "Coal"),
    ("coking coal",            "Coal"),
    ("thermal coal",           "Coal"),
    # ── Electric Power ───────────────────────────────────────────────────────
    ("electricity",            "Electric Power"),
    ("power prices",           "Electric Power"),
    ("power grid",             "Electric Power"),
    # ── Energy Transition ────────────────────────────────────────────────────
    ("renewables",             "Energy Transition"),
    ("renewable energy",       "Energy Transition"),
    ("energy transition",      "Energy Transition"),
    ("solar",                  "Energy Transition"),
    ("wind power",             "Energy Transition"),
    ("wind energy",            "Energy Transition"),
    ("wind farm",              "Energy Transition"),
    ("offshore wind",          "Energy Transition"),
    ("onshore wind",           "Energy Transition"),
    ("solar power",            "Energy Transition"),
    ("solar capacity",         "Energy Transition"),
    ("clean energy",           "Energy Transition"),
    ("green energy",           "Energy Transition"),
    ("carbon credits",         "Energy Transition"),
    ("carbon allowances",      "Energy Transition"),
    ("emissions allowances",   "Energy Transition"),
    ("emissions trading",      "Energy Transition"),
    ("carbon market",          "Energy Transition"),
    ("carbon price",           "Energy Transition"),
    ("decarbonisation",        "Energy Transition"),
    ("decarbonization",        "Energy Transition"),
    # ── Chemicals ────────────────────────────────────────────────────────────
    ("petrochemicals",         "Chemicals"),
    ("polymers",               "Chemicals"),
    ("plastics",               "Chemicals"),
    # ── Metals ───────────────────────────────────────────────────────────────
    ("iron ore",               "Metals"),
    ("scrap metal",            "Metals"),
    ("steel",                  "Metals"),
    ("aluminium",              "Metals"),
    ("aluminum",               "Metals"),
    ("copper",                 "Metals"),
    ("stainless steel",        "Metals"),
    # ── Agriculture ──────────────────────────────────────────────────────────
    ("grains",                 "Agriculture"),
    ("oilseeds",               "Agriculture"),
    ("vegetable oil",          "Agriculture"),
    ("palm oil",               "Agriculture"),
    # ── Fertilizers ──────────────────────────────────────────────────────────
    ("fertilizer",             "Fertilizers"),
    ("fertiliser",             "Fertilizers"),
    # ── Shipping (not in taxonomy; S&P has a dedicated feed — classify ICIS too)
    ("shipping rates",         "Shipping"),
    ("freight rates",          "Shipping"),
    ("tanker rates",           "Shipping"),
    ("tanker",                 "Shipping"),
    ("vlcc",                   "Shipping"),
    ("bunker fuel",            "Shipping"),
    ("charter rate",           "Shipping"),
]

# ---------------------------------------------------------------------------
# Keyword index — built once, cached for the process lifetime
# ---------------------------------------------------------------------------

# Each entry: (compiled_regex, filter_category, keyword_length)
_KeywordIndex = list[tuple[re.Pattern[str], str, int]]

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


def classify_categories(
    title: str,
    description: str = "",
    *,
    max_categories: Optional[int] = MAX_CATEGORIES_PER_ARTICLE,
) -> list[str]:
    """Classify article text into canonical category labels."""
    text = f"{title} {description}"
    index = _get_keyword_index()
    seen_cats: set[str] = set()

    for pattern, category, _ in index:
        if category in seen_cats:
            continue
        if pattern.search(text):
            seen_cats.add(category)
            if len(seen_cats) >= len(CANONICAL_CATEGORIES):
                break

    matched = sorted(seen_cats, key=lambda cat: _CATEGORY_PRIORITY[cat])
    if max_categories and max_categories > 0:
        matched = matched[:max_categories]
    return matched


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
    - Optionally classifies by title/description if no canonical category exists
    - Writes deterministic `categories` array and `category` primary label
    """
    merged = merge_category_lists(
        article.get("categories"),
        article.get("category"),
        max_categories=None,
    )

    _, unknown_from_categories = normalize_categories(article.get("categories"), max_categories=None)
    _, unknown_from_category = normalize_categories(article.get("category"), max_categories=None)
    unknown_tokens = list(dict.fromkeys(unknown_from_categories + unknown_from_category))

    # "General" is a placeholder, not an informative classification.
    # Keep trying classifier fallback when categories are empty or only General.
    has_informative_category = any(cat != "General" for cat in merged)

    used_classifier = False
    if classify_fallback and not has_informative_category:
        classified = classify_categories(
            article.get("title", ""),
            article.get("description", ""),
            max_categories=max_categories,
        )
        if classified:
            merged = classified
            used_classifier = True

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


def build_keyword_index() -> _KeywordIndex:
    """
    Build and return the keyword classification index.

    Loads commodity_taxonomy.json, derives keywords from every commodity name,
    alias, and variant, then appends the supplement list.  Sorted longest-first
    so more specific phrases beat shorter ones.
    """
    entries: _KeywordIndex = []
    seen: set[str] = set()

    def _add(kw: str, cat: str) -> None:
        key = kw.strip().lower()
        if not key or len(key) < 2 or key in seen:
            return
        seen.add(key)
        # Negative lookbehind/ahead enforces word boundaries without relying
        # solely on \b, which handles hyphenated / numeric terms correctly.
        pattern = re.compile(
            r"(?<![A-Za-z0-9])" + re.escape(key) + r"(?![A-Za-z0-9])",
            re.IGNORECASE,
        )
        entries.append((pattern, cat, len(key)))

    # ── 1. Taxonomy-derived keywords ─────────────────────────────────────────
    if TAXONOMY_PATH.exists():
        try:
            with open(TAXONOMY_PATH, encoding="utf-8") as f:
                taxonomy = json.load(f)

            for cat in taxonomy.get("categories", []):
                cat_name: str = cat["name"]
                for sub in cat.get("subcategories", []):
                    sub_name: str = sub["name"]

                    # Resolve filter category for this subcategory
                    filter_cat = _TAXONOMY_FILTER_MAP.get((cat_name, sub_name))
                    if filter_cat is None:
                        filter_cat = _TAXONOMY_FILTER_MAP.get((cat_name, None))
                    if filter_cat is None:
                        continue  # e.g. Construction Materials — no pill yet

                    # Index the subcategory name itself
                    _add(sub_name, filter_cat)

                    for commodity in sub.get("commodities", []):
                        fc = filter_cat
                        # LNG disambiguation within "Natural Gas and LNG"
                        if cat_name == "Energy" and sub_name == "Natural Gas and LNG":
                            cname_lower = commodity["name"].lower()
                            aliases_lower = [a.lower() for a in commodity.get("aliases", [])]
                            if cname_lower in _LNG_NAMES or any(
                                a in _LNG_NAMES for a in aliases_lower
                            ):
                                fc = "LNG"

                        terms = (
                            [commodity["name"]]
                            + commodity.get("aliases", [])
                            + commodity.get("variants", [])
                        )
                        for term in terms:
                            _add(term, fc)

        except Exception as exc:
            logger.warning(f"Could not load taxonomy for classification: {exc}")
    else:
        logger.warning(
            f"Taxonomy not found at {TAXONOMY_PATH}; articles will remain unclassified."
        )

    # ── 2. Supplement keywords ────────────────────────────────────────────────
    for kw, cat in _SUPPLEMENT_KEYWORDS:
        _add(kw, cat)

    # Sort longest-first so more specific phrases win over shorter ones
    entries.sort(key=lambda x: -x[2])

    logger.info(f"Keyword index built: {len(entries)} keywords")
    return entries


def _get_keyword_index() -> _KeywordIndex:
    """Return the cached keyword index, building it on first call."""
    global _KEYWORD_INDEX
    if _KEYWORD_INDEX is None:
        _KEYWORD_INDEX = build_keyword_index()
    return _KEYWORD_INDEX


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_category(title: str, description: str = "") -> Optional[str]:
    """
    Classify a commodity article into up to two frontend filter categories.

    Keyword-matches the article title and description against the taxonomy
    index and returns up to two distinct category matches joined by ", "
    (e.g. "Oil - Crude" or "Oil - Crude, Shipping").

    Returns None if no commodity keywords matched, leaving the caller's
    original category unchanged.

    Args:
        title:       Article headline text.
        description: Optional article summary / description text.

    Returns:
        A category string, a comma-joined pair of categories, or None.
    """
    matched = classify_categories(title, description, max_categories=MAX_CATEGORIES_PER_ARTICLE)
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

    By default only ICIS articles (source == "ICIS") are reclassified, since
    S&P feeds already carry the correct category from the scraper.  Pass
    all_sources=True to reclassify every article regardless of source.
    """
    if not input_path.exists():
        logger.error(f"Feed file not found: {input_path}")
        return

    with open(input_path, encoding="utf-8") as f:
        feed = json.load(f)

    articles: list[dict] = feed.get("articles", [])
    changed = 0
    unchanged = 0
    skipped = 0

    for article in articles:
        source: str = article.get("source", "")
        title: str = article.get("title", "")
        old_categories = merge_category_lists(
            article.get("categories"),
            article.get("category"),
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        if not old_categories:
            old_categories = ["General"]

        # Decide whether to reclassify this article
        if not all_sources and source != "ICIS":
            skipped += 1
            continue

        classified = classify_categories(
            title,
            article.get("description", ""),
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        new_categories = classified if classified else ["General"]
        new_primary = new_categories[0]

        if new_categories != old_categories:
            if not dry_run:
                article["categories"] = new_categories
                article["category"] = new_primary
            changed += 1
            logger.debug(
                '  "%s" → %r ⟶ %r',
                title[:60],
                old_categories,
                new_categories,
            )
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
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feed, f, indent=2, ensure_ascii=False)
        print(f"Saved → {output_path}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Re-classify commodity articles in feed.json using the taxonomy keyword index."
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
