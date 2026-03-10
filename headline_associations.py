from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Pattern, TypedDict

from feed_io import load_feed_json


DEFAULT_HEADLINE_LIMIT = 10
MAX_HEADLINE_LIMIT = 25
_CATEGORY_SCORE = 1
_TITLE_SCORE = 3
_BODY_SCORE = 2


class RelatedHeadlineRecord(TypedDict):
    id: str | None
    title: str
    source: str
    published: str | None
    link: str | None


@dataclass(frozen=True)
class SeriesHeadlineRule:
    categories: tuple[str, ...]
    aliases: tuple[str, ...]
    blocked_aliases: tuple[str, ...] = ()
    minimum_score: int = 3
    require_category_match: bool = False


@dataclass(frozen=True)
class CompiledSeriesHeadlineRule:
    categories: frozenset[str]
    alias_patterns: tuple[Pattern[str], ...]
    blocked_patterns: tuple[Pattern[str], ...]
    minimum_score: int
    require_category_match: bool

    def score_article(self, article: Mapping[str, Any]) -> int:
        title_text = _normalize_text(article.get("title"))
        description_text = _normalize_text(article.get("description"))
        ner_text = _normalize_text(_build_ner_text(article.get("ner")))
        combined_text = " ".join(part for part in [title_text, description_text, ner_text] if part)

        if not combined_text:
            return 0

        if any(pattern.search(combined_text) for pattern in self.blocked_patterns):
            return 0

        category_match = bool(self.categories.intersection(_collect_categories(article)))
        if self.require_category_match and not category_match:
            return 0

        score = _CATEGORY_SCORE if category_match else 0
        matched_alias = False

        for pattern in self.alias_patterns:
            if pattern.search(title_text):
                score += _TITLE_SCORE
                matched_alias = True
                continue

            if pattern.search(description_text) or pattern.search(ner_text):
                score += _BODY_SCORE
                matched_alias = True

        if not matched_alias or score < self.minimum_score:
            return 0

        return score


STRICT_SERIES_HEADLINE_RULES: dict[str, SeriesHeadlineRule] = {
    "crude_oil_brent": SeriesHeadlineRule(
        categories=("Oil - Crude", "Shipping"),
        aliases=("Brent",),
    ),
    "crude_oil_wti": SeriesHeadlineRule(
        categories=("Oil - Crude", "Shipping"),
        aliases=("WTI", "West Texas Intermediate"),
        minimum_score=4,
    ),
    "crude_oil_dubai": SeriesHeadlineRule(
        categories=("Oil - Crude",),
        aliases=("Platts cash Dubai", "Cash Dubai", "Dubai crude", "Dubai/Oman"),
        minimum_score=4,
    ),
    "natural_gas_henry_hub": SeriesHeadlineRule(
        categories=("Natural Gas", "LNG"),
        aliases=("Henry Hub",),
    ),
    "natural_gas_ttf": SeriesHeadlineRule(
        categories=("Natural Gas", "LNG"),
        aliases=("TTF", "Dutch TTF", "Title Transfer Facility"),
        minimum_score=4,
    ),
    "lng_asia_japan_import_proxy": SeriesHeadlineRule(
        categories=("LNG", "Natural Gas"),
        aliases=(
            "LNG",
            "JKM",
            "Japan Korea Marker",
            "East Asia",
            "Asia",
            "Asian",
            "Northeast Asia",
            "South Asia",
            "Bangladesh",
            "India",
            "Pakistan",
            "China",
            "Japan",
            "Korea",
            "South Korea",
        ),
        minimum_score=3,
        require_category_match=True,
    ),
    "gold_worldbank_monthly": SeriesHeadlineRule(
        categories=("Metals",),
        aliases=("Gold",),
        minimum_score=4,
    ),
}

METALS_SERIES_ALIASES: dict[str, tuple[str, ...]] = {
    "silver_worldbank_monthly": ("Silver",),
    "copper_worldbank_monthly": ("Copper",),
    "aluminium_worldbank_monthly": ("Aluminum", "Aluminium"),
    "platinum_worldbank_monthly": ("Platinum",),
    "palladium_imf_monthly": ("Palladium",),
    "nickel_worldbank_monthly": ("Nickel",),
    "zinc_worldbank_monthly": ("Zinc",),
    "iron_ore_62pct_china_monthly": ("Iron Ore",),
    "lithium_metal_imf_monthly": ("Lithium",),
    "cobalt_imf_monthly": ("Cobalt",),
}

# "lead" is intentionally excluded from automatic headline matching because the
# token is too ambiguous in commodity-news prose and creates noisy false positives.

AGRI_SERIES_RULES: dict[str, SeriesHeadlineRule] = {
    "wheat_global_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture",),
        aliases=("Wheat",),
        minimum_score=3,
        require_category_match=True,
    ),
    "corn_global_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture",),
        aliases=("Corn",),
        minimum_score=3,
        require_category_match=True,
    ),
    "soybeans_global_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Soybeans", "Soybean", "Soy"),
        minimum_score=3,
        require_category_match=True,
    ),
    "soybean_oil_global_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Soybean Oil", "Soy Oil", "Soyoil"),
        minimum_score=3,
        require_category_match=True,
    ),
    "palm_oil_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "Oil - Refined Products", "Oil - Crude"),
        aliases=("Palm Oil",),
        minimum_score=3,
        require_category_match=True,
    ),
    "rice_thai_5pct_monthly": SeriesHeadlineRule(
        categories=("Agriculture",),
        aliases=("Rice", "Thai Rice"),
        minimum_score=4,
        require_category_match=True,
    ),
    "lumber_monthly_ppi_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Lumber",),
        minimum_score=3,
        require_category_match=True,
    ),
    "coffee_arabica_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Arabica", "Coffee"),
        minimum_score=3,
        require_category_match=True,
    ),
    "coffee_robusta_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Robusta", "Coffee"),
        minimum_score=3,
        require_category_match=True,
    ),
    "sugar_no11_world_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Sugar",),
        minimum_score=3,
        require_category_match=True,
    ),
    "cotton_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Cotton",),
        minimum_score=3,
        require_category_match=True,
    ),
    "cocoa_monthly_proxy": SeriesHeadlineRule(
        categories=("Agriculture", "General"),
        aliases=("Cocoa",),
        minimum_score=3,
        require_category_match=True,
    ),
}

ENERGY_COMPLEX_RULES: dict[str, SeriesHeadlineRule] = {
    "rbob_gasoline_spot_proxy": SeriesHeadlineRule(
        categories=("Oil - Refined Products", "Oil - Crude"),
        aliases=("Gasoline", "RBOB"),
        minimum_score=3,
        require_category_match=True,
    ),
    "heating_oil_no2_nyharbor": SeriesHeadlineRule(
        categories=("Oil - Refined Products", "Oil - Crude"),
        aliases=("Diesel", "Heating Oil", "ULSD", "Gasoil"),
        minimum_score=3,
        require_category_match=True,
    ),
    "thermal_coal_newcastle": SeriesHeadlineRule(
        categories=("Coal",),
        aliases=("Thermal Coal", "Coal", "Newcastle"),
        minimum_score=3,
        require_category_match=True,
    ),
    "rubber_rss3_monthly": SeriesHeadlineRule(
        categories=("Chemicals", "General"),
        aliases=("Rubber",),
        minimum_score=3,
        require_category_match=True,
    ),
}

RAW_SERIES_HEADLINE_RULES: dict[str, SeriesHeadlineRule] = {
    **STRICT_SERIES_HEADLINE_RULES,
    **{
        series_key: SeriesHeadlineRule(
            categories=("Metals",),
            aliases=aliases,
            minimum_score=3,
            require_category_match=True,
        )
        for series_key, aliases in METALS_SERIES_ALIASES.items()
    },
    **AGRI_SERIES_RULES,
    **ENERGY_COMPLEX_RULES,
}


class RelatedHeadlineService:
    def __init__(self, feed_path: Path):
        self.feed_path = feed_path
        self._cached_mtime_ns: int | None = None
        self._cached_articles: list[dict[str, Any]] = []

    def list_related(self, series: Mapping[str, Any], *, limit: int = DEFAULT_HEADLINE_LIMIT) -> list[RelatedHeadlineRecord]:
        matcher = self._build_matcher(series)
        if matcher is None:
            return []

        normalized_limit = max(1, min(limit, MAX_HEADLINE_LIMIT))
        matched: list[tuple[datetime, RelatedHeadlineRecord]] = []
        seen_keys: set[str] = set()

        for article in self._load_articles():
            if matcher.score_article(article) == 0:
                continue

            payload = _to_related_headline(article)
            dedupe_key = payload["link"] or f"{payload['title']}|{payload['published'] or ''}"
            if dedupe_key in seen_keys:
                continue

            seen_keys.add(dedupe_key)
            matched.append((_parse_sort_timestamp(payload["published"]), payload))

        matched.sort(key=lambda item: item[0], reverse=True)
        return [payload for _, payload in matched[:normalized_limit]]

    def _build_matcher(self, series: Mapping[str, Any]) -> CompiledSeriesHeadlineRule | None:
        series_key = str(series.get("series_key") or "")
        return COMPILED_SERIES_HEADLINE_RULES.get(series_key)

    def _load_articles(self) -> list[dict[str, Any]]:
        if not self.feed_path.exists():
            raise FileNotFoundError(f"Headline feed not found: {self.feed_path}")

        feed_stat = self.feed_path.stat()
        if self._cached_mtime_ns == feed_stat.st_mtime_ns:
            return self._cached_articles

        payload = load_feed_json(self.feed_path)
        if isinstance(payload, list):
            articles = payload
        elif isinstance(payload, dict):
            raw_articles = payload.get("articles") or payload.get("items") or []
            if not isinstance(raw_articles, list):
                raise ValueError(f"Headline feed has invalid article list: {self.feed_path}")
            articles = raw_articles
        else:
            raise ValueError(f"Headline feed has invalid JSON shape: {self.feed_path}")

        self._cached_articles = [article for article in articles if isinstance(article, dict)]
        self._cached_mtime_ns = feed_stat.st_mtime_ns
        return self._cached_articles


def parse_headline_limit(value: str | None) -> int:
    if value is None or value == "":
        return DEFAULT_HEADLINE_LIMIT

    limit = int(value)
    if limit <= 0:
        raise ValueError("Invalid limit: expected a positive integer")

    return min(limit, MAX_HEADLINE_LIMIT)


def _compile_alias_pattern(value: str) -> Pattern[str]:
    escaped = re.escape(value.strip())
    return re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", re.IGNORECASE)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _collect_categories(article: Mapping[str, Any]) -> set[str]:
    categories: set[str] = set()
    raw_categories = article.get("categories")

    if isinstance(raw_categories, list):
        categories.update(_normalize_text(category) for category in raw_categories if category)

    raw_category = article.get("category")
    if raw_category:
        categories.add(_normalize_text(raw_category))

    return categories


def _build_ner_text(raw_ner: Any) -> str:
    if not isinstance(raw_ner, dict):
        return ""

    entity_values: list[str] = []
    for entity in raw_ner.get("entities") or []:
        if isinstance(entity, dict) and entity.get("text"):
            entity_values.append(str(entity["text"]))

    for country in raw_ner.get("countries") or []:
        if country:
            entity_values.append(str(country))

    return " ".join(entity_values)


def _parse_sort_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _to_related_headline(article: Mapping[str, Any]) -> RelatedHeadlineRecord:
    source = article.get("source") or article.get("feed") or "Unknown source"
    published = article.get("published")
    link = article.get("link")

    return {
        "id": str(article.get("id")) if article.get("id") is not None else None,
        "title": str(article.get("title") or "Untitled article"),
        "source": str(source),
        "published": str(published) if published else None,
        "link": str(link) if link else None,
    }


COMPILED_SERIES_HEADLINE_RULES: dict[str, CompiledSeriesHeadlineRule] = {
    series_key: CompiledSeriesHeadlineRule(
        categories=frozenset(_normalize_text(category) for category in rule.categories if category),
        alias_patterns=tuple(_compile_alias_pattern(alias) for alias in rule.aliases if alias),
        blocked_patterns=tuple(_compile_alias_pattern(alias) for alias in rule.blocked_aliases if alias),
        minimum_score=rule.minimum_score,
        require_category_match=rule.require_category_match,
    )
    for series_key, rule in RAW_SERIES_HEADLINE_RULES.items()
}
