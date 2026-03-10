"""
Shared article normalization, deduplication, and sorting helpers.

These helpers are intentionally backend-only and keep the scraper modules thin
without changing their public contracts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

from classifier import (
    MAX_CATEGORIES_PER_ARTICLE,
    merge_category_lists,
    normalize_article_categories,
)
from dedupe_utils import canonical_article_dedupe_key


Article = dict[str, Any]
ParsePubDate = Callable[[str], Optional[datetime]]
GenerateArticleId = Callable[[Any, Any], str]


def to_utc_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def normalize_article_published(
    article: Article,
    *,
    parse_pub_date: ParsePubDate,
) -> None:
    raw = article.get("published")
    if raw is None:
        article["published"] = None
        return

    if isinstance(raw, datetime):
        article["published"] = to_utc_iso(raw)
        return

    if isinstance(raw, str):
        article["published"] = to_utc_iso(parse_pub_date(raw))
        return

    article["published"] = None


def merge_duplicate_articles(
    existing: Article,
    incoming: Article,
    *,
    generate_article_id: GenerateArticleId,
    max_categories: int = MAX_CATEGORIES_PER_ARTICLE,
) -> Article:
    existing_key = _merge_preference_key(existing)
    incoming_key = _merge_preference_key(incoming)
    winner, loser = (
        (incoming, existing) if incoming_key > existing_key else (existing, incoming)
    )

    merged = dict(winner)
    for key, value in loser.items():
        if key in {"id", "category", "categories"}:
            continue
        if key not in merged or _is_effectively_empty(merged.get(key)):
            if not _is_effectively_empty(value):
                merged[key] = value

    merged_categories = merge_category_lists(
        winner.get("categories"),
        winner.get("category"),
        loser.get("categories"),
        loser.get("category"),
        max_categories=max_categories,
    )
    if not merged_categories:
        merged_categories = ["General"]

    merged["categories"] = merged_categories
    merged["category"] = merged_categories[0]
    merged["id"] = generate_article_id(merged.get("link", ""), merged.get("title", ""))
    return merged


def deduplicate_articles(
    articles: list[Article],
    *,
    parse_pub_date: ParsePubDate,
    generate_article_id: GenerateArticleId,
    max_categories: int = MAX_CATEGORIES_PER_ARTICLE,
) -> tuple[list[Article], dict[str, int]]:
    seen: dict[str, Article] = {}
    merged_count = 0

    for article in articles:
        normalize_article_published(article, parse_pub_date=parse_pub_date)
        normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=max_categories,
        )
        article["id"] = generate_article_id(article.get("link", ""), article.get("title", ""))

        dedupe_key = canonical_article_dedupe_key(article)
        if not dedupe_key:
            dedupe_key = f"id:{article['id']}"

        existing = seen.get(dedupe_key)
        if existing is None:
            seen[dedupe_key] = article
            continue

        seen[dedupe_key] = merge_duplicate_articles(
            existing,
            article,
            generate_article_id=generate_article_id,
            max_categories=max_categories,
        )
        merged_count += 1

    deduped = list(seen.values())
    for article in deduped:
        normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=max_categories,
        )
        article["id"] = generate_article_id(article.get("link", ""), article.get("title", ""))

    return deduped, {
        "input": len(articles),
        "new": len(deduped),
        "merged": merged_count,
    }


def sort_articles_by_date(
    articles: list[Article],
    *,
    parse_pub_date: ParsePubDate,
    descending: bool = True,
) -> list[Article]:
    return sorted(
        articles,
        key=lambda article: _article_sort_key(
            article,
            parse_pub_date=parse_pub_date,
            descending=descending,
        ),
    )


def _is_effectively_empty(value: Any) -> bool:
    return value in (None, "", [], {})


def _merge_preference_key(article: Article) -> tuple[int, int, int, int, str, str, str]:
    description = str(article.get("description") or "")
    title = str(article.get("title") or "")
    return (
        1 if article.get("published") else 0,
        1 if description.strip() else 0,
        len(description.strip()),
        len(title.strip()),
        str(article.get("source") or ""),
        str(article.get("feed") or ""),
        str(article.get("link") or ""),
    )


def _article_sort_tiebreaker(article: Article) -> tuple[str, str, str]:
    return (
        str(article.get("id") or ""),
        str(article.get("link") or ""),
        str(article.get("title") or ""),
    )


def _published_datetime_for_sort(
    article: Article,
    *,
    parse_pub_date: ParsePubDate,
) -> Optional[datetime]:
    raw = article.get("published")
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, str):
        dt = parse_pub_date(raw)
    else:
        dt = None

    if dt is None:
        article["published"] = None
        return None

    article["published"] = to_utc_iso(dt)
    return dt


def _article_sort_key(
    article: Article,
    *,
    parse_pub_date: ParsePubDate,
    descending: bool,
) -> tuple[int, int, str, str, str]:
    dt = _published_datetime_for_sort(article, parse_pub_date=parse_pub_date)
    tie = _article_sort_tiebreaker(article)
    if dt is None:
        return (1, 0, *tie)

    epoch_us = int(dt.timestamp() * 1_000_000)
    dt_key = -epoch_us if descending else epoch_us
    return (0, dt_key, *tie)
