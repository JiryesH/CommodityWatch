#!/usr/bin/env python3
"""
One-time migration for feed category contract.

Rewrites legacy/unknown category payloads into canonical:
  - article["categories"]: array of canonical labels
  - article["category"]: deterministic primary canonical label
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from classifier import (
    CANONICAL_CATEGORIES,
    LEGACY_CATEGORY_MAP,
    MAX_CATEGORIES_PER_ARTICLE,
    classify_categories,
    iter_raw_category_tokens,
    merge_category_lists,
    normalize_article_categories,
    normalize_category_token,
)
from dedupe_utils import canonical_article_dedupe_key, canonical_article_id
from feed_io import load_feed_json, save_feed_json

def _article_raw_tokens(article: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for token in iter_raw_category_tokens(article.get("categories")) + iter_raw_category_tokens(article.get("category")):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def migrate_feed(path: Path, *, dry_run: bool, max_categories: int) -> int:
    if not path.exists():
        print(f"ERROR: feed file not found: {path}")
        return 1

    payload = load_feed_json(path)
    articles = payload.get("articles", [])

    changed_articles = 0
    unknown_tokens_rewritten = 0
    classifier_fallback_used = 0
    truncated_articles = 0
    dedupe_ids_rewritten = 0
    dedupe_collapsed = 0
    legacy_rewrite_counts: Counter[tuple[str, str]] = Counter()
    unknown_after: set[str] = set()
    dedupe_index: dict[str, dict[str, Any]] = {}
    deduped_articles: list[dict[str, Any]] = []

    for article in articles:
        before_serialized = json.dumps(
            {
                "id": article.get("id"),
                "category": article.get("category"),
                "categories": article.get("categories"),
            },
            sort_keys=True,
            ensure_ascii=False,
        )

        raw_tokens = _article_raw_tokens(article)
        for token in raw_tokens:
            normalized = normalize_category_token(token)
            if normalized is None:
                continue
            if token != normalized:
                legacy_rewrite_counts[(token, normalized)] += 1

        merged_unbounded = merge_category_lists(
            article.get("categories"),
            article.get("category"),
            max_categories=None,
        )
        if not merged_unbounded:
            merged_unbounded = classify_categories(
                article.get("title", ""),
                article.get("description", ""),
                max_categories=None,
            )
        if max_categories > 0 and len(merged_unbounded) > max_categories:
            truncated_articles += 1

        result = normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=max_categories,
        )
        canonical_id = canonical_article_id(article.get("link", ""), article.get("title", ""))
        if article.get("id") != canonical_id:
            dedupe_ids_rewritten += 1
        article["id"] = canonical_id

        unknown_tokens_rewritten += len(result.get("unknown_tokens", []))
        classifier_fallback_used += 1 if result.get("used_classifier") else 0

        for cat in article.get("categories", []):
            if cat not in CANONICAL_CATEGORIES:
                unknown_after.add(cat)

        dedupe_key = canonical_article_dedupe_key(article)
        if not dedupe_key:
            dedupe_key = f"id:{article['id']}"

        existing = dedupe_index.get(dedupe_key)
        if existing is None:
            dedupe_index[dedupe_key] = article
            deduped_articles.append(article)
        else:
            merged_categories = merge_category_lists(
                existing.get("categories"),
                existing.get("category"),
                article.get("categories"),
                article.get("category"),
                max_categories=max_categories,
            )
            if not merged_categories:
                merged_categories = ["General"]
            existing["categories"] = merged_categories
            existing["category"] = merged_categories[0]
            for field in ("description", "published", "source", "feed", "link", "title"):
                if not existing.get(field) and article.get(field):
                    existing[field] = article[field]
            existing["id"] = canonical_article_id(
                existing.get("link", ""),
                existing.get("title", ""),
            )
            dedupe_collapsed += 1

        after_serialized = json.dumps(
            {
                "id": article.get("id"),
                "category": article.get("category"),
                "categories": article.get("categories"),
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        if before_serialized != after_serialized:
            changed_articles += 1

    payload["articles"] = deduped_articles

    category_contract = {
        "canonical_categories": list(CANONICAL_CATEGORIES),
        "max_categories_per_article": max_categories,
        "articles_updated": changed_articles,
        "legacy_rewrites_applied": sum(legacy_rewrite_counts.values()),
        "unknown_tokens_rewritten": unknown_tokens_rewritten,
        "classifier_fallback_used": classifier_fallback_used,
        "truncated_to_max_categories": truncated_articles,
        "dedupe_id_rewrites": dedupe_ids_rewritten,
        "dedupe_collapsed": dedupe_collapsed,
        "unknown_categories_after": len(unknown_after),
    }
    payload.setdefault("metadata", {})
    payload["metadata"]["canonical_categories"] = list(CANONICAL_CATEGORIES)
    payload["metadata"]["total_articles"] = len(deduped_articles)
    payload["metadata"]["dedupe"] = {
        "new": len(deduped_articles),
        "updated": changed_articles,
        "merged": dedupe_collapsed,
    }
    payload["metadata"]["category_contract"] = category_contract

    if not dry_run:
        save_feed_json(path, payload)

    print(f"Processed articles: {len(articles)}")
    print(f"Articles updated: {changed_articles}")
    print(f"Legacy rewrites applied: {sum(legacy_rewrite_counts.values())}")
    print(f"Unknown tokens rewritten/dropped: {unknown_tokens_rewritten}")
    print(f"Classifier fallback used: {classifier_fallback_used}")
    print(f"Articles truncated to max={max_categories}: {truncated_articles}")
    print(f"Dedupe ID rewrites: {dedupe_ids_rewritten}")
    print(f"Dedupe rows collapsed: {dedupe_collapsed}")
    print(f"Articles after dedupe collapse: {len(deduped_articles)}")
    print(f"VALIDATION unknown_categories_after={len(unknown_after)}")
    print("Rewrite rules:")
    for legacy, canonical in sorted(LEGACY_CATEGORY_MAP.items()):
        print(f"  {legacy} -> {canonical}")

    if legacy_rewrite_counts:
        print("Rewrite counts:")
        for (legacy, canonical), count in sorted(legacy_rewrite_counts.items()):
            print(f"  {legacy} -> {canonical}: {count}")

    return 1 if unknown_after else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate feed categories to canonical contract.")
    parser.add_argument(
        "--feed",
        default=str(Path(__file__).parent / "data" / "feed.json"),
        help="Path to feed.json (default: data/feed.json)",
    )
    parser.add_argument(
        "--max-categories",
        type=int,
        default=MAX_CATEGORIES_PER_ARTICLE,
        help=f"Maximum categories per article (default: {MAX_CATEGORIES_PER_ARTICLE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run migration checks without writing changes",
    )
    args = parser.parse_args()

    exit_code = migrate_feed(
        path=Path(args.feed),
        dry_run=args.dry_run,
        max_categories=max(1, args.max_categories),
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
