#!/usr/bin/env python3
"""Build a fresh holdout classifier evaluation sample from data/feed.json."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Callable

from build_sample import (
    AMBIGUOUS_ENERGY,
    CHEM_FERT,
    CRUDE_REFINED,
    DUAL_POTENTIAL,
    GAS_LNG,
    GENERAL_ONLY,
    GEOPOLITICAL,
    MARKET_SNAPSHOT,
    PLANT_STATUS,
    SHIPPING,
    SampleRow,
    build_record,
    compute_tags,
    load_articles,
    normalize_text,
    source_key,
    stable_key,
)


DEFAULT_INPUT = Path("data/feed.json")
DEFAULT_EXCLUDE_GOLD = Path("benchmarks/classifier/gold.jsonl")
DEFAULT_OUTPUT = Path("benchmarks/classifier/holdout_sample_scaffold.jsonl")
DEFAULT_SAMPLE_SIZE = 80
DEFAULT_SEED = 20260311

SOURCE_TARGETS: dict[str, int] = {
    "ICIS": 27,
    "Argus Media": 32,
    "S&P Global": 17,
    "Fastmarkets": 4,
}

STRATA: list[tuple[str, int, Callable[[dict[str, Any], set[str]], bool]]] = [
    ("ICIS_general", 7, lambda a, tags: source_key(a) == "ICIS" and GENERAL_ONLY in tags),
    ("ICIS_dual_potential", 5, lambda a, tags: source_key(a) == "ICIS" and DUAL_POTENTIAL in tags),
    ("ICIS_plant_status", 5, lambda a, tags: source_key(a) == "ICIS" and PLANT_STATUS in tags),
    (
        "ICIS_geopolitical_energy",
        6,
        lambda a, tags: source_key(a) == "ICIS" and (GEOPOLITICAL in tags or AMBIGUOUS_ENERGY in tags),
    ),
    ("ICIS_market_snapshot", 4, lambda a, tags: source_key(a) == "ICIS" and MARKET_SNAPSHOT in tags),
    ("Argus_general", 12, lambda a, tags: source_key(a) == "Argus Media" and GENERAL_ONLY in tags),
    ("Argus_shipping_freight", 4, lambda a, tags: source_key(a) == "Argus Media" and SHIPPING in tags),
    ("Argus_geopolitical", 5, lambda a, tags: source_key(a) == "Argus Media" and GEOPOLITICAL in tags),
    ("Argus_market_snapshot", 5, lambda a, tags: source_key(a) == "Argus Media" and MARKET_SNAPSHOT in tags),
    (
        "Argus_commodity_ambiguity",
        5,
        lambda a, tags: source_key(a) == "Argus Media" and any(tag in tags for tag in (CHEM_FERT, GAS_LNG, CRUDE_REFINED)),
    ),
    ("SP_dual_potential", 5, lambda a, tags: source_key(a) == "S&P Global" and DUAL_POTENTIAL in tags),
    ("SP_general", 2, lambda a, tags: source_key(a) == "S&P Global" and GENERAL_ONLY in tags),
    (
        "SP_shipping_or_transition",
        4,
        lambda a, tags: source_key(a) == "S&P Global" and (SHIPPING in tags or "family:energy_transition" in tags),
    ),
    (
        "SP_commodity_ambiguity",
        4,
        lambda a, tags: source_key(a) == "S&P Global" and any(tag in tags for tag in (CHEM_FERT, GAS_LNG, CRUDE_REFINED)),
    ),
    ("Fastmarkets_general", 1, lambda a, tags: source_key(a) == "Fastmarkets" and GENERAL_ONLY in tags),
    ("Fastmarkets_other", 3, lambda a, tags: source_key(a) == "Fastmarkets"),
]


def load_exclusions(path: Path) -> tuple[set[str], set[tuple[str, str]]]:
    excluded_ids: set[str] = set()
    excluded_titles: set[tuple[str, str]] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            article_id = str(row.get("id") or "")
            source = str(row.get("source") or "Unknown")
            title = normalize_text(str(row.get("title") or ""))
            if article_id:
                excluded_ids.add(article_id)
            if title:
                excluded_titles.add((title, source))
    return excluded_ids, excluded_titles


def pick_rows(
    articles: list[dict[str, Any]],
    *,
    seed: int,
    sample_size: int,
    excluded_ids: set[str],
    excluded_titles: set[tuple[str, str]],
) -> list[SampleRow]:
    article_tags = {str(article.get("id")): compute_tags(article) for article in articles}
    selected_ids: set[str] = set()
    selected_title_keys: set[tuple[str, str]] = set()
    rows: list[SampleRow] = []

    def is_excluded(article: dict[str, Any]) -> bool:
        article_id = str(article.get("id") or "")
        title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
        return article_id in excluded_ids or title_key in excluded_titles

    def eligible_for(label: str, predicate: Callable[[dict[str, Any], set[str]], bool]) -> list[dict[str, Any]]:
        eligible: list[dict[str, Any]] = []
        for article in articles:
            article_id = str(article.get("id") or "")
            title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
            tags = article_tags[article_id]
            if is_excluded(article) or article_id in selected_ids or title_key in selected_title_keys:
                continue
            if predicate(article, tags):
                eligible.append(article)
        eligible.sort(key=lambda article: stable_key(article, seed, label))
        return eligible

    for label, quota, predicate in STRATA:
        for article in eligible_for(label, predicate)[:quota]:
            article_id = str(article.get("id") or "")
            title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
            selected_ids.add(article_id)
            selected_title_keys.add(title_key)
            rows.append(
                SampleRow(
                    article=article,
                    primary_stratum=label,
                    tags=tuple(sorted(article_tags[article_id])),
                )
            )

    if len(rows) > sample_size:
        raise ValueError(f"Strata over-selected: {len(rows)} rows for requested sample size {sample_size}")

    source_counts = Counter(source_key(row.article) for row in rows)
    remaining_by_source = {
        source: max(target - source_counts.get(source, 0), 0)
        for source, target in SOURCE_TARGETS.items()
    }

    filler_label = "source_fill"
    filler_candidates: list[dict[str, Any]] = []
    for article in articles:
        article_id = str(article.get("id") or "")
        title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
        if is_excluded(article) or article_id in selected_ids or title_key in selected_title_keys:
            continue
        filler_candidates.append(article)
    filler_candidates.sort(key=lambda article: stable_key(article, seed, filler_label))

    for article in filler_candidates:
        source = source_key(article)
        if remaining_by_source.get(source, 0) <= 0:
            continue
        article_id = str(article.get("id") or "")
        title_key = (normalize_text(str(article.get("title") or "")), source)
        selected_ids.add(article_id)
        selected_title_keys.add(title_key)
        remaining_by_source[source] -= 1
        rows.append(
            SampleRow(
                article=article,
                primary_stratum=f"{source}_fill",
                tags=tuple(sorted(article_tags[article_id])),
            )
        )
        if len(rows) >= sample_size:
            break

    if len(rows) < sample_size:
        raise ValueError(f"Could not reach requested sample size {sample_size}; selected {len(rows)}")

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--exclude-gold", type=Path, default=DEFAULT_EXCLUDE_GOLD)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    articles = load_articles(args.input)
    excluded_ids, excluded_titles = load_exclusions(args.exclude_gold)
    rows = pick_rows(
        articles,
        seed=args.seed,
        sample_size=args.sample_size,
        excluded_ids=excluded_ids,
        excluded_titles=excluded_titles,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(build_record(row), ensure_ascii=True) + "\n")

    print(f"Wrote {len(rows)} sampled articles to {args.output}")
    print("Source counts:")
    for source, count in Counter(source_key(row.article) for row in rows).most_common():
        print(f"  {source}: {count}")
    print("Primary strata:")
    for label, count in Counter(row.primary_stratum for row in rows).most_common():
        print(f"  {label}: {count}")


if __name__ == "__main__":
    main()
