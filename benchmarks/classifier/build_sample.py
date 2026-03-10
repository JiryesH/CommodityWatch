#!/usr/bin/env python3
"""Build a reproducible stratified classifier evaluation sample from data/feed.json."""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from classifier import classify_categories


DEFAULT_INPUT = Path("data/feed.json")
DEFAULT_OUTPUT = Path("benchmarks/classifier/sample_scaffold.jsonl")
DEFAULT_SAMPLE_SIZE = 120
DEFAULT_SEED = 20260310


GENERAL_ONLY = "current_general"
DUAL_POTENTIAL = "dual_potential"
PLANT_STATUS = "plant_status_outage"
GEOPOLITICAL = "geopolitical"
MARKET_SNAPSHOT = "market_snapshot"
SHIPPING = "shipping_freight"
AMBIGUOUS_ENERGY = "ambiguous_energy"
CHEM_FERT = "chemicals_vs_fertilizers"
GAS_LNG = "natural_gas_vs_lng"
CRUDE_REFINED = "crude_vs_refined"


PLANT_STATUS_RE = re.compile(
    r"\b("
    r"plant status|outage|shutdown|shut|restart|turnaround|maintenance|"
    r"force majeure|incident|cracker|refinery|unit|operating rates?|"
    r"run rates?"
    r")\b",
    re.IGNORECASE,
)
GEOPOLITICAL_RE = re.compile(
    r"\b("
    r"war|conflict|sanction|sanctions|tariff|embargo|attack|attacks|"
    r"drone|drones|officials|trump|iran|israel|hormuz|opec|ceasefire|"
    r"retaliation|naval|military|government|ministry"
    r")\b",
    re.IGNORECASE,
)
MARKET_SNAPSHOT_RE = re.compile(
    r"\b("
    r"snapshot|market|markets|prices|premium|discount|spread|margins?|"
    r"outlook|curve|assessments?|weekly|daily|monthly|surge|rally|"
    r"slump|falls?|rises?|futures|cash premium|cash differential"
    r")\b",
    re.IGNORECASE,
)
SHIPPING_RE = re.compile(
    r"\b("
    r"shipping|freight|tanker|vlcc|charter|vessel|cargo|bunker|container|"
    r"aframax|suezmax|lr1|lr2"
    r")\b",
    re.IGNORECASE,
)
AMBIGUOUS_ENERGY_RE = re.compile(r"\benergy\b", re.IGNORECASE)
CHEM_FERT_RE = re.compile(
    r"\b("
    r"ammonia|urea|phosphate|phosphates|potash|sulphur|sulfur|nitrate|"
    r"fertili[sz]er|ammonium|methanol"
    r")\b",
    re.IGNORECASE,
)
GAS_LNG_RE = re.compile(
    r"\b("
    r"natural gas|lng|liquefied natural gas|jkm|gas storage|regas|"
    r"pipeline gas|gas hub|ttf|nbp"
    r")\b",
    re.IGNORECASE,
)
CRUDE_REFINED_RE = re.compile(
    r"\b("
    r"crude|refinery|diesel|gasoil|jet fuel|naphtha|fuel oil|gasoline|"
    r"lpg|oil products|refined products|lsfo|hsfo"
    r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SampleRow:
    article: dict[str, Any]
    primary_stratum: str
    tags: tuple[str, ...]


def stable_key(article: dict[str, Any], seed: int, label: str) -> tuple[float, str]:
    article_id = str(article.get("id") or "")
    rng = random.Random(f"{seed}:{label}:{article_id}")
    return (rng.random(), article_id)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def load_articles(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text())
    articles = payload.get("articles")
    if not isinstance(articles, list):
        raise ValueError(f"{path} does not contain a list at 'articles'")
    return [article for article in articles if isinstance(article, dict) and (article.get("title") or "").strip()]


def source_key(article: dict[str, Any]) -> str:
    return str(article.get("source") or "Unknown")


def category_family(article: dict[str, Any]) -> str:
    category = str(article.get("category") or "General")
    if category.startswith("Oil - "):
        return "oil"
    if category in {"Natural Gas", "LNG"}:
        return "gas_lng"
    if category in {"Chemicals", "Fertilizers"}:
        return "chem_fert"
    if category == "Electric Power":
        return "power"
    if category == "Energy Transition":
        return "energy_transition"
    if category == "Metals":
        return "metals"
    if category == "Agriculture":
        return "agriculture"
    if category == "Shipping":
        return "shipping"
    if category == "Coal":
        return "coal"
    return "general"


def compute_tags(article: dict[str, Any]) -> set[str]:
    title = str(article.get("title") or "")
    description = str(article.get("description") or "")
    text = f"{title} {description}".strip()
    predicted = classify_categories(title, description, max_categories=None)

    tags: set[str] = {f"source:{source_key(article)}", f"family:{category_family(article)}"}
    if article.get("category") == "General":
        tags.add(GENERAL_ONLY)
    if len(predicted) >= 2 or len(article.get("categories") or []) >= 2:
        tags.add(DUAL_POTENTIAL)
    if PLANT_STATUS_RE.search(text):
        tags.add(PLANT_STATUS)
    if GEOPOLITICAL_RE.search(text):
        tags.add(GEOPOLITICAL)
    if MARKET_SNAPSHOT_RE.search(text):
        tags.add(MARKET_SNAPSHOT)
    if SHIPPING_RE.search(text):
        tags.add(SHIPPING)
    if AMBIGUOUS_ENERGY_RE.search(text) and len(predicted) <= 1:
        tags.add(AMBIGUOUS_ENERGY)
    if CHEM_FERT_RE.search(text):
        tags.add(CHEM_FERT)
    if GAS_LNG_RE.search(text):
        tags.add(GAS_LNG)
    if CRUDE_REFINED_RE.search(text):
        tags.add(CRUDE_REFINED)
    return tags


def _has_tag(tag: str) -> Callable[[dict[str, Any], set[str]], bool]:
    return lambda article, tags: tag in tags


def _has_any(*tags_needed: str) -> Callable[[dict[str, Any], set[str]], bool]:
    return lambda article, tags: any(tag in tags for tag in tags_needed)


def _source_is(source: str) -> Callable[[dict[str, Any], set[str]], bool]:
    return lambda article, tags: source_key(article) == source


STRATA: list[tuple[str, int, Callable[[dict[str, Any], set[str]], bool]]] = [
    ("ICIS_general", 10, lambda a, tags: source_key(a) == "ICIS" and GENERAL_ONLY in tags),
    ("ICIS_dual_potential", 8, lambda a, tags: source_key(a) == "ICIS" and DUAL_POTENTIAL in tags),
    ("ICIS_plant_status", 8, lambda a, tags: source_key(a) == "ICIS" and PLANT_STATUS in tags),
    (
        "ICIS_geopolitical_energy",
        8,
        lambda a, tags: source_key(a) == "ICIS" and (GEOPOLITICAL in tags or AMBIGUOUS_ENERGY in tags),
    ),
    ("ICIS_market_snapshot", 6, lambda a, tags: source_key(a) == "ICIS" and MARKET_SNAPSHOT in tags),
    ("Argus_general", 18, lambda a, tags: source_key(a) == "Argus Media" and GENERAL_ONLY in tags),
    ("Argus_shipping_freight", 6, lambda a, tags: source_key(a) == "Argus Media" and SHIPPING in tags),
    ("Argus_geopolitical", 8, lambda a, tags: source_key(a) == "Argus Media" and GEOPOLITICAL in tags),
    ("Argus_market_snapshot", 8, lambda a, tags: source_key(a) == "Argus Media" and MARKET_SNAPSHOT in tags),
    (
        "Argus_commodity_ambiguity",
        8,
        lambda a, tags: source_key(a) == "Argus Media" and any(tag in tags for tag in (CHEM_FERT, GAS_LNG, CRUDE_REFINED)),
    ),
    ("SP_dual_potential", 8, lambda a, tags: source_key(a) == "S&P Global" and DUAL_POTENTIAL in tags),
    ("SP_general", 4, lambda a, tags: source_key(a) == "S&P Global" and GENERAL_ONLY in tags),
    (
        "SP_shipping_or_transition",
        6,
        lambda a, tags: source_key(a) == "S&P Global" and (SHIPPING in tags or "family:energy_transition" in tags),
    ),
    (
        "SP_commodity_ambiguity",
        8,
        lambda a, tags: source_key(a) == "S&P Global" and any(tag in tags for tag in (CHEM_FERT, GAS_LNG, CRUDE_REFINED)),
    ),
    ("Fastmarkets_general", 2, lambda a, tags: source_key(a) == "Fastmarkets" and GENERAL_ONLY in tags),
    ("Fastmarkets_other", 4, lambda a, tags: source_key(a) == "Fastmarkets"),
]


SOURCE_TARGETS: dict[str, int] = {
    "ICIS": 40,
    "Argus Media": 48,
    "S&P Global": 26,
    "Fastmarkets": 6,
}


def pick_rows(articles: list[dict[str, Any]], seed: int, sample_size: int) -> list[SampleRow]:
    article_tags = {str(article.get("id")): compute_tags(article) for article in articles}
    selected_ids: set[str] = set()
    selected_title_keys: set[tuple[str, str]] = set()
    rows: list[SampleRow] = []

    def eligible_for(label: str, predicate: Callable[[dict[str, Any], set[str]], bool]) -> list[dict[str, Any]]:
        eligible = []
        for article in articles:
            article_id = str(article.get("id"))
            title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
            tags = article_tags[article_id]
            if article_id in selected_ids or title_key in selected_title_keys:
                continue
            if predicate(article, tags):
                eligible.append(article)
        eligible.sort(key=lambda article: stable_key(article, seed, label))
        return eligible

    for label, quota, predicate in STRATA:
        for article in eligible_for(label, predicate)[:quota]:
            article_id = str(article.get("id"))
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
    filler_candidates = []
    for article in articles:
        article_id = str(article.get("id"))
        title_key = (normalize_text(str(article.get("title") or "")), source_key(article))
        if article_id in selected_ids or title_key in selected_title_keys:
            continue
        filler_candidates.append(article)
    filler_candidates.sort(key=lambda article: stable_key(article, seed, filler_label))

    for article in filler_candidates:
        source = source_key(article)
        if remaining_by_source.get(source, 0) <= 0:
            continue
        article_id = str(article.get("id"))
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


def build_record(row: SampleRow) -> dict[str, Any]:
    article = row.article
    return {
        "id": article.get("id"),
        "source": article.get("source"),
        "feed": article.get("feed"),
        "published": article.get("published"),
        "title": article.get("title"),
        "description": article.get("description"),
        "current_category": article.get("category"),
        "current_categories": article.get("categories"),
        "sample": {
            "primary_stratum": row.primary_stratum,
            "tags": list(row.tags),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    articles = load_articles(args.input)
    rows = pick_rows(articles, seed=args.seed, sample_size=args.sample_size)

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
