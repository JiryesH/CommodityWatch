#!/usr/bin/env python3
"""Build a reproducible stratified NER evaluation sample from data/feed.json."""

from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pycountry


DEFAULT_INPUT = Path("data/feed.json")
DEFAULT_OUTPUT = Path("benchmarks/ner_spacy/sample_scaffold.jsonl")
DEFAULT_SAMPLE_SIZE = 84
DEFAULT_SEED = 20260310

ABBREV_PATTERNS = {
    "US": re.compile(r"\bU\.?S\.?\b|\bUSA\b"),
    "UK": re.compile(r"\bU\.?K\.?\b"),
    "UAE": re.compile(r"\bU\.?A\.?E\.?\b"),
}

COMPANY_RE = re.compile(
    r"\b("
    r"adnoc|aramco|sinopec|petrobras|shell|chevron|exxon|ineos|basf|"
    r"orlen|google|meta|microsoft|equinor|wintershall|ongc|yara|"
    r"nextera|storengy|tata|formosa|cnooc|eneos|sabic|orica|bp"
    r")\b",
    re.IGNORECASE,
)

GEOPOLITICAL_RE = re.compile(
    r"\b("
    r"war|conflict|sanction|sanctions|officials|government|govt|"
    r"ministry|military|tariff|embargo|attack|attacks|drone|drones|"
    r"commission|parliament|trump|biden|opec|ceasefire|retaliation|"
    r"blockade|ukmto"
    r")\b",
    re.IGNORECASE,
)

COMMODITY_RE = re.compile(
    r"\b("
    r"crude|oil|diesel|naphtha|benzene|ethylene|propane|lng|gas|coal|"
    r"copper|iron ore|refinery|refineries|petchem|styrene|polyethylene|"
    r"jet fuel|fuel oil|methanol|ammonia|urea|pta|px|meg|mtbe|pvc|vcm|"
    r"lpg|gasoil|lsfo|naptha"
    r")\b",
    re.IGNORECASE,
)

REGION_RE = re.compile(
    r"\b("
    r"middle east|mideast|asia|europe|africa|americas|north sea|"
    r"gulf|hormuz|eu|european|asian|american"
    r")\b",
    re.IGNORECASE,
)


ALIASES = {
    "u.s": "United States",
    "us": "United States",
    "usa": "United States",
    "u.a.e": "United Arab Emirates",
    "uae": "United Arab Emirates",
    "uk": "United Kingdom",
    "britain": "United Kingdom",
    "russia": "Russia",
    "south korea": "South Korea",
    "north korea": "North Korea",
}

CITY_TO_COUNTRY = {
    "abu dhabi": "United Arab Emirates",
    "dubai": "United Arab Emirates",
    "fujairah": "United Arab Emirates",
    "ruwais": "United Arab Emirates",
    "yanbu": "Saudi Arabia",
    "singapore": "Singapore",
    "london": "United Kingdom",
}


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def build_country_patterns() -> list[tuple[re.Pattern[str], str]]:
    names: dict[str, str] = {}
    for country in pycountry.countries:
        canonical = str(country.name)
        display = canonical
        if canonical == "Russian Federation":
            display = "Russia"
        elif canonical == "Korea, Republic of":
            display = "South Korea"
        elif canonical == "Korea, Democratic People's Republic of":
            display = "North Korea"
        elif canonical == "Iran, Islamic Republic of":
            display = "Iran"
        elif canonical == "Viet Nam":
            display = "Vietnam"
        elif canonical == "Lao People's Democratic Republic":
            display = "Laos"
        elif canonical == "Taiwan, Province of China":
            display = "Taiwan"
        names[_normalize(canonical)] = display
        for attr in ("official_name", "common_name"):
            value = getattr(country, attr, None)
            if value:
                names[_normalize(str(value))] = display

    names.update({key: value for key, value in ALIASES.items()})
    names.update({key: value for key, value in CITY_TO_COUNTRY.items()})
    keys = sorted(names.keys(), key=len, reverse=True)
    patterns: list[tuple[re.Pattern[str], str]] = []
    for key in keys:
        patterns.append((re.compile(rf"\b{re.escape(key)}\b", re.IGNORECASE), names[key]))
    return patterns


COUNTRY_PATTERNS = build_country_patterns()


def extract_country_mentions(text: str) -> list[str]:
    normalized_text = _normalize(text)
    seen: set[str] = set()
    out: list[str] = []
    for pattern, country in COUNTRY_PATTERNS:
        if pattern.search(normalized_text) and country not in seen:
            seen.add(country)
            out.append(country)
    return out


@dataclass(frozen=True)
class SampleRow:
    article: dict[str, Any]
    primary_stratum: str
    tags: tuple[str, ...]


def stable_key(article: dict[str, Any], seed: int, label: str) -> tuple[float, str]:
    article_id = str(article.get("id") or "")
    rng = random.Random(f"{seed}:{label}:{article_id}")
    return (rng.random(), article_id)


def load_articles(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text())
    articles = data.get("articles")
    if not isinstance(articles, list):
        raise ValueError(f"{path} does not contain a list at 'articles'")
    return [a for a in articles if isinstance(a, dict) and (a.get("title") or "").strip()]


def compute_tags(article: dict[str, Any]) -> set[str]:
    title = str(article.get("title") or "")
    description = str(article.get("description") or "")
    title_countries = extract_country_mentions(title)
    desc_countries = extract_country_mentions(description)
    combined_countries = extract_country_mentions(f"{title}. {description}")

    tags: set[str] = set()
    if title_countries:
        tags.add("obvious_country_title")
    if any(pattern.search(title) for pattern in ABBREV_PATTERNS.values()):
        tags.add("abbreviation_title")
    if COMPANY_RE.search(title):
        tags.add("company_heavy")
    if GEOPOLITICAL_RE.search(title):
        tags.add("geopolitical")
    if COMMODITY_RE.search(title) and not title_countries and not REGION_RE.search(title):
        tags.add("commodity_weak_geo")
    if desc_countries and set(desc_countries) != set(title_countries):
        tags.add("description_geo_only_or_disagree")
    if description and combined_countries and not title_countries:
        tags.add("description_geo_only_or_disagree")
    return tags


def pick_rows(
    articles: list[dict[str, Any]],
    quotas: list[tuple[str, int]],
    seed: int,
) -> list[SampleRow]:
    selected_ids: set[str] = set()
    selected_titles: set[tuple[str, str]] = set()
    rows: list[SampleRow] = []

    article_tags = {str(a.get("id")): compute_tags(a) for a in articles}

    for label, quota in quotas:
        eligible = [
            a
            for a in articles
            if label in article_tags[str(a.get("id"))]
            and str(a.get("id")) not in selected_ids
            and (_normalize(str(a.get("title") or "")), str(a.get("source") or "")) not in selected_titles
        ]
        eligible.sort(key=lambda a: stable_key(a, seed, label))
        for article in eligible[:quota]:
            article_id = str(article.get("id"))
            title_key = (_normalize(str(article.get("title") or "")), str(article.get("source") or ""))
            selected_ids.add(article_id)
            selected_titles.add(title_key)
            rows.append(
                SampleRow(
                    article=article,
                    primary_stratum=label,
                    tags=tuple(sorted(article_tags[article_id])),
                )
            )

    return rows


def quotas_for_size(sample_size: int) -> list[tuple[str, int]]:
    base = [
        ("obvious_country_title", 16),
        ("abbreviation_title", 12),
        ("company_heavy", 12),
        ("geopolitical", 12),
        ("commodity_weak_geo", 16),
        ("description_geo_only_or_disagree", 16),
    ]
    total = sum(count for _, count in base)
    if sample_size == total:
        return base
    scaled: list[tuple[str, int]] = []
    running = 0
    for idx, (label, count) in enumerate(base):
        if idx == len(base) - 1:
            scaled_count = sample_size - running
        else:
            scaled_count = round(sample_size * count / total)
            running += scaled_count
        scaled.append((label, scaled_count))
    return scaled


def build_record(row: SampleRow) -> dict[str, Any]:
    article = row.article
    return {
        "id": article.get("id"),
        "source": article.get("source"),
        "feed": article.get("feed"),
        "published": article.get("published"),
        "title": article.get("title"),
        "description": article.get("description"),
        "sample": {
            "primary_stratum": row.primary_stratum,
            "tags": list(row.tags),
        },
        "gold": {
            "title": {
                "countries": [],
                "entities": [],
                "score_countries": True,
                "score_entities": True,
            },
            "title_description": {
                "countries": [],
                "entities": [],
                "score_countries": True,
                "score_entities": True,
            },
        },
        "notes": [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    articles = load_articles(args.input)
    rows = pick_rows(articles, quotas_for_size(args.sample_size), args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(build_record(row), ensure_ascii=False) + "\n")

    counts = Counter(row.primary_stratum for row in rows)
    print(f"Wrote {len(rows)} sampled articles to {args.output}")
    for key, value in counts.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
