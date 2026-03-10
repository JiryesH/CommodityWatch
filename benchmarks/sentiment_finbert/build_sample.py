#!/usr/bin/env python3
"""Build a reproducible stratified sentiment benchmark sample from data/feed.json."""

from __future__ import annotations

import argparse
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path("data/feed.json")
DEFAULT_OUTPUT = Path("benchmarks/sentiment_finbert/sample_scaffold.jsonl")
DEFAULT_SAMPLE_SIZE = 112
DEFAULT_SEED = 20260310


BULLISH_RE = re.compile(
    r"\b("
    r"boosts?|raises?|higher|gain|gains|surges?|rall(?:y|ies)|"
    r"tight(?:er|ens?|ness)|support(?:ive)?|strong(?:er)?|rebound|"
    r"improv(?:e|es|ing)|deficit|shortage|scarcity|uptrend|upside"
    r")\b",
    re.IGNORECASE,
)

BEARISH_RE = re.compile(
    r"\b("
    r"cuts?|lower(?:s|ed|ing)?|falls?|drops?|weak(?:er|ens?)|"
    r"oversupply|surplus|glut|slump|decline|pressure|bearish|"
    r"slowdown|hangover|loss(?:es)?|soft(?:er|ens?)"
    r")\b",
    re.IGNORECASE,
)

NEUTRAL_RE = re.compile(
    r"\b("
    r"plans?|sees?|expects?|says?|launches?|starts?|opens?|"
    r"considers?|outlook|report|reports?|snapshot|summary|"
    r"update|weekly|daily|databank|roundup|wrap|analysis"
    r")\b",
    re.IGNORECASE,
)

GEOPOLITICAL_RE = re.compile(
    r"\b("
    r"war|attack|attacks|drone|drones|missile|missiles|sanction|"
    r"sanctions|tariff|tariffs|conflict|military|retaliation|"
    r"embargo|hormuz|houthi|houthis|israel|iran|russia|ukraine|"
    r"shipping lane|strait|ceasefire|opec\+?"
    r")\b",
    re.IGNORECASE,
)

OUTAGE_RE = re.compile(
    r"\b("
    r"outage|shutdown|shut|restart|maintenance|force majeure|"
    r"fire|blast|explosion|offline|halt|halts|disruption|"
    r"disrupted|strike|plant status|refinery|cracker|turnaround|"
    r"operating rates|delays .* restart"
    r")\b",
    re.IGNORECASE,
)

MACRO_RE = re.compile(
    r"\b("
    r"eia|iea|fed|ecb|inflation|gdp|interest rates?|policy|"
    r"regulation|tax|subsid(?:y|ies)|export curb|export rebate|"
    r"government|commission|parliament|minister|epa|cbam|quota|"
    r"mandate|legislation|tariff|tariffs|opec\+?"
    r")\b",
    re.IGNORECASE,
)

GENERIC_TITLE_RE = re.compile(
    r"\b("
    r"snapshot|summary|roundup|wrap|update|databank|weekly|"
    r"daily|market watch|factbox|insight"
    r")\b",
    re.IGNORECASE,
)

DESCRIPTION_DIRECTION_RE = re.compile(
    r"\b("
    r"higher|lower|up|down|tight|weak|support|pressure|disruption|"
    r"supply|demand|bullish|bearish|deficit|surplus|outage|shutdown|"
    r"war|conflict|sanction|tariff|inventory|shortage|glut"
    r")\b",
    re.IGNORECASE,
)

AMBIGUOUS_RE = re.compile(
    r"\b("
    r"could|may|might|uncertain|uncertainty|mixed|volatile|"
    r"likely|watch|monitor|rebalanced|rebalancing|offset|"
    r"despite|amid|weighs?|caps?|limits?"
    r")\b",
    re.IGNORECASE,
)

PRICE_DIRECTION_RE = re.compile(
    r"\b("
    r"prices?|margins?|spreads?|imports?|exports?|output|"
    r"loadings?|stocks?|inventories?|costs?"
    r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SampleRow:
    article: dict[str, Any]
    primary_stratum: str
    tags: tuple[str, ...]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def stable_key(article: dict[str, Any], seed: int, label: str) -> tuple[float, str]:
    article_id = str(article.get("id") or "")
    rng = random.Random(f"{seed}:{label}:{article_id}")
    return (rng.random(), article_id)


def load_articles(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text())
    articles = data.get("articles")
    if not isinstance(articles, list):
        raise ValueError(f"{path} does not contain a list at 'articles'")
    return [a for a in articles if isinstance(a, dict) and normalize_text(str(a.get("title") or ""))]


def compute_tags(article: dict[str, Any]) -> set[str]:
    title = normalize_text(str(article.get("title") or ""))
    description = normalize_text(str(article.get("description") or ""))
    tags: set[str] = set()

    if BULLISH_RE.search(title) and not BEARISH_RE.search(title):
        tags.add("bullish")
    if BEARISH_RE.search(title) and not BULLISH_RE.search(title):
        tags.add("bearish")
    if NEUTRAL_RE.search(title) and not (BULLISH_RE.search(title) or BEARISH_RE.search(title)):
        tags.add("neutral")
    if GEOPOLITICAL_RE.search(title) or GEOPOLITICAL_RE.search(description):
        tags.add("geopolitical")
    if OUTAGE_RE.search(title) or OUTAGE_RE.search(description):
        tags.add("outage")
    if MACRO_RE.search(title) or MACRO_RE.search(description):
        tags.add("macro")
    if description and (
        (GENERIC_TITLE_RE.search(title) and DESCRIPTION_DIRECTION_RE.search(description))
        or (not DESCRIPTION_DIRECTION_RE.search(title) and DESCRIPTION_DIRECTION_RE.search(description))
    ):
        tags.add("description_needed")
    if AMBIGUOUS_RE.search(title) or (
        PRICE_DIRECTION_RE.search(title)
        and not (
            BULLISH_RE.search(title)
            or BEARISH_RE.search(title)
            or GEOPOLITICAL_RE.search(title)
            or OUTAGE_RE.search(title)
        )
    ):
        tags.add("ambiguous")
    return tags


def quotas_for_size(sample_size: int) -> list[tuple[str, int]]:
    strata = [
        "bullish",
        "bearish",
        "neutral",
        "geopolitical",
        "outage",
        "macro",
        "description_needed",
        "ambiguous",
    ]
    base = sample_size // len(strata)
    remainder = sample_size % len(strata)
    quotas: list[tuple[str, int]] = []
    for index, label in enumerate(strata):
        quotas.append((label, base + (1 if index < remainder else 0)))
    return quotas


def pick_rows(
    articles: list[dict[str, Any]],
    quotas: list[tuple[str, int]],
    seed: int,
) -> list[SampleRow]:
    article_tags = {str(article.get("id")): compute_tags(article) for article in articles}
    selected_ids: set[str] = set()
    selected_titles: set[tuple[str, str]] = set()
    rows: list[SampleRow] = []

    for label, quota in quotas:
        eligible = [
            article
            for article in articles
            if label in article_tags[str(article.get("id"))]
            and str(article.get("id")) not in selected_ids
            and (
                normalize_text(str(article.get("title") or "")).casefold(),
                str(article.get("source") or ""),
            ) not in selected_titles
        ]
        eligible.sort(key=lambda article: stable_key(article, seed, label))
        for article in eligible[:quota]:
            article_id = str(article.get("id"))
            title_key = (
                normalize_text(str(article.get("title") or "")).casefold(),
                str(article.get("source") or ""),
            )
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


def build_record(row: SampleRow) -> dict[str, Any]:
    article = row.article
    return {
        "id": article.get("id"),
        "source": article.get("source"),
        "feed": article.get("feed"),
        "category": article.get("category"),
        "published": article.get("published"),
        "title": normalize_text(str(article.get("title") or "")),
        "description": normalize_text(str(article.get("description") or "")),
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
    quotas = quotas_for_size(args.sample_size)
    rows = pick_rows(articles, quotas, args.seed)
    if len(rows) != args.sample_size:
        raise RuntimeError(
            f"Requested {args.sample_size} rows but only selected {len(rows)}. "
            "Relax the strata rules or reduce the sample size."
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(build_record(row), ensure_ascii=True) + "\n")

    quota_text = ", ".join(f"{label}={count}" for label, count in quotas)
    print(f"Wrote {args.output} with {len(rows)} rows ({quota_text})")


if __name__ == "__main__":
    main()
