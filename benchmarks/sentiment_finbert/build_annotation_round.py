#!/usr/bin/env python3
"""Build a larger scaffold for a new sentiment annotation round."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from build_sample import (
    DEFAULT_INPUT,
    DEFAULT_SEED,
    build_record,
    load_articles,
    pick_rows,
    quotas_for_size,
)


DEFAULT_OUTPUT = Path("benchmarks/sentiment_finbert/annotation_round_scaffold.jsonl")
DEFAULT_REVIEW_OUTPUT = Path("benchmarks/sentiment_finbert/annotation_round_review.md")
DEFAULT_SAMPLE_SIZE = 240
DEFAULT_EXCLUDE = Path("benchmarks/sentiment_finbert/gold.jsonl")


def load_excluded_ids(paths: list[Path]) -> set[str]:
    excluded: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        if path.suffix == ".jsonl":
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if isinstance(row, dict) and row.get("id") is not None:
                        excluded.add(str(row["id"]))
            continue
        data = json.loads(path.read_text())
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("id") is not None:
                    excluded.add(str(item["id"]))
                elif item is not None:
                    excluded.add(str(item))
            continue
        if isinstance(data, dict):
            ids = data.get("ids")
            if isinstance(ids, list):
                excluded.update(str(item) for item in ids)
                continue
        raise ValueError(f"Unsupported exclude file format: {path}")
    return excluded


def render_review_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Sentiment Annotation Round",
        "",
        "Review each item with the in-repo rubric. Leave labels blank until annotated.",
        "",
    ]
    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"## {index}. {row['id']}",
                "",
                f"- Source: {row.get('source') or ''}",
                f"- Feed: {row.get('feed') or ''}",
                f"- Category: {row.get('category') or ''}",
                f"- Published: {row.get('published') or ''}",
                f"- Primary stratum: {row.get('sample', {}).get('primary_stratum', '')}",
                f"- Tags: {', '.join(row.get('sample', {}).get('tags', []))}",
                "",
                f"Title: {row.get('title') or ''}",
                "",
                f"Description: {row.get('description') or ''}",
                "",
                "Title label: ",
                "Title rationale: ",
                "Title ambiguity: ",
                "",
                "Title+description label: ",
                "Title+description rationale: ",
                "Title+description ambiguity: ",
                "",
                "Notes: ",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED + 1)
    parser.add_argument(
        "--exclude",
        type=Path,
        action="append",
        default=[DEFAULT_EXCLUDE],
        help="JSON/JSONL file whose ids should be excluded. Can be passed multiple times.",
    )
    parser.add_argument(
        "--review-output",
        type=Path,
        default=DEFAULT_REVIEW_OUTPUT,
        help="Markdown review packet to generate alongside the JSONL scaffold.",
    )
    args = parser.parse_args()

    excluded_ids = load_excluded_ids(args.exclude)
    articles = [
        article
        for article in load_articles(args.input)
        if str(article.get("id")) not in excluded_ids
    ]
    quotas = quotas_for_size(args.sample_size)
    rows = pick_rows(articles, quotas, args.seed)
    if len(rows) != args.sample_size:
        raise ValueError(
            f"Requested {args.sample_size} rows but only found {len(rows)} eligible rows after exclusions."
        )

    records: list[dict[str, Any]] = []
    for row in rows:
        record = build_record(row)
        record["annotation_scaffold"] = {
            "title": {"label": "", "rationale": "", "ambiguity": ""},
            "title_description": {"label": "", "rationale": "", "ambiguity": ""},
            "notes": "",
        }
        records.append(record)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    print(f"Wrote {args.output}")

    if args.review_output:
        args.review_output.write_text(render_review_markdown(records), encoding="utf-8")
        print(f"Wrote {args.review_output}")


if __name__ == "__main__":
    main()
