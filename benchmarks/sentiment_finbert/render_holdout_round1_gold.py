#!/usr/bin/env python3
"""Render the first unseen sentiment holdout gold JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from holdout_round1_annotations import ANNOTATIONS


DEFAULT_SAMPLE = Path("benchmarks/sentiment_finbert/holdout_round1_scaffold.jsonl")
DEFAULT_OUTPUT = Path("benchmarks/sentiment_finbert/holdout_round1_gold.jsonl")


def load_sample(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = load_sample(args.sample)
    rendered: list[dict] = []
    for row in rows:
        article_id = str(row.get("id"))
        annotation = ANNOTATIONS.get(article_id)
        if annotation is None:
            raise KeyError(f"Missing annotation for sampled article id {article_id}")
        rendered.append(
            {
                **row,
                "gold": {
                    "title": annotation["title"],
                    "title_description": annotation["title_description"],
                },
                "notes": annotation.get("notes", ""),
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rendered:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    print(f"Wrote {args.output} with {len(rendered)} rows")


if __name__ == "__main__":
    main()
