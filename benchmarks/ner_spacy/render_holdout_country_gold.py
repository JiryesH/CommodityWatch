#!/usr/bin/env python3
"""Render the holdout country annotation source into a gold JSONL file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from holdout_country_annotations import HOLDOUT_COUNTRY_ANNOTATIONS


DEFAULT_SAMPLE = Path("benchmarks/ner_spacy/holdout_sample_scaffold.jsonl")
DEFAULT_OUTPUT = Path("benchmarks/ner_spacy/holdout_country_gold.jsonl")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = [
        json.loads(line)
        for line in args.sample.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    sample_ids = {str(row.get("id")) for row in rows}
    annotation_ids = set(HOLDOUT_COUNTRY_ANNOTATIONS)
    missing = sorted(sample_ids - annotation_ids)
    extra = sorted(annotation_ids - sample_ids)
    if missing or extra:
        raise SystemExit(
            f"Annotation coverage mismatch. Missing={missing} Extra={extra}"
        )

    rendered = []
    for row in rows:
        article_id = str(row["id"])
        annotation = HOLDOUT_COUNTRY_ANNOTATIONS[article_id]
        row["gold"] = {
            "title": annotation["title"],
            "title_description": annotation["title_description"],
        }
        row["notes"] = annotation["notes"]
        rendered.append(row)

    args.output.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rendered),
        encoding="utf-8",
    )
    print(f"Wrote {len(rendered)} gold rows to {args.output}")


if __name__ == "__main__":
    main()
