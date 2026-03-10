#!/usr/bin/env python3
"""Score sentiment predictions against the sentiment gold benchmark."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from score_common import load_json, load_jsonl, load_subset_ids, score_predictions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=["title", "title_description", "auto"],
        required=True,
        help="Gold slice to score against. Use 'auto' for selective-context runs.",
    )
    parser.add_argument(
        "--subset",
        type=Path,
        help="Optional subset of gold ids to score (JSON, JSONL, or {\"ids\": [...]})",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = score_predictions(
        gold_rows=load_jsonl(args.gold),
        prediction_data=load_json(args.predictions),
        mode=args.mode,
        subset_ids=load_subset_ids(args.subset),
    )

    output_text = json.dumps(result, indent=2, ensure_ascii=True)
    if args.output:
        args.output.write_text(output_text + "\n", encoding="utf-8")
        print(f"Wrote {args.output}")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
