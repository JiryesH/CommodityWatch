#!/usr/bin/env python3
"""Compare two benchmark score outputs side by side."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from score_common import LABELS, load_json


def delta(baseline: float, candidate: float) -> float:
    return round(candidate - baseline, 4)


def compare_metrics(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    baseline_records = {
        str(record["id"]): record
        for record in baseline.get("records", [])
        if isinstance(record, dict) and record.get("id") is not None
    }
    candidate_records = {
        str(record["id"]): record
        for record in candidate.get("records", [])
        if isinstance(record, dict) and record.get("id") is not None
    }

    if not baseline_records or not candidate_records:
        raise ValueError(
            "Both inputs must include a 'records' list. Re-run score_sample.py with the current version."
        )

    common_ids = sorted(set(baseline_records) & set(candidate_records))
    fixed_examples: list[dict[str, Any]] = []
    newly_broken_examples: list[dict[str, Any]] = []
    changed_predictions: list[dict[str, Any]] = []

    for article_id in common_ids:
        base = baseline_records[article_id]
        cand = candidate_records[article_id]
        if base["predicted_label"] != cand["predicted_label"] or base["correct"] != cand["correct"]:
            changed_predictions.append(
                {
                    "id": article_id,
                    "title": cand.get("title") or base.get("title"),
                    "expected_label": cand.get("expected_label") or base.get("expected_label"),
                    "baseline_label": base["predicted_label"],
                    "candidate_label": cand["predicted_label"],
                    "baseline_correct": base["correct"],
                    "candidate_correct": cand["correct"],
                    "baseline_input_mode": base.get("predicted_input_mode"),
                    "candidate_input_mode": cand.get("predicted_input_mode"),
                }
            )
        if not base["correct"] and cand["correct"]:
            fixed_examples.append(changed_predictions[-1])
        if base["correct"] and not cand["correct"]:
            newly_broken_examples.append(changed_predictions[-1])

    per_class_delta = {
        label: {
            "precision": delta(
                float(baseline["per_class"][label]["precision"]),
                float(candidate["per_class"][label]["precision"]),
            ),
            "recall": delta(
                float(baseline["per_class"][label]["recall"]),
                float(candidate["per_class"][label]["recall"]),
            ),
            "f1": delta(
                float(baseline["per_class"][label]["f1"]),
                float(candidate["per_class"][label]["f1"]),
            ),
        }
        for label in LABELS
    }

    confusion_delta = {
        expected: {
            predicted: int(candidate["confusion_matrix"][expected][predicted])
            - int(baseline["confusion_matrix"][expected][predicted])
            for predicted in LABELS
        }
        for expected in LABELS
    }

    return {
        "baseline": {
            "mode": baseline.get("mode"),
            "sample_size": baseline.get("sample_size"),
            "accuracy": baseline.get("accuracy"),
            "macro_f1": baseline.get("macro_f1"),
        },
        "candidate": {
            "mode": candidate.get("mode"),
            "sample_size": candidate.get("sample_size"),
            "accuracy": candidate.get("accuracy"),
            "macro_f1": candidate.get("macro_f1"),
        },
        "deltas": {
            "accuracy": delta(float(baseline["accuracy"]), float(candidate["accuracy"])),
            "macro_f1": delta(float(baseline["macro_f1"]), float(candidate["macro_f1"])),
            "per_class": per_class_delta,
            "confusion_matrix": confusion_delta,
        },
        "fixed_examples": fixed_examples[:25],
        "newly_broken_examples": newly_broken_examples[:25],
        "changed_predictions": changed_predictions[:50],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = compare_metrics(
        baseline=load_json(args.baseline),
        candidate=load_json(args.candidate),
    )

    output_text = json.dumps(result, indent=2, ensure_ascii=True)
    if args.output:
        args.output.write_text(output_text + "\n", encoding="utf-8")
        print(f"Wrote {args.output}")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
