#!/usr/bin/env python3
"""Shared scoring helpers for the sentiment benchmark scripts."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Optional


LABELS = ("positive", "negative", "neutral")
BUCKETS = (
    (0.0, 0.5),
    (0.5, 0.6),
    (0.6, 0.7),
    (0.7, 0.8),
    (0.8, 0.9),
    (0.9, 1.01),
)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def metric_dict(tp: int, fp: int, fn: int, support: int) -> dict[str, Any]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "support": support,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def bucket_for(confidence: float) -> str:
    for lower, upper in BUCKETS:
        if lower <= confidence < upper:
            return f"{lower:.1f}-{min(upper, 1.0):.1f}"
    return "unknown"


def expected_probability(row: dict[str, Any], expected_label: str) -> float:
    probs = row.get("probabilities") or {}
    value = probs.get(expected_label)
    return float(value) if isinstance(value, (int, float)) else 0.0


def load_subset_ids(path: Optional[Path]) -> Optional[set[str]]:
    if path is None:
        return None
    if path.suffix == ".jsonl":
        return {
            str(row.get("id"))
            for row in load_jsonl(path)
            if isinstance(row, dict) and row.get("id") is not None
        }
    data = load_json(path)
    if isinstance(data, dict):
        ids = data.get("ids")
        if isinstance(ids, list):
            return {str(item) for item in ids}
    if isinstance(data, list):
        out: set[str] = set()
        for item in data:
            if isinstance(item, dict) and item.get("id") is not None:
                out.add(str(item["id"]))
            elif item is not None:
                out.add(str(item))
        return out
    raise ValueError(f"Unsupported subset format: {path}")


def gold_slice_for_mode(
    row: dict[str, Any],
    *,
    requested_mode: str,
    sentiment: dict[str, Any],
) -> tuple[str, str]:
    if requested_mode == "title":
        actual_input_mode = str(sentiment.get("input_mode") or "")
        if actual_input_mode != "title":
            raise ValueError(
                f"Article {row.get('id')} has input_mode={actual_input_mode!r}, expected 'title'"
            )
        return "title", "title"

    if requested_mode == "title_description":
        actual_input_mode = str(sentiment.get("input_mode") or "")
        if actual_input_mode != "title+description":
            raise ValueError(
                "Article "
                f"{row.get('id')} has input_mode={actual_input_mode!r}, expected 'title+description'"
            )
        return "title_description", "title+description"

    if requested_mode == "auto":
        actual_input_mode = str(sentiment.get("input_mode") or "")
        if actual_input_mode == "title":
            return "title", actual_input_mode
        if actual_input_mode == "title+description":
            return "title_description", actual_input_mode
        raise ValueError(
            f"Article {row.get('id')} has unsupported input_mode {actual_input_mode!r} for auto scoring"
        )

    raise ValueError(f"Unsupported scoring mode: {requested_mode}")


def score_predictions(
    *,
    gold_rows: list[dict[str, Any]],
    prediction_data: dict[str, Any],
    mode: str,
    subset_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    prediction_articles = {
        str(article.get("id")): article
        for article in prediction_data.get("articles", [])
        if isinstance(article, dict) and article.get("id") is not None
    }

    confusion = {label: Counter() for label in LABELS}
    gold_counts: Counter[str] = Counter()
    pred_counts: Counter[str] = Counter()
    per_class_tp = Counter()
    per_class_fp = Counter()
    per_class_fn = Counter()
    neutral_baseline_correct = 0
    correct = 0
    records: list[dict[str, Any]] = []

    calibration: dict[str, list[dict[str, float]]] = {}
    for lower, upper in BUCKETS:
        calibration[f"{lower:.1f}-{min(upper, 1.0):.1f}"] = []

    for row in gold_rows:
        article_id = str(row.get("id"))
        if subset_ids is not None and article_id not in subset_ids:
            continue

        predicted_article = prediction_articles.get(article_id)
        if predicted_article is None:
            raise KeyError(f"Missing predicted article for id {article_id}")

        sentiment = predicted_article.get("sentiment") or {}
        predicted_label = str(sentiment.get("label") or "")
        if predicted_label not in LABELS:
            raise ValueError(f"Article {article_id} missing valid sentiment label")

        gold_mode, actual_input_mode = gold_slice_for_mode(
            row,
            requested_mode=mode,
            sentiment=sentiment,
        )

        confidence = float(sentiment.get("confidence") or 0.0)
        expected = row["gold"][gold_mode]
        expected_label = str(expected["label"])
        if expected_label not in LABELS:
            raise ValueError(f"Invalid gold label {expected_label!r} for id {article_id}")

        gold_counts[expected_label] += 1
        pred_counts[predicted_label] += 1
        confusion[expected_label][predicted_label] += 1
        if expected_label == predicted_label:
            correct += 1
            per_class_tp[expected_label] += 1
        else:
            per_class_fp[predicted_label] += 1
            per_class_fn[expected_label] += 1

        if expected_label == "neutral":
            neutral_baseline_correct += 1

        bucket = bucket_for(confidence)
        calibration[bucket].append(
            {
                "confidence": confidence,
                "correct": 1.0 if expected_label == predicted_label else 0.0,
            }
        )

        records.append(
            {
                "id": article_id,
                "title": row.get("title"),
                "description": row.get("description"),
                "gold_mode_used": gold_mode,
                "predicted_input_mode": actual_input_mode,
                "expected_label": expected_label,
                "expected_rationale": expected.get("rationale"),
                "expected_ambiguity": bool(expected.get("ambiguity")),
                "predicted_label": predicted_label,
                "confidence": confidence,
                "probabilities": sentiment.get("probabilities") or {},
                "compound": sentiment.get("compound"),
                "pipeline_mode": sentiment.get("pipeline_mode"),
                "backend": sentiment.get("backend"),
                "sample": row.get("sample") or {},
                "correct": expected_label == predicted_label,
                "expected_probability": expected_probability(sentiment, expected_label),
            }
        )

    sample_size = len(records)
    per_class = {
        label: metric_dict(
            tp=per_class_tp[label],
            fp=per_class_fp[label],
            fn=per_class_fn[label],
            support=gold_counts[label],
        )
        for label in LABELS
    }
    macro_f1 = sum(per_class[label]["f1"] for label in LABELS) / len(LABELS)
    accuracy = correct / sample_size if sample_size else 0.0
    neutral_baseline_accuracy = neutral_baseline_correct / sample_size if sample_size else 0.0

    calibration_rows: list[dict[str, Any]] = []
    ece = 0.0
    for bucket, values in calibration.items():
        count = len(values)
        if not count:
            continue
        avg_conf = sum(item["confidence"] for item in values) / count
        avg_acc = sum(item["correct"] for item in values) / count
        ece += abs(avg_conf - avg_acc) * (count / sample_size)
        calibration_rows.append(
            {
                "bucket": bucket,
                "count": count,
                "avg_confidence": round(avg_conf, 4),
                "accuracy": round(avg_acc, 4),
            }
        )

    mismatches = [row for row in records if not row["correct"]]
    mismatches.sort(key=lambda row: (-row["confidence"], row["id"]))

    return {
        "mode": mode,
        "sample_size": sample_size,
        "accuracy": round(accuracy, 4),
        "neutral_baseline_accuracy": round(neutral_baseline_accuracy, 4),
        "macro_f1": round(macro_f1, 4),
        "gold_distribution": {label: gold_counts[label] for label in LABELS},
        "predicted_distribution": {label: pred_counts[label] for label in LABELS},
        "per_class": per_class,
        "confusion_matrix": {
            expected: {predicted: confusion[expected][predicted] for predicted in LABELS}
            for expected in LABELS
        },
        "confidence_calibration": {
            "ece": round(ece, 4),
            "buckets": calibration_rows,
        },
        "high_confidence_mistakes": [
            row for row in mismatches if row["confidence"] >= 0.8
        ][:20],
        "mismatches": mismatches,
        "records": records,
    }
