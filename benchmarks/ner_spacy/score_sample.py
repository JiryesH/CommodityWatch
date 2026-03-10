#!/usr/bin/env python3
"""Score ner_spacy.py predictions against a gold JSONL benchmark."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_gold(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def normalize_text(value: str) -> str:
    value = (value or "").replace("’", "'").replace("`", "'")
    return re.sub(r"\s+", " ", value).strip()


def relaxed_text(value: str) -> str:
    value = normalize_text(value).casefold()
    value = re.sub(r"'s\b", "", value)
    value = re.sub(r"[^\w\s]+", "", value)
    value = re.sub(r"^(the|a|an)\s+", "", value)
    return re.sub(r"\s+", " ", value).strip()


def country_set(items: list[str]) -> set[str]:
    return {normalize_text(str(item)) for item in items if normalize_text(str(item))}


def entity_set(
    items: list[dict[str, Any]],
    *,
    with_label: bool,
    relaxed: bool,
) -> set[tuple[str, str] | str]:
    out: set[tuple[str, str] | str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "")
        label = str(item.get("label") or "")
        if not text:
            continue
        normalized = relaxed_text(text) if relaxed else normalize_text(text)
        if not normalized:
            continue
        if with_label:
            out.add((normalized, label))
        else:
            out.add(normalized)
    return out


def metric_dict(tp: int, fp: int, fn: int) -> dict[str, Any]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def classify_country_error(
    title_gold: set[str],
    gold: set[str],
    pred: set[str],
    article: dict[str, Any],
) -> list[tuple[str, str]]:
    examples: list[tuple[str, str]] = []
    title = article.get("title") or ""
    description = article.get("description") or ""
    pred_entities = article.get("pred_entities") or []
    pred_entity_texts = {normalize_text(str(entity.get("text") or "")) for entity in pred_entities}

    for missed in sorted(gold - pred):
        if missed in title_gold:
            category = "missed_country_present_in_title"
        elif missed and missed in description:
            category = "missed_country_only_in_description"
        else:
            category = "missed_country_contextual_or_demonym"
        examples.append((category, missed))

    for extra in sorted(pred - gold):
        category = "false_country_prediction"
        if extra in pred_entity_texts and extra not in title and extra not in description:
            category = "country_normalization_failure"
        examples.append((category, extra))

    return examples


def classify_entity_error(
    gold: set[tuple[str, str]],
    pred: set[tuple[str, str]],
    pred_text_only: set[str],
) -> list[tuple[str, str]]:
    examples: list[tuple[str, str]] = []
    gold_texts = {text for text, _ in gold}
    pred_texts = {text for text, _ in pred}

    for text, label in sorted(gold - pred):
        if text in pred_texts:
            examples.append(("entity_label_confusion", f"{text}|{label}"))
        else:
            examples.append(("missed_entity", f"{text}|{label}"))

    for text, label in sorted(pred - gold):
        if text in gold_texts:
            examples.append(("entity_label_confusion", f"{text}|{label}"))
        elif text in pred_text_only:
            examples.append(("low_value_or_extra_entity", f"{text}|{label}"))
        else:
            examples.append(("extra_entity", f"{text}|{label}"))

    return examples


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=["title", "title_description"],
        required=True,
        help="Gold slice to score against.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    gold_rows = load_gold(args.gold)
    prediction_data = load_json(args.predictions)
    predicted_articles = {
        str(article.get("id")): article
        for article in prediction_data.get("articles", [])
        if isinstance(article, dict) and article.get("id") is not None
    }

    country_tp = country_fp = country_fn = 0
    entity_text_tp = entity_text_fp = entity_text_fn = 0
    entity_label_tp = entity_label_fp = entity_label_fn = 0
    entity_relaxed_tp = entity_relaxed_fp = entity_relaxed_fn = 0

    country_error_counts: Counter[str] = Counter()
    entity_error_counts: Counter[str] = Counter()
    error_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    scored_articles = 0
    skipped_country = 0
    skipped_entity = 0

    for row in gold_rows:
        article_id = str(row.get("id"))
        predicted = predicted_articles.get(article_id)
        if predicted is None:
            raise KeyError(f"Missing predicted article for id {article_id}")

        gold_slice = row["gold"][args.mode]
        pred_ner = predicted.get("ner") or {}
        pred_countries = country_set(pred_ner.get("countries") or [])
        pred_entities_raw = pred_ner.get("entities") or []

        pred_entity_text = entity_set(pred_entities_raw, with_label=False, relaxed=False)
        pred_entity_label = entity_set(pred_entities_raw, with_label=True, relaxed=False)
        pred_entity_relaxed = entity_set(pred_entities_raw, with_label=False, relaxed=True)

        if gold_slice.get("score_countries", True):
            gold_countries = country_set(gold_slice.get("countries") or [])
            gold_title_countries = country_set(row["gold"]["title"].get("countries") or [])
            country_tp += len(pred_countries & gold_countries)
            country_fp += len(pred_countries - gold_countries)
            country_fn += len(gold_countries - pred_countries)
            for category, value in classify_country_error(
                gold_title_countries,
                gold_countries,
                pred_countries,
                {
                    "title": row.get("title"),
                    "description": row.get("description"),
                    "pred_entities": pred_entities_raw,
                },
            ):
                country_error_counts[category] += 1
                if len(error_examples[category]) < 8:
                    error_examples[category].append(
                        {
                            "id": article_id,
                            "title": row.get("title"),
                            "value": value,
                            "predicted_countries": sorted(pred_countries),
                            "expected_countries": sorted(gold_countries),
                        }
                    )
        else:
            skipped_country += 1

        if gold_slice.get("score_entities", True):
            scored_articles += 1
            gold_entities = gold_slice.get("entities") or []
            gold_entity_text = entity_set(gold_entities, with_label=False, relaxed=False)
            gold_entity_label = entity_set(gold_entities, with_label=True, relaxed=False)
            gold_entity_relaxed = entity_set(gold_entities, with_label=False, relaxed=True)

            entity_text_tp += len(pred_entity_text & gold_entity_text)
            entity_text_fp += len(pred_entity_text - gold_entity_text)
            entity_text_fn += len(gold_entity_text - pred_entity_text)

            entity_label_tp += len(pred_entity_label & gold_entity_label)
            entity_label_fp += len(pred_entity_label - gold_entity_label)
            entity_label_fn += len(gold_entity_label - pred_entity_label)

            entity_relaxed_tp += len(pred_entity_relaxed & gold_entity_relaxed)
            entity_relaxed_fp += len(pred_entity_relaxed - gold_entity_relaxed)
            entity_relaxed_fn += len(gold_entity_relaxed - pred_entity_relaxed)

            for category, value in classify_entity_error(
                gold_entity_label,
                pred_entity_label,
                pred_entity_text,
            ):
                entity_error_counts[category] += 1
                if len(error_examples[category]) < 8:
                    error_examples[category].append(
                        {
                            "id": article_id,
                            "title": row.get("title"),
                            "value": value,
                            "predicted_entities": sorted(list(pred_entity_label)),
                            "expected_entities": sorted(list(gold_entity_label)),
                        }
                    )
        else:
            skipped_entity += 1

    result = {
        "mode": args.mode,
        "sample_size": len(gold_rows),
        "scored_articles": scored_articles,
        "skipped_country_articles": skipped_country,
        "skipped_entity_articles": skipped_entity,
        "country_metrics": metric_dict(country_tp, country_fp, country_fn),
        "entity_metrics": {
            "text_only_exact": metric_dict(entity_text_tp, entity_text_fp, entity_text_fn),
            "text_plus_label_exact": metric_dict(entity_label_tp, entity_label_fp, entity_label_fn),
            "text_only_relaxed": metric_dict(entity_relaxed_tp, entity_relaxed_fp, entity_relaxed_fn),
        },
        "country_error_counts": dict(country_error_counts.most_common()),
        "entity_error_counts": dict(entity_error_counts.most_common()),
        "error_examples": error_examples,
    }

    if args.output:
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
