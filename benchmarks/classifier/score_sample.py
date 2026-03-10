#!/usr/bin/env python3
"""Score classifier.py against a manually reviewed category benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from classifier import (
    CANONICAL_CATEGORIES,
    classify_categories,
    iter_raw_category_tokens,
    merge_category_lists,
    normalize_article_categories,
    normalize_categories,
    normalize_category_token,
)


DEFAULT_GOLD = Path("benchmarks/classifier/gold.jsonl")
DEFAULT_OUTPUT = Path("benchmarks/classifier/metrics.json")

S_AND_P_FEED_CATEGORY: dict[str, str] = {
    "S&P Oil - Crude": "Oil - Crude",
    "S&P Oil - Refined Products": "Oil - Refined Products",
    "S&P Fertilizers": "Fertilizers",
    "S&P Electric Power": "Electric Power",
    "S&P Natural Gas": "Natural Gas",
    "S&P Coal": "Coal",
    "S&P Chemicals": "Chemicals",
    "S&P Metals": "Metals",
    "S&P Shipping": "Shipping",
    "S&P Agriculture": "Agriculture",
    "S&P LNG": "LNG",
    "S&P Energy Transition": "Energy Transition",
    "S&P Blog": "General",
}


def load_gold(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


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


def round4(value: float) -> float:
    return round(value, 4)


def family_for_primary(category: str) -> str:
    if category.startswith("Oil - "):
        return "oil"
    if category in {"Natural Gas", "LNG"}:
        return "gas_lng"
    if category == "Electric Power":
        return "power"
    if category in {"Chemicals", "Fertilizers"}:
        return "chemicals_fertilizers"
    if category == "Metals":
        return "metals"
    if category == "Agriculture":
        return "agriculture"
    if category == "Shipping":
        return "shipping"
    if category == "Energy Transition":
        return "energy_transition"
    if category == "Coal":
        return "coal"
    return "general_non_specific"


def gold_slice(row: dict[str, Any], mode: str) -> dict[str, Any]:
    return row["gold"][mode]


def reconstruct_source_payload(row: dict[str, Any]) -> dict[str, Any]:
    source = str(row.get("source") or "")
    feed = str(row.get("feed") or "")
    if source in {"ICIS", "Fastmarkets", "Argus Media"}:
        base_category = "General"
    else:
        base_category = S_AND_P_FEED_CATEGORY.get(feed, "General")
    return {
        "title": row.get("title") or "",
        "description": row.get("description") or "",
        "category": base_category,
        "categories": [base_category],
    }


def current_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": row.get("title") or "",
        "description": row.get("description") or "",
        "category": row.get("current_category"),
        "categories": list(row.get("current_categories") or []),
    }


def _fallback_unbounded_categories(article: dict[str, Any]) -> list[str]:
    merged = merge_category_lists(
        article.get("categories"),
        article.get("category"),
        max_categories=None,
    )
    informative = any(cat != "General" for cat in merged)
    if informative:
        return merged
    return classify_categories(
        str(article.get("title") or ""),
        str(article.get("description") or ""),
        max_categories=None,
    )


def predict_classifier(row: dict[str, Any], mode: str) -> dict[str, Any]:
    title = str(row.get("title") or "")
    description = str(row.get("description") or "") if mode == "title_description" else ""
    raw = classify_categories(title, description, max_categories=2)
    unbounded = classify_categories(title, description, max_categories=None)
    scored = raw or ["General"]
    return {
        "raw_categories": raw,
        "categories": scored,
        "primary": scored[0],
        "truncated_before_cap": len(unbounded) > 2,
        "no_match": not raw,
        "used_classifier": True,
        "unknown_tokens": [],
    }


def predict_normalized(
    row: dict[str, Any],
    payload_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    article = payload_builder(row)
    unbounded = _fallback_unbounded_categories(article)
    normalized_input, unknown_categories = normalize_categories(article.get("categories"), max_categories=None)
    normalized_primary, unknown_category = normalize_categories(article.get("category"), max_categories=None)
    result = normalize_article_categories(article, classify_fallback=True)
    categories = list(result["categories"])
    return {
        "raw_categories": categories,
        "categories": categories,
        "primary": categories[0],
        "truncated_before_cap": len(unbounded) > 2,
        "no_match": False,
        "used_classifier": bool(result.get("used_classifier")),
        "unknown_tokens": unknown_categories + unknown_category,
        "normalized_input_categories": normalized_input,
        "normalized_input_primary": normalized_primary,
    }


def token_contract_stats(
    rows: list[dict[str, Any]],
    payload_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    token_total = 0
    canonical_passthrough = 0
    legacy_rewrites = 0
    unknown_tokens: Counter[str] = Counter()
    rewrite_counts: Counter[str] = Counter()

    for row in rows:
        article = payload_builder(row)
        seen: set[str] = set()
        tokens = iter_raw_category_tokens(article.get("categories")) + iter_raw_category_tokens(article.get("category"))
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            token_total += 1
            normalized = normalize_category_token(token)
            if normalized is None:
                unknown_tokens[token] += 1
            elif normalized == token:
                canonical_passthrough += 1
            else:
                legacy_rewrites += 1
                rewrite_counts[f"{token} -> {normalized}"] += 1

    return {
        "token_total": token_total,
        "canonical_passthrough": canonical_passthrough,
        "legacy_rewrites": legacy_rewrites,
        "unknown_token_total": sum(unknown_tokens.values()),
        "unknown_tokens": dict(unknown_tokens.most_common()),
        "rewrite_counts": dict(rewrite_counts.most_common()),
    }


def error_bucket(
    gold_categories: list[str],
    pred_categories: list[str],
    gold_primary: str,
    pred_primary: str,
) -> str:
    gold_set = set(gold_categories)
    pred_set = set(pred_categories)
    if pred_set == {"General"} and gold_set != {"General"}:
        return "wrongly_classified_as_general"
    if gold_set == {"General"} and pred_set != {"General"}:
        return "should_have_been_general"
    if len(gold_categories) == 2 and len(pred_categories) == 1 and pred_set.issubset(gold_set):
        return "missed_valid_second_category"
    if len(gold_categories) == 2 and len(pred_categories) == 2 and pred_set != gold_set:
        return "wrong_dual_label_combination"
    if pred_set == gold_set and pred_primary != gold_primary:
        return "wrong_primary_category_order"
    if len(pred_categories) == 1 and len(gold_categories) == 1 and pred_primary != gold_primary:
        return "single_label_misclassification"
    return "other_misclassification"


def evaluate_rows(
    rows: list[dict[str, Any]],
    *,
    mode: str,
    predictor_name: str,
    predictor: Callable[[dict[str, Any], str], dict[str, Any]],
    contract_payload_builder: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    include_examples: bool = True,
) -> dict[str, Any]:
    per_category: dict[str, dict[str, int]] = {
        category: {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
        for category in CANONICAL_CATEGORIES
    }

    exact_match = 0
    primary_correct = 0
    dual_gold_rows = 0
    dual_pred_rows = 0
    dual_exact_rows = 0
    second_category_hits = 0
    no_match_rows = 0
    truncation_rows = 0
    used_classifier_rows = 0
    primary_order_rows = 0
    primary_order_correct = 0
    general_pred_rows = 0
    general_gold_rows = 0
    mismatch_counts: Counter[str] = Counter()
    mismatch_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        gold = gold_slice(row, mode)
        gold_categories = list(gold["categories"])
        gold_primary = str(gold["primary"])
        predicted = predictor(row, mode)
        pred_categories = list(predicted["categories"])
        pred_primary = str(predicted["primary"])

        gold_set = set(gold_categories)
        pred_set = set(pred_categories)

        if gold_set == pred_set:
            exact_match += 1
        else:
            bucket = error_bucket(gold_categories, pred_categories, gold_primary, pred_primary)
            mismatch_counts[bucket] += 1
            if include_examples and len(mismatch_examples[bucket]) < 12:
                mismatch_examples[bucket].append(
                    {
                        "id": row.get("id"),
                        "source": row.get("source"),
                        "feed": row.get("feed"),
                        "title": row.get("title"),
                        "description": row.get("description"),
                        "predicted_categories": pred_categories,
                        "expected_categories": gold_categories,
                        "predicted_primary": pred_primary,
                        "expected_primary": gold_primary,
                    }
                )

        if pred_primary == gold_primary:
            primary_correct += 1

        if len(gold_categories) == 2:
            dual_gold_rows += 1
            if len(pred_categories) == 2 and pred_set == gold_set:
                dual_exact_rows += 1
            if len(pred_categories) >= 2 and set(gold_categories[1:2]).issubset(pred_set):
                second_category_hits += 1
            elif gold_categories[1] in pred_set:
                second_category_hits += 1
        if len(pred_categories) == 2:
            dual_pred_rows += 1

        if pred_set == gold_set and len(gold_categories) == 2:
            primary_order_rows += 1
            if pred_primary == gold_primary:
                primary_order_correct += 1

        if pred_set == {"General"}:
            general_pred_rows += 1
        if gold_set == {"General"}:
            general_gold_rows += 1

        if predicted.get("no_match"):
            no_match_rows += 1
        if predicted.get("truncated_before_cap"):
            truncation_rows += 1
        if predicted.get("used_classifier"):
            used_classifier_rows += 1

        for category in CANONICAL_CATEGORIES:
            in_gold = category in gold_set
            in_pred = category in pred_set
            if in_gold and in_pred:
                per_category[category]["tp"] += 1
            elif in_pred and not in_gold:
                per_category[category]["fp"] += 1
            elif in_gold and not in_pred:
                per_category[category]["fn"] += 1
            else:
                per_category[category]["tn"] += 1

    per_category_metrics: dict[str, dict[str, Any]] = {}
    macro_f1_sum = 0.0
    macro_count = 0
    for category in CANONICAL_CATEGORIES:
        values = per_category[category]
        metrics = metric_dict(values["tp"], values["fp"], values["fn"])
        metrics["false_positive_rate"] = round4(
            values["fp"] / (values["fp"] + values["tn"]) if (values["fp"] + values["tn"]) else 0.0
        )
        metrics["false_negative_rate"] = round4(
            values["fn"] / (values["fn"] + values["tp"]) if (values["fn"] + values["tp"]) else 0.0
        )
        per_category_metrics[category] = metrics
        macro_f1_sum += metrics["f1"]
        macro_count += 1

    result = {
        "predictor": predictor_name,
        "mode": mode,
        "sample_size": len(rows),
        "exact_match_accuracy": round4(exact_match / len(rows) if rows else 0.0),
        "primary_category_accuracy": round4(primary_correct / len(rows) if rows else 0.0),
        "per_category": per_category_metrics,
        "macro_f1": round4(macro_f1_sum / macro_count if macro_count else 0.0),
        "general_rate": round4(general_pred_rows / len(rows) if rows else 0.0),
        "general_precision": per_category_metrics["General"]["precision"],
        "general_recall": per_category_metrics["General"]["recall"],
        "two_category_rate": round4(dual_pred_rows / len(rows) if rows else 0.0),
        "multilabel_article_recall": round4(dual_exact_rows / dual_gold_rows if dual_gold_rows else 0.0),
        "second_category_recall": round4(second_category_hits / dual_gold_rows if dual_gold_rows else 0.0),
        "truncated_before_cap_rate": round4(truncation_rows / len(rows) if rows else 0.0),
        "primary_order_accuracy_when_set_correct": round4(
            primary_order_correct / primary_order_rows if primary_order_rows else 0.0
        ),
        "used_classifier_rate": round4(used_classifier_rows / len(rows) if rows else 0.0),
        "no_match_rate": round4(no_match_rows / len(rows) if rows else 0.0),
        "error_bucket_counts": dict(mismatch_counts.most_common()),
        "error_examples": mismatch_examples if include_examples else {},
    }

    if contract_payload_builder is not None:
        result["token_contract_stats"] = token_contract_stats(rows, contract_payload_builder)

    return result


def subset_summary(
    rows: list[dict[str, Any]],
    *,
    mode: str,
    predictor_name: str,
    predictor: Callable[[dict[str, Any], str], dict[str, Any]],
    group_by: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[group_by(row)].append(row)

    summary: dict[str, Any] = {}
    for key, subset in grouped.items():
        metrics = evaluate_rows(
            subset,
            mode=mode,
            predictor_name=predictor_name,
            predictor=predictor,
            include_examples=False,
        )
        summary[key] = {
            "sample_size": metrics["sample_size"],
            "exact_match_accuracy": metrics["exact_match_accuracy"],
            "primary_category_accuracy": metrics["primary_category_accuracy"],
            "macro_f1": metrics["macro_f1"],
            "general_rate": metrics["general_rate"],
            "two_category_rate": metrics["two_category_rate"],
        }
    return dict(sorted(summary.items(), key=lambda item: (-item[1]["sample_size"], item[0])))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = load_gold(args.gold)

    evaluations = {
        "classify_title": evaluate_rows(
            rows,
            mode="title",
            predictor_name="classify_categories",
            predictor=predict_classifier,
        ),
        "classify_title_description": evaluate_rows(
            rows,
            mode="title_description",
            predictor_name="classify_categories",
            predictor=predict_classifier,
        ),
        "normalize_source_payload": evaluate_rows(
            rows,
            mode="title_description",
            predictor_name="normalize_article_categories",
            predictor=lambda row, mode: predict_normalized(row, reconstruct_source_payload),
            contract_payload_builder=reconstruct_source_payload,
        ),
        "normalize_current_payload": evaluate_rows(
            rows,
            mode="title_description",
            predictor_name="normalize_article_categories",
            predictor=lambda row, mode: predict_normalized(row, current_payload),
            contract_payload_builder=current_payload,
        ),
    }

    breakdowns = {
        "classify_title_description_by_source": subset_summary(
            rows,
            mode="title_description",
            predictor_name="classify_categories",
            predictor=predict_classifier,
            group_by=lambda row: str(row.get("source") or "Unknown"),
        ),
        "classify_title_description_by_primary_family": subset_summary(
            rows,
            mode="title_description",
            predictor_name="classify_categories",
            predictor=predict_classifier,
            group_by=lambda row: family_for_primary(str(gold_slice(row, "title_description")["primary"])),
        ),
        "normalize_source_payload_by_source": subset_summary(
            rows,
            mode="title_description",
            predictor_name="normalize_article_categories",
            predictor=lambda row, mode: predict_normalized(row, reconstruct_source_payload),
            group_by=lambda row: str(row.get("source") or "Unknown"),
        ),
    }

    result = {
        "sample_size": len(rows),
        "canonical_categories": list(CANONICAL_CATEGORIES),
        "evaluations": evaluations,
        "breakdowns": breakdowns,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
