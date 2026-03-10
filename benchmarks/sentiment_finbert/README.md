# FinBERT Sentiment Benchmark

This directory contains a repeatable benchmark for `sentiment_finbert.py`
against a stratified sample from `data/feed.json`.

Files:

- `build_sample.py`: deterministic sample builder for the benchmark scaffold
- `build_annotation_round.py`: larger scaffold generator for future annotation rounds
- `sample_scaffold.jsonl`: machine-generated sample skeleton with raw article text
- `annotation_rules.md`: benchmark annotation policy
- `gold_annotations.py`: manual annotation source keyed by sampled article id
- `render_gold.py`: renders the annotation source into the scored gold dataset
- `gold.jsonl`: manually reviewed benchmark annotations
- `regression_subset.jsonl`: locked subset of high-cost failures for fast checks
- `score_common.py`: shared scoring helpers used by the benchmark scripts
- `score_sample.py`: scorer for `title`, `title_description`, and `auto` runs
- `compare_runs.py`: side-by-side comparison utility for two scored runs
- `report.md`: benchmark findings, metrics, error analysis, and verdict

See [docs/sentiment-finbert.md](/Users/jiryes/Desktop/Projects/Contango/docs/sentiment-finbert.md) for pipeline stages, CLI/config options, and benchmark commands.
