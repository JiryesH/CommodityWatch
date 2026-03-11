# Sentiment FinBERT Pipeline

`sentiment_finbert.py` now supports two benchmarkable modes:

- `baseline`: preserves the prior single-pass FinBERT behavior.
- `commodity_v1`: adds selective context, a neutral-vs-directional gate, and commodity-aware post-rules before the final label is written.

The output label space stays `positive | negative | neutral`, and the existing payload fields are unchanged:

- `label`
- `confidence`
- `probabilities`
- `compound`
- `model`
- `input_mode`
- `text_hash`
- `scored_at`

Additional metadata is written in a backward-compatible way:

- `backend`
- `pipeline_mode`
- `pipeline_version`
- `pipeline`

## Stages

`commodity_v1` runs these stages in order:

1. Context selection
   - `title`
   - `title+description`
   - `auto`
2. Directional gate
   - routes weak summary/procedural/mixed items to `neutral`
   - only directional items go to model-backed polarity resolution
3. Backend scoring
   - default backend: `finbert`
   - default model: `ProsusAI/finbert`
4. Commodity-aware post-processing
   - operational positives: `restart`, `commission`, `starts ... production/test`, `resume`, `boosts output`
   - operational negatives: `force majeure`, `halts`, `shuts`, `cut runs`, `stops production`, `offline`, `outage`
   - expansion / relief cues: `debottlenecking`, facility `enhancement`, lower utility tariff / rate headlines
   - harmful blockage cues: `cannot pay`, `buckling under`, `end ... exports`
   - market-meaning rules:
     - supply tightening / shortages / upward pressure can map to `positive`
     - supply-growth cues such as `Opec+ boosts production` can map to `negative`
     - mixed and offsetting cues stay `neutral`
     - geopolitical severity words alone do not force `negative`

## Context Selection

`--context-mode auto` prefers:

- `title` for strong headline signals, wrappers, and mixed/factual titles
- `title+description` when the title is weak/procedural and the description adds directional cues

Procedural wrappers such as `Viewpoint:` only gate to `neutral` when the remaining headline lacks clear directional cues; directional tails such as `rebound` or `under pressure` still flow through to polarity resolution.

This avoids the old global `--use-description` tradeoff where descriptions helped some rows but hurt mixed summaries and topic pages.

## CLI

Baseline title-only:

```bash
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.baseline.title.json \
  --force-rescore \
  --pipeline-mode baseline \
  --context-mode title
```

Baseline title+description:

```bash
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.baseline.title_desc.json \
  --force-rescore \
  --pipeline-mode baseline \
  --use-description
```

Improved selective-context pipeline:

```bash
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.commodity_auto.json \
  --force-rescore \
  --pipeline-mode commodity_v1 \
  --context-mode auto
```

Relevant options:

- `--backend finbert`
- `--pipeline-mode baseline|commodity_v1`
- `--context-mode auto|title|title+description`
- `--use-description` as a legacy explicit override

The same config fields are available through `SentimentConfig`, `rss_scraper.py`, and the control API payload:

- `sentiment_backend`
- `sentiment_pipeline_mode`
- `sentiment_context_mode`

## Benchmark Commands

Score baseline title-only:

```bash
python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/gold.jsonl \
  --predictions /tmp/feed.sentiment.baseline.title.json \
  --mode title \
  --output /tmp/title.metrics.json
```

Score baseline title+description:

```bash
python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/gold.jsonl \
  --predictions /tmp/feed.sentiment.baseline.title_desc.json \
  --mode title_description \
  --output /tmp/title_desc.metrics.json
```

Score `commodity_v1` auto mode:

```bash
python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/gold.jsonl \
  --predictions /tmp/feed.sentiment.commodity_auto.json \
  --mode auto \
  --output /tmp/commodity_auto.metrics.json
```

Compare two scored runs:

```bash
python3 benchmarks/sentiment_finbert/compare_runs.py \
  --baseline /tmp/title.metrics.json \
  --candidate /tmp/commodity_auto.metrics.json
```

Score only the locked regression subset:

```bash
python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/gold.jsonl \
  --predictions /tmp/feed.sentiment.commodity_auto.json \
  --mode auto \
  --subset benchmarks/sentiment_finbert/regression_subset.jsonl
```

## Additional Annotation Rounds

Create a larger scaffold from the live feed while excluding the current gold ids:

```bash
python3 benchmarks/sentiment_finbert/build_annotation_round.py \
  --input data/feed.json \
  --sample-size 240 \
  --output benchmarks/sentiment_finbert/annotation_round_scaffold.jsonl \
  --review-output benchmarks/sentiment_finbert/annotation_round_review.md
```

Outputs:

- JSONL scaffold with blank annotation fields
- Markdown review packet for manual review

## Locked Regression Subset

`benchmarks/sentiment_finbert/regression_subset.jsonl` is a gold-backed subset of the highest-cost cases from the audit:

- summary wrappers and topic-page updates
- operational positives and negatives
- bullish tightness vs bearish supply-growth cues
- explicit mixed headlines
- description-needed rows

It is intended for fast regression checks before rerunning the full benchmark.
