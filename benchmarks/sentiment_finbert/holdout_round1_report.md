# Sentiment FinBERT Holdout Audit Round 1

## Summary

This audit reruns the current sentiment pipeline on a fresh manually labeled holdout set that does not overlap the original 112-row development benchmark.

- Holdout size: 104 headlines
- Source: `data/feed.json`
- Overlap with original gold set: 0 rows
- Annotation rubric: market/news interpretation, not emotional wording
- Labels: `positive | negative | neutral`

Key result:

- The current `commodity_v1` pipeline improves over the old title-only baseline on unseen data.
- The lift is modest, not dramatic.
- The previously reported `0.9107` / `0.9087` result on the old benchmark did not generalize to this holdout.

## Commands Run

```bash
python3 benchmarks/sentiment_finbert/build_annotation_round.py \
  --input data/feed.json \
  --sample-size 104 \
  --output benchmarks/sentiment_finbert/holdout_round1_scaffold.jsonl \
  --review-output benchmarks/sentiment_finbert/holdout_round1_review.md

python3 benchmarks/sentiment_finbert/render_holdout_round1_gold.py

pytest -q tests/test_sentiment_finbert.py
PYTHONPATH=. pytest -q tests/test_enrichment_utils.py

HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.audit2.baseline.title.json \
  --force-rescore \
  --pipeline-mode baseline \
  --context-mode title

HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.audit2.baseline.title_desc.json \
  --force-rescore \
  --pipeline-mode baseline \
  --context-mode title+description

HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 python3 sentiment_finbert.py \
  --input data/feed.json \
  --output /tmp/feed.sentiment.audit2.commodity_auto.json \
  --force-rescore \
  --pipeline-mode commodity_v1 \
  --context-mode auto

python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/holdout_round1_gold.jsonl \
  --predictions /tmp/feed.sentiment.audit2.baseline.title.json \
  --mode title \
  --output /tmp/holdout.title.metrics.json

python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/holdout_round1_gold.jsonl \
  --predictions /tmp/feed.sentiment.audit2.baseline.title_desc.json \
  --mode title_description \
  --output /tmp/holdout.title_desc.metrics.json

python3 benchmarks/sentiment_finbert/score_sample.py \
  --gold benchmarks/sentiment_finbert/holdout_round1_gold.jsonl \
  --predictions /tmp/feed.sentiment.audit2.commodity_auto.json \
  --mode auto \
  --output /tmp/holdout.commodity_auto.metrics.json
```

## Metrics

### Overall

| Mode | Accuracy | Macro F1 | Neutral Baseline |
| --- | ---: | ---: | ---: |
| Baseline title | 0.5481 | 0.5378 | 0.3942 |
| Baseline title+description | 0.5096 | 0.5080 | 0.3942 |
| `commodity_v1` auto | 0.6058 | 0.5900 | 0.3942 |

Delta vs baseline title:

- Accuracy: `+0.0577`
- Macro F1: `+0.0522`

### Per-class F1

| Mode | Positive F1 | Negative F1 | Neutral F1 |
| --- | ---: | ---: | ---: |
| Baseline title | 0.4211 | 0.6133 | 0.5789 |
| Baseline title+description | 0.4828 | 0.5676 | 0.4737 |
| `commodity_v1` auto | 0.4615 | 0.6562 | 0.6522 |

Notable pattern:

- The current pipeline improved `negative` and `neutral` materially.
- `positive` remained the weakest class.
- Positive recall stayed at `0.3871` for both baseline title and `commodity_v1`, so the model is still missing many bullish commodity headlines.

### Confusion Matrix

Baseline title:

| Expected \\ Predicted | Positive | Negative | Neutral |
| --- | ---: | ---: | ---: |
| Positive | 12 | 12 | 7 |
| Negative | 3 | 23 | 6 |
| Neutral | 11 | 8 | 22 |

`commodity_v1` auto:

| Expected \\ Predicted | Positive | Negative | Neutral |
| --- | ---: | ---: | ---: |
| Positive | 12 | 6 | 13 |
| Negative | 3 | 21 | 8 |
| Neutral | 6 | 5 | 30 |

Interpretation:

- The new pipeline cut false positive/negative calls on neutral stories.
- It also reduced positive-to-negative reversals.
- But it still over-neutralizes many truly directional positive headlines.

### Class Distribution

Gold holdout distribution:

- Positive: 31
- Negative: 32
- Neutral: 41

Predicted distribution:

- Baseline title: positive 26, negative 43, neutral 35
- Baseline title+description: positive 27, negative 42, neutral 35
- `commodity_v1` auto: positive 21, negative 32, neutral 51

Interpretation:

- The new pipeline no longer overcalls `negative` the way the old baseline did.
- It now slightly overpredicts `neutral` and underpredicts `positive`.

## Confidence Quality

Calibration improved on holdout:

- Baseline title ECE: `0.2960`
- Baseline title+description ECE: `0.3253`
- `commodity_v1` auto ECE: `0.1507`

High-confidence mistakes also improved:

- Baseline title: 20 mistakes at confidence `>= 0.8`
- Baseline title+description: 20
- `commodity_v1` auto: 11

This is a real gain, but confidence is still not fully trustworthy for downstream use without guardrails. Several wrong calls remain highly confident, especially on supply-tightening headlines that the pipeline reads in the wrong direction.

## Ambiguity Split

Accuracy by ambiguity flag:

| Mode | Ambiguous | Clear |
| --- | ---: | ---: |
| Baseline title | 0.3143 | 0.6667 |
| Baseline title+description | 0.3182 | 0.5610 |
| `commodity_v1` auto | 0.4000 | 0.7101 |

Interpretation:

- The new pipeline helps on both clear and ambiguous stories.
- But ambiguous commodity headlines remain hard: it is still right only 40% of the time on those cases.

## Context Selection

Auto context usage on holdout:

- `title`: 99 rows, accuracy `0.6364`
- `title+description`: 5 rows, accuracy `0.0000`

This is a weak point in the current pipeline.

Every holdout row where auto mode chose `title+description` was wrong:

- `India's ONGC says new projects to reverse declining oil output from matured fields`
- `Americas top stories: weekly summary`
- `India urges refiners to maximize LPG output, prioritize household sector`
- `CRUDE SUMMARY: Crude prices climb as Middle East conflict threatens supply chains`
- `FACTBOX: Oil futures cross $110/b as Middle East conflict hits production`

The title-only rule layer generalized better than the current context-selection logic.

## Error Analysis

### 1. False neutralization of directional headlines

The new gate helps with mixed stories, but it now suppresses some clearly directional items.

Examples:

- `Plant status: Taiwan's FPCC cuts No 2&3 cracker runs amid feedstock concerns`
  - expected `negative`, predicted `neutral`, confidence `0.7200`
- `INSIGHT: Europe methanol market turns bullish on gas rally, supply threats`
  - expected `positive`, predicted `neutral`, confidence `0.7200`
- `US utilities boost capex plans to records on AI demand`
  - expected `positive`, predicted `neutral`, confidence `0.7200`
- `Shintech's expansion plans could pressure US VCM producers, add length`
  - expected `negative`, predicted `neutral`, confidence `0.7200`

### 2. Direction reversal on commodity-market tightening

The pipeline still confuses emotionally negative events with bullish market direction.

Examples:

- `China major producers’ polyolefin inventories fall amid rising buying activities`
  - expected `positive`, predicted `negative`, confidence `0.8794`
- `Methanex sees 18-20 million tonnes/year of Middle East methanol exports shut in – CEO`
  - expected `positive`, predicted `negative`, confidence `0.8656`
- `CRUDE SUMMARY: Crude prices climb as Middle East conflict threatens supply chains`
  - expected `positive`, predicted `negative`, confidence `0.8655`
- `FACTBOX: Oil futures cross $110/b as Middle East conflict hits production`
  - expected `positive`, predicted `negative`, confidence `0.5219`

### 3. Context-selection regressions

The current auto mode sometimes adds description where it should not, especially summary and explainers.

Examples:

- `Americas top stories: weekly summary`
  - expected `neutral`, predicted `negative`, confidence `0.4824`, input mode `title+description`
- `India's ONGC says new projects to reverse declining oil output from matured fields`
  - expected `positive`, predicted `negative`, confidence `0.8780`, input mode `title+description`

### 4. Mixed/factual trade-flow and stock headlines

Some factual headlines are still forced into a direction.

Examples:

- `Indian DAP stocks down, despite lower sales`
  - expected `neutral`, predicted `negative`, confidence `0.8696`
- `US raid complicates Venezuela urea loading, ops ongoing`
  - expected `neutral`, predicted `negative`, confidence `0.8574`
- `French urea imports surge in 2H 2025`
  - expected `neutral`, predicted `positive`, confidence `0.8229`
- `India’s NPK/NP stocks rise in January`
  - expected `neutral`, predicted `positive`, confidence `0.7409`

## Decision Assessment

### Is current quality good enough for production use?

Not yet as a high-trust standalone signal.

The fresh holdout result of `0.6058` accuracy / `0.5900` macro F1 is better than the old baseline, but still too error-prone for direct downstream trust in a commodity-news setting.

### Is title-only scoring materially hurting quality?

On this holdout, yes and no:

- Old pure title-only baseline is clearly weaker than the new pipeline.
- But the gains came mostly from the new gate and rule layer, not from adding description.
- Title+description by itself was worse than title-only.
- Auto description selection is currently a liability on unseen data.

### Is the biggest issue the model, the label rubric, or insufficient context?

The biggest issue remains task mismatch:

- The task is commodity market-direction classification.
- FinBERT still tends to read event severity or emotional negativity instead of market implication.
- The current rule layer helps, but the core classifier still misses many bullish tightening / shortage / price-rise cases.

The rubric is workable. The remaining weakness is more about model/task fit and conservative gating than about annotation noise.

### Does the model overuse neutral?

On this holdout, the current pipeline mildly overuses `neutral`:

- gold neutral count: 41
- predicted neutral count: 51

That is an improvement over the old baseline's negative bias, but it now creates false-neutral misses on clear directional headlines.

### Are confidence scores trustworthy enough to use downstream?

Better than before, but still not trustworthy enough on their own.

- Calibration improved substantially.
- High-confidence mistakes dropped.
- But some of the worst remaining errors are still highly confident and directionally wrong.

## Verdict

The second-pass pipeline did improve on unseen data, but the very large gain reported on the old benchmark did not fully generalize.

Evidence-based conclusion:

- The new system is better than the original baseline.
- The improvement is meaningful but modest on unseen headlines.
- There are clear signs that part of the in-sample gain came from tuning that does not fully transfer.
- The next work should focus on fresh annotation rounds and model/task fit, not another broad round of rule tuning against the same development set.

## Files Created For This Audit

- `benchmarks/sentiment_finbert/holdout_round1_scaffold.jsonl`
- `benchmarks/sentiment_finbert/holdout_round1_review.md`
- `benchmarks/sentiment_finbert/holdout_round1_annotations.py`
- `benchmarks/sentiment_finbert/render_holdout_round1_gold.py`
- `benchmarks/sentiment_finbert/holdout_round1_gold.jsonl`
- `benchmarks/sentiment_finbert/holdout_round1_report.md`
