# FinBERT Sentiment Audit

## Scope

This audit measures the current `sentiment_finbert.py` pipeline on real
commodity/news headlines from `data/feed.json`. It does **not** mix in
`ner_spacy.py`, change product behavior, retune the model, or replace
`ProsusAI/finbert`.

## Pipeline inspection

`sentiment_finbert.py` currently:

- Scores `build_enrichment_text(article, use_description)`.
- Uses title only by default.
- Uses `"title. description"` when `--use-description` is enabled and the
  article has a non-empty description.
- Writes `article["sentiment"]` with:
  - `label`
  - `confidence`
  - `probabilities`
  - `compound` (`positive - negative`)
  - `model`
  - `input_mode`
  - `text_hash`
  - `scored_at`

Likely domain failure modes from the code path:

- Title-only mode strips away context that can distinguish bullish tightness
  from harmful disruption.
- Title+description mode concatenates all available text without any filtering,
  so mixed summaries and topical update pages can become noisier rather than
  clearer.
- Single-label output forces a direction on many mixed commodity headlines.
- FinBERT appears sensitive to event severity words (`war`, `disruption`,
  `pressure`, `cuts`) even when the market implication is bullish.

## Benchmark summary

- Sample size: `112` headlines
- Feed size at audit time: `2,057` articles
- Sample seed: `20260310`
- Strata: `bullish`, `bearish`, `neutral`, `geopolitical`, `outage`, `macro`,
  `description_needed`, `ambiguous`
- Per-stratum quota: `14`
- Sample rows with descriptions: `58`
- Sample rows without descriptions: `54`
- Gold rows whose label changes between title-only and title+description: `6`

Annotation rubric:

- `positive`: supportive / bullish / improving conditions for the subject in
  context
- `negative`: harmful / bearish / disruptive / weakening conditions for the
  subject in context
- `neutral`: factual, mixed, unclear, or not directionally meaningful
- Title-only gold uses only the title text.
- Title+description gold uses the description when available.
- Ambiguous title-only cases were usually marked `neutral` or flagged with
  `ambiguity=true`.

Commands run:

```bash
python3 benchmarks/sentiment_finbert/build_sample.py
python3 benchmarks/sentiment_finbert/render_gold.py
python3 sentiment_finbert.py --input data/feed.json --output /tmp/feed.sentiment.title.json --force-rescore
python3 sentiment_finbert.py --input data/feed.json --output /tmp/feed.sentiment.title_desc.json --force-rescore --use-description
python3 benchmarks/sentiment_finbert/score_sample.py --gold benchmarks/sentiment_finbert/gold.jsonl --predictions /tmp/feed.sentiment.title.json --mode title --output benchmarks/sentiment_finbert/title_metrics.json
python3 benchmarks/sentiment_finbert/score_sample.py --gold benchmarks/sentiment_finbert/gold.jsonl --predictions /tmp/feed.sentiment.title_desc.json --mode title_description --output benchmarks/sentiment_finbert/title_description_metrics.json
```

Scoring method:

- Exact-match single-label comparison against manually reviewed gold labels
- Metrics: accuracy, per-class precision/recall/F1, macro F1, confusion matrix
- Baseline: always predict `neutral`
- Confidence audit: fixed confidence buckets plus ECE

## Metrics

### Headline-only (`title`)

- Accuracy: `0.5982`
- Macro F1: `0.5946`
- Neutral baseline accuracy: `0.4375`
- Gold distribution: `positive 30`, `negative 33`, `neutral 49`
- Predicted distribution: `positive 27`, `negative 49`, `neutral 36`

Per-class metrics:

| class | precision | recall | F1 | support |
| --- | ---: | ---: | ---: | ---: |
| positive | 0.5926 | 0.5333 | 0.5614 | 30 |
| negative | 0.5306 | 0.7879 | 0.6341 | 33 |
| neutral | 0.6944 | 0.5102 | 0.5882 | 49 |

Confusion matrix:

| expected \\ predicted | positive | negative | neutral |
| --- | ---: | ---: | ---: |
| positive | 16 | 9 | 5 |
| negative | 1 | 26 | 6 |
| neutral | 10 | 14 | 25 |

### Title + description (`title+description`)

- Accuracy: `0.5982`
- Macro F1: `0.5917`
- Neutral baseline accuracy: `0.3839`
- Gold distribution: `positive 32`, `negative 37`, `neutral 43`
- Predicted distribution: `positive 30`, `negative 53`, `neutral 29`

Per-class metrics:

| class | precision | recall | F1 | support |
| --- | ---: | ---: | ---: | ---: |
| positive | 0.6000 | 0.5625 | 0.5806 | 32 |
| negative | 0.5660 | 0.8108 | 0.6667 | 37 |
| neutral | 0.6552 | 0.4419 | 0.5278 | 43 |

Confusion matrix:

| expected \\ predicted | positive | negative | neutral |
| --- | ---: | ---: | ---: |
| positive | 18 | 10 | 4 |
| negative | 1 | 30 | 6 |
| neutral | 11 | 13 | 19 |

### Comparison

- Title+description did **not** improve topline accuracy.
- Title+description slightly improved `positive` and `negative` F1.
- Title+description materially hurt `neutral` recall (`0.5102` -> `0.4419`).
- Description mode changed the model prediction on `21/112` rows.
- Net effect of those changes:
  - fixed by description: `8`
  - broken by description: `8`
  - changed but still wrong: `5`

Per-stratum accuracy:

| stratum | title | title+description |
| --- | ---: | ---: |
| bullish | 0.500 | 0.714 |
| bearish | 0.500 | 0.571 |
| neutral | 0.571 | 0.500 |
| geopolitical | 0.643 | 0.429 |
| outage | 0.714 | 0.643 |
| macro | 0.643 | 0.643 |
| description_needed | 0.714 | 0.857 |
| ambiguous | 0.500 | 0.429 |

Interpretation:

- `--use-description` helps where the description actually resolves market
  direction.
- It hurts when the description is a mixed market summary, topical update page,
  or a longer geopolitical explainer that contains both bullish and bearish
  cues.

## Confidence calibration

Headline-only:

- ECE: `0.2268`
- `0.9-1.0` bucket: `51` rows, average confidence `0.9360`, accuracy `0.7843`
- `23/45` wrong predictions still had confidence `>= 0.8`
- Average confidence:
  - correct predictions: `0.8603`
  - wrong predictions: `0.7726`

Title+description:

- ECE: `0.2088`
- `0.9-1.0` bucket: `49` rows, average confidence `0.9406`, accuracy `0.7143`
- `22/45` wrong predictions still had confidence `>= 0.8`
- Average confidence:
  - correct predictions: `0.8443`
  - wrong predictions: `0.7514`

Takeaway:

- Confidence is directionally useful, but not reliable enough for hard gating.
- The model stays highly confident on many wrong calls, especially wrong
  `negative` calls.

## Error analysis

### 1. Mixed or factual headlines get forced into a direction

Recurring pattern:

- The model often turns neutral/mixed headlines into strong `negative` or
  `positive` predictions instead of staying neutral.

Examples:

- `European PET market splits on cost pressure and panic buying`
  - expected `neutral`
  - title predicted `negative` at `0.956`
  - mixed headline; model ignored the explicit split
- `NOON SNAPSHOT - Americas Markets Summary`
  - expected `neutral`
  - title+description predicted `negative` at `0.963`
  - summary headline covers multiple markets, not one clean direction
- `Mexico inflation slows to 3.7pc in Dec`
  - expected `neutral`
  - both modes predicted `negative` at `0.930`
  - macro data got forced into market sentiment

### 2. Geopolitical severity overrides market meaning

Recurring pattern:

- Words like `war`, `disruption`, `uncertainty`, `pressure`, and `cuts`
  frequently push the model toward `negative`, even when the market implication
  is bullish/tightening.

Examples:

- `Asia MDI faces upward pressure amid spate of price hikes, feedstock concerns`
  - expected `positive`
  - title predicted `negative` at `0.954`
  - title+description predicted `negative` at `0.941`
- `India PP output may tighten as government prioritizes LPG production`
  - expected `positive`
  - title predicted `negative` at `0.868`
  - title+description predicted `negative` at `0.957`
- `Urea derivatives surge on Middle East conflict`
  - expected `positive`
  - title+description predicted `negative` at `0.769`
- `Opec+ boosts production in November`
  - expected `negative`
  - both modes predicted `positive` at `0.919`
  - model also misses plain supply-side bearishness in the other direction

### 3. Operational headlines are often neutralized or misread

Recurring pattern:

- The model struggles with operational verbs like `restart`, `commission`,
  `starts`, `cut runs`, `force majeure`, and `stops production`.

Examples:

- `Plant status: Singapore's Aster to cut cracker runs further, FM issued`
  - expected `negative`
  - title+description predicted `neutral` at `0.940`
- `Plant status: Taiwan’s Nan Ya Plastics stops BDO production`
  - expected `negative`
  - title predicted `neutral` at `0.836`
- `OCP to restart tMAP output at end of Jan`
  - expected `positive`
  - both modes predicted `neutral` at `0.899`
- `Dimeca to commission central Mexico shredder`
  - expected `positive`
  - both modes predicted `neutral` at `0.879`
- `Erex starts second biomass co-firing test in Vietnam`
  - expected `positive`
  - both modes predicted `neutral` at `0.861`

### 4. Description helps some title-only misses, but hurts mixed summaries

Description fixed real title-only misses:

- `Asia toluene bid-offers rally higher, supply disruption fears shake up demand`
  - title `negative` -> title+description `positive`
  - expected `positive`
- `MONTHLY ASIA CHICKEN WRAP: Japanese boneless leg shortage deepens in Feb as Brazil, Thailand falter`
  - title `negative` -> title+description `positive`
  - expected `positive`
- `China's Tangshan Zhonghao declares force majeure on ADA`
  - title `neutral` -> title+description `negative`
  - expected `negative`
- `Marine insurers seek solutions as Gulf shipping threats evolve`
  - title `positive` -> title+description `negative`
  - expected `negative`

Description also introduced new wrong calls:

- `NOON SNAPSHOT - Asia Markets Summary`
  - title `neutral` correct
  - title+description `positive` wrong
- `EVENING SNAPSHOT - Europe Markets Summary`
  - title `neutral` correct
  - title+description `positive` wrong
- `NYMEX: US gas futures rise following increased storage withdrawal`
  - title `positive` correct
  - title+description `negative` wrong
- `South Korea sees no LNG shortages despite Middle East supply disruptions`
  - title `positive` correct
  - title+description `negative` wrong

### 5. Truly ambiguous commodity headlines stay hard

Recurring pattern:

- On ambiguous price/margin headlines, title and title+description often flip in
  opposite directions while gold remains neutral.

Examples:

- `China faces LNG supply test amid Middle East crisis, spot price increase`
  - title predicted `negative`
  - title+description predicted `positive`
  - expected `neutral`
- `LNG Weekly Market Report: Historic week rocks LNG, with Asia at big price premium`
  - title predicted `positive`
  - title+description predicted `negative`
  - expected `neutral`
- `Asia PVC carbide-based producers skirt naphtha shocks but uptake mixed`
  - title predicted `positive`
  - title+description predicted `negative`
  - expected `neutral`

## Verdict

### Is current sentiment quality good enough for production use?

Not for high-trust downstream use. `~0.60` accuracy and `~0.59` macro F1 are
too weak for a production signal that will be treated as a meaningful market
label without human review, confidence filtering, or additional guardrails.

### Is title-only scoring materially hurting quality?

Not materially on this benchmark. Title+description produced the **same**
accuracy and slightly worse macro F1. Description helps on a narrow subset of
context-dependent headlines, but those gains are cancelled out by new errors on
summaries, explainers, and mixed geopolitical articles.

### What is the biggest issue: model, label rubric, or insufficient context?

The largest issue is **model-task mismatch**, amplified by **insufficient
context** on some titles. The rubric is not the main blocker. The observed
errors are coherent and repeated:

- event severity gets mapped to `negative`
- mixed market summaries get forced directional
- operational verbs are handled poorly
- supply-tightening headlines are often read as bad news instead of bullish

### Does the model overuse `neutral`?

Not on this benchmark. It undercalls `neutral` and overcalls `negative`.

- Title gold neutral support: `49`; title predictions neutral: `36`
- Title+description gold neutral support: `43`; title+description predictions
  neutral: `29`

Note: on the full feed outputs, `neutral` is still the most common predicted
label (`831/2056` title-only, `782/2056` title+description), but relative to
this manually reviewed benchmark the model is **not** neutral-heavy.

### Are confidence scores trustworthy enough for downstream use?

No. They are somewhat rank-order useful, but too overconfident for hard gating.
Both modes make many `>= 0.8` confidence mistakes, and the `0.9-1.0` bucket is
still only `71-78%` accurate.

## Recommendations

### Evaluation infrastructure improvements

1. Keep this benchmark in-repo and rerun it before any model or rubric change.
2. Expand the gold set with a second reviewer for ambiguous commodity headlines.
3. Add a small locked regression subset of especially costly failures:
   mixed summaries, outage headlines, supply-tightening headlines, policy/macro
   headlines, and price-move headlines.

### Safe prompt-free pipeline improvements

1. Add a rule-based abstain/neutral guard for obvious summary wrappers:
   `SNAPSHOT`, `SUMMARY`, `WRAP`, `REPORT`, `PODCAST`, `Q&A`, `UPDATE TO TOPICS`.
2. Add post-rules for high-signal operational verbs:
   `restart`, `commission`, `starts` -> lean positive;
   `force majeure`, `halts`, `stops`, `cut runs`, `shuts` -> lean negative.
3. Consider down-weighting pure geopolitical severity words when paired with
   clearly bullish market cues like `price hikes`, `surge`, `tighten`,
   `premiums soar`, `supply shortage`.
4. Treat low-margin or mixed-cue price headlines as neutral by default when the
   title has offsetting signals.

### Model/pipeline upgrades

1. Benchmark a finance/news model that is less stock-news-centric and compare it
   against this exact gold set before any swap.
2. Try a lightweight commodity-domain classifier trained or calibrated on this
   rubric, especially for `positive` vs `negative` on supply disruptions.
3. If description use stays enabled, add preprocessing to strip summary wrappers
   and noisy topic-page boilerplate before sending text to the model.

## Files created

- `benchmarks/sentiment_finbert/README.md`
- `benchmarks/sentiment_finbert/build_sample.py`
- `benchmarks/sentiment_finbert/annotation_rules.md`
- `benchmarks/sentiment_finbert/gold_annotations.py`
- `benchmarks/sentiment_finbert/render_gold.py`
- `benchmarks/sentiment_finbert/gold.jsonl`
- `benchmarks/sentiment_finbert/score_sample.py`
- `benchmarks/sentiment_finbert/title_metrics.json`
- `benchmarks/sentiment_finbert/title_description_metrics.json`
- `benchmarks/sentiment_finbert/report.md`
