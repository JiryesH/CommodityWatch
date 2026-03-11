# Second Holdout Country Audit

This is a second fresh country-only audit for `ner_spacy.py`.

- Sample size: 60 articles
- Sample seed: `20260313`
- Input: `data/feed.json`
- Exclusions: every article already used in:
  - `benchmarks/ner_spacy/gold.jsonl`
  - `benchmarks/ner_spacy/holdout_country_gold.jsonl`
- Sampling strata:
  - `obvious_country_title`: 11
  - `abbreviation_title`: 9
  - `company_heavy`: 9
  - `geopolitical`: 9
  - `commodity_weak_geo`: 11
  - `description_geo_only_or_disagree`: 11

This audit uses the same annotation policy as
`benchmarks/ner_spacy/holdout_country_rules.md`.
Only countries are scored. Entities are intentionally disabled.

## Files

- Sample scaffold:
  `benchmarks/ner_spacy/holdout2_sample_scaffold.jsonl`
- Review bundle:
  `benchmarks/ner_spacy/holdout2_review_bundle.jsonl`
- Manual annotations:
  `benchmarks/ner_spacy/holdout2_country_annotations.py`
- Rendered gold:
  `benchmarks/ner_spacy/holdout2_country_gold.jsonl`

## Commands Run

```bash
python3 benchmarks/ner_spacy/build_sample.py \
  --input data/feed.json \
  --output benchmarks/ner_spacy/holdout2_sample_scaffold.jsonl \
  --sample-size 60 \
  --seed 20260313 \
  --exclude-jsonl benchmarks/ner_spacy/gold.jsonl \
  --exclude-jsonl benchmarks/ner_spacy/holdout_country_gold.jsonl

python3 benchmarks/ner_spacy/render_holdout2_country_gold.py

python3 benchmarks/ner_spacy/score_sample.py \
  --gold benchmarks/ner_spacy/holdout2_country_gold.jsonl \
  --predictions /tmp/feed.ner.title.holdout.json \
  --mode title \
  --output benchmarks/ner_spacy/holdout2_title_metrics_old.json

python3 benchmarks/ner_spacy/score_sample.py \
  --gold benchmarks/ner_spacy/holdout2_country_gold.jsonl \
  --predictions /tmp/feed.ner.title.generalized.json \
  --mode title \
  --output benchmarks/ner_spacy/holdout2_title_metrics_generalized_verify.json

python3 benchmarks/ner_spacy/score_sample.py \
  --gold benchmarks/ner_spacy/holdout2_country_gold.jsonl \
  --predictions /tmp/feed.ner.title_desc.holdout.json \
  --mode title_description \
  --output benchmarks/ner_spacy/holdout2_title_description_metrics_old.json

python3 benchmarks/ner_spacy/score_sample.py \
  --gold benchmarks/ner_spacy/holdout2_country_gold.jsonl \
  --predictions /tmp/feed.ner.title_desc.generalized.json \
  --mode title_description \
  --output benchmarks/ner_spacy/holdout2_title_description_metrics_generalized_verify.json
```

## Results

### Title Only

Old pre-generalized system:
- Precision: `1.0000`
- Recall: `0.8621`
- F1: `0.9259`
- Exact country-set match: `52/60`

Current generalized system:
- Precision: `1.0000`
- Recall: `0.9483`
- F1: `0.9735`
- Exact country-set match: `57/60`

Delta:
- Precision: `+0.0000`
- Recall: `+0.0862`
- F1: `+0.0476`
- Exact country-set match: `+5`

### Title + Description

Old pre-generalized system:
- Precision: `0.9828`
- Recall: `0.8636`
- F1: `0.9194`
- Exact country-set match: `50/60`

Current generalized system:
- Precision: `0.9844`
- Recall: `0.9545`
- F1: `0.9692`
- Exact country-set match: `56/60`

Delta:
- Precision: `+0.0016`
- Recall: `+0.0909`
- F1: `+0.0498`
- Exact country-set match: `+6`

## Meaningful Improvement?

Yes.

On this second unseen sample, the generalized demonym/place-mapping pass
improved recall materially in both modes without hurting precision.

- Title-only improved from `0.9259` to `0.9735` F1.
- Title+description improved from `0.9194` to `0.9692` F1.
- The changed predictions produced `5` correct title-only recoveries and `6`
  correct title+description recoveries.
- There were `0` regressions on this sample.

## What It Fixed On This New Sample

Title-only true gains:
- `Abu Dhabi's Adnoc raises Feb sulphur price by $10/t`
  - old: `[]`
  - new: `['United Arab Emirates']`
- `Abu Dhabi's Adnoc rolls over March sulphur price`
  - old: `[]`
  - new: `['United Arab Emirates']`
- `Romanian gas demand to rise on higher power-sector use`
  - old: `[]`
  - new: `['Romania']`
- `Major Kazakh coal companies to increase production`
  - old: `[]`
  - new: `['Kazakhstan']`
- `Australian wheat demand stands to rise in Southeast Asia amid Middle East conflict`
  - old: `[]`
  - new: `['Australia']`

Title+description extra true gains:
- `FACTBOX: Middle East war raises farm-to-fork food inflation risks on fuel, freight, fertilizer disruptions`
  - old: `[]`
  - new: `['Philippines']`
- `IEA raises forecasts for 2023, 2024 global refinery runs`
  - old: `[]`
  - new: `['China']`

These are exactly the kind of generalized demonym/place cases the previous
holdout exposed, so this is evidence of real improvement rather than a purely
benchmark-specific patch.

## Remaining Errors

Title-only and title+description still miss:
- `Platts cash Dubai differential surges past $10/b premium`
  - expected: `['United Arab Emirates']`
  - predicted: `[]`
- `ET Highlights: ... US states sue ... Japan's NEDO backs Kawasaki's liquid hydrogen projects`
  - expected: `['Japan', 'United States']`
  - predicted: `['United States']`
- `Saudi Aramco Q4 net income falls on higher operating costs, lower revenue`
  - expected: `['Saudi Arabia']`
  - predicted: `[]`

Title+description still has one false positive:
- `Mideast ex-tank UAE base oils surge with conflict-driven supply disruptions`
  - expected: `['Iran', 'United Arab Emirates', 'United States']`
  - predicted: `['Iran', 'Singapore', 'United Arab Emirates', 'United States']`
  - issue: dateline `Singapore` still leaks into `countries`

## Assessment

The generalized pass held up on a second unseen sample.

It did not reach perfection, but it clearly improved unseen-country recall in a
way that matches the intended design change:

- broader demonym handling
- broader place-to-country mapping
- preserved precision

The remaining misses are now concentrated in:
- additional unmapped place aliases such as `Dubai`
- country demonyms embedded in organization names such as `Saudi Aramco`
- text-quality issues such as mojibake around `Japan's`
- one remaining dateline leakage case in description mode
