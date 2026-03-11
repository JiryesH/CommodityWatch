# Holdout Country Audit

## Scope

- Goal: test whether the improved country extractor generalizes beyond the
  original benchmark sample
- Dataset snapshot: `data/feed.json` at 2,089 articles
- Holdout sample size: 60 articles
- Exclusion rule: every article id in `gold.jsonl` was excluded from sampling
- Sample file: `holdout_sample_scaffold.jsonl`
- Gold file: `holdout_country_gold.jsonl`

This audit scores only countries, not entities.

## Rules

Rules are documented in `holdout_country_rules.md`.

Highlights:

- Count explicit country names, abbreviations, and clear demonyms.
- Count clear subnational locations when they strongly imply one country:
  `Abu Dhabi`, `Sabine Pass`, `Bilbao`.
- Ignore datelines and broad regions:
  `Europe`, `Asia`, `Middle East`, `Hormuz`, `Strait of Hormuz`.

## Commands

```bash
python3 benchmarks/ner_spacy/build_sample.py --sample-size 60 --seed 20260311 --exclude-jsonl benchmarks/ner_spacy/gold.jsonl --output benchmarks/ner_spacy/holdout_sample_scaffold.jsonl
python3 benchmarks/ner_spacy/render_holdout_country_gold.py
python3 ner_spacy.py --input data/feed.json --output /tmp/feed.ner.title.holdout.json --force-rescore
python3 ner_spacy.py --input data/feed.json --output /tmp/feed.ner.title_desc.holdout.json --force-rescore --use-description
python3 benchmarks/ner_spacy/score_sample.py --gold benchmarks/ner_spacy/holdout_country_gold.jsonl --predictions /tmp/feed.ner.title.holdout.json --mode title --output benchmarks/ner_spacy/holdout_title_metrics.json
python3 benchmarks/ner_spacy/score_sample.py --gold benchmarks/ner_spacy/holdout_country_gold.jsonl --predictions /tmp/feed.ner.title_desc.holdout.json --mode title_description --output benchmarks/ner_spacy/holdout_title_description_metrics.json
```

## Metrics

| Mode | Precision | Recall | F1 | TP | FP | FN | Exact article sets |
|---|---:|---:|---:|---:|---:|---:|---:|
| title-only | 1.000 | 0.845 | 0.916 | 49 | 0 | 9 | 52/60 |
| title+description | 0.983 | 0.843 | 0.908 | 59 | 1 | 11 | 49/60 |

Compared with the seen benchmark:

- Seen benchmark title-only country F1: `1.000`
- Holdout title-only country F1: `0.916`
- Seen benchmark title+description country F1: `1.000`
- Holdout title+description country F1: `0.908`

## Recurring Misses

Almost all holdout misses are recall failures on new variants that the explicit
rule layer does not cover yet.

### New demonyms not covered

- `S Korean` -> `South Korea`
- `Dutch` -> `Netherlands`
- `Belgian` -> `Belgium`
- `Chinese` -> `China`
- `Australian` -> `Australia`
- `Thai` -> `Thailand`
- `Swiss` -> `Switzerland` (description mode)
- `Israeli` -> `Israel` (description mode)

### New location mappings not covered

- `Abu Dhabi` -> `United Arab Emirates`
- `Sabine Pass` -> `United States`
- `Bilbao` -> `Spain`

### False positive

Only one false positive appeared in the holdout:

- `Singapore` from `Singapore time` in a market snapshot description

## Example Errors

- `Abu Dhabi's Adnoc raises Jan sulphur price by $25/t`
  - expected: `United Arab Emirates`
  - predicted: none
- `Equinor halts Dutch CCS-H2 plans, Belgian site still on`
  - expected: `Netherlands`, `Belgium`
  - predicted: none
- `Chinese domestic sulphur prices rise on US-Iran war`
  - expected: `China`, `United States`, `Iran`
  - predicted: `United States`, `Iran`
- `GAS ALERT: FluxSwiss cancels April monthly gas capacity auctions`
  - title+description expected: `Switzerland`
  - predicted: none
- `INSIGHT: Hormuz risk premium - from crude to critical minerals, China energy security faces three-stage shock`
  - title+description expected: `China`, `United States`, `Iran`, `Israel`
  - predicted: `China`, `United States`, `Iran`

## Verdict

The country extractor did improve materially, but the perfect score on the
original benchmark did not generalize.

What generalized:

- Precision remained excellent.
- Dateline stripping held up well.
- Previously fixed aliases and place mappings stayed fixed.

What did not generalize:

- Recall on unseen demonyms and unseen location-to-country mappings.
- The current approach is still largely a curated coverage layer rather than a
  general solution to country normalization.

Practical interpretation:

- The extractor is now strong enough to be useful in production if high
  precision matters most.
- It is not yet robust enough to claim broad country recall across new headline
  wording without further expansion of demonym and place coverage.
