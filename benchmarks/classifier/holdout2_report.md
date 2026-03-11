# Second Fresh Holdout Benchmark

This report reruns the classifier benchmark on a second disjoint holdout sample
from `data/feed.json`. The sample excludes:

- all rows used in the original 120-row benchmark
- all rows used in the first 80-row holdout benchmark
- title/source duplicates of those earlier samples

## Scope

- Sample size: 80 manually adjudicated articles
- Source mix:
  - Argus Media: 32
  - ICIS: 27
  - S&P Global: 17
  - Fastmarkets: 4
- Sampling:
  - Stratified using `build_holdout_sample.py`
  - Exclusions combined from `gold.jsonl` and `holdout_gold.jsonl`
- Annotation rubric:
  - Reused `annotation_rules.md`
  - Allowed up to 2 canonical categories
  - `General` only for genuinely broad or non-specific coverage

## Commands

```bash
python3 benchmarks/classifier/build_holdout_sample.py \
  --exclude-gold benchmarks/classifier/combined_exclude.jsonl \
  --output benchmarks/classifier/holdout2_sample_scaffold.jsonl
python3 benchmarks/classifier/render_holdout2_gold.py
python3 benchmarks/classifier/score_sample.py \
  --gold benchmarks/classifier/holdout2_gold.jsonl \
  --output benchmarks/classifier/holdout2_metrics.json
```

## Key Results

Title + description (`classify_categories(title, description)`):

- Exact-match accuracy: `0.6625`
- Primary-category accuracy: `0.7125`
- Macro F1: `0.6535`
- `General` precision: `0.4194`
- `General` recall: `1.0000`
- `General` rate: `0.3875`
- Multi-label exact recall: `0.4286`
- Second-category recall: `0.5000`

Normalization:

- `normalize_source_payload`
  - Exact-match: `0.6875`
  - Primary accuracy: `0.7500`
  - Macro F1: `0.6712`
- `normalize_current_payload`
  - Exact-match: `0.6125`
  - Primary accuracy: `0.6875`
  - Macro F1: `0.6194`

Gold `General` rate on this holdout is only `0.1625`, so the classifier is
again materially over-predicting `General`.

## Comparison to Prior Benchmarks

- Original tuned benchmark (same current classifier): `1.0000` exact-match
- First fresh holdout after tuning: `0.9750` exact-match
- Second fresh holdout: `0.6625` exact-match

The second fresh holdout does not confirm the apparent generalization gains seen
on the first holdout.

## Main Findings

- The dominant failure mode remains false `General`.
- The largest new weak spots are shorthand and sector-specific titles that are
  still commodity-specific but do not hit current rules:
  - `TiO2`
  - `rPET`
  - `ethane`
  - `Mn alloy`
  - `polyhalite`
  - `bunker demand`
  - upstream oil/blocks headlines without explicit `crude`
- Argus is the weakest source on this holdout:
  - exact-match `0.5000`
  - `General` rate `0.6250`
- Oil and shipping families are especially weak on this holdout:
  - oil-family exact-match `0.4375`
  - shipping-family exact-match `0.5455`

## Representative Errors

Wrongly classified as `General`:

- `INSIGHT: Europe, China TIO2 more susceptible to cost increases due to Middle East conflict`
- `EU rPET more positive in 2026: Petcore`
- `US ethane output growth to slow in 2026`
- `Kuwait port closure could limit petchem trade flows`
- `Hormuz ship traffic down 94pc since Iran conflict began`
- `Mitsubishi to invest in UK polyhalite mine`
- `BW Energy, M&P buy Azule out of Angolan offshore blocks`
- `Venezuelan oil heads to Italy, Spain, Israel: Update 2`
- `Viewpoint: Suez bunker demand may rebound in 2026`

Missed valid second category:

- `INSIGHT: Crude prices climb amid US-Iran conflict; styrene and derivatives pressure would come from upstream feedstocks`
- `Urea vessel in Venezuela updates destination signal`
- `US POWER TRACKER: Midcontinent April power rises despite mild forecasts, weak gas`
- `Lithium price volatility creates BESS cost uncertainty with hedging in infancy: ESS 2026`

Other misclassifications:

- `Plant status: China’s Shaanxi Yanchang Coal Yulin restarts LLDPE unit, to restart PP unit`
  - coal false positive from company name
- `ET Fuels secures bunkering buyer for Texas e-methanol`
  - still misses transition/shipping framing
- `LNG carrier headed for Boston on high spot gas prices`
  - still favors `Natural Gas` over `LNG`

## Verdict

This second disjoint holdout is strong evidence that the classifier is still
overfitting to whichever benchmark it has been tuned against most recently.
The first holdout gains were real for that sample, but they did not generalize
cleanly to another equally fresh holdout.

The next improvement pass should focus on:

1. Reducing false `General` on commodity-specific shorthand and sector jargon.
2. Expanding oil and shipping entity/event coverage without making roundup
   suppression too loose.
3. Fixing company-name and parent-name false positives such as coal/company-name
   collisions.
