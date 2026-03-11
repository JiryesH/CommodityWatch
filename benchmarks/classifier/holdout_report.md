# Fresh Holdout Benchmark

This report reruns the deterministic classifier benchmark on a fresh holdout
sample from `data/feed.json`, excluding all rows and title/source duplicates
from the original 120-row gold benchmark.

## Scope

- Sample size: 80 manually reviewed articles
- Source mix:
  - Argus Media: 32
  - ICIS: 27
  - S&P Global: 17
  - Fastmarkets: 4
- Sampling:
  - Stratified using `build_holdout_sample.py`
  - Excluded all IDs in `gold.jsonl`
  - Excluded title/source duplicates of the original benchmark
- Annotation rubric:
  - Reused `annotation_rules.md`
  - Allowed up to 2 categories
  - `General` used only for genuinely broad or non-specific snippets

## Commands

```bash
python3 benchmarks/classifier/build_holdout_sample.py
python3 benchmarks/classifier/render_holdout_gold.py
python3 benchmarks/classifier/score_sample.py \
  --gold benchmarks/classifier/holdout_gold.jsonl \
  --output benchmarks/classifier/holdout_metrics.json
```

## Key Results

Title + description (`classify_categories(title, description)`):

- Exact-match accuracy: `0.7125`
- Primary-category accuracy: `0.7125`
- Macro F1: `0.6650`
- `General` precision: `0.5000`
- `General` recall: `0.8235`
- `General` rate: `0.3500`
- Multi-label exact recall: `0.5455`
- Second-category recall: `0.7273`

Normalization:

- `normalize_source_payload`
  - Exact-match: `0.7000`
  - Primary accuracy: `0.7000`
  - Macro F1: `0.7042`
- `normalize_current_payload`
  - Exact-match: `0.6250`
  - Primary accuracy: `0.6750`
  - Macro F1: `0.6671`

## Main Findings

- The classifier does not generalize nearly as well as the tuned 120-row
  benchmark now suggests.
- The dominant fresh-holdout failure mode is still false `General`.
- `Energy Transition` is the weakest family on the holdout:
  - Precision: `0.5000`
  - Recall: `0.1429`
  - F1: `0.2222`
- `Chemicals`, `Oil - Crude`, `Fertilizers`, and `Shipping` still have useful
  signal, but each shows meaningful recall gaps on the holdout.
- `Metals` remains strong on this holdout.

## Recurring Error Types

Wrongly classified as `General`:

- `INSIGHT: How US-Iran tension may reroute Asia polyolefin trade and where US exports fit`
- `Venture Global nears CP2 Phase 2 FID`
- `N America chemical rail traffic rises for fourth week, US up 8.6%`
- `PSV traders holding off positioning on summer products as conflict continues`
- `India sets 2026-27 budget for P, K subsidy`
- `US biofuel tax rule to benefit resellers, farmers`
- `STJ endossa Cbios, mas estoques limitam preços`
- `Viewpoint: Latam looks at domestic markets to spur H2`
- `US to remove oil sanctions on some countries until Iran situation straightens out: Trump`

Should have been `General`:

- `Americas top stories: weekly summary`
- `COMMODITY TRACKER: 6 charts to watch this week`
- `FACTBOX: Middle East war raises farm-to-fork food inflation risks on fuel, freight, fertilizer disruptions`

Missed valid second category:

- `Caustic soda freight costs rise on Mideast Gulf war`
- `War pushes crude tankers to record highs`
- `Rotterdam biomarine 2025 sales fall as bio-LNG surges`
- `Iran says targeting Western-linked ships at Hormuz, denies full closure`

## Verdict

The current classifier is substantially stronger than the original pre-audit
state, but the fresh holdout shows a large gap versus the tuned benchmark.
This is enough evidence to treat the 1.000 exact-match result on the original
gold set as benchmark saturation rather than true solved performance.

The most important next improvements are:

1. Reduce false `General` on entity/project shorthand and policy shorthand
   (`CP2`, `PSV`, `P, K`, `CBios`, `H2`, rail/logistics commodity phrasing).
2. Rebuild `Energy Transition` coverage from fresh evidence rather than the
   old tuned sample.
3. Tighten roundup/factbox suppression while preserving specific commodity
   stories that happen to mention macro conflict context.
