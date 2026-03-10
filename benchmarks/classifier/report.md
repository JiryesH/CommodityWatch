# Classifier Audit Report

## Scope

This audit measures two separate behaviors:

1. `classify_categories(title, description)` as a deterministic keyword classifier.
2. Category-contract preservation in `normalize_category_token`,
   `normalize_categories`, `merge_category_lists`, and
   `normalize_article_categories`.

The benchmark does **not** treat existing `category` / `categories` fields in
`data/feed.json` as ground truth. Those fields are used only for provenance
inspection and for a separate contract-preservation check.

## Sample and Annotation

- Corpus at audit time: 2,061 articles in `data/feed.json`
- Gold sample size: 120 articles
- Source mix:
  - Argus Media: 48
  - ICIS: 40
  - S&P Global: 26
  - Fastmarkets: 6
- Strata covered: current `General`, dual-label potential, plant status/outage,
  geopolitical, market snapshot, shipping/freight, energy-transition ambiguity,
  chemicals-vs-fertilizers ambiguity, gas-vs-LNG ambiguity, crude-vs-refined
- Gold title-only ambiguity flags: 37 / 120
- Gold title+description ambiguity flags: 12 / 120
- Gold `General` rows:
  - Title-only: 19
  - Title+description: 18
- Gold dual-label rows:
  - Title-only: 11
  - Title+description: 16

Annotation rules live in `annotation_rules.md`. Main decisions:

- `General` is allowed only for truly broad, cross-commodity, or non-specific
  text.
- Dual labels are allowed only when both categories are explicit and central.
- Plant/process/company stories are assigned to a commodity only when the
  produced/traded commodity is explicit.
- `Natural Gas` vs `LNG`, `Oil - Crude` vs `Oil - Refined Products`,
  `Chemicals` vs `Fertilizers`, and `Energy Transition` vs conventional energy
  were resolved explicitly in the rubric.

## Commands and Scoring

Commands used:

```bash
python3 benchmarks/classifier/build_sample.py
python3 benchmarks/classifier/render_gold.py
python3 benchmarks/classifier/score_sample.py
```

Scoring method:

- Article-level category-set exact match ignores ordering.
- Primary-category accuracy compares predicted primary vs gold primary.
- Per-category precision / recall / F1 are article-level one-vs-rest metrics.
- `macro_f1` is the mean per-category F1 across all 13 canonical categories.
- For pure `classify_categories`, empty output is scored as `General` for
  contract comparison, but `no_match_rate` is reported separately because
  `classify_categories` never emits `General` directly.
- Contract behavior was tested in two realistic modes:
  - `normalize_source_payload`: reconstructed source payloads before fallback
    (`General` for ICIS/Argus/Fastmarkets, feed category for S&P)
  - `normalize_current_payload`: current `category` / `categories` fields from
    `data/feed.json`

## Headline Metrics

### Classification quality

| Variant | Exact match | Primary acc. | Macro F1 | `General` rate | `General` precision | `General` recall | 2-cat rate | Dual exact recall | Second-cat recall | No-match rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `classify_categories(title)` | 0.5500 | 0.5583 | 0.6552 | 0.5167 | 0.2903 | 0.9474 | 0.0667 | 0.3636 | 0.4545 | 0.5167 |
| `classify_categories(title, description)` | 0.4583 | 0.4917 | 0.6000 | 0.4083 | 0.3061 | 0.8333 | 0.2333 | 0.1875 | 0.5625 | 0.4083 |

Interpretation:

- Title-only looks better on exact match because it abstains heavily.
- Adding descriptions reduces `General` overuse but introduces many wrong
  secondary labels and wrong dual combinations.
- The title+description path is the operationally relevant one for ICIS and
  most S&P stories, and it is **not** reliable enough yet.

### Contract behavior

| Variant | Exact match | Primary acc. | Macro F1 | `General` rate | `General` precision | 2-cat rate | Dual exact recall | Second-cat recall | Used-classifier rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `normalize_source_payload` | 0.4583 | 0.5250 | 0.5759 | 0.3583 | 0.3256 | 0.1667 | 0.1250 | 0.4375 | 0.4583 |
| `normalize_current_payload` | 0.4333 | 0.4667 | 0.5862 | 0.3917 | 0.2979 | 0.2750 | 0.2500 | 0.6250 | 0.0000 |

Contract takeaways:

- The normalization primitives preserve the canonical contract cleanly on the
  sampled payloads:
  - Source-payload raw tokens: 120 canonical passthrough, 0 legacy rewrites,
    0 unknown tokens
  - Current-payload raw tokens: 153 canonical passthrough, 0 legacy rewrites,
    0 unknown tokens
- The problem is not token normalization; the problem is what labels get
  preserved or injected.
- `normalize_current_payload` cannot recover from already-informative but wrong
  labels, because fallback only runs when categories are empty or only
  `General`.

## Per-Category Results

Title+description `classify_categories` metrics:

| Category | Precision | Recall | F1 | FPR | FNR |
| --- | ---: | ---: | ---: | ---: | ---: |
| Oil - Crude | 0.3125 | 0.3846 | 0.3448 | 0.1028 | 0.6154 |
| Oil - Refined Products | 0.6842 | 0.8125 | 0.7429 | 0.0577 | 0.1875 |
| Natural Gas | 0.6667 | 0.4444 | 0.5333 | 0.0180 | 0.5556 |
| LNG | 0.8750 | 1.0000 | 0.9333 | 0.0088 | 0.0000 |
| Coal | 1.0000 | 1.0000 | 1.0000 | 0.0000 | 0.0000 |
| Electric Power | 0.0000 | 0.0000 | 0.0000 | 0.0084 | 1.0000 |
| Energy Transition | 0.5556 | 0.4167 | 0.4762 | 0.0370 | 0.5833 |
| Chemicals | 1.0000 | 0.5652 | 0.7222 | 0.0000 | 0.4348 |
| Metals | 0.7143 | 0.6250 | 0.6667 | 0.0179 | 0.3750 |
| Agriculture | 1.0000 | 0.5000 | 0.6667 | 0.0000 | 0.5000 |
| Fertilizers | 0.6000 | 0.6000 | 0.6000 | 0.0364 | 0.4000 |
| Shipping | 1.0000 | 0.5000 | 0.6667 | 0.0000 | 0.5000 |
| General | 0.3061 | 0.8333 | 0.4478 | 0.3333 | 0.1667 |

Strongest categories:

- `LNG`
- `Oil - Refined Products`
- `Chemicals` on precision, though recall is still only moderate

Weakest categories:

- `Electric Power`
- `Oil - Crude`
- `Energy Transition`
- `General` on precision

## Source and Segment Breakouts

Title+description `classify_categories` by source:

| Source | N | Exact match | Primary acc. | Macro F1 | `General` rate | 2-cat rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Argus Media | 48 | 0.6042 | 0.5833 | 0.4102 | 0.5208 | 0.0208 |
| ICIS | 40 | 0.3000 | 0.3500 | 0.4663 | 0.3500 | 0.4250 |
| S&P Global | 26 | 0.4615 | 0.5385 | 0.5450 | 0.2308 | 0.3846 |
| Fastmarkets | 6 | 0.3333 | 0.5000 | 0.0824 | 0.6667 | 0.0000 |

Title+description `classify_categories` by gold primary family:

| Family | N | Exact match | Primary acc. | Macro F1 | `General` rate | 2-cat rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chemicals / fertilizers | 32 | 0.3125 | 0.4062 | 0.2175 | 0.2812 | 0.3438 |
| oil | 25 | 0.4800 | 0.5200 | 0.1803 | 0.2800 | 0.1600 |
| general / non-specific | 18 | 0.8333 | 0.8333 | 0.0699 | 0.8333 | 0.1111 |
| gas / LNG | 12 | 0.5000 | 0.5000 | 0.1846 | 0.3333 | 0.1667 |
| energy transition | 11 | 0.0909 | 0.1818 | 0.1179 | 0.6364 | 0.3636 |
| shipping | 10 | 0.7000 | 0.5000 | 0.1916 | 0.2000 | 0.3000 |
| metals | 8 | 0.5000 | 0.6250 | 0.0592 | 0.3750 | 0.0000 |

Operational full-corpus behavior (not gold-scored):

| Variant | Corpus | No-match | 2-cat rate | Truncation >2 |
| --- | --- | ---: | ---: | ---: |
| `classify(title, description)` | All 2,061 | 0.4216 | 0.0961 | 0.0165 |
| `classify(title, description)` | ICIS 514 | 0.1907 | 0.2179 | 0.0506 |
| `classify(title, description)` | Argus 1,409 | 0.5295 | 0.0312 | 0.0007 |
| `classify(title, description)` | S&P 121 | 0.1653 | 0.3306 | 0.0579 |

This explains the behavior seen in the sample:

- Argus is mostly title-only and still misses many obvious commodity terms.
- ICIS and S&P descriptions reduce abstention, but they also create many bad
  dual labels and background-category leaks.

## Error Analysis

### 1. Should not have been `General`

This is the dominant failure mode.

Examples:

- `Italian gas-grid operator unfazed by seasonal backwardation ahead of injection season`
  - Predicted: `General`
  - Gold: `Natural Gas`
  - Cause: no coverage for `gas-grid`, `injection season`
- `Europe MDI market tested as producers pursue conflict-triggered price increases`
  - Predicted: `General`
  - Gold: `Chemicals`
  - Cause: missing chemical term coverage for `MDI`
- `Opec+ boosts production in November`
  - Predicted: `General`
  - Gold: `Oil - Crude`
  - Cause: no `Opec+` / `OPEC` keyword coverage

### 2. False triggers from ambiguous keywords

The taxonomy and supplement lists contain short or overloaded terms that fire on
locations, company names, or common verbs.

Observed trigger collisions:

- `Map Ta Phut` matched `MAP` fertilizer, causing false `Fertilizers`
  - `Plant status: Thailand’s PTTGC shuts LDPE line for maintenance`
  - Predicted: `Chemicals`, `Fertilizers`
  - Gold: `Chemicals`
- `Dubai` matched the Dubai crude benchmark, causing false `Oil - Crude`
  - `How the US-Iran war is reshaping polymers trade routes in the Middle East`
  - Predicted: `Oil - Crude`, `Chemicals`
  - Gold: `Chemicals`, `Shipping`
- `scrap` in `scrap its heating mandate` matched metals scrap
  - `Gas to remain in Germany’s residential demand mix amid proposed heating law reset`
  - Predicted: `Energy Transition`, `Metals`
  - Gold: `Natural Gas`, `Energy Transition`
- `petroleum` in `Zhejiang Petroleum & Chemical` added false crude
  - `Plant status: China’s ZPC to shut VAM unit in April`
  - Predicted: `Oil - Crude`, `Chemicals`
  - Gold: `Chemicals`

### 3. Background commodities outrank the real subject

Descriptions often mention crude, freight, or gas as causal context. The
classifier captures those background terms and then sorts categories by fixed
contract priority, not by article salience.

Examples:

- `India PP output may tighten as government prioritizes LPG production`
  - Predicted: `Oil - Crude`, `Oil - Refined Products`
  - Gold: `Chemicals`, `Oil - Refined Products`
- `Global BD prices jump on Middle East conflict; feedstock shortages in Asia`
  - Predicted: `Oil - Crude`, `Oil - Refined Products`
  - Gold: `Chemicals`
- `Asian naphtha markets vulnerable to Middle East refinery and shipping disruptions`
  - Unbounded matches: `Shipping`, `Oil - Crude`, `Oil - Refined Products`
  - Stored prediction: `Oil - Crude`, `Oil - Refined Products`
  - Gold: `Oil - Refined Products`, `Shipping`
  - Cause: match set is truncated after fixed priority sorting, so `Shipping`
    gets dropped

### 4. Category-definition and mapping ambiguities

Some commodity concepts need more explicit policy decisions than the current
rules provide.

Examples:

- `ME ammonia futures rise to $590/t fob for April`
  - Predicted: `Energy Transition`
  - Gold: `Fertilizers`
  - Cause: `ammonia` maps too eagerly toward transition themes when low-carbon
    context is absent
- `India's crude throughput remains stable in January`
  - Predicted: `Oil - Crude`
  - Gold: `Oil - Refined Products`
  - Cause: refinery-throughput stories should map to refining / products
- `Risk of prolonged conflict could leave Germany switching to coal, power imports amid unprofitable CCGTs`
  - Predicted: `Natural Gas`, `Coal`
  - Gold: `Electric Power`, `Coal`
  - Cause: power-system stories lack enough power-grid / power-market coverage

### 5. Contract layer preserves wrong informative labels

`normalize_article_categories` only falls back to classification when the
existing labels are empty or only `General`. That means bad feed labels and bad
previous labels persist.

Examples:

- `Australia's final carbon leakage review recommends CBAM-like scheme for high-risk sectors`
  - Feed: `S&P Fertilizers`
  - `normalize_source_payload` prediction: `Fertilizers`
  - Gold: `Energy Transition`
- `Singapore's carbon tax serves as 'key pillar' of climate strategy: PM`
  - Feed: `S&P Fertilizers`
  - `normalize_source_payload` prediction: `Fertilizers`
  - Gold: `Energy Transition`
- `India says fertilizer stocks secure despite Middle East war`
  - Current payload: `['Energy Transition', 'Chemicals']`
  - Gold: `Fertilizers`
  - Current-payload normalization preserves the bad informative labels

## Verdict

### Is `classifier.py` good enough for production fallback use?

Not yet. It is usable as a weak heuristic, but the current measured behavior is
too noisy for confident fallback:

- Main fallback path (`normalize_source_payload`) exact-match accuracy is
  45.8%, primary accuracy 52.5%
- Pure classifier on title+description is 45.8% exact and 49.2% primary

That is below the bar for a fallback system that is supposed to repair missing
or weak categories.

### Is it overusing `General`?

Yes.

- Gold `General` rate on the reviewed title+description sample: 15.0% (18 / 120)
- Predicted `General` rate: 40.8%
- `General` precision: 30.6%

The classifier is much better at abstaining than at choosing a specific but
correct category.

### Strongest categories

- `LNG`
- `Oil - Refined Products`
- `Chemicals` on precision

### Weakest categories

- `Electric Power`
- `Oil - Crude`
- `Energy Transition`
- `General` precision

### Main root causes

1. Missing keyword / synonym coverage
2. Ambiguous taxonomy terms and supplements causing false triggers
3. Fixed priority ordering and truncation that favor background commodities
4. Contract preservation of bad feed labels

The category contract itself is mostly coherent. The bigger problems are rule
coverage and ranking, not the canonical taxonomy list.

### Top 3 highest-leverage fixes

1. Add safe, high-yield coverage for common missing terms:
   - `gas-grid`, `injection season`, `gas exchange`
   - `Opec+`, `condensate`, `Trans Mountain`
   - `MDI`, `glycol ethers`, `methacrylic acid`
   - `biomethane` / `biometano`, `biocarbon`, `carbon tax`, `CBAM`
   - `sulphur`
2. Constrain or remove ambiguous triggers:
   - short aliases like `MAP`
   - location / benchmark collisions like `Dubai`
   - common words like `scrap`
   - company-name terms like `petroleum`
3. Change label selection from fixed-priority truncation to evidence scoring:
   - weight title matches above description matches
   - downweight purely causal/background terms
   - select the top 2 supported categories by evidence, not by static contract
     order

## Recommendations

### Evaluation infrastructure

- Keep this benchmark and rerun it on every classifier change.
- Add a keyword-trace debug mode that prints matched terms per category.
- Add a small regression suite from the concrete error examples above.
- Track a weak-silver monitor by source/feed to catch feed-category drift, but
  keep the human-reviewed set as the real benchmark.

### Safe rule changes

- Add the missing keywords above.
- Add stoplists / negative contexts for `MAP`, `Dubai`, `scrap`, and similar
  overloaded tokens.
- Prefer title evidence over description evidence when selecting primary and
  secondary labels.

### Taxonomy / supplement changes

- Review whether `ammonia` should default to `Fertilizers` unless paired with
  `blue`, `green`, `clean`, `hydrogen`, or similar transition language.
- Add power-market vocabulary beyond `electricity` and `power prices` so
  power-system headlines stop falling into `Natural Gas` / `Coal`.
- Expand energy-transition vocabulary to include `battery`, `carbon tax`,
  `CBAM`, and biocarbon / biomethane variants.

### Broader redesigns

- Replace pure first-match / priority behavior with scored evidence accumulation.
- Distinguish title matches from description matches.
- Allow `normalize_article_categories` to augment or override informative source
  labels when the source/feed category looks stale or clearly contradicted by
  article text.

## Files Created

- `benchmarks/classifier/build_sample.py`
- `benchmarks/classifier/annotation_rules.md`
- `benchmarks/classifier/gold_annotations.py`
- `benchmarks/classifier/render_gold.py`
- `benchmarks/classifier/gold.jsonl`
- `benchmarks/classifier/score_sample.py`
- `benchmarks/classifier/metrics.json`
- `benchmarks/classifier/report.md`
