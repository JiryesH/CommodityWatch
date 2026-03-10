# `ner_spacy.py` Benchmark Report

## Scope

- Target file: `ner_spacy.py`
- Dataset: `data/feed.json`
- Sample size: 84 articles
- Sample design: deterministic stratified sample built from six strata:
  `obvious_country_title` (16), `abbreviation_title` (12),
  `company_heavy` (12), `geopolitical` (12), `commodity_weak_geo` (16),
  `description_geo_only_or_disagree` (16)
- Sample source mix: 46 Argus Media, 34 ICIS, 3 S&P Global, 1 Fastmarkets

The sample intentionally overweights ICIS-style description-heavy items because
the main production question is whether `--use-description` helps or harms
country and entity extraction.

## Annotation Rules

Rules are documented in `annotation_rules.md`.

Key choices:

- Countries are scored as normalized sets per article.
- Demonyms and unambiguous subnational locations count toward countries:
  `Indian -> India`, `Guyanese -> Guyana`, `Fujairah -> UAE`,
  `Ras Tanura -> Saudi Arabia`, `Cabinda -> Angola`, `Houston -> US`,
  `Lake Charles -> US`, `New York -> US`.
- Broad regions do not count as countries:
  `Europe`, `Asia`, `Middle East`, `Mediterranean`, `Hormuz`.
- Description datelines are ignored as country/entity gold:
  `LONDON (ICIS)--`, `SINGAPORE (ICIS)--`, `HOUSTON (ICIS)--`.
- Entity gold keeps one canonical mention per real-world entity per article.
- Dates, quantities, prices, and generic commodity abbreviations are excluded as
  low-value entities.

## Commands Run

```bash
python3 benchmarks/ner_spacy/build_sample.py
python3 ner_spacy.py --input data/feed.json --output /tmp/feed.ner.title.json --force-rescore
python3 ner_spacy.py --input data/feed.json --output /tmp/feed.ner.title_desc.json --force-rescore --use-description
python3 benchmarks/ner_spacy/render_gold.py
python3 benchmarks/ner_spacy/score_sample.py --gold benchmarks/ner_spacy/gold.jsonl --predictions /tmp/feed.ner.title.json --mode title --output benchmarks/ner_spacy/title_metrics.json
python3 benchmarks/ner_spacy/score_sample.py --gold benchmarks/ner_spacy/gold.jsonl --predictions /tmp/feed.ner.title_desc.json --mode title_description --output benchmarks/ner_spacy/title_description_metrics.json
```

## Scoring Method

- Countries: micro precision/recall/F1 over article-level normalized country
  sets.
- Entities: micro precision/recall/F1 over article-level entity sets.
- Entity views:
  - exact text-only
  - exact text+label
  - relaxed text-only
- Relaxed entity matching lowercases text and ignores punctuation,
  possessives, and leading articles such as `the`.

## Metrics

| Mode | Country P | Country R | Country F1 | Entity text exact F1 | Entity text+label exact F1 | Entity text relaxed F1 |
|---|---:|---:|---:|---:|---:|---:|
| title-only | 0.984 | 0.785 | 0.873 | 0.799 | 0.709 | 0.805 |
| title+description | 0.755 | 0.819 | 0.786 | 0.579 | 0.515 | 0.604 |

Supporting counts:

- Title-only countries: TP 62, FP 1, FN 17
- Title+description countries: TP 77, FP 25, FN 17
- Title-only entities text+label: TP 111, FP 54, FN 37
- Title+description entities text+label: TP 128, FP 197, FN 44
- Country exact-set accuracy: title-only 66/84, title+description 45/84
- Entity exact-set accuracy: title-only 31/84, title+description 20/84

Stratum highlights:

- Title-only country F1 is strong on explicit abbreviations (`1.000`) but
  collapses on weak-geo commodity headlines (`0.308`).
- Title+description country F1 drops hardest on company-heavy
  (`0.636`) and description-geo/disagreement (`0.717`) strata because
  datelines add false countries.
- Title+description entity F1 is below `0.50` on four of six strata.

## Error Analysis

### 1. Missed country present in title

This is the main title-only country failure mode.

Repeated patterns:

- Demonyms are not converted to countries:
  - `Indian LPG stocks limited as Iran conflict broadens` missed `India`
  - `Viewpoint: Rising Guyanese crude flows open new markets` missed `Guyana`
  - `French urea imports surge in 2H 2025` missed `France`
  - `German B7 diesel premiums surge on quota rise` missed `Germany`
- Unambiguous city/subnational mentions are not mapped:
  - `UAE Fujairah bunker market braced for impact` missed `United Arab Emirates`
  - `Aramco shuts Ras Tanura refinery after drone strike` missed `Saudi Arabia`
  - `Cabinda refinery still in testing, no supply yet` missed `Angola`
  - `Seized oil tanker likely heading to Houston` missed `United States`
  - `Lake Charles LNG terminates some offtake contracts` missed `United States`
  - `Trinseo notified of New York Stock Exchange de-listing plans` missed `United States`
- Country abbreviations swallowed by entity spans are missed:
  - `US E15 group floats fewer biofuel mandate waivers` missed `United States`
  - `Plant status: S Korea's S-Oil commences No 2 RFCC planned turnaround on 6 Mar`
    missed `South Korea`

### 2. Title+description gains recall but mostly through noisy datelines

Title+description improves country recall only slightly (`0.785 -> 0.819`) and
reduces country precision sharply (`0.984 -> 0.755`).

False country additions in the sample:

- `Singapore`: 14
- `United Kingdom`: 9
- `Venezuela`: 1
- `Virgin Islands, U.S.`: 1

Most of these come from ICIS/S&P datelines rather than article geography.

Examples:

- `BASF plans maintenance stoppage on AA, acrylate esters at Ludwigshafen, Germany`
  predicted `['Germany', 'United Kingdom']`; expected `['Germany']`
- `Sinopec raises east China phenol offers by CNY400/tonne`
  predicted `['China', 'Singapore']`; expected `['China']`
- `US–Iran war triggers severe strain on chemical tanker shipping, global logistics`
  predicted `['Iran', 'United Kingdom', 'United States', 'Virgin Islands, U.S.']`;
  expected `['Iran', 'United States']`

Title+description only clearly fixed three title-only country misses in this
sample (`S Korea`, `New York Stock Exchange`, `East China`), but introduced 25
false country tags.

### 3. Country normalization/post-processing gaps

The extractor only derives countries from `GPE`/`LOC`, then normalizes with
`pycountry` and a short alias list.

Observed gaps:

- No demonym-to-country bridge for `NORP`
- No alias for `S Korea`
- No city/province-to-country map for high-frequency feed locations:
  `Fujairah`, `Ras Tanura`, `Cabinda`, `Houston`, `Lake Charles`, `Ruwais`,
  `Onsan`, `Zhejiang`, `East China`
- Fuzzy normalization can invent countries from non-country ORGs:
  `Yara -> Venezuela`
  `U.S. -> Virgin Islands, U.S.` in one description-mode article

### 4. Entity extraction is moderately useful on titles, noisy on descriptions

Title-only entity extraction is usable as a rough signal but not clean enough to
trust as a final stored entity list.

Repeated title-only issues:

- Wrong labels for important entities:
  - `Sinopec -> PERSON`
  - `Trump -> ORG`
  - `Maduro -> GPE`
  - `Valero -> PRODUCT`
  - `Methanex -> PERSON`
  - `Yara -> GPE`
  - `Ras Tanura -> PERSON`
- Low-value entities are stored alongside useful ones:
  - `December`, `Jan-Feb`, `20th consecutive month`, `70mn`, `CNY400/tonne`
- ORG recall is the biggest miss bucket:
  title-only missed gold labels include `ORG` 20 times vs `LOC` 6 and `GPE` 5

Description mode makes the entity list substantially worse:

- Extra entity labels in title+description mode:
  `DATE` 50, `GPE` 43, `ORG` 42, `PERSON` 11, `LOC` 11
- Most common extra entity texts:
  `SINGAPORE` 14, `LONDON` 10, `the Middle East` 6, `HOUSTON` 4, `ICIS)--Here` 4

Examples:

- `Plant status: ADNOC refinery No 2 shut on incident - sources`
  title-only entities: `No 2/WORK_OF_ART`
  title+description entities include `SINGAPORE`, `Abu Dhabi`, `Refining Location: Ruwais`
- `CBAM suspension for ferts would be 'surprising': Yara`
  title-only entities: `CBAM/ORG`, `Yara/GPE`
  countries: `['Venezuela']` even though no country is present
- `Sinopec raises east China phenol offers by CNY400/tonne`
  title+description entities include `SINGAPORE`, `Chinese`, `5 March`, `Thursday`
  in addition to the useful `China`

## Verdict

### Country extraction

- Title-only country extraction is not reliable enough for production if the
  downstream use expects good geographic coverage.
- It is precision-heavy and recall-light: explicit abbreviations work well, but
  the pipeline misses many real headline geographies expressed as demonyms,
  city names, provinces, or country abbreviations outside the tiny alias list.
- Title+description in its current form is worse operationally. The small recall
  gain does not justify the dateline-driven false positives.

### Entity extraction

- Title-only entity extraction is marginally useful as a noisy candidate list.
  It is not clean enough to expose directly without filtering.
- Title+description entity extraction is too noisy for production storage in its
  current form.

### Root cause split

Main causes, in order:

1. Title-only input misses geography that is only in descriptions, but this is
   not the dominant problem.
2. Post-processing is too weak for country derivation:
   demonyms, city-to-country mapping, and alias coverage are the largest gaps.
3. Raw spaCy entity output is stored with almost no quality filter, so label
   noise and low-value entities flow straight into `article["ner"]["entities"]`.
4. Description mode is harmed by boilerplate text handling, especially wire
   datelines.

## Highest-Payoff Improvements

### Evaluation infrastructure

1. Keep this benchmark in-repo and run the scorer before any NER change.
   The new files already support that.

### Safe extraction logic

2. Strip or preprocess description datelines before NER.
   This is the clearest title+description fix and should remove most of the
   `Singapore`/`United Kingdom` false positives immediately.

3. Expand country derivation beyond the current `GPE`/`LOC` + `pycountry`
   matcher.
   Add:
   - demonym support from `NORP`
   - alias coverage for forms like `S Korea`
   - a small high-frequency location-to-country map for feed-specific places
     like `Fujairah`, `Ras Tanura`, `Ruwais`, `Cabinda`, `Houston`,
     `Lake Charles`, `Zhejiang`, `Queensland`

4. Filter stored entities to a useful subset and fix a few recurring label/path
   errors in post-processing.
   At minimum, drop `DATE`, `MONEY`, `QUANTITY`, `PERCENT`, `CARDINAL`,
   `ORDINAL`, and boilerplate source tokens from the stored entity list.

### Bigger pipeline changes

5. Only after the safe fixes above, test a stronger/domain-adapted NER pipeline
   or rule layer for organization detection and geopolitical normalization.
   The benchmark is now in place to measure whether that actually helps.

## Limitations

- This benchmark uses 84 sampled articles, not the full 2,053-article feed.
- The sample deliberately stresses edge cases and description-heavy ICIS items,
  so it is harder than the raw source distribution.
- Entity scoring uses operationally useful gold entities rather than every
  possible spaCy-recognizable mention, because the production question is
  whether the stored entity list is useful, not whether every date is detected.
