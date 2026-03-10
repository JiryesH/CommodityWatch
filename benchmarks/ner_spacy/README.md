# spaCy NER Benchmark

This directory contains a repeatable benchmark for `ner_spacy.py` against a
stratified sample from `data/feed.json`.

Files:

- `build_sample.py`: deterministic sample builder for the benchmark scaffold
- `sample_scaffold.jsonl`: machine-generated sample skeleton with raw article text
- `annotation_rules.md`: benchmark annotation policy
- `gold_annotations.py`: manual annotation source keyed by sampled article id
- `render_gold.py`: renders the annotation source into the scored gold dataset
- `gold.jsonl`: manually reviewed benchmark annotations
- `score_sample.py`: scorer for title-only and title+description outputs
- `report.md`: benchmark findings, metrics, error analysis, and verdict

Benchmark rules are documented inside `gold.jsonl` notes and the audit report
generated from the scored runs.
