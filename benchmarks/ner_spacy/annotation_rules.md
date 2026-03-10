# Annotation Rules

This benchmark scores `ner_spacy.py` against a manually reviewed sample from
`data/feed.json`.

Country rules:

- Score normalized countries as article-level sets.
- Count explicit country names, standard abbreviations (`US`, `UK`, `UAE`,
  `S Korea`), demonyms (`Indian`, `Guyanese`), and unambiguous subnational
  place names (`Texas`, `New York`, `Fujairah`, `Ruwais`, `Lake Charles`) as
  evidence for a country.
- Do not count broad regions (`Europe`, `Asia`, `Middle East`,
  `Mediterranean`, `Hormuz`) as countries.
- Ignore source datelines such as `LONDON (ICIS)--`, `SINGAPORE (ICIS)--`, and
  `HOUSTON (ICIS)--` unless the location is also part of the article content.

Entity rules:

- Score one canonical mention per distinct real-world entity per article/mode.
  Repeated aliases in the same article are not duplicated in gold.
- Include salient `ORG`, `PERSON`, `GPE`, `LOC`, `NORP`, `FAC`, and occasional
  policy/program entities when they are operationally useful.
- Exclude dates, quantities, money, percentages, generic commodity abbreviations
  (`LNG`, `MEG`, `PX`, `MMA`, `GMAA`, `RFCC`), and boilerplate source markers
  such as `ICIS`.
- Title-only gold uses only the title text.
- Title+description gold adds non-dateline entities and countries that appear
  in the available description snippet.

Ambiguity policy:

- If a mention is a broad region or otherwise does not map cleanly to a single
  country, it stays as an entity only.
- If a label choice is materially ambiguous, prefer the most operationally
  useful label and document the choice in notes.
