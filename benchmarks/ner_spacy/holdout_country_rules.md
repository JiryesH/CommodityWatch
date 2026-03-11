# Holdout Country Audit Rules

This holdout audit measures only country extraction on a fresh sample that
excludes every article from the original `gold.jsonl` benchmark.

Rules:

- Score countries as normalized sets per article.
- Use title-only gold for headline text alone.
- Use title+description gold for title plus the available description snippet.
- Count explicit country names, standard abbreviations (`US`, `UK`, `UAE`,
  `S Korea`), and clear demonyms (`Indian`, `Chinese`, `Dutch`, `Belgian`,
  `Swiss`, `Israeli`) as country evidence.
- Count unambiguous subnational locations when they clearly imply a single
  country in normal news usage, for example `Abu Dhabi`, `Ruwais`,
  `Sabine Pass`, and `Bilbao`.
- Do not count broad regions or waterways as countries:
  `Europe`, `Asia`, `Middle East`, `Mideast`, `Mediterranean`, `Hormuz`,
  `Strait of Hormuz`, `Arab Gulf`, `Mideast Gulf`.
- Ignore dateline boilerplate such as `LONDON (ICIS)--` and
  `SINGAPORE (ICIS)--`.
- When a location is too indirect or too obscure for confident country mapping
  from the title alone, leave it out of title-only gold and use the description
  only if it becomes explicit there.
