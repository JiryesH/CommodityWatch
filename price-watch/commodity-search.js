function normalizeSearchValue(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function addNormalizedTerm(terms, value) {
  const normalized = normalizeSearchValue(value);
  if (normalized) {
    terms.add(normalized);
  }
}

export function normalizeCommoditySearchQuery(query) {
  return normalizeSearchValue(query);
}

export function buildCommoditySearchText(definition) {
  const terms = new Set();

  addNormalizedTerm(terms, definition?.id);
  addNormalizedTerm(terms, definition?.primaryLabel);
  addNormalizedTerm(terms, definition?.group);
  addNormalizedTerm(terms, definition?.groupLabel);
  addNormalizedTerm(terms, definition?.sortLabel);
  addNormalizedTerm(terms, definition?.visual?.tile?.name);
  addNormalizedTerm(terms, definition?.visual?.tile?.ticker);
  addNormalizedTerm(terms, definition?.visual?.tile?.code);
  addNormalizedTerm(terms, definition?.visual?.tile?.symbol);

  (definition?.seriesOptions || []).forEach((seriesOption) => {
    addNormalizedTerm(terms, seriesOption?.seriesKey);
    addNormalizedTerm(terms, seriesOption?.optionLabel);
    addNormalizedTerm(terms, seriesOption?.displayLabel);
    addNormalizedTerm(terms, seriesOption?.targetConcept);
    addNormalizedTerm(terms, seriesOption?.actualSeriesName);
    addNormalizedTerm(terms, seriesOption?.sourceSeriesCode);
    addNormalizedTerm(terms, seriesOption?.geography);
  });

  return Array.from(terms).join(" ");
}

export function matchesCommoditySearch(definition, query) {
  const normalizedQuery = normalizeCommoditySearchQuery(query);
  if (!normalizedQuery) {
    return true;
  }

  const searchText = buildCommoditySearchText(definition);
  return normalizedQuery.split(" ").every((term) => searchText.includes(term));
}
