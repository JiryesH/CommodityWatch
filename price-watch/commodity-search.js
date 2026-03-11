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

export function buildCommoditySeriesSearchText(definition, seriesOption) {
  const terms = new Set();

  addNormalizedTerm(terms, definition?.id);
  addNormalizedTerm(terms, definition?.primaryLabel);
  addNormalizedTerm(terms, definition?.displayLabel);
  addNormalizedTerm(terms, definition?.sectorId);
  addNormalizedTerm(terms, definition?.sectorLabel);
  addNormalizedTerm(terms, definition?.subsectorId);
  addNormalizedTerm(terms, definition?.subsectorLabel);
  addNormalizedTerm(terms, definition?.visualFamily);

  addNormalizedTerm(terms, seriesOption?.seriesKey);
  addNormalizedTerm(terms, seriesOption?.optionLabel);
  addNormalizedTerm(terms, seriesOption?.displayLabel);
  addNormalizedTerm(terms, seriesOption?.targetConcept);
  addNormalizedTerm(terms, seriesOption?.actualSeriesName);
  addNormalizedTerm(terms, seriesOption?.sourceSeriesCode);
  addNormalizedTerm(terms, seriesOption?.sourceName);
  addNormalizedTerm(terms, seriesOption?.geography);

  return Array.from(terms).join(" ");
}

export function buildCommoditySearchText(definition) {
  return (definition?.seriesOptions || [])
    .map((seriesOption) => buildCommoditySeriesSearchText(definition, seriesOption))
    .join(" ");
}

export function getMatchingSeriesOptions(definition, query) {
  const normalizedQuery = normalizeCommoditySearchQuery(query);
  const seriesOptions = definition?.seriesOptions || [];

  if (!normalizedQuery) {
    return seriesOptions;
  }

  const terms = normalizedQuery.split(" ");

  return seriesOptions.filter((seriesOption) => {
    const searchText = buildCommoditySeriesSearchText(definition, seriesOption);
    return terms.every((term) => searchText.includes(term));
  });
}

export function matchesCommoditySearch(definition, query) {
  return getMatchingSeriesOptions(definition, query).length > 0;
}
