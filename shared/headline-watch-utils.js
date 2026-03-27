const COUNTRY_NAME_OVERRIDES = Object.freeze({
  "Iran, Islamic Republic of": "Iran",
  "Korea, Republic of": "South Korea",
  "Korea, Democratic People\'s Republic of": "North Korea",
  "Russian Federation": "Russia",
  "Syrian Arab Republic": "Syria",
  "Venezuela, Bolivarian Republic of": "Venezuela",
  "Bolivia, Plurinational State of": "Bolivia",
  "Moldova, Republic of": "Moldova",
  "Tanzania, United Republic of": "Tanzania",
  "Taiwan, Province of China": "Taiwan",
  "Viet Nam": "Vietnam",
  "Lao People\'s Democratic Republic": "Laos",
  "Brunei Darussalam": "Brunei",
  "Libyan Arab Jamahiriya": "Libya",
});

export function cleanCountryName(country) {
  const raw = String(country || "").trim();
  if (!raw) {
    return "";
  }

  return COUNTRY_NAME_OVERRIDES[raw] || raw;
}

export function countryKey(country) {
  return cleanCountryName(country).toLowerCase();
}

export function collectCountries(articles) {
  const seenKey = new Set();
  const out = [];

  (articles || []).forEach((article) => {
    const ner = article && article.ner;
    const countries = ner && Array.isArray(ner.countries) ? ner.countries : [];

    countries.forEach((country) => {
      const label = cleanCountryName(country);
      const key = countryKey(country);
      if (!label || !key || seenKey.has(key)) {
        return;
      }

      seenKey.add(key);
      out.push({ key, label });
    });
  });

  out.sort((left, right) => left.label.localeCompare(right.label));
  return out;
}

export function buildCountryAvailability(articles, includeArticle = () => true) {
  const counts = new Map();

  (articles || []).forEach((article) => {
    if (!includeArticle(article)) {
      return;
    }

    const ner = article && article.ner;
    const countries = ner && Array.isArray(ner.countries) ? ner.countries : [];

    countries.forEach((country) => {
      const key = countryKey(country);
      if (!key) {
        return;
      }

      counts.set(key, (counts.get(key) || 0) + 1);
    });
  });

  return counts;
}

export function areSetsEqual(a, b) {
  if (a.size !== b.size) {
    return false;
  }

  for (const value of a) {
    if (!b.has(value)) {
      return false;
    }
  }

  return true;
}

export function eventPathIncludes(event, element) {
  if (!event || !element) {
    return false;
  }

  if (typeof event.composedPath === "function") {
    const path = event.composedPath();
    if (Array.isArray(path) && path.includes(element)) {
      return true;
    }
  }

  return element.contains(event.target);
}

const api = Object.freeze({
  cleanCountryName,
  countryKey,
  collectCountries,
  buildCountryAvailability,
  areSetsEqual,
  eventPathIncludes,
});

globalThis.CommodityWatchHeadlineWatchUtils = api;

export default api;
