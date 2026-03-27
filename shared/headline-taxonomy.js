import contract from "./headline-taxonomy.json" with { type: "json" };

function freezeArrayRecord(record) {
  return Object.freeze(
    Object.fromEntries(Object.entries(record).map(([key, values]) => [key, Object.freeze([...values])]))
  );
}

function freezeObjectRecord(record) {
  return Object.freeze(
    Object.fromEntries(Object.entries(record).map(([key, value]) => [key, Object.freeze({ ...value })]))
  );
}

export const CANONICAL_CATEGORIES = Object.freeze([...contract.canonical_categories]);
export const CATEGORY_PRIORITY = Object.freeze(
  Object.fromEntries(CANONICAL_CATEGORIES.map((category, index) => [category, index]))
);
export const CAT_COLOR = Object.freeze({ ...contract.color_classes });
export const CAT_LABEL = Object.freeze({ ...contract.short_labels });
export const SECTOR_MAP = freezeArrayRecord(contract.sector_map);
export const ENERGY_CATS = SECTOR_MAP.energy || Object.freeze([]);
export const DASHBOARD_CATEGORY_TAGS = freezeObjectRecord(contract.dashboard_category_tags);
export const ALWAYS_RELEVANT_CATEGORIES = Object.freeze(
  Object.entries(DASHBOARD_CATEGORY_TAGS)
    .filter(([, metadata]) => metadata.alwaysRelevant)
    .map(([category]) => category)
);

function hasCanonicalCategory(category) {
  return Object.prototype.hasOwnProperty.call(CATEGORY_PRIORITY, category);
}

export function canonicalCategoriesForArticle(article) {
  const raw = article?.categories;
  let tokens = [];

  if (Array.isArray(raw)) {
    tokens = raw.slice();
  } else if (article?.category) {
    tokens = String(article.category).split(",");
  }

  const categories = [];
  const seen = new Set();

  tokens.forEach((token) => {
    const category = String(token || "").trim();
    if (!category || !hasCanonicalCategory(category) || seen.has(category)) {
      return;
    }

    seen.add(category);
    categories.push(category);
  });

  if (!categories.length) {
    categories.push("General");
  }

  categories.sort((left, right) => CATEGORY_PRIORITY[left] - CATEGORY_PRIORITY[right]);
  return categories;
}

export function normalizeArticleCategoriesInPlace(article) {
  if (!article || typeof article !== "object") {
    return article;
  }

  const categories = canonicalCategoriesForArticle(article);
  article.categories = categories;
  article.category = categories[0];
  return article;
}

export function dotClass(article) {
  const categoryColorPriority = {
    energy: 3,
    metals: 2,
    agri: 1,
    other: 0,
  };
  let best = "other";

  canonicalCategoriesForArticle(article).forEach((category) => {
    const color = CAT_COLOR[category] || "other";
    if (categoryColorPriority[color] > categoryColorPriority[best]) {
      best = color;
    }
  });

  return best;
}

export function dotLabel(article) {
  const primaryCategory = canonicalCategoriesForArticle(article)[0] || "General";
  return CAT_LABEL[primaryCategory] || primaryCategory;
}

export function getDashboardCategoryTag(category) {
  return DASHBOARD_CATEGORY_TAGS[category] || null;
}

const api = Object.freeze({
  CANONICAL_CATEGORIES,
  CATEGORY_PRIORITY,
  CAT_COLOR,
  CAT_LABEL,
  ENERGY_CATS,
  SECTOR_MAP,
  DASHBOARD_CATEGORY_TAGS,
  ALWAYS_RELEVANT_CATEGORIES,
  canonicalCategoriesForArticle,
  normalizeArticleCategoriesInPlace,
  dotClass,
  dotLabel,
  getDashboardCategoryTag,
});

globalThis.CommodityWatchHeadlineTaxonomy = api;

export default api;
