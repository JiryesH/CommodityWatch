(function attachHeadlineTaxonomy(globalScope) {
  var CANONICAL_CATEGORIES = [
    "Oil - Crude",
    "Oil - Refined Products",
    "Natural Gas",
    "LNG",
    "Coal",
    "Electric Power",
    "Energy Transition",
    "Chemicals",
    "Metals",
    "Agriculture",
    "Fertilizers",
    "Shipping",
    "General",
  ];

  var CATEGORY_PRIORITY = {};
  CANONICAL_CATEGORIES.forEach(function (category, index) {
    CATEGORY_PRIORITY[category] = index;
  });

  var CAT_COLOR = {
    "Oil - Crude": "energy",
    "Oil - Refined Products": "energy",
    "Natural Gas": "energy",
    LNG: "energy",
    Coal: "energy",
    "Electric Power": "energy",
    "Energy Transition": "energy",
    Chemicals: "energy",
    Metals: "metals",
    Agriculture: "agri",
    Fertilizers: "agri",
    Shipping: "other",
    General: "other",
  };

  var CAT_LABEL = {
    General: "General",
    "Oil - Crude": "Crude",
    "Oil - Refined Products": "Refined",
    "Natural Gas": "Nat Gas",
    "Electric Power": "Power",
    "Energy Transition": "Energy Trans",
  };

  var ENERGY_CATS = [
    "Oil - Crude",
    "Oil - Refined Products",
    "Natural Gas",
    "LNG",
    "Coal",
    "Electric Power",
    "Energy Transition",
  ];

  var SECTOR_MAP = {
    energy: ENERGY_CATS,
    chemicals: ["Chemicals"],
    metals_and_mining: ["Metals"],
    agriculture: ["Agriculture"],
    fertilizers: ["Fertilizers"],
    shipping: ["Shipping"],
  };

  function canonicalCategoriesForArticle(article) {
    var raw = article && article.categories;
    var tokens = [];

    if (Array.isArray(raw)) {
      tokens = raw.slice();
    } else if (article && article.category) {
      tokens = String(article.category).split(",");
    }

    var out = [];
    var seen = new Set();
    tokens.forEach(function (token) {
      var category = String(token || "").trim();
      if (!category || !Object.prototype.hasOwnProperty.call(CATEGORY_PRIORITY, category) || seen.has(category)) {
        return;
      }

      seen.add(category);
      out.push(category);
    });

    if (out.length === 0) {
      out.push("General");
    }

    out.sort(function (left, right) {
      return CATEGORY_PRIORITY[left] - CATEGORY_PRIORITY[right];
    });

    return out;
  }

  function normalizeArticleCategoriesInPlace(article) {
    var categories = canonicalCategoriesForArticle(article);
    article.categories = categories;
    article.category = categories[0];
    return article;
  }

  function dotClass(article) {
    var categories = canonicalCategoriesForArticle(article);
    var priority = { energy: 3, metals: 2, agri: 1, other: 0 };
    var best = "other";

    categories.forEach(function (category) {
      var color = CAT_COLOR[category] || "other";
      if (priority[color] > priority[best]) {
        best = color;
      }
    });

    return best;
  }

  function dotLabel(article) {
    var primary = canonicalCategoriesForArticle(article)[0] || "General";
    return CAT_LABEL[primary] || primary;
  }

  globalScope.ContangoHeadlineTaxonomy = {
    CANONICAL_CATEGORIES: CANONICAL_CATEGORIES,
    CATEGORY_PRIORITY: CATEGORY_PRIORITY,
    CAT_COLOR: CAT_COLOR,
    CAT_LABEL: CAT_LABEL,
    ENERGY_CATS: ENERGY_CATS,
    SECTOR_MAP: SECTOR_MAP,
    canonicalCategoriesForArticle: canonicalCategoriesForArticle,
    normalizeArticleCategoriesInPlace: normalizeArticleCategoriesInPlace,
    dotClass: dotClass,
    dotLabel: dotLabel,
  };
})(window);
