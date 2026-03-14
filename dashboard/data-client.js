import { CommodityApiClient } from "../price-watch/commodities-client.js";
import { FEED_URLS } from "./config.js";

const commodityClient = new CommodityApiClient("");
const historyCache = new Map();
const relatedHeadlinesCache = new Map();
const calendarCache = new Map();
let latestPromise = null;
let feedPromise = null;

function getHeadlineTaxonomy() {
  return globalThis.window?.ContangoHeadlineTaxonomy || globalThis.ContangoHeadlineTaxonomy || null;
}

function safeDateValue(value) {
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function sortNewest(first, second) {
  return safeDateValue(second.published) - safeDateValue(first.published);
}

function normalizeFeedArticle(article) {
  const categories = Array.isArray(article?.categories)
    ? article.categories.filter(Boolean)
    : article?.category
      ? [article.category]
      : [];

  const normalizedArticle = {
    id: article?.id || null,
    title: article?.title || "",
    description: article?.description || "",
    link: article?.link || "",
    published: article?.published || null,
    source: article?.source || "Unknown source",
    categories,
    sentiment: article?.sentiment || null,
    ner: article?.ner || null,
  };

  const headlineTaxonomy = getHeadlineTaxonomy();
  if (headlineTaxonomy?.normalizeArticleCategoriesInPlace) {
    return headlineTaxonomy.normalizeArticleCategoriesInPlace(normalizedArticle);
  }

  return normalizedArticle;
}

async function fetchJsonWithFallback(urls) {
  let lastError = null;

  for (const url of urls) {
    try {
      const response = await fetch(`${url}?t=${Date.now()}`, {
        headers: {
          Accept: "application/json",
        },
      });

      if (!response.ok) {
        lastError = new Error(`Request failed for ${url}: ${response.status}`);
        continue;
      }

      return response.json();
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Unable to load JSON payload");
}

export async function fetchLatestSeries() {
  if (!latestPromise) {
    latestPromise = commodityClient.listLatest().catch((error) => {
      latestPromise = null;
      throw error;
    });
  }

  return latestPromise;
}

export async function fetchLatestSeriesMap() {
  const rows = await fetchLatestSeries();
  return new Map(rows.map((row) => [row.series_key, row]));
}

export async function fetchSeriesHistory(seriesKey) {
  if (!historyCache.has(seriesKey)) {
    historyCache.set(
      seriesKey,
      commodityClient.getHistory(seriesKey).catch((error) => {
        historyCache.delete(seriesKey);
        throw error;
      })
    );
  }

  return historyCache.get(seriesKey);
}

export async function fetchRelatedHeadlines(seriesKey, limit = 8) {
  const cacheKey = `${seriesKey}:${limit}`;
  if (!relatedHeadlinesCache.has(cacheKey)) {
    relatedHeadlinesCache.set(
      cacheKey,
      commodityClient.getRelatedHeadlines(seriesKey, { limit }).catch((error) => {
        relatedHeadlinesCache.delete(cacheKey);
        throw error;
      })
    );
  }

  return relatedHeadlinesCache.get(cacheKey);
}

export async function fetchCalendarEvents({ from, to, sectors = [] }) {
  const params = new URLSearchParams();
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  if (sectors.length) {
    params.set("sectors", sectors.join(","));
  }

  const cacheKey = params.toString();
  if (!calendarCache.has(cacheKey)) {
    calendarCache.set(
      cacheKey,
      fetch(`/api/calendar?${params.toString()}`, {
        headers: {
          Accept: "application/json",
        },
      })
        .then(async (response) => {
          if (!response.ok) {
            throw new Error(`Calendar API request failed with ${response.status}`);
          }

          const payload = await response.json();
          if (!Array.isArray(payload?.data)) {
            throw new Error("Calendar API returned an unexpected payload");
          }

          return payload.data;
        })
        .catch((error) => {
          calendarCache.delete(cacheKey);
          throw error;
        })
    );
  }

  return calendarCache.get(cacheKey);
}

export async function fetchHeadlineFeed() {
  if (!feedPromise) {
    feedPromise = fetchJsonWithFallback(FEED_URLS)
      .then((payload) => {
        const articles = Array.isArray(payload?.articles)
          ? payload.articles.map(normalizeFeedArticle).sort(sortNewest)
          : [];
        const byId = new Map(articles.filter((article) => article.id).map((article) => [article.id, article]));
        return {
          articles,
          byId,
          metadata: payload?.metadata || {},
        };
      })
      .catch((error) => {
        feedPromise = null;
        throw error;
      });
  }

  return feedPromise;
}
