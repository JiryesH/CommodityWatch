import { CommodityApiClient } from "../price-watch/commodities-client.js";
import { fetchJsonWithFallback, normalizeHeadlineFeedArticle } from "../shared/headline-feed.js";

const commodityClient = new CommodityApiClient("");
const historyCache = new Map();
const relatedHeadlinesCache = new Map();
const calendarCache = new Map();
let latestPromise = null;
let feedPromise = null;

function safeDateValue(value) {
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function sortNewest(first, second) {
  return safeDateValue(second.published) - safeDateValue(first.published);
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
    feedPromise = fetchJsonWithFallback()
      .then((payload) => {
        const articles = Array.isArray(payload?.articles)
          ? payload.articles.map(normalizeHeadlineFeedArticle).sort(sortNewest)
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
