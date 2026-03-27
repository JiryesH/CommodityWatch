import { normalizeArticleCategoriesInPlace } from "./headline-taxonomy.js";

export const HEADLINE_FEED_URLS = Object.freeze(["/data/feed.local.json", "/data/feed.json"]);

function withCacheBuster(url, token) {
  const separator = String(url).includes("?") ? "&" : "?";
  return `${url}${separator}t=${token}`;
}

export function normalizeHeadlineFeedArticle(article) {
  const normalizedArticle = {
    ...(article && typeof article === "object" ? article : {}),
    id: article?.id || null,
    title: article?.title || "",
    description: article?.description || "",
    link: article?.link || "",
    published: article?.published || null,
    source: article?.source || "Unknown source",
    sentiment: article?.sentiment || null,
    ner: article?.ner || null,
  };

  if (Array.isArray(article?.categories)) {
    normalizedArticle.categories = article.categories.filter(Boolean);
  } else {
    delete normalizedArticle.categories;
  }

  return normalizeArticleCategoriesInPlace(normalizedArticle);
}

export async function fetchJsonWithFallback(
  urls = HEADLINE_FEED_URLS,
  { fetchImpl = globalThis.fetch, headers = { Accept: "application/json" }, ...requestInit } = {}
) {
  if (typeof fetchImpl !== "function") {
    throw new Error("A fetch implementation is required");
  }

  const cacheToken = Date.now();
  let lastError = null;

  for (const url of urls) {
    try {
      const response = await fetchImpl(withCacheBuster(url, cacheToken), {
        ...requestInit,
        headers,
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
