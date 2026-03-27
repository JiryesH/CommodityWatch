import test from "node:test";
import assert from "node:assert/strict";

import { fetchJsonWithFallback, normalizeHeadlineFeedArticle } from "../../shared/headline-feed.js";

test("headline feed fetch falls back across configured URLs", async () => {
  const seenUrls = [];
  const payload = { articles: [{ id: "a-1", title: "WTI rises", category: "Oil - Crude" }] };

  const result = await fetchJsonWithFallback(["/missing.json", "/feed.json"], {
    fetchImpl: async (url) => {
      seenUrls.push(url);
      if (String(url).startsWith("/missing.json?")) {
        return { ok: false, status: 404 };
      }

      return {
        ok: true,
        async json() {
          return payload;
        },
      };
    },
  });

  assert.deepEqual(result, payload);
  assert.equal(seenUrls.length, 2);
  assert.match(seenUrls[0], /^\/missing\.json\?t=\d+$/);
  assert.match(seenUrls[1], /^\/feed\.json\?t=\d+$/);
  assert.equal(seenUrls[0].split("=").at(-1), seenUrls[1].split("=").at(-1));
});

test("headline feed normalization applies defaults and canonical category ordering", () => {
  assert.deepEqual(
    normalizeHeadlineFeedArticle({
      id: "a-1",
      title: "Shipping update",
      category: "Shipping, Oil - Crude",
      source: "",
    }),
    {
      id: "a-1",
      title: "Shipping update",
      description: "",
      link: "",
      published: null,
      source: "Unknown source",
      categories: ["Oil - Crude", "Shipping"],
      category: "Oil - Crude",
      sentiment: null,
      ner: null,
    }
  );
});
