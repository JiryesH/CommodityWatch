import test from "node:test";
import assert from "node:assert/strict";

import {
  fetchDemandConceptDetail,
  fetchDemandScorecard,
  fetchDemandWatchPageData,
  resetDemandWatchPageDataCache,
} from "../api-client.js";

function jsonResponse(payload, { ok = true, status = 200 } = {}) {
  return {
    ok,
    status,
    text: async () => JSON.stringify(payload),
  };
}

async function withMockFetch(handler, fn) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = handler;
  resetDemandWatchPageDataCache();

  try {
    await fn();
  } finally {
    resetDemandWatchPageDataCache();
    globalThis.fetch = originalFetch;
  }
}

function demandBootstrapPayload({
  verticalDetails = [],
  verticalErrors = [],
  coverageVerticals = [],
  expiresAt = "2099-04-08T00:05:00Z",
} = {}) {
  return {
    module: "demandwatch",
    generated_at: "2099-04-08T00:00:00Z",
    expires_at: expiresAt,
    macro_strip: {
      generated_at: "2099-04-08T00:00:00Z",
      items: [],
    },
    scorecard: {
      generated_at: "2099-04-08T00:00:00Z",
      items: [],
    },
    movers: {
      generated_at: "2099-04-08T00:00:00Z",
      items: [],
    },
    coverage_notes: {
      generated_at: "2099-04-08T00:00:00Z",
      markdown: "",
      summary: {
        vertical_count: 4,
        series_count: 11,
        status_counts: {
          live: 8,
          partial: 0,
          deferred: 3,
          blocked: 0,
        },
      },
      verticals: coverageVerticals,
    },
    vertical_details: verticalDetails.map((item) => ({
      generated_at: "2099-04-08T00:00:00Z",
      ...item,
    })),
    vertical_errors: verticalErrors,
    next_release_dates: {
      generated_at: "2099-04-08T00:00:00Z",
      items: [],
    },
  };
}

test("fetchDemandScorecard rejects malformed payloads", async () => {
  await withMockFetch(
    async () =>
      jsonResponse({
        generated_at: "2026-04-08T00:00:00Z",
        items: {},
      }),
    async () => {
      await assert.rejects(fetchDemandScorecard(), /scorecard payload is invalid/i);
    }
  );
});

test("fetchDemandWatchPageData boots from one snapshot request and keeps transparent vertical errors", async () => {
  const requestedUrls = [];
  await withMockFetch(
    async (url) => {
      requestedUrls.push(url);

      if (url === "/api/snapshot/demandwatch") {
        return jsonResponse(
          demandBootstrapPayload({
            coverageVerticals: [
              { id: "crude-products" },
              { id: "electricity" },
              { id: "grains" },
              { id: "base-metals" },
            ],
            verticalDetails: [
              { id: "crude-products", code: "crude_products" },
              { id: "electricity", code: "electricity" },
              { id: "grains", code: "grains_oilseeds" },
            ],
            verticalErrors: [{ vertical_id: "base-metals", message: "Synthetic failure for test." }],
          })
        );
      }

      throw new Error(`Unexpected URL: ${url}`);
    },
    async () => {
      const payload = await fetchDemandWatchPageData();

      assert.equal(payload.verticalDetails.length, 3);
      assert.deepEqual(payload.verticalDetails.map((item) => item.id), ["crude-products", "electricity", "grains"]);
      assert.deepEqual(payload.verticalErrors, [
        {
          verticalId: "base-metals",
          message: "Synthetic failure for test.",
        },
      ]);
      assert.deepEqual(requestedUrls, ["/api/snapshot/demandwatch"]);
    }
  );
});

test("fetchDemandWatchPageData reuses a fresh bootstrap payload from cache", async () => {
  let requestCount = 0;

  await withMockFetch(
    async (url) => {
      requestCount += 1;
      assert.equal(url, "/api/snapshot/demandwatch");
      return jsonResponse(
        demandBootstrapPayload({
          coverageVerticals: [{ id: "crude-products" }],
          verticalDetails: [{ id: "crude-products", code: "crude_products" }],
        })
      );
    },
    async () => {
      const firstPayload = await fetchDemandWatchPageData();
      const secondPayload = await fetchDemandWatchPageData();

      assert.equal(requestCount, 1);
      assert.deepEqual(secondPayload, firstPayload);
    }
  );
});

test("fetchDemandWatchPageData rejects mixed-version bootstrap payloads", async () => {
  await withMockFetch(
    async (url) => {
      assert.equal(url, "/api/snapshot/demandwatch");
      const payload = demandBootstrapPayload();
      payload.scorecard.generated_at = "2099-04-07T23:59:00Z";
      return jsonResponse(payload);
    },
    async () => {
      await assert.rejects(
        fetchDemandWatchPageData(),
        /bootstrap versions/i,
        "the client should reject mixed-version bootstrap payloads"
      );
    }
  );
});

test("fetchDemandWatchPageData rejects bootstrap payloads that omit a covered vertical from detail and error lists", async () => {
  await withMockFetch(
    async (url) => {
      assert.equal(url, "/api/snapshot/demandwatch");
      return jsonResponse(
        demandBootstrapPayload({
          coverageVerticals: [
            { id: "crude-products" },
            { id: "electricity" },
          ],
          verticalDetails: [{ id: "crude-products", code: "crude_products" }],
          verticalErrors: [],
        })
      );
    },
    async () => {
      await assert.rejects(
        fetchDemandWatchPageData(),
        /missing vertical detail coverage/i,
        "the client should reject incomplete bootstrap coverage"
      );
    }
  );
});

test("fetchDemandConceptDetail requests the routed concept endpoint", async () => {
  const requestedUrls = [];

  await withMockFetch(
    async (url) => {
      requestedUrls.push(url);
      return jsonResponse({
        code: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
      });
    },
    async () => {
      const payload = await fetchDemandConceptDetail("crude-products", "EIA_US_TOTAL_PRODUCT_SUPPLIED");
      assert.equal(payload.code, "EIA_US_TOTAL_PRODUCT_SUPPLIED");
      assert.deepEqual(requestedUrls, [
        "/api/demandwatch/verticals/crude-products/concepts/EIA_US_TOTAL_PRODUCT_SUPPLIED",
      ]);
    }
  );
});
