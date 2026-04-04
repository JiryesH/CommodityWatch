const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");
const Module = require("node:module");

const frontendRoot = path.resolve(__dirname, "..");
const sourceRoot = path.join(frontendRoot, "src");
const originalResolveFilename = Module._resolveFilename;

Module._resolveFilename = function patchedResolveFilename(request, parent, isMain, options) {
  if (request.startsWith("@/")) {
    request = path.join(sourceRoot, request.slice(2));
  }
  return originalResolveFilename.call(this, request, parent, isMain, options);
};

require(path.join(frontendRoot, "node_modules", "sucrase", "register", "ts"));

const { fetchIndicatorData, fetchIndicatorLatest, fetchInventorySnapshot } = require(path.join(sourceRoot, "lib", "api", "inventory.ts"));

async function withJsonPayload(payload, callback) {
  const originalFetch = global.fetch;
  global.fetch = async () => ({
    ok: true,
    status: 200,
    json: async () => payload,
  });

  try {
    await callback();
  } finally {
    global.fetch = originalFetch;
  }
}

test("snapshot mapping keeps period end, release date, and CommodityWatch updated distinct", async () => {
  await withJsonPayload(
    {
      module: "inventorywatch",
      generated_at: "2026-03-28T08:00:00Z",
      expires_at: "2026-03-28T08:05:00Z",
      cards: [
        {
          indicator_id: "current-only",
          code: "EIA_CURRENT_ONLY_STOCKS",
          name: "Current only stocks",
          commodity_code: "crude_oil",
          geography_code: "US",
          latest_value: 438940,
          unit: "kb",
          frequency: "weekly",
          is_seasonal: false,
          change_abs: -3340,
          deviation_abs: null,
          signal: "tightening",
          sparkline: [442280, 438940],
          latest_period_end_at: "2026-03-20T00:00:00Z",
          latest_release_date: "2026-03-26T14:30:00Z",
          commoditywatch_updated_at: "2026-03-26T15:05:00Z",
          stale: false,
          seasonal_median: null,
          seasonal_p10: null,
          seasonal_p25: null,
          seasonal_p75: null,
          seasonal_p90: null,
        },
        {
          indicator_id: "seasonal-public",
          code: "EIA_SEASONAL_PUBLIC_STOCKS",
          name: "Seasonal public stocks",
          commodity_code: "crude_oil",
          geography_code: "US",
          latest_value: 628000,
          unit: "kb",
          frequency: "weekly",
          is_seasonal: true,
          change_abs: -3000,
          deviation_abs: 7988,
          signal: "tightening",
          sparkline: [631000, 628000],
          latest_period_end_at: "2026-03-20T00:00:00Z",
          latest_release_date: "2026-03-26T14:30:00Z",
          commoditywatch_updated_at: "2026-03-26T15:20:00Z",
          stale: false,
          seasonal_median: 620012,
          seasonal_p10: 610012,
          seasonal_p25: 615012,
          seasonal_p75: 625012,
          seasonal_p90: 630012,
        },
      ],
    },
    async () => {
      const snapshot = await fetchInventorySnapshot();
      const currentOnly = snapshot.cards.find((card) => card.code === "EIA_CURRENT_ONLY_STOCKS");
      const seasonal = snapshot.cards.find((card) => card.code === "EIA_SEASONAL_PUBLIC_STOCKS");

      assert.equal(currentOnly.periodEndAt, "2026-03-20T00:00:00Z");
      assert.equal(currentOnly.releaseDate, "2026-03-26T14:30:00Z");
      assert.equal(currentOnly.commodityWatchUpdatedAt, "2026-03-26T15:05:00Z");
      assert.equal(currentOnly.isSeasonal, false);
      assert.equal(currentOnly.seasonalMedian, null);

      assert.equal(seasonal.periodEndAt, "2026-03-20T00:00:00Z");
      assert.equal(seasonal.releaseDate, "2026-03-26T14:30:00Z");
      assert.equal(seasonal.commodityWatchUpdatedAt, "2026-03-26T15:20:00Z");
      assert.equal(seasonal.isSeasonal, true);
      assert.ok(Math.abs(seasonal.seasonalMedian - 620.012) < 1e-9);
      assert.ok(Math.abs(seasonal.seasonalP10 - 610.012) < 1e-9);
      assert.ok(Math.abs(seasonal.seasonalP90 - 630.012) < 1e-9);
    },
  );
});

test("latest mapping preserves current-only date semantics and seasonal nullability", async () => {
  await withJsonPayload(
    {
      indicator: { id: "current-only", code: "EIA_CURRENT_ONLY_STOCKS" },
      latest: {
        period_end_at: "2026-03-20T00:00:00Z",
        release_date: "2026-03-26T14:30:00Z",
        commoditywatch_updated_at: "2026-03-26T15:05:00Z",
        value: 438940,
        unit: "kb",
        change_from_prior_abs: -3340,
        change_from_prior_pct: -0.8,
        deviation_from_seasonal_abs: null,
        deviation_from_seasonal_zscore: null,
        revision_sequence: 1,
      },
    },
    async () => {
      const payload = await fetchIndicatorLatest("current-only");
      assert.equal(payload.latest.periodEndAt, "2026-03-20T00:00:00Z");
      assert.equal(payload.latest.releaseDate, "2026-03-26T14:30:00Z");
      assert.equal(payload.latest.commodityWatchUpdatedAt, "2026-03-26T15:05:00Z");
      assert.equal(payload.latest.deviationFromSeasonalAbs, null);
      assert.equal(payload.latest.deviationFromSeasonalZscore, null);
    },
  );
});

test("data mapping keeps metadata date semantics and seasonal gating explicit", async () => {
  await withJsonPayload(
    {
      indicator: {
        id: "seasonal-public",
        code: "EIA_SEASONAL_PUBLIC_STOCKS",
        name: "Seasonal public stocks",
        description: null,
        modules: ["inventorywatch"],
        commodity_code: "crude_oil",
        geography_code: "US",
        frequency: "weekly",
        measure_family: "stock",
        unit: "kb",
        period_type: "weekly",
        marketing_year_start_month: null,
        is_seasonal: true,
      },
      series: [
        {
          period_start_at: "2026-03-14T00:00:00Z",
          period_end_at: "2026-03-20T00:00:00Z",
          release_date: "2026-03-26T14:30:00Z",
          vintage_at: "2026-03-26T15:20:00Z",
          value: 628000,
          unit: "kb",
          observation_kind: "actual",
          revision_sequence: 1,
        },
      ],
      seasonal_range: [
        {
          period_index: 12,
          p10: 610012,
          p25: 615012,
          p50: 620012,
          p75: 625012,
          p90: 630012,
          mean: 620512,
          stddev: 4000,
        },
      ],
      metadata: {
        latest_release_id: null,
        latest_release_at: "2026-03-26T14:30:00Z",
        latest_period_end_at: "2026-03-20T00:00:00Z",
        latest_vintage_at: "2026-03-26T15:20:00Z",
        source_url: "https://inventory.example/test",
      },
    },
    async () => {
      const seasonal = await fetchIndicatorData("seasonal-public");
      assert.equal(seasonal.indicator.isSeasonal, true);
      assert.equal(seasonal.metadata.latestPeriodEndAt, "2026-03-20T00:00:00Z");
      assert.equal(seasonal.metadata.latestReleaseAt, "2026-03-26T14:30:00Z");
      assert.equal(seasonal.metadata.latestVintageAt, "2026-03-26T15:20:00Z");
      assert.equal(seasonal.seasonalRange.length, 1);
      assert.ok(Math.abs(seasonal.seasonalRange[0].p50 - 620.012) < 1e-9);
    },
  );

  await withJsonPayload(
    {
      indicator: {
        id: "current-only",
        code: "EIA_CURRENT_ONLY_STOCKS",
        name: "Current only stocks",
        description: null,
        modules: ["inventorywatch"],
        commodity_code: "crude_oil",
        geography_code: "US",
        frequency: "weekly",
        measure_family: "stock",
        unit: "kb",
        period_type: "weekly",
        marketing_year_start_month: null,
        is_seasonal: false,
      },
      series: [
        {
          period_start_at: "2026-03-14T00:00:00Z",
          period_end_at: "2026-03-20T00:00:00Z",
          release_date: "2026-03-26T14:30:00Z",
          vintage_at: "2026-03-26T15:05:00Z",
          value: 438940,
          unit: "kb",
          observation_kind: "actual",
          revision_sequence: 1,
        },
      ],
      seasonal_range: [],
      metadata: {
        latest_release_id: null,
        latest_release_at: "2026-03-26T14:30:00Z",
        latest_period_end_at: "2026-03-20T00:00:00Z",
        latest_vintage_at: "2026-03-26T15:05:00Z",
        source_url: "https://inventory.example/test",
      },
    },
    async () => {
      const currentOnly = await fetchIndicatorData("current-only");
      assert.equal(currentOnly.indicator.isSeasonal, false);
      assert.deepEqual(currentOnly.seasonalRange, []);
      assert.equal(currentOnly.metadata.latestPeriodEndAt, "2026-03-20T00:00:00Z");
      assert.equal(currentOnly.metadata.latestVintageAt, "2026-03-26T15:05:00Z");
    },
  );
});

test("snapshot contract rejects missing period-end semantics instead of falling back", async () => {
  await withJsonPayload(
    {
      module: "inventorywatch",
      generated_at: "2026-03-28T08:00:00Z",
      expires_at: "2026-03-28T08:05:00Z",
      cards: [
        {
          indicator_id: "broken-card",
          code: "EIA_CURRENT_ONLY_STOCKS",
          name: "Broken card",
          commodity_code: "crude_oil",
          geography_code: "US",
          latest_value: 438940,
          unit: "kb",
          frequency: "weekly",
          is_seasonal: false,
          change_abs: -3340,
          deviation_abs: null,
          signal: "tightening",
          sparkline: [442280, 438940],
          latest_release_date: "2026-03-26T14:30:00Z",
          commoditywatch_updated_at: "2026-03-26T15:05:00Z",
          stale: false,
          seasonal_median: null,
          seasonal_p10: null,
          seasonal_p25: null,
          seasonal_p75: null,
          seasonal_p90: null,
        },
      ],
    },
    async () => {
      await assert.rejects(
        fetchInventorySnapshot(),
        (error) => error?.name === "ZodError" && JSON.stringify(error.issues).includes("latest_period_end_at"),
      );
    },
  );
});

test("latest and data contracts reject missing explicit updated timestamps", async () => {
  await withJsonPayload(
    {
      indicator: { id: "current-only", code: "EIA_CURRENT_ONLY_STOCKS" },
      latest: {
        period_end_at: "2026-03-20T00:00:00Z",
        release_date: "2026-03-26T14:30:00Z",
        value: 438940,
        unit: "kb",
        change_from_prior_abs: -3340,
        change_from_prior_pct: -0.8,
        deviation_from_seasonal_abs: null,
        deviation_from_seasonal_zscore: null,
        revision_sequence: 1,
      },
    },
    async () => {
      await assert.rejects(
        fetchIndicatorLatest("current-only"),
        (error) => error?.name === "FrontendApiError" && error.message === "Response validation failed.",
      );
    },
  );

  await withJsonPayload(
    {
      indicator: {
        id: "current-only",
        code: "EIA_CURRENT_ONLY_STOCKS",
        name: "Current only stocks",
        description: null,
        modules: ["inventorywatch"],
        commodity_code: "crude_oil",
        geography_code: "US",
        frequency: "weekly",
        measure_family: "stock",
        unit: "kb",
        period_type: "weekly",
        marketing_year_start_month: null,
        is_seasonal: false,
      },
      series: [
        {
          period_start_at: "2026-03-14T00:00:00Z",
          period_end_at: "2026-03-20T00:00:00Z",
          release_date: "2026-03-26T14:30:00Z",
          vintage_at: "2026-03-26T15:05:00Z",
          value: 438940,
          unit: "kb",
          observation_kind: "actual",
          revision_sequence: 1,
        },
      ],
      seasonal_range: [],
      metadata: {
        latest_release_id: null,
        latest_release_at: "2026-03-26T14:30:00Z",
        latest_period_end_at: "2026-03-20T00:00:00Z",
        source_url: "https://inventory.example/test",
      },
    },
    async () => {
      await assert.rejects(
        fetchIndicatorData("current-only"),
        (error) => error?.name === "ZodError" && JSON.stringify(error.issues).includes("latest_vintage_at"),
      );
    },
  );
});
