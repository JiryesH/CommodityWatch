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

const {
  alertKindFromSeasonalPosition,
  alertKindFromSnapshotCard,
  describeMedianDeviation,
  snapshotSeasonalComparisonText,
} = require(path.join(sourceRoot, "features", "inventory", "selectors.ts"));

test("snapshot comparison text says median and not avg", () => {
  const comparison = snapshotSeasonalComparisonText({
    deviationAbs: 12.5,
    unit: "mb",
    seasonalMedian: 400,
    seasonalP10: 360,
    seasonalP25: 380,
    seasonalP75: 420,
    seasonalP90: 440,
  });

  assert.equal(comparison.title, "vs 5Y median");
  assert.match(comparison.value, /above median$/);
  assert.doesNotMatch(comparison.value, /avg/i);
});

test("non-seasonal cards render an explicit current-only state", () => {
  const comparison = snapshotSeasonalComparisonText({
    deviationAbs: null,
    unit: "mb",
    seasonalMedian: null,
    seasonalP10: null,
    seasonalP25: null,
    seasonalP75: null,
    seasonalP90: null,
  });

  assert.deepEqual(comparison, {
    title: "Seasonal baseline",
    value: "Current only",
  });
});

test("snapshot and detail use the same percentile extreme rule", () => {
  const seasonalPoint = {
    p10: 90,
    p25: 100,
    p50: 120,
    p75: 140,
    p90: 150,
  };

  assert.equal(alertKindFromSeasonalPosition(85, seasonalPoint), "extreme-low");
  assert.equal(alertKindFromSeasonalPosition(155, seasonalPoint), "extreme-high");
  assert.equal(
    alertKindFromSnapshotCard({
      latestValue: 85,
      seasonalMedian: 120,
      seasonalP10: 90,
      seasonalP25: 100,
      seasonalP75: 140,
      seasonalP90: 150,
    }),
    "extreme-low",
  );
  assert.equal(
    alertKindFromSnapshotCard({
      latestValue: 155,
      seasonalMedian: 120,
      seasonalP10: 90,
      seasonalP25: 100,
      seasonalP75: 140,
      seasonalP90: 150,
    }),
    "extreme-high",
  );
});

test("median deviation helper stays scan-friendly", () => {
  assert.equal(describeMedianDeviation(0, "mb"), "At median");
  assert.equal(describeMedianDeviation(null, "mb"), "Median unavailable");
  assert.match(describeMedianDeviation(-8, "mb"), /below median$/);
});
