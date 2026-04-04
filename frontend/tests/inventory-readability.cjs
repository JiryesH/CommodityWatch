const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");
const Module = require("node:module");

const frontendRoot = path.resolve(__dirname, "..");
const sourceRoot = path.join(frontendRoot, "src");
const React = require(path.join(frontendRoot, "node_modules", "react"));
const { renderToStaticMarkup } = require(path.join(frontendRoot, "node_modules", "react-dom", "server"));
global.React = React;
const originalResolveFilename = Module._resolveFilename;
const originalLoad = Module._load;

Module._resolveFilename = function patchedResolveFilename(request, parent, isMain, options) {
  if (request.startsWith("@/")) {
    request = path.join(sourceRoot, request.slice(2));
  }
  return originalResolveFilename.call(this, request, parent, isMain, options);
};

Module._load = function patchedLoad(request, parent, isMain) {
  if (request === "next/link") {
    return function Link(props) {
      const { href, children, ...rest } = props;
      return React.createElement("a", { href: typeof href === "string" ? href : String(href), ...rest }, children);
    };
  }
  return originalLoad.call(this, request, parent, isMain);
};

require(path.join(frontendRoot, "node_modules", "sucrase", "register", "ts"));
require(path.join(frontendRoot, "node_modules", "sucrase", "register", "tsx"));

const {
  buildVisibleSnapshotSections,
  changeReferenceLabel,
  trendWindowLabel,
} = require(path.join(sourceRoot, "features", "inventory", "selectors.ts"));
const { IndicatorCard } = require(path.join(sourceRoot, "components", "shared", "indicator-card.tsx"));

test("change labels stay specific to cadence", () => {
  assert.equal(changeReferenceLabel({ frequency: "weekly" }), "vs prior week");
  assert.equal(changeReferenceLabel({ frequency: "monthly" }), "vs prior month");
  assert.equal(changeReferenceLabel({ frequency: "daily" }), "vs prior day");
  assert.equal(changeReferenceLabel({ frequency: null, periodType: null }), "vs prior period");
});

test("trend label explains the observation window", () => {
  assert.equal(trendWindowLabel(12), "Trend · last 12 observations");
  assert.equal(trendWindowLabel(1), "Trend · last 1 observation");
  assert.equal(trendWindowLabel(0), "Trend unavailable");
});

test("filtered snapshot sections drop empty groups", () => {
  const sections = buildVisibleSnapshotSections(
    {
      "Crude Oil": [{ indicatorId: "1" }],
      "Refined Products": [],
      "Natural Gas": [{ indicatorId: "2" }],
    },
    ["Crude Oil", "Refined Products", "Natural Gas"],
  );

  assert.deepEqual(
    sections.map(([name]) => name),
    ["Crude Oil", "Natural Gas"],
  );
});

test("indicator card markup explains latest value, change reference, and trend window", () => {
  const markup = renderToStaticMarkup(
    React.createElement(IndicatorCard, {
      href: "/inventory/energy/test",
      card: {
        indicatorId: "test",
        code: "EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR",
        name: "Commercial crude stocks",
        commodityCode: "crude_oil",
        geographyCode: "US",
        frequency: "weekly",
        periodType: "weekly",
        marketingYearStartMonth: null,
        isSeasonal: true,
        latestValue: 420,
        unit: "mb",
        changeAbs: -3,
        deviationAbs: 5,
        seasonalMedian: 415,
        seasonalP10: 400,
        seasonalP25: 405,
        seasonalP75: 425,
        seasonalP90: 430,
        signal: "tightening",
        sparkline: [410, 412, 414, 418, 420, 421, 419, 417, 416, 415, 418, 420],
        periodEndAt: "2026-03-27T00:00:00+00:00",
        releaseDate: "2026-04-03T14:00:00+00:00",
        commodityWatchUpdatedAt: "2026-04-03T14:00:00+00:00",
        freshness: "current",
        stale: false,
        sourceLabel: "EIA",
        sourceHref: "https://example.com",
        description: "Weekly stocks",
        semanticMode: "inventory",
        snapshotGroup: "Crude Oil",
      },
    }),
  );

  assert.match(markup, /Latest value/);
  assert.match(markup, /vs prior week/);
  assert.match(markup, /Trend · last 12 observations/);
});
