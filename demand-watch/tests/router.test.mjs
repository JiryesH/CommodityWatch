import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDemandConceptHref,
  buildDemandOverviewHref,
  parseDemandRoute,
} from "../router.js";

test("DemandWatch router preserves overview and concept detail paths", () => {
  assert.deepEqual(parseDemandRoute("/demand-watch/"), {
    view: "overview",
    verticalId: null,
    conceptCode: null,
  });

  assert.deepEqual(parseDemandRoute("/demand-watch/grains/USDA_US_CORN_TOTAL_USE_WASDE/"), {
    view: "detail",
    verticalId: "grains",
    conceptCode: "USDA_US_CORN_TOTAL_USE_WASDE",
  });

  assert.equal(buildDemandOverviewHref(), "/demand-watch/");
  assert.equal(
    buildDemandConceptHref("crude-products", "EIA_US_TOTAL_PRODUCT_SUPPLIED"),
    "/demand-watch/crude-products/EIA_US_TOTAL_PRODUCT_SUPPLIED/"
  );
});

test("DemandWatch router rejects unknown verticals and malformed paths", () => {
  assert.equal(parseDemandRoute("/demand-watch/not-a-vertical/concept/").view, "not-found");
  assert.equal(parseDemandRoute("/demand-watch/grains/").view, "not-found");
});
