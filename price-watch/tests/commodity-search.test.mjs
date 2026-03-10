import test from "node:test";
import assert from "node:assert/strict";

import { buildCommodityDefinitions } from "../commodity-presentation.js";
import {
  buildCommoditySearchText,
  matchesCommoditySearch,
  normalizeCommoditySearchQuery,
} from "../commodity-search.js";

function buildDefinitions(seriesRows, latestRows) {
  return buildCommodityDefinitions(seriesRows, latestRows);
}

test("commodity search matches grouped coffee cards by parent label and constituent series", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "coffee_arabica_monthly_proxy",
        target_concept: "Arabica Coffee",
        actual_series_name: "Coffee, Arabica",
        source_name: "FRED",
      },
      {
        series_key: "coffee_robusta_monthly_proxy",
        target_concept: "Robusta Coffee",
        actual_series_name: "Coffee, Robusta",
        source_name: "FRED",
      },
      {
        series_key: "cocoa_monthly_proxy",
        target_concept: "Cocoa",
        actual_series_name: "Cocoa price",
        source_name: "FRED",
      },
    ],
    [
      { series_key: "coffee_arabica_monthly_proxy", value: 6.24 },
      { series_key: "coffee_robusta_monthly_proxy", value: 4.17 },
      { series_key: "cocoa_monthly_proxy", value: 9200 },
    ]
  );

  const coffee = definitions.find((definition) => definition.id === "coffee_benchmarks");
  assert.ok(coffee);
  assert.equal(matchesCommoditySearch(coffee, "coffee"), true);
  assert.equal(matchesCommoditySearch(coffee, "arabica"), true);
  assert.equal(matchesCommoditySearch(coffee, "robusta coffee"), true);
  assert.equal(matchesCommoditySearch(coffee, "cocoa"), false);
});

test("commodity search matches grouped benchmark cards from active series labels and metadata", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "crude_oil_brent",
        target_concept: "Brent Crude Oil",
        actual_series_name: "Crude Oil Prices: Brent - Europe",
        source_name: "FRED",
      },
      {
        series_key: "crude_oil_wti",
        target_concept: "WTI Crude Oil",
        actual_series_name: "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
        source_name: "FRED",
      },
      {
        series_key: "crude_oil_dubai",
        target_concept: "Dubai / Oman Crude Oil",
        actual_series_name: "Global price of Dubai Crude",
        source_name: "FRED",
      },
      {
        series_key: "rbob_gasoline_spot_proxy",
        target_concept: "RBOB Gasoline",
        actual_series_name: "RBOB spot gasoline",
        source_name: "FRED",
      },
    ],
    [
      { series_key: "crude_oil_brent", value: 71.23 },
      { series_key: "crude_oil_wti", value: 68.11 },
      { series_key: "crude_oil_dubai", value: 69.02 },
      { series_key: "rbob_gasoline_spot_proxy", value: 2.13 },
    ]
  );

  const crude = definitions.find((definition) => definition.id === "crude_benchmarks");
  assert.ok(crude);
  assert.equal(matchesCommoditySearch(crude, "wti"), true);
  assert.equal(matchesCommoditySearch(crude, "oman crude"), true);
  assert.equal(matchesCommoditySearch(crude, "gasoline"), false);
});

test("commodity search normalizes punctuation and surfaces useful aggregated metadata", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "natural_gas_ttf",
        target_concept: "TTF Natural Gas",
        actual_series_name: "Dutch TTF Natural Gas Forward",
        source_name: "FRED",
        source_series_code: "DTTFNG",
      },
    ],
    [{ series_key: "natural_gas_ttf", value: 9.84 }]
  );

  const gas = definitions.find((definition) => definition.id === "natural_gas_benchmarks");
  assert.ok(gas);
  assert.equal(normalizeCommoditySearchQuery("  TTF / gas  "), "ttf gas");
  assert.match(buildCommoditySearchText(gas), /dttfng/);
  assert.equal(matchesCommoditySearch(gas, "TTF / gas"), true);
});
