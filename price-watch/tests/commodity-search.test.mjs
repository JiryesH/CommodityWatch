import test from "node:test";
import assert from "node:assert/strict";

import { buildCommodityDefinitions } from "../commodity-presentation.js";
import {
  buildCommoditySearchText,
  getMatchingSeriesOptions,
  matchesCommoditySearch,
  normalizeCommoditySearchQuery,
} from "../commodity-search.js";

function buildDefinitions(seriesRows, latestRows) {
  return buildCommodityDefinitions(seriesRows, latestRows);
}

test("commodity search matches grouped coffee cards by benchmark label and subsector taxonomy", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "coffee_arabica_monthly_proxy",
        target_concept: "Coffee",
        actual_series_name: "Coffee, Other Mild Arabica",
        source_name: "FRED",
      },
      {
        series_key: "coffee_robusta_monthly_proxy",
        target_concept: "Coffee Robusta",
        actual_series_name: "Coffee, Robustas",
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
  assert.equal(getMatchingSeriesOptions(coffee, "coffee").length, 2);
  assert.equal(getMatchingSeriesOptions(coffee, "arabica").length, 1);
  assert.equal(getMatchingSeriesOptions(coffee, "soft commodities").length, 2);
  assert.equal(matchesCommoditySearch(coffee, "cocoa"), false);
});

test("commodity search counts grouped crude constituents by matching series metadata", () => {
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
    ],
    [
      { series_key: "crude_oil_brent", value: 71.23 },
      { series_key: "crude_oil_wti", value: 68.11 },
      { series_key: "crude_oil_dubai", value: 69.02 },
    ]
  );

  const crude = definitions.find((definition) => definition.id === "crude_benchmarks");
  assert.ok(crude);
  assert.equal(getMatchingSeriesOptions(crude, "wti").length, 1);
  assert.equal(getMatchingSeriesOptions(crude, "oman crude").length, 1);
  assert.equal(getMatchingSeriesOptions(crude, "crude oil benchmarks").length, 3);
});

test("commodity search matches grouped agricultural benchmark cards by constituent labels", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "sugar_no11_world_monthly_proxy",
        target_concept: "Sugar world",
        actual_series_name: "Sugar No. 11 world",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "sugar_eu_monthly",
        target_concept: "Sugar EU",
        actual_series_name: "Sugar Europe",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "sugar_us_monthly",
        target_concept: "Sugar US",
        actual_series_name: "Sugar United States",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "bananas_europe_monthly",
        target_concept: "Bananas Europe",
        actual_series_name: "Bananas Europe",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "bananas_us_monthly",
        target_concept: "Bananas US",
        actual_series_name: "Bananas United States",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "orange_monthly",
        target_concept: "Orange",
        actual_series_name: "Orange juice concentrate",
        source_name: "World Bank Pink Sheet",
      },
    ],
    [
      { series_key: "sugar_no11_world_monthly_proxy", value: 0.21 },
      { series_key: "sugar_eu_monthly", value: 0.51 },
      { series_key: "sugar_us_monthly", value: 0.64 },
      { series_key: "bananas_europe_monthly", value: 1260 },
      { series_key: "bananas_us_monthly", value: 1350 },
      { series_key: "orange_monthly", value: 221 },
    ]
  );

  const sugar = definitions.find((definition) => definition.id === "sugar_benchmarks");
  const bananas = definitions.find((definition) => definition.id === "banana_benchmarks");

  assert.ok(sugar);
  assert.equal(getMatchingSeriesOptions(sugar, "sugar eu").length, 1);
  assert.equal(getMatchingSeriesOptions(sugar, "soft commodities").length, 3);

  assert.ok(bananas);
  assert.equal(getMatchingSeriesOptions(bananas, "bananas us").length, 1);
  assert.equal(matchesCommoditySearch(bananas, "orange"), false);
});

test("commodity search indexes sector and subsector labels for standalone series", () => {
  const definitions = buildDefinitions(
    [
      {
        series_key: "urea_monthly",
        target_concept: "Urea",
        actual_series_name: "Urea",
        source_name: "World Bank Pink Sheet",
        source_series_code: "UREA_EE_BULK",
      },
    ],
    [{ series_key: "urea_monthly", value: 381 }]
  );

  const urea = definitions.find((definition) => definition.id === "urea_monthly");
  assert.ok(urea);
  assert.equal(normalizeCommoditySearchQuery("  Nitrogen / fertilizer  "), "nitrogen fertilizer");
  assert.match(buildCommoditySearchText(urea), /fertilizers and agricultural chemicals/);
  assert.equal(matchesCommoditySearch(urea, "nitrogen fertilizer"), true);
});
