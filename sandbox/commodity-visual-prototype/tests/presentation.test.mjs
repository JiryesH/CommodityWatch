import test from "node:test";
import assert from "node:assert/strict";

import { buildCommodityDefinitions } from "../commodity-presentation.js";

test("buildCommodityDefinitions keeps published series metadata while shortening outer card labels", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "lng_asia_japan_import_proxy",
        target_concept: "JKM LNG",
        actual_series_name: "Global price of LNG, Asia",
        benchmark_series: "JKM LNG",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PNGASJPUSDM",
        source_url: "https://fred.example/lng",
        frequency: "monthly",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "Asia",
        active: true,
        notes: "Related published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "gold_worldbank_monthly",
        target_concept: "Gold",
        actual_series_name: "Gold",
        benchmark_series: "Gold",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "PGOLDUSDM",
        source_url: "https://fred.example/gold",
        frequency: "monthly",
        unit: "USD per troy ounce",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Exact published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ],
    [
      {
        series_key: "lng_asia_japan_import_proxy",
        target_concept: "JKM LNG",
        actual_series_name: "Global price of LNG, Asia",
        benchmark_series: "JKM LNG",
        match_type: "related",
        observation_date: "2026-02-01",
        value: 10.435,
        unit: "USD per MMBtu",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PNGASJPUSDM",
        source_url: "https://fred.example/lng",
        geography: "Asia",
        updated_at: "2026-03-08T10:30:00Z",
        notes: "Related published series",
        previous_value: 9.8,
        delta_value: 0.635,
        delta_pct: 6.48,
      },
      {
        series_key: "gold_worldbank_monthly",
        target_concept: "Gold",
        actual_series_name: "Gold",
        benchmark_series: "Gold",
        match_type: "exact",
        observation_date: "2026-02-01",
        value: 2250.1,
        unit: "USD per troy ounce",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PGOLDUSDM",
        source_url: "https://fred.example/gold",
        geography: "Global",
        updated_at: "2026-03-08T10:30:00Z",
        notes: "Exact published series",
        previous_value: 2210,
        delta_value: 40.1,
        delta_pct: 1.81,
      },
    ]
  );

  const lng = definitions.find((definition) => definition.id === "lng_asia_japan_import_proxy");
  const gold = definitions.find((definition) => definition.id === "gold_worldbank_monthly");

  assert.ok(lng);
  assert.equal(lng.primaryLabel, "LNG");
  assert.equal(lng.seriesOptions[0].actualSeriesName, "Global price of LNG, Asia");
  assert.equal(lng.seriesOptions[0].targetConcept, "JKM LNG");
  assert.equal(lng.seriesOptions[0].matchType, "related");
  assert.equal(lng.group, "energy");

  assert.ok(gold);
  assert.equal(gold.primaryLabel, "Gold");
  assert.equal(gold.seriesOptions[0].actualSeriesName, "Gold");
  assert.equal(gold.group, "metals");
});

test("buildCommodityDefinitions groups crude benchmarks and places cobalt and lithium in metals", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "crude_oil_brent",
        target_concept: "Brent Crude Oil",
        actual_series_name: "Crude Oil Prices: Brent - Europe",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "POILBREUSDM",
        source_url: "https://fred.example/brent",
        frequency: "daily",
        unit: "USD/barrel",
        currency: "USD",
        geography: "Europe",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "crude_oil_wti",
        target_concept: "WTI Crude Oil",
        actual_series_name: "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "DCOILWTICO",
        source_url: "https://fred.example/wti",
        frequency: "daily",
        unit: "USD/barrel",
        currency: "USD",
        geography: "United States",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "crude_oil_dubai",
        target_concept: "Dubai / Oman Crude Oil",
        actual_series_name: "Global price of Dubai Crude",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "POILDUBUSDM",
        source_url: "https://fred.example/dubai",
        frequency: "monthly",
        unit: "USD/barrel",
        currency: "USD",
        geography: "Middle East",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "cobalt_imf_monthly",
        target_concept: "Cobalt",
        actual_series_name: "Cobalt, minimum 99.80% purity, LME spot price",
        match_type: "exact",
        source_name: "IMF",
        source_series_code: "PCOBLT",
        source_url: "https://imf.example/cobalt",
        frequency: "monthly",
        unit: "USD/metric ton",
        currency: "USD",
        geography: "Global",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "lithium_metal_imf_monthly",
        target_concept: "Lithium Metal",
        actual_series_name: "Lithium Metal =99%, Battery Grade",
        match_type: "exact",
        source_name: "IMF",
        source_series_code: "PLITH",
        source_url: "https://imf.example/lithium",
        frequency: "monthly",
        unit: "USD/metric ton",
        currency: "USD",
        geography: "Global",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ],
    [
      {
        series_key: "crude_oil_brent",
        observation_date: "2026-03-06",
        value: 71.23,
        previous_value: 70.4,
        delta_value: 0.83,
        delta_pct: 1.18,
        unit: "USD/barrel",
        currency: "USD",
        frequency: "daily",
        source_name: "FRED",
        source_series_code: "POILBREUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "crude_oil_wti",
        observation_date: "2026-03-06",
        value: 68.11,
        previous_value: 67.2,
        delta_value: 0.91,
        delta_pct: 1.35,
        unit: "USD/barrel",
        currency: "USD",
        frequency: "daily",
        source_name: "FRED",
        source_series_code: "DCOILWTICO",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "crude_oil_dubai",
        observation_date: "2026-02-01",
        value: 69.02,
        previous_value: 68.1,
        delta_value: 0.92,
        delta_pct: 1.35,
        unit: "USD/barrel",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "POILDUBUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "cobalt_imf_monthly",
        observation_date: "2026-02-01",
        value: 29874,
        previous_value: 29100,
        delta_value: 774,
        delta_pct: 2.66,
        unit: "USD/metric ton",
        currency: "USD",
        frequency: "monthly",
        source_name: "IMF",
        source_series_code: "PCOBLT",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "lithium_metal_imf_monthly",
        observation_date: "2026-02-01",
        value: 12000,
        previous_value: 11800,
        delta_value: 200,
        delta_pct: 1.69,
        unit: "USD/metric ton",
        currency: "USD",
        frequency: "monthly",
        source_name: "IMF",
        source_series_code: "PLITH",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ]
  );

  const crude = definitions.find((definition) => definition.id === "crude_benchmarks");
  const cobalt = definitions.find((definition) => definition.id === "cobalt_imf_monthly");
  const lithium = definitions.find((definition) => definition.id === "lithium_metal_imf_monthly");

  assert.ok(crude);
  assert.equal(crude.primaryLabel, "Crude");
  assert.deepEqual(
    crude.seriesOptions.map((series) => series.optionLabel),
    ["Brent", "WTI", "Dubai"]
  );
  assert.equal(crude.group, "energy");

  assert.ok(cobalt);
  assert.equal(cobalt.group, "metals");
  assert.equal(cobalt.primaryLabel, "Cobalt");

  assert.ok(lithium);
  assert.equal(lithium.group, "metals");
  assert.equal(lithium.primaryLabel, "Lithium");
});
