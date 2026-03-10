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

test("buildCommodityDefinitions groups Henry Hub and TTF into one gas card", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "natural_gas_henry_hub",
        target_concept: "Henry Hub Natural Gas",
        actual_series_name: "Natural Gas Spot Price at Henry Hub",
        benchmark_series: "Henry Hub",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "DHHNGSP",
        source_url: "https://fred.example/henry-hub",
        frequency: "daily",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "United States",
        active: true,
        notes: "Exact published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "natural_gas_ttf",
        target_concept: "TTF Natural Gas",
        actual_series_name: "Dutch TTF Natural Gas Forward",
        benchmark_series: "TTF",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "DTTFNG",
        source_url: "https://fred.example/ttf",
        frequency: "daily",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "Europe",
        active: true,
        notes: "Related published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
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
    ],
    [
      {
        series_key: "natural_gas_henry_hub",
        observation_date: "2026-03-06",
        value: 2.14,
        previous_value: 2.06,
        delta_value: 0.08,
        delta_pct: 3.88,
        unit: "USD per MMBtu",
        currency: "USD",
        frequency: "daily",
        source_name: "FRED",
        source_series_code: "DHHNGSP",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "natural_gas_ttf",
        observation_date: "2026-03-06",
        value: 9.84,
        previous_value: 9.51,
        delta_value: 0.33,
        delta_pct: 3.47,
        unit: "USD per MMBtu",
        currency: "USD",
        frequency: "daily",
        source_name: "FRED",
        source_series_code: "DTTFNG",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "lng_asia_japan_import_proxy",
        observation_date: "2026-02-01",
        value: 10.435,
        previous_value: 9.8,
        delta_value: 0.635,
        delta_pct: 6.48,
        unit: "USD per MMBtu",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PNGASJPUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ]
  );

  const gas = definitions.find((definition) => definition.id === "natural_gas_benchmarks");
  const asiaLng = definitions.find((definition) => definition.id === "lng_asia_japan_import_proxy");

  assert.ok(gas);
  assert.equal(gas.primaryLabel, "Gas");
  assert.equal(gas.group, "energy");
  assert.deepEqual(
    gas.seriesOptions.map((series) => series.optionLabel),
    ["Henry Hub", "TTF"]
  );
  assert.equal(definitions.some((definition) => definition.id === "natural_gas_henry_hub"), false);
  assert.equal(definitions.some((definition) => definition.id === "natural_gas_ttf"), false);

  assert.ok(asiaLng);
  assert.equal(asiaLng.primaryLabel, "LNG");
});

test("buildCommodityDefinitions groups Arabica and Robusta into one coffee card", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "coffee_arabica_monthly_proxy",
        target_concept: "Arabica Coffee",
        actual_series_name: "Coffee, Arabica",
        benchmark_series: "Arabica",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PCOFFOTMUSDM",
        source_url: "https://fred.example/coffee-arabica",
        frequency: "monthly",
        unit: "USD per kilogram",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Related published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "coffee_robusta_monthly_proxy",
        target_concept: "Robusta Coffee",
        actual_series_name: "Coffee, Robusta",
        benchmark_series: "Robusta",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PROBUSDM",
        source_url: "https://fred.example/coffee-robusta",
        frequency: "monthly",
        unit: "USD per kilogram",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Related published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "cocoa_monthly_proxy",
        target_concept: "Cocoa",
        actual_series_name: "Cocoa price",
        benchmark_series: "Cocoa",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PCOCOUSDM",
        source_url: "https://fred.example/cocoa",
        frequency: "monthly",
        unit: "USD per metric ton",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Related published series",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ],
    [
      {
        series_key: "coffee_arabica_monthly_proxy",
        observation_date: "2026-02-01",
        value: 6.24,
        previous_value: 6.01,
        delta_value: 0.23,
        delta_pct: 3.83,
        unit: "USD per kilogram",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PCOFFOTMUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "coffee_robusta_monthly_proxy",
        observation_date: "2026-02-01",
        value: 4.17,
        previous_value: 4.05,
        delta_value: 0.12,
        delta_pct: 2.96,
        unit: "USD per kilogram",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PROBUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "cocoa_monthly_proxy",
        observation_date: "2026-02-01",
        value: 9200,
        previous_value: 9050,
        delta_value: 150,
        delta_pct: 1.66,
        unit: "USD per metric ton",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PCOCOUSDM",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ]
  );

  const coffee = definitions.find((definition) => definition.id === "coffee_benchmarks");
  const cocoa = definitions.find((definition) => definition.id === "cocoa_monthly_proxy");

  assert.ok(coffee);
  assert.equal(coffee.primaryLabel, "Coffee");
  assert.equal(coffee.group, "agri");
  assert.deepEqual(
    coffee.seriesOptions.map((series) => series.optionLabel),
    ["Arabica", "Robusta"]
  );
  assert.equal(definitions.some((definition) => definition.id === "coffee_arabica_monthly_proxy"), false);
  assert.equal(definitions.some((definition) => definition.id === "coffee_robusta_monthly_proxy"), false);

  assert.ok(cocoa);
  assert.equal(cocoa.primaryLabel, "Cocoa");
});

test("buildCommodityDefinitions infers metals and agriculture groups from commodity metadata when series keys change", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "gold_spot_proxy_v2",
        target_concept: "Gold",
        actual_series_name: "Gold spot price",
        benchmark_series: "Gold",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PGOLDALT",
        source_url: "https://fred.example/gold-v2",
        frequency: "monthly",
        unit: "USD per troy ounce",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Renamed gold series",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "corn_cash_proxy_v2",
        target_concept: "Corn",
        actual_series_name: "Global corn cash price",
        benchmark_series: "Corn",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PCORNALT",
        source_url: "https://fred.example/corn-v2",
        frequency: "monthly",
        unit: "USD per metric ton",
        currency: "USD",
        geography: "Global",
        active: true,
        notes: "Renamed corn series",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ],
    [
      {
        series_key: "gold_spot_proxy_v2",
        observation_date: "2026-02-01",
        value: 2250.1,
        previous_value: 2210,
        delta_value: 40.1,
        delta_pct: 1.81,
        unit: "USD per troy ounce",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PGOLDALT",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "corn_cash_proxy_v2",
        observation_date: "2026-02-01",
        value: 183.4,
        previous_value: 179.8,
        delta_value: 3.6,
        delta_pct: 2.0,
        unit: "USD per metric ton",
        currency: "USD",
        frequency: "monthly",
        source_name: "FRED",
        source_series_code: "PCORNALT",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ]
  );

  const gold = definitions.find((definition) => definition.id === "gold_spot_proxy_v2");
  const corn = definitions.find((definition) => definition.id === "corn_cash_proxy_v2");

  assert.ok(gold);
  assert.equal(gold.group, "metals");

  assert.ok(corn);
  assert.equal(corn.group, "agri");
});
