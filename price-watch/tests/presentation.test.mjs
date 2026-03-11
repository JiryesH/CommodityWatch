import test from "node:test";
import assert from "node:assert/strict";

import { buildCommodityDefinitions } from "../commodity-presentation.js";

test("buildCommodityDefinitions exposes taxonomy placement and grouped card metadata", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "lng_asia_japan_import_proxy",
        target_concept: "JKM LNG",
        actual_series_name: "Global price of LNG, Asia",
        match_type: "related",
        source_name: "FRED",
        source_series_code: "PNGASJPUSDM",
        source_url: "https://fred.example/lng",
        frequency: "monthly",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "Asia",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "natural_gas_henry_hub",
        target_concept: "Henry Hub Natural Gas",
        actual_series_name: "Henry Hub Natural Gas Spot Price",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "DHHNGSP",
        source_url: "https://fred.example/hh",
        frequency: "daily",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "US",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "natural_gas_ttf",
        target_concept: "TTF Natural Gas",
        actual_series_name: "Dutch TTF Natural Gas Forward",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "PNGASEUUSDM",
        source_url: "https://fred.example/ttf",
        frequency: "monthly",
        unit: "USD per MMBtu",
        currency: "USD",
        geography: "Europe",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "gold_worldbank_monthly",
        target_concept: "Gold",
        actual_series_name: "Gold",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
        source_series_code: "GOLD",
        source_url: "https://fred.example/gold",
        frequency: "monthly",
        unit: "USD per troy ounce",
        currency: "USD",
        geography: "Global",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "rbob_gasoline_spot_proxy",
        target_concept: "RBOB Gasoline",
        actual_series_name: "RBOB spot gasoline",
        match_type: "exact",
        source_name: "FRED",
        source_series_code: "GASRB",
        source_url: "https://fred.example/rbob",
        frequency: "daily",
        unit: "USD per gallon",
        currency: "USD",
        geography: "US",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "gasoline_regular_usgc_daily",
        target_concept: "Gasoline USGC",
        actual_series_name: "Regular gasoline US Gulf Coast",
        match_type: "exact",
        source_name: "Argus",
        source_series_code: "GAS-USGC",
        source_url: "https://argus.example/gas-usgc",
        frequency: "daily",
        unit: "USD per gallon",
        currency: "USD",
        geography: "US Gulf Coast",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "gasoline_regular_nyh_daily",
        target_concept: "Gasoline NYH",
        actual_series_name: "Regular gasoline New York Harbor",
        match_type: "exact",
        source_name: "Argus",
        source_series_code: "GAS-NYH",
        source_url: "https://argus.example/gas-nyh",
        frequency: "daily",
        unit: "USD per gallon",
        currency: "USD",
        geography: "New York Harbor",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "thermal_coal_newcastle",
        target_concept: "Newcastle Coal",
        actual_series_name: "Thermal coal Newcastle",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
        source_series_code: "COAL-AUS",
        source_url: "https://example.test/newcastle",
        frequency: "monthly",
        unit: "USD per metric ton",
        currency: "USD",
        geography: "Australia",
        updated_at: "2026-03-08T10:30:00Z",
      },
      {
        series_key: "coal_south_africa_monthly",
        target_concept: "Coal South Africa",
        actual_series_name: "Coal South Africa",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
        source_series_code: "COAL-SA",
        source_url: "https://example.test/coal-sa",
        frequency: "monthly",
        unit: "USD per metric ton",
        currency: "USD",
        geography: "South Africa",
        updated_at: "2026-03-08T10:30:00Z",
      },
    ],
    [
      { series_key: "lng_asia_japan_import_proxy", value: 10.435, observation_date: "2026-02-01" },
      { series_key: "natural_gas_henry_hub", value: 2.12, observation_date: "2026-03-06" },
      { series_key: "natural_gas_ttf", value: 9.84, observation_date: "2026-02-01" },
      { series_key: "gold_worldbank_monthly", value: 2250.1, observation_date: "2026-02-01" },
      { series_key: "rbob_gasoline_spot_proxy", value: 2.76, observation_date: "2026-03-06" },
      { series_key: "gasoline_regular_usgc_daily", value: 2.51, observation_date: "2026-03-06" },
      { series_key: "gasoline_regular_nyh_daily", value: 2.59, observation_date: "2026-03-06" },
      { series_key: "thermal_coal_newcastle", value: 119.4, observation_date: "2026-02-01" },
      { series_key: "coal_south_africa_monthly", value: 104.2, observation_date: "2026-02-01" },
    ]
  );

  const gas = definitions.find((definition) => definition.id === "natural_gas_benchmarks");
  const lng = definitions.find((definition) => definition.id === "lng_asia_japan_import_proxy");
  const gold = definitions.find((definition) => definition.id === "gold_worldbank_monthly");
  const gasoline = definitions.find((definition) => definition.id === "gasoline_benchmarks");
  const coal = definitions.find((definition) => definition.id === "coal_benchmarks");

  assert.ok(gas);
  assert.equal(gas.primaryLabel, "Natural gas");
  assert.equal(gas.displayLabel, "Natural gas benchmarks");
  assert.equal(gas.sectorId, "energy");
  assert.equal(gas.subsectorId, "natural_gas_and_lng");
  assert.equal(gas.sectorOrder, 1);
  assert.equal(gas.subsectorOrder, 2);
  assert.deepEqual(
    gas.seriesOptions.map((seriesOption) => seriesOption.optionLabel),
    ["Henry Hub", "TTF"]
  );

  assert.ok(lng);
  assert.equal(lng.primaryLabel, "Asia LNG");
  assert.equal(lng.sectorId, "energy");
  assert.equal(lng.subsectorId, "natural_gas_and_lng");

  assert.ok(gold);
  assert.equal(gold.sectorId, "metals_and_mining");
  assert.equal(gold.subsectorId, "precious_metals");
  assert.equal(gold.visual.type, "periodicTile");

  assert.ok(gasoline);
  assert.equal(gasoline.primaryLabel, "Gasoline");
  assert.deepEqual(
    gasoline.seriesOptions.map((seriesOption) => seriesOption.optionLabel),
    ["RBOB", "Gasoline USGC", "Gasoline NYH"]
  );

  assert.ok(coal);
  assert.equal(coal.primaryLabel, "Coal");
  assert.equal(coal.subsectorId, "coal");
  assert.deepEqual(
    coal.seriesOptions.map((seriesOption) => seriesOption.seriesKey),
    ["thermal_coal_newcastle", "coal_south_africa_monthly"]
  );
});

test("buildCommodityDefinitions honors taxonomy reclassifications and sector ordering", () => {
  const definitions = buildCommodityDefinitions(
    [
      {
        series_key: "wheat_global_monthly_proxy",
        target_concept: "Wheat",
        actual_series_name: "Global wheat price",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "wheat_us_srw_monthly",
        target_concept: "Wheat US SRW",
        actual_series_name: "US SRW wheat",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "rubber_rss3_monthly",
        target_concept: "Rubber",
        actual_series_name: "Global price of Rubber",
        match_type: "exact",
        source_name: "FRED",
      },
      {
        series_key: "rubber_tsr20_monthly",
        target_concept: "Rubber TSR20",
        actual_series_name: "Global price of Rubber TSR20",
        match_type: "exact",
        source_name: "FRED",
      },
      {
        series_key: "lumber_monthly_ppi_proxy",
        target_concept: "Lumber",
        actual_series_name: "Producer Price Index: Lumber",
        match_type: "related",
        source_name: "FRED",
      },
      {
        series_key: "urea_monthly",
        target_concept: "Urea",
        actual_series_name: "Urea",
        match_type: "exact",
        source_name: "World Bank Pink Sheet",
      },
      {
        series_key: "beef_monthly",
        target_concept: "Beef",
        actual_series_name: "Global price of Beef",
        match_type: "exact",
        source_name: "FRED",
      },
      {
        series_key: "coffee_arabica_monthly_proxy",
        target_concept: "Coffee",
        actual_series_name: "Global price of Coffee, Other Mild Arabica",
        match_type: "related",
        source_name: "FRED",
      },
      {
        series_key: "coffee_robusta_monthly_proxy",
        target_concept: "Coffee Robusta",
        actual_series_name: "Global price of Coffee, Robustas",
        match_type: "related",
        source_name: "FRED",
      },
    ],
    [
      { series_key: "wheat_global_monthly_proxy", value: 265 },
      { series_key: "wheat_us_srw_monthly", value: 251 },
      { series_key: "rubber_rss3_monthly", value: 2.1 },
      { series_key: "rubber_tsr20_monthly", value: 1.86 },
      { series_key: "lumber_monthly_ppi_proxy", value: 725 },
      { series_key: "urea_monthly", value: 381 },
      { series_key: "beef_monthly", value: 4.8 },
      { series_key: "coffee_arabica_monthly_proxy", value: 2.9 },
      { series_key: "coffee_robusta_monthly_proxy", value: 2.4 },
    ]
  );

  assert.deepEqual(
    definitions.map((definition) => definition.id),
    [
      "wheat_benchmarks",
      "coffee_benchmarks",
      "rubber_benchmarks",
      "urea_monthly",
      "beef_monthly",
      "lumber_monthly_ppi_proxy",
    ]
  );

  const wheat = definitions.find((definition) => definition.id === "wheat_benchmarks");
  const rubber = definitions.find((definition) => definition.id === "rubber_benchmarks");
  const urea = definitions.find((definition) => definition.id === "urea_monthly");
  const beef = definitions.find((definition) => definition.id === "beef_monthly");
  const lumber = definitions.find((definition) => definition.id === "lumber_monthly_ppi_proxy");
  const coffee = definitions.find((definition) => definition.id === "coffee_benchmarks");

  assert.equal(wheat.sectorId, "agriculture");
  assert.equal(wheat.subsectorId, "grains_and_cereals");
  assert.deepEqual(
    wheat.seriesOptions.map((seriesOption) => seriesOption.seriesKey),
    ["wheat_global_monthly_proxy", "wheat_us_srw_monthly"]
  );

  assert.equal(rubber.sectorId, "agriculture");
  assert.equal(rubber.subsectorId, "industrial_agriculture_materials");
  assert.deepEqual(
    rubber.seriesOptions.map((seriesOption) => seriesOption.seriesKey),
    ["rubber_rss3_monthly", "rubber_tsr20_monthly"]
  );

  assert.equal(urea.sectorId, "fertilizers_and_agricultural_chemicals");
  assert.equal(urea.subsectorId, "nitrogen");

  assert.equal(beef.sectorId, "livestock_dairy_and_seafood");
  assert.equal(beef.subsectorId, "meat_and_livestock");

  assert.equal(lumber.sectorId, "forest_and_wood_products");
  assert.equal(lumber.subsectorId, "wood_products");
  assert.equal(lumber.visual.type, "marketTile");

  assert.ok(coffee.isGrouped);
  assert.deepEqual(
    coffee.seriesOptions.map((seriesOption) => seriesOption.seriesKey),
    ["coffee_arabica_monthly_proxy", "coffee_robusta_monthly_proxy"]
  );
});
