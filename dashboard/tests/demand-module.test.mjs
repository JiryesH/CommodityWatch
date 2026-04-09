import test from "node:test";
import assert from "node:assert/strict";

import { mapDemandScorecardItems } from "../../demand-watch/data.js";
import { createDefaultFilter, toggleSector } from "../filter-state.js";
import { selectDemandWidgetItems } from "../modules.js";

test("demand widget selection uses live scorecard items for the home preview", () => {
  const liveItems = mapDemandScorecardItems([
    {
      id: "base-metals",
      code: "base_metals",
      label: "Base Metals",
      nav_label: "Metals",
      short_label: "Base Metals",
      sector: "metals",
      scorecard_label: "Industrial production",
      display_value: "103.8",
      yoy_label: "+1.2% YoY",
      trend: "up",
      latest_period_label: "Mar 2026",
      freshness: "3w ago",
      freshness_state: "fresh",
      stale: false,
      primary_series_code: "FRED_US_INDUSTRIAL_PRODUCTION",
    },
    {
      id: "crude-products",
      code: "crude_products",
      label: "Crude Oil + Refined Products",
      nav_label: "Crude",
      short_label: "Crude + Products",
      sector: "energy",
      scorecard_label: "US product supplied",
      display_value: "9.6 mb/d",
      yoy_label: "+6.7% YoY",
      trend: "up",
      latest_period_label: "Week ending 27 Mar 2026",
      freshness: "5d ago",
      freshness_state: "fresh",
      stale: false,
      primary_series_code: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
    },
    {
      id: "electricity",
      code: "electricity",
      label: "Electricity / Power",
      nav_label: "Power",
      short_label: "Electricity",
      sector: "energy",
      scorecard_label: "US grid load",
      display_value: "428 GW",
      yoy_label: "+2.1% YoY",
      trend: "up",
      latest_period_label: "04 Apr 2026",
      freshness: "1d ago",
      freshness_state: "fresh",
      stale: false,
      primary_series_code: "EIA_US_ELECTRICITY_GRID_LOAD",
    },
  ]);

  const selection = selectDemandWidgetItems(toggleSector(createDefaultFilter(), "energy"), liveItems);

  assert.deepEqual(selection.items.map((item) => item.id), ["crude-products", "electricity"]);
  assert.equal(selection.items[0].scorecard.value, "9.6 mb/d");
  assert.equal(selection.items[1].accent, "var(--color-natural-gas)");
});
