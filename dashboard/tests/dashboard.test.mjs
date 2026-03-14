import test from "node:test";
import assert from "node:assert/strict";

import {
  getAllCommodityIdsForGroup,
  getSeriesKeysForGroup,
} from "../config.js";
import {
  createDefaultFilter,
  getFilterLabel,
  getSummaryPills,
  isAllCommoditySelection,
  isAllFilter,
  normalizeFilter,
  toggleCommodity,
  toggleGroup,
  toggleSector,
} from "../filter-state.js";
import { MODULE_REGISTRY } from "../module-registry.js";

test("default home filter resolves to the all-commodities state", () => {
  const filter = createDefaultFilter();

  assert.equal(isAllFilter(filter), true);
  assert.equal(getFilterLabel(filter), "All Commodities");
  assert.deepEqual(getSummaryPills(filter), [{ id: "all", label: "All Commodities", tone: "neutral" }]);
});

test("selecting a sector from the all state isolates that sector", () => {
  const filter = toggleSector(createDefaultFilter(), "energy");

  assert.equal(filter.sectorId, "energy");
  assert.equal(filter.groupId, null);
  assert.deepEqual(filter.commodityIds, []);
  assert.equal(getFilterLabel(filter), "Energy");
});

test("selecting a group activates every commodity in that group", () => {
  const filter = toggleGroup(toggleSector(createDefaultFilter(), "energy"), "crude-oil");

  assert.equal(filter.sectorId, "energy");
  assert.equal(filter.groupId, "crude-oil");
  assert.deepEqual(filter.commodityIds, getAllCommodityIdsForGroup("crude-oil"));
  assert.equal(isAllCommoditySelection(filter), true);
  assert.equal(getFilterLabel(filter), "Energy / Crude Oil");
});

test("commodity toggling narrows from all benchmarks and restores the full group when emptied", () => {
  let filter = normalizeFilter({
    sectorId: "energy",
    groupId: "crude-oil",
    commodityIds: getAllCommodityIdsForGroup("crude-oil"),
  });

  filter = toggleCommodity(filter, "brent");
  assert.deepEqual(filter.commodityIds, ["brent"]);
  assert.equal(getFilterLabel(filter), "Energy / Crude Oil / Brent");

  filter = toggleCommodity(filter, "wti");
  assert.deepEqual(filter.commodityIds, ["brent", "wti"]);

  filter = toggleCommodity(filter, "wti");
  assert.deepEqual(filter.commodityIds, ["brent"]);

  filter = toggleCommodity(filter, "brent");
  assert.deepEqual(filter.commodityIds, getAllCommodityIdsForGroup("crude-oil"));
  assert.equal(isAllCommoditySelection(filter), true);
});

test("series mappings for crude oil match the live home benchmark selection", () => {
  assert.deepEqual(getSeriesKeysForGroup("crude-oil"), [
    "crude_oil_brent",
    "crude_oil_wti",
    "crude_oil_dubai",
  ]);
});

test("home registry only renders the live modules in their declared slots", () => {
  assert.deepEqual(
    MODULE_REGISTRY.map((moduleDefinition) => moduleDefinition.id),
    ["prices", "headlines", "calendar"]
  );
  assert.deepEqual(
    MODULE_REGISTRY.map((moduleDefinition) => moduleDefinition.slot),
    ["main-top", "main-left", "sidebar"]
  );
});
