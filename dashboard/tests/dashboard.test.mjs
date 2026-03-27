import test from "node:test";
import assert from "node:assert/strict";

import {
  DEFAULT_HOME_SERIES_KEYS,
  ALWAYS_HEADLINE_CATEGORIES,
  getAllCommodityIdsForGroup,
  getSectorById,
  getSeriesKeysForGroup,
  getTagForFeedCategory,
} from "../config.js";
import {
  createDefaultFilter,
  getFilterLabel,
  getSelectedCommodities,
  getSelectedGroup,
  getSelectedGroups,
  getSelectedSectors,
  getSummaryPills,
  hasPartialGroupSelection,
  isAllCommoditySelection,
  isAllFilter,
  normalizeFilter,
  toggleCommodity,
  toggleGroup,
  toggleSector,
} from "../filter-state.js";
import { MODULE_REGISTRY } from "../module-registry.js";
import { createModuleRenderGate } from "../modules.js";
import {
  ALWAYS_RELEVANT_CATEGORIES,
  SECTOR_MAP as HEADLINE_SECTOR_MAP,
  canonicalCategoriesForArticle,
} from "../../shared/headline-taxonomy.js";
import { COMMODITY_SERIES_CONTRACT } from "../../shared/commodity-series-contract.js";

test("default home filter resolves to the all-commodities state with all sectors active", () => {
  const filter = createDefaultFilter();

  assert.equal(isAllFilter(filter), true);
  assert.equal(getFilterLabel(filter), "All Commodities");
  assert.deepEqual(getSummaryPills(filter), [{ id: "all", label: "All Commodities", tone: "neutral" }]);
  assert.deepEqual(
    getSelectedSectors(filter).map((sector) => sector.id),
    ["energy", "metals", "agriculture"]
  );
});

test("selecting a sector from the all state isolates that sector while keeping all of its groups active", () => {
  const filter = toggleSector(createDefaultFilter(), "energy");

  assert.equal(isAllFilter(filter), false);
  assert.deepEqual(filter.sectorIds, ["energy"]);
  assert.deepEqual(
    getSelectedGroups(filter).map((group) => group.id),
    ["crude-oil", "natural-gas", "lng", "coal", "power"]
  );
  assert.equal(getFilterLabel(filter), "Energy");
});

test("multiple sectors can be selected together", () => {
  let filter = toggleSector(createDefaultFilter(), "energy");
  filter = toggleSector(filter, "metals");

  assert.deepEqual(filter.sectorIds, ["energy", "metals"]);
  assert.deepEqual(
    getSelectedSectors(filter).map((sector) => sector.id),
    ["energy", "metals"]
  );
  assert.equal(getFilterLabel(filter), "Energy + Metals");
});

test("toggling one group from a selected sector creates a partial parent state and can be restored", () => {
  let filter = toggleSector(createDefaultFilter(), "energy");

  filter = toggleGroup(filter, "coal");

  assert.equal(hasPartialGroupSelection(filter), true);
  assert.deepEqual(
    getSelectedGroups(filter).map((group) => group.id),
    ["crude-oil", "natural-gas", "lng", "power"]
  );

  filter = toggleGroup(filter, "coal");

  assert.equal(hasPartialGroupSelection(filter), false);
  assert.deepEqual(
    getSelectedGroups(filter).map((group) => group.id),
    ["crude-oil", "natural-gas", "lng", "coal", "power"]
  );
});

test("commodity toggling narrows from all benchmarks within a single group and restores the full group when emptied", () => {
  let filter = normalizeFilter({
    sectorIds: ["energy"],
    groupIdsBySector: {
      energy: ["crude-oil"],
    },
    commodityIds: [],
  });

  assert.equal(getSelectedGroup(filter)?.id, "crude-oil");
  assert.equal(isAllCommoditySelection(filter), true);
  assert.equal(getFilterLabel(filter), "Energy / Crude Oil");

  filter = toggleCommodity(filter, "brent");
  assert.deepEqual(
    getSelectedCommodities(filter).map((commodity) => commodity.id),
    ["wti", "dubai-oman"]
  );
  assert.equal(isAllCommoditySelection(filter), false);

  filter = toggleCommodity(filter, "wti");
  assert.deepEqual(
    getSelectedCommodities(filter).map((commodity) => commodity.id),
    ["dubai-oman"]
  );

  filter = toggleCommodity(filter, "dubai-oman");
  assert.deepEqual(
    getSelectedCommodities(filter).map((commodity) => commodity.id),
    getAllCommodityIdsForGroup("crude-oil")
  );
  assert.equal(isAllCommoditySelection(filter), true);
});

test("removing the last selected sector resets the filter back to all commodities", () => {
  let filter = toggleSector(createDefaultFilter(), "energy");
  filter = toggleSector(filter, "energy");

  assert.equal(isAllFilter(filter), true);
  assert.equal(getFilterLabel(filter), "All Commodities");
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

test("module render gates ignore stale async writes after a newer render starts", () => {
  const section = {};
  const body = { innerHTML: "" };

  const firstGate = createModuleRenderGate(section);
  const secondGate = createModuleRenderGate(section);

  firstGate.commit(body, "stale");
  assert.equal(body.innerHTML, "");
  assert.equal(firstGate.isCurrent(), false);

  secondGate.commit(body, "fresh");
  assert.equal(body.innerHTML, "fresh");
  assert.equal(secondGate.isCurrent(), true);
});

test("dashboard config derives headline category semantics from the shared taxonomy contract", () => {
  assert.deepEqual(DEFAULT_HOME_SERIES_KEYS, COMMODITY_SERIES_CONTRACT.dashboard.default_home_series_keys);
  assert.deepEqual(ALWAYS_HEADLINE_CATEGORIES, [...ALWAYS_RELEVANT_CATEGORIES]);
  assert.deepEqual(getSectorById("energy").feedCategories, [...HEADLINE_SECTOR_MAP.energy]);
  assert.deepEqual(getSectorById("metals").feedCategories, [...HEADLINE_SECTOR_MAP.metals_and_mining]);
  assert.deepEqual(getSectorById("agriculture").feedCategories, [
    ...HEADLINE_SECTOR_MAP.agriculture,
    ...HEADLINE_SECTOR_MAP.fertilizers,
  ]);
  assert.deepEqual(getTagForFeedCategory("Shipping"), {
    label: "Cross-Commodity",
    sectorId: "cross-commodity",
  });
});

test("shared headline taxonomy normalizes canonical category lists deterministically", () => {
  assert.deepEqual(
    canonicalCategoriesForArticle({
      categories: ["Shipping", "Unknown", "Oil - Crude", "Shipping"],
    }),
    ["Oil - Crude", "Shipping"]
  );
  assert.deepEqual(
    canonicalCategoriesForArticle({
      category: "Shipping, Oil - Crude, Shipping",
    }),
    ["Oil - Crude", "Shipping"]
  );
});
