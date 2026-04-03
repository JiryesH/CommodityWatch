import test from "node:test";
import assert from "node:assert/strict";

import {
  COMMODITY_GROUPS,
  FRESHNESS_BADGES,
  commodityGroupForCode,
  convertUnitValue,
  formatSignedValue,
  formatValue,
  getIndicatorRegistryEntry,
  nextReleaseStatus,
  snapshotGroupForCommodity,
} from "../catalog.js";
import {
  areAllInventoryGroupsSelected,
  buildChangeBarSeries,
  buildRecentReleaseRows,
  buildSeasonalSeries,
  buildYtdChangeStats,
  filterCardsByCommodityGroup,
  filterCardsByCommodityGroups,
  normalizeInventorySearchQuery,
  percentileBracketLabel,
  searchSnapshotCards,
  snapshotSignalDescriptor,
  sortSnapshotCards,
  snapshotSectionEntries,
  toggleInventoryGroupSelection,
} from "../model.js";
import {
  buildInventoryDetailHref,
  buildInventorySnapshotHref,
  parseInventoryRoute,
} from "../router.js";

test("inventory commodity routing maps backend commodity codes into native CommodityWatch groups", () => {
  assert.equal(commodityGroupForCode("crude_oil"), "energy");
  assert.equal(commodityGroupForCode("natural_gas"), "natural-gas");
  assert.equal(commodityGroupForCode("copper"), "base-metals");
  assert.equal(commodityGroupForCode("wheat"), "grains");
  assert.equal(commodityGroupForCode("coal"), "coal");
  assert.equal(commodityGroupForCode("unknown_series"), "all");
});

test("inventory metadata keeps the public group copy and badge labels aligned", () => {
  assert.equal(
    COMMODITY_GROUPS.find((group) => group.slug === "all")?.shortDescription,
    "Full InventoryWatch market snapshot."
  );
  assert.equal(
    COMMODITY_GROUPS.find((group) => group.slug === "natural-gas")?.shortDescription,
    "US and European gas storage."
  );
  assert.equal(FRESHNESS_BADGES.current.label, "Live");
  assert.match(String(COMMODITY_GROUPS.find((group) => group.slug === "grains")?.commodityCodes), /rice/);
  assert.equal(snapshotGroupForCommodity("rice"), "Grains");
});

test("inventory registry keeps source labels and commodity groups aligned across source families", () => {
  assert.equal(getIndicatorRegistryEntry("ETF_GLD_HOLDINGS").sourceLabel, "ETF Holdings");
  assert.equal(getIndicatorRegistryEntry("ETF_GLD_HOLDINGS").snapshotGroup, "Precious Metals");
  assert.equal(getIndicatorRegistryEntry("LME_COPPER_WAREHOUSE_STOCKS").sourceLabel, "LME");
  assert.equal(getIndicatorRegistryEntry("USDA_US_RICE_ENDING_STOCKS").snapshotGroup, "Grains");
  assert.equal(getIndicatorRegistryEntry("ICE_COTTON_CERTIFIED_STOCKS").snapshotGroup, "Softs");
});

test("inventory router preserves module route conventions for snapshot and detail views", () => {
  assert.deepEqual(parseInventoryRoute("/inventory-watch/"), {
    view: "snapshot",
    groupSlug: "all",
    indicatorId: null,
  });
  assert.deepEqual(parseInventoryRoute("/inventory-watch/natural-gas/"), {
    view: "snapshot",
    groupSlug: "natural-gas",
    indicatorId: null,
  });
  assert.deepEqual(parseInventoryRoute("/inventory-watch/natural-gas/test-indicator/"), {
    view: "detail",
    groupSlug: "natural-gas",
    indicatorId: "test-indicator",
  });
  assert.equal(buildInventorySnapshotHref("all"), "/inventory-watch/");
  assert.equal(buildInventorySnapshotHref("grains"), "/inventory-watch/grains/");
  assert.equal(buildInventoryDetailHref("energy", "abc-123"), "/inventory-watch/energy/abc-123/");
});

test("inventory router rejects unknown top-level route segments", () => {
  const route = parseInventoryRoute("/inventory-watch/not-a-group/");

  assert.equal(route.view, "not-found");
  assert.match(route.reason, /Unknown InventoryWatch group/);
});

test("snapshot filtering retains only cards that belong to the active group", () => {
  const cards = [
    { indicatorId: "1", commodityCode: "crude_oil" },
    { indicatorId: "2", commodityCode: "natural_gas" },
    { indicatorId: "3", commodityCode: "copper" },
  ];

  assert.deepEqual(
    filterCardsByCommodityGroup(cards, "energy").map((card) => card.indicatorId),
    ["1"]
  );
  assert.deepEqual(
    filterCardsByCommodityGroup(cards, "natural-gas").map((card) => card.indicatorId),
    ["2"]
  );
  assert.deepEqual(
    filterCardsByCommodityGroup(cards, "all").map((card) => card.indicatorId),
    ["1", "2", "3"]
  );
});

test("snapshot multi-select filters isolate, expand, and reset like the other watch modules", () => {
  const available = ["energy", "natural-gas", "grains"];

  assert.equal(areAllInventoryGroupsSelected(available, available), true);
  assert.deepEqual(toggleInventoryGroupSelection(available, "energy", available), ["energy"]);
  assert.deepEqual(toggleInventoryGroupSelection(["energy"], "grains", available), ["energy", "grains"]);
  assert.deepEqual(toggleInventoryGroupSelection(["energy", "grains"], "energy", available), ["grains"]);
  assert.deepEqual(toggleInventoryGroupSelection(["energy"], "energy", available), available);
});

test("snapshot multi-select card filtering retains cards across active sectors", () => {
  const cards = [
    { indicatorId: "1", commodityCode: "crude_oil" },
    { indicatorId: "2", commodityCode: "natural_gas" },
    { indicatorId: "3", commodityCode: "rice" },
  ];

  assert.deepEqual(
    filterCardsByCommodityGroups(cards, ["energy", "grains"]).map((card) => card.indicatorId),
    ["1", "3"]
  );
  assert.deepEqual(
    filterCardsByCommodityGroups(cards, []).map((card) => card.indicatorId),
    ["1", "2", "3"]
  );
});

test("inventory search normalizes whitespace and matches indicator metadata across fields", () => {
  const cards = [
    {
      indicatorId: "EIA_GASOLINE_US_TOTAL_STOCKS",
      code: "EIA_GASOLINE_US_TOTAL_STOCKS",
      name: "EIA US Total Motor Gasoline Stocks",
      sourceLabel: "EIA",
      snapshotGroup: "Refined Products",
      commodityCode: "gasoline",
      frequency: "weekly",
      alerts: [{ label: "Below seasonal" }],
    },
    {
      indicatorId: "LME_COPPER_WAREHOUSE_STOCKS",
      code: "LME_COPPER_WAREHOUSE_STOCKS",
      name: "LME Copper Warehouse Stocks",
      sourceLabel: "LME",
      snapshotGroup: "Base Metals",
      commodityCode: "copper",
      frequency: "daily",
    },
  ];

  assert.equal(normalizeInventorySearchQuery("  Motor   Gasoline  "), "motor gasoline");
  assert.deepEqual(
    searchSnapshotCards(cards, "motor gasoline").map((card) => card.indicatorId),
    ["EIA_GASOLINE_US_TOTAL_STOCKS"]
  );
  assert.deepEqual(
    searchSnapshotCards(cards, "eia refined").map((card) => card.indicatorId),
    ["EIA_GASOLINE_US_TOTAL_STOCKS"]
  );
  assert.deepEqual(
    searchSnapshotCards(cards, "base metals daily").map((card) => card.indicatorId),
    ["LME_COPPER_WAREHOUSE_STOCKS"]
  );
});

test("snapshot signal descriptors map seasonal percentiles into readable card labels", () => {
  assert.deepEqual(
    snapshotSignalDescriptor({
      latestValue: 94,
      isSeasonal: true,
      seasonalP10: 95,
      seasonalP25: 100,
      seasonalP75: 120,
      seasonalP90: 125,
    }),
    { state: "well-below-seasonal", label: "Well below seasonal" }
  );

  assert.deepEqual(
    snapshotSignalDescriptor({
      latestValue: 98,
      isSeasonal: true,
      seasonalP10: 95,
      seasonalP25: 100,
      seasonalP75: 120,
      seasonalP90: 125,
    }),
    { state: "below-seasonal", label: "Below seasonal" }
  );

  assert.deepEqual(
    snapshotSignalDescriptor({
      latestValue: 110,
      isSeasonal: true,
      seasonalP10: 95,
      seasonalP25: 100,
      seasonalP75: 120,
      seasonalP90: 125,
    }),
    { state: "seasonal-range", label: "Seasonal range" }
  );

  assert.deepEqual(
    snapshotSignalDescriptor({
      latestValue: 123,
      isSeasonal: true,
      seasonalP10: 95,
      seasonalP25: 100,
      seasonalP75: 120,
      seasonalP90: 125,
    }),
    { state: "above-seasonal", label: "Above seasonal" }
  );

  assert.deepEqual(
    snapshotSignalDescriptor({
      latestValue: 128,
      isSeasonal: true,
      seasonalP10: 95,
      seasonalP25: 100,
      seasonalP75: 120,
      seasonalP90: 125,
    }),
    { state: "well-above-seasonal", label: "Well above seasonal" }
  );
});

test("snapshot ordering prefers analyst-first card ranks over backend lexical order", () => {
  const cards = [
    { code: "GIE_NATURAL_GAS_EU_AT_STORAGE", name: "Austria", snapshotGroup: "Natural Gas" },
    { code: "GIE_NATURAL_GAS_EU_TOTAL_STORAGE", name: "EU Total", snapshotGroup: "Natural Gas" },
    { code: "EIA_NATURAL_GAS_US_WORKING_STORAGE", name: "US", snapshotGroup: "Natural Gas" },
    { code: "GIE_NATURAL_GAS_EU_DE_STORAGE", name: "Germany", snapshotGroup: "Natural Gas" },
  ];

  assert.deepEqual(sortSnapshotCards(cards).map((card) => card.code), [
    "EIA_NATURAL_GAS_US_WORKING_STORAGE",
    "GIE_NATURAL_GAS_EU_TOTAL_STORAGE",
    "GIE_NATURAL_GAS_EU_DE_STORAGE",
    "GIE_NATURAL_GAS_EU_AT_STORAGE",
  ]);

  const grouped = {
    "Natural Gas": cards,
    Inventory: [{ code: "UNMAPPED", name: "Fallback", snapshotGroup: "Inventory" }],
  };

  assert.deepEqual(
    snapshotSectionEntries(grouped).map(([sectionName, sectionCards]) => [sectionName, sectionCards.map((card) => card.code)]),
    [
      [
        "Natural Gas",
        [
          "EIA_NATURAL_GAS_US_WORKING_STORAGE",
          "GIE_NATURAL_GAS_EU_TOTAL_STORAGE",
          "GIE_NATURAL_GAS_EU_DE_STORAGE",
          "GIE_NATURAL_GAS_EU_AT_STORAGE",
        ],
      ],
      ["Inventory", ["UNMAPPED"]],
    ]
  );
});

test("seasonal selectors build chart and release-table inputs from backend indicator payloads", () => {
  const payload = {
    indicator: {
      frequency: "weekly",
      periodType: "weekly",
      unit: "bcf",
    },
    series: [
      { periodEndAt: "2025-01-10T00:00:00Z", releaseDate: "2025-01-16T14:30:00Z", value: 100 },
      { periodEndAt: "2025-01-17T00:00:00Z", releaseDate: "2025-01-23T14:30:00Z", value: 108 },
      { periodEndAt: "2026-01-09T00:00:00Z", releaseDate: "2026-01-15T14:30:00Z", value: 96 },
      { periodEndAt: "2026-01-16T00:00:00Z", releaseDate: "2026-01-22T14:30:00Z", value: 92 },
    ],
    seasonalRange: [
      { periodIndex: 2, p10: 88, p25: 92, p50: 98, p75: 104, p90: 108 },
      { periodIndex: 3, p10: 84, p25: 89, p50: 95, p75: 101, p90: 105 },
    ],
  };

  const seasonalSeries = buildSeasonalSeries(payload);
  const changeSeries = buildChangeBarSeries(payload);
  const releaseRows = buildRecentReleaseRows(payload);
  const ytdStats = buildYtdChangeStats(payload);

  assert.equal(seasonalSeries.mode, "week");
  assert.equal(seasonalSeries.currentYear.length, 2);
  assert.equal(seasonalSeries.priorYear.length, 2);
  assert.equal(changeSeries.length, 3);
  assert.equal(typeof changeSeries[0].seasonalAverageChange, "number");
  assert.equal(releaseRows[0].percentileRankLabel, "25th-75th");
  assert.equal(percentileBracketLabel(110, payload.seasonalRange[0]), "Above 90th");
  assert.equal(ytdStats.ytdChange, -4);
});

test("inventory number formatting preserves sign and converted unit display", () => {
  assert.equal(formatValue(convertUnitValue(1234.4, "kb"), "mb"), "1.2 mb");
  assert.equal(formatSignedValue(-18, "bcf"), "-18 bcf");
});

test("countdown schedules compute the next expected fixed release", () => {
  const status = nextReleaseStatus(
    {
      code: "EIA_GASOLINE_US_TOTAL_STOCKS",
      commodityCode: "gasoline",
      latestReleaseDate: "2026-03-25T14:30:00Z",
    },
    new Date("2026-03-31T12:00:00Z")
  );

  assert.match(status.label, /Next release in|Expected today/);
});
