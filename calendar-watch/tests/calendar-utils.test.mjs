import test from "node:test";
import assert from "node:assert/strict";

import { MOCK_EVENTS } from "../calendar-data.js";
import {
  CADENCE_OPTIONS,
  SECTOR_OPTIONS,
  areAllFiltersSelected,
  buildMonthGrid,
  buildRangeForView,
  buildWeekDays,
  clampAnchorDateForView,
  cloneDefaultFilters,
  countActiveFilters,
  endOfUtcYear,
  filterEvents,
  filterEventsByRange,
  getMaximumAnchorDate,
  getMinimumAnchorDate,
  searchEvents,
  toIsoDay,
  toggleFilterSelection,
} from "../calendar-utils.js";

test("buildWeekDays anchors the week on Monday in UTC", () => {
  const weekDays = buildWeekDays(new Date("2026-03-12T10:00:00Z"));

  assert.deepEqual(
    weekDays.map((day) => toIsoDay(day)),
    ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13"]
  );
});

test("buildMonthGrid returns a full Monday-first calendar grid", () => {
  const grid = buildMonthGrid(new Date("2026-03-12T10:00:00Z"));

  assert.equal(grid.length, 42);
  assert.equal(toIsoDay(grid[0]), "2026-02-23");
  assert.equal(toIsoDay(grid[grid.length - 1]), "2026-04-05");
});

test("buildRangeForView constrains week view to Monday through Friday", () => {
  const range = buildRangeForView("week", new Date("2026-03-12T10:00:00Z"));

  assert.equal(range.from, "2026-03-09T00:00:00.000Z");
  assert.equal(range.to, "2026-03-13T23:59:59.000Z");
});

test("filterEventsByRange keeps only events inside the requested window", () => {
  const events = [
    { id: "a", event_date: "2026-03-08T23:00:00Z" },
    { id: "b", event_date: "2026-03-09T10:00:00Z" },
    { id: "c", event_date: "2026-03-13T18:00:00Z" },
    { id: "d", event_date: "2026-03-14T00:00:00Z" },
  ];

  const filtered = filterEventsByRange(events, {
    from: "2026-03-09T00:00:00.000Z",
    to: "2026-03-13T23:59:59.000Z",
  });

  assert.deepEqual(filtered.map((event) => event.id), ["b", "c"]);
});

test("filterEvents applies sector, cadence, and confirmed-only controls together", () => {
  const events = [
    {
      id: "energy-weekly-confirmed",
      cadence: "weekly",
      commodity_sectors: ["energy"],
      is_confirmed: true,
    },
    {
      id: "macro-monthly-confirmed",
      cadence: "monthly",
      commodity_sectors: ["macro"],
      is_confirmed: true,
    },
    {
      id: "energy-monthly-provisional",
      cadence: "monthly",
      commodity_sectors: ["energy"],
      is_confirmed: false,
    },
  ];

  const filters = cloneDefaultFilters();
  filters.sectors = ["energy"];
  filters.cadences = ["monthly"];
  filters.confirmedOnly = true;

  const filtered = filterEvents(events, filters);

  assert.deepEqual(filtered.map((event) => event.id), []);
});

test("countActiveFilters tracks only non-default filter state", () => {
  const filters = cloneDefaultFilters();

  assert.equal(countActiveFilters(filters), 0);

  filters.sectors = ["energy", "macro"];
  filters.confirmedOnly = true;

  assert.equal(countActiveFilters(filters), 2);
});

test("toggleFilterSelection isolates from all and resets when emptied", () => {
  const allSectors = SECTOR_OPTIONS.map((option) => option.id);

  const isolated = toggleFilterSelection(allSectors, "energy", allSectors);
  assert.deepEqual(isolated, ["energy"]);

  const expanded = toggleFilterSelection(isolated, "macro", allSectors);
  assert.deepEqual(expanded, ["energy", "macro"]);

  const reset = toggleFilterSelection(["energy"], "energy", allSectors);
  assert.deepEqual(reset, allSectors);

  assert.equal(areAllFiltersSelected(reset, allSectors), true);
});

test("minimum anchor clamps week and month navigation to the current period", () => {
  const reference = new Date("2026-03-13T08:00:00Z");
  const maximum = new Date("2026-12-31T23:59:59Z");

  assert.equal(toIsoDay(getMinimumAnchorDate("week", reference)), "2026-03-09");
  assert.equal(toIsoDay(getMinimumAnchorDate("month", reference)), "2026-03-01");
  assert.equal(toIsoDay(getMaximumAnchorDate("week", maximum)), "2026-12-28");
  assert.equal(toIsoDay(getMaximumAnchorDate("month", maximum)), "2026-12-01");
  assert.equal(
    toIsoDay(clampAnchorDateForView("week", new Date("2026-03-02T00:00:00Z"), reference, maximum)),
    "2026-03-09"
  );
  assert.equal(
    toIsoDay(clampAnchorDateForView("month", new Date("2026-02-01T00:00:00Z"), reference, maximum)),
    "2026-03-01"
  );
  assert.equal(
    toIsoDay(clampAnchorDateForView("week", new Date("2027-01-04T00:00:00Z"), reference, maximum)),
    "2026-12-28"
  );
  assert.equal(
    toIsoDay(clampAnchorDateForView("month", new Date("2027-02-01T00:00:00Z"), reference, maximum)),
    "2026-12-01"
  );
});

test("endOfUtcYear rolls forward with the current year instead of a baked-in cap", () => {
  assert.equal(endOfUtcYear(new Date("2026-03-13T08:00:00Z")).toISOString(), "2026-12-31T23:59:59.000Z");
  assert.equal(endOfUtcYear(new Date("2027-01-03T08:00:00Z")).toISOString(), "2027-12-31T23:59:59.000Z");
});

test("searchEvents matches release names, organisers, and cadence terms", () => {
  const events = [
    {
      id: "fomc",
      name: "Federal Reserve FOMC Decision",
      organiser: "Board of Governors of the Federal Reserve System",
      source_label: "Federal Reserve",
      notes: "Standard statement release",
      commodity_sectors: ["macro"],
      cadence: "ad_hoc",
    },
    {
      id: "wasde",
      name: "USDA WASDE Monthly Report",
      organiser: "USDA Office of the Chief Economist",
      source_label: "USDA OCE",
      notes: "World Agricultural Supply and Demand Estimates",
      commodity_sectors: ["agriculture"],
      cadence: "monthly",
    },
  ];

  assert.deepEqual(searchEvents(events, "fomc").map((event) => event.id), ["fomc"]);
  assert.deepEqual(searchEvents(events, "chief economist").map((event) => event.id), ["wasde"]);
  assert.deepEqual(searchEvents(events, "monthly agriculture").map((event) => event.id), ["wasde"]);
});

test("mock calendar data covers the full filter surface", () => {
  const sectors = [...new Set(MOCK_EVENTS.flatMap((event) => event.commodity_sectors))].sort();
  const cadences = [...new Set(MOCK_EVENTS.map((event) => event.cadence))].sort();

  assert.equal(MOCK_EVENTS.length >= 30, true);
  assert.deepEqual(sectors, ["agriculture", "cross-commodity", "energy", "macro", "metals"]);
  assert.deepEqual(cadences, CADENCE_OPTIONS.map((option) => option.id).sort());
});
