import test from "node:test";
import assert from "node:assert/strict";

import { getCommodityTaxonomy } from "../commodity-presentation.js";
import { CommodityFilterSelection } from "../filter-selection.js";

function buildSelection() {
  return new CommodityFilterSelection(getCommodityTaxonomy());
}

test("sector selection starts fully selected across all sectors and subsectors", () => {
  const selection = buildSelection();

  assert.equal(selection.isAllSelected(), true);
  assert.equal(selection.getSectorSelectionState("energy"), "full");
  assert.equal(selection.isSubsectorSelected("energy", "crude_oil_benchmarks"), true);
  assert.equal(selection.isSubsectorSelected("energy", "coal"), true);
});

test("selecting a sector from all state isolates it while keeping all of its subsectors selected", () => {
  const selection = buildSelection();

  selection.toggleSectorSelection("energy");

  assert.equal(selection.isAllSelected(), false);
  assert.deepEqual([...selection.selectedSectors], ["energy"]);
  assert.equal(selection.getSectorSelectionState("energy"), "full");
  assert.equal(selection.isSubsectorSelected("energy", "crude_oil_benchmarks"), true);
  assert.equal(selection.isSubsectorSelected("energy", "natural_gas_and_lng"), true);
  assert.equal(selection.isSubsectorSelected("metals_and_mining", "precious_metals"), false);
});

test("toggling one subsector from a fully selected sector creates a partial parent state", () => {
  const selection = buildSelection();

  selection.toggleSectorSelection("energy");
  selection.toggleSubsectorSelection("energy", "coal");

  assert.equal(selection.getSectorSelectionState("energy"), "partial");
  assert.equal(selection.hasPartialSubsectorSelection(), true);
  assert.equal(selection.isSubsectorSelected("energy", "coal"), false);
  assert.equal(selection.isSubsectorSelected("energy", "crude_oil_benchmarks"), true);

  selection.toggleSubsectorSelection("energy", "coal");

  assert.equal(selection.getSectorSelectionState("energy"), "full");
  assert.equal(selection.hasPartialSubsectorSelection(), false);
  assert.equal(selection.isSubsectorSelected("energy", "coal"), true);
});

test("clearing subsector refinements restores full selection for each selected sector", () => {
  const selection = buildSelection();

  selection.toggleSectorSelection("energy");
  selection.toggleSectorSelection("agriculture");
  selection.toggleSubsectorSelection("energy", "coal");
  selection.toggleSubsectorSelection("agriculture", "soft_commodities");

  assert.equal(selection.getSectorSelectionState("energy"), "partial");
  assert.equal(selection.getSectorSelectionState("agriculture"), "partial");

  selection.clearSubsectorSelections();

  assert.equal(selection.getSectorSelectionState("energy"), "full");
  assert.equal(selection.getSectorSelectionState("agriculture"), "full");
  assert.equal(selection.hasPartialSubsectorSelection(), false);
});

test("removing the last subsector from the only selected sector resets back to all sectors", () => {
  const selection = buildSelection();

  selection.toggleSectorSelection("forest_and_wood_products");
  selection.toggleSubsectorSelection("forest_and_wood_products", "wood_products");

  assert.equal(selection.isAllSelected(), true);
  assert.equal(selection.getSectorSelectionState("forest_and_wood_products"), "full");
  assert.equal(selection.getSectorSelectionState("energy"), "full");
});
