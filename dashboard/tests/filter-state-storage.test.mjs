import test from "node:test";
import assert from "node:assert/strict";

import {
  HOME_FILTER_COLLAPSE_STORAGE_KEY,
  HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX,
  HOME_FILTER_STORAGE_KEY,
  HOME_FILTER_STORAGE_KEY_SUFFIX,
} from "../config.js";
import {
  createDefaultFilter,
  readStoredCollapseState,
  readStoredFilter,
  writeStoredCollapseState,
  writeStoredFilter,
} from "../filter-state.js";

function createStorage(seed = {}) {
  const store = new Map(Object.entries(seed));
  return {
    get length() {
      return store.size;
    },
    key(index) {
      return Array.from(store.keys())[index] ?? null;
    },
    getItem(key) {
      return store.has(key) ? store.get(key) : null;
    },
    setItem(key, value) {
      store.set(key, String(value));
    },
    removeItem(key) {
      store.delete(key);
    },
  };
}

test("readStoredFilter migrates a matching legacy-style storage key", () => {
  const legacyValue = JSON.stringify({ sectorId: "energy", groupId: null, commodityIds: [] });
  const legacyKey = `legacy${HOME_FILTER_STORAGE_KEY_SUFFIX}`;
  const storage = createStorage({ [legacyKey]: legacyValue });

  assert.deepEqual(readStoredFilter(storage), normalizeExpectedFilter("energy"));
  assert.equal(storage.getItem(HOME_FILTER_STORAGE_KEY), legacyValue);
  assert.equal(storage.getItem(legacyKey), null);
});

test("writeStoredFilter persists the canonical CommodityWatch storage key", () => {
  const storage = createStorage();

  writeStoredFilter({ sectorIds: ["metals"], groupIdsBySector: {}, commodityIds: [] }, storage);

  assert.equal(storage.getItem(HOME_FILTER_STORAGE_KEY), JSON.stringify(normalizeExpectedFilter("metals")));
  assert.equal(storage.getItem(`legacy${HOME_FILTER_STORAGE_KEY_SUFFIX}`), null);
});

test("readStoredCollapseState migrates a matching legacy-style storage key", () => {
  const legacyKey = `legacy${HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX}`;
  const storage = createStorage({ [legacyKey]: "true" });

  assert.equal(readStoredCollapseState(storage), true);
  assert.equal(storage.getItem(HOME_FILTER_COLLAPSE_STORAGE_KEY), "true");
  assert.equal(storage.getItem(legacyKey), null);
});

test("writeStoredCollapseState persists the canonical CommodityWatch storage key", () => {
  const storage = createStorage();

  writeStoredCollapseState(true, storage);

  assert.equal(storage.getItem(HOME_FILTER_COLLAPSE_STORAGE_KEY), "true");
  assert.equal(storage.getItem(`legacy${HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX}`), null);
});

function normalizeExpectedFilter(sectorId) {
  const filter = createDefaultFilter();
  filter.sectorIds = [sectorId];
  return filter;
}
