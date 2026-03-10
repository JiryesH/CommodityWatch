import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

import { buildCommodityDefinitions } from "../commodity-presentation.js";

const contractPath = new URL("../../shared/commodity-series-contract.json", import.meta.url);
const contract = JSON.parse(fs.readFileSync(contractPath, "utf8"));

function buildSeriesRows() {
  return Object.keys(contract.series).map((seriesKey, index) => ({
    series_key: seriesKey,
    target_concept: seriesKey.replaceAll("_", " "),
    actual_series_name: `Series ${index + 1}`,
    match_type: "exact",
    source_name: "Fixture",
    source_series_code: `code-${seriesKey}`,
    source_url: `https://example.test/${seriesKey}`,
    frequency: "monthly",
    unit: "USD",
    currency: "USD",
    geography: "Global",
    active: true,
    notes: "Fixture",
    updated_at: "2026-03-10T00:00:00Z",
  }));
}

function buildLatestRows() {
  return Object.keys(contract.series).map((seriesKey, index) => ({
    series_key: seriesKey,
    observation_date: "2026-03-01",
    value: index + 1,
    previous_value: index,
    delta_value: 1,
    delta_pct: 1,
    unit: "USD",
    currency: "USD",
    frequency: "monthly",
    source_name: "Fixture",
    source_series_code: `code-${seriesKey}`,
    updated_at: "2026-03-10T00:00:00Z",
  }));
}

test("commodity presentation matches the shared series contract", () => {
  const definitions = buildCommodityDefinitions(buildSeriesRows(), buildLatestRows());
  const definitionBySeriesKey = new Map();

  definitions.forEach((definition) => {
    definition.seriesOptions.forEach((option) => {
      assert.equal(definitionBySeriesKey.has(option.seriesKey), false);
      definitionBySeriesKey.set(option.seriesKey, definition);
    });
  });

  assert.equal(definitionBySeriesKey.size, Object.keys(contract.series).length);

  Object.entries(contract.series).forEach(([seriesKey, metadata]) => {
    const definition = definitionBySeriesKey.get(seriesKey);
    assert.ok(definition, `Missing definition for ${seriesKey}`);
    assert.equal(definition.group, metadata.group);
  });

  Object.entries(contract.grouped_cards).forEach(([cardId, expectedSeriesKeys]) => {
    const definition = definitions.find((entry) => entry.id === cardId);
    assert.ok(definition, `Missing grouped card ${cardId}`);
    assert.deepEqual(
      definition.seriesOptions.map((option) => option.seriesKey),
      expectedSeriesKeys
    );
  });
});
