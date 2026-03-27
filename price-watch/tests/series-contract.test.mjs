import test from "node:test";
import assert from "node:assert/strict";

import { buildCommodityDefinitions } from "../commodity-presentation.js";
import { COMMODITY_SERIES_CONTRACT as contract } from "../../shared/commodity-series-contract.js";

function buildSeriesRows() {
  return Object.keys(contract.series).map((seriesKey, index) => ({
    series_key: seriesKey,
    target_concept: contract.series[seriesKey].displayLabel,
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

test("commodity presentation matches the shared taxonomy contract", () => {
  const definitions = buildCommodityDefinitions(buildSeriesRows(), buildLatestRows());
  const definitionBySeriesKey = new Map();

  definitions.forEach((definition) => {
    definition.seriesOptions.forEach((seriesOption) => {
      assert.equal(definitionBySeriesKey.has(seriesOption.seriesKey), false);
      definitionBySeriesKey.set(seriesOption.seriesKey, definition);
    });
  });

  assert.equal(definitionBySeriesKey.size, Object.keys(contract.series).length);

  Object.entries(contract.series).forEach(([seriesKey, metadata]) => {
    const definition = definitionBySeriesKey.get(seriesKey);
    assert.ok(definition, `Missing definition for ${seriesKey}`);
    assert.equal(definition.sectorId, metadata.sectorId);
    assert.equal(definition.subsectorId, metadata.subsectorId);
  });

  Object.entries(contract.grouped_cards).forEach(([cardId, groupedCard]) => {
    const definition = definitions.find((entry) => entry.id === cardId);
    assert.ok(definition, `Missing grouped card ${cardId}`);
    assert.equal(definition.sectorId, groupedCard.sectorId);
    assert.equal(definition.subsectorId, groupedCard.subsectorId);
    assert.deepEqual(
      definition.seriesOptions.map((seriesOption) => seriesOption.seriesKey),
      groupedCard.seriesKeys
    );
  });
});
