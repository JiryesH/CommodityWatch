import contract from "./commodity-series-contract.json" with { type: "json" };

function freezeObjectRecord(record) {
  return Object.freeze(
    Object.fromEntries(Object.entries(record).map(([key, value]) => [key, Object.freeze({ ...value })]))
  );
}

function freezeSectorRecord(record) {
  return Object.freeze(
    record.map((sector) =>
      Object.freeze({
        ...sector,
        subsectors: Object.freeze(sector.subsectors.map((subsector) => Object.freeze({ ...subsector }))),
      })
    )
  );
}

function freezeGroupedCardRecord(record) {
  return Object.freeze(
    Object.fromEntries(
      Object.entries(record).map(([key, value]) => [
        key,
        Object.freeze({
          ...value,
          seriesKeys: Object.freeze([...value.seriesKeys]),
        }),
      ])
    )
  );
}

function freezeDashboardRecord(record) {
  return Object.freeze({
    ...(record || {}),
    default_home_series_keys: Object.freeze([...(record?.default_home_series_keys || [])]),
  });
}

function compareGroupedCardPlacement(left, right) {
  const leftPlacement = getCommodityPlacement(left.sectorId, left.subsectorId);
  const rightPlacement = getCommodityPlacement(right.sectorId, right.subsectorId);

  return (
    leftPlacement.sectorOrder - rightPlacement.sectorOrder ||
    leftPlacement.subsectorOrder - rightPlacement.subsectorOrder ||
    left.cardOrder - right.cardOrder
  );
}

export const COMMODITY_SERIES_CONTRACT = Object.freeze({
  ...contract,
  sectors: freezeSectorRecord(contract.sectors),
  series: freezeObjectRecord(contract.series),
  grouped_cards: freezeGroupedCardRecord(contract.grouped_cards),
  dashboard: freezeDashboardRecord(contract.dashboard),
});

export const ORDERED_COMMODITY_SECTORS = COMMODITY_SERIES_CONTRACT.sectors;
export const DEFAULT_HOME_SERIES_KEYS = Object.freeze([
  ...COMMODITY_SERIES_CONTRACT.dashboard.default_home_series_keys,
]);

const sectorById = new Map(ORDERED_COMMODITY_SECTORS.map((sector) => [sector.id, sector]));
const subsectorByKey = new Map(
  ORDERED_COMMODITY_SECTORS.flatMap((sector) =>
    sector.subsectors.map((subsector) => [
      `${sector.id}:${subsector.id}`,
      Object.freeze({
        ...subsector,
        sectorId: sector.id,
        sectorLabel: sector.label,
        sectorOrder: sector.order,
      }),
    ])
  )
);

export const ORDERED_GROUPED_CARD_ENTRIES = Object.freeze(
  Object.entries(COMMODITY_SERIES_CONTRACT.grouped_cards).sort(([, left], [, right]) =>
    compareGroupedCardPlacement(left, right)
  )
);

export function getCommodityPlacement(sectorId, subsectorId) {
  const sector = sectorById.get(sectorId);
  const subsector = subsectorByKey.get(`${sectorId}:${subsectorId}`);

  if (!sector || !subsector) {
    throw new Error(`Unknown taxonomy placement: ${sectorId}/${subsectorId}`);
  }

  return {
    sectorId,
    sectorLabel: sector.label,
    sectorOrder: sector.order,
    subsectorId,
    subsectorLabel: subsector.label,
    subsectorOrder: subsector.order,
  };
}

export function getCommodityContract() {
  return COMMODITY_SERIES_CONTRACT;
}
