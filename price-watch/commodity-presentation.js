import {
  ARTIFACT_ENTITY_OVERRIDES,
  ARTIFACT_FAMILY_PRESETS,
  ARTIFACT_PALETTES,
} from "./artifact-tile-presets.js";
import {
  COMMODITY_SERIES_CONTRACT as contract,
  ORDERED_COMMODITY_SECTORS as SECTORS,
  ORDERED_GROUPED_CARD_ENTRIES as GROUPED_CARD_ENTRIES,
  getCommodityPlacement,
} from "../shared/commodity-series-contract.js";

const ENERGY_ICONS = {
  oil: "assets/energy-icons/oil-droplet-fill.svg",
  naturalGas: "assets/energy-icons/natgas-fire.svg",
  lng: "assets/energy-icons/lng-database-fill.svg",
  gasoline: "assets/energy-icons/gasoline-fuel-pump-fill.svg",
  diesel: "assets/energy-icons/diesel-fuel-pump-diesel-fill.svg",
  jetFuel: "assets/energy-icons/gasoline-fuel-pump-fill.svg",
  propane: "assets/energy-icons/natgas-fire.svg",
  thermalCoal: "assets/energy-icons/thermal-coal-minecart-loaded.svg",
  default: "assets/energy-icons/oil-droplet-fill.svg",
};

const ENERGY_VISUAL_PRESETS = {
  crude_benchmarks: {
    family: "oil",
    code: "CRUDE",
    ticker: "BWD",
    name: "Brent / WTI / Dubai",
  },
  natural_gas_benchmarks: {
    family: "naturalGas",
    code: "GAS",
    ticker: "HH/TTF",
    name: "Henry Hub / TTF",
  },
  gasoline_benchmarks: {
    family: "gasoline",
    code: "GAS",
    ticker: "RBOB",
    name: "RBOB / USGC / NYH",
  },
  diesel_benchmarks: {
    family: "diesel",
    code: "DSL",
    ticker: "ULSD",
    name: "ULSD USGC / NYH",
  },
  coal_benchmarks: {
    family: "thermalCoal",
    code: "COAL",
    ticker: "NEWC/SA",
    name: "Newcastle / South Africa",
  },
  crude_oil_brent: {
    family: "oil",
    code: "BRENT",
    ticker: "BRN",
    name: "Brent",
  },
  crude_oil_wti: {
    family: "oil",
    code: "WTI",
    ticker: "WTI",
    name: "WTI",
  },
  crude_oil_dubai: {
    family: "oil",
    code: "DUBAI",
    ticker: "DUB",
    name: "Dubai",
  },
  natural_gas_henry_hub: {
    family: "naturalGas",
    code: "HH",
    ticker: "NG",
    name: "Henry Hub",
  },
  natural_gas_ttf: {
    family: "naturalGas",
    code: "TTF",
    ticker: "TTF",
    name: "TTF Gas",
  },
  lng_asia_japan_import_proxy: {
    family: "lng",
    code: "LNG",
    ticker: "JKM",
    name: "Asia LNG",
  },
  rbob_gasoline_spot_proxy: {
    family: "gasoline",
    code: "RBOB",
    ticker: "RB",
    name: "RBOB",
  },
  heating_oil_no2_nyharbor: {
    family: "diesel",
    code: "HOIL",
    ticker: "HO",
    name: "Heating oil",
  },
  jet_fuel_usgc_daily: {
    family: "jetFuel",
    code: "JET",
    ticker: "JET",
    name: "Jet fuel",
  },
  propane_mont_belvieu_daily: {
    family: "propane",
    code: "LPG",
    ticker: "MB",
    name: "Propane",
  },
  ulsd_usgc_daily: {
    family: "diesel",
    code: "ULSD",
    ticker: "USGC",
    name: "ULSD USGC",
  },
  ulsd_nyh_daily: {
    family: "diesel",
    code: "ULSD",
    ticker: "NYH",
    name: "ULSD NYH",
  },
  gasoline_regular_usgc_daily: {
    family: "gasoline",
    code: "GAS",
    ticker: "USGC",
    name: "Gasoline USGC",
  },
  gasoline_regular_nyh_daily: {
    family: "gasoline",
    code: "GAS",
    ticker: "NYH",
    name: "Gasoline NYH",
  },
  thermal_coal_newcastle: {
    family: "thermalCoal",
    code: "COAL",
    ticker: "NEWC",
    name: "Newcastle",
  },
  coal_south_africa_monthly: {
    family: "thermalCoal",
    code: "COAL",
    ticker: "SA",
    name: "South Africa",
  },
};

const METAL_TILE_PRESETS = {
  gold: {
    symbol: "AU",
    number: "79",
    mass: "196.97",
    name: "Gold",
  },
  silver: {
    symbol: "AG",
    number: "47",
    mass: "107.87",
    name: "Silver",
  },
  copper: {
    symbol: "CU",
    number: "29",
    mass: "63.55",
    name: "Copper",
  },
  aluminum: {
    symbol: "AL",
    number: "13",
    mass: "26.98",
    name: "Aluminium",
  },
  platinum: {
    symbol: "PT",
    number: "78",
    mass: "195.08",
    name: "Platinum",
  },
  palladium: {
    symbol: "PD",
    number: "46",
    mass: "106.42",
    name: "Palladium",
  },
  nickel: {
    symbol: "NI",
    number: "28",
    mass: "58.69",
    name: "Nickel",
  },
  zinc: {
    symbol: "ZN",
    number: "30",
    mass: "65.38",
    name: "Zinc",
  },
  lead: {
    symbol: "PB",
    number: "82",
    mass: "207.2",
    name: "Lead",
  },
  tin: {
    symbol: "SN",
    number: "50",
    mass: "118.71",
    name: "Tin",
  },
  iron: {
    symbol: "FE",
    number: "26",
    mass: "55.85",
    name: "Iron ore",
  },
  lithium: {
    symbol: "LI",
    number: "3",
    mass: "6.94",
    name: "Lithium",
  },
  cobalt: {
    symbol: "CO",
    number: "27",
    mass: "58.93",
    name: "Cobalt",
  },
};

const MARKET_VISUAL_PRESETS = {
  wheat_benchmarks: {
    code: "WHEAT",
    ticker: "GLB/US",
    name: "Global / US SRW",
  },
  rice_benchmarks: {
    code: "RICE",
    ticker: "TH/VN",
    name: "Thai / Vietnam",
  },
  sugar_benchmarks: {
    code: "SUGAR",
    ticker: "WLD/EU/US",
    name: "World / EU / US",
  },
  coffee_benchmarks: {
    code: "COFFEE",
    ticker: "ARA/ROB",
    name: "Arabica / Robusta",
  },
  banana_benchmarks: {
    code: "BANANA",
    ticker: "EU/US",
    name: "Europe / US",
  },
  rubber_benchmarks: {
    code: "RUBBER",
    ticker: "RSS/TSR",
    name: "RSS3 / TSR20",
  },
};

function toShortCode(value) {
  return String(value || "")
    .replaceAll(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part.slice(0, 4).toUpperCase())
    .join(" ");
}

function toTicker(value) {
  return String(value || "")
    .replaceAll(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .map((part) => part[0] || "")
    .join("")
    .slice(0, 4)
    .toUpperCase();
}

function toCompactSourceLabel(value) {
  const label = String(value || "").trim();
  if (!label) {
    return "FRED";
  }

  if (/world bank/i.test(label)) {
    return "WB";
  }

  if (/fred/i.test(label)) {
    return "FRED";
  }

  if (/food and agriculture organization|^fao$/i.test(label)) {
    return "FAO";
  }

  if (/argus/i.test(label)) {
    return "ARGUS";
  }

  if (/mpob/i.test(label)) {
    return "MPOB";
  }

  const acronym = label
    .split(/[^A-Za-z0-9]+/g)
    .filter(Boolean)
    .map((part) => part[0] || "")
    .join("")
    .toUpperCase();

  if (acronym) {
    return acronym.slice(0, 6);
  }

  return label.slice(0, 6).toUpperCase();
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function inferDecimals(unit, value) {
  if (unit && /cents?/i.test(unit)) {
    return 2;
  }

  if (!isFiniteNumber(value)) {
    return 2;
  }

  const absoluteValue = Math.abs(value);

  if (absoluteValue >= 1000) {
    return 0;
  }

  if (absoluteValue >= 100) {
    return 1;
  }

  if (absoluteValue >= 10) {
    return 2;
  }

  return 3;
}

function fallbackDisplayLabel(row) {
  return row.target_concept || row.actual_series_name || row.series_key;
}

function buildEnergyVisual(entityId, row, taxonomyConfig) {
  const preset = ENERGY_VISUAL_PRESETS[entityId] || {
    family: taxonomyConfig.visualFamily,
    code: toShortCode(taxonomyConfig.shortLabel),
    ticker: toTicker(taxonomyConfig.shortLabel),
    name: taxonomyConfig.shortLabel,
  };

  return {
    type: "energyTile",
    tile: {
      family: preset.family,
      code: preset.code,
      ticker: preset.ticker,
      venue: row?.source_name || "FRED",
      name: preset.name,
      icon: ENERGY_ICONS[preset.family] || ENERGY_ICONS.default,
    },
  };
}

function buildMetalVisual(taxonomyConfig) {
  const preset = METAL_TILE_PRESETS[taxonomyConfig.visualFamily] || {
    symbol: toTicker(taxonomyConfig.shortLabel) || "M",
    number: "--",
    mass: "--",
    name: taxonomyConfig.shortLabel,
  };

  return {
    type: "periodicTile",
    tile: {
      family: taxonomyConfig.visualFamily,
      symbol: preset.symbol,
      number: preset.number,
      mass: preset.mass,
      name: preset.name,
    },
  };
}

function buildMarketVisual(entityId, row, taxonomyConfig) {
  const preset = MARKET_VISUAL_PRESETS[entityId] || {
    code: toShortCode(taxonomyConfig.shortLabel),
    ticker: toTicker(taxonomyConfig.shortLabel),
    name: taxonomyConfig.shortLabel,
  };

  return {
    type: "marketTile",
    tile: {
      family: taxonomyConfig.visualFamily,
      code: preset.code,
      ticker: preset.ticker,
      venue: row?.source_name || "FRED",
      name: preset.name,
    },
  };
}

function buildAgricultureVisual(entityId, row, taxonomyConfig) {
  return buildArtifactVisual(entityId, row, taxonomyConfig);
}

function buildArtifactVisual(entityId, row, taxonomyConfig) {
  const familyPreset = ARTIFACT_FAMILY_PRESETS[taxonomyConfig.visualFamily] || {};
  const entityPreset = ARTIFACT_ENTITY_OVERRIDES[entityId] || {};
  const paletteKey = entityPreset.paletteKey || familyPreset.paletteKey || "default";
  const palette = ARTIFACT_PALETTES[paletteKey] || ARTIFACT_PALETTES.default;
  const sourceName = row?.source_name || "FRED";

  return {
    type: "artifactTile",
    tile: {
      family: taxonomyConfig.visualFamily,
      sectorClass: entityPreset.sectorClass || familyPreset.sectorClass || "agriculture",
      pattern: entityPreset.pattern || familyPreset.pattern || "grain",
      code: entityPreset.code || familyPreset.code || toTicker(taxonomyConfig.shortLabel) || "CM",
      badge: entityPreset.badge || familyPreset.badge || toTicker(taxonomyConfig.shortLabel) || "CM",
      venue: sourceName,
      venueBadge: toCompactSourceLabel(sourceName),
      nameLabel: entityPreset.nameLabel || familyPreset.nameLabel || taxonomyConfig.shortLabel,
      descriptor:
        entityPreset.descriptor || familyPreset.descriptor || toShortCode(taxonomyConfig.shortLabel) || "MARKET",
      lot: entityPreset.lot || familyPreset.lot || "TRADE LOT",
      serial: entityPreset.serial || familyPreset.serial || "LOT REGISTER / MARKET BOARD",
      stamp: entityPreset.stamp || familyPreset.stamp || "MARKET LOT",
      emblem: Boolean(entityPreset.emblem ?? familyPreset.emblem),
      palette,
    },
  };
}

function buildVisual(entityId, row, taxonomyConfig) {
  if (taxonomyConfig.sectorId === "energy") {
    return buildEnergyVisual(entityId, row, taxonomyConfig);
  }

  if (taxonomyConfig.sectorId === "metals_and_mining") {
    return buildMetalVisual(taxonomyConfig);
  }

  if (taxonomyConfig.sectorId === "agriculture") {
    return buildAgricultureVisual(entityId, row, taxonomyConfig);
  }

  if (taxonomyConfig.sectorId === "fertilizers_and_agricultural_chemicals") {
    return buildArtifactVisual(entityId, row, taxonomyConfig);
  }

  if (taxonomyConfig.sectorId === "livestock_dairy_and_seafood") {
    return buildArtifactVisual(entityId, row, taxonomyConfig);
  }

  if (taxonomyConfig.sectorId === "forest_and_wood_products") {
    return buildArtifactVisual(entityId, row, taxonomyConfig);
  }

  return buildMarketVisual(entityId, row, taxonomyConfig);
}

function buildSeriesOption(row, latestRow, taxonomyConfig) {
  const placement = getCommodityPlacement(taxonomyConfig.sectorId, taxonomyConfig.subsectorId);

  return {
    seriesKey: row.series_key,
    actualSeriesName: row.actual_series_name,
    targetConcept: row.target_concept,
    optionLabel: taxonomyConfig.shortLabel,
    displayLabel: taxonomyConfig.displayLabel || fallbackDisplayLabel(row),
    sortLabel: taxonomyConfig.displayLabel || fallbackDisplayLabel(row),
    sectorId: placement.sectorId,
    sectorLabel: placement.sectorLabel,
    sectorOrder: placement.sectorOrder,
    subsectorId: placement.subsectorId,
    subsectorLabel: placement.subsectorLabel,
    subsectorOrder: placement.subsectorOrder,
    visualFamily: taxonomyConfig.visualFamily,
    cardOrder: taxonomyConfig.cardOrder,
    supportsRelatedHeadlines: Boolean(taxonomyConfig.supports_related_headlines),
    sourceName: latestRow?.source_name || row.source_name,
    sourceUrl: row.source_url,
    sourceSeriesCode: latestRow?.source_series_code || row.source_series_code,
    frequency: latestRow?.frequency || row.frequency,
    unit: latestRow?.unit || row.unit,
    currency: latestRow?.currency || row.currency,
    geography: latestRow?.geography || row.geography,
    notes: latestRow?.notes || row.notes,
    matchType: row.match_type,
    observationDate: latestRow?.observation_date || null,
    updatedAt: latestRow?.updated_at || row.updated_at,
    value: latestRow?.value ?? null,
    previousValue: latestRow?.previous_value ?? null,
    deltaValue: latestRow?.delta_value ?? null,
    deltaPct: latestRow?.delta_pct ?? null,
    decimals: inferDecimals(latestRow?.unit || row.unit, latestRow?.value),
  };
}

function buildDefinition(config) {
  const placement = getCommodityPlacement(config.sectorId, config.subsectorId);
  const representativeSeries =
    config.seriesOptions.find((seriesOption) => seriesOption.seriesKey === config.defaultSeriesKey) || config.seriesOptions[0];

  const taxonomyConfig = {
    sectorId: placement.sectorId,
    subsectorId: placement.subsectorId,
    displayLabel: config.displayLabel,
    shortLabel: config.shortLabel,
    visualFamily: config.visualFamily,
  };

  return {
    id: config.id,
    primaryLabel: config.shortLabel,
    displayLabel: config.displayLabel,
    sortLabel: config.displayLabel,
    isGrouped: Boolean(config.isGrouped),
    defaultSeriesKey: representativeSeries?.seriesKey || config.defaultSeriesKey || null,
    seriesOptions: config.seriesOptions,
    visual: buildVisual(config.id, representativeSeries, taxonomyConfig),
    visualFamily: config.visualFamily,
    sectorId: placement.sectorId,
    sectorLabel: placement.sectorLabel,
    sectorOrder: placement.sectorOrder,
    subsectorId: placement.subsectorId,
    subsectorLabel: placement.subsectorLabel,
    subsectorOrder: placement.subsectorOrder,
    cardOrder: config.cardOrder,
  };
}

export function getCommodityTaxonomy() {
  return SECTORS.map((sector) => ({
    ...sector,
    subsectors: sector.subsectors.map((subsector) => ({ ...subsector })),
  }));
}

export function getCommodityContract() {
  return contract;
}

export function buildCommodityDefinitions(seriesRows, latestRows) {
  const latestBySeriesKey = new Map(latestRows.map((row) => [row.series_key, row]));
  const optionBySeriesKey = new Map();

  seriesRows.forEach((row) => {
    const taxonomyConfig = contract.series[row.series_key];
    if (!taxonomyConfig) {
      return;
    }

    optionBySeriesKey.set(row.series_key, buildSeriesOption(row, latestBySeriesKey.get(row.series_key), taxonomyConfig));
  });

  const definitions = [];
  const consumedSeriesKeys = new Set();

  GROUPED_CARD_ENTRIES.forEach(([cardId, groupedConfig]) => {
    const groupedOptions = groupedConfig.seriesKeys
      .map((seriesKey) => optionBySeriesKey.get(seriesKey))
      .filter(Boolean);

    if (!groupedOptions.length) {
      return;
    }

    groupedOptions.forEach((seriesOption) => consumedSeriesKeys.add(seriesOption.seriesKey));
    definitions.push(
      buildDefinition({
        id: cardId,
        displayLabel: groupedConfig.displayLabel,
        shortLabel: groupedConfig.shortLabel,
        visualFamily: groupedConfig.visualFamily,
        sectorId: groupedConfig.sectorId,
        subsectorId: groupedConfig.subsectorId,
        cardOrder: groupedConfig.cardOrder,
        defaultSeriesKey: groupedConfig.defaultSeriesKey,
        seriesOptions: groupedOptions,
        isGrouped: true,
      })
    );
  });

  Array.from(optionBySeriesKey.values())
    .filter((seriesOption) => !consumedSeriesKeys.has(seriesOption.seriesKey))
    .forEach((seriesOption) => {
      definitions.push(
        buildDefinition({
          id: seriesOption.seriesKey,
          displayLabel: seriesOption.displayLabel,
          shortLabel: seriesOption.optionLabel,
          visualFamily: seriesOption.visualFamily,
          sectorId: seriesOption.sectorId,
          subsectorId: seriesOption.subsectorId,
          cardOrder: seriesOption.cardOrder,
          defaultSeriesKey: seriesOption.seriesKey,
          seriesOptions: [seriesOption],
          isGrouped: false,
        })
      );
    });

  return definitions.sort(
    (left, right) =>
      left.sectorOrder - right.sectorOrder ||
      left.subsectorOrder - right.subsectorOrder ||
      left.cardOrder - right.cardOrder ||
      left.sortLabel.localeCompare(right.sortLabel)
  );
}
