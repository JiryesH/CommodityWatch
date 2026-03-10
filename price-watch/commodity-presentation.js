const GROUP_LABELS = {
  energy: "Energy",
  metals: "Metals",
  agri: "Agriculture",
};

const ENERGY_ICONS = {
  oil: "assets/energy-icons/oil-droplet-fill.svg",
  naturalGas: "assets/energy-icons/natgas-fire.svg",
  lng: "assets/energy-icons/lng-database-fill.svg",
  gasoline: "assets/energy-icons/gasoline-fuel-pump-fill.svg",
  thermalCoal: "assets/energy-icons/thermal-coal-minecart-loaded.svg",
  diesel: "assets/energy-icons/diesel-fuel-pump-diesel-fill.svg",
  rubber: "assets/energy-icons/rubber-vinyl-fill.svg",
};

const ENERGY_PRESETS = {
  crude_benchmarks: {
    family: "oil",
    code: "CRUDE",
    ticker: "BWD",
    venue: "FRED",
    name: "Brent / WTI / Dubai",
  },
  natural_gas_benchmarks: {
    family: "naturalGas",
    code: "GAS",
    ticker: "HH/TTF",
    venue: "FRED",
    name: "Henry Hub / TTF",
  },
  crude_oil_brent: {
    family: "oil",
    code: "BRENT",
    ticker: "BRN",
    venue: "FRED",
    name: "Brent",
  },
  crude_oil_wti: {
    family: "oil",
    code: "WTI",
    ticker: "WTI",
    venue: "FRED",
    name: "WTI",
  },
  crude_oil_dubai: {
    family: "oil",
    code: "DUBAI",
    ticker: "DUB",
    venue: "FRED",
    name: "Dubai",
  },
  natural_gas_henry_hub: {
    family: "naturalGas",
    code: "HH",
    ticker: "NG",
    venue: "FRED",
    name: "Henry Hub",
  },
  natural_gas_ttf: {
    family: "naturalGas",
    code: "TTF",
    ticker: "TTF",
    venue: "FRED",
    name: "TTF Gas",
  },
  lng_asia_japan_import_proxy: {
    family: "lng",
    code: "LNG",
    ticker: "JKM",
    venue: "FRED",
    name: "Asia LNG",
  },
  rbob_gasoline_spot_proxy: {
    family: "gasoline",
    code: "RBOB",
    ticker: "RB",
    venue: "FRED",
    name: "Gasoline",
  },
  heating_oil_no2_nyharbor: {
    family: "diesel",
    code: "ULSD",
    ticker: "HO",
    venue: "FRED",
    name: "Heating Oil",
  },
  thermal_coal_newcastle: {
    family: "thermalCoal",
    code: "COAL",
    ticker: "NEWC",
    venue: "FRED",
    name: "Thermal Coal",
  },
  rubber_rss3_monthly: {
    family: "rubber",
    code: "RUBBER",
    ticker: "RSS3",
    venue: "FRED",
    name: "Rubber",
  },
};

const METAL_PRESETS = {
  gold_worldbank_monthly: {
    family: "gold",
    symbol: "AU",
    number: "79",
    mass: "196.97",
    name: "Gold",
  },
  silver_worldbank_monthly: {
    family: "silver",
    symbol: "AG",
    number: "47",
    mass: "107.87",
    name: "Silver",
  },
  copper_worldbank_monthly: {
    family: "copper",
    symbol: "CU",
    number: "29",
    mass: "63.55",
    name: "Copper",
  },
  aluminium_worldbank_monthly: {
    family: "aluminum",
    symbol: "AL",
    number: "13",
    mass: "26.98",
    name: "Aluminum",
  },
  platinum_worldbank_monthly: {
    family: "platinum",
    symbol: "PT",
    number: "78",
    mass: "195.08",
    name: "Platinum",
  },
  palladium_imf_monthly: {
    family: "palladium",
    symbol: "PD",
    number: "46",
    mass: "106.42",
    name: "Palladium",
  },
  nickel_worldbank_monthly: {
    family: "nickel",
    symbol: "NI",
    number: "28",
    mass: "58.69",
    name: "Nickel",
  },
  zinc_worldbank_monthly: {
    family: "zinc",
    symbol: "ZN",
    number: "30",
    mass: "65.38",
    name: "Zinc",
  },
  lead_worldbank_monthly: {
    family: "lead",
    symbol: "PB",
    number: "82",
    mass: "207.2",
    name: "Lead",
  },
  iron_ore_62pct_china_monthly: {
    family: "iron",
    symbol: "FE",
    number: "26",
    mass: "55.85",
    name: "Iron Ore",
  },
  lithium_metal_imf_monthly: {
    family: "lithium",
    symbol: "LI",
    number: "3",
    mass: "6.94",
    name: "Lithium",
  },
  cobalt_imf_monthly: {
    family: "cobalt",
    symbol: "CO",
    number: "27",
    mass: "58.93",
    name: "Cobalt",
  },
};

const AGRI_PRESETS = {
  wheat_global_monthly_proxy: {
    family: "wheat",
    code: "WHT",
    ticker: "WH",
    venue: "FRED",
    name: "Wheat",
  },
  corn_global_monthly_proxy: {
    family: "corn",
    code: "CRN",
    ticker: "C",
    venue: "FRED",
    name: "Corn",
  },
  soybeans_global_monthly_proxy: {
    family: "soybeans",
    code: "SOY",
    ticker: "S",
    venue: "FRED",
    name: "Soybeans",
  },
  soybean_oil_global_monthly_proxy: {
    family: "soybeanOil",
    code: "SYO",
    ticker: "BO",
    venue: "FRED",
    name: "Soy Oil",
  },
  palm_oil_monthly_proxy: {
    family: "palmOil",
    code: "PALM",
    ticker: "PO",
    venue: "FRED",
    name: "Palm Oil",
  },
  rice_thai_5pct_monthly: {
    family: "rice",
    code: "RICE",
    ticker: "RT",
    venue: "FRED",
    name: "Rice",
  },
  lumber_monthly_ppi_proxy: {
    family: "lumber",
    code: "LMBR",
    ticker: "LBR",
    venue: "FRED",
    name: "Lumber",
  },
  coffee_arabica_monthly_proxy: {
    family: "coffee",
    code: "ARB",
    ticker: "KC",
    venue: "FRED",
    name: "Arabica",
  },
  coffee_robusta_monthly_proxy: {
    family: "coffee",
    code: "ROB",
    ticker: "RC",
    venue: "FRED",
    name: "Robusta",
  },
  sugar_no11_world_monthly_proxy: {
    family: "sugar",
    code: "SGR",
    ticker: "SB",
    venue: "FRED",
    name: "Sugar",
  },
  cotton_monthly_proxy: {
    family: "cotton",
    code: "CTN",
    ticker: "CT",
    venue: "FRED",
    name: "Cotton",
  },
  cocoa_monthly_proxy: {
    family: "cocoa",
    code: "COCOA",
    ticker: "CC",
    venue: "FRED",
    name: "Cocoa",
  },
};

const SERIES_DISPLAY_PRESETS = {
  crude_oil_brent: { label: "Crude", optionLabel: "Brent" },
  crude_oil_wti: { label: "Crude", optionLabel: "WTI" },
  crude_oil_dubai: { label: "Crude", optionLabel: "Dubai" },
  natural_gas_henry_hub: { label: "US Gas", optionLabel: "Henry Hub" },
  natural_gas_ttf: { label: "EU Gas", optionLabel: "TTF" },
  lng_asia_japan_import_proxy: { label: "LNG", optionLabel: "Asia LNG" },
  thermal_coal_newcastle: { label: "Coal", optionLabel: "Coal" },
  rbob_gasoline_spot_proxy: { label: "Gasoline", optionLabel: "RBOB" },
  heating_oil_no2_nyharbor: { label: "Diesel", optionLabel: "Diesel" },
  rubber_rss3_monthly: { label: "Rubber", optionLabel: "Rubber" },
  gold_worldbank_monthly: { label: "Gold", optionLabel: "Gold" },
  silver_worldbank_monthly: { label: "Silver", optionLabel: "Silver" },
  copper_worldbank_monthly: { label: "Copper", optionLabel: "Copper" },
  aluminium_worldbank_monthly: { label: "Aluminum", optionLabel: "Aluminum" },
  platinum_worldbank_monthly: { label: "Platinum", optionLabel: "Platinum" },
  palladium_imf_monthly: { label: "Palladium", optionLabel: "Palladium" },
  nickel_worldbank_monthly: { label: "Nickel", optionLabel: "Nickel" },
  zinc_worldbank_monthly: { label: "Zinc", optionLabel: "Zinc" },
  lead_worldbank_monthly: { label: "Lead", optionLabel: "Lead" },
  iron_ore_62pct_china_monthly: { label: "Iron Ore", optionLabel: "Iron Ore" },
  lithium_metal_imf_monthly: { label: "Lithium", optionLabel: "Lithium" },
  cobalt_imf_monthly: { label: "Cobalt", optionLabel: "Cobalt" },
  wheat_global_monthly_proxy: { label: "Wheat", optionLabel: "Wheat" },
  corn_global_monthly_proxy: { label: "Corn", optionLabel: "Corn" },
  soybeans_global_monthly_proxy: { label: "Soybeans", optionLabel: "Soybeans" },
  soybean_oil_global_monthly_proxy: { label: "Soy Oil", optionLabel: "Soy Oil" },
  palm_oil_monthly_proxy: { label: "Palm Oil", optionLabel: "Palm Oil" },
  rice_thai_5pct_monthly: { label: "Rice", optionLabel: "Rice" },
  lumber_monthly_ppi_proxy: { label: "Lumber", optionLabel: "Lumber" },
  coffee_arabica_monthly_proxy: { label: "Arabica Coffee", optionLabel: "Arabica" },
  coffee_robusta_monthly_proxy: { label: "Robusta Coffee", optionLabel: "Robusta" },
  sugar_no11_world_monthly_proxy: { label: "Sugar", optionLabel: "Sugar" },
  cotton_monthly_proxy: { label: "Cotton", optionLabel: "Cotton" },
  cocoa_monthly_proxy: { label: "Cocoa", optionLabel: "Cocoa" },
};

const GROUPED_CARD_PRESETS = [
  {
    id: "crude_benchmarks",
    label: "Crude",
    group: "energy",
    visualKey: "crude_benchmarks",
    defaultSeriesKey: "crude_oil_brent",
    seriesKeys: ["crude_oil_brent", "crude_oil_wti", "crude_oil_dubai"],
    lookupPatterns: [/\bbrent\b/i, /\bwti\b|\bwest texas intermediate\b/i, /\bdubai\b|\boman\b/i],
  },
  {
    id: "natural_gas_benchmarks",
    label: "Gas",
    group: "energy",
    visualKey: "natural_gas_benchmarks",
    defaultSeriesKey: "natural_gas_henry_hub",
    seriesKeys: ["natural_gas_henry_hub", "natural_gas_ttf"],
    lookupPatterns: [/\bhenry hub\b/i, /\bttf\b/i],
  },
  {
    id: "coffee_benchmarks",
    label: "Coffee",
    group: "agri",
    visualKey: "coffee_arabica_monthly_proxy",
    defaultSeriesKey: "coffee_arabica_monthly_proxy",
    seriesKeys: ["coffee_arabica_monthly_proxy", "coffee_robusta_monthly_proxy"],
    lookupPatterns: [/\barabica\b/i, /\brobusta\b/i, /\bcoffee\b/i],
  },
];

const GROUP_ORDER = ["energy", "metals", "agri"];
const METAL_KEYWORDS = [
  /\bgold\b/i,
  /\bsilver\b/i,
  /\bcopper\b/i,
  /\balumin(?:um|ium)\b/i,
  /\bplatinum\b/i,
  /\bpalladium\b/i,
  /\bnickel\b/i,
  /\bzinc\b/i,
  /\blead\b/i,
  /\biron ore\b/i,
  /\blithium\b/i,
  /\bcobalt\b/i,
];
const AGRI_KEYWORDS = [
  /\bwheat\b/i,
  /\bcorn\b/i,
  /\bmaize\b/i,
  /\bsoy(?:bean|beans)?\b/i,
  /\bsoy(?:bean)? oil\b/i,
  /\bpalm oil\b/i,
  /\brace\b/i,
  /\blumber\b/i,
  /\bcoffee\b/i,
  /\bsugar\b/i,
  /\bcotton\b/i,
  /\bcocoa\b/i,
];

function toShortCode(value) {
  return value
    .replaceAll(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part.slice(0, 4).toUpperCase())
    .join(" ");
}

function toTicker(value) {
  return value
    .replaceAll(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .map((part) => part[0] || "")
    .join("")
    .slice(0, 4)
    .toUpperCase();
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function buildCommodityLookupText(record) {
  if (!record) {
    return "";
  }

  return [
    record.series_key,
    record.seriesKey,
    record.target_concept,
    record.targetConcept,
    record.actual_series_name,
    record.actualSeriesName,
    record.benchmark_series,
    record.benchmarkSeries,
    record.displayLabel,
    record.optionLabel,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function matchesKeywordList(lookupText, patterns) {
  return patterns.some((pattern) => pattern.test(lookupText));
}

function deriveGroup(recordOrSeriesKey) {
  const seriesKey =
    typeof recordOrSeriesKey === "string"
      ? recordOrSeriesKey
      : recordOrSeriesKey?.series_key || recordOrSeriesKey?.seriesKey || "";

  if (METAL_PRESETS[seriesKey]) {
    return "metals";
  }

  if (AGRI_PRESETS[seriesKey]) {
    return "agri";
  }

  if (ENERGY_PRESETS[seriesKey]) {
    return "energy";
  }

  const lookupText =
    typeof recordOrSeriesKey === "string" ? recordOrSeriesKey.toLowerCase() : buildCommodityLookupText(recordOrSeriesKey);

  if (matchesKeywordList(lookupText, METAL_KEYWORDS)) {
    return "metals";
  }

  if (matchesKeywordList(lookupText, AGRI_KEYWORDS)) {
    return "agri";
  }

  return "energy";
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

function buildEnergyVisual(seriesKey, row) {
  const preset = ENERGY_PRESETS[seriesKey] || {
    family: "oil",
    code: toShortCode(row.target_concept),
    ticker: toTicker(row.target_concept),
    venue: row.source_name,
    name: row.target_concept,
  };

  return {
    type: "energyTile",
    tile: {
      code: preset.code,
      ticker: preset.ticker,
      venue: preset.venue,
      family: preset.family,
      name: preset.name,
      icon: ENERGY_ICONS[preset.family] || ENERGY_ICONS.oil,
    },
  };
}

function buildMetalVisual(seriesKey, row) {
  const preset = METAL_PRESETS[seriesKey] || {
    family: "default",
    symbol: toTicker(row.target_concept) || "M",
    number: "--",
    mass: "--",
    name: row.target_concept,
  };

  return {
    type: "periodicTile",
    tile: preset,
  };
}

function buildAgriVisual(seriesKey, row) {
  const preset = AGRI_PRESETS[seriesKey] || {
    family: "default",
    code: toShortCode(row.target_concept),
    ticker: toTicker(row.target_concept),
    venue: row.source_name,
    name: row.target_concept,
  };

  return {
    type: "agriTile",
    tile: preset,
  };
}

function buildVisual(seriesKey, row) {
  const group = deriveGroup({
    series_key: seriesKey,
    target_concept: row.target_concept,
    actual_series_name: row.actual_series_name,
  });

  if (group === "metals") {
    return buildMetalVisual(seriesKey, row);
  }

  if (group === "agri") {
    return buildAgriVisual(seriesKey, row);
  }

  return buildEnergyVisual(seriesKey, row);
}

function fallbackDisplayLabel(row) {
  return row.target_concept || row.actual_series_name || row.series_key;
}

function mergeSeriesOption(row, latestRow) {
  const preset = SERIES_DISPLAY_PRESETS[row.series_key] || {};

  return {
    seriesKey: row.series_key,
    actualSeriesName: row.actual_series_name,
    targetConcept: row.target_concept,
    optionLabel: preset.optionLabel || fallbackDisplayLabel(row),
    displayLabel: preset.label || fallbackDisplayLabel(row),
    group: deriveGroup(row),
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

function buildCardDefinition(config) {
  const representativeSeries =
    config.seriesOptions.find((series) => series.seriesKey === config.defaultSeriesKey) || config.seriesOptions[0];

  return {
    id: config.id,
    group: config.group,
    groupLabel: GROUP_LABELS[config.group],
    primaryLabel: config.label,
    defaultSeriesKey: representativeSeries?.seriesKey || config.defaultSeriesKey || null,
    seriesOptions: config.seriesOptions,
    visual: buildVisual(config.visualKey || representativeSeries?.seriesKey, {
      target_concept: representativeSeries?.targetConcept || config.label,
      source_name: representativeSeries?.sourceName || "FRED",
    }),
    sortLabel: config.label,
  };
}

function getGroupedPresetMatchRank(groupedPreset, seriesOption) {
  const exactSeriesIndex = groupedPreset.seriesKeys.indexOf(seriesOption.seriesKey);
  if (exactSeriesIndex !== -1) {
    return exactSeriesIndex;
  }

  if (!groupedPreset.lookupPatterns?.length) {
    return -1;
  }

  const lookupText = buildCommodityLookupText(seriesOption);
  return groupedPreset.lookupPatterns.findIndex((pattern) => pattern.test(lookupText));
}

export function buildCommodityDefinitions(seriesRows, latestRows) {
  const latestBySeriesKey = new Map(latestRows.map((row) => [row.series_key, row]));
  const seriesOptions = seriesRows.map((row) => mergeSeriesOption(row, latestBySeriesKey.get(row.series_key)));
  const consumedSeriesKeys = new Set();
  const definitions = [];

  GROUPED_CARD_PRESETS.forEach((groupedPreset) => {
    const groupedOptions = seriesOptions
      .map((option) => ({
        option,
        matchRank: getGroupedPresetMatchRank(groupedPreset, option),
      }))
      .filter(({ option, matchRank }) => matchRank !== -1 && !consumedSeriesKeys.has(option.seriesKey))
      .sort((left, right) => left.matchRank - right.matchRank)
      .map(({ option }) => option);

    if (!groupedOptions.length) {
      return;
    }

    groupedOptions.forEach((option) => consumedSeriesKeys.add(option.seriesKey));
    definitions.push(
      buildCardDefinition({
        id: groupedPreset.id,
        label: groupedPreset.label,
        group: groupedPreset.group,
        visualKey: groupedPreset.visualKey,
        defaultSeriesKey: groupedPreset.defaultSeriesKey,
        seriesOptions: groupedOptions,
      })
    );
  });

  seriesOptions.forEach((option) => {
    if (consumedSeriesKeys.has(option.seriesKey)) {
      return;
    }

    definitions.push(
      buildCardDefinition({
        id: option.seriesKey,
        label: option.displayLabel,
        group: option.group,
        defaultSeriesKey: option.seriesKey,
        seriesOptions: [option],
      })
    );
  });

  return definitions.sort((left, right) => {
    const groupDelta = GROUP_ORDER.indexOf(left.group) - GROUP_ORDER.indexOf(right.group);
    if (groupDelta !== 0) {
      return groupDelta;
    }

    return left.sortLabel.localeCompare(right.sortLabel);
  });
}

export function groupDefinitions(definitions) {
  return {
    energy: definitions.filter((definition) => definition.group === "energy"),
    metals: definitions.filter((definition) => definition.group === "metals"),
    agri: definitions.filter((definition) => definition.group === "agri"),
  };
}

export { GROUP_LABELS };
