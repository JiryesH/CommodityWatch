export const COMMODITY_GROUPS = [
  {
    slug: "all",
    label: "All",
    commodityCodes: [],
    shortDescription: "Cross-market inventory snapshot across energy, metals, and agriculture.",
  },
  {
    slug: "energy",
    label: "Energy",
    commodityCodes: ["crude_oil", "gasoline", "distillates", "propane", "jet_fuel"],
    shortDescription: "Crude and refined product storage balances.",
  },
  {
    slug: "natural-gas",
    label: "Natural Gas",
    commodityCodes: ["natural_gas"],
    shortDescription: "US and European gas storage balances.",
  },
  {
    slug: "base-metals",
    label: "Base Metals",
    commodityCodes: ["copper", "aluminum", "nickel", "zinc", "lead", "tin"],
    shortDescription: "Exchange warehouse stocks across base metals.",
  },
  {
    slug: "grains",
    label: "Grains",
    commodityCodes: ["corn", "wheat", "soybeans", "oilseeds"],
    shortDescription: "Official grain and oilseed stock reports.",
  },
  {
    slug: "softs",
    label: "Softs",
    commodityCodes: ["coffee", "cocoa", "sugar", "cotton"],
    shortDescription: "Certified and exchange-reported soft commodity stocks.",
  },
  {
    slug: "precious-metals",
    label: "Precious Metals",
    commodityCodes: ["gold", "silver", "platinum", "palladium"],
    shortDescription: "Vault, warehouse, and ETF stock context for precious metals.",
  },
];

export const FRESHNESS_BADGES = {
  live: { label: "Live", tone: "fresh" },
  current: { label: "Live", tone: "fresh" },
  lagged: { label: "Lagged", tone: "waiting" },
  structural: { label: "Structural", tone: "structural" },
  aged: { label: "Aged", tone: "aged" },
};

const SNAPSHOT_GROUP_BY_COMMODITY = {
  crude_oil: "Crude Oil",
  gasoline: "Refined Products",
  distillates: "Refined Products",
  propane: "Refined Products",
  jet_fuel: "Refined Products",
  natural_gas: "Natural Gas",
  copper: "Base Metals",
  aluminum: "Base Metals",
  nickel: "Base Metals",
  zinc: "Base Metals",
  lead: "Base Metals",
  tin: "Base Metals",
  corn: "Grains",
  wheat: "Grains",
  soybeans: "Grains",
  oilseeds: "Grains",
  coffee: "Softs",
  cocoa: "Softs",
  sugar: "Softs",
  cotton: "Softs",
  gold: "Precious Metals",
  silver: "Precious Metals",
  platinum: "Precious Metals",
  palladium: "Precious Metals",
};

const FILTER_PILL_COLOR_BY_GROUP = {
  all: "var(--color-primary)",
  energy: "var(--color-energy)",
  "natural-gas": "color-mix(in srgb, var(--color-energy) 56%, var(--color-metals) 44%)",
  "base-metals": "var(--color-metals)",
  grains: "var(--color-agri)",
  softs: "var(--color-agri)",
  "precious-metals": "var(--color-metals)",
};

const REGISTRY = {
  EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR: {
    description: "Weekly U.S. commercial crude oil stocks excluding the Strategic Petroleum Reserve.",
    sourceLabel: "EIA",
    snapshotGroup: "Crude Oil",
  },
  EIA_CRUDE_US_CUSHING_STOCKS: {
    description: "Weekly commercial crude stocks at Cushing, Oklahoma.",
    sourceLabel: "EIA",
    snapshotGroup: "Crude Oil",
  },
  EIA_CRUDE_US_SPR_STOCKS: {
    description: "Weekly Strategic Petroleum Reserve crude stocks.",
    sourceLabel: "EIA",
    snapshotGroup: "Crude Oil",
  },
  EIA_GASOLINE_US_TOTAL_STOCKS: {
    description: "Weekly total motor gasoline stocks for the United States.",
    sourceLabel: "EIA",
    snapshotGroup: "Refined Products",
  },
  EIA_DISTILLATE_US_TOTAL_STOCKS: {
    description: "Weekly distillate fuel oil stocks for the United States.",
    sourceLabel: "EIA",
    snapshotGroup: "Refined Products",
  },
  EIA_PROPANE_US_TOTAL_STOCKS: {
    description: "Weekly U.S. propane and propylene stocks.",
    sourceLabel: "EIA",
    snapshotGroup: "Refined Products",
  },
  EIA_JET_FUEL_US_TOTAL_STOCKS: {
    description: "Weekly U.S. kerosene-type jet fuel stocks.",
    sourceLabel: "EIA",
    snapshotGroup: "Refined Products",
  },
  EIA_NATURAL_GAS_US_WORKING_STORAGE: {
    description: "Weekly working gas in storage across the Lower 48.",
    sourceLabel: "EIA",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_TOTAL_STORAGE: {
    description: "Daily EU aggregate gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_DE_STORAGE: {
    description: "Daily German gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_FR_STORAGE: {
    description: "Daily French gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_IT_STORAGE: {
    description: "Daily Italian gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_NL_STORAGE: {
    description: "Daily Dutch gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
  GIE_NATURAL_GAS_EU_AT_STORAGE: {
    description: "Daily Austrian gas in storage from GIE AGSI+.",
    sourceLabel: "GIE / AGSI+",
    snapshotGroup: "Natural Gas",
  },
};

const absoluteFormatter = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

function precisionForUnit(unit) {
  if (unit === "mb") {
    return 1;
  }
  if (unit === "bcf") {
    return 0;
  }
  if (unit === "twh") {
    return 1;
  }
  if (unit === "%") {
    return 1;
  }
  return 1;
}

export function isCommodityGroupSlug(value) {
  return COMMODITY_GROUPS.some((group) => group.slug === value);
}

export function getCommodityGroup(slug) {
  return COMMODITY_GROUPS.find((group) => group.slug === slug) || COMMODITY_GROUPS[0];
}

export function snapshotGroupForCommodity(commodityCode) {
  return SNAPSHOT_GROUP_BY_COMMODITY[commodityCode] || "Inventory";
}

export function filterPillColorForGroup(slug) {
  return FILTER_PILL_COLOR_BY_GROUP[slug] || "var(--color-subtext)";
}

export function getIndicatorRegistryEntry(code, commodityCode = "") {
  if (REGISTRY[code]) {
    return REGISTRY[code];
  }

  const inferredSnapshotGroup = snapshotGroupForCommodity(commodityCode);

  if (String(code || "").startsWith("EIA_")) {
    return {
      description: "Public-domain inventory indicator from EIA.",
      sourceLabel: "EIA",
      snapshotGroup: inferredSnapshotGroup,
    };
  }

  if (String(code || "").startsWith("GIE_")) {
    return {
      description: "European gas storage indicator from GIE AGSI+.",
      sourceLabel: "GIE / AGSI+",
      snapshotGroup: inferredSnapshotGroup,
    };
  }

  if (String(code || "").startsWith("LME_")) {
    return {
      description: "Warehouse inventory indicator from the London Metal Exchange.",
      sourceLabel: "LME",
      snapshotGroup: inferredSnapshotGroup,
    };
  }

  if (String(code || "").startsWith("USDA_")) {
    return {
      description: "Inventory indicator sourced from USDA publications.",
      sourceLabel: "USDA",
      snapshotGroup: inferredSnapshotGroup,
    };
  }

  return {
    description: "Commodity inventory indicator.",
    sourceLabel: "CommodityWatch API",
    snapshotGroup: inferredSnapshotGroup,
  };
}

export function commodityGroupForCode(commodityCode) {
  if (!commodityCode) {
    return "all";
  }
  if (["crude_oil", "gasoline", "distillates", "propane", "jet_fuel"].includes(commodityCode)) {
    return "energy";
  }
  if (commodityCode === "natural_gas") {
    return "natural-gas";
  }
  if (["copper", "aluminum", "nickel", "zinc", "lead", "tin"].includes(commodityCode)) {
    return "base-metals";
  }
  if (["corn", "wheat", "soybeans", "oilseeds"].includes(commodityCode)) {
    return "grains";
  }
  if (["coffee", "cocoa", "sugar", "cotton"].includes(commodityCode)) {
    return "softs";
  }
  if (["gold", "silver", "platinum", "palladium"].includes(commodityCode)) {
    return "precious-metals";
  }
  return "all";
}

export function semanticModeForCommodity(commodityCode) {
  const groupSlug = commodityGroupForCode(commodityCode);
  return groupSlug === "energy" || groupSlug === "natural-gas" ? "inventory" : "generic";
}

export function ageInHours(timestamp) {
  if (!timestamp) {
    return Number.POSITIVE_INFINITY;
  }

  return (Date.now() - new Date(timestamp).getTime()) / (1000 * 60 * 60);
}

export function formatUtcTimestamp(timestamp) {
  if (!timestamp) {
    return "Awaiting update";
  }

  return `${absoluteFormatter.format(new Date(timestamp)).replace(",", "")} UTC`;
}

export function isoDate(timestamp) {
  if (!timestamp) {
    return "N/A";
  }

  return new Date(timestamp).toISOString().slice(0, 10);
}

export function freshnessFor(frequency, timestamp, stale = false) {
  if (!timestamp || stale) {
    return "aged";
  }

  const hours = ageInHours(timestamp);
  if (hours <= 24) {
    return "live";
  }
  if (frequency === "daily" && hours <= 72) {
    return "current";
  }
  if (frequency === "weekly" && hours <= 24 * 10) {
    return "current";
  }
  if (frequency === "monthly" && hours <= 24 * 45) {
    return "current";
  }
  if (frequency === "quarterly" && hours <= 24 * 120) {
    return "structural";
  }
  if (frequency === "annual" && hours <= 24 * 400) {
    return "structural";
  }
  if (hours <= 24 * 21) {
    return "lagged";
  }
  return "aged";
}

export function displayUnitFor(unit) {
  if (unit === "kb") {
    return { unit: "mb", factor: 0.001 };
  }

  return { unit: unit || "", factor: 1 };
}

export function convertUnitValue(value, unit) {
  if (value == null) {
    return null;
  }

  return value * displayUnitFor(unit).factor;
}

export function formatValue(value, unit) {
  if (value == null) {
    return "No data";
  }

  const { unit: displayUnit } = displayUnitFor(unit);
  const precision = precisionForUnit(displayUnit);
  return `${Number(value).toLocaleString("en-US", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  })}${displayUnit ? ` ${displayUnit}` : ""}`;
}

export function formatSignedValue(value, unit) {
  if (value == null) {
    return "No change";
  }

  const { unit: displayUnit } = displayUnitFor(unit);
  const precision = precisionForUnit(displayUnit);
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${Math.abs(value).toLocaleString("en-US", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  })}${displayUnit ? ` ${displayUnit}` : ""}`;
}

export function formatPercent(value) {
  if (value == null) {
    return "N/A";
  }

  return `${Number(value).toLocaleString("en-US", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })}%`;
}

export function groupDescriptionFor(slug) {
  return getCommodityGroup(slug).shortDescription;
}
