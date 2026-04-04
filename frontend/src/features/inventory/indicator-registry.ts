import { getCommodityGroup, type CommodityGroupSlug } from "@/config/commodities";
import { ageInHours } from "@/lib/format/dates";
import type { FreshnessState } from "@/types/api";

interface IndicatorRegistryEntry {
  description: string;
  sourceLabel: string;
  sourceHref?: string;
  snapshotGroup: string;
}

const REGISTRY: Record<string, IndicatorRegistryEntry> = {
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
  LME_COPPER_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  LME_ALUMINIUM_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  LME_ZINC_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  LME_NICKEL_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  LME_TIN_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  LME_LEAD_WAREHOUSE_STOCKS: {
    description: "Warehouse inventory indicator from the London Metal Exchange.",
    sourceLabel: "LME",
    snapshotGroup: "Base Metals",
  },
  USDA_US_CORN_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_US_SOYBEAN_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_US_WHEAT_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_US_RICE_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_WORLD_CORN_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_WORLD_SOYBEAN_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  USDA_WORLD_WHEAT_ENDING_STOCKS: {
    description: "Inventory indicator sourced from USDA publications.",
    sourceLabel: "USDA",
    snapshotGroup: "Grains",
  },
  COMEX_GOLD_WAREHOUSE_STOCKS: {
    description: "Warehouse stock indicator sourced from CME delivery reports.",
    sourceLabel: "COMEX / CME",
    snapshotGroup: "Precious Metals",
  },
  COMEX_SILVER_WAREHOUSE_STOCKS: {
    description: "Warehouse stock indicator sourced from CME delivery reports.",
    sourceLabel: "COMEX / CME",
    snapshotGroup: "Precious Metals",
  },
  ETF_GLD_HOLDINGS: {
    description: "Daily ETF holdings indicator.",
    sourceLabel: "ETF Holdings",
    snapshotGroup: "Precious Metals",
  },
  ETF_IAU_HOLDINGS: {
    description: "Daily ETF holdings indicator.",
    sourceLabel: "ETF Holdings",
    snapshotGroup: "Precious Metals",
  },
  ETF_SLV_HOLDINGS: {
    description: "Daily ETF holdings indicator.",
    sourceLabel: "ETF Holdings",
    snapshotGroup: "Precious Metals",
  },
  ICE_ARABICA_COFFEE_CERTIFIED_STOCKS: {
    description: "Certified stock indicator sourced from ICE.",
    sourceLabel: "ICE",
    snapshotGroup: "Softs",
  },
  ICE_ROBUSTA_COFFEE_CERTIFIED_STOCKS: {
    description: "Certified stock indicator sourced from ICE.",
    sourceLabel: "ICE",
    snapshotGroup: "Softs",
  },
  ICE_RAW_SUGAR_CERTIFIED_STOCKS: {
    description: "Certified stock indicator sourced from ICE.",
    sourceLabel: "ICE",
    snapshotGroup: "Softs",
  },
  ICE_COTTON_CERTIFIED_STOCKS: {
    description: "Certified stock indicator sourced from ICE.",
    sourceLabel: "ICE",
    snapshotGroup: "Softs",
  },
  ICE_COCOA_CERTIFIED_STOCKS: {
    description: "Certified stock indicator sourced from ICE.",
    sourceLabel: "ICE",
    snapshotGroup: "Softs",
  },
};

export function getIndicatorRegistryEntry(code: string) {
  if (REGISTRY[code]) {
    return REGISTRY[code];
  }

  if (code.startsWith("EIA_")) {
    return { description: "Public-domain inventory indicator from EIA.", sourceLabel: "EIA", snapshotGroup: "Energy" };
  }

  if (code.startsWith("GIE_")) {
    return { description: "European gas storage indicator from GIE AGSI+.", sourceLabel: "GIE / AGSI+", snapshotGroup: "Natural Gas" };
  }

  if (code.startsWith("LME_")) {
    return { description: "Warehouse inventory indicator from the London Metal Exchange.", sourceLabel: "LME", snapshotGroup: "Base Metals" };
  }

  if (code.startsWith("USDA_")) {
    return { description: "Inventory indicator sourced from USDA publications.", sourceLabel: "USDA", snapshotGroup: "Grains" };
  }

  if (code.startsWith("COMEX_")) {
    return { description: "Warehouse stock indicator sourced from CME delivery reports.", sourceLabel: "COMEX / CME", snapshotGroup: "Precious Metals" };
  }

  if (code.startsWith("ETF_")) {
    return { description: "Daily ETF holdings indicator.", sourceLabel: "ETF Holdings", snapshotGroup: "Precious Metals" };
  }

  if (code.startsWith("ICE_")) {
    return { description: "Certified stock indicator sourced from ICE.", sourceLabel: "ICE", snapshotGroup: "Softs" };
  }

  return {
    description: "Commodity inventory indicator.",
    sourceLabel: "CommodityWatch API",
    snapshotGroup: "Inventory",
  };
}

export function commodityGroupForCode(commodityCode: string | null | undefined): CommodityGroupSlug {
  if (!commodityCode) return "all";
  if (["crude_oil", "gasoline", "distillates", "propane", "jet_fuel"].includes(commodityCode)) return "energy";
  if (commodityCode === "natural_gas") return "natural-gas";
  if (["copper", "aluminum", "nickel", "zinc", "lead", "tin"].includes(commodityCode)) return "base-metals";
  if (["corn", "wheat", "soybeans", "oilseeds", "rice"].includes(commodityCode)) return "grains";
  if (["coffee", "cocoa", "sugar", "cotton"].includes(commodityCode)) return "softs";
  if (["gold", "silver", "platinum", "palladium"].includes(commodityCode)) return "precious-metals";
  return "all";
}

export function semanticModeForCommodity(commodityCode: string | null | undefined) {
  return commodityGroupForCode(commodityCode) === "energy" || commodityGroupForCode(commodityCode) === "natural-gas"
    ? "inventory"
    : "generic";
}

function freshnessThresholds(frequency: string | undefined) {
  switch (frequency) {
    case "daily":
      return { currentHours: 72, agedHours: 24 * 7, structural: false };
    case "monthly":
      return { currentHours: 24 * 45, agedHours: 24 * 75, structural: false };
    case "quarterly":
      return { currentHours: 24 * 120, agedHours: 24 * 200, structural: true };
    case "annual":
      return { currentHours: 24 * 400, agedHours: 24 * 550, structural: true };
    default:
      return { currentHours: 24 * 10, agedHours: 24 * 21, structural: false };
  }
}

export function freshnessFor(
  frequency: string | undefined,
  releaseTimestamp: string | null | undefined,
  observedTimestamp: string | null | undefined,
): FreshnessState {
  const timestamp = releaseTimestamp ?? observedTimestamp;
  if (!timestamp) {
    return "aged";
  }

  const hours = ageInHours(timestamp);
  if (hours <= 24) {
    return "live";
  }

  const thresholds = freshnessThresholds(frequency);
  if (hours <= thresholds.currentHours) {
    return thresholds.structural ? "structural" : "current";
  }

  if (hours <= thresholds.agedHours) {
    return "lagged";
  }

  return "aged";
}

export function groupDescriptionFor(slug: CommodityGroupSlug) {
  return getCommodityGroup(slug).shortDescription;
}
