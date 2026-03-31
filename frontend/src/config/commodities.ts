export type CommodityGroupSlug =
  | "all"
  | "energy"
  | "natural-gas"
  | "base-metals"
  | "grains"
  | "softs"
  | "precious-metals";

export interface CommodityGroup {
  slug: CommodityGroupSlug;
  label: string;
  commodityCodes: string[];
  shortDescription: string;
}

export const COMMODITY_GROUPS: CommodityGroup[] = [
  {
    slug: "all",
    label: "All",
    commodityCodes: [],
    shortDescription: "Full InventoryWatch market snapshot.",
  },
  {
    slug: "energy",
    label: "Energy",
    commodityCodes: ["crude_oil", "gasoline", "distillates", "propane", "jet_fuel"],
    shortDescription: "Crude and refined product inventories.",
  },
  {
    slug: "natural-gas",
    label: "Natural Gas",
    commodityCodes: ["natural_gas"],
    shortDescription: "US and European gas storage.",
  },
  {
    slug: "base-metals",
    label: "Base Metals",
    commodityCodes: ["copper", "aluminum", "nickel", "zinc", "lead", "tin"],
    shortDescription: "LME warehouse stocks and exchange inventory context.",
  },
  {
    slug: "grains",
    label: "Grains",
    commodityCodes: ["corn", "wheat", "soybeans", "oilseeds"],
    shortDescription: "USDA grain and oilseed stocks.",
  },
  {
    slug: "softs",
    label: "Softs",
    commodityCodes: ["coffee", "cocoa", "sugar", "cotton"],
    shortDescription: "Exchange-certified stocks across soft commodities.",
  },
  {
    slug: "precious-metals",
    label: "Precious Metals",
    commodityCodes: ["gold", "silver", "platinum", "palladium"],
    shortDescription: "Warehouse stocks and ETF holdings.",
  },
];

export function isCommodityGroupSlug(value: string): value is CommodityGroupSlug {
  return COMMODITY_GROUPS.some((group) => group.slug === value);
}

export function getCommodityGroup(slug: CommodityGroupSlug) {
  return COMMODITY_GROUPS.find((group) => group.slug === slug) ?? COMMODITY_GROUPS[0];
}
