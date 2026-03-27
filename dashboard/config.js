import {
  ALWAYS_RELEVANT_CATEGORIES,
  DASHBOARD_CATEGORY_TAGS,
  SECTOR_MAP as HEADLINE_SECTOR_MAP,
} from "../shared/headline-taxonomy.js";
import { DEFAULT_HOME_SERIES_KEYS as SHARED_DEFAULT_HOME_SERIES_KEYS } from "../shared/commodity-series-contract.js";

export const SECTOR_META = {
  energy: { label: "Energy", accent: "var(--color-energy)" },
  metals: { label: "Metals", accent: "var(--color-metals)" },
  agriculture: { label: "Agriculture", accent: "var(--color-agri)" },
  macro: { label: "Macro", accent: "var(--color-macro)" },
  "cross-commodity": { label: "Cross-Commodity", accent: "var(--color-cross)" },
};

export const ALWAYS_HEADLINE_CATEGORIES = [...ALWAYS_RELEVANT_CATEGORIES];
export const ALWAYS_CALENDAR_SECTORS = ["macro", "cross-commodity"];
export const HOME_FILTER_STORAGE_KEY_SUFFIX = ".home.filter";
export const HOME_FILTER_STORAGE_KEY = `commoditywatch${HOME_FILTER_STORAGE_KEY_SUFFIX}`;
export const HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX = ".home.filter.collapsed";
export const HOME_FILTER_COLLAPSE_STORAGE_KEY = `commoditywatch${HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX}`;

export const DEFAULT_HOME_SERIES_KEYS = [...SHARED_DEFAULT_HOME_SERIES_KEYS];

export const HOME_FILTER_TAXONOMY = [
  {
    id: "energy",
    label: "Energy",
    accent: "var(--color-energy)",
    feedCategories: [...HEADLINE_SECTOR_MAP.energy],
    groups: [
      {
        id: "crude-oil",
        label: "Crude Oil",
        feedCategories: ["Oil - Crude", "Shipping"],
        feedKeywords: ["brent", "wti", "west texas intermediate", "dubai", "oman", "crude"],
        calendarKeywords: [
          "crude",
          "petroleum",
          "oil market",
          "rig count",
          "baker hughes",
          "weekly petroleum",
          "steo",
          "opec",
          "iea",
        ],
        commodities: [
          { id: "brent", label: "Brent", seriesKey: "crude_oil_brent" },
          { id: "wti", label: "WTI", seriesKey: "crude_oil_wti" },
          { id: "dubai-oman", label: "Dubai/Oman", seriesKey: "crude_oil_dubai" },
        ],
      },
      {
        id: "natural-gas",
        label: "Natural Gas",
        feedCategories: ["Natural Gas"],
        feedKeywords: ["natural gas", "henry hub", "ttf", "storage"],
        calendarKeywords: ["natural gas", "gas storage", "henry hub", "ttf", "eia weekly natural gas"],
        commodities: [
          { id: "henry-hub", label: "Henry Hub", seriesKey: "natural_gas_henry_hub" },
          { id: "ttf", label: "TTF", seriesKey: "natural_gas_ttf" },
        ],
      },
      {
        id: "lng",
        label: "LNG",
        feedCategories: ["LNG", "Natural Gas"],
        feedKeywords: ["lng", "jkm", "japan korea marker", "liquefied natural gas"],
        calendarKeywords: ["lng", "liquefied natural gas"],
        commodities: [{ id: "jkm", label: "JKM LNG", seriesKey: "lng_asia_japan_import_proxy" }],
      },
      {
        id: "coal",
        label: "Coal",
        feedCategories: ["Coal"],
        feedKeywords: ["coal", "newcastle", "richards bay", "south african coal"],
        calendarKeywords: ["coal"],
        commodities: [
          { id: "newcastle", label: "Newcastle", seriesKey: "thermal_coal_newcastle" },
          { id: "south-africa", label: "South Africa", seriesKey: "coal_south_africa_monthly" },
        ],
      },
      {
        id: "power",
        label: "Power",
        feedCategories: ["Electric Power", "Energy Transition", "Natural Gas"],
        feedKeywords: ["power", "electricity", "baseload", "grid"],
        calendarKeywords: ["electric power", "electricity", "power"],
        commodities: [],
      },
    ],
  },
  {
    id: "metals",
    label: "Metals",
    accent: "var(--color-metals)",
    feedCategories: [...HEADLINE_SECTOR_MAP.metals_and_mining],
    groups: [
      {
        id: "precious",
        label: "Precious",
        feedCategories: ["Metals"],
        feedKeywords: ["gold", "silver", "platinum", "palladium", "bullion"],
        calendarKeywords: ["gold", "silver", "platinum", "palladium", "bullion"],
        commodities: [
          { id: "gold", label: "Gold", seriesKey: "gold_worldbank_monthly" },
          { id: "silver", label: "Silver", seriesKey: "silver_worldbank_monthly" },
          { id: "platinum", label: "Platinum", seriesKey: "platinum_worldbank_monthly" },
        ],
      },
      {
        id: "base-metals",
        label: "Base Metals",
        feedCategories: ["Metals"],
        feedKeywords: ["copper", "aluminium", "aluminum", "nickel", "zinc", "iron ore"],
        calendarKeywords: [
          "copper",
          "aluminium",
          "aluminum",
          "nickel",
          "zinc",
          "iron ore",
          "icsg",
          "insg",
          "alcoa",
          "bhp",
          "rio tinto",
        ],
        commodities: [
          { id: "copper", label: "Copper", seriesKey: "copper_worldbank_monthly" },
          { id: "aluminium", label: "Aluminium", seriesKey: "aluminium_worldbank_monthly" },
          { id: "nickel", label: "Nickel", seriesKey: "nickel_worldbank_monthly" },
          { id: "zinc", label: "Zinc", seriesKey: "zinc_worldbank_monthly" },
          { id: "iron-ore", label: "Iron Ore", seriesKey: "iron_ore_62pct_china_monthly" },
        ],
      },
      {
        id: "battery-metals",
        label: "Battery Metals",
        feedCategories: ["Metals"],
        feedKeywords: ["lithium", "cobalt", "battery"],
        calendarKeywords: ["lithium", "cobalt", "battery"],
        commodities: [
          { id: "lithium", label: "Lithium", seriesKey: "lithium_metal_imf_monthly" },
          { id: "cobalt", label: "Cobalt", seriesKey: "cobalt_imf_monthly" },
        ],
      },
    ],
  },
  {
    id: "agriculture",
    label: "Agriculture",
    accent: "var(--color-agri)",
    feedCategories: [...HEADLINE_SECTOR_MAP.agriculture, ...HEADLINE_SECTOR_MAP.fertilizers],
    groups: [
      {
        id: "grains-oilseeds",
        label: "Grains & Oilseeds",
        feedCategories: ["Agriculture"],
        feedKeywords: ["wheat", "corn", "maize", "soybean", "soybeans", "soy", "grain", "oilseed"],
        calendarKeywords: ["wheat", "corn", "maize", "soy", "grain", "oilseed", "wasde", "crop", "export sales"],
        commodities: [
          { id: "wheat", label: "Wheat", seriesKey: "wheat_global_monthly_proxy" },
          { id: "corn", label: "Corn", seriesKey: "corn_global_monthly_proxy" },
          { id: "soybeans", label: "Soybeans", seriesKey: "soybeans_global_monthly_proxy" },
        ],
      },
      {
        id: "softs",
        label: "Softs",
        feedCategories: ["Agriculture"],
        feedKeywords: ["coffee", "sugar", "cocoa", "cotton", "rubber"],
        calendarKeywords: ["coffee", "sugar", "cocoa", "cotton", "rubber"],
        commodities: [
          { id: "coffee", label: "Coffee", seriesKey: "coffee_arabica_monthly_proxy" },
          { id: "sugar", label: "Sugar", seriesKey: "sugar_no11_world_monthly_proxy" },
          { id: "cocoa", label: "Cocoa", seriesKey: "cocoa_monthly_proxy" },
        ],
      },
      {
        id: "livestock",
        label: "Livestock",
        feedCategories: ["Agriculture"],
        feedKeywords: ["beef", "cattle", "swine", "pork", "hog", "livestock", "poultry", "egg", "milk", "dairy"],
        calendarKeywords: ["beef", "cattle", "swine", "pork", "hog", "livestock", "poultry", "egg", "milk", "dairy", "slaughter"],
        commodities: [
          { id: "beef", label: "Beef", seriesKey: "beef_monthly" },
          { id: "swine", label: "Swine", seriesKey: "swine_monthly" },
        ],
      },
    ],
  },
];

const sectorById = new Map(HOME_FILTER_TAXONOMY.map((sector) => [sector.id, sector]));
const groupById = new Map(
  HOME_FILTER_TAXONOMY.flatMap((sector) => sector.groups.map((group) => [group.id, { ...group, sectorId: sector.id }]))
);
const commodityById = new Map(
  HOME_FILTER_TAXONOMY.flatMap((sector) =>
    sector.groups.flatMap((group) =>
      group.commodities.map((commodity) => [commodity.id, { ...commodity, sectorId: sector.id, groupId: group.id }])
    )
  )
);
const commodityBySeriesKey = new Map(
  HOME_FILTER_TAXONOMY.flatMap((sector) =>
    sector.groups.flatMap((group) =>
      group.commodities
        .filter((commodity) => commodity.seriesKey)
        .map((commodity) => [commodity.seriesKey, { ...commodity, sectorId: sector.id, groupId: group.id }])
    )
  )
);

const categoryTagMap = Object.fromEntries(
  Object.entries(DASHBOARD_CATEGORY_TAGS).map(([category, metadata]) => [
    category,
    {
      label: metadata.label,
      sectorId: metadata.sectorId,
    },
  ])
);

function uniqueStrings(values) {
  return [...new Set(values.filter(Boolean))];
}

function buildPattern(keywords) {
  if (!keywords || !keywords.length) {
    return null;
  }

  const escaped = keywords.map((keyword) => keyword.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return new RegExp(`\\b(?:${escaped.join("|")})\\b`, "i");
}

export function getSectorById(sectorId) {
  return sectorById.get(sectorId) || null;
}

export function getGroupById(groupId) {
  return groupById.get(groupId) || null;
}

export function getCommodityById(commodityId) {
  return commodityById.get(commodityId) || null;
}

export function getCommodityBySeriesKey(seriesKey) {
  return commodityBySeriesKey.get(seriesKey) || null;
}

export function getTagForFeedCategory(category) {
  return categoryTagMap[category] || null;
}

export function getAllSectorIds() {
  return HOME_FILTER_TAXONOMY.map((sector) => sector.id);
}

export function getAllGroupIdsForSector(sectorId) {
  return getSectorById(sectorId)?.groups.map((group) => group.id) || [];
}

export function getAllCommodityIdsForGroup(groupId) {
  return getGroupById(groupId)?.commodities.map((commodity) => commodity.id) || [];
}

export function getSeriesKeysForCommodityIds(commodityIds) {
  return uniqueStrings(
    commodityIds.map((commodityId) => getCommodityById(commodityId)?.seriesKey).filter(Boolean)
  );
}

export function getSeriesKeysForGroup(groupId) {
  return getSeriesKeysForCommodityIds(getAllCommodityIdsForGroup(groupId));
}

export function getSectorPattern(sectorId) {
  return buildPattern(getSectorById(sectorId)?.feedCategories || []);
}

export function getGroupFeedPattern(groupId) {
  const group = getGroupById(groupId);
  return buildPattern([...(group?.feedKeywords || []), ...(group?.feedCategories || [])]);
}

export function getGroupCalendarPattern(groupId) {
  return buildPattern(getGroupById(groupId)?.calendarKeywords || []);
}

export function getCommodityCalendarPattern(commodityId) {
  const commodity = getCommodityById(commodityId);
  if (!commodity) {
    return null;
  }

  const group = getGroupById(commodity.groupId);
  return buildPattern([commodity.label, ...(group?.calendarKeywords || [])]);
}
