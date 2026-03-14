import {
  HOME_FILTER_COLLAPSE_STORAGE_KEY,
  HOME_FILTER_STORAGE_KEY,
  HOME_FILTER_TAXONOMY,
  getAllCommodityIdsForGroup,
  getCommodityById,
  getGroupById,
  getSectorById,
} from "./config.js";

export function createDefaultFilter() {
  return {
    sectorId: null,
    groupId: null,
    commodityIds: [],
  };
}

export function normalizeFilter(candidate) {
  if (!candidate || !candidate.sectorId) {
    return createDefaultFilter();
  }

  const sector = getSectorById(candidate.sectorId);
  if (!sector) {
    return createDefaultFilter();
  }

  if (!candidate.groupId) {
    return {
      sectorId: sector.id,
      groupId: null,
      commodityIds: [],
    };
  }

  const group = getGroupById(candidate.groupId);
  if (!group || group.sectorId !== sector.id) {
    return {
      sectorId: sector.id,
      groupId: null,
      commodityIds: [],
    };
  }

  const allCommodityIds = getAllCommodityIdsForGroup(group.id);
  const validCommodityIds = Array.isArray(candidate.commodityIds)
    ? allCommodityIds.filter((commodityId) => candidate.commodityIds.includes(commodityId))
    : [];

  return {
    sectorId: sector.id,
    groupId: group.id,
    commodityIds: validCommodityIds.length ? validCommodityIds : allCommodityIds,
  };
}

export function isAllFilter(filter) {
  return normalizeFilter(filter).sectorId === null;
}

export function hasGroupSelection(filter) {
  return Boolean(normalizeFilter(filter).groupId);
}

export function hasCommoditySelection(filter) {
  const normalized = normalizeFilter(filter);
  return Boolean(normalized.groupId && normalized.commodityIds.length);
}

export function isAllCommoditySelection(filter) {
  const normalized = normalizeFilter(filter);
  if (!normalized.groupId) {
    return true;
  }

  return normalized.commodityIds.length === getAllCommodityIdsForGroup(normalized.groupId).length;
}

export function readStoredFilter(storage = globalThis.localStorage) {
  if (!storage) {
    return createDefaultFilter();
  }

  try {
    const rawValue = storage.getItem(HOME_FILTER_STORAGE_KEY);
    return rawValue ? normalizeFilter(JSON.parse(rawValue)) : createDefaultFilter();
  } catch {
    return createDefaultFilter();
  }
}

export function writeStoredFilter(filter, storage = globalThis.localStorage) {
  if (!storage) {
    return;
  }

  try {
    storage.setItem(HOME_FILTER_STORAGE_KEY, JSON.stringify(normalizeFilter(filter)));
  } catch {
    // Ignore storage failures.
  }
}

export function readStoredCollapseState(storage = globalThis.localStorage) {
  if (!storage) {
    return false;
  }

  try {
    return storage.getItem(HOME_FILTER_COLLAPSE_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

export function writeStoredCollapseState(collapsed, storage = globalThis.localStorage) {
  if (!storage) {
    return;
  }

  try {
    storage.setItem(HOME_FILTER_COLLAPSE_STORAGE_KEY, collapsed ? "true" : "false");
  } catch {
    // Ignore storage failures.
  }
}

export function toggleSector(filter, sectorId) {
  const normalized = normalizeFilter(filter);
  if (!sectorId || !getSectorById(sectorId)) {
    return normalized;
  }

  if (normalized.sectorId !== sectorId) {
    return {
      sectorId,
      groupId: null,
      commodityIds: [],
    };
  }

  if (normalized.groupId) {
    return {
      sectorId,
      groupId: null,
      commodityIds: [],
    };
  }

  return createDefaultFilter();
}

export function toggleGroup(filter, groupId) {
  const normalized = normalizeFilter(filter);
  const group = getGroupById(groupId);
  if (!group) {
    return normalized;
  }

  if (normalized.sectorId !== group.sectorId) {
    return {
      sectorId: group.sectorId,
      groupId: group.id,
      commodityIds: getAllCommodityIdsForGroup(group.id),
    };
  }

  if (normalized.groupId === group.id) {
    return {
      sectorId: group.sectorId,
      groupId: null,
      commodityIds: [],
    };
  }

  return {
    sectorId: group.sectorId,
    groupId: group.id,
    commodityIds: getAllCommodityIdsForGroup(group.id),
  };
}

export function toggleCommodity(filter, commodityId) {
  const normalized = normalizeFilter(filter);
  const commodity = getCommodityById(commodityId);
  if (!commodity || normalized.groupId !== commodity.groupId) {
    return normalized;
  }

  const allCommodityIds = getAllCommodityIdsForGroup(commodity.groupId);
  const selectedIds = normalized.commodityIds.length ? [...normalized.commodityIds] : [...allCommodityIds];

  if (selectedIds.length === allCommodityIds.length) {
    return {
      sectorId: normalized.sectorId,
      groupId: normalized.groupId,
      commodityIds: [commodityId],
    };
  }

  if (selectedIds.includes(commodityId)) {
    const nextCommodityIds = selectedIds.filter((selectedCommodityId) => selectedCommodityId !== commodityId);

    return {
      sectorId: normalized.sectorId,
      groupId: normalized.groupId,
      commodityIds: nextCommodityIds.length ? nextCommodityIds : allCommodityIds,
    };
  }

  return {
    sectorId: normalized.sectorId,
    groupId: normalized.groupId,
    commodityIds: allCommodityIds.filter(
      (candidateCommodityId) =>
        selectedIds.includes(candidateCommodityId) || candidateCommodityId === commodityId
    ),
  };
}

export function clearCommodities(filter) {
  const normalized = normalizeFilter(filter);
  if (!normalized.groupId) {
    return normalized;
  }

  return {
    sectorId: normalized.sectorId,
    groupId: normalized.groupId,
    commodityIds: getAllCommodityIdsForGroup(normalized.groupId),
  };
}

export function getFilterLabel(filter) {
  const normalized = normalizeFilter(filter);
  if (!normalized.sectorId) {
    return "All Commodities";
  }

  const sector = getSectorById(normalized.sectorId);
  if (!normalized.groupId) {
    return sector?.label || "All Commodities";
  }

  const group = getGroupById(normalized.groupId);
  if (!group) {
    return sector?.label || "All Commodities";
  }

  if (isAllCommoditySelection(normalized) || !group.commodities.length) {
    return `${sector?.label || ""} / ${group.label}`;
  }

  const commodityLabels = normalized.commodityIds
    .map((commodityId) => getCommodityById(commodityId)?.label)
    .filter(Boolean);

  if (commodityLabels.length === 1) {
    return `${sector?.label || ""} / ${group.label} / ${commodityLabels[0]}`;
  }

  if (commodityLabels.length <= 3) {
    return `${sector?.label || ""} / ${group.label} / ${commodityLabels.join(" + ")}`;
  }

  return `${sector?.label || ""} / ${group.label} / ${commodityLabels.slice(0, 2).join(" + ")} +${
    commodityLabels.length - 2
  }`;
}

export function getSummaryPills(filter) {
  const normalized = normalizeFilter(filter);
  if (!normalized.sectorId) {
    return [{ id: "all", label: "All Commodities", tone: "neutral" }];
  }

  const sector = getSectorById(normalized.sectorId);
  const pills = [{ id: sector?.id || "sector", label: sector?.label || "Selected", tone: normalized.sectorId }];

  if (!normalized.groupId) {
    return pills;
  }

  const group = getGroupById(normalized.groupId);
  pills.push({ id: group?.id || "group", label: group?.label || "Group", tone: normalized.sectorId });

  if (!isAllCommoditySelection(normalized)) {
    normalized.commodityIds.forEach((commodityId) => {
      const commodity = getCommodityById(commodityId);
      if (commodity) {
        pills.push({ id: commodity.id, label: commodity.label, tone: normalized.sectorId });
      }
    });
  }

  return pills;
}

export function getSelectedSector(filter) {
  return getSectorById(normalizeFilter(filter).sectorId);
}

export function getSelectedGroup(filter) {
  return getGroupById(normalizeFilter(filter).groupId);
}

export function getSelectedCommodities(filter) {
  return normalizeFilter(filter).commodityIds.map((commodityId) => getCommodityById(commodityId)).filter(Boolean);
}

export function getExpandedGroupsForSector(sectorId) {
  return getSectorById(sectorId)?.groups || [];
}

export function getAllSectors() {
  return HOME_FILTER_TAXONOMY;
}
