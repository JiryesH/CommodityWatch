import {
  HOME_FILTER_STORAGE_KEY,
  HOME_FILTER_COLLAPSE_STORAGE_KEY,
  HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX,
  HOME_FILTER_STORAGE_KEY_SUFFIX,
  HOME_FILTER_TAXONOMY,
  getAllCommodityIdsForGroup,
  getAllGroupIdsForSector,
  getAllSectorIds,
  getCommodityById,
  getGroupById,
  getSectorById,
} from "./config.js";

function getExplicitSectorIds(candidate) {
  if (Array.isArray(candidate?.sectorIds)) {
    return candidate.sectorIds;
  }

  if (candidate?.sectorId) {
    return [candidate.sectorId];
  }

  return [];
}

function getRawGroupIdsBySector(candidate, sectorId) {
  if (candidate?.groupIdsBySector && Array.isArray(candidate.groupIdsBySector[sectorId])) {
    return candidate.groupIdsBySector[sectorId];
  }

  if (Array.isArray(candidate?.groupIds) && getExplicitSectorIds(candidate).length === 1) {
    return candidate.groupIds;
  }

  if (candidate?.groupId && getExplicitSectorIds(candidate).length <= 1) {
    return [candidate.groupId];
  }

  return [];
}

function getEffectiveSectorIds(filter) {
  const allSectorIds = getAllSectorIds();
  if (!Array.isArray(filter?.sectorIds) || !filter.sectorIds.length) {
    return allSectorIds;
  }

  return allSectorIds.filter((sectorId) => filter.sectorIds.includes(sectorId));
}

function getEffectiveGroupIdsForSector(filter, sectorId) {
  if (!getEffectiveSectorIds(filter).includes(sectorId)) {
    return [];
  }

  const allGroupIds = getAllGroupIdsForSector(sectorId);
  if (!allGroupIds.length) {
    return [];
  }

  const explicitGroupIds = Array.isArray(filter?.groupIdsBySector?.[sectorId])
    ? allGroupIds.filter((groupId) => filter.groupIdsBySector[sectorId].includes(groupId))
    : [];

  return explicitGroupIds.length ? explicitGroupIds : allGroupIds;
}

function getSingleSelectedGroupId(filter) {
  const selectedGroupIds = getEffectiveSectorIds(filter).flatMap((sectorId) => getEffectiveGroupIdsForSector(filter, sectorId));
  return selectedGroupIds.length === 1 ? selectedGroupIds[0] : null;
}

function getEffectiveCommodityIds(filter) {
  const selectedGroupId = getSingleSelectedGroupId(filter);
  if (!selectedGroupId) {
    return [];
  }

  const allCommodityIds = getAllCommodityIdsForGroup(selectedGroupId);
  if (!allCommodityIds.length) {
    return [];
  }

  const explicitCommodityIds = Array.isArray(filter?.commodityIds)
    ? allCommodityIds.filter((commodityId) => filter.commodityIds.includes(commodityId))
    : [];

  return explicitCommodityIds.length ? explicitCommodityIds : allCommodityIds;
}

function formatSelectionTrail(labels) {
  if (!labels.length) {
    return "";
  }

  if (labels.length <= 3) {
    return labels.join(" + ");
  }

  return `${labels.slice(0, 2).join(" + ")} +${labels.length - 2}`;
}

export function createDefaultFilter() {
  return {
    sectorIds: [],
    groupIdsBySector: {},
    commodityIds: [],
  };
}

export function normalizeFilter(candidate) {
  const allSectorIds = getAllSectorIds();
  const validSectorIds = allSectorIds.filter((sectorId) => getExplicitSectorIds(candidate).includes(sectorId));
  const sectorIds = validSectorIds.length && validSectorIds.length < allSectorIds.length ? validSectorIds : [];
  const effectiveSectorIds = sectorIds.length ? sectorIds : allSectorIds;

  const groupIdsBySector = {};
  effectiveSectorIds.forEach((sectorId) => {
    const allGroupIds = getAllGroupIdsForSector(sectorId);
    const validGroupIds = allGroupIds.filter((groupId) => getRawGroupIdsBySector(candidate, sectorId).includes(groupId));
    if (validGroupIds.length && validGroupIds.length < allGroupIds.length) {
      groupIdsBySector[sectorId] = validGroupIds;
    }
  });

  const selectedGroupId = getSingleSelectedGroupId({ sectorIds, groupIdsBySector });
  if (!selectedGroupId) {
    return {
      sectorIds,
      groupIdsBySector,
      commodityIds: [],
    };
  }

  const allCommodityIds = getAllCommodityIdsForGroup(selectedGroupId);
  const validCommodityIds = allCommodityIds.filter((commodityId) =>
    Array.isArray(candidate?.commodityIds) ? candidate.commodityIds.includes(commodityId) : false
  );

  return {
    sectorIds,
    groupIdsBySector,
    commodityIds:
      validCommodityIds.length && validCommodityIds.length < allCommodityIds.length ? validCommodityIds : [],
  };
}

export function isAllFilter(filter) {
  const normalized = normalizeFilter(filter);
  return normalized.sectorIds.length === 0 && !Object.keys(normalized.groupIdsBySector).length && !normalized.commodityIds.length;
}

export function hasGroupSelection(filter) {
  return getSelectedGroups(filter).length > 0;
}

export function hasPartialGroupSelection(filter) {
  return Object.keys(normalizeFilter(filter).groupIdsBySector).length > 0;
}

export function hasPartialGroupSelectionForSector(filter, sectorId) {
  return Array.isArray(normalizeFilter(filter).groupIdsBySector[sectorId]);
}

export function hasCommoditySelection(filter) {
  return Boolean(getSelectedGroup(filter) && normalizeFilter(filter).commodityIds.length);
}

export function isAllCommoditySelection(filter) {
  return normalizeFilter(filter).commodityIds.length === 0;
}

function readStoredValue(storage, keys) {
  const [primaryKey, fallbackSuffix] = keys;
  const primaryValue = storage.getItem(primaryKey);
  if (primaryValue !== null) {
    return primaryValue;
  }

  if (!fallbackSuffix || typeof storage.length !== "number" || typeof storage.key !== "function") {
    return null;
  }

  for (let index = 0; index < storage.length; index += 1) {
    const candidateKey = storage.key(index);
    if (!candidateKey || candidateKey === primaryKey || !candidateKey.endsWith(fallbackSuffix)) {
      continue;
    }

    const candidateValue = storage.getItem(candidateKey);
    if (candidateValue === null) {
      continue;
    }

    try {
      storage.setItem(primaryKey, candidateValue);
    } catch {
      // Ignore storage failures.
    }

    try {
      storage.removeItem?.(candidateKey);
    } catch {
      // Ignore storage failures.
    }

    return candidateValue;
  }

  return null;
}

export function readStoredFilter(storage = globalThis.localStorage) {
  if (!storage) {
    return createDefaultFilter();
  }

  try {
    const rawValue = readStoredValue(storage, [HOME_FILTER_STORAGE_KEY, HOME_FILTER_STORAGE_KEY_SUFFIX]);
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
    return readStoredValue(storage, [HOME_FILTER_COLLAPSE_STORAGE_KEY, HOME_FILTER_COLLAPSE_STORAGE_KEY_SUFFIX]) === "true";
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
  const allSectorIds = getAllSectorIds();
  if (!sectorId || !allSectorIds.includes(sectorId)) {
    return normalized;
  }

  if (isAllFilter(normalized)) {
    return {
      sectorIds: [sectorId],
      groupIdsBySector: {},
      commodityIds: [],
    };
  }

  const nextSectorIds = new Set(getEffectiveSectorIds(normalized));
  if (nextSectorIds.has(sectorId)) {
    nextSectorIds.delete(sectorId);
  } else {
    nextSectorIds.add(sectorId);
  }

  if (!nextSectorIds.size) {
    return createDefaultFilter();
  }

  const orderedSectorIds = allSectorIds.filter((candidateSectorId) => nextSectorIds.has(candidateSectorId));
  const nextGroupIdsBySector = { ...normalized.groupIdsBySector };
  Object.keys(nextGroupIdsBySector).forEach((candidateSectorId) => {
    if (!nextSectorIds.has(candidateSectorId)) {
      delete nextGroupIdsBySector[candidateSectorId];
    }
  });

  return normalizeFilter({
    sectorIds: orderedSectorIds.length === allSectorIds.length ? [] : orderedSectorIds,
    groupIdsBySector: nextGroupIdsBySector,
    commodityIds: [],
  });
}

export function toggleGroup(filter, groupId) {
  const normalized = normalizeFilter(filter);
  const group = getGroupById(groupId);
  if (!group) {
    return normalized;
  }

  const allSectorIds = getAllSectorIds();
  const nextSectorIds = new Set(getEffectiveSectorIds(normalized));
  nextSectorIds.add(group.sectorId);

  const allGroupIds = getAllGroupIdsForSector(group.sectorId);
  const nextGroupIds = new Set(getEffectiveGroupIdsForSector(normalized, group.sectorId));

  if (nextGroupIds.has(group.id)) {
    nextGroupIds.delete(group.id);
  } else {
    nextGroupIds.add(group.id);
  }

  const nextGroupIdsBySector = { ...normalized.groupIdsBySector };

  if (!nextGroupIds.size) {
    nextSectorIds.delete(group.sectorId);
    delete nextGroupIdsBySector[group.sectorId];
  } else if (nextGroupIds.size === allGroupIds.length) {
    delete nextGroupIdsBySector[group.sectorId];
  } else {
    nextGroupIdsBySector[group.sectorId] = allGroupIds.filter((candidateGroupId) => nextGroupIds.has(candidateGroupId));
  }

  if (!nextSectorIds.size) {
    return createDefaultFilter();
  }

  return normalizeFilter({
    sectorIds:
      nextSectorIds.size === allSectorIds.length
        ? []
        : allSectorIds.filter((candidateSectorId) => nextSectorIds.has(candidateSectorId)),
    groupIdsBySector: nextGroupIdsBySector,
    commodityIds: [],
  });
}

export function toggleCommodity(filter, commodityId) {
  const normalized = normalizeFilter(filter);
  const selectedGroup = getSelectedGroup(normalized);
  const commodity = getCommodityById(commodityId);

  if (!selectedGroup || !commodity || commodity.groupId !== selectedGroup.id) {
    return normalized;
  }

  const allCommodityIds = getAllCommodityIdsForGroup(selectedGroup.id);
  if (!allCommodityIds.length) {
    return normalized;
  }

  const nextCommodityIds = new Set(getEffectiveCommodityIds(normalized));

  if (nextCommodityIds.has(commodityId)) {
    nextCommodityIds.delete(commodityId);
  } else {
    nextCommodityIds.add(commodityId);
  }

  if (!nextCommodityIds.size || nextCommodityIds.size === allCommodityIds.length) {
    return normalizeFilter({
      ...normalized,
      commodityIds: [],
    });
  }

  return normalizeFilter({
    ...normalized,
    commodityIds: allCommodityIds.filter((candidateCommodityId) => nextCommodityIds.has(candidateCommodityId)),
  });
}

export function clearGroups(filter) {
  const normalized = normalizeFilter(filter);
  return normalizeFilter({
    sectorIds: normalized.sectorIds,
    groupIdsBySector: {},
    commodityIds: [],
  });
}

export function clearCommodities(filter) {
  const normalized = normalizeFilter(filter);
  return normalizeFilter({
    ...normalized,
    commodityIds: [],
  });
}

export function getFilterLabel(filter) {
  const normalized = normalizeFilter(filter);
  if (isAllFilter(normalized)) {
    return "All Commodities";
  }

  const selectedSectors = getSelectedSectors(normalized);
  const selectedGroups = getSelectedGroups(normalized);
  const selectedGroup = getSelectedGroup(normalized);

  if (!hasPartialGroupSelection(normalized) && !hasCommoditySelection(normalized)) {
    return formatSelectionTrail(selectedSectors.map((sector) => sector.label));
  }

  if (selectedGroup && !hasCommoditySelection(normalized)) {
    return `${formatSelectionTrail(selectedSectors.map((sector) => sector.label))} / ${selectedGroup.label}`;
  }

  if (selectedGroup && hasCommoditySelection(normalized)) {
    return `${formatSelectionTrail(selectedSectors.map((sector) => sector.label))} / ${selectedGroup.label} / ${formatSelectionTrail(
      getSelectedCommodities(normalized).map((commodity) => commodity.label)
    )}`;
  }

  return `${formatSelectionTrail(selectedSectors.map((sector) => sector.label))} / ${formatSelectionTrail(
    selectedGroups.map((group) => group.label)
  )}`;
}

export function getSummaryPills(filter) {
  const normalized = normalizeFilter(filter);
  if (isAllFilter(normalized)) {
    return [{ id: "all", label: "All Commodities", tone: "neutral" }];
  }

  const pills = getSelectedSectors(normalized).map((sector) => ({
    id: sector.id,
    label: sector.label,
    tone: sector.id,
  }));

  if (hasPartialGroupSelection(normalized)) {
    getSelectedGroups(normalized).forEach((group) => {
      pills.push({ id: group.id, label: group.label, tone: group.sectorId });
    });
  }

  if (hasCommoditySelection(normalized)) {
    getSelectedCommodities(normalized).forEach((commodity) => {
      pills.push({ id: commodity.id, label: commodity.label, tone: commodity.sectorId });
    });
  }

  return pills;
}

export function getSelectedSectors(filter) {
  return getEffectiveSectorIds(normalizeFilter(filter))
    .map((sectorId) => getSectorById(sectorId))
    .filter(Boolean);
}

export function getSelectedSector(filter) {
  const selectedSectors = getSelectedSectors(filter);
  return selectedSectors.length === 1 ? selectedSectors[0] : null;
}

export function getSelectedGroupsForSector(filter, sectorId) {
  return getEffectiveGroupIdsForSector(normalizeFilter(filter), sectorId)
    .map((groupId) => getGroupById(groupId))
    .filter(Boolean);
}

export function getSelectedGroups(filter) {
  const normalized = normalizeFilter(filter);
  return getSelectedSectors(normalized).flatMap((sector) => getSelectedGroupsForSector(normalized, sector.id));
}

export function getSelectedGroup(filter) {
  return getGroupById(getSingleSelectedGroupId(normalizeFilter(filter)));
}

export function getSelectedCommodities(filter) {
  return getEffectiveCommodityIds(normalizeFilter(filter))
    .map((commodityId) => getCommodityById(commodityId))
    .filter(Boolean);
}

export function getExpandedGroupsForSector(sectorId) {
  return getSectorById(sectorId)?.groups || [];
}

export function getAllSectors() {
  return HOME_FILTER_TAXONOMY;
}
