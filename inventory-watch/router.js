import { isCommodityGroupSlug } from "./catalog.js";

export const INVENTORY_ROUTE_PREFIX = "/inventory-watch";

function trimTrailingSlash(value) {
  if (!value || value === "/") {
    return value || "/";
  }

  return value.endsWith("/") ? value.slice(0, -1) : value;
}

export function buildInventorySnapshotHref(groupSlug = "all") {
  return groupSlug === "all"
    ? `${INVENTORY_ROUTE_PREFIX}/`
    : `${INVENTORY_ROUTE_PREFIX}/${encodeURIComponent(groupSlug)}/`;
}

export function buildInventoryDetailHref(groupSlug, indicatorId) {
  const normalizedGroup = isCommodityGroupSlug(groupSlug) ? groupSlug : "all";
  return `${INVENTORY_ROUTE_PREFIX}/${encodeURIComponent(normalizedGroup)}/${encodeURIComponent(indicatorId)}/`;
}

export function parseInventoryRoute(pathname) {
  const normalizedPath = trimTrailingSlash(pathname || "/");
  const prefix = trimTrailingSlash(INVENTORY_ROUTE_PREFIX);

  if (normalizedPath === prefix || normalizedPath === `${prefix}`) {
    return { view: "snapshot", groupSlug: "all", indicatorId: null };
  }

  const rawSegments = normalizedPath.startsWith(`${prefix}/`)
    ? normalizedPath.slice(prefix.length + 1).split("/").filter(Boolean)
    : [];

  if (!rawSegments.length) {
    return { view: "snapshot", groupSlug: "all", indicatorId: null };
  }

  const [groupCandidate, indicatorCandidate] = rawSegments.map((segment) => decodeURIComponent(segment));
  if (!isCommodityGroupSlug(groupCandidate)) {
    return {
      view: "not-found",
      groupSlug: "all",
      indicatorId: null,
      reason: `Unknown InventoryWatch group: ${groupCandidate}`,
    };
  }

  if (!indicatorCandidate) {
    return { view: "snapshot", groupSlug: groupCandidate, indicatorId: null };
  }

  return {
    view: "detail",
    groupSlug: groupCandidate,
    indicatorId: indicatorCandidate,
  };
}
