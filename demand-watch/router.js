import { getDemandVerticalById } from "./data.js";

export const DEMAND_ROUTE_PREFIX = "/demand-watch";

function trimTrailingSlash(value) {
  if (!value || value === "/") {
    return value || "/";
  }

  return value.endsWith("/") ? value.slice(0, -1) : value;
}

export function normalizeDemandPath(pathname) {
  if (!pathname || pathname === "/") {
    return "/";
  }

  return pathname.endsWith("/") ? pathname : `${pathname}/`;
}

export function buildDemandOverviewHref() {
  return `${DEMAND_ROUTE_PREFIX}/`;
}

export function buildDemandConceptHref(verticalId, conceptCode) {
  return `${DEMAND_ROUTE_PREFIX}/${encodeURIComponent(verticalId)}/${encodeURIComponent(conceptCode)}/`;
}

export function parseDemandRoute(pathname) {
  const normalizedPath = trimTrailingSlash(pathname || "/");
  const prefix = trimTrailingSlash(DEMAND_ROUTE_PREFIX);

  if (normalizedPath === prefix) {
    return { view: "overview", verticalId: null, conceptCode: null };
  }

  const rawSegments = normalizedPath.startsWith(`${prefix}/`)
    ? normalizedPath.slice(prefix.length + 1).split("/").filter(Boolean)
    : [];

  if (!rawSegments.length) {
    return { view: "overview", verticalId: null, conceptCode: null };
  }

  if (rawSegments.length !== 2) {
    return {
      view: "not-found",
      verticalId: null,
      conceptCode: null,
      reason: "DemandWatch route does not match a known destination.",
    };
  }

  const [verticalId, conceptCode] = rawSegments.map((segment) => decodeURIComponent(segment));
  if (!getDemandVerticalById(verticalId)) {
    return {
      view: "not-found",
      verticalId: null,
      conceptCode: null,
      reason: `Unknown DemandWatch vertical: ${verticalId}`,
    };
  }

  if (!conceptCode) {
    return {
      view: "not-found",
      verticalId: null,
      conceptCode: null,
      reason: "DemandWatch concept code is required.",
    };
  }

  return {
    view: "detail",
    verticalId,
    conceptCode,
  };
}
