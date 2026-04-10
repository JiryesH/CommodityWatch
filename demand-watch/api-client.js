function compactResponseBody(bodyText) {
  const normalized = String(bodyText || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }

  if (normalized.length <= 180) {
    return normalized;
  }

  return `${normalized.slice(0, 177)}...`;
}

function buildQueryString(params = {}) {
  const search = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }

    search.set(key, String(value));
  });

  const query = search.toString();
  return query ? `?${query}` : "";
}

const bootstrapCache = {
  promise: null,
  value: null,
  expiresAt: 0,
};

async function fetchJson(url) {
  const response = await fetch(url, {
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
    },
  });

  const bodyText = await response.text();
  let payload = null;

  if (bodyText) {
    try {
      payload = JSON.parse(bodyText);
    } catch {
      payload = null;
    }
  }

  if (!response.ok) {
    const detail =
      (payload && (payload.detail || payload.error || payload.message)) || compactResponseBody(bodyText) || "Request failed.";
    throw new Error(typeof detail === "string" ? detail : "Request failed.");
  }

  if (!payload || typeof payload !== "object") {
    throw new Error("DemandWatch API returned an invalid payload.");
  }

  return payload;
}

function assertArrayField(payload, fieldName, label) {
  if (!Array.isArray(payload?.[fieldName])) {
    throw new Error(`${label} payload is invalid.`);
  }

  return payload;
}

function parseTimestamp(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function assertMatchingGeneratedAt(value, expectedValue, label) {
  const actualTimestamp = parseTimestamp(value);
  const expectedTimestamp = parseTimestamp(expectedValue);

  if (!actualTimestamp || !expectedTimestamp || actualTimestamp !== expectedTimestamp) {
    throw new Error(`${label} payload mixes bootstrap versions.`);
  }
}

function hasFreshBootstrapCache() {
  if (!bootstrapCache.value) {
    return false;
  }

  if (!bootstrapCache.expiresAt) {
    return true;
  }

  return bootstrapCache.expiresAt > Date.now();
}

function mapBootstrapPayload(payload) {
  if (payload?.module !== "demandwatch") {
    throw new Error("DemandWatch bootstrap payload is invalid.");
  }
  if (!payload?.generated_at || !parseTimestamp(payload.generated_at)) {
    throw new Error("DemandWatch bootstrap payload is invalid.");
  }

  assertArrayField(payload?.macro_strip, "items", "DemandWatch macro strip");
  assertArrayField(payload?.scorecard, "items", "DemandWatch scorecard");
  assertArrayField(payload?.movers, "items", "DemandWatch movers");

  if (!payload?.coverage_notes || typeof payload.coverage_notes !== "object") {
    throw new Error("DemandWatch coverage payload is invalid.");
  }

  if (!Array.isArray(payload.coverage_notes.verticals) || !payload.coverage_notes.summary) {
    throw new Error("DemandWatch coverage payload is invalid.");
  }

  if (!Array.isArray(payload?.vertical_details)) {
    throw new Error("DemandWatch bootstrap vertical detail payload is invalid.");
  }

  if (!Array.isArray(payload?.vertical_errors)) {
    throw new Error("DemandWatch bootstrap vertical error payload is invalid.");
  }

  const expectedVerticalIds = new Set();
  payload.coverage_notes.verticals.forEach((item) => {
    if (!item || typeof item !== "object" || typeof item.id !== "string" || !item.id) {
      throw new Error("DemandWatch coverage payload is invalid.");
    }
    if (expectedVerticalIds.has(item.id)) {
      throw new Error("DemandWatch coverage payload is invalid.");
    }
    expectedVerticalIds.add(item.id);
  });

  const representedVerticalIds = new Set();
  payload.vertical_details.forEach((item) => {
    if (!item || typeof item !== "object" || typeof item.id !== "string" || !item.id || typeof item.code !== "string") {
      throw new Error("DemandWatch bootstrap vertical detail payload is invalid.");
    }
    if (representedVerticalIds.has(item.id)) {
      throw new Error("DemandWatch bootstrap vertical detail payload is invalid.");
    }
    representedVerticalIds.add(item.id);
  });
  payload.vertical_errors.forEach((item) => {
    if (!item || typeof item !== "object" || typeof item.vertical_id !== "string" || !item.vertical_id || typeof item.message !== "string") {
      throw new Error("DemandWatch bootstrap vertical error payload is invalid.");
    }
    if (representedVerticalIds.has(item.vertical_id)) {
      throw new Error("DemandWatch bootstrap vertical error payload is invalid.");
    }
    representedVerticalIds.add(item.vertical_id);
  });
  if (
    representedVerticalIds.size !== expectedVerticalIds.size ||
    [...expectedVerticalIds].some((verticalId) => !representedVerticalIds.has(verticalId))
  ) {
    throw new Error("DemandWatch bootstrap payload is missing vertical detail coverage.");
  }

  if (!payload?.next_release_dates || !Array.isArray(payload.next_release_dates.items)) {
    throw new Error("DemandWatch release calendar payload is invalid.");
  }
  assertMatchingGeneratedAt(payload.macro_strip.generated_at, payload.generated_at, "DemandWatch macro strip");
  assertMatchingGeneratedAt(payload.scorecard.generated_at, payload.generated_at, "DemandWatch scorecard");
  assertMatchingGeneratedAt(payload.movers.generated_at, payload.generated_at, "DemandWatch movers");
  assertMatchingGeneratedAt(payload.coverage_notes.generated_at, payload.generated_at, "DemandWatch coverage");
  assertMatchingGeneratedAt(payload.next_release_dates.generated_at, payload.generated_at, "DemandWatch release calendar");
  payload.vertical_details.forEach((item) => {
    assertMatchingGeneratedAt(item?.generated_at, payload.generated_at, "DemandWatch vertical detail");
  });

  return {
    generated_at: payload.generated_at,
    expires_at: payload.expires_at || null,
    macroStrip: payload.macro_strip,
    scorecard: payload.scorecard,
    movers: payload.movers,
    coverageNotes: payload.coverage_notes,
    verticalDetails: payload.vertical_details,
    verticalErrors: payload.vertical_errors.map((item) => ({
      verticalId: item.vertical_id,
      message: item.message,
    })),
    nextReleaseDates: payload.next_release_dates,
  };
}

export async function fetchDemandMacroStrip() {
  const payload = await fetchJson("/api/demandwatch/macro-strip");
  return assertArrayField(payload, "items", "DemandWatch macro strip");
}

export async function fetchDemandScorecard() {
  const payload = await fetchJson("/api/demandwatch/scorecard");
  return assertArrayField(payload, "items", "DemandWatch scorecard");
}

export async function fetchDemandMovers({ limit = 6 } = {}) {
  const payload = await fetchJson(`/api/demandwatch/movers${buildQueryString({ limit })}`);
  return assertArrayField(payload, "items", "DemandWatch movers");
}

export async function fetchDemandVerticalDetail(verticalId) {
  const payload = await fetchJson(`/api/demandwatch/verticals/${encodeURIComponent(verticalId)}`);
  if (!payload || typeof payload !== "object" || typeof payload.id !== "string") {
    throw new Error("DemandWatch vertical detail payload is invalid.");
  }

  return payload;
}

export async function fetchDemandConceptDetail(verticalId, conceptCode) {
  const payload = await fetchJson(
    `/api/demandwatch/verticals/${encodeURIComponent(verticalId)}/concepts/${encodeURIComponent(conceptCode)}`
  );
  if (!payload || typeof payload !== "object" || typeof payload.code !== "string") {
    throw new Error("DemandWatch concept detail payload is invalid.");
  }

  return payload;
}

export async function fetchDemandCoverageNotes() {
  const payload = await fetchJson("/api/demandwatch/coverage-notes");
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.verticals) || !payload.summary) {
    throw new Error("DemandWatch coverage payload is invalid.");
  }

  return payload;
}

export async function fetchDemandWatchPageData({ force = false } = {}) {
  if (force) {
    resetDemandWatchPageDataCache();
  }

  if (hasFreshBootstrapCache()) {
    return bootstrapCache.value;
  }

  if (!bootstrapCache.promise) {
    bootstrapCache.promise = fetchJson("/api/snapshot/demandwatch")
      .then((payload) => {
        const mappedPayload = mapBootstrapPayload(payload);
        bootstrapCache.value = mappedPayload;
        bootstrapCache.expiresAt = parseTimestamp(mappedPayload.expires_at);
        bootstrapCache.promise = null;
        return mappedPayload;
      })
      .catch((error) => {
        bootstrapCache.promise = null;
        throw error;
      });
  }

  return bootstrapCache.promise;
}

export function resetDemandWatchPageDataCache() {
  bootstrapCache.promise = null;
  bootstrapCache.value = null;
  bootstrapCache.expiresAt = 0;
}
