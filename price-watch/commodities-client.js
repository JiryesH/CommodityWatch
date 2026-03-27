/**
 * @typedef {"exact" | "related"} MatchType
 */

/**
 * @typedef {Object} PublishedSeriesRecord
 * @property {string} series_key
 * @property {string} target_concept
 * @property {string} actual_series_name
 * @property {string | null} benchmark_series
 * @property {MatchType} match_type
 * @property {string} source_name
 * @property {string} source_series_code
 * @property {string | null} source_url
 * @property {string} frequency
 * @property {string | null} unit
 * @property {string | null} currency
 * @property {string | null} geography
 * @property {boolean} active
 * @property {string | null} notes
 * @property {string | null} updated_at
 */

/**
 * @typedef {Object} PublishedLatestRecord
 * @property {string} series_key
 * @property {string} target_concept
 * @property {string} actual_series_name
 * @property {string | null} benchmark_series
 * @property {MatchType} match_type
 * @property {string} observation_date
 * @property {number} value
 * @property {string | null} unit
 * @property {string | null} currency
 * @property {string} frequency
 * @property {string} source_name
 * @property {string} source_series_code
 * @property {string | null} source_url
 * @property {string | null} geography
 * @property {string | null} updated_at
 * @property {string | null} notes
 * @property {number | null} previous_value
 * @property {number | null} delta_value
 * @property {number | null} delta_pct
 */

/**
 * @typedef {Object} PublishedObservationRecord
 * @property {string} series_key
 * @property {string} target_concept
 * @property {string} actual_series_name
 * @property {string | null} benchmark_series
 * @property {MatchType} match_type
 * @property {string} observation_date
 * @property {number} value
 * @property {string | null} unit
 * @property {string | null} currency
 * @property {string} frequency
 * @property {string} source_name
 * @property {string} source_series_code
 * @property {string | null} source_url
 * @property {string | null} geography
 * @property {string | null} release_date
 * @property {string | null} retrieved_at
 * @property {string | number | null} raw_artifact_id
 * @property {string | null} inserted_at
 * @property {string | null} updated_at
 * @property {string | null} notes
 */

/**
 * @template T
 * @typedef {Object} ApiEnvelope
 * @property {string} generated_at
 * @property {T} data
 * @property {Record<string, unknown>=} meta
 */

/**
 * @typedef {Object} RelatedHeadlineRecord
 * @property {string | null} id
 * @property {string} title
 * @property {string} source
 * @property {string | null} published
 * @property {string | null} link
 */

function compactResponseBody(bodyText) {
  const normalized = String(bodyText || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }

  if (normalized.length <= 160) {
    return normalized;
  }

  return `${normalized.slice(0, 157)}...`;
}

async function fetchEnvelope(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
    },
  });
  const bodyText = await response.text();

  if (!response.ok) {
    const detail = compactResponseBody(bodyText);
    throw new Error(`Request failed for ${url}: ${response.status}${detail ? ` ${detail}` : ""}`);
  }

  let payload;

  try {
    payload = JSON.parse(bodyText);
  } catch {
    const detail = compactResponseBody(bodyText);
    throw new Error(`Invalid JSON response from ${url}${detail ? `: ${detail}` : ""}`);
  }

  if (!payload || typeof payload !== "object") {
    throw new Error(`Malformed commodity API response from ${url}: expected an object envelope`);
  }

  return /** @type {ApiEnvelope<any>} */ (payload);
}

async function fetchArrayEnvelope(url) {
  const payload = await fetchEnvelope(url);

  if (!Array.isArray(payload.data)) {
    throw new Error(`Malformed commodity API response from ${url}: expected data to be an array`);
  }

  return payload.data;
}

export class CommodityApiClient {
  constructor(baseUrl = "") {
    this.baseUrl = baseUrl;
  }

  /**
   * @returns {Promise<PublishedSeriesRecord[]>}
   */
  async listSeries() {
    return /** @type {Promise<PublishedSeriesRecord[]>} */ (
      fetchArrayEnvelope(`${this.baseUrl}/api/commodities/series`)
    );
  }

  /**
   * @returns {Promise<PublishedLatestRecord[]>}
   */
  async listLatest() {
    return /** @type {Promise<PublishedLatestRecord[]>} */ (
      fetchArrayEnvelope(`${this.baseUrl}/api/commodities/latest`)
    );
  }

  /**
   * @param {string} seriesKey
   * @param {{ start?: string | null, end?: string | null }=} options
   * @returns {Promise<PublishedObservationRecord[]>}
   */
  async getHistory(seriesKey, options = {}) {
    const params = new URLSearchParams();

    if (options.start) {
      params.set("start", options.start);
    }

    if (options.end) {
      params.set("end", options.end);
    }

    const query = params.toString();
    return /** @type {Promise<PublishedObservationRecord[]>} */ (
      fetchArrayEnvelope(
        `${this.baseUrl}/api/commodities/${encodeURIComponent(seriesKey)}/history${query ? `?${query}` : ""}`
      )
    );
  }

  /**
   * @param {string} seriesKey
   * @param {{ limit?: number }=} options
   * @returns {Promise<RelatedHeadlineRecord[]>}
   */
  async getRelatedHeadlines(seriesKey, options = {}) {
    const params = new URLSearchParams();

    if (typeof options.limit === "number" && Number.isFinite(options.limit)) {
      params.set("limit", String(Math.max(1, Math.trunc(options.limit))));
    }

    const query = params.toString();
    return /** @type {Promise<RelatedHeadlineRecord[]>} */ (
      fetchArrayEnvelope(
        `${this.baseUrl}/api/commodities/${encodeURIComponent(seriesKey)}/headlines${query ? `?${query}` : ""}`
      )
    );
  }
}
