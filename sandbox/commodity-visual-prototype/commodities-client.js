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

async function fetchEnvelope(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Request failed for ${url}: ${response.status} ${errorText}`);
  }

  return /** @type {Promise<ApiEnvelope<any>>} */ (response.json());
}

export class CommodityApiClient {
  constructor(baseUrl = "") {
    this.baseUrl = baseUrl;
  }

  /**
   * @returns {Promise<PublishedSeriesRecord[]>}
   */
  async listSeries() {
    const payload = await fetchEnvelope(`${this.baseUrl}/api/commodities/series`);
    return /** @type {PublishedSeriesRecord[]} */ (payload.data);
  }

  /**
   * @returns {Promise<PublishedLatestRecord[]>}
   */
  async listLatest() {
    const payload = await fetchEnvelope(`${this.baseUrl}/api/commodities/latest`);
    return /** @type {PublishedLatestRecord[]} */ (payload.data);
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
    const payload = await fetchEnvelope(
      `${this.baseUrl}/api/commodities/${encodeURIComponent(seriesKey)}/history${query ? `?${query}` : ""}`
    );
    return /** @type {PublishedObservationRecord[]} */ (payload.data);
  }
}
