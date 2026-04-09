import test from "node:test";
import assert from "node:assert/strict";


class FakeElement {
  constructor(id = "") {
    this.id = id;
    this.innerHTML = "";
    this.listeners = new Map();
    const classes = new Set();
    this.classList = {
      add: (...tokens) => tokens.forEach((token) => classes.add(token)),
      remove: (...tokens) => tokens.forEach((token) => classes.delete(token)),
      toggle: (token, force) => {
        if (force === undefined) {
          if (classes.has(token)) {
            classes.delete(token);
            return false;
          }
          classes.add(token);
          return true;
        }

        if (force) {
          classes.add(token);
          return true;
        }

        classes.delete(token);
        return false;
      },
      contains: (token) => classes.has(token),
    };
  }

  addEventListener(type, handler) {
    const handlers = this.listeners.get(type) || [];
    handlers.push(handler);
    this.listeners.set(type, handlers);
  }

  querySelector() {
    return null;
  }

  querySelectorAll() {
    return [];
  }

  setAttribute() {}
}

function createDomHarness() {
  const nodes = new Map(
    ["demand-root", "demand-filter-root", "to-top-btn"].map((id) => [id, new FakeElement(id)])
  );

  return {
    nodes,
    document: {
      getElementById(id) {
        return nodes.get(id) || null;
      },
      querySelectorAll() {
        return [];
      },
    },
    window: {
      location: { hash: "" },
      scrollY: 0,
      addEventListener() {},
      scrollTo() {},
    },
  };
}

function snapshotTimestamp() {
  return "2099-04-08T00:00:00Z";
}

function releaseTimestamp() {
  return "2099-04-08T00:00:00Z";
}

function scorecardItem({
  id,
  code,
  label,
  navLabel,
  shortLabel,
  sector,
  scorecardLabel,
  displayValue,
  primarySeriesCode,
}) {
  return {
    id,
    code,
    label,
    nav_label: navLabel,
    short_label: shortLabel,
    sector,
    scorecard_label: scorecardLabel,
    latest_value: 1,
    unit_code: "index",
    unit_symbol: "index",
    display_value: displayValue,
    yoy_value: 1.2,
    yoy_label: "+1.2% YoY",
    trend: "up",
    latest_period_label: "Mar 2099",
    freshness: "1d ago",
    freshness_state: "fresh",
    stale: false,
    source_url: "https://example.com/source",
    primary_series_code: primarySeriesCode,
  };
}

function crudeVerticalDetail() {
  return {
    generated_at: snapshotTimestamp(),
    id: "crude-products",
    code: "crude_products",
    label: "Crude Oil + Refined Products",
    nav_label: "Crude",
    short_label: "Crude + Products",
    sector: "energy",
    summary: "Direct consumption remains firm.",
    scorecard: scorecardItem({
      id: "crude-products",
      code: "crude_products",
      label: "Crude Oil + Refined Products",
      navLabel: "Crude",
      shortLabel: "Crude + Products",
      sector: "energy",
      scorecardLabel: "US product supplied",
      displayValue: "9.6 mb/d",
      primarySeriesCode: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
    }),
    facts: [],
    sections: [
      {
        id: "direct",
        title: "Direct Consumption",
        description: "Measured demand anchors from the public-domain EIA petroleum releases.",
        indicators: [
          {
            series_id: "eia-series",
            indicator_id: "eia-indicator",
            code: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
            title: "Total product supplied",
            tier: "t1_direct",
            tier_label: "T1 · Direct",
            latest_value: 9600,
            unit_code: "kb_d",
            unit_symbol: "kb/d",
            display_value: "9.6 mb/d",
            change_label: "+6.7% YoY",
            detail: "4-week average at 9.5 mb/d",
            trend: "up",
            sparkline: [9300, 9400, 9500, 9600],
            freshness: "1d ago",
            freshness_state: "fresh",
            latest_period_label: "Week ending 27 Mar 2099",
            latest_release_date: releaseTimestamp(),
            latest_vintage_at: releaseTimestamp(),
            source_url: "https://example.com/eia",
            coverage_status: "live",
            vintage_count: 2,
          },
        ],
        table_rows: [
          {
            series_id: "eia-series",
            indicator_id: "eia-indicator",
            code: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
            label: "Total product supplied",
            latest_value: 9600,
            unit_code: "kb_d",
            unit_symbol: "kb/d",
            latest_display: "9.6 mb/d",
            change_display: "+600 kb/d",
            yoy_display: "+6.7% YoY",
            freshness: "1d ago",
            freshness_state: "fresh",
            trend: "up",
            latest_period_label: "Week ending 27 Mar 2099",
            latest_release_date: releaseTimestamp(),
            source_url: "https://example.com/eia",
            vintage_count: 2,
          },
        ],
      },
    ],
    calendar: [],
    notes: [],
  };
}

function demandBootstrapPayload({
  verticalDetails = [crudeVerticalDetail()],
  verticalErrors = [],
} = {}) {
  return {
    module: "demandwatch",
    generated_at: snapshotTimestamp(),
    expires_at: "2099-04-08T00:05:00Z",
    macro_strip: {
      generated_at: snapshotTimestamp(),
      items: [
        {
          id: "indpro",
          code: "FRED_US_INDUSTRIAL_PRODUCTION",
          label: "US Industrial Production",
          descriptor: "Federal Reserve G.17",
          latest_value: 103.8,
          unit_code: "index",
          unit_symbol: "index",
          display_value: "103.8",
          change_label: "+1.2% YoY",
          trend: "up",
          freshness: "1d ago",
          freshness_state: "fresh",
          latest_period_label: "Mar 2099",
          latest_release_date: releaseTimestamp(),
          source_url: "https://example.com/fred",
        },
      ],
    },
    scorecard: {
      generated_at: snapshotTimestamp(),
      items: [
        scorecardItem({
          id: "crude-products",
          code: "crude_products",
          label: "Crude Oil + Refined Products",
          navLabel: "Crude",
          shortLabel: "Crude + Products",
          sector: "energy",
          scorecardLabel: "US product supplied",
          displayValue: "9.6 mb/d",
          primarySeriesCode: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
        }),
        scorecardItem({
          id: "base-metals",
          code: "base_metals",
          label: "Base Metals",
          navLabel: "Metals",
          shortLabel: "Base Metals",
          sector: "metals",
          scorecardLabel: "Industrial production",
          displayValue: "103.8",
          primarySeriesCode: "FRED_US_INDUSTRIAL_PRODUCTION",
        }),
      ],
    },
    movers: {
      generated_at: snapshotTimestamp(),
      items: [],
    },
    coverage_notes: {
      generated_at: snapshotTimestamp(),
      markdown: "# DemandWatch Coverage Audit",
      summary: {
        vertical_count: 2,
        series_count: 2,
        status_counts: {
          live: 2,
          partial: 0,
          deferred: 0,
          blocked: 0,
        },
      },
      verticals: [
        {
          id: "crude-products",
          code: "crude_products",
          name: "Crude Oil + Refined Products",
          commodity_code: "crude_products",
          sector: "energy",
          counts: { live: 1, partial: 0, deferred: 0, blocked: 0 },
          live: [],
          partial: [],
          deferred: [],
          blocked: [],
        },
        {
          id: "base-metals",
          code: "base_metals",
          name: "Base Metals",
          commodity_code: "base_metals",
          sector: "metals",
          counts: { live: 1, partial: 0, deferred: 0, blocked: 0 },
          live: [],
          partial: [],
          deferred: [],
          blocked: [],
        },
      ],
    },
    vertical_details: verticalDetails,
    vertical_errors: verticalErrors,
    next_release_dates: {
      generated_at: snapshotTimestamp(),
      items: [],
    },
  };
}

async function withAppHarness(fn) {
  const originalDocument = globalThis.document;
  const originalWindow = globalThis.window;
  const originalFetch = globalThis.fetch;
  const harness = createDomHarness();
  const apiClient = await import(new URL("../api-client.js", import.meta.url).href);

  globalThis.document = harness.document;
  globalThis.window = harness.window;
  apiClient.resetDemandWatchPageDataCache();

  try {
    await fn({
      demandRoot: harness.nodes.get("demand-root"),
      importApp: async () =>
        import(new URL(`../app.js?test=${Date.now()}-${Math.random()}`, import.meta.url).href),
    });
  } finally {
    apiClient.resetDemandWatchPageDataCache();
    globalThis.document = originalDocument;
    globalThis.window = originalWindow;
    globalThis.fetch = originalFetch;
  }
}

function jsonResponse(payload) {
  return {
    ok: true,
    text: async () => JSON.stringify(payload),
  };
}

test("DemandWatch app boot uses one snapshot request and cached reload stays request-free", async () => {
  await withAppHarness(async ({ demandRoot, importApp }) => {
    const requestedUrls = [];
    globalThis.fetch = async (url) => {
      requestedUrls.push(url);
      return jsonResponse(demandBootstrapPayload());
    };

    const app = await importApp();
    await app.initialDemandWatchLoad;

    assert.deepEqual(requestedUrls, ["/api/snapshot/demandwatch"]);
    assert.match(demandRoot.innerHTML, /Demand Pulse/);

    await app.loadDemandWatch();

    assert.deepEqual(requestedUrls, ["/api/snapshot/demandwatch"]);
    assert.match(demandRoot.innerHTML, /Demand Pulse/);
  });
});

test("DemandWatch app keeps the page bootable when one bootstrap vertical detail is unavailable", async () => {
  await withAppHarness(async ({ demandRoot, importApp }) => {
    globalThis.fetch = async (url) => {
      assert.equal(url, "/api/snapshot/demandwatch");
      return jsonResponse(
        demandBootstrapPayload({
          verticalDetails: [crudeVerticalDetail()],
          verticalErrors: [
            {
              vertical_id: "base-metals",
              message: "Synthetic failure for test.",
            },
          ],
        })
      );
    };

    const app = await importApp();
    await app.initialDemandWatchLoad;

    assert.match(demandRoot.innerHTML, /Demand Pulse/);
    assert.match(demandRoot.innerHTML, /Detail unavailable/);
    assert.match(demandRoot.innerHTML, /Synthetic failure for test\./);
  });
});
