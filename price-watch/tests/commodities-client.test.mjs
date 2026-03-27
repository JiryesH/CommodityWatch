import test from "node:test";
import assert from "node:assert/strict";

import { CommodityApiClient } from "../commodities-client.js";

async function withMockFetch(handler, fn) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = handler;

  try {
    await fn();
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test("CommodityApiClient returns array payloads from valid envelopes", async () => {
  await withMockFetch(
    async () => ({
      ok: true,
      status: 200,
      text: async () =>
        JSON.stringify({
          generated_at: "2026-03-26T00:00:00Z",
          data: [{ series_key: "series-a", value: 12.5 }],
        }),
    }),
    async () => {
      const client = new CommodityApiClient("");
      const series = await client.listSeries();

      assert.deepEqual(series, [{ series_key: "series-a", value: 12.5 }]);
    }
  );
});

test("CommodityApiClient surfaces malformed responses with targeted errors", async () => {
  await withMockFetch(
    async () => ({
      ok: true,
      status: 200,
      text: async () => "<html>not json</html>",
    }),
    async () => {
      const client = new CommodityApiClient("");
      await assert.rejects(client.listLatest(), /Invalid JSON response/);
    }
  );

  await withMockFetch(
    async () => ({
      ok: true,
      status: 200,
      text: async () =>
        JSON.stringify({
          generated_at: "2026-03-26T00:00:00Z",
          data: { series_key: "series-a" },
        }),
    }),
    async () => {
      const client = new CommodityApiClient("");
      await assert.rejects(client.getHistory("series-a"), /expected data to be an array/);
    }
  );
});
