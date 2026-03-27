import test from "node:test";
import assert from "node:assert/strict";

import { loadCalendarEvents } from "../calendar-data.js";

test("loadCalendarEvents omits invalid date parameters and keeps valid ones", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (url) => {
    calls.push(url);
    return {
      ok: true,
      json: async () => ({ data: [] }),
    };
  };

  try {
    await loadCalendarEvents({
      from: "not-a-date",
      to: new Date("2026-03-12T00:00:00Z"),
      sectors: [],
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(calls, ["/api/calendar?to=2026-03-12"]);
});
