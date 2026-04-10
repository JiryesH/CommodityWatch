import test from "node:test";
import assert from "node:assert/strict";

import { findCalendarEventForDemandRelease, loadCalendarEvents } from "../calendar-data.js";

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

test("findCalendarEventForDemandRelease matches exact CalendarWatch events by name and UTC timestamp", () => {
  const release = {
    release_name: "EIA Weekly Petroleum Status Report",
    scheduled_for: "2026-04-08T14:30:00Z",
  };
  const events = [
    {
      id: "demand_eia_wpsr:2026-04-08",
      name: "EIA Weekly Petroleum Status Report",
      event_date: "2026-04-08T14:30:00Z",
    },
  ];

  assert.equal(findCalendarEventForDemandRelease(release, events)?.id, "demand_eia_wpsr:2026-04-08");
  assert.equal(
    findCalendarEventForDemandRelease(
      { release_name: "EIA Weekly Petroleum Status Report", scheduled_for: "2026-04-09T14:30:00Z" },
      events
    ),
    null
  );
});
