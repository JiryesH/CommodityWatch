import test from "node:test";
import assert from "node:assert/strict";

import { buildCalendarEventHref, parseCalendarLocation } from "../router.js";

test("CalendarWatch deep-link helpers round-trip exact event ids", () => {
  const href = buildCalendarEventHref("demand_eia_wpsr:2026-04-22");
  assert.equal(href, "/calendar-watch/?event=demand_eia_wpsr%3A2026-04-22");

  assert.deepEqual(
    parseCalendarLocation({ search: "?event=demand_eia_wpsr%3A2026-04-22" }),
    { eventId: "demand_eia_wpsr:2026-04-22" }
  );
});
