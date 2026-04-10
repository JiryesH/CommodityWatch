import test from "node:test";
import assert from "node:assert/strict";

import { CalendarWatchApp } from "../app.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
}

function buildEvent(id, eventDate) {
  return {
    id,
    name: `Event ${id}`,
    organiser: "Test organiser",
    cadence: "weekly",
    commodity_sectors: ["energy"],
    event_date: eventDate,
    calendar_url: "https://example.com/calendar",
    source_label: "Example",
    notes: null,
    is_confirmed: true,
  };
}

function fakeNode() {
  return {
    hidden: false,
    innerHTML: "",
    classList: {
      toggle() {},
    },
    addEventListener() {},
    setAttribute() {},
  };
}

function createApp(loadEvents, options = {}) {
  globalThis.document ??= {
    addEventListener() {},
    body: { classList: { toggle() {} } },
  };
  globalThis.window ??= {
    addEventListener() {},
    scrollTo() {},
    scrollY: 0,
  };

  const app = new CalendarWatchApp({
    root: fakeNode(),
    filterRoot: fakeNode(),
    drawerOverlay: fakeNode(),
    drawerPanel: fakeNode(),
    toTopButton: fakeNode(),
    navSearch: { ...fakeNode(), classList: { toggle() {} } },
    searchToggle: { ...fakeNode(), setAttribute() {} },
    searchInput: { value: "", addEventListener() {} },
    loadEvents,
    ...options,
  });

  app.render = () => {};
  app.renderDrawer = () => {};
  app.renderSearchUi = () => {};

  return app;
}

test("refreshData ignores stale success responses from earlier requests", async () => {
  const first = deferred();
  const second = deferred();
  let callCount = 0;
  const app = createApp(() => {
    callCount += 1;
    return callCount === 1 ? first.promise : second.promise;
  });

  const firstRefresh = app.refreshData();
  const secondRefresh = app.refreshData();

  second.resolve([buildEvent("fresh", "2026-03-10T12:00:00Z")]);
  await secondRefresh;

  assert.deepEqual(app.state.events.map((event) => event.id), ["fresh"]);
  assert.deepEqual(app.state.visibleEvents.map((event) => event.id), ["fresh"]);
  assert.equal(app.state.loading, false);
  assert.equal(app.state.error, null);

  first.resolve([buildEvent("stale", "2026-03-11T12:00:00Z")]);
  await firstRefresh;

  assert.deepEqual(app.state.events.map((event) => event.id), ["fresh"]);
  assert.deepEqual(app.state.visibleEvents.map((event) => event.id), ["fresh"]);
  assert.equal(app.state.loading, false);
  assert.equal(app.state.error, null);
});

test("refreshData ignores stale failures after a newer request succeeds", async () => {
  const first = deferred();
  const second = deferred();
  let callCount = 0;
  const app = createApp(() => {
    callCount += 1;
    return callCount === 1 ? first.promise : second.promise;
  });

  const firstRefresh = app.refreshData();
  const secondRefresh = app.refreshData();

  second.resolve([buildEvent("fresh", "2026-03-10T12:00:00Z")]);
  await secondRefresh;

  first.reject(new Error("stale failure"));
  await firstRefresh;

  assert.deepEqual(app.state.events.map((event) => event.id), ["fresh"]);
  assert.deepEqual(app.state.visibleEvents.map((event) => event.id), ["fresh"]);
  assert.equal(app.state.loading, false);
  assert.equal(app.state.error, null);
});

test("openEvent tolerates malformed release dates", () => {
  const app = createApp(() => Promise.resolve([]));
  app.state.visibleEvents = [
    {
      id: "broken",
      name: "Broken event",
      organiser: "Test organiser",
      cadence: "weekly",
      commodity_sectors: ["energy"],
      event_date: "not-a-date",
      calendar_url: "https://example.com/calendar",
      source_label: "Example",
      notes: null,
      is_confirmed: true,
    },
  ];

  app.openEvent("broken", "calendar");

  assert.equal(app.state.panelMode, "event");
  assert.equal(app.state.selectedEventId, "broken");
  assert.equal(app.state.panelDayIso, null);
});

test("init opens an exact deep-linked event when an initial event id is provided", async () => {
  const app = createApp(
    () =>
      Promise.resolve([
        buildEvent("demand_eia_wpsr:2026-04-22", "2026-04-22T14:30:00Z"),
      ]),
    { initialEventId: "demand_eia_wpsr:2026-04-22" }
  );

  await app.init();

  assert.equal(app.state.panelMode, "event");
  assert.equal(app.state.selectedEventId, "demand_eia_wpsr:2026-04-22");
  assert.equal(app.state.viewMode, "week");
});
