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

function createApp(loadEvents) {
  const app = new CalendarWatchApp({
    root: {},
    filterRoot: {},
    drawerOverlay: {},
    drawerPanel: {},
    toTopButton: {},
    navSearch: {},
    searchToggle: {},
    searchInput: { value: "" },
    loadEvents,
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
