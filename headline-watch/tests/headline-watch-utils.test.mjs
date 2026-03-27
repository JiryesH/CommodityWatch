import test from "node:test";
import assert from "node:assert/strict";

import {
  areSetsEqual,
  buildCountryAvailability,
  cleanCountryName,
  collectCountries,
  countryKey,
  eventPathIncludes,
} from "../../shared/headline-watch-utils.js";

test("country names are normalized through the shared headline watch helpers", () => {
  assert.equal(cleanCountryName("  Viet Nam  "), "Vietnam");
  assert.equal(countryKey("Korea, Republic of"), "south korea");
  assert.equal(cleanCountryName("Brazil"), "Brazil");
});

test("country collection deduplicates and sorts by display label", () => {
  assert.deepEqual(
    collectCountries([
      {
        ner: { countries: ["Viet Nam", "Brazil", "Viet Nam"] },
      },
      {
        ner: { countries: ["Iran, Islamic Republic of", "Brazil"] },
      },
    ]),
    [
      { key: "brazil", label: "Brazil" },
      { key: "iran", label: "Iran" },
      { key: "vietnam", label: "Vietnam" },
    ]
  );
});

test("country availability counts only included articles", () => {
  const counts = buildCountryAvailability(
    [
      { id: "1", ner: { countries: ["Brazil", "Iran, Islamic Republic of"] } },
      { id: "2", ner: { countries: ["Brazil"] } },
      { id: "3", ner: { countries: ["Vietnam"] } },
    ],
    (article) => article.id !== "2"
  );

  assert.equal(counts.get("brazil"), 1);
  assert.equal(counts.get("iran"), 1);
  assert.equal(counts.get("vietnam"), 1);
});

test("set equality and event path helpers remain stable", () => {
  assert.equal(areSetsEqual(new Set(["a", "b"]), new Set(["b", "a"])), true);
  assert.equal(areSetsEqual(new Set(["a"]), new Set(["a", "b"])), false);

  const target = {
    contains(node) {
      return node === this;
    },
  };

  assert.equal(
    eventPathIncludes(
      {
        composedPath() {
          return [target];
        },
        target: {},
      },
      target
    ),
    true
  );
  assert.equal(eventPathIncludes({ target }, target), true);
  assert.equal(eventPathIncludes(null, target), false);
});
