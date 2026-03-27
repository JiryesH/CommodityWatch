import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import test from "node:test";
import assert from "node:assert/strict";

function createClassList() {
  const classes = new Set();

  return {
    toggle(token, force) {
      const nextState = typeof force === "boolean" ? force : !classes.has(token);
      if (nextState) {
        classes.add(token);
      } else {
        classes.delete(token);
      }
      return nextState;
    },
    contains(token) {
      return classes.has(token);
    },
  };
}

function createElement() {
  const listeners = new Map();

  return {
    hidden: false,
    classList: createClassList(),
    attributes: {},
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || new Set();
      handlers.add(handler);
      listeners.set(type, handlers);
    },
    removeEventListener(type, handler) {
      listeners.get(type)?.delete(handler);
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    dispatchEvent(type, event = {}) {
      listeners.get(type)?.forEach((handler) => handler({ target: this, ...event }));
    },
    listenerCount(type) {
      return listeners.get(type)?.size || 0;
    },
    contains(target) {
      return target === this;
    },
  };
}

function createDocument() {
  const listeners = new Map();

  return {
    listeners,
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || new Set();
      handlers.add(handler);
      listeners.set(type, handlers);
    },
    removeEventListener(type, handler) {
      listeners.get(type)?.delete(handler);
    },
  };
}

function loadFilterShell(windowObject) {
  const sourcePath = fileURLToPath(new URL("../../shared/filter-shell.js", import.meta.url));
  const source = readFileSync(sourcePath, "utf8");
  vm.runInNewContext(source, windowObject, { filename: sourcePath });
}

test("filter shell binding stays idempotent across repeated initialization", () => {
  const document = createDocument();
  const windowObject = {
    document,
  };
  windowObject.window = windowObject;

  loadFilterShell(windowObject);

  const shell = createElement();
  const toggle = createElement();
  const layer = createElement();

  const firstBinding = windowObject.CommodityWatchFilterShell.bindExclusiveLayers({
    shell,
    items: [{ toggle, layer }],
  });

  assert.equal(document.listeners.get("click").size, 1);
  assert.equal(document.listeners.get("keydown").size, 1);
  assert.equal(toggle.listenerCount("click"), 1);

  const secondBinding = windowObject.CommodityWatchFilterShell.bindExclusiveLayers({
    shell,
    items: [{ toggle, layer }],
  });

  assert.equal(shell.__commodityWatchFilterShellBinding, secondBinding);
  assert.equal(document.listeners.get("click").size, 1);
  assert.equal(document.listeners.get("keydown").size, 1);
  assert.equal(toggle.listenerCount("click"), 1);

  toggle.dispatchEvent("click");
  assert.equal(layer.hidden, false);
  assert.equal(layer.attributes["aria-hidden"], "false");

  secondBinding.destroy();
  assert.equal(document.listeners.get("click").size, 0);
  assert.equal(document.listeners.get("keydown").size, 0);
  assert.equal(toggle.listenerCount("click"), 0);
  assert.equal(shell.__commodityWatchFilterShellBinding, undefined);

  firstBinding.destroy();
});
