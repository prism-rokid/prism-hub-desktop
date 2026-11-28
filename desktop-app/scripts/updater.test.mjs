import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import { runInNewContext } from "node:vm";

function read(relativePath) {
  return readFileSync(new URL(relativePath, import.meta.url), "utf8");
}

const shellSource = read("../frontend/app.js");
const shellHTML = read("../frontend/index.html");
const dashboardHTML = read("../../cmd/prism/dashboard_assets/index.html");
const origin = "http://127.0.0.1:17171";

function dashboardScript(start, end) {
  const from = dashboardHTML.indexOf(start);
  const to = dashboardHTML.indexOf(end, from);
  assert.ok(from >= 0 && to > from, "Dashboard updater code is present");
  return dashboardHTML.slice(from, to);
}

// Run the Dashboard's real updater state and click handlers without starting
// its unrelated services. Mock the DOM, iframe bridge, and native commands.
const dashboardSource = [
  dashboardScript("const desktopUpdate =", "function setLocale("),
  dashboardScript(
    'document.getElementById("desktop-update-btn").addEventListener',
    'document.getElementById("open-changelog-btn").addEventListener',
  ),
].join("\n");

function eventTarget() {
  const listeners = new Map();
  return {
    addEventListener(name, listener) {
      const callbacks = listeners.get(name) || [];
      callbacks.push(listener);
      listeners.set(name, callbacks);
    },
    dispatch(name, event) {
      for (const listener of listeners.get(name) || []) void listener(event);
    },
  };
}

function documentFrom(html) {
  const elements = new Map();
  for (const [tag, id] of html.matchAll(/<[^>]* id="([^"]+)"[^>]*>/g)) {
    const attributes = new Map([...tag.matchAll(/([\w-]+)="([^"]*)"/g)].map(([, key, value]) => [key, value]));
    elements.set(id, {
      ...eventTarget(),
      disabled: false,
      hidden: /\shidden(?=[\s>])/.test(tag),
      textContent: "",
      style: {},
      dataset: {},
      classList: { toggle() {} },
      setAttribute(name, value) { attributes.set(name, value); },
      getAttribute(name) { return attributes.get(name) ?? null; },
    });
  }
  return {
    documentElement: { dataset: {} },
    getElementById(id) { return elements.get(id) ?? null; },
  };
}

function harness() {
  const shellDocument = documentFrom(shellHTML);
  const dashboardDocument = documentFrom(dashboardHTML);
  const shellWindow = eventTarget();
  const dashboardWindow = eventTarget();
  const shell = id => shellDocument.getElementById(id);
  const dashboard = id => dashboardDocument.getElementById(id);
  const timers = new Map();
  const nativeEvents = new Map();
  const commands = [];
  let pauseDashboardUpdateState = false;
  let timerId = 0;
  let periodicCheck;
  let currentVersion = "0.1.22";

  const parent = {
    postMessage(data) {
      shellWindow.dispatch("message", { source: frame, origin, data });
    },
  };
  const frame = {
    postMessage(data, targetOrigin) {
      assert.equal(targetOrigin, origin);
      if (pauseDashboardUpdateState && data.type === "prism:update-state") return;
      dashboardWindow.dispatch("message", { source: parent, data });
    },
  };
  dashboardWindow.parent = parent;
  shell("dashboard").contentWindow = frame;

  const emit = (name, payload) => nativeEvents.get(name)?.({ payload });
  const handlers = {
    check_for_update() { return currentVersion ? { version: currentVersion } : null; },
    install_update() {
      if (!currentVersion) return false;
      emit("prism://update-version", currentVersion);
      return true;
    },
  };

  Object.assign(shellWindow, {
    location: { origin: "tauri://localhost" },
    localStorage: { getItem() {}, setItem() {} },
    __TAURI__: {
      core: {
        async invoke(command, args) {
          commands.push({ command, args });
          return handlers[command]?.(args);
        },
      },
      event: { listen(name, callback) { nativeEvents.set(name, callback); } },
    },
    setTimeout(callback, ms) {
      timers.set(++timerId, { callback, ms });
      return timerId;
    },
    clearTimeout(id) { timers.delete(id); },
    setInterval(callback) { periodicCheck = callback; },
  });

  runInNewContext(dashboardSource, {
    window: dashboardWindow,
    document: dashboardDocument,
    t: key => key,
    closeDrawer() {},
  });
  runInNewContext(shellSource, {
    window: shellWindow,
    document: shellDocument,
    navigator: { languages: ["en-US"] },
    URL,
    console: { warn() {}, error() {} },
  });

  async function click(element) {
    assert.equal(element.hidden || element.disabled, false, "update button is actionable");
    element.dispatch("click");
    await setImmediate();
  }

  return {
    shell, dashboard, handlers, emit,
    publish(version) { currentVersion = version; },
    pauseDashboardUpdates() { pauseDashboardUpdateState = true; },
    calls(command) { return commands.filter(call => call.command === command).length; },
    menuVersion() { return commands.findLast(call => call.command === "set_update_menu_state").args.version; },
    nativeMenu() { shellWindow.__prismCheckForUpdate(); },
    clickDashboard(id) { return click(dashboard(id)); },
    clickShell(id) { return click(shell(id)); },
    async startup() {
      const [id, timer] = [...timers.entries()].find(([, item]) => item.ms === 3000);
      timers.delete(id);
      timer.callback();
      await setImmediate();
    },
    async periodic() {
      periodicCheck();
      await setImmediate();
    },
  };
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((success, failure) => { resolve = success; reject = failure; });
  return { promise, resolve, reject };
}

function assertToast(h, tone) {
  assert.equal(h.shell("update-toast").getAttribute("aria-hidden"), "false");
  assert.equal(h.shell("update-toast").dataset.tone, tone);
}

test("top bar shows the refreshed release and its download progress", async () => {
  const h = harness();
  await h.startup();
  assert.equal(h.menuVersion(), "0.1.22");
  h.publish("0.1.24");
  const install = h.handlers.install_update;
  const result = deferred();
  h.handlers.install_update = () => {
    install();
    return result.promise;
  };

  await h.clickDashboard("desktop-update-btn");
  assert.equal(h.dashboard("desktop-update-btn").disabled, true);
  h.emit("prism://update-progress", { downloaded: 1024, total: 2048 });
  assert.equal(h.dashboard("update-badge").textContent, "50%");
  assert.match(h.shell("update-detail").textContent, /0\.1\.24/);

  result.resolve(true);
  await setImmediate();
  assert.equal(h.calls("plugin:process|restart"), 1);
  assert.equal(h.shell("update-toast").getAttribute("aria-hidden"), "true");
});

for (const [entry, id] of [["top bar", "desktop-update-btn"], ["drawer", "check-update-btn"]]) {
  test(entry + " surfaces update failure and allows another install attempt", async () => {
    const h = harness();
    const install = h.handlers.install_update;
    h.handlers.install_update = () => { throw new Error("signature verification failed"); };
    await h.startup();
    h.publish("0.1.24");

    await h.clickDashboard(id);
    assertToast(h, "err");
    assert.equal(h.calls("plugin:process|restart"), 0);
    h.handlers.install_update = install;
    await h.clickDashboard(id);
    assert.equal(h.calls("install_update"), 2);
    assert.equal(h.calls("plugin:process|restart"), 1);
  });
}

test("manual check shares the startup request and reports no update", async () => {
  const h = harness();
  const result = deferred();
  h.handlers.check_for_update = () => result.promise;
  await h.startup();
  h.nativeMenu();
  assert.equal(h.calls("check_for_update"), 1);
  result.resolve(null);
  await setImmediate();
  assertToast(h, "ok");
});

test("manual drawer check surfaces an existing background check's error", async () => {
  const h = harness();
  const result = deferred();
  h.handlers.check_for_update = () => result.promise;
  await h.startup();
  await h.clickDashboard("check-update-btn");
  assert.equal(h.calls("check_for_update"), 1);
  result.reject(new Error("update feed unavailable"));
  await setImmediate();
  assertToast(h, "err");
});

test("periodic checks refresh a previously detected release", async () => {
  const h = harness();
  await h.startup();
  h.publish("0.1.24");
  await h.periodic();
  assert.equal(h.calls("check_for_update"), 2);
  assert.equal(h.menuVersion(), "0.1.24");
});

test("native dialog cannot start a second update during a top bar install", async () => {
  const h = harness();
  const result = deferred();
  h.handlers.install_update = () => result.promise;
  await h.startup();
  await h.clickDashboard("desktop-update-btn");
  h.nativeMenu();
  assert.equal(h.shell("update-install").disabled, true);
  assert.equal(h.calls("install_update"), 1);
  result.resolve(true);
  await setImmediate();
  assert.equal(h.calls("plugin:process|restart"), 1);
});

test("withdrawn release clears the offer without restarting", async () => {
  const h = harness();
  await h.startup();
  h.publish(null);
  await h.clickDashboard("desktop-update-btn");
  assert.equal(h.dashboard("desktop-update-btn").hidden, true);
  assert.equal(h.menuVersion(), null);
  assert.equal(h.calls("plugin:process|restart"), 0);
  assertToast(h, "ok");
});

test("clicking a stale Dashboard offer after withdrawal reports the check result", async () => {
  const h = harness();
  await h.startup();
  h.pauseDashboardUpdates();
  h.publish(null);
  await h.periodic();

  assert.equal(h.menuVersion(), null);
  assert.equal(h.dashboard("desktop-update-btn").hidden, false);
  await h.clickDashboard("desktop-update-btn");
  assert.equal(h.calls("check_for_update"), 3);
  assert.equal(h.calls("install_update"), 0);
  assertToast(h, "ok");
});

test("restart failure clears the offer and a late manifest cannot reinstate it", async () => {
  const h = harness();
  await h.startup();
  const earlierCheck = deferred();
  h.handlers.check_for_update = () => earlierCheck.promise;
  await h.periodic();
  h.handlers["plugin:process|restart"] = () => { throw new Error("restart unavailable"); };

  await h.clickDashboard("desktop-update-btn");
  assertToast(h, "ok");
  assert.match(h.shell("update-toast-detail").textContent, /quit and reopen/i);
  earlierCheck.resolve({ version: "0.1.22" });
  await setImmediate();
  assert.equal(h.menuVersion(), null);
  assert.equal(h.dashboard("desktop-update-btn").hidden, true);
});

test("native update dialog keeps its failure visible and enables retry", async () => {
  const h = harness();
  const install = h.handlers.install_update;
  h.handlers.install_update = () => { throw new Error("download failed"); };
  await h.startup();
  h.nativeMenu();
  await h.clickShell("update-install");

  assert.equal(h.shell("update-dialog").getAttribute("aria-hidden"), "false");
  assert.match(h.shell("update-detail").textContent, /did not complete/i);
  assert.equal(h.calls("plugin:process|restart"), 0);
  h.handlers.install_update = install;
  await h.clickShell("update-install");
  assert.equal(h.calls("plugin:process|restart"), 1);
});

test("a late manifest cannot replace the version displayed during installation", async () => {
  const h = harness();
  await h.startup();
  const earlierCheck = deferred();
  h.handlers.check_for_update = () => earlierCheck.promise;
  await h.periodic();
  h.publish("0.1.24");
  const install = h.handlers.install_update;
  const result = deferred();
  h.handlers.install_update = () => {
    install();
    return result.promise;
  };

  await h.clickDashboard("desktop-update-btn");
  earlierCheck.resolve({ version: "0.1.23" });
  await setImmediate();
  assert.match(h.shell("update-detail").textContent, /0\.1\.24/);
  assert.equal(h.menuVersion(), "0.1.24");
  result.resolve(true);
  await setImmediate();
});
