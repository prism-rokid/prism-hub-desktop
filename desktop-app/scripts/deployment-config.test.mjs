import assert from "node:assert/strict";
import test from "node:test";
import { deploymentEnvironment, desktopProductName } from "./deployment-config.mjs";

test("the online deployment is the default", () => {
  assert.equal(deploymentEnvironment([]), "prod");
  assert.throws(() => deploymentEnvironment(["--environment", "staging"]), /missing or invalid/);
  assert.throws(() => deploymentEnvironment(["--environment"]), /missing or invalid/);
});

test("environment controls the visible desktop product name", () => {
  assert.equal(deploymentEnvironment(["--environment", "test"]), "test");
  assert.equal(desktopProductName("dev"), "Prism Hub Dev");
  assert.equal(desktopProductName("test"), "Prism Hub Test");
  assert.equal(desktopProductName("prod"), "Prism Hub");
});
