import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { deploymentBuildEnvironment, deploymentEnvironment, loadDeployment } from "./deployment-config.mjs";

const desktopRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const environment = deploymentEnvironment();
const deployment = loadDeployment(environment);
const tauri = process.platform === "win32" ? "tauri.cmd" : "tauri";
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const buildEnvironment = { ...process.env, ...deploymentBuildEnvironment(deployment) };
const run = (command, args) => spawnSync(command, args, {
  cwd: desktopRoot,
  stdio: "inherit",
  env: buildEnvironment,
  shell: process.platform === "win32",
});
const prepare = run(npm, ["run", "prepare:sidecar"]);
if (prepare.error) throw prepare.error;
if (prepare.status !== 0) process.exit(prepare.status ?? 1);
const result = run(tauri, ["dev"]);
if (result.error) throw result.error;
process.exit(result.status ?? 1);
