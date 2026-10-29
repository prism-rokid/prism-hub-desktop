import { existsSync, mkdirSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { deploymentBuildEnvironment, loadDeployment } from "./deployment-config.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = resolve(scriptDir, "..");
const repoRoot = resolve(desktopRoot, "..");
const resourcesRoot = resolve(desktopRoot, "resources");
const outputDir = join(resourcesRoot, "bin");
const binaryName = process.platform === "win32" ? "prism-sidecar.exe" : "prism-sidecar";
const outputPath = join(outputDir, binaryName);
const legacyBinaryName = process.platform === "win32" ? "prism.exe" : "prism";
const legacyOutputPath = join(outputDir, legacyBinaryName);
const hubVersion = String(JSON.parse(readFileSync(join(desktopRoot, "src-tauri", "tauri.conf.json"), "utf8")).version || "").trim();
if (!/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(hubVersion)) {
  throw new Error(`Invalid Prism Hub version in tauri.conf.json: ${hubVersion}`);
}

const buildEnvironmentKeys = [
  "PRISM_BUILD_ENVIRONMENT",
  "PRISM_BUILD_API_URL_CN",
  "PRISM_BUILD_REALTIME_URL_CN",
  "PRISM_BUILD_API_URL_GLOBAL",
  "PRISM_BUILD_REALTIME_URL_GLOBAL",
];
const inheritedBuildEnvironment = Object.fromEntries(buildEnvironmentKeys.map((key) => [key, process.env[key]?.trim() || ""]));
const requestedEnvironment = process.env.PRISM_BUILD_ENVIRONMENT?.trim().toLowerCase();
if (!requestedEnvironment || !["dev", "test", "prod"].includes(requestedEnvironment)) {
  throw new Error("PRISM_BUILD_ENVIRONMENT must be explicitly set to dev, test, or prod before preparing the sidecar");
}
const buildEnvironment = buildEnvironmentKeys.every((key) => inheritedBuildEnvironment[key])
  ? inheritedBuildEnvironment
  : deploymentBuildEnvironment(loadDeployment(requestedEnvironment));

mkdirSync(outputDir, { recursive: true });
rmSync(legacyOutputPath, { force: true });
rmSync(join(resourcesRoot, ".DS_Store"), { force: true });
for (const stale of ["plugins", "sdk", "runtime"]) {
  rmSync(join(resourcesRoot, stale), { recursive: true, force: true });
}

const linkerValues = [
  ["HubVersion", hubVersion],
  ["DeploymentEnvironment", buildEnvironment.PRISM_BUILD_ENVIRONMENT],
  ["APIURLCN", buildEnvironment.PRISM_BUILD_API_URL_CN],
  ["RealtimeURLCN", buildEnvironment.PRISM_BUILD_REALTIME_URL_CN],
  ["APIURLGlobal", buildEnvironment.PRISM_BUILD_API_URL_GLOBAL],
  ["RealtimeURLGlobal", buildEnvironment.PRISM_BUILD_REALTIME_URL_GLOBAL],
];
const ldflags = linkerValues.map(([key, value]) => `-X rokid-prism-hub/internal/buildinfo.${key}=${value}`).join(" ");
const build = spawnSync("go", ["build", "-trimpath", "-ldflags", ldflags, "-o", outputPath, "./cmd/prism"], { cwd: repoRoot, stdio: "inherit" });
if (build.status !== 0 || !existsSync(outputPath)) process.exit(build.status ?? 1);

if (process.platform === "darwin") {
  for (const attribute of ["com.apple.provenance", "com.apple.quarantine"]) {
    const clean = spawnSync("xattr", ["-dr", attribute, resourcesRoot], { stdio: "inherit" });
    if (clean.status !== 0) process.exit(clean.status ?? 1);
  }
}

console.log(`prepared Prism Hub sidecar without bundled plugins or runtimes: ${outputPath}`);
console.log(`Prism Hub build environment: ${buildEnvironment.PRISM_BUILD_ENVIRONMENT}`);
