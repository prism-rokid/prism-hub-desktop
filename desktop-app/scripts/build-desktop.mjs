import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { deploymentBuildEnvironment, deploymentEnvironment, desktopProductName, loadDeployment } from "./deployment-config.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = join(scriptDir, "..");
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
// 通过 Node 调用项目本地 CLI，不依赖 tauri.cmd。
// Windows 上全局安装的 tauri.cmd 可能在子进程 shell 中优先匹配，
// 导致无子命令运行并让工作流误判成功，却没有生成安装包。
const tauri = process.platform === "win32" ? process.execPath : "tauri";
const tauriPrefixArgs = process.platform === "win32"
  ? [join(desktopRoot, "node_modules", "@tauri-apps", "cli", "tauri.js")]
  : [];

function run(command, args, env, options = {}) {
  const result = spawnSync(command, args, {
    cwd: desktopRoot,
    stdio: "inherit",
    env,
    // npm.cmd and tauri.cmd are batch files. Node cannot launch them directly
    // on Windows, including when this script is invoked by a GitHub Action.
    // Real executables (node.exe) are spawned without cmd.exe: with
    // shell:true, Node only concatenates the command line, so an unquoted
    // executable path containing spaces (C:\Program Files\...) is split by
    // cmd into "C:\Program" and fails with "'C:\Program' is not recognized".
    shell: options.noShell ? false : process.platform === "win32",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function signMacOSSidecar() {
  if (process.platform !== "darwin") return;
  const signingIdentity = String(process.env.APPLE_SIGNING_IDENTITY || "").trim();
  if (!signingIdentity || signingIdentity === "-") return;

  const sidecarPath = join(desktopRoot, "resources", "bin", "prism-sidecar");
  if (!existsSync(sidecarPath)) {
    throw new Error(`未找到待签名的 macOS sidecar：${sidecarPath}`);
  }

  // Go sidecar 是应用包中的独立可执行文件，必须在 Tauri 打包前使用
  // Developer ID 签名、可信时间戳和 Hardened Runtime，否则公证会拒绝整个 DMG。
  run(
    "codesign",
    ["--force", "--options", "runtime", "--timestamp", "--sign", signingIdentity, sidecarPath],
    buildEnvironment,
    { noShell: true },
  );
  const inspection = spawnSync("codesign", ["-dvv", sidecarPath], {
    encoding: "utf8",
  });
  const signingDetails = `${inspection.stdout || ""}${inspection.stderr || ""}`;
  if (
    inspection.status !== 0 ||
    !signingDetails.includes("Authority=Developer ID Application:") ||
    !/flags=.*runtime/.test(signingDetails) ||
    !signingDetails.includes("Timestamp=")
  ) {
    throw new Error(`macOS sidecar 签名校验失败：${sidecarPath}`);
  }
}

function verifyPreparedSidecar(environment, deployment, productName, generatedConfigPath) {
  const sidecarPath = join(desktopRoot, "resources", "bin", process.platform === "win32" ? "prism-sidecar.exe" : "prism-sidecar");
  if (!existsSync(sidecarPath)) {
    throw new Error(`未找到已构建的 Hub sidecar：${sidecarPath}`);
  }
  const bytes = readFileSync(sidecarPath);
  const expectedEndpoints = [
    deployment.regions.cn.apiURL,
    deployment.regions.cn.realtimeURL,
    deployment.regions.global.apiURL,
    deployment.regions.global.realtimeURL,
  ];
  for (const endpoint of expectedEndpoints) {
    if (!bytes.includes(Buffer.from(endpoint))) {
      throw new Error(`Hub sidecar 缺少 ${environment} 环境 endpoint，拒绝继续打包：${endpoint}`);
    }
  }
  const generatedConfig = JSON.parse(readFileSync(generatedConfigPath, "utf8"));
  if (generatedConfig.productName !== productName) {
    throw new Error(`Desktop productName 与 ${environment} 环境不一致：${generatedConfig.productName}`);
  }
  console.log(`verified Prism Hub package environment: ${environment} (${productName})`);
}

const environment = deploymentEnvironment();
const deployment = loadDeployment(environment);
const buildEnvironment = { ...process.env, ...deploymentBuildEnvironment(deployment) };
const generatedConfigDir = join(desktopRoot, "src-tauri", "target", "deployment-config");
const generatedConfigPath = join(generatedConfigDir, `tauri.${environment}.conf.json`);
const productName = desktopProductName(environment);

// Keep endpoint data out of the Tauri configuration. This generated overlay
// only makes non-production Desktop bundles visibly distinguishable.
const tauriConfig = JSON.parse(readFileSync(join(desktopRoot, "src-tauri", "tauri.conf.json"), "utf8"));
// 本机构建永远不启用更新；仅 GitHub 的显式发布构建生成更新配置和签名制品。
const createUpdaterArtifacts = process.env.GITHUB_ACTIONS === "true" && process.env.PRISM_ENABLE_UPDATER === "1";
buildEnvironment.PRISM_BUILD_UPDATER_ENABLED = createUpdaterArtifacts ? "1" : "0";
const updaterPublicKey = String(process.env.TAURI_UPDATER_PUBKEY || "").trim();
if (createUpdaterArtifacts && !updaterPublicKey) {
  throw new Error("GitHub 更新构建缺少 TAURI_UPDATER_PUBKEY");
}
tauriConfig.bundle = {
  ...tauriConfig.bundle,
  createUpdaterArtifacts,
};
tauriConfig.productName = productName;
tauriConfig.app.windows = tauriConfig.app.windows.map((window) => ({
  ...window,
  title: productName,
}));
if (createUpdaterArtifacts) {
  tauriConfig.plugins = {
    ...(tauriConfig.plugins || {}),
    updater: {
      pubkey: updaterPublicKey,
      endpoints: [
        `${deployment.regions.cn.apiURL.replace(/\/$/, "")}/public/releases/current?channel=${environment}&format=tauri`,
      ],
      windows: {
        installMode: "passive",
      },
    },
  };
} else if (tauriConfig.plugins?.updater) {
  delete tauriConfig.plugins.updater;
}
if (process.platform === "darwin") {
  const generatedIconsDir = join(generatedConfigDir, "icons");
  const sourceIconsDir = join(desktopRoot, "src-tauri", "icons");
  mkdirSync(generatedIconsDir, { recursive: true });
  tauriConfig.bundle.icon = tauriConfig.bundle.icon.map((iconPath) => {
    const name = iconPath.split("/").at(-1);
    const generatedIcon = join(generatedIconsDir, name);
    // Rewriting the bytes creates a clean generated file without source xattrs.
    writeFileSync(generatedIcon, readFileSync(join(sourceIconsDir, name)));
    return generatedIcon;
  });
}
tauriConfig.bundle.macOS = {
  ...(tauriConfig.bundle.macOS || {}),
  // Sign before Tauri creates the DMG so its default Finder layout remains
  // intact. A local ad-hoc identity keeps nested executables verifiable.
  signingIdentity: String(process.env.APPLE_SIGNING_IDENTITY || "-").trim() || "-",
};
tauriConfig.build.beforeBundleCommand = "node ./scripts/sanitize-macos-bundle-inputs.mjs";
mkdirSync(generatedConfigDir, { recursive: true });
writeFileSync(generatedConfigPath, `${JSON.stringify(tauriConfig, null, 2)}\n`);

// 在同一父进程中执行构建阶段，确保 sidecar 在 Tauri 创建应用包前已完成签名。
run(npm, ["run", "prepare:sidecar"], buildEnvironment);
verifyPreparedSidecar(environment, deployment, productName, generatedConfigPath);
signMacOSSidecar();
const tauriArgs = ["build", "--config", generatedConfigPath];
if (process.env.PRISM_TAURI_VERBOSE === "1") {
  tauriArgs.push("--verbose");
}
// Local dev/test installers do not need an ad-hoc signature. CI and production
// builds keep their configured signing behaviour unless explicitly overridden.
const skipCodesign = process.env.PRISM_SKIP_CODESIGN === "1" ||
  (!process.env.CI && environment !== "prod");
if (skipCodesign) {
  tauriArgs.push("--no-sign");
}
const bundles = process.env.PRISM_BUNDLES?.trim();
if (bundles) {
  tauriArgs.push("--bundles", bundles);
}
run(tauri, [...tauriPrefixArgs, ...tauriArgs], buildEnvironment, { noShell: process.platform === "win32" });
if (!bundles || bundles.split(",").map((item) => item.trim()).includes("dmg")) {
  run(process.execPath, ["./scripts/hide-dmg-volume-icon.mjs"], buildEnvironment, { noShell: true });
}
