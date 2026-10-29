import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

if (process.platform !== "darwin") {
  process.exit(0);
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = resolve(scriptDir, "..");
const tauriConfig = JSON.parse(readFileSync(join(desktopRoot, "src-tauri", "tauri.conf.json"), "utf8"));
const productName = String(process.env.PRISM_BUILD_PRODUCT_NAME || tauriConfig.productName || "Prism Hub").trim();
const appPath = join(desktopRoot, "src-tauri", "target", "release", "bundle", "macos", `${productName}.app`);

if (!existsSync(appPath)) {
  console.error(`macOS app bundle was not produced: ${appPath}`);
  process.exit(1);
}

const verify = spawnSync("codesign", ["--verify", "--deep", "--strict", "--verbose=4", appPath], {
  stdio: "inherit",
});
if (verify.status !== 0) {
  process.exit(verify.status ?? 1);
}

console.log(`verified macOS app bundle: ${appPath}`);
