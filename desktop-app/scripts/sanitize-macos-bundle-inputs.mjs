import { existsSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

if (process.platform !== "darwin") {
  process.exit(0);
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = resolve(scriptDir, "..");
const releaseRoot = join(desktopRoot, "src-tauri", "target", "release");
const resourcesRoot = join(desktopRoot, "resources");
const productName = String(process.env.PRISM_BUILD_PRODUCT_NAME || "Prism Hub").trim();
const appPath = join(releaseRoot, "bundle", "macos", `${productName}.app`);

// This hook runs after Cargo finishes and immediately before Tauri bundles.
// Remove the stale generated bundle so its inherited xattrs cannot block signing.
rmSync(appPath, { recursive: true, force: true });

for (const path of [releaseRoot, resourcesRoot]) {
  if (!existsSync(path)) continue;
  for (const attribute of ["com.apple.provenance", "com.apple.quarantine"]) {
    const clean = spawnSync("xattr", ["-dr", attribute, path], { stdio: "inherit" });
    if (clean.status !== 0) process.exit(clean.status ?? 1);
  }
}
