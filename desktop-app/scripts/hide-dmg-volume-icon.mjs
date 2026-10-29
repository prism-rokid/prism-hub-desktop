import { existsSync, mkdtempSync, readFileSync, renameSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

if (process.platform !== "darwin") {
  process.exit(0);
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = join(scriptDir, "..");
const tauriConfig = JSON.parse(readFileSync(join(desktopRoot, "src-tauri", "tauri.conf.json"), "utf8"));
const productName = String(process.env.PRISM_BUILD_PRODUCT_NAME || tauriConfig.productName || "Prism Hub").trim();
const version = String(tauriConfig.version || "").trim();
const arch = process.arch === "arm64" ? "aarch64" : process.arch;
const dmgPath = join(desktopRoot, "src-tauri", "target", "release", "bundle", "dmg", `${productName}_${version}_${arch}.dmg`);

if (!existsSync(dmgPath)) {
  console.error(`macOS DMG was not produced: ${dmgPath}`);
  process.exit(1);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: "utf8", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    process.stderr.write(result.stderr || result.stdout || "");
    process.exit(result.status ?? 1);
  }
  return result.stdout;
}

const workDir = mkdtempSync(join(tmpdir(), "prism-hub-dmg-"));
const writableDmg = join(workDir, "writable.dmg");
const repackedDmg = join(workDir, "repacked.dmg");
let deviceName = "";

try {
  run("hdiutil", ["convert", dmgPath, "-format", "UDRW", "-o", writableDmg]);
  const attachment = run("hdiutil", ["attach", writableDmg, "-readwrite", "-noverify", "-noautoopen", "-nobrowse"]);
  const mountLine = attachment.split("\n").find((line) => line.includes("/Volumes/"));
  const mountPath = mountLine?.slice(mountLine.indexOf("/Volumes/"));
  const deviceMatch = attachment.match(/^\/dev\/disk\d+/m);

  if (!mountPath || !deviceMatch) {
    throw new Error(`Unable to identify mounted DMG volume: ${attachment}`);
  }

  deviceName = deviceMatch[0];
  const volumeIcon = join(mountPath, ".VolumeIcon.icns");
  if (!existsSync(volumeIcon)) {
    // Repacked signed images intentionally contain only the app and the
    // Applications alias. Absence of an optional volume icon must not turn a
    // valid distributable DMG into a failed build.
    run("hdiutil", ["detach", deviceName]);
    deviceName = "";
    console.log(`DMG volume icon is absent; skipping icon hide: ${dmgPath}`);
  } else {
    run("SetFile", ["-a", "V", volumeIcon]);
    if (run("GetFileInfo", ["-aV", volumeIcon]).trim() !== "1") {
      throw new Error("Failed to mark .VolumeIcon.icns as hidden");
    }

    run("hdiutil", ["detach", deviceName]);
    deviceName = "";
    run("hdiutil", ["convert", writableDmg, "-format", "UDZO", "-o", repackedDmg]);
    renameSync(repackedDmg, dmgPath);
    console.log(`hid DMG volume icon: ${dmgPath}`);
  }
} finally {
  if (deviceName) {
    spawnSync("hdiutil", ["detach", deviceName], { stdio: "ignore" });
  }
  rmSync(workDir, { recursive: true, force: true });
}
