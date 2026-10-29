import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const allowedEnvironments = new Set(["dev", "test", "prod"]);
const regions = ["cn", "global"];
const desktopRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

export function desktopProductName(environment) {
  switch (environment) {
    case "dev":
      return "Prism Hub Dev";
    case "test":
      return "Prism Hub Test";
    case "prod":
      return "Prism Hub";
    default:
      throw new Error(`unsupported deployment environment: ${environment}`);
  }
}

export function deploymentEnvironment(argv = process.argv.slice(2)) {
  const index = argv.indexOf("--environment");
  const value = index >= 0 ? argv[index + 1] : "";
  const environment = String(value ?? "").trim().toLowerCase();
  if (!allowedEnvironments.has(environment)) {
    throw new Error("missing or invalid --environment; use dev, test, or prod");
  }
  return environment;
}

export function loadDeployment(environment) {
  const configuredPath = process.env.PRISM_DEPLOYMENT_CONFIG?.trim();
  const path = configuredPath || join(desktopRoot, "config", "deployments.local.properties");
  if (!existsSync(path)) {
    throw new Error(`deployment configuration not found: ${path}; set PRISM_DEPLOYMENT_CONFIG to override it`);
  }
  const values = parseProperties(readFileSync(path, "utf8"));
  const deployment = { environment, regions: {} };
  for (const region of regions) {
    const prefix = `${environment}.${region}.`;
    const entry = {
      apiURL: value(values, `${prefix}api_url`, path),
      realtimeURL: value(values, `${prefix}realtime_url`, path),
      panelURL: value(values, `${prefix}panel_url`, path),
    };
    requireURL(entry.apiURL, "https:", `${prefix}api_url`);
    requireURL(entry.realtimeURL, "wss:", `${prefix}realtime_url`);
    requireURL(entry.panelURL, "https:", `${prefix}panel_url`);
    deployment.regions[region] = entry;
  }
  return deployment;
}

export function deploymentBuildEnvironment(deployment) {
  return {
    PRISM_BUILD_ENVIRONMENT: deployment.environment,
    PRISM_BUILD_PRODUCT_NAME: desktopProductName(deployment.environment),
    PRISM_BUILD_API_URL_CN: deployment.regions.cn.apiURL,
    PRISM_BUILD_REALTIME_URL_CN: deployment.regions.cn.realtimeURL,
    PRISM_BUILD_API_URL_GLOBAL: deployment.regions.global.apiURL,
    PRISM_BUILD_REALTIME_URL_GLOBAL: deployment.regions.global.realtimeURL,
    PRISM_BUILD_PANEL_URL: deployment.regions.cn.panelURL,
  };
}

function parseProperties(contents) {
  const values = new Map();
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const separator = line.indexOf("=");
    if (separator <= 0) throw new Error(`invalid deployment property: ${rawLine}`);
    values.set(line.slice(0, separator).trim(), line.slice(separator + 1).trim());
  }
  return values;
}

function value(values, key, path) {
  const result = values.get(key)?.trim();
  if (!result) throw new Error(`missing ${key} in ${path}`);
  return result;
}

function requireURL(raw, protocol, key) {
  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`${key} is not a URL`);
  }
  if (url.protocol !== protocol || !url.hostname) {
    throw new Error(`${key} must use ${protocol}//`);
  }
}
