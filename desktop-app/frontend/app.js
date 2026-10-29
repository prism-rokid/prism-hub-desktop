const hubURL = "http://127.0.0.1:17171/";
const dashboard = document.getElementById("dashboard");
const updateDialog = document.getElementById("update-dialog");
const updateDetail = document.getElementById("update-detail");
const updateInstall = document.getElementById("update-install");
const updateLater = document.getElementById("update-later");
const updateToast = document.getElementById("update-toast");
const updateTitle = document.getElementById("update-title");
const updateToastIcon = document.getElementById("update-toast-icon");
const updateToastTitle = document.getElementById("update-toast-title");
const updateToastDetail = document.getElementById("update-toast-detail");
let availableUpdate = null;
let updateCheckInFlight = false;
let updateProgress = null;
let updateToastTimer = 0;
let shellLocale = detectShellLocale();
let updateDialogState = "available";
let dashboardRestartInFlight = false;

const updateTranslations = {
  "zh-CN": {
    availableTitle: "发现新版本",
    availableDetail: "版本 {version} 已准备好。下载与校验完成后，Prism Hub 将自动重新启动。",
    installNow: "立即更新",
    installLater: "稍后更新",
    installingDetail: "正在下载并校验版本 {version}。",
    installingProgressDetail: "正在下载并校验版本 {version}（{percent}%）。",
    installFailedDetail: "更新未完成，请稍后重试。",
    latestTitle: "已是最新版本",
    latestDetail: "当前 Prism Hub 已是最新版本。",
    checkFailedTitle: "检查更新失败",
    checkFailedDetail: "暂时无法连接更新服务，请稍后重试。",
    updaterDisabledDetail: "此构建未启用自动更新。",
  },
  "en-US": {
    availableTitle: "New version available",
    availableDetail: "Version {version} is ready. Prism Hub will restart after download and verification finish.",
    installNow: "Update now",
    installLater: "Later",
    installingDetail: "Downloading and verifying version {version}.",
    installingProgressDetail: "Downloading and verifying version {version} ({percent}%).",
    installFailedDetail: "The update did not complete. Please try again later.",
    latestTitle: "You're up to date",
    latestDetail: "Prism Hub is already on the latest version.",
    checkFailedTitle: "Couldn't check for updates",
    checkFailedDetail: "The update service is unavailable. Please try again later.",
    updaterDisabledDetail: "Automatic updates are disabled in this build.",
  },
  "ja-JP": {
    availableTitle: "新しいバージョンがあります",
    availableDetail: "バージョン {version} の準備ができました。ダウンロードと検証が完了すると、Prism Hub は自動的に再起動します。",
    installNow: "今すぐ更新",
    installLater: "後で",
    installingDetail: "バージョン {version} をダウンロードして検証しています。",
    installingProgressDetail: "バージョン {version} をダウンロードして検証しています（{percent}%）。",
    installFailedDetail: "更新を完了できませんでした。しばらくしてからもう一度お試しください。",
    latestTitle: "最新の状態です",
    latestDetail: "Prism Hub はすでに最新バージョンです。",
    checkFailedTitle: "更新を確認できませんでした",
    checkFailedDetail: "更新サービスに接続できません。しばらくしてからもう一度お試しください。",
    updaterDisabledDetail: "このビルドでは自動更新が有効になっていません。",
  },
};

function detectShellLocale() {
  for (const candidate of [...(navigator.languages || []), navigator.language]) {
    const locale = String(candidate || "").toLowerCase();
    if (locale.startsWith("zh")) return "zh-CN";
    if (locale.startsWith("ja")) return "ja-JP";
  }
  return "en-US";
}

function updateText(key, params = {}) {
  const value = updateTranslations[shellLocale]?.[key] || updateTranslations["en-US"][key] || key;
  return value.replace(/\{([A-Za-z][A-Za-z0-9_]*)\}/g, (_, name) => String(params[name] ?? `{${name}}`));
}

function setShellLocale(locale) {
  shellLocale = Object.hasOwn(updateTranslations, locale) ? locale : detectShellLocale();
  document.documentElement.lang = shellLocale;
  // Keep the native tray menu and app menu in the Dashboard's language. The
  // Dashboard pushes prism:locale-change on boot and on every manual switch;
  // this initial call covers the time before the Dashboard iframe has loaded.
  window.__TAURI__?.core?.invoke("set_tray_locale", { locale: shellLocale })
    .catch((error) => console.warn("Unable to apply Prism tray locale", error));
  applyUpdateMenuState();
  if (availableUpdate && updateDialog.getAttribute("aria-hidden") === "false") renderUpdateDialog();
}

function applyShellTheme(theme) {
  const resolved = theme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = resolved;
  try { window.localStorage.setItem("prism.desktop.theme", resolved); } catch {}
  // The macOS transparent title bar reveals the native window background, so
  // keep it aligned with the dashboard canvas instead of leaving a light bar
  // above an otherwise dark interface.
  if (window.__TAURI__?.core?.invoke) {
    window.__TAURI__.core.invoke("set_window_theme", { theme: resolved })
      .catch((error) => console.warn("Unable to apply Prism window theme", error));
  }
}

try {
  applyShellTheme(
    window.localStorage.getItem("prism.desktop.theme") ||
      (window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light"),
  );
} catch {}

async function invoke(command, args) {
  if (!window.__TAURI__?.core?.invoke) {
    throw new Error("Prism Hub 桌面运行时不可用。");
  }
  return window.__TAURI__.core.invoke(command, args);
}

function dashboardURL({ reload = false } = {}) {
  if (!reload) return hubURL;
  // A restart reuses the same localhost URL. Give the WebView a distinct
  // navigation so it cannot retain the failed document from the old sidecar.
  return `${hubURL}?desktop_restart=${Date.now()}`;
}

async function startDashboard({ reload = false } = {}) {
  try {
    await invoke("ensure_hub_ready");
    dashboard.src = dashboardURL({ reload });
  } catch (error) {
    console.error("Prism Hub startup failed", error);
    window.setTimeout(() => void startDashboard({ reload }), 2000);
  }
}

// The outgoing sidecar may keep its listener for a moment after scheduling
// its own shutdown. ensure_hub_ready treats any answering process as the
// healthy one, so wait until the old listener actually drops before asking
// for the replacement; otherwise the iframe reloads a page from a process
// that is about to disappear.
async function waitForPreviousHubExit({ timeoutMs = 6000, intervalMs = 200 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      // no-cors: the sidecar serves no CORS headers, so a cors-mode fetch
      // would reject on policy even while the hub is perfectly reachable.
      // The opaque response is enough — resolve means listening, reject
      // means the old listener is finally gone.
      await fetch(hubURL, { mode: "no-cors", cache: "no-store" });
    } catch {
      return;
    }
    await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
  }
}

async function restartDashboardAfterHubRestart() {
  if (dashboardRestartInFlight) return;
  dashboardRestartInFlight = true;
  try {
    // The sidecar schedules its own shutdown shortly after sending the
    // restart response. Waiting prevents ensure_hub_ready from observing
    // that old, still-healthy process and returning before the restart has
    // happened.
    await waitForPreviousHubExit();
    await invoke("ensure_hub_ready");
    dashboard.src = dashboardURL({ reload: true });
  } catch (error) {
    console.error("Prism Hub restart recovery failed", error);
    // Retain the same recovery behaviour as initial startup. This retry is
    // intentionally owned by the shell, so the Dashboard never turns the
    // expected restart window into a failed login message.
    window.setTimeout(() => void restartDashboardAfterHubRestart(), 2000);
  } finally {
    dashboardRestartInFlight = false;
  }
}

async function provideRemotePanelQRCode() {
  try {
    const payload = await invoke("remote_panel_qr");
    if (!payload?.svg || !dashboard.contentWindow) return;
    dashboard.contentWindow.postMessage(
      { type: "prism:remote-panel-qr", svg: payload.svg },
      new URL(hubURL).origin,
    );
  } catch (error) {
    // A missing panel URL is valid for a standalone/local build. The existing
    // button behaviour remains available and the dashboard simply omits the
    // hover affordance in that case.
    console.warn("Unable to prepare the remote panel QR code", error);
  }
}

dashboard.addEventListener("load", () => {
  void provideRemotePanelQRCode();
  // A reloaded Dashboard loses the in-page update state; re-announce it so
  // the drawer entry keeps advertising a pending update.
  notifyDashboardUpdateState();
});

function renderUpdateDialog() {
  const version = availableUpdate?.version || "";
  updateTitle.textContent = updateText("availableTitle");
  updateLater.textContent = updateText("installLater");
  updateInstall.textContent = updateText("installNow");
  const installing = updateDialogState === "installing";
  let detail = updateText("availableDetail", { version });
  if (installing) {
    const percent = updateProgress?.total
      ? Math.min(100, Math.floor((updateProgress.downloaded / updateProgress.total) * 100))
      : null;
    detail = percent === null
      ? updateText("installingDetail", { version })
      : updateText("installingProgressDetail", { version, percent });
  } else if (updateDialogState === "failed") {
    detail = updateText("installFailedDetail");
  }
  updateDetail.textContent = detail;
  const progress = document.getElementById("update-progress");
  if (progress) {
    progress.setAttribute("aria-hidden", installing ? "false" : "true");
    const fill = document.getElementById("update-progress-fill");
    if (fill) {
      const percent = updateProgress?.total
        ? Math.min(100, Math.max(2, Math.floor((updateProgress.downloaded / updateProgress.total) * 100)))
        : 0;
      fill.style.width = `${installing && updateProgress?.total ? percent : 0}%`;
    }
  }
}

function showUpdateDialog() {
  updateDialogState = "available";
  renderUpdateDialog();
  updateDialog.setAttribute("aria-hidden", "false");
}

function hideUpdateDialog() {
  updateDialog.setAttribute("aria-hidden", "true");
}

function showUpdateToast({ title, detail, tone = "err" }) {
  updateToast.dataset.tone = tone;
  updateToastIcon.textContent = tone === "ok" ? "✓" : "!";
  updateToastTitle.textContent = title;
  updateToastDetail.textContent = detail;
  updateToast.setAttribute("aria-hidden", "false");
  window.clearTimeout(updateToastTimer);
  updateToastTimer = window.setTimeout(hideUpdateToast, 5000);
}

function showLatestVersionToast() {
  showUpdateToast({
    title: updateText("latestTitle"),
    detail: updateText("latestDetail"),
    tone: "ok",
  });
}

function showUpdateCheckFailure(error) {
  const rawMessage = String(error?.message || error || "");
  const detail = /未启用自动更新|automatic updates are disabled/i.test(rawMessage)
    ? updateText("updaterDisabledDetail")
    : updateText("checkFailedDetail");
  showUpdateToast({ title: updateText("checkFailedTitle"), detail });
}

function hideUpdateToast() {
  window.clearTimeout(updateToastTimer);
  updateToastTimer = 0;
  updateToast.setAttribute("aria-hidden", "true");
}

async function checkForDesktopUpdate({ manual = false } = {}) {
  if (updateCheckInFlight || availableUpdate || !window.__TAURI__?.core?.invoke) return;
  updateCheckInFlight = true;
  try {
    const update = await invoke("check_for_update");
    if (update) {
      availableUpdate = update;
      applyUpdateMenuState();
      notifyDashboardUpdateState();
    } else if (manual) {
      showLatestVersionToast();
    }
  } catch (error) {
    if (manual) {
      showUpdateCheckFailure(error);
    } else {
      // An unavailable release service must never prevent the local Hub from
      // starting. The next periodic check will retry automatically.
      console.warn("Unable to check for Prism Hub updates", error);
    }
  } finally {
    updateCheckInFlight = false;
  }
}

// Mirrors the pending update into the native app menu and the Dashboard
// drawer so every entry point offers the install affordance.
function applyUpdateMenuState() {
  window.__TAURI__?.core?.invoke("set_update_menu_state", {
    locale: shellLocale,
    available: Boolean(availableUpdate),
    version: availableUpdate?.version || null,
  }).catch((error) => console.warn("Unable to update the Prism menu state", error));
}

function notifyDashboardUpdateState() {
  dashboard.contentWindow?.postMessage(
    {
      type: "prism:update-state",
      state: updateDialogState === "installing" ? "installing" : availableUpdate ? "available" : "idle",
      version: availableUpdate?.version || "",
      progress: updateProgress || null,
    },
    new URL(hubURL).origin,
  );
}

async function installDesktopUpdate() {
  if (!availableUpdate || updateDialogState === "installing") return;
  updateInstall.disabled = true;
  updateLater.disabled = true;
  updateDialogState = "installing";
  updateProgress = null;
  renderUpdateDialog();
  notifyDashboardUpdateState();
  try {
    await invoke("install_update");
    await invoke("plugin:process|restart");
  } catch (error) {
    console.error("Unable to install Prism Hub update", error);
    updateDialogState = "failed";
    updateProgress = null;
    renderUpdateDialog();
    notifyDashboardUpdateState();
    updateInstall.disabled = false;
    updateLater.disabled = false;
  }
}

// Triggered by the native app menu's "检查更新…" item so the user gets
// feedback even when no update was found. With an update already discovered
// the menu entry is an install affordance, so reopen the dialog instead of
// silently no-op'ing behind the in-flight guard.
window.__prismCheckForUpdate = () => {
  if (availableUpdate) {
    showUpdateDialog();
    return;
  }
  void checkForDesktopUpdate({ manual: true });
};

updateLater.addEventListener("click", hideUpdateDialog);
updateInstall.addEventListener("click", () => void installDesktopUpdate());

window.addEventListener("message", async (event) => {
  if (event.origin !== new URL(hubURL).origin) {
    return;
  }
  try {
    if (event.data?.type === "prism:theme-change") {
      applyShellTheme(event.data.theme);
    } else if (event.data?.type === "prism:locale-change") {
      setShellLocale(String(event.data.locale || ""));
    } else if (event.data?.type === "prism:hub-lifecycle-status-request") {
      const lifecycle = await invoke("hub_lifecycle_status");
      event.source?.postMessage(
        { type: "prism:hub-lifecycle-status", state: String(lifecycle || "unavailable") },
        event.origin,
      );
    } else if (event.data?.type === "prism:hub-restart-scheduled") {
      void restartDashboardAfterHubRestart();
    } else if (event.data?.type === "prism:quit-hub") {
      await invoke("quit_prism_hub");
    } else if (event.data?.type === "prism:open-remote-panel") {
      await invoke("open_remote_panel");
    } else if (event.data?.type === "prism:open-forgot-password") {
      await invoke("open_forgot_password");
    } else if (event.data?.type === "prism:open-register-account") {
      await invoke("open_register_account");
    } else if (event.data?.type === "prism:check-for-update") {
      await checkForDesktopUpdate({ manual: true });
    } else if (event.data?.type === "prism:install-update") {
      await installDesktopUpdate();
    } else if (event.data?.type === "prism:open-changelog") {
      try {
        await invoke("open_changelog", { locale: String(event.data.locale || "") });
      } catch (error) {
        dashboard.contentWindow?.postMessage(
          { type: "prism:open-changelog-error", error: String(error || "") },
          event.origin,
        );
      }
    }
  } catch (error) {
    console.error("Prism Hub desktop action failed", error);
  }
});

// Seed the native tray with the system-derived locale right away; the
// Dashboard's own (possibly user-overridden) locale replaces it once loaded.
setShellLocale(shellLocale);

// Forward updater download progress from the Rust installer into the dialog
// and the Dashboard drawer entry.
window.__TAURI__?.event?.listen?.("prism://update-progress", (event) => {
  updateProgress = event.payload ?? null;
  if (updateDialogState === "installing") renderUpdateDialog();
  notifyDashboardUpdateState();
});

startDashboard();
window.setTimeout(() => void checkForDesktopUpdate(), 3000);
window.setInterval(() => void checkForDesktopUpdate(), 6 * 60 * 60 * 1000);

// WKWebView does not consistently report visibilitychange when a native
// window is hidden. Forward native lifecycle events into the Dashboard.
let desktopWindowVisible = true;
function notifyDashboardWindowVisibility() {
  dashboard.contentWindow?.postMessage(
    { type: "prism:window-visibility", visible: desktopWindowVisible },
    new URL(hubURL).origin,
  );
}
dashboard.addEventListener("load", notifyDashboardWindowVisibility);
window.__TAURI__?.event?.listen?.("prism://window-visibility", (event) => {
  desktopWindowVisible = event.payload !== false;
  notifyDashboardWindowVisibility();
});
