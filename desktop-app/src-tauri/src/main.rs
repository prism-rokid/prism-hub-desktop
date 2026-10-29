// Release builds must be GUI-subsystem executables: without this attribute
// the Windows linker emits a console (CUI) app, which opens an extra console
// window on launch and lets child-process stderr (e.g. the legacy schtasks
// cleanup) surface to the user.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use qrcode::render::svg;
use qrcode::QrCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(not(target_os = "macos"))]
use tauri::menu::HELP_SUBMENU_ID;
use tauri::menu::{IsMenuItem, Menu, MenuItem, MenuItemKind, PredefinedMenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::window::Color;
use tauri::{AppHandle, Emitter, Manager, RunEvent, Window, WindowEvent};
use tauri_plugin_updater::{Update, UpdaterExt};

const HUB_ADDR: &str = "127.0.0.1:17171";
const HUB_STATUS_MAX_BYTES: usize = 512 * 1024;
const SIDECAR_DIAGNOSTIC_TAIL_MAX_BYTES: usize = 4 * 1024;
const TRAY_ID: &str = "prism-hub-tray";
const MENU_SHOW: &str = "show";
const MENU_QUIT: &str = "quit";
const MENU_CHECK_UPDATE: &str = "check-update";
const MENU_USAGE_PREFIX: &str = "usage-";
const APP_NAME: &str = match option_env!("PRISM_BUILD_PRODUCT_NAME") {
    Some(value) => value,
    None => "Prism Hub",
};
const REMOTE_PANEL_URL: &str = match option_env!("PRISM_BUILD_PANEL_URL") {
    Some(value) => value,
    None => "",
};

fn updater_enabled() -> bool {
    option_env!("PRISM_BUILD_UPDATER_ENABLED") == Some("1")
}
fn account_web_url_for(environment: &str, path: &str) -> String {
    let origin = if environment == "prod" {
        "https://account-web.rokid.com"
    } else {
        "https://account-web-test.rokid.com"
    };
    format!("{origin}/{path}")
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RemotePanelQRCode {
    svg: String,
}

#[derive(Default)]
struct AppLifecycle {
    quitting: AtomicBool,
}

#[derive(Default)]
struct PendingUpdate {
    value: Mutex<Option<Update>>,
}

// The tray follows the Dashboard's locale. The shell webview pushes it on
// boot and again for every prism:locale-change, mirroring how it already
// tracks the Dashboard theme. Keep the dictionary in the same shape as the
// Dashboard CSV locales (zh-CN / en-US / ja-JP) with an en-US fallback.
const TRAY_LOCALES: [&str; 3] = ["zh-CN", "en-US", "ja-JP"];
const TRAY_DEFAULT_LOCALE: &str = "zh-CN";

// Everything the rendered tray depends on: the Dashboard locale, the latest
// usage projection, and the last hub status label. Any input change funnels
// into render_tray, which diffs against what is actually on screen so the
// native menu is only swapped when its visible content changed.
#[derive(Default)]
struct TrayState {
    locale: Mutex<String>,
    usage_lines: Mutex<Vec<AgentUsageLine>>,
    status_label: Mutex<String>,
    rendered: Mutex<RenderedTray>,
}

#[derive(Default, PartialEq)]
struct RenderedTray {
    locale: String,
    menu_labels: Vec<String>,
    tooltip: String,
}

fn normalize_tray_locale(value: &str) -> String {
    let value = value.trim();
    if TRAY_LOCALES.contains(&value) {
        value.to_string()
    } else {
        TRAY_DEFAULT_LOCALE.to_string()
    }
}

fn tray_locale(state: &TrayState) -> String {
    state
        .locale
        .lock()
        .map(|locale| normalize_tray_locale(&locale))
        .unwrap_or_else(|_| TRAY_DEFAULT_LOCALE.to_string())
}

// Same lookup chain as the Dashboard t(): requested locale, then en-US, then
// the key itself, so a missing entry is obvious instead of silently Chinese.
fn tray_text(locale: &str, key: &'static str) -> &'static str {
    let normalized = normalize_tray_locale(locale);
    let dictionary = match normalized.as_str() {
        "zh-CN" => tray_dictionary_zh_cn(),
        "ja-JP" => tray_dictionary_ja_jp(),
        _ => tray_dictionary_en_us(),
    };
    if let Some((_, value)) = dictionary.iter().find(|(entry_key, _)| *entry_key == key) {
        return value;
    }
    tray_dictionary_en_us()
        .iter()
        .find(|(entry_key, _)| *entry_key == key)
        .map(|(_, value)| *value)
        .unwrap_or(key)
}

fn tray_text_with(locale: &str, key: &'static str, params: &[(&str, &str)]) -> String {
    let mut value = tray_text(locale, key).to_string();
    for (name, replacement) in params {
        value = value.replace(&format!("{{{name}}}"), replacement);
    }
    value
}

fn tray_dictionary_zh_cn() -> &'static [(&'static str, &'static str)] {
    &[
        ("tray.show", "显示主窗口"),
        ("tray.quit", "完全退出"),
        ("menu.check_update", "检查更新…"),
        ("menu.update_available", "新版本 v{version} · 立即更新"),
        ("tray.tooltip.starting", "正在启动"),
        ("tray.status.starting_or_unavailable", "启动中或不可用"),
        ("tray.status.running", "运行中"),
        ("tray.status.running_signed_out", "运行中（未登录）"),
        ("tray.status.running_connection", "运行中（{label}）"),
        ("tray.status.hub_unavailable", "异常：Hub 不可用"),
    ]
}

fn tray_dictionary_en_us() -> &'static [(&'static str, &'static str)] {
    &[
        ("tray.show", "Show Main Window"),
        ("tray.quit", "Fully Quit"),
        ("menu.check_update", "Check for Updates…"),
        ("menu.update_available", "New version v{version} · Update now"),
        ("tray.tooltip.starting", "Starting"),
        (
            "tray.status.starting_or_unavailable",
            "Starting or unavailable",
        ),
        ("tray.status.running", "Running"),
        ("tray.status.running_signed_out", "Running (signed out)"),
        ("tray.status.running_connection", "Running ({label})"),
        ("tray.status.hub_unavailable", "Error: Hub unavailable"),
    ]
}

fn tray_dictionary_ja_jp() -> &'static [(&'static str, &'static str)] {
    &[
        ("tray.show", "メインウィンドウを表示"),
        ("tray.quit", "完全に終了"),
        ("menu.check_update", "更新を確認…"),
        ("menu.update_available", "新バージョン v{version} · 今すぐ更新"),
        ("tray.tooltip.starting", "起動中"),
        (
            "tray.status.starting_or_unavailable",
            "起動中または利用不可",
        ),
        ("tray.status.running", "実行中"),
        ("tray.status.running_signed_out", "実行中（未ログイン）"),
        ("tray.status.running_connection", "実行中（{label}）"),
        ("tray.status.hub_unavailable", "エラー：Hub 利用不可"),
    ]
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AvailableUpdate {
    version: String,
}

impl AppLifecycle {
    fn request_quit(&self) {
        self.quitting.store(true, Ordering::SeqCst);
    }

    fn is_quitting(&self) -> bool {
        self.quitting.load(Ordering::SeqCst)
    }
}

struct HubSupervisor {
    child: Mutex<Option<Child>>,
    launch_lock: Mutex<()>,
    lifecycle: Mutex<HubLifecycleState>,
    legacy_service_retired: AtomicBool,
}

#[derive(Clone, Copy, Default)]
enum HubLifecycleState {
    #[default]
    Starting,
    Running,
    Unavailable,
}

impl HubLifecycleState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Unavailable => "unavailable",
        }
    }
}

impl HubSupervisor {
    fn new() -> Self {
        Self {
            child: Mutex::new(None),
            launch_lock: Mutex::new(()),
            lifecycle: Mutex::new(HubLifecycleState::Starting),
            legacy_service_retired: AtomicBool::new(false),
        }
    }

    fn set_lifecycle(&self, lifecycle: HubLifecycleState) {
        if let Ok(mut current) = self.lifecycle.lock() {
            *current = lifecycle;
        }
    }

    fn lifecycle_status(&self) -> &'static str {
        self.lifecycle
            .lock()
            .map(|lifecycle| lifecycle.as_str())
            .unwrap_or(HubLifecycleState::Unavailable.as_str())
    }

    fn ensure_ready(&self) -> Result<HubStatus, String> {
        self.set_lifecycle(HubLifecycleState::Starting);
        let _guard = self
            .launch_lock
            .lock()
            .map_err(|_| "Prism Hub 启动锁不可用".to_string())?;
        self.retire_legacy_service();
        let binary = sidecar_binary_path().map_err(|error| {
            self.set_lifecycle(HubLifecycleState::Unavailable);
            error
        })?;
        let generation = sidecar_generation(&binary).map_err(|error| {
            self.set_lifecycle(HubLifecycleState::Unavailable);
            error
        })?;
        if let Some(status) = probe_hub() {
            if status.sidecar_generation == generation {
                // Another Desktop window from the same package may already
                // own this local sidecar. Keep that healthy process shared.
                self.set_lifecycle(HubLifecycleState::Running);
                return Ok(status);
            }
            // A freshly rebuilt or upgraded Desktop must not keep serving an
            // orphaned sidecar from an older package. Its runtime state can
            // be stale even while the local HTTP endpoint still answers.
            terminate_running_sidecar(&status);
        }

        self.clear_exited_child();
        let mut command = Command::new(&binary);
        command
            .arg("sidecar")
            .env("PRISM_DESKTOP_MANAGED", "1")
            // A sidecar must not survive an abnormal Desktop exit. Its owner
            // PID is checked by the Hub so orphaned plugin process groups are
            // cancelled even when macOS reparents them to launchd.
            .env("PRISM_DESKTOP_OWNER_PID", std::process::id().to_string())
            .env("PRISM_SIDECAR_GENERATION", &generation)
            .stdin(Stdio::null());
        let diagnostic_log = sidecar_diagnostic_log_path();
        if let Some(log_path) = diagnostic_log.as_ref() {
            if let Ok((stdout, stderr)) = sidecar_diagnostic_log_streams(log_path) {
                command.stdout(stdout).stderr(stderr);
            } else {
                command.stdout(Stdio::null()).stderr(Stdio::null());
            }
        } else {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
        #[cfg(target_os = "windows")]
        {
            // The sidecar is a console-subsystem Go binary. Without
            // CREATE_NO_WINDOW Windows allocates a visible console window for
            // it on every launch (the black window next to the app).
            command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        }

        let child = command.spawn().map_err(|error| {
            self.set_lifecycle(HubLifecycleState::Unavailable);
            format!("无法启动 Prism Hub：{error}")
        })?;
        *self
            .child
            .lock()
            .map_err(|_| "Prism Hub 进程状态不可用".to_string())? = Some(child);

        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            if let Some(status) = probe_hub() {
                self.set_lifecycle(HubLifecycleState::Running);
                return Ok(status);
            }
            self.clear_exited_child();
            std::thread::sleep(Duration::from_millis(150));
        }

        self.stop_owned_process();
        self.set_lifecycle(HubLifecycleState::Unavailable);
        let diagnostic = diagnostic_log
            .as_deref()
            .and_then(sidecar_diagnostic_tail)
            .map(|tail| format!("\n最近启动日志：{tail}"))
            .unwrap_or_default();
        Err(format!(
            "Prism Hub 启动超时，请查看日志或重启应用。{diagnostic}"
        ))
    }

    fn stop_owned_process(&self) {
        let mut child = match self.child.lock() {
            Ok(child) => child,
            Err(_) => return,
        };
        if let Some(mut process) = child.take() {
            terminate_sidecar(&mut process);
        }
    }

    fn clear_exited_child(&self) {
        let mut child = match self.child.lock() {
            Ok(child) => child,
            Err(_) => return,
        };
        let exited = child
            .as_mut()
            .and_then(|process| process.try_wait().ok())
            .flatten()
            .is_some();
        if exited {
            *child = None;
        }
    }

    fn retire_legacy_service(&self) {
        if self.legacy_service_retired.swap(true, Ordering::SeqCst) {
            return;
        }
        retire_legacy_background_service();
    }
}

// The sidecar owns separate plugin process groups. Terminate it gracefully so
// its context cancellation can close those groups before any hard fallback.
fn terminate_sidecar(process: &mut Child) {
    #[cfg(unix)]
    {
        let _ = Command::new("kill")
            .args(["-TERM", &process.id().to_string()])
            .status();
    }

    #[cfg(not(unix))]
    {
        let _ = process.kill();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match process.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(_) => return,
        }
    }
    let _ = process.kill();
    let _ = process.wait();
}

fn terminate_running_sidecar(status: &HubStatus) {
    if status.hub_pid == 0 {
        return;
    }

    #[cfg(unix)]
    {
        let pid = status.hub_pid.to_string();
        let _ = Command::new("kill").args(["-TERM", &pid]).status();

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match probe_hub() {
                Some(current) if current.hub_pid == status.hub_pid => {
                    thread::sleep(Duration::from_millis(100));
                }
                _ => return,
            }
        }
        let _ = Command::new("kill").args(["-KILL", &pid]).status();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match probe_hub() {
                Some(current) if current.hub_pid == status.hub_pid => {
                    thread::sleep(Duration::from_millis(100));
                }
                _ => return,
            }
        }
    }

    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &status.hub_pid.to_string(), "/T", "/F"])
            .creation_flags(0x08000000) // CREATE_NO_WINDOW
            .status();
    }
}

#[derive(Clone, Deserialize)]
struct HubStatus {
    #[serde(default)]
    hub_running: bool,
    #[serde(default)]
    hub_reachable: bool,
    #[serde(default)]
    hub_pid: i32,
    #[serde(default)]
    logged_in: bool,
    #[serde(default)]
    connection_label: String,
    #[serde(default)]
    sidecar_generation: String,
    // Monitored subscriptions surface their quota projection here. Only apps
    // that actually report agent_usage produce a tray line; the rest stay
    // hidden instead of showing an empty placeholder.
    #[serde(default)]
    desktop_apps: HashMap<String, HubDesktopApp>,
}

#[derive(Clone, Deserialize, Default)]
struct HubDesktopApp {
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    agent_usage: Option<HubAgentUsage>,
}

#[derive(Clone, Deserialize, Default)]
struct HubAgentUsage {
    #[serde(default)]
    five_hour: Option<HubUsageWindow>,
    #[serde(default)]
    weekly: Option<HubUsageWindow>,
}

#[derive(Clone, Copy, Deserialize, Default)]
struct HubUsageWindow {
    #[serde(default)]
    used_percent: i64,
}

#[derive(Clone)]
struct AgentUsageLine {
    app_id: String,
    label: String,
}

fn usage_remaining_percent(window: &HubUsageWindow) -> i64 {
    (100 - window.used_percent).clamp(0, 100)
}

fn retire_legacy_background_service() {
    #[cfg(target_os = "macos")]
    {
        let uid = Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .unwrap_or_default();
        let uid = uid.trim();
        if uid.is_empty() {
            return;
        }
        let home = env::var("HOME").unwrap_or_default();
        let plist = PathBuf::from(home)
            .join("Library")
            .join("LaunchAgents")
            .join("com.rokid.prism.hub.plist");
        if plist.is_file() {
            let _ = Command::new("launchctl")
                .args(["bootout", &format!("gui/{uid}"), &plist.to_string_lossy()])
                .status();
            let _ = std::fs::remove_file(plist);
        }
    }
    #[cfg(target_os = "linux")]
    {
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", "com.rokid.prism.hub.service"])
            .status();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = Command::new("schtasks")
            .args(["/Delete", "/TN", "RokidPrismHub", "/F"])
            .creation_flags(0x08000000) // CREATE_NO_WINDOW
            .status();
    }
}

impl HubStatus {
    fn tray_label(&self, locale: &str) -> String {
        if !self.hub_running || !self.hub_reachable {
            return tray_text(locale, "tray.status.starting_or_unavailable").to_string();
        }
        if !self.logged_in {
            return tray_text(locale, "tray.status.running_signed_out").to_string();
        }
        if self.connection_label.trim().is_empty() {
            tray_text(locale, "tray.status.running").to_string()
        } else {
            tray_text_with(
                locale,
                "tray.status.running_connection",
                &[("label", self.connection_label.trim())],
            )
        }
    }

    // One tray line per monitored subscription that reports quota windows,
    // formatted like "Codex: 5h 90% / 7d 68%" (remaining, not consumed).
    fn agent_usage_lines(&self) -> Vec<AgentUsageLine> {
        let mut lines = Vec::new();
        for (app_id, app) in &self.desktop_apps {
            let Some(usage) = app.agent_usage.as_ref() else {
                continue;
            };
            let mut parts = Vec::new();
            if let Some(window) = usage.five_hour.as_ref() {
                parts.push(format!("5h {}%", usage_remaining_percent(window)));
            }
            if let Some(window) = usage.weekly.as_ref() {
                parts.push(format!("7d {}%", usage_remaining_percent(window)));
            }
            if parts.is_empty() {
                continue;
            }
            let name = if app.display_name.trim().is_empty() {
                app_id.clone()
            } else {
                app.display_name.trim().to_string()
            };
            lines.push(AgentUsageLine {
                app_id: app_id.clone(),
                label: format!("{name}: {}", parts.join(" / ")),
            });
        }
        // HashMap iteration order is random; keep the menu visually stable.
        lines.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        lines
    }
}

#[tauri::command]
fn ensure_hub_ready(supervisor: tauri::State<'_, HubSupervisor>) -> Result<(), String> {
    supervisor.ensure_ready().map(|_| ())
}

#[tauri::command]
fn hub_lifecycle_status(supervisor: tauri::State<'_, HubSupervisor>) -> String {
    supervisor.lifecycle_status().to_string()
}

#[tauri::command]
fn quit_prism_hub(app: AppHandle) {
    request_quit(&app);
}

#[tauri::command]
async fn check_for_update(
    app: AppHandle,
    pending: tauri::State<'_, PendingUpdate>,
) -> Result<Option<AvailableUpdate>, String> {
    if !updater_enabled() {
        return Err("此本地测试构建未启用自动更新。".to_string());
    }
    let update = app
        .updater()
        .map_err(|error| format!("初始化更新服务失败：{error}"))?
        .check()
        .await
        .map_err(|error| format!("检查更新失败：{error}"))?;
    let available = update.as_ref().map(|item| AvailableUpdate {
        version: item.version.clone(),
    });
    *pending
        .value
        .lock()
        .map_err(|_| "更新状态不可用。".to_string())? = update;
    Ok(available)
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateProgress {
    downloaded: u64,
    total: Option<u64>,
}

#[tauri::command]
async fn install_update(
    app: AppHandle,
    pending: tauri::State<'_, PendingUpdate>,
) -> Result<(), String> {
    if !updater_enabled() {
        return Err("此本地测试构建未启用自动更新。".to_string());
    }
    let update = pending
        .value
        .lock()
        .map_err(|_| "更新状态不可用。".to_string())?
        .take()
        .ok_or_else(|| "没有可安装的 Prism Hub 更新。".to_string())?;
    // download_and_install borrows the update, so a failed attempt can be
    // restored and retried without another /current lookup.
    let progress_app = app.clone();
    let mut last_emit = std::time::Instant::now();
    let mut last_emitted_bytes: u64 = 0;
    let result = update
        .download_and_install(
            move |downloaded: usize, total: Option<u64>| {
                let downloaded = downloaded as u64;
                let now = std::time::Instant::now();
                let fresh = downloaded.saturating_sub(last_emitted_bytes) >= 512 * 1024;
                if !fresh && now.duration_since(last_emit).as_millis() < 200 {
                    return;
                }
                last_emit = now;
                last_emitted_bytes = downloaded;
                let _ = progress_app.emit(
                    "prism://update-progress",
                    UpdateProgress {
                        downloaded,
                        total,
                    },
                );
            },
            || {},
        )
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Ok(mut guard) = pending.value.lock() {
                *guard = Some(update);
            }
            Err(format!("下载或安装更新失败：{error}"))
        }
    }
}

fn dashboard_window_background(theme: &str) -> Color {
    match theme {
        "dark" => Color(12, 16, 21, 255),
        _ => Color(247, 247, 250, 255),
    }
}

#[tauri::command]
fn set_window_theme(app: AppHandle, theme: String) -> Result<(), String> {
    let window = app
        .get_webview_window("main")
        .ok_or_else(|| "Prism Hub 主窗口不可用。".to_string())?;
    window
        .set_background_color(Some(dashboard_window_background(theme.trim())))
        .map_err(|error| format!("更新窗口主题失败：{error}"))
}

#[tauri::command]
fn open_remote_panel() -> Result<(), String> {
    if REMOTE_PANEL_URL.is_empty() {
        return Err("此 Prism Desktop 构建未配置远程面板地址。".to_string());
    }
    #[cfg(target_os = "macos")]
    let mut command = Command::new("/usr/bin/open");
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command
            .args(["/C", "start", "", REMOTE_PANEL_URL])
            .creation_flags(0x08000000); // CREATE_NO_WINDOW
        command
    };

    #[cfg(not(target_os = "windows"))]
    command.arg(REMOTE_PANEL_URL);

    command
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("打开远程面板失败：{error}"))
}

// The configured panel address varies by build environment and region. Render
// the QR code in the native shell so the local dashboard never has to know or
// duplicate the deployment configuration.
#[tauri::command]
fn remote_panel_qr() -> Result<Option<RemotePanelQRCode>, String> {
    remote_panel_qr_for(REMOTE_PANEL_URL).map(|svg| svg.map(|svg| RemotePanelQRCode { svg }))
}

fn remote_panel_qr_for(panel_url: &str) -> Result<Option<String>, String> {
    let panel_url = panel_url.trim();
    if panel_url.is_empty() {
        return Ok(None);
    }
    let code = QrCode::new(panel_url.as_bytes())
        .map_err(|error| format!("生成远程面板二维码失败：{error}"))?;
    Ok(Some(
        code.render::<svg::Color>()
            .min_dimensions(176, 176)
            .quiet_zone(true)
            .build(),
    ))
}

#[tauri::command]
fn open_forgot_password() -> Result<(), String> {
    open_account_page("forget")
}

#[tauri::command]
fn open_register_account() -> Result<(), String> {
    open_account_page("register")
}

fn open_account_page(path: &str) -> Result<(), String> {
    let url = account_web_url_for(
        option_env!("PRISM_BUILD_ENVIRONMENT").unwrap_or("dev"),
        path,
    );
    #[cfg(target_os = "macos")]
    let mut command = Command::new("/usr/bin/open");
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command
            .args(["/C", "start", "", &url])
            .creation_flags(0x08000000); // CREATE_NO_WINDOW
        command
    };

    #[cfg(not(target_os = "windows"))]
    command.arg(&url);

    command
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("打开账号页面失败：{error}"))
}

fn changelog_url_for(panel_url: &str, locale: &str) -> Result<String, String> {
    let panel_url = panel_url.trim();
    if panel_url.is_empty() {
        return Err("此 Prism Desktop 构建未配置远程面板地址。".to_string());
    }
    // The page origin is compile-time deployment configuration. The panel
    // itself lives below /panel, while the changelog is served from the site
    // root. Consequently dev, test, and production builds each open the
    // changelog on their own configured origin. Only the language is accepted
    // from the Dashboard, then mapped to an allowlist.
    let scheme_end = panel_url
        .find("://")
        .ok_or_else(|| "远程面板地址格式无效。".to_string())?
        + 3;
    let origin_end = panel_url[scheme_end..]
        .find('/')
        .map(|index| scheme_end + index)
        .unwrap_or(panel_url.len());
    let origin = &panel_url[..origin_end];
    let language = if locale.trim().eq_ignore_ascii_case("zh-CN") {
        "zh-CN"
    } else {
        "en"
    };
    Ok(format!(
        "{origin}/changelog?product=hub&lang={language}",
    ))
}

fn changelog_url(locale: &str) -> Result<String, String> {
    changelog_url_for(REMOTE_PANEL_URL, locale)
}

#[tauri::command]
fn open_changelog(locale: String) -> Result<(), String> {
    let url = changelog_url(&locale)?;
    #[cfg(target_os = "macos")]
    let mut command = Command::new("/usr/bin/open");
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command
            .args(["/C", "start", "", &url])
            .creation_flags(0x08000000); // CREATE_NO_WINDOW
        command
    };

    #[cfg(not(target_os = "windows"))]
    command.arg(&url);

    command
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("打开更新日志失败：{error}"))
}

fn main() {
    let builder = tauri::Builder::default()
        // The Desktop app is the sole owner of the local Hub lifecycle. A
        // second launch must wake the existing window before it can construct
        // another supervisor or compare sidecar generations.
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            focus_main_window(app);
        }));
    let builder = if updater_enabled() {
        builder.plugin(tauri_plugin_updater::Builder::new().build())
    } else {
        builder
    };
    builder
        .plugin(tauri_plugin_process::init())
        .manage(AppLifecycle::default())
        .manage(PendingUpdate::default())
        .manage(TrayState::default())
        .manage(HubSupervisor::new())
        .setup(|app| {
            setup_app_menu(app.handle())?;
            setup_tray(app.handle())?;
            start_status_monitor(app.handle().clone());
            Ok(())
        })
        .on_window_event(handle_window_event)
        .invoke_handler(tauri::generate_handler![
            ensure_hub_ready,
            hub_lifecycle_status,
            quit_prism_hub,
            set_tray_locale,
            check_for_update,
            install_update,
            set_update_menu_state,
            set_window_theme,
            open_remote_panel,
            remote_panel_qr,
            open_forgot_password,
            open_register_account,
            open_changelog
        ])
        .build(tauri::generate_context!())
        .expect("failed to build Prism Hub desktop shell")
        .run(handle_run_event);
}

fn handle_window_event(window: &Window, event: &WindowEvent) {
    if matches!(event, WindowEvent::Focused(_) | WindowEvent::Resized(_)) {
        let visible = window.is_visible().unwrap_or(false)
            && !window.is_minimized().unwrap_or(false);
        let _ = window.emit("prism://window-visibility", visible);
    }
    if let WindowEvent::CloseRequested { api, .. } = event {
        if !window.app_handle().state::<AppLifecycle>().is_quitting() {
            api.prevent_close();
            let _ = window.hide();
            let _ = window.emit("prism://window-visibility", false);
            // Hiding the window alone keeps the app pinned to the macOS Dock.
            // Drop to the tray-only presence so the menu bar icon stays the
            // sole entry point while the Dashboard is closed.
            set_dock_icon_visible(window.app_handle(), false);
        }
    }
}

fn handle_run_event(app: &AppHandle, event: RunEvent) {
    match event {
        RunEvent::ExitRequested { api, .. } => {
            if !app.state::<AppLifecycle>().is_quitting() {
                api.prevent_exit();
                request_quit(app);
            }
        }
        // macOS sends this when the user clicks the Dock icon while Prism Hub
        // is still running but its only window was hidden to the tray.
        #[cfg(target_os = "macos")]
        RunEvent::Reopen { .. } => focus_main_window(app),
        // Some macOS quit paths reach the final Exit event without delivering
        // a usable ExitRequested callback. Keep this idempotent last line of
        // defense so the Desktop never leaves its sidecar and plugin groups
        // orphaned under launchd.
        RunEvent::Exit => {
            app.state::<AppLifecycle>().request_quit();
            app.state::<HubSupervisor>().stop_owned_process();
            if let Some(status) = probe_hub() {
                terminate_running_sidecar(&status);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod changelog_tests {
    use super::{changelog_url_for, remote_panel_qr_for};

    #[test]
    fn changelog_url_uses_the_configured_environment_origin_and_allowlisted_language() {
        assert_eq!(
            changelog_url_for("https://prism-dev.rokid.com/panel", "zh-CN").unwrap(),
            "https://prism-dev.rokid.com/changelog?product=hub&lang=zh-CN"
        );
        assert_eq!(
            changelog_url_for("https://prism-test.rokid.com/panel/", "ja-JP").unwrap(),
            "https://prism-test.rokid.com/changelog?product=hub&lang=en"
        );
        assert_eq!(
            changelog_url_for("https://prism.rokid.com/panel", "en-US").unwrap(),
            "https://prism.rokid.com/changelog?product=hub&lang=en"
        );
    }

    #[test]
    fn changelog_url_rejects_missing_configured_origin() {
        assert!(changelog_url_for("", "en-US").is_err());
    }

    #[test]
    fn remote_panel_qr_uses_the_exact_configured_url() {
        let svg = remote_panel_qr_for("https://prism-dev.rokid.com/panel")
            .expect("QR code should render")
            .expect("configured panel URL should produce a QR code");
        assert!(svg.contains("<svg"));
        assert!(svg.contains("viewBox"));
        assert_eq!(remote_panel_qr_for("").unwrap(), None);
    }
}

// Replace the default app menu with one that carries a manual update check.
// The click is routed to the Dashboard webview, which owns the updater
// dialogs and can also report an up-to-date result.
fn setup_app_menu(app: &AppHandle) -> Result<(), Box<dyn std::error::Error>> {
    let locale = tray_locale(&app.state::<TrayState>());
    let menu = Menu::default(app)?;
    let check_update = MenuItem::with_id(
        app,
        MENU_CHECK_UPDATE,
        tray_text(&locale, "menu.check_update"),
        true,
        None::<&str>,
    )?;
    #[cfg(target_os = "macos")]
    {
        let separator = PredefinedMenuItem::separator(app)?;
        // The first submenu of the default menu is the app menu named after
        // the product; slot the entry below "About" like native macOS apps.
        let app_menu = menu.items()?.into_iter().find_map(|item| match item {
            MenuItemKind::Submenu(submenu) => Some(submenu),
            _ => None,
        });
        if let Some(app_menu) = app_menu {
            app_menu.insert(&separator, 1)?;
            app_menu.insert(&check_update, 2)?;
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let separator = PredefinedMenuItem::separator(app)?;
        for item in menu.items()? {
            if let MenuItemKind::Submenu(submenu) = item {
                if submenu.id().0.as_str() == HELP_SUBMENU_ID {
                    submenu.append(&separator)?;
                    submenu.append(&check_update)?;
                    break;
                }
            }
        }
    }
    app.set_menu(menu)?;
    app.on_menu_event(|app, event| match event.id().as_ref() {
        MENU_CHECK_UPDATE => {
            focus_main_window(app);
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.eval("window.__prismCheckForUpdate?.()");
            }
        }
        _ => {}
    });
    Ok(())
}

fn build_tray_menu(
    app: &AppHandle,
    locale: &str,
    usage_lines: &[AgentUsageLine],
) -> tauri::Result<Menu<tauri::Wry>> {
    let show = MenuItem::with_id(
        app,
        MENU_SHOW,
        tray_text(locale, "tray.show"),
        true,
        None::<&str>,
    )?;
    let quit = MenuItem::with_id(
        app,
        MENU_QUIT,
        tray_text(locale, "tray.quit"),
        true,
        None::<&str>,
    )?;

    let mut items: Vec<Box<dyn IsMenuItem<tauri::Wry>>> = vec![Box::new(show)];
    if !usage_lines.is_empty() {
        items.push(Box::new(PredefinedMenuItem::separator(app)?));
        for line in usage_lines {
            // Usage lines stay clickable and behave like the show item: they
            // open the main window, where the full quota details live.
            items.push(Box::new(MenuItem::with_id(
                app,
                format!("{MENU_USAGE_PREFIX}{}", line.app_id),
                line.label.clone(),
                true,
                None::<&str>,
            )?));
        }
        items.push(Box::new(PredefinedMenuItem::separator(app)?));
    }
    items.push(Box::new(quit));
    let refs: Vec<&dyn IsMenuItem<tauri::Wry>> = items.iter().map(|item| item.as_ref()).collect();
    Menu::with_items(app, &refs)
}

fn setup_tray(app: &AppHandle) -> Result<(), Box<dyn std::error::Error>> {
    let locale = tray_locale(&app.state::<TrayState>());
    let menu = build_tray_menu(app, &locale, &[])?;
    let mut tray = TrayIconBuilder::with_id(TRAY_ID)
        .menu(&menu)
        .tooltip(format!(
            "{APP_NAME} — {}",
            tray_text(&locale, "tray.tooltip.starting")
        ))
        // Clicking the status icon opens the menu itself; the show item inside
        // the menu is the path back to the main window.
        .show_menu_on_left_click(true);
    #[cfg(target_os = "macos")]
    {
        let icon = tauri::image::Image::from_bytes(include_bytes!("../icons/tray-template.png"))?;
        tray = tray.icon(icon).icon_as_template(true);
    }
    #[cfg(not(target_os = "macos"))]
    if let Some(icon) = app.default_window_icon().cloned() {
        tray = tray.icon(icon);
    }
    tray.on_menu_event(|app, event| match event.id().as_ref() {
        MENU_SHOW => focus_main_window(app),
        MENU_QUIT => request_quit(app),
        // Clicking a usage line opens the main window, which carries the full
        // quota readout (reset times, history) the one-line menu cannot.
        id if id.starts_with(MENU_USAGE_PREFIX) => focus_main_window(app),
        _ => {}
    })
    .build(app)?;
    Ok(())
}

fn start_status_monitor(app: AppHandle) {
    std::thread::spawn(move || loop {
        if app.state::<AppLifecycle>().is_quitting() {
            return;
        }
        let status = app.state::<HubSupervisor>().ensure_ready().ok();
        update_tray_status(&app, status.as_ref());
        let visible = app.get_webview_window("main")
            .map(|window| window.is_visible().unwrap_or(false) && !window.is_minimized().unwrap_or(false))
            .unwrap_or(false);
        std::thread::sleep(Duration::from_secs(if visible { 3 } else { 15 }));
    });
}

fn update_tray_status(app: &AppHandle, status: Option<&HubStatus>) {
    let state = app.state::<TrayState>();
    let locale = tray_locale(&state);
    let label = status
        .map(|status| status.tray_label(&locale))
        .unwrap_or_else(|| tray_text(&locale, "tray.status.hub_unavailable").to_string());
    let usage_lines = status.map(HubStatus::agent_usage_lines).unwrap_or_default();
    if let Ok(mut cached) = state.status_label.lock() {
        *cached = label;
    }
    if let Ok(mut cached) = state.usage_lines.lock() {
        *cached = usage_lines;
    }
    render_tray(app);
}

// Applies the cached locale / status label / usage lines to the native tray.
// The status monitor polls every few seconds, so unchanged content must not
// rebuild the menu: swapping an identical native menu would flicker (and
// momentarily close it) while nothing visible changed.
fn render_tray(app: &AppHandle) {
    let Some(tray) = app.tray_by_id(TRAY_ID) else {
        return;
    };
    let state = app.state::<TrayState>();
    let locale = tray_locale(&state);
    let status_label = state
        .status_label
        .lock()
        .map(|label| label.clone())
        .unwrap_or_default();
    let usage_lines = state
        .usage_lines
        .lock()
        .map(|lines| lines.clone())
        .unwrap_or_default();
    let tooltip = if status_label.is_empty() {
        format!(
            "{APP_NAME} — {}",
            tray_text(&locale, "tray.tooltip.starting")
        )
    } else {
        format!("{APP_NAME} — {status_label}")
    };
    let menu_labels: Vec<String> = usage_lines.iter().map(|line| line.label.clone()).collect();

    let (menu_changed, tooltip_changed) = match state.rendered.lock() {
        Ok(mut rendered) => {
            let menu_changed = rendered.locale != locale || rendered.menu_labels != menu_labels;
            let tooltip_changed = rendered.tooltip != tooltip;
            if menu_changed {
                rendered.locale = locale.clone();
                rendered.menu_labels = menu_labels.clone();
            }
            if tooltip_changed {
                rendered.tooltip = tooltip.clone();
            }
            (menu_changed, tooltip_changed)
        }
        Err(_) => (true, true),
    };
    if tooltip_changed {
        let _ = tray.set_tooltip(Some(tooltip));
    }
    if menu_changed {
        if let Ok(menu) = build_tray_menu(app, &locale, &usage_lines) {
            let _ = tray.set_menu(Some(menu));
        }
    }
}

#[tauri::command]
fn set_tray_locale(app: AppHandle, locale: String) {
    let locale = normalize_tray_locale(&locale);
    let state = app.state::<TrayState>();
    if let Ok(mut cached) = state.locale.lock() {
        // normalize_tray_locale falls back to zh-CN for anything unknown;
        // only a real change should trigger a menu rebuild.
        if *cached == locale {
            return;
        }
        *cached = locale.clone();
    }
    update_app_menu_language(&app, &locale);
    render_tray(&app);
}

// The native app menu carries its own "check for updates" entry that was
// rendered before the webview reported the Dashboard locale.
fn find_menu_item(app: &AppHandle, id: &str) -> Option<MenuItem<tauri::Wry>> {
    let menu = app.menu()?;
    let items = menu.items().ok()?;
    for item in items {
        if let MenuItemKind::Submenu(submenu) = item {
            let Ok(sub_items) = submenu.items() else {
                continue;
            };
            for sub_item in sub_items {
                if let MenuItemKind::MenuItem(menu_item) = sub_item {
                    if menu_item.id().as_ref() == id {
                        return Some(menu_item);
                    }
                }
            }
        }
    }
    None
}

fn update_app_menu_language(app: &AppHandle, locale: &str) {
    if let Some(menu_item) = find_menu_item(app, MENU_CHECK_UPDATE) {
        let _ = menu_item.set_text(tray_text(locale, "menu.check_update"));
    }
}

// Turns the native "check for updates" entry into an install affordance while
// an update is pending, so the menu keeps advertising the new version.
#[tauri::command]
fn set_update_menu_state(
    app: AppHandle,
    locale: String,
    available: bool,
    version: Option<String>,
) -> Result<(), String> {
    let Some(item) = find_menu_item(&app, MENU_CHECK_UPDATE) else {
        return Ok(());
    };
    let text = if available {
        tray_text_with(
            &locale,
            "menu.update_available",
            &[("version", version.as_deref().unwrap_or(""))],
        )
    } else {
        tray_text(&locale, "menu.check_update").to_string()
    };
    item.set_text(text)
        .map_err(|error| format!("更新菜单状态失败：{error}"))
}

fn focus_main_window(app: &AppHandle) {
    // Returning from the tray must restore the Dock icon and the native app
    // menu before the window is shown and focused.
    set_dock_icon_visible(app, true);
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
        let _ = window.emit("prism://window-visibility", true);
    }
}

// Toggles the macOS activation policy: Regular keeps the Dock icon, while
// Accessory removes it and leaves only the menu bar tray icon. Other
// platforms have no Dock concept, so this is a no-op there.
#[cfg(target_os = "macos")]
fn set_dock_icon_visible(app: &AppHandle, visible: bool) {
    let policy = if visible {
        tauri::ActivationPolicy::Regular
    } else {
        tauri::ActivationPolicy::Accessory
    };
    let _ = app.set_activation_policy(policy);
}

#[cfg(not(target_os = "macos"))]
fn set_dock_icon_visible(_app: &AppHandle, _visible: bool) {}

fn request_quit(app: &AppHandle) {
    app.state::<AppLifecycle>().request_quit();
    app.state::<HubSupervisor>().stop_owned_process();
    if let Some(status) = probe_hub() {
        terminate_running_sidecar(&status);
    }
    app.exit(0);
}

fn probe_hub() -> Option<HubStatus> {
    let address: SocketAddr = HUB_ADDR.parse().ok()?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(800)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1200)))
        .ok()?;
    stream
        .write_all(b"GET /api/status?lightweight=1 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .ok()?;
    read_hub_status(&mut stream)
}

fn read_hub_status(reader: &mut impl Read) -> Option<HubStatus> {
    let mut response = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                response.extend_from_slice(&chunk[..read]);
                if response.len() > HUB_STATUS_MAX_BYTES {
                    return None;
                }
                if let Some(status) = parse_hub_status_response(&response) {
                    return Some(status);
                }
            }
            Err(error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => {
                break
            }
            Err(_) => return None,
        }
    }
    parse_hub_status_response(&response)
}

fn parse_hub_status_response(response: &[u8]) -> Option<HubStatus> {
    let response = std::str::from_utf8(response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 2") && !head.starts_with("HTTP/1.0 2") {
        return None;
    }
    let body = if head.lines().any(|line| {
        line.trim()
            .eq_ignore_ascii_case("transfer-encoding: chunked")
    }) {
        decode_chunked_body(body.as_bytes())?
    } else {
        body.as_bytes().to_vec()
    };
    serde_json::from_slice(&body).ok()
}

fn decode_chunked_body(mut encoded: &[u8]) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let header_end = encoded.windows(2).position(|bytes| bytes == b"\r\n")?;
        let header = std::str::from_utf8(&encoded[..header_end]).ok()?;
        let size = usize::from_str_radix(header.split(';').next()?.trim(), 16).ok()?;
        encoded = &encoded[header_end + 2..];
        if encoded.len() < size + 2 || &encoded[size..size + 2] != b"\r\n" {
            return None;
        }
        body.extend_from_slice(&encoded[..size]);
        encoded = &encoded[size + 2..];
        if size == 0 {
            return Some(body);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn account_pages_follow_deployment_environment() {
        for environment in ["dev", "test", "prod"] {
            let origin = if environment == "prod" {
                "https://account-web.rokid.com"
            } else {
                "https://account-web-test.rokid.com"
            };
            for path in ["register", "forget"] {
                assert_eq!(
                    super::account_web_url_for(environment, path),
                    format!("{origin}/{path}")
                );
            }
        }
    }

    use super::*;

    #[test]
    fn hub_lifecycle_state_separates_startup_from_failure() {
        assert_eq!(HubLifecycleState::Starting.as_str(), "starting");
        assert_eq!(HubLifecycleState::Running.as_str(), "running");
        assert_eq!(HubLifecycleState::Unavailable.as_str(), "unavailable");
    }

    #[test]
    fn parses_complete_status_without_waiting_for_eof() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 77\r\n\r\n",
            "{\"hub_running\":true,\"hub_reachable\":true,\"logged_in\":true,\"connection_label\":\"connected\"}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        assert!(status.hub_running);
        assert!(status.hub_reachable);
        assert!(status.logged_in);
        assert_eq!(status.connection_label, "connected");
    }

    #[test]
    fn parses_chunked_status_response() {
        let body = "{\"hub_running\":true,\"hub_reachable\":true}";
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
            body.len()
        );

        let status =
            parse_hub_status_response(response.as_bytes()).expect("chunked status response");
        assert!(status.hub_running);
        assert!(status.hub_reachable);
    }

    #[test]
    fn parses_sidecar_identity_from_status() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 73\r\n\r\n",
            "{\"hub_running\":true,\"hub_pid\":123,\"sidecar_generation\":\"42-99\"}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        assert_eq!(status.hub_pid, 123);
        assert_eq!(status.sidecar_generation, "42-99");
    }

    #[test]
    fn formats_agent_usage_lines_as_remaining_percent() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 256\r\n\r\n",
            "{\"hub_running\":true,\"desktop_apps\":{",
            "\"codex\":{\"display_name\":\"Codex\",\"agent_usage\":{",
            "\"five_hour\":{\"used_percent\":10,\"window_minutes\":300,\"resets_at\":1783000000000},",
            "\"weekly\":{\"used_percent\":32,\"window_minutes\":10080,\"resets_at\":1783500000000}}},",
            "\"hermes\":{\"display_name\":\"Hermes\"}}}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        let lines = status.agent_usage_lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].app_id, "codex");
        assert_eq!(lines[0].label, "Codex: 5h 90% / 7d 68%");
    }

    #[test]
    fn agent_usage_lines_skip_apps_without_reported_windows() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 200\r\n\r\n",
            "{\"hub_running\":true,\"desktop_apps\":{",
            "\"codex\":{\"display_name\":\"Codex\",\"agent_usage\":{\"updated_at\":1783000000000}},",
            "\"hermes\":{\"display_name\":\"Hermes\"}}}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        assert!(status.agent_usage_lines().is_empty());
    }

    #[test]
    fn agent_usage_lines_report_only_available_windows() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 180\r\n\r\n",
            "{\"hub_running\":true,\"desktop_apps\":{",
            "\"codex\":{\"display_name\":\"Codex\",\"agent_usage\":{",
            "\"weekly\":{\"used_percent\":105,\"window_minutes\":10080,\"resets_at\":1783500000000}}}}}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        let lines = status.agent_usage_lines();
        assert_eq!(lines.len(), 1);
        // An over-consumed window clamps to 0% remaining instead of going negative.
        assert_eq!(lines[0].label, "Codex: 7d 0%");
    }

    #[test]
    fn agent_usage_lines_fall_back_to_app_id_without_display_name() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 160\r\n\r\n",
            "{\"hub_running\":true,\"desktop_apps\":{",
            "\"codex\":{\"agent_usage\":{",
            "\"five_hour\":{\"used_percent\":25,\"window_minutes\":300,\"resets_at\":1783000000000}}}}}"
        );

        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        let lines = status.agent_usage_lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].label, "codex: 5h 75%");
    }

    #[test]
    fn tray_dictionaries_keep_the_same_key_set_across_locales() {
        let dictionaries: Vec<(&str, Vec<&str>)> = [
            ("zh-CN", tray_dictionary_zh_cn()),
            ("en-US", tray_dictionary_en_us()),
            ("ja-JP", tray_dictionary_ja_jp()),
        ]
        .iter()
        .map(|(locale, entries)| {
            let mut keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
            keys.sort_unstable();
            (*locale, keys)
        })
        .collect();
        let (reference_locale, reference_keys) = &dictionaries[0];
        for (locale, keys) in &dictionaries[1..] {
            assert_eq!(
                keys, reference_keys,
                "{locale} tray dictionary must match {reference_locale} keys"
            );
        }
    }

    #[test]
    fn tray_text_translates_known_keys_per_locale() {
        assert_eq!(tray_text("zh-CN", "tray.show"), "显示主窗口");
        assert_eq!(tray_text("en-US", "tray.show"), "Show Main Window");
        assert_eq!(tray_text("ja-JP", "tray.show"), "メインウィンドウを表示");
        assert_eq!(tray_text("zh-CN", "tray.quit"), "完全退出");
        assert_eq!(tray_text("en-US", "tray.quit"), "Fully Quit");
        assert_eq!(tray_text("ja-JP", "tray.quit"), "完全に終了");
        assert_eq!(tray_text("zh-CN", "menu.check_update"), "检查更新…");
        assert_eq!(
            tray_text("en-US", "menu.check_update"),
            "Check for Updates…"
        );
        assert_eq!(tray_text("ja-JP", "menu.check_update"), "更新を確認…");
    }

    #[test]
    fn tray_text_falls_back_to_en_us_then_the_key() {
        // Unknown locales normalize to the zh-CN default, matching the shell's
        // behavior before the webview reports a real Dashboard locale.
        assert_eq!(
            tray_text("fr-FR", "tray.show"),
            tray_text("zh-CN", "tray.show")
        );
        assert_eq!(tray_text("en-US", "missing.key"), "missing.key");
    }

    #[test]
    fn tray_text_with_substitutes_placeholders() {
        assert_eq!(
            tray_text_with(
                "en-US",
                "tray.status.running_connection",
                &[("label", "connected")]
            ),
            "Running (connected)"
        );
        assert_eq!(
            tray_text_with(
                "zh-CN",
                "tray.status.running_connection",
                &[("label", "已连接")]
            ),
            "运行中（已连接）"
        );
    }

    #[test]
    fn hub_status_tray_label_translates_per_locale() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 90\r\n\r\n",
            "{\"hub_running\":true,\"hub_reachable\":true,\"logged_in\":true,\"connection_label\":\"\"}"
        );
        let status = parse_hub_status_response(response.as_bytes()).expect("status response");
        assert_eq!(status.tray_label("zh-CN"), "运行中");
        assert_eq!(status.tray_label("en-US"), "Running");
        assert_eq!(status.tray_label("ja-JP"), "実行中");

        let signed_out = concat!(
            "HTTP/1.1 200 OK\r\nContent-Length: 60\r\n\r\n",
            "{\"hub_running\":true,\"hub_reachable\":true,\"logged_in\":false}"
        );
        let status = parse_hub_status_response(signed_out.as_bytes()).expect("status response");
        assert_eq!(status.tray_label("en-US"), "Running (signed out)");
    }

    #[test]
    fn reads_complete_chunked_status_larger_than_legacy_probe_limit() {
        let padding = "x".repeat(20 * 1024);
        let body = format!(
            r#"{{"hub_running":true,"hub_reachable":true,"logged_in":true,"padding":"{padding}"}}"#
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
            body.len()
        );
        let mut reader = std::io::Cursor::new(response.into_bytes());

        let status = read_hub_status(&mut reader).expect("large chunked status response");
        assert!(status.hub_running);
        assert!(status.hub_reachable);
        assert!(status.logged_in);
    }

    #[test]
    fn reads_recent_sidecar_diagnostics() {
        let path = std::env::temp_dir().join(format!(
            "prism-sidecar-diagnostics-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::write(
            &path,
            format!(
                "{}\nstart hub failed: address already in use",
                "x".repeat(8192)
            ),
        )
        .expect("write diagnostic log");

        let tail = sidecar_diagnostic_tail(&path).expect("diagnostic tail");
        assert!(tail.ends_with("start hub failed: address already in use"));

        fs::remove_file(path).expect("remove diagnostic log");
    }
}

fn sidecar_binary_path() -> Result<PathBuf, String> {
    if let Ok(value) = env::var("PRISM_HUB_BINARY") {
        let override_path = PathBuf::from(value.trim());
        if override_path.is_file() {
            return Ok(override_path);
        }
    }
    let name = if cfg!(target_os = "windows") {
        "prism-sidecar.exe"
    } else {
        "prism-sidecar"
    };
    let executable =
        env::current_exe().map_err(|error| format!("resolve application path: {error}"))?;
    let mut candidates = Vec::new();
    if let Some(parent) = executable.parent() {
        candidates.push(parent.join("resources").join("bin").join(name));
        candidates.push(parent.join("bin").join(name));
        if let Some(macos_dir) = parent.parent() {
            candidates.push(
                macos_dir
                    .join("Resources")
                    .join("resources")
                    .join("bin")
                    .join(name),
            );
        }
    }
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| "未找到随应用安装的 Prism Hub 程序。".to_string())
}

// The sidecar runs without a terminal in released Desktop builds. Retain its
// most recent output so a startup failure can be diagnosed instead of being
// reported only as a generic readiness timeout.
fn sidecar_diagnostic_log_path() -> Option<PathBuf> {
    let home = env::var_os("HOME")?;
    let log_dir = PathBuf::from(home).join(".prism").join("logs");
    fs::create_dir_all(&log_dir).ok()?;
    Some(log_dir.join("desktop-sidecar.log"))
}

fn sidecar_diagnostic_log_streams(path: &PathBuf) -> std::io::Result<(Stdio, Stdio)> {
    // Truncate for each launch so a repeated supervisor retry always leaves
    // the output from the failing sidecar, rather than stale diagnostics.
    let output = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let error = output.try_clone()?;
    Ok((Stdio::from(output), Stdio::from(error)))
}

fn sidecar_diagnostic_tail(path: &std::path::Path) -> Option<String> {
    let output = fs::read(path).ok()?;
    let start = output
        .len()
        .saturating_sub(SIDECAR_DIAGNOSTIC_TAIL_MAX_BYTES);
    let text = String::from_utf8_lossy(&output[start..]);
    let tail = text.trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn sidecar_generation(binary: &PathBuf) -> Result<String, String> {
    let metadata =
        fs::metadata(binary).map_err(|error| format!("读取 Prism Hub 程序信息失败：{error}"))?;
    let modified = metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(format!("{}-{modified}", metadata.len()))
}
