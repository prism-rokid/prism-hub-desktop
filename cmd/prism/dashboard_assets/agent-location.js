/* Shared location dialog for installed local Agents. No local files are uploaded. */
let agentLocationEditor = null;
let agentLocationRevision = 0;
const agentLocationPickerRequests = new Map();
function agentLocationParentOrigin() {
  const configured = new URLSearchParams(location.search).get("desktop_origin");
  if (["tauri://localhost", "http://tauri.localhost", "https://tauri.localhost", "null"].includes(configured)) return configured;
  try {
    const referrer = new URL(document.referrer).origin;
    return ["tauri://localhost", "http://tauri.localhost", "https://tauri.localhost"].includes(referrer) ? referrer : "";
  } catch { return ""; }
}


function agentLocationReplyOriginAllowed(origin) {
  const expected = agentLocationParentOrigin();
  return expected === "null" ? ["null", "tauri://localhost"].includes(origin) : Boolean(expected) && origin === expected;
}

function agentLocationActionMarkup(item) {
  if (!item?.location) return "";
  return `<button class="recovery-action" type="button" data-locate-agent="${escapeHtml(item.plugin_id)}"><i data-lucide="folder-search" aria-hidden="true"></i>${escapeHtml(t("location.action"))}</button>`;
}
function agentLocationRuntimeReason(item) {
  const code = item.runtime_status?.code || "runtime_offline";
  const reason = ["program_not_found", "location_invalid", "data_unavailable", "authentication_failed"].includes(code)
    ? t(`location.${code}`)
    : t(code === "runtime_unknown" ? "location.runtime_unknown" : "location.runtime_offline");
  const recovery = item.location_recovery;
  return recovery?.state === "failed" && recovery.mode === item.current_runtime_mode && recovery.error
    ? `${reason} ${recovery.error}` : reason;
}
function bindAgentLocationActions(root = document) {
  root.querySelectorAll("[data-locate-agent]").forEach((button) => {
    if (button.dataset.locationBound) return;
    button.dataset.locationBound = "true";
    button.addEventListener("click", (event) => {
      event.preventDefault(); event.stopPropagation();
      void openAgentLocation(button.dataset.locateAgent);
    });
  });
}
function locationURL(id, mode, suffix = "") {
  return `/api/agents/${encodeURIComponent(id)}/location${suffix}?mode=${encodeURIComponent(mode)}`;
}
async function locationRequest(url, options = {}) {
  const response = await fetch(url, { ...options, headers: { "Content-Type": "application/json" }, cache: "no-store" });
  const data = await response.json();
  if (!response.ok || !data.ok) {
    const error = new Error(data.code ? t(`location.error_${data.code}`) : (data.error || t("location.failed")));
    error.data = data; throw error;
  }
  return data;
}
async function openAgentLocation(id) {
  const item = state.lastStatus?.plugins?.find((entry) => entry.plugin_id === id);
  if (!item?.location) return;
  const revision = ++agentLocationRevision;
  agentLocationEditor = { id, mode: item.current_runtime_mode, revision, busy: false, returnFocus: document.activeElement };
  const overlay = document.getElementById("agent-location-overlay");
  overlay.hidden = false;
  document.getElementById("agent-location-title").textContent = t("location.title", { name: pluginDisplayName(id) });
  document.getElementById("agent-location-error").textContent = "";
  document.getElementById("agent-location-candidates-row").hidden = true;
  document.getElementById("agent-location-candidates").replaceChildren();
  document.getElementById("agent-location-form").reset();
  document.getElementById("agent-location-current").textContent = "";
  document.getElementById("agent-location-hint").textContent = t("location.loading");
  setAgentLocationBusy(true);
  overlay.querySelector("section").focus();
  try {
    const data = await locationRequest(locationURL(id, agentLocationEditor.mode));
    if (agentLocationEditor?.revision !== revision) return;
    const rule = data.rule;
    document.getElementById("agent-location-hint").textContent = `${agentLocationEditor.mode} · ${t(rule.launch_kind === "desktop-app" ? "location.desktop_hint" : "location.cli_hint", { names: rule.names.join(" / ") })} ${t("location.host_hint")}`;
    document.getElementById("agent-location-directory").value = data.selection?.directory || data.resolved?.directory || "";
    document.getElementById("agent-location-current").textContent = data.resolved
      ? `${t(`location.source_${data.resolved.source}`)} · ${data.resolved.executable}` : "";
    if (data.selection?.executable) agentLocationEditor.executable = data.selection.executable;
    if (data.error) document.getElementById("agent-location-error").textContent = t(`location.error_${data.error.code}`);
  } catch (error) {
    if (agentLocationEditor?.revision === revision) document.getElementById("agent-location-error").textContent = error.message;
  } finally {
    if (agentLocationEditor?.revision === revision) { setAgentLocationBusy(false); document.getElementById("agent-location-directory").focus(); }
  }
}
function setAgentLocationBusy(busy) {
  if (!agentLocationEditor) return;
  agentLocationEditor.busy = busy;
  document.getElementById("agent-location-form").querySelectorAll("button, input, select").forEach((element) => { element.disabled = busy; });
  document.getElementById("agent-location-form").setAttribute("aria-busy", String(busy));
}
function closeAgentLocation() {
  if (!agentLocationEditor || agentLocationEditor.busy) return;
  const { returnFocus, id } = agentLocationEditor;
  agentLocationEditor = null; ++agentLocationRevision;
  document.getElementById("agent-location-overlay").hidden = true;
  const focus = returnFocus?.isConnected ? returnFocus : [...document.querySelectorAll("[data-agent-menu]")].find((element) => element.dataset.agentMenu === id);
  focus?.focus();
}
function showLocationCandidates(candidates, directory) {
  const select = document.getElementById("agent-location-candidates");
  select.replaceChildren(new Option(t("location.choose_target"), ""));
  for (const candidate of candidates) {
    const separator = candidate.directory.includes("\\") ? "\\" : "/";
    const prefix = candidate.directory.replace(/[\\/]+$/, "") + separator;
    const relative = candidate.executable.startsWith(prefix) ? candidate.executable.slice(prefix.length) : "";
    if (relative) select.add(new Option(candidate.executable, relative));
  }
  document.getElementById("agent-location-candidates-row").hidden = false;
}
async function saveAgentLocation(reset = false) {
  const editor = agentLocationEditor;
  if (!editor || editor.busy) return;
  setAgentLocationBusy(true);
  document.getElementById("agent-location-error").textContent = "";
  try {
    const directory = document.getElementById("agent-location-directory").value.trim();
    const executable = document.getElementById("agent-location-candidates").value || editor.executable || "";
    const body = JSON.stringify({ mode: editor.mode, directory, executable });
    if (!reset) {
      const validation = await locationRequest(locationURL(editor.id, editor.mode, "/validate"), { method: "POST", body });
      if (validation.candidates.length > 1) { showLocationCandidates(validation.candidates, directory); throw new Error(t("location.error_multiple_candidates")); }
    }
    await locationRequest(locationURL(editor.id, editor.mode), reset ? { method: "DELETE" } : { method: "PUT", body });
    setAgentLocationBusy(false); closeAgentLocation();
    setScopedMessage("plugins-message", t("location.saved"), "ok");
    await refreshAll({ forceStatus: true });
    scheduleStatusSettle(() => state.lastStatus?.plugins?.find((item) => item.plugin_id === editor.id)?.location_recovery?.state !== "connecting");
  } catch (error) {
    if (agentLocationEditor === editor) document.getElementById("agent-location-error").textContent = error.message;
  } finally { if (agentLocationEditor === editor) setAgentLocationBusy(false); }
}
async function refreshAgentLocation(id) {
  const item = state.lastStatus?.plugins?.find((entry) => entry.plugin_id === id);
  if (!item?.location) return refreshAll({ forceStatus: true });
  await locationRequest(locationURL(id, item.current_runtime_mode, "/recover"), { method: "POST", body: JSON.stringify({ mode: item.current_runtime_mode }) });
  await refreshAll({ forceStatus: true });
  scheduleStatusSettle(() => state.lastStatus?.plugins?.find((entry) => entry.plugin_id === id)?.location_recovery?.state !== "connecting");
}
window.addEventListener("message", (event) => {
  if (event.source !== window.parent || window.parent === window || !agentLocationReplyOriginAllowed(event.origin) || event.data?.type !== "prism:agent-folder-result") return;
  const pending = agentLocationPickerRequests.get(event.data.requestId);
  if (!pending) return;
  agentLocationPickerRequests.delete(event.data.requestId);
  if (event.data.error) pending.reject(new Error(event.data.error)); else pending.resolve(event.data.path || "");
});
document.getElementById("agent-location-browse").addEventListener("click", async () => {
  if (!agentLocationEditor) return;
  if (window.parent === window || !agentLocationParentOrigin()) { document.getElementById("agent-location-error").textContent = t("location.browser_hint"); document.getElementById("agent-location-directory").focus(); return; }
  const editor = agentLocationEditor;
  setAgentLocationBusy(true);
  try {
    const requestId = crypto.randomUUID();
    const path = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => { agentLocationPickerRequests.delete(requestId); reject(new Error(t("location.picker_timeout"))); }, 120000);
      agentLocationPickerRequests.set(requestId, { resolve: (value) => { clearTimeout(timeout); resolve(value); }, reject: (error) => { clearTimeout(timeout); reject(error); } });
      window.parent.postMessage({ type: "prism:choose-agent-folder", requestId }, ["null", "tauri://localhost"].includes(agentLocationParentOrigin()) ? "*" : agentLocationParentOrigin());
    });
    if (agentLocationEditor === editor && path) { document.getElementById("agent-location-directory").value = path; editor.executable = ""; document.getElementById("agent-location-candidates-row").hidden = true; document.getElementById("agent-location-candidates").replaceChildren(); }
  } catch (error) { if (agentLocationEditor === editor) document.getElementById("agent-location-error").textContent = error.message; }
  finally { if (agentLocationEditor === editor) setAgentLocationBusy(false); }
});
document.getElementById("agent-location-directory").addEventListener("input", () => {
  if (agentLocationEditor) agentLocationEditor.executable = "";
  document.getElementById("agent-location-candidates-row").hidden = true;
  document.getElementById("agent-location-candidates").replaceChildren();
});
document.getElementById("agent-location-form").addEventListener("submit", (event) => { event.preventDefault(); void saveAgentLocation(); });
document.getElementById("agent-location-reset").addEventListener("click", () => void saveAgentLocation(true));
document.getElementById("agent-location-cancel").addEventListener("click", closeAgentLocation);
document.getElementById("agent-location-overlay").addEventListener("click", (event) => { if (event.target === event.currentTarget) closeAgentLocation(); });
document.getElementById("agent-location-overlay").addEventListener("keydown", (event) => {
  if (event.key === "Escape") { event.stopPropagation(); closeAgentLocation(); }
  if (event.key !== "Tab") return;
  const focusable = [...event.currentTarget.querySelectorAll("button:not(:disabled), input:not(:disabled), select:not(:disabled)")].filter((el) => !el.closest("[hidden]"));
  if (!focusable.length) { event.preventDefault(); return; }
  const first = focusable[0], last = focusable[focusable.length - 1];
  if (event.shiftKey && (document.activeElement === first || !focusable.includes(document.activeElement))) { event.preventDefault(); last.focus(); }
  else if (!event.shiftKey && (document.activeElement === last || !focusable.includes(document.activeElement))) { event.preventDefault(); first.focus(); }
});

