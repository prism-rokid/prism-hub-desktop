import { readFileSync } from 'node:fs'
import { runInNewContext } from 'node:vm'
import { test } from 'node:test'
import assert from 'node:assert/strict'
const html = readFileSync(new URL('../cmd/prism/dashboard_assets/index.html', import.meta.url), 'utf8')
const source = html.slice(html.indexOf('    function finishLoginTransition'), html.indexOf('    async function logout()'))
function harness({ success = true, restart = false, refresh, request } = {}) {
  const elements = new Map()
  function element(id) {
    if (!elements.has(id)) elements.set(id, { value: id === 'login-username' ? 'test' : id === 'login-password' ? 'secret' : '', hidden: true, textContent: '', disabled: false, attributes: {}, setAttribute(k,v) { this.attributes[k]=v }, removeAttribute(k) { delete this.attributes[k] }, querySelector() { return element(id+'-span') } })
    return elements.get(id)
  }
  let requests = 0, switches = [], restarts = 0
  const toasts = [], messages = []
  const timers = new Map()
  let nextTimer = 0
  const context = { state: { region: 'cn', authRevision: 0, reauthRequired: false, loginConnectionNoticeDetail: '' }, document: { getElementById: element },
    window: { setTimeout: (fn, delay) => { timers.set(++nextTimer, { fn, delay }); return nextTimer }, clearTimeout: id => timers.delete(id) },
    AbortController, loginHandoffWindowMs: 30_000,
    t: key => key, setScopedMessage: (...args) => messages.push(args), dashboardRequestMessage: e => e.message,
    applyStatus: data => { context.state.lastStatus=data }, switchPage: page => switches.push(page),
    fetch: async (url, options) => { requests++; return request ? request(requests, options) : { ok: success, json: async () => ({ ok: success, error: 'invalid password', account: { account_id: 'account' } }) } },
    hubRestartScheduled: () => restart, handleScheduledHubRestart: () => { restarts++ },
    refreshAll: refresh || (async () => {}), scheduleStatusSettle() {}, cancelStatusSettle() {},
    showToast: (message, tone) => toasts.push({ message, tone }), cloudConnectionActive: () => false,
  }
  runInNewContext(source, context)
  return { context, element, switches, requests: () => requests, restarts: () => restarts, toasts, messages,
    expire: delay => [...timers.values()].find(timer => timer.delay === delay).fn() }
}
test('success enters Dashboard immediately and uses a short success toast', async () => {
  let release
  const h = harness({ refresh: () => new Promise(resolve => { release=resolve }) })
  const pending = h.context.submitPasswordLogin()
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(h.switches, ['dashboard'])
  assert.equal(h.element('login-transition-status').hidden, true)
  assert.deepEqual(h.toasts, [{ message: 'login.success', tone: 'ok' }])
  assert.equal(h.element('login-password').value, '')
  assert.equal(h.element('password-login-btn').disabled, true)
  release()
  await pending
  assert.equal(h.element('password-login-btn').disabled, false)
})
test('scheduled restart switches before handing control to Desktop', async () => {
  const h = harness({ restart: true })
  await h.context.submitPasswordLogin()
  assert.deepEqual(h.switches, ['dashboard'])
  assert.equal(h.restarts(), 1)
})
test('delayed connection notice is compact and can be dismissed', () => {
  const h = harness()
  h.context.state.lastStatus = { logged_in: true, connection_state: 'connecting' }
  h.context.showLoginConnectionNotice('connection pending')
  assert.equal(h.element('login-transition-status').hidden, false)
  assert.equal(h.element('login-transition-detail').textContent, 'connection pending')
  h.context.finishLoginTransition()
  assert.equal(h.element('login-transition-status').hidden, true)
})
test('authentication failure stays on login and restores its submit button', async () => {
  const h = harness({ success: false })
  await assert.rejects(h.context.submitPasswordLogin(), /invalid password/)
  assert.equal(h.switches.length, 0)
  assert.equal(h.context.state.reauthRequired, true)
  assert.equal(h.context.state.authRevision, 1)
  assert.equal(h.element('password-login-btn').disabled, false)
  assert.equal(h.element('password-login-btn').attributes['aria-busy'], undefined)
})
test('login timeout shows a retry message, re-enables submit and ignores a late response body', async () => {
  let releaseBody, firstSignal
  const h = harness({ request: (attempt, { signal }) => {
    if (attempt === 1) {
      firstSignal = signal
      return { ok: true, json: () => new Promise(resolve => { releaseBody = resolve }) }
    }
    return { ok: true, json: async () => ({ ok: true }) }
  } })
  const actionsSource = html.slice(html.indexOf('    function dashboardRequestMessage'), html.indexOf('    function pluginModeFor'))
  runInNewContext(actionsSource, h.context)
  h.context.runDashboardAction('login-message', h.context.submitPasswordLogin)
  await new Promise(resolve => setImmediate(resolve))
  h.expire(15_000)
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(firstSignal.aborted, true)
  assert.deepEqual(h.messages.at(-1), ['login-message', 'login.submit_timeout', 'err'])
  assert.equal(h.element('password-login-btn').disabled, false)
  assert.equal(h.element('login-form').attributes['aria-busy'], undefined)
  assert.equal(h.element('login-password').value, 'secret')
  releaseBody({ ok: true })
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(h.switches, [])
  await h.context.submitPasswordLogin()
  assert.equal(h.requests(), 2)
  assert.deepEqual(h.switches, ['dashboard'])
})

test('an upstream timeout uses the same message and restores submit', async () => {
  const h = harness({ request: () => ({ ok: false, status: 504 }) })
  await assert.rejects(h.context.submitPasswordLogin(), /login\.submit_timeout/)
  assert.equal(h.element('password-login-btn').disabled, false)
})

test('double submission issues only one authentication request', async () => {
  const h = harness()
  await Promise.all([h.context.submitPasswordLogin(), h.context.submitPasswordLogin()])
  assert.equal(h.requests(), 1)
})
test('startup failure remains on Dashboard with a distinct connection message', async () => {
  const h = harness({ refresh: async () => { throw new Error('connection unavailable') } })
  await h.context.submitPasswordLogin()
  assert.deepEqual(h.switches, ['dashboard'])
  assert.equal(h.element('login-transition-status').hidden, true)
})

test('status response started before login cannot return the user to the login page', async () => {
  const statusSource = html.slice(html.indexOf('    async function loadStatus('), html.indexOf('    function requestDesktopHubLifecycle('))
  let resolveFetch
  const applied = []
  const context = { state: { authRevision: 0 }, fetch: () => new Promise(resolve => { resolveFetch=resolve }), doctorStatusDegraded: () => false, applyStatus: data => applied.push(data), offlineStatusFromLast: () => ({ offline: true }) }
  runInNewContext(statusSource, context)
  const pending = context.loadStatus()
  context.state.authRevision++
  resolveFetch({ ok: true, json: async () => ({ ok: true, logged_in: false }) })
  await pending
  assert.equal(applied.length, 0)
})

test('stale status failure is ignored after a newer refresh starts', async () => {
  const statusSource = html.slice(html.indexOf('    async function loadStatus('), html.indexOf('    function requestDesktopHubLifecycle('))
  let rejectFetch
  const applied = []
  const context = { state: { authRevision: 0, refreshGeneration: 1, lastStatus: { logged_in: true } }, fetch: () => new Promise((_, reject) => { rejectFetch = reject }), doctorStatusDegraded: () => false, applyStatus: data => applied.push(data), offlineStatusFromLast: () => ({ offline: true }) }
  runInNewContext(statusSource, context)
  const pending = context.loadStatus({ requestGeneration: 1 })
  context.state.refreshGeneration = 2
  rejectFetch(new Error('Load failed'))
  await pending
  assert.deepEqual(applied, [])
})

test('browser load failures use the localized connection message', () => {
  const source = html.slice(html.indexOf('    function dashboardRequestMessage'), html.indexOf('    function pluginModeFor'))
  const context = { t: key => key, state: { awaitingHubRestart: false }, setScopedMessage() {} }
  runInNewContext(source, context)
  assert.equal(context.dashboardRequestMessage(new Error('Load failed')), 'error.connection_lost')
  assert.equal(context.dashboardRequestMessage(new Error('Failed to fetch')), 'error.connection_lost')
})

test('background load failures are suppressed during the login recovery window', async () => {
  const source = html.slice(html.indexOf('    function dashboardRequestMessage'), html.indexOf('    function pluginModeFor'))
  const messages = []
  const context = {
    t: key => key,
    state: { awaitingHubRestart: false, loginHandoff: false, loginRecoveryGraceUntil: Date.now() + 10_000 },
    setScopedMessage: (...args) => messages.push(args),
  }
  runInNewContext(source, context)
  context.runDashboardAction('home-message', async () => { throw new Error('Load failed') })
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(messages, [])
})

const shell = readFileSync(new URL('../desktop-app/frontend/app.js', import.meta.url), 'utf8')
const handoffBootstrap = html.slice(html.indexOf('    const loginHandoffWindowMs'), html.indexOf('    const state = {'))
const statusRenderer = html.slice(html.indexOf('    function statusRenderFingerprint'), html.indexOf('    function requestDesktopHubLifecycle'))

async function restartedDashboard() {
  let now = 100_000
  const Date = { now: () => now }
  const previousDashboard = { state: { loginHandoff: now + 30_000 }, Date, window: {
    parent: { postMessage: message => { previousDashboard.restartMessage = message } },
  } }
  runInNewContext(html.slice(html.indexOf('    function handleScheduledHubRestart'), html.indexOf('    async function watchForHubReturn')), previousDashboard)
  previousDashboard.handleScheduledHubRestart({ preserveLoginHandoff: true })

  const desktop = { hubURL: 'http://localhost:8080/', URL, Date, window: { location: { origin: 'tauri://localhost' } },
    dashboard: {}, waitForPreviousHubExit: async () => { now += 1000 }, invoke: async () => {}, dashboardRestartInFlight: false }
  runInNewContext(shell.slice(shell.indexOf('function dashboardURL'), shell.indexOf('async function startDashboard')) +
    shell.slice(shell.indexOf('async function restartDashboardAfterHubRestart'), shell.indexOf('async function provideRemotePanelQRCode')), desktop)
  await desktop.restartDashboardAfterHubRestart(previousDashboard.restartMessage.loginHandoffUntil)

  // 验证替换文档中的实际渲染和状态轮询；测试环境不提供 WebView 存储，因此由 Shell URL 传递提示。
  const switches = []
  const node = { classList: { toggle() {} }, style: {}, dataset: {}, querySelector: () => node }
  const context = { URL, Date, navigator: {}, state: { authRevision: 0, refreshGeneration: 0 },
    window: { location: { href: desktop.dashboard.src }, history: { replaceState() {} }, clearTimeout() {} },
    document: { querySelector: () => node, getElementById: () => node },
    t: key => key, initialsForAccount: () => '', cloudConnectionLabel: () => '', hostPluginSyncPresentation: () => ({}),
    syncServerRecoveryControl() {}, cloudConnectionUsable: () => false, cloudConnectionActive: () => false,
    renderWarnings() {}, renderAgentRecoveryNotices() {}, renderDoctorSummary() {}, renderPluginCards() {},
    finishLoginTransition() {}, doctorStatusDegraded: () => false, offlineStatusFromLast: () => ({ ok: false }),
    switchPage: page => { context.state.page = page; switches.push(page) },
    fetch: async () => { if (context.offline) throw new Error('offline'); return { ok: true, json: async () => context.snapshot } },
  }
  runInNewContext(handoffBootstrap + `\nstate.loginHandoff = loginHandoffPending; state.page = loginHandoffPending ? 'dashboard' : 'login';\n` + statusRenderer, context)
  return { context, switches, advance: ms => { now += ms }, poll: async snapshot => { context.snapshot = snapshot; await context.loadStatus() } }
}

test('desktop reload keeps Dashboard through offline and temporarily signed-out snapshots', async () => {
  const { context, switches, poll } = await restartedDashboard()
  assert.equal(context.state.page, 'dashboard')
  context.offline = true
  await assert.rejects(context.loadStatus(), /offline/)
  context.offline = false
  await poll({ ok: true, logged_in: false })
  await poll({ ok: true, logged_in: true, connection_state: 'connecting' })
  await poll({ ok: true, logged_in: false })
  await poll({ ok: true, logged_in: true, connection_state: 'connected' })
  assert.deepEqual(switches, [])
  assert.equal(context.state.lastStatus.connection_state, 'connected')
})

test('explicit session rejection bypasses the handoff window', async () => {
  const { context, switches, poll } = await restartedDashboard()
  await poll({ ok: true, logged_in: false, connection_state: 'auth_expired' })
  assert.deepEqual(switches, ['login'])
  assert.equal(context.state.loginHandoff, 0)
})

test('a confirmed signed-out status returns to login after the handoff deadline', async () => {
  const { context, advance, switches, poll } = await restartedDashboard()
  await poll({ ok: true, logged_in: false })
  advance(30_000)
  context.offline = true
  await assert.rejects(context.loadStatus(), /offline/)
  assert.deepEqual(switches, [])
  context.offline = false
  await poll({ ok: true, logged_in: false })
  assert.deepEqual(switches, ['login'])
})

test('cold start keeps the form hidden until Hub confirms the session state', async () => {
  const { context, poll } = await restartedDashboard()
  const nodes = new Map()
  const originalElement = context.document.getElementById
  context.document.getElementById = id => {
    if (!id.startsWith('login-bootstrap-')) return originalElement(id)
    if (!nodes.has(id)) nodes.set(id, { dataset: {}, hidden: true })
    return nodes.get(id)
  }
  const page = context.document.querySelector()
  page.dataset.loginState = 'checking'
  Object.assign(context.state, { page: 'login', loginHandoff: 0, statusReady: false })
  let expire
  context.window.setTimeout = fn => { expire = fn; return 1 }
  context.startLoginBootstrapCheck()
  context.offline = true
  await assert.rejects(context.loadStatus(), /offline/)
  expire()
  assert.equal(page.dataset.loginState, 'checking')
  assert.equal(nodes.get('login-bootstrap-status').textContent, 'login.checking_timeout')
  assert.equal(nodes.get('login-bootstrap-actions').hidden, false)
  context.startLoginBootstrapCheck()
  assert.equal(nodes.get('login-bootstrap-status').textContent, 'login.checking_session')
  assert.equal(nodes.get('login-bootstrap-actions').hidden, true)
  context.offline = false
  await poll({ ok: true, logged_in: false })
  assert.equal(page.dataset.loginState, 'ready')
  assert.equal(nodes.get('login-bootstrap-status').hidden, true)
})

test('login bootstrap does not render a stale Hub-not-running notice', () => {
  assert.match(html, /login_bootstrap: true/)
  assert.match(html, /data\.login_bootstrap \? "starting" : "not_running"/)
  assert.match(html, /const loginRecoveryActive = Date\.now\(\) < Number\(state\.loginRecoveryGraceUntil \|\| 0\)/)
})

test('login recovery waits before showing refresh errors and serializes status generations', () => {
  assert.match(html, /scheduleLoginConnectionNotice\(dashboardRequestMessage\(error\)\)/)
  assert.match(html, /refreshGeneration: 0/)
  assert.match(html, /expectedGeneration === state\.refreshGeneration/)
  assert.match(html, /document\.getElementById\("hub-restart-btn"\)\.disabled = hubState !== "running"/)
  assert.doesNotMatch(html, /state\.loginTransition = false;\n      switchPage\("dashboard"\);\n      scheduleLoginConnectionNotice\(\);/)
})
