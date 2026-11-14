import { readFileSync } from 'node:fs'
import { runInNewContext } from 'node:vm'
import { test } from 'node:test'
import assert from 'node:assert/strict'
const html = readFileSync(new URL('../cmd/prism/dashboard_assets/index.html', import.meta.url), 'utf8')
const source = html.slice(html.indexOf('    function finishLoginTransition'), html.indexOf('    async function logout()'))
function harness({ success = true, restart = false, refresh } = {}) {
  const elements = new Map()
  function element(id) {
    if (!elements.has(id)) elements.set(id, { value: id === 'login-username' ? 'test' : id === 'login-password' ? 'secret' : '', hidden: true, textContent: '', disabled: false, attributes: {}, setAttribute(k,v) { this.attributes[k]=v }, removeAttribute(k) { delete this.attributes[k] }, querySelector() { return element(id+'-span') } })
    return elements.get(id)
  }
  let requests = 0, switches = [], restarts = 0
  const toasts = []
  const context = { state: { region: 'cn', authRevision: 0, reauthRequired: false }, document: { getElementById: element }, window: { setTimeout: () => 1, clearTimeout() {} },
    t: key => key, setScopedMessage() {}, dashboardRequestMessage: e => e.message,
    applyStatus: data => { context.state.lastStatus=data }, switchPage: page => switches.push(page),
    fetch: async () => { requests++; return { ok: success, json: async () => ({ ok: success, error: 'invalid password', account: { account_id: 'account' } }) } },
    hubRestartScheduled: () => restart, handleScheduledHubRestart: () => { restarts++ },
    refreshAll: refresh || (async () => {}), scheduleStatusSettle() {}, scheduleLoginConnectionNotice() {}, cancelStatusSettle() {},
    showToast: (message, tone) => toasts.push({ message, tone }), cloudConnectionActive: () => false,
  }
  runInNewContext(source, context)
  return { context, element, switches, requests: () => requests, restarts: () => restarts, toasts }
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
test('double submission issues only one authentication request', async () => {
  const h = harness()
  await Promise.all([h.context.submitPasswordLogin(), h.context.submitPasswordLogin()])
  assert.equal(h.requests(), 1)
})
test('startup failure remains on Dashboard with a distinct connection message', async () => {
  const h = harness({ refresh: async () => { throw new Error('connection unavailable') } })
  await h.context.submitPasswordLogin()
  assert.deepEqual(h.switches, ['dashboard'])
  assert.equal(h.element('login-transition-detail').textContent, 'connection unavailable')
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

test('login bootstrap does not render a stale Hub-not-running notice', () => {
  assert.match(html, /login_bootstrap: true/)
  assert.match(html, /data\.login_bootstrap \? "starting" : "not_running"/)
  assert.match(html, /const loginRecoveryActive = Date\.now\(\) < Number\(state\.loginRecoveryGraceUntil \|\| 0\)/)
})
