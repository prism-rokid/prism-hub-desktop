import { readFileSync } from 'node:fs'
import { runInNewContext } from 'node:vm'
import { test } from 'node:test'
import assert from 'node:assert/strict'

const html = readFileSync(new URL('../cmd/prism/dashboard_assets/index.html', import.meta.url), 'utf8')
const source = html.slice(html.indexOf('    function dashboardIsHidden()'), html.indexOf('    // The plugin reloader'))
function harness() {
  let id = 0
  const timers = new Map()
  const state = { settleTimers: new Set(), browserOnline: true, desktopWindowVisible: true }
  const document = { visibilityState: 'visible' }
  let refreshes = 0
  const context = { state, document, window: {
    setTimeout(fn) { const key = ++id; timers.set(key, fn); return key },
    clearTimeout(key) { timers.delete(key) },
  }, refreshAll: async () => { refreshes++ }, setScopedMessage() {}, dashboardRequestMessage: String }
  runInNewContext(source, context)
  return { context, state, document, timers, refreshes: () => refreshes,
    async tick() { const [key, fn] = timers.entries().next().value; timers.delete(key); await fn() } }
}

test('reloader settle accepts null and stops after its bounded samples', async () => {
  const h = harness()
  h.context.scheduleStatusSettle(null, [1500, 4000, 8000])
  assert.equal(h.timers.size, 1)
  for (let i = 0; i < 3; i++) await h.tick()
  assert.equal(h.refreshes(), 3)
  assert.equal(h.timers.size, 0)
  assert.equal(h.state.settleTimers.size, 0)
})
test('settle stops immediately when expected state is observed', async () => {
  const h = harness()
  h.context.scheduleStatusSettle(() => h.refreshes() > 0)
  await h.tick()
  assert.equal(h.refreshes(), 1)
  assert.equal(h.timers.size, 0)
})
test('hidden/offline windows neither refresh nor schedule periodic work', async () => {
  const h = harness()
  h.context.scheduleStatusSettle(null)
  h.document.visibilityState = 'hidden'
  await h.tick()
  h.context.startAutoRefresh()
  assert.equal(h.timers.size, 0)
  assert.equal(h.refreshes(), 0)
  h.document.visibilityState = 'visible'
  h.state.browserOnline = false
  h.context.startAutoRefresh()
  h.context.scheduleStatusSettle(null)
  assert.equal(h.timers.size, 0)
})
test('suspension cancels all pending background timers', () => {
  const h = harness()
  h.context.startAutoRefresh()
  h.context.scheduleStatusSettle(null)
  h.context.stopAutoRefresh()
  h.context.cancelStatusSettle()
  assert.equal(h.timers.size, 0)
})

test('native window hide suspends work even when WebView visibility stays visible', () => {
  const h = harness()
  h.state.desktopWindowVisible = false
  h.context.startAutoRefresh()
  h.context.scheduleStatusSettle(null)
  assert.equal(h.timers.size, 0)
  h.state.desktopWindowVisible = true
  h.context.startAutoRefresh()
  assert.equal(h.timers.size, 1)
})
