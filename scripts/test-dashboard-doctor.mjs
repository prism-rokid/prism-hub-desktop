import { readFileSync } from 'node:fs'
import { runInNewContext } from 'node:vm'
import { test } from 'node:test'
import assert from 'node:assert/strict'

const html = readFileSync(new URL('../cmd/prism/dashboard_assets/index.html', import.meta.url), 'utf8')
function section(start, end) { return html.slice(html.indexOf(start), html.indexOf(end)) }
function harness() {
  const pending = []
  const rendered = []
  const state = { doctorPanelOpen: true, doctorReportRevision: 0, doctorRunning: false }
  const context = { state, fetch: url => new Promise(resolve => pending.push({ url, resolve })),
    document: { getElementById: () => null }, escapeHtml: String, showToast() {}, dashboardRequestMessage: String,
    t: key => key, renderDoctorReport: report => rendered.push(report), loadDoctorHistory: async () => {},
    loadStatus: async () => {}, loadHostPlugins: async () => {} }
  runInNewContext(section('    async function loadDoctorReport(', '    function doctorHistoryLabel'), context)
  runInNewContext(section('    async function refreshAll(', '    function pluginCatalogIcon'), context)
  runInNewContext(section('    async function runDoctorChecks()', '    function openDoctorPanel'), context)
  return { context, state, pending, rendered,
    respond(report) { pending.shift().resolve({ ok: true, json: async () => ({ ok: true, doctor: report }) }) } }
}

test('open diagnostics fetches fresh reports on every dashboard refresh', async () => {
  const h = harness()
  for (const report of [{ findings: ['old'] }, { findings: [] }]) {
    const refresh = h.context.refreshAll({ automatic: true })
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(h.pending[0].url, '/api/doctor/report')
    h.respond(report)
    await refresh
    assert.equal(h.state.lastDoctorReport, report)
  }
  assert.equal(h.rendered.length, 2)
})

test('closed diagnostics does not poll and diagnostics failures do not fail main refresh', async () => {
  const h = harness()
  h.state.doctorPanelOpen = false
  await h.context.refreshAll({ automatic: true })
  assert.equal(h.pending.length, 0)
  h.state.doctorPanelOpen = true
  const refresh = h.context.refreshAll({ automatic: true })
  await new Promise(resolve => setImmediate(resolve))
  h.pending.shift().resolve({ ok: false, json: async () => ({ error: 'temporary failure' }) })
  await refresh
})

test('concurrent reports coalesce and a pre-check response cannot overwrite newer results', async () => {
  const h = harness()
  const first = h.context.loadDoctorReport()
  const second = h.context.loadDoctorReport()
  assert.equal(h.pending.length, 1)
  h.state.doctorReportRevision++
  h.state.lastDoctorReport = { findings: [] }
  h.respond({ findings: ['stale'] })
  await Promise.all([first, second])
  assert.equal(h.rendered.length, 0)
  assert.equal(h.state.lastDoctorReport.findings.length, 0)
})

test('post-action reload waits for an older request then reads a new report', async () => {
  const h = harness()
  const old = h.context.loadDoctorReport()
  const fresh = h.context.loadDoctorReport({ fresh: true })
  h.respond({ findings: ['stale'] })
  await old
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(h.pending.length, 1)
  h.respond({ findings: [] })
  await fresh
  assert.equal(h.rendered.length, 1)
  assert.equal(h.state.lastDoctorReport.findings.length, 0)
})


test('manual check refreshes summary and history and remains newer than an earlier report', async () => {
  const h = harness()
  let historyLoads = 0
  let statusLoads = 0
  h.context.loadDoctorHistory = async () => { historyLoads++ }
  h.context.loadStatus = async () => { statusLoads++ }
  const old = h.context.loadDoctorReport()
  const run = h.context.runDoctorChecks()
  await h.context.runDoctorChecks()
  assert.equal(h.pending.length, 2, 'double clicks must not start another check')
  assert.equal(h.pending[1].url, '/api/doctor/run')
  h.pending.pop().resolve({ ok: true, json: async () => ({ ok: true, doctor: { findings: [] } }) })
  await run
  assert.equal(statusLoads, 1)
  assert.equal(historyLoads, 1)
  assert.equal(h.state.doctorRunning, false)
  h.respond({ findings: ['stale'] })
  await old
  assert.equal(h.rendered.length, 1)
  assert.equal(h.state.lastDoctorReport.findings.length, 0)
})
