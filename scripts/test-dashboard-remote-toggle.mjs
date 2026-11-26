import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { runInNewContext } from 'node:vm'

const html = readFileSync(new URL('../cmd/prism/dashboard_assets/index.html', import.meta.url), 'utf8')
const source = html.slice(html.indexOf('    function localPluginCard('), html.indexOf('    function remotePluginCard('))

test('an offline local Agent can turn off an existing remote registration', () => {
  let registered = true
  const context = {
    hostPluginFor: id => registered ? { plugin_id: id } : null,
    localPresentation: item => item,
    pluginRuntimeModes: () => [],
    pluginModeFor: () => '',
    agentRuntimeRecovery: item => ({ unavailable: item.runtime_status.code !== 'ready' }),
    t: String,
    escapeHtml: String,
    deviceOrigin: () => '',
    recoveryMenuMarkup: () => '',
    recoveryActionMarkup: () => '',
    recoveryToggleMarkup: () => '',
    agentLocationActionMarkup: () => '',
    agentIconURL: () => '',
    agentIconMarkup: () => '',
  }
  runInNewContext(source, context)
  const renderSwitch = code => context.localPluginCard({ plugin_id: 'dsh', runtime_status: { code } }, {})
    .match(/<input[^>]+data-local-remote-enabled="dsh"[^>]*>/)[0]

  const registeredOffline = renderSwitch('not_running')
  assert.match(registeredOffline, /\bchecked\b/)
  assert.doesNotMatch(registeredOffline, /\sdisabled\b/)

  registered = false
  assert.match(renderSwitch('not_running'), /\sdisabled\b/)
  assert.doesNotMatch(renderSwitch('ready'), /\sdisabled\b/)
})
