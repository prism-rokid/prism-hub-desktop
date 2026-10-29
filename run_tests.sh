#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

TEST_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/prism-unit.XXXXXX")"
cleanup() {
  rm -rf "${TEST_DATA_DIR}"
}
trap cleanup EXIT

# Defense in depth: audited tests use t.TempDir(), but no automated run may
# resolve a user's ~/.prism path if a future test accidentally uses defaults.
export PRISM_DATA_DIR="${TEST_DATA_DIR}"

echo "=========================================="
echo "Prism - safe unit validation"
echo "=========================================="
echo ""

echo "1. Hub core packages"
echo "------------------------------------------"
go test \
  ./internal/api \
  ./internal/bridge \
  ./internal/config \
  ./internal/db \
  ./internal/doctor \
  ./internal/host \
  ./internal/lock \
  ./internal/observability \
  ./internal/plugin \
  ./internal/pluginruntime \
  ./internal/realtime
echo ""

echo "2. Plugin registry, installer, and shared runtimes"
echo "------------------------------------------"
go test ./internal/pluginregistry ./internal/plugininstall ./internal/runtimes
echo ""

echo "3. Isolated Hub CLI cases"
echo "------------------------------------------"
go test ./cmd/prism -run 'Test(BridgePluginTestCommandWithFakePlugin|BridgePluginTestPersistsVisibilityCheck|TryHandleSubcommandHubStatus|TryHandleSubcommandDashboardRequiresRunningHub|CollectDashboardStatusReflectsConfig|CollectDashboardStatusToleratesBrokenConfig|CollectServingHubDashboardStatusSkipsSelfHealthProbe|CollectDashboardStatusSuggestsLoginWhenMissingToken|CollectDashboardStatusUsesDaemonRuntimeStateForAuthExpired|CollectDashboardStatusNormalizesStaleConnectedRuntimeState|DashboardTranslationJSON|DashboardTranslationJSONRejectsInvalidRows)$' -count=1
echo ""

echo "4. Dashboard refresh, login, and diagnostics regression cases"
echo "------------------------------------------"
node --test scripts/test-dashboard-*.mjs
echo ""

echo "All safe unit validation checks passed."
