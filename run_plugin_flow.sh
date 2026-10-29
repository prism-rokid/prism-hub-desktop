#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_plugin_flow.sh codex --cwd /abs/project/path
  ./run_plugin_flow.sh hermes --cwd /abs/project/path
  ./run_plugin_flow.sh openclaw

What it does:
  1. Ensures prism is installed locally
  2. Runs `prism login`
  3. Installs the chosen plugin
  4. Runs `prism plugin test`
  5. Prints the daemon command to run next
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

plugin="$1"
shift

cwd_arg=""
plugin_workspace="${PRISM_PLUGIN_WORKSPACE:-$(cd "$(dirname "$0")/.." && pwd)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cwd)
      if [[ $# -lt 2 ]]; then
        echo "--cwd requires a path" >&2
        exit 2
      fi
      cwd_arg="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

case "$plugin" in
  codex)
    plugin_path="${plugin_workspace}/prism-plugin-codex"
    test_args=(--path "$plugin_path")
    if [[ -n "$cwd_arg" ]]; then
      test_args+=(--cwd "$cwd_arg")
    else
      echo "codex flow usually needs --cwd /abs/project/path" >&2
    fi
    ;;
  hermes)
    plugin_path="${plugin_workspace}/prism-plugin-hermes"
    test_args=(--path "$plugin_path" --json)
    if [[ -n "$cwd_arg" ]]; then
      test_args+=(--cwd "$cwd_arg")
    fi
    ;;
  openclaw)
    plugin_path="${plugin_workspace}/prism-plugin-openclaw"
    test_args=(--path "$plugin_path" --json)
    ;;
  *)
    echo "Unsupported plugin: $plugin" >&2
    usage
    exit 2
    ;;
esac

echo "==> Installing prism locally"
go install ./cmd/prism

echo "==> Logging into Prism"
prism login

echo "==> Installing plugin: $plugin_path"
prism plugin install "$plugin_path"

echo "==> Testing plugin"
prism plugin test "${test_args[@]}"

echo
echo "Next:"
echo "  prism daemon"
