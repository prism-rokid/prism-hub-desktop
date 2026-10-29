#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_conversation_flow.sh codex --conversation conv-1 --cwd /abs/project/path --message "hello" [--message2 "continue"]
  ./run_conversation_flow.sh hermes --conversation conv-1 --cwd /abs/project/path --message "hello" [--message2 "continue"]
  ./run_conversation_flow.sh openclaw --conversation conv-1 --message "hello" [--message2 "continue"]

What it does:
  1. Ensures `prism` is installed locally
  2. Sends a first local plugin test message with a fixed conversation id
  3. Optionally sends a second message with the same conversation id
  4. Prints the native session ids so you can verify whether the conversation was resumed

Notes:
  - This validates local plugin/session continuity.
  - It does not inject a real message into Prism Realtime; the actual glasses path still needs `prism daemon`.
EOF
}

resolve_installed_prism() {
  local gobin
  gobin="$(go env GOBIN)"
  if [[ -n "$gobin" ]]; then
    printf '%s\n' "$gobin/prism"
    return
  fi
  printf '%s\n' "$(go env GOPATH)/bin/prism"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

plugin="$1"
shift

conversation=""
cwd_arg=""
message1=""
message2=""
timeout_arg="60s"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --conversation)
      conversation="${2:-}"
      shift 2
      ;;
    --cwd)
      cwd_arg="${2:-}"
      shift 2
      ;;
    --message)
      message1="${2:-}"
      shift 2
      ;;
    --message2)
      message2="${2:-}"
      shift 2
      ;;
    --timeout)
      timeout_arg="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$conversation" ]]; then
  echo "--conversation is required" >&2
  exit 2
fi

if [[ -z "$message1" ]]; then
  echo "--message is required" >&2
  exit 2
fi

plugin_workspace="${PRISM_PLUGIN_WORKSPACE:-$(cd "$(dirname "$0")/.." && pwd)}"

if [[ -n "$cwd_arg" ]]; then
  mkdir -p "$cwd_arg"
fi

case "$plugin" in
  codex)
    plugin_path="${plugin_workspace}/prism-plugin-codex"
    if [[ -z "$cwd_arg" ]]; then
      echo "codex requires --cwd /abs/project/path" >&2
      exit 2
    fi
    extra_args=(--cwd "$cwd_arg")
    ;;
  hermes)
    plugin_path="${plugin_workspace}/prism-plugin-hermes"
    if [[ -z "$cwd_arg" ]]; then
      echo "hermes requires --cwd /abs/project/path" >&2
      exit 2
    fi
    extra_args=(--cwd "$cwd_arg" --timeout "$timeout_arg")
    ;;
  openclaw)
    plugin_path="${plugin_workspace}/prism-plugin-openclaw"
    extra_args=(--timeout "$timeout_arg")
    ;;
  *)
    echo "Unsupported plugin: $plugin" >&2
    usage
    exit 2
    ;;
esac

run_test() {
  local msg="$1"
  "$prism_bin" plugin test \
    --path "$plugin_path" \
    --conversation "$conversation" \
    --message "$msg" \
    "${extra_args[@]}" \
    --json
}

extract_native_session() {
  sed -n 's/.*"native_session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1
}

echo "==> Installing prism locally"
go install ./cmd/prism
prism_bin="$(resolve_installed_prism)"
if [[ ! -x "$prism_bin" ]]; then
  echo "installed prism not found at $prism_bin" >&2
  exit 1
fi

echo "==> First message"
first_json="$(run_test "$message1")"
printf '%s\n' "$first_json"
first_session="$(printf '%s\n' "$first_json" | extract_native_session)"
echo "first_native_session_id=${first_session:-<empty>}"

if [[ -n "$message2" ]]; then
  echo
  echo "==> Second message (same conversation id)"
  second_json="$(run_test "$message2")"
  printf '%s\n' "$second_json"
  second_session="$(printf '%s\n' "$second_json" | extract_native_session)"
  echo "second_native_session_id=${second_session:-<empty>}"
  if [[ -n "$first_session" && -n "$second_session" && "$first_session" == "$second_session" ]]; then
    echo "conversation_resumed=true"
  else
    echo "conversation_resumed=false"
  fi
fi
