#!/usr/bin/env bash
set -euo pipefail

PRISM_INSTALL_REF="${PRISM_INSTALL_REF:-main}"
PRISM_INSTALL_TARBALL_URL="${PRISM_INSTALL_TARBALL_URL:-}"
PRISM_INSTALL_REPO_URL="${PRISM_INSTALL_REPO_URL:-}"
PRISM_INSTALL_GO_MODULE="${PRISM_INSTALL_GO_MODULE:-}"

WORK_DIR=""

cleanup() {
  if [[ -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "${cmd} is required but not found in PATH" >&2
    exit 1
  fi
}

looks_like_repo_root() {
  local dir="$1"
  [[ -f "${dir}/go.mod" && -f "${dir}/cmd/prism/main.go" ]]
}

resolve_local_repo_root() {
  local source="${BASH_SOURCE[0]:-}"
  if [[ -z "${source}" || "${source}" == "bash" ]]; then
    return 1
  fi
  local script_dir
  script_dir="$(cd "$(dirname "${source}")" && pwd)"
  local repo_root
  repo_root="$(cd "${script_dir}/.." && pwd)"
  if looks_like_repo_root "${repo_root}"; then
    printf '%s\n' "${repo_root}"
    return 0
  fi
  return 1
}

find_repo_root_under() {
  local search_root="$1"
  while IFS= read -r candidate; do
    if looks_like_repo_root "${candidate}"; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done < <(find "${search_root}" -type f -name go.mod -print | sed 's#/go.mod$##')
  return 1
}

install_from_repo() {
  local repo_root="$1"
  echo "==> installing prism from source"
  echo "repo: ${repo_root}"
  cd "${repo_root}"
  go install ./cmd/prism
}

install_from_tarball() {
  local tarball_url="$1"
  require_cmd curl
  require_cmd tar
  WORK_DIR="$(mktemp -d)"
  local archive_path="${WORK_DIR}/prism.tar.gz"
  echo "==> downloading prism source archive"
  echo "archive: ${tarball_url}"
  curl -fsSL "${tarball_url}" -o "${archive_path}"
  tar -xzf "${archive_path}" -C "${WORK_DIR}"
  local repo_root
  repo_root="$(find_repo_root_under "${WORK_DIR}")" || {
    echo "failed to locate prism source root inside downloaded archive" >&2
    exit 1
  }
  install_from_repo "${repo_root}"
}

install_from_git() {
  local repo_url="$1"
  require_cmd git
  WORK_DIR="$(mktemp -d)"
  local clone_dir="${WORK_DIR}/repo"
  echo "==> cloning prism source"
  echo "repo: ${repo_url}"
  echo "ref: ${PRISM_INSTALL_REF}"
  git clone --depth 1 --branch "${PRISM_INSTALL_REF}" "${repo_url}" "${clone_dir}"
  install_from_repo "${clone_dir}"
}

install_from_module() {
  local module_path="$1"
  local version="${PRISM_INSTALL_REF}"
  if [[ -z "${version}" ]]; then
    version="latest"
  fi
  echo "==> installing prism via go module"
  echo "module: ${module_path}@${version}"
  go install "${module_path}@${version}"
}

print_success() {
  local gobin_value gopath_value bin_dir
  gobin_value="$(go env GOBIN)"
  gopath_value="$(go env GOPATH)"
  if [[ -n "${gobin_value}" ]]; then
    bin_dir="${gobin_value}"
  else
    bin_dir="${gopath_value}/bin"
  fi

  if "${bin_dir}/prism" hub start; then
    echo "Hub user service registered and start requested."
  else
    echo "warning: prism was installed, but Hub service registration/start needs attention. Run: prism hub start" >&2
  fi

  echo ""
  echo "prism installed to: ${bin_dir}/prism"
  echo ""
  echo "next steps:"
  echo "  prism login"
  echo "  prism hub start"
  echo "  prism dashboard"
  echo ""
  echo "if 'prism: command not found', add this to your shell profile:"
  echo "  export PATH=\"${bin_dir}:\$PATH\""
}

print_remote_help() {
  cat >&2 <<'EOF'
remote install source not specified.

Supported modes:
  1. Local repo:
     ./scripts/install.sh

  2. curl | bash with source archive:
     curl -fsSL <install-script-url> | PRISM_INSTALL_TARBALL_URL=<source-tarball-url> bash

  3. curl | bash with git repo:
     curl -fsSL <install-script-url> | PRISM_INSTALL_REPO_URL=<git-repo-url> PRISM_INSTALL_REF=main bash

  4. curl | bash with public go module:
     curl -fsSL <install-script-url> | PRISM_INSTALL_GO_MODULE=<module-path> PRISM_INSTALL_REF=latest bash
EOF
  exit 1
}

require_cmd go

if repo_root="$(resolve_local_repo_root)"; then
  install_from_repo "${repo_root}"
elif [[ -n "${PRISM_INSTALL_TARBALL_URL}" ]]; then
  install_from_tarball "${PRISM_INSTALL_TARBALL_URL}"
elif [[ -n "${PRISM_INSTALL_REPO_URL}" ]]; then
  install_from_git "${PRISM_INSTALL_REPO_URL}"
elif [[ -n "${PRISM_INSTALL_GO_MODULE}" ]]; then
  install_from_module "${PRISM_INSTALL_GO_MODULE}"
else
  print_remote_help
fi

print_success
