#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

cd "${PROJECT_ROOT}"

PYTHON="${PROJECT_ROOT}/.venv/bin/python"
PORTAL_CLI="${PROJECT_ROOT}/.venv/bin/portal-cli"
LC_BENCH="${PROJECT_ROOT}/target/release/lc-bench"

EXPERIMENT_TOML="${PROJECT_ROOT}/config/experiments/blob/put.toml"
REMOTE_INSTANCES_FILE="/local/cloudlab-workspace/config/instances/single-node.toml"

log() {
  printf '[single-node-blob-put] %s\n' "$*"
}

require_file() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    printf 'missing file: %s\n' "${path}" >&2
    exit 1
  fi
}

require_executable() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    printf 'missing executable: %s\n' "${path}" >&2
    exit 1
  fi
}

require_config_value() {
  local file="$1"
  local key="$2"
  local expected="$3"
  if ! grep -Eq "^[[:space:]]*${key}[[:space:]]*=[[:space:]]*${expected//\//\\/}[[:space:]]*$" "${file}"; then
    printf 'expected %s to contain: %s = %s\n' "${file}" "${key}" "${expected}" >&2
    exit 1
  fi
}
