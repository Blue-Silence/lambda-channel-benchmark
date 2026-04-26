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
CLOUDLAB_CONFIG="${CLOUDLAB_CONFIG:-cloudlab/.config/cloudlab.ini}"
AWS_GC_WORKERS="${AWS_GC_WORKERS:-16}"
AWS_GC_BUCKET_PREFIXES=(
  "lcbench-blob-put"
)
AWS_GC_TABLE_PREFIXES=(
  "lcbench_blob_put_meta"
  "lcbench_blob_put_holders"
  "lcbench-metadata-"
)

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

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    printf 'missing command in PATH: %s\n' "${name}" >&2
    exit 1
  fi
}

append_aws_gc_args() {
  AWS_GC_ARGS=("--config" "${CLOUDLAB_CONFIG}")
  for prefix in "${AWS_GC_BUCKET_PREFIXES[@]}"; do
    AWS_GC_ARGS+=("--bucket-prefix" "${prefix}")
  done
  for prefix in "${AWS_GC_TABLE_PREFIXES[@]}"; do
    AWS_GC_ARGS+=("--table-prefix" "${prefix}")
  done
  AWS_GC_ARGS+=("--workers" "${AWS_GC_WORKERS}")
}

run_aws_gc_empty_only() {
  log "running AWS GC preflight: delete empty prefixed resources only"
  local AWS_GC_ARGS
  append_aws_gc_args
  "${PYTHON}" cloudlab/scripts/entrypoints/gc_aws_resources.py \
    "${AWS_GC_ARGS[@]}" \
    --s3-mode empty-only \
    --yes
}

run_aws_gc_force() {
  log "running AWS GC finalizer: force-delete prefixed resources"
  local AWS_GC_ARGS
  append_aws_gc_args
  "${PYTHON}" cloudlab/scripts/entrypoints/gc_aws_resources.py \
    "${AWS_GC_ARGS[@]}" \
    --s3-mode force \
    --yes
}
