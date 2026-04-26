#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

log "checking local workflow prerequisites"
require_file "cloudlab/.config/cloudlab.ini"
require_file "cloudlab/.config/allocate.ini"
require_file "cloudlab/.secrets/cloudlab.jwt"
require_file "${EXPERIMENT_TOML}"
require_executable "${PYTHON}"
require_executable "${PORTAL_CLI}"

require_config_value "cloudlab/.config/cloudlab.ini" "remote_instances_file" "${REMOTE_INSTANCES_FILE}"
require_config_value "cloudlab/.config/allocate.ini" "portal_cli" "${PORTAL_CLI}"

"${PORTAL_CLI}" --help >/dev/null

log "running Rust tests"
cargo test

if [[ ! -x "${LC_BENCH}" ]]; then
  log "release binary missing; building"
  cargo build --release
else
  log "release binary exists: ${LC_BENCH}"
fi

log "local check complete"
