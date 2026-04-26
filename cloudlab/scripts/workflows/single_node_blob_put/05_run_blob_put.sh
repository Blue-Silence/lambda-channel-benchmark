#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"
require_executable "${LC_BENCH}"

log "running local proxy against CloudLab node for ${EXPERIMENT_TOML}"
"${PYTHON}" cloudlab/scripts/entrypoints/run_proxy_experiment.py \
  --binary "${LC_BENCH}" \
  --experiment "${EXPERIMENT_TOML}"
log "blob put experiment complete"
