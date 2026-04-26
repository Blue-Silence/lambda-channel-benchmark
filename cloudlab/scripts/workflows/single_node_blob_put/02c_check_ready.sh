#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"

log "checking CloudLab experiment/node readiness"
"${PYTHON}" cloudlab/scripts/entrypoints/check_experiment_ready.py "$@"
