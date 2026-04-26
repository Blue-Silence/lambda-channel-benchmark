#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

log "creating CloudLab source bundle"
"${PYTHON}" cloudlab/scripts/entrypoints/package.py
log "package complete"
