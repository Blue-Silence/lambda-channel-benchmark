#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

log "allocating CloudLab profile from cloudlab/.config/allocate.ini"
"${PYTHON}" cloudlab/scripts/entrypoints/allocate_profile.py
log "allocation complete; nodes written to cloudlab/.generated/nodes.ini"
