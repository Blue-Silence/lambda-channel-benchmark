#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"

log "stopping remote lc-bench node daemon"
"${PYTHON}" cloudlab/scripts/entrypoints/kill_expr_servers.py
log "remote node stopped"
