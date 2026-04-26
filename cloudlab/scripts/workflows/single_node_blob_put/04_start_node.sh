#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"
require_config_value "cloudlab/.config/cloudlab.ini" "remote_instances_file" "${REMOTE_INSTANCES_FILE}"

log "starting remote lc-bench node daemon"
"${PYTHON}" cloudlab/scripts/entrypoints/start_expr_servers.py
log "remote node started"
