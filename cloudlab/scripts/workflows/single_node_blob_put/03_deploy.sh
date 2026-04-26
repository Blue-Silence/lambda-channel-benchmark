#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"
require_file "cloudlab/.generated/package/source-bundle.tar.gz"

log "deploying package and building release binary on CloudLab node"
"${PYTHON}" cloudlab/scripts/entrypoints/deploy.py
log "deploy complete"
