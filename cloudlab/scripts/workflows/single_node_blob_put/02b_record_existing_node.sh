#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ -z "${CLOUDLAB_HOST:-}" ]]; then
  cat >&2 <<'EOF'
Set CLOUDLAB_HOST to the public SSH hostname of an already allocated CloudLab node.

Example:
  CLOUDLAB_HOST=pc123.utah.cloudlab.us \
    cloudlab/scripts/workflows/single_node_blob_put/02b_record_existing_node.sh

Optional:
  CLOUDLAB_USER=Finch
  CLOUDLAB_EXPERIMENT=lc-test
EOF
  exit 2
fi

CLOUDLAB_USER="${CLOUDLAB_USER:-Finch}"
CLOUDLAB_EXPERIMENT="${CLOUDLAB_EXPERIMENT:-lc-test}"

log "recording existing CloudLab node ${CLOUDLAB_USER}@${CLOUDLAB_HOST}"
"${PYTHON}" cloudlab/scripts/entrypoints/record_single.py \
  --experiment "${CLOUDLAB_EXPERIMENT}" \
  --host "${CLOUDLAB_HOST}" \
  --user "${CLOUDLAB_USER}" \
  --node-name node-0
log "nodes written to cloudlab/.generated/nodes.ini"
