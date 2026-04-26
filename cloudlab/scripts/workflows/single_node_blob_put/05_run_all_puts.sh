#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"
require_executable "${LC_BENCH}"

experiments=(
  "config/experiments/blob/put.toml"
  "config/experiments/blob/put-s3.toml"
  "config/experiments/blob/put-p2p.toml"
)

for experiment in "${experiments[@]}"; do
  log "running local proxy against CloudLab node for ${experiment}"
  "${PYTHON}" cloudlab/scripts/entrypoints/run_proxy_experiment.py \
    --binary "${LC_BENCH}" \
    --experiment "${experiment}"
done

log "all blob put experiments complete"
