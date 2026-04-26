#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_file "cloudlab/.generated/nodes.ini"
require_executable "${LC_BENCH}"
require_command "aws"

experiments=(
  "config/experiments/blob/put.toml"
  "config/experiments/blob/put-s3.toml"
  "config/experiments/blob/put-p2p.toml"
)

if [[ "${LC_BENCH_SKIP_AWS_GC:-0}" != "1" ]]; then
  run_aws_gc_empty_only
  trap 'run_aws_gc_force || log "AWS GC finalizer failed; rerun gc_aws_resources.py manually"' EXIT
else
  log "skipping AWS GC because LC_BENCH_SKIP_AWS_GC=1"
fi

for experiment in "${experiments[@]}"; do
  log "running local proxy against CloudLab node for ${experiment}"
  "${PYTHON}" cloudlab/scripts/entrypoints/run_proxy_experiment.py \
    --binary "${LC_BENCH}" \
    --experiment "${experiment}"
done

log "all blob put experiments complete"
