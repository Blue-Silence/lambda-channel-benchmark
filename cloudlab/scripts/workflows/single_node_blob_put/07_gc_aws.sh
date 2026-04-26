#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_command "aws"

mode="${1:-force}"
case "${mode}" in
  empty-only)
    run_aws_gc_empty_only
    ;;
  force)
    run_aws_gc_force
    ;;
  *)
    printf 'usage: %s [empty-only|force]\n' "$0" >&2
    exit 2
    ;;
esac
