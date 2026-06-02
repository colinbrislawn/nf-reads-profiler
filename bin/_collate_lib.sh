#!/usr/bin/env bash
# Shared helpers for the collate_* table-reduce scripts. Source, don't execute:
#   source "$(dirname "${BASH_SOURCE[0]}")/_collate_lib.sh"
#
# Memory tracking: container RSS counts toward host MemUsed, so sampling
# `free`'s used column captures a collate run's total memory high-water without
# any per-image tooling. Useful for sizing these reduces at scale (e.g. the
# Diversigen 1440-sample HUMAnN join) and for de-risking the in-pipeline
# (Route B) combines, which run the same tools.

# start_mem_sampler <tag>: begin sampling host MemUsed (MiB) every 3s in the bg.
start_mem_sampler() {
  _MEM_TAG="${1:-collate}"
  _MEM_LOG="$(mktemp)"
  ( while :; do free -m | awk '/^Mem:/{print $3}'; sleep 3; done ) >"$_MEM_LOG" 2>/dev/null &
  _MEM_PID=$!
}

# report_mem: stop the sampler and print baseline/peak/delta MemUsed.
report_mem() {
  [ -n "${_MEM_PID:-}" ] || return 0
  kill "$_MEM_PID" 2>/dev/null || true
  local base peak
  base=$(head -1 "$_MEM_LOG" 2>/dev/null); base=${base:-0}
  peak=$(sort -n "$_MEM_LOG" 2>/dev/null | tail -1); peak=${peak:-0}
  echo "[$_MEM_TAG][mem] host MemUsed baseline=${base}MiB peak=${peak}MiB delta=$((peak-base))MiB"
  rm -f "$_MEM_LOG"; _MEM_PID=""
}
