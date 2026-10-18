#!/usr/bin/env bash
# Reproducible benchmark of the controller's hand-off latencies.
#
# Usage:  bench/run.sh <label> [rounds]
#
# Run it against a cluster where the controller and MinIO are already deployed
# (see the README); whatever controller image is deployed at that moment is what
# gets measured, <label> just names the result files (bench/results/<label>*).
# bench/README.md describes how to compare two versions.
#
# Scenarios (every job name is prefixed with the label, so runs never collide):
#   dag      <rounds> chains of 6 jobs; measures upstream DONE -> downstream SUBMITTED
#   evict    <rounds> times: two big + one small batch job running, then a realtime job
#            that needs an eviction; measures EVICTED -> KILL_CONFIRMED and READY -> RUNNING
#   handoff  one root job and $CHILDREN (default 60) children of 2000m each on a full
#            cluster; measures worker exit -> next job SUBMITTED, i.e. how fast a freed slot
#            is handed to a waiting job
#
# Requirements: kubectl pointing at the cluster, an 8-CPU node (the sizes below assume it).
set -uo pipefail

LABEL=${1:?usage: bench/run.sh <label> [rounds]}
ROUNDS=${2:-20}
CHILDREN=${CHILDREN:-60}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$HERE/results}
mkdir -p "$OUT"

# job <name> <priority> <cpu> <memory> <duration-seconds> [dependency]
job() {
  local deps=""
  [ -n "${6:-}" ] && deps="$6"
  printf 'apiVersion: pipeline.io/v1\nkind: PipelineJob\nmetadata: {name: %s, namespace: default}\n' "$1"
  printf 'spec: {priority: %s, requestCPU: "%s", requestMemory: "%s", storageMarker: "s3://test-bucket/%s/_SUCCESS", timeoutSeconds: 900, jobDurationSeconds: %s, stage: "background", dependencies: [%s]}\n---\n' \
    "$2" "$3" "$4" "$1" "$5" "$deps"
}

state_of() { kubectl get pipelinejob "$1" -o jsonpath='{.status.state}' 2>/dev/null; }

wait_state() { # <name> <timeout-seconds> <state>...
  local name=$1 timeout=$2 n=0 st; shift 2
  while true; do
    st=$(state_of "$name")
    for want in "$@"; do [ "$st" = "$want" ] && return 0; done
    sleep 1; n=$((n + 1))
    [ $n -ge "$timeout" ] && { echo "WARN: $name did not reach $* within ${timeout}s (state=$st)" >&2; return 1; }
  done
}

wait_no_workers() {
  local n=0
  while [ "$(kubectl get pods --no-headers 2>/dev/null | grep -c '^job-')" != "0" ]; do
    sleep 2; n=$((n + 1)); [ $n -gt 120 ] && { echo "WARN: worker pods still present" >&2; return; }
  done
}

cleanup() { kubectl delete pipelinejobs --all >/dev/null 2>&1; wait_no_workers; }

dag() {
  echo "== dag: $ROUNDS chains" >&2
  local r i dep
  for r in $(seq 1 "$ROUNDS"); do
    {
      for i in 1 2 3 4 5 6; do
        dep=""; [ $i -gt 1 ] && dep="d-$LABEL-r$r-c$((i - 1))"
        job "d-$LABEL-r$r-c$i" batch 100m 128Mi 1 "$dep"
      done
    } | kubectl apply -f - >/dev/null
    wait_state "d-$LABEL-r$r-c6" 180 DONE
    cleanup
  done
}

evict() {
  echo "== evict: $ROUNDS rounds" >&2
  local r p
  for r in $(seq 1 "$ROUNDS"); do
    p="e-$LABEL-r$r"
    { job "$p-small" batch 200m 256Mi 300; job "$p-a" batch 3000m 3Gi 300; job "$p-b" batch 3000m 2Gi 300; } | kubectl apply -f - >/dev/null
    wait_state "$p-small" 90 RUNNING; wait_state "$p-a" 90 RUNNING; wait_state "$p-b" 90 RUNNING
    job "$p-urgent" realtime 3000m 1Gi 5 | kubectl apply -f - >/dev/null
    wait_state "$p-urgent" 180 RUNNING DONE
    cleanup
  done
}

handoff() {
  echo "== handoff: 1 root + $CHILDREN children" >&2
  local k name
  {
    job "h-$LABEL-root" batch 100m 128Mi 1
    for k in $(seq 1 "$CHILDREN"); do job "h-$LABEL-c$k" batch 2000m 256Mi 2 "h-$LABEL-root"; done
  } | kubectl apply -f - >/dev/null
  wait_state "h-$LABEL-c$CHILDREN" 900 DONE
  # The last child finishing does not mean every child finished; wait for all of them.
  for k in $(seq 1 "$CHILDREN"); do wait_state "h-$LABEL-c$k" 300 DONE; done
  : > "$OUT/$LABEL-handoff.txt"
  for k in $(seq 1 "$CHILDREN"); do
    name="h-$LABEL-c$k"
    ms=$(kubectl logs "job/job-$name" 2>/dev/null | sed -n 's/^worker_exit_ms=//p' | tail -1)
    [ -n "$ms" ] && echo "$name $ms" >> "$OUT/$LABEL-handoff.txt"
  done
  cleanup
}

cleanup
dag
evict
handoff
kubectl logs deploy/dag-pipeline-controller --tail=-1 > "$OUT/$LABEL.log"
echo "wrote $OUT/$LABEL.log and $OUT/$LABEL-handoff.txt" >&2
