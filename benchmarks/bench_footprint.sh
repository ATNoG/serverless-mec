#!/usr/bin/env bash
# bench_footprint.sh
#
# Measures the resource footprint of the management plane with the Metrics
# Server, in two conditions, both with the C-ITS pipeline deployed:
#   idle    — sniffer and retransmitter running, no traffic
#   loaded  — its-producer emitting one CAM and one DENM per second through
#             the sniffer, the Kafka-backed Broker, and the retransmitter
#
# Each condition takes N samples of `kubectl top pods -A` and
# `kubectl top nodes`, one every INTERVAL seconds, into ONE NDJSON file
# (one row per pod or node per sample). Summarize it with
# analyze_footprint.py, which is invoked automatically at the end.
#
# Usage:
#   ./bench_footprint.sh                  # 20 samples, 30 s apart, per condition
#   ./bench_footprint.sh -n 10 -i 15      # 10 samples, 15 s apart
#   ./bench_footprint.sh -k               # keep the pipeline deployed afterwards
#
# Options:
#   -n N    samples per condition (default 20)
#   -i S    seconds between samples (default 30)
#   -s S    settle time before each condition (default 120)
#   -k      keep the pipeline deployed when done (default: delete it)
#
# Prerequisites: the MEC operator, the checkpoint/restore daemon, the Kafka
# Broker, and the http-sink helper are installed; the Metrics Server is
# running (bundled with K3s).

set -uo pipefail

SAMPLES=20
INTERVAL=30
SETTLE=120
KEEP=false
while getopts "n:i:s:kh" opt; do
    case "$opt" in
        n) SAMPLES="$OPTARG" ;;
        i) INTERVAL="$OPTARG" ;;
        s) SETTLE="$OPTARG" ;;
        k) KEEP=true ;;
        h|*)
            sed -n '2,29p' "$0"
            exit 0
            ;;
    esac
done

NAMESPACE="default"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EA_DIR="$REPO_DIR/edgeApplications"
OUTDIR="$SCRIPT_DIR/footprint_logs"
mkdir -p "$OUTDIR"
OUTFILE="$OUTDIR/footprint_$(date +%Y%m%d_%H%M%S).ndjson"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Image pulls on the RSUs can take several minutes, so the waits are long.
READY_TIMEOUT="1200s"

cleanup() {
    if ! $KEEP; then
        log "Removing the pipeline..."
        kubectl delete -f "$EA_DIR/its-producer.yaml" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        kubectl delete -f "$EA_DIR/its-sniffer.yaml" -f "$EA_DIR/retransmitter.yaml" \
            --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

sample() {
    # sample <phase>: appends SAMPLES samples of pod and node metrics to OUTFILE
    local phase="$1" i ts
    for (( i = 1; i <= SAMPLES; i++ )); do
        ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        timeout 25 kubectl top pods -A --no-headers 2>/dev/null | awk -v p="$phase" -v s="$i" -v t="$ts" \
            '{cpu=$3; mem=$4; sub(/m$/,"",cpu); sub(/Mi$/,"",mem);
              printf "{\"phase\":\"%s\",\"sample\":%d,\"ts\":\"%s\",\"kind\":\"pod\",\"ns\":\"%s\",\"name\":\"%s\",\"cpu_m\":%s,\"mem_mi\":%s}\n", p,s,t,$1,$2,cpu,mem}' >> "$OUTFILE"
        timeout 25 kubectl top nodes --no-headers 2>/dev/null | awk -v p="$phase" -v s="$i" -v t="$ts" \
            '$2 !~ /unknown/ {cpu=$2; mem=$4; sub(/m$/,"",cpu); sub(/Mi$/,"",mem);
              printf "{\"phase\":\"%s\",\"sample\":%d,\"ts\":\"%s\",\"kind\":\"node\",\"name\":\"%s\",\"cpu_m\":%s,\"mem_mi\":%s}\n", p,s,t,$1,cpu,mem}' >> "$OUTFILE"
        log "  $phase sample $i/$SAMPLES"
        (( i < SAMPLES )) && sleep "$INTERVAL"
    done
}

log "output: $OUTFILE"

# ---- idle: pipeline deployed, no traffic -----------------------------------
log "Deploying the sniffer and the retransmitter..."
kubectl apply -f "$EA_DIR/its-sniffer.yaml" -f "$EA_DIR/retransmitter.yaml" >/dev/null || exit 1
for svc in its-sniffer retransmitter; do
    # The operator creates the Knative Service shortly after the EdgeApplication.
    for _ in $(seq 1 30); do kubectl get ksvc "$svc" -n "$NAMESPACE" >/dev/null 2>&1 && break; sleep 2; done
    if ! kubectl wait ksvc/"$svc" -n "$NAMESPACE" --for=condition=Ready --timeout="$READY_TIMEOUT" >/dev/null; then
        log "ERROR: $svc did not become Ready"; exit 1
    fi
done
log "Pipeline ready; settling for ${SETTLE}s..."
sleep "$SETTLE"
log "Sampling the idle condition..."
sample idle

# ---- loaded: generator running ---------------------------------------------
log "Starting the generator (its-producer)..."
kubectl apply -f "$EA_DIR/its-producer.yaml" >/dev/null || exit 1
if ! kubectl rollout status daemonset/its-producer -n "$NAMESPACE" --timeout="$READY_TIMEOUT" >/dev/null; then
    log "ERROR: its-producer did not become ready"; exit 1
fi
log "Generator running; settling for ${SETTLE}s..."
sleep "$SETTLE"
if ! kubectl logs -n "$NAMESPACE" -l serving.knative.dev/service=retransmitter -c user-container --since=60s 2>/dev/null \
        | grep -q '"component":"retransmitter"'; then
    log "WARNING: no events reached the retransmitter in the last 60 s"
fi
log "Sampling the loaded condition..."
sample loaded

log "Done."
python3 "$SCRIPT_DIR/analyze_footprint.py" "$OUTFILE"
