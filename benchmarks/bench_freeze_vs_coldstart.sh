#!/usr/bin/env bash
# bench_freeze_vs_coldstart.sh
#
# Self-contained benchmark of CRIU thaw vs Knative cold-start latency.
#
# This script handles every prerequisite automatically:
#   1. Saves and later restores the its-sniffer EdgeApplication (if present)
#      — its events would otherwise keep the retransmitter warm and
#      prevent scale-to-zero.
#   2. Saves and restores the retransmitter EdgeApplication's
#      freezeEnabled / minScale / nodeSelector so the benchmark can:
#        a) target a cgroup-v2 worker (required for CRIU), and
#        b) toggle freezeEnabled between phases.
#   3. Spawns an in-cluster curl pod so events can reach both the pod
#      IP (for the CRIU thaw, which bypasses routing) and the ksvc URL
#      (for the cold start).
#   4. Runs ITERATIONS CRIU thaw measurements, then ITERATIONS cold
#      start measurements, writing NDJSON rows to OUTFILE.
#   5. Analyzes the results inline (python3 — stdlib only) and prints
#      a comparison report.
#
# Usage:
#   ./bench_freeze_vs_coldstart.sh [ITERATIONS]
# Default: 25 iterations per mode.

set -uo pipefail

ITERATIONS="${1:-25}"
NAMESPACE="default"
SERVICE_NAME="retransmitter"
EA_NAME="retransmitter"
SNIFFER_NAME="its-sniffer"
# cgroup-v2 worker where CRIU checkpoint is supported
BENCH_NODE_SELECTOR_KEY="vm-id"
BENCH_NODE_SELECTOR_VAL="worker-1"
SERVICE_URL="http://retransmitter.default.svc.cluster.local"
IDLE_TIMEOUT=30
QUEUE_PROXY_PORT=8012

OUTDIR="$(cd "$(dirname "$0")" && pwd)/benchlogs"
mkdir -p "$OUTDIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$OUTDIR/freeze_vs_coldstart_${TIMESTAMP}.ndjson"

SNIFFER_BACKUP=""
EA_BACKUP=""

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# ---- state save/restore ------------------------------------------------------

save_state() {
    log "Saving current state..."
    if kubectl get edgeapplication "$SNIFFER_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
        SNIFFER_BACKUP=$(kubectl get edgeapplication "$SNIFFER_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null)
        log "  Saved $SNIFFER_NAME EdgeApplication."
    fi
    EA_BACKUP=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null || echo "")
    if [[ -n "$EA_BACKUP" ]]; then
        log "  Saved $EA_NAME EdgeApplication."
    else
        log "  ERROR: $EA_NAME EdgeApplication not found."
        exit 1
    fi
}

restore_state() {
    log "Restoring original state..."
    if [[ -n "$EA_BACKUP" ]]; then
        # Strip status/resourceVersion so apply replays cleanly
        echo "$EA_BACKUP" | python3 -c "
import sys, yaml
d = yaml.safe_load(sys.stdin)
d.pop('status', None)
d.get('metadata', {}).pop('resourceVersion', None)
d.get('metadata', {}).pop('uid', None)
d.get('metadata', {}).pop('generation', None)
d.get('metadata', {}).pop('creationTimestamp', None)
print(yaml.safe_dump(d))
" 2>/dev/null | kubectl apply -f - >/dev/null 2>&1 || \
            log "  WARNING: failed to restore $EA_NAME"
        log "  Restored $EA_NAME EdgeApplication."
    fi
    if [[ -n "$SNIFFER_BACKUP" ]]; then
        echo "$SNIFFER_BACKUP" | python3 -c "
import sys, yaml
d = yaml.safe_load(sys.stdin)
d.pop('status', None)
d.get('metadata', {}).pop('resourceVersion', None)
d.get('metadata', {}).pop('uid', None)
d.get('metadata', {}).pop('generation', None)
d.get('metadata', {}).pop('creationTimestamp', None)
print(yaml.safe_dump(d))
" 2>/dev/null | kubectl apply -f - >/dev/null 2>&1 || \
            log "  WARNING: failed to restore $SNIFFER_NAME"
        log "  Restored $SNIFFER_NAME EdgeApplication."
    fi
}

cleanup() {
    log "Cleanup..."
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    restore_state
}
trap cleanup EXIT

# ---- curl pod ---------------------------------------------------------------

setup_curl_pod() {
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2
    # 6h lifetime is plenty for even 100 iterations of both phases
    kubectl run bench-curl -n "$NAMESPACE" --image=curlimages/curl \
        --restart=Never --command -- sleep 21600 >/dev/null 2>&1
    log "Waiting for bench-curl pod..."
    kubectl wait --for=condition=Ready pod/bench-curl -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1
    log "bench-curl pod ready."
}

send_event_internal() {
    local url="$1"
    local out
    out=$(kubectl exec bench-curl -n "$NAMESPACE" -- \
        curl -s -o /dev/null \
        -w '%{time_namelookup},%{time_connect},%{time_starttransfer},%{time_total},%{http_code}' \
        -X POST "$url" \
        -H "Content-Type: application/json" \
        -H "Ce-Id: bench-$(date +%s%N)" \
        -H "Ce-Specversion: 1.0" \
        -H "Ce-Type: its.cam" \
        -H "Ce-Source: benchmark" \
        -H "Host: retransmitter.default.svc.cluster.local" \
        -d '{"benchmark":true}' \
        --max-time 120 2>&1)
    if [[ -z "$out" || "$out" != *,* ]]; then
        log "  send_event error: $out"
        echo "0,0,0,0,0"
    else
        echo "$out"
    fi
}

# ---- pod helpers ------------------------------------------------------------

get_pod_name() {
    # Return the name of a Running pod whose template has the freezer
    # annotation. During preflight/patch cycles there can be leftover pods
    # from an older revision without the plugin; we must ignore those.
    kubectl get pods -n "$NAMESPACE" -l "serving.knative.dev/service=$SERVICE_NAME" \
        --field-selector=status.phase=Running \
        -o jsonpath='{range .items[?(@.metadata.annotations.qpoption\.knative\.dev/freezer-activate=="enable")]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
        | head -1
}

get_pod_ip() {
    kubectl get pod "$1" -n "$NAMESPACE" -o jsonpath='{.status.podIP}' 2>/dev/null
}

wait_for_pod_ready() {
    log "  Waiting for pod 2/2 Ready (freezer-enabled revision)..."
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local pod
        pod=$(get_pod_name)
        if [[ -n "$pod" ]]; then
            local ready_count
            ready_count=$(kubectl get pod "$pod" -n "$NAMESPACE" \
                -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null \
                | tr ' ' '\n' | grep -c '^true$' || echo 0)
            ready_count=$(echo "$ready_count" | head -1)
            if [[ "$ready_count" -ge 2 ]] 2>/dev/null; then
                log "  Pod 2/2 Ready ($pod)."
                return 0
            fi
        fi
        sleep 2
    done
    log "  WARNING: pod not 2/2 ready within 120s"
    return 1
}

wait_for_freeze() {
    local pod="$1"
    local wait_time=$((IDLE_TIMEOUT + 20))
    log "  Waiting up to ${wait_time}s for freeze..."
    local deadline=$((SECONDS + wait_time))
    while (( SECONDS < deadline )); do
        local seen
        seen=$(kubectl logs "$pod" -n "$NAMESPACE" -c queue-proxy --tail=5 2>/dev/null \
            | grep -c 'fake listener started' || echo 0)
        seen=$(echo "$seen" | head -1)
        if [[ "$seen" -gt 0 ]] 2>/dev/null; then
            # Make sure we haven't already thawed
            local thawed
            thawed=$(kubectl logs "$pod" -n "$NAMESPACE" -c queue-proxy --tail=3 2>/dev/null \
                | grep -c 'thawing' || echo 0)
            thawed=$(echo "$thawed" | head -1)
            if [[ "$thawed" -eq 0 ]] 2>/dev/null; then
                log "  Container frozen."
                return 0
            fi
        fi
        sleep 2
    done
    log "  WARNING: freeze not confirmed"
    return 1
}

wait_for_scale_to_zero() {
    log "  Waiting for scale-to-zero (no running pods)..."
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        local running
        running=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$SERVICE_NAME" \
            --no-headers 2>/dev/null | wc -l)
        if [[ "$running" -eq 0 ]]; then
            log "  Scaled to zero."
            return 0
        fi
        sleep 3
    done
    log "  WARNING: scale-to-zero not reached within 180s"
    return 1
}

delete_retransmitter_pod() {
    local pod
    pod=$(get_pod_name)
    if [[ -n "$pod" ]]; then
        kubectl delete pod "$pod" -n "$NAMESPACE" --grace-period=1 >/dev/null 2>&1 || true
        sleep 2
    fi
}

# ---- result emission --------------------------------------------------------

emit_result() {
    local mode="$1" iter="$2" timings="$3" ts_before="$4" ts_after="$5"
    IFS=',' read -r t_dns t_connect t_ttfb t_total http_code <<< "$timings"
    printf '{"mode":"%s","iteration":%d,"t_dns_s":%s,"t_connect_s":%s,"t_ttfb_s":%s,"t_total_s":%s,"http_code":%s,"ts_before":"%s","ts_after":"%s"}\n' \
        "$mode" "$iter" "$t_dns" "$t_connect" "$t_ttfb" "$t_total" "$http_code" "$ts_before" "$ts_after" \
        >> "$OUTFILE"
}

# ---- preflight --------------------------------------------------------------

wait_for_freezer_annotation() {
    log "  Waiting for operator to propagate freezer annotation to ksvc..."
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        local ann
        ann=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.spec.template.metadata.annotations.qpoption\.knative\.dev/freezer-activate}' 2>/dev/null)
        if [[ "$ann" == "enable" ]]; then
            log "  Freezer annotation present on ksvc."
            return 0
        fi
        sleep 2
    done
    log "  WARNING: freezer annotation never appeared on ksvc"
    return 1
}

preflight() {
    log "Preflight: ensuring retransmitter targets $BENCH_NODE_SELECTOR_KEY=$BENCH_NODE_SELECTOR_VAL (cgroup v2)"
    # Replace nodeSelector entirely; the original is saved in EA_BACKUP.
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"replace\",\"path\":\"/spec/service/nodeSelector\",\"value\":{\"$BENCH_NODE_SELECTOR_KEY\":\"$BENCH_NODE_SELECTOR_VAL\"}}]" \
        >/dev/null 2>&1 || \
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"add\",\"path\":\"/spec/service/nodeSelector\",\"value\":{\"$BENCH_NODE_SELECTOR_KEY\":\"$BENCH_NODE_SELECTOR_VAL\"}}]" \
        >/dev/null 2>&1

    # Phase 1: freezeEnabled=true. The operator now automatically forces
    # minScale>=1 whenever freezeEnabled is true (CRIU replaces
    # scale-to-zero as the idle reclamation mechanism), so we don't need
    # to set minScale here. Clear any leftover minScale from a prior
    # aborted run to exercise that path.
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"freezeEnabled":true,"minScale":null}}}' >/dev/null 2>&1

    log "Deleting its-sniffer (stops its.cam/its.denm events from keeping retransmitter warm)"
    kubectl delete edgeapplication "$SNIFFER_NAME" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    # give kube time to tear down
    sleep 10

    # Wait for the operator to propagate the freezer annotation, otherwise
    # the first iterations would target old-revision pods that don't have
    # the freezer plugin loaded.
    wait_for_freezer_annotation
    # Force a fresh pod from the latest revision before phase 1.
    delete_retransmitter_pod
}

# ---- CRIU thaw benchmark ----------------------------------------------------

run_criu_thaw_benchmark() {
    log "=== PHASE 1: CRIU THAW ($ITERATIONS iterations) ==="
    log "Sends directly to pod IP:$QUEUE_PROXY_PORT (bypasses endpoint exclusion of not-Ready pods)"

    local i pod pod_ip ts_before ts_after timings
    for (( i=1; i<=ITERATIONS; i++ )); do
        log "--- CRIU thaw iteration $i/$ITERATIONS ---"

        # CRIU can only checkpoint a container once per lifetime, so each
        # iteration needs a brand-new pod.
        delete_retransmitter_pod
        wait_for_pod_ready

        pod=$(get_pod_name)
        pod_ip=$(get_pod_ip "$pod")
        if [[ -z "$pod" || -z "$pod_ip" ]]; then
            log "  ERROR: no pod/IP found, skipping"
            continue
        fi

        wait_for_freeze "$pod"

        ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        timings=$(send_event_internal "http://${pod_ip}:${QUEUE_PROXY_PORT}")
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

        IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
        log "  Result: ${t_total}s (HTTP $http_code)"
        emit_result "criu_thaw" "$i" "$timings" "$ts_before" "$ts_after"
    done
}

# ---- cold start benchmark ---------------------------------------------------

run_cold_start_benchmark() {
    log "=== PHASE 2: COLD START ($ITERATIONS iterations) ==="
    log "Patching EdgeApplication: freezeEnabled=false, minScale=0"
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"freezeEnabled":false,"minScale":0}}}' >/dev/null 2>&1
    sleep 10

    local i ts_before ts_after timings
    for (( i=1; i<=ITERATIONS; i++ )); do
        log "--- Cold start iteration $i/$ITERATIONS ---"

        # Delete any running pod so scale-to-zero isn't gated by
        # whatever pod was brought up by the previous iteration.
        delete_retransmitter_pod
        wait_for_scale_to_zero
        sleep 3  # endpoint reprogramming

        ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        timings=$(send_event_internal "$SERVICE_URL")
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

        IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
        log "  Result: ${t_total}s (HTTP $http_code)"
        emit_result "cold_start" "$i" "$timings" "$ts_before" "$ts_after"
    done
}

# ---- inline analysis --------------------------------------------------------

analyze() {
    log "Analyzing results..."
    python3 - "$OUTFILE" <<'PY'
import json, math, statistics, sys

path = sys.argv[1]
rows = []
with open(path) as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass

def stats(vals):
    if not vals:
        n = float("nan")
        return dict(n=0, mean=n, median=n, stdev=n, p95=n, p99=n, min=n, max=n)
    s = sorted(vals)
    n = len(s)
    return dict(
        n=n,
        mean=statistics.fmean(s),
        median=statistics.median(s),
        stdev=statistics.stdev(s) if n > 1 else 0.0,
        p95=s[int(n * 0.95)] if n > 1 else s[-1],
        p99=s[int(n * 0.99)] if n > 1 else s[-1],
        min=s[0],
        max=s[-1],
    )

def fmt(x):
    return "n/a" if isinstance(x, float) and math.isnan(x) else f"{x:.1f} ms"

def print_block(label, st):
    print(f"  {label}:")
    for k in ("n", "mean", "median", "stdev", "p95", "p99", "min", "max"):
        v = st[k]
        val = str(v) if k == "n" else fmt(v)
        print(f"    {k:<7} = {val}")

criu = [r["t_total_s"] * 1000 for r in rows if r.get("mode") == "criu_thaw"  and r.get("t_total_s", 0) > 0]
cold = [r["t_total_s"] * 1000 for r in rows if r.get("mode") == "cold_start" and r.get("t_total_s", 0) > 0]

c_st = stats(criu)
k_st = stats(cold)

print()
print("=" * 60)
print("  CRIU Thaw vs Cold Start — Benchmark Results")
print("=" * 60)
print(f"  CRIU thaw samples:  {c_st['n']}")
print(f"  Cold start samples: {k_st['n']}")
print()
print("-" * 60)
print("  Total Response Time")
print("-" * 60)
print_block("CRIU Thaw", c_st)
print()
print_block("Cold Start", k_st)
print()
if c_st["n"] and k_st["n"] and c_st["mean"] > 0 and k_st["mean"] > 0:
    speedup = k_st["mean"] / c_st["mean"]
    diff = k_st["mean"] - c_st["mean"]
    print("  Comparison:")
    print(f"    CRIU thaw is {speedup:.1f}x faster than cold start")
    print(f"    Mean difference: {diff:.0f} ms")
    print(f"    Medians: CRIU {c_st['median']:.0f} ms  vs  Cold {k_st['median']:.0f} ms")
    print()

codes_c, codes_k = {}, {}
for r in rows:
    c = str(r.get("http_code", "?"))
    if r.get("mode") == "criu_thaw":
        codes_c[c] = codes_c.get(c, 0) + 1
    elif r.get("mode") == "cold_start":
        codes_k[c] = codes_k.get(c, 0) + 1
print("-" * 60)
print("  HTTP Status Codes")
print("-" * 60)
print(f"  CRIU thaw:  {codes_c}")
print(f"  Cold start: {codes_k}")
print()

print("-" * 60)
print("  Per-Iteration (ms)")
print("-" * 60)
print(f"  {'#':>3}  {'CRIU thaw':>12}  {'Cold start':>12}")
print(f"  {'':>3}  {'-'*12}  {'-'*12}")
m = max(len(criu), len(cold))
for i in range(m):
    a = f"{criu[i]:.1f}" if i < len(criu) else "-"
    b = f"{cold[i]:.1f}" if i < len(cold) else "-"
    print(f"  {i+1:3d}  {a:>12}  {b:>12}")
print()
PY
}

# ---- main -------------------------------------------------------------------

main() {
    log "Benchmark: CRIU thaw vs cold start"
    log "Iterations per mode: $ITERATIONS"
    log "Output: $OUTFILE"
    echo

    save_state
    preflight
    setup_curl_pod

    run_criu_thaw_benchmark
    echo
    run_cold_start_benchmark
    echo

    analyze
    log "Done. Raw results: $OUTFILE"
}

main
