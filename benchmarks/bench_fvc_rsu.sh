#!/usr/bin/env bash
# bench_fvc_rsu.sh — RSU-friendly Freeze vs Cold-start benchmark
#
# Avoids the revision storm of bench_freeze_vs_coldstart.sh by creating
# the EdgeApplication with the right config from the start for each phase,
# and doing a full EA delete+recreate between phases instead of patching.
#
# Defaults to rsu-id=rsu-a. Override with env vars for other nodes:
#   BENCH_NODE_SELECTOR_KEY=vm-id BENCH_NODE=worker-1 ./bench_fvc_rsu.sh [ITERATIONS] [LABEL]
#
# Usage:
#   ./bench_fvc_rsu.sh [ITERATIONS] [LABEL] [MODE]
#   MODE: both (default), cold_start, thaw

set -uo pipefail

ITERATIONS="${1:-3}"
LABEL="${2:-}"
BENCH_MODE="${3:-both}"
NAMESPACE="default"
SERVICE_NAME="retransmitter"
EA_NAME="retransmitter"
BENCH_NODE_SELECTOR_KEY="${BENCH_NODE_SELECTOR_KEY:-rsu-id}"
BENCH_NODE_SELECTOR_VAL="${BENCH_NODE:-rsu-a}"
SERVICE_URL="http://retransmitter.default.svc.cluster.local"
QUEUE_PROXY_PORT=8012
FREEZE_WAIT_TIMEOUT=300
CHECKPOINT_CLEANUP_INTERVAL="${CHECKPOINT_CLEANUP_INTERVAL:-100}"

# Optional: override Knative scale-to-zero timing for the cold start phase.
# If set, patches config-autoscaler before cold start and restores after.
# Value in seconds. Unset or empty = no patch.
COLD_START_SCALE_TO_ZERO="${COLD_START_SCALE_TO_ZERO:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="$SCRIPT_DIR/freeze_vs_coldstart_rsu_logs"
mkdir -p "$OUTDIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [[ -n "$LABEL" ]]; then
    OUTFILE="$OUTDIR/freeze_vs_coldstart_rsu_${LABEL}_${TIMESTAMP}.ndjson"
else
    OUTFILE="$OUTDIR/freeze_vs_coldstart_rsu_${TIMESTAMP}.ndjson"
fi

BENCH_NODE_NAME=""
FREEZE_DAEMON_POD=""

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# ---- EA templates -----------------------------------------------------------

ea_yaml() {
    local freeze_enabled="$1"
    cat <<EOFEA
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplication
metadata:
  name: $EA_NAME
  namespace: $NAMESPACE
spec:
  dId: retransmitter-appd-v1
  dVersion: "1.0.0"
  description: App to retransmit packet
  infoName: App to retransmit packet
  name: retransmitter
  provider: example.mec
  service:
    container:
      env:
      - name: FORWARD_URL
        value: http://http-sink.default.svc.cluster.local
      - name: HOP_NAME
        value: edge-bench-1
      - name: OUT_TYPE
        value: its.cam.benchmarked
      image: ghcr.io/pmacoutinho/retransmitter:latest
    freezeEnabled: $freeze_enabled
    freezeIdleTimeout: 5
    nodeSelector:
      $BENCH_NODE_SELECTOR_KEY: $BENCH_NODE_SELECTOR_VAL
    triggerFilters:
    - type: its.cam
    - type: its.denm
  softVersion: "1.0.0"
EOFEA
}

# ---- kyverno restartPolicy=Never -------------------------------------------

ensure_restart_policy_kyverno() {
    kubectl apply -f - >/dev/null 2>&1 <<'KYVERNO'
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: bench-restart-policy-never
spec:
  rules:
  - name: set-restart-policy-never
    match:
      any:
      - resources:
          kinds: [Pod]
          namespaces: [default]
          selector:
            matchLabels:
              serving.knative.dev/service: retransmitter
    mutate:
      patchesJson6902: |-
        - op: add
          path: /spec/containers/0/restartPolicy
          value: Never
KYVERNO
    log "  Kyverno restartPolicy=Never policy applied."
}

remove_restart_policy_kyverno() {
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true
}

# ---- scale-to-zero patching -------------------------------------------------

ORIG_STABLE_WINDOW=""
ORIG_STZ_GRACE=""
STZ_PATCHED=false

patch_scale_to_zero() {
    if [[ -z "$COLD_START_SCALE_TO_ZERO" ]]; then return; fi
    log "  Patching config-autoscaler: stable-window=${COLD_START_SCALE_TO_ZERO}s, grace=${COLD_START_SCALE_TO_ZERO}s"
    ORIG_STABLE_WINDOW=$(kubectl get configmap config-autoscaler -n knative-serving \
        -o jsonpath='{.data.stable-window}' 2>/dev/null)
    ORIG_STZ_GRACE=$(kubectl get configmap config-autoscaler -n knative-serving \
        -o jsonpath='{.data.scale-to-zero-grace-period}' 2>/dev/null)
    kubectl patch configmap config-autoscaler -n knative-serving --type merge \
        -p "{\"data\":{\"stable-window\":\"${COLD_START_SCALE_TO_ZERO}s\",\"scale-to-zero-grace-period\":\"${COLD_START_SCALE_TO_ZERO}s\"}}" >/dev/null 2>&1
    STZ_PATCHED=true
    sleep 3  # let autoscaler pick up the change
}

restore_scale_to_zero() {
    if [[ "$STZ_PATCHED" != "true" ]]; then return; fi
    log "  Restoring config-autoscaler: stable-window=$ORIG_STABLE_WINDOW, grace=$ORIG_STZ_GRACE"
    local patch="{\"data\":{"
    [[ -n "$ORIG_STABLE_WINDOW" ]] && patch+="\"stable-window\":\"$ORIG_STABLE_WINDOW\","
    [[ -n "$ORIG_STZ_GRACE" ]] && patch+="\"scale-to-zero-grace-period\":\"$ORIG_STZ_GRACE\","
    patch="${patch%,}}}"
    kubectl patch configmap config-autoscaler -n knative-serving --type merge \
        -p "$patch" >/dev/null 2>&1
    STZ_PATCHED=false
}

# ---- helpers ----------------------------------------------------------------

resolve_bench_node() {
    BENCH_NODE_NAME=$(kubectl get nodes -l "$BENCH_NODE_SELECTOR_KEY=$BENCH_NODE_SELECTOR_VAL" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    log "Bench node: $BENCH_NODE_NAME"
    # Find the freeze daemon pod on this node
    FREEZE_DAEMON_POD=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
        | grep freeze-daemon-containerd | grep "$BENCH_NODE_NAME" | awk '{print $1}')
    log "Freeze daemon: $FREEZE_DAEMON_POD"
}

LOAD_CHECK_INTERVAL="${LOAD_CHECK_INTERVAL:-50}"
LOAD_HIGH_THRESHOLD=7
LOAD_LOW_THRESHOLD=4

get_node_load() {
    kubectl exec "$FREEZE_DAEMON_POD" -n knative-serving -- cat /proc/loadavg 2>/dev/null \
        | awk '{print $1}'
}

check_load() {
    local load
    load=$(get_node_load)
    [[ -z "$load" ]] && return
    local load_int=${load%%.*}
    if (( load_int >= LOAD_HIGH_THRESHOLD )); then
        log "  High load on $BENCH_NODE_NAME: $load — waiting for it to drop below $LOAD_LOW_THRESHOLD..."
        local deadline=$((SECONDS + 300))
        while (( SECONDS < deadline )); do
            sleep 15
            load=$(get_node_load)
            [[ -z "$load" ]] && continue
            load_int=${load%%.*}
            if (( load_int < LOAD_LOW_THRESHOLD )); then
                log "  Load dropped to $load — resuming"
                return
            fi
            log "  Load: $load — still waiting..."
        done
        log "  WARNING: load did not drop within 5min, continuing anyway"
    fi
}

restart_freeze_daemon() {
    log "  Restarting freeze daemon on $BENCH_NODE_NAME..."
    kubectl delete pod "$FREEZE_DAEMON_POD" -n knative-serving --wait=true 2>/dev/null
    # Wait for the DaemonSet to recreate it
    local _deadline=$((SECONDS + 60))
    while (( SECONDS < _deadline )); do
        FREEZE_DAEMON_POD=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
            | grep freeze-daemon-containerd | grep "$BENCH_NODE_NAME" \
            | grep Running | awk '{print $1}')
        if [[ -n "$FREEZE_DAEMON_POD" ]]; then
            log "  Freeze daemon restarted: $FREEZE_DAEMON_POD"
            return 0
        fi
        sleep 3
    done
    log "  ERROR: freeze daemon did not restart within 60s"
    return 1
}

setup_curl_pod() {
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 3
    local attempt
    for (( attempt=1; attempt<=5; attempt++ )); do
        if kubectl run bench-curl -n "$NAMESPACE" --image=curlimages/curl \
            --restart=Never --command -- sleep 21600 >/dev/null 2>&1; then
            if kubectl wait --for=condition=Ready pod/bench-curl -n "$NAMESPACE" --timeout=120s >/dev/null 2>&1; then
                log "bench-curl pod ready."
                return 0
            fi
        fi
        log "  bench-curl attempt $attempt failed, retrying in 20s..."
        kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        sleep 20
    done
    log "ERROR: bench-curl pod failed to start"
    exit 1
}

send_event_internal() {
    local url="$1"
    kubectl exec bench-curl -n "$NAMESPACE" -- \
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
        --max-time 120 2>/dev/null
}

get_pod_name() {
    local latest_rev
    latest_rev=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
    [[ -z "$latest_rev" ]] && return
    kubectl get pods -n "$NAMESPACE" \
        -l "serving.knative.dev/revision=$latest_rev" \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

get_pod_ip() {
    kubectl get pod "$1" -n "$NAMESPACE" -o jsonpath='{.status.podIP}' 2>/dev/null
}

wait_for_pod_ready() {
    log "  Waiting for pod 2/2 Ready..."
    local deadline=$((SECONDS + 180))  # 3 min for ARM64
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
        sleep 3
    done
    log "  WARNING: pod not 2/2 ready within 180s"
    return 1
}

get_freeze_count() {
    local pod="$1"
    kubectl logs "$FREEZE_DAEMON_POD" -n knative-serving 2>/dev/null \
        | grep -c -F "pause request received, freezing pod: ${NAMESPACE}/${pod}" || echo 0
}

wait_for_freeze() {
    local pod="$1"
    local prev_count="${2:-}"
    log "  Waiting for freeze (watching freeze daemon)..."

    if [[ -z "$FREEZE_DAEMON_POD" ]]; then
        log "  ERROR: FREEZE_DAEMON_POD not set"
        return 1
    fi

    # Watch the freeze daemon logs instead of queue-proxy logs.
    # After CRIU restore the pod shows 1/2 Error (cosmetic) and kubectl logs
    # for queue-proxy returns nothing. The freeze daemon is never checkpointed,
    # so its logs are always accessible.
    local freeze_pattern="pause request received, freezing pod: ${NAMESPACE}/${pod}"
    local error_pattern="freezing pod ${NAMESPACE}/${pod} failed"

    # Use caller-provided baseline count if available. With short idle
    # timeouts the pod can re-freeze before we start polling, so callers
    # should snapshot get_freeze_count() right after each thaw.
    # For the first call (warmup), no baseline is provided — use 0 so
    # that any existing freeze is detected immediately.
    if [[ -z "$prev_count" ]]; then
        prev_count=0
    fi

    local start_epoch daemon_restarted=0
    start_epoch=$(date +%s)
    while :; do
        local cur_count
        cur_count=$(kubectl logs "$FREEZE_DAEMON_POD" -n knative-serving 2>/dev/null \
            | grep -c -F "$freeze_pattern") || cur_count=0
        if (( cur_count > prev_count )); then
            # Verify no error was logged for this checkpoint.
            local err_count
            err_count=$(kubectl logs "$FREEZE_DAEMON_POD" -n knative-serving 2>/dev/null \
                | grep -c -F "$error_pattern") || err_count=0
            if (( err_count >= cur_count )); then
                log "  WARNING: freeze daemon reported checkpoint failure"
                return 1
            fi
            # Wait for the fake listener to start. After CRIU checkpoint,
            # there is a gap before the queue-proxy's fake listener binds
            # to port 8080. Requests during this gap get "connection
            # refused" and never trigger ApproveRequest()/thaw.
            # Count fake listener messages to avoid matching stale logs
            # from previous freeze cycles.
            local _fl_deadline=$((SECONDS + 60))
            while (( SECONDS < _fl_deadline )); do
                local fl_count
                fl_count=$(kubectl logs "$pod" -c queue-proxy -n "$NAMESPACE" 2>/dev/null \
                    | grep -c "fake listener started") || fl_count=0
                if (( fl_count >= cur_count )); then
                    break
                fi
                sleep 2
            done
            log "  Container frozen (confirmed by freeze daemon)."
            return 0
        fi

        local now=$(($(date +%s) - start_epoch))
        if (( now >= FREEZE_WAIT_TIMEOUT )); then
            log "  WARNING: freeze did not happen within ${FREEZE_WAIT_TIMEOUT}s"
            return 1
        fi
        # If freeze hasn't happened after 150s, restart the daemon and retry.
        if (( now >= 150 && now < 153 && !daemon_restarted )); then
            log "  Freeze not detected after 150s — restarting freeze daemon..."
            if restart_freeze_daemon; then
                daemon_restarted=1
                log "  Retrying freeze detection with new daemon pod..."
            else
                log "  WARNING: daemon restart failed, continuing to wait..."
            fi
        fi
        if (( now % 30 == 0 && now > 0 )); then
            log "  [+${now}s] still waiting for freeze..."
        fi
        sleep 2
    done
}

wait_for_scale_to_zero() {
    log "  Waiting for scale-to-zero..."
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
        sleep 5
    done
    log "  WARNING: scale-to-zero not reached within 180s"
    return 1
}

delete_ea_and_wait() {
    log "  Deleting EdgeApplication and waiting for cleanup..."
    kubectl delete edgeapplication "$EA_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
    sleep 5
    # Force-delete any lingering pods
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    # Wait until all pods, ksvc, revisions are gone
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local count
        count=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$SERVICE_NAME" \
            --no-headers 2>/dev/null | wc -l)
        if [[ "$count" -eq 0 ]]; then
            # Also check ksvc is gone
            if ! kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
                log "  Clean slate."
                return 0
            fi
        fi
        sleep 5
    done
    # Final force cleanup
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 3
    log "  Cleanup done (forced)."
}

create_ea_and_wait() {
    local freeze_enabled="$1"
    log "  Creating EA (freezeEnabled=$freeze_enabled)..."
    ea_yaml "$freeze_enabled" | kubectl apply -f - >/dev/null 2>&1
    # Wait for revision to stabilize (single revision, no churn)
    local deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
        local created ready
        created=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
        ready=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
        if [[ -n "$created" && -n "$ready" && "$created" == "$ready" ]]; then
            log "  Revision stable: $ready"
            return 0
        fi
        sleep 5
    done
    log "  WARNING: revision did not stabilize within 300s, proceeding"
    return 1
}

emit_result() {
    local mode="$1" iteration="$2" timings="$3" ts_before="$4" ts_after="$5"
    IFS=',' read -r t_dns t_connect t_ttfb t_total http_code <<< "$timings"
    printf '{"mode":"%s","iteration":%d,"t_dns_s":%s,"t_connect_s":%s,"t_ttfb_s":%s,"t_total_s":%s,"http_code":%s,"ts_before":"%s","ts_after":"%s","node":"%s","label":"%s"}\n' \
        "$mode" "$iteration" "$t_dns" "$t_connect" "$t_ttfb" "$t_total" "$http_code" \
        "$ts_before" "$ts_after" "$BENCH_NODE_NAME" "$LABEL" >> "$OUTFILE"
}

# ---- disk cleanup -----------------------------------------------------------

# Light cleanup: checkpoint dirs + containerd checkpoint images/snapshots only.
# No crictl rmi --prune or image re-pull (heavy I/O on eMMC).
cleanup_node_disk_light() {
    if [[ -z "$BENCH_NODE_NAME" ]]; then return; fi

    local pod_name="bench-disk-cleanup"
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2

    kubectl run "$pod_name" -n "$NAMESPACE" --restart=Never --image=busybox \
        --overrides='{
          "spec": {
            "nodeName": "'"$BENCH_NODE_NAME"'",
            "hostPID": true,
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
              "name": "cleanup",
              "image": "busybox",
              "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c",
                "n=$(find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d 2>/dev/null | wc -l); find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d -exec rm -rf {} + 2>/dev/null; rm -rf /var/lib/kubelet/checkpoints/* 2>/dev/null; k3s ctr -n k8s.io images ls -q 2>/dev/null | grep '\''^containerd.io/checkpoint/'\'' | xargs -r k3s ctr -n k8s.io images rm >/dev/null 2>&1; k3s ctr -n k8s.io content prune references >/dev/null 2>&1; k3s ctr -n k8s.io snapshots ls 2>/dev/null | grep parent-view | sed \"s/ .*//\" | xargs -r -n1 k3s ctr -n k8s.io snapshots rm >/dev/null 2>&1; echo cleaned_checkpoints=$n"],
              "securityContext": {"privileged": true}
            }]
          }
        }' >/dev/null 2>&1
    if kubectl wait pod "$pod_name" -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s >/dev/null 2>&1; then
        local result
        result=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
        if [[ -n "$result" ]]; then
            log "  Disk cleanup (light) on $BENCH_NODE_NAME: $result"
        fi
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# Full cleanup: everything in light + crictl rmi --prune + image re-pull.
# Use in cold start loop and final cleanup where images accumulate.
cleanup_node_disk() {
    if [[ -z "$BENCH_NODE_NAME" ]]; then return; fi

    local app_image qp_image
    app_image=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.spec.service.container.image}' 2>/dev/null)
    qp_image=$(kubectl get configmap config-deployment -n knative-serving \
        -o jsonpath='{.data.queue-sidecar-image}' 2>/dev/null)

    local pod_name="bench-disk-cleanup"
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2

    local pull_cmds=""
    if [[ -n "$app_image" ]]; then
        pull_cmds="k3s crictl pull '$app_image' >/dev/null 2>&1;"
    fi
    if [[ -n "$qp_image" ]]; then
        pull_cmds="${pull_cmds} k3s crictl pull '$qp_image' >/dev/null 2>&1;"
    fi

    kubectl run "$pod_name" -n "$NAMESPACE" --restart=Never --image=busybox \
        --overrides='{
          "spec": {
            "nodeName": "'"$BENCH_NODE_NAME"'",
            "hostPID": true,
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
              "name": "cleanup",
              "image": "busybox",
              "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c",
                "n=$(find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d 2>/dev/null | wc -l); find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d -exec rm -rf {} + 2>/dev/null; rm -rf /var/lib/kubelet/checkpoints/* 2>/dev/null; k3s crictl rmi --prune >/dev/null 2>&1; k3s ctr -n k8s.io images ls -q 2>/dev/null | grep '\''^containerd.io/checkpoint/'\'' | xargs -r k3s ctr -n k8s.io images rm >/dev/null 2>&1; k3s ctr -n k8s.io content prune references >/dev/null 2>&1; k3s ctr -n k8s.io snapshots ls 2>/dev/null | grep parent-view | sed \"s/ .*//\" | xargs -r -n1 k3s ctr -n k8s.io snapshots rm >/dev/null 2>&1; '"$pull_cmds"' echo cleaned_checkpoints=$n"],
              "securityContext": {"privileged": true}
            }]
          }
        }' >/dev/null 2>&1
    if kubectl wait pod "$pod_name" -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s >/dev/null 2>&1; then
        local result
        result=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
        if [[ -n "$result" ]]; then
            log "  Disk cleanup on $BENCH_NODE_NAME: $result"
        fi
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# ---- cleanup ----------------------------------------------------------------

cleanup() {
    log "Cleanup: removing benchmark resources..."
    restore_scale_to_zero
    cleanup_node_disk
    remove_restart_policy_kyverno
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    # Don't delete the EA here — leave it for the user to restore manually
    # since we don't know the original state.
    log "Cleanup done. NOTE: retransmitter EA may need manual restore."
}
trap cleanup EXIT

# ---- analysis ---------------------------------------------------------------

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

def t_inv(p, df):
    # Approximation of two-tailed t-distribution inverse
    if df <= 0: return 2.0
    if df == 1: return 12.706
    if df == 2: return 4.303
    if df <= 5: return 2.571
    if df <= 10: return 2.228
    if df <= 30: return 2.042
    return 1.96

def summarize(label, vals):
    if not vals:
        print(f"\n{label}: no data")
        return
    n = len(vals)
    mu = statistics.mean(vals)
    med = statistics.median(vals)
    sd = statistics.stdev(vals) if n > 1 else 0.0
    se = sd / math.sqrt(n) if n > 1 else 0.0
    t = t_inv(0.975, n - 1)
    ci = t * se
    mn, mx = min(vals), max(vals)
    print(f"\n{label} (n={n}):")
    print(f"  mean   = {mu*1000:.1f} ms")
    print(f"  median = {med*1000:.1f} ms")
    print(f"  stdev  = {sd*1000:.1f} ms")
    print(f"  CI95   = [{(mu-ci)*1000:.1f}, {(mu+ci)*1000:.1f}] ms")
    print(f"  range  = [{mn*1000:.1f}, {mx*1000:.1f}] ms")

thaw = [r["t_total_s"] for r in rows if r["mode"] == "criu_thaw"]
cold = [r["t_total_s"] for r in rows if r["mode"] == "cold_start"]
summarize("CRIU Thaw", thaw)
summarize("Cold Start", cold)
if thaw and cold:
    mu_t, mu_c = statistics.mean(thaw), statistics.mean(cold)
    if mu_c > 0:
        pct = (1 - mu_t / mu_c) * 100
        print(f"\nSpeedup: {pct:.1f}% faster thaw vs cold start")
PY
}

# ---- main -------------------------------------------------------------------

log "RSU-friendly Freeze vs Cold-start Benchmark"
log "Iterations per mode: $ITERATIONS"
log "Output: $OUTFILE"
echo

resolve_bench_node
setup_curl_pod

# ---- PHASE 1: CRIU THAW ----------------------------------------------------

if [[ "$BENCH_MODE" == "both" || "$BENCH_MODE" == "thaw" ]]; then
log "=== PHASE 1: CRIU THAW ($ITERATIONS iterations) ==="

delete_ea_and_wait
ensure_restart_policy_kyverno
log "  Creating EA (freezeEnabled=true)..."
ea_yaml true | kubectl apply -f - >/dev/null 2>&1

# Delete triggers immediately so the pod goes idle after startup.
# Don't wait for the ksvc revision to become Ready first — on RSU nodes
# the freeze daemon can checkpoint the pod before Knative marks the
# revision Ready, causing create_ea_and_wait to hang forever (the pod
# shows 1/2 Error after checkpoint, which is cosmetic but prevents the
# revision from reaching Ready status).
sleep 5
log "  Deleting triggers to isolate pod from event stream..."
kubectl delete trigger -n "$NAMESPACE" -l "mec.atnog.org/app=$EA_NAME" --ignore-not-found --wait=false >/dev/null 2>&1

# Wait for a pod to appear. With short idle timeouts the pod may freeze
# before reaching 2/2 Ready, so we just wait for it to exist and have an IP.
log "  Waiting for pod to appear..."
pod=""
pod_ip=""
warmup_deadline=$((SECONDS + 180))
while (( SECONDS < warmup_deadline )); do
    pod=$(kubectl get pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -n "$pod" ]]; then
        pod_ip=$(get_pod_ip "$pod")
        if [[ -n "$pod_ip" ]]; then
            # Verify Kyverno injected restartPolicy=Never. If not (e.g. Kyverno
            # webhook was briefly down), delete the pod and wait for a new one.
            _rp=$(kubectl get pod "$pod" -n "$NAMESPACE" \
                -o jsonpath='{.spec.containers[?(@.name=="user-container")].restartPolicy}' 2>/dev/null)
            if [[ "$_rp" != "Never" ]]; then
                log "  WARNING: pod $pod missing restartPolicy=Never ($_rp), recycling..."
                kubectl delete pod "$pod" -n "$NAMESPACE" --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
                pod="" ; pod_ip=""
                sleep 5
                continue
            fi
            log "  Pod found: $pod ($pod_ip)"
            break
        fi
    fi
    sleep 3
done

freeze_baseline=""
if [[ -n "$pod" && -n "$pod_ip" ]]; then
    log "  Warmup: waiting for freeze on $pod..."
    wait_for_freeze "$pod"
    # Snapshot freeze count BEFORE warmup thaw so we catch the re-freeze.
    freeze_baseline=$(get_freeze_count "$pod")
    # Send a warmup thaw to exercise the restore path (direct to pod IP)
    send_event_internal "http://${pod_ip}:${QUEUE_PROXY_PORT}" >/dev/null 2>&1
    sleep 3
fi

# Pod reuse: the warmup pod is already thawed. Each iteration waits for
# it to re-freeze (via idle timeout), measures the thaw, and repeats.
# No pod deletion — avoids overwhelming RSU nodes with repeated cold starts.
for (( i=1; i<=ITERATIONS; i++ )); do
    log "--- CRIU thaw iteration $i/$ITERATIONS ---"

    if [[ -z "$pod" || -z "$pod_ip" ]]; then
        log "  ERROR: no pod/IP, skipping"
        continue
    fi

    # Periodic disk cleanup (between thaw and re-freeze).
    # Safe: runs after previous thaw consumed the checkpoint reference
    # and before the next freeze creates a new one.
    if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
        log "  Running periodic disk cleanup on $BENCH_NODE_NAME (every ${CHECKPOINT_CLEANUP_INTERVAL} iterations)..."
        cleanup_node_disk_light
    fi

    # Periodic load check — pause if RSU is overloaded.
    if (( i % LOAD_CHECK_INTERVAL == 0 )); then
        check_load
    fi

    if ! wait_for_freeze "$pod" "$freeze_baseline"; then
        log "  SKIPPED: freeze failed — recycling pod..."
        _old_pod="$pod"
        kubectl delete pod "$pod" -n "$NAMESPACE" --grace-period=0 --force >/dev/null 2>&1 || true
        # Send a request to trigger Knative to scale up a new pod
        send_event_internal "http://${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local" >/dev/null 2>&1 || true
        # Wait for new pod to be Running with an IP
        _recycle_deadline=$((SECONDS + 300))
        pod="" pod_ip=""
        while (( SECONDS < _recycle_deadline )); do
            pod=$(kubectl get pods -n "$NAMESPACE" \
                -l "serving.knative.dev/service=$SERVICE_NAME" \
                --field-selector=status.phase!=Succeeded,status.phase!=Failed \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
            if [[ -n "$pod" && "$pod" != "$_old_pod" ]]; then
                pod_ip=$(get_pod_ip "$pod")
                if [[ -n "$pod_ip" ]]; then
                    log "  New pod: $pod ($pod_ip)"
                    freeze_baseline=""
                    break
                fi
            fi
            sleep 5
        done
        if [[ -z "$pod" || -z "$pod_ip" ]]; then
            log "  ERROR: no new pod appeared after recycle, aborting CRIU phase"
            break
        fi
        continue
    fi

    # Snapshot freeze count BEFORE thaw. After the thaw the pod re-freezes
    # quickly (5s idle timeout), so snapshotting after would race.
    freeze_baseline=$(get_freeze_count "$pod")

    # Measure via direct pod IP — this is the production path. When a pod is
    # frozen (fake listener keeps it Ready), Knative routes directly to it
    # (serve mode). The request hits queue-proxy, which triggers CRIU restore.
    ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    timings=$(send_event_internal "http://${pod_ip}:${QUEUE_PROXY_PORT}")
    ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

    IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
    if [[ "$http_code" == "422" ]]; then
        log "  Result: ${t_total}s (HTTP $http_code)"
        emit_result "criu_thaw" "$i" "$timings" "$ts_before" "$ts_after"
    elif [[ "$http_code" == "000" ]]; then
        log "  SKIPPED: HTTP 000 (timeout/connection refused) — pod likely dead, recycling..."
        _old_pod="$pod"
        kubectl delete pod "$pod" -n "$NAMESPACE" --grace-period=0 --force >/dev/null 2>&1 || true
        send_event_internal "http://${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local" >/dev/null 2>&1 || true
        _recycle_deadline=$((SECONDS + 300))
        pod="" pod_ip=""
        while (( SECONDS < _recycle_deadline )); do
            pod=$(kubectl get pods -n "$NAMESPACE" \
                -l "serving.knative.dev/service=$SERVICE_NAME" \
                --field-selector=status.phase!=Succeeded,status.phase!=Failed \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
            if [[ -n "$pod" && "$pod" != "$_old_pod" ]]; then
                pod_ip=$(get_pod_ip "$pod")
                if [[ -n "$pod_ip" ]]; then
                    log "  New pod: $pod ($pod_ip)"
                    freeze_baseline=""
                    break
                fi
            fi
            sleep 5
        done
        if [[ -z "$pod" || -z "$pod_ip" ]]; then
            log "  ERROR: no new pod appeared after recycle, aborting CRIU phase"
            break
        fi
    else
        log "  SKIPPED: HTTP $http_code (expected 422)"
    fi
done

fi  # end BENCH_MODE thaw

# ---- PHASE 2: COLD START ---------------------------------------------------

if [[ "$BENCH_MODE" == "both" || "$BENCH_MODE" == "cold_start" ]]; then
log ""
log "=== PHASE 2: COLD START ($ITERATIONS iterations) ==="

# Clean break: delete EA entirely, recreate without freeze
delete_ea_and_wait
remove_restart_policy_kyverno
patch_scale_to_zero
ea_yaml false | kubectl apply -f - >/dev/null 2>&1
log "  Waiting for ksvc to appear..."
cs_deadline=$((SECONDS + 120))
while (( SECONDS < cs_deadline )); do
    if kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then break; fi
    sleep 3
done

# Warmup: one throwaway cold start (from scale-to-zero)
wait_for_scale_to_zero || true
send_event_internal "$SERVICE_URL" >/dev/null 2>&1
sleep 5

for (( i=1; i<=ITERATIONS; i++ )); do
    log "--- Cold start iteration $i/$ITERATIONS ---"

    if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
        log "  Running periodic disk cleanup on $BENCH_NODE_NAME (every ${CHECKPOINT_CLEANUP_INTERVAL} iterations)..."
        cleanup_node_disk
    fi

    if (( i % LOAD_CHECK_INTERVAL == 0 )); then
        check_load
    fi

    # Wait for natural scale-to-zero. Don't force-delete — that causes the
    # deployment to immediately recreate the pod, adding an extra pod lifecycle.
    if ! wait_for_scale_to_zero; then
        log "  SKIPPED: pods still running"
        continue
    fi

    ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    timings=$(send_event_internal "$SERVICE_URL")
    ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

    IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
    if [[ "$http_code" == "422" ]]; then
        log "  Result: ${t_total}s (HTTP $http_code)"
        emit_result "cold_start" "$i" "$timings" "$ts_before" "$ts_after"
    else
        log "  SKIPPED: HTTP $http_code (expected 422)"
    fi
done

restore_scale_to_zero
fi  # end BENCH_MODE cold_start

# ---- results ----------------------------------------------------------------

echo
analyze
log "Results written to: $OUTFILE"
