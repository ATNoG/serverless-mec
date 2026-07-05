#!/usr/bin/env bash
set -euo pipefail

# ===========================================================================
#  Handoff Benchmark
#
#  Measures the full handoff pipeline via direct CR apply:
#    CR apply -> operator reconciles -> target KService created -> pod ready
#    -> CR Ready (cold start) or thawCompleted (CRIU freeze).
#
#  Scenarios:
#    rsu-cold    RSU-A cold start handoff (no freeze, source on RSU-B)
#    rsu-freeze  RSU-A CRIU thaw handoff  (freeze enabled, 5s idle timeout)
#    rsu         Both RSU-A scenarios (cold + freeze)
#    worker1-cold   Worker-1 cold start handoff
#    worker1-freeze Worker-1 CRIU thaw handoff
#    worker2-cold   Worker-2 cold start handoff
#    worker2-freeze Worker-2 CRIU thaw handoff
#    all            All scenarios
#
#  The source pod (on RSU-B or worker-1) is unused — the handoff CR is
#  applied directly via kubectl, bypassing the retransmission event path.
#
#  Freeze scenarios use CRIU checkpoint/restore. After warmup, the target
#  pod freezes via idle timeout. Each iteration thaws the frozen pod and
#  measures latency to thawCompleted (not phase=Ready, since kubelet
#  doesn't update container state after CRIU restore).
#
#  Cold start scenarios do NOT run periodic disk cleanup — there are no
#  CRIU checkpoints to clean, and image pruning + re-pull would add I/O
#  contention that skews measurements.
#
#  Environment variables:
#    ITERATIONS                    Number of iterations per scenario (default: 20)
#    SCENARIO                      Scenario to run (default: all)
#    CHECKPOINT_CLEANUP_INTERVAL   Freeze iterations between disk cleanups (default: 50)
#    FREEZER_IDLE_TIMEOUT          Seconds before freezer checkpoints idle pod (default: 5)
#
#  Usage:
#    ./bench_handoff.sh [--iterations N] [--scenario rsu|rsu-cold|rsu-freeze|...]
#    CHECKPOINT_CLEANUP_INTERVAL=2 ./bench_handoff.sh --iterations 3 --scenario rsu
# ===========================================================================

# ---- configuration --------------------------------------------------------

EA_NAME="retransmitter-handoff"
NAMESPACE="default"
SERVICE_NAME="retransmitter-handoff"
SERVICE_URL="http://retransmitter-handoff.default.svc.cluster.local"

ITERATIONS=${ITERATIONS:-20}
SCENARIO=${SCENARIO:-"all"}

HANDOFF_CR_PREFIX="${EA_NAME}-to-"
HANDOFF_POLL_TIMEOUT=600  # seconds (cold start scenarios)
FREEZE_THAW_TIMEOUT=60   # seconds (freeze scenarios — thaw should be fast)
CHECKPOINT_CLEANUP_INTERVAL="${CHECKPOINT_CLEANUP_INTERVAL:-50}"

QUEUE_PROXY_PORT=8012
FREEZE_WAIT_TIMEOUT=60   # seconds — max wait for pod to re-freeze
FREEZER_IDLE_TIMEOUT=5   # seconds — configured via EdgeApplication CRD (freezeIdleTimeout)

LOAD_HIGH_THRESHOLD=7
LOAD_LOW_THRESHOLD=4

QP_IMAGE_BACKUP=""
EA_CREATED_BY_US=false

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_DIR="$SCRIPT_DIR/../knative-freezer-plugin"
LOG_DIR="${SCRIPT_DIR}/handoff_bench_logs"
mkdir -p "$LOG_DIR"
OUTFILE="${LOG_DIR}/handoff_bench_$(date +%Y%m%d_%H%M%S).ndjson"

BENCH_NODE_SELECTOR_KEY="vm-id"
BENCH_NODE_SELECTOR_VAL="worker-1"

# ---- logging --------------------------------------------------------------

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---- retry helper ----------------------------------------------------------

kubectl_retry() {
    # Retries a kubectl command up to N times with exponential backoff.
    # Usage: kubectl_retry [max_attempts] kubectl <args...>
    # WARNING: do NOT use with stdin-based commands (kubectl apply -f -).
    # Stdin is consumed on the first attempt; retries get empty input.
    # Use kubectl_apply_retry for those.
    local max_attempts="${1:-3}"; shift
    local attempt delay=2
    for (( attempt=1; attempt<=max_attempts; attempt++ )); do
        if "$@" ; then
            return 0
        fi
        if (( attempt < max_attempts )); then
            log "  WARNING: command failed (attempt $attempt/$max_attempts), retrying in ${delay}s..."
            sleep "$delay"
            delay=$(( delay * 2 ))
        fi
    done
    log "  ERROR: command failed after $max_attempts attempts: $*"
    return 1
}

kubectl_apply_retry() {
    # Retries kubectl apply with YAML from a variable (stdin-safe).
    # Each attempt gets a fresh copy of the YAML via printf pipe.
    # Usage: kubectl_apply_retry <yaml_string> [max_attempts]
    local yaml="$1" max_attempts="${2:-3}"
    local attempt delay=2
    for (( attempt=1; attempt<=max_attempts; attempt++ )); do
        if printf '%s' "$yaml" | kubectl apply -f - >/dev/null 2>&1; then
            return 0
        fi
        if (( attempt < max_attempts )); then
            log "  WARNING: kubectl apply failed (attempt $attempt/$max_attempts), retrying in ${delay}s..."
            sleep "$delay"
            delay=$(( delay * 2 ))
        fi
    done
    log "  ERROR: kubectl apply failed after $max_attempts attempts"
    return 1
}

# ---- CR builder ------------------------------------------------------------

_build_handoff_cr() {
    # Build EdgeApplicationHandoff CR YAML with optional nodeSelector.
    # Usage: _build_handoff_cr <target> <cleanupOnDelete> [node_selector_json]
    local target="$1" cleanup="$2" ns_json="${3:-}"
    cat <<EOF
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplicationHandoff
metadata:
  name: ${HANDOFF_CR_PREFIX}${target}
  namespace: ${NAMESPACE}
spec:
  edgeApplicationName: ${EA_NAME}
  targetReplicaName: ${target}
  cleanupOnDelete: ${cleanup}
EOF
    if [[ -n "$ns_json" ]]; then
        echo "  nodeSelector:"
        python3 -c "
import json, sys
for k, v in json.loads(sys.argv[1]).items():
    print(f'    {k}: {v}')
" "$ns_json"
    fi
}

# ---- helpers ---------------------------------------------------------------

strip_server_fields() {
    python3 -c "
import sys, json, yaml
docs = list(yaml.safe_load_all(sys.stdin))
out = []
for d in docs:
    if not isinstance(d, dict):
        continue
    d.pop('status', None)
    m = d.get('metadata', {})
    for k in ('resourceVersion','uid','creationTimestamp','generation',
              'managedFields','annotations','selfLink'):
        m.pop(k, None)
    out.append(d)
print('---\n'.join(yaml.dump(d) for d in out))
"
}

emit_result() {
    local scenario="$1" iteration="$2" timings="$3" ts_before="$4" ts_after="$5"
    local handoff_phase="$6" handoff_target="$7" handoff_wall_ms="${8:-0}"

    IFS=',' read -r t_dns t_connect t_ttfb t_total http_code <<< "$timings"

    # Collect raw timestamps from Kubernetes objects for pipeline breakdown.
    local pipeline_json="{}"
    if [[ "$handoff_phase" == "Ready" || "$handoff_phase" == "Applied" || "$handoff_phase" == "Failed" ]]; then
        pipeline_json=$(collect_pipeline_timestamps "$handoff_target")
    fi

    python3 -c "
import json, sys
r = {
    'scenario': sys.argv[1],
    'iteration': int(sys.argv[2]),
    't_dns_s': float(sys.argv[3]),
    't_connect_s': float(sys.argv[4]),
    't_ttfb_s': float(sys.argv[5]),
    't_total_s': float(sys.argv[6]),
    'http_code': sys.argv[7],
    'ts_before': sys.argv[8],
    'ts_after': sys.argv[9],
    'handoff_phase': sys.argv[10],
    'pipeline': json.loads(sys.argv[11]),
    'handoff_wall_ms': float(sys.argv[12]),
}
print(json.dumps(r, separators=(',',':')))
" "$scenario" "$iteration" "$t_dns" "$t_connect" "$t_ttfb" "$t_total" \
  "$http_code" "$ts_before" "$ts_after" "$handoff_phase" "$pipeline_json" \
  "$handoff_wall_ms" >> "$OUTFILE"
}

collect_pipeline_timestamps() {
    # Extracts raw timestamps from the handoff CR, target KService, and target
    # pod. The analysis script computes phase durations from these.
    # Includes microsecond-precision timestamps from status.timestamps when
    # available (operator v2+).
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    local target_svc="${EA_NAME}-${target}"

    python3 -c "
import json, subprocess, sys

def kubectl_json(args):
    r = subprocess.run(['kubectl'] + args, capture_output=True, text=True, timeout=10)
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else None

cr_name, ns, target_svc = sys.argv[1], sys.argv[2], sys.argv[3]
out = {}

# Handoff CR timestamps
cr = kubectl_json(['get', 'edgeapplicationhandoff', cr_name, '-n', ns, '-o', 'json'])
if cr:
    out['cr_created'] = cr.get('metadata', {}).get('creationTimestamp', '')
    for c in cr.get('status', {}).get('conditions', []):
        t = c.get('type', '')
        out[f'cr_cond_{t.lower()}_ts'] = c.get('lastTransitionTime', '')
        out[f'cr_cond_{t.lower()}_reason'] = c.get('reason', '')
    # Microsecond-precision timestamps from operator
    ts = cr.get('status', {}).get('timestamps', {})
    if ts:
        for k in ('reconcileStart', 'replicaApplied', 'thawStarted', 'thawCompleted', 'podRunning', 'podReady', 'kserviceReady', 'triggerReady', 'ready'):
            if ts.get(k):
                out[f'micro_{k}'] = ts[k]

# Target KService timestamps
svc = kubectl_json(['get', 'ksvc', target_svc, '-n', ns, '-o', 'json'])
if svc:
    out['ksvc_created'] = svc.get('metadata', {}).get('creationTimestamp', '')
    out['ksvc_latest_ready_rev'] = svc.get('status', {}).get('latestReadyRevisionName', '')

# Target pod timestamps and image pull detection
pods = kubectl_json(['get', 'pods', '-n', ns,
    '-l', f'serving.knative.dev/service={target_svc}',
    '--sort-by=.metadata.creationTimestamp', '-o', 'json'])
if pods and pods.get('items'):
    pod = pods['items'][-1]  # latest pod
    out['pod_name'] = pod.get('metadata', {}).get('name', '')
    out['pod_created'] = pod.get('metadata', {}).get('creationTimestamp', '')
    for c in pod.get('status', {}).get('conditions', []):
        t = c.get('type', '')
        out[f'pod_cond_{t.lower()}_ts'] = c.get('lastTransitionTime', '')
    # Container started timestamps
    for cs in pod.get('status', {}).get('containerStatuses', []):
        name = cs.get('name', '')
        running = cs.get('state', {}).get('running', {})
        if running:
            out[f'container_{name}_started'] = running.get('startedAt', '')

print(json.dumps(out, separators=(',',':')))
" "$cr_name" "$NAMESPACE" "$target_svc" 2>/dev/null || echo "{}"
}

# ---- queue-proxy image pinning ----------------------------------------------
# Pin the queue-proxy sidecar image to a fixed digest so imagePullPolicy
# becomes IfNotPresent, eliminating registry checks from measurements.

pin_queue_proxy_image() {
    local current
    current=$(kubectl get configmap config-deployment -n knative-serving \
        -o jsonpath='{.data.queue-sidecar-image}' 2>/dev/null)
    QP_IMAGE_BACKUP="$current"

    # Already pinned to a digest — nothing to do
    if [[ "$current" == *"@sha256:"* ]]; then
        log "  Queue-proxy image already pinned: ${current##*@}"
        return
    fi

    # Resolve the tag to a digest from a running pod
    local digest
    digest=$(kubectl get pods -n "$NAMESPACE" --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.status.containerStatuses[?(@.name=="queue-proxy")].imageID}{"\n"}{end}' 2>/dev/null \
        | grep -o 'sha256:[a-f0-9]*' | head -1)

    if [[ -z "$digest" ]]; then
        log "  WARNING: could not resolve queue-proxy digest, skipping pin"
        return
    fi

    local repo
    repo="${current%:*}"
    local pinned="${repo}@sha256:${digest#sha256:}"

    kubectl patch configmap config-deployment -n knative-serving --type=merge \
        -p "{\"data\":{\"queue-sidecar-image\":\"$pinned\"}}" >/dev/null 2>&1
    log "  Pinned queue-proxy image to digest: sha256:${digest#sha256:}"
}

restore_queue_proxy_image() {
    if [[ -n "$QP_IMAGE_BACKUP" ]]; then
        kubectl patch configmap config-deployment -n knative-serving --type=merge \
            -p "{\"data\":{\"queue-sidecar-image\":\"$QP_IMAGE_BACKUP\"}}" >/dev/null 2>&1
    fi
}

# ---- curl pod --------------------------------------------------------------

setup_curl_pod() {
    local attempt max_attempts=5
    for (( attempt=1; attempt<=max_attempts; attempt++ )); do
        kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
        if ! kubectl run bench-curl -n "$NAMESPACE" --image=curlimages/curl \
            --restart=Never --command -- sleep 21600 >/dev/null 2>&1; then
            log "  bench-curl create failed (attempt $attempt/$max_attempts), retrying in 20s..."
            sleep 20
            continue
        fi
        log "Waiting for bench-curl pod..."
        if kubectl wait --for=condition=Ready pod/bench-curl -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1; then
            log "bench-curl pod ready."
            return 0
        fi
        log "  bench-curl not ready (attempt $attempt/$max_attempts), retrying in 20s..."
        sleep 20
    done
    log "ERROR: bench-curl pod failed to start after $max_attempts attempts"
    exit 1
}

ensure_curl_pod() {
    if ! kubectl get pod bench-curl -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q Running; then
        log "  bench-curl pod not running, restarting..."
        setup_curl_pod
    fi
}

DEFAULT_EVENT_PAYLOAD='{"frame_raw_hex":"0011223344556677","frame_number":1}'
send_event() {
    local url="$1"
    local host_header="${2:-}"
    local payload="${3:-$DEFAULT_EVENT_PAYLOAD}"
    local out
    local -a extra_args=()
    if [[ -n "$host_header" ]]; then
        extra_args+=(-H "Host: $host_header")
    fi
    out=$(kubectl exec bench-curl -n "$NAMESPACE" -- \
        curl -s -o /dev/null \
        -w '%{time_namelookup},%{time_connect},%{time_starttransfer},%{time_total},%{http_code}' \
        -X POST "$url" \
        -H "Content-Type: application/json" \
        -H "Ce-Id: bench-$(date +%s%N)" \
        -H "Ce-Specversion: 1.0" \
        -H "Ce-Type: its.cam" \
        -H "Ce-Source: benchmark" \
        "${extra_args[@]}" \
        -d "$payload" \
        --max-time 600 2>/dev/null)
    if [[ -z "$out" || "$out" != *,* ]]; then
        log "  send_event error: ${out:-<empty>}"
        echo "0,0,0,0,0"
    else
        echo "$out"
    fi
}

# ---- scale helpers ---------------------------------------------------------

wait_for_scale_to_zero() {
    local svc="${1:-$SERVICE_NAME}"
    local max_wait=180 elapsed=0
    log "  Waiting for scale-to-zero ($svc)..."
    while (( elapsed < max_wait )); do
        local count
        count=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$svc" \
            --field-selector=status.phase=Running \
            --no-headers 2>/dev/null | wc -l)
        if (( count == 0 )); then
            log "  Scaled to zero."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log "  WARNING: pods still running after ${max_wait}s"
    return 1
}

# ---- handoff helpers -------------------------------------------------------

delete_handoff_cr() {
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    kubectl delete edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
        --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
}

wait_for_handoff_ready() {
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    # Use kubectl wait with jsonpath to block until phase=Ready or phase=Failed,
    # eliminating polling noise from the wall clock measurement.
    if kubectl wait edgeapplicationhandoff/"$cr_name" -n "$NAMESPACE" \
        --for=jsonpath='{.status.phase}'=Ready \
        --timeout="${HANDOFF_POLL_TIMEOUT}s" >/dev/null 2>&1; then
        return 0
    fi
    # kubectl wait exited non-zero: check if it's Failed or a timeout.
    local phase
    phase=$(kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [[ "$phase" == "Failed" ]]; then
        return 1
    fi
    return 2  # timeout
}

get_handoff_phase() {
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown"
}

wait_for_handoff_thaw() {
    # For freeze scenarios: wait for the operator to record thawCompleted in
    # the CR status timestamps. After CRIU restore, the container state in
    # kubelet still shows Terminated/Error so the KService never reaches Ready
    # and the CR stays at Applied. thawCompleted is the correct success signal.
    local target="$1"
    local timeout="${2:-$HANDOFF_POLL_TIMEOUT}"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        local thaw_ts
        thaw_ts=$(kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
            -o jsonpath='{.status.timestamps.thawCompleted}' 2>/dev/null)
        if [[ -n "$thaw_ts" ]]; then
            return 0
        fi
        # Also check for failure
        local phase
        phase=$(kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null)
        if [[ "$phase" == "Failed" ]]; then
            return 1
        fi
        sleep 0.5
    done
    return 2  # timeout
}

# ---- freeze helpers (for freeze scenarios) ----------------------------------

get_target_pod_name() {
    local target_svc="$1"
    local latest_rev
    latest_rev=$(kubectl get ksvc "$target_svc" -n "$NAMESPACE" \
        -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
    if [[ -z "$latest_rev" ]]; then
        return
    fi
    # Return the newest Running pod to avoid picking a stale pod that is
    # about to be replaced by a Kyverno-triggered rollout.
    kubectl get pods -n "$NAMESPACE" \
        -l "serving.knative.dev/revision=$latest_rev" \
        --field-selector=status.phase=Running \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null
}

get_pod_ip() {
    kubectl get pod "$1" -n "$NAMESPACE" -o jsonpath='{.status.podIP}' 2>/dev/null
}

wait_for_target_pod_ready() {
    local target_svc="$1"
    log "  Waiting for target pod 2/2 Ready (freezer-enabled, $target_svc)..."
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        local pod
        pod=$(get_target_pod_name "$target_svc")
        if [[ -n "$pod" ]]; then
            local ready_count
            ready_count=$(kubectl get pod "$pod" -n "$NAMESPACE" \
                -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null \
                | tr ' ' '\n' | grep -c '^true$' || echo 0)
            ready_count=$(echo "$ready_count" | head -1)
            if [[ "$ready_count" -ge 2 ]] 2>/dev/null; then
                log "  Target pod 2/2 Ready ($pod)."
                return 0
            fi
        fi
        sleep 2
    done
    log "  WARNING: target pod not 2/2 ready within 180s"
    return 1
}

get_target_freeze_count() {
    local pod="$1" freeze_daemon_pod="$2"
    kubectl logs "$freeze_daemon_pod" -n knative-serving 2>/dev/null \
        | grep -c -F "pause request received, freezing pod: ${NAMESPACE}/${pod}" || echo 0
}

_TARGET_FREEZE_DAEMON=""  # set by caller; refreshed by _refresh_target_daemon
_TARGET_NODE_NAME=""      # set by caller for daemon re-resolve

# Re-resolve the freeze daemon pod name on the target node.
# Kyverno policy rollouts replace daemon pods every ~20 iterations,
# making the cached name stale.
_refresh_target_daemon() {
    local _new
    _new=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
        | grep freeze-daemon-containerd | grep "$_TARGET_NODE_NAME" \
        | grep Running | awk '{print $1}')
    if [[ -n "$_new" && "$_new" != "$_TARGET_FREEZE_DAEMON" ]]; then
        log "  Freeze daemon refreshed: $_TARGET_FREEZE_DAEMON → $_new"
        _TARGET_FREEZE_DAEMON="$_new"
    fi
}

_LOAD_CHECK_POD=""
_LOAD_CHECK_NODE_VAL=""

setup_load_check_pod() {
    # Launch a lightweight busybox pod on the target node for load monitoring.
    # The freeze daemon is a distroless image with no shell, so we can't exec into it.
    # Reuses existing pod if already on the same target node.
    local node_key="$1" node_val="$2"
    if [[ "$_LOAD_CHECK_NODE_VAL" == "$node_val" ]] && [[ -n "$_LOAD_CHECK_POD" ]]; then
        # Already have a load-check pod on this node — verify it's still running
        if kubectl get pod "$_LOAD_CHECK_POD" -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q Running; then
            log "  Load-check pod $_LOAD_CHECK_POD already running on $node_key=$node_val"
            return 0
        fi
    fi
    local pod_name="bench-load-${node_val}"
    _LOAD_CHECK_POD="$pod_name"
    _LOAD_CHECK_NODE_VAL="$node_val"
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
    kubectl run "$pod_name" -n "$NAMESPACE" --image=busybox --restart=Never \
        --overrides="{\"spec\":{\"nodeSelector\":{\"$node_key\":\"$node_val\"}}}" \
        --command -- sleep 86400 >/dev/null 2>&1
    kubectl wait --for=condition=Ready "pod/$pod_name" -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1
    log "  Load-check pod $pod_name ready on $node_key=$node_val"
}

cleanup_load_check_pod() {
    if [[ -n "$_LOAD_CHECK_POD" ]]; then
        kubectl delete pod "$_LOAD_CHECK_POD" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        _LOAD_CHECK_POD=""
    fi
}

get_target_node_load() {
    [[ -z "$_LOAD_CHECK_POD" ]] && return
    kubectl exec "$_LOAD_CHECK_POD" -n "$NAMESPACE" -- cat /proc/loadavg 2>/dev/null \
        | awk '{print $1}'
}

check_target_load() {
    local load
    load=$(get_target_node_load)
    # exec failed — likely daemon was replaced (Kyverno). Don't block.
    [[ -z "$load" ]] && return
    local load_int=${load%%.*}
    if (( load_int >= LOAD_HIGH_THRESHOLD )); then
        log "  High load on target node: $load — waiting for it to drop below $LOAD_LOW_THRESHOLD..."
        local deadline=$((SECONDS + 300))
        while (( SECONDS < deadline )); do
            sleep 15
            load=$(get_target_node_load)
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

# Restart the freeze daemon on a target node.
# Matches FvC approach: graceful delete + wait for DaemonSet to recreate.
_restart_freeze_daemon() {
    local target_node_name="$1"
    local _old_daemon="$_TARGET_FREEZE_DAEMON"
    log "  Restarting freeze daemon $_old_daemon on $target_node_name..."
    kubectl delete pod "$_TARGET_FREEZE_DAEMON" -n knative-serving --wait=true 2>/dev/null || true
    # Wait for the DaemonSet to recreate it
    local _rd_deadline=$((SECONDS + 120)) _new_daemon=""
    while (( SECONDS < _rd_deadline )); do
        _new_daemon=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
            | grep freeze-daemon-containerd | grep "$target_node_name" \
            | grep Running | awk '{print $1}')
        if [[ -n "$_new_daemon" && "$_new_daemon" != "$_old_daemon" ]]; then
            _TARGET_FREEZE_DAEMON="$_new_daemon"
            log "  Freeze daemon restarted: $_TARGET_FREEZE_DAEMON"
            return 0
        fi
        sleep 3
    done
    log "  WARNING: freeze daemon did not restart within 120s"
    return 1
}

wait_for_target_freeze() {
    local pod="$1"
    local prev_count="${2:-}"
    local target_node_name="${3:-}"
    log "  Waiting for freezer to checkpoint target pod $pod..."

    # Re-resolve daemon name in case Kyverno replaced it.
    _refresh_target_daemon

    if [[ -z "$_TARGET_FREEZE_DAEMON" ]]; then
        log "  ERROR: _TARGET_FREEZE_DAEMON not set"
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
    # should snapshot get_target_freeze_count() right after each thaw.
    # For the first call (warmup), no baseline is provided — use 0 so
    # that any existing freeze is detected immediately.
    if [[ -z "$prev_count" ]]; then
        prev_count=0
    fi

    local start_epoch
    start_epoch=$(date +%s)
    local last_diag_epoch=$start_epoch
    local _prev_daemon="$_TARGET_FREEZE_DAEMON"
    while :; do
        local cur_count
        cur_count=$(kubectl logs "$_TARGET_FREEZE_DAEMON" -n knative-serving 2>/dev/null \
            | grep -c -F "$freeze_pattern") || cur_count=0

        # If daemon was replaced mid-loop, re-resolve and reset baseline
        # (new daemon's logs won't have old freeze entries).
        if (( cur_count == 0 )); then
            _refresh_target_daemon
            if [[ "$_TARGET_FREEZE_DAEMON" != "$_prev_daemon" ]]; then
                prev_count=0
                _prev_daemon="$_TARGET_FREEZE_DAEMON"
                cur_count=$(kubectl logs "$_TARGET_FREEZE_DAEMON" -n knative-serving 2>/dev/null \
                    | grep -c -F "$freeze_pattern") || cur_count=0
            fi
        fi

        if (( cur_count > prev_count )); then
            # Checkpoint initiated. Wait for it to complete on ARM64.
            sleep 5
            # Verify no error was logged for this checkpoint.
            local err_count
            err_count=$(kubectl logs "$_TARGET_FREEZE_DAEMON" -n knative-serving 2>/dev/null \
                | grep -c -F "$error_pattern") || err_count=0
            if (( err_count >= cur_count )); then
                log "  WARNING: freeze daemon reported checkpoint failure"
                return 1
            fi
            log "  Target container frozen (confirmed by freeze daemon)."
            return 0
        fi

        # Check pod still exists and is not Terminating
        local _pod_phase
        _pod_phase=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.metadata.deletionTimestamp},{.status.phase}' 2>/dev/null)
        if [[ -z "$_pod_phase" ]]; then
            log "  WARNING: pod $pod no longer exists"
            return 1
        fi
        if [[ "$_pod_phase" == *","* && "${_pod_phase%%,*}" != "" ]]; then
            log "  WARNING: pod $pod is Terminating"
            return 1
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$((now - start_epoch))

        if (( elapsed >= FREEZE_WAIT_TIMEOUT )); then
            log "  WARNING: freezer did not checkpoint within ${FREEZE_WAIT_TIMEOUT}s"
            return 1
        fi

        # NOTE: Unlike FvC (where daemon is local), handoff targets a REMOTE RSU.
        # Restarting the daemon on the RSU is destructive — the RSU often cannot
        # recreate the pod (slow DNS, flaky image pulls, context deadline exceeded).
        # Do NOT restart the daemon here; let the timeout expire and rely on
        # proactive pod recycling to prevent CRIU degradation in the first place.

        if (( now - last_diag_epoch >= 30 )); then
            log "  [+${elapsed}s] still waiting for freeze..."
            last_diag_epoch=$now
        fi

        sleep 2
    done
}

wait_for_target_frozen_state() {
    local target_svc="$1"
    local deadline=$((SECONDS + FREEZE_WAIT_TIMEOUT))
    log "  Waiting for target to freeze (user-container Terminated)..."
    while (( SECONDS < deadline )); do
        local terminated
        terminated=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$target_svc" \
            --field-selector=status.phase=Running \
            -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="user-container")].state.terminated.exitCode}' \
            2>/dev/null)
        if [[ -n "$terminated" ]]; then
            log "  Target pod frozen (user-container terminated)."
            return 0
        fi
        sleep 1
    done
    log "  WARNING: target did not freeze within ${FREEZE_WAIT_TIMEOUT}s"
    return 1
}

wait_for_knative_rollout() {
    local svc="$1"
    local deadline=$((SECONDS + 300))
    log "  Waiting for ksvc $svc revisions to stabilize..."
    while (( SECONDS < deadline )); do
        local created ready
        created=$(kubectl get ksvc "$svc" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
        ready=$(kubectl get ksvc "$svc" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
        if [[ -n "$created" && -n "$ready" && "$created" == "$ready" ]]; then
            log "  Revision stable: $ready"
            return 0
        fi
        sleep 2
    done
    log "  WARNING: revisions did not stabilize within 300s"
    return 1
}

ensure_restart_policy_kyverno() {
    local target_svc="${1:-}"
    if [[ -z "$target_svc" ]]; then
        log "  ERROR: ensure_restart_policy_kyverno requires target KService name"
        return 1
    fi
    # Delete any stale policy first (match selector may have changed)
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true
    log "  Applying kyverno policy: inject restartPolicy=Never on $target_svc"
    local _policy_yaml
    _policy_yaml=$(cat <<POLICY
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: bench-restart-policy-never
  labels:
    app.kubernetes.io/managed-by: bench-handoff
spec:
  rules:
    - name: set-restart-policy-never
      match:
        any:
          - resources:
              kinds:
                - Deployment
              selector:
                matchLabels:
                  serving.knative.dev/service: "${target_svc}"
      mutate:
        patchStrategicMerge:
          spec:
            template:
              spec:
                containers:
                  - name: user-container
                    restartPolicy: Never
POLICY
    )
    if ! kubectl_apply_retry "$_policy_yaml" 5; then
        log "  WARNING: Kyverno policy apply failed, waiting 10s and retrying..."
        sleep 10
        if ! kubectl_apply_retry "$_policy_yaml" 5; then
            log "  ERROR: failed to apply Kyverno restartPolicy=Never policy after extended retry"
            return 1
        fi
    fi

}


wait_for_operator() {
    local timeout=60 elapsed=0
    while (( elapsed < timeout )); do
        local ready
        ready=$(kubectl get pods -n operator-system \
            -l control-plane=controller-manager \
            -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
        if [[ "$ready" == "True" ]]; then
            return 0
        fi
        sleep 2
        (( elapsed += 2 ))
    done
    log "  WARNING: operator not ready after ${timeout}s"
    return 1
}

# ---- disk cleanup ----------------------------------------------------------

BENCH_NODE_NAME=""
resolve_bench_node() {
    BENCH_NODE_NAME=$(kubectl get nodes -l "$BENCH_NODE_SELECTOR_KEY=$BENCH_NODE_SELECTOR_VAL" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    log "Bench node resolved to $BENCH_NODE_NAME"
}

cleanup_node_disk() {
    # Cleans up disk space on a node to prevent disk-pressure taints:
    #  1) /tmp/ctrd-checkpoint* — CRIU checkpoint temp dirs (~49 MB each)
    #  2) /var/lib/kubelet/checkpoints/* — kubelet checkpoint archives
    #  NOTE: /run/freezer-checkpoints/* and /run/freezer-fifo/* are NOT cleaned
    #        here — the freeze daemon holds in-memory references to active
    #        checkpoints stored there. Deleting them breaks the next restore.
    #  3) crictl rmi --prune — unused container images
    #  4) Remove checkpoint images + snapshots from containerd — CRIU checkpoint
    #     blobs stay referenced in containerd's metadata DB and are never
    #     collected by 'ctr content prune references'. Checkpoint images are
    #     named 'containerd.io/checkpoint/{hash}:{timestamp}'. Stale snapshots
    #     named '*-parent-view' block future checkpoints. Removing both lets
    #     content prune reclaim ~13 MB per checkpoint without destroying other
    #     pods' image blobs.
    #  5) Re-pull app + queue-proxy images so they're cached for next iteration
    #
    # Usage: cleanup_node_disk [node_name]
    #   If node_name is omitted, defaults to BENCH_NODE_NAME.
    local node_name="${1:-$BENCH_NODE_NAME}"
    if [[ -z "$node_name" ]]; then return; fi

    # Resolve images to re-pull after pruning
    local app_image qp_image
    app_image=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.spec.service.container.image}' 2>/dev/null)
    qp_image=$(kubectl get configmap config-deployment -n knative-serving \
        -o jsonpath='{.data.queue-sidecar-image}' 2>/dev/null)

    local pod_name="bench-disk-cleanup-$(echo "$node_name" | cut -d- -f1)"
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2

    # Build the re-pull commands — only for images we actually resolved
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
            "nodeName": "'"$node_name"'",
            "hostPID": true,
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
              "name": "cleanup",
              "image": "busybox",
              "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c",
                "n=$(find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d 2>/dev/null | wc -l); find /tmp -maxdepth 1 -name \"ctrd-checkpoint*\" -type d -exec rm -rf {} + 2>/dev/null; rm -rf /var/lib/kubelet/checkpoints/* 2>/dev/null; k3s crictl rmi --prune >/dev/null 2>&1; k3s ctr -n k8s.io images ls -q 2>/dev/null | grep '^containerd.io/checkpoint/' | xargs -r k3s ctr -n k8s.io images rm >/dev/null 2>&1; k3s ctr -n k8s.io content prune references >/dev/null 2>&1; k3s ctr -n k8s.io snapshots ls 2>/dev/null | grep parent-view | sed \"s/ .*//\" | xargs -r -n1 k3s ctr -n k8s.io snapshots rm >/dev/null 2>&1; '"$pull_cmds"' echo cleaned_checkpoints=$n"],
              "securityContext": {"privileged": true}
            }]
          }
        }' >/dev/null 2>&1
    # Poll for pod completion — kubectl wait hangs if the pod doesn't exist
    # or goes to Failed instead of Succeeded.
    local _cleanup_deadline=$((SECONDS + 180))
    while (( SECONDS < _cleanup_deadline )); do
        local _cleanup_phase
        _cleanup_phase=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null)
        [[ -z "$_cleanup_phase" ]] && break
        [[ "$_cleanup_phase" == "Succeeded" || "$_cleanup_phase" == "Failed" ]] && break
        sleep 2
    done
    local result
    result=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
    if [[ -n "$result" && "$result" != "cleaned_checkpoints=0" ]]; then
        log "  Disk cleanup on $node_name: $result"
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
}

cleanup_node_disk_light() {
    # Light cleanup: checkpoint dirs + containerd checkpoint images/snapshots only.
    # Does NOT run crictl rmi --prune or image re-pull (heavy eMMC I/O that can
    # kill frozen pods). Safe to use during freeze iterations.
    local node_name="${1:-$BENCH_NODE_NAME}"
    if [[ -z "$node_name" ]]; then return; fi

    local pod_name="bench-disk-cleanup-$(echo "$node_name" | cut -d- -f1)"
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2

    kubectl run "$pod_name" -n "$NAMESPACE" --restart=Never --image=busybox \
        --overrides='{
          "spec": {
            "nodeName": "'"$node_name"'",
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
    local _cleanup_deadline=$((SECONDS + 180))
    while (( SECONDS < _cleanup_deadline )); do
        local _cleanup_phase
        _cleanup_phase=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null)
        [[ -z "$_cleanup_phase" ]] && break
        [[ "$_cleanup_phase" == "Succeeded" || "$_cleanup_phase" == "Failed" ]] && break
        sleep 2
    done
    local result
    result=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
    if [[ -n "$result" && "$result" != "cleaned_checkpoints=0" ]]; then
        log "  Disk cleanup (light) on $node_name: $result"
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
}

# ---- EA patching -----------------------------------------------------------

ea_yaml() {
    local freeze="$1" node_key="$2" node_val="$3"
    local target_replica="$4" node_selector_json="$5"
    local cleanup_on_delete="${6:-true}" idle_timeout="${7:-$FREEZER_IDLE_TIMEOUT}"
    cat <<EOFEA
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplication
metadata:
  name: $EA_NAME
  namespace: $NAMESPACE
spec:
  dId: "retransmitter-handoff-appd-v1"
  name: "retransmitter-handoff"
  provider: "example.mec"
  softVersion: "1.0.0"
  dVersion: "1.0.0"
  infoName: "Retransmitter with handoff"
  description: "Retransmits packet then triggers an EdgeApplicationHandoff to a target replica"
  service:
    freezeEnabled: $freeze
    freezeIdleTimeout: $idle_timeout
    nodeSelector:
      $node_key: "$node_val"
    container:
      image: ghcr.io/pmacoutinho/retransmitter-handoff:latest
      env:
        - name: FORWARD_URL
          value: "http://http-sink.default.svc.cluster.local"
        - name: HOP_NAME
          value: "retransmitter-handoff"
        - name: HANDOFF_EA_NAME
          value: "$EA_NAME"
        - name: HANDOFF_TARGET_REPLICA
          value: "$target_replica"
        - name: HANDOFF_NAMESPACE
          value: "$NAMESPACE"
        - name: HANDOFF_CLEANUP_ON_DELETE
          value: "$cleanup_on_delete"
        - name: HANDOFF_NODE_SELECTOR
          value: '$node_selector_json'
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
        limits:
          cpu: 250m
          memory: 128Mi
      securityContext:
        runAsUser: 0
    serviceAccountName: retransmitter-handoff-sa
    triggerFilters: []
EOFEA
}

delete_ea_and_wait() {
    log "  Deleting EdgeApplication and waiting for cleanup..."
    kubectl delete edgeapplication "$EA_NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
    sleep 3
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local count
        count=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$SERVICE_NAME" \
            --no-headers 2>/dev/null | wc -l)
        if [[ "$count" -eq 0 ]]; then
            if ! kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
                log "  Clean slate."
                return 0
            fi
        fi
        sleep 3
    done
    log "  WARNING: cleanup timed out"
}

# ---- self-contained resource creation --------------------------------------

ensure_handoff_rbac() {
    # Create the RBAC resources needed by the retransmitter-handoff service account.
    # Idempotent — safe to call multiple times.
    kubectl_apply_retry "$(cat <<'RBAC_EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: retransmitter-handoff-sa
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: edgeapplicationhandoff-manager
rules:
  - apiGroups: ["mec.atnog.org"]
    resources: ["edgeapplicationhandoffs"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: retransmitter-handoff-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: edgeapplicationhandoff-manager
subjects:
  - kind: ServiceAccount
    name: retransmitter-handoff-sa
    namespace: default
RBAC_EOF
)" 5
    log "  RBAC resources ensured."
}

delete_handoff_resources() {
    log "  Deleting EdgeApplication and RBAC created by benchmark..."
    kubectl delete edgeapplication "$EA_NAME" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
    kubectl delete clusterrolebinding retransmitter-handoff-binding --ignore-not-found >/dev/null 2>&1 || true
    kubectl delete clusterrole edgeapplicationhandoff-manager --ignore-not-found >/dev/null 2>&1 || true
    kubectl delete serviceaccount retransmitter-handoff-sa -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
    log "  Handoff resources deleted."
}

# ---- cleanup ---------------------------------------------------------------

EA_BACKUP=""
TRIGGERS_BACKUP=""
cleanup() {
    log "Cleanup..."
    # Clean all nodes that may have accumulated CRIU checkpoint blobs.
    cleanup_node_disk "$BENCH_NODE_NAME"
    local w2_node
    w2_node=$(kubectl get nodes -l vm-id=worker-2 \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -n "$w2_node" && "$w2_node" != "$BENCH_NODE_NAME" ]]; then
        cleanup_node_disk "$w2_node"
    fi
    # Clean RSU nodes used as handoff targets
    local rsu_node
    for rsu_label in rsu-a rsu-b; do
        rsu_node=$(kubectl get nodes -l "rsu-id=$rsu_label" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [[ -n "$rsu_node" && "$rsu_node" != "$BENCH_NODE_NAME" ]]; then
            cleanup_node_disk "$rsu_node"
        fi
    done
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    cleanup_load_check_pod
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true
    kubectl delete clusterpolicy bench-freeze-idle-timeout --ignore-not-found >/dev/null 2>&1 || true
    restore_queue_proxy_image

    # Delete any leftover handoff CRs and target KServices
    kubectl delete edgeapplicationhandoff --all -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
    # Delete target ksvc replicas (operator may not clean up fast enough)
    kubectl get ksvc -n "$NAMESPACE" --no-headers 2>/dev/null \
        | awk '{print $1}' | grep -v "^${SERVICE_NAME}$" \
        | xargs -r kubectl delete ksvc -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true

    if [[ "$EA_CREATED_BY_US" == "true" ]]; then
        # We created the EA and RBAC — delete them entirely.
        delete_handoff_resources
    else
        # Restore original EA (delete + create to avoid merge issues with nodeSelector)
        if [[ -n "$EA_BACKUP" ]]; then
            log "Restoring original EdgeApplication..."
            kubectl delete edgeapplication "$EA_NAME" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
            sleep 2
            echo "$EA_BACKUP" | strip_server_fields 2>/dev/null \
                | kubectl apply -f - >/dev/null 2>&1 \
                || log "  WARNING: failed to restore $EA_NAME"
            log "  Restored $EA_NAME."
        fi

        # Restore triggers (only if none exist — the EA controller may
        # have already recreated them from triggerFilters during EA restore)
        local existing_triggers
        existing_triggers=$(kubectl get triggers -n "$NAMESPACE" \
            -l "mec.atnog.org/app=$EA_NAME" --no-headers 2>/dev/null | wc -l)
        if [[ "${existing_triggers:-0}" -eq 0 ]] && \
           [[ -n "$TRIGGERS_BACKUP" ]] && printf '%s' "$TRIGGERS_BACKUP" | grep -q 'apiVersion:'; then
            echo "$TRIGGERS_BACKUP" | strip_server_fields 2>/dev/null \
                | kubectl apply -f - >/dev/null 2>&1 \
                || log "  WARNING: failed to restore triggers"
            log "  Restored Knative Triggers from backup."
        else
            log "  Triggers already present (operator recreated them)."
        fi
    fi
}
trap cleanup EXIT

# ---- single iteration (cold start handoff) ---------------------------------
#
# Applies the handoff CR directly (no retransmission event) so cold-start
# and freeze iterations measure the same pipeline: CR apply → operator
# reconciles → target KService created → pod ready → CR Ready.

run_handoff_iteration() {
    local scenario="$1" iteration="$2" handoff_target="$3"
    local handoff_node_selector="$4"

    # Delete any existing handoff CR from previous iteration
    delete_handoff_cr "$handoff_target"

    # Wait for target KService and pods cleanup (operator deletes via cleanupOnDelete)
    local target_svc="${EA_NAME}-${handoff_target}"
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
    kubectl wait pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$target_svc" \
        --for=delete --timeout=90s >/dev/null 2>&1 || true

    # Build and apply handoff CR — measure from CR apply to CR Ready
    local _cr_yaml
    _cr_yaml=$(_build_handoff_cr "$handoff_target" "true" "$handoff_node_selector")

    local ts_before handoff_start
    ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    handoff_start=$(date +%s%N)

    if ! kubectl_apply_retry "$_cr_yaml"; then
        log "  SKIPPED: failed to apply handoff CR after retries"
        return
    fi
    log "  Waiting for target pod 2/2 Ready..."

    local handoff_end handoff_wall_ms phase ts_after
    if wait_for_handoff_ready "$handoff_target"; then
        handoff_end=$(date +%s%N)
        handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        phase=$(get_handoff_phase "$handoff_target")
        log "  Result: ${handoff_wall_ms}ms cold start (phase=$phase)"
        emit_result "$scenario" "$iteration" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
    else
        handoff_end=$(date +%s%N)
        handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        phase=$(get_handoff_phase "$handoff_target")
        log "  TIMEOUT: ${handoff_wall_ms}ms (phase=$phase)"
        emit_result "$scenario" "$iteration" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
    fi

    # Cleanup handoff CR so next iteration starts fresh
    delete_handoff_cr "$handoff_target"

    # Wait for target KService and ALL its pods to fully terminate.
    # On I/O-constrained nodes (RSU ARM64), a Terminating pod causes severe
    # contention with the next iteration's pod creation, inflating times.
    log "  Waiting for scale to zero..."
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
    kubectl wait pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$target_svc" \
        --for=delete --timeout=90s >/dev/null 2>&1 || true
}

# ---- scenario runners -------------------------------------------------------

run_scenario() {
    local scenario="$1" freeze="$2" node_key="$3" node_val="$4"
    local handoff_target="$5" handoff_node_selector="$6"
    local iterations="$7"

    # Resolve target node name from the handoff_node_selector JSON
    # e.g. '{"rsu-id":"rsu-a"}' → find node with label rsu-id=rsu-a
    local target_node_name=""
    if [[ -n "$handoff_node_selector" ]]; then
        local _sel_key _sel_val
        _sel_key=$(echo "$handoff_node_selector" | python3 -c "import sys,json; d=json.load(sys.stdin); print(list(d.keys())[0])" 2>/dev/null)
        _sel_val=$(echo "$handoff_node_selector" | python3 -c "import sys,json; d=json.load(sys.stdin); print(list(d.values())[0])" 2>/dev/null)
        if [[ -n "$_sel_key" && -n "$_sel_val" ]]; then
            target_node_name=$(kubectl get nodes -l "$_sel_key=$_sel_val" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        fi
    fi

    log "=== SCENARIO: $scenario ($iterations iterations) ==="
    log "  Measures full cold start handoff latency (direct CR apply)"
    log "  Source: $node_key=$node_val, Target: $handoff_target (node: ${target_node_name:-unknown})"

    # Set up load-check pod on the target node and wait for it to settle.
    if [[ -n "$_sel_key" && -n "$_sel_val" ]]; then
        setup_load_check_pod "$_sel_key" "$_sel_val"
        check_target_load
    fi

    delete_ea_and_wait
    log "  Creating EA (freezeEnabled=$freeze)..."
    ea_yaml "$freeze" "$node_key" "$node_val" "$handoff_target" "$handoff_node_selector" "true" \
        | kubectl apply -f - >/dev/null 2>&1

    # Source pod rollout is not waited on — we apply the handoff CR directly
    # so the source pod is unused. Let it stabilize in the background.
    log "  Warmup iteration (direct CR apply)..."
    delete_handoff_cr "$handoff_target"
    local _warmup_cr
    _warmup_cr=$(_build_handoff_cr "$handoff_target" "true" "$handoff_node_selector")
    kubectl_apply_retry "$_warmup_cr" || true
    # Wait for warmup handoff with a short timeout. The CR may get deleted
    # by the operator (cleanupOnDelete=true) before reaching Ready, causing
    # kubectl wait to hang on a non-existent resource. Use a direct poll
    # loop with a 120s timeout instead.
    local _warmup_deadline=$((SECONDS + 120))
    while (( SECONDS < _warmup_deadline )); do
        # Check if CR still exists — if not, operator deleted it (cleanupOnDelete)
        if ! kubectl get edgeapplicationhandoff "${HANDOFF_CR_PREFIX}${handoff_target}" \
            -n "$NAMESPACE" --no-headers >/dev/null 2>&1; then
            break
        fi
        local _warmup_phase
        _warmup_phase=$(kubectl get edgeapplicationhandoff "${HANDOFF_CR_PREFIX}${handoff_target}" \
            -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)
        if [[ "$_warmup_phase" == "Ready" || "$_warmup_phase" == "Failed" ]]; then
            break
        fi
        sleep 2
    done
    delete_handoff_cr "$handoff_target"
    # Wait for target KService AND all pods to fully terminate before first
    # measured iteration. On RSU, leftover Terminating pods cause I/O contention.
    local target_svc="${EA_NAME}-${handoff_target}"
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
    kubectl wait pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$target_svc" \
        --for=delete --timeout=90s >/dev/null 2>&1 || true

    # Wait for load to settle after warmup before starting measured iterations
    log "  Waiting for load to settle after warmup..."
    check_target_load

    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- $scenario iteration $i/$iterations ---"

        # Pre-check: wait for target node load to settle
        check_target_load

        # Pre-check: ensure operator is running before starting iteration
        if ! wait_for_operator; then
            log "  SKIPPED: operator not ready"
            continue
        fi

        run_handoff_iteration "$scenario" "$i" "$handoff_target" "$handoff_node_selector"
    done
}

# ---- freeze scenario --------------------------------------------------------
#
# This scenario measures full handoff latency when the target pod is frozen
# via CRIU. Like cold-start scenarios, each iteration does a complete handoff:
#   event → retransmitter → handoff CR → operator detects frozen target →
#   CRIU thaw → handoff Ready.
#
# The target KService is pre-created (primed) and its pod freezes after ~30s
# idle. cleanupOnDelete=false keeps the KService alive across iterations.
# Between iterations, we wait for the target to re-freeze.
#
# triggerFilters are cleared so broker traffic doesn't wake frozen pods.
#
# Flow:
#   1) Apply kyverno restartPolicy=Never (required for CRIU)
#   2) Patch EA: freeze=true, triggerFilters=[], cleanupOnDelete=false
#   3) Warmup: send event → handoff CR → target KService created → Ready
#   4) Delete warmup CR, wait for target to freeze
#   5) For each iteration:
#      a) Delete old handoff CR (KService survives)
#      b) Scale source to zero
#      c) Verify target is frozen
#      d) Send event → retransmitter creates handoff CR
#      e) Operator thaws frozen target → handoff Ready
#      f) Measure time
#      g) Wait for target to re-freeze (~30s)

run_freeze_scenario() {
    local scenario_name="$1" iterations="$2" handoff_target="$3"
    local handoff_node_selector="$4" target_node_key="$5" target_node_val="$6"
    local idle_timeout="${7:-$FREEZER_IDLE_TIMEOUT}"
    local target_svc="${EA_NAME}-${handoff_target}"

    # Resolve the target node name for disk cleanup during iterations
    local target_node_name
    target_node_name=$(kubectl get nodes -l "$target_node_key=$target_node_val" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    # Find the freeze daemon pod on the target node for freeze detection
    _TARGET_NODE_NAME="$target_node_name"
    _TARGET_FREEZE_DAEMON=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
        | grep freeze-daemon-containerd | grep "$target_node_name" | awk '{print $1}')

    log "=== SCENARIO: $scenario_name ($iterations iterations) ==="
    log "  Measures full handoff latency with CRIU-frozen target"
    log "  Target KService: $target_svc (node: $target_node_name, daemon: $_TARGET_FREEZE_DAEMON)"

    # Step 0: Launch load-check pod and wait for RSU to settle before proceeding.
    setup_load_check_pod "$target_node_key" "$target_node_val"
    check_target_load

    # Step 1: Delete EA and recreate with the right config from the start.
    # triggerFilters=[] prevents the broker from waking frozen pods.
    # cleanupOnDelete=false so the target KService survives handoff CR deletion.
    delete_ea_and_wait
    ensure_restart_policy_kyverno "$target_svc"
    log "  Creating EA (freezeEnabled=true, idle=${idle_timeout}s)..."
    ea_yaml "true" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
        "$handoff_target" "$handoff_node_selector" "false" "$idle_timeout" \
        | kubectl apply -f - >/dev/null 2>&1

    # Step 2: Clean any lingering target state from previous runs.
    delete_handoff_cr "$handoff_target"
    kubectl delete ksvc "$target_svc" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=30s >/dev/null 2>&1 || true
    kubectl wait pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$target_svc" \
        --for=delete --timeout=90s >/dev/null 2>&1 || true

    # Step 4: Warmup — create the target KService via direct handoff CR.
    # The Kyverno policy is already in place, so the first pod will have
    # restartPolicy=Never. It will start, run briefly, then CRIU will
    # checkpoint it after the 5s idle timeout. We wait for freeze directly.
    log "  Warmup: priming target KService via direct CR..."

    if ! kubectl apply -f - >/dev/null 2>&1 <<EOF
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplicationHandoff
metadata:
  name: ${HANDOFF_CR_PREFIX}${handoff_target}
  namespace: ${NAMESPACE}
spec:
  edgeApplicationName: ${EA_NAME}
  targetReplicaName: ${handoff_target}
  cleanupOnDelete: false
  nodeSelector:
    ${target_node_key}: ${target_node_val}
EOF
    then
        log "  ERROR: failed to apply warmup handoff CR, aborting freeze scenario"
        return
    fi

    # Don't wait for handoff CR Ready — with short idle timeouts the freezer
    # checkpoints the user-container before the KService reaches Ready, leaving
    # the CR stuck at Applied. Instead, wait for the target pod to exist and
    # freeze directly. The warmup only needs a frozen pod, not a Ready CR.
    # Retry if the pod disappears (Kyverno rollout can replace it).
    local warmup_pod="" freeze_ok=false freeze_baseline="0"
    local deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
        warmup_pod=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$target_svc" \
            --field-selector=status.phase!=Succeeded,status.phase!=Failed \
            --sort-by=.metadata.creationTimestamp \
            -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
        if [[ -z "$warmup_pod" ]]; then
            sleep 2
            continue
        fi
        # Wait for the pod to be ready (2/2) before expecting a freeze.
        # The idle timeout only starts once the container is serving.
        log "  Target pod: $warmup_pod — waiting for ready..."
        if kubectl wait pod "$warmup_pod" -n "$NAMESPACE" \
            --for=condition=Ready --timeout=120s >/dev/null 2>&1; then
            log "  Target pod ready — waiting for freeze..."
        else
            log "  Target pod did not reach Ready, waiting for freeze anyway..."
        fi
        if wait_for_target_freeze "$warmup_pod" "" "$target_node_name"; then
            freeze_ok=true
            break
        fi
        # Pod disappeared or freeze failed — retry with a newer one
        log "  Retrying with next target pod..."
        sleep 2
    done

    delete_handoff_cr "$handoff_target"

    if [[ "$freeze_ok" != "true" ]]; then
        log "  ERROR: target did not freeze after warmup within 300s, aborting"
        return
    fi
    log "  Target frozen — waiting for load to settle after warmup..."
    check_target_load
    log "  Starting measured iterations"

    local _consec_freeze_fails=0

    # Step 5: Run measured iterations.
    # The target pod is already frozen from warmup. Each iteration triggers
    # a handoff (CRIU thaw), measures the latency, then waits for the pod
    # to re-freeze via idle timeout before the next iteration.
    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- $scenario_name iteration $i/$iterations ---"

        # Pre-check: wait for RSU load to settle
        check_target_load

        # Pre-check: ensure operator is running before starting iteration
        if ! wait_for_operator; then
            log "  SKIPPED: operator not ready"
            continue
        fi

        # Re-resolve freeze daemon if it was lost (e.g. after a failed restart).
        # Block and wait — on RSU the DaemonSet pod can take 5-20 minutes to
        # come back. Spinning through iterations wastes all remaining samples.
        if [[ -z "$_TARGET_FREEZE_DAEMON" ]]; then
            log "  Waiting for freeze daemon to come back on $target_node_name (up to 1200s)..."
            local _daemon_wait_deadline=$((SECONDS + 1200))
            while (( SECONDS < _daemon_wait_deadline )); do
                _TARGET_FREEZE_DAEMON=$(kubectl get pods -n knative-serving -o wide --no-headers 2>/dev/null \
                    | grep freeze-daemon-containerd | grep "$target_node_name" \
                    | grep Running | awk '{print $1}')
                if [[ -n "$_TARGET_FREEZE_DAEMON" ]]; then
                    log "  Freeze daemon recovered: $_TARGET_FREEZE_DAEMON"
                    break
                fi
                sleep 10
            done
            if [[ -z "$_TARGET_FREEZE_DAEMON" ]]; then
                log "  ERROR: freeze daemon did not recover within 1200s, aborting scenario"
                break
            fi
        fi

        # Delete handoff CR from previous iteration (KService survives)
        delete_handoff_cr "$handoff_target"

        # Wait for the target to re-freeze before triggering the next handoff.
        local target_pod
        target_pod=$(kubectl get pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$target_svc" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

        # Helper: wait for ALL pods on the target KService to fully terminate.
        # Prevents pod pile-up that overwhelms the RSU.
        # Force-deletes Terminating pods after 60s — CRIU-frozen containers
        # have no meaningful graceful shutdown, and stuck Terminating pods
        # cause I/O contention that spirals into RSU overload.
        _wait_all_target_pods_gone() {
            log "  Waiting for all target pods to terminate..."
            local _gone_deadline=$((SECONDS + 180)) _force_deleted=false
            while (( SECONDS < _gone_deadline )); do
                local _count
                _count=$(kubectl get pods -n "$NAMESPACE" \
                    -l "serving.knative.dev/service=$target_svc" \
                    --no-headers 2>/dev/null | wc -l)
                if (( _count == 0 )); then
                    return 0
                fi
                # After 60s, force-delete any remaining pods (likely stuck Terminating)
                if (( SECONDS > _gone_deadline - 120 )) && [[ "$_force_deleted" != "true" ]]; then
                    log "  Force-deleting stuck pods on target KService..."
                    kubectl delete pods -n "$NAMESPACE" \
                        -l "serving.knative.dev/service=$target_svc" \
                        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
                    _force_deleted=true
                fi
                sleep 5
            done
            log "  WARNING: pods still present after 180s"
            return 1
        }

        # Helper: recycle the target pod and re-prime the KService
        _reprime_target() {
            log "  Recycling target — deleting pod and re-priming KService..."
            delete_handoff_cr "$handoff_target"
            kubectl delete pod "$target_pod" -n "$NAMESPACE" --grace-period=0 --force >/dev/null 2>&1 || true
            kubectl delete ksvc "$target_svc" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
            # Wait for ALL pods to be fully gone — not just the target pod.
            # On RSU, Terminating pods cause I/O contention that prevents new
            # pods from freezing, creating a death spiral.
            _wait_all_target_pods_gone
            # Wait for RSU load to settle before creating a new pod.
            # The recycle itself (pod deletion, CRIU cleanup) spikes load.
            check_target_load
            local _reprime_cr
            _reprime_cr=$(_build_handoff_cr "$handoff_target" "false" "$handoff_node_selector")
            if ! kubectl_apply_retry "$_reprime_cr"; then
                return 1
            fi
            local _reprime_deadline=$((SECONDS + 300))
            while (( SECONDS < _reprime_deadline )); do
                target_pod=$(kubectl get pods -n "$NAMESPACE" \
                    -l "serving.knative.dev/service=$target_svc" \
                    --field-selector=status.phase!=Succeeded,status.phase!=Failed \
                    --sort-by=.metadata.creationTimestamp \
                    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
                if [[ -n "$target_pod" ]]; then
                    log "  Re-primed target pod: $target_pod — waiting for freeze..."
                    if wait_for_target_freeze "$target_pod" "" "$target_node_name"; then
                        freeze_baseline=$(get_target_freeze_count "$target_pod" "$_TARGET_FREEZE_DAEMON")
                        delete_handoff_cr "$handoff_target"
                        return 0
                    fi
                    break
                fi
                sleep 2
            done
            delete_handoff_cr "$handoff_target"
            return 1
        }

        # Proactive pod recycling: CRIU on ARM64 RSU degrades after ~70-80
        # checkpoint cycles. Recycle before that and clean up disk.
        local _skip_freeze_wait=false _skip_measurement=false
        if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
            log "  Proactive pod recycle (every ${CHECKPOINT_CLEANUP_INTERVAL} iterations)..."
            cleanup_node_disk_light "$target_node_name"
            if _reprime_target; then
                log "  Proactive recycle complete — pod frozen and ready"
                _skip_freeze_wait=true
                _skip_measurement=true
            else
                log "  WARNING: proactive recycle failed, continuing with current pod"
            fi
        fi

        if $_skip_freeze_wait; then
            : # Pod already frozen by proactive recycle
        elif [[ -z "$target_pod" ]]; then
            log "  WARNING: target pod gone, re-priming..."
            if ! _reprime_target; then
                log "  SKIPPED: failed to re-prime and freeze target"
                continue
            fi
            log "  Target re-primed and frozen — resuming iterations"
            _skip_measurement=true
        else
            log "  Waiting for target to re-freeze ($target_pod)..."
            if ! wait_for_target_freeze "$target_pod" "$freeze_baseline" "$target_node_name"; then
                # Before recycling, check if Kyverno replaced the pod and
                # the replacement is already running. If so, just switch to it.
                local _new_pod
                _new_pod=$(kubectl get pods -n "$NAMESPACE" \
                    -l "serving.knative.dev/service=$target_svc" \
                    --field-selector=status.phase!=Succeeded,status.phase!=Failed \
                    --sort-by=.metadata.creationTimestamp \
                    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
                if [[ -n "$_new_pod" && "$_new_pod" != "$target_pod" ]]; then
                    log "  Pod replaced by Kyverno rollout: $target_pod → $_new_pod"
                    target_pod="$_new_pod"
                    # The new pod may already be frozen — check directly
                    if wait_for_target_freeze "$target_pod" "" "$target_node_name"; then
                        freeze_baseline=$(get_target_freeze_count "$target_pod" "$_TARGET_FREEZE_DAEMON")
                        log "  Replacement pod already frozen — continuing"
                    else
                        log "  Replacement pod failed to freeze — recycling..."
                        sleep 60
                        if ! _reprime_target; then
                            log "  SKIPPED: recycle failed"
                            continue
                        fi
                        _skip_measurement=true
                    fi
                else
                    # Genuine freeze failure — recycle with backoff.
                    log "  Freeze failed — waiting 60s for RSU to settle before recycling..."
                    sleep 60
                    if _reprime_target; then
                        log "  Target recycled and frozen — resuming"
                        _skip_measurement=true
                    else
                        log "  Recycle failed — backing off 5min to let RSU recover..."
                        sleep 300
                        if _reprime_target; then
                            log "  Recovery succeeded after backoff"
                            _skip_measurement=true
                        else
                            log "  SKIPPED: RSU still not recovering"
                            continue
                        fi
                    fi
                fi
            fi
        fi
        if $_skip_measurement; then
            log "  Post-recycle warmup — skipping measurement this iteration"
        fi
        log "  Container frozen (confirmed by freeze daemon)."

        # Snapshot freeze count BEFORE thaw — after thaw the pod re-freezes
        # quickly, so snapshotting after would race.
        freeze_baseline=$(get_target_freeze_count "$target_pod" "$_TARGET_FREEZE_DAEMON")

        # Create the handoff CR and measure time to thawCompleted.
        local ts_before handoff_start
        ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        handoff_start=$(date +%s%N)

        local _cr_yaml
        _cr_yaml=$(cat <<EOF
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplicationHandoff
metadata:
  name: ${HANDOFF_CR_PREFIX}${handoff_target}
  namespace: ${NAMESPACE}
spec:
  edgeApplicationName: ${EA_NAME}
  targetReplicaName: ${handoff_target}
  cleanupOnDelete: false
  nodeSelector:
    ${target_node_key}: ${target_node_val}
EOF
        )
        if ! kubectl_apply_retry "$_cr_yaml"; then
            log "  SKIPPED: failed to apply handoff CR after retries"
            continue
        fi

        local handoff_end handoff_wall_ms phase ts_after
        if wait_for_handoff_thaw "$handoff_target" "$FREEZE_THAW_TIMEOUT"; then
            handoff_end=$(date +%s%N)
            handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
            ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
            phase=$(get_handoff_phase "$handoff_target")
            if $_skip_measurement; then
                log "  WARMUP (post-recycle): ${handoff_wall_ms}ms thaw — not recorded"
            else
                log "  Result: ${handoff_wall_ms}ms thaw (phase=$phase)"
                emit_result "$scenario_name" "$i" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
            fi
        else
            handoff_end=$(date +%s%N)
            handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
            ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
            phase=$(get_handoff_phase "$handoff_target")
            log "  TIMEOUT: ${handoff_wall_ms}ms thaw (phase=$phase) — recycling..."
            if ! $_skip_measurement; then
                emit_result "$scenario_name" "$i" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
            fi
            # Thaw timed out — wait for RSU to settle, then recycle.
            log "  Waiting 60s for RSU to settle before recycling..."
            sleep 60
            if _reprime_target; then
                log "  Recovery recycle succeeded"
            else
                log "  Recovery failed — backing off 5min..."
                sleep 300
                _reprime_target || log "  WARNING: recovery still failing"
            fi
        fi
    done
}

# ---- main ------------------------------------------------------------------

main() {
    # Parse args
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --iterations) ITERATIONS="$2"; shift 2 ;;
            --scenario)   SCENARIO="$2"; shift 2 ;;
            *)            log "Unknown arg: $1"; exit 1 ;;
        esac
    done

    log "Handoff Benchmark"
    log "  iterations=$ITERATIONS, scenario=$SCENARIO"
    log "  cleanup_interval=$CHECKPOINT_CLEANUP_INTERVAL, freeze_idle_timeout=${FREEZER_IDLE_TIMEOUT}s"
    log "  output=$OUTFILE"

    # Backup EA state and triggers if they exist; ensure RBAC is in place.
    EA_BACKUP=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null || echo "")
    TRIGGERS_BACKUP=$(kubectl get triggers -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" -o yaml 2>/dev/null || echo "")
    if [[ -z "$EA_BACKUP" ]]; then
        EA_CREATED_BY_US=true
    fi
    # Ensure RBAC exists (idempotent). Each scenario creates the EA itself.
    ensure_handoff_rbac

    resolve_bench_node

    # Check for disk-pressure taint — clean up before proceeding
    local disk_pressure
    disk_pressure=$(kubectl get node "$BENCH_NODE_NAME" \
        -o jsonpath='{.spec.taints[?(@.key=="node.kubernetes.io/disk-pressure")].effect}' 2>/dev/null)
    if [[ -n "$disk_pressure" ]]; then
        log "WARNING: $BENCH_NODE_NAME has disk-pressure taint, running cleanup..."
        cleanup_node_disk
        sleep 10
        disk_pressure=$(kubectl get node "$BENCH_NODE_NAME" \
            -o jsonpath='{.spec.taints[?(@.key=="node.kubernetes.io/disk-pressure")].effect}' 2>/dev/null)
        if [[ -n "$disk_pressure" ]]; then
            log "ERROR: $BENCH_NODE_NAME still has disk-pressure taint after cleanup."
            log "       SSH into the node and free disk manually, then retry."
            exit 1
        fi
        log "  Disk-pressure taint cleared."
    fi

    # Remove any global restartPolicy=Never policy that may linger from
    # bench_freeze_vs_coldstart.sh or manual testing.  The handoff bench
    # applies its own targeted policy per-scenario AFTER the warmup pod is
    # confirmed 2/2 Ready.  A global policy would inject restartPolicy=Never
    # into the warmup pod, causing CRIU to kill the user-container with no
    # restart (exit 137) before we even confirm readiness.
    kubectl delete clusterpolicy inject-container-restart-policy-never \
        --ignore-not-found >/dev/null 2>&1 || true

    pin_queue_proxy_image

    # Run selected scenarios — each is isolated so one failure doesn't abort the rest.
    local _scenario_failed=0
    case "$SCENARIO" in
        rsu-freeze|rsu|all)
            local _saved_key="$BENCH_NODE_SELECTOR_KEY" _saved_val="$BENCH_NODE_SELECTOR_VAL"
            BENCH_NODE_SELECTOR_KEY="rsu-id" BENCH_NODE_SELECTOR_VAL="rsu-b"
            run_freeze_scenario "rsu-a-freeze" "$ITERATIONS" \
                "rsu-a" '{"rsu-id":"rsu-a"}' "rsu-id" "rsu-a" "$FREEZER_IDLE_TIMEOUT" \
                || { log "ERROR: rsu-a-freeze scenario failed, continuing..."; _scenario_failed=1; }
            BENCH_NODE_SELECTOR_KEY="$_saved_key" BENCH_NODE_SELECTOR_VAL="$_saved_val"
            ;;&
        rsu-cold|rsu|all)
            run_scenario "rsu-a-coldstart" "false" "rsu-id" "rsu-b" \
                "rsu-a" '{"rsu-id":"rsu-a"}' "$ITERATIONS" \
                || { log "ERROR: rsu-a-coldstart scenario failed, continuing..."; _scenario_failed=1; }
            ;;&
        worker1-cold|all)
            run_scenario "worker1-coldstart" "false" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
                "worker1-target" '{"vm-id":"worker-1"}' "$ITERATIONS" \
                || { log "ERROR: worker1-coldstart scenario failed, continuing..."; _scenario_failed=1; }
            ;;&
        worker1-freeze|all)
            run_freeze_scenario "worker1-freeze" "$ITERATIONS" \
                "worker1-target" '{"vm-id":"worker-1"}' "vm-id" "worker-1" \
                || { log "ERROR: worker1-freeze scenario failed, continuing..."; _scenario_failed=1; }
            ;;&
        worker2-cold|all)
            run_scenario "worker2-coldstart" "false" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
                "worker2-target" '{"vm-id":"worker-2"}' "$ITERATIONS" \
                || { log "ERROR: worker2-coldstart scenario failed, continuing..."; _scenario_failed=1; }
            ;;&
        worker2-freeze|all)
            run_freeze_scenario "worker2-freeze" "$ITERATIONS" \
                "worker2-target" '{"vm-id":"worker-2"}' "vm-id" "worker-2" \
                || { log "ERROR: worker2-freeze scenario failed, continuing..."; _scenario_failed=1; }
            ;;
    esac

    if (( _scenario_failed )); then
        log "WARNING: one or more scenarios failed — check logs above"
    fi

    if [[ -f "$OUTFILE" ]]; then
        log "Analyzing results..."
        python3 "${SCRIPT_DIR}/analyze_handoff_bench.py" "$OUTFILE"
    else
        log "No results to analyze (no data written)"
    fi

    log "Done. Results: $OUTFILE"
}

main "$@"
