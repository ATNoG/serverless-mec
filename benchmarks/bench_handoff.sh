#!/usr/bin/env bash
set -euo pipefail

# ===========================================================================
#  Handoff Benchmark
#
#  Measures the full pipeline: event send -> retransmission -> handoff CR
#  creation -> operator applies replica -> target KService ready.
#
#  Three scenarios:
#    1) RSU-A cold start handoff   (normal queue-proxy, no freeze)
#    2) Worker-1 cold start handoff (no freeze, handoff to worker-1)
#    3) Worker-1 CRIU thaw handoff  (freeze enabled on target, measures
#       thaw latency of a frozen target pod after handoff is primed)
#
#  Usage:
#    ./bench_handoff.sh [--iterations N] [--scenario all|rsu-a|worker1-cold|worker1-freeze]
# ===========================================================================

# ---- configuration --------------------------------------------------------

EA_NAME="retransmitter-handoff"
NAMESPACE="default"
SERVICE_NAME="retransmitter-handoff"
SERVICE_URL="http://retransmitter-handoff.default.svc.cluster.local"

ITERATIONS=${ITERATIONS:-20}
SCENARIO=${SCENARIO:-"all"}

HANDOFF_CR_PREFIX="${EA_NAME}-to-"
HANDOFF_POLL_TIMEOUT=300  # seconds
CHECKPOINT_CLEANUP_INTERVAL=5

QUEUE_PROXY_PORT=8012
FREEZE_WAIT_TIMEOUT=300

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_DIR="$SCRIPT_DIR/../knative-freezer-plugin"
LOG_DIR="${SCRIPT_DIR}/handoff_bench_logs"
mkdir -p "$LOG_DIR"
OUTFILE="${LOG_DIR}/handoff_bench_$(date +%Y%m%d_%H%M%S).ndjson"

BENCH_NODE_SELECTOR_KEY="vm-id"
BENCH_NODE_SELECTOR_VAL="worker-1"

# ---- logging --------------------------------------------------------------

log() { echo "[$(date +%H:%M:%S)] $*"; }

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
    local handoff_phase="$6" handoff_time_ms="$7"

    IFS=',' read -r t_dns t_connect t_ttfb t_total http_code <<< "$timings"

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
    'handoff_time_ms': float(sys.argv[11]) if sys.argv[11] != '' else None,
}
print(json.dumps(r, separators=(',',':')))
" "$scenario" "$iteration" "$t_dns" "$t_connect" "$t_ttfb" "$t_total" \
  "$http_code" "$ts_before" "$ts_after" "$handoff_phase" "$handoff_time_ms" \
  >> "$OUTFILE"
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

send_event() {
    local url="$1"
    local host_header="${2:-}"
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
        -d '{"frame_raw_hex":"0011223344556677","frame_number":1}' \
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
    local max_wait=$HANDOFF_POLL_TIMEOUT elapsed=0
    while (( elapsed < max_wait )); do
        local phase
        phase=$(kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [[ "$phase" == "Ready" ]]; then
            return 0
        fi
        if [[ "$phase" == "Failed" ]]; then
            return 1
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    return 2  # timeout
}

get_handoff_phase() {
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown"
}

# ---- freeze helpers (for worker1-freeze scenario) --------------------------

get_target_pod_name() {
    local target_svc="$1"
    kubectl get pods -n "$NAMESPACE" -l "serving.knative.dev/service=$target_svc" \
        --field-selector=status.phase=Running \
        -o jsonpath='{range .items[?(@.metadata.annotations.qpoption\.knative\.dev/freezer-activate=="enable")]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
        | head -1
}

get_pod_ip() {
    kubectl get pod "$1" -n "$NAMESPACE" -o jsonpath='{.status.podIP}' 2>/dev/null
}

wait_for_target_pod_ready() {
    local target_svc="$1"
    log "  Waiting for target pod 2/2 Ready (freezer-enabled, $target_svc)..."
    local deadline=$((SECONDS + 120))
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
    log "  WARNING: target pod not 2/2 ready within 120s"
    return 1
}

wait_for_target_freeze() {
    local pod="$1"
    log "  Waiting for freezer to checkpoint target pod $pod..."

    # Poll the queue-proxy logs (without -f) rather than streaming. This
    # avoids a race where `kubectl logs -f` exits when the underlying
    # containerd log stream is disrupted by CRIU checkpoint operations.
    local start_epoch
    start_epoch=$(date +%s)
    local last_diag_epoch=$start_epoch
    while :; do
        if kubectl logs "$pod" -n "$NAMESPACE" -c queue-proxy --tail=50 2>/dev/null \
            | grep -q -F 'fake listener started'; then
            log "  Target container frozen (fake listener active)."
            return 0
        fi

        # Check pod still exists
        if ! kubectl get pod "$pod" -n "$NAMESPACE" --no-headers >/dev/null 2>&1; then
            log "  WARNING: pod $pod no longer exists"
            return 1
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$((now - start_epoch))

        if (( elapsed >= FREEZE_WAIT_TIMEOUT )); then
            log "  WARNING: freezer did not checkpoint within ${FREEZE_WAIT_TIMEOUT}s"
            return 1
        fi

        if (( now - last_diag_epoch >= 30 )); then
            log "  [+${elapsed}s] still waiting for freeze..."
            last_diag_epoch=$now
        fi

        sleep 3
    done
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
        sleep 5
    done
    log "  WARNING: revisions did not stabilize within 300s"
    return 1
}

ensure_restart_policy_kyverno() {
    if kubectl get clusterpolicy bench-restart-policy-never >/dev/null 2>&1; then
        return 0
    fi
    log "  Applying kyverno policy: inject restartPolicy=Never on user-container"
    kubectl apply -f - >/dev/null 2>&1 <<'POLICY'
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
                  serving.knative.dev/service: "*"
      mutate:
        patchStrategicMerge:
          spec:
            template:
              spec:
                containers:
                  - name: user-container
                    restartPolicy: Never
POLICY
}

# ---- disk cleanup ----------------------------------------------------------

BENCH_NODE_NAME=""
resolve_bench_node() {
    BENCH_NODE_NAME=$(kubectl get nodes -l "$BENCH_NODE_SELECTOR_KEY=$BENCH_NODE_SELECTOR_VAL" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    log "Bench node resolved to $BENCH_NODE_NAME"
}

cleanup_node_disk() {
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
                "n=$(ls -d /tmp/ctrd-checkpoint* 2>/dev/null | wc -l); rm -rf /tmp/ctrd-checkpoint*; k3s crictl rmi --prune >/dev/null 2>&1; k3s ctr content prune references >/dev/null 2>&1; echo $n"],
              "securityContext": {"privileged": true}
            }]
          }
        }' >/dev/null 2>&1
    if kubectl wait pod "$pod_name" -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null 2>&1; then
        local count
        count=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
        if [[ "${count:-0}" -gt 0 ]]; then
            log "  Cleaned $count checkpoint dirs on $BENCH_NODE_NAME"
        fi
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# ---- EA patching -----------------------------------------------------------

patch_ea() {
    local freeze="$1" node_key="$2" node_val="$3" target_replica="$4" node_selector_json="$5"

    # Patch freezeEnabled + nodeSelector
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"freezeEnabled":'"$freeze"'}}}' >/dev/null 2>&1

    # Remove + re-add nodeSelector (avoids merge issues)
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"remove\",\"path\":\"/spec/service/nodeSelector\"}]" \
        >/dev/null 2>&1 || true
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"add\",\"path\":\"/spec/service/nodeSelector\",\"value\":{\"$node_key\":\"$node_val\"}}]" \
        >/dev/null 2>&1

    # Patch env vars for handoff target
    local env_patch
    env_patch=$(python3 -c "
import json
envs = [
    {'name':'FORWARD_URL','value':'http://http-sink.default.svc.cluster.local'},
    {'name':'HOP_NAME','value':'retransmitter-handoff'},
    {'name':'HANDOFF_EA_NAME','value':'$EA_NAME'},
    {'name':'HANDOFF_TARGET_REPLICA','value':'$target_replica'},
    {'name':'HANDOFF_NAMESPACE','value':'$NAMESPACE'},
    {'name':'HANDOFF_CLEANUP_ON_DELETE','value':'true'},
    {'name':'HANDOFF_NODE_SELECTOR','value':json.dumps(json.loads('$node_selector_json')) if '$node_selector_json' else ''},
]
print(json.dumps({'spec':{'service':{'container':{'env':envs}}}}))
")
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p "$env_patch" >/dev/null 2>&1

    sleep 5
}

# ---- cleanup ---------------------------------------------------------------

EA_BACKUP=""
TRIGGERS_BACKUP=""
cleanup() {
    log "Cleanup..."
    cleanup_node_disk
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true

    # Delete any leftover handoff CRs
    kubectl delete edgeapplicationhandoff --all -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true

    # Restore original EA
    if [[ -n "$EA_BACKUP" ]]; then
        log "Restoring original EdgeApplication..."
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
}
trap cleanup EXIT

# ---- single iteration (cold start handoff) ---------------------------------

run_handoff_iteration() {
    local scenario="$1" iteration="$2" handoff_target="$3"

    ensure_curl_pod

    # Force-delete any running pods
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true

    # Delete any existing handoff CR from previous iteration
    delete_handoff_cr "$handoff_target"

    if ! wait_for_scale_to_zero; then
        log "  SKIPPED: pods still running"
        return
    fi
    sleep 3

    # Send event — this triggers retransmission + handoff inside the container
    local ts_before ts_after timings
    ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    timings=$(send_event "$SERVICE_URL")
    ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

    IFS=',' read -r _ _ _ t_total http_code <<< "$timings"

    if [[ "$http_code" != "422" ]]; then
        log "  SKIPPED: unexpected HTTP $http_code (expected 422)"
        emit_result "$scenario" "$iteration" "$timings" "$ts_before" "$ts_after" "Skipped" ""
        return
    fi

    log "  Retransmission done in ${t_total}s (HTTP $http_code)"

    # Now poll the handoff CR until it becomes Ready
    local handoff_start handoff_end handoff_time_ms
    handoff_start=$(date +%s%N)

    if wait_for_handoff_ready "$handoff_target"; then
        handoff_end=$(date +%s%N)
        handoff_time_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        local phase
        phase=$(get_handoff_phase "$handoff_target")
        log "  Handoff $phase in ${handoff_time_ms}ms"
        emit_result "$scenario" "$iteration" "$timings" "$ts_before" "$ts_after" "$phase" "$handoff_time_ms"
    else
        handoff_end=$(date +%s%N)
        handoff_time_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        local phase
        phase=$(get_handoff_phase "$handoff_target")
        log "  Handoff $phase after ${handoff_time_ms}ms"
        emit_result "$scenario" "$iteration" "$timings" "$ts_before" "$ts_after" "$phase" "$handoff_time_ms"
    fi

    # Cleanup handoff CR so next iteration starts fresh
    delete_handoff_cr "$handoff_target"
    sleep 2
}

# ---- scenario runners -------------------------------------------------------

run_scenario() {
    local scenario="$1" freeze="$2" node_key="$3" node_val="$4"
    local handoff_target="$5" handoff_node_selector="$6"
    local iterations="$7"

    log "=== SCENARIO: $scenario ($iterations iterations) ==="
    log "  freeze=$freeze, source=$node_key=$node_val, target=$handoff_target"

    patch_ea "$freeze" "$node_key" "$node_val" "$handoff_target" "$handoff_node_selector"

    # Wait for new revision to roll out
    sleep 10

    # Force-delete old pods and do a warmup iteration
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 3

    log "  Warmup iteration..."
    delete_handoff_cr "$handoff_target"
    wait_for_scale_to_zero || true
    send_event "$SERVICE_URL" >/dev/null 2>&1 || true
    sleep 5
    # Wait for handoff to complete or timeout, then clean up
    wait_for_handoff_ready "$handoff_target" || true
    delete_handoff_cr "$handoff_target"
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 5

    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- $scenario iteration $i/$iterations ---"

        if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
            cleanup_node_disk
        fi

        run_handoff_iteration "$scenario" "$i" "$handoff_target"
    done
}

# ---- worker1-freeze scenario -----------------------------------------------
#
# This scenario measures CRIU thaw latency on the handoff TARGET pod.
# Unlike the cold-start scenarios (which create+delete the handoff CR each
# iteration), freeze requires a persistent target pod to checkpoint.
#
# Flow:
#   1) Prime: create handoff CR, wait for target KService + pod ready
#   2) Delete target triggers so broker traffic doesn't wake frozen pods
#   3) Apply kyverno restartPolicy=Never (required for CRIU)
#   4) For each iteration:
#      a) Delete target pod -> new pod comes up (minScale=1)
#      b) Wait for 2/2 Ready with freezer annotation
#      c) Wait for freeze (queue-proxy log: "fake listener started")
#      d) Send request to frozen pod IP:8012 -> measures thaw
#   5) Cleanup: delete handoff CR, kyverno policy

run_freeze_scenario() {
    local iterations="$1"
    local handoff_target="worker1-target"
    local handoff_node_selector='{"vm-id":"worker-1"}'
    local target_svc="${EA_NAME}-${handoff_target}"

    log "=== SCENARIO: worker1-freeze ($iterations iterations) ==="
    log "  Measures CRIU thaw on target pod after handoff is primed"
    log "  Target KService: $target_svc"

    # Step 1: Ensure kyverno policy for restartPolicy=Never BEFORE enabling
    # freeze. The policy must be in place before the operator creates the
    # target deployment, so the kyverno webhook injects restartPolicy=Never
    # on CREATE. Without this, CRIU checkpoint kills the container and
    # kubelet restarts it instead of leaving it dead for CRIU restore.
    ensure_restart_policy_kyverno

    # Step 2: Configure EA with freeze enabled
    patch_ea "true" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
        "$handoff_target" "$handoff_node_selector"

    # Step 3: Create the handoff CR to prime the target replica
    log "  Priming: creating handoff CR..."
    delete_handoff_cr "$handoff_target"
    sleep 2

    # Force scale-to-zero then trigger the retransmitter to create the handoff CR
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    wait_for_scale_to_zero || true
    sleep 3

    send_event "$SERVICE_URL" >/dev/null 2>&1 || true
    sleep 5

    log "  Waiting for handoff CR to become Ready..."
    if ! wait_for_handoff_ready "$handoff_target"; then
        local phase
        phase=$(get_handoff_phase "$handoff_target")
        log "  ERROR: handoff CR did not reach Ready (phase=$phase), aborting freeze scenario"
        return
    fi
    log "  Handoff CR Ready — target KService $target_svc exists"

    # Step 4: Delete triggers to isolate from broker traffic.
    # Do this BEFORE waiting for pod ready, because clearing triggerFilters
    # causes the EA controller to reconcile the KService (creating a new
    # revision that kills the current pod).
    log "  Deleting target triggers to prevent broker from waking frozen pods..."
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"triggerFilters":[]}}}' >/dev/null 2>&1
    kubectl delete trigger -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" --ignore-not-found >/dev/null 2>&1
    sleep 5

    # Step 5: Wait for all revisions to stabilize after trigger deletion
    wait_for_knative_rollout "$target_svc"

    # Step 6: Make sure target pod is up with the final revision
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$target_svc" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 3

    if ! wait_for_target_pod_ready "$target_svc"; then
        log "  ERROR: target pod never became ready, aborting freeze scenario"
        return
    fi

    # Warmup: wait for first freeze, send one thaw request (not measured)
    local warmup_pod warmup_ip
    warmup_pod=$(get_target_pod_name "$target_svc")
    if [[ -n "$warmup_pod" ]]; then
        wait_for_target_freeze "$warmup_pod"
        warmup_ip=$(get_pod_ip "$warmup_pod")
        if [[ -n "$warmup_ip" ]]; then
            log "  Warmup thaw..."
            send_event "http://${warmup_ip}:${QUEUE_PROXY_PORT}" \
                "${target_svc}.${NAMESPACE}.svc.cluster.local" >/dev/null 2>&1 || true
            sleep 3
        fi
    fi

    # Step 7: Run iterations
    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- worker1-freeze iteration $i/$iterations ---"

        if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
            cleanup_node_disk
        fi

        ensure_curl_pod

        # Delete target pod so a fresh one comes up (CRIU can only
        # checkpoint a container once per lifetime)
        kubectl delete pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$target_svc" \
            --grace-period=1 --wait=false >/dev/null 2>&1 || true
        sleep 2

        if ! wait_for_target_pod_ready "$target_svc"; then
            log "  SKIPPED: target pod never became ready"
            continue
        fi

        local pod pod_ip
        pod=$(get_target_pod_name "$target_svc")
        pod_ip=$(get_pod_ip "$pod")
        if [[ -z "$pod" || -z "$pod_ip" ]]; then
            log "  SKIPPED: no pod/IP found"
            continue
        fi

        if ! wait_for_target_freeze "$pod"; then
            log "  SKIPPED: freeze did not happen"
            continue
        fi

        # Send request to frozen pod — this triggers CRIU restore (thaw)
        local ts_before ts_after timings
        ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        timings=$(send_event "http://${pod_ip}:${QUEUE_PROXY_PORT}" \
            "${target_svc}.${NAMESPACE}.svc.cluster.local")
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

        IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
        if [[ "$http_code" == "422" ]]; then
            local t_total_ms
            t_total_ms=$(python3 -c "print(int(float('$t_total') * 1000))")
            log "  Thaw result: ${t_total}s (HTTP $http_code)"
            emit_result "worker1-freeze" "$i" "$timings" "$ts_before" "$ts_after" "Ready" "$t_total_ms"
        else
            log "  SKIPPED: unexpected HTTP $http_code (expected 422)"
            emit_result "worker1-freeze" "$i" "$timings" "$ts_before" "$ts_after" "Skipped" ""
        fi
    done

    # Do NOT delete the handoff CR here — cleanup() handles it
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
    log "  output=$OUTFILE"

    # Backup EA state and triggers
    EA_BACKUP=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null || echo "")
    TRIGGERS_BACKUP=$(kubectl get triggers -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" -o yaml 2>/dev/null || echo "")
    if [[ -z "$EA_BACKUP" ]]; then
        log "ERROR: EdgeApplication $EA_NAME not found. Apply the YAML first:"
        log "  kubectl apply -f edgeApplications/retransmitter-handoff-rbac.yaml"
        log "  kubectl apply -f edgeApplications/retransmitter-handoff.yaml"
        exit 1
    fi

    resolve_bench_node
    setup_curl_pod

    # Run selected scenarios
    case "$SCENARIO" in
        rsu-a|all)
            run_scenario "rsu-a-coldstart" "false" "rsu-id" "rsu-b" \
                "rsu-a" "" "$ITERATIONS"
            ;;&
        worker1-cold|all)
            run_scenario "worker1-coldstart" "false" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
                "worker1-target" '{"vm-id":"worker-1"}' "$ITERATIONS"
            ;;&
        worker1-freeze|all)
            run_freeze_scenario "$ITERATIONS"
            ;;
    esac

    if [[ -f "$OUTFILE" ]]; then
        log "Analyzing results..."
        python3 "${SCRIPT_DIR}/analyze_handoff_bench.py" "$OUTFILE"
    else
        log "No results to analyze (no data written)"
    fi

    log "Done. Results: $OUTFILE"
}

main "$@"
