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
#    ./bench_handoff.sh [--iterations N] [--scenario all|rsu-a|worker1-cold|worker1-freeze|worker2-cold|worker2-freeze]
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
FREEZER_IDLE_TIMEOUT=5  # seconds — configured via EdgeApplication CRD (freezeIdleTimeout)

QP_IMAGE_BACKUP=""

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
    local deadline=$(( $(date +%s) + HANDOFF_POLL_TIMEOUT ))
    while (( $(date +%s) < deadline )); do
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
    done
    return 2  # timeout
}

get_handoff_phase() {
    local target="$1"
    local cr_name="${HANDOFF_CR_PREFIX}${target}"
    kubectl get edgeapplicationhandoff "$cr_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown"
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
    kubectl get pods -n "$NAMESPACE" \
        -l "serving.knative.dev/revision=$latest_rev" \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
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

wait_for_target_freeze() {
    local pod="$1"
    log "  Waiting for freezer to checkpoint target pod $pod..."

    # Stream the queue-proxy log to a temp file and poll for "fake listener
    # started". This matches the approach in bench_freeze_vs_coldstart.sh
    # and is more reliable than polling --tail=50 (which can miss the line).
    # Use --since=30s to capture recent freeze events (the pod may have
    # already frozen by the time we start watching, e.g. 5s idle timeout).
    local debug_log
    debug_log=$(mktemp -t qproxy_log.XXXXXX)
    kubectl logs -f "$pod" -n "$NAMESPACE" -c queue-proxy --since=30s \
        >"$debug_log" 2>/dev/null &
    local watcher_pid=$!

    local start_epoch
    start_epoch=$(date +%s)
    local last_diag_epoch=$start_epoch
    while :; do
        if grep -q -F 'fake listener started' "$debug_log" 2>/dev/null; then
            kill "$watcher_pid" 2>/dev/null; wait "$watcher_pid" 2>/dev/null
            rm -f "$debug_log"
            log "  Target container frozen (fake listener active)."
            return 0
        fi

        # Check pod still exists
        if ! kubectl get pod "$pod" -n "$NAMESPACE" --no-headers >/dev/null 2>&1; then
            kill "$watcher_pid" 2>/dev/null; wait "$watcher_pid" 2>/dev/null
            rm -f "$debug_log"
            log "  WARNING: pod $pod no longer exists"
            return 1
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$((now - start_epoch))

        if (( elapsed >= FREEZE_WAIT_TIMEOUT )); then
            kill "$watcher_pid" 2>/dev/null; wait "$watcher_pid" 2>/dev/null
            rm -f "$debug_log"
            log "  WARNING: freezer did not checkpoint within ${FREEZE_WAIT_TIMEOUT}s"
            return 1
        fi

        if (( now - last_diag_epoch >= 30 )); then
            log "  [+${elapsed}s] still waiting for freeze..."
            last_diag_epoch=$now
        fi

        sleep 1
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
    kubectl_apply_retry "$_policy_yaml"

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
    if kubectl wait pod "$pod_name" -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s >/dev/null 2>&1; then
        local result
        result=$(kubectl logs "$pod_name" -n "$NAMESPACE" 2>/dev/null | tail -1)
        if [[ -n "$result" && "$result" != "cleaned_checkpoints=0" ]]; then
            log "  Disk cleanup on $node_name: $result"
        fi
    fi
    kubectl delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# ---- EA patching -----------------------------------------------------------

patch_ea() {
    local freeze="$1" node_key="$2" node_val="$3" target_replica="$4" node_selector_json="$5"
    local cleanup_on_delete="${6:-true}"
    local clear_triggers="${7:-false}"

    # Single atomic JSON patch to avoid multiple reconciliation cycles.
    # Multiple sequential patches cause the operator to create a new Knative
    # revision for each change, leading to reconciliation storms.
    local json_ops
    json_ops=$(python3 -c "
import json
envs = [
    {'name':'FORWARD_URL','value':'http://http-sink.default.svc.cluster.local'},
    {'name':'HOP_NAME','value':'retransmitter-handoff'},
    {'name':'HANDOFF_EA_NAME','value':'$EA_NAME'},
    {'name':'HANDOFF_TARGET_REPLICA','value':'$target_replica'},
    {'name':'HANDOFF_NAMESPACE','value':'$NAMESPACE'},
    {'name':'HANDOFF_CLEANUP_ON_DELETE','value':'$cleanup_on_delete'},
    {'name':'HANDOFF_NODE_SELECTOR','value':json.dumps(json.loads('$node_selector_json')) if '$node_selector_json' else ''},
]
ops = [
    {'op': 'replace', 'path': '/spec/service/nodeSelector', 'value': {'$node_key': '$node_val'}},
    {'op': 'replace', 'path': '/spec/service/freezeEnabled', 'value': '$freeze' == 'true'},
    {'op': 'replace', 'path': '/spec/service/container/env', 'value': envs},
]
if '$clear_triggers' == 'true':
    ops.append({'op': 'replace', 'path': '/spec/service/triggerFilters', 'value': []})
print(json.dumps(ops))
")
    kubectl_retry 3 kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "$json_ops" >/dev/null 2>&1

    sleep 5
}

# ---- cleanup ---------------------------------------------------------------

EA_BACKUP=""
TRIGGERS_BACKUP=""
cleanup() {
    log "Cleanup..."
    # Clean all worker nodes that may have accumulated CRIU checkpoint blobs.
    # Different scenarios target different nodes; clean them all.
    cleanup_node_disk "$BENCH_NODE_NAME"
    local w2_node
    w2_node=$(kubectl get nodes -l vm-id=worker-2 \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -n "$w2_node" && "$w2_node" != "$BENCH_NODE_NAME" ]]; then
        cleanup_node_disk "$w2_node"
    fi
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true
    kubectl delete clusterpolicy bench-freeze-idle-timeout --ignore-not-found >/dev/null 2>&1 || true
    restore_queue_proxy_image

    # Delete any leftover handoff CRs
    kubectl delete edgeapplicationhandoff --all -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true

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

    # Wait for target KService cleanup (operator deletes it via cleanupOnDelete)
    local target_svc="${EA_NAME}-${handoff_target}"
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
    sleep 1

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

    if wait_for_handoff_ready "$handoff_target"; then
        local handoff_end handoff_wall_ms phase ts_after
        handoff_end=$(date +%s%N)
        handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        phase=$(get_handoff_phase "$handoff_target")
        log "  Handoff $phase in ${handoff_wall_ms}ms"
        emit_result "$scenario" "$iteration" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
    else
        local handoff_end handoff_wall_ms phase ts_after
        handoff_end=$(date +%s%N)
        handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        phase=$(get_handoff_phase "$handoff_target")
        log "  Handoff $phase after ${handoff_wall_ms}ms (timeout)"
        emit_result "$scenario" "$iteration" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
    fi

    # Cleanup handoff CR so next iteration starts fresh
    delete_handoff_cr "$handoff_target"
    sleep 1
}

# ---- scenario runners -------------------------------------------------------

run_scenario() {
    local scenario="$1" freeze="$2" node_key="$3" node_val="$4"
    local handoff_target="$5" handoff_node_selector="$6"
    local iterations="$7"

    log "=== SCENARIO: $scenario ($iterations iterations) ==="
    log "  freeze=$freeze, source=$node_key=$node_val, target=$handoff_target"

    if ! patch_ea "$freeze" "$node_key" "$node_val" "$handoff_target" "$handoff_node_selector"; then
        log "  ERROR: patch_ea failed, retrying once..."
        sleep 5
        patch_ea "$freeze" "$node_key" "$node_val" "$handoff_target" "$handoff_node_selector" || {
            log "  ERROR: patch_ea failed twice, aborting scenario $scenario"
            return 1
        }
    fi

    # Wait for the new revision to stabilize before proceeding
    wait_for_knative_rollout "$SERVICE_NAME" || log "  WARNING: rollout did not stabilize, proceeding anyway"

    # Force-delete old pods and do a warmup iteration
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 2

    log "  Warmup iteration (direct CR apply)..."
    delete_handoff_cr "$handoff_target"
    local _warmup_cr
    _warmup_cr=$(_build_handoff_cr "$handoff_target" "true" "$handoff_node_selector")
    kubectl_apply_retry "$_warmup_cr" || true
    sleep 2
    wait_for_handoff_ready "$handoff_target" || true
    delete_handoff_cr "$handoff_target"
    # Wait for target KService cleanup before first measured iteration
    local target_svc="${EA_NAME}-${handoff_target}"
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
    sleep 2

    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- $scenario iteration $i/$iterations ---"

        # Pre-check: ensure operator is running before starting iteration
        if ! wait_for_operator; then
            log "  SKIPPED: operator not ready"
            continue
        fi

        if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
            cleanup_node_disk
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
    local target_svc="${EA_NAME}-${handoff_target}"

    # Resolve the target node name for disk cleanup during iterations
    local target_node_name
    target_node_name=$(kubectl get nodes -l "$target_node_key=$target_node_val" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    log "=== SCENARIO: $scenario_name ($iterations iterations) ==="
    log "  Measures full handoff latency with CRIU-frozen target"
    log "  Target KService: $target_svc (node: $target_node_name)"

    # Step 1: Patch EA — freeze=true, clear triggerFilters.
    # triggerFilters must be empty to prevent the broker from dispatching
    # events to the target pod (which would keep it awake and prevent freeze).
    # cleanupOnDelete=false so the target KService survives handoff CR deletion.
    if ! patch_ea "true" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
        "$handoff_target" "$handoff_node_selector" "false" "true"; then
        log "  ERROR: patch_ea failed, retrying once..."
        sleep 5
        patch_ea "true" "$BENCH_NODE_SELECTOR_KEY" "$BENCH_NODE_SELECTOR_VAL" \
            "$handoff_target" "$handoff_node_selector" "false" "true" || {
            log "  ERROR: patch_ea failed twice, aborting scenario $scenario_name"
            return 1
        }
    fi

    kubectl delete trigger -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" --ignore-not-found >/dev/null 2>&1

    wait_for_knative_rollout "$SERVICE_NAME" || log "  WARNING: rollout did not stabilize, proceeding anyway"

    # Step 2: Apply Kyverno policy BEFORE the target KService exists.
    # This ensures the first pod created gets restartPolicy=Never via the
    # admission webhook on Deployment CREATE. Applying it after causes
    # a rollout fight: Kyverno changes the pod template → new ReplicaSet
    # → new pod freezes → 1/2 Error → deployment replaces it → loop.
    ensure_restart_policy_kyverno "$target_svc"

    # Step 3: Clean slate — delete any lingering target state from previous
    # runs. Create a temporary CR with cleanupOnDelete=true to properly
    # remove the replica from the EA spec, then delete it.
    log "  Cleaning lingering target state..."
    delete_handoff_cr "$handoff_target"
    kubectl delete ksvc "$target_svc" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl apply -f - >/dev/null 2>&1 <<CLEANUP || true
apiVersion: mec.atnog.org/v1alpha1
kind: EdgeApplicationHandoff
metadata:
  name: ${HANDOFF_CR_PREFIX}${handoff_target}
  namespace: ${NAMESPACE}
spec:
  edgeApplicationName: ${EA_NAME}
  targetReplicaName: ${handoff_target}
  cleanupOnDelete: true
  nodeSelector:
    ${target_node_key}: ${target_node_val}
CLEANUP
    sleep 1
    delete_handoff_cr "$handoff_target"
    kubectl wait ksvc "$target_svc" -n "$NAMESPACE" --for=delete --timeout=30s >/dev/null 2>&1 || true
    log "  Clean slate established."

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

    if ! wait_for_handoff_ready "$handoff_target"; then
        local phase
        phase=$(get_handoff_phase "$handoff_target")
        log "  ERROR: warmup handoff did not reach Ready (phase=$phase), aborting freeze scenario"
        return
    fi
    log "  Warmup handoff Ready — target KService $target_svc exists"

    delete_handoff_cr "$handoff_target"
    wait_for_knative_rollout "$target_svc" || log "  WARNING: target rollout did not stabilize, proceeding anyway"

    # Wait for a stable target pod and freeze. With restartPolicy=Never and
    # 5s idle timeout, the pod freezes shortly after starting. We poll for
    # a Running pod and then wait for the freeze marker in its logs.
    # Retry if the pod disappears (old pod from rollout being cleaned up).
    local warmup_pod="" freeze_ok=false
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        warmup_pod=$(get_target_pod_name "$target_svc")
        if [[ -z "$warmup_pod" ]]; then
            sleep 2
            continue
        fi
        if wait_for_target_freeze "$warmup_pod"; then
            freeze_ok=true
            break
        fi
        # Pod disappeared — retry with a newer one
        log "  Retrying with next target pod..."
        sleep 2
    done
    if [[ "$freeze_ok" != "true" ]]; then
        log "  ERROR: target did not freeze after warmup within 180s, aborting"
        return
    fi
    log "  Target frozen — starting measured iterations"

    # Step 5: Run measured iterations.
    # Each iteration uses a fresh pod to guarantee clean CRIU state.
    # After CRIU restore, the 3rd checkpoint/restore cycle on the same pod
    # fails (CRIU limitation with accumulated TCP/PID state). Fresh pods
    # ensure every measurement is from a clean first-restore.
    local i
    for (( i=1; i<=iterations; i++ )); do
        log "--- $scenario_name iteration $i/$iterations ---"

        # Pre-check: ensure operator is running before starting iteration
        if ! wait_for_operator; then
            log "  SKIPPED: operator not ready"
            continue
        fi

        if (( i % CHECKPOINT_CLEANUP_INTERVAL == 0 )); then
            cleanup_node_disk "$target_node_name"
        fi

        # Delete handoff CR from previous iteration (KService survives)
        delete_handoff_cr "$handoff_target"

        # Delete the target pod so the KService creates a fresh one.
        # This guarantees clean CRIU state for every measurement.
        local old_pod
        old_pod=$(get_target_pod_name "$target_svc")
        if [[ -n "$old_pod" ]]; then
            log "  Recycling target pod for clean CRIU state..."
            kubectl delete pod "$old_pod" -n "$NAMESPACE" --grace-period=1 --wait=false >/dev/null 2>&1 || true
            kubectl wait pod "$old_pod" -n "$NAMESPACE" --for=delete --timeout=60s >/dev/null 2>&1 || true
        fi

        # Wait for replacement pod to be ready
        if ! wait_for_target_pod_ready "$target_svc"; then
            log "  SKIPPED: replacement target pod not ready"
            continue
        fi

        # Trigger freeze directly via the freezer daemon API
        local target_pod
        target_pod=$(get_target_pod_name "$target_svc")
        if [[ -z "$target_pod" ]]; then
            log "  SKIPPED: no target pod found"
            continue
        fi
        # Wait for freeze via idle timeout (~30s) + CRIU checkpoint (~5s)
        if ! wait_for_target_freeze "$target_pod"; then
            log "  SKIPPED: target did not freeze"
            continue
        fi
        log "  Target confirmed frozen."

        # Create the handoff CR and measure time to Ready.
        # handoff_wall_ms captures the full lifecycle: CR creation →
        # operator detection → CRIU thaw → CR Ready.
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

        if wait_for_handoff_ready "$handoff_target"; then
            local handoff_end handoff_wall_ms phase ts_after
            handoff_end=$(date +%s%N)
            handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
            ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
            phase=$(get_handoff_phase "$handoff_target")
            log "  Handoff $phase in ${handoff_wall_ms}ms"
            emit_result "$scenario_name" "$i" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
        else
            local handoff_end handoff_wall_ms phase ts_after
            handoff_end=$(date +%s%N)
            handoff_wall_ms=$(( (handoff_end - handoff_start) / 1000000 ))
            ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
            phase=$(get_handoff_phase "$handoff_target")
            log "  Handoff $phase after ${handoff_wall_ms}ms"
            emit_result "$scenario_name" "$i" "0,0,0,0,0" "$ts_before" "$ts_after" "$phase" "$handoff_target" "$handoff_wall_ms"
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
        rsu-a|all)
            run_scenario "rsu-a-coldstart" "false" "rsu-id" "rsu-b" \
                "rsu-a" "" "$ITERATIONS" \
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
