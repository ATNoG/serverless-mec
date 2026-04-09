#!/usr/bin/env bash
# bench_freeze_vs_coldstart.sh
#
# Self-contained benchmark of CRIU thaw vs Knative cold-start latency.
#
# This script handles every prerequisite automatically:
#   1. Saves and later restores the retransmitter EdgeApplication so any
#      mutation made by the benchmark (nodeSelector, freezeEnabled,
#      triggerFilters) is rolled back on exit.
#   2. In preflight, SAVES and then DIRECTLY DELETES the retransmitter's
#      Knative Triggers (and clears triggerFilters on the EA so the
#      operator does not try to recreate them). With no triggers, the
#      kafka broker can't dispatch buffered events to the retransmitter,
#      so the pod actually goes idle and the freezer plugin can
#      checkpoint it. The sniffer is left running — only the
#      retransmitter is isolated from the event stream during the
#      benchmark. We delete the triggers directly because the mec
#      operator only *creates* triggers from triggerFilters; it does
#      not reconcile them away when triggerFilters becomes empty, so
#      patching the EA alone is not sufficient. On cleanup the saved
#      trigger YAML is re-applied verbatim, restoring the cluster.
#   3. Spawns an in-cluster curl pod so events can reach both the pod
#      IP (for the CRIU thaw, which bypasses routing) and the ksvc URL
#      (for the cold start).
#   4. Detects freeze by tailing the queue-proxy log and waiting for the
#      freezer plugin's "fake listener started" line — no fixed timeout,
#      so the script tolerates any backlog drain time.
#   5. Runs ITERATIONS CRIU thaw measurements, then ITERATIONS cold
#      start measurements, writing NDJSON rows to OUTFILE.
#   6. Analyzes the results inline (python3 — stdlib only) and prints
#      a comparison report.
#
# Usage:
#   ./bench_freeze_vs_coldstart.sh [ITERATIONS] [LABEL]
#
# Defaults: 25 iterations per mode, no label.
# The output filename ALWAYS contains a timestamp, so re-running with the
# same iteration count or label never overwrites a previous file. The
# optional LABEL is added as a prefix purely for convenience, e.g.
#   ./bench_freeze_vs_coldstart.sh 50 baseline
# produces freeze_vs_coldstart_logs/freeze_vs_coldstart_baseline_<ts>.ndjson

set -uo pipefail

ITERATIONS="${1:-25}"
LABEL="${2:-}"
NAMESPACE="default"
SERVICE_NAME="retransmitter"
EA_NAME="retransmitter"
# cgroup-v2 worker where CRIU checkpoint is supported
BENCH_NODE_SELECTOR_KEY="vm-id"
BENCH_NODE_SELECTOR_VAL="worker-1"
SERVICE_URL="http://retransmitter.default.svc.cluster.local"
QUEUE_PROXY_PORT=8012
# Hard ceiling on how long we wait for the freezer plugin to checkpoint
# the pod. The actual signal we wait for is the "fake listener started"
# log line on the queue-proxy; this is just a safety net.
FREEZE_WAIT_TIMEOUT=300

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_DIR="$SCRIPT_DIR/../knative-freezer-plugin"
QP_PATCHED_BY_US=false

OUTDIR="$SCRIPT_DIR/freeze_vs_coldstart_logs"
mkdir -p "$OUTDIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [[ -n "$LABEL" ]]; then
    LABEL_CLEAN="$(printf '%s' "$LABEL" | tr -c 'A-Za-z0-9._-' '-')"
    OUTFILE="$OUTDIR/freeze_vs_coldstart_${LABEL_CLEAN}_${TIMESTAMP}.ndjson"
else
    OUTFILE="$OUTDIR/freeze_vs_coldstart_${TIMESTAMP}.ndjson"
fi

EA_BACKUP=""
TRIGGERS_BACKUP=""

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# Strip mutable/server-side fields that make `kubectl apply` reject a
# round-tripped object. Reads YAML on stdin, writes cleaned YAML on stdout.
strip_server_fields() {
    python3 -c "
import sys, yaml
docs = list(yaml.safe_load_all(sys.stdin))
def clean(d):
    if not isinstance(d, dict):
        return d
    d.pop('status', None)
    m = d.get('metadata') or {}
    for k in ('resourceVersion','uid','generation','creationTimestamp','managedFields'):
        m.pop(k, None)
    return d
out = []
for d in docs:
    if isinstance(d, dict) and d.get('kind') == 'List':
        items = d.get('items') or []
        for it in items:
            out.append(clean(it))
    elif d is not None:
        out.append(clean(d))
print(yaml.safe_dump_all(out))
"
}

# ---- state save/restore ------------------------------------------------------

save_state() {
    log "Saving current state..."
    EA_BACKUP=$(kubectl get edgeapplication "$EA_NAME" -n "$NAMESPACE" -o yaml 2>/dev/null || echo "")
    if [[ -z "$EA_BACKUP" ]]; then
        log "  ERROR: $EA_NAME EdgeApplication not found."
        exit 1
    fi
    log "  Saved $EA_NAME EdgeApplication."

    # Save the Knative Triggers belonging to this EA. We delete these
    # in preflight (clearing triggerFilters alone is not enough — the
    # operator does not reconcile triggers away) and re-apply them
    # verbatim in restore_state.
    TRIGGERS_BACKUP=$(kubectl get triggers -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" -o yaml 2>/dev/null || echo "")
    local n
    n=$(printf '%s' "$TRIGGERS_BACKUP" | grep -c '^- apiVersion:' || true)
    log "  Saved $n trigger(s) for $EA_NAME."
}

restore_state() {
    log "Restoring original state..."
    if [[ -n "$EA_BACKUP" ]]; then
        echo "$EA_BACKUP" | strip_server_fields 2>/dev/null \
            | kubectl apply -f - >/dev/null 2>&1 \
            || log "  WARNING: failed to restore $EA_NAME"
        log "  Restored $EA_NAME EdgeApplication."
    fi

    # Re-apply the saved triggers. The operator will not recreate them
    # from triggerFilters, so we have to put them back ourselves. The
    # ownerReferences in the saved YAML still point at the EA's UID,
    # which has not changed (we patched, not recreated, the EA).
    if [[ -n "$TRIGGERS_BACKUP" ]] && printf '%s' "$TRIGGERS_BACKUP" | grep -q 'apiVersion:'; then
        echo "$TRIGGERS_BACKUP" | strip_server_fields 2>/dev/null \
            | kubectl apply -f - >/dev/null 2>&1 \
            || log "  WARNING: failed to restore triggers"
        log "  Restored Knative Triggers for $EA_NAME."
    fi
}

cleanup() {
    log "Cleanup..."
    kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl delete clusterpolicy bench-restart-policy-never --ignore-not-found >/dev/null 2>&1 || true
    # Revert queue-proxy BEFORE restoring the EA so that the final revision
    # Knative creates uses the original queue-proxy image + original EA spec
    # (nodeSelector, triggers, etc.) in one shot, avoiding extra churn.
    if [[ "$QP_PATCHED_BY_US" == "true" ]]; then
        log "  Reverting freezer queue-proxy patch..."
        bash "$PLUGIN_DIR/unpatch.sh" >/dev/null 2>&1 || true
    fi
    restore_state
}
trap cleanup EXIT

# ---- curl pod ---------------------------------------------------------------

setup_curl_pod() {
    # Kyverno's admission webhook on this cluster restarts every ~15min with
    # failurePolicy: Fail, creating ~30s windows where resource creation is
    # rejected. Retry up to 5 times with 20s backoff to outlast the window.
    local attempt max_attempts=5
    for (( attempt=1; attempt<=max_attempts; attempt++ )); do
        kubectl delete pod bench-curl -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
        # 6h lifetime is plenty for even 100 iterations of both phases
        if ! kubectl run bench-curl -n "$NAMESPACE" --image=curlimages/curl \
            --restart=Never --command -- sleep 21600 >/dev/null 2>&1; then
            log "  bench-curl create failed (attempt $attempt/$max_attempts), retrying in 20s..."
            sleep 20
            continue
        fi
        log "Waiting for bench-curl pod..."
        if kubectl wait --for=condition=Ready pod/bench-curl -n "$NAMESPACE" --timeout=60s >/dev/null 2>&1; then
            local started
            started=$(kubectl get pod bench-curl -n "$NAMESPACE" -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)
            if [[ -n "$started" ]]; then
                log "bench-curl pod ready."
                return 0
            fi
        fi
        log "  bench-curl did not become ready (attempt $attempt/$max_attempts), retrying in 20s..."
        sleep 20
    done
    log "ERROR: bench-curl pod failed to start after $max_attempts attempts"
    exit 1
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
        --max-time 120 2>/dev/null)
    if [[ -z "$out" || "$out" != *,* ]]; then
        log "  send_event error: ${out:-<empty>}"
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
    # Stream the queue-proxy log from "now" and exit on the first
    # "fake listener started" line, which the freezer plugin emits as
    # soon as CRIU checkpoint succeeds and the fake listener takes over
    # the user-container's port. This is the only signal we need.
    #
    # Alongside the log stream we run a periodic diagnostic loop that
    # reports, every 30s:
    #   - how many Knative Triggers currently point at this EA (should
    #     be 0 — if it is not, the operator is recreating them and the
    #     benchmark is not isolating the pod properly);
    #   - how many bench events the user-container logged in the last
    #     30s (non-zero means the pod is still receiving traffic, which
    #     keeps resetting the freezer plugin's idle timer).
    # This makes a stalled freeze diagnosable without extra tooling.
    local pod="$1"
    log "  Waiting for freezer plugin to checkpoint pod (watching queue-proxy log)..."

    # The watcher writes a single sentinel line to a tempfile when
    # "fake listener started" is observed. We can't rely on subshell
    # exit codes because `kubectl logs -f | grep -m1` trips pipefail
    # (grep exits 0, but kubectl gets SIGPIPE and returns 141), so we
    # use file existence as the success signal instead.
    local sentinel
    sentinel=$(mktemp -t freeze_sentinel.XXXXXX)
    rm -f "$sentinel"

    # Stream the queue-proxy log to a debug file alongside OUTFILE. We then
    # poll that file for "fake listener started" in the wait loop below.
    # Polling the file (instead of piping `kubectl logs -f` through
    # `grep -m1`) avoids a subtle bug: tee/grep in a pipe block-buffers
    # stdout on most systems, which delays the match by minutes in a
    # low-volume log stream and makes freeze look like it never happened.
    # `--since=1s` is critical so we don't replay stale "fake listener
    # started" lines from a previous freeze cycle on the same pod.
    local debug_log="${OUTFILE%.ndjson}.qproxy_${pod}.log"
    : > "$debug_log"
    kubectl logs -f "$pod" -n "$NAMESPACE" -c queue-proxy --since=1s \
        >"$debug_log" 2>/dev/null &
    local watcher_pid=$!
    log "  Queue-proxy log saved to $(basename "$debug_log")"

    local start_epoch
    start_epoch=$(date +%s)
    local last_diag_epoch=$start_epoch
    while :; do
        if grep -q -F 'fake listener started' "$debug_log" 2>/dev/null; then
            kill "$watcher_pid" 2>/dev/null
            wait "$watcher_pid" 2>/dev/null
            rm -f "$sentinel"
            log "  Container frozen (fake listener active)."
            return 0
        fi
        if ! kill -0 "$watcher_pid" 2>/dev/null; then
            # Watcher died without producing the sentinel — pod/log
            # stream went away before the freeze happened.
            wait "$watcher_pid" 2>/dev/null
            rm -f "$sentinel"
            log "  WARNING: log watcher exited before freeze (pod may have died)"
            return 1
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$((now - start_epoch))

        if (( elapsed >= FREEZE_WAIT_TIMEOUT )); then
            log "  WARNING: freezer plugin did not checkpoint within ${FREEZE_WAIT_TIMEOUT}s"
            kill "$watcher_pid" 2>/dev/null
            wait "$watcher_pid" 2>/dev/null
            rm -f "$sentinel"
            return 1
        fi

        if (( now - last_diag_epoch >= 30 )); then
            local trig_count bench_count
            trig_count=$(kubectl get triggers -n "$NAMESPACE" \
                -l "mec.atnog.org/app=$EA_NAME" --no-headers 2>/dev/null | wc -l | tr -d ' ')
            bench_count=$(kubectl logs "$pod" -n "$NAMESPACE" \
                -c user-container --since=30s --tail=-1 2>/dev/null \
                | grep -c '"kind":"bench"')
            [[ -z "$bench_count" ]] && bench_count=0
            log "  [+${elapsed}s] triggers=${trig_count}  bench_events_last_30s=${bench_count}"
            last_diag_epoch=$now
        fi

        sleep 2
    done
}

wait_for_knative_rollout() {
    # Waits until the ksvc has no in-flight revisions: the
    # latestCreatedRevisionName equals latestReadyRevisionName.
    # This is the real "settled" signal — it means every config change
    # has been processed and the resulting revision is ready.
    local deadline=$((SECONDS + 300))
    log "  Waiting for ksvc $SERVICE_NAME revisions to stabilize..."
    while (( SECONDS < deadline )); do
        local created ready
        created=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
        ready=$(kubectl get ksvc "$SERVICE_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
        if [[ -n "$created" && -n "$ready" && "$created" == "$ready" ]]; then
            log "  Revision stable: $ready (latestCreated == latestReady)"
            return 0
        fi
        log "  created=$created ready=$ready — still rolling out..."
        sleep 5
    done
    log "  WARNING: revisions did not stabilize within 300s, proceeding anyway"
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

ensure_restart_policy_kyverno() {
    # Knative's field mask strips the per-container restartPolicy field, so we
    # can't set it via the ksvc or a direct deployment patch (Knative's
    # controller reverts it). A kyverno MutatingAdmissionWebhook re-injects
    # restartPolicy: Never on every Deployment CREATE/UPDATE, which survives
    # Knative reconciliation.
    #
    # The script applies the policy itself so it does not depend on the policy
    # being pre-installed — only on kyverno being present in the cluster.
    if kubectl get clusterpolicy bench-restart-policy-never >/dev/null 2>&1; then
        return 0  # already exists from a previous run or manual apply
    fi
    log "  Applying kyverno policy: inject restartPolicy=Never on user-container"
    kubectl apply -f - >/dev/null 2>&1 <<'POLICY'
apiVersion: kyverno.io/v1
kind: ClusterPolicy
metadata:
  name: bench-restart-policy-never
  labels:
    app.kubernetes.io/managed-by: bench-freeze-vs-coldstart
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

check_operator_healthy() {
    # The mec operator must be running for any of our EA patches to take
    # effect (freezer annotation propagation, ksvc recreation, etc.).
    # Fail fast if it isn't, so we don't waste 5 minutes waiting for a
    # freeze that will never happen.
    local status
    status=$(kubectl get pods -n operator-system \
        -l control-plane=controller-manager \
        -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null)
    if [[ "$status" != "true" ]]; then
        log "  ERROR: mec operator pod is not Ready in operator-system namespace."
        log "         Run: kubectl get pods -n operator-system"
        log "         If it is in CrashLoopBackOff (often: leader-election lost),"
        log "         delete it to force a restart and try again."
        exit 1
    fi
}

preflight() {
    log "Preflight: checking freezer queue-proxy is configured"
    local qp_image
    qp_image=$(kubectl get configmap config-deployment -n knative-serving \
        -o jsonpath='{.data.queue-sidecar-image}' 2>/dev/null)
    if [[ "$qp_image" != *freezer-queue-proxy* ]]; then
        log "  Queue-proxy is not the freezer plugin, patching now..."
        if [[ ! -f "$PLUGIN_DIR/patch.sh" ]]; then
            log "  ERROR: $PLUGIN_DIR/patch.sh not found"
            exit 1
        fi
        bash "$PLUGIN_DIR/patch.sh" >/dev/null 2>&1
        QP_PATCHED_BY_US=true
        log "  Freezer queue-proxy patched (will be reverted on cleanup)."
        log "  Waiting for Knative to roll out the new queue-proxy image..."
        # After patching config-deployment, Knative creates a new revision
        # for every ksvc. We wait for the retransmitter's latest revision
        # deployment to be fully rolled out before proceeding, so the first
        # benchmark iteration doesn't hit a half-ready revision.
        wait_for_knative_rollout
    fi

    log "Preflight: checking mec operator is healthy"
    check_operator_healthy

    log "Preflight: pinning retransmitter to $BENCH_NODE_SELECTOR_KEY=$BENCH_NODE_SELECTOR_VAL (cgroup v2)"
    # Replace nodeSelector entirely; the original is saved in EA_BACKUP.
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"replace\",\"path\":\"/spec/service/nodeSelector\",\"value\":{\"$BENCH_NODE_SELECTOR_KEY\":\"$BENCH_NODE_SELECTOR_VAL\"}}]" \
        >/dev/null 2>&1 || \
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=json \
        -p "[{\"op\":\"add\",\"path\":\"/spec/service/nodeSelector\",\"value\":{\"$BENCH_NODE_SELECTOR_KEY\":\"$BENCH_NODE_SELECTOR_VAL\"}}]" \
        >/dev/null 2>&1

    log "Preflight: deleting Knative Triggers for $EA_NAME so the broker stops dispatching events"
    # This is what makes the benchmark deterministic. With no Knative
    # Triggers, the kafka-backed broker can't deliver buffered ITS
    # events to the retransmitter, so the pod actually goes idle and
    # the freezer plugin can checkpoint it. The sniffer stays running;
    # we only isolate the retransmitter from the event stream.
    #
    # We have to delete the triggers DIRECTLY: the mec operator only
    # creates triggers from triggerFilters, it does not reconcile them
    # away when triggerFilters becomes empty. We also clear the EA's
    # triggerFilters so that any later operator reconcile (e.g. when
    # we patch freezeEnabled) does not see a mismatch and try to add
    # them back. The originals are restored from TRIGGERS_BACKUP on
    # cleanup.
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"triggerFilters":[]}}}' >/dev/null 2>&1
    kubectl delete trigger -n "$NAMESPACE" \
        -l "mec.atnog.org/app=$EA_NAME" --ignore-not-found >/dev/null 2>&1

    # Ensure kyverno policy exists to inject restartPolicy: Never on
    # user-containers. Must be in place BEFORE enabling freeze, so the
    # new deployment created by the operator gets the mutation on CREATE.
    # Knative's field mask strips the per-container restartPolicy field,
    # so only an admission webhook can inject it persistently.
    ensure_restart_policy_kyverno

    log "Preflight: enabling freeze (operator will force minScale>=1 automatically)"
    # The operator auto-injects minScale=1 whenever freezeEnabled=true,
    # because CRIU replaces scale-to-zero as the idle reclamation
    # mechanism. Clear any leftover minScale from a prior aborted run
    # so we exercise that operator path explicitly.
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"freezeEnabled":true,"minScale":null}}}' >/dev/null 2>&1

    # Give the operator a moment to reconcile (delete old triggers,
    # propagate freezer annotation onto a new ksvc revision).
    sleep 10

    # Wait for the operator to propagate the freezer annotation,
    # otherwise the first iterations would target old-revision pods
    # that don't have the freezer plugin loaded.
    wait_for_freezer_annotation

    # Wait for the Knative rollout to fully settle. The preflight changes
    # (nodeSelector, triggerFilters, freezeEnabled, kyverno policy) can
    # cause the operator + Knative to create multiple revisions in
    # sequence. If we start benchmarking before the final revision is
    # stable, the operator may kill the pod mid-freeze by rolling out
    # yet another revision.
    log "Preflight: waiting for revision rollout to stabilize..."
    wait_for_knative_rollout

    # Now that revisions are stable, delete any leftover pod and wait
    # for a fresh one from the final revision to be 2/2 Ready. This
    # frees hostPorts/resources on the target node.
    delete_retransmitter_pod
    if ! wait_for_pod_ready; then
        log "  WARNING: warmup pod did not become ready, proceeding anyway"
    fi
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
        if ! wait_for_pod_ready; then
            log "  SKIPPED: pod never became ready"
            continue
        fi

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
        if [[ "$http_code" == "422" ]]; then
            log "  Result: ${t_total}s (HTTP $http_code)"
            emit_result "criu_thaw" "$i" "$timings" "$ts_before" "$ts_after"
        else
            log "  SKIPPED: unexpected HTTP $http_code (expected 422), not recording"
        fi
    done
}

# ---- cold start benchmark ---------------------------------------------------

run_cold_start_benchmark() {
    log "=== PHASE 2: COLD START ($ITERATIONS iterations) ==="
    log "Patching EdgeApplication: freezeEnabled=false, minScale=0"
    kubectl patch edgeapplication "$EA_NAME" -n "$NAMESPACE" --type=merge \
        -p '{"spec":{"service":{"freezeEnabled":false,"minScale":0}}}' >/dev/null 2>&1
    sleep 10

    # Phase 1 leaves the pod CRIU-frozen. A graceful `kubectl delete` on a
    # frozen pod is very slow: SIGTERM cannot reach the checkpointed user
    # process, and the container has to be resumed before it can exit, so
    # termination can drag past the 180s scale-to-zero wait. Force-delete
    # all retransmitter pods up front so phase 2 starts from a clean slate.
    log "  Force-deleting any surviving retransmitter pods (frozen pods terminate slowly)"
    kubectl delete pods -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$SERVICE_NAME" \
        --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    sleep 3

    # Warmup: do one throwaway cold-start cycle so the new revision's image
    # is pulled and the ksvc routing is settled before real measurements.
    log "  Warming up cold-start revision..."
    wait_for_scale_to_zero || true
    send_event_internal "$SERVICE_URL" >/dev/null 2>&1
    sleep 5

    local i ts_before ts_after timings
    for (( i=1; i<=ITERATIONS; i++ )); do
        log "--- Cold start iteration $i/$ITERATIONS ---"

        # Force-delete any running pod so scale-to-zero isn't gated by
        # whatever pod was brought up by the previous iteration.
        kubectl delete pods -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$SERVICE_NAME" \
            --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
        if ! wait_for_scale_to_zero; then
            log "  SKIPPED: pods still running, not a true cold start"
            continue
        fi
        sleep 3  # endpoint reprogramming

        ts_before=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
        timings=$(send_event_internal "$SERVICE_URL")
        ts_after=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

        IFS=',' read -r _ _ _ t_total http_code <<< "$timings"
        if [[ "$http_code" == "422" ]]; then
            log "  Result: ${t_total}s (HTTP $http_code)"
            emit_result "cold_start" "$i" "$timings" "$ts_before" "$ts_after"
        else
            log "  SKIPPED: unexpected HTTP $http_code (expected 422), not recording"
        fi
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
