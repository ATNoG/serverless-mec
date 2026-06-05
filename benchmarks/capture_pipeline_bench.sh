#!/usr/bin/env bash
# capture_pipeline_bench.sh
#
# Captures sniffer + retransmitter bench events into ONE combined NDJSON file.
#
# Each row keeps its original payload and gains:
#   run_id          — timestamp of this capture session
#   _source         — "sniffer" | "retransmitter"
#   bench_pair_key  — "<run_id>::<ce_id>" (or fallback "<run_id>::frame::<n>")
#
# The pair key namespaces ce_id by run_id, so multiple sessions can be appended
# to the same file (or analyzed together) without ce_id collisions when the
# sniffer's counter resets.
#
# Usage:
#   ./capture_pipeline_bench.sh                       # unlimited, run_id=<timestamp>
#   ./capture_pipeline_bench.sh -n 1000               # stop after 1000 packets per stream
#   ./capture_pipeline_bench.sh -n 500 baseline       # label "baseline" + timestamp
#   ./capture_pipeline_bench.sh -a -n 1000 baseline   # append to shared file
#
# The output filename ALWAYS contains a timestamp, so re-running with the
# same label never overwrites a previous file. The optional positional label
# is added as a prefix purely for convenience.
#
# Options:
#   -n N    stop after N bench rows have been captured on EACH stream
#           (so the analyzer has up to N pairs). Default 0 = unlimited.
#   -a      append to pipeline_logs/pipeline_combined.ndjson instead of a per-run file
#
# Press Ctrl+C at any time to stop early. The analyzer is invoked automatically.

set -uo pipefail
set -m  # job control: each background pipeline gets its own process group

NAMESPACE="default"
SNIFFER_LABEL="serving.knative.dev/service=its-sniffer"
RETRANS_LABEL="serving.knative.dev/service=retransmitter"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="$SCRIPT_DIR/pipeline_logs"
mkdir -p "$OUTDIR"

COUNT=0
MODE="new"
while getopts "n:ah" opt; do
    case "$opt" in
        n) COUNT="$OPTARG" ;;
        a) MODE="append" ;;
        h|*)
            sed -n '2,25p' "$0"
            exit 0
            ;;
    esac
done
shift $((OPTIND - 1))

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LABEL="${1:-}"
if [[ -n "$LABEL" ]]; then
    # Sanitize: replace anything that isn't alnum/dot/dash/underscore with '-'
    LABEL_CLEAN="$(printf '%s' "$LABEL" | tr -c 'A-Za-z0-9._-' '-')"
    RUN_ID="${LABEL_CLEAN}_${TIMESTAMP}"
else
    RUN_ID="$TIMESTAMP"
fi

if [[ "$MODE" == "append" ]]; then
    COMBINED="$OUTDIR/pipeline_combined.ndjson"
else
    COMBINED="$OUTDIR/pipeline_${RUN_ID}.ndjson"
fi

log() { echo "[$(date +%H:%M:%S)] $*"; }
log "run_id=$RUN_ID"
log "output : $COMBINED"
if [[ "$COUNT" -gt 0 ]]; then
    log "limit  : $COUNT bench rows per stream (Ctrl+C to stop early)"
else
    log "limit  : unlimited (press Ctrl+C to stop and analyze)"
fi
echo

TAGGER="$(mktemp -t pipeline_tagger.XXXXXX.py)"
trap 'rm -f "$TAGGER"' EXIT

cat > "$TAGGER" <<'PY'
import sys, json, time, datetime, os
run_id, src, out, limit_s = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

def log_stderr(msg):
    """Atomic write to stderr so concurrent processes don't interleave lines."""
    os.write(2, (msg + "\n").encode())
limit = int(limit_s)

# Print a progress line every PROGRESS_EVERY rows AND at least every
# PROGRESS_INTERVAL_S seconds, so the user knows the pipeline is alive even
# if events are sparse.
PROGRESS_EVERY = 10
PROGRESS_INTERVAL_S = 3.0

# Right-align "sniffer" / "retransmitter" in a fixed-width column so the two
# progress streams align nicely on screen.
tag = f"{src:>13}"

n = 0
last_emit = time.monotonic()

def emit(force=False):
    global last_emit
    now = time.monotonic()
    if not force and (now - last_emit) < PROGRESS_INTERVAL_S and n % PROGRESS_EVERY != 0:
        return
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    if limit > 0:
        msg = f"[{ts}] {tag}: {n:>6d} / {limit} rows"
    else:
        msg = f"[{ts}] {tag}: {n:>6d} rows"
    log_stderr(msg)
    last_emit = now

with open(out, "a", buffering=1) as f:
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        try:
            d = json.loads(s)
        except Exception:
            continue
        if d.get("kind") != "bench":
            continue
        if d.get("component") != src:
            continue
        ce = d.get("ce_id")
        if isinstance(ce, str) and ce:
            d["bench_pair_key"] = f"{run_id}::{ce}"
        else:
            fr = d.get("frame_no", d.get("frame_number"))
            if fr is not None:
                d["bench_pair_key"] = f"{run_id}::frame::{fr}"
        d["run_id"] = run_id
        d["_source"] = src
        f.write(json.dumps(d, separators=(",", ":")) + "\n")
        n += 1
        emit()
        if limit > 0 and n >= limit:
            emit(force=True)
            log_stderr(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {tag}: limit reached, exiting")
            break

# Final count if loop ended without hitting the limit (e.g. SIGPIPE).
emit(force=True)
PY

kubectl logs -f --tail=0 -n "$NAMESPACE" -l "$SNIFFER_LABEL" -c user-container 2>/dev/null \
    | python3 -u "$TAGGER" "$RUN_ID" sniffer "$COMBINED" "$COUNT" &
JOB_SNIFF=$!

kubectl logs -f --tail=0 -n "$NAMESPACE" -l "$RETRANS_LABEL" -c user-container 2>/dev/null \
    | python3 -u "$TAGGER" "$RUN_ID" retransmitter "$COMBINED" "$COUNT" &
JOB_RETRANS=$!

stopped=0
cleanup() {
    [[ $stopped -eq 1 ]] && return
    stopped=1
    echo
    log "stopping streams..."
    kill -- "-$JOB_SNIFF" "-$JOB_RETRANS" 2>/dev/null
    wait 2>/dev/null
    n=$(wc -l < "$COMBINED" 2>/dev/null || echo 0)
    log "captured $n bench rows total (this file)"
    echo
    if [[ -s "$COMBINED" ]]; then
        python3 "$SCRIPT_DIR/analyze_pipeline_bench.py" --run-id "$RUN_ID" --iqr "$COMBINED"
    else
        log "no rows captured — skipping analysis"
    fi
}
trap cleanup INT TERM

# Wait for both taggers to finish naturally (limit reached) OR for Ctrl+C.
wait "$JOB_SNIFF" 2>/dev/null
wait "$JOB_RETRANS" 2>/dev/null
cleanup
