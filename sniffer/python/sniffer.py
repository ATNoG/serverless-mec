#!/usr/bin/env python3
import os
import sys
import time
import queue
import signal
import logging
import threading
from datetime import datetime, timezone

import pcapy  # pcapy-ng
import requests
from requests.adapters import HTTPAdapter


# -------------------------
# Config
# -------------------------
IFACE = os.getenv("IFACE", "eth0").strip()
BPF = os.getenv("BPF", "").strip()  # optional; if empty we won't setfilter()
SINK_URL = os.getenv("K_SINK", "").strip()

CE_TYPE = os.getenv("CE_TYPE", "its.packet").strip()

HOST_ID = os.getenv("K8S_NODE_NAME") or os.getenv("NODE_NAME") or os.getenv("HOSTNAME") or "host"
CE_SOURCE = os.getenv("CE_SOURCE", f"sniffer://{HOST_ID}/{IFACE}").strip()

PROMISCUOUS = os.getenv("PROMISCUOUS", "0").lower() in ("1", "true", "yes")

SNAPLEN = int(os.getenv("SNAPLEN", "65535"))
PCAP_TIMEOUT_MS = int(os.getenv("PCAP_TIMEOUT_MS", "1"))  # small for low latency

# Pipeline
SEND_QUEUE_MAX = int(os.getenv("SEND_QUEUE_MAX", "20000"))
SENDER_WORKERS = int(os.getenv("SENDER_WORKERS", "4"))

# Sending behavior
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5"))
MEASURE_POST = os.getenv("MEASURE_POST", "1").lower() in ("1", "true", "yes")
LOG_EVERY = int(os.getenv("LOG_EVERY", "0"))  # 0 disables periodic logs
INCLUDE_TIME = os.getenv("INCLUDE_TIME", "0").lower() in ("1", "true", "yes")

# ID generation (faster than uuid): host + run_id + counter
RUN_ID = int(time.time() * 1_000_000)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("fast-sniffer")

# -------------------------
# State
# -------------------------
SEND_QUEUE: "queue.Queue[tuple[int, int, int, bytes]]" = queue.Queue(maxsize=SEND_QUEUE_MAX)
STOP = False


def now_rfc3339() -> str:
    # only used if INCLUDE_TIME=1
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ts_to_rfc3339(sec: int, usec: int) -> str:
    # only used if INCLUDE_TIME=1
    dt = datetime.fromtimestamp(sec + usec / 1_000_000.0, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def make_ce_headers(frame_no: int, dlt: int, sec: int, usec: int) -> dict:
    # CloudEvents binary mode: attributes in headers, raw bytes in body
    # https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md
    ce_id = f"{HOST_ID}-{RUN_ID}-{frame_no}"

    h = {
        "Content-Type": "application/octet-stream",
        "ce-specversion": "1.0",
        "ce-type": CE_TYPE,
        "ce-source": CE_SOURCE,
        "ce-id": ce_id,
        "ce-subject": str(frame_no),
        # extensions:
        "ce-dlt": str(dlt),
        "ce-iface": IFACE,
    }

    if INCLUDE_TIME:
        if sec and usec:
            h["ce-time"] = ts_to_rfc3339(sec, usec)
        else:
            h["ce-time"] = now_rfc3339()

    return h


def sender_worker(worker_id: int):
    # One Session per worker (better than sharing a Session across threads)
    sess = requests.Session()
    sess.trust_env = False

    # Increase pool size for concurrency
    adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    while True:
        try:
            frame_no, sec, usec, pkt = SEND_QUEUE.get(timeout=0.5)
        except queue.Empty:
            if STOP:
                break
            continue

        try:
            # dlt is stable per capture; we stash it in thread-local global later
            dlt = GLOBAL_DLT
            headers = make_ce_headers(frame_no, dlt, sec, usec)

            if MEASURE_POST:
                t0 = time.perf_counter_ns()
                r = sess.post(SINK_URL, headers=headers, data=pkt, timeout=HTTP_TIMEOUT)
                dt = time.perf_counter_ns() - t0
                log.info("sniffer POST elapsed_ns=%d status=%d", dt, r.status_code)
            else:
                r = sess.post(SINK_URL, headers=headers, data=pkt, timeout=HTTP_TIMEOUT)

            r.raise_for_status()
        except Exception as e:
            # keep going; this is a throughput test
            log.warning(f"[WARN] sender[{worker_id}] failed: {e}")
        finally:
            SEND_QUEUE.task_done()


def handle_signal(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Global set once after opening pcap
GLOBAL_DLT = 0


def run():
    global STOP, GLOBAL_DLT

    if not SINK_URL:
        log.error("K_SINK is empty. Set K_SINK to your Knative broker ingress URL.")
        sys.exit(2)

    log.info(f">> FAST capture iface='{IFACE}' promisc={'on' if PROMISCUOUS else 'off'} snaplen={SNAPLEN} timeout_ms={PCAP_TIMEOUT_MS}")
    log.info(f">> BPF='{BPF}'")
    log.info(f">> CloudEvents sink: on -> {SINK_URL}")
    log.info(f">> SEND_QUEUE_MAX={SEND_QUEUE_MAX} SENDER_WORKERS={SENDER_WORKERS} INCLUDE_TIME={int(INCLUDE_TIME)}")

    cap = pcapy.open_live(IFACE, SNAPLEN, 1 if PROMISCUOUS else 0, PCAP_TIMEOUT_MS)
    GLOBAL_DLT = cap.datalink()
    log.info(f">> DLT={GLOBAL_DLT}")

    if BPF:
        cap.setfilter(BPF)

    # Start sender workers
    threads = []
    for i in range(max(1, SENDER_WORKERS)):
        t = threading.Thread(target=sender_worker, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    captured = 0
    enqueued = 0
    dropped = 0
    frame_no = 0

    try:
        while not STOP:
            hdr, data = cap.next()
            if not hdr or not data:
                continue

            captured += 1
            frame_no += 1

            # pcapy sometimes gives str; normalize to bytes
            if isinstance(data, str):
                pkt = data.encode("latin1", errors="ignore")
            else:
                pkt = data

            # timestamp
            try:
                sec, usec = hdr.getts()
            except Exception:
                sec, usec = 0, 0

            # enqueue (drop if full)
            try:
                SEND_QUEUE.put((frame_no, sec, usec, pkt), timeout=0.0)
                enqueued += 1
            except queue.Full:
                dropped += 1

            if LOG_EVERY > 0 and (captured % LOG_EVERY == 0):
                log.info(
                    f"[{datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}] "
                    f"captured={captured} enqueued={enqueued} dropped={dropped} qsize={SEND_QUEUE.qsize()}"
                )
    finally:
        STOP = True
        try:
            cap.close()
        except Exception:
            pass

        # Let workers exit after queue drains a bit (don’t block forever)
        # For pure throughput tests you usually don’t care about draining fully.
        time.sleep(0.5)

        for t in threads:
            try:
                t.join(timeout=1.0)
            except Exception:
                pass


if __name__ == "__main__":
    run()
