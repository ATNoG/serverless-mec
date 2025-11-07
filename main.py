#!/usr/bin/env python3
import os
import sys
import json
import uuid
import shutil
import logging
import subprocess
from datetime import datetime, timezone

import requests
import pyshark

from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured

# -------------------------
# Config via environment
# -------------------------
IFACE = os.getenv("IFACE", "eth0").strip()
BPF = os.getenv(
    "BPF",
    "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001",
).strip()
DISPLAY_FILTER = os.getenv("DISPLAY_FILTER", "").strip() or None
LOG_EVERY = int(os.getenv("LOG_EVERY", "10"))
CE_TYPE = os.getenv("CE_TYPE", "its.cam")
INCLUDE_RAW_HEX = os.getenv("INCLUDE_RAW_HEX", "").lower() in ("1", "true", "yes")
SINK_URL = SINK_URL = os.getenv("K_SINK", "").strip() or resolve_kafka_broker()
STDOUT_NDJSON = os.getenv("STDOUT_NDJSON", "1") in ("1", "true", "yes")
PROMISCUOUS = os.getenv("PROMISCUOUS", "0") in ("1", "true", "yes")

# Generate a reasonable CloudEvent source string
HOST_ID = os.getenv("K8S_NODE_NAME") or os.getenv("NODE_NAME") or os.getenv("HOSTNAME") or "host"
CE_SOURCE = f"sniffer://{HOST_ID}/{IFACE}"

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(message)s",
)
log = logging.getLogger("live-capture")

# -------------------------
# Helpers
# -------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _val_to_str(x):
    try:
        if hasattr(x, "show"):
            return x.show
        if hasattr(x, "showname_value"):
            return x.showname_value
        if hasattr(x, "value"):
            return x.value
    except Exception:
        pass
    return str(x)

def extract_layer_fields(layer) -> dict:
    out = {}
    names = getattr(layer, "field_names", []) or []
    for name in names:
        v = getattr(layer, name, None)
        if v is None:
            continue
        if isinstance(v, list):
            out[name] = _val_to_str(v[0]) if len(v) == 1 else [_val_to_str(e) for e in v]
        else:
            out[name] = _val_to_str(v)
    return out

def packet_to_record(pkt) -> dict | None:
    try:
        its_layer = pkt["its"]
    except KeyError:
        return None

    rec = {
        "timestamp": now_iso(),
        "frame_number": getattr(pkt, "number", None),
        "cam_layer": "its",
        "cam_fields": extract_layer_fields(its_layer),
    }

    if INCLUDE_RAW_HEX:
        try:
            raw = getattr(pkt.frame_raw, "value", None)
            if raw:
                rec["frame_raw_hex"] = raw
        except Exception:
            pass

    return rec

# -------------------------
# CloudEvents (official SDK)
# -------------------------
SESSION = requests.Session()

def post_cloudevent_structured(sink_url: str, event_type: str, source: str, data: dict,
                               subject: str | None = None, event_id: str | None = None,
                               event_time: str | None = None, timeout: float = 5.0):
    st = None
    try:
        st = data.get("cam_fields", {}).get("stationtype")
    except Exception:
        pass

    attrs = {
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": event_id or str(uuid.uuid4()),
        "time": event_time or now_iso(),
        "datacontenttype": "application/json",
    }
    if subject:
        attrs["subject"] = subject
        
    # Add as CloudEvent extension (must be lowercase key)
    if st is not None:
        attrs["stationtype"] = str(st)

    event = CloudEvent(attrs, data)
    headers, body = to_structured(event)
    resp = SESSION.post(sink_url, headers=headers, data=body, timeout=timeout)
    resp.raise_for_status()

# -------------------------
# Live capture loop
# -------------------------
def run_live():
    # Diagnostics
    tshark_path = shutil.which("tshark")
    if not tshark_path:
        log.error("tshark is not installed in the image. Please ensure 'tshark' is present.")
    log.info(f">> LIVE capture iface='{IFACE}' promisc={'on' if PROMISCUOUS else 'off'}")
    log.info(f">> BPF='{BPF}'")
    if DISPLAY_FILTER:
        log.info(f">> DISPLAY_FILTER='{DISPLAY_FILTER}'")
    sink_on = bool(SINK_URL)
    log.info(f">> CloudEvents sink: {'on' if sink_on else 'off'} -> {SINK_URL or '-'}")

    # Build custom parameters; toggle promiscuous with env
    # tshark params:
    # -p : disable promiscuous mode (avoid needing NET_ADMIN)
    custom_params = []
    if not PROMISCUOUS:
        custom_params.append("-p")

    cap = pyshark.LiveCapture(
        interface=IFACE,
        bpf_filter=BPF,
        display_filter=DISPLAY_FILTER,
        custom_parameters=custom_params,
    )

    processed = 0

    try:
        for pkt in cap.sniff_continuously():
            rec = packet_to_record(pkt)
            if rec is None:
                continue

            # Write NDJSON
            line = json.dumps(rec, ensure_ascii=False)
            if STDOUT_NDJSON:
                print(line, flush=True)

            # Post CloudEvent if configured
            if sink_on:
                try:
                    subject = str(rec.get("frame_number")) if rec.get("frame_number") is not None else None
                    post_cloudevent_structured(
                        sink_url=SINK_URL,
                        event_type=CE_TYPE,
                        source=CE_SOURCE,
                        data=rec,
                        subject=subject,
                    )
                except Exception as e:
                    log.warning(f"[WARN] CloudEvent POST failed: {e}")

            processed += 1
            if LOG_EVERY > 0 and processed % LOG_EVERY == 0:
                log.info(f"[{now_iso()}] processed {processed} CAM packets (file=on, sink={'on' if sink_on else 'off'})")
    finally:
        try:
            cap.close()
        except Exception:
            pass

# -------------------------
# Get the Kafka Broker for K_SINK
# -------------------------
def resolve_kafka_broker():
    """Try to resolve the broker service; fallback to backend pod IP."""
    try:
        import socket
        socket.gethostbyname("kafka-broker-ingress.knative-eventing.svc.cluster.local")
        return "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    except Exception:
        try:
            ip = subprocess.check_output([
                "kubectl", "-n", "knative-eventing", "get", "endpoints",
                "kafka-broker-ingress", "-o", "jsonpath={.subsets[*].addresses[*].ip}"
            ]).decode().strip()
            if ip:
                return f"http://{ip}:8080/default/default"
        except Exception:
            pass
    return None

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    try:
        run_live()
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
