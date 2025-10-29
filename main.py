#!/usr/bin/env python3
import os
import sys
import json
import uuid
import time
import logging
from datetime import datetime, timezone

import requests
import pyshark
from cloudevents.http import CloudEvent, to_structured  # official SDK

# -------------------------
# Config via environment
# -------------------------
IFACE = os.getenv("IFACE", "eth0")
BPF = os.getenv(
    "BPF",
    "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001",
)
LOG_EVERY = int(os.getenv("LOG_EVERY", "10"))
CE_TYPE = os.getenv("CE_TYPE", "its.cam")
INCLUDE_RAW_HEX = os.getenv("INCLUDE_RAW_HEX", "").lower() in ("1", "true", "yes")
SINK_URL = os.getenv("K_SINK", "").strip()

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
    """
    Convert a pyshark field value to a printable JSON-safe value.
    """
    try:
        # pyshark LayerField often has .show or .showname_value / .value
        if hasattr(x, "show"):
            return x.show
        if hasattr(x, "showname_value"):
            return x.showname_value
        if hasattr(x, "value"):
            return x.value
    except Exception:
        pass
    # Fallback
    return str(x)

def extract_layer_fields(layer) -> dict:
    """
    Extract all fields from a pyshark layer (e.g., 'its') into a dict.
    """
    out = {}
    names = getattr(layer, "field_names", []) or []
    for name in names:
        v = getattr(layer, name, None)
        if v is None:
            continue
        if isinstance(v, list):
            if len(v) == 1:
                out[name] = _val_to_str(v[0])
            else:
                out[name] = [_val_to_str(e) for e in v]
        else:
            out[name] = _val_to_str(v)
    return out

def packet_to_record(pkt) -> dict | None:
    """
    Build the NDJSON record for a packet (only if it has an ITS layer).
    """
    # Require the ITS dissector layer
    try:
        its_layer = pkt["its"]
    except KeyError:
        return None

    cam_fields = extract_layer_fields(its_layer)

    rec = {
        "timestamp": now_iso(),
        "frame_number": getattr(pkt, "number", None) or getattr(pkt, "frame_info", {}).number if hasattr(pkt, "frame_info") else None,
        "cam_layer": "its",
        "cam_fields": cam_fields,
    }

    if INCLUDE_RAW_HEX:
        # Best effort: raw frame hex via packet layers
        try:
            # pyshark can expose frame_raw in some versions; otherwise skip
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
    """
    Send a structured CloudEvent using the official SDK.
    Promotes cam_fields.stationtype into a CE extension (stationtype)
    """
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
    headers, body = to_structured(event)  # sets Content-Type: application/cloudevents+json
    resp = SESSION.post(sink_url, headers=headers, data=body, timeout=timeout)
    resp.raise_for_status()

# -------------------------
# Live capture loop
# -------------------------
def run_live():
    sink_on = bool(SINK_URL)
    log.info(f">> LIVE capture iface='{IFACE}' bpf='{BPF}' sink={'on' if sink_on else 'off'}")

    # tshark params:
    # -p : disable promiscuous mode (avoid needing NET_ADMIN)
    # You can add additional '-o' preferences here if needed.
    custom_params = ["-p"]

    cap = pyshark.LiveCapture(interface=IFACE, bpf_filter=BPF, custom_parameters=custom_params)

    processed = 0
    outfile_path = "/var/log/cam.ndjson"
    # Keep the file open for append; if log shipping is used, you can write to stdout instead.
    out = open(outfile_path, "a", buffering=1)

    try:
        for pkt in cap.sniff_continuously():
            rec = packet_to_record(pkt)
            if rec is None:
                continue

            # Write NDJSON
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")

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
            if processed % LOG_EVERY == 0:
                log.info(f"[{now_iso()}] processed {processed} CAM packets (file=on, sink={'on' if sink_on else 'off'})")

    except KeyboardInterrupt:
        pass
    finally:
        try:
            out.flush()
            out.close()
        except Exception:
            pass
        try:
            cap.close()
        except Exception:
            pass

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    try:
        run_live()
    except Exception as e:
        log.exception(e)
        sys.exit(1)
