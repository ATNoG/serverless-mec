import sys
import os
import json
import uuid
import shutil
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
import pyshark
from cloudevents.http import CloudEvent, to_structured  # official SDK

# -------------------------
# Config via environment
# -------------------------
IFACE = os.getenv("IFACE", "eth0").strip()
ETHER_BPF_DEFAULT = "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001"
BPF = os.getenv("BPF", ETHER_BPF_DEFAULT).strip()
DISPLAY_FILTER = os.getenv("DISPLAY_FILTER", "").strip() or None
LOG_EVERY = int(os.getenv("LOG_EVERY", "10"))
CE_TYPE = os.getenv("CE_TYPE", "its.cam")
INCLUDE_RAW_HEX = os.getenv("INCLUDE_RAW_HEX", "").lower() in ("1", "true", "yes")
SINK_URL = os.getenv("K_SINK", "").strip()
STDOUT_NDJSON = os.getenv("STDOUT_NDJSON", "1").lower() in ("1", "true", "yes")
PROMISCUOUS = os.getenv("PROMISCUOUS", "0").lower() in ("1", "true", "yes")
OUT_PATH = os.getenv("OUT_PATH", "/var/log/cam.ndjson")

# Enable 802.11/radiotap live capture
CAPTURE_80211 = os.getenv("CAPTURE_80211", "0").lower() in ("1", "true", "yes")
# Ask tshark to enable monitor mode on supported wifi interfaces
MONITOR = os.getenv("MONITOR", "0").lower() in ("1", "true", "yes")

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
    # best-effort stringify pyshark field values
    try:
        if hasattr(x, "show"):
            return x.show
        if hasattr(x, "value"):
            return x.value
    except Exception:
        pass
    try:
        return str(x)
    except Exception:
        return repr(x)

def extract_layer_fields(layer) -> dict:
    out = {}
    names = getattr(layer, "field_names", []) or []
    for name in names:
        v = getattr(layer, name, None)
        if v is None:
            continue
        if isinstance(v, list):
            out[name] = [_val_to_str(i) for i in v]
        else:
            out[name] = _val_to_str(v)
    return out

def packet_to_record(pkt) -> Optional[dict]:
    # Expect Wireshark's "its" dissector to be present when 0x8947 is decoded
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

def post_cloudevent_structured(
    sink_url: str,
    event_type: str,
    source: str,
    data: dict,
    timeout: float = 5.0,
) -> None:
    """Send JSON payload as a structured CloudEvent."""
    # Optional extension: stationtype if available in record
    st = None
    try:
        st = data.get("cam_fields", {}).get("stationtype")
    except Exception:
        st = None

    attrs = {
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": str(uuid.uuid4()),
        "time": now_iso(),
        "datacontenttype": "application/json",
    }
    if st is not None:
        attrs["stationtype"] = str(st)

    event = CloudEvent(attrs, data)
    headers, body = to_structured(event)
    resp = SESSION.post(sink_url, headers=headers, data=body, timeout=timeout)
    resp.raise_for_status()

def run_live():
    # Diagnostics
    tshark_path = shutil.which("tshark")
    if not tshark_path:
        log.error("tshark is not installed in the image. Please ensure 'tshark' is present.")

    log.info(
        f">> LIVE capture iface='{IFACE}' "
        f"promisc={'on' if PROMISCUOUS else 'off'} "
        f"802.11={'on' if CAPTURE_80211 else 'off'} "
        f"monitor={'on' if MONITOR else 'off'}"
    )

    bpf_effective = None if CAPTURE_80211 and BPF == ETHER_BPF_DEFAULT else (BPF or None)
    disp_effective = DISPLAY_FILTER
    if CAPTURE_80211 and disp_effective is None:
        disp_effective = "its || udp.port == 2001"

    log.info(f">> BPF='{bpf_effective or '-'}'")
    if disp_effective:
        log.info(f">> DISPLAY_FILTER='{disp_effective}'")
    sink_on = bool(SINK_URL)
    log.info(f">> CloudEvents sink: {'on' if sink_on else 'off'} -> {SINK_URL or '-'}")

    custom_params = []
    if not PROMISCUOUS:
        custom_params.append("-p")
    if MONITOR:
        custom_params.append("-I")

    cap = pyshark.LiveCapture(
        interface=IFACE,
        bpf_filter=bpf_effective,
        display_filter=disp_effective,
        custom_parameters=custom_params,
    )

    processed = 0

    try:
        for pkt in cap.sniff_continuously():
            rec = packet_to_record(pkt)
            if rec is None:
                continue

            line = json.dumps(rec, ensure_ascii=False)

            if STDOUT_NDJSON:
                print(line, flush=True)

            if SINK_URL:
                try:
                    post_cloudevent_structured(SINK_URL, CE_TYPE, CE_SOURCE, rec)
                except Exception as e:
                    log.warning(f"Failed to send CloudEvent: {e}")

            processed += 1
            if LOG_EVERY and processed % LOG_EVERY == 0:
                log.info(f"processed={processed}")

    finally:
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
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
