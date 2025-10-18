#!/usr/bin/env python3
import os, json, logging, socket
from typing import Any, Dict

from flask import Flask, request, jsonify
from cloudevents.http import from_http
import paho.mqtt.client as mqtt

# --------- Config (env) ----------
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto.default.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))          # 0,1,2
MQTT_RETAIN = os.getenv("MQTT_RETAIN", "false").lower() in ("1","true","yes")
# Topic template may use: {type}, {source}, {stationtype}
MQTT_TOPIC_TEMPLATE = os.getenv("MQTT_TOPIC_TEMPLATE", "its/{type}/st{stationtype}")

PUBLISH_MODE = os.getenv("PUBLISH_MODE", "data").lower()  # "data" or "event"
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"mqtt-forwarder-{socket.gethostname()}")

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO").upper()

# --------- Logging ----------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mqtt-forwarder")

# --------- MQTT client ----------
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
client.loop_start()

# --------- App ----------
app = Flask(__name__)

def _coerce_json(obj: Any) -> str:
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, ensure_ascii=False)
    # try to parse if it looks like json already
    if isinstance(obj, (bytes, bytearray)):
        try:
            return json.dumps(json.loads(obj.decode("utf-8")), ensure_ascii=False)
        except Exception:
            return obj.decode("utf-8", errors="replace")
    if isinstance(obj, str):
        try:
            return json.dumps(json.loads(obj), ensure_ascii=False)
        except Exception:
            return obj
    return json.dumps(obj, ensure_ascii=False, default=str)

def _sanitize_source(src: str) -> str:
    if not src:
        return "unknown"
    return src.replace("://","/").replace("//","/").strip("/").replace("/",".")

def _extract_stationtype(evt_attrs: Dict[str, Any], evt_data: Any) -> str:
    # extension attr first (your sniffer sets this)
    st = evt_attrs.get("stationtype")
    if st:
        return str(st)
    # fallback to payload cam_fields.stationtype
    if isinstance(evt_data, dict):
        st = (evt_data.get("cam_fields") or {}).get("stationtype")
        if st:
            return str(st)
    return "unknown"

def _render_topic(evt_attrs: Dict[str, Any], evt_data: Any) -> str:
    variables = {
        "type": (evt_attrs.get("type") or "unknown").replace("/", "."),
        "source": _sanitize_source(evt_attrs.get("source") or "unknown"),
        "stationtype": _extract_stationtype(evt_attrs, evt_data),
    }
    try:
        return MQTT_TOPIC_TEMPLATE.format(**variables)
    except Exception:
        log.warning("Topic template failed; using fallback 'events/%s'", variables["type"])
        return f"events/{variables['type']}"

@app.route("/", methods=["POST"])
def receive():
    try:
        event = from_http(request.headers, request.get_data())  # works for structured or binary
        attrs = {k: event.get(k) for k in ("id","type","source","subject","time","datacontenttype","specversion")}
        # include extensions (like stationtype) if present:
        for k in ("stationtype",):
            v = event.get(k)
            if v is not None:
                attrs[k] = v

        topic = _render_topic(attrs, event.data)

        if PUBLISH_MODE == "event":
            # publish the full event (attributes + data)
            payload = _coerce_json({"attributes": attrs, "data": event.data})
        else:
            # publish only the data section
            payload = _coerce_json(event.data)

        info = client.publish(topic, payload=payload, qos=MQTT_QOS, retain=MQTT_RETAIN)
        # Fail fast if paho returns an error code
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("MQTT publish failed rc=%s topic=%s", info.rc, topic)
            return ("publish failed", 500)

        # Only wait for confirmation with QoS >= 1
        if MQTT_QOS > 0:
            ok = info.wait_for_publish(timeout=5)  # returns True on success, False on timeout
            if not ok:
                log.error("MQTT publish not confirmed (timeout) topic=%s", topic)
                return ("publish timeout", 500)

        log.info("Published MQTT topic=%s len=%d qos=%d", topic, len(payload.encode("utf-8")), MQTT_QOS)
        return ("", 202)
    except Exception as e:
        log.exception("Failed to process incoming CloudEvent")
        return (str(e), 400)

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True})
