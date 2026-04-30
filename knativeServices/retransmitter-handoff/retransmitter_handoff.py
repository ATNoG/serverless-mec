#!/usr/bin/env python3
import os
import json
import time
import logging
import threading

import requests
from flask import Flask, request, Response
from cloudevents.v1.http import from_http

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("retransmitter-handoff")

app = Flask(__name__)

FORWARD_URL = os.getenv("FORWARD_URL", "").strip()
HOP_NAME = os.getenv("HOP_NAME", "retransmitter-handoff")
OUT_CONTENT_TYPE = os.getenv("OUT_CONTENT_TYPE", "application/octet-stream").strip()

# Handoff configuration (from EdgeApplication env vars)
HANDOFF_EA_NAME = os.getenv("HANDOFF_EA_NAME", "").strip()
HANDOFF_TARGET_REPLICA = os.getenv("HANDOFF_TARGET_REPLICA", "").strip()
HANDOFF_NAMESPACE = os.getenv("HANDOFF_NAMESPACE", "default").strip()
HANDOFF_CLEANUP_ON_DELETE = os.getenv("HANDOFF_CLEANUP_ON_DELETE", "true").strip().lower() == "true"

# Optional: explicit nodeSelector for the handoff target (JSON string)
# e.g. '{"vm-id":"worker-1"}' — if empty, the operator defaults to mec.atnog.org/rsu=<targetReplicaName>
HANDOFF_NODE_SELECTOR = os.getenv("HANDOFF_NODE_SELECTOR", "").strip()

SESSION = requests.Session()

# Track whether a handoff has already been triggered (one per pod lifetime)
_handoff_done = False
_handoff_lock = threading.Lock()


def hex_to_bytes(hex_str: str) -> bytes:
    s = (hex_str or "").strip()
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    return bytes.fromhex(s)


def as_int(v):
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str) and v.strip():
            return int(v.strip())
    except Exception:
        return None
    return None


def get_ce_int(event, *names):
    for name in names:
        v = as_int(event.get(name))
        if v is not None:
            return v
    return None


def emit_bench(record: dict):
    print(json.dumps(record, separators=(",", ":"), ensure_ascii=False), flush=True)


def _create_handoff_cr():
    """Create an EdgeApplicationHandoff CR via the Kubernetes API."""
    from kubernetes import client, config

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    api = client.CustomObjectsApi()

    cr_name = f"{HANDOFF_EA_NAME}-to-{HANDOFF_TARGET_REPLICA}"
    body = {
        "apiVersion": "mec.atnog.org/v1alpha1",
        "kind": "EdgeApplicationHandoff",
        "metadata": {
            "name": cr_name,
            "namespace": HANDOFF_NAMESPACE,
        },
        "spec": {
            "edgeApplicationName": HANDOFF_EA_NAME,
            "targetReplicaName": HANDOFF_TARGET_REPLICA,
            "cleanupOnDelete": HANDOFF_CLEANUP_ON_DELETE,
        },
    }

    if HANDOFF_NODE_SELECTOR:
        try:
            body["spec"]["nodeSelector"] = json.loads(HANDOFF_NODE_SELECTOR)
        except json.JSONDecodeError:
            log.warning("Invalid HANDOFF_NODE_SELECTOR JSON: %s", HANDOFF_NODE_SELECTOR)

    # Delete any existing handoff CR first (idempotent)
    try:
        api.delete_namespaced_custom_object(
            group="mec.atnog.org",
            version="v1alpha1",
            namespace=HANDOFF_NAMESPACE,
            plural="edgeapplicationhandoffs",
            name=cr_name,
        )
        log.info("Deleted existing handoff CR %s", cr_name)
        time.sleep(1)
    except client.ApiException as e:
        if e.status != 404:
            raise

    api.create_namespaced_custom_object(
        group="mec.atnog.org",
        version="v1alpha1",
        namespace=HANDOFF_NAMESPACE,
        plural="edgeapplicationhandoffs",
        body=body,
    )
    return cr_name


@app.post("/")
def handle():
    global _handoff_done

    if not FORWARD_URL:
        return Response("FORWARD_URL not configured", status=500)

    t_recv = time.time_ns()
    t_recv_mono = time.monotonic_ns()

    # 1) Read raw HTTP request
    headers_in = dict(request.headers)
    body_in = request.get_data()
    t_body = time.time_ns()
    t_body_mono = time.monotonic_ns()

    # 2) Parse CloudEvent
    try:
        event_in = from_http(headers_in, body_in)
    except Exception:
        return Response("Invalid CloudEvent", status=400)
    t_parsed = time.time_ns()
    t_parsed_mono = time.monotonic_ns()

    ce_id = event_in.get("id")
    ce_type = event_in.get("type")
    ce_source = event_in.get("source")

    # 3) Extract packet hex from CE data
    data_in = event_in.data
    if not isinstance(data_in, dict):
        return Response("CloudEvent data is not a JSON object", status=422)

    frame_hex = data_in.get("frame_raw_hex")
    if not isinstance(frame_hex, str) or not frame_hex.strip():
        return Response("Missing data.frame_raw_hex", status=422)

    try:
        packet_bytes = hex_to_bytes(frame_hex)
    except Exception:
        return Response("Invalid frame_raw_hex", status=422)

    size_in = len(body_in)
    size_out = len(packet_bytes)

    # 4) Forward raw bytes (retransmission)
    headers_out = {
        "Content-Type": OUT_CONTENT_TYPE or "application/octet-stream",
        "X-Hop-Name": HOP_NAME,
    }
    if ce_id:
        headers_out["X-Orig-CE-Id"] = str(ce_id)

    t_forward_start = time.time_ns()
    t_forward_start_mono = time.monotonic_ns()
    forward_error = None
    try:
        resp = SESSION.post(FORWARD_URL, headers=headers_out, data=packet_bytes, timeout=5.0)
        forward_status = resp.status_code
    except Exception as exc:
        forward_status = -1
        forward_error = str(exc)
    t_forward_end = time.time_ns()
    t_forward_end_mono = time.monotonic_ns()

    # 5) Trigger handoff (once per pod lifetime, if configured)
    #    Fire-and-forget: create the CR and return immediately.
    #    The bench script polls the CR externally for readiness.
    t_handoff_cr_created = None
    t_handoff_cr_created_mono = None
    handoff_cr_name = None
    handoff_error = None

    if HANDOFF_EA_NAME and HANDOFF_TARGET_REPLICA:
        with _handoff_lock:
            should_handoff = not _handoff_done
            _handoff_done = True

        if should_handoff:
            try:
                handoff_cr_name = _create_handoff_cr()
                t_handoff_cr_created = time.time_ns()
                t_handoff_cr_created_mono = time.monotonic_ns()
                log.info("Created handoff CR: %s", handoff_cr_name)
            except Exception as exc:
                log.error("Handoff CR creation failed: %s", exc)
                handoff_error = str(exc)

    record = {
        "kind": "bench",
        "component": "retransmitter-handoff",
        "hop": HOP_NAME,
        "ce_id": str(ce_id) if ce_id is not None else None,
        "ce_type": str(ce_type) if ce_type is not None else None,
        "ce_source": str(ce_source) if ce_source is not None else None,

        # Retransmission timestamps
        "t_recv_unix_ns": t_recv,
        "t_body_unix_ns": t_body,
        "t_parsed_unix_ns": t_parsed,
        "t_forward_start_unix_ns": t_forward_start,
        "t_forward_end_unix_ns": t_forward_end,
        "t_recv_mono_ns": t_recv_mono,
        "t_body_mono_ns": t_body_mono,
        "t_parsed_mono_ns": t_parsed_mono,
        "t_forward_start_mono_ns": t_forward_start_mono,
        "t_forward_end_mono_ns": t_forward_end_mono,

        "forward_status": forward_status,
        "forward_error": forward_error,
        "size_in": size_in,
        "size_out": size_out,

        # Handoff CR creation timestamp
        "t_handoff_cr_created_unix_ns": t_handoff_cr_created,
        "t_handoff_cr_created_mono_ns": t_handoff_cr_created_mono,
        "handoff_cr_name": handoff_cr_name,
        "handoff_error": handoff_error,
    }
    emit_bench(record)

    return Response(status=422)
