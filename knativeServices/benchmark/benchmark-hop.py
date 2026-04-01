#!/usr/bin/env python3
import os
import time
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, Response
from cloudevents.v1.http import from_http

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("benchmark-hop")

app = Flask(__name__)

FORWARD_URL = os.getenv("FORWARD_URL", "").strip()
HOP_NAME = os.getenv("HOP_NAME", "bench-hop")

# Optional: override outgoing content-type (default is raw bytes)
OUT_CONTENT_TYPE = os.getenv("OUT_CONTENT_TYPE", "application/octet-stream").strip()

SESSION = requests.Session()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(ts: str) -> datetime | None:
    try:
        # handles "...Z" as UTC
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def hex_to_bytes(hex_str: str) -> bytes:
    s = (hex_str or "").strip()
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    # bytes.fromhex tolerates whitespace between bytes; it will fail on non-hex chars.
    return bytes.fromhex(s)


@app.post("/")
def handle():
    if not FORWARD_URL:
        log.error("FORWARD_URL is not set")
        return Response("FORWARD_URL not configured", status=500)

    t0 = time.perf_counter_ns()
    recv_iso = now_iso()

    # 1) Read raw HTTP request
    headers_in = dict(request.headers)
    body_in = request.get_data()
    t1 = time.perf_counter_ns()

    size_in = len(body_in)

    # 2) Parse CloudEvent (incoming)
    try:
        event_in = from_http(headers_in, body_in)
    except Exception as e:
        log.exception("Failed to parse CloudEvent: %s", e)
        return Response("Invalid CloudEvent", status=400)
    t2 = time.perf_counter_ns()

    # Extract original attributes (for logging/headers)
    orig_id = event_in.get("id")
    orig_type = event_in.get("type")
    orig_source = event_in.get("source")
    orig_subject = event_in.get("subject")
    orig_time = event_in.get("time")

    # Compute end-to-end latency from CE time, if present
    e2e_ms = None
    if orig_time:
        parsed = parse_iso(orig_time)
        if parsed is not None:
            e2e_ms = (datetime.now(timezone.utc) - parsed).total_seconds() * 1000.0

    # 3) Extract the raw frame hex from CE data and recreate the packet bytes
    data_in = event_in.data
    if not isinstance(data_in, dict):
        return Response("CloudEvent data is not a JSON object", status=422)

    frame_hex = data_in.get("frame_raw_hex")
    if not isinstance(frame_hex, str) or not frame_hex.strip():
        return Response("Missing data.frame_raw_hex", status=422)

    try:
        packet_bytes = hex_to_bytes(frame_hex)
    except Exception as e:
        log.exception("Failed to decode frame_raw_hex: %s", e)
        return Response("Invalid frame_raw_hex", status=422)

    size_out = len(packet_bytes)
    t3 = time.perf_counter_ns()

    # Optional sanity check if frame_len exists
    frame_len = data_in.get("frame_len")
    if isinstance(frame_len, int) and frame_len != size_out:
        log.warning(
            "frame_len mismatch: frame_len=%d decoded_len=%d (still forwarding decoded bytes)",
            frame_len,
            size_out,
        )

    # 4) Forward the recreated packet as raw bytes (NOT a CloudEvent)
    headers_out = {
        "Content-Type": OUT_CONTENT_TYPE or "application/octet-stream",
        # Helpful metadata for downstream (safe to remove if you want a “pure” payload)
        "X-Hop-Name": HOP_NAME,
    }
    if orig_id:
        headers_out["X-Orig-CE-Id"] = str(orig_id)
    if orig_time:
        headers_out["X-Orig-CE-Time"] = str(orig_time)
    if orig_source:
        headers_out["X-Orig-CE-Source"] = str(orig_source)
    if orig_type:
        headers_out["X-Orig-CE-Type"] = str(orig_type)
    if orig_subject:
        headers_out["X-Orig-CE-Subject"] = str(orig_subject)
    if e2e_ms is not None:
        headers_out["X-E2E-Ms-From-CE-Time"] = f"{e2e_ms:.3f}"
    if isinstance(data_in.get("frame_number"), int):
        headers_out["X-Frame-Number"] = str(data_in["frame_number"])
    if isinstance(data_in.get("timestamp"), str):
        headers_out["X-Frame-Timestamp"] = data_in["timestamp"]

    t_emit_start = time.perf_counter_ns()
    resp = SESSION.post(FORWARD_URL, headers=headers_out, data=packet_bytes, timeout=5.0)
    t4 = time.perf_counter_ns()

    # timing details
    t_recv_to_body = t1 - t0
    t_body_to_parsed = t2 - t1
    t_parsed_to_built = t3 - t2
    t_built_to_emitted = t4 - t_emit_start
    total_ns = t4 - t0

    log.info(
        "hop=%s orig_time=%s recv_iso=%s total_ns=%d recv->body_ns=%d body->parsed_ns=%d "
        "parsed->built_ns=%d built->emit_ns=%d e2e_ms=%s status=%d size_in=%d size_out=%d",
        HOP_NAME,
        orig_time,
        recv_iso,
        total_ns,
        t_recv_to_body,
        t_body_to_parsed,
        t_parsed_to_built,
        t_built_to_emitted,
        f"{e2e_ms:.3f}" if e2e_ms is not None else "None",
        resp.status_code,
        size_in,
        size_out,
    )

    # Consider surfacing downstream errors
    if resp.status_code >= 400:
        log.error("Downstream returned status=%d body=%r", resp.status_code, resp.text[:500])

    return Response(status=204)
