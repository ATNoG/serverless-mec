#!/usr/bin/env python3
import os
import time
import uuid
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, Response
from cloudevents.http import from_http, CloudEvent
from cloudevents.conversion import to_structured

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("benchmark-hop")

app = Flask(__name__)

FORWARD_URL = os.getenv("FORWARD_URL", "").strip()
HOP_NAME = os.getenv("HOP_NAME", "bench-hop")
OUT_TYPE = os.getenv("OUT_TYPE", "its.cam.benchmarked")

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

    # 2) Parse CloudEvent
    event_in = from_http(headers_in, body_in)
    t2 = time.perf_counter_ns()

    # Extract original attributes
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

    # 3) Build new benchmarked payload
    data_in = event_in.data

    # ensure dict
    if not isinstance(data_in, dict):
        data_in = {"_raw": str(data_in)}

    # timings so far
    t_recv_to_body = t1 - t0
    t_body_to_parsed = t2 - t1

    bench_partial = {
        "hop": HOP_NAME,
        "recv_iso": recv_iso,
        "original_time": orig_time,
        "timings_ns": {
            "recv_to_body": t_recv_to_body,
            "body_to_parsed": t_body_to_parsed,
        },
        "sizes_bytes": {
            "in": size_in,
        },
        "e2e_ms_from_ce_time": e2e_ms,
        "orig_type": orig_type,
        "orig_source": orig_source,
        "orig_subject": orig_subject,
    }

    # wrap original data inside "original"
    data_out = {
        "original": data_in,
        "bench": bench_partial,
    }

    t3 = time.perf_counter_ns()

    # 4) Create new CloudEvent with extensions
    attrs_out = {
        "specversion": "1.0",
        "type": OUT_TYPE,
        "source": orig_source or f"benchmark://{HOP_NAME}",
        "id": str(uuid.uuid4()),
        "time": now_iso(),
    }
    if orig_subject:
        attrs_out["subject"] = orig_subject

    # extensions for quick inspection
    if e2e_ms is not None:
        attrs_out["e2e_ms"] = f"{e2e_ms:.3f}"
    attrs_out["hop"] = HOP_NAME

    event_out = CloudEvent(attrs_out, data_out)

    # 5) Serialize to structured mode & POST to next sink
    headers_out, body_out = to_structured(event_out)
    size_out = len(body_out)

    # update sizes in bench
    data_out["bench"]["sizes_bytes"]["out"] = size_out

    # re-serialize with updated data
    event_out = CloudEvent(attrs_out, data_out)
    headers_out, body_out = to_structured(event_out)

    t_emit_start = time.perf_counter_ns()
    resp = SESSION.post(FORWARD_URL, headers=headers_out, data=body_out, timeout=5.0)
    t4 = time.perf_counter_ns()

    # timing details
    t_parsed_to_built = t3 - t2
    t_built_to_emitted = t4 - t_emit_start
    total_ns = t4 - t0

    # log everything
    log.info(
        "hop=%s orig_time=%s recv_iso=%s total_ns=%d recv->body_ns=%d body->parsed_ns=%d "
        "parsed->built_ns=%d built->emit_ns=%d e2e_ms=%s status=%d",
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
    )   

    return Response(status=204)
