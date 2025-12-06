#!/usr/bin/env python3
import os
import time
import uuid
import logging
from datetime import datetime, timezone

from flask import Flask, request, Response
from cloudevents.http import from_http

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("direct-bench")

app = Flask(__name__)

HOP_NAME = os.getenv("HOP_NAME", "direct-bench")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(ts: str):
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        from datetime import datetime as _dt
        from datetime import timezone as _tz
        return _dt.fromisoformat(ts).astimezone(_tz.utc)
    except Exception:
        return None


@app.post("/")
def handle():
    t0 = time.perf_counter_ns()
    recv_iso = now_iso()

    # 1) Read HTTP request
    headers_in = dict(request.headers)
    body_in = request.get_data()
    t1 = time.perf_counter_ns()
    size_in = len(body_in)

    # 2) Parse CloudEvent
    event_in = from_http(headers_in, body_in)
    t2 = time.perf_counter_ns()

    orig_type = event_in.get("type")
    orig_source = event_in.get("source")
    orig_subject = event_in.get("subject")
    orig_time = event_in.get("time")

    # 3) Compute e2e from sniffer CE time
    e2e_ms = None
    if orig_time:
        parsed = parse_iso(orig_time)
        if parsed is not None:
            e2e_ms = (datetime.now(timezone.utc) - parsed).total_seconds() * 1000.0

    t_recv_to_body = t1 - t0
    t_body_to_parsed = t2 - t1
    total_ns = t2 - t0  # since we don't forward anywhere

    log.info(
        "hop=%s orig_time=%s recv_iso=%s total_ns=%d recv->body_ns=%d "
        "body->parsed_ns=%d size_in=%d e2e_ms=%s type=%s source=%s subject=%s",
        HOP_NAME,
        orig_time,
        recv_iso,
        total_ns,
        t_recv_to_body,
        t_body_to_parsed,
        size_in,
        f"{e2e_ms:.3f}" if e2e_ms is not None else "None",
        orig_type,
        orig_source,
        orig_subject,
    )

    return Response(status=204)
