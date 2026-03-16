#!/usr/bin/env python3
"""
analyze_bench.py — correlate sniffer + retransmitter NDJSON bench logs and print averages + stats.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _open_text(path: str) -> io.TextIOBase:
    if path == "-":
        return io.TextIOWrapper(getattr(__import__("sys"), "stdin").buffer, encoding="utf-8", errors="replace")
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def read_ndjson(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with _open_text(path) as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    rows.append(obj)
            except json.JSONDecodeError:
                continue
    return rows


def ns_to_ms(ns: float) -> float:
    return ns / 1_000_000.0


def _percentile(sorted_vals: List[float], p: float) -> float:
    """Linear interpolation percentile. sorted_vals must be sorted and non-empty."""
    if not sorted_vals:
        return float("nan")
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    n = len(sorted_vals)
    idx = (p / 100.0) * (n - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


@dataclass
class Stats:
    n: int
    mean: float
    median: float
    p95: float
    p99: float
    min: float
    max: float

    @staticmethod
    def from_values(vals: List[float]) -> "Stats":
        if not vals:
            return Stats(0, float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan"))
        s = sorted(vals)
        return Stats(
            n=len(s),
            mean=statistics.fmean(s),
            median=statistics.median(s),
            p95=_percentile(s, 95.0),
            p99=_percentile(s, 99.0),
            min=s[0],
            max=s[-1],
        )


@dataclass
class SignSummary:
    negative: int
    zero: int
    positive: int

    @staticmethod
    def from_values(vals: List[float]) -> "SignSummary":
        neg = sum(1 for v in vals if v < 0)
        zer = sum(1 for v in vals if v == 0)
        pos = sum(1 for v in vals if v > 0)
        return SignSummary(negative=neg, zero=zer, positive=pos)


def fmt_ms(x: float) -> str:
    if math.isnan(x):
        return "nan"
    return f"{x:9.3f} ms"


def fmt_int(x: Any) -> str:
    try:
        return str(int(x))
    except Exception:
        return "null"


def _get_int(d: Dict[str, Any], k: str) -> Optional[int]:
    v = d.get(k, None)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _key_sniffer(d: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    ce_id = d.get("ce_id")
    if isinstance(ce_id, str) and ce_id:
        return ce_id, None
    return None, _get_int(d, "frame_no")


def _key_retrans(d: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    ce_id = d.get("ce_id")
    if isinstance(ce_id, str) and ce_id:
        return ce_id, None
    return None, _get_int(d, "frame_number")


def _filter_rows(rows: List[Dict[str, Any]], component: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("kind") != "bench":
            continue
        if r.get("component") != component:
            continue
        out.append(r)
    return out


def _table(title: str, rows: List[Tuple[str, Stats]]) -> str:
    lines: List[str] = []
    lines.append(title)
    lines.append("-" * len(title))
    header = f"{'metric':38s} {'n':>6s} {'mean':>12s} {'median':>12s} {'p95':>12s} {'p99':>12s} {'min':>12s} {'max':>12s}"
    lines.append(header)
    lines.append("-" * len(header))
    for name, st in rows:
        lines.append(
            f"{name:38s} {st.n:6d} {fmt_ms(st.mean):>12s} {fmt_ms(st.median):>12s} {fmt_ms(st.p95):>12s} {fmt_ms(st.p99):>12s} {fmt_ms(st.min):>12s} {fmt_ms(st.max):>12s}"
        )
    return "\n".join(lines)


def _add_value(vals: List[float], v: Optional[int]) -> None:
    if v is None:
        return
    vals.append(float(v))


def _add_delta(vals: List[float], a: Optional[int], b: Optional[int]) -> None:
    if a is None or b is None:
        return
    vals.append(float(b - a))


def _add_field_or_delta(vals: List[float], explicit_ns: Optional[int], a: Optional[int], b: Optional[int]) -> None:
    if explicit_ns is not None:
        vals.append(float(explicit_ns))
        return
    _add_delta(vals, a, b)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sniffer", required=True, help="sniffer NDJSON (or .gz) or '-' for stdin")
    ap.add_argument("--retrans", required=True, help="retransmitter NDJSON (or .gz)")
    ap.add_argument("--csv", default="", help="optional path to write matched rows as CSV (minimal)")
    args = ap.parse_args()

    sn_all = read_ndjson(args.sniffer)
    rt_all = read_ndjson(args.retrans)

    sn = _filter_rows(sn_all, "sniffer")
    rt = _filter_rows(rt_all, "retransmitter")

    # Index sniffer by ce_id (and frame_no as fallback)
    sn_by_id: Dict[str, Dict[str, Any]] = {}
    sn_by_frame: Dict[int, Dict[str, Any]] = {}
    sn_dupes = 0
    for r in sn:
        ce_id, frame = _key_sniffer(r)
        if ce_id:
            if ce_id in sn_by_id:
                sn_dupes += 1
            else:
                sn_by_id[ce_id] = r
        if frame is not None and frame not in sn_by_frame:
            sn_by_frame[frame] = r

    # Match retrans to sniffer
    matched: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    rt_unmatched = 0
    for r in rt:
        ce_id, frame = _key_retrans(r)
        s = None
        if ce_id and ce_id in sn_by_id:
            s = sn_by_id[ce_id]
        elif frame is not None and frame in sn_by_frame:
            s = sn_by_frame[frame]
        if s is None:
            rt_unmatched += 1
        else:
            matched.append((s, r))

    # Count unmatched sniffers across both id and frame fallback keys.
    matched_keys = set()
    for s, _ in matched:
        ce_id, frame = _key_sniffer(s)
        matched_keys.add((ce_id, frame))
    sn_unmatched = sum(1 for r in sn if _key_sniffer(r) not in matched_keys)

    # ---- Metrics (all in ns; convert to ms at the end) ----

    # Sniffer-only
    sn_cap_to_built: List[float] = []
    sn_built_to_enqueue: List[float] = []
    sn_enqueue_to_send_start: List[float] = []
    sn_send_start_to_end: List[float] = []
    sn_cap_to_send_end: List[float] = []

    for s in sn:
        t_cap = _get_int(s, "t_capture_unix_ns")
        t_bld = _get_int(s, "t_ce_built_unix_ns")
        t_enq = _get_int(s, "t_enqueue_unix_ns")
        t_ss = _get_int(s, "t_send_start_unix_ns")
        t_se = _get_int(s, "t_send_end_unix_ns")
        send_elapsed_ns = _get_int(s, "send_elapsed_ns")

        _add_delta(sn_cap_to_built, t_cap, t_bld)
        _add_delta(sn_built_to_enqueue, t_bld, t_enq)
        _add_delta(sn_enqueue_to_send_start, t_enq, t_ss)
        _add_field_or_delta(sn_send_start_to_end, send_elapsed_ns, t_ss, t_se)
        _add_delta(sn_cap_to_send_end, t_cap, t_se)

    # Matched end-to-end
    e2e_cap_to_rt_recv: List[float] = []
    e2e_send_start_to_rt_recv: List[float] = []
    diag_post_complete_to_rt_recv: List[float] = []

    rt_recv_to_body: List[float] = []
    rt_body_to_parsed: List[float] = []
    rt_parsed_to_fwd_start: List[float] = []
    rt_fwd_start_to_fwd_end: List[float] = []
    rt_recv_to_fwd_end: List[float] = []

    e2e_cap_to_fwd_start: List[float] = []
    e2e_cap_to_fwd_end: List[float] = []

    status_counts: Dict[str, int] = {}
    sizes_in: List[float] = []
    sizes_out: List[float] = []

    for s, r in matched:
        t_cap = _get_int(s, "t_capture_unix_ns")
        t_ss = _get_int(s, "t_send_start_unix_ns")
        t_se = _get_int(s, "t_send_end_unix_ns")

        t_rr = _get_int(r, "t_recv_unix_ns")
        t_rb = _get_int(r, "t_body_unix_ns")
        t_rp = _get_int(r, "t_parsed_unix_ns")
        t_fs = _get_int(r, "t_forward_start_unix_ns")
        t_fe = _get_int(r, "t_forward_end_unix_ns")

        body_read_elapsed_ns = _get_int(r, "body_read_elapsed_ns")
        parse_elapsed_ns = _get_int(r, "parse_elapsed_ns")
        forward_prep_elapsed_ns = _get_int(r, "forward_prep_elapsed_ns")
        forward_elapsed_ns = _get_int(r, "forward_elapsed_ns")
        handler_elapsed_ns = _get_int(r, "handler_elapsed_ns")

        _add_delta(e2e_cap_to_rt_recv, t_cap, t_rr)
        _add_delta(e2e_send_start_to_rt_recv, t_ss, t_rr)
        _add_delta(diag_post_complete_to_rt_recv, t_se, t_rr)

        _add_field_or_delta(rt_recv_to_body, body_read_elapsed_ns, t_rr, t_rb)
        _add_field_or_delta(rt_body_to_parsed, parse_elapsed_ns, t_rb, t_rp)
        _add_field_or_delta(rt_parsed_to_fwd_start, forward_prep_elapsed_ns, t_rp, t_fs)
        _add_field_or_delta(rt_fwd_start_to_fwd_end, forward_elapsed_ns, t_fs, t_fe)
        _add_field_or_delta(rt_recv_to_fwd_end, handler_elapsed_ns, t_rr, t_fe)

        _add_delta(e2e_cap_to_fwd_start, t_cap, t_fs)
        _add_delta(e2e_cap_to_fwd_end, t_cap, t_fe)

        st = r.get("forward_status")
        st_key = "null" if st is None else str(st)
        status_counts[st_key] = status_counts.get(st_key, 0) + 1

        si = _get_int(r, "size_in")
        so = _get_int(r, "size_out")
        if si is not None:
            sizes_in.append(float(si))
        if so is not None:
            sizes_out.append(float(so))

    def stats_ms(vals_ns: List[float]) -> Stats:
        return Stats.from_values([ns_to_ms(v) for v in vals_ns])

    diag_signs = SignSummary.from_values(diag_post_complete_to_rt_recv)

    # ---- Print report ----
    print()
    print("Bench Log Analysis")
    print("==================")
    print(f"sniffer events:        {len(sn)}   (duplicate ce_id ignored: {sn_dupes})")
    print(f"retransmitter events:  {len(rt)}")
    print(f"matched pairs:         {len(matched)}")
    print(f"sniffer unmatched:     {sn_unmatched}")
    print(f"retrans unmatched:     {rt_unmatched}")
    print()

    print(_table("Sniffer timing (per event)", [
        ("capture -> ce_built", stats_ms(sn_cap_to_built)),
        ("ce_built -> enqueue", stats_ms(sn_built_to_enqueue)),
        ("enqueue -> send_start", stats_ms(sn_enqueue_to_send_start)),
        ("send_start -> send_end", stats_ms(sn_send_start_to_end)),
        ("capture -> send_end", stats_ms(sn_cap_to_send_end)),
    ]))
    print()

    print(_table("End-to-end (matched by ce_id/frame)", [
        ("sniffer capture -> retrans recv", stats_ms(e2e_cap_to_rt_recv)),
        ("sniffer send_start -> retrans recv", stats_ms(e2e_send_start_to_rt_recv)),
        ("retrans recv -> body read", stats_ms(rt_recv_to_body)),
        ("retrans body -> parsed", stats_ms(rt_body_to_parsed)),
        ("retrans parsed -> forward_start", stats_ms(rt_parsed_to_fwd_start)),
        ("retrans forward_start -> forward_end", stats_ms(rt_fwd_start_to_fwd_end)),
        ("retrans recv -> forward_end", stats_ms(rt_recv_to_fwd_end)),
        ("sniffer capture -> retrans fwrd_start", stats_ms(e2e_cap_to_fwd_start)),
        ("sniffer capture -> retrans forward_end", stats_ms(e2e_cap_to_fwd_end)),
    ]))
    print()

    print(_table("Topology-dependent diagnostic (do not read as one-way latency)", [
        ("sniffer post_complete -> retrans recv", stats_ms(diag_post_complete_to_rt_recv)),
    ]))
    print(f"signs: negative={diag_signs.negative} zero={diag_signs.zero} positive={diag_signs.positive}")
    if diag_signs.negative > 0:
        print("note: negative values here are expected when the sniffer's POST completes after the retransmitter has already received the request (direct POST semantics).")
    if diag_signs.negative > 0 and diag_signs.positive > 0:
        print("note: mixed signs usually mean this is not a direct point-to-point hop; there is likely an intermediate sink/broker/proxy between producer and retransmitter.")
    print()

    print("Retransmitter forward_status distribution")
    print("----------------------------------------")
    for k in sorted(status_counts.keys(), key=lambda x: (x == "null", x)):
        print(f"{k:>8s}: {status_counts[k]}")
    print()

    if sizes_in or sizes_out:
        def _size_stats(vals: List[float]) -> Stats:
            return Stats.from_values(vals)

        def fmt_num(x: float) -> str:
            if math.isnan(x):
                return "nan"
            return f"{x:9.1f}"

        def size_line(name: str, st: Stats) -> str:
            return (
                f"{name:16s} {st.n:6d} "
                f"{fmt_num(st.mean):>10s} {fmt_num(st.median):>10s} {fmt_num(st.p95):>10s} {fmt_num(st.p99):>10s} "
                f"{fmt_num(st.min):>10s} {fmt_num(st.max):>10s}"
            )

        print("Sizes (bytes)")
        print("------------")
        print(f"{'metric':16s} {'n':>6s} {'mean':>10s} {'median':>10s} {'p95':>10s} {'p99':>10s} {'min':>10s} {'max':>10s}")
        print("-" * 86)
        if sizes_in:
            print(size_line("size_in", _size_stats(sizes_in)))
        if sizes_out:
            print(size_line("size_out", _size_stats(sizes_out)))
        print()

    if args.csv:
        import csv
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ce_id",
                "frame_no",
                "sn_t_capture_ns",
                "sn_t_send_start_ns",
                "sn_t_send_end_ns",
                "rt_t_recv_ns",
                "rt_t_forward_start_ns",
                "rt_t_forward_end_ns",
                "e2e_capture_to_recv_ms",
                "e2e_send_start_to_recv_ms",
                "diag_post_complete_to_recv_ms",
                "e2e_capture_to_forward_start_ms",
                "e2e_capture_to_forward_end_ms",
                "rt_recv_to_fwd_end_ms",
                "forward_status",
            ])
            for s, r in matched:
                ce_id = s.get("ce_id") or r.get("ce_id") or ""
                frame_no = s.get("frame_no") or r.get("frame_number") or ""
                t_cap = _get_int(s, "t_capture_unix_ns")
                t_ss = _get_int(s, "t_send_start_unix_ns")
                t_se = _get_int(s, "t_send_end_unix_ns")
                t_rr = _get_int(r, "t_recv_unix_ns")
                t_fs = _get_int(r, "t_forward_start_unix_ns")
                t_fe = _get_int(r, "t_forward_end_unix_ns")
                handler_elapsed_ns = _get_int(r, "handler_elapsed_ns")

                e2e_ms = ns_to_ms(float(t_rr - t_cap)) if (t_rr is not None and t_cap is not None) else float("nan")
                ss_rr_ms = ns_to_ms(float(t_rr - t_ss)) if (t_rr is not None and t_ss is not None) else float("nan")
                diag_se_rr_ms = ns_to_ms(float(t_rr - t_se)) if (t_rr is not None and t_se is not None) else float("nan")
                cap_fs_ms = ns_to_ms(float(t_fs - t_cap)) if (t_fs is not None and t_cap is not None) else float("nan")
                cap_fe_ms = ns_to_ms(float(t_fe - t_cap)) if (t_fe is not None and t_cap is not None) else float("nan")
                rt_ms = ns_to_ms(float(handler_elapsed_ns)) if handler_elapsed_ns is not None else (
                    ns_to_ms(float(t_fe - t_rr)) if (t_fe is not None and t_rr is not None) else float("nan")
                )

                w.writerow([
                    ce_id,
                    frame_no,
                    fmt_int(t_cap),
                    fmt_int(t_ss),
                    fmt_int(t_se),
                    fmt_int(t_rr),
                    fmt_int(t_fs),
                    fmt_int(t_fe),
                    f"{e2e_ms:.3f}" if not math.isnan(e2e_ms) else "",
                    f"{ss_rr_ms:.3f}" if not math.isnan(ss_rr_ms) else "",
                    f"{diag_se_rr_ms:.3f}" if not math.isnan(diag_se_rr_ms) else "",
                    f"{cap_fs_ms:.3f}" if not math.isnan(cap_fs_ms) else "",
                    f"{cap_fe_ms:.3f}" if not math.isnan(cap_fe_ms) else "",
                    f"{rt_ms:.3f}" if not math.isnan(rt_ms) else "",
                    r.get("forward_status"),
                ])
        print(f"Wrote CSV: {args.csv}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
