#!/usr/bin/env python3
"""
analyze_pipeline_bench.py

Correlates sniffer + retransmitter NDJSON bench logs and computes all timing
deltas from raw timestamps only.

Accepts either:
  - A combined NDJSON file (default, positional args) where sniffer and
    retransmitter rows are interleaved, as produced by capture_pipeline_bench.sh
  - Separate sniffer/retransmitter files via --sniffer and --retrans flags

Important:
- No elapsed/delta values are expected from the sniffer or retransmitter.
- This script derives all durations from raw timestamp fields.
- For same-process phase timings, monotonic timestamps are preferred when
  available because they are better suited for measuring local elapsed time.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
import statistics
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _open_text(path: str) -> io.TextIOBase:
    if path == "-":
        return io.TextIOWrapper(
            sys.stdin.buffer,
            encoding="utf-8",
            errors="replace",
        )
    if path.endswith(".gz"):
        return io.TextIOWrapper(
            gzip.open(path, "rb"),
            encoding="utf-8",
            errors="replace",
        )
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


def iqr_filter(vals: List[float]) -> List[float]:
    """Remove outliers using Tukey's fences: keep values in [Q1-1.5*IQR, Q3+1.5*IQR]."""
    if len(vals) < 4:
        return vals
    s = sorted(vals)
    n = len(s)
    q1 = s[n // 4]
    q3 = s[(3 * n) // 4]
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return [v for v in vals if lo <= v <= hi]


@dataclass
class Stats:
    n: int
    mean: float
    stddev: float
    median: float
    p95: float
    p99: float
    min: float
    max: float

    @staticmethod
    def from_values(vals: List[float]) -> "Stats":
        if not vals:
            return Stats(0, *([float("nan")] * 7))
        s = sorted(vals)
        sd = statistics.stdev(s) if len(s) >= 2 else 0.0
        return Stats(
            n=len(s),
            mean=statistics.fmean(s),
            stddev=sd,
            median=statistics.median(s),
            p95=_percentile(s, 95.0),
            p99=_percentile(s, 99.0),
            min=s[0],
            max=s[-1],
        )


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


def _delta_ns(start: Optional[int], end: Optional[int]) -> Optional[float]:
    if start is None or end is None:
        return None
    return float(end - start)


def _preferred_delta_ns(
    mono_start: Optional[int],
    mono_end: Optional[int],
    unix_start: Optional[int],
    unix_end: Optional[int],
) -> Optional[float]:
    d = _delta_ns(mono_start, mono_end)
    if d is not None:
        return d
    return _delta_ns(unix_start, unix_end)


def _add_delta(vals: List[float], start: Optional[int], end: Optional[int]) -> None:
    d = _delta_ns(start, end)
    if d is not None:
        vals.append(d)


def _add_preferred_delta(
    vals: List[float],
    mono_start: Optional[int],
    mono_end: Optional[int],
    unix_start: Optional[int],
    unix_end: Optional[int],
) -> None:
    d = _preferred_delta_ns(mono_start, mono_end, unix_start, unix_end)
    if d is not None:
        vals.append(d)


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
    return [r for r in rows if r.get("kind") == "bench" and r.get("component") == component]


def _table(title: str, rows: List[Tuple[str, Stats]]) -> str:
    lines: List[str] = []
    lines.append(title)
    lines.append("-" * len(title))
    header = (
        f"{'metric':38s} {'n':>6s} {'mean':>12s} {'stddev':>12s} {'median':>12s} "
        f"{'p95':>12s} {'p99':>12s} {'min':>12s} {'max':>12s}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for name, st in rows:
        lines.append(
            f"{name:38s} {st.n:6d} "
            f"{fmt_ms(st.mean):>12s} {fmt_ms(st.stddev):>12s} {fmt_ms(st.median):>12s} "
            f"{fmt_ms(st.p95):>12s} {fmt_ms(st.p99):>12s} "
            f"{fmt_ms(st.min):>12s} {fmt_ms(st.max):>12s}"
        )
    return "\n".join(lines)


def _build_plot_df(metric_data: List[tuple], use_iqr: bool):
    import pandas as pd
    records = []
    for label, vals_ns in metric_data:
        vals_ms = [ns_to_ms(v) for v in vals_ns]
        if use_iqr:
            vals_ms = iqr_filter(vals_ms)
        for v in vals_ms:
            records.append({"Phase": label, "Time (ms)": v})
    return pd.DataFrame(records)


def _generate_plot(metric_data: List[tuple], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df = _build_plot_df(metric_data, use_iqr)
    if df.empty:
        print("  No data for plot", file=sys.stderr)
        return
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.boxplot(
        data=df, x="Phase", y="Time (ms)",
        color="#2196F3",
        showfliers=True, flierprops=dict(marker="o", markersize=4, alpha=0.5),
        ax=ax,
    )
    ax.set_title("Pipeline Timing Breakdown")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Plot saved to {output}")


def _generate_ci_plot(metric_data: List[tuple], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df = _build_plot_df(metric_data, use_iqr)
    if df.empty:
        print("  No data for CI plot", file=sys.stderr)
        return
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(
        data=df, x="Phase", y="Time (ms)",
        color="#2196F3", errorbar=("ci", 95), capsize=0.1,
        ax=ax,
    )
    ax.set_title("Pipeline Timing Breakdown — Mean (95% CI)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  CI plot saved to {output}")


# ── Combined file handling ──

def _iter_rows(paths: List[str]):
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def _list_run_ids(paths: List[str]) -> None:
    counts: Dict[str, Dict[str, int]] = {}
    for r in _iter_rows(paths):
        rid = r.get("run_id", "(none)")
        src = r.get("_source", r.get("component", "?"))
        counts.setdefault(rid, {"sniffer": 0, "retransmitter": 0, "other": 0})
        if src in counts[rid]:
            counts[rid][src] += 1
        else:
            counts[rid]["other"] += 1
    print("run_id                              sniffer    retrans    other")
    print("-" * 64)
    for rid in sorted(counts):
        c = counts[rid]
        print(f"{rid:35s}  {c['sniffer']:7d}    {c['retransmitter']:7d}    {c['other']:5d}")


def _split_combined(
    paths: List[str],
    run_id_filter: Optional[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split combined NDJSON files into sniffer and retransmitter row lists."""
    sn_rows: List[Dict[str, Any]] = []
    rt_rows: List[Dict[str, Any]] = []
    for r in _iter_rows(paths):
        if run_id_filter and r.get("run_id") != run_id_filter:
            continue
        src = r.get("_source") or r.get("component")
        if src not in ("sniffer", "retransmitter"):
            continue
        pair = r.get("bench_pair_key")
        if pair:
            r["ce_id"] = pair
        if src == "sniffer":
            sn_rows.append(r)
        else:
            rt_rows.append(r)
    return sn_rows, rt_rows


# ── Main analysis ──

def analyze(sn_all: List[Dict[str, Any]], rt_all: List[Dict[str, Any]],
            use_iqr: bool, csv_path: str,
            plot_path: Optional[str], plot_ci_path: Optional[str],
            sniffer_label: str) -> int:
    sn = _filter_rows(sn_all, "sniffer")
    rt = _filter_rows(rt_all, "retransmitter")

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

    matched_keys = set()
    for s, _ in matched:
        matched_keys.add(_key_sniffer(s))
    sn_unmatched = sum(1 for r in sn if _key_sniffer(r) not in matched_keys)

    # Sniffer-only phase timings (nanoseconds).
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
        t_ss_mono = _get_int(s, "t_send_start_mono_ns")
        t_se_mono = _get_int(s, "t_send_end_mono_ns")

        _add_delta(sn_cap_to_built, t_cap, t_bld)
        _add_delta(sn_built_to_enqueue, t_bld, t_enq)
        _add_delta(sn_enqueue_to_send_start, t_enq, t_ss)
        _add_preferred_delta(sn_send_start_to_end, t_ss_mono, t_se_mono, t_ss, t_se)
        _add_delta(sn_cap_to_send_end, t_cap, t_se)

    # Matched end-to-end and retransmitter timings.
    e2e_cap_to_rt_recv: List[float] = []
    e2e_send_start_to_rt_recv: List[float] = []
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
        t_rr = _get_int(r, "t_recv_unix_ns")
        t_rb = _get_int(r, "t_body_unix_ns")
        t_rp = _get_int(r, "t_parsed_unix_ns")
        t_fs = _get_int(r, "t_forward_start_unix_ns")
        t_fe = _get_int(r, "t_forward_end_unix_ns")
        t_rr_mono = _get_int(r, "t_recv_mono_ns")
        t_rb_mono = _get_int(r, "t_body_mono_ns")
        t_rp_mono = _get_int(r, "t_parsed_mono_ns")
        t_fs_mono = _get_int(r, "t_forward_start_mono_ns")
        t_fe_mono = _get_int(r, "t_forward_end_mono_ns")

        _add_delta(e2e_cap_to_rt_recv, t_cap, t_rr)
        _add_delta(e2e_send_start_to_rt_recv, t_ss, t_rr)
        _add_preferred_delta(rt_recv_to_body, t_rr_mono, t_rb_mono, t_rr, t_rb)
        _add_preferred_delta(rt_body_to_parsed, t_rb_mono, t_rp_mono, t_rb, t_rp)
        _add_preferred_delta(rt_parsed_to_fwd_start, t_rp_mono, t_fs_mono, t_rp, t_fs)
        _add_preferred_delta(rt_fwd_start_to_fwd_end, t_fs_mono, t_fe_mono, t_fs, t_fe)
        _add_preferred_delta(rt_recv_to_fwd_end, t_rr_mono, t_fe_mono, t_rr, t_fe)
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
        vals_ms = [ns_to_ms(v) for v in vals_ns]
        if use_iqr:
            vals_ms = iqr_filter(vals_ms)
        return Stats.from_values(vals_ms)

    # Report
    print()
    print("Bench Log Analysis")
    print("==================")
    print("input mode: raw timestamps only; all deltas below are computed here")
    print("same-process phase timings prefer raw monotonic timestamps when available")
    if use_iqr:
        print("IQR outlier filtering: ON")
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
                f"{fmt_num(st.mean):>10s} {fmt_num(st.stddev):>10s} {fmt_num(st.median):>10s} "
                f"{fmt_num(st.p95):>10s} {fmt_num(st.p99):>10s} "
                f"{fmt_num(st.min):>10s} {fmt_num(st.max):>10s}"
            )

        print("Sizes (bytes)")
        print("------------")
        print(
            f"{'metric':16s} {'n':>6s} {'mean':>10s} {'stddev':>10s} {'median':>10s} "
            f"{'p95':>10s} {'p99':>10s} {'min':>10s} {'max':>10s}"
        )
        print("-" * 97)
        if sizes_in:
            print(size_line("size_in", _size_stats(sizes_in)))
        if sizes_out:
            print(size_line("size_out", _size_stats(sizes_out)))
        print()

    # CSV export
    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ce_id", "frame_no",
                "sn_t_capture_ns", "sn_t_send_start_ns", "sn_t_send_end_ns",
                "rt_t_recv_ns", "rt_t_forward_start_ns", "rt_t_forward_end_ns",
                "e2e_capture_to_recv_ms", "e2e_send_start_to_recv_ms",
                "e2e_capture_to_forward_start_ms", "e2e_capture_to_forward_end_ms",
                "rt_recv_to_fwd_end_ms", "forward_status",
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
                t_rr_mono = _get_int(r, "t_recv_mono_ns")
                t_fe_mono = _get_int(r, "t_forward_end_mono_ns")

                e2e_ms = ns_to_ms(float(t_rr - t_cap)) if (t_rr is not None and t_cap is not None) else float("nan")
                ss_rr_ms = ns_to_ms(float(t_rr - t_ss)) if (t_rr is not None and t_ss is not None) else float("nan")
                cap_fs_ms = ns_to_ms(float(t_fs - t_cap)) if (t_fs is not None and t_cap is not None) else float("nan")
                cap_fe_ms = ns_to_ms(float(t_fe - t_cap)) if (t_fe is not None and t_cap is not None) else float("nan")
                rt_delta_ns = _preferred_delta_ns(t_rr_mono, t_fe_mono, t_rr, t_fe)
                rt_ms = ns_to_ms(rt_delta_ns) if rt_delta_ns is not None else float("nan")

                w.writerow([
                    ce_id, frame_no,
                    fmt_int(t_cap), fmt_int(t_ss), fmt_int(t_se),
                    fmt_int(t_rr), fmt_int(t_fs), fmt_int(t_fe),
                    f"{e2e_ms:.3f}" if not math.isnan(e2e_ms) else "",
                    f"{ss_rr_ms:.3f}" if not math.isnan(ss_rr_ms) else "",
                    f"{cap_fs_ms:.3f}" if not math.isnan(cap_fs_ms) else "",
                    f"{cap_fe_ms:.3f}" if not math.isnan(cap_fe_ms) else "",
                    f"{rt_ms:.3f}" if not math.isnan(rt_ms) else "",
                    r.get("forward_status"),
                ])
        print(f"Wrote CSV: {csv_path}")
        print()

    # Plots
    metric_data = [
        ("capture → ce_built", sn_cap_to_built),
        ("ce_built → enqueue", sn_built_to_enqueue),
        ("enqueue → send_start", sn_enqueue_to_send_start),
        ("send_start → send_end", sn_send_start_to_end),
        ("capture → send_end", sn_cap_to_send_end),
        ("capture → retrans recv", e2e_cap_to_rt_recv),
        ("retrans recv → fwd_end", rt_recv_to_fwd_end),
        ("capture → fwd_end", e2e_cap_to_fwd_end),
    ]

    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if plot_path is not None:
        if plot_path == "auto":
            plot_path = os.path.join(os.path.dirname(sniffer_label), f"pipeline_boxplot_{ts}.svg")
        elif os.path.isdir(plot_path):
            plot_path = os.path.join(plot_path, f"pipeline_boxplot_{ts}.svg")
        _generate_plot(metric_data, plot_path, use_iqr=use_iqr)

    if plot_ci_path is not None:
        if plot_ci_path == "auto":
            plot_ci_path = os.path.join(os.path.dirname(sniffer_label), f"pipeline_ci_{ts}.svg")
        elif os.path.isdir(plot_ci_path):
            plot_ci_path = os.path.join(plot_ci_path, f"pipeline_ci_{ts}.svg")
        _generate_ci_plot(metric_data, plot_ci_path, use_iqr=use_iqr)

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze pipeline benchmark NDJSON logs.",
        epilog="Examples:\n"
               "  %(prog)s pipeline_logs/*.ndjson\n"
               "  %(prog)s --run-id 20260413_102500 pipeline_logs/pipeline_combined.ndjson\n"
               "  %(prog)s --sniffer sn.ndjson --retrans rt.ndjson\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("inputs", nargs="*", help="combined pipeline NDJSON file(s)")
    ap.add_argument("--sniffer", default="", help="(legacy) separate sniffer NDJSON file")
    ap.add_argument("--retrans", default="", help="(legacy) separate retransmitter NDJSON file")
    ap.add_argument("--run-id", default="", help="analyze only rows with this run_id (combined mode)")
    ap.add_argument("--list-runs", action="store_true", help="list run_ids found in the file(s) and exit")
    ap.add_argument("--csv", default="", help="optional CSV output path")
    ap.add_argument("--iqr", action="store_true", help="filter outliers using Tukey's IQR fences")
    ap.add_argument("--plot", nargs="?", const="auto", default=None,
                    help="generate box plot (optionally specify output path)")
    ap.add_argument("--plot-ci", nargs="?", const="auto", default=None,
                    help="generate bar chart with 95%% CI error bars")
    args = ap.parse_args()

    # Legacy mode: separate sniffer/retransmitter files
    if args.sniffer and args.retrans:
        sn_all = read_ndjson(args.sniffer)
        rt_all = read_ndjson(args.retrans)
        return analyze(sn_all, rt_all, args.iqr, args.csv, args.plot, args.plot_ci,
                       sniffer_label=args.sniffer)

    if not args.inputs:
        ap.error("provide combined NDJSON file(s) as positional args, or use --sniffer/--retrans")

    if args.list_runs:
        _list_run_ids(args.inputs)
        return 0

    rid = args.run_id or None
    sn_rows, rt_rows = _split_combined(args.inputs, rid)

    print(f"[analyze] inputs           : {', '.join(args.inputs)}")
    if rid:
        print(f"[analyze] run_id filter    : {rid}")
    print(f"[analyze] sniffer rows     : {len(sn_rows)}")
    print(f"[analyze] retransmitter    : {len(rt_rows)}")
    print()

    if not sn_rows or not rt_rows:
        print("[analyze] not enough rows to match — aborting")
        return 1

    return analyze(sn_rows, rt_rows, args.iqr, args.csv, args.plot, args.plot_ci,
                   sniffer_label=args.inputs[0])


if __name__ == "__main__":
    raise SystemExit(main())
