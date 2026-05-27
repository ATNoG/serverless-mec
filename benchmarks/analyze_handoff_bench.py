#!/usr/bin/env python3
"""
analyze_handoff_bench.py

Standalone analyzer for NDJSON results produced by `bench_handoff.sh`.

Each row contains raw timestamps in the `pipeline` object. This script
computes all derived phase durations from those raw timestamps.

Scenarios: rsu-a-coldstart, worker1-coldstart, worker1-freeze
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


def _read(paths: List[str]) -> List[dict]:
    rows: List[dict] = []
    for p in paths:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
                except json.JSONDecodeError:
                    continue
    return rows


def _parse_ts(s: Optional[str]) -> Optional[float]:
    """Parse a Kubernetes RFC3339 timestamp to epoch seconds."""
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            continue
    return None


def _phase_duration_ms(pipeline: dict, start_key: str, end_key: str) -> Optional[float]:
    """Compute duration in ms between two pipeline timestamp keys."""
    t0 = _parse_ts(pipeline.get(start_key))
    t1 = _parse_ts(pipeline.get(end_key))
    if t0 is not None and t1 is not None and t1 >= t0:
        return (t1 - t0) * 1000.0
    return None


def _compute_phases(row: dict) -> dict:
    """Compute derived phase durations from raw pipeline timestamps."""
    p = row.get("pipeline", {})
    phases = {}

    # Handoff wall-clock time with ms precision (measured by bench script)
    hw = row.get("handoff_wall_ms")
    if isinstance(hw, (int, float)) and hw > 0:
        phases["handoff_wall_ms"] = hw

    # CR creation -> CR Progressing (reason=Applied) condition
    d = _phase_duration_ms(p, "cr_created", "cr_cond_progressing_ts")
    if d is not None:
        phases["cr_to_applied_ms"] = d

    # CR creation -> CR Ready condition (full handoff)
    d = _phase_duration_ms(p, "cr_created", "cr_cond_ready_ts")
    if d is not None:
        phases["cr_to_ready_ms"] = d

    # KService creation -> pod creation (scheduling overhead)
    d = _phase_duration_ms(p, "ksvc_created", "pod_created")
    if d is not None:
        phases["ksvc_to_pod_ms"] = d

    # Pod creation -> PodScheduled
    d = _phase_duration_ms(p, "pod_created", "pod_cond_podscheduled_ts")
    if d is not None:
        phases["pod_scheduling_ms"] = d

    # PodScheduled -> ContainersReady
    d = _phase_duration_ms(p, "pod_cond_podscheduled_ts", "pod_cond_containersready_ts")
    if d is not None:
        phases["container_startup_ms"] = d

    # ContainersReady -> Pod Ready
    d = _phase_duration_ms(p, "pod_cond_containersready_ts", "pod_cond_ready_ts")
    if d is not None:
        phases["readiness_probe_ms"] = d

    # Pod creation -> Pod Ready (total pod startup)
    d = _phase_duration_ms(p, "pod_created", "pod_cond_ready_ts")
    if d is not None:
        phases["pod_total_startup_ms"] = d

    # CR Progressing -> CR Ready (service startup after operator applies)
    d = _phase_duration_ms(p, "cr_cond_progressing_ts", "cr_cond_ready_ts")
    if d is not None:
        phases["applied_to_ready_ms"] = d

    return phases


def _t_inv(p: float, df: int) -> float:
    """Approximate inverse of Student's t CDF (two-tailed)."""
    t_ = math.sqrt(-2.0 * math.log(1.0 - p))
    xp = t_ - (2.515517 + 0.802853 * t_ + 0.010328 * t_ ** 2) / \
              (1.0 + 1.432788 * t_ + 0.189269 * t_ ** 2 + 0.001308 * t_ ** 3)
    g1 = (xp ** 3 + xp) / (4 * df)
    g2 = (5 * xp ** 5 + 16 * xp ** 3 + 3 * xp) / (96 * df ** 2)
    return xp + g1 + g2


def _ci95(vals: List[float]) -> Tuple[float, float]:
    n = len(vals)
    if n < 2:
        nan = float("nan")
        return (nan, nan)
    mean = statistics.fmean(vals)
    se = statistics.stdev(vals) / math.sqrt(n)
    t = _t_inv(0.975, n - 1)
    return (mean - t * se, mean + t * se)


def _stats(vals: List[float]) -> Dict[str, float]:
    if not vals:
        nan = float("nan")
        return dict(n=0, mean=nan, median=nan, stdev=nan,
                    ci95_lo=nan, ci95_hi=nan,
                    p95=nan, p99=nan, min=nan, max=nan)
    s = sorted(vals)
    n = len(s)
    lo, hi = _ci95(s)
    return dict(
        n=n,
        mean=statistics.fmean(s),
        median=statistics.median(s),
        stdev=statistics.stdev(s) if n > 1 else 0.0,
        ci95_lo=lo,
        ci95_hi=hi,
        p95=s[min(n - 1, int(round(n * 0.95)) - 1) if n > 1 else 0],
        p99=s[min(n - 1, int(round(n * 0.99)) - 1) if n > 1 else 0],
        min=s[0],
        max=s[-1],
    )


def _fmt_ms(x: float) -> str:
    return "n/a" if isinstance(x, float) and math.isnan(x) else f"{x:.1f} ms"


def _iqr_filter(vals: List[float]) -> List[float]:
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


SCENARIOS = [
    "rsu-a-coldstart",
    "worker1-coldstart", "worker1-freeze",
    "worker2-coldstart", "worker2-freeze",
]
SCENARIO_LABELS = {
    "rsu-a-coldstart": "RSU-A Cold Start",
    "worker1-coldstart": "Worker-1 Cold Start",
    "worker1-freeze": "Worker-1 Freeze Handoff",
    "worker2-coldstart": "Worker-2 Cold Start",
    "worker2-freeze": "Worker-2 Freeze Handoff",
}
SCENARIO_COLORS = {
    "RSU-A Cold Start": "#FF9800",
    "Worker-1 Cold Start": "#4CAF50",
    "Worker-1 Freeze Handoff": "#2196F3",
    "Worker-2 Cold Start": "#9C27B0",
    "Worker-2 Freeze Handoff": "#E91E63",
}

# Pipeline phases to report (key in computed phases -> display label)
# Cold-start phases include the full KService creation pipeline.
# Freeze phases only include CR lifecycle (KSvc/pod timestamps are from
# pre-freeze warmup, not the measured handoff, so they're meaningless).
PIPELINE_PHASES_COLDSTART = [
    ("cr_to_applied_ms", "CR -> Applied"),
    ("ksvc_to_pod_ms", "KSvc -> Pod Created"),
    ("pod_scheduling_ms", "Pod Scheduling"),
    ("container_startup_ms", "Container Startup"),
    ("readiness_probe_ms", "Readiness Probe"),
    ("pod_total_startup_ms", "Pod Total Startup"),
    ("applied_to_ready_ms", "Applied -> Ready"),
    ("cr_to_ready_ms", "CR -> Ready (k8s ts)"),
    ("handoff_wall_ms", "Handoff Wall Clock"),
]
PIPELINE_PHASES_FREEZE = [
    ("cr_to_ready_ms", "CR -> Ready (k8s ts)"),
    ("handoff_wall_ms", "Handoff Wall Clock"),
]


def _get_phase_values(rows: List[dict], scenario: str, phase_key: str,
                      computed: Dict[int, dict]) -> List[float]:
    out: List[float] = []
    for idx, r in enumerate(rows):
        if r.get("scenario") != scenario:
            continue
        if r.get("handoff_phase") != "Ready":
            continue
        phases = computed.get(idx, {})
        v = phases.get(phase_key)
        if isinstance(v, (int, float)) and v >= 0:
            out.append(v)
    return out


def _http_codes(rows: List[dict], scenario: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        if r.get("scenario") != scenario:
            continue
        c = str(r.get("http_code", "?"))
        out[c] = out.get(c, 0) + 1
    return out


def _handoff_phases(rows: List[dict], scenario: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        if r.get("scenario") != scenario:
            continue
        p = r.get("handoff_phase", "Unknown")
        out[p] = out.get(p, 0) + 1
    return out


def _print_stats_block(label: str, st: Dict[str, float]) -> None:
    print(f"  {label}:")
    for k in ("n", "mean", "median", "stdev", "ci95", "p95", "p99", "min", "max"):
        if k == "ci95":
            lo, hi = st["ci95_lo"], st["ci95_hi"]
            if math.isnan(lo):
                val = "n/a"
            else:
                val = f"[{lo:.1f}, {hi:.1f}] ms"
            print(f"    {k:<7} = {val}")
        else:
            v = st[k]
            val = str(v) if k == "n" else _fmt_ms(v)
            print(f"    {k:<7} = {val}")


def _ci_str(vals: List[float]) -> str:
    lo, hi = _ci95(vals)
    if math.isnan(lo):
        return "n/a"
    return f"[{lo:.1f}, {hi:.1f}]"


def _build_plot_df(rows: List[dict], computed: Dict[int, dict], use_iqr: bool):
    import pandas as pd

    # Plot the pipeline phases that matter most
    plot_phases = [
        ("CR -> Applied", "cr_to_applied_ms"),
        ("Pod Total Startup", "pod_total_startup_ms"),
        ("Applied -> Ready", "applied_to_ready_ms"),
        ("Handoff Wall Clock", "handoff_wall_ms"),
        ("Retransmission", None),  # from t_total_s
    ]

    records = []
    for label, phase_key in plot_phases:
        for scenario in SCENARIOS:
            sl = SCENARIO_LABELS.get(scenario, scenario)
            if phase_key is None:
                # Retransmission from curl timing
                vals = []
                for r in rows:
                    if r.get("scenario") != scenario:
                        continue
                    v = r.get("t_total_s")
                    if isinstance(v, (int, float)) and v > 0:
                        vals.append(v * 1000.0)
            else:
                vals = _get_phase_values(rows, scenario, phase_key, computed)
            if use_iqr:
                vals = _iqr_filter(vals)
            for v in vals:
                records.append({"Phase": label, "Scenario": sl, "Time (ms)": v})

    return pd.DataFrame(records), SCENARIO_COLORS


def _generate_plot(rows: List[dict], computed: Dict[int, dict],
                   output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _build_plot_df(rows, computed, use_iqr)
    if df.empty:
        print("  No data for plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(14, 7))
    sns.boxplot(
        data=df, x="Phase", y="Time (ms)", hue="Scenario",
        palette=palette,
        showfliers=True, flierprops=dict(marker="o", markersize=4, alpha=0.5),
        ax=ax,
    )
    ax.set_title("Handoff Benchmark — Pipeline Phase Breakdown")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Plot saved to {output}")


def _generate_ci_plot(rows: List[dict], computed: Dict[int, dict],
                      output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _build_plot_df(rows, computed, use_iqr)
    if df.empty:
        print("  No data for CI plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(14, 7))
    sns.barplot(
        data=df, x="Phase", y="Time (ms)", hue="Scenario",
        palette=palette, errorbar=("ci", 95), capsize=0.1,
        ax=ax,
    )
    ax.set_title("Handoff Benchmark — Mean Pipeline Phase Duration (95% CI)")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  CI plot saved to {output}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="handoff bench NDJSON file(s)")
    ap.add_argument("--iqr", action="store_true",
                    help="filter outliers using Tukey's IQR fences before computing stats")
    ap.add_argument("--plot", nargs="?", const="auto", default=None,
                    help="generate box plot (optionally specify output path, default: auto)")
    ap.add_argument("--plot-ci", nargs="?", const="auto", default=None,
                    help="generate bar chart with 95%% CI error bars")
    args = ap.parse_args()

    rows = _read(args.inputs)
    if not rows:
        print("no rows found", file=sys.stderr)
        return 1

    # Compute derived phase durations for every row
    computed: Dict[int, dict] = {}
    for idx, row in enumerate(rows):
        computed[idx] = _compute_phases(row)

    # Determine which scenarios have data
    present = [s for s in SCENARIOS if any(r.get("scenario") == s for r in rows)]

    print()
    print("=" * 70)
    print("  Handoff Benchmark Results")
    print("=" * 70)
    print(f"  inputs   : {', '.join(args.inputs)}")
    print(f"  scenarios: {', '.join(present)}")
    print()

    for scenario in present:
        label = SCENARIO_LABELS.get(scenario, scenario)

        print("-" * 70)
        print(f"  {label}")
        print("-" * 70)

        # Retransmission (curl total)
        retrans_raw = []
        for r in rows:
            if r.get("scenario") != scenario:
                continue
            v = r.get("t_total_s")
            if isinstance(v, (int, float)) and v > 0:
                retrans_raw.append(v * 1000.0)
        retrans = _iqr_filter(retrans_raw) if args.iqr else retrans_raw

        if retrans:
            _print_stats_block("Retransmission (curl total)", _stats(retrans))
            print()

        # Pipeline phase breakdown (freeze scenarios only show CR lifecycle)
        is_freeze = "freeze" in scenario
        phases_to_show = PIPELINE_PHASES_FREEZE if is_freeze else PIPELINE_PHASES_COLDSTART
        print(f"  {'Phase':<30}  {'n':>4}  {'mean':>10}  {'median':>10}  {'stdev':>10}  {'95% CI':>22}")
        print(f"  {'-'*30}  {'-'*4}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*22}")
        for phase_key, phase_label in phases_to_show:
            vals = _get_phase_values(rows, scenario, phase_key, computed)
            if args.iqr:
                vals = _iqr_filter(vals)
            if not vals:
                continue
            st = _stats(vals)
            print(f"  {phase_label:<30}  {st['n']:>4}  {_fmt_ms(st['mean']):>10}  "
                  f"{_fmt_ms(st['median']):>10}  {_fmt_ms(st['stdev']):>10}  {_ci_str(vals):>22}")
        print()

        # HTTP codes & handoff phase distribution
        print(f"  HTTP codes    : {_http_codes(rows, scenario)}")
        print(f"  Handoff phases: {_handoff_phases(rows, scenario)}")
        print()

    # Cross-scenario comparison
    if len(present) > 1:
        print("=" * 70)
        print("  Cross-Scenario Comparison (Handoff Wall Clock)")
        print("=" * 70)
        for scenario in present:
            label = SCENARIO_LABELS.get(scenario, scenario)
            vals = _get_phase_values(rows, scenario, "handoff_wall_ms", computed)
            if args.iqr:
                vals = _iqr_filter(vals)
            st = _stats(vals)
            print(f"  {label:<25}  mean={_fmt_ms(st['mean']):>10}  "
                  f"median={_fmt_ms(st['median']):>10}  "
                  f"stdev={_fmt_ms(st['stdev']):>10}  "
                  f"p95={_fmt_ms(st['p95']):>10}")
        print()

        # Pairwise speedups
        for i, s1 in enumerate(present):
            for s2 in present[i + 1:]:
                v1 = _get_phase_values(rows, s1, "handoff_wall_ms", computed)
                v2 = _get_phase_values(rows, s2, "handoff_wall_ms", computed)
                if args.iqr:
                    v1, v2 = _iqr_filter(v1), _iqr_filter(v2)
                if v1 and v2:
                    m1, m2 = statistics.fmean(v1), statistics.fmean(v2)
                    if m1 > 0 and m2 > 0:
                        l1 = SCENARIO_LABELS.get(s1, s1)
                        l2 = SCENARIO_LABELS.get(s2, s2)
                        if m1 > m2:
                            print(f"  {l2} is {m1 / m2:.1f}x faster than {l1} (by mean)")
                        else:
                            print(f"  {l1} is {m2 / m1:.1f}x faster than {l2} (by mean)")
        print()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.plot is not None:
        if args.plot == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            plot_path = f"{base}_boxplot_{ts}.svg"
        elif os.path.isdir(args.plot):
            plot_path = os.path.join(args.plot, f"handoff_bench_boxplot_{ts}.svg")
        else:
            plot_path = args.plot
        _generate_plot(rows, computed, plot_path, use_iqr=args.iqr)

    if args.plot_ci is not None:
        if args.plot_ci == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            ci_path = f"{base}_ci_{ts}.svg"
        elif os.path.isdir(args.plot_ci):
            ci_path = os.path.join(args.plot_ci, f"handoff_bench_ci_{ts}.svg")
        else:
            ci_path = args.plot_ci
        _generate_ci_plot(rows, computed, ci_path, use_iqr=args.iqr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
