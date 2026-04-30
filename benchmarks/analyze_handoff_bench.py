#!/usr/bin/env python3
"""
analyze_handoff_bench.py

Standalone analyzer for NDJSON results produced by `bench_handoff.sh`.

Each row:
  {"scenario":"rsu-a-coldstart","iteration":1,"t_dns_s":...,"t_connect_s":...,
   "t_ttfb_s":...,"t_total_s":...,"http_code":"422",
   "ts_before":"...","ts_after":"...",
   "handoff_phase":"Ready","handoff_time_ms":1234.0}

Scenarios: rsu-a-coldstart, worker1-coldstart, worker1-freeze
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from typing import Dict, List, Tuple


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


def _t_inv(p: float, df: int) -> float:
    """Approximate inverse of Student's t CDF (two-tailed)."""
    import math as _m
    t_ = _m.sqrt(-2.0 * _m.log(1.0 - p))
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


def _print_block(label: str, st: Dict[str, float]) -> None:
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


def _iqr_filter(vals: List[float]) -> List[float]:
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


SCENARIOS = ["rsu-a-coldstart", "worker1-coldstart", "worker1-freeze"]
SCENARIO_LABELS = {
    "rsu-a-coldstart": "RSU-A Cold Start",
    "worker1-coldstart": "Worker-1 Cold Start",
    "worker1-freeze": "Worker-1 CRIU Thaw",
}
SCENARIO_COLORS = {
    "RSU-A Cold Start": "#FF9800",
    "Worker-1 Cold Start": "#4CAF50",
    "Worker-1 CRIU Thaw": "#2196F3",
}


def _get_values(rows: List[dict], scenario: str, key: str, scale: float = 1.0) -> List[float]:
    out: List[float] = []
    for r in rows:
        if r.get("scenario") != scenario:
            continue
        v = r.get(key)
        if isinstance(v, (int, float)) and v > 0:
            out.append(v * scale)
    return out


def _handoff_values(rows: List[dict], scenario: str) -> List[float]:
    """Get handoff_time_ms for successful handoffs."""
    out: List[float] = []
    for r in rows:
        if r.get("scenario") != scenario:
            continue
        if r.get("handoff_phase") != "Ready":
            continue
        v = r.get("handoff_time_ms")
        if isinstance(v, (int, float)) and v > 0:
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


def _build_plot_df(rows: List[dict], use_iqr: bool):
    import pandas as pd

    metrics: List[Tuple[str, str, float]] = [
        ("DNS", "t_dns_s", 1000.0),
        ("TCP Connect", "t_connect_s", 1000.0),
        ("TTFB", "t_ttfb_s", 1000.0),
        ("Retransmission (total)", "t_total_s", 1000.0),
        ("Handoff", "handoff_time_ms", 1.0),
    ]

    records = []
    for label, key, scale in metrics:
        for scenario in SCENARIOS:
            sl = SCENARIO_LABELS.get(scenario, scenario)
            if key == "handoff_time_ms":
                vals = _handoff_values(rows, scenario)
            else:
                vals = _get_values(rows, scenario, key, scale)
            if use_iqr:
                vals = _iqr_filter(vals)
            for v in vals:
                records.append({"Metric": label, "Scenario": sl, "Time (ms)": v})

    return pd.DataFrame(records), SCENARIO_COLORS


def _generate_plot(rows: List[dict], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _build_plot_df(rows, use_iqr)
    if df.empty:
        print("  No data for plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.boxplot(
        data=df, x="Metric", y="Time (ms)", hue="Scenario",
        palette=palette,
        showfliers=True, flierprops=dict(marker="o", markersize=4, alpha=0.5),
        ax=ax,
    )
    ax.set_title("Handoff Benchmark — Response Time Breakdown")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Plot saved to {output}")


def _generate_ci_plot(rows: List[dict], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _build_plot_df(rows, use_iqr)
    if df.empty:
        print("  No data for CI plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(
        data=df, x="Metric", y="Time (ms)", hue="Scenario",
        palette=palette, errorbar=("ci", 95), capsize=0.1,
        ax=ax,
    )
    ax.set_title("Handoff Benchmark — Mean Response Time (95% CI)")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  CI plot saved to {output}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="handoff bench NDJSON file(s)")
    ap.add_argument("--per-iter", action="store_true",
                    help="print per-iteration table (default: skip if >50 samples)")
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

        # Retransmission (t_total_s)
        retrans_raw = _get_values(rows, scenario, "t_total_s", 1000.0)
        retrans = _iqr_filter(retrans_raw) if args.iqr else retrans_raw

        # Handoff time
        handoff_raw = _handoff_values(rows, scenario)
        handoff = _iqr_filter(handoff_raw) if args.iqr else handoff_raw

        print("-" * 70)
        print(f"  {label}")
        print("-" * 70)
        if args.iqr:
            print(f"  IQR filtering: retrans {len(retrans_raw)}->{len(retrans)}, "
                  f"handoff {len(handoff_raw)}->{len(handoff)}")

        print()
        _print_block("Retransmission (total curl time)", _stats(retrans))
        print()
        _print_block("Handoff time (CR create -> Ready)", _stats(handoff))
        print()

        # Phase breakdown (curl phases)
        phases: List[Tuple[str, str]] = [
            ("DNS", "t_dns_s"),
            ("TCP Connect", "t_connect_s"),
            ("TTFB", "t_ttfb_s"),
            ("Total", "t_total_s"),
        ]

        def _ci_str(vals: List[float]) -> str:
            lo, hi = _ci95(vals)
            if math.isnan(lo):
                return "n/a"
            return f"[{lo:.1f}, {hi:.1f}]"

        print(f"  {'phase':<20}  {'mean':>10}  {'95% CI':>22}")
        print(f"  {'-'*20}  {'-'*10}  {'-'*22}")
        for plabel, key in phases:
            vals = _get_values(rows, scenario, key, 1000.0)
            if args.iqr:
                vals = _iqr_filter(vals)
            mean = statistics.fmean(vals) if vals else float("nan")
            print(f"  {plabel:<20}  {_fmt_ms(mean):>10}  {_ci_str(vals):>22}")
        print()

        # HTTP codes & handoff phase distribution
        print(f"  HTTP codes    : {_http_codes(rows, scenario)}")
        print(f"  Handoff phases: {_handoff_phases(rows, scenario)}")
        print()

    # Cross-scenario comparison
    if len(present) > 1:
        print("=" * 70)
        print("  Cross-Scenario Comparison (Handoff Time)")
        print("=" * 70)
        for scenario in present:
            label = SCENARIO_LABELS.get(scenario, scenario)
            vals = _handoff_values(rows, scenario)
            if args.iqr:
                vals = _iqr_filter(vals)
            st = _stats(vals)
            print(f"  {label:<25}  mean={_fmt_ms(st['mean']):>10}  "
                  f"median={_fmt_ms(st['median']):>10}  "
                  f"p95={_fmt_ms(st['p95']):>10}")
        print()

        # Pairwise speedups
        for i, s1 in enumerate(present):
            for s2 in present[i+1:]:
                v1 = _handoff_values(rows, s1)
                v2 = _handoff_values(rows, s2)
                if args.iqr:
                    v1, v2 = _iqr_filter(v1), _iqr_filter(v2)
                if v1 and v2:
                    m1, m2 = statistics.fmean(v1), statistics.fmean(v2)
                    if m1 > 0 and m2 > 0:
                        l1 = SCENARIO_LABELS.get(s1, s1)
                        l2 = SCENARIO_LABELS.get(s2, s2)
                        if m1 > m2:
                            print(f"  {l2} is {m1/m2:.1f}x faster than {l1} (by mean)")
                        else:
                            print(f"  {l1} is {m2/m1:.1f}x faster than {l2} (by mean)")
        print()

    # Per-iteration table
    for scenario in present:
        label = SCENARIO_LABELS.get(scenario, scenario)
        s_rows = [r for r in rows if r.get("scenario") == scenario]
        show = args.per_iter or len(s_rows) <= 50
        if show:
            print("-" * 70)
            print(f"  Per-Iteration: {label}")
            print("-" * 70)
            print(f"  {'#':>3}  {'retrans_ms':>12}  {'handoff_ms':>12}  {'phase':>10}")
            print(f"  {'':>3}  {'-'*12}  {'-'*12}  {'-'*10}")
            for r in s_rows:
                it = r.get("iteration", "?")
                t = r.get("t_total_s")
                t_str = f"{t*1000:.1f}" if isinstance(t, (int, float)) and t > 0 else "-"
                h = r.get("handoff_time_ms")
                h_str = f"{h:.0f}" if isinstance(h, (int, float)) and h > 0 else "-"
                phase = r.get("handoff_phase", "?")
                print(f"  {it:>3}  {t_str:>12}  {h_str:>12}  {phase:>10}")
            print()
        else:
            print(f"  (per-iteration table for {label} suppressed; pass --per-iter to show)")
            print()

    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.plot is not None:
        if args.plot == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            plot_path = f"{base}_boxplot_{ts}.svg"
        elif os.path.isdir(args.plot):
            plot_path = os.path.join(args.plot, f"handoff_bench_boxplot_{ts}.svg")
        else:
            plot_path = args.plot
        _generate_plot(rows, plot_path, use_iqr=args.iqr)

    if args.plot_ci is not None:
        if args.plot_ci == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            ci_path = f"{base}_ci_{ts}.svg"
        elif os.path.isdir(args.plot_ci):
            ci_path = os.path.join(args.plot_ci, f"handoff_bench_ci_{ts}.svg")
        else:
            ci_path = args.plot_ci
        _generate_ci_plot(rows, ci_path, use_iqr=args.iqr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
