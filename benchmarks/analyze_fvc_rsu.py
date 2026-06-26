#!/usr/bin/env python3
"""
analyze_fvc_rsu.py — Analyze all RSU Freeze vs Cold-start benchmark results.

Automatically finds all NDJSON files in freeze_vs_coldstart_logs/ that contain
RSU node data, aggregates them, and prints per-run and combined statistics.

Usage:
  ./analyze_fvc_rsu.py [--iqr] [--plot] [--plot-ci] [--per-run] [--node NODE]
  ./analyze_fvc_rsu.py path/to/file.ndjson [...]  # specific files
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import statistics
import sys
from typing import Dict, List, Tuple


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG_DIR = os.path.join(SCRIPT_DIR, "freeze_vs_coldstart_logs")


def read_ndjson(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
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


def find_rsu_files(log_dir: str, node: str) -> List[str]:
    pattern = os.path.join(log_dir, "freeze_vs_coldstart_*.ndjson")
    rsu_files = []
    for path in sorted(glob.glob(pattern)):
        rows = read_ndjson(path)
        if rows and rows[0].get("node", "").startswith(node):
            rsu_files.append(path)
    return rsu_files


def t_inv(p: float, df: int) -> float:
    t_ = math.sqrt(-2.0 * math.log(1.0 - p))
    xp = t_ - (2.515517 + 0.802853 * t_ + 0.010328 * t_ ** 2) / \
              (1.0 + 1.432788 * t_ + 0.189269 * t_ ** 2 + 0.001308 * t_ ** 3)
    g1 = (xp ** 3 + xp) / (4 * df)
    g2 = (5 * xp ** 5 + 16 * xp ** 3 + 3 * xp) / (96 * df ** 2)
    return xp + g1 + g2


def ci95(vals: List[float]) -> Tuple[float, float]:
    n = len(vals)
    if n < 2:
        return (float("nan"), float("nan"))
    mean = statistics.fmean(vals)
    se = statistics.stdev(vals) / math.sqrt(n)
    t = t_inv(0.975, n - 1)
    return (mean - t * se, mean + t * se)


def calc_stats(vals: List[float]) -> Dict[str, float]:
    if not vals:
        nan = float("nan")
        return dict(n=0, mean=nan, median=nan, stdev=nan,
                    ci95_lo=nan, ci95_hi=nan, p5=nan, p95=nan, min=nan, max=nan)
    s = sorted(vals)
    n = len(s)
    lo, hi = ci95(s)
    return dict(
        n=n,
        mean=statistics.fmean(s),
        median=statistics.median(s),
        stdev=statistics.stdev(s) if n > 1 else 0.0,
        ci95_lo=lo, ci95_hi=hi,
        p5=s[max(0, int(round(n * 0.05)) - 1)],
        p95=s[min(n - 1, int(round(n * 0.95)) - 1)],
        min=s[0], max=s[-1],
    )


def iqr_filter(vals: List[float]) -> List[float]:
    if len(vals) < 4:
        return vals
    s = sorted(vals)
    n = len(s)
    q1, q3 = s[n // 4], s[(3 * n) // 4]
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return [v for v in vals if lo <= v <= hi]


def extract_values(rows: List[dict], mode: str) -> List[float]:
    return [
        r["t_total_s"] * 1000.0
        for r in rows
        if r.get("mode") == mode
        and isinstance(r.get("t_total_s"), (int, float))
        and r["t_total_s"] > 0
        and r.get("http_code") not in (0, "0", "000", None)
    ]


def phase_values(rows: List[dict], mode: str, key: str) -> List[float]:
    return [
        r[key] * 1000.0
        for r in rows
        if r.get("mode") == mode
        and isinstance(r.get(key), (int, float))
        and r[key] > 0
    ]


def fmt_ms(x: float) -> str:
    return "n/a" if math.isnan(x) else f"{x:.1f}"


def fmt_ci(vals: List[float]) -> str:
    lo, hi = ci95(vals)
    if math.isnan(lo):
        return "n/a"
    return f"[{lo:.1f}, {hi:.1f}]"


def print_stats_block(label: str, st: Dict[str, float]) -> None:
    print(f"  {label} (n={st['n']}):")
    print(f"    mean   = {fmt_ms(st['mean'])} ms")
    print(f"    median = {fmt_ms(st['median'])} ms")
    print(f"    stdev  = {fmt_ms(st['stdev'])} ms")
    lo, hi = st["ci95_lo"], st["ci95_hi"]
    ci_str = f"[{lo:.1f}, {hi:.1f}] ms" if not math.isnan(lo) else "n/a"
    print(f"    CI95   = {ci_str}")
    print(f"    p5/p95 = {fmt_ms(st['p5'])} / {fmt_ms(st['p95'])} ms")
    print(f"    range  = [{fmt_ms(st['min'])}, {fmt_ms(st['max'])}] ms")


def print_run_summary(path: str, rows: List[dict], use_iqr: bool) -> None:
    name = os.path.basename(path)
    thaw_raw = extract_values(rows, "criu_thaw")
    cold_raw = extract_values(rows, "cold_start")
    thaw = iqr_filter(thaw_raw) if use_iqr else thaw_raw
    cold = iqr_filter(cold_raw) if use_iqr else cold_raw

    ts = rows[0].get("ts_before", "?")[:10] if rows else "?"
    thaw_mean = f"{statistics.fmean(thaw):.0f}" if thaw else "-"
    cold_mean = f"{statistics.fmean(cold):.0f}" if cold else "-"
    thaw_ci = fmt_ci(thaw) if thaw else "-"
    cold_ci = fmt_ci(cold) if cold else "-"

    print(f"  {name:<55} {ts}  thaw: n={len(thaw):>3} mean={thaw_mean:>6}ms {thaw_ci:>20}  "
          f"cold: n={len(cold):>3} mean={cold_mean:>6}ms {cold_ci:>20}")


def generate_boxplot(rows: List[dict], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    records = []
    for label, key in [("DNS", "t_dns_s"), ("TCP Connect", "t_connect_s"),
                        ("TTFB", "t_ttfb_s"), ("Total", "t_total_s")]:
        for mode, mode_label in [("criu_thaw", "CRIU Thaw"), ("cold_start", "Cold Start")]:
            vals = phase_values(rows, mode, key)
            if use_iqr:
                vals = iqr_filter(vals)
            for v in vals:
                records.append({"Phase": label, "Scenario": mode_label, "Time (ms)": v})

    import pandas as pd
    df = pd.DataFrame(records)
    if df.empty:
        print("  No data for plot", file=sys.stderr)
        return

    palette = {"CRIU Thaw": "#2196F3", "Cold Start": "#4CAF50"}
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(data=df, x="Phase", y="Time (ms)", hue="Scenario",
                palette=palette, showfliers=True,
                flierprops=dict(marker="o", markersize=4, alpha=0.5), ax=ax)
    ax.set_title("RSU FvC — CRIU Thaw vs Cold Start (all runs)")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Box plot saved to {output}")


def generate_ci_plot(rows: List[dict], output: str, use_iqr: bool) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    records = []
    for label, key in [("DNS", "t_dns_s"), ("TCP Connect", "t_connect_s"),
                        ("TTFB", "t_ttfb_s"), ("Total", "t_total_s")]:
        for mode, mode_label in [("criu_thaw", "CRIU Thaw"), ("cold_start", "Cold Start")]:
            vals = phase_values(rows, mode, key)
            if use_iqr:
                vals = iqr_filter(vals)
            for v in vals:
                records.append({"Phase": label, "Scenario": mode_label, "Time (ms)": v})

    import pandas as pd
    df = pd.DataFrame(records)
    if df.empty:
        print("  No data for CI plot", file=sys.stderr)
        return

    palette = {"CRIU Thaw": "#2196F3", "Cold Start": "#4CAF50"}
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=df, x="Phase", y="Time (ms)", hue="Scenario",
                palette=palette, errorbar=("ci", 95), capsize=0.1, ax=ax)
    ax.set_title("RSU FvC — Mean Response Time with 95% CI (all runs)")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  CI plot saved to {output}")


def generate_timeline_plot(all_runs: List[Tuple[str, List[dict]]], output: str, use_iqr: bool) -> None:
    """Plot mean thaw and cold start times per run over time."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime

    run_labels = []
    thaw_means = []
    cold_means = []

    for path, rows in all_runs:
        thaw = extract_values(rows, "criu_thaw")
        cold = extract_values(rows, "cold_start")
        if use_iqr:
            thaw = iqr_filter(thaw)
            cold = iqr_filter(cold)
        if not thaw and not cold:
            continue

        # Extract date from filename
        base = os.path.basename(path).replace("freeze_vs_coldstart_", "").replace(".ndjson", "")
        try:
            dt = datetime.strptime(base, "%Y%m%d_%H%M%S")
            label = dt.strftime("%m/%d %H:%M")
        except ValueError:
            label = base

        run_labels.append(label)
        thaw_means.append(statistics.fmean(thaw) if thaw else None)
        cold_means.append(statistics.fmean(cold) if cold else None)

    fig, ax = plt.subplots(figsize=(12, 5))
    x = range(len(run_labels))
    if any(v is not None for v in thaw_means):
        ax.plot(x, thaw_means, "o-", color="#2196F3", label="CRIU Thaw", markersize=6)
    if any(v is not None for v in cold_means):
        ax.plot(x, cold_means, "s-", color="#4CAF50", label="Cold Start", markersize=6)
    ax.set_xticks(list(x))
    ax.set_xticklabels(run_labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Mean response time (ms)")
    ax.set_title("RSU FvC — Mean Response Time per Run")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Timeline plot saved to {output}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="*", help="specific NDJSON file(s) (default: auto-detect RSU files)")
    ap.add_argument("--iqr", action="store_true", help="filter outliers using Tukey's IQR fences")
    ap.add_argument("--plot", nargs="?", const="auto", default=None, help="generate box plot")
    ap.add_argument("--plot-ci", nargs="?", const="auto", default=None, help="generate CI bar chart")
    ap.add_argument("--plot-timeline", nargs="?", const="auto", default=None, help="generate per-run timeline")
    ap.add_argument("--per-run", action="store_true", help="show per-run summary table")
    ap.add_argument("--node", default="rsu", help="node name prefix to filter (default: rsu)")
    args = ap.parse_args()

    if args.inputs:
        files = args.inputs
    else:
        files = find_rsu_files(DEFAULT_LOG_DIR, args.node)
        if not files:
            print(f"No RSU files found in {DEFAULT_LOG_DIR}", file=sys.stderr)
            return 1

    all_runs: List[Tuple[str, List[dict]]] = []
    all_rows: List[dict] = []
    for path in files:
        rows = read_ndjson(path)
        if rows:
            all_runs.append((path, rows))
            all_rows.extend(rows)

    if not all_rows:
        print("No data found", file=sys.stderr)
        return 1

    thaw_raw = extract_values(all_rows, "criu_thaw")
    cold_raw = extract_values(all_rows, "cold_start")
    thaw = iqr_filter(thaw_raw) if args.iqr else thaw_raw
    cold = iqr_filter(cold_raw) if args.iqr else cold_raw

    print()
    print("=" * 80)
    print("  RSU Freeze vs Cold-start — Aggregated Results")
    print("=" * 80)
    print(f"  Files: {len(files)}")
    print(f"  Node:  {all_rows[0].get('node', '?')}")
    if args.iqr:
        print(f"  IQR:   ON (thaw {len(thaw_raw)}->{len(thaw)}, cold {len(cold_raw)}->{len(cold)})")
    print()

    # Per-run summary
    if args.per_run or len(all_runs) <= 20:
        print("-" * 80)
        print("  Per-Run Summary")
        print("-" * 80)
        for path, rows in all_runs:
            print_run_summary(path, rows, args.iqr)
        print()

    # Combined stats
    print("-" * 80)
    print("  Combined Statistics (ms)")
    print("-" * 80)
    print_stats_block("CRIU Thaw", calc_stats(thaw))
    print()
    print_stats_block("Cold Start", calc_stats(cold))
    print()

    # Speedup
    thaw_st = calc_stats(thaw)
    cold_st = calc_stats(cold)
    if thaw_st["n"] and cold_st["n"] and thaw_st["mean"] > 0 and cold_st["mean"] > 0:
        speedup = cold_st["mean"] / thaw_st["mean"]
        diff = cold_st["mean"] - thaw_st["mean"]
        pct = (diff / cold_st["mean"]) * 100
        print("  Comparison:")
        print(f"    CRIU thaw is {speedup:.1f}x faster ({pct:.0f}% reduction)")
        print(f"    Mean diff:   {diff:.0f} ms")
        print(f"    Median diff: {cold_st['median'] - thaw_st['median']:.0f} ms")
        print()

    # Phase breakdown
    print("-" * 80)
    print("  Phase Breakdown (mean ms)")
    print("-" * 80)
    phases = [("DNS", "t_dns_s"), ("TCP Connect", "t_connect_s"),
              ("TTFB", "t_ttfb_s"), ("Total", "t_total_s")]
    print(f"  {'phase':<14}  {'CRIU mean':>10}  {'CRIU CI95':>22}  {'Cold mean':>10}  {'Cold CI95':>22}")
    print(f"  {'-'*14}  {'-'*10}  {'-'*22}  {'-'*10}  {'-'*22}")
    for label, key in phases:
        cv = phase_values(all_rows, "criu_thaw", key)
        kv = phase_values(all_rows, "cold_start", key)
        if args.iqr:
            cv = iqr_filter(cv)
            kv = iqr_filter(kv)
        c_mean = fmt_ms(statistics.fmean(cv)) if cv else "n/a"
        k_mean = fmt_ms(statistics.fmean(kv)) if kv else "n/a"
        print(f"  {label:<14}  {c_mean:>10}  {fmt_ci(cv):>22}  {k_mean:>10}  {fmt_ci(kv):>22}")
    print()

    # HTTP codes
    print("-" * 80)
    print("  HTTP Status Codes")
    print("-" * 80)
    for mode, label in [("criu_thaw", "CRIU thaw"), ("cold_start", "Cold start")]:
        codes: Dict[str, int] = {}
        for r in all_rows:
            if r.get("mode") == mode:
                c = str(r.get("http_code", "?"))
                codes[c] = codes.get(c, 0) + 1
        print(f"  {label}: {codes}")
    print()

    # Plots
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fig_dir = os.path.join(SCRIPT_DIR, "figures")
    os.makedirs(fig_dir, exist_ok=True)

    if args.plot is not None:
        out = args.plot if args.plot != "auto" else os.path.join(fig_dir, f"rsu_fvc_boxplot_{ts}.svg")
        generate_boxplot(all_rows, out, args.iqr)

    if args.plot_ci is not None:
        out = args.plot_ci if args.plot_ci != "auto" else os.path.join(fig_dir, f"rsu_fvc_ci_{ts}.svg")
        generate_ci_plot(all_rows, out, args.iqr)

    if args.plot_timeline is not None:
        out = args.plot_timeline if args.plot_timeline != "auto" else os.path.join(fig_dir, f"rsu_fvc_timeline_{ts}.svg")
        generate_timeline_plot(all_runs, out, args.iqr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
