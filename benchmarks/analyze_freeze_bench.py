#!/usr/bin/env python3
"""
analyze_freeze_bench.py

Standalone analyzer for the NDJSON results file produced by
`bench_freeze_vs_coldstart.sh`.

Each input row is an object like:
  {"mode":"criu_thaw","iteration":1,"t_dns_s":...,"t_connect_s":...,
   "t_ttfb_s":...,"t_total_s":0.642,"http_code":200,
   "ts_before":"...","ts_after":"..."}

Multiple result files can be passed at once and they will be aggregated.
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
    """Approximate inverse of Student's t CDF (two-tailed).

    Uses the Abramowitz & Stegun rational approximation for the normal
    quantile, then applies a Cornish-Fisher expansion to convert to
    the t-distribution.  Accurate to ~0.01 for df >= 2.
    """
    import math as _m
    # Normal quantile via A&S 26.2.23
    t_ = _m.sqrt(-2.0 * _m.log(1.0 - p))
    xp = t_ - (2.515517 + 0.802853 * t_ + 0.010328 * t_ ** 2) / \
              (1.0 + 1.432788 * t_ + 0.189269 * t_ ** 2 + 0.001308 * t_ ** 3)
    # Cornish-Fisher expansion for t
    g1 = (xp ** 3 + xp) / (4 * df)
    g2 = (5 * xp ** 5 + 16 * xp ** 3 + 3 * xp) / (96 * df ** 2)
    return xp + g1 + g2


def _ci95(vals: List[float]) -> Tuple[float, float]:
    """Return (lower, upper) bounds of the 95 % confidence interval for the mean."""
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


def _values(rows: List[dict], mode: str) -> List[float]:
    return [
        r["t_total_s"] * 1000.0
        for r in rows
        if r.get("mode") == mode
        and isinstance(r.get("t_total_s"), (int, float))
        and r["t_total_s"] > 0
        and r.get("http_code") not in (0, "0", None)
    ]


def _phase_values(rows: List[dict], mode: str, key: str) -> List[float]:
    out: List[float] = []
    for r in rows:
        if r.get("mode") != mode:
            continue
        v = r.get(key)
        if isinstance(v, (int, float)) and v > 0:
            out.append(v * 1000.0)
    return out


def _http_codes(rows: List[dict], mode: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        if r.get("mode") != mode:
            continue
        c = str(r.get("http_code", "?"))
        out[c] = out.get(c, 0) + 1
    return out


def _plot_phases(rows: List[dict], use_iqr: bool) -> Tuple:
    """Build phase data shared by both plot types."""
    import pandas as pd

    phases: List[Tuple[str, str]] = [
        ("DNS", "t_dns_s"),
        ("TCP Connect", "t_connect_s"),
        ("TTFB", "t_ttfb_s"),
        ("Total", "t_total_s"),
    ]
    palette = {"CRIU Thaw": "#2196F3", "Cold Start": "#4CAF50"}

    records = []
    for label, key in phases:
        for mode, mode_label in [("criu_thaw", "CRIU Thaw"), ("cold_start", "Cold Start")]:
            vals = _phase_values(rows, mode, key)
            if use_iqr:
                vals = _iqr_filter(vals)
            for v in vals:
                records.append({"Phase": label, "Scenario": mode_label, "Time (ms)": v})

    return pd.DataFrame(records), palette


def _generate_plot(rows: List[dict], output: str, use_iqr: bool) -> None:
    """Generate seaborn box plots comparing CRIU thaw vs Cold start."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _plot_phases(rows, use_iqr)
    if df.empty:
        print("  No data for plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(
        data=df, x="Phase", y="Time (ms)", hue="Scenario",
        palette=palette,
        showfliers=True, flierprops=dict(marker="o", markersize=4, alpha=0.5),
        ax=ax,
    )
    ax.set_title("CRIU Thaw vs Cold Start — Response Time Breakdown")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  Plot saved to {output}")


def _generate_ci_plot(rows: List[dict], output: str, use_iqr: bool) -> None:
    """Generate bar chart with 95% CI error bars comparing CRIU thaw vs Cold start."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns

    df, palette = _plot_phases(rows, use_iqr)
    if df.empty:
        print("  No data for CI plot", file=sys.stderr)
        return

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(
        data=df, x="Phase", y="Time (ms)", hue="Scenario",
        palette=palette, errorbar=("ci", 95), capsize=0.1,
        ax=ax,
    )
    ax.set_title("CRIU Thaw vs Cold Start — Mean Response Time (95% CI)")
    ax.legend(loc="upper left")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    print(f"  CI plot saved to {output}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="freeze_vs_coldstart NDJSON file(s)")
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

    criu_raw = _values(rows, "criu_thaw")
    cold_raw = _values(rows, "cold_start")

    criu = _iqr_filter(criu_raw) if args.iqr else criu_raw
    cold = _iqr_filter(cold_raw) if args.iqr else cold_raw

    c_st = _stats(criu)
    k_st = _stats(cold)

    print()
    print("=" * 60)
    print("  CRIU Thaw vs Cold Start — Benchmark Results")
    print("=" * 60)
    print(f"  inputs            : {', '.join(args.inputs)}")
    if args.iqr:
        print(f"  IQR filtering     : ON (CRIU {len(criu_raw)}->{len(criu)}, Cold {len(cold_raw)}->{len(cold)})")
    print(f"  CRIU thaw samples : {c_st['n']}")
    print(f"  Cold start samples: {k_st['n']}")
    print()

    print("-" * 60)
    print("  Total Response Time")
    print("-" * 60)
    _print_block("CRIU Thaw", c_st)
    print()
    _print_block("Cold Start", k_st)
    print()

    if c_st["n"] and k_st["n"] and c_st["mean"] > 0 and k_st["mean"] > 0:
        speedup = k_st["mean"] / c_st["mean"]
        diff = k_st["mean"] - c_st["mean"]
        print("  Comparison:")
        print(f"    CRIU thaw is {speedup:.1f}x faster than cold start (by mean)")
        print(f"    Mean difference   : {diff:.0f} ms")
        print(f"    Median difference : {k_st['median'] - c_st['median']:.0f} ms")
        print(f"    Medians           : CRIU {c_st['median']:.0f} ms  vs  Cold {k_st['median']:.0f} ms")
        print()

    # Curl phase breakdown
    print("-" * 60)
    print("  Curl Phase Breakdown (mean ms)")
    print("-" * 60)
    phases: List[Tuple[str, str]] = [
        ("DNS",        "t_dns_s"),
        ("TCP connect","t_connect_s"),
        ("TTFB",       "t_ttfb_s"),
        ("Total",      "t_total_s"),
    ]
    def _ci_str(vals: List[float]) -> str:
        lo, hi = _ci95(vals)
        if math.isnan(lo):
            return "n/a"
        return f"[{lo:.1f}, {hi:.1f}]"

    print(f"  {'phase':<14}  {'CRIU mean':>12}  {'CRIU 95%CI':>22}  {'Cold mean':>12}  {'Cold 95%CI':>22}")
    print(f"  {'-'*14}  {'-'*12}  {'-'*22}  {'-'*12}  {'-'*22}")
    for label, key in phases:
        cv = _phase_values(rows, "criu_thaw", key)
        kv = _phase_values(rows, "cold_start", key)
        if args.iqr:
            cv = _iqr_filter(cv)
            kv = _iqr_filter(kv)
        c_mean = statistics.fmean(cv) if cv else float("nan")
        k_mean = statistics.fmean(kv) if kv else float("nan")
        print(f"  {label:<14}  {_fmt_ms(c_mean):>12}  {_ci_str(cv):>22}  {_fmt_ms(k_mean):>12}  {_ci_str(kv):>22}")
    print()

    # HTTP status code distribution
    print("-" * 60)
    print("  HTTP Status Codes")
    print("-" * 60)
    print(f"  CRIU thaw : {_http_codes(rows, 'criu_thaw')}")
    print(f"  Cold start: {_http_codes(rows, 'cold_start')}")
    print()

    # Per-iteration table
    show_per_iter = args.per_iter or (len(criu) <= 50 and len(cold) <= 50)
    if show_per_iter:
        print("-" * 60)
        print("  Per-Iteration (ms)")
        print("-" * 60)
        print(f"  {'#':>3}  {'CRIU thaw':>12}  {'Cold start':>12}")
        print(f"  {'':>3}  {'-' * 12}  {'-' * 12}")
        m = max(len(criu), len(cold))
        for i in range(m):
            a = f"{criu[i]:.1f}" if i < len(criu) else "-"
            b = f"{cold[i]:.1f}" if i < len(cold) else "-"
            print(f"  {i + 1:3d}  {a:>12}  {b:>12}")
        print()
    else:
        print(f"  (per-iteration table suppressed; pass --per-iter to show all {max(len(criu), len(cold))} rows)")
        print()

    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.plot is not None:
        if args.plot == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            plot_path = f"{base}_boxplot_{ts}.svg"
        elif os.path.isdir(args.plot):
            plot_path = os.path.join(args.plot, f"freeze_vs_coldstart_boxplot_{ts}.svg")
        else:
            plot_path = args.plot
        _generate_plot(rows, plot_path, use_iqr=args.iqr)

    if args.plot_ci is not None:
        if args.plot_ci == "auto":
            base = os.path.splitext(args.inputs[0])[0]
            ci_path = f"{base}_ci_{ts}.svg"
        elif os.path.isdir(args.plot_ci):
            ci_path = os.path.join(args.plot_ci, f"freeze_vs_coldstart_ci_{ts}.svg")
        else:
            ci_path = args.plot_ci
        _generate_ci_plot(rows, ci_path, use_iqr=args.iqr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
