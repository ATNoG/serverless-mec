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


def _stats(vals: List[float]) -> Dict[str, float]:
    if not vals:
        nan = float("nan")
        return dict(n=0, mean=nan, median=nan, stdev=nan, p95=nan, p99=nan, min=nan, max=nan)
    s = sorted(vals)
    n = len(s)
    return dict(
        n=n,
        mean=statistics.fmean(s),
        median=statistics.median(s),
        stdev=statistics.stdev(s) if n > 1 else 0.0,
        p95=s[min(n - 1, int(round(n * 0.95)) - 1) if n > 1 else 0],
        p99=s[min(n - 1, int(round(n * 0.99)) - 1) if n > 1 else 0],
        min=s[0],
        max=s[-1],
    )


def _fmt_ms(x: float) -> str:
    return "n/a" if isinstance(x, float) and math.isnan(x) else f"{x:.1f} ms"


def _print_block(label: str, st: Dict[str, float]) -> None:
    print(f"  {label}:")
    for k in ("n", "mean", "median", "stdev", "p95", "p99", "min", "max"):
        v = st[k]
        val = str(v) if k == "n" else _fmt_ms(v)
        print(f"    {k:<7} = {val}")


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="freeze_vs_coldstart NDJSON file(s)")
    ap.add_argument("--per-iter", action="store_true",
                    help="print per-iteration table (default: skip if >50 samples)")
    args = ap.parse_args()

    rows = _read(args.inputs)
    if not rows:
        print("no rows found", file=sys.stderr)
        return 1

    criu = _values(rows, "criu_thaw")
    cold = _values(rows, "cold_start")

    c_st = _stats(criu)
    k_st = _stats(cold)

    print()
    print("=" * 60)
    print("  CRIU Thaw vs Cold Start — Benchmark Results")
    print("=" * 60)
    print(f"  inputs            : {', '.join(args.inputs)}")
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
    print(f"  {'phase':<14}  {'CRIU':>12}  {'Cold':>12}")
    print(f"  {'-'*14}  {'-'*12}  {'-'*12}")
    for label, key in phases:
        cv = _phase_values(rows, "criu_thaw", key)
        kv = _phase_values(rows, "cold_start", key)
        c_mean = statistics.fmean(cv) if cv else float("nan")
        k_mean = statistics.fmean(kv) if kv else float("nan")
        print(f"  {label:<14}  {_fmt_ms(c_mean):>12}  {_fmt_ms(k_mean):>12}")
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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
