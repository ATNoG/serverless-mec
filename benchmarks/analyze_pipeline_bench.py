#!/usr/bin/env python3
"""
analyze_pipeline_bench.py

Analyzes a combined pipeline bench NDJSON file produced by
`capture_pipeline_bench.sh`.

The combined file mixes sniffer + retransmitter rows. Each row carries
a `bench_pair_key` of the form "<run_id>::<ce_id>" so multiple capture
sessions can coexist in one file without ce_id collisions.

This script demuxes the combined file into per-component temp files
(rewriting `ce_id` to `bench_pair_key` so the matching key is globally
unique), then delegates to the existing `analyze_bench.py` for the
heavy lifting.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from typing import Iterable, List, Optional


HERE = os.path.dirname(os.path.abspath(__file__))
ANALYZE_BENCH = os.path.join(HERE, "analyze_bench.py")


def _iter_rows(paths: Iterable[str]):
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
    counts = {}
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


def _split_to_temp(
    paths: List[str],
    run_id_filter: Optional[str],
) -> tuple[str, str, int, int]:
    sn_fd, sn_path = tempfile.mkstemp(prefix="pipeline_sn_", suffix=".ndjson")
    rt_fd, rt_path = tempfile.mkstemp(prefix="pipeline_rt_", suffix=".ndjson")
    sn_n = rt_n = 0
    with os.fdopen(sn_fd, "w") as sn_f, os.fdopen(rt_fd, "w") as rt_f:
        for r in _iter_rows(paths):
            if run_id_filter and r.get("run_id") != run_id_filter:
                continue
            src = r.get("_source") or r.get("component")
            if src not in ("sniffer", "retransmitter"):
                continue
            # Use the namespaced pair key as the matching id, so cross-run
            # ce_id collisions are impossible.
            pair = r.get("bench_pair_key")
            if pair:
                r["ce_id"] = pair
            line = json.dumps(r, separators=(",", ":")) + "\n"
            if src == "sniffer":
                sn_f.write(line)
                sn_n += 1
            else:
                rt_f.write(line)
                rt_n += 1
    return sn_path, rt_path, sn_n, rt_n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="combined pipeline NDJSON file(s)")
    ap.add_argument("--run-id", default="", help="analyze only rows with this run_id (default: all)")
    ap.add_argument("--list-runs", action="store_true", help="list run_ids found in the file(s) and exit")
    ap.add_argument("--csv", default="", help="optional CSV path forwarded to analyze_bench.py")
    ap.add_argument("--iqr", action="store_true",
                    help="filter outliers using Tukey's IQR fences (forwarded to analyze_bench.py)")
    ap.add_argument("--plot", nargs="?", const="auto", default=None,
                    help="generate box plot (forwarded to analyze_bench.py)")
    ap.add_argument("--plot-ci", nargs="?", const="auto", default=None,
                    help="generate bar chart with 95%% CI error bars (forwarded to analyze_bench.py)")
    args = ap.parse_args()

    if args.list_runs:
        _list_run_ids(args.inputs)
        return 0

    rid = args.run_id or None
    sn_path, rt_path, sn_n, rt_n = _split_to_temp(args.inputs, rid)

    print(f"[analyze] inputs           : {', '.join(args.inputs)}")
    if rid:
        print(f"[analyze] run_id filter    : {rid}")
    print(f"[analyze] sniffer rows     : {sn_n}")
    print(f"[analyze] retransmitter    : {rt_n}")
    print()

    if sn_n == 0 or rt_n == 0:
        print("[analyze] not enough rows to match — aborting")
        os.unlink(sn_path)
        os.unlink(rt_path)
        return 1

    cmd = [sys.executable, ANALYZE_BENCH, "--sniffer", sn_path, "--retrans", rt_path]
    if args.csv:
        cmd += ["--csv", args.csv]
    if args.iqr:
        cmd += ["--iqr"]
    if args.plot is not None:
        cmd += ["--plot"] if args.plot == "auto" else ["--plot", args.plot]
    if args.plot_ci is not None:
        cmd += ["--plot-ci"] if args.plot_ci == "auto" else ["--plot-ci", args.plot_ci]

    try:
        rc = subprocess.call(cmd)
    finally:
        os.unlink(sn_path)
        os.unlink(rt_path)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
