#!/usr/bin/env python3
"""Generate IEEE-style plots for the CNSM paper using seaborn."""

import json
import glob
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# Use seaborn defaults with serif font for IEEE style
sns.set_theme(style="whitegrid", font="serif", rc={
    'font.size': 8,
    'axes.labelsize': 8,
    'xtick.labelsize': 8,
    'ytick.labelsize': 8,
    'legend.fontsize': 8,
    'figure.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.05,
})

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
FIGURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'figures')
palette = sns.color_palette("colorblind")

def iqr_filter(data):
    q1, q3 = np.percentile(data, [25, 75])
    iqr = q3 - q1
    return [x for x in data if q1 - 1.5*iqr <= x <= q3 + 1.5*iqr]

# ── 1. Checkpoint/Restore vs Cold Start Box Plot ──
def plot_freeze_vs_coldstart():
    files = sorted(glob.glob(f'{BENCH_DIR}/freeze_vs_coldstart_logs/*.ndjson'))
    thaw_ttfb, cold_ttfb = [], []
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('mode') == 'criu_thaw' and 't_ttfb_s' in d:
                    thaw_ttfb.append(d['t_ttfb_s'] * 1000)
                elif d.get('mode') == 'cold_start' and 't_ttfb_s' in d:
                    cold_ttfb.append(d['t_ttfb_s'] * 1000)

    thaw_f = iqr_filter(thaw_ttfb)
    cold_f = iqr_filter(cold_ttfb)

    thaw_med = np.median(thaw_f)
    cold_med = np.median(cold_f)
    thaw_std = np.std(thaw_f)
    cold_std = np.std(cold_f)

    fig, ax = plt.subplots(figsize=(3.5, 2.4))
    bp = ax.boxplot([thaw_f, cold_f],
                    tick_labels=['Checkpoint\nRestore', 'Cold Start'],
                    widths=0.5,
                    patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    bp['boxes'][0].set_facecolor(palette[0])
    bp['boxes'][1].set_facecolor(palette[1])

    ax.annotate(f'{thaw_med:.0f} $\\pm$ {thaw_std:.0f} ms',
                xy=(1, thaw_med),
                xytext=(18, 8), textcoords='offset points',
                fontsize=8, ha='left')
    ax.annotate(f'{cold_med:.0f} $\\pm$ {cold_std:.0f} ms',
                xy=(2, cold_med),
                xytext=(-18, -18), textcoords='offset points',
                fontsize=8, ha='right')

    ymax = max(max(cold_f), max(thaw_f))
    ax.set_ylim(top=ymax * 1.08)
    ax.set_ylabel('TTFB (ms)')
    ax.grid(axis='y', alpha=0.3)
    fig.savefig(f'{FIGURES_DIR}/freeze_vs_coldstart.pdf')
    plt.close()
    print(f'Checkpoint/Restore: n={len(thaw_f)}, median={thaw_med:.0f}ms, std={thaw_std:.0f}ms')
    print(f'Cold Start:         n={len(cold_f)}, median={cold_med:.0f}ms, std={cold_std:.0f}ms')
    print(f'Speedup: {cold_med/thaw_med:.1f}x')

# ── 2. Pipeline Latency CDF (capture → forward start) ──
def plot_pipeline():
    files = sorted(glob.glob(f'{BENCH_DIR}/pipeline_logs/*.ndjson'))
    sniffer, retransmitter = {}, {}
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                k = d.get('bench_pair_key')
                if not k:
                    continue
                if d.get('component') == 'sniffer':
                    sniffer[k] = d
                elif d.get('component') == 'retransmitter':
                    retransmitter[k] = d

    keys = set(sniffer.keys()) & set(retransmitter.keys())
    latencies = []
    for k in keys:
        s, r = sniffer[k], retransmitter[k]
        cap = s.get('t_capture_unix_ns')
        fwd_start = r.get('t_forward_start_unix_ns')
        if cap and fwd_start:
            lat_ms = (fwd_start - cap) / 1e6
            if 0 < lat_ms < 10000:
                latencies.append(lat_ms)

    filt = iqr_filter(latencies)

    fig, ax = plt.subplots(figsize=(3.5, 2.4))
    sorted_lat = np.sort(filt)
    cdf = np.arange(1, len(sorted_lat)+1) / len(sorted_lat)
    ax.plot(sorted_lat, cdf, color=palette[0], linewidth=1.2)
    ax.set_xlabel('End-to-End Latency (ms)')
    ax.set_ylabel('CDF')

    med = np.median(filt)
    p99 = np.percentile(filt, 99)
    std = np.std(filt)

    ax.axvline(med, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.annotate(f'Median: {med:.1f} $\\pm$ {std:.1f} ms', xy=(med, 0.5),
                xytext=(10, 0), textcoords='offset points',
                fontsize=8, va='center')

    ax.axvline(p99, color='gray', linestyle=':', linewidth=0.8, alpha=0.7)
    ax.annotate(f'P99: {p99:.1f} ms', xy=(p99, 0.92),
                xytext=(14, -10), textcoords='offset points',
                fontsize=8, ha='right', va='center')

    ax.set_xlim(right=sorted_lat[-1] + 0.8)
    ax.grid(alpha=0.3)
    fig.savefig(f'{FIGURES_DIR}/pipeline_cdf.pdf')
    plt.close()
    print(f'Pipeline: n={len(filt)}, median={med:.1f}ms, std={std:.1f}ms, p99={p99:.1f}ms')

# ── 3. Handoff Box Plot ──
def plot_handoff():
    files = sorted(glob.glob(f'{BENCH_DIR}/handoff_bench_logs/*.ndjson') +
                    glob.glob(f'{BENCH_DIR}/handoff_bench_logs/old_metrics/*.ndjson'))
    scenarios = {}
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('handoff_phase') == 'Ready' and 'handoff_wall_ms' in d:
                    sc = d.get('scenario', 'unknown')
                    scenarios.setdefault(sc, []).append(d['handoff_wall_ms'])

    labels_map = {
        'worker2-freeze': 'VM→VM\nCheckpoint',
        'worker1-freeze': 'RSU→VM\nCheckpoint',
        'worker2-coldstart': 'VM→VM\nCold Start',
        'worker1-coldstart': 'RSU→VM\nCold Start',
        'rsu-a-coldstart': 'RSU→RSU\nCold Start',
    }
    order = ['worker2-freeze', 'worker1-freeze', 'worker2-coldstart', 'worker1-coldstart', 'rsu-a-coldstart']
    data = []
    labels = []
    stats = []
    for sc in order:
        if sc in scenarios:
            filt = [x / 1000 for x in iqr_filter(scenarios[sc])]
            data.append(filt)
            labels.append(labels_map.get(sc, sc))
            med = np.median(filt)
            std = np.std(filt)
            stats.append((med, std))
            print(f'Handoff {sc}: n={len(filt)}, median={med:.1f}s, std={std:.1f}s')

    fig, ax = plt.subplots(figsize=(5.0, 2.6))
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(palette[i])

    for i, (d, (med, std)) in enumerate(zip(data, stats), 1):
        q3 = np.percentile(d, 75)
        iqr = q3 - np.percentile(d, 25)
        top_whisker = max(v for v in d if v <= q3 + 1.5 * iqr)
        y_offset = 14 if i == len(data) else 8
        ax.annotate(f'{med:.1f} $\\pm$ {std:.1f} s',
                    xy=(i, top_whisker),
                    xytext=(0, y_offset), textcoords='offset points',
                    fontsize=8, ha='center')

    all_vals = [v for sublist in data for v in sublist]
    ax.set_ylim(top=max(all_vals) * 1.18)
    ax.set_ylabel('Handoff Time (s)')
    ax.grid(axis='y', alpha=0.3)
    fig.savefig(f'{FIGURES_DIR}/handoff_boxplot.pdf')
    plt.close()

if __name__ == '__main__':
    plot_freeze_vs_coldstart()
    plot_pipeline()
    plot_handoff()
    print('All plots generated.')
