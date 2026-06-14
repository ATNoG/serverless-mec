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


def ci95_filter(data):
    """Remove outliers outside the 95% confidence interval (mean ± 1.96 * sd)."""
    if len(data) < 4:
        return data
    mean = np.mean(data)
    sd = np.std(data, ddof=1)
    lo, hi = mean - 1.96 * sd, mean + 1.96 * sd
    return [x for x in data if lo <= x <= hi]


# ── 1. Checkpoint/Restore vs Cold Start Box Plot ──
# Ordered worst to best (cold start first)
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

    thaw_f = ci95_filter(thaw_ttfb)[:2000]
    cold_f = ci95_filter(cold_ttfb)[:2000]

    thaw_mean = np.mean(thaw_f)
    cold_mean = np.mean(cold_f)
    thaw_std = np.std(thaw_f, ddof=1)
    cold_std = np.std(cold_f, ddof=1)

    # Order: worst to best (cold start, then checkpoint restore)
    fig, ax = plt.subplots(figsize=(3.5, 2.4))
    bp = ax.boxplot([cold_f, thaw_f],
                    tick_labels=['Cold Start', 'Checkpoint\nRestore'],
                    widths=0.5,
                    patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    bp['boxes'][0].set_facecolor(palette[1])
    bp['boxes'][1].set_facecolor(palette[0])

    # Cold start: right side, at ~80% of the worst result height
    cold_max = max(cold_f)
    ax.annotate(f'{cold_mean:.0f} $\\pm$ {cold_std:.0f} ms',
                xy=(1.3, cold_max * 0.80), xytext=(-20, 0), textcoords='offset points',
                fontsize=8, ha='left', va='center')
    # Checkpoint: directly above the worst result
    ax.annotate(f'{thaw_mean:.0f} $\\pm$ {thaw_std:.0f} ms',
                xy=(2, max(thaw_f)),
                xytext=(0, 10), textcoords='offset points',
                fontsize=8, ha='center')

    ymax = max(max(cold_f), max(thaw_f))
    ax.set_ylim(top=ymax * 1.12)
    ax.set_ylabel('TTFB (ms)')
    ax.grid(axis='y', alpha=0.3)
    fig.savefig(f'{FIGURES_DIR}/freeze_vs_coldstart.pdf')
    plt.close()
    print(f'Cold Start:         n={len(cold_f)}, mean={cold_mean:.0f}ms, std={cold_std:.0f}ms')
    print(f'Checkpoint/Restore: n={len(thaw_f)}, mean={thaw_mean:.0f}ms, std={thaw_std:.0f}ms')
    print(f'Speedup: {cold_mean/thaw_mean:.1f}x')


# ── 2. Pipeline Latency Stage Boxplot ──
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

    sn_processing = []  # capture -> send_end
    network = []        # send_end -> recv
    rt_processing = []  # recv -> forward_start
    e2e = []            # capture -> forward_start

    for k in keys:
        s, r = sniffer[k], retransmitter[k]
        t_cap = s.get('t_capture_unix_ns')
        t_se = s.get('t_send_end_unix_ns')
        t_rr = r.get('t_recv_unix_ns')
        t_fs = r.get('t_forward_start_unix_ns')

        if t_cap and t_se:
            v = (t_se - t_cap) / 1e6
            if 0 < v < 10000:
                sn_processing.append(v)
        if t_se and t_rr:
            v = (t_rr - t_se) / 1e6
            if 0 < v < 10000:
                network.append(v)
        if t_rr and t_fs:
            v = (t_fs - t_rr) / 1e6
            if 0 < v < 10000:
                rt_processing.append(v)
        if t_cap and t_fs:
            v = (t_fs - t_cap) / 1e6
            if 0 < v < 10000:
                e2e.append(v)

    sn_f = ci95_filter(sn_processing)[:20000]
    net_f = ci95_filter(network)[:20000]
    rt_f = ci95_filter(rt_processing)[:20000]
    e2e_f = ci95_filter(e2e)[:20000]

    fig, ax = plt.subplots(figsize=(3.5, 2.4))
    data = [sn_f, net_f, rt_f, e2e_f]
    labels = ['Sniffer', 'Network\nTransit', 'Retrans-\nmitter', 'End-to-\nEnd']
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(palette[i])

    # Annotate means: stages above worst result, E2E below best result
    for i, d in enumerate(data, 1):
        mean = np.mean(d)
        std = np.std(d, ddof=1)
        if i < len(data):  # stages: above worst result
            ax.annotate(f'{mean:.1f} $\\pm$ {std:.1f} ms',
                        xy=(i, max(d)),
                        xytext=(0, 8), textcoords='offset points',
                        fontsize=7, ha='center')
        else:  # E2E: below best result
            ax.annotate(f'{mean:.1f} $\\pm$ {std:.1f} ms',
                        xy=(i, min(d)),
                        xytext=(0, -10), textcoords='offset points',
                        fontsize=7, ha='center', va='top')

    ax.set_ylabel('Latency (ms)')
    ax.grid(axis='y', alpha=0.3)
    fig.savefig(f'{FIGURES_DIR}/pipeline_stages.pdf')
    plt.close()

    for label, d in zip(['Sniffer', 'Network', 'Retransmitter', 'E2E'], data):
        print(f'Pipeline {label}: n={len(d)}, mean={np.mean(d):.1f}ms, std={np.std(d, ddof=1):.1f}ms')


# ── 3. Handoff Box Plot ──
# Ordered worst to best
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
        'rsu-a-coldstart': 'RSU→RSU\nCold Start',
        'worker1-coldstart': 'VM→VM\nCold Start',
        'worker2-coldstart': 'RSU→VM\nCold Start',
        'worker1-freeze': 'VM→VM\nCheckpoint',
        'worker2-freeze': 'RSU→VM\nCheckpoint',
    }
    # Worst to best
    order = ['rsu-a-coldstart', 'worker1-coldstart', 'worker2-coldstart', 'worker1-freeze', 'worker2-freeze']
    data = []
    labels = []
    stats = []
    for sc in order:
        if sc in scenarios:
            filt = [x / 1000 for x in ci95_filter(scenarios[sc])][:1000]
            data.append(filt)
            labels.append(labels_map.get(sc, sc))
            mean = np.mean(filt)
            std = np.std(filt, ddof=1)
            stats.append((mean, std))
            print(f'Handoff {sc}: n={len(filt)}, mean={mean:.1f}s, std={std:.1f}s')

    fig, ax = plt.subplots(figsize=(5.0, 2.6))
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(palette[i])

    for i, (d, (mean, std)) in enumerate(zip(data, stats), 1):
        ax.annotate(f'{mean:.1f} $\\pm$ {std:.1f} s',
                    xy=(i, max(d)),
                    xytext=(0, 8), textcoords='offset points',
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
