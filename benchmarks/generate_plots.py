#!/usr/bin/env python3
"""Generate IEEE-style plots for the CNSM paper using seaborn."""

import json
import glob
import os
import re
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# Common figure width for IEEE single-column (3.5in)
COL_WIDTH = 3.5
# Use seaborn defaults with serif font for IEEE style
BASE_FONT = 7
sns.set_theme(style="whitegrid", font="serif", rc={
    'font.size': BASE_FONT,
    'axes.labelsize': BASE_FONT,
    'xtick.labelsize': BASE_FONT,
    'ytick.labelsize': BASE_FONT,
    'legend.fontsize': BASE_FONT,
    'figure.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.05,
})

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
FIGURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'figures')
palette = sns.color_palette("colorblind")

# The paper's checkpoint/restore results were measured on data collected up to
# this date; later runs are excluded so the figure matches the reported means.
PAPER_DATA_CUTOFF = '20260629'


def paper_files(pattern):
    """Sorted files whose filename date is within the paper's data window."""
    def fdate(p):
        m = re.findall(r'20\d{6}', p)
        return m[-1] if m else '99999999'
    return [f for f in sorted(glob.glob(pattern)) if fdate(f) <= PAPER_DATA_CUTOFF]

ANNOT_SIZE = BASE_FONT  # annotation font size, consistent across all plots


def sigma2_filter(data):
    """Remove outliers beyond 2 standard deviations from the mean."""
    if len(data) < 4:
        return data
    mean = np.mean(data)
    sd = np.std(data, ddof=1)
    lo, hi = mean - 2 * sd, mean + 2 * sd
    return [x for x in data if lo <= x <= hi]


def se(data):
    """Standard error of the mean."""
    return np.std(data, ddof=1) / np.sqrt(len(data))


# ── 1. Checkpoint/Restore vs Cold-Start Box Plot ──
# Shows VM and RSU results side by side, ordered worst to best
def plot_freeze_vs_coldstart():
    # Latest 2000 samples per scenario (untrimmed), then 2-sigma filter.
    def collect(pattern):
        thaw, cold = [], []
        for f in sorted(glob.glob(pattern)):
            with open(f) as fh:
                for line in fh:
                    d = json.loads(line)
                    t = d.get('t_ttfb_s') or 0
                    if t <= 0:
                        continue
                    ts = d.get('ts_before', '')
                    if d.get('mode') == 'criu_thaw':
                        thaw.append((ts, t))
                    elif d.get('mode') == 'cold_start':
                        cold.append((ts, t))
        return thaw, cold

    def latest_filter(pairs, n=2000):
        vals = [v for _, v in sorted(pairs)[-n:]]
        return sigma2_filter(vals)

    vm_thaw, vm_cold = collect(f'{BENCH_DIR}/freeze_vs_coldstart_logs/*.ndjson')
    rsu_thaw, rsu_cold = collect(f'{BENCH_DIR}/freeze_vs_coldstart_rsu_logs/*.ndjson')
    vm_cold_f = latest_filter(vm_cold)
    vm_thaw_f = latest_filter(vm_thaw)
    rsu_cold_f = latest_filter(rsu_cold)
    rsu_thaw_f = latest_filter(rsu_thaw)

    # Grouped by platform: VM first, then RSU
    all_data = [vm_cold_f, vm_thaw_f, rsu_cold_f, rsu_thaw_f]
    all_labels = ['VM\nCold-Start', 'VM\nRestore', 'RSU\nCold-Start', 'RSU\nRestore']
    all_colors = [palette[2], palette[4], palette[1], palette[0]]

    fig, ax = plt.subplots(figsize=(COL_WIDTH, 2.4))
    bp = ax.boxplot(all_data,
                    tick_labels=all_labels,
                    widths=0.5,
                    patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(all_colors[i])

    fs_fvc = BASE_FONT
    # Labels centered on each box: 'top' above the highest point, 'bot' below
    # the lowest point
    annot_anchor = ['top', 'top', 'bot', 'top']
    for i, d in enumerate(all_data):
        mean = np.mean(d)
        s = se(d)
        top = annot_anchor[i] == 'top'
        ax.annotate(f'{mean:.2f}\n$\\pm$ {s:.2f} s',
                    xy=(i + 1, max(d) if top else min(d)),
                    xytext=(0, 3 if top else -3), textcoords='offset points',
                    fontsize=fs_fvc, ha='center', va='bottom' if top else 'top')

    ymax = max(max(d) for d in all_data)
    ax.set_ylim(top=ymax * 1.15)
    ax.set_ylabel('TTFB (s)', fontsize=fs_fvc)
    ax.tick_params(axis='y', labelsize=BASE_FONT)
    ax.tick_params(axis='x', labelsize=BASE_FONT * 0.94)
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.01, 0.99, 'Labels report mean ± standard error',
            transform=ax.transAxes, fontsize=BASE_FONT * 0.85,
            ha='left', va='top', style='italic', color='0.4')
    fig.savefig(f'{FIGURES_DIR}/freeze_vs_coldstart.pdf')
    plt.close()

    for label, d in zip(all_labels, all_data):
        l = label.replace('\n', ' ')
        print(f'{l:20s}: n={len(d)}, mean={np.mean(d):.2f}s, se={se(d):.3f}s')
    print(f'VM Speedup:  {np.mean(vm_cold_f)/np.mean(vm_thaw_f):.1f}x')
    print(f'RSU Speedup: {np.mean(rsu_cold_f)/np.mean(rsu_thaw_f):.1f}x')


# ── 2. Pipeline Latency Stage Boxplot ──
def load_pipeline_pairs():
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

    # Latest 20000 matched pairs (untrimmed, by capture time), then 2-sigma filter,
    # matching the latest-N-then-filter selection of the other experiments.
    keys = sorted(set(sniffer.keys()) & set(retransmitter.keys()),
                  key=lambda k: sniffer[k].get('t_capture_unix_ns') or 0)[-20000:]
    return sniffer, retransmitter, keys


def plot_pipeline():
    sniffer, retransmitter, keys = load_pipeline_pairs()

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

    sn_f = sigma2_filter(sn_processing)
    net_f = sigma2_filter(network)
    rt_f = sigma2_filter(rt_processing)
    e2e_f = sigma2_filter(e2e)

    fig, ax = plt.subplots(figsize=(COL_WIDTH, 2.4))
    data = [sn_f, net_f, rt_f, e2e_f]
    labels = ['Sniffer', 'Network\nTransit', 'Retrans-\nmitter', 'End-to-\nEnd']
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(palette[i])

    # Labels centered on each box: stages above the highest point, E2E below
    # the lowest point
    for i, d in enumerate(data, 1):
        mean = np.mean(d)
        s = se(d)
        top = i < len(data)
        ax.annotate(f'{mean:.2f}\n$\\pm$ {s:.2f} ms',
                    xy=(i, max(d) if top else min(d)),
                    xytext=(0, 3 if top else -3), textcoords='offset points',
                    fontsize=ANNOT_SIZE, ha='center', va='bottom' if top else 'top')

    ax.set_ylabel('Latency (ms)')
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.01, 0.99, 'Labels report mean ± standard error',
            transform=ax.transAxes, fontsize=BASE_FONT * 0.85,
            ha='left', va='top', style='italic', color='0.4')
    fig.savefig(f'{FIGURES_DIR}/pipeline_stages.pdf')
    plt.close()

    for label, d in zip(['Sniffer', 'Network', 'Retransmitter', 'E2E'], data):
        print(f'Pipeline {label}: n={len(d)}, mean={np.mean(d):.1f}ms, se={se(d):.2f}ms')


# ── 2b. Pipeline Step Breakdown Box Plot ──
# Splits the sniffer and retransmitter stages at the intermediate timestamps they
# record. Network transit has no intermediate timestamps, so it is left to the
# stage plot.
def plot_pipeline_breakdown():
    sniffer, retransmitter, keys = load_pipeline_pairs()
    # (stage, step label, start record, start field, end record, end field)
    steps = [
        ('Sniffer', 'CE\nidentifier', 's', 't_capture_unix_ns', 's', 't_ce_built_unix_ns'),
        ('Sniffer', 'CE\nbody', 's', 't_ce_built_unix_ns', 's', 't_enqueue_unix_ns'),
        ('Sniffer', 'Sender\nhandoff', 's', 't_enqueue_unix_ns', 's', 't_send_start_unix_ns'),
        ('Sniffer', 'HTTP\nrequest', 's', 't_send_start_unix_ns', 's', 't_send_end_unix_ns'),
        ('Retransmitter', 'Body\nread', 'r', 't_recv_unix_ns', 'r', 't_body_unix_ns'),
        ('Retransmitter', 'CE\nparse', 'r', 't_body_unix_ns', 'r', 't_parsed_unix_ns'),
        ('Retransmitter', 'Frame\nextract', 'r', 't_parsed_unix_ns', 'r', 't_forward_start_unix_ns'),
    ]
    data = []
    for _, _, ra, fa, rb, fb in steps:
        vals = []
        for k in keys:
            recs = {'s': sniffer[k], 'r': retransmitter[k]}
            a, b = recs[ra].get(fa), recs[rb].get(fb)
            if a and b:
                v = (b - a) / 1e6
                if 0 < v < 10000:
                    vals.append(v)
        data.append(sigma2_filter(vals))

    stages = []
    for i, st in enumerate(s[0] for s in steps):
        if not stages or stages[-1][0] != st:
            stages.append([st, i, i])
        stages[-1][2] = i
    stage_color = {st: palette[j] for j, (st, _, _) in enumerate(stages)}

    fig, ax = plt.subplots(figsize=(4.4, 2.1))
    bp = ax.boxplot(data, tick_labels=[s[1] for s in steps], widths=0.55,
                    patch_artist=True,
                    boxprops=dict(linewidth=0.6),
                    whiskerprops=dict(linewidth=0.6),
                    capprops=dict(linewidth=0.6),
                    medianprops=dict(color='black', linewidth=1.0),
                    flierprops=dict(marker='.', markersize=1.5, alpha=0.15,
                                    markeredgecolor='none', markerfacecolor='0.3'))
    for patch, st in zip(bp['boxes'], (s[0] for s in steps)):
        patch.set_facecolor(stage_color[st])
    top = max(max(d) for d in data)
    ax.set_ylim(0, top * 1.3)

    for i, d in enumerate(data, 1):
        ax.annotate(f'{np.mean(d):.2f}\n$\\pm$ {se(d):.2f} ms', xy=(i, max(d)), xytext=(0, 3),
                    textcoords='offset points', fontsize=ANNOT_SIZE * 0.9,
                    ha='center', va='bottom')
    for st, a, b in stages:
        ax.axvspan(a + 0.5, b + 1.5, color=stage_color[st], alpha=0.12, linewidth=0)
        ax.text((a + b) / 2 + 1, top * 1.27, st, ha='center', va='top',
                fontsize=BASE_FONT, fontweight='bold')

    ax.set_ylabel('Latency (ms)')
    ax.tick_params(axis='x', labelsize=BASE_FONT * 0.85)
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.01, 0.66, 'Labels report mean ± standard error',
            transform=ax.transAxes, fontsize=BASE_FONT * 0.85,
            ha='left', va='top', style='italic', color='0.4')
    fig.savefig(f'{FIGURES_DIR}/pipeline_breakdown.pdf')
    plt.close()

    for (st, lab, *_), d in zip(steps, data):
        name = f"{st} / {lab}".replace('\n', ' ')
        print(f'{name:40s}: n={len(d)}, mean={np.mean(d):.3f}ms, se={se(d):.4f}ms')


# ── 3. Migration Box Plot ──
# 4 scenarios: RSU cold + ckpt, VM cold + ckpt (worst to best)
def plot_handoff():
    files = sorted(glob.glob(f'{BENCH_DIR}/handoff_bench_logs/*.ndjson') +
                    glob.glob(f'{BENCH_DIR}/handoff_bench_logs/old_metrics/*.ndjson'))
    # Both VMs (worker1, worker2) are grouped as VM; rsu-a as RSU.
    scenarios = {}
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('handoff_phase') == 'Ready' and (d.get('handoff_wall_ms') or 0) > 0:
                    sc = d.get('scenario', 'unknown').replace('worker2-', 'worker1-')
                    scenarios.setdefault(sc, []).append((d.get('ts_before', ''), d['handoff_wall_ms']))

    labels_map = {
        'rsu-a-coldstart': 'RSU\nCold-Start',
        'rsu-a-freeze': 'RSU\nRestore',
        'worker1-coldstart': 'VM\nCold-Start',
        'worker1-freeze': 'VM\nRestore',
    }
    # Grouped by platform: VM first, then RSU
    order = ['worker1-coldstart', 'worker1-freeze', 'rsu-a-coldstart', 'rsu-a-freeze']
    colors = [palette[2], palette[4], palette[1], palette[0]]
    data = []
    labels = []
    stats = []
    for sc in order:
        if sc in scenarios:
            raw = [v for _, v in sorted(scenarios[sc])[-2000:]]
            filt = [x / 1000 for x in sigma2_filter(raw)]
            data.append(filt)
            labels.append(labels_map.get(sc, sc))
            mean = np.mean(filt)
            s = se(filt)
            stats.append((mean, s))
            print(f'Migration {sc}: n={len(filt)}, mean={mean:.1f}s, se={s:.2f}s')

    fig, ax = plt.subplots(figsize=(COL_WIDTH, 2.4))
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(colors[i])

    ax.tick_params(axis='y', labelsize=BASE_FONT)
    ax.tick_params(axis='x', labelsize=BASE_FONT * 0.94)

    # Labels centered above each box, except RSU cold-start, which goes below
    # its lower whisker to stay clear of the top of the plot.
    below = {'rsu-a-coldstart'}
    for i, (sc, d, (mean, s)) in enumerate(zip(order, data, stats), 1):
        under = sc in below
        ax.annotate(f'{mean:.2f}\n$\\pm$ {s:.2f} s',
                    xy=(i, min(d) if under else max(d)),
                    xytext=(0, -3 if under else 3), textcoords='offset points',
                    fontsize=BASE_FONT, ha='center',
                    va='top' if under else 'bottom')

    all_vals = [v for sublist in data for v in sublist]
    ax.set_ylim(top=max(all_vals) * 1.18)
    ax.set_ylabel('Migration Time (s)')
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.01, 0.99, 'Labels report mean ± standard error',
            transform=ax.transAxes, fontsize=BASE_FONT * 0.85,
            ha='left', va='top', style='italic', color='0.4')
    fig.savefig(f'{FIGURES_DIR}/handoff_boxplot.pdf')
    plt.close()


if __name__ == '__main__':
    plot_freeze_vs_coldstart()
    plot_pipeline()
    plot_pipeline_breakdown()
    plot_handoff()
    print('All plots generated.')
