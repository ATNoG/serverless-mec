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


# ── 1. Checkpoint/Restore vs Cold Start Box Plot ──
# Shows VM and RSU results side by side, ordered worst to best
def plot_freeze_vs_coldstart():
    # VM data
    vm_thaw, vm_cold = [], []
    for f in paper_files(f'{BENCH_DIR}/freeze_vs_coldstart_logs/*.ndjson'):
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('mode') == 'criu_thaw' and 't_ttfb_s' in d:
                    vm_thaw.append(d['t_ttfb_s'])
                elif d.get('mode') == 'cold_start' and 't_ttfb_s' in d:
                    vm_cold.append(d['t_ttfb_s'])

    # RSU data
    rsu_thaw, rsu_cold = [], []
    for f in paper_files(f'{BENCH_DIR}/freeze_vs_coldstart_rsu_logs/*.ndjson'):
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('mode') == 'criu_thaw' and 't_ttfb_s' in d:
                    rsu_thaw.append(d['t_ttfb_s'])
                elif d.get('mode') == 'cold_start' and 't_ttfb_s' in d:
                    rsu_cold.append(d['t_ttfb_s'])

    vm_cold_f = sigma2_filter(vm_cold)[:2000]
    vm_thaw_f = sigma2_filter(vm_thaw)[:2000]
    rsu_cold_f = sigma2_filter(rsu_cold)[:1000]
    rsu_thaw_f = sigma2_filter(rsu_thaw)[:1000]

    # Grouped by platform: VM first, then RSU
    all_data = [vm_cold_f, vm_thaw_f, rsu_cold_f, rsu_thaw_f]
    all_labels = ['VM\nCold Start', 'VM\nCheckpoint', 'RSU\nCold Start', 'RSU\nCheckpoint']
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

    fs_fvc = ANNOT_SIZE * 1.1
    # (anchor, xoff, yoff): 'top' anchors to max(d), 'bot' anchors to min(d)
    annot_cfg = [('top', 13, 8), ('top', 0, 20), ('bot', 0, -10), ('bot', -15, -2)]
    for i, d in enumerate(all_data):
        mean = np.mean(d)
        s = se(d)
        anchor, xoff, yoff = annot_cfg[i]
        y_anchor = max(d) if anchor == 'top' else min(d)
        va = 'bottom' if anchor == 'top' else 'top'
        ax.annotate(f'{mean:.2f} $\\pm$ {s:.2f} s',
                    xy=(i + 1, y_anchor),
                    xytext=(xoff, yoff), textcoords='offset points',
                    fontsize=fs_fvc, ha='center', va=va)

    ymax = max(max(d) for d in all_data)
    ax.set_ylim(top=ymax * 1.15)
    ax.set_ylabel('TTFB (s)', fontsize=fs_fvc)
    ax.tick_params(axis='both', labelsize=fs_fvc)
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

    sn_f = sigma2_filter(sn_processing)[:20000]
    net_f = sigma2_filter(network)[:20000]
    rt_f = sigma2_filter(rt_processing)[:20000]
    e2e_f = sigma2_filter(e2e)[:20000]

    fig, ax = plt.subplots(figsize=(COL_WIDTH, 2.4))
    data = [sn_f, net_f, rt_f, e2e_f]
    labels = ['Sniffer', 'Network\nTransit', 'Retrans-\nmitter', 'End-to-\nEnd']
    bp = ax.boxplot(data, tick_labels=labels, widths=0.5, patch_artist=True,
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='.', markersize=3, alpha=0.5))
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(palette[i])

    # Annotate means: stages above worst result, E2E below best result
    # Sniffer and Network Transit need horizontal offsets to avoid overlap
    stage_offsets = [(10, 8), (0, 18), (0, 8)]  # (x_offset, y_offset) for first 3 stages
    stage_ha = ['center', 'center', 'center']
    for i, d in enumerate(data, 1):
        mean = np.mean(d)
        s = se(d)
        if i < len(data):  # stages: above worst result
            xoff, yoff = stage_offsets[i - 1]
            ax.annotate(f'{mean:.1f} $\\pm$ {s:.2f} ms',
                        xy=(i, max(d)),
                        xytext=(xoff, yoff), textcoords='offset points',
                        fontsize=ANNOT_SIZE, ha=stage_ha[i - 1])
        else:  # E2E: below best result
            ax.annotate(f'{mean:.1f} $\\pm$ {s:.2f} ms',
                        xy=(i, min(d)),
                        xytext=(-10, -10), textcoords='offset points',
                        fontsize=ANNOT_SIZE, ha='center', va='top')

    ax.set_ylabel('Latency (ms)')
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.01, 0.99, 'Labels report mean ± standard error',
            transform=ax.transAxes, fontsize=BASE_FONT * 0.85,
            ha='left', va='top', style='italic', color='0.4')
    fig.savefig(f'{FIGURES_DIR}/pipeline_stages.pdf')
    plt.close()

    for label, d in zip(['Sniffer', 'Network', 'Retransmitter', 'E2E'], data):
        print(f'Pipeline {label}: n={len(d)}, mean={np.mean(d):.1f}ms, se={se(d):.2f}ms')


# ── 3. Migration Box Plot ──
# 4 scenarios: RSU cold + ckpt, VM cold + ckpt (worst to best)
def plot_handoff():
    files = sorted(glob.glob(f'{BENCH_DIR}/handoff_bench_logs/*.ndjson') +
                    glob.glob(f'{BENCH_DIR}/handoff_bench_logs/old_metrics/*.ndjson'))
    # For RSU scenarios, only use recent samples (last 3 days) to match
    # the current testbed configuration.
    RSU_DATE_CUTOFF = '2026-06-26'
    scenarios = {}
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                if d.get('handoff_phase') == 'Ready' and 'handoff_wall_ms' in d:
                    sc = d.get('scenario', 'unknown')
                    if sc.startswith('rsu-'):
                        ts = d.get('ts_before', '')
                        if ts < RSU_DATE_CUTOFF:
                            continue
                    scenarios.setdefault(sc, []).append(d['handoff_wall_ms'])

    labels_map = {
        'rsu-a-coldstart': 'RSU\nCold Start',
        'rsu-a-freeze': 'RSU\nCheckpoint',
        'worker1-coldstart': 'VM\nCold Start',
        'worker1-freeze': 'VM\nCheckpoint',
    }
    # Grouped by platform: VM first, then RSU
    order = ['worker1-coldstart', 'worker1-freeze', 'rsu-a-coldstart', 'rsu-a-freeze']
    colors = [palette[2], palette[4], palette[1], palette[0]]
    data = []
    labels = []
    stats = []
    for sc in order:
        if sc in scenarios:
            raw = scenarios[sc]
            cap = 500 if sc.startswith('rsu-') else 1000
            filt = [x / 1000 for x in sigma2_filter(raw)][:cap]
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

    annot_off = [(0, 8), (0, 16), (0, 6), (0, 8)]
    for i, (d, (mean, s)) in enumerate(zip(data, stats), 1):
        xoff, yoff = annot_off[i - 1]
        ax.annotate(f'{mean:.1f} $\\pm$ {s:.2f} s',
                    xy=(i, max(d)),
                    xytext=(xoff, yoff), textcoords='offset points',
                    fontsize=BASE_FONT, ha='center')

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
    plot_handoff()
    print('All plots generated.')
